package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// A10 D1 review: the JailedImager's closed graph and output authority. The
// exact expected backing name comes from the Store (ImageRef.ExpectedBacking),
// never from the guest-influenced header; the header is only compared, before
// and after the tool runs, and the tool gets the verified base as an explicit
// fd. Every output byte is bounded at collection.

// storageQcow writes a qcow2 v3 header of virtual size with backing name.
func storageQcow(t *testing.T, p, backing string, size uint64) {
	t.Helper()
	raw := make([]byte, 512+len(backing))
	copy(raw, "QFI\xfb")
	binary.BigEndian.PutUint32(raw[4:8], 3)
	if backing != "" {
		binary.BigEndian.PutUint64(raw[8:16], 512)
		binary.BigEndian.PutUint32(raw[16:20], uint32(len(backing)))
		copy(raw[512:], backing)
	}
	binary.BigEndian.PutUint32(raw[20:24], 16)
	binary.BigEndian.PutUint64(raw[24:32], size)
	binary.BigEndian.PutUint32(raw[100:104], 104)
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// storageToolRunner is a fake ToolRunner: it records requests, writes the
// configured output through the request's writers (stopping at the first
// write error, as a real copy does) and returns the configured outcome.
type storageToolRunner struct {
	mu       sync.Mutex
	requests []ToolRequest
	stdout   []byte
	stderr   []byte
	exit     int
	err      error
	during   func(req ToolRequest)
	held     map[[2]uint64]bool
}

func (r *storageToolRunner) RunTool(_ context.Context, req ToolRequest) (ToolOutcome, error) {
	r.mu.Lock()
	r.requests = append(r.requests, req)
	stdout, stderr, exit, err, during := r.stdout, r.stderr, r.exit, r.err, r.during
	r.mu.Unlock()
	if during != nil {
		during(req)
	}
	for chunk := range slicesChunk(stdout, 64<<10) {
		if req.Stdout == nil {
			break
		}
		if _, werr := req.Stdout.Write(chunk); werr != nil {
			break
		}
	}
	if req.Stderr != nil {
		_, _ = req.Stderr.Write(stderr)
	}
	return ToolOutcome{ExitCode: exit}, err
}

func slicesChunk(b []byte, n int) func(func([]byte) bool) {
	return func(yield func([]byte) bool) {
		for len(b) > 0 {
			k := min(n, len(b))
			if !yield(b[:k]) {
				return
			}
			b = b[k:]
		}
	}
}

func (r *storageToolRunner) TargetHeld(dev, ino uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.held[[2]uint64{dev, ino}]
}

func (r *storageToolRunner) calls() []ToolRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ToolRequest(nil), r.requests...)
}

const storageBaseName = "/var/lib/bbraft/images/v1/root.qcow2"

// storageImagerFixture creates a standalone base and an overlay naming it.
type storageImagerFixture struct {
	dir           string
	base, overlay *os.File
	overlayPath   string
}

func newStorageImagerFixture(t *testing.T, stored string) *storageImagerFixture {
	t.Helper()
	dir := t.TempDir()
	f := &storageImagerFixture{dir: dir, overlayPath: filepath.Join(dir, "disk.qcow2")}
	storageQcow(t, filepath.Join(dir, "root.qcow2"), "", 10<<30)
	storageQcow(t, f.overlayPath, stored, 10<<30)
	open := func(p string, flag int) *os.File {
		file, err := os.OpenFile(p, flag, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { file.Close() })
		return file
	}
	f.base = open(filepath.Join(dir, "root.qcow2"), os.O_RDONLY)
	f.overlay = open(f.overlayPath, os.O_RDWR)
	return f
}

func (f *storageImagerFixture) ref() ImageRef {
	return ImageRef{File: f.overlay, Root: ImageRootData, Path: "vms/bx_11111111/disk.qcow2", Format: "qcow2",
		ExpectedBacking: storageBaseName, Base: f.base}
}

func storageInfoJSON(format string, size uint64, backing string) []byte {
	if backing == "" {
		return fmt.Appendf(nil, `{"format":%q,"virtual-size":%d}`, format, size)
	}
	return fmt.Appendf(nil, `{"format":%q,"virtual-size":%d,"backing-filename":%q}`, format, size, backing)
}

func TestJailedImagerExpectedBackingAuthority(t *testing.T) {
	t.Run("overlay of its known base", func(t *testing.T) {
		f := newStorageImagerFixture(t, storageBaseName)
		runner := &storageToolRunner{stdout: storageInfoJSON("qcow2", 10<<30, storageBaseName)}
		info, err := NewJailedImager(runner).Info(t.Context(), f.ref())
		if err != nil {
			t.Fatal(err)
		}
		if info.Format != "qcow2" || info.VirtualSize != 10<<30 || info.BackingFile != storageBaseName {
			t.Fatalf("info %+v", info)
		}
		reqs := runner.calls()
		// The base is pinned and proven standalone by the Store, but info
		// never opens a backing, so the tool gets the target alone.
		if len(reqs) != 1 || reqs[0].Op != "info" || reqs[0].Driver != "qcow2" || reqs[0].Access != ImageGroupRO ||
			reqs[0].Target.Base != nil || reqs[0].Target.File != f.overlay {
			t.Fatalf("request %+v", reqs)
		}
	})
	for _, stored := range []string{
		"/etc/shadow",
		`json:{"driver":"file","filename":"/etc/shadow"}`,
		"nbd://203.0.113.10/export",
		"../root.qcow2",
		"root.qcow2",
		storageBaseName + "\x00",
		"",
	} {
		t.Run("stored backing "+strings.ToValidUTF8(stored, "?"), func(t *testing.T) {
			f := newStorageImagerFixture(t, stored)
			runner := &storageToolRunner{stdout: storageInfoJSON("qcow2", 10<<30, storageBaseName)}
			im := NewJailedImager(runner)
			if _, err := im.Info(t.Context(), f.ref()); err == nil {
				t.Fatal("Info accepted a stored backing name the Store did not expect")
			}
			if err := im.Check(t.Context(), f.ref()); err == nil {
				t.Fatal("Check accepted it")
			}
			if err := im.Grow(t.Context(), f.ownerRef(), 20<<30); err == nil {
				t.Fatal("Grow accepted it")
			}
			if n := len(runner.calls()); n != 0 {
				t.Fatalf("%d tool runs started before the precheck refused", n)
			}
		})
	}
	t.Run("standalone expected but header names a backing", func(t *testing.T) {
		f := newStorageImagerFixture(t, storageBaseName)
		ref := f.ref()
		ref.ExpectedBacking, ref.Base = "", nil
		runner := &storageToolRunner{stdout: storageInfoJSON("qcow2", 10<<30, "")}
		if _, err := NewJailedImager(runner).Info(t.Context(), ref); err == nil || len(runner.calls()) != 0 {
			t.Fatalf("Info = %v with %d runs", err, len(runner.calls()))
		}
	})
	t.Run("overlay without its pinned base", func(t *testing.T) {
		f := newStorageImagerFixture(t, storageBaseName)
		ref := f.ref()
		ref.Base = nil
		runner := &storageToolRunner{}
		if _, err := NewJailedImager(runner).Info(t.Context(), ref); err == nil || len(runner.calls()) != 0 {
			t.Fatal("an overlay without an explicit base was run")
		}
	})
	t.Run("base is itself an overlay", func(t *testing.T) {
		f := newStorageImagerFixture(t, storageBaseName)
		storageQcow(t, filepath.Join(f.dir, "root.qcow2"), "/elsewhere.qcow2", 10<<30)
		runner := &storageToolRunner{}
		if _, err := NewJailedImager(runner).Info(t.Context(), f.ref()); err == nil || len(runner.calls()) != 0 {
			t.Fatal("a base with its own backing chain was accepted")
		}
	})
}

func (f *storageImagerFixture) ownerRef() ImageRef {
	ref := f.ref()
	ref.File = f.overlay
	return ref
}

// TestJailedImagerHeaderChangedAfterPrecheck: a header rewritten while the
// tool runs (after the precheck) fails the operation; the request carried
// the explicit base, so the stored name was never the graph.
func TestJailedImagerHeaderChangedAfterPrecheck(t *testing.T) {
	for _, op := range []string{"info", "check", "resize"} {
		t.Run(op, func(t *testing.T) {
			f := newStorageImagerFixture(t, storageBaseName)
			runner := &storageToolRunner{stdout: storageInfoJSON("qcow2", 10<<30, storageBaseName)}
			if op == "check" {
				runner.stdout = []byte(`{"check-errors":0}`)
			}
			runner.during = func(ToolRequest) { storageQcow(t, f.overlayPath, "/etc/shadow", 10<<30) }
			im := NewJailedImager(runner)
			var err error
			switch op {
			case "info":
				_, err = im.Info(t.Context(), f.ref())
			case "check":
				err = im.Check(t.Context(), f.ref())
			case "resize":
				err = im.Grow(t.Context(), f.ownerRef(), 20<<30)
			}
			if err == nil {
				t.Fatal("a header changed after the precheck was accepted")
			}
			wantBase := f.base
			if op == "info" {
				wantBase = nil // info opens no backing
			}
			if reqs := runner.calls(); len(reqs) != 1 || reqs[0].Target.Base != wantBase {
				t.Fatalf("the run's base is %v, want %v: %+v", reqs[0].Target.Base, wantBase, reqs)
			}
		})
	}
}

func TestJailedImagerOutputAuthority(t *testing.T) {
	cases := map[string]struct {
		stdout []byte
		stderr []byte
		exit   int
		want   string // substring the error must contain; "" means success
	}{
		"valid":            {stdout: storageInfoJSON("qcow2", 10<<30, storageBaseName)},
		"oversized stdout": {stdout: bytes.Repeat([]byte(" "), 2<<20), want: "exceeds"},
		"malformed":        {stdout: []byte(`{"format":`), want: "json"},
		"trailing value":   {stdout: append(storageInfoJSON("qcow2", 10<<30, storageBaseName), []byte(`{}`)...), want: "json"},
		"trailing garbage": {stdout: append(storageInfoJSON("qcow2", 10<<30, storageBaseName), []byte(` x`)...), want: "json"},
		"wrong format":     {stdout: storageInfoJSON("raw", 10<<30, storageBaseName), want: "format"},
		"wrong backing":    {stdout: storageInfoJSON("qcow2", 10<<30, "/etc/shadow"), want: "backing"},
		"zero size":        {stdout: storageInfoJSON("qcow2", 0, storageBaseName), want: "size"},
		"nonzero exit":     {stdout: nil, stderr: bytes.Repeat([]byte("e"), 10<<10), exit: 1, want: "exit"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newStorageImagerFixture(t, storageBaseName)
			runner := &storageToolRunner{stdout: tc.stdout, stderr: tc.stderr, exit: tc.exit}
			_, err := NewJailedImager(runner).Info(t.Context(), f.ref())
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Fatalf("Info = %v, want an error about %q", err, tc.want)
			}
			if len(err.Error()) > 2048 {
				t.Fatalf("error carries %d bytes of tool output; stderr must be bounded", len(err.Error()))
			}
		})
	}
}

func TestJailedImagerRefusesProbingAndPropagatesRetention(t *testing.T) {
	for _, format := range []string{"", "vmdk", "QCOW2"} {
		f := newStorageImagerFixture(t, storageBaseName)
		ref := f.ref()
		ref.Format = format
		runner := &storageToolRunner{}
		if _, err := NewJailedImager(runner).Info(t.Context(), ref); err == nil || len(runner.calls()) != 0 {
			t.Fatalf("format %q was run (raw format probing is never allowed)", format)
		}
	}
	f := newStorageImagerFixture(t, storageBaseName)
	runner := &storageToolRunner{err: fmt.Errorf("jailed: %w", ErrToolRetained)}
	if _, err := NewJailedImager(runner).Info(t.Context(), f.ref()); !errors.Is(err, ErrToolRetained) {
		t.Fatalf("Info = %v, want ErrToolRetained", err)
	}
}

func TestJailedImagerCreateOverlay(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "disk.qcow2")
	dst, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	runner := &storageToolRunner{during: func(ToolRequest) { storageQcow(t, p, storageBaseName, 10<<30) }}
	ref := ImageRef{File: dst, Root: ImageRootData, Path: "vms/bx_11111111/disk.qcow2", Format: "qcow2", BackingName: storageBaseName}
	if err := NewJailedImager(runner).CreateOverlay(t.Context(), ref, 10<<30); err != nil {
		t.Fatal(err)
	}
	reqs := runner.calls()
	if len(reqs) != 1 || reqs[0].Op != "create" || reqs[0].Access != ImageOwnerRW || reqs[0].Size != 10<<30 ||
		reqs[0].Target.BackingName != storageBaseName || reqs[0].Target.Base != nil {
		t.Fatalf("create request %+v", reqs)
	}
	// A create whose result names another backing is refused.
	p2 := filepath.Join(dir, "disk2.qcow2")
	dst2, err := os.OpenFile(p2, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer dst2.Close()
	runner2 := &storageToolRunner{during: func(ToolRequest) { storageQcow(t, p2, "/etc/shadow", 10<<30) }}
	ref.File = dst2
	if err := NewJailedImager(runner2).CreateOverlay(t.Context(), ref, 10<<30); err == nil {
		t.Fatal("a created overlay naming another backing was accepted")
	}
}
