package node

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"uuid"

	"golang.org/x/sys/unix"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

// jailToolsCopyTrusted copies the trusted base into dst (a new exclusive
// file, then root-owned 0444 like the role publishes it) and returns dst.
func jailToolsCopyTrusted(t *testing.T, src, dst string) string {
	t.Helper()
	in, err := os.OpenFile(src, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	n, err := io.Copy(out, in)
	if err == nil {
		err = out.Chmod(0o444)
	}
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	srcInfo, serr := in.Stat()
	if err != nil || serr != nil || n != srcInfo.Size() {
		t.Fatalf("copy the trusted base to %s: %v, %v (%d bytes)", dst, err, serr, n)
	}
	return dst
}

// TestPrivilegedJailedImagerRealQemuImg runs the REAL qemu-img through the
// REAL jailer and JailedImager: info/check/resize/create, a malicious stored
// backing, a header changed after the precheck (to a FIFO the closed graph
// must never open), a timed-out run and a failing tool. It is the lead's VM
// gate: compile as the normal user, run only the built binary as root in a
// throwaway VM, never on a developer machine or a real host:
//
//	go test -c -o "$dir/node.test" ./internal/node
//	sudo env BBRAFT_TEST_JAILER=1 \
//	  BBRAFT_TEST_JAILER_CGROUP=/sys/fs/cgroup/bbraft-test \
//	  BBRAFT_TEST_JAILER_IO_DEVICE=254:0 \
//	  BBRAFT_TEST_IMAGE_DIR=/path/to/images/v1 \
//	  [BBRAFT_TEST_QEMU_IMG=/usr/bin/qemu-img] \
//	  "$dir/node.test" -test.run TestPrivilegedJailedImagerRealQemuImg -test.v
//
// The image dir holds the trusted standalone root.qcow2, which must already
// be root-owned 0444 (proved here, never changed). Every overlay is created
// by the jailed tool itself; root never runs qemu-img. Host uids 61030-61039.
func TestPrivilegedJailedImagerRealQemuImg(t *testing.T) {
	if os.Getenv("BBRAFT_TEST_JAILER") != "1" {
		t.Skip("set BBRAFT_TEST_JAILER=1 (root throwaway VM only)")
	}
	if os.Geteuid() != 0 {
		t.Fatal("run the built test binary as root inside the throwaway VM")
	}
	cgroup, devText, imageDir := os.Getenv("BBRAFT_TEST_JAILER_CGROUP"), os.Getenv("BBRAFT_TEST_JAILER_IO_DEVICE"), os.Getenv("BBRAFT_TEST_IMAGE_DIR")
	if cgroup == "" || devText == "" || imageDir == "" {
		t.Fatal("BBRAFT_TEST_JAILER_CGROUP, BBRAFT_TEST_JAILER_IO_DEVICE and BBRAFT_TEST_IMAGE_DIR are required")
	}
	qemuImg := cmp.Or(os.Getenv("BBRAFT_TEST_QEMU_IMG"), DefaultJailToolBinary)
	majText, minText, _ := strings.Cut(devText, ":")
	maj, err1 := strconv.ParseUint(majText, 10, 32)
	minor, err2 := strconv.ParseUint(minText, 10, 32)
	if err1 != nil || err2 != nil || maj == 0 {
		t.Fatalf("BBRAFT_TEST_JAILER_IO_DEVICE %q is not MAJ:MIN", devText)
	}
	basePath := filepath.Join(imageDir, "root.qcow2")
	var st syscall.Stat_t
	if err := syscall.Lstat(basePath, &st); err != nil {
		t.Fatal(err)
	}
	if st.Uid != 0 || st.Mode&0o7222 != 0 || st.Mode&0o004 == 0 || st.Mode&syscall.S_IFMT != syscall.S_IFREG {
		t.Fatalf("%s must already be a root-owned 0444 regular file (mode %#o uid %d)", basePath, st.Mode, st.Uid)
	}

	work := t.TempDir()
	// Fresh install: Store.Open creates both missing roots root-owned 0700
	// whatever the umask (the lead also runs this gate under umask 077), and
	// every jailed tool below works from them by descriptor only: the
	// trusted base is copied into a version dir of the fresh image root, and
	// every overlay and the self-check's toolcheck/ live in the fresh data
	// root.
	data := filepath.Join(work, "fresh", "data")
	freshImages := filepath.Join(work, "fresh-images", "images")
	store, err := storage.Open(storage.Config{DataDir: data, ImageDir: freshImages})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{data, filepath.Dir(data), freshImages, filepath.Dir(freshImages)} {
		var st syscall.Stat_t
		if err := syscall.Lstat(p, &st); err != nil || st.Mode&syscall.S_IFMT != syscall.S_IFDIR || st.Mode&0o7777 != 0o700 || st.Uid != 0 {
			t.Fatalf("fresh root %s: mode %#o uid %d, %v; want a root-owned 0700 directory", p, st.Mode, st.Uid, err)
		}
	}
	// The version dir as the role publishes it: <image root>/<id>/root.qcow2,
	// the dir then 0555 (an existing shared dir the Store never chmods).
	version := filepath.Join(freshImages, filepath.Base(imageDir))
	if err := os.Mkdir(version, 0o700); err != nil {
		t.Fatal(err)
	}
	basePath = jailToolsCopyTrusted(t, basePath, filepath.Join(version, "root.qcow2"))
	if err := os.Chmod(version, 0o555); err != nil {
		t.Fatal(err)
	}
	imageDir = version
	rc, err := NewJailReclaimer(t.Context(), data, imageDir)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	jm, err := jailer.New(jailer.Config{
		StateDir: filepath.Join(work, "jailer"), CgroupParent: cgroup,
		UIDs: jailer.UIDRange{Min: 61030, Max: 61039}, Reclaim: rc.Reclaim,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer jm.Close()
	io := jailer.IOLimits{Device: jailer.BlockDevice{Major: uint32(maj), Minor: uint32(minor)}, ReadBPS: 1 << 30}
	runs := &atomic.Int64{}
	real := jailToolRunner{m: jm, binary: qemuImg, io: io}
	var before func()
	last := &runnerOutcome{}
	counting := countingRunner{inner: real, runs: runs, last: last, before: func() {
		if before != nil {
			before()
		}
	}}
	imager := storage.NewJailedImager(counting)

	base, err := os.Open(basePath)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	settled := func(t *testing.T) {
		t.Helper()
		if recs := jm.Records(); len(recs) != 0 {
			t.Fatalf("tool records remain: %+v", recs)
		}
	}
	baseRef := storage.ImageRef{File: base, Root: storage.ImageRootImages, Path: "root.qcow2", Format: "qcow2"}
	info, err := imager.Info(t.Context(), baseRef)
	if err != nil {
		t.Fatalf("Info(base): %v", err)
	}
	if err := imager.Check(t.Context(), baseRef); err != nil {
		t.Fatalf("Check(base): %v", err)
	}
	settled(t)

	// overlay creates a new root:root 0600 file under data and lets the
	// jailed tool make it an overlay of the trusted base.
	n := 0
	overlay := func(t *testing.T) (string, string) {
		t.Helper()
		n++
		rel := filepath.Join("vms", "bx_jjjjjjj"+strconv.Itoa(n+1), "disk.qcow2")
		p := filepath.Join(data, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		ref := storage.ImageRef{File: f, Root: storage.ImageRootData, Path: rel, Format: "qcow2", BackingName: basePath}
		if err := imager.CreateOverlay(t.Context(), ref, info.VirtualSize); err != nil {
			t.Fatalf("CreateOverlay: %v", err)
		}
		if got := realBacking(t, p); got != basePath {
			t.Fatalf("created overlay stores backing %q, want %q", got, basePath)
		}
		settled(t)
		return p, rel
	}
	pin := func(t *testing.T, p string, flag int) *os.File {
		t.Helper()
		f, err := os.OpenFile(p, flag, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		return f
	}
	refOf := func(t *testing.T, p, rel string, flag int) storage.ImageRef {
		return storage.ImageRef{File: pin(t, p, flag), Root: storage.ImageRootData, Path: rel, Format: "qcow2", ExpectedBacking: basePath, Base: base}
	}

	t.Run("info check resize create", func(t *testing.T) {
		p, rel := overlay(t)
		// The raw report (info opens no backing: target alone), for the
		// lead's record of what backing-filename means; stderr is a bounded
		// diagnostic.
		var raw bytes.Buffer
		diag := &boundedDiag{limit: 4096}
		if _, err := jm.RunTool(t.Context(), jailer.ToolSpec{
			Attempt: uuid.NewV7().String(), Binary: qemuImg, Op: jailer.ToolInfo, Driver: "qcow2",
			Target: pin(t, p, os.O_RDONLY), TargetRoot: jailer.TargetRootData, TargetPath: rel, TargetAccess: jailer.AccessGroupRO,
			Limits:  jailer.ToolLimits{CPUQuotaPercent: 100, MemoryBytes: jailToolMemory, Pids: 64, IO: io},
			Timeout: time.Minute, Stdout: &raw, Stderr: diag,
		}); err != nil {
			t.Fatalf("raw info: %v (stderr %q)", err, diag.String())
		}
		t.Logf("raw qemu-img info under the closed graph: %s", raw.String())
		got, err := imager.Info(t.Context(), refOf(t, p, rel, os.O_RDONLY))
		if err != nil {
			t.Fatalf("Info(overlay): %v (see the raw report above)", err)
		}
		if got.VirtualSize != info.VirtualSize || got.BackingFile != basePath {
			t.Fatalf("Info(overlay) = %+v", got)
		}
		if err := imager.Check(t.Context(), refOf(t, p, rel, os.O_RDONLY)); err != nil {
			t.Fatalf("Check(overlay): %v", err)
		}
		if err := imager.Grow(t.Context(), refOf(t, p, rel, os.O_RDWR), info.VirtualSize+1<<30); err != nil {
			t.Fatalf("Grow: %v", err)
		}
		if got, err := imager.Info(t.Context(), refOf(t, p, rel, os.O_RDONLY)); err != nil || got.VirtualSize != info.VirtualSize+1<<30 {
			t.Fatalf("Info after Grow = %+v, %v", got, err)
		}
		var st syscall.Stat_t
		if err := syscall.Lstat(p, &st); err != nil || st.Uid != 0 || st.Mode&0o777 != 0o600 {
			t.Fatalf("reclaimed overlay uid %d mode %#o, %v; want root 0600", st.Uid, st.Mode&0o777, err)
		}
		settled(t)
	})
	t.Run("malicious stored backing is refused before any run", func(t *testing.T) {
		p, rel := overlay(t)
		setRealBacking(t, p, "/etc/shadow")
		start := runs.Load()
		if _, err := imager.Info(t.Context(), refOf(t, p, rel, os.O_RDONLY)); err == nil {
			t.Fatal("Info accepted a stored backing the Store did not expect")
		}
		if runs.Load() != start {
			t.Fatal("a tool ran on an unexpected stored backing")
		}
	})
	t.Run("header changed after precheck to a FIFO", func(t *testing.T) {
		p, rel := overlay(t)
		fifo := filepath.Join(work, "trap.fifo")
		if err := unix.Mkfifo(fifo, 0o600); err != nil {
			t.Fatal(err)
		}
		before = func() { setRealBacking(t, p, fifo) }
		defer func() { before = nil }()
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		_, err := imager.Info(ctx, refOf(t, p, rel, os.O_RDONLY))
		if err == nil {
			t.Fatal("Info accepted a header changed after the precheck")
		}
		if ctx.Err() != nil {
			t.Fatal("the tool blocked on the FIFO: the stored name was followed")
		}
		// The protection must be the storage postcheck after a SUCCESSFUL
		// closed-graph run: a graph that followed the stored name fails the
		// tool itself (ENOENT, EACCES or a hang), which is not protection.
		out, runErr := last.get()
		if runErr != nil || out.ExitCode != 0 {
			t.Fatalf("the real tool did not succeed under the closed graph (exit %d, %v); Info error %v", out.ExitCode, runErr, err)
		}
		if !strings.Contains(err.Error(), "header changed during qemu-img info") {
			t.Fatalf("Info failed for another reason than the postcheck: %v", err)
		}
		settled(t)
	})
	t.Run("timed-out run is killed and settled", func(t *testing.T) {
		p, rel := overlay(t)
		_, err := jm.RunTool(t.Context(), jailer.ToolSpec{
			Attempt: uuid.NewV7().String(), Binary: qemuImg, Op: jailer.ToolCheck, Driver: "qcow2",
			Target: pin(t, p, os.O_RDONLY), TargetRoot: jailer.TargetRootData, TargetPath: rel, TargetAccess: jailer.AccessGroupRO,
			Base: base, Limits: jailer.ToolLimits{CPUQuotaPercent: 100, MemoryBytes: jailToolMemory, Pids: 64, IO: io},
			Timeout: time.Millisecond,
		})
		if err == nil {
			t.Fatal("a 1ms run succeeded")
		}
		if errors.Is(err, jailer.ErrToolRetained) {
			t.Fatalf("the timed-out tool was not reaped: %v", err)
		}
		settled(t)
	})
	t.Run("failing tool", func(t *testing.T) {
		p, rel := overlay(t)
		if err := os.Truncate(p, 512); err != nil {
			t.Fatal(err)
		}
		if err := imager.Check(t.Context(), refOf(t, p, rel, os.O_RDONLY)); err == nil {
			t.Fatal("Check of a truncated overlay succeeded")
		}
		settled(t)
	})
	// The installed qemu-img self-check through the real Store (its lock and
	// leases), JailedImager, jailer and reclaimer, over the same root-owned
	// verified base (the Store's image root is imageDir's parent). Run twice:
	// the first creates toolcheck/, the second adopts it on the EEXIST path
	// and repeats recovery and cleanup on the existing parent.
	t.Run("installed qemu-img self-check", func(t *testing.T) {
		id := filepath.Base(imageDir)
		if !storage.ValidImageID(id) {
			t.Fatalf("BBRAFT_TEST_IMAGE_DIR's base name %q is not an image id", id)
		}
		parent := filepath.Join(data, "toolcheck")
		if _, err := os.Lstat(parent); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("toolcheck exists before the first self-check: %v", err)
		}
		baseBefore := selfCheckGateStat(t, basePath)
		cgroupsBefore := selfCheckGateCgroups(t, cgroup)
		gate := &selfCheckGateRunner{inner: real}
		store, err := storage.Open(storage.Config{
			DataDir: data, ImageDir: filepath.Dir(imageDir),
			Imager: storage.NewJailedImager(gate), Leases: jailLeases{jailerRunner{jm}},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		img := storage.Image{ID: id, Dir: imageDir, RootVirtualSize: info.VirtualSize, RootVirtualSizeGiB: (info.VirtualSize + 1<<30 - 1) >> 30}
		wantSteps := []string{"recover", "create-overlay", "info-no-backing", "check-graph", "resize-graph", "create-absent", "info-absent-backing", "cleanup"}
		for run := range 2 {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			report, err := store.ToolSelfCheck(ctx, img)
			cancel()
			if err != nil {
				t.Fatalf("run %d: ToolSelfCheck: %v", run, err)
			}
			t.Logf("run %d: self-check %+v", run, report)
			if !slices.Equal(report.Steps, wantSteps) || report.Image != id || report.Recovered != 0 {
				t.Fatalf("run %d: report %+v, want steps %q, image %s, nothing recovered", run, report, wantSteps, id)
			}
			info, err := os.Lstat(parent)
			if err != nil {
				t.Fatalf("run %d: %v", run, err)
			}
			pst := info.Sys().(*syscall.Stat_t)
			if !info.IsDir() || pst.Uid != 0 || pst.Gid != 0 || info.Mode()&(os.ModePerm|os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0o700 {
				t.Fatalf("run %d: toolcheck mode %v uid %d gid %d; want a root:root 0700 directory", run, info.Mode(), pst.Uid, pst.Gid)
			}
			if entries, err := os.ReadDir(parent); err != nil || len(entries) != 0 {
				t.Fatalf("run %d: toolcheck holds %v, %v; want it exactly empty", run, entries, err)
			}
			settled(t)
			targets, bad := gate.take()
			if len(targets) != 7 {
				t.Fatalf("run %d: %d tool runs, want 7: %v", run, len(targets), targets)
			}
			for _, b := range bad {
				t.Errorf("run %d: %s", run, b)
			}
			for _, tg := range targets {
				if ref, held := jm.TargetHolder(tg.dev, tg.ino); held {
					t.Errorf("run %d: %s (%s) still leased by %+v", run, tg.path, tg.op, ref)
				}
			}
			if got := selfCheckGateCgroups(t, cgroup); !slices.Equal(got, cgroupsBefore) {
				t.Fatalf("run %d: tool cgroups %q remain (before %q)", run, got, cgroupsBefore)
			}
			if got := selfCheckGateStat(t, basePath); got != baseBefore {
				t.Fatalf("run %d: the verified base changed: %+v, was %+v", run, got, baseBefore)
			}
		}
	})
}

// selfCheckGateTarget is one self-check tool target, as pinned.
type selfCheckGateTarget struct {
	op, path string
	dev, ino uint64
}

// selfCheckGateRunner wraps the real jailed runner: it records each target
// and, right after each run returned (settled and reclaimed), proves the
// pinned inode is back to root:root 0600 with one link.
type selfCheckGateRunner struct {
	inner   storage.ToolRunner
	mu      sync.Mutex
	targets []selfCheckGateTarget
	bad     []string
}

func (g *selfCheckGateRunner) RunTool(ctx context.Context, req storage.ToolRequest) (storage.ToolOutcome, error) {
	out, err := g.inner.RunTool(ctx, req)
	var st syscall.Stat_t
	serr := syscall.Fstat(int(req.Target.File.Fd()), &st)
	g.mu.Lock()
	defer g.mu.Unlock()
	g.targets = append(g.targets, selfCheckGateTarget{op: req.Op, path: req.Target.Path, dev: uint64(st.Dev), ino: st.Ino})
	switch {
	case serr != nil:
		g.bad = append(g.bad, fmt.Sprintf("%s %s: fstat after the run: %v", req.Op, req.Target.Path, serr))
	case st.Uid != 0 || st.Gid != 0 || st.Mode&0o7777 != 0o600 || st.Nlink != 1:
		g.bad = append(g.bad, fmt.Sprintf("%s %s after the run: uid %d gid %d mode %#o nlink %d; want root:root 0600, one link",
			req.Op, req.Target.Path, st.Uid, st.Gid, st.Mode&0o7777, st.Nlink))
	}
	if !strings.HasPrefix(req.Target.Path, "toolcheck/") || req.Target.Root != storage.ImageRootData {
		g.bad = append(g.bad, fmt.Sprintf("%s ran on %s:%s, not a toolcheck fixture", req.Op, req.Target.Root, req.Target.Path))
	}
	return out, err
}

func (g *selfCheckGateRunner) TargetHeld(dev, ino uint64) bool { return g.inner.TargetHeld(dev, ino) }

// take returns and clears what the runs recorded.
func (g *selfCheckGateRunner) take() ([]selfCheckGateTarget, []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	targets, bad := g.targets, g.bad
	g.targets, g.bad = nil, nil
	return targets, bad
}

// selfCheckGateStat is a file's identity, owner and mode.
func selfCheckGateStat(t *testing.T, p string) [5]uint64 {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Lstat(p, &st); err != nil {
		t.Fatal(err)
	}
	return [5]uint64{uint64(st.Dev), st.Ino, uint64(st.Uid), uint64(st.Gid), uint64(st.Mode)}
}

// selfCheckGateCgroups lists the child cgroups under parent.
func selfCheckGateCgroups(t *testing.T, parent string) []string {
	t.Helper()
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out
}

// countingRunner counts runs, runs before ahead of each and records the
// real runner's last outcome.
type countingRunner struct {
	inner  storage.ToolRunner
	runs   *atomic.Int64
	last   *runnerOutcome
	before func()
}

func (c countingRunner) RunTool(ctx context.Context, req storage.ToolRequest) (storage.ToolOutcome, error) {
	c.runs.Add(1)
	c.before()
	out, err := c.inner.RunTool(ctx, req)
	c.last.set(out, err)
	return out, err
}

// runnerOutcome is the last real ToolRunner result.
type runnerOutcome struct {
	mu  sync.Mutex
	out storage.ToolOutcome
	err error
}

func (r *runnerOutcome) set(out storage.ToolOutcome, err error) {
	r.mu.Lock()
	r.out, r.err = out, err
	r.mu.Unlock()
}

func (r *runnerOutcome) get() (storage.ToolOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.out, r.err
}

func (c countingRunner) TargetHeld(dev, ino uint64) bool { return c.inner.TargetHeld(dev, ino) }

// realBacking reads a qcow2 header's stored backing name.
func realBacking(t *testing.T, p string) string {
	t.Helper()
	raw, err := os.ReadFile(p)
	if err != nil || len(raw) < 20 {
		t.Fatalf("read %s header: %v", p, err)
	}
	off, size := binary.BigEndian.Uint64(raw[8:16]), binary.BigEndian.Uint32(raw[16:20])
	if size == 0 {
		return ""
	}
	if off+uint64(size) > uint64(len(raw)) {
		t.Fatalf("%s backing name is out of range", p)
	}
	return string(raw[off : off+uint64(size)])
}

// setRealBacking rewrites a qcow2 header's stored backing name in place (a
// Go byte edit of the test's own overlay, never a qemu-img run as root).
func setRealBacking(t *testing.T, p, name string) {
	t.Helper()
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var hdr [20]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		t.Fatal(err)
	}
	off := binary.BigEndian.Uint64(hdr[8:16])
	if off == 0 {
		t.Fatal("overlay has no backing name area")
	}
	if _, err := f.WriteAt([]byte(name), int64(off)); err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(hdr[16:20], uint32(len(name)))
	if _, err := f.WriteAt(hdr[16:20], 16); err != nil {
		t.Fatal(err)
	}
}

// boundedDiag keeps at most limit bytes of a tool's diagnostic stderr.
type boundedDiag struct {
	mu    sync.Mutex
	limit int
	buf   []byte
}

func (b *boundedDiag) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if room := b.limit - len(b.buf); room > 0 {
		b.buf = append(b.buf, p[:min(room, len(p))]...)
	}
	return len(p), nil
}

func (b *boundedDiag) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}
