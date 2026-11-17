package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A10 D1 fix round 1: the imager is ctx-first and ctx-terminal, and the
// no-follow helpers keep their plain-file and directory positive controls.

func TestJailedImagerHonorsContext(t *testing.T) {
	f := newStorageImagerFixture(t, storageBaseName)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	runner := &storageToolRunner{stdout: storageInfoJSON("qcow2", 10<<30, storageBaseName)}
	if _, err := NewJailedImager(runner).Info(ctx, f.ref()); !errors.Is(err, context.Canceled) || len(runner.calls()) != 0 {
		t.Fatalf("pre-canceled Info = %v with %d runs", err, len(runner.calls()))
	}
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	runner = &storageToolRunner{stdout: storageInfoJSON("qcow2", 10<<30, storageBaseName), during: func(ToolRequest) { cancel() }}
	if _, err := NewJailedImager(runner).Info(ctx, f.ref()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Info canceled during the run = %v, want Canceled", err)
	}
}

func TestNoFollowHelpersKeepPlainEntries(t *testing.T) {
	s, img := storageJailFixture(t, nil, nil)
	pins, err := s.PinLaunch(t.Context(), storageJailID, img)
	if err != nil {
		t.Fatal(err)
	}
	pins.Close()
	f, err := s.OpenVMEntry(t.Context(), storageJailID, diskName, os.O_RDONLY)
	if err != nil {
		t.Fatalf("plain file: %v", err)
	}
	f.Close()
	d, err := s.OpenVMEntry(t.Context(), storageJailID, ControlDirName, os.O_RDONLY|0o200000)
	if err != nil {
		t.Fatalf("plain directory: %v", err)
	}
	d.Close()
	if pins, err := s.PinLaunch(t.Context(), storageJailID, img); err != nil {
		t.Fatalf("re-pin of plain entries: %v", err)
	} else {
		pins.Close()
	}
	if _, err := pinNoFollow(s.root, filepath.Join(vmsDir, storageJailID, diskName), os.O_RDONLY); err != nil {
		t.Fatalf("pinNoFollow of a plain file: %v", err)
	}
}

// TestLocalToolArgvPerOp: the dev runner's fixed argv matches the jailer's:
// info names the target alone, check and resize carry the explicit base.
func TestLocalToolArgvPerOp(t *testing.T) {
	const fd3 = "driver=qcow2,file.driver=file,file.filename=/proc/self/fd/3"
	const graph = ",backing.driver=qcow2,backing.file.driver=file,backing.file.filename=/proc/self/fd/4,backing.read-only=on"
	// A real, test-owned descriptor: wrapping fd 0 would hand stdin to an
	// *os.File whose finalizer closes it.
	base, err := os.Create(filepath.Join(t.TempDir(), "base.qcow2"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { base.Close() })
	for _, tc := range []struct {
		req  ToolRequest
		want string
	}{
		// Even with a base in the request, info names the target alone.
		{ToolRequest{Op: "info", Driver: "qcow2", Target: ImageRef{Base: base}}, "info --output=json --image-opts " + fd3},
		{ToolRequest{Op: "check", Driver: "qcow2", Target: ImageRef{Base: base}}, "check --output=json --image-opts " + fd3 + graph},
		{ToolRequest{Op: "resize", Driver: "qcow2", Target: ImageRef{Base: base}, Size: 5}, "resize --image-opts " + fd3 + graph + " 5"},
	} {
		if got := strings.Join(toolArgv(tc.req), " "); got != tc.want {
			t.Errorf("toolArgv(%s) = %q\nwant %q", tc.req.Op, got, tc.want)
		}
		// The descriptors follow the graph: no fd 4 for info.
		wantFiles := 2
		if tc.req.Op == "info" {
			wantFiles = 1
		}
		if got := toolFiles(tc.req); len(got) != wantFiles || (wantFiles == 2 && got[1] != base) {
			t.Errorf("toolFiles(%s) = %d files, want %d", tc.req.Op, len(got), wantFiles)
		}
	}
}
