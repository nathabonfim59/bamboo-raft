package guest

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// TestReadFileEnforcesMaxBytesWhileGrowing proves max_bytes is enforced on
// the bytes actually read, not only on the initial fstat: the send callback
// appends to the file between chunks, so the next read crosses the limit.
func TestReadFileEnforcesMaxBytesWhileGrowing(t *testing.T) {
	base := t.TempDir()
	home := filepath.Join(base, "home")
	tmp := filepath.Join(base, "tmp")
	for _, d := range []string{home, tmp} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	const chunk = 1 << 20
	path := filepath.Join(home, "grow")
	if err := os.WriteFile(path, bytes.Repeat([]byte("a"), chunk), 0o644); err != nil {
		t.Fatal(err)
	}
	homeRoot, err := os.OpenRoot(home)
	if err != nil {
		t.Fatal(err)
	}
	defer homeRoot.Close()
	tmpRoot, err := os.OpenRoot(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer tmpRoot.Close()
	fs := newFileServer(homeRoot, tmpRoot, nil, nil)

	var sawInfo, grew bool
	var sent int64
	err = fs.readFile(t.Context(), "grow", chunk, func(resp *nodev1.ReadFileResponse) error {
		switch part := resp.GetPart().(type) {
		case *nodev1.ReadFileResponse_Info:
			if sawInfo || sent > 0 {
				t.Fatal("info was not the first and only info frame")
			}
			sawInfo = true
		case *nodev1.ReadFileResponse_Data:
			if !sawInfo {
				t.Fatal("data before info")
			}
			if len(part.Data) > maxReadChunk {
				t.Fatalf("chunk of %d bytes exceeds %d", len(part.Data), maxReadChunk)
			}
			sent += int64(len(part.Data))
			if !grew {
				grew = true
				f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, werr := f.Write(bytes.Repeat([]byte("b"), chunk))
				cerr := f.Close()
				if werr != nil || cerr != nil {
					t.Fatalf("grow file: %v, %v", werr, cerr)
				}
			}
		}
		return nil
	})
	if !sawInfo || !grew {
		t.Fatalf("stream did not reach info and data (info %v, grew %v)", sawInfo, grew)
	}
	if got := status.Code(err); got != codes.OutOfRange {
		t.Fatalf("growing ReadFile = %v (%v), want OutOfRange", err, got)
	}
	if sent > chunk {
		t.Fatalf("sent %d bytes, above the %d byte limit", sent, chunk)
	}
}

// TestEnsureParentsPreservesConcurrentMode stages the mkdir race: the
// directory exists by the time Mkdir runs (EEXIST), so its mode belongs to
// the concurrent creator and must not be rewritten to 0755.
func TestEnsureParentsPreservesConcurrentMode(t *testing.T) {
	base := t.TempDir()
	home := filepath.Join(base, "home")
	tmp := filepath.Join(base, "tmp")
	for _, d := range []string{home, tmp} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	homeRoot, err := os.OpenRoot(home)
	if err != nil {
		t.Fatal(err)
	}
	defer homeRoot.Close()
	tmpRoot, err := os.OpenRoot(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer tmpRoot.Close()
	fs := newFileServer(homeRoot, tmpRoot, nil, nil)
	rt := &root{r: homeRoot, spec: homeSpec}

	existing := filepath.Join(home, "existing")
	if err := os.Mkdir(existing, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := fs.mkdirParentLevel(rt, "existing"); err != nil {
		t.Fatalf("mkdirParentLevel on an existing directory: %v", err)
	}
	if fi, err := os.Stat(existing); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("concurrent creator's mode = %v, %v; want 0700", fi.Mode(), err)
	}
	// ensureParents takes the same path when the prefix exists.
	if err := fs.ensureParents(rt, "existing/child", "/home/user/existing/child", "mkdir"); err != nil {
		t.Fatalf("ensureParents: %v", err)
	}
	if fi, err := os.Stat(existing); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("ensureParents changed an existing parent mode to %v, %v; want 0700", fi.Mode(), err)
	}
	// A level this server really creates gets 0755.
	if err := fs.mkdirParentLevel(rt, "fresh"); err != nil {
		t.Fatalf("mkdirParentLevel fresh: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(home, "fresh")); err != nil || fi.Mode().Perm() != 0o755 {
		t.Fatalf("created parent mode = %v, %v; want 0755", fi.Mode(), err)
	}
}
