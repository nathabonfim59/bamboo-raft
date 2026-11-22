package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// fleeceImager writes a minimal qcow2 v3 header for standalone creates and
// counts them; size and backing let a test make a bad template.
type fleeceImager struct {
	testImager
	mu      sync.Mutex
	creates []string
	err     error
}

func (f *fleeceImager) CreateStandalone(_ context.Context, dst ImageRef, virtualSize uint64) error {
	f.mu.Lock()
	f.creates = append(f.creates, dst.Path)
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return err
	}
	if dst.BackingName != "" || dst.Root != ImageRootData {
		return errors.New("standalone create with a backing or outside the data root")
	}
	return fleeceWriteHeader(dst.File, virtualSize, "")
}

func (f *fleeceImager) createCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.creates)
}

// fleeceWriteHeader writes a qcow2 v3 header of size, storing backing when
// it is not empty.
func fleeceWriteHeader(f *os.File, size uint64, backing string) error {
	h := make([]byte, 104)
	copy(h, "QFI\xfb")
	binary.BigEndian.PutUint32(h[4:], 3)
	binary.BigEndian.PutUint32(h[20:], 16)
	binary.BigEndian.PutUint64(h[24:], size)
	binary.BigEndian.PutUint32(h[100:], 104)
	if backing != "" {
		binary.BigEndian.PutUint64(h[8:], 512)
		binary.BigEndian.PutUint32(h[16:], uint32(len(backing)))
		if _, err := f.WriteAt([]byte(backing), 512); err != nil {
			return err
		}
	}
	_, err := f.WriteAt(h, 0)
	return err
}

func fleeceStore(t *testing.T, imager Imager) (*Store, string) {
	t.Helper()
	s, err := Open(Config{DataDir: t.TempDir(), ImageDir: t.TempDir(), Imager: imager})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	id := ids.Sandbox()
	if err := s.root.Mkdir(path.Join(vmsDir, id), 0o700); err != nil {
		t.Fatal(err)
	}
	return s, id
}

func fleeceAssertCopy(t *testing.T, s *Store, id string, f *os.File, size uint64) {
	t.Helper()
	defer f.Close()
	if err := checkFleeceHeader(f, size); err != nil {
		t.Fatalf("fleece header: %v", err)
	}
	info, err := os.Lstat(s.FleecePath(id))
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("fleece file = %v, %v; want a 0600 regular file", info, err)
	}
	if _, err := f.WriteAt([]byte{0}, 1<<20); err != nil {
		t.Fatalf("fleece is not writable: %v", err)
	}
}

// TestPrepareFleeceTemplateOnce: the first cut of a size creates its
// template through the standalone create; later cuts (any VM) only copy it.
func TestPrepareFleeceTemplateOnce(t *testing.T) {
	im := &fleeceImager{}
	s, id := fleeceStore(t, im)
	const size = 10 << 30
	for range 3 {
		f, err := s.PrepareFleece(t.Context(), id, size)
		if err != nil {
			t.Fatalf("PrepareFleece: %v", err)
		}
		fleeceAssertCopy(t, s, id, f, size)
	}
	if n := im.createCount(); n != 1 {
		t.Fatalf("standalone creates = %d, want 1", n)
	}
	if im.creates[0] != fleeceTemplateName(size)+manifestTmpExt {
		t.Fatalf("created %q", im.creates[0])
	}
	// Another size gets its own template.
	f, err := s.PrepareFleece(t.Context(), id, 20<<30)
	if err != nil {
		t.Fatal(err)
	}
	fleeceAssertCopy(t, s, id, f, 20<<30)
	if n := im.createCount(); n != 2 {
		t.Fatalf("standalone creates = %d, want 2", n)
	}
}

// TestPrepareFleeceBadTemplateRecreated: a template that fails its check
// (not qcow2, wrong size, a backing name, a symlink) is recreated.
func TestPrepareFleeceBadTemplateRecreated(t *testing.T) {
	const size = 4 << 30
	tests := map[string]func(t *testing.T, s *Store, name string){
		"garbage": func(t *testing.T, s *Store, name string) {
			if err := os.WriteFile(filepath.Join(s.dataDir, name), []byte("not qcow2"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"wrong size": func(t *testing.T, s *Store, name string) {
			f, err := os.OpenFile(filepath.Join(s.dataDir, name), os.O_RDWR|os.O_CREATE, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if err := fleeceWriteHeader(f, size*2, ""); err != nil {
				t.Fatal(err)
			}
		},
		"backing": func(t *testing.T, s *Store, name string) {
			f, err := os.OpenFile(filepath.Join(s.dataDir, name), os.O_RDWR|os.O_CREATE, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if err := fleeceWriteHeader(f, size, "/etc/shadow"); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, plant := range tests {
		t.Run(name, func(t *testing.T) {
			im := &fleeceImager{}
			s, id := fleeceStore(t, im)
			if err := s.root.Mkdir(phase2Dir, 0o700); err != nil {
				t.Fatal(err)
			}
			plant(t, s, fleeceTemplateName(size))
			f, err := s.PrepareFleece(t.Context(), id, size)
			if err != nil {
				t.Fatalf("PrepareFleece: %v", err)
			}
			fleeceAssertCopy(t, s, id, f, size)
			if n := im.createCount(); n != 1 {
				t.Fatalf("standalone creates = %d, want 1 (recreated)", n)
			}
		})
	}
}

// TestPrepareFleeceRefusals: a failed create leaves no template and no
// fleece; a non-regular fleece name is left alone; bad sizes and ids are
// refused before anything is written.
func TestPrepareFleeceRefusals(t *testing.T) {
	t.Run("create fails", func(t *testing.T) {
		im := &fleeceImager{err: errors.New("qemu-img: exit 1")}
		s, id := fleeceStore(t, im)
		if _, err := s.PrepareFleece(t.Context(), id, 1<<30); err == nil {
			t.Fatal("PrepareFleece succeeded with a failing create")
		}
		for _, rel := range []string{fleeceTemplateName(1 << 30), fleeceTemplateName(1<<30) + manifestTmpExt, path.Join(vmsDir, id, FleeceName)} {
			if _, err := s.root.Lstat(rel); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("%s left behind: %v", rel, err)
			}
		}
	})
	t.Run("fleece name is a directory", func(t *testing.T) {
		s, id := fleeceStore(t, &fleeceImager{})
		if err := os.Mkdir(s.FleecePath(id), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := s.PrepareFleece(t.Context(), id, 1<<30); err == nil {
			t.Fatal("PrepareFleece replaced a directory")
		}
		if info, err := os.Lstat(s.FleecePath(id)); err != nil || !info.IsDir() {
			t.Fatalf("directory not left alone: %v, %v", info, err)
		}
	})
	t.Run("imager without standalone create", func(t *testing.T) {
		s, id := fleeceStore(t, testImager{})
		if _, err := s.PrepareFleece(t.Context(), id, 1<<30); err == nil {
			t.Fatal("PrepareFleece succeeded without a standalone creator")
		}
	})
	t.Run("bad arguments", func(t *testing.T) {
		s, id := fleeceStore(t, &fleeceImager{})
		for _, tc := range []struct {
			id   string
			size uint64
		}{{"../x", 1 << 30}, {id, 0}, {id, 1000}} {
			if _, err := s.PrepareFleece(t.Context(), tc.id, tc.size); err == nil {
				t.Errorf("PrepareFleece(%q, %d) succeeded", tc.id, tc.size)
			}
		}
	})
}

// TestJailedImagerStandaloneScope: the standalone create only targets
// phase2/ in the data root, never with a backing name, and an overlay
// create still needs one.
func TestJailedImagerStandaloneScope(t *testing.T) {
	j := NewJailedImager(localToolRunner{})
	f, err := os.CreateTemp(t.TempDir(), "img")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for name, ref := range map[string]ImageRef{
		"vm path":      {File: f, Root: ImageRootData, Path: "vms/bx_1/disk.qcow2", Format: "qcow2"},
		"images root":  {File: f, Root: ImageRootImages, Path: "phase2/t.qcow2", Format: "qcow2"},
		"with backing": {File: f, Root: ImageRootData, Path: "phase2/t.qcow2", Format: "qcow2", BackingName: "b"},
		"raw":          {File: f, Root: ImageRootData, Path: "phase2/t.qcow2", Format: "raw"},
	} {
		if err := j.CreateStandalone(t.Context(), ref, 1<<30); err == nil {
			t.Errorf("%s: CreateStandalone accepted it", name)
		}
	}
	if err := j.CreateOverlay(t.Context(), ImageRef{File: f, Root: ImageRootData, Path: "vms/bx_1/disk.qcow2", Format: "qcow2"}, 1<<30); err == nil {
		t.Error("CreateOverlay accepted an empty backing name")
	}
}

// TestPrepareFleeceRealQemuImg makes a template with the host qemu-img
// (the unjailed dev tool path) and proves the copy. Needs BBRAFT_TEST_KVM=1
// (those hosts have qemu-utils) and a non-root user.
func TestPrepareFleeceRealQemuImg(t *testing.T) {
	if os.Getenv("BBRAFT_TEST_KVM") != "1" {
		t.Skip("BBRAFT_TEST_KVM=1 not set")
	}
	if os.Geteuid() == 0 {
		t.Skip("the unjailed qemu-img refuses to run as root")
	}
	s, id := fleeceStore(t, QemuImg{})
	const size = 10 << 30
	f, err := s.PrepareFleece(t.Context(), id, size)
	if err != nil {
		t.Fatalf("PrepareFleece: %v", err)
	}
	fleeceAssertCopy(t, s, id, f, size)
	info, err := os.Stat(filepath.Join(s.dataDir, fleeceTemplateName(size)))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 4<<20 {
		t.Fatalf("template is %d bytes; expected an empty qcow2 of a few hundred KiB", info.Size())
	}
}
