package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type testImager struct {
	virtualSize uint64
	format      string
	backing     string
	checkErr    error
}

func (t testImager) CreateOverlay(_ context.Context, dst ImageRef, _ uint64) error {
	return os.WriteFile(dst.File.Name(), nil, 0o600)
}
func (t testImager) Grow(context.Context, ImageRef, uint64) error { return nil }
func (t testImager) Info(context.Context, ImageRef) (QemuInfo, error) {
	return QemuInfo{Format: t.format, VirtualSize: t.virtualSize, BackingFile: t.backing}, nil
}
func (t testImager) Check(context.Context, ImageRef) error { return t.checkErr }

const testGiB = uint64(10)

type imageFile struct {
	name string
	data []byte
}

func testImageArtifacts() []imageFile {
	return []imageFile{
		{"kernel", []byte("kernel")},
		{"initrd", []byte("initrd")},
		{"root.qcow2", []byte("qcow2")},
		{"packages.json", []byte("{}")},
	}
}

// writeImage builds a valid image and then lets the test mutate the manifest
// or the directory before verification.
func writeImage(t *testing.T, root, id string, mutate func(dir string, m map[string]any)) {
	t.Helper()
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	artifacts := map[string]any{}
	for _, f := range testImageArtifacts() {
		if err := os.WriteFile(filepath.Join(dir, f.name), f.data, 0o644); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(f.data)
		artifacts[f.name] = map[string]any{"sha256": hex.EncodeToString(sum[:]), "size": len(f.data)}
	}
	m := map[string]any{
		"schemaVersion": 1,
		"id":            id,
		"root": map[string]any{
			"filesystem":       "ext4",
			"format":           "qcow2",
			"diskGiB":          testGiB,
			"virtualSizeBytes": testGiB << 30,
		},
		"artifacts": artifacts,
	}
	if mutate != nil {
		mutate(dir, m)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// storageWriteBase writes image id's standalone base under s's image dir:
// overlays are always run with their pinned base.
func storageWriteBase(t *testing.T, s *Store, id string) {
	t.Helper()
	if err := os.MkdirAll(s.ImagePath(id), 0o755); err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 512)
	copy(raw, "QFI\xfb")
	raw[7] = 3
	raw[103] = 104
	if err := os.WriteFile(filepath.Join(s.ImagePath(id), "root.qcow2"), raw, 0o444); err != nil {
		t.Fatal(err)
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(Config{
		DataDir:  t.TempDir(),
		ImageDir: t.TempDir(),
		Imager:   testImager{virtualSize: testGiB << 30, format: "qcow2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestValidImageID(t *testing.T) {
	for _, tc := range []struct {
		id   string
		want bool
	}{
		{"ubuntu-24.04-v0", true},
		{"a", true},
		{"a.b_c-d", true},
		{"", false},
		{"UPPER", false},
		{".hidden", false},
		{"a/b", false},
		{"a b", false},
		{"a..b", true},
		{"-a", false},
		{string(make([]byte, 65)), false},
	} {
		if got := ValidImageID(tc.id); got != tc.want {
			t.Errorf("ValidImageID(%q) = %t, want %t", tc.id, got, tc.want)
		}
	}
}

func TestVerifyImagesAcceptsValidAndRejectsBroken(t *testing.T) {
	cases := []struct {
		name    string
		wantErr string
		mutate  func(t *testing.T, dir string, m map[string]any)
	}{
		{"valid", "", nil},
		{"hash mismatch", "sha256 mismatch", func(_ *testing.T, _ string, m map[string]any) {
			// A well-formed sha256 that is simply wrong must reach the
			// hashing comparison, not fail validation.
			m["artifacts"].(map[string]any)["kernel"].(map[string]any)["sha256"] = strings.Repeat("0", 64)
		}},
		{"artifact size mismatch", "size", func(t *testing.T, dir string, _ map[string]any) {
			data, err := os.ReadFile(filepath.Join(dir, "kernel"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "kernel"), append(data, 'x'), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"extra file", "unexpected entry", func(t *testing.T, dir string, _ map[string]any) {
			if err := os.WriteFile(filepath.Join(dir, "extra"), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlink artifact", "symlink", func(t *testing.T, dir string, _ map[string]any) {
			// The target lives outside the image root, so the exact entry set
			// still holds and only the symlink guard can reject it.
			target := filepath.Join(t.TempDir(), "outside-kernel")
			if err := os.WriteFile(target, []byte("kernel"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(dir, "kernel")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(dir, "kernel")); err != nil {
				t.Fatal(err)
			}
		}},
		{"manifest id mismatch", "manifest id", func(_ *testing.T, _ string, m map[string]any) {
			m["id"] = "other-v9"
		}},
		{"wrong filesystem", "filesystem", func(_ *testing.T, _ string, m map[string]any) {
			m["root"].(map[string]any)["filesystem"] = "xfs"
		}},
		{"disk size disagreement", "disagree", func(_ *testing.T, _ string, m map[string]any) {
			m["root"].(map[string]any)["diskGiB"] = uint64(11)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			const id = "ubuntu-24.04-v0"
			writeImage(t, s.ImageDir(), id, func(dir string, m map[string]any) {
				if tc.mutate != nil {
					tc.mutate(t, dir, m)
				}
			})
			root, err := os.OpenRoot(s.ImageDir())
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			_, verifyErr := s.verifyImage(t.Context(), root, id)
			images, err := s.VerifyImages(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantErr == "" {
				if verifyErr != nil {
					t.Fatalf("verifyImage: %v", verifyErr)
				}
				if len(images) != 1 || images[0].ID != id || images[0].RootSHA256 == "" {
					t.Fatalf("images = %v", images)
				}
				if images[0].RootVirtualSizeGiB != testGiB {
					t.Fatalf("base %d GiB", images[0].RootVirtualSizeGiB)
				}
				return
			}
			if verifyErr == nil || !strings.Contains(verifyErr.Error(), tc.wantErr) {
				t.Fatalf("verifyImage error = %v, want it to contain %q", verifyErr, tc.wantErr)
			}
			if len(images) != 0 {
				t.Fatalf("broken image was accepted: %v", images)
			}
		})
	}
}

// TestVerifyImageImagerBranches exercises the qemu-img result checks with an
// injected Imager, so every branch is reachable without QEMU.
func TestVerifyImageImagerBranches(t *testing.T) {
	cases := []struct {
		name    string
		imager  testImager
		wantErr string
	}{
		{"non-qcow2", testImager{format: "raw", virtualSize: testGiB << 30}, "want qcow2"},
		{"backing file", testImager{format: "qcow2", backing: "base.qcow2", virtualSize: testGiB << 30}, "backing file"},
		{"wrong virtual size", testImager{format: "qcow2", virtualSize: (testGiB + 1) << 30}, "virtual size"},
		{"qemu-img check failure", testImager{format: "qcow2", virtualSize: testGiB << 30, checkErr: errors.New("corrupt")}, "corrupt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Open(Config{DataDir: t.TempDir(), ImageDir: t.TempDir(), Imager: tc.imager})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			const id = "img-v0"
			writeImage(t, s.ImageDir(), id, nil)
			root, err := os.OpenRoot(s.ImageDir())
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			_, err = s.verifyImage(t.Context(), root, id)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("verifyImage error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestVerifyImagesHonoursContext(t *testing.T) {
	s := newTestStore(t)
	writeImage(t, s.ImageDir(), "img-v0", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.VerifyImages(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("VerifyImages with canceled context: %v", err)
	}
}

func TestStoreManifestLaunchAndLock(t *testing.T) {
	dataDir := t.TempDir()
	imageDir := t.TempDir()
	imager := testImager{virtualSize: testGiB << 30, format: "qcow2"}
	s, err := Open(Config{DataDir: dataDir, ImageDir: imageDir, Imager: imager})
	if err != nil {
		t.Fatal(err)
	}
	img := Image{ID: "img-v0", Dir: filepath.Join(imageDir, "img-v0"), RootVirtualSize: testGiB << 30, RootVirtualSizeGiB: testGiB}
	id := "bx_22222222"
	meta := VMMeta{
		SandboxID: id,
		Image:     "img-v0",
		Resources: Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 10},
		Env:       map[string]string{"A": "b"},
		VsockCID:  7,
	}
	if err := s.CreateVM(t.Context(), meta, img); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadVM(id)
	if err != nil || got.Env["A"] != "b" || got.VsockCID != 7 {
		t.Fatalf("ReadVM = %+v, %v", got, err)
	}
	if _, err := Open(Config{DataDir: dataDir, ImageDir: imageDir, Imager: imager}); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open = %v, want ErrLocked", err)
	}
	if err := s.WriteLaunchIntent(id); err != nil {
		t.Fatal(err)
	}
	got, err = s.ReadVM(id)
	if err != nil || got.QEMU == nil || got.QEMU.State != QEMULaunching || got.QEMU.BootID == "" {
		t.Fatalf("launch intent = %+v, %v", got.QEMU, err)
	}
	if err := s.CommitRunning(id, os.Getpid()); err != nil {
		t.Fatal(err)
	}
	got, err = s.ReadVM(id)
	if err != nil || got.QEMU.State != QEMURunning || got.QEMU.PID != os.Getpid() || got.QEMU.StartTicks == 0 {
		t.Fatalf("running record = %+v, %v", got.QEMU, err)
	}
	if err := s.RemoveVM(id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "vms", id)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("RemoveVM left the directory: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := Open(Config{DataDir: dataDir, ImageDir: imageDir, Imager: imager})
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	again.Close()
}

func TestQuarantineUsesUniqueNames(t *testing.T) {
	s := newTestStore(t)
	img := Image{ID: "img-v0", Dir: filepath.Join(s.ImageDir(), "img-v0"), RootVirtualSize: testGiB << 30, RootVirtualSizeGiB: testGiB}
	for _, id := range []string{"bx_33333333", "bx_44444444"} {
		if err := s.CreateVM(t.Context(), VMMeta{
			SandboxID: id,
			Image:     "img-v0",
			Resources: Resources{VCPUs: 1, MemoryMiB: 128, DiskGiB: 10},
			VsockCID:  3,
		}, img); err != nil {
			t.Fatal(err)
		}
	}
	first, err := s.Quarantine("bx_33333333")
	if err != nil {
		t.Fatal(err)
	}
	// Recreate the same id and quarantine again: names must differ.
	if err := s.CreateVM(t.Context(), VMMeta{
		SandboxID: "bx_33333333",
		Image:     "img-v0",
		Resources: Resources{VCPUs: 1, MemoryMiB: 128, DiskGiB: 10},
		VsockCID:  4,
	}, img); err != nil {
		t.Fatal(err)
	}
	second, err := s.Quarantine("bx_33333333")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("quarantine names collided: %q", first)
	}
	for _, name := range []string{first, second} {
		if _, err := os.Stat(filepath.Join(s.DataDir(), "orphans", name)); err != nil {
			t.Fatalf("quarantined dir %s: %v", name, err)
		}
	}
	if _, err := s.Quarantine("../escape"); err == nil {
		t.Fatal("Quarantine accepted a non-id")
	}
}

func TestManifestTempSymlinkIsNotFollowed(t *testing.T) {
	s := newTestStore(t)
	img := Image{ID: "img-v0", Dir: filepath.Join(s.ImageDir(), "img-v0"), RootVirtualSize: testGiB << 30, RootVirtualSizeGiB: testGiB}
	id := "bx_55555555"
	if err := s.CreateVM(t.Context(), VMMeta{
		SandboxID: id,
		Image:     "img-v0",
		Resources: Resources{VCPUs: 1, MemoryMiB: 128, DiskGiB: 10},
		VsockCID:  5,
	}, img); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(s.VMDir(id), manifestName+manifestTmpExt)); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteLaunchIntent(id); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "unchanged" {
		t.Fatalf("manifest temp symlink was followed: %q, %v", data, err)
	}
	if _, err := s.ReadVM(id); err != nil {
		t.Fatalf("manifest not published: %v", err)
	}
}

func TestReadVMRejectsInvalidTypedMetadata(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*VMMeta)
	}{
		{"bad image id", func(m *VMMeta) { m.Image = "Bad/ID" }},
		{"zero vcpus", func(m *VMMeta) { m.Resources.VCPUs = 0 }},
		{"tiny memory", func(m *VMMeta) { m.Resources.MemoryMiB = 1 }},
		{"zero disk", func(m *VMMeta) { m.Resources.DiskGiB = 0 }},
		{"reserved cid", func(m *VMMeta) { m.VsockCID = 2 }},
		{"any cid", func(m *VMMeta) { m.VsockCID = math.MaxUint32 }},
		{"unknown qemu state", func(m *VMMeta) {
			m.QEMU = &QEMURecord{State: "weird", BootID: "boot"}
		}},
		{"running without pid", func(m *VMMeta) {
			m.QEMU = &QEMURecord{State: QEMURunning, BootID: "boot", StartTicks: 5}
		}},
		{"running without ticks", func(m *VMMeta) {
			m.QEMU = &QEMURecord{State: QEMURunning, BootID: "boot", PID: 1}
		}},
		{"qemu without boot id", func(m *VMMeta) {
			m.QEMU = &QEMURecord{State: QEMULaunching}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			id := "bx_22222222"
			img := Image{ID: "img-v0", Dir: filepath.Join(s.ImageDir(), "img-v0"), RootVirtualSize: testGiB << 30, RootVirtualSizeGiB: testGiB}
			if err := s.CreateVM(t.Context(), VMMeta{
				SandboxID: id,
				Image:     "img-v0",
				Resources: Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 10},
				VsockCID:  7,
			}, img); err != nil {
				t.Fatal(err)
			}
			meta, err := s.ReadVM(id)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(&meta)
			raw, err := json.Marshal(meta)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(s.VMDir(id), manifestName), raw, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ReadVM(id); err == nil {
				t.Fatal("ReadVM accepted invalid typed metadata")
			}
		})
	}

	// A well-formed running record still validates.
	s := newTestStore(t)
	id := "bx_33333333"
	img := Image{ID: "img-v0", Dir: filepath.Join(s.ImageDir(), "img-v0"), RootVirtualSize: testGiB << 30, RootVirtualSizeGiB: testGiB}
	if err := s.CreateVM(t.Context(), VMMeta{
		SandboxID: id,
		Image:     "img-v0",
		Resources: Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 10},
		VsockCID:  7,
	}, img); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteLaunchIntent(id); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitRunning(id, os.Getpid()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadVM(id); err != nil {
		t.Fatalf("valid running record: %v", err)
	}
}

func TestCreateVMRejectsInvalidInput(t *testing.T) {
	s := newTestStore(t)
	img := Image{ID: "img-v0", Dir: filepath.Join(s.ImageDir(), "img-v0"), RootVirtualSize: testGiB << 30, RootVirtualSizeGiB: testGiB}
	for _, id := range []string{"", "../escape", "bx_bad/slash", "UPPER"} {
		if err := s.CreateVM(t.Context(), VMMeta{SandboxID: id, Image: "img-v0"}, img); err == nil {
			t.Errorf("CreateVM accepted id %q", id)
		}
	}
	if err := s.CreateVM(t.Context(), VMMeta{
		SandboxID: "bx_66666666",
		Image:     "img-v0",
		Resources: Resources{VCPUs: 1, MemoryMiB: 128, DiskGiB: 1},
	}, img); err == nil {
		t.Fatal("CreateVM accepted a disk smaller than the image base")
	}
	storageWriteBase(t, s, "img-v0")
	if err := s.CreateVM(t.Context(), VMMeta{
		SandboxID: "bx_77777777",
		Image:     "img-v0",
		Resources: Resources{VCPUs: 1, MemoryMiB: 128, DiskGiB: 10},
	}, Image{ID: "img-v0", Dir: filepath.Join(s.ImageDir(), "img-v0"), RootVirtualSize: 0, RootVirtualSizeGiB: 0}); err != nil {
		t.Fatalf("CreateVM with zero-size base image: %v", err)
	}
}
