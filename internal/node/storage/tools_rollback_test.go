package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
)

// A10 D1: a retained (unproven) tool keeps its target and every directory
// containing it, on the same run's rollback paths and at restart; a proven
// tool failure still rolls back.

// toolFaultImager is snapshotImager with per-operation injected errors.
type toolFaultImager struct {
	snapshotImager
	mu                        sync.Mutex
	create, grow, info, check error
	// afterFault, when set, runs with the target path when a fault fires.
	afterFault func(path string)
	// match, when set, limits faults to targets whose path contains it.
	match string
}

func (i *toolFaultImager) fault(which *error, target string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.match != "" && !strings.Contains(target, i.match) {
		return nil
	}
	return *which
}

// fired runs afterFault for target.
func (i *toolFaultImager) fired(target string) {
	i.mu.Lock()
	hook := i.afterFault
	i.mu.Unlock()
	if hook != nil {
		hook(target)
	}
}

func (i *toolFaultImager) set(which *error, err error) {
	i.mu.Lock()
	*which = err
	i.mu.Unlock()
}

func (i *toolFaultImager) CreateOverlay(ctx context.Context, dst ImageRef, n uint64) error {
	o := dst.File.Name()
	if err := i.fault(&i.create, o); err != nil {
		// A real tool may have created the overlay before it was retained.
		_ = os.WriteFile(o, []byte("partial"), 0o600)
		i.fired(o)
		return err
	}
	return i.snapshotImager.CreateOverlay(ctx, dst, n)
}

func (i *toolFaultImager) Grow(ctx context.Context, ref ImageRef, n uint64) error {
	f := ref.File.Name()
	if err := i.fault(&i.grow, f); err != nil {
		i.fired(f)
		return err
	}
	return i.snapshotImager.Grow(ctx, ref, n)
}

func (i *toolFaultImager) Info(ctx context.Context, ref ImageRef) (QemuInfo, error) {
	f := ref.File.Name()
	if err := i.fault(&i.info, f); err != nil {
		i.fired(f)
		return QemuInfo{}, err
	}
	return i.snapshotImager.Info(ctx, ref)
}

func (i *toolFaultImager) Check(ctx context.Context, ref ImageRef) error {
	f := ref.File.Name()
	if err := i.fault(&i.check, f); err != nil {
		i.fired(f)
		return err
	}
	return i.snapshotImager.Check(ctx, ref)
}

// toolLeases is a TargetLeases holding a set of inodes.
type toolLeases struct {
	mu   sync.Mutex
	held map[[2]uint64]bool
}

func (l *toolLeases) TargetHeld(dev, ino uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held[[2]uint64{dev, ino}]
}

func (l *toolLeases) hold(t *testing.T, p string) {
	info, err := os.Lstat(p)
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		panic(err)
	}
	st := info.Sys().(*syscall.Stat_t)
	l.mu.Lock()
	if l.held == nil {
		l.held = map[[2]uint64]bool{}
	}
	l.held[[2]uint64{uint64(st.Dev), st.Ino}] = true
	l.mu.Unlock()
}

const toolSourceVM = "bx_22222222"

func toolRollbackStore(t *testing.T) (*Store, Image, *toolFaultImager, *toolLeases) {
	t.Helper()
	dataDir, imageDir := t.TempDir(), t.TempDir()
	img := Image{
		ID: "img-v0", Dir: filepath.Join(imageDir, "img-v0"),
		RootVirtualSize: testGiB << 30, RootVirtualSizeGiB: testGiB,
		RootSHA256: strings.Repeat("a", 64),
	}
	imager := &toolFaultImager{snapshotImager: snapshotImager{basePath: filepath.Join(img.Dir, "root.qcow2"), virtualSize: testGiB << 30}}
	leases := &toolLeases{}
	s, err := Open(Config{DataDir: dataDir, ImageDir: imageDir, Imager: imager, Leases: leases})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	storageWriteBase(t, s, img.ID)
	meta := VMMeta{SandboxID: toolSourceVM, Image: img.ID, Resources: Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: testGiB}, VsockCID: 3}
	if err := s.CreateVM(t.Context(), meta, img); err != nil {
		t.Fatal(err)
	}
	return s, img, imager, leases
}

func toolRetained(op string) error {
	return fmt.Errorf("storage: %s: %w", op, ErrToolRetained)
}

func toolVMMeta(id string, gib uint64) VMMeta {
	return VMMeta{SandboxID: id, Image: "img-v0", Resources: Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: gib}, VsockCID: 4}
}

func toolExists(t *testing.T, p string) bool {
	t.Helper()
	_, err := os.Lstat(p)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return err == nil
}

func TestCreateVMRollbackKeepsRetainedToolTarget(t *testing.T) {
	cases := []struct {
		name   string
		gib    uint64
		inject func(i *toolFaultImager)
		keep   bool
	}{
		{"overlay create retained", testGiB, func(i *toolFaultImager) { i.set(&i.create, toolRetained("create")) }, true},
		{"grow retained", testGiB + 5, func(i *toolFaultImager) { i.set(&i.grow, toolRetained("resize")) }, true},
		{"overlay create proven failure", testGiB, func(i *toolFaultImager) { i.set(&i.create, errors.New("qemu-img exit 1")) }, false},
		{"grow proven failure", testGiB + 5, func(i *toolFaultImager) { i.set(&i.grow, errors.New("qemu-img exit 1")) }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, img, imager, _ := toolRollbackStore(t)
			tc.inject(imager)
			const id = "bx_33333333"
			err := s.CreateVM(t.Context(), toolVMMeta(id, tc.gib), img)
			if err == nil {
				t.Fatal("CreateVM succeeded")
			}
			if errors.Is(err, ErrToolRetained) != tc.keep {
				t.Fatalf("CreateVM = %v; retained %v", err, tc.keep)
			}
			if got := toolExists(t, s.VMDir(id)); got != tc.keep {
				t.Fatalf("vm dir kept %v, want %v", got, tc.keep)
			}
			if tc.keep && !toolExists(t, s.DiskPath(id)) {
				t.Fatal("the retained tool's target was removed")
			}
		})
	}
}

func TestCreateVMFromSnapshotRollbackKeepsRetainedToolTarget(t *testing.T) {
	cases := map[string]func(i *toolFaultImager){
		"info retained":  func(i *toolFaultImager) { i.set(&i.info, toolRetained("info")) },
		"check retained": func(i *toolFaultImager) { i.set(&i.check, toolRetained("check")) },
		"grow retained":  func(i *toolFaultImager) { i.set(&i.grow, toolRetained("resize")) },
	}
	for name, inject := range cases {
		t.Run(name, func(t *testing.T) {
			s, img, imager, _ := toolRollbackStore(t)
			snap := publishSnapshotForTest(t, s, img, "snap / "+name)
			const id = "bx_44444444"
			imager.match = "/vms/" + id + "/"
			inject(imager)
			err := s.CreateVMFromSnapshot(t.Context(), toolVMMeta(id, testGiB+5), img, snap)
			if !errors.Is(err, ErrToolRetained) {
				t.Fatalf("CreateVMFromSnapshot = %v, want ErrToolRetained", err)
			}
			if !toolExists(t, s.DiskPath(id)) {
				t.Fatal("the retained tool's snapshot child was removed")
			}
		})
	}
}

func TestSnapshotAbortKeepsRetainedToolTarget(t *testing.T) {
	for name, inject := range map[string]func(i *toolFaultImager){
		"info retained":  func(i *toolFaultImager) { i.set(&i.info, toolRetained("info")) },
		"check retained": func(i *toolFaultImager) { i.set(&i.check, toolRetained("check")) },
	} {
		t.Run(name, func(t *testing.T) {
			s, img, imager, _ := toolRollbackStore(t)
			stage, err := s.BeginSnapshot("snap")
			if err != nil {
				t.Fatal(err)
			}
			inject(imager)
			if err := stage.Copy(t.Context(), toolSourceVM, img, testGiB<<30); !errors.Is(err, ErrToolRetained) {
				t.Fatalf("Copy = %v, want ErrToolRetained", err)
			}
			if err := stage.Abort(); !errors.Is(err, ErrToolRetained) {
				t.Fatalf("Abort = %v, want ErrToolRetained (stage kept)", err)
			}
			staged := filepath.Join(s.DataDir(), snapshotStagingDir, SnapshotKey("snap"), diskName)
			if !toolExists(t, staged) {
				t.Fatal("Abort removed the retained tool's staged disk")
			}
		})
	}
}

func TestRecoverSnapshotsKeepsHeldStage(t *testing.T) {
	s, img, _, leases := toolRollbackStore(t)
	for _, id := range []string{"held", "free"} {
		if _, err := s.BeginSnapshot(id); err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(s.DataDir(), snapshotStagingDir, SnapshotKey(id), diskName)
		if err := os.WriteFile(p, []byte("staged"), 0o600); err != nil {
			t.Fatal(err)
		}
		if id == "held" {
			leases.hold(t, p)
		}
	}
	if _, err := s.RecoverSnapshots(t.Context(), []Image{img}); err != nil {
		t.Fatal(err)
	}
	if !toolExists(t, filepath.Join(s.DataDir(), snapshotStagingDir, SnapshotKey("held"), diskName)) {
		t.Fatal("recovery removed a stage holding a leased target")
	}
	if toolExists(t, filepath.Join(s.DataDir(), snapshotStagingDir, SnapshotKey("free"))) {
		t.Fatal("recovery kept an unleased owned stage")
	}
}

func TestRemoveVMRefusesHeldTarget(t *testing.T) {
	s, _, _, leases := toolRollbackStore(t)
	leases.hold(t, s.DiskPath(toolSourceVM))
	if err := s.RemoveVM(toolSourceVM); err == nil {
		t.Fatal("RemoveVM removed a leased overlay")
	}
	if !toolExists(t, s.DiskPath(toolSourceVM)) {
		t.Fatal("the leased overlay is gone")
	}
}

// TestReflinkCopyIsInProcess: the snapshot copy runs in the node process
// (FICLONE, copy_file_range, then a bounded userspace copy), never a cp
// subprocess found on PATH, and honors cancellation.
func TestReflinkCopyIsInProcess(t *testing.T) {
	t.Setenv("PATH", "")
	dir := t.TempDir()
	content := make([]byte, 3<<20+17)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "src"), content, 0o600); err != nil {
		t.Fatal(err)
	}
	open := func(name string, flag int) *os.File {
		f, err := os.OpenFile(filepath.Join(dir, name), flag, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		return f
	}
	src := open("src", os.O_RDONLY)
	dst := open("dst", os.O_RDWR|os.O_CREATE|os.O_EXCL)
	if err := reflinkCopy(t.Context(), src, dst); err != nil {
		t.Fatalf("reflinkCopy: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "dst"))
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("copy differs (%d bytes, %v)", len(got), err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := reflinkCopy(ctx, open("src", os.O_RDONLY), open("dst2", os.O_RDWR|os.O_CREATE|os.O_EXCL)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reflinkCopy = %v, want Canceled", err)
	}
}

// toolTree records path → inode, mode and content for every entry under dir.
func toolTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, _ os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Lstat(p)
		if err != nil {
			return err
		}
		st := info.Sys().(*syscall.Stat_t)
		v := fmt.Sprintf("%d/%d %v", st.Dev, st.Ino, info.Mode())
		if info.Mode().IsRegular() {
			raw, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			v += " " + string(raw)
		}
		out[p] = v
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func toolTreesEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// TestRollbackConsultsLeasesOnOrdinaryErrors: when a tool fails with an
// ordinary error (not ErrToolRetained) but its target is still leased, the
// rollback keeps the target and its directory exactly (inode, mode,
// content, manifest included).
func TestRollbackConsultsLeasesOnOrdinaryErrors(t *testing.T) {
	cases := []struct {
		name   string
		gib    uint64
		inject func(s *Store, i *toolFaultImager, l *toolLeases, snap func() map[string]string, saved *map[string]string)
	}{
		{"create, busy target", testGiB, func(s *Store, i *toolFaultImager, l *toolLeases, snap func() map[string]string, saved *map[string]string) {
			i.set(&i.create, errors.New("jailer: the target inode is held by another record"))
			i.mu.Lock()
			i.afterFault = func(path string) { l.hold(nil, path); *saved = snap() }
			i.mu.Unlock()
		}},
		{"grow, ordinary failure", testGiB + 5, func(s *Store, i *toolFaultImager, l *toolLeases, snap func() map[string]string, saved *map[string]string) {
			i.set(&i.grow, errors.New("qemu-img: interrupted"))
			i.mu.Lock()
			i.afterFault = func(path string) { l.hold(nil, path); *saved = snap() }
			i.mu.Unlock()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, img, imager, leases := toolRollbackStore(t)
			const id = "bx_55555555"
			var saved map[string]string
			snap := func() map[string]string { return toolTree(t, s.VMDir(id)) }
			tc.inject(s, imager, leases, snap, &saved)
			if err := s.CreateVM(t.Context(), toolVMMeta(id, tc.gib), img); err == nil {
				t.Fatal("CreateVM succeeded")
			}
			if saved == nil {
				t.Fatal("the fault never ran")
			}
			if after := toolTree(t, s.VMDir(id)); !toolTreesEqual(saved, after) {
				t.Fatalf("the leased target's directory changed\nbefore %v\nafter  %v", saved, after)
			}
		})
	}
}

// TestSnapshotAbortAndRecoveryKeepHeldStageExactly: a stage holding a
// leased inode survives Abort and recovery with every inode, mode and
// content unchanged.
func TestSnapshotAbortAndRecoveryKeepHeldStageExactly(t *testing.T) {
	s, img, imager, leases := toolRollbackStore(t)
	stage, err := s.BeginSnapshot("snap")
	if err != nil {
		t.Fatal(err)
	}
	imager.set(&imager.check, errors.New("qemu-img check: interrupted"))
	stageDir := filepath.Join(s.DataDir(), snapshotStagingDir, SnapshotKey("snap"))
	imager.mu.Lock()
	imager.afterFault = func(path string) { leases.hold(nil, path) }
	imager.mu.Unlock()
	if err := stage.Copy(t.Context(), toolSourceVM, img, testGiB<<30); err == nil {
		t.Fatal("Copy succeeded")
	}
	before := toolTree(t, stageDir)
	if err := stage.Abort(); err == nil {
		t.Fatal("Abort reported success over a leased stage")
	}
	if after := toolTree(t, stageDir); !toolTreesEqual(before, after) {
		t.Fatalf("Abort changed a leased stage\nbefore %v\nafter  %v", before, after)
	}
	if _, err := s.RecoverSnapshots(t.Context(), []Image{img}); err != nil {
		t.Fatal(err)
	}
	if after := toolTree(t, stageDir); !toolTreesEqual(before, after) {
		t.Fatalf("recovery changed a leased stage\nbefore %v\nafter  %v", before, after)
	}
}

// TestCreateVMFromSnapshotProvenFailureRemovesChild: a proven (unleased,
// non-retained) tool failure still rolls the child back.
func TestCreateVMFromSnapshotProvenFailureRemovesChild(t *testing.T) {
	for name, inject := range map[string]func(i *toolFaultImager){
		"info":  func(i *toolFaultImager) { i.set(&i.info, errors.New("qemu-img info: exit 1")) },
		"check": func(i *toolFaultImager) { i.set(&i.check, errors.New("qemu-img check: exit 2")) },
		"grow":  func(i *toolFaultImager) { i.set(&i.grow, errors.New("qemu-img resize: exit 1")) },
	} {
		t.Run(name, func(t *testing.T) {
			s, img, imager, _ := toolRollbackStore(t)
			snap := publishSnapshotForTest(t, s, img, "snap / "+name)
			const id = "bx_66666666"
			imager.match = "/vms/" + id + "/"
			inject(imager)
			var fired bool
			imager.afterFault = func(string) { fired = true }
			if err := s.CreateVMFromSnapshot(t.Context(), toolVMMeta(id, testGiB+5), img, snap); err == nil || errors.Is(err, ErrToolRetained) {
				t.Fatalf("CreateVMFromSnapshot = %v, want a proven failure", err)
			}
			if !fired {
				t.Fatal("the fault never reached the child (vacuous control)")
			}
			if toolExists(t, s.VMDir(id)) {
				t.Fatal("a proven failure left the child directory")
			}
		})
	}
}
