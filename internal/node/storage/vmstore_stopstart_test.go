package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// stopImager is an imager whose Info reflects the last written size per
// file, so GrowVMDisk's compare-against-actual logic is exercised.
type stopImager struct {
	mu    sync.Mutex
	sizes map[string]uint64
	grows []string
}

func (i *stopImager) size(file string) uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.sizes[file]
}

func (i *stopImager) CreateOverlay(_ context.Context, dst ImageRef, virtualSize uint64) error {
	return i.Grow(context.Background(), dst, virtualSize)
}

func (i *stopImager) Grow(_ context.Context, ref ImageRef, size uint64) error {
	file := ref.File.Name()
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.sizes == nil {
		i.sizes = map[string]uint64{}
	}
	i.sizes[file] = size
	i.grows = append(i.grows, file)
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], size)
	return os.WriteFile(file, data[:], 0o600)
}

func (i *stopImager) Info(_ context.Context, ref ImageRef) (QemuInfo, error) {
	file := ref.File.Name()
	i.mu.Lock()
	defer i.mu.Unlock()
	size := i.sizes[file]
	return QemuInfo{Format: "qcow2", VirtualSize: size}, nil
}

func (i *stopImager) Check(context.Context, ImageRef) error { return nil }

// grownFiles lists the files Grow was called on.
func (i *stopImager) grownFiles() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.grows
}

// stopTestStore opens a store with the size-aware imager.
func stopTestStore(t *testing.T) (*Store, *stopImager) {
	t.Helper()
	imager := &stopImager{}
	s, err := Open(Config{
		DataDir:  t.TempDir(),
		ImageDir: t.TempDir(),
		Imager:   imager,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	storageWriteBase(t, s, "img-v0")
	return s, imager
}

// stopTestVM creates one VM and returns its durable meta.
func stopTestVM(t *testing.T, s *Store, id string) VMMeta {
	t.Helper()
	meta := VMMeta{
		SandboxID: id,
		Image:     "img-v0",
		Resources: Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: testGiB},
		Env:       map[string]string{"OLD": "1"},
		Hostname:  "first",
		VsockCID:  7,
	}
	img := Image{ID: "img-v0", Dir: filepath.Join(s.ImageDir(), "img-v0"), RootVirtualSize: testGiB << 30, RootVirtualSizeGiB: testGiB}
	if err := s.CreateVM(t.Context(), meta, img); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestCommitStoppedClearsProcessAndError(t *testing.T) {
	s, _ := stopTestStore(t)
	id := "bx_22222222"
	stopTestVM(t, s, id)
	if err := s.WriteLaunchIntent(id); err != nil {
		t.Fatal(err)
	}
	if err := s.SetVMError(id, "boot failed hard"); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitStopped(id, 1234567); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Stopped() || got.StoppedAtUnixMs != 1234567 {
		t.Fatalf("CommitStopped left = %+v", got)
	}
	if got.QEMU != nil || got.LastError != "" {
		t.Fatalf("process record or error survived: %+v", got)
	}
	if err := s.CommitStopped(id, 0); err == nil {
		t.Fatal("CommitAccepted a zero timestamp")
	}
	if err := s.CommitStopped("bx_99999999", 1); err == nil {
		t.Fatal("CommitStopped an unknown vm")
	}
}

func TestCommitReadyRecordsStartAndClearsStop(t *testing.T) {
	s, _ := stopTestStore(t)
	id := "bx_22222222"
	stopTestVM(t, s, id)
	if err := s.CommitStopped(id, 1000); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitReady(id, 2000); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.StartedAtUnixMs != 2000 || got.StoppedAtUnixMs != 0 {
		t.Fatalf("CommitReady left = started %d stopped %d", got.StartedAtUnixMs, got.StoppedAtUnixMs)
	}
	if got.Stopped() {
		t.Fatalf("a ready VM still reports stopped: %+v", got)
	}
	if err := s.CommitReady(id, 0); err == nil {
		t.Fatal("CommitReady accepted a zero timestamp")
	}
}

func TestPrepareStartReplacesFieldsButKeepsStopMarker(t *testing.T) {
	s, _ := stopTestStore(t)
	id := "bx_22222222"
	stopTestVM(t, s, id)
	if err := s.CommitStopped(id, 1000); err != nil {
		t.Fatal(err)
	}
	err := s.PrepareStart(id, Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: testGiB + 5},
		map[string]string{"NEW": "2"}, "second")
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Stopped() {
		t.Fatalf("PrepareStart cleared the stop marker: %+v", got)
	}
	if got.Resources.DiskGiB != testGiB+5 || got.Resources.VCPUs != 4 {
		t.Fatalf("resources not applied: %+v", got.Resources)
	}
	if len(got.Env) != 1 || got.Env["NEW"] != "2" {
		t.Fatalf("env not replaced: %v", got.Env)
	}
	if got.Hostname != "second" {
		t.Fatalf("hostname not applied: %q", got.Hostname)
	}

	// A shrink is refused; a non-stopped VM is refused.
	if err := s.PrepareStart(id, Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: testGiB}, nil, ""); err == nil {
		t.Fatal("PrepareStart accepted a shrinking disk")
	}
	if err := s.WriteLaunchIntent(id); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareStart(id, Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: testGiB + 9}, nil, ""); err == nil {
		t.Fatal("PrepareStart accepted a VM with a process record")
	}
}

func TestRevertStartRestoresTheOriginalFields(t *testing.T) {
	s, _ := stopTestStore(t)
	id := "bx_22222222"
	original := stopTestVM(t, s, id)
	if err := s.CommitStopped(id, 1000); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareStart(id, Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: testGiB + 5},
		map[string]string{"NEW": "2"}, "second"); err != nil {
		t.Fatal(err)
	}
	// A revert may restore a size smaller than the (grown) disk file.
	if err := s.RevertStart(id, original.Resources, original.Env, original.Hostname); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Resources != original.Resources || got.Hostname != original.Hostname {
		t.Fatalf("revert left = %+v", got)
	}
	if len(got.Env) != 1 || got.Env["OLD"] != "1" {
		t.Fatalf("env not reverted: %v", got.Env)
	}
	if !got.Stopped() {
		t.Fatalf("revert lost the stop marker: %+v", got)
	}
	// A revert of a VM with a live process record is refused.
	if err := s.WriteLaunchIntent(id); err != nil {
		t.Fatal(err)
	}
	if err := s.RevertStart(id, original.Resources, nil, ""); err == nil {
		t.Fatal("RevertStart accepted a VM with a process record")
	}
}

func TestGrowVMDiskComparesAgainstTheActualDisk(t *testing.T) {
	s, imager := stopTestStore(t)
	id := "bx_22222222"
	stopTestVM(t, s, id)
	disk := s.DiskPath(id)
	if got := imager.size(disk); got != testGiB<<30 {
		t.Fatalf("overlay starts at %d", got)
	}
	if err := s.GrowVMDisk(t.Context(), id, testGiB+2); err != nil {
		t.Fatal(err)
	}
	if got := imager.size(disk); got != (testGiB+2)<<30 {
		t.Fatalf("disk not grown: %d", got)
	}
	grows := len(imager.grownFiles())
	// Idempotent: an already-large disk is left untouched.
	if err := s.GrowVMDisk(t.Context(), id, testGiB+2); err != nil {
		t.Fatal(err)
	}
	if got := len(imager.grownFiles()); got != grows {
		t.Fatalf("Grow ran again: %d > %d", got, grows)
	}
	if err := s.GrowVMDisk(t.Context(), id, testGiB+9); err != nil {
		t.Fatal(err)
	}
	if got := imager.size(disk); got != (testGiB+9)<<30 {
		t.Fatalf("disk not grown to the larger request: %d", got)
	}
	if err := s.GrowVMDisk(t.Context(), "bx_99999999", testGiB); err == nil {
		t.Fatal("GrowVMDisk accepted an unknown vm")
	}
}

func TestSyncVMDataFlushesTheKeptDisk(t *testing.T) {
	s, _ := stopTestStore(t)
	id := "bx_22222222"
	stopTestVM(t, s, id)
	if err := s.SyncVMData(id); err != nil {
		t.Fatalf("SyncVMData: %v", err)
	}
	if err := s.SyncVMData("bx_99999999"); err == nil {
		t.Fatal("SyncVMData accepted an unknown vm")
	}
	// A missing disk file is an error, not a silent success.
	if err := os.Remove(s.DiskPath(id)); err != nil {
		t.Fatal(err)
	}
	if err := s.SyncVMData(id); err == nil {
		t.Fatal("SyncVMData accepted a vanished disk")
	}
}

func TestReadVMRejectsNegativeLifecycleTimestamps(t *testing.T) {
	s, _ := stopTestStore(t)
	id := "bx_22222222"
	meta := stopTestVM(t, s, id)
	meta.StartedAtUnixMs = -1
	if err := s.writeManifest(meta); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadVM(id); err == nil {
		t.Fatal("ReadVM accepted a negative started timestamp")
	}
	meta.StartedAtUnixMs = 0
	meta.StoppedAtUnixMs = -5
	if err := s.writeManifest(meta); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadVM(id); err == nil {
		t.Fatal("ReadVM accepted a negative stopped timestamp")
	}
	// A stopped manifest without a process record reads back Stopped().
	meta.StoppedAtUnixMs = 42
	if err := s.writeManifest(meta); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadVM(id)
	if err != nil || !got.Stopped() {
		t.Fatalf("ReadVM of stopped manifest = %+v, %v", got, err)
	}
	// ...and the directory is still owned until it is deleted.
	if _, err := os.Stat(filepath.Join(s.DataDir(), "vms", id)); err != nil {
		t.Fatalf("stopped vm directory vanished: %v", err)
	}
	if err := s.RemoveVM(id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(s.DataDir(), "vms", id)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("RemoveVM left the stopped directory: %v", err)
	}
}

func TestPrepareStartRenameFaultLeavesOldManifest(t *testing.T) {
	s, _ := stopTestStore(t)
	id := "bx_22222222"
	original := stopTestVM(t, s, id)
	if err := s.CommitStopped(id, 1000); err != nil {
		t.Fatal(err)
	}
	s.cfg.Snapshots.Rename = func(root *os.Root, old, new string) error {
		return errors.New("rename fault")
	}
	err := s.PrepareStart(id, Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: testGiB + 5}, map[string]string{"N": "1"}, "next")
	if err == nil {
		t.Fatal("PrepareStart survived a rename fault")
	}
	if errors.Is(err, ErrManifestDurabilityUnknown) {
		t.Fatalf("a rename that never happened is not uncertain: %v", err)
	}
	got, rerr := s.ReadVM(id)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if got.Resources != original.Resources || got.Hostname != original.Hostname {
		t.Fatalf("manifest changed despite the failed rename: %+v", got)
	}
	if !got.Stopped() {
		t.Fatalf("manifest lost its stop marker: %+v", got)
	}
}

func TestPrepareStartDirSyncFaultIsUncertainCommit(t *testing.T) {
	s, _ := stopTestStore(t)
	id := "bx_22222222"
	stopTestVM(t, s, id)
	if err := s.CommitStopped(id, 1000); err != nil {
		t.Fatal(err)
	}
	s.cfg.Snapshots.SyncDir = func(root *os.Root, name string) error {
		return errors.New("directory sync fault")
	}
	err := s.PrepareStart(id, Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: testGiB + 5}, map[string]string{"N": "1"}, "next")
	if err == nil {
		t.Fatal("PrepareStart survived a directory-sync fault")
	}
	if !errors.Is(err, ErrManifestDurabilityUnknown) {
		t.Fatalf("post-rename sync failure must be uncertain: %v", err)
	}
	// The rename already published the new content: callers cannot assume
	// the old manifest stands.
	got, rerr := s.ReadVM(id)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if got.Resources.DiskGiB != testGiB+5 || got.Hostname != "next" {
		t.Fatalf("manifest = %+v; the rename already happened", got)
	}
	if !got.Stopped() {
		t.Fatalf("manifest lost its stop marker: %+v", got)
	}
}
