package node

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// prepareStoppedVM writes a durably stopped VM record with the given CID, as a
// previous node process would have left it.
func prepareStoppedVM(t *testing.T, dataDir, imageDir, id string, cid uint32) {
	t.Helper()
	store, err := storage.Open(storage.Config{
		DataDir:  dataDir,
		ImageDir: imageDir,
		Imager:   &fakeImager{diskGiB: defaultTestBaseGiB},
		Log:      quietLog(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	meta := storage.VMMeta{
		SandboxID:       id,
		Image:           testImageID,
		Resources:       storage.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: defaultTestBaseGiB},
		VsockCID:        cid,
		CreatedAtUnixMs: time.Now().UnixMilli(),
	}
	img := storage.Image{
		ID:                 testImageID,
		Dir:                filepath.Join(imageDir, testImageID),
		RootVirtualSize:    defaultTestBaseGiB << 30,
		RootVirtualSizeGiB: defaultTestBaseGiB,
	}
	if err := store.CreateVM(t.Context(), meta, img); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitStopped(id, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
}

// TestRecoveredVMKeepsCIDOutsideConfiguredRange proves a durable CID is never
// checked against the configured allocation range: a VM recorded at CID 40 is
// adopted with its CID while the range is 3..10, keeps it through StartVM, and
// a new VM allocates inside 3..10 and never that CID.
func TestRecoveredVMKeepsCIDOutsideConfiguredRange(t *testing.T) {
	const recordedCID uint32 = 40
	adoptedID := ids.Sandbox()
	freshID := ids.Sandbox()
	h := newHarness(t, harnessOptions{
		prepare: func(t *testing.T, dataDir, imageDir string) {
			prepareStoppedVM(t, dataDir, imageDir, adoptedID, recordedCID)
		},
		configure: func(cfg *Config) {
			cfg.MinCID, cfg.MaxCID = 3, 10
		},
	})

	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: adoptedID})
	if err != nil {
		t.Fatalf("GetVM adopted: %v", err)
	}
	if vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("adopted state = %s (%v), want STOPPED", vm.GetState(), vm.GetError())
	}
	meta, err := h.mg.store.ReadVM(adoptedID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.VsockCID != recordedCID {
		t.Fatalf("adopted CID = %d, want the recorded %d", meta.VsockCID, recordedCID)
	}

	// The recorded CID stays usable: the allocation range bounds new CIDs
	// only, never an existing VM's.
	started, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: adoptedID})
	if err != nil {
		t.Fatalf("StartVM adopted: %v", err)
	}
	if started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("started state = %s (%v), want READY", started.GetState(), started.GetError())
	}
	if meta, err = h.mg.store.ReadVM(adoptedID); err != nil {
		t.Fatal(err)
	}
	if meta.VsockCID != recordedCID {
		t.Fatalf("CID after StartVM = %d, want the recorded %d", meta.VsockCID, recordedCID)
	}

	h.create(freshID, nil)
	h.mg.mu.Lock()
	freshCID := h.mg.vms[freshID].meta.VsockCID
	h.mg.mu.Unlock()
	if freshCID < 3 || freshCID > 10 {
		t.Fatalf("new VM CID = %d, want inside the configured 3..10", freshCID)
	}
	if freshCID == recordedCID {
		t.Fatalf("new VM CID = %d, reusing the adopted VM's out-of-range CID", freshCID)
	}

	h.delete(freshID)
	h.delete(adoptedID)
}

// TestCreateVMHonoursConfiguredCIDRange proves new allocations stay inside
// MIN..MAX and report exhaustion at MAX.
func TestCreateVMHonoursConfiguredCIDRange(t *testing.T) {
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.MinCID, cfg.MaxCID = 5, 7
	}})
	var created []string
	for range 3 {
		id := ids.Sandbox()
		h.create(id, nil)
		created = append(created, id)
	}
	for i, id := range created {
		h.mg.mu.Lock()
		got := h.mg.vms[id].meta.VsockCID
		h.mg.mu.Unlock()
		if want := uint32(5 + i); got != want {
			t.Fatalf("VM %d CID = %d, want %d", i, got, want)
		}
	}

	_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: ids.Sandbox(),
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("CreateVM past MAX error = %v, want ResourceExhausted", err)
	}
	if !strings.Contains(err.Error(), "no vsock CID is free") {
		t.Fatalf("CreateVM error %q does not explain the exhausted CID range", err)
	}

	for _, id := range created {
		h.delete(id)
	}
}
