package node

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("got %v (%v), want code %v", err, got, want)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestCreateIsIdempotentAndSharesBoot(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	var wg sync.WaitGroup
	results := make([]*nodev1.VM, 8)
	errs := make([]error, 8)
	for i := range results {
		wg.Go(func() {
			results[i], errs[i] = h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
				SandboxId: id,
				Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
			})
		})
	}
	wg.Wait()
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("create %d: %v", i, errs[i])
		}
		if results[i].GetState() != nodev1.VMState_VM_STATE_READY {
			t.Fatalf("create %d state %s", i, results[i].GetState())
		}
		if results[i].GetCreatedAtUnixMs() != results[0].GetCreatedAtUnixMs() {
			t.Fatalf("create %d returned a different VM", i)
		}
	}
	if got := h.launcher.count(); got != 1 {
		t.Fatalf("launched %d QEMUs for one sandbox, want 1", got)
	}
	got, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || got.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("GetVM = %v, %v", got, err)
	}
	h.delete(id)
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetVM after delete: %v", err)
	}
}

func TestCapacityIncludingDiskAndOverflow(t *testing.T) {
	h := newHarness(t, harnessOptions{capacity: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1}})
	h.create(ids.Sandbox(), &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1})

	_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: ids.Sandbox(),
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 100, DiskGib: 1},
	})
	wantCode(t, err, codes.ResourceExhausted)

	for _, res := range []*nodev1.Resources{
		nil,
		{Vcpus: 0, MemoryMib: 100, DiskGib: 1},
		{Vcpus: 1, MemoryMib: 0, DiskGib: 1},
		{Vcpus: 1, MemoryMib: 100, DiskGib: 0},
		{Vcpus: 1, MemoryMib: 100, DiskGib: math.MaxUint64},
	} {
		_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: ids.Sandbox(), Resources: res})
		wantCode(t, err, codes.InvalidArgument)
	}
}

func TestCreateValidation(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	res := &nodev1.Resources{Vcpus: 1, MemoryMib: 256, DiskGib: 1}

	_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: "not-an-id", Resources: res})
	wantCode(t, err, codes.InvalidArgument)

	_, err = h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: ids.Sandbox(), Resources: res, Image: "missing"})
	wantCode(t, err, codes.NotFound)

	_, err = h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: ids.Sandbox(), Resources: res, FromSnapshot: "missing"})
	wantCode(t, err, codes.NotFound)

	_, err = h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: ids.Sandbox(), Resources: res, Hostname: "bad host name"})
	wantCode(t, err, codes.InvalidArgument)

	// Empty snapshot ids are invalid for save; deletion stays idempotent.
	if _, err := h.mg.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if _, err := h.mg.DeleteSnapshot(t.Context(), &nodev1.DeleteSnapshotRequest{SnapshotId: "nope"}); err != nil {
		t.Fatalf("DeleteSnapshot(unknown): %v", err)
	}
}

func TestCreateCancellationAbortsAndAllowsRecreate(t *testing.T) {
	launcher := &fakeLauncher{block: true}
	h := newHarness(t, harnessOptions{launcher: launcher})
	id := ids.Sandbox()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(ctx, &nodev1.CreateVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
		})
		done <- err
	}()
	waitFor(t, "launch start", func() bool { return launcher.count() > 0 })
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("CreateVM after cancel: %v", err)
	}
	waitFor(t, "boot cleanup", func() bool {
		_, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		return status.Code(err) == codes.NotFound
	})
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("aborted VM left its directory: %v", err)
	}

	launcher.setBlock(false)
	h.create(id, &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1})
}

func TestDeleteWhileBooting(t *testing.T) {
	launcher := &fakeLauncher{block: true}
	h := newHarness(t, harnessOptions{launcher: launcher})
	id := ids.Sandbox()
	done := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
		})
		done <- err
	}()
	waitFor(t, "launch start", func() bool { return launcher.count() > 0 })
	if _, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("DeleteVM while booting: %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("CreateVM succeeded although DeleteVM aborted the boot")
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted booting VM left its directory: %v", err)
	}
}

func TestBootFailureKeepsFailedRecordAndReleasesCapacity(t *testing.T) {
	launcher := &fakeLauncher{failOnce: errors.New("qemu refused to start")}
	h := newHarness(t, harnessOptions{
		launcher: launcher,
		capacity: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
	})
	id := ids.Sandbox()
	_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
	})
	wantCode(t, err, codes.Internal)

	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatal(err)
	}
	if vm.GetState() != nodev1.VMState_VM_STATE_FAILED || vm.GetError() == "" {
		t.Fatalf("failed VM = %v", vm)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id, "vm.json")); err != nil {
		t.Fatalf("failed VM lost its manifest: %v", err)
	}
	// Capacity was released: a full-size VM now fits.
	h.create(ids.Sandbox(), &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1})

	// A same-id create does not launch over the FAILED record.
	before := launcher.count()
	_, err = h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 256, DiskGib: 1},
	})
	wantCode(t, err, codes.Internal)
	if got := launcher.count(); got != before {
		t.Fatalf("same-id create launched again: %d -> %d", before, got)
	}
	h.delete(id)
}

func TestVMDiesPreservesRecordAndDisk(t *testing.T) {
	h := newHarness(t, harnessOptions{capacity: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1}})
	id := ids.Sandbox()
	h.create(id, &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1})
	proc := h.launcher.lastProcess()
	proc.exit(errors.New("qemu: exited"))

	waitFor(t, "FAILED state", func() bool {
		vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		return err == nil && vm.GetState() == nodev1.VMState_VM_STATE_FAILED
	})
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.GetError() == "" {
		t.Fatalf("GetVM = %v, %v", vm, err)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id, "vm.json")); err != nil {
		t.Fatalf("dead VM lost its manifest: %v", err)
	}
	list, err := h.mg.ListVMs(t.Context(), &nodev1.ListVMsRequest{})
	if err != nil || len(list.Vms) != 1 || list.Vms[0].GetState() != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("ListVMs = %v, %v", list, err)
	}
	// Runtime capacity was released while the disk stays.
	h.create(ids.Sandbox(), &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1})
	h.delete(id)
}

func TestDeleteStopErrorKeepsRecord(t *testing.T) {
	launcher := &fakeLauncher{stopErr: errors.New("still running")}
	h := newHarness(t, harnessOptions{launcher: launcher})
	id := ids.Sandbox()
	h.create(id, nil)

	_, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id})
	wantCode(t, err, codes.Internal)
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("DeleteVM error removed the record: %v", err)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); err != nil {
		t.Fatalf("DeleteVM error removed the disk: %v", err)
	}
	h.launcher.lastProcess().setStopErr(nil)
	h.delete(id)
}

func TestShutdownRetryAfterTimeout(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)

	// While the manager runs it holds the exclusive data lock.
	if _, err := storage.Open(storage.Config{DataDir: h.dataDir, ImageDir: h.imageDir, Imager: &fakeImager{diskGiB: defaultTestBaseGiB}}); !errors.Is(err, storage.ErrLocked) {
		t.Fatalf("second store while the manager runs = %v, want ErrLocked", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := h.mg.Shutdown(ctx); err == nil {
		t.Fatal("Shutdown with a canceled context reported success")
	}
	if err := h.mg.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	// A timed-out first call must not make the completion a false success:
	// the lock is genuinely gone now.
	again, err := storage.Open(storage.Config{DataDir: h.dataDir, ImageDir: h.imageDir, Imager: &fakeImager{diskGiB: defaultTestBaseGiB}})
	if err != nil {
		t.Fatalf("data lock was not released by shutdown: %v", err)
	}
	if err := again.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInfoDeepClonesImagesAndBoundsDisk(t *testing.T) {
	h := newHarness(t, harnessOptions{capacity: &nodev1.Resources{Vcpus: 8, MemoryMib: 8192, DiskGib: 2}})
	h.create(ids.Sandbox(), &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1})

	info, err := h.mg.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if info.GetDefaultImage() != testImageID || len(info.GetImages()) != 1 {
		t.Fatalf("Info images = %v", info.GetImages())
	}
	if info.GetImages()[0].GetSha256() == "" {
		t.Fatal("Info image has no sha256")
	}
	if info.GetDiskFreeMib() > 1*1024 {
		t.Fatalf("DiskFreeMib %d offers more than the remaining 10 GiB", info.GetDiskFreeMib())
	}
	// Caller mutation must not leak into the next response.
	info.Images[0].Id = "mutated"
	again, err := h.mg.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if again.GetImages()[0].GetId() != testImageID {
		t.Fatalf("Info reused an image message: %q", again.GetImages()[0].GetId())
	}
}

func TestDeleteUnknownIsOK(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	for _, id := range []string{"", "not-an-id", ids.Sandbox()} {
		if _, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
			t.Fatalf("DeleteVM(%q): %v", id, err)
		}
	}
}
