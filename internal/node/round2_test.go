package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func waiterCount(h *harness, id string) int {
	h.mg.mu.Lock()
	v := h.mg.vms[id]
	h.mg.mu.Unlock()
	if v == nil {
		return -1
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.waiters
}

func TestExistingIDIgnoresEveryOtherField(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	first := h.create(id, &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1})

	cases := []*nodev1.CreateVMRequest{
		{SandboxId: id},
		{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 128, DiskGib: 1}},
		{SandboxId: id, Image: "missing"},
		{SandboxId: id, FromSnapshot: "missing"},
		{SandboxId: id, Hostname: "bad host name"},
		{SandboxId: id, Env: map[string]string{"CHANGED": "1"}},
	}
	for i, req := range cases {
		got, err := h.mg.CreateVM(t.Context(), req)
		if err != nil {
			t.Fatalf("case %d: existing id rejected: %v", i, err)
		}
		if got.GetState() != nodev1.VMState_VM_STATE_READY || got.GetCreatedAtUnixMs() != first.GetCreatedAtUnixMs() {
			t.Fatalf("case %d returned a different VM: %v", i, got)
		}
	}
	if got := h.launcher.count(); got != 1 {
		t.Fatalf("launched %d times for one id", got)
	}
	meta, err := h.mg.store.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Env) != 0 {
		t.Fatalf("existing-id call mutated the environment: %v", meta.Env)
	}
}

func TestExistingBootingIDJoinsWithoutValidation(t *testing.T) {
	launcher := &fakeLauncher{}
	gate := make(chan struct{})
	launcher.setGate(gate)
	h := newHarness(t, harnessOptions{launcher: launcher})
	id := ids.Sandbox()
	first := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
		})
		first <- err
	}()
	waitFor(t, "boot start", func() bool { return launcher.count() > 0 })

	second := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id})
		second <- err
	}()
	select {
	case err := <-second:
		t.Fatalf("booting duplicate returned before boot finished: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(gate)
	if err := <-first; err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("joined create: %v", err)
	}
	if got := launcher.count(); got != 1 {
		t.Fatalf("booted %d VMs for one id", got)
	}
}

func TestConcurrentDeletesHaveOneCleanupOwner(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	proc := h.launcher.lastProcess()
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	proc.stageStop(gate, entered)

	first := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id})
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first delete never reached Stop")
	}
	second := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id})
		second <- err
	}()
	waitFor(t, "second delete to join", func() bool {
		h.mg.mu.Lock()
		v := h.mg.vms[id]
		h.mg.mu.Unlock()
		if v == nil {
			return false
		}
		v.mu.Lock()
		defer v.mu.Unlock()
		return v.deleteWaiters == 1
	})
	if got := h.launcher.stopCount(); got != 1 {
		t.Fatalf("two deletes called Stop %d times, want 1", got)
	}
	close(gate)
	if err := <-first; err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetVM after deletes: %v", err)
	}
	// Reusing the id must not be undone by the joined delete.
	vm := h.create(id, nil)
	if vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("recreated VM = %v", vm)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id, "disk.qcow2")); err != nil {
		t.Fatalf("recreated VM lost its disk: %v", err)
	}
}

func TestWatcherWriteIsSerializedWithDeleteAndRecreate(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	proc := h.launcher.lastProcess()

	entered := make(chan struct{})
	release := make(chan struct{})
	h.mg.beforePersist = func() {
		close(entered)
		<-release
	}
	proc.exit(errors.New("died"))

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher never reached the persist hook")
	}
	deleted := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id})
		deleted <- err
	}()
	select {
	case err := <-deleted:
		t.Fatalf("delete completed while an old watcher held v.mu: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(release)
	if err := <-deleted; err != nil {
		t.Fatalf("delete: %v", err)
	}
	h.mg.beforePersist = nil

	vm := h.create(id, nil)
	meta, err := h.mg.store.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	if meta.CreatedAtUnixMs != vm.GetCreatedAtUnixMs() {
		t.Fatalf("manifest belongs to the old generation: %+v", meta)
	}
	if meta.LastError != "" {
		t.Fatalf("old watcher overwrote the replacement manifest: %q", meta.LastError)
	}
}

func TestLastWaiterCancelBeforeReadyAborts(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	entered := make(chan struct{})
	release := make(chan struct{})
	h.mg.beforeReady = func() {
		close(entered)
		<-release
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(ctx, &nodev1.CreateVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
		})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("boot never reached the terminal transition")
	}
	cancel()
	// The last waiter leaves (waiters 0) or the boot cleanup already removed
	// the record (-1); both mean the boot was abandoned.
	waitFor(t, "last waiter to leave", func() bool { return waiterCount(h, id) <= 0 })
	close(release)
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("CreateVM = %v, want Canceled", err)
	}
	waitFor(t, "aborted boot cleanup", func() bool {
		_, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		return status.Code(err) == codes.NotFound
	})
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("aborted boot left its directory: %v", err)
	}
}

func TestOneOfTwoWaitersLeavingPreservesBoot(t *testing.T) {
	guest := &gatedPingGuest{entered: make(chan struct{}, 4), release: make(chan struct{})}
	h := newHarness(t, harnessOptions{fallback: guest, stubGuest: true})
	id := ids.Sandbox()
	ctx, cancel := context.WithCancel(t.Context())
	first := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(ctx, &nodev1.CreateVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
		})
		first <- err
	}()
	select {
	case <-guest.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("guest Ping never started")
	}
	second := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id})
		second <- err
	}()
	waitFor(t, "second waiter", func() bool { return waiterCount(h, id) == 2 })
	cancel()
	waitFor(t, "first waiter gone", func() bool { return waiterCount(h, id) == 1 })
	close(guest.release)
	if err := <-second; err != nil {
		t.Fatalf("remaining waiter: %v", err)
	}
	<-first // its outcome may be Canceled or Ready depending on the select
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("VM after partial waiter cancel = %v, %v", vm, err)
	}
	if got := h.launcher.count(); got != 1 {
		t.Fatalf("launched %d times", got)
	}
}

func TestStopFailureAfterPingKeepsHandleAndReservation(t *testing.T) {
	launcher := &fakeLauncher{stopErr: errors.New("cannot stop")}
	h := newHarness(t, harnessOptions{
		launcher:    launcher,
		fallback:    errorPingGuest{},
		stubGuest:   true,
		bootTimeout: 300 * time.Millisecond,
		capacity:    &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
	})
	id := ids.Sandbox()
	res := &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1}
	_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id, Resources: res})
	wantCode(t, err, codes.Internal)
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("failed VM = %v, %v", vm, err)
	}
	// The process might still be alive, so its reservation stays.
	h.mg.mu.Lock()
	held := h.mg.reserved.vcpus
	h.mg.mu.Unlock()
	if held != res.GetVcpus() {
		t.Fatalf("reservation after unproven stop = %d, want %d", held, res.GetVcpus())
	}

	// Delete retries the stop; once it succeeds the record and capacity go.
	h.launcher.lastProcess().setStopErr(nil)
	h.delete(id)
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetVM after delete: %v", err)
	}
	h.mg.mu.Lock()
	held = h.mg.reserved.vcpus
	h.mg.mu.Unlock()
	if held != 0 {
		t.Fatalf("capacity not released after a successful delete: %d", held)
	}
}

func TestDeleteRemoveFailureKeepsRetryableRecord(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	vmsDir := filepath.Join(h.dataDir, "vms")
	if err := os.Chmod(vmsDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(vmsDir, 0o755) })

	if _, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); status.Code(err) != codes.Internal {
		t.Fatalf("DeleteVM with an undeletable disk: %v", err)
	}
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("record after failed delete = %v, %v", vm, err)
	}
	h.mg.mu.Lock()
	held := h.mg.reserved.diskGiB
	h.mg.mu.Unlock()
	if held == 0 {
		t.Fatal("failed delete released the reservation")
	}
	if err := os.Chmod(vmsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	h.delete(id)
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("retry delete: %v", err)
	}
}

func TestSparseReservationsAreChargedAgainstFreeSpace(t *testing.T) {
	free := uint64(20) << 30
	capacity := &nodev1.Resources{Vcpus: 8, MemoryMib: 8192, DiskGib: 100}
	h := newHarness(t, harnessOptions{capacity: capacity, freeBytes: &free})
	res := func() *nodev1.Resources { return &nodev1.Resources{Vcpus: 1, MemoryMib: 128, DiskGib: 15} }
	h.create(ids.Sandbox(), res())

	info, err := h.mg.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if want := uint64(5 * 1024); info.GetDiskFreeMib() != want {
		t.Fatalf("Info free = %d MiB, want %d after a 15 GiB promise on 20 GiB", info.GetDiskFreeMib(), want)
	}
	_, err = h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: ids.Sandbox(), Resources: res()})
	wantCode(t, err, codes.ResourceExhausted)
}

func TestDiskBelowImageBaseIsInvalid(t *testing.T) {
	h := newHarness(t, harnessOptions{baseGiB: 10})
	_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: ids.Sandbox(),
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 128, DiskGib: 1},
	})
	wantCode(t, err, codes.InvalidArgument)
}

func TestCorruptManifestWithLiveOwnerFailsStartup(t *testing.T) {
	dataDir := t.TempDir()
	imageDir := t.TempDir()
	writeTestImage(t, imageDir, testImageID, 1)
	id := ids.Sandbox()
	work := filepath.Join(dataDir, "vms", id)
	if err := os.MkdirAll(work, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "vm.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	disk := filepath.Join(work, "disk.qcow2")
	if err := os.WriteFile(disk, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := qemu.Config{
		SandboxID: id,
		WorkDir:   work,
		ImageDir:  filepath.Join(imageDir, testImageID),
		DiskPath:  disk,
		VCPUs:     1,
		MemoryMiB: 128,
		VsockCID:  13,
	}
	helper, exe := spawnNodeHelper(t, id, cfg)
	free := uint64(1) << 40
	m, err := New(t.Context(), Config{
		NodeID:       "node-test",
		DataDir:      dataDir,
		ImageDir:     imageDir,
		DefaultImage: testImageID,
		QemuVersion:  "test",
		Capacity:     &nodev1.Resources{Vcpus: 4, MemoryMib: 4096, DiskGib: 10},
		Launcher:     &fakeLauncher{},
		Imager:       &fakeImager{diskGiB: 1},
		// Test-factory construction: the dev/test user-mode network.
		AllowUserNetworking: true,
		diskFree:            func() (uint64, error) { return free, nil },
		qemuExe:             exe,
	})
	if err == nil {
		m.Shutdown(context.Background())
		t.Fatal("New accepted a corrupt manifest with a live owned process")
	}
	if !strings.Contains(err.Error(), "live process") {
		t.Fatalf("New error = %v", err)
	}
	if err := helper.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("startup check killed the live process: %v", err)
	}
	if _, err := os.Stat(work); err != nil {
		t.Fatalf("corrupt live-owner state was quarantined: %v", err)
	}
}

func TestRecoverVMWithUninstalledImagePreservesRecord(t *testing.T) {
	id := ids.Sandbox()
	h := newHarness(t, harnessOptions{
		fallback:  pingOnlyGuest{},
		stubGuest: true,
		qemuExe:   "/nonexistent/qemu-system-x86_64",
		prepare: func(t *testing.T, dataDir, imageDir string) {
			store, err := storage.Open(storage.Config{
				DataDir: dataDir, ImageDir: imageDir, Imager: &fakeImager{diskGiB: defaultTestBaseGiB},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			meta := storage.VMMeta{
				SandboxID: id,
				Image:     "gone-v1",
				Resources: storage.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: defaultTestBaseGiB},
				VsockCID:  16,
			}
			img := storage.Image{
				ID:                 "gone-v1",
				Dir:                filepath.Join(imageDir, "gone-v1"),
				RootVirtualSize:    defaultTestBaseGiB << 30,
				RootVirtualSizeGiB: defaultTestBaseGiB,
			}
			if err := store.CreateVM(t.Context(), meta, img); err != nil {
				t.Fatal(err)
			}
			if err := store.WriteLaunchIntent(id); err != nil {
				t.Fatal(err)
			}
		},
	})
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatal(err)
	}
	if vm.GetState() != nodev1.VMState_VM_STATE_FAILED || !strings.Contains(vm.GetError(), "not installed") {
		t.Fatalf("uninstalled-image record = %v", vm)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id, "vm.json")); err != nil {
		t.Fatalf("record was removed: %v", err)
	}
	h.mg.mu.Lock()
	held := h.mg.reserved.vcpus
	h.mg.mu.Unlock()
	if held != 0 {
		t.Fatalf("uninstalled image reserved %d vcpus", held)
	}
}

// TestNewFailureAfterAdoptionCleansUp proves a later reconciliation failure
// releases the lock and goroutines while preserving the adopted VM for the
// next manager.
func TestNewFailureAfterAdoptionCleansUp(t *testing.T) {
	dataDir := t.TempDir()
	imageDir := t.TempDir()
	writeTestImage(t, imageDir, testImageID, defaultTestBaseGiB)
	adoptID := "bx_22222222"
	corruptID := "bx_77777777"

	prepareVM(t, dataDir, imageDir, adoptID, 20, 0)
	// The second entry is syntactically corrupt and has a live owned process
	// using its paths, so reconciliation must stop after adopting the first.
	corruptDir := filepath.Join(dataDir, "vms", corruptID)
	if err := os.MkdirAll(corruptDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(corruptDir, "vm.json"), []byte("{bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	corruptDisk := filepath.Join(corruptDir, "disk.qcow2")
	if err := os.WriteFile(corruptDisk, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	firstCfg := qemu.Config{
		SandboxID: adoptID,
		WorkDir:   filepath.Join(dataDir, "vms", adoptID),
		ImageDir:  filepath.Join(imageDir, testImageID),
		DiskPath:  filepath.Join(dataDir, "vms", adoptID, "disk.qcow2"),
		VCPUs:     2,
		MemoryMiB: 2048,
		VsockCID:  20,
	}
	adoptHelper, exe := spawnNodeHelper(t, adoptID, firstCfg)
	pinned, err := pinProc(adoptHelper.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pinned.close() })

	corruptCfg := qemu.Config{
		SandboxID: corruptID,
		WorkDir:   corruptDir,
		ImageDir:  filepath.Join(imageDir, testImageID),
		DiskPath:  corruptDisk,
		VCPUs:     1,
		MemoryMiB: 128,
		VsockCID:  21,
	}
	corruptHelper, _ := spawnNodeHelper(t, corruptID, corruptCfg)

	capacity := &nodev1.Resources{Vcpus: 8, MemoryMib: 8192, DiskGib: 20}
	free := uint64(1) << 40
	dialer := newFakeGuestDialer(t)
	dialer.fallback = pingOnlyGuest{}
	t.Cleanup(func() { dialer.stop(t) })
	newManager := func(find func(storage.VMMeta) (*pinnedProc, error)) (*Manager, error) {
		return New(t.Context(), Config{
			NodeID:       "node-test",
			DataDir:      dataDir,
			ImageDir:     imageDir,
			DefaultImage: testImageID,
			QemuVersion:  "test",
			Capacity:     capacity,
			Launcher:     &fakeLauncher{},
			GuestDialer:  dialer,
			Imager:       &fakeImager{diskGiB: defaultTestBaseGiB},
			findProcess:  find,
			diskFree:     func() (uint64, error) { return free, nil },
			qemuExe:      exe,
			// Test-factory construction: the dev/test user-mode network.
			AllowUserNetworking: true,
		})
	}

	_, err = newManager(func(meta storage.VMMeta) (*pinnedProc, error) {
		if meta.SandboxID == adoptID {
			return pinned, nil
		}
		return nil, errors.New("ambiguous")
	})
	if err == nil {
		t.Fatal("New accepted a corrupt VM with a live owned process")
	}
	if !strings.Contains(err.Error(), "live process") {
		t.Fatalf("first New failed for the wrong reason: %v", err)
	}
	if err := adoptHelper.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("partial New failure killed the adopted process: %v", err)
	}
	// All of the failed New's resources are released: the data lock can be
	// taken again immediately.
	probe, err := storage.Open(storage.Config{
		DataDir: dataDir, ImageDir: imageDir, Imager: &fakeImager{diskGiB: defaultTestBaseGiB},
	})
	if err != nil {
		t.Fatalf("failed New held the data lock: %v", err)
	}
	probe.Close()

	// Resolve the corrupt owner and start again: the ready VM is preserved
	// and adoptable, the corrupt entry is quarantined.
	_ = corruptHelper.Process.Kill()
	_ = corruptHelper.Wait()
	// The failed manager's watcher released the original pidfd.
	pinned2, err := pinProc(adoptHelper.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pinned2.close() })
	m, err := newManager(func(meta storage.VMMeta) (*pinnedProc, error) {
		if meta.SandboxID == adoptID {
			return pinned2, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		m.Shutdown(ctx)
		dialer.stop(t)
	})
	vm, err := m.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: adoptID})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("preserved VM = %v, %v", vm, err)
	}
	if _, err := m.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: corruptID}); status.Code(err) != codes.NotFound {
		t.Fatalf("corrupt entry = %v", err)
	}
}

// realRecovery runs the real /proc scan and QMP verification against a
// test-owned process with a QEMU-shaped argv and a QMP listener.
func realRecovery(t *testing.T, committed bool) {
	t.Helper()
	id := ids.Sandbox()
	var exe string
	h := newHarness(t, harnessOptions{
		fallback:  pingOnlyGuest{},
		stubGuest: true,
		prepare: func(t *testing.T, dataDir, imageDir string) {
			prepareVM(t, dataDir, imageDir, id, 14, 0)
			cfg := qemu.Config{
				SandboxID: id,
				WorkDir:   filepath.Join(dataDir, "vms", id),
				ImageDir:  filepath.Join(imageDir, testImageID),
				DiskPath:  filepath.Join(dataDir, "vms", id, "disk.qcow2"),
				VCPUs:     2,
				MemoryMiB: 2048,
				VsockCID:  14,
			}
			helper, helperExe := spawnNodeHelper(t, id, cfg)
			exe = helperExe
			if committed {
				store, err := storage.Open(storage.Config{
					DataDir: dataDir, ImageDir: imageDir, Imager: &fakeImager{diskGiB: defaultTestBaseGiB},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := store.CommitRunning(id, helper.Process.Pid); err != nil {
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			}
		},
		configure: func(cfg *Config) { cfg.qemuExe = exe },
	})
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatal(err)
	}
	if vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("recovered state = %s (%s)", vm.GetState(), vm.GetError())
	}
	meta, err := h.mg.store.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	if meta.QEMU == nil || meta.QEMU.State != storage.QEMURunning || meta.QEMU.PID == 0 {
		t.Fatalf("recovered manifest = %+v", meta.QEMU)
	}
}

func TestAdoptedWatchUnknownPollKeepsHandleAndDoneOpen(t *testing.T) {
	pinned, _ := pinnedHelper(t)
	pinned.pollErr = func() error { return errors.New("synthetic poll failure") }
	proc := newAdoptedProcess("bx_22222222", pinned, nil)
	watchDone := make(chan struct{})
	go func() {
		proc.watch(t.Context())
		close(watchDone)
	}()
	select {
	case <-watchDone:
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not observe the poll failure")
	}
	select {
	case <-proc.Done():
		t.Fatal("unknown poll failure was treated as process exit")
	default:
	}
	if proc.Err() == nil {
		t.Fatal("watch did not record the unknown poll state")
	}
	// The pidfd must still be usable: the VM may be alive and only explicit
	// owner shutdown may release this handle.
	if !pinned.alive() {
		t.Fatal("unknown poll failure closed the only liveness handle")
	}
	if err := proc.Release(); err != nil {
		t.Fatal(err)
	}
	if alive, err := pinned.state(); err == nil || alive {
		t.Fatalf("state after Release = %t, %v; want a closed-fd error", alive, err)
	}
}

func TestPinnedCloseWaitsForInflightSyscall(t *testing.T) {
	pinned, _ := pinnedHelper(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	pinned.afterLock = func() {
		close(entered)
		<-release
	}
	stateDone := make(chan struct{})
	var alive bool
	var stateErr error
	go func() {
		alive, stateErr = pinned.state()
		close(stateDone)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("state never reached the pidfd syscall")
	}
	closed := make(chan error, 1)
	go func() { closed <- pinned.close() }()
	select {
	case err := <-closed:
		t.Fatalf("close completed while a pidfd syscall was in flight (fd reuse window): %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	<-stateDone
	if !alive || stateErr != nil {
		t.Fatalf("in-flight state = %t, %v", alive, stateErr)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestPinnedCloseWaitsForInflightPoll(t *testing.T) {
	pinned, _ := pinnedHelper(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	pinned.afterLock = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	proc := newAdoptedProcess("bx_22222222", pinned, nil)
	watchDone := make(chan struct{})
	go func() {
		proc.watch(t.Context())
		close(watchDone)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("watch never polled the pidfd")
	}
	closed := make(chan error, 1)
	go func() { closed <- pinned.close() }()
	select {
	case err := <-closed:
		t.Fatalf("close completed during an in-flight poll (fd reuse window): %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("close did not finish after the bounded poll")
	}
	select {
	case <-watchDone:
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not finish after the fd closed")
	}
	select {
	case <-proc.Done():
		t.Fatal("closed pidfd was reported as process exit")
	default:
	}
}

func TestPinnedStateAfterCloseNeverUsesRecycledFD(t *testing.T) {
	pinned, _ := pinnedHelper(t)
	if err := pinned.close(); err != nil {
		t.Fatal(err)
	}
	// Try to make the descriptor number reusable.
	var files []*os.File
	for range 64 {
		f, err := os.Open(os.DevNull)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	if alive, err := pinned.state(); err == nil || alive {
		t.Fatalf("state after close = %t, %v; want a closed-fd error", alive, err)
	}
	if err := pinned.signal(syscall.SIGTERM); err == nil {
		t.Fatal("signal after close succeeded")
	}
}

func TestDeleteFencesEarlierWaiters(t *testing.T) {
	launcher := &fakeLauncher{}
	gate := make(chan struct{})
	launcher.setGate(gate)
	h := newHarness(t, harnessOptions{launcher: launcher})
	id := ids.Sandbox()
	first := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
		})
		first <- err
	}()
	waitFor(t, "boot start", func() bool { return launcher.count() > 0 })
	second := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id})
		second <- err
	}()
	waitFor(t, "two waiters", func() bool { return waiterCount(h, id) == 2 })

	if _, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("DeleteVM: %v", err)
	}
	for i, ch := range []chan error{first, second} {
		if err := <-ch; status.Code(err) != codes.Aborted {
			t.Fatalf("waiter %d = %v, want Aborted", i, err)
		}
	}
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("fenced VM still visible: %v", err)
	}
	// Only a create that starts after DeleteVM returned may recreate it.
	close(gate)
	vm := h.create(id, nil)
	if vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("create after delete = %v", vm)
	}
}

func TestDeleteFenceKeepsMappingUntilCleanupReturns(t *testing.T) {
	launcher := &fakeLauncher{}
	gate := make(chan struct{})
	launcher.setGate(gate)
	h := newHarness(t, harnessOptions{launcher: launcher})
	id := ids.Sandbox()
	first := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
		})
		first <- err
	}()
	waitFor(t, "boot start", func() bool { return launcher.count() > 0 })

	entered := make(chan struct{})
	release := make(chan struct{})
	h.mg.beforeDeleteRemove = func() {
		close(entered)
		<-release
	}
	deleted := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id})
		deleted <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("delete never reached the directory-removal barrier")
	}

	// A create that starts before DeleteVM returned must be fenced even
	// though the aborted boot already removed its disk.
	made := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id})
		made <- err
	}()
	select {
	case err := <-made:
		if status.Code(err) != codes.Aborted {
			t.Fatalf("create during delete = %v, want Aborted", err)
		}
	case <-time.After(2 * time.Second):
		close(gate)
		t.Fatal("create during delete was not fenced")
	}
	if got := launcher.count(); got != 1 {
		t.Fatalf("fenced create launched a new QEMU: %d starts", got)
	}
	close(release)
	if err := <-deleted; err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := <-first; status.Code(err) != codes.Aborted {
		t.Fatalf("aborted boot = %v", err)
	}
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted VM still visible: %v", err)
	}

	close(gate)
	vm := h.create(id, nil)
	if vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("recreate after delete = %v", vm)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id, "disk.qcow2")); err != nil {
		t.Fatalf("replacement disk missing: %v", err)
	}
}

func TestFailedDeleteRetryAndJoinerUseOwnAttempt(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	vmsDir := filepath.Join(h.dataDir, "vms")
	if err := os.Chmod(vmsDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(vmsDir, 0o755) })

	proc := h.launcher.lastProcess()
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	proc.stageStop(gate, entered)
	first := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id})
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first delete never stopped the process")
	}
	joined := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id})
		joined <- err
	}()
	waitFor(t, "delete joiner", func() bool {
		h.mg.mu.Lock()
		v := h.mg.vms[id]
		h.mg.mu.Unlock()
		if v == nil {
			return false
		}
		v.mu.Lock()
		defer v.mu.Unlock()
		return v.deleteWaiters == 1
	})
	close(gate)
	if err := <-first; status.Code(err) != codes.Internal {
		t.Fatalf("failed delete = %v", err)
	}
	if err := <-joined; status.Code(err) != codes.Internal {
		t.Fatalf("joined delete = %v", err)
	}

	// The retry gets a fresh attempt; old waiters cannot be stranded by it.
	if err := os.Chmod(vmsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	h.delete(id)
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("retry delete: %v", err)
	}
}

func TestUnresolvedPidfdUnknownErrorKeepsProtection(t *testing.T) {
	id := ids.Sandbox()
	pinned, _ := pinnedHelper(t)
	h := newHarness(t, harnessOptions{
		fallback: pingOnlyGuest{},
		prepare: func(t *testing.T, dataDir, imageDir string) {
			prepareVM(t, dataDir, imageDir, id, 15, 0)
		},
		findProcess: func(storage.VMMeta) (*pinnedProc, error) { return pinned, nil },
		verifyProc: func(context.Context, *pinnedProc, storage.VMMeta) (*qmp.Client, error) {
			return nil, errors.New("identity unproven")
		},
	})
	// Closing the pidfd makes every later state query fail with EBADF. That
	// is not proof of exit: Delete must keep the record, disk and reservation.
	if err := pinned.close(); err != nil {
		t.Fatal(err)
	}
	_, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id})
	wantCode(t, err, codes.Internal)
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id, "vm.json")); err != nil {
		t.Fatalf("DeleteVM released protection on an unknown pidfd state: %v", err)
	}
	h.mg.mu.Lock()
	held := h.mg.reserved.vcpus
	h.mg.mu.Unlock()
	if held == 0 {
		t.Fatal("DeleteVM released the reservation on an unknown pidfd state")
	}
}

func TestRealRecoveryLaunchIntentBeforePIDCommit(t *testing.T) {
	realRecovery(t, false)
}

func TestRealRecoveryPersistedIdentity(t *testing.T) {
	realRecovery(t, true)
}
