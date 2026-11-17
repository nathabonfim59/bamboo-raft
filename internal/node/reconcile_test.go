package node

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

type pingOnlyGuest struct {
	guestv1.UnimplementedGuestServer
}

func (pingOnlyGuest) Ping(context.Context, *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	return &guestv1.PingResponse{Version: "test"}, nil
}

// The A2 restart tests use this fake guest only to probe adoption. A recovered
// VM now probes lease recovery too, so report an always-thawed guest instead of
// Unimplemented for those two RPCs.
func (pingOnlyGuest) Freeze(context.Context, *guestv1.FreezeRequest) (*guestv1.FreezeResponse, error) {
	return &guestv1.FreezeResponse{LeaseToken: "test-lease", LeaseRemainingMs: 60_000}, nil
}

func (pingOnlyGuest) Thaw(context.Context, *guestv1.ThawRequest) (*guestv1.ThawResponse, error) {
	return &guestv1.ThawResponse{}, nil
}

func quietLog() *slog.Logger { return slog.New(slog.DiscardHandler) }

func mustProcStartTicks(t *testing.T, pid int) uint64 {
	t.Helper()
	ticks, err := storage.ProcStartTicks(pid)
	if err != nil {
		t.Fatal(err)
	}
	return ticks
}

// prepareVM writes a durable VM box with a launch intent (QEMU Launching) and,
// when pid != 0, a committed running record. It simulates a node crash at the
// chosen point.
func prepareVM(t *testing.T, dataDir, imageDir, id string, cid uint32, pid int) {
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
	if err := store.WriteLaunchIntent(id); err != nil {
		t.Fatal(err)
	}
	if pid != 0 {
		if err := store.CommitRunning(id, pid); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecoverLaunchBeforePIDCommit(t *testing.T) {
	id := ids.Sandbox()
	pinned, cmd := pinnedHelper(t)
	qmpSrv := startFakeQMP(t, id)
	h := newHarness(t, harnessOptions{
		fallback: pingOnlyGuest{},
		prepare: func(t *testing.T, dataDir, imageDir string) {
			prepareVM(t, dataDir, imageDir, id, 7, 0)
		},
		findProcess: func(meta storage.VMMeta) (*pinnedProc, error) {
			if meta.SandboxID == id {
				return pinned, nil
			}
			return nil, nil
		},
		verifyProc: func(ctx context.Context, _ *pinnedProc, _ storage.VMMeta) (*qmp.Client, error) {
			return qmp.Dial(ctx, qmpSrv.path, quietLog())
		},
	})
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatal(err)
	}
	if vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("adopted VM state %s: %v", vm.GetState(), vm.GetError())
	}
	meta, err := h.mg.store.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	if meta.QEMU == nil || meta.QEMU.State != storage.QEMURunning || meta.QEMU.PID != cmd.Process.Pid {
		t.Fatalf("manifest was not committed after adoption: %+v", meta.QEMU)
	}
}

func TestRecoverVanishedProcessMarksFailed(t *testing.T) {
	id := ids.Sandbox()
	h := newHarness(t, harnessOptions{
		fallback: pingOnlyGuest{},
		prepare: func(t *testing.T, dataDir, imageDir string) {
			prepareVM(t, dataDir, imageDir, id, 8, 0)
		},
		findProcess: func(storage.VMMeta) (*pinnedProc, error) { return nil, nil },
	})
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatal(err)
	}
	if vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("state %s, want FAILED", vm.GetState())
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id, "vm.json")); err != nil {
		t.Fatalf("vanished VM lost its record: %v", err)
	}
	h.delete(id)
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("DeleteVM did not remove a proved-absent VM: %v", err)
	}
}

func TestAmbiguousOwnerBlocksDeleteUntilAbsent(t *testing.T) {
	id := ids.Sandbox()
	helper := helperSleep(t)
	h := newHarness(t, harnessOptions{
		fallback: pingOnlyGuest{},
		prepare: func(t *testing.T, dataDir, imageDir string) {
			prepareVM(t, dataDir, imageDir, id, 9, 0)
		},
		findProcess: func(storage.VMMeta) (*pinnedProc, error) {
			return nil, errors.New("two processes claim this vm")
		},
	})
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); err != nil {
		t.Fatal(err)
	}
	_, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id})
	wantCode(t, err, codes.Internal)
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); err != nil {
		t.Fatalf("unresolved DeleteVM removed the disk: %v", err)
	}
	if err := helper.Process.Kill(); err != nil {
		t.Logf("helper already gone: %v", err)
	}
	_ = helper.Wait()

	// Now no process claims the VM: delete is allowed.
	h.mg.findProcess = func(storage.VMMeta) (*pinnedProc, error) { return nil, nil }
	h.delete(id)
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("resolved DeleteVM kept the disk: %v", err)
	}
}

func TestUnresolvedPinnedOwnerBlocksDelete(t *testing.T) {
	id := ids.Sandbox()
	pinned, cmd := pinnedHelper(t)
	h := newHarness(t, harnessOptions{
		fallback: pingOnlyGuest{},
		prepare: func(t *testing.T, dataDir, imageDir string) {
			prepareVM(t, dataDir, imageDir, id, 10, 0)
		},
		findProcess: func(meta storage.VMMeta) (*pinnedProc, error) { return pinned, nil },
		verifyProc: func(context.Context, *pinnedProc, storage.VMMeta) (*qmp.Client, error) {
			return nil, errors.New("qmp identity mismatch")
		},
	})
	_, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id})
	wantCode(t, err, codes.Internal)
	if !pinned.alive() {
		t.Fatal("DeleteVM killed a process it could not verify")
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); err != nil {
		t.Fatalf("unresolved DeleteVM removed the disk: %v", err)
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if !pidfdWait(pinned, 5*time.Second) {
		t.Fatal("helper did not exit")
	}
	h.delete(id)
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("DeleteVM after absence kept the disk: %v", err)
	}
}

func TestCorruptRecordQuarantinesWithoutKilling(t *testing.T) {
	id := ids.Sandbox()
	helper := helperSleep(t)
	h := newHarness(t, harnessOptions{
		fallback: pingOnlyGuest{},
		// No QEMU is installed in unit-test environments; a placeholder
		// ensures no unrelated process matches this id's owned paths.
		qemuExe: "/nonexistent/qemu-system-x86_64",
		prepare: func(t *testing.T, dataDir, _ string) {
			dir := filepath.Join(dataDir, "vms", id)
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "vm.json"), []byte("{not json"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
	})
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("corrupt VM surfaced as %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(h.dataDir, "orphans"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("orphans = %v, %v", entries, err)
	}
	if err := helper.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("unrelated process was killed: %v", err)
	}
	// A same-id launch could collide with a live QEMU that owned the
	// quarantined state.
	_, err = h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 128, DiskGib: 1},
	})
	wantCode(t, err, codes.Internal)
}

func TestRecoveredVMBeyondOfferBlocksNewWork(t *testing.T) {
	id := ids.Sandbox()
	pinned, _ := pinnedHelper(t)
	qmpSrv := startFakeQMP(t, id)
	h := newHarness(t, harnessOptions{
		capacity: &nodev1.Resources{Vcpus: 1, MemoryMib: 1024, DiskGib: 1},
		fallback: pingOnlyGuest{},
		prepare: func(t *testing.T, dataDir, imageDir string) {
			// The record asks for 2 vCPU / 2 GiB, more than the offer.
			prepareVM(t, dataDir, imageDir, id, 11, 0)
		},
		findProcess: func(meta storage.VMMeta) (*pinnedProc, error) { return pinned, nil },
		verifyProc: func(ctx context.Context, _ *pinnedProc, _ storage.VMMeta) (*qmp.Client, error) {
			return qmp.Dial(ctx, qmpSrv.path, quietLog())
		},
	})
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("recovered VM = %v, %v", vm, err)
	}
	info, err := h.mg.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if info.GetDiskFreeMib() != 0 {
		t.Fatalf("Info offers %d MiB while a recovered VM exceeds the offer", info.GetDiskFreeMib())
	}
	_, err = h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: ids.Sandbox(),
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 128, DiskGib: 1},
	})
	wantCode(t, err, codes.ResourceExhausted)
}

func TestOwnedByUsesPersistedImageAndExactPaths(t *testing.T) {
	cfg := qemu.Config{
		SandboxID: ids.Sandbox(),
		WorkDir:   "/var/lib/bbraft/vms/" + ids.Sandbox(),
		ImageDir:  "/srv/images/" + testImageID,
		DiskPath:  "/var/lib/bbraft/vms/" + ids.Sandbox() + "/disk.qcow2",
		VCPUs:     2,
		MemoryMiB: 2048,
		VsockCID:  7,
		// Args is pure and needs injected host facts for the auto policy.
		HostCPU: &qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 1},
	}
	cfg.WorkDir = "/var/lib/bbraft/vms/" + cfg.SandboxID
	cfg.DiskPath = cfg.WorkDir + "/disk.qcow2"
	argv, err := cfg.Args()
	if err != nil {
		t.Fatal(err)
	}
	meta := storage.VMMeta{SandboxID: cfg.SandboxID, Image: testImageID}
	info := procInfo{pid: 1, exe: "/usr/bin/qemu-system-x86_64", argv: argv}
	if !ownedBy(info, meta, "/srv/images", cfg.WorkDir, cfg.DiskPath, info.exe) {
		t.Fatal("exact owned process did not match")
	}
	if ownedBy(info, meta, "/srv/images", "/other/data/vms/"+cfg.SandboxID, cfg.DiskPath, info.exe) {
		t.Fatal("same sandbox id in another data root matched")
	}
	if ownedBy(info, storage.VMMeta{SandboxID: cfg.SandboxID, Image: "other-image"}, "/srv/images", cfg.WorkDir, cfg.DiskPath, info.exe) {
		t.Fatal("wrong persisted image id matched")
	}
	if ownedBy(info, meta, "/srv/images", cfg.WorkDir, cfg.DiskPath, "/usr/bin/qemu-system-i386") {
		t.Fatal("different executable matched")
	}
}

func TestVerifyQMPRejectsStaleIdentity(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	pinned, cmd := pinnedHelper(t)
	bootID, err := storage.HostBootID()
	if err != nil {
		t.Fatal(err)
	}
	ticks := mustProcStartTicks(t, cmd.Process.Pid)
	meta := storage.VMMeta{
		SandboxID: ids.Sandbox(),
		QEMU: &storage.QEMURecord{
			State:      storage.QEMURunning,
			PID:        cmd.Process.Pid,
			BootID:     bootID,
			StartTicks: ticks,
		},
	}
	meta.QEMU.BootID = "previous-boot"
	if _, err := h.mg.verifyQMP(t.Context(), pinned, meta); err == nil {
		t.Fatal("verifyQMP accepted a process from a previous boot")
	}
	meta.QEMU.BootID = bootID
	meta.QEMU.StartTicks = ticks + 1
	if _, err := h.mg.verifyQMP(t.Context(), pinned, meta); err == nil {
		t.Fatal("verifyQMP accepted stale start ticks")
	}
}

func TestVerifyQMPRejectsWrongPeer(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	pinned, cmd := pinnedHelper(t)
	id := ids.Sandbox()
	srv := startFakeQMP(t, id)
	bootID, err := storage.HostBootID()
	if err != nil {
		t.Fatal(err)
	}
	meta := storage.VMMeta{
		SandboxID: id,
		QEMU: &storage.QEMURecord{
			State:      storage.QEMURunning,
			PID:        cmd.Process.Pid,
			BootID:     bootID,
			StartTicks: mustProcStartTicks(t, cmd.Process.Pid),
		},
	}
	if _, err := h.mg.verifyQMPAt(t.Context(), pinned, meta, srv.path); err == nil {
		t.Fatal("verifyQMP accepted a QMP peer that is not the pinned pid")
	}
}

func TestRecoverPausedVMResumesBeforePing(t *testing.T) {
	id := ids.Sandbox()
	srv := startFakeQMP(t, id)
	srv.startPaused = true
	client, err := qmp.Dial(t.Context(), srv.path, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := resumePaused(t.Context(), client); err != nil {
		t.Fatal(err)
	}
	if got := srv.contCount(); got != 1 {
		t.Fatalf("cont calls = %d, want 1", got)
	}
	var status struct {
		Status string `json:"status"`
	}
	if err := client.Execute(t.Context(), "query-status", nil, &status); err != nil {
		t.Fatal(err)
	}
	if status.Status != "running" {
		t.Fatalf("status after resume = %q", status.Status)
	}
}
