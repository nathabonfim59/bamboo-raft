// Operations on a VM record whose QEMU a node restart could not resolve must
// resolve it first, exactly as DeleteVM does, or refuse with UNAVAILABLE
// without committing anything or starting a QEMU.

package node

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// unresolvedTestRecord returns the manager's live record for id.
func unresolvedTestRecord(t *testing.T, h *harness, id string) *vm {
	t.Helper()
	h.mg.mu.Lock()
	defer h.mg.mu.Unlock()
	v := h.mg.vms[id]
	if v == nil {
		t.Fatalf("no VM record for %s", id)
	}
	return v
}

// unresolvedTestMark turns a record into one whose restart recovery could not
// decide ownership, optionally pinning an owner and publishing a state.
func unresolvedTestMark(t *testing.T, v *vm, state nodev1.VMState, pinned *pinnedProc) {
	t.Helper()
	v.mu.Lock()
	defer v.mu.Unlock()
	if state != nodev1.VMState_VM_STATE_UNSPECIFIED {
		v.state = state
	}
	v.ownershipUnresolved = true
	v.unresolved = pinned
}

// unresolvedTestWantRefused asserts the operation refused with UNAVAILABLE
// naming the unresolved QEMU.
func unresolvedTestWantRefused(t *testing.T, err error) {
	t.Helper()
	wantCode(t, err, codes.Unavailable)
	if !strings.Contains(err.Error(), "could not be resolved after a restart") {
		t.Fatalf("error %q does not name the unresolved restart", err)
	}
}

// recoveredUnresolvedHarness builds the production shape of the bug: a
// readable record whose process recovery cannot identify, so it is kept
// FAILED with ownershipUnresolved.
func recoveredUnresolvedHarness(t *testing.T) (*harness, string) {
	t.Helper()
	id := ids.Sandbox()
	h := newHarness(t, harnessOptions{
		fallback: pingOnlyGuest{},
		prepare: func(t *testing.T, dataDir, imageDir string) {
			prepareVM(t, dataDir, imageDir, id, 30, 0)
		},
		findProcess: func(storage.VMMeta) (*pinnedProc, error) {
			return nil, errors.New("two processes claim this vm")
		},
	})
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatalf("GetVM: %v", err)
	}
	if vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("recovery left state %s, want FAILED", vm.GetState())
	}
	return h, id
}

// TestStopVMUnresolvedRecordRefuses: a recovered FAILED record whose owner is
// still ambiguous never runs a stop and never commits STOPPED.
func TestStopVMUnresolvedRecordRefuses(t *testing.T) {
	h, id := recoveredUnresolvedHarness(t)
	_, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	unresolvedTestWantRefused(t, err)
	if vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); err != nil || vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("record after the refusal = %v, %v; want FAILED", vm, err)
	}
	meta, err := h.mg.store.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Stopped() {
		t.Fatal("StopVM committed STOPPED for an unresolved record")
	}
}

// TestStopVMStoppedUnresolvedRecordRefuses: even a record that already reads
// STOPPED is resolved before the idempotent answer, so a QEMU that may still
// run behind it is not vouched for.
func TestStopVMStoppedUnresolvedRecordRefuses(t *testing.T) {
	pinned, _ := pinnedHelper(t)
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	v := unresolvedTestRecord(t, h, id)
	unresolvedTestMark(t, v, nodev1.VMState_VM_STATE_STOPPED, pinned)

	_, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	unresolvedTestWantRefused(t, err)
	meta, err := h.mg.store.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Stopped() {
		t.Fatal("StopVM committed STOPPED for an unresolved record")
	}
}

// TestStartVMUnresolvedRecordRefuses: a STOPPED-looking unresolved record
// never gets a second QEMU on the same disk.
func TestStartVMUnresolvedRecordRefuses(t *testing.T) {
	pinned, _ := pinnedHelper(t)
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	before := h.launcher.count()
	v := unresolvedTestRecord(t, h, id)
	unresolvedTestMark(t, v, nodev1.VMState_VM_STATE_STOPPED, pinned)

	_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	unresolvedTestWantRefused(t, err)
	if got := h.launcher.count(); got != before {
		t.Fatalf("StartVM launched %d QEMUs, want no launch after %d", got, before)
	}
	if got := vmStateOf(v); got != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("record state after the refusal = %s, want STOPPED", got)
	}
}

// TestSnapshotVMStoppedUnresolvedRecordRefuses: the offline path refuses
// before its maintenance QEMU exists.
func TestSnapshotVMStoppedUnresolvedRecordRefuses(t *testing.T) {
	env := snapvmStoppedSetup(t)
	pinned, _ := pinnedHelper(t)
	v := unresolvedTestRecord(t, env.h, maintTestID)
	unresolvedTestMark(t, v, nodev1.VMState_VM_STATE_UNSPECIFIED, pinned)

	_, err := env.h.mg.SnapshotVM(t.Context(), &nodev1.SnapshotVMRequest{SandboxId: maintTestID, ChainId: uuid.NewV7().String()})
	unresolvedTestWantRefused(t, err)
	if got := env.ml.maintenanceStarts(); got != 0 {
		t.Fatalf("SnapshotVM launched %d maintenance QEMUs for an unresolved record", got)
	}
}

// TestSnapshotVMReadyUnresolvedRecordRefuses: the live path refuses too, even
// though the record reads READY.
func TestSnapshotVMReadyUnresolvedRecordRefuses(t *testing.T) {
	pinned, _ := pinnedHelper(t)
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.Store = memstore.New()
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	v := unresolvedTestRecord(t, h, id)
	unresolvedTestMark(t, v, nodev1.VMState_VM_STATE_READY, pinned)

	_, err := h.mg.SnapshotVM(t.Context(), &nodev1.SnapshotVMRequest{SandboxId: id, ChainId: uuid.NewV7().String()})
	unresolvedTestWantRefused(t, err)
}

// TestResetVMUnresolvedRecordRefuses: a reset never reaches system_reset on an
// unresolved machine.
func TestResetVMUnresolvedRecordRefuses(t *testing.T) {
	pinned, _ := pinnedHelper(t)
	env := resetvmSetup(t, resetvmOptions{})
	env.create()
	v := unresolvedTestRecord(t, env.h, env.id)
	unresolvedTestMark(t, v, nodev1.VMState_VM_STATE_READY, pinned)

	_, err := env.h.mg.ResetVM(t.Context(), &nodev1.ResetVMRequest{SandboxId: env.id})
	unresolvedTestWantRefused(t, err)
	if got := env.srv.resetCount(); got != 0 {
		t.Fatalf("ResetVM sent %d system_reset commands to an unresolved QEMU", got)
	}
	if vm, err := env.h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: env.id}); err != nil || vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("record after the refusal = %v, %v; want READY", vm, err)
	}
}

// TestMaintenanceUnresolvedRecordRefuses: direct maintenance launches (offline
// snapshot and, later, restore writes) refuse too.
func TestMaintenanceUnresolvedRecordRefuses(t *testing.T) {
	env := snapvmStoppedSetup(t)
	pinned, _ := pinnedHelper(t)
	v := unresolvedTestRecord(t, env.h, maintTestID)
	unresolvedTestMark(t, v, nodev1.VMState_VM_STATE_UNSPECIFIED, pinned)
	before := env.ml.maintenanceStarts()

	err := env.h.mg.maintenance(t.Context(), v, func(context.Context, *maintSession) error {
		return errors.New("maintenance ran on an unresolved record")
	})
	unresolvedTestWantRefused(t, err)
	if got := env.ml.maintenanceStarts(); got != before {
		t.Fatalf("maintenance launched %d QEMUs, want no launch after %d", got, before)
	}
}

// TestUnresolvedRecordConcurrentStopAndDelete runs the paths that share a
// record's operation gate and its resolver against one unresolved record: they
// must serialize without deadlocking, keep the protection while the owner is
// ambiguous, and then both complete once it is proven gone.
func TestUnresolvedRecordConcurrentStopAndDelete(t *testing.T) {
	h, id := recoveredUnresolvedHarness(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// While the owner is ambiguous, neither path may act: StopVM refuses
	// UNAVAILABLE (or NOT_FOUND if a delete attempt is in flight) and DeleteVM
	// keeps its INTERNAL refusal.
	for range 20 {
		var wg sync.WaitGroup
		var stopErr, delErr error
		wg.Go(func() {
			_, stopErr = h.mg.StopVM(ctx, &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
		})
		wg.Go(func() {
			_, delErr = h.mg.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id})
		})
		wg.Wait()
		switch code := status.Code(stopErr); code {
		case codes.Unavailable:
			unresolvedTestWantRefused(t, stopErr)
		case codes.NotFound:
		default:
			t.Fatalf("concurrent StopVM = %v (%v), want UNAVAILABLE or NOT_FOUND", stopErr, code)
		}
		wantCode(t, delErr, codes.Internal)
	}

	// The ambiguity clears: whichever path wins, the record is resolved once
	// and DeleteVM removes it exactly once.
	h.mg.findProcess = func(storage.VMMeta) (*pinnedProc, error) { return nil, nil }
	var wg sync.WaitGroup
	var stopErr, delErr error
	wg.Go(func() {
		_, stopErr = h.mg.StopVM(ctx, &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	})
	wg.Go(func() {
		_, delErr = h.mg.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id})
	})
	wg.Wait()
	if delErr != nil {
		t.Fatalf("DeleteVM after resolution: %v", delErr)
	}
	if code := status.Code(stopErr); stopErr != nil && code != codes.NotFound {
		t.Fatalf("concurrent StopVM after resolution = %v (%v), want STOPPED or NOT_FOUND", stopErr, code)
	}
	if _, err := h.mg.GetVM(ctx, &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("record survived the delete: %v", err)
	}
}

// TestUnresolvedRecordResolvesThenProceeds is the positive control: once the
// QEMU is proven gone, the same operations run normally.
func TestUnresolvedRecordResolvesThenProceeds(t *testing.T) {
	h, id := recoveredUnresolvedHarness(t)
	// The ambiguity clears: no live process claims the record any more.
	h.mg.findProcess = func(storage.VMMeta) (*pinnedProc, error) { return nil, nil }

	vm, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if err != nil {
		t.Fatalf("StopVM after resolution: %v", err)
	}
	if vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM after resolution returned %s, want STOPPED", vm.GetState())
	}
	meta, err := h.mg.store.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Stopped() {
		t.Fatal("StopVM after resolution did not commit STOPPED")
	}

	vm, err = h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if err != nil {
		t.Fatalf("StartVM after resolution: %v", err)
	}
	if vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM after resolution returned %s, want READY", vm.GetState())
	}
	if got := h.launcher.count(); got != 1 {
		t.Fatalf("StartVM launched %d QEMUs, want 1", got)
	}
}

// TestSaveSnapshotUnresolvedRecordRefuses: the Phase 1 snapshot save resolves
// an unresolved record first, like every other operation: a record whose
// QEMU a restart could not prove gone refuses with UNAVAILABLE, nothing is
// frozen or copied and no snapshot is committed.
func TestSaveSnapshotUnresolvedRecordRefuses(t *testing.T) {
	for _, state := range []nodev1.VMState{nodev1.VMState_VM_STATE_READY, nodev1.VMState_VM_STATE_STOPPED} {
		t.Run(state.String(), func(t *testing.T) {
			pinned, _ := pinnedHelper(t)
			h := newHarness(t, harnessOptions{})
			id := ids.Sandbox()
			h.create(id, nil)
			freezes, _, _ := h.freezer.state()
			v := unresolvedTestRecord(t, h, id)
			unresolvedTestMark(t, v, state, pinned)

			const snapshotID = "unresolved-source"
			_, err := h.mg.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{SandboxId: id, SnapshotId: snapshotID})
			unresolvedTestWantRefused(t, err)
			if after, _, _ := h.freezer.state(); after != freezes {
				t.Fatalf("SaveSnapshot froze the guest %d times for an unresolved record", after-freezes)
			}
			if infoHasSnapshot(t, h.mg, snapshotID) {
				t.Fatal("a refused SaveSnapshot committed a snapshot")
			}
			if _, err := os.Stat(filepath.Join(h.dataDir, "snapshots", "committed", storage.SnapshotKey(snapshotID))); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("committed snapshot dir after the refusal: %v", err)
			}
		})
	}
}
