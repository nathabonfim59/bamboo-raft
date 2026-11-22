package lifecycle

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// reconcileVMs compares each known node's ListVMs result with the durable
// sandbox ownership rows. It repairs only what durable state explains:
//
//   - a VM for a soft-deleted sandbox is cleaned up idempotently;
//   - a ready-ish sandbox whose VM is missing or FAILED becomes error, its
//     VM is deleted and its reservation is released after confirmed cleanup;
//   - an evicted copy (an open vm_evictions row: the sandbox moved to
//     another node) is deleted before anything else, and its row closed;
//   - an unknown VM id is preserved and logged, never deleted on a guess;
//   - a live provisioning sandbox is left to its job.
//
// Run calls it on startup and every reaper interval; a reconnected or
// replaced node is therefore reconciled on the next poll without any Hub
// dependency. A node that cannot be reached this round is retried later.
func (w *Worker) reconcileVMs(ctx context.Context) {
	nodes, err := w.store.NodeIDs(ctx)
	if err != nil {
		w.log.Warn("VM reconciliation node list failed", "reason", classify(err))
		return
	}
	for _, nodeID := range nodes {
		if ctx.Err() != nil {
			return
		}
		w.reconcileNodeVMs(ctx, nodeID)
	}
}

// reconcileNodeVMs reconciles one node's inventory.
func (w *Worker) reconcileNodeVMs(ctx context.Context, nodeID string) {
	vms, err := w.listVMs(ctx, nodeID)
	if err != nil {
		// Disconnected or stale inventory: nothing is proven, so do not
		// touch anything. The next poll retries.
		return
	}
	// Evicted copies go first: they are deleted before anything
	// else looks at this node's inventory, and the VM pass skips them.
	evicted, ok := w.reconcileEvictions(ctx, nodeID)
	if !ok {
		return
	}
	rows, err := w.store.SandboxesOnNode(ctx, nodeID)
	if err != nil {
		w.log.Warn("VM reconciliation sandbox list failed", "node", nodeID, "reason", classify(err))
		return
	}
	byID := make(map[string]db.Sandbox, len(rows))
	for _, sbx := range rows {
		byID[sbx.ID] = sbx
	}
	seen := make(map[string]bool, len(vms))
	for _, vm := range vms {
		id := vm.GetSandboxId()
		if id == "" {
			continue
		}
		if evicted[id] {
			continue // an orphaned copy, never this node's sandbox VM
		}
		seen[id] = true
		sbx, ok := byID[id]
		if !ok {
			// Never blanket-delete node inventory: an unknown VM may be
			// from another control plane or an operator.
			w.log.Warn("unknown VM preserved", "node", nodeID, "vmid", id)
			continue
		}
		if sbx.DeletedAt != nil {
			w.reconcileDeletedVM(ctx, sbx)
			continue
		}
		if sbx.State == db.StateError {
			continue // repaired in the row pass, with or without a VM
		}
		if readyState(sbx.State) && vm.GetState() == nodev1.VMState_VM_STATE_FAILED {
			w.reconcileBrokenReady(ctx, sbx, "vm_failed")
		}
	}
	for _, sbx := range rows {
		if ctx.Err() != nil {
			return
		}
		if sbx.DeletedAt != nil {
			continue
		}
		if sbx.State == db.StateError {
			// Idempotent DeleteVM + release retry even when the VM is
			// already absent: a previous repair may have deleted the VM
			// and then failed to release the reservation.
			w.reconcileErrorVM(ctx, sbx)
			continue
		}
		if seen[sbx.ID] || !readyState(sbx.State) {
			continue
		}
		w.reconcileBrokenReady(ctx, sbx, "vm_missing")
	}
}

// listVMs asks one gateway-bound node for its inventory, bounded so a stuck
// node cannot stall the reaper.
func (w *Worker) listVMs(ctx context.Context, nodeID string) ([]*nodev1.VM, error) {
	client, closer, err := w.dial(nodeID)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	lctx, cancel := context.WithTimeout(ctx, nodeInventoryTimeout)
	defer cancel()
	resp, err := client.ListVMs(lctx, &nodev1.ListVMsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetVms(), nil
}

// getVM re-probes the canonical target under the guard. A list snapshot is
// not proof: a boot may have completed between ListVMs and the sandbox row
// read, so only this call may declare a VM missing or failed.
func (w *Worker) getVM(ctx context.Context, nodeID, sandboxID string) (*nodev1.VM, error) {
	client, closer, err := w.dial(nodeID)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	vctx, cancel := context.WithTimeout(ctx, nodeInventoryTimeout)
	defer cancel()
	return client.GetVM(vctx, &nodev1.GetVMRequest{SandboxId: sandboxID})
}

// reconcileDeletedVM removes a VM whose sandbox deletion is durable. The
// delete job completes the operation; this retries cleanup even if that job
// already exhausted its attempts or a stale boot raced the acceptance.
func (w *Worker) reconcileDeletedVM(ctx context.Context, sbx db.Sandbox) {
	guard, opCtx, stop, err := w.acquireReapGuard(ctx, sbx.OwnerID, sbx.ID)
	if err != nil {
		return
	}
	defer func() {
		stop()
		guard.Release(ctx)
	}()
	cur, err := w.store.SandboxAny(ctx, sbx.OwnerID, sbx.ID)
	if err != nil || cur.DeletedAt == nil {
		return
	}
	if err := w.deleteVMUnderGuard(opCtx, guard, cur); err != nil {
		w.log.Warn("deleted VM cleanup pending", "sandbox", cur.ID, "reason", classify(err))
	}
}

// reconcileErrorVM retries cleanup/release for a terminal-error sandbox. The
// reservation is released only after DeleteVM confirmed.
func (w *Worker) reconcileErrorVM(ctx context.Context, sbx db.Sandbox) {
	guard, opCtx, stop, err := w.acquireReapGuard(ctx, sbx.OwnerID, sbx.ID)
	if err != nil {
		return
	}
	defer func() {
		stop()
		guard.Release(ctx)
	}()
	cur, err := w.store.SandboxAny(ctx, sbx.OwnerID, sbx.ID)
	if err != nil || cur.DeletedAt != nil || cur.State != db.StateError {
		return
	}
	w.cleanupTerminalUnderGuard(ctx, cur, guard, opCtx, "error_cleanup")
}

// reconcileBrokenReady turns a ready-ish row whose VM is missing or FAILED
// into a terminal error, then cleans up and releases capacity after
// confirmation. Under the guard it re-reads the row and re-probes GetVM, so a
// stale ListVMs page that predates a successful boot cannot delete a VM that
// is now ready.
func (w *Worker) reconcileBrokenReady(ctx context.Context, sbx db.Sandbox, reason string) {
	guard, opCtx, stop, err := w.acquireReapGuard(ctx, sbx.OwnerID, sbx.ID)
	if err != nil {
		return
	}
	defer func() {
		stop()
		guard.Release(ctx)
	}()
	cur, err := w.store.SandboxAny(ctx, sbx.OwnerID, sbx.ID)
	if err != nil || cur.DeletedAt != nil || !readyState(cur.State) {
		return
	}
	vm, err := w.getVM(opCtx, derefNode(cur.NodeID), cur.ID)
	switch {
	case err == nil && vm.GetState() != nodev1.VMState_VM_STATE_FAILED:
		return // recovered: the list snapshot was stale
	case err == nil:
		// FAILED: repair below.
	case status.Code(err) == codes.NotFound:
		// Genuinely missing.
	default:
		return // unproven failure; retry next round
	}
	w.cleanupTerminalUnderGuard(ctx, cur, guard, opCtx, reason)
}

// cleanupTerminalUnderGuard routes a broken live sandbox to error (if it is
// not already), deletes its VM idempotently and only then releases the
// reservation. The guard is already held and the row has been re-read under
// it.
func (w *Worker) cleanupTerminalUnderGuard(ctx context.Context, sbx db.Sandbox, guard *db.SandboxGuard, opCtx context.Context, reason string) {
	owner, sandboxID := sbx.OwnerID, sbx.ID
	if sbx.State != db.StateError {
		if !CanTransition(sbx.State, db.StateError) {
			return
		}
		_, err := w.store.TransitionSandbox(ctx, db.TransitionParams{
			SandboxID:       sbx.ID,
			Owner:           owner,
			FromStates:      []db.SandboxState{sbx.State},
			ExpectedVersion: sbx.Version,
			To:              db.StateError,
			ErrorReason:     reason,
		})
		switch {
		case errors.Is(err, db.ErrNotFound), errors.Is(err, db.ErrConflict):
			return
		case err != nil:
			w.log.Warn("VM repair transition failed", "sandbox", sandboxID, "reason", classify(err))
			return
		}
		sbx.State = db.StateError
	}
	if err := w.deleteVMUnderGuard(opCtx, guard, sbx); err != nil {
		w.log.Warn("VM repair cleanup pending", "sandbox", sandboxID, "reason", classify(err))
		return
	}
	if guard.Err() != nil || opCtx.Err() != nil {
		return // unconfirmed: keep the reservation for the next round
	}
	if err := w.store.ReleaseSandboxReservation(ctx, owner, sandboxID); err != nil {
		w.log.Warn("VM repair release failed", "sandbox", sandboxID, "reason", classify(err))
		return
	}
	w.releaseSnapshotPin(ctx, owner, sandboxID)
}

// readyState lists the states in which a sandbox should have a working VM.
func readyState(state db.SandboxState) bool {
	switch state {
	case db.StateReady, db.StateIdle, db.StateRunning:
		return true
	default:
		return false
	}
}
