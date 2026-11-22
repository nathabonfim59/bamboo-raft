package lifecycle

import (
	"context"
	"encoding/json/v2"
	"errors"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// deleteVM performs one accepted sandbox deletion. The deletion row is
// already soft-deleted; this handler confirms ownership from the canonical
// row, acquires the same per-sandbox guard provisioning uses (which drains
// any in-flight boot and compensation), deletes the VM idempotently and only
// then completes the deletion, releasing the reservation exactly once.
func (w *Worker) deleteVM(ctx context.Context, job db.Job, lease *Lease) error {
	if job.SandboxID == nil {
		return permanent("missing sandbox id")
	}
	var p db.DeleteVMPayload
	if err := json.Unmarshal([]byte(job.Payload), &p); err != nil {
		return permanent("invalid delete payload")
	}
	owner, sandboxID := job.OwnerID, *job.SandboxID
	w.precondWarned.forget(sandboxID) // a deleted sandbox never snapshots again

	sbx, err := w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return permanent("sandbox missing")
	}
	if err != nil {
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if sbx.DeletedAt == nil {
		return permanent("deletion not accepted")
	}
	if p.VMID != sbx.ID || p.NodeID != derefNode(sbx.NodeID) || p.OperationID == "" {
		return permanent("job payload does not match sandbox")
	}
	if sbx.Reserved != (db.Resources{}) && p.Resources != sbx.Reserved {
		return permanent("job payload does not match sandbox resources")
	}

	op, err := w.store.ActiveSandboxDeletion(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return w.completedDeletion(ctx, owner, p.OperationID, sandboxID)
	}
	if err != nil {
		return retry("deletion operation read failed", w.backoff(job.Attempts))
	}
	if op.ID != p.OperationID || op.TargetID != sandboxID || op.Kind != db.DeletionSandbox {
		return permanent("job payload does not match deletion operation")
	}

	if _, err := w.store.BeginDeletionAttempt(ctx, owner, op.ID); err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil // a duplicate attempt completed it first
		}
		return retry("begin deletion attempt failed", w.backoff(job.Attempts))
	}

	guard, opCtx, stop, err := w.acquireGuarded(ctx, lease, owner, sandboxID)
	if err != nil {
		if lease.Lost() || ctx.Err() != nil {
			return nil
		}
		return retry("sandbox guard unavailable", w.backoff(job.Attempts))
	}
	defer func() {
		stop()
		guard.Release(ctx)
	}()

	// Re-read everything under the guard. The guard serializes our own
	// provisioning/deletion operations, but it is NOT proof that a node-side
	// boot stopped: a cancelled CreateVM may still be running remotely.
	// DeleteVM's frozen contract is the fence that aborts and joins every
	// earlier CreateVM for this id before it returns.
	sbx, err = w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return permanent("sandbox missing")
	}
	if err != nil {
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if sbx.DeletedAt == nil {
		return permanent("deletion not accepted")
	}
	op, err = w.store.ActiveSandboxDeletion(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return w.completedDeletion(ctx, owner, p.OperationID, sandboxID)
	}
	if err != nil {
		return retry("deletion operation read failed", w.backoff(job.Attempts))
	}
	if op.ID != p.OperationID {
		return permanent("job payload does not match deletion operation")
	}

	if err := w.deleteVMOnNode(opCtx, derefNode(sbx.NodeID), sbx.ID); err != nil {
		if guard.Err() != nil {
			return retry("sandbox guard lost", w.backoff(job.Attempts))
		}
		if lease.Lost() || ctx.Err() != nil {
			return nil
		}
		return retry(classify(err), w.backoff(job.Attempts))
	}
	// The node answered, but the guard or claim may have vanished while the
	// cleanup ran. A vanished guard makes the answer unconfirmed: do not
	// complete or release here; redelivery/reconciliation retries.
	if guard.Err() != nil {
		return retry("sandbox guard lost", w.backoff(job.Attempts))
	}
	if lease.Lost() {
		return nil
	}
	if ctx.Err() != nil || opCtx.Err() != nil {
		return retry("worker shutdown", 0)
	}

	_, err = w.store.CompleteDeletion(ctx, owner, op.ID)
	if errors.Is(err, db.ErrNotFound) {
		return permanent("deletion operation missing")
	}
	if errors.Is(err, db.ErrConflict) {
		return retry("deletion completion conflict", w.backoff(job.Attempts))
	}
	if err != nil {
		return retry("deletion completion failed", w.backoff(job.Attempts))
	}
	// CompleteDeletion zeroed the reservation only after the node confirmed
	// the VM is gone; that is the second confirmed outcome that may release
	// a restore pin.
	w.releaseSnapshotPin(ctx, owner, sandboxID)
	return nil
}

// completedDeletion treats a delivery of an already completed operation as
// success. A missing operation id is a tampered or orphaned job.
func (w *Worker) completedDeletion(ctx context.Context, owner uuid.UUID, opID, sandboxID string) error {
	op, err := w.store.DeletionOperation(ctx, owner, opID)
	if errors.Is(err, db.ErrNotFound) {
		return permanent("deletion operation missing")
	}
	if err != nil {
		return retry("deletion operation read failed", w.backoff(1))
	}
	if op.TargetID != sandboxID || op.Kind != db.DeletionSandbox {
		return permanent("job payload does not match deletion operation")
	}
	if op.Status != db.DeletionCompleted {
		return retry("deletion operation reordered", w.backoff(1))
	}
	return nil
}
