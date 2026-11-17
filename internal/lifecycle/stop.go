package lifecycle

import (
	"context"
	"encoding/json/v2"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Bounds for StopVM. One call may wait out the guest's grace period (the
// node's default is 30 s), and a refused call (the VM is still BOOTING) is
// retried inside the same guard only for a bounded budget, so one stuck boot
// can neither pin a worker nor starve a redelivered provision attempt of the
// guard.
const (
	stopVMBudget    = 90 * time.Second
	stopBootRetry   = 500 * time.Millisecond
	forceGraceSecs  = 1
	missingDiskStop = "vm_missing"
)

// stopSandbox archives one sandbox. The durable StopOperation, reread under
// the guard instead of trusted from the job payload, is the canonical record
// of force escalation and retry state. Node side effects run under the shared
// per-sandbox guard, and a stop accepted while the initial boot is still
// queued or running checks that job BEFORE taking the guard and retries
// without it, so a pending stop can never starve the boot of the guard it
// needs; once the stop holds the guard the first disk exists (or the boot
// terminally failed) and StopVM runs.
func (w *Worker) stopSandbox(ctx context.Context, job db.Job, lease *Lease) error {
	if job.SandboxID == nil {
		return permanent("missing sandbox id")
	}
	owner, sandboxID := job.OwnerID, *job.SandboxID
	var p db.StopPayload
	if err := json.Unmarshal([]byte(job.Payload), &p); err != nil || p.OperationID == "" {
		return permanent("invalid stop payload")
	}

	sbx, err := w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return permanent("sandbox missing")
	}
	if err != nil {
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if sbx.DeletedAt != nil {
		// The deletion job owns the VM and its retained disk.
		return nil
	}
	op, err := w.store.StopOperation(ctx, owner, sandboxID, p.OperationID)
	if errors.Is(err, db.ErrNotFound) {
		return permanent("stop operation missing")
	}
	if err != nil {
		return retry("stop operation read failed", w.backoff(job.Attempts))
	}
	if op.Status == db.StopCompleted || op.Status == db.StopSuperseded {
		return nil // duplicate delivery of decided work
	}
	if sbx.State != db.StateArchiving {
		return permanent("sandbox not archiving")
	}

	// A stop that raced the initial provision must let that boot finish (or
	// terminally fail) before StopVM: the disk it wants to retain is the one
	// the boot creates. The check deliberately runs before the guard is
	// taken, and the retry path holds no guard, so the boot keeps full
	// access to it.
	if err := w.awaitInitialProvision(ctx, job, sbx); err != nil {
		return err
	}

	guard, opCtx, stopGuard, err := w.acquireGuarded(ctx, lease, owner, sandboxID)
	if err != nil {
		if lease.Lost() || ctx.Err() != nil {
			return nil
		}
		return retry("sandbox guard unavailable", w.backoff(job.Attempts))
	}
	defer func() {
		stopGuard()
		guard.Release(ctx)
	}()

	// Canonical reread under the guard: the payload never authorizes.
	sbx, op, proceed, err := w.rereadStopTarget(ctx, job, owner, sandboxID, p.OperationID)
	if err != nil {
		return err
	}
	if !proceed {
		return nil
	}

	// StopVM until the node confirms the VM is off with its disk kept. The
	// grace comes from the canonical operation: a force escalation meanwhile
	// is visible here, never through the stale payload.
	grace := uint32(0) // the node's default
	if op.ForceRequested {
		grace = forceGraceSecs
	}
	err = w.stopVMConfirmed(opCtx, derefNode(sbx.NodeID), sbx.ID, grace)
	if guard.Err() != nil {
		return retry("sandbox guard lost", w.backoff(job.Attempts))
	}
	if lease.Lost() || ctx.Err() != nil {
		// The VM may still be stopping remotely; redelivery re-runs the
		// idempotent StopVM and finalizes.
		return nil
	}
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return w.stopDiskMissing(ctx, job, lease, guard, opCtx, sbx, op)
		}
		return w.failStopAttempt(ctx, job, lease, owner, op, classify(err))
	}

	// The node confirmed the VM off. Finalize under the live claim; a PATCH
	// that bumped the version during the RPC only costs one requeue.
	return w.finalizeStop(ctx, job, lease, owner, sandboxID, db.FinalizeStopParams{
		Owner: owner, SandboxID: sandboxID, StopID: op.ID, ExpectedVersion: sbx.Version,
	})
}

// awaitInitialProvision reports whether the initial provision (lifecycle
// generation zero) has settled enough for StopVM: its canonical job must not
// be queued or running. It never holds the sandbox guard, so the boot keeps
// its turn.
func (w *Worker) awaitInitialProvision(ctx context.Context, job db.Job, sbx db.Sandbox) error {
	if sbx.LifecycleGeneration != 0 {
		return nil // a resumed sandbox already owns its disk
	}
	boot, err := w.store.LatestSandboxJob(ctx, sbx.OwnerID, sbx.ID, db.JobProvision)
	switch {
	case errors.Is(err, db.ErrNotFound):
		return nil // never had a boot job; nothing to wait for
	case err != nil:
		return retry("provision job read failed", w.backoff(job.Attempts))
	case boot.State == db.JobQueued || boot.State == db.JobRunning:
		return retry("initial boot in flight", w.backoff(job.Attempts))
	default:
		return nil // done or terminally failed: the disk question is settled
	}
}

// rereadStopTarget re-reads the sandbox and operation under the guard.
// proceed reports whether this attempt still owns the archival: decided
// operations, deleted rows and rows that moved on belong to another attempt
// (the caller reports success), while read failures and stale identities
// retry or fail closed.
func (w *Worker) rereadStopTarget(ctx context.Context, job db.Job, owner uuid.UUID, sandboxID, stopID string) (db.Sandbox, db.StopOperation, bool, error) {
	sbx, err := w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return sbx, db.StopOperation{}, false, permanent("sandbox missing")
	}
	if err != nil {
		return sbx, db.StopOperation{}, false, retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if sbx.DeletedAt != nil {
		return sbx, db.StopOperation{}, false, nil
	}
	op, err := w.store.StopOperation(ctx, owner, sandboxID, stopID)
	if errors.Is(err, db.ErrNotFound) {
		return sbx, op, false, permanent("stop operation missing")
	}
	if err != nil {
		return sbx, op, false, retry("stop operation read failed", w.backoff(job.Attempts))
	}
	if op.Status == db.StopCompleted || op.Status == db.StopSuperseded {
		return sbx, op, false, nil
	}
	if sbx.State != db.StateArchiving {
		return sbx, op, false, nil // moved on under a decided operation
	}
	if sbx.StopOperationID == nil || *sbx.StopOperationID != op.ID {
		return sbx, op, false, permanent("stop operation no longer current")
	}
	return sbx, op, true, nil
}

// stopVMConfirmed calls StopVM until the node confirms THIS sandbox's VM is
// STOPPED with its disk kept, or the budget ends. A BOOTING VM is refused
// (FAILED_PRECONDITION): the boot is an abandoned in-flight RPC (no live
// worker needs the guard for it), so the loop waits briefly for it to resolve
// instead of returning to the queue. Every other error, including NOT_FOUND,
// is returned to the caller for its own policy.
func (w *Worker) stopVMConfirmed(ctx context.Context, nodeID, sandboxID string, graceSeconds uint32) error {
	client, closer, err := w.dial(nodeID)
	if err != nil {
		return err
	}
	defer closer.Close()
	budget, cancel := context.WithTimeout(ctx, stopVMBudget)
	defer cancel()
	for {
		if budget.Err() != nil {
			return budget.Err()
		}
		vm, err := client.StopVM(budget, &nodev1.StopVMRequest{
			SandboxId: sandboxID, GraceSeconds: graceSeconds,
		})
		switch {
		case err == nil:
			if vm.GetSandboxId() != sandboxID || vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
				return status.Error(codes.Internal, "StopVM returned an unexpected VM")
			}
			return nil
		case status.Code(err) == codes.FailedPrecondition:
			if !sleepCtx(budget, stopBootRetry) {
				return budget.Err()
			}
		default:
			return err
		}
	}
}

// stopDiskMissing ends a stop whose VM and retained disk the node confirms
// missing, the orchestrator-approved terminal outcome: DeleteVM first (it
// fences any ambiguous earlier CreateVM/StartVM), then one fenced transaction
// fails the sandbox with the fixed vm_missing classification and releases
// every remaining reservation exactly once. The row and operation stay
// queryable for GET, a normal DELETE still works, and exactly one WARN is
// emitted for the whole outcome — the persisted row is the dedupe boundary.
// This outcome is never retried or reconciled into a requeue; transient
// failures elsewhere on the stop path keep their normal retries.
func (w *Worker) stopDiskMissing(ctx context.Context, job db.Job, lease *Lease, guard *db.SandboxGuard, opCtx context.Context, sbx db.Sandbox, op db.StopOperation) error {
	owner := sbx.OwnerID
	// StopVM's NOT_FOUND alone is not proof: confirm with the canonical
	// probe while still holding the guard.
	if _, err := w.getVM(opCtx, derefNode(sbx.NodeID), sbx.ID); err == nil {
		// The target reappeared (a raced boot finished); the ordinary stop
		// path retries.
		return retry("stop target reappeared", w.backoff(job.Attempts))
	} else if status.Code(err) != codes.NotFound {
		return w.failStopAttempt(ctx, job, lease, owner, op, classify(err))
	}
	// The fence comes first: DeleteVM aborts and joins any earlier boot for
	// this id, so nothing can (re)create the VM after it returns.
	if err := w.deleteVMUnderGuard(opCtx, guard, sbx); err != nil {
		return w.failStopAttempt(ctx, job, lease, owner, op, classify(err))
	}
	if guard.Err() != nil || opCtx.Err() != nil {
		return retry("sandbox guard lost", w.backoff(job.Attempts))
	}
	if lease.Lost() || ctx.Err() != nil {
		return nil // redelivery replays the fenced outcome idempotently
	}
	p := db.FinalizeStopParams{Owner: owner, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: sbx.Version}
	_, _, applied, err := w.store.FinalizeSandboxStopMissingDisk(ctx, job.ID, w.workerID, job.LeaseToken, p)
	switch {
	case err == nil:
		if applied {
			w.log.Warn("stop target missing; sandbox failed",
				"sandbox", sbx.ID, "node", derefNode(sbx.NodeID), "reason", missingDiskStop)
		}
		return permanent(missingDiskStop)
	case errors.Is(err, db.ErrJobNotLeased), errors.Is(err, db.ErrNotFound),
		errors.Is(err, db.ErrStopSuperseded):
		return nil
	case errors.Is(err, db.ErrConflict):
		// Distinguish an unresolved current intent (a PATCH raced the fence;
		// retry with the fresh version) from authoritative newer outcomes:
		// the already-persisted terminal marker, a superseded/completed
		// operation, a row that left archiving, or deletion. A failed reread
		// retries.
		cur, rerr := w.store.SandboxAny(ctx, owner, sbx.ID)
		if rerr != nil {
			if errors.Is(rerr, db.ErrNotFound) {
				return nil
			}
			return retry("missing-disk reread failed", w.backoff(job.Attempts))
		}
		if cur.DeletedAt != nil {
			return nil
		}
		if cur.State == db.StateError && cur.ErrorReason == missingDiskStop &&
			cur.StopOperationID != nil && *cur.StopOperationID == op.ID {
			return nil // the outcome is already persisted; stay silent
		}
		if cur.State != db.StateArchiving {
			return nil // a newer intent owns the row
		}
		final, oerr := w.store.StopOperation(ctx, owner, sbx.ID, op.ID)
		if oerr != nil {
			return retry("missing-disk reread failed", w.backoff(job.Attempts))
		}
		if final.Status == db.StopCompleted || final.Status == db.StopSuperseded {
			return nil
		}
		return retry("missing-disk finalization conflict", w.backoff(job.Attempts))
	default:
		return retry("missing-disk finalization failed", w.backoff(job.Attempts))
	}
}

// failStopAttempt records one failed archival attempt on the canonical
// operation under the live claim (the operation becomes failing and is
// retried automatically), then requeues the job. A lost claim or shutdown
// leaves everything to redelivery.
func (w *Worker) failStopAttempt(ctx context.Context, job db.Job, lease *Lease, owner uuid.UUID, op db.StopOperation, reason string) error {
	if lease.Lost() || ctx.Err() != nil {
		return nil
	}
	if _, err := w.store.FailSandboxStopAttempt(ctx, job.ID, w.workerID, job.LeaseToken, owner, op.ID, reason); err != nil {
		if errors.Is(err, db.ErrJobNotLeased) {
			return nil
		}
		return retry("stop attempt record failed", w.backoff(job.Attempts))
	}
	return retry(reason, w.backoff(job.Attempts))
}

// finalizeStop completes the archival after the node confirmed the VM is off.
// A version conflict costs one requeue (a PATCH raced the StopVM RPC) unless
// the canonical reread shows an authoritative newer outcome — deletion, a
// decided operation or a row that left archiving; superseded and lost-claim
// results are success for this delivery. A failed reread retries: it proves
// nothing about the row.
func (w *Worker) finalizeStop(ctx context.Context, job db.Job, lease *Lease, owner uuid.UUID, sandboxID string, p db.FinalizeStopParams) error {
	if lease.Lost() || ctx.Err() != nil {
		return nil
	}
	_, _, err := w.store.FinalizeSandboxStop(ctx, job.ID, w.workerID, job.LeaseToken, p)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, db.ErrJobNotLeased), errors.Is(err, db.ErrNotFound),
		errors.Is(err, db.ErrStopSuperseded):
		return nil
	case errors.Is(err, db.ErrConflict):
		sbx, rerr := w.store.SandboxAny(ctx, owner, sandboxID)
		if rerr != nil {
			if errors.Is(rerr, db.ErrNotFound) {
				return nil
			}
			return retry("stop finalization reread failed", w.backoff(job.Attempts))
		}
		if sbx.DeletedAt != nil || sbx.State != db.StateArchiving {
			return nil // a newer intent or another attempt owns the row
		}
		op, oerr := w.store.StopOperation(ctx, owner, sandboxID, p.StopID)
		if oerr != nil {
			return retry("stop finalization reread failed", w.backoff(job.Attempts))
		}
		if op.Status == db.StopCompleted || op.Status == db.StopSuperseded {
			return nil
		}
		if sbx.StopOperationID == nil || *sbx.StopOperationID != op.ID {
			return nil // a newer stop owns the machine
		}
		return retry("stop finalization conflict", w.backoff(job.Attempts))
	default:
		return retry("stop finalization failed", w.backoff(job.Attempts))
	}
}
