package lifecycle

import (
	"context"
	"encoding/json/v2"
	"errors"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// ReapOnce runs one maintenance round: fail stale claims, accept TTL
// auto-stops, expire the stored data of sandboxes stopped past the
// data-retention window, accept stopped-retention deletions, walk every
// terminally failed job page from the oldest, fail pending snapshot
// generations past their deadline, move the sandboxes of lost nodes, and
// reconcile node VM inventories. Tests call it directly; Run calls it on the
// reaper interval.
func (w *Worker) ReapOnce(ctx context.Context) {
	if n, err := w.store.FailStaleJobs(ctx); err != nil {
		w.log.Warn("fail stale jobs failed", "reason", classify(err))
	} else if n > 0 {
		w.log.Warn("failed jobs with expired leases", "count", n)
	}
	w.reapTTL(ctx)
	w.reapDataExpiry(ctx)
	w.reapStoppedRetention(ctx)
	w.reconcileFailedJobs(ctx)
	w.reconcilePendingGenerations(ctx)
	w.reapLostNodes(ctx)
	w.reapDiskPressure(ctx)
	w.reconcileVMs(ctx)
}

// reaperLoop runs ReapOnce immediately and then once per interval until ctx
// ends.
func (w *Worker) reaperLoop(ctx context.Context) {
	w.ReapOnce(ctx)
	ticker := time.NewTicker(w.reaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.ReapOnce(ctx)
		}
	}
}

// reapTTL accepts a durable stop for every active sandbox whose TTL passed:
// reaching the deadline archives the sandbox (auto-stop) instead of deleting
// it. Acceptance is transactional — the expected row version and the deadline
// are rechecked under the row lock, so a concurrent PATCH extension or manual
// stop wins — and the stop job performs the archival. Saved snapshots are
// independent and are never touched here.
func (w *Worker) reapTTL(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		now := time.Now()
		due, err := w.store.SandboxesExpiring(ctx, now, deletionReaperPageSize)
		if err != nil {
			w.log.Warn("TTL scan failed", "reason", classify(err))
			return
		}
		accepted := 0
		for _, sbx := range due {
			if ctx.Err() != nil {
				return
			}
			if _, _, _, _, err := w.store.AcceptSandboxExpiryStop(ctx, sbx.OwnerID, sbx.ID, sbx.Version, now, false); err != nil {
				switch {
				case errors.Is(err, db.ErrConflict), errors.Is(err, db.ErrNotFound), errors.Is(err, db.ErrInvalidState):
					// Extended, stopped, resumed or deleted meanwhile; the
					// next round re-reads the fresh row.
				default:
					w.log.Warn("TTL stop acceptance failed", "sandbox", sbx.ID, "reason", classify(err))
				}
			} else {
				accepted++
			}
		}
		if len(due) < deletionReaperPageSize {
			return
		}
		if accepted == 0 {
			// A full page with no accepted stop cannot make progress by
			// re-reading the same rows; leave it for the next round.
			w.log.Warn("TTL scan made no progress", "page", len(due))
			return
		}
	}
}

// reapStoppedRetention deletes archived sandboxes whose retained disk has
// been stopped longer than the configured retention. It uses the normal
// deletion acceptance (recorded as an explicit deletion: this is ordinary
// local retention, not a ZDR policy) and the normal delete worker; an
// acceptance rechecks state, stopped time and concurrent stop/resume work
// under the row lock, so an old candidate can never delete a sandbox that
// resumed or was deleted meanwhile. A zero retention disables the scan.
func (w *Worker) reapStoppedRetention(ctx context.Context) {
	if w.stoppedRetention <= 0 {
		return
	}
	for {
		if ctx.Err() != nil {
			return
		}
		cutoff := time.Now().Add(-w.stoppedRetention)
		due, err := w.store.StoppedSandboxesBefore(ctx, cutoff, deletionReaperPageSize)
		if err != nil {
			w.log.Warn("retention scan failed", "reason", classify(err))
			return
		}
		accepted := 0
		for _, sbx := range due {
			if ctx.Err() != nil {
				return
			}
			_, _, ok, err := w.store.AcceptStoppedRetentionDeletion(ctx, sbx.OwnerID, sbx.ID, cutoff, db.DeletionExplicit)
			if err != nil {
				if !errors.Is(err, db.ErrNotFound) {
					w.log.Warn("retention deletion acceptance failed", "sandbox", sbx.ID, "reason", classify(err))
				}
				continue
			}
			if ok {
				accepted++
			}
		}
		if len(due) < deletionReaperPageSize {
			return
		}
		if accepted == 0 {
			w.log.Warn("retention scan made no progress", "page", len(due))
			return
		}
	}
}

// reconcileFailedJobs walks all failed jobs in (updated_at, id) order,
// restarting from the beginning every round. An unreconciled old failure can
// never starve behind newer rows, and re-reading is safe because every
// reconciler action is idempotent.
func (w *Worker) reconcileFailedJobs(ctx context.Context) {
	var after *db.FailedJobCursor
	for {
		if ctx.Err() != nil {
			return
		}
		page, next, err := w.store.FailedJobs(ctx, after, failedJobsPageSize)
		if err != nil {
			w.log.Warn("failed job walk failed", "reason", classify(err))
			return
		}
		for _, job := range page {
			w.reconcileFailedJob(ctx, job)
		}
		if next == nil {
			return
		}
		after = next
	}
}

// reconcileFailedJob repairs the durable state a terminal job left behind.
// Snapshot jobs are not ours yet: B2 registers their handler later, so an
// unrelated failed kind is left untouched.
func (w *Worker) reconcileFailedJob(ctx context.Context, job db.Job) {
	switch job.Kind {
	case db.JobProvision:
		w.reconcileFailedProvision(ctx, job)
	case db.JobStop:
		w.reconcileFailedStop(ctx, job)
	case db.JobResume:
		w.reconcileFailedResume(ctx, job)
	case db.JobDeleteVM:
		w.reconcileFailedDelete(ctx, job)
	case db.JobFork:
		w.reconcileFailedFork(ctx, job)
	}
}

// reconcileFailedProvision finishes a boot that failed terminally but whose
// error state, VM cleanup or reservation release did not commit before the
// attempt ended. It only owns lifecycle generation zero — the initial
// provision: a row provisioning again after a resume belongs to its own
// resume job and retained disk, and this path's cleanup would destroy that
// disk. A ready sandbox is never torn down because a stale queue row says
// otherwise.
func (w *Worker) reconcileFailedProvision(ctx context.Context, job db.Job) {
	if job.SandboxID == nil {
		return
	}
	owner, sandboxID := job.OwnerID, *job.SandboxID
	sbx, err := w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return
	}
	if err != nil {
		w.log.Warn("failed provision reconciliation read failed", "sandbox", sandboxID, "reason", classify(err))
		return
	}
	if sbx.DeletedAt != nil {
		// VM inventory reconciliation removes known-deleted VMs; the
		// delete job completes the operation.
		return
	}
	if sbx.LifecycleGeneration != 0 {
		// A resume advanced the boot: the initial-create failure is stale and
		// must not mutate (or clean up) the later boot's retained disk.
		return
	}
	if sbx.State != db.StateProvisioning && sbx.State != db.StateError {
		return
	}

	guard, opCtx, stop, err := w.acquireReapGuard(ctx, owner, sandboxID)
	if err != nil {
		if ctx.Err() == nil {
			w.log.Warn("failed provision guard unavailable", "sandbox", sandboxID, "reason", classify(err))
		}
		return
	}
	defer func() {
		stop()
		guard.Release(ctx)
	}()

	// Re-read under the guard; another reconciler or a late transition may
	// have finished while we waited. The generation fence is rechecked HERE
	// too: a stop and resume accepted while this reconciler waited for the
	// guard can have advanced the boot, and an old initial-create failure
	// must not delete the later boot's retained disk.
	sbx, err = w.store.SandboxAny(ctx, owner, sandboxID)
	if err != nil || sbx.DeletedAt != nil {
		return
	}
	if sbx.LifecycleGeneration != 0 {
		return // a resume advanced the boot while we waited for the guard
	}
	if sbx.State == db.StateProvisioning {
		if !CanTransition(sbx.State, db.StateError) {
			return
		}
		_, err = w.store.TransitionSandbox(ctx, db.TransitionParams{
			SandboxID:       sbx.ID,
			Owner:           owner,
			FromStates:      []db.SandboxState{db.StateProvisioning},
			ExpectedVersion: sbx.Version,
			To:              db.StateError,
			ErrorReason:     "provision_failed",
		})
		switch {
		case errors.Is(err, db.ErrNotFound):
			return // deleted while we transitioned
		case errors.Is(err, db.ErrConflict):
			return // another actor moved it; next round rereads
		case err != nil:
			w.log.Warn("failed provision error transition failed", "sandbox", sandboxID, "reason", classify(err))
			return
		}
		sbx.State = db.StateError
	}
	if sbx.State != db.StateError {
		return
	}
	if err := w.deleteVMUnderGuard(opCtx, guard, sbx); err != nil {
		// Cleanup unconfirmed: keep the reservation for the next round.
		w.log.Warn("failed provision cleanup pending", "sandbox", sandboxID, "reason", classify(err))
		return
	}
	if guard.Err() != nil || opCtx.Err() != nil {
		// The guard vanished after the node answered: the cleanup is not
		// confirmed, so capacity stays reserved for the next round.
		return
	}
	if err := w.store.ReleaseSandboxReservation(ctx, owner, sbx.ID); err != nil {
		w.log.Warn("failed provision release failed", "sandbox", sandboxID, "reason", classify(err))
		return
	}
	w.releaseSnapshotPin(ctx, owner, sbx.ID)
}

// reconcileFailedDelete turns an exhausted delete job into a blocked
// operation plus a fresh queued delete job (deduplicated). The canonical row
// supplies the node and operation id; the failed payload is never trusted.
func (w *Worker) reconcileFailedDelete(ctx context.Context, job db.Job) {
	if job.SandboxID == nil {
		return
	}
	owner, sandboxID := job.OwnerID, *job.SandboxID
	sbx, err := w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return
	}
	if err != nil || sbx.DeletedAt == nil {
		return
	}
	op, err := w.store.ActiveSandboxDeletion(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return // completed or never accepted
	}
	if err != nil {
		w.log.Warn("failed delete operation read failed", "sandbox", sandboxID, "reason", classify(err))
		return
	}
	payload, err := json.Marshal(db.DeleteVMPayload{
		NodeID:      derefNode(sbx.NodeID),
		VMID:        sbx.ID,
		Resources:   sbx.Reserved,
		OperationID: op.ID,
	})
	if err != nil {
		return
	}
	if _, added, err := w.store.RetrySandboxDeletion(ctx, owner, op.ID, sandboxID, db.JobInput{
		Kind:      db.JobDeleteVM,
		OwnerID:   owner,
		SandboxID: &sandboxID,
		Payload:   string(payload),
	}); err != nil {
		w.log.Warn("retry delete enqueue failed", "sandbox", sandboxID, "reason", classify(err))
	} else if !added {
		// A newer attempt is already queued/running; leave its status.
		return
	}
}

// reconcileFailedStop requeues archival for a stop whose job exhausted its
// attempts while the operation is still pending or failing and the sandbox is
// still archiving. The canonical row and operation supply the payload; a
// queued or running stop job means a newer attempt owns the work.
//
// The complete terminal missing-disk outcome — the sandbox in error state
// with reason vm_missing under its current stop operation, which is failing
// with the same fixed error — is the one canonical unrecoverable stop and is
// never requeued. An operation error of vm_missing alone does not skip: the
// safe transient StopVM NOT_FOUND retry path can record that reason while the
// row is still archiving with its reservations held, and it must keep
// retrying until the guarded terminal policy or a normal stop resolves it.
func (w *Worker) reconcileFailedStop(ctx context.Context, job db.Job) {
	if job.SandboxID == nil {
		return
	}
	owner, sandboxID := job.OwnerID, *job.SandboxID
	sbx, err := w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) || err != nil || sbx.DeletedAt != nil {
		return
	}
	op, err := w.store.LatestSandboxStop(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return
	}
	if err != nil {
		w.log.Warn("failed stop reconciliation read failed", "sandbox", sandboxID, "reason", classify(err))
		return
	}
	if sbx.State == db.StateError && sbx.ErrorReason == missingDiskStop &&
		sbx.StopOperationID != nil && *sbx.StopOperationID == op.ID &&
		op.Status == db.StopFailing && op.Error == missingDiskStop {
		return // the canonical unrecoverable outcome; never retried
	}
	if op.Status != db.StopPending && op.Status != db.StopFailing {
		return // decided by a completing attempt, a resume or a deletion
	}
	if sbx.State != db.StateArchiving {
		return // moved on; the owning actor finishes it
	}
	payload, err := json.Marshal(db.StopPayload{
		OperationID: op.ID,
		NodeID:      derefNode(sbx.NodeID),
		VMID:        sbx.ID,
		Force:       op.ForceRequested,
	})
	if err != nil {
		return
	}
	if _, _, err := w.store.EnqueueJobIfAbsent(ctx, db.JobInput{
		Kind:      db.JobStop,
		OwnerID:   owner,
		SandboxID: &sandboxID,
		Payload:   string(payload),
	}); err != nil {
		w.log.Warn("retry stop enqueue failed", "sandbox", sandboxID, "reason", classify(err))
	}
}

// reconcileFailedResume requeues a resume whose job exhausted its attempts
// while the sandbox still sits in the failed job's own provisioning
// generation: redelivery retries the boot or finalizes its failure. The
// failed job's payload generation is bound to the canonical row before any
// retry is built, so a stale failed generation can never trigger work on a
// later boot; the rebuilt payload comes from the canonical row.
func (w *Worker) reconcileFailedResume(ctx context.Context, job db.Job) {
	if job.SandboxID == nil {
		return
	}
	owner, sandboxID := job.OwnerID, *job.SandboxID
	var failed db.ResumePayload
	if err := json.Unmarshal([]byte(job.Payload), &failed); err != nil ||
		failed.Generation <= 0 || failed.VMID != sandboxID {
		return // malformed or mismatched failed payload: never enqueue work
	}
	sbx, err := w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) || err != nil || sbx.DeletedAt != nil {
		return
	}
	if sbx.LifecycleGeneration != failed.Generation {
		return // the failed attempt is stale; a later boot owns the row
	}
	if sbx.State != db.StateProvisioning {
		return // ready, archived or otherwise moved on
	}
	if sbx.NodeID == nil || *sbx.NodeID == "" {
		return
	}
	payload, err := json.Marshal(db.ResumePayload{
		NodeID:      *sbx.NodeID,
		VMID:        sbx.ID,
		MachineType: sbx.MachineType,
		Resources:   sbx.Desired,
		FromStopID:  derefStopID(sbx.StopOperationID),
		Generation:  sbx.LifecycleGeneration,
		// A cross-node resume keeps its restore source and the
		// copy it orphaned: the generation is bound to the row above, and
		// only its admission chose them.
		FromManifest: failed.FromManifest,
		Image:        failed.Image,
		EvictNode:    failed.EvictNode,
	})
	if err != nil {
		return
	}
	if _, _, err := w.store.EnqueueJobIfAbsent(ctx, db.JobInput{
		Kind:               db.JobResume,
		OwnerID:            owner,
		SandboxID:          &sandboxID,
		Payload:            string(payload),
		SourceGenerationID: job.SourceGenerationID,
	}); err != nil {
		w.log.Warn("retry resume enqueue failed", "sandbox", sandboxID, "reason", classify(err))
	}
}
