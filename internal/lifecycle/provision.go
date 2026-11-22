package lifecycle

import (
	"context"
	"encoding/json/v2"
	"errors"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// provision boots one sandbox. Every side effect runs under the per-sandbox
// PostgreSQL guard, and every state change presents the job's lease token.
// The job payload is never trusted for the node, VM id or resources: those
// come from the canonical row read under the guard.
func (w *Worker) provision(ctx context.Context, job db.Job, lease *Lease) error {
	if job.SandboxID == nil {
		return permanent("missing sandbox id")
	}
	owner, sandboxID := job.OwnerID, *job.SandboxID
	var p db.ProvisionPayload
	if err := json.Unmarshal([]byte(job.Payload), &p); err != nil {
		return permanent("invalid provision payload")
	}

	sbx, err := w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return permanent("sandbox missing")
	}
	if err != nil {
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if sbx.DeletedAt != nil {
		// Deletion was accepted while this attempt was queued or running.
		// The delete job owns VM cleanup; never boot a deleted sandbox.
		return nil
	}
	if !payloadMatches(sbx, p) {
		return permanent("job payload does not match sandbox")
	}
	switch sbx.State {
	case db.StateProvisioning:
		// Continue below.
	case db.StateArchiving:
		// A stop accepted while this first boot was queued or running still
		// needs the disk the boot creates: complete the boot (the state
		// machine never returns an archiving row to provisioning) and leave
		// the archival itself to the stop job. The generation fence in
		// payloadMatches already proved this is the initial boot; a resumed
		// row (generation > 0) never reaches here.
		if sbx.LifecycleGeneration != 0 {
			return permanent("sandbox not provisionable")
		}
	case db.StateReady, db.StateIdle, db.StateRunning, db.StateProvisioned:
		return nil // duplicate delivery of already-booted work
	case db.StateError:
		return nil // terminal failure already recorded; reaper cleans
	default:
		return permanent("sandbox not provisionable")
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

	// Canonical recheck under the guard: deletion acceptance updates the
	// row without a version bump, so this read is what forbids a stale
	// boot from starting after the guard was handed over.
	sbx, err = w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return permanent("sandbox missing")
	}
	if err != nil {
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if sbx.DeletedAt != nil {
		return nil
	}
	if !payloadMatches(sbx, p) {
		return permanent("job payload does not match sandbox")
	}
	// The under-guard recheck accepts the same two states as the eligibility
	// switch above: a normal provisioning boot, and the initial boot of a
	// sandbox whose stop was accepted mid-boot (the disk must still be
	// created for the stop to retain).
	if sbx.State != db.StateProvisioning &&
		!(sbx.State == db.StateArchiving && sbx.LifecycleGeneration == 0) {
		return nil
	}

	env, err := w.store.DecryptSandboxEnvForNode(ctx, owner, sandboxID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil // deleted between the two reads
		}
		return retry("env unavailable", w.backoff(job.Attempts))
	}
	client, closer, err := w.dial(p.NodeID)
	if err != nil {
		return retry("node gateway unavailable", w.backoff(job.Attempts))
	}
	defer closer.Close()

	bootCtx, bootCancel := context.WithTimeout(opCtx, w.bootTimeout)
	defer bootCancel()
	vm, err := client.CreateVM(bootCtx, &nodev1.CreateVMRequest{
		SandboxId: sbx.ID,
		Resources: resourcesOf(sbx.Reserved),
		Image:     "", // the node's default image, always
		// from_snapshot and from_manifest are the canonical immutable
		// sources recorded on the sandbox row, never a payload or a
		// re-resolved name.
		FromSnapshot: sbx.FromSnapshotID,
		FromManifest: sbx.FromManifestKey,
		Env:          env,
		// Hostname stays empty: the display name is user input that may
		// contain spaces, unicode or exceed a hostname limit. The node
		// chooses its canonical default.
		Hostname: "",
	})
	bootCancel()
	if gerr := guard.Err(); gerr != nil {
		return w.afterGuardLoss(ctx, lease, guard, sbx)
	}
	if lease.Lost() || ctx.Err() != nil {
		return w.abortedBoot(ctx, lease, guard, sbx)
	}
	if err != nil {
		if sbx.FromManifestKey != "" && status.Code(err) == codes.FailedPrecondition {
			return w.manifestRefused(ctx, job, lease, guard, opCtx, sbx, p, client)
		}
		return w.bootFailed(ctx, job, lease, guard, opCtx, sbx, err)
	}
	if vm.GetSandboxId() != sbx.ID || vm.GetState() != nodev1.VMState_VM_STATE_READY {
		return w.bootFailed(ctx, job, lease, guard, opCtx, sbx,
			status.Error(codes.Internal, "CreateVM returned a non-ready VM"))
	}

	// Success: verify deletion did not slip in while the RPC ran, then
	// move the row with the claim's token.
	sbx, err = w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return nil
	}
	if err != nil {
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if sbx.DeletedAt != nil {
		return w.compensateDeleted(opCtx, sbx)
	}
	if sbx.State == db.StateArchiving && sbx.LifecycleGeneration == 0 {
		// The first disk exists and its guest answered (CreateVM returned
		// READY): the confirmed-ready outcome may release a restore pin, and
		// the stop job finalizes the archival this boot was racing.
		w.releaseSnapshotPin(ctx, owner, sbx.ID)
		w.markObjectStore(ctx, sbx)
		return nil
	}
	if sbx.State != db.StateProvisioning {
		return nil
	}
	if !CanTransition(sbx.State, db.StateReady) {
		return permanent("illegal provisioning transition")
	}
	if guard.Err() != nil {
		return w.afterGuardLoss(ctx, lease, guard, sbx)
	}
	_, err = w.store.TransitionSandboxJob(ctx, job.ID, w.workerID, job.LeaseToken, db.TransitionParams{
		SandboxID:       sbx.ID,
		Owner:           owner,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: sbx.Version,
		To:              db.StateReady,
		// Restores are eager: a READY manifest restore has its
		// whole disk local.
		Hydrated: sbx.FromManifestKey != "",
	})
	switch {
	case err == nil:
		// The boot is confirmed ready: the artifact pin may now release.
		w.releaseSnapshotPin(ctx, owner, sbx.ID)
		w.markObjectStore(ctx, sbx)
		return nil
	case errors.Is(err, db.ErrJobNotLeased):
		return nil
	case errors.Is(err, db.ErrNotFound):
		// Deleted while transitioning: the delete job cleans up.
		return nil
	case errors.Is(err, db.ErrConflict):
		// A newer attempt or another actor moved the row; re-read to
		// decide only the deleted case, never overwrite.
		cur, rerr := w.store.SandboxAny(ctx, owner, sandboxID)
		if rerr == nil && cur.DeletedAt != nil {
			return w.compensateDeleted(opCtx, cur)
		}
		return nil
	default:
		return retry("state transition failed", w.backoff(job.Attempts))
	}
}

// bootFailed handles a CreateVM error. Retryable transport/boot failures get
// bounded attempts after an idempotent partial-VM delete; terminal failures
// move the sandbox to error, confirm cleanup and only then release the
// owner-aware reservation.
func (w *Worker) bootFailed(ctx context.Context, job db.Job, lease *Lease, guard *db.SandboxGuard,
	opCtx context.Context, sbx db.Sandbox, cause error,
) error {
	reason := classify(cause)
	terminal := bootFailureReason(sbx, cause)
	if status.Code(cause) == codes.Aborted {
		// A DeleteVM fenced this boot. The deletion job owns the rest; if
		// the row is somehow still live it is terminally broken.
		cur, err := w.store.SandboxAny(ctx, job.OwnerID, sbx.ID)
		if err != nil || cur.DeletedAt != nil {
			return nil
		}
		return permanent("sandbox deleted")
	}
	if retryableProvision(cause) && !errors.Is(cause, errNoReadyMachine) && job.Attempts < job.MaxAttempts {
		// A failed boot may still have left a disk or VM, but the guarded
		// pre-RPC snapshot is stale the moment the RPC ran: reread the
		// canonical row and let ITS owner decide whether cleaning up is
		// ours to do.
		cur, err := w.store.SandboxAny(ctx, job.OwnerID, sbx.ID)
		if err != nil {
			// A read failure proves nothing: retry without cleanup.
			return retry(reason, w.backoff(job.Attempts))
		}
		switch {
		case cur.DeletedAt != nil:
			// Deletion was accepted: it owns the VM and disk; compensate
			// best-effort and settle this job.
			return w.compensateDeleted(opCtx, cur)
		case cur.LifecycleGeneration != 0:
			// A later resume owns the boot and its retained disk; settle
			// the stale initial job without touching the node.
			return permanent(reason)
		case cur.State == db.StateArchiving || cur.State == db.StateArchived:
			// The accepted stop owns the disk and the archival. Keep the
			// partial boot for it (CreateVM is idempotent) and let the
			// retry settle the initial job under the archiving allowance.
			return retry(reason, w.backoff(job.Attempts))
		case cur.State == db.StateProvisioning:
			// Still our own initial boot: remove the partial VM so the
			// retry starts from a clean node, keeping the reservation.
			if err := w.deleteVMUnderGuard(opCtx, guard, cur); err != nil {
				w.log.Warn("partial boot cleanup failed", "sandbox", cur.ID, "reason", classify(err))
			}
			return retry(reason, w.backoff(job.Attempts))
		default:
			// A concurrent attempt succeeded or the failure was recorded:
			// leave the row to its owner.
			return nil
		}
	}

	// Terminal. Record the safe reason before cleanup, then clean up and
	// release capacity only after the node confirmed the VM is gone.
	cur, err := w.store.SandboxAny(ctx, job.OwnerID, sbx.ID)
	if errors.Is(err, db.ErrNotFound) {
		return permanent(terminal)
	}
	if err != nil {
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if cur.DeletedAt != nil {
		return w.compensateDeleted(opCtx, cur)
	}
	// Initial-create terminal cleanup owns only generation zero rows whose
	// durable outcome is this attempt's (provisioning, or the error state it
	// just recorded). A nonzero generation means a later resume owns the boot
	// and its retained disk; an archiving or archived generation-zero row
	// belongs to the accepted stop, which also owns the case where the first
	// boot never produced a disk. In every one of those cases the stale
	// attempt neither deletes nor releases: its owning job resolves the row.
	if cur.LifecycleGeneration != 0 || cur.State == db.StateArchiving || cur.State == db.StateArchived {
		return permanent(terminal)
	}
	if cur.State == db.StateProvisioning {
		if !CanTransition(cur.State, db.StateError) {
			return permanent("illegal failure transition")
		}
		_, err = w.store.TransitionSandboxJob(ctx, job.ID, w.workerID, job.LeaseToken, db.TransitionParams{
			SandboxID:       cur.ID,
			Owner:           job.OwnerID,
			FromStates:      []db.SandboxState{db.StateProvisioning},
			ExpectedVersion: cur.Version,
			To:              db.StateError,
			ErrorReason:     terminal,
		})
		switch {
		case err == nil:
			cur.State = db.StateError
		case errors.Is(err, db.ErrJobNotLeased):
			// Another worker owns the outcome; do not release capacity
			// under a stale claim.
			return nil
		case errors.Is(err, db.ErrNotFound):
			// Deleted while failing: the delete job completes cleanup and
			// release; this attempt only confirms the VM is gone below.
		case errors.Is(err, db.ErrConflict):
			fresh, rerr := w.store.SandboxAny(ctx, job.OwnerID, cur.ID)
			if rerr != nil {
				return nil
			}
			if fresh.DeletedAt != nil {
				return w.compensateDeleted(opCtx, fresh)
			}
			if fresh.State != db.StateError {
				return nil // recovered or moved by another actor
			}
			cur = fresh
		default:
			return retry("failure transition failed", w.backoff(job.Attempts))
		}
	}
	if cur.State == db.StateReady || cur.State == db.StateIdle || cur.State == db.StateRunning {
		return nil // a concurrent attempt succeeded; leave it alone
	}
	if err := w.deleteVMUnderGuard(opCtx, guard, cur); err != nil {
		// Cleanup not confirmed: leave the reservation for the reaper,
		// which retries this now-failed job.
		return permanent(terminal)
	}
	if guard.Err() != nil || opCtx.Err() != nil || lease.Lost() {
		// The guard or claim vanished after the delete: cleanup is
		// unconfirmed until reconciliation proves it. Do not release.
		return permanent(terminal)
	}
	if err := w.store.ReleaseSandboxReservation(ctx, job.OwnerID, cur.ID); err != nil {
		return permanent(terminal)
	}
	w.releaseSnapshotPin(ctx, job.OwnerID, cur.ID)
	return permanent(terminal)
}

// abortedBoot handles cancellation (shutdown or lost lease). When deletion
// was accepted meanwhile, compensate under the still-held guard; otherwise
// leave the row for the redelivered attempt, whose CreateVM is idempotent.
func (w *Worker) abortedBoot(ctx context.Context, lease *Lease, guard *db.SandboxGuard, sbx db.Sandbox) error {
	cur, err := w.store.SandboxAny(ctx, lease.Job().OwnerID, sbx.ID)
	if err != nil {
		return nil
	}
	if cur.DeletedAt != nil {
		return w.compensateDeleted(ctx, cur)
	}
	return nil
}

// afterGuardLoss cancels the attempt and tries a bounded re-acquire to
// compensate a VM that may have been created before the guard connection
// died. If it cannot re-acquire in time, periodic reconciliation owns the
// cleanup (it deletes VMs whose canonical sandbox row is deleted).
func (w *Worker) afterGuardLoss(ctx context.Context, lease *Lease, guard *db.SandboxGuard, sbx db.Sandbox) error {
	guard.Release(ctx)
	if ctx.Err() != nil {
		return nil // shutdown: the lease expires and redelivery retries
	}
	reacquire, cancel := context.WithTimeout(ctx, w.guardReacquire)
	defer cancel()
	g, gctx, stop, err := w.acquireGuardWith(ctx, reacquire, lease.Job().OwnerID, sbx.ID)
	if err != nil {
		w.log.Warn("guard reacquire failed; reconciliation owns cleanup", "sandbox", sbx.ID)
		return retry("sandbox guard lost", w.backoff(lease.Job().Attempts))
	}
	defer func() {
		stop()
		g.Release(ctx)
	}()
	cur, err := w.store.SandboxAny(ctx, lease.Job().OwnerID, sbx.ID)
	if err == nil && cur.DeletedAt != nil {
		if err := w.deleteVMUnderGuard(gctx, g, cur); err != nil {
			w.log.Warn("guard-loss compensation deferred to reconciliation", "sandbox", cur.ID)
		}
		return nil // the delete job owns completion
	}
	// The row still wants a boot: this attempt was aborted, so redeliver
	// rather than wrongly marking the job done.
	return retry("sandbox guard lost", w.backoff(lease.Job().Attempts))
}

// compensateDeleted removes the VM for a canonical deleted row while the
// guard is held. The deletion job performs the final idempotent cleanup and
// CompleteDeletion; this is belt-and-braces for a boot that raced a delete.
func (w *Worker) compensateDeleted(ctx context.Context, sbx db.Sandbox) error {
	if err := w.deleteVMOnNode(ctx, derefNode(sbx.NodeID), sbx.ID); err != nil {
		w.log.Warn("deleted-sandbox compensation failed", "sandbox", sbx.ID, "reason", classify(err))
	}
	return nil
}

// deleteVMUnderGuard calls the canonical node with the canonical sandbox id.
// A guard loss cancels opCtx; the caller treats that as an unconfirmed
// cleanup and never releases capacity.
func (w *Worker) deleteVMUnderGuard(ctx context.Context, guard *db.SandboxGuard, sbx db.Sandbox) error {
	if guard.Err() != nil {
		return db.ErrSandboxGuardLost
	}
	return w.deleteVMOnNode(ctx, derefNode(sbx.NodeID), sbx.ID)
}

// deleteVMOnNode is the one idempotent node cleanup call. nodeID and id come
// from the canonical row, never from a job payload. The call is bounded per
// operation so one stuck node cannot pin a worker, a reaper page or TTL
// acceptance until the worker shuts down.
func (w *Worker) deleteVMOnNode(ctx context.Context, nodeID, sandboxID string) error {
	if nodeID == "" {
		return nil // no node was ever assigned; nothing to clean
	}
	client, closer, err := w.dial(nodeID)
	if err != nil {
		return err
	}
	defer closer.Close()
	cctx, cancel := context.WithTimeout(ctx, w.cleanupTimeout)
	defer cancel()
	_, err = client.DeleteVM(cctx, &nodev1.DeleteVMRequest{SandboxId: sandboxID})
	return err
}

// acquireGuarded takes the per-sandbox advisory guard under the lease's
// lifetime and returns a context cancelled by lease loss, worker shutdown or
// guard connection loss. The guard stays held until the caller's deferred
// Release.
func (w *Worker) acquireGuarded(ctx context.Context, lease *Lease, owner uuid.UUID, sandboxID string) (*db.SandboxGuard, context.Context, context.CancelFunc, error) {
	// The operation context follows the claim and worker, so a lost lease
	// cancels the blocking client call. That cancellation alone proves
	// nothing about the node: the frozen contract has DeleteVM fence and
	// join earlier boots, so cleanup and deletion completion still go
	// through DeleteVM.
	return w.acquireGuardWith(lease.Context(), lease.Context(), owner, sandboxID)
}

// acquireReapGuard bounds how long one reconciliation item may wait for a
// guard. A stuck or offline sandbox cannot starve the rest of a TTL page or
// failed-job walk: this item is skipped and retried next round.
func (w *Worker) acquireReapGuard(ctx context.Context, owner uuid.UUID, sandboxID string) (*db.SandboxGuard, context.Context, context.CancelFunc, error) {
	waitCtx, cancel := context.WithTimeout(ctx, w.reapGuardWait)
	defer cancel()
	return w.acquireGuardWith(ctx, waitCtx, owner, sandboxID)
}

// acquireGuardWith waits for the per-sandbox advisory lock under waitCtx,
// then builds the operation context from opParent and the guard lifetime. The
// watcher is explicitly joined by stop so no goroutine outlives the
// operation.
func (w *Worker) acquireGuardWith(opParent, waitCtx context.Context, owner uuid.UUID, sandboxID string) (*db.SandboxGuard, context.Context, context.CancelFunc, error) {
	guard, err := w.store.AcquireSandboxGuard(waitCtx, owner, sandboxID)
	if err != nil {
		return nil, nil, nil, err
	}
	opCtx, cancel := context.WithCancelCause(opParent)
	stopCh := make(chan struct{})
	var (
		once sync.Once
		wg   sync.WaitGroup
	)
	wg.Go(func() {
		select {
		case <-guard.Done():
			cancel(db.ErrSandboxGuardLost)
		case <-stopCh:
		}
	})
	stop := func() {
		once.Do(func() {
			close(stopCh)
			wg.Wait()
			cancel(nil)
		})
	}
	return guard, opCtx, stop, nil
}

// payloadMatches validates a provision payload against the canonical row.
// The node id, resource tuple, immutable from-snapshot artifact and the
// lifecycle generation are authorization-relevant: a tampered or stale job
// must not redirect a VM to another node, misstate its size, boot a different
// saved generation, or run the initial create against a row whose boot a
// resume already advanced. Initial provisions are ALWAYS generation zero on
// both sides: a positive generation in a provision payload is forged or
// stale even when it happens to match the row, and a row past generation
// zero belongs to resume jobs alone.
func payloadMatches(sbx db.Sandbox, p db.ProvisionPayload) bool {
	return p.Generation == 0 && sbx.LifecycleGeneration == 0 &&
		p.NodeID != "" && sbx.NodeID != nil && *sbx.NodeID == p.NodeID &&
		sbx.Reserved == p.Resources && sbx.FromSnapshotID == p.FromSnapshot &&
		sbx.FromManifestKey == p.FromManifest
}

// releaseSnapshotPin is the best-effort release after a confirmed outcome. A
// failure is logged by fixed classification and left to the snapshot
// reconciler's required eligible-pin scan; it is never retried by unpinning
// early or after a lost lease.
func (w *Worker) releaseSnapshotPin(ctx context.Context, owner uuid.UUID, sandboxID string) {
	err := w.store.ReleaseSnapshotPin(ctx, owner, sandboxID)
	switch {
	case err == nil, errors.Is(err, db.ErrNotFound), errors.Is(err, db.ErrSnapshotPinNotReleasable):
		return
	default:
		w.log.Warn("snapshot pin release deferred", "sandbox", sandboxID, "reason", classify(err))
	}
}

// resourcesOf converts database resources for the frozen CreateVM request.
func resourcesOf(r db.Resources) *nodev1.Resources {
	return &nodev1.Resources{Vcpus: r.VCPUs, MemoryMib: r.MemoryMiB, DiskGib: r.DiskGiB}
}

// derefNode reads an optional node id.
func derefNode(nodeID *string) string {
	if nodeID == nil {
		return ""
	}
	return *nodeID
}
