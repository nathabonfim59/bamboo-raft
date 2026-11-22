package lifecycle

// Fork jobs: the API admits a fork sandbox and a [db.JobFork], and this worker
// produces the generation it restores. A ready source is cut a fresh
// generation under its own guard; a stopped (archived) source contributes its
// latest completed generation. The generation is then handed to the fork's
// provision job in one lease-fenced transaction, so a lost lease can never
// boot the same fork twice.

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// Reasons recorded on a fork sandbox that could not be handed off. They are
// fixed codes, never node or driver text.
const forkFailedReason = "fork_failed"

// Errors CutGeneration reports. Callers map them to their own durable outcome:
// the fork handler ends the fork (unavailable/refused/failed), retries (busy),
// or settles a lost claim (aborted).
var (
	// ErrCutSourceUnavailable means the source cannot be snapshotted now: it
	// is gone, deleted, not ready-ish, has snapshots off or has no node.
	ErrCutSourceUnavailable = errors.New("lifecycle: source sandbox cannot be snapshotted")

	// ErrCutSnapshotBusy means another snapshot of the source is in flight
	// (or the chunk collector is sweeping): retry shortly.
	ErrCutSnapshotBusy = errors.New("lifecycle: source snapshot already in flight")

	// ErrCutSnapshotFailed means the node's snapshot attempt failed; a retry
	// may succeed.
	ErrCutSnapshotFailed = errors.New("lifecycle: source snapshot failed")

	// ErrCutSnapshotRefused means the node cannot snapshot this sandbox at
	// all (no object store, or a VM it cannot snapshot); retrying will not
	// help until the node's capability is fixed.
	ErrCutSnapshotRefused = errors.New("lifecycle: source node cannot snapshot")

	// ErrCutAborted means the caller's claim or guard was lost, or the
	// worker is shutting down, while the cut ran. The redelivered attempt
	// owns the outcome.
	ErrCutAborted = errors.New("lifecycle: generation cut aborted")

	// errCutStore means a database call failed while cutting; the caller
	// retries like any other store fault.
	errCutStore = errors.New("lifecycle: generation cut store failed")
)

// CutGeneration takes one fresh generation of the owner's sandbox under its
// guard and returns the completed row. It wraps takeGeneration, so it is the
// single way a job cuts a generation for a fork or a saved snapshot; callers
// must not call BeginGeneration themselves.
//
// The caller holds the job claim (lease) but not the sandbox guard: this method
// acquires it, rereads the sandbox under it and releases it before returning.
// A nil object store answers [ErrCutSnapshotRefused]. Every outcome is
// classified by the [ErrCutSourceUnavailable], [ErrCutSnapshotBusy],
// [ErrCutSnapshotFailed], [ErrCutSnapshotRefused] and [ErrCutAborted]
// sentinels; a database fault keeps its own wrap.
func (w *Worker) CutGeneration(ctx context.Context, lease *Lease, owner uuid.UUID, sandboxID string) (db.Generation, error) {
	if w.objectStore == nil {
		return db.Generation{}, ErrCutSnapshotRefused
	}
	sbx, err := w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return db.Generation{}, ErrCutSourceUnavailable
	}
	if err != nil {
		return db.Generation{}, fmt.Errorf("lifecycle: cut generation read: %w", err)
	}
	if !snapshotCandidate(sbx) {
		return db.Generation{}, ErrCutSourceUnavailable
	}

	guard, opCtx, stop, err := w.acquireGuarded(ctx, lease, owner, sandboxID)
	if err != nil {
		if lease.Lost() || ctx.Err() != nil {
			return db.Generation{}, ErrCutAborted
		}
		return db.Generation{}, ErrCutSnapshotBusy
	}
	defer func() {
		stop()
		guard.Release(ctx)
	}()

	// Canonical reread under the guard: a stop, resume or delete that held
	// the guard before us changed the row.
	sbx, err = w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return db.Generation{}, ErrCutSourceUnavailable
	}
	if err != nil {
		return db.Generation{}, fmt.Errorf("lifecycle: cut generation read: %w", err)
	}
	if !snapshotCandidate(sbx) {
		return db.Generation{}, ErrCutSourceUnavailable
	}

	att := w.takeGeneration(ctx, lease, guard, opCtx, sbx)
	switch att.outcome {
	case generationCompleted:
		return att.gen, nil
	case generationRefused:
		return db.Generation{}, ErrCutSnapshotRefused
	case generationInFlight, generationSweeping:
		return db.Generation{}, ErrCutSnapshotBusy
	case generationFailed:
		return db.Generation{}, ErrCutSnapshotFailed
	case generationAbandoned:
		return db.Generation{}, ErrCutAborted
	case generationStoreError:
		return db.Generation{}, errCutStore
	default:
		return db.Generation{}, ErrCutSourceUnavailable
	}
}

// forkSandbox is the sandbox.fork handler: it resolves the generation the
// admitted fork restores and hands the fork to its provision job. A fork that
// cannot be handed off ends in the error state with its reservation released.
func (w *Worker) forkSandbox(ctx context.Context, job db.Job, lease *Lease) error {
	if job.SandboxID == nil {
		return permanent("missing sandbox id")
	}
	owner, forkID := job.OwnerID, *job.SandboxID
	var p db.ForkPayload
	if err := json.Unmarshal([]byte(job.Payload), &p); err != nil {
		return permanent("invalid fork payload")
	}
	if p.SourceID == "" || p.NodeID == "" || p.SourceID == forkID {
		return permanent("invalid fork payload")
	}

	fork, err := w.store.SandboxAny(ctx, owner, forkID)
	if errors.Is(err, db.ErrNotFound) {
		return nil
	}
	if err != nil {
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if fork.DeletedAt != nil || !forkHandOffState(fork) {
		// A duplicate delivery, or the fork already moved on: the provision
		// job, a stop or an accepted deletion owns the row.
		return nil
	}

	source, err := w.store.SandboxAny(ctx, owner, p.SourceID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return w.forkFailed(ctx, job, fork)
		}
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if source.DeletedAt != nil {
		return w.forkFailed(ctx, job, fork)
	}

	gen, err := w.forkGeneration(ctx, job, lease, source)
	if err != nil {
		return w.forkGenerationFailure(ctx, job, lease, fork, err)
	}

	_, err = w.store.StartFork(ctx, job.ID, w.workerID, job.LeaseToken, db.StartForkParams{
		ForkID:       forkID,
		Owner:        owner,
		SourceID:     p.SourceID,
		NodeID:       p.NodeID,
		GenerationID: gen.ID,
	})
	switch {
	case err == nil:
		return nil
	case errors.Is(err, db.ErrJobNotLeased), errors.Is(err, db.ErrNotFound):
		// The claim moved or the fork was deleted: no side effect of ours
		// remains to finish.
		return nil
	case errors.Is(err, db.ErrSnapshotNotFound), errors.Is(err, db.ErrSnapshotDiskTooSmall),
		errors.Is(err, db.ErrConflict), errors.Is(err, db.ErrInvalidJobPayload):
		return w.forkFailed(ctx, job, fork)
	default:
		return retry("fork hand-off failed", w.backoff(job.Attempts))
	}
}

// forkGeneration resolves the generation the fork restores. A ready source is
// cut fresh under its guard; a source that stopped after the fork was admitted
// contributes its latest completed generation, which a running stop already
// took. A stopped source uses the generation admission pinned, so the fork
// restores exactly what the client was answered for.
func (w *Worker) forkGeneration(ctx context.Context, job db.Job, lease *Lease, source db.Sandbox) (db.Generation, error) {
	if !source.SnapshotsEnabled {
		return db.Generation{}, db.ErrSnapshotsDisabled
	}
	if source.DataExpiredAt != nil {
		return db.Generation{}, db.ErrSnapshotDataExpired
	}
	switch {
	case readyState(source.State):
		return w.CutGeneration(ctx, lease, job.OwnerID, source.ID)
	case source.State == db.StateArchived:
		if job.SourceGenerationID != nil {
			return w.store.GenerationByID(ctx, job.OwnerID, source.ID, *job.SourceGenerationID)
		}
		gen, ok, err := w.store.LatestGeneration(ctx, job.OwnerID, source.ID)
		if err != nil {
			return db.Generation{}, err
		}
		if !ok {
			return db.Generation{}, db.ErrSnapshotNotFound
		}
		return gen, nil
	default:
		return db.Generation{}, db.ErrInvalidState
	}
}

// forkGenerationFailure maps a resolved-generation failure to the job's
// durable outcome: a lost claim or worker shutdown settles the attempt; a
// busy or failed snapshot retries under the attempt budget; an unavailable,
// refused or state-changed source ends the fork.
func (w *Worker) forkGenerationFailure(ctx context.Context, job db.Job, lease *Lease, fork db.Sandbox, cause error) error {
	switch {
	case errors.Is(cause, ErrCutAborted):
		if lease.Lost() || ctx.Err() != nil {
			return nil
		}
		return retry("fork generation aborted", w.backoff(job.Attempts))
	case errors.Is(cause, ErrCutSnapshotBusy), errors.Is(cause, ErrCutSnapshotFailed):
		return retry("fork generation unavailable", w.backoff(job.Attempts))
	case errors.Is(cause, ErrCutSourceUnavailable), errors.Is(cause, ErrCutSnapshotRefused),
		errors.Is(cause, db.ErrSnapshotsDisabled), errors.Is(cause, db.ErrSnapshotDataExpired),
		errors.Is(cause, db.ErrSnapshotNotFound), errors.Is(cause, db.ErrNotFound),
		errors.Is(cause, db.ErrInvalidState):
		return w.forkFailed(ctx, job, fork)
	default:
		return retry("fork generation failed", w.backoff(job.Attempts))
	}
}

// forkHandOffState reports whether the fork row still wants its hand-off: a
// first boot in provisioning, or an archiving first boot whose stop needs the
// disk. Anything else is a duplicate delivery, a stop that already finished or
// a deleted row.
func forkHandOffState(fork db.Sandbox) bool {
	switch fork.State {
	case db.StateProvisioning:
		return true
	case db.StateArchiving:
		return fork.LifecycleGeneration == 0
	default:
		return false
	}
}

// forkFailed ends a fork that cannot be handed off: the row moves to error
// with the fixed fork_failed reason and its reservation is released. A stop or
// a deletion that moved the row first owns the cleanup instead. The job
// settles permanently; if the release does not commit, the reaper's failed-fork
// reconciliation retries it.
func (w *Worker) forkFailed(ctx context.Context, job db.Job, fork db.Sandbox) error {
	if fork.State != db.StateProvisioning {
		return nil
	}
	fresh, err := w.store.SandboxAny(ctx, job.OwnerID, fork.ID)
	if errors.Is(err, db.ErrNotFound) {
		return nil
	}
	if err != nil {
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if fresh.DeletedAt != nil || fresh.State != db.StateProvisioning || fresh.LifecycleGeneration != 0 {
		return nil
	}
	if !CanTransition(fresh.State, db.StateError) {
		return permanent("illegal failure transition")
	}
	_, err = w.store.TransitionSandboxJob(ctx, job.ID, w.workerID, job.LeaseToken, db.TransitionParams{
		SandboxID:       fresh.ID,
		Owner:           job.OwnerID,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: fresh.Version,
		To:              db.StateError,
		ErrorReason:     forkFailedReason,
	})
	switch {
	case err == nil:
	case errors.Is(err, db.ErrJobNotLeased), errors.Is(err, db.ErrNotFound), errors.Is(err, db.ErrConflict):
		return nil
	default:
		return retry("fork failure transition failed", w.backoff(job.Attempts))
	}
	if err := w.store.ReleaseSandboxReservation(ctx, job.OwnerID, fresh.ID); err != nil {
		// The reaper retries the release from the failed job row.
		w.log.Warn("fork reservation release deferred", "sandbox", fresh.ID, "reason", classify(err))
	}
	return permanent(forkFailedReason)
}

// reconcileFailedFork finishes a fork that failed terminally before it could
// either hand off or record its own failure: it ends the fork in error and
// releases the reservation. A fork whose provision job exists already belongs
// to that boot; a fork in any other state was finished by its own actor.
func (w *Worker) reconcileFailedFork(ctx context.Context, job db.Job) {
	if job.SandboxID == nil {
		return
	}
	owner, forkID := job.OwnerID, *job.SandboxID
	fork, err := w.store.SandboxAny(ctx, owner, forkID)
	if errors.Is(err, db.ErrNotFound) || err != nil || fork.DeletedAt != nil {
		return
	}
	if fork.State != db.StateProvisioning || fork.LifecycleGeneration != 0 {
		return
	}
	if _, err := w.store.LatestSandboxJob(ctx, owner, forkID, db.JobProvision); err == nil {
		return // the hand-off committed; the provision job owns the boot
	} else if !errors.Is(err, db.ErrNotFound) {
		return
	}
	_, err = w.store.TransitionSandbox(ctx, db.TransitionParams{
		SandboxID:       forkID,
		Owner:           owner,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: fork.Version,
		To:              db.StateError,
		ErrorReason:     forkFailedReason,
	})
	switch {
	case err == nil:
	case errors.Is(err, db.ErrNotFound), errors.Is(err, db.ErrConflict):
		// A deletion or a stop moved the row first: its own actor owns the
		// cleanup, so this path must not release capacity under it.
		return
	default:
		w.log.Warn("failed fork error transition failed", "sandbox", forkID, "reason", classify(err))
		return
	}
	if err := w.store.ReleaseSandboxReservation(ctx, owner, forkID); err != nil {
		w.log.Warn("failed fork release failed", "sandbox", forkID, "reason", classify(err))
	}
}
