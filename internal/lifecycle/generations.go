package lifecycle

// Object-store snapshot generations.
//
// With an object store configured, every ready sandbox with snapshots on
// gets one generation per snapshot interval: a ticker enqueues a
// sandbox.snapshot job for each due sandbox, and the job takes the
// per-sandbox guard, inserts the pending generation row, asks the sandbox's
// node to write the manifest (SnapshotVM) and completes or fails the row.
// The stop handler reuses the same routine for its final generation. At most
// one SnapshotVM is in flight per sandbox: the guard serializes the callers,
// and the database refuses a second pending row.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Snapshot scheduling defaults (-lifecycle.snapshot-interval-seconds and
// -lifecycle.snapshot-timeout-seconds).
const (
	DefaultSnapshotInterval = 60 * time.Second
	DefaultSnapshotTimeout  = 10 * time.Minute
)

const (
	// snapshotJitter spreads each next snapshot time over ±10 % of the
	// interval so sandboxes created together do not snapshot together.
	snapshotJitter = 0.1
	// snapshotRefusedFactor is the back-off, in intervals, after a node
	// refused SnapshotVM with FAILED_PRECONDITION (no object store on the
	// node, or a VM that is not snapshotable).
	snapshotRefusedFactor = 10
	// maxGenerationTries bounds the ALREADY_EXISTS renumbering of one job.
	maxGenerationTries = 3
	// gcSweepRetry is how soon a job retries while the chunk GC sweeps.
	gcSweepRetry = 5 * time.Second
	// pendingGenerationGrace is how long past its deadline a pending row is
	// left alone before the reaper fails it, covering clock skew between
	// the database and the node's cancellation. It is capped by the snapshot
	// timeout.
	pendingGenerationGrace = 5 * time.Second
	// snapshotTickMin is the shortest scheduling tick.
	snapshotTickMin = time.Second
	// snapshotPageSize pages the due and pending scans.
	snapshotPageSize = 100
	// warnOnceCap bounds the per-Worker refusal log memory.
	warnOnceCap = 4096
)

// generationOutcome is how one takeGeneration call ended.
type generationOutcome int

const (
	// generationCompleted: the row is completed with the node's report.
	generationCompleted generationOutcome = iota + 1
	// generationRefused: the node answered FAILED_PRECONDITION; the row is
	// failed and the sandbox backs off.
	generationRefused
	// generationFailed: the row is failed; the sandbox keeps its schedule.
	generationFailed
	// generationSweeping: the chunk GC sweeps; no row was inserted.
	generationSweeping
	// generationInFlight: another pending row exists for the sandbox.
	generationInFlight
	// generationSkipped: the sandbox is gone, has snapshots off, has no
	// node or moved to another node; nothing to cut.
	generationSkipped
	// generationAbandoned: the lease or guard was lost, or the worker is
	// shutting down, during SnapshotVM. The row stays pending until the
	// reaper fails it past its deadline, or the next attempt takes it over
	// once this claim is gone: a stale attempt never completes.
	generationAbandoned
	// generationStoreError: a database call failed; the job retries.
	generationStoreError
)

// generationAttempt is takeGeneration's result. reason is a fixed, safe text.
type generationAttempt struct {
	outcome generationOutcome
	gen     db.Generation
	reason  string
}

// snapshotSandbox is the sandbox.snapshot handler: one generation of one
// ready sandbox, under its guard. The job itself succeeds whatever the node
// answered (the next tick schedules the next attempt); it retries only for a
// GC sweep, a busy guard or a database failure.
func (w *Worker) snapshotSandbox(ctx context.Context, job db.Job, lease *Lease) error {
	if w.objectStore == nil {
		return nil // Phase 1 mode schedules none; a stray job is a no-op
	}
	if job.SandboxID == nil {
		return permanent("missing sandbox id")
	}
	owner, sandboxID := job.OwnerID, *job.SandboxID
	sbx, err := w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		w.precondWarned.forget(sandboxID)
		return nil
	}
	if err != nil {
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if !snapshotCandidate(sbx) {
		w.precondWarned.forget(sandboxID)
		return nil
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

	// Canonical reread under the guard: a stop, resume or delete that held
	// the guard before us changed the row.
	sbx, err = w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		w.precondWarned.forget(sandboxID)
		return nil
	}
	if err != nil {
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if !snapshotCandidate(sbx) {
		w.precondWarned.forget(sandboxID)
		return nil
	}

	att := w.takeGeneration(ctx, lease, guard, opCtx, sbx)
	switch att.outcome {
	case generationCompleted:
		w.precondWarned.forget(sandboxID)
	case generationRefused:
		w.warnSnapshotRefused(sbx)
	case generationSweeping:
		return retry(att.reason, gcSweepRetry)
	case generationStoreError:
		return retry(att.reason, w.backoff(job.Attempts))
	}
	return nil
}

// snapshotCandidate reports whether the periodic worker may cut a generation
// of the sandbox: live, snapshots on, ready-ish and placed on a node.
func snapshotCandidate(sbx db.Sandbox) bool {
	return sbx.DeletedAt == nil && sbx.SnapshotsEnabled && readyState(sbx.State) &&
		derefNode(sbx.NodeID) != ""
}

// takeGeneration cuts one generation of sbx on its node. The caller holds the
// sandbox guard; opCtx is cancelled by guard loss, lease loss or shutdown and
// bounds the node call. The pending row records the caller's job and claim, so
// an attempt whose worker dies can be taken over by the next one instead of
// blocking it until the dead row's deadline. ALREADY_EXISTS (a manifest with
// that number is in the store) fails the row and retries at once with the next
// number, at most maxGenerationTries times. ctx carries the database calls.
func (w *Worker) takeGeneration(ctx context.Context, lease *Lease, guard *db.SandboxGuard, opCtx context.Context, sbx db.Sandbox) generationAttempt {
	nodeID := derefNode(sbx.NodeID)
	job := lease.Job()
	for range maxGenerationTries {
		gen, err := w.store.BeginGeneration(ctx, db.BeginGenerationParams{
			Owner: sbx.OwnerID, SandboxID: sbx.ID, Timeout: w.snapshotTimeout,
			JobID: job.ID, LeaseToken: job.LeaseToken,
		})
		switch {
		case errors.Is(err, db.ErrGCSweeping):
			return generationAttempt{outcome: generationSweeping, reason: "chunk gc sweeping"}
		case errors.Is(err, db.ErrNotFound), errors.Is(err, db.ErrSnapshotsDisabled),
			errors.Is(err, db.ErrInvalidState):
			return generationAttempt{outcome: generationSkipped, reason: "not snapshotable"}
		case errors.Is(err, db.ErrConflict):
			return generationAttempt{outcome: generationInFlight, reason: "snapshot in flight"}
		case err != nil:
			return generationAttempt{outcome: generationStoreError, reason: "generation begin failed"}
		}
		if gen.NodeID != nodeID {
			// The row names the node Begin saw; a sandbox that moved after the
			// guarded reread is not this attempt's to cut.
			w.failGeneration(ctx, gen, "node_changed", time.Now())
			return generationAttempt{outcome: generationSkipped, gen: gen, reason: "sandbox moved"}
		}

		reply, err := w.snapshotVM(opCtx, nodeID, sbx.ID, gen)
		if lease.Lost() || guard.Err() != nil || ctx.Err() != nil {
			return generationAttempt{outcome: generationAbandoned, gen: gen, reason: "attempt abandoned"}
		}
		now := time.Now()
		switch {
		case err == nil:
			res, ok := generationResult(reply, gen)
			if !ok {
				w.failGeneration(ctx, gen, "bad_reply", w.nextSnapshotAt(now))
				return generationAttempt{outcome: generationFailed, gen: gen, reason: "bad_reply"}
			}
			done, err := w.store.CompleteGeneration(ctx, gen.ID, res, w.nextSnapshotAt(now))
			switch {
			case errors.Is(err, db.ErrConflict), errors.Is(err, db.ErrNotFound):
				// The reaper failed the row past its deadline meanwhile.
				return generationAttempt{outcome: generationFailed, gen: gen, reason: "deadline"}
			case err != nil:
				return generationAttempt{outcome: generationStoreError, gen: gen, reason: "generation complete failed"}
			}
			w.markNodeObjectStoreOK(ctx, nodeID)
			return generationAttempt{outcome: generationCompleted, gen: done}
		case status.Code(err) == codes.AlreadyExists:
			w.failGeneration(ctx, gen, "exists", now)
		case status.Code(err) == codes.FailedPrecondition:
			w.failGeneration(ctx, gen, "precondition", now.Add(snapshotRefusedFactor*w.snapshotInterval))
			return generationAttempt{outcome: generationRefused, gen: gen, reason: "precondition"}
		default:
			reason := generationFailure(err)
			w.failGeneration(ctx, gen, reason, w.nextSnapshotAt(now))
			return generationAttempt{outcome: generationFailed, gen: gen, reason: reason}
		}
	}
	return generationAttempt{outcome: generationFailed, reason: "exists"}
}

// snapshotVM asks the sandbox's node for the generation's manifest. The call
// ends at the row's deadline, so a pending row past it (plus grace) has no
// live call left and the reaper may fail it.
func (w *Worker) snapshotVM(ctx context.Context, nodeID, sandboxID string, gen db.Generation) (*nodev1.Generation, error) {
	client, closer, err := w.dial(nodeID)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	cctx, cancel := context.WithDeadline(ctx, gen.DeadlineAt)
	defer cancel()
	return client.SnapshotVM(cctx, &nodev1.SnapshotVMRequest{
		SandboxId:  sandboxID,
		ChainId:    gen.ChainID.String(),
		Generation: gen.Number,
		Freeze:     new(true), // a STOPPED VM's disk is read offline either way
	})
}

// generationResult checks that the node's reply describes the row's manifest
// and converts it; a reply for another key or without a cut time is refused.
func generationResult(reply *nodev1.Generation, gen db.Generation) (db.GenerationResult, bool) {
	if reply.GetManifestKey() != gen.ManifestKey || reply.GetChainId() != gen.ChainID.String() ||
		reply.GetGeneration() != gen.Number || reply.GetCreatedAtUnixMs() <= 0 {
		return db.GenerationResult{}, false
	}
	return db.GenerationResult{
		Image:          reply.GetImage(),
		DiskBytes:      reply.GetDiskBytes(),
		Chunks:         reply.GetChunks(),
		ChunksUploaded: reply.GetChunksUploaded(),
		BytesUploaded:  reply.GetBytesUploaded(),
		ContentBytes:   reply.GetContentBytes(),
		Frozen:         reply.GetFrozen(),
		CutAt:          time.UnixMilli(reply.GetCreatedAtUnixMs()),
	}, true
}

// generationFailure maps a SnapshotVM error to the short code stored on the
// failed row. Node text never reaches the row.
func generationFailure(err error) string {
	switch status.Code(err) {
	case codes.NotFound:
		return "vm_missing"
	case codes.DeadlineExceeded:
		return "timeout"
	case codes.Unavailable:
		return "node_unavailable"
	case codes.ResourceExhausted:
		return "disk_full"
	case codes.InvalidArgument:
		return "rejected"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "failed"
}

// failGeneration fails a pending row with a fixed reason and schedules the
// sandbox's next attempt. A row another path already finished is left as is;
// a database failure leaves the row pending for the reaper.
func (w *Worker) failGeneration(ctx context.Context, gen db.Generation, reason string, next time.Time) {
	_, err := w.store.FailGeneration(ctx, gen.ID, reason, next)
	if err != nil && !errors.Is(err, db.ErrConflict) && !errors.Is(err, db.ErrNotFound) {
		w.log.Warn("generation failure not recorded", "sandbox", gen.SandboxID,
			"generation", gen.Number, "reason", classify(err))
	}
}

// nextSnapshotAt is now plus the interval, jittered by ±snapshotJitter.
func (w *Worker) nextSnapshotAt(now time.Time) time.Time {
	spread := (w.snapshotRand()*2 - 1) * snapshotJitter * float64(w.snapshotInterval)
	return now.Add(w.snapshotInterval + time.Duration(spread))
}

// warnSnapshotRefused logs a node's FAILED_PRECONDITION once per sandbox and
// node, not on every back-off.
func (w *Worker) warnSnapshotRefused(sbx db.Sandbox) {
	node := derefNode(sbx.NodeID)
	if w.precondWarned.mark(sbx.ID, node) {
		w.log.Warn("node cannot snapshot sandbox; backing off",
			"sandbox", sbx.ID, "node", node, "retry_in", snapshotRefusedFactor*w.snapshotInterval)
	}
}

// warnOnce remembers the sandbox and node pairs whose refusal was logged. It
// belongs to one Worker and is bounded: an entry goes when the sandbox
// snapshots again or leaves the ready states, and the whole map is cleared
// if it ever reaches warnOnceCap.
type warnOnce struct {
	mu   sync.Mutex
	seen map[string]string // sandbox id → node id
}

// mark records the pair and reports whether it is new.
func (o *warnOnce) mark(sandboxID, nodeID string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if node, ok := o.seen[sandboxID]; ok && node == nodeID {
		return false
	}
	if o.seen == nil {
		o.seen = make(map[string]string)
	}
	if len(o.seen) >= warnOnceCap {
		clear(o.seen)
	}
	o.seen[sandboxID] = nodeID
	return true
}

// forget drops the sandbox's entry.
func (o *warnOnce) forget(sandboxID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.seen, sandboxID)
}

// snapshotLoop enqueues due snapshot jobs at once and then every quarter
// interval (at least snapshotTickMin) until ctx ends.
func (w *Worker) snapshotLoop(ctx context.Context) {
	tick := func() {
		if _, err := w.EnqueueDueSnapshots(ctx); err != nil && ctx.Err() == nil {
			w.log.Warn("snapshot scheduling failed", "reason", classify(err))
		}
	}
	tick()
	ticker := time.NewTicker(max(w.snapshotInterval/4, snapshotTickMin))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick()
		}
	}
}

// EnqueueDueSnapshots enqueues one sandbox.snapshot job for every sandbox
// whose next generation is due and reports how many it enqueued. Without an
// object store it does nothing. Run calls it on the scheduling tick; tests
// call it directly.
func (w *Worker) EnqueueDueSnapshots(ctx context.Context) (int, error) {
	if w.objectStore == nil {
		return 0, nil
	}
	total := 0
	for {
		due, err := w.store.DueSnapshotSandboxes(ctx, time.Now(), snapshotPageSize)
		if err != nil {
			return total, fmt.Errorf("lifecycle: list due snapshots: %w", err)
		}
		enqueued := 0
		for _, sbx := range due {
			if err := ctx.Err(); err != nil {
				return total + enqueued, err
			}
			ok, err := w.store.EnqueueSnapshotJob(ctx, sbx.OwnerID, sbx.ID)
			switch {
			case errors.Is(err, db.ErrNotFound):
				continue // deleted meanwhile
			case err != nil:
				return total + enqueued, fmt.Errorf("lifecycle: enqueue snapshot of %s: %w", sbx.ID, err)
			case ok:
				enqueued++
			}
		}
		total += enqueued
		// Enqueued sandboxes leave the due set, so a full page with progress
		// is followed by the next one; no progress means the rest waits.
		if len(due) < snapshotPageSize || enqueued == 0 {
			return total, nil
		}
	}
}

// reconcilePendingGenerations fails pending rows whose deadline passed (plus
// grace): their attempt lost its lease or guard, or its worker died, and its
// node call ended at the deadline, so no completion can come. The number
// stays consumed and the sandbox is due again at once.
func (w *Worker) reconcilePendingGenerations(ctx context.Context) {
	pending, err := w.store.PendingGenerations(ctx, snapshotPageSize)
	if err != nil {
		w.log.Warn("pending generation scan failed", "reason", classify(err))
		return
	}
	grace := min(pendingGenerationGrace, w.snapshotTimeout)
	now := time.Now()
	for _, gen := range pending {
		if ctx.Err() != nil {
			return
		}
		if now.Before(gen.DeadlineAt.Add(grace)) {
			continue
		}
		w.failGeneration(ctx, gen, "deadline", now)
	}
}
