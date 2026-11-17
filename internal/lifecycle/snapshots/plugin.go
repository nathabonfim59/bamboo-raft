// Package snapshots implements the durable saved-snapshot job plugin. The
// server freezes its handler map at construction (Handlers) and registers it
// in lifecycle.Options.Handlers; the reconciler runs beside the lifecycle
// role. It never assumes DeleteSnapshot fences an earlier SaveSnapshot: lease
// fencing and repeated node-inventory reconciliation converge instead.
package snapshots

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"maps"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Defaults bound each node RPC and retry schedule.
const (
	DefaultSaveTimeout      = 30 * time.Minute
	DefaultDeleteTimeout    = 30 * time.Second
	DefaultInventoryTimeout = 10 * time.Second
	DefaultRetryBase        = 5 * time.Second
	DefaultMaxBackoff       = 2 * time.Minute
	DefaultInterval         = time.Minute
	DefaultPageSize         = 100
)

// Store is the database surface the plugin consumes. *db.Store implements it.
type Store interface {
	SavedSnapshot(ctx context.Context, owner uuid.UUID, id uuid.UUID) (db.SavedSnapshot, error)
	SandboxAny(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error)
	AcquireSandboxGuard(ctx context.Context, owner uuid.UUID, sandboxID string) (*db.SandboxGuard, error)

	CompleteSnapshotSave(ctx context.Context, p db.CompleteSnapshotSaveParams) (db.SavedSnapshot, bool, error)
	FailSnapshotSave(ctx context.Context, p db.FailSnapshotSaveParams) (db.SavedSnapshot, bool, error)
	ReconcileSnapshotSaveFailure(ctx context.Context, p db.ReconcileSnapshotSaveFailureParams) (db.SavedSnapshot, bool, error)
	FailedJobs(ctx context.Context, after *db.FailedJobCursor, limit int) ([]db.Job, *db.FailedJobCursor, error)
	// SavingSnapshots is required: the reconciler must discover a saving
	// generation whose save job row is missing, not only terminally failed
	// jobs. It is never an optional capability.
	SavingSnapshots(ctx context.Context, after *db.SnapshotCleanupCursor, limit int) ([]db.SavedSnapshot, *db.SnapshotCleanupCursor, error)

	PurgeableSnapshots(ctx context.Context, after *db.SnapshotCleanupCursor, limit int) ([]db.SavedSnapshot, *db.SnapshotCleanupCursor, error)
	CleanupArtifactsOnNode(ctx context.Context, nodeID string, after *db.SnapshotCleanupCursor, limit int) ([]db.SavedSnapshot, *db.SnapshotCleanupCursor, error)
	SavedSnapshotByArtifact(ctx context.Context, artifactID string) (db.SavedSnapshot, bool, error)
	ReopenSnapshotCleanup(ctx context.Context, owner uuid.UUID, snapshotID uuid.UUID, artifactID string) (db.SavedSnapshot, bool, error)
	EnqueueSnapshotDelete(ctx context.Context, owner uuid.UUID, snapshotID uuid.UUID) (bool, error)
	MarkSnapshotPurged(ctx context.Context, owner uuid.UUID, snapshotID uuid.UUID, artifactID string) (db.SavedSnapshot, error)
	ReleaseEligibleSnapshotPins(ctx context.Context, after *db.SnapshotPinCursor, limit int) (int, *db.SnapshotPinCursor, error)
	NodeIDs(ctx context.Context) ([]string, error)
}

var _ Store = (*db.Store)(nil)

// Options configures the frozen job plugin.
type Options struct {
	Store            Store
	Dial             lifecycle.Dial
	Logger           *slog.Logger
	SaveTimeout      time.Duration
	DeleteTimeout    time.Duration
	InventoryTimeout time.Duration
	RetryBase        time.Duration
	MaxBackoff       time.Duration
}

// Plugin owns the immutable saved-snapshot handlers.
type Plugin struct {
	store            Store
	dial             lifecycle.Dial
	log              *slog.Logger
	saveTimeout      time.Duration
	deleteTimeout    time.Duration
	inventoryTimeout time.Duration
	retryBase        time.Duration
	maxBackoff       time.Duration
	handlers         map[db.JobKind]lifecycle.Handler
}

// New validates options and freezes the handler map.
func New(opts Options) (*Plugin, error) {
	if opts.Store == nil {
		return nil, errors.New("snapshots: New needs a Store")
	}
	if opts.Dial == nil {
		return nil, errors.New("snapshots: New needs a Dial function")
	}
	if opts.Logger == nil {
		return nil, errors.New("snapshots: New needs a logger")
	}
	if opts.SaveTimeout < 0 || opts.DeleteTimeout < 0 || opts.InventoryTimeout < 0 ||
		opts.RetryBase < 0 || opts.MaxBackoff < 0 {
		return nil, errors.New("snapshots: durations must not be negative")
	}
	p := &Plugin{
		store:            opts.Store,
		dial:             opts.Dial,
		log:              opts.Logger,
		saveTimeout:      cmp.Or(opts.SaveTimeout, DefaultSaveTimeout),
		deleteTimeout:    cmp.Or(opts.DeleteTimeout, DefaultDeleteTimeout),
		inventoryTimeout: cmp.Or(opts.InventoryTimeout, DefaultInventoryTimeout),
		retryBase:        cmp.Or(opts.RetryBase, DefaultRetryBase),
		maxBackoff:       cmp.Or(opts.MaxBackoff, DefaultMaxBackoff),
	}
	p.handlers = map[db.JobKind]lifecycle.Handler{
		db.JobSaveSnapshot:   p.save,
		db.JobDeleteSnapshot: p.delete,
	}
	return p, nil
}

// Handlers returns a fresh copy of the immutable handler map for
// lifecycle.Options.Handlers. The plugin never mutates its own map.
func (p *Plugin) Handlers() map[db.JobKind]lifecycle.Handler {
	return maps.Clone(p.handlers)
}

// validateSaveArtifactJob binds one save job to the immutable artifact row:
// job snapshot id, owner, source sandbox, VMID, node, artifact id and
// generation must all agree. The artifact id must also pass the server's
// exact shape check, so no node RPC can run for a tampered row even when the
// payload and row agree.
func validateSaveArtifactJob(job db.Job, payload db.SaveSnapshotPayload, snap db.SavedSnapshot) error {
	if !db.ValidSnapshotArtifactID(payload.ArtifactID) || !db.ValidSnapshotArtifactID(snap.ArtifactID) {
		return errors.New("snapshot artifact id is not canonical")
	}
	if job.SnapshotID == nil || *job.SnapshotID != payload.SnapshotID {
		return errors.New("snapshot job id mismatch")
	}
	if job.SandboxID == nil || *job.SandboxID == "" || payload.VMID != *job.SandboxID {
		return errors.New("snapshot job sandbox mismatch")
	}
	if snap.ID != payload.SnapshotID || snap.OwnerID != job.OwnerID || snap.SandboxID != *job.SandboxID {
		return errors.New("snapshot row identity mismatch")
	}
	if snap.ArtifactID != payload.ArtifactID || snap.Generation != payload.Generation {
		return errors.New("snapshot row generation mismatch")
	}
	if snap.NodeID == nil || *snap.NodeID != payload.NodeID {
		return errors.New("snapshot row node mismatch")
	}
	return nil
}

// validateSaveSourceJob binds the source sandbox to the same owner and node.
func validateSaveSourceJob(job db.Job, payload db.SaveSnapshotPayload, sbx db.Sandbox) error {
	if job.SandboxID == nil || sbx.ID != *job.SandboxID || sbx.OwnerID != job.OwnerID {
		return errors.New("source sandbox identity mismatch")
	}
	if sbx.NodeID == nil || *sbx.NodeID != payload.NodeID {
		return errors.New("source sandbox node mismatch")
	}
	return nil
}

// validateDeleteJob binds one cleanup job to the immutable artifact row and
// its recorded node. An unsafe artifact id is rejected before any DeleteSnapshot
// RPC even when the payload and row agree.
func validateDeleteJob(job db.Job, payload db.DeleteSnapshotPayload, row db.SavedSnapshot) error {
	if !db.ValidSnapshotArtifactID(payload.ArtifactID) || !db.ValidSnapshotArtifactID(row.ArtifactID) {
		return errors.New("snapshot artifact id is not canonical")
	}
	if job.SnapshotID == nil || *job.SnapshotID != payload.SnapshotID || row.ID != payload.SnapshotID {
		return errors.New("delete job id mismatch")
	}
	if row.OwnerID != job.OwnerID {
		return errors.New("delete job owner mismatch")
	}
	if job.SandboxID == nil || row.SandboxID != *job.SandboxID {
		return errors.New("delete job sandbox mismatch")
	}
	if row.ArtifactID != payload.ArtifactID {
		return errors.New("delete job artifact mismatch")
	}
	if row.NodeID == nil || *row.NodeID != payload.NodeID {
		return errors.New("delete job node mismatch")
	}
	return nil
}

// save runs one snapshot.save job under its live lease and the per-sandbox
// guard. It never writes the artifact row after losing the claim.
func (p *Plugin) save(ctx context.Context, job db.Job, lease *lifecycle.Lease) error {
	if job.SandboxID == nil {
		return permanent("missing sandbox id")
	}
	var payload db.SaveSnapshotPayload
	if err := json.Unmarshal([]byte(job.Payload), &payload); err != nil {
		return permanent("invalid save payload")
	}
	snap, err := p.store.SavedSnapshot(ctx, job.OwnerID, payload.SnapshotID)
	if errors.Is(err, db.ErrNotFound) {
		return permanent("snapshot missing")
	}
	if err != nil {
		return retry("snapshot read failed", p.backoff(job.Attempts))
	}
	if err := validateSaveArtifactJob(job, payload, snap); err != nil {
		return permanent("snapshot job does not match artifact")
	}
	if snap.Status != db.SnapshotSaving {
		return nil // already done or terminally failed
	}

	sbx, err := p.store.SandboxAny(ctx, job.OwnerID, *job.SandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return p.failSnapshot(ctx, job, snap, "source missing")
	}
	if err != nil {
		return retry("source read failed", p.backoff(job.Attempts))
	}
	if err := validateSaveSourceJob(job, payload, sbx); err != nil {
		return permanent("source sandbox does not match artifact")
	}
	if sbx.DeletedAt != nil {
		return p.failSnapshot(ctx, job, snap, "source deleted")
	}
	if !readySourceState(sbx.State) {
		return p.failSnapshot(ctx, job, snap, "source not ready")
	}

	guard, opCtx, stop, err := p.acquireGuard(lease, job.OwnerID, sbx.ID)
	if err != nil {
		if lease.Lost() || ctx.Err() != nil {
			return nil
		}
		return retry("sandbox guard unavailable", p.backoff(job.Attempts))
	}
	defer func() {
		stop()
		guard.Release(ctx)
	}()

	// Canonical recheck under the guard: deletion or another save may have
	// changed the row between admission and this attempt.
	snap, err = p.store.SavedSnapshot(ctx, job.OwnerID, payload.SnapshotID)
	if errors.Is(err, db.ErrNotFound) {
		return nil
	}
	if err != nil {
		return retry("snapshot read failed", p.backoff(job.Attempts))
	}
	if snap.Status != db.SnapshotSaving {
		return nil
	}
	if err := validateSaveArtifactJob(job, payload, snap); err != nil {
		return permanent("snapshot job does not match artifact")
	}
	sbx, err = p.store.SandboxAny(ctx, job.OwnerID, *job.SandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return p.failSnapshot(ctx, job, snap, "source missing")
	}
	if err != nil {
		return retry("source read failed", p.backoff(job.Attempts))
	}
	if err := validateSaveSourceJob(job, payload, sbx); err != nil {
		return permanent("source sandbox does not match artifact")
	}
	if sbx.DeletedAt != nil {
		return p.failSnapshot(ctx, job, snap, "source deleted")
	}
	if !readySourceState(sbx.State) {
		return p.failSnapshot(ctx, job, snap, "source not ready")
	}

	client, closeClient, err := dialNode(p.dial, payload.NodeID)
	if err != nil {
		return retry("node gateway unavailable", p.backoff(job.Attempts))
	}
	defer closeClient()

	size, err := p.saveOnNode(opCtx, client, sbx.ID, snap.ArtifactID, payload.NodeID)
	if guard.Err() != nil {
		if lease.Lost() || ctx.Err() != nil {
			return nil
		}
		// The guard is gone but the claim may still be live: this attempt
		// cannot prove the node result, so redeliver rather than mark the
		// generation ready.
		return retry("sandbox guard lost", p.backoff(job.Attempts))
	}
	if lease.Lost() || ctx.Err() != nil {
		// The claim or worker died while the RPC ran. Never switch the row;
		// redelivery or reconciliation owns the outcome.
		return nil
	}
	if err != nil {
		if terminalSaveError(err) {
			return p.failSnapshot(ctx, job, snap, saveReason(err))
		}
		return retry(saveReason(err), p.backoff(job.Attempts))
	}

	_, _, err = p.store.CompleteSnapshotSave(ctx, db.CompleteSnapshotSaveParams{
		Owner:      job.OwnerID,
		SnapshotID: snap.ID,
		ArtifactID: snap.ArtifactID,
		Generation: snap.Generation,
		NodeID:     payload.NodeID,
		SizeBytes:  &size,
		JobID:      job.ID,
		Worker:     job.LeasedBy,
		Token:      job.LeaseToken,
	})
	switch {
	case errors.Is(err, db.ErrJobNotLeased):
		return nil
	case err != nil:
		return retry("snapshot completion failed", p.backoff(job.Attempts))
	default:
		return nil
	}
}

// saveOnNode calls SaveSnapshot, resolving the one idempotent collision case
// through Info. Any other AlreadyExists is an unexplained collision and is
// terminal for this artifact.
func (p *Plugin) saveOnNode(ctx context.Context, client lifecycle.NodeClient, sandboxID, artifactID, nodeID string) (int64, error) {
	callCtx, cancel := context.WithTimeout(ctx, p.saveTimeout)
	defer cancel()
	snap, err := client.SaveSnapshot(callCtx, &nodev1.SaveSnapshotRequest{
		SandboxId: sandboxID, SnapshotId: artifactID,
	})
	if err == nil {
		if snap.GetSnapshotId() != artifactID || snap.GetSourceSandboxId() != sandboxID {
			return 0, status.Error(codes.Internal, "SaveSnapshot returned a different artifact")
		}
		if snap.GetSizeBytes() < 0 {
			return 0, status.Error(codes.Internal, "SaveSnapshot returned a negative size")
		}
		return snap.GetSizeBytes(), nil
	}
	if status.Code(err) != codes.AlreadyExists {
		return 0, err
	}
	infoCtx, infoCancel := context.WithTimeout(ctx, p.inventoryTimeout)
	defer infoCancel()
	info, infoErr := client.Info(infoCtx, &nodev1.InfoRequest{})
	if infoErr != nil {
		return 0, infoErr
	}
	if info.GetNodeId() != nodeID {
		return 0, status.Error(codes.Internal, "Info came from a different node")
	}
	for _, known := range info.GetSnapshots() {
		if known.GetSnapshotId() == artifactID && known.GetSourceSandboxId() == sandboxID {
			if known.GetSizeBytes() < 0 {
				return 0, status.Error(codes.Internal, "Info returned a negative snapshot size")
			}
			return known.GetSizeBytes(), nil
		}
	}
	return 0, status.Error(codes.AlreadyExists, "snapshot id collision")
}

// delete runs one snapshot.delete job. It re-reads the immutable row, refuses
// to delete while a restore pin exists or the row is not terminal, and only
// calls DeleteSnapshot on the recorded node. DeleteSnapshot is idempotent, so
// retries are safe; it is not treated as a fence for a late SaveSnapshot.
func (p *Plugin) delete(ctx context.Context, job db.Job, lease *lifecycle.Lease) error {
	var payload db.DeleteSnapshotPayload
	if err := json.Unmarshal([]byte(job.Payload), &payload); err != nil {
		return permanent("invalid delete payload")
	}
	if job.SnapshotID != nil && *job.SnapshotID != payload.SnapshotID {
		return permanent("job snapshot mismatch")
	}
	row, err := p.store.SavedSnapshot(ctx, job.OwnerID, payload.SnapshotID)
	if errors.Is(err, db.ErrNotFound) {
		return nil // nothing durable; never guess an artifact
	}
	if err != nil {
		return retry("snapshot read failed", p.backoff(job.Attempts))
	}
	if err := validateDeleteJob(job, payload, row); err != nil {
		return permanent("delete job does not match artifact")
	}
	if row.PurgedAt != nil || row.Pins > 0 ||
		(row.DeletedAt == nil && row.RetiredAt == nil && row.Status != db.SnapshotFailed) {
		return nil // not immutable/purgeable right now
	}

	client, closeClient, err := dialNode(p.dial, payload.NodeID)
	if err != nil {
		return retry("node gateway unavailable", p.backoff(job.Attempts))
	}
	defer closeClient()
	callCtx, cancel := context.WithTimeout(lease.Context(), p.deleteTimeout)
	_, err = client.DeleteSnapshot(callCtx, &nodev1.DeleteSnapshotRequest{SnapshotId: row.ArtifactID})
	cancel()
	if lease.Lost() || ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return retry(deleteReason(err), p.backoff(job.Attempts))
	}
	if _, err := p.store.MarkSnapshotPurged(ctx, job.OwnerID, row.ID, row.ArtifactID); err != nil {
		switch {
		case errors.Is(err, db.ErrNotFound), errors.Is(err, db.ErrSnapshotNotPurgeable):
			return nil
		default:
			return retry("snapshot purge record failed", p.backoff(job.Attempts))
		}
	}
	return nil
}

// failSnapshot records a terminal artifact failure under the live claim. If
// the claim was lost it does nothing; the redelivered attempt or reconciler
// owns the outcome.
func (p *Plugin) failSnapshot(ctx context.Context, job db.Job, snap db.SavedSnapshot, reason string) error {
	_, _, err := p.store.FailSnapshotSave(ctx, db.FailSnapshotSaveParams{
		Owner:      job.OwnerID,
		SnapshotID: snap.ID,
		ArtifactID: snap.ArtifactID,
		Generation: snap.Generation,
		Reason:     reason,
		JobID:      job.ID,
		Worker:     job.LeasedBy,
		Token:      job.LeaseToken,
	})
	switch {
	case errors.Is(err, db.ErrJobNotLeased):
		return nil
	case err != nil:
		return retry("snapshot failure record failed", p.backoff(job.Attempts))
	default:
		return permanent(reason)
	}
}

// acquireGuard waits a bounded time for the per-sandbox advisory guard and
// builds an operation context cancelled by lease loss, worker shutdown or
// guard loss. stop joins the watcher.
func (p *Plugin) acquireGuard(lease *lifecycle.Lease,
	owner uuid.UUID, sandboxID string,
) (*db.SandboxGuard, context.Context, context.CancelFunc, error) {
	waitCtx, cancel := context.WithTimeout(lease.Context(), p.inventoryTimeout)
	defer cancel()
	guard, err := p.store.AcquireSandboxGuard(waitCtx, owner, sandboxID)
	if err != nil {
		return nil, nil, nil, err
	}
	opCtx, opCancel := context.WithCancelCause(lease.Context())
	stopCh := make(chan struct{})
	var (
		once sync.Once
		wg   sync.WaitGroup
	)
	wg.Go(func() {
		select {
		case <-guard.Done():
			opCancel(db.ErrSandboxGuardLost)
		case <-stopCh:
		}
	})
	stop := func() {
		once.Do(func() {
			close(stopCh)
			wg.Wait()
			opCancel(nil)
		})
	}
	return guard, opCtx, stop, nil
}

// backoff is bounded exponential backoff for a job attempt.
func (p *Plugin) backoff(attempt int) time.Duration {
	d := p.retryBase
	for range max(attempt-1, 0) {
		if d >= p.maxBackoff/2 {
			return p.maxBackoff
		}
		d *= 2
	}
	return min(d, p.maxBackoff)
}

// readySourceState reports the live states a save may start from.
func readySourceState(state db.SandboxState) bool {
	switch state {
	case db.StateReady, db.StateIdle, db.StateRunning:
		return true
	default:
		return false
	}
}

// terminalSaveError reports node failures that retrying cannot cure.
func terminalSaveError(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.Unimplemented,
		codes.PermissionDenied, codes.Unauthenticated, codes.AlreadyExists:
		return true
	default:
		return false
	}
}

// saveReason is a safe fixed classification for logs, job causes and the
// artifact error field.
func saveReason(err error) string {
	switch status.Code(err) {
	case codes.Unavailable:
		return "node unavailable"
	case codes.DeadlineExceeded:
		return "node timeout"
	case codes.InvalidArgument:
		return "node rejected save"
	case codes.NotFound:
		return "node source not found"
	case codes.AlreadyExists:
		return "snapshot id collision"
	case codes.PermissionDenied, codes.Unauthenticated:
		return "node authorization failed"
	case codes.Unimplemented:
		return "node does not support snapshots"
	}
	return "snapshot save failed"
}

// deleteReason is a safe fixed classification for cleanup retries.
func deleteReason(err error) string {
	switch status.Code(err) {
	case codes.Unavailable:
		return "node unavailable"
	case codes.DeadlineExceeded:
		return "node timeout"
	case codes.InvalidArgument:
		return "node rejected delete"
	case codes.PermissionDenied, codes.Unauthenticated:
		return "node authorization failed"
	}
	return "snapshot delete failed"
}

// dialNode guards the optional Dial factories: a nil client, a closer returned
// with an error, or a nil closer must not panic, and any non-nil closer is
// closed exactly once.
func dialNode(dial lifecycle.Dial, nodeID string) (lifecycle.NodeClient, func(), error) {
	client, closer, err := dial(nodeID)
	closeOnce := func() {
		if closer != nil {
			_ = closer.Close() //nolint:errcheck // close failures are not actionable here
		}
	}
	if err != nil {
		closeOnce()
		return nil, func() {}, err
	}
	if client == nil {
		closeOnce()
		return nil, func() {}, errors.New("node gateway returned a nil client")
	}
	closed := false
	return client, func() {
		if !closed {
			closed = true
			closeOnce()
		}
	}, nil
}

// permanent and retry are the lifecycle plugin's terminal/retry outcomes.
func permanent(reason string) *lifecycle.RetryError {
	return &lifecycle.RetryError{Reason: reason, Permanent: true}
}

func retry(reason string, backoff time.Duration) *lifecycle.RetryError {
	return &lifecycle.RetryError{Reason: reason, Backoff: backoff}
}
