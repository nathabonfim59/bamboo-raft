package snapshots

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// ReconcilerOptions configures the bounded saved-snapshot maintenance loop.
type ReconcilerOptions struct {
	Store            Store
	Dial             lifecycle.Dial
	Logger           *slog.Logger
	Interval         time.Duration
	InventoryTimeout time.Duration
	PageSize         int
}

// Reconciler repairs durable snapshot state with bounded, restartable passes:
// failed save jobs become failed artifacts, confirmed restores release pins,
// purgeable artifacts get cleanup jobs, and node Info is compared with every
// known terminal generation (including previously purged rows that reappear).
// It never assumes DeleteSnapshot fences an earlier SaveSnapshot.
type Reconciler struct {
	store            Store
	dial             lifecycle.Dial
	log              *slog.Logger
	interval         time.Duration
	inventoryTimeout time.Duration
	pageSize         int
}

// NewReconciler validates options and returns a reconciler.
func NewReconciler(opts ReconcilerOptions) (*Reconciler, error) {
	if opts.Store == nil {
		return nil, errors.New("snapshots: NewReconciler needs a Store")
	}
	if opts.Dial == nil {
		return nil, errors.New("snapshots: NewReconciler needs a Dial function")
	}
	if opts.Logger == nil {
		return nil, errors.New("snapshots: NewReconciler needs a logger")
	}
	if opts.Interval < 0 || opts.InventoryTimeout < 0 {
		return nil, errors.New("snapshots: reconciler durations must not be negative")
	}
	pageSize := cmp.Or(opts.PageSize, DefaultPageSize)
	if pageSize < 1 || pageSize > 500 {
		return nil, errors.New("snapshots: reconciler page size must be from 1 to 500")
	}
	return &Reconciler{
		store:            opts.Store,
		dial:             opts.Dial,
		log:              opts.Logger,
		interval:         cmp.Or(opts.Interval, DefaultInterval),
		inventoryTimeout: cmp.Or(opts.InventoryTimeout, DefaultInventoryTimeout),
		pageSize:         pageSize,
	}, nil
}

// Run immediately reconciles once and then once per interval until ctx ends.
// It joins its ticker and never leaves a goroutine behind.
func (r *Reconciler) Run(ctx context.Context) error {
	r.ReconcileOnce(ctx)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.ReconcileOnce(ctx)
		}
	}
}

// ReconcileOnce runs one bounded maintenance round. Errors are fixed
// classifications logged inside; the returned error never carries raw node or
// driver text.
func (r *Reconciler) ReconcileOnce(ctx context.Context) error {
	failed := false
	if err := r.reconcileFailedSaves(ctx); err != nil {
		failed = true
		r.log.Warn("snapshot failed-save reconciliation incomplete", "reason", "reconcile incomplete")
	}
	if err := r.reconcileSaving(ctx); err != nil {
		failed = true
		r.log.Warn("snapshot saving reconciliation incomplete", "reason", "reconcile incomplete")
	}
	if err := r.releasePins(ctx); err != nil {
		failed = true
		r.log.Warn("snapshot pin reconciliation incomplete", "reason", "reconcile incomplete")
	}
	if err := r.enqueuePurgeable(ctx); err != nil {
		failed = true
		r.log.Warn("snapshot cleanup reconciliation incomplete", "reason", "reconcile incomplete")
	}
	if err := r.reconcileNodes(ctx); err != nil {
		failed = true
		r.log.Warn("snapshot node reconciliation incomplete", "reason", "reconcile incomplete")
	}
	if failed {
		return errSnapshotReconcile
	}
	return nil
}

var errSnapshotReconcile = errors.New("snapshots: reconcile incomplete")

// reconcileFailedSaves walks every terminally failed job page and fails the
// matching artifact only through the DB's terminal/missing-job proof.
func (r *Reconciler) reconcileFailedSaves(ctx context.Context) error {
	var (
		after  *db.FailedJobCursor
		failed bool
	)
	for {
		jobs, next, err := r.store.FailedJobs(ctx, after, r.pageSize)
		if err != nil {
			return err
		}
		for _, job := range jobs {
			if job.Kind != db.JobSaveSnapshot || job.SnapshotID == nil {
				continue
			}
			var payload db.SaveSnapshotPayload
			if err := json.Unmarshal([]byte(job.Payload), &payload); err != nil {
				r.log.Warn("snapshot job payload invalid", "job", job.ID)
				failed = true
				continue
			}
			if *job.SnapshotID != payload.SnapshotID {
				r.log.Warn("snapshot job binding invalid", "job", job.ID)
				failed = true
				continue
			}
			_, _, err := r.store.ReconcileSnapshotSaveFailure(ctx, db.ReconcileSnapshotSaveFailureParams{
				Owner: job.OwnerID, SnapshotID: payload.SnapshotID,
				ArtifactID: payload.ArtifactID, Generation: payload.Generation,
				Reason: "save job failed",
			})
			switch {
			case err == nil, errors.Is(err, db.ErrNotFound), errors.Is(err, db.ErrSnapshotJobActive),
				errors.Is(err, db.ErrConflict):
				// Terminal/missing proof succeeded, or another attempt owns
				// the row; either way this walk must keep moving.
			default:
				r.log.Warn("snapshot failed-save record failed", "job", job.ID, "reason", "store error")
				failed = true
			}
		}
		if next == nil {
			if failed {
				return errSnapshotReconcile
			}
			return nil
		}
		after = next
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// reconcileSaving discovers artifacts left saving without a job row. The paged
// store walk is bounded; the atomic ReconcileSnapshotSaveFailure call still
// proves every matching job is terminal or missing before writing.
func (r *Reconciler) reconcileSaving(ctx context.Context) error {
	var (
		after  *db.SnapshotCleanupCursor
		failed bool
	)
	for {
		rows, next, err := r.store.SavingSnapshots(ctx, after, r.pageSize)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if row.Status != db.SnapshotSaving {
				continue
			}
			_, _, err := r.store.ReconcileSnapshotSaveFailure(ctx, db.ReconcileSnapshotSaveFailureParams{
				Owner: row.OwnerID, SnapshotID: row.ID, ArtifactID: row.ArtifactID,
				Generation: row.Generation, Reason: "save job missing",
			})
			switch {
			case err == nil, errors.Is(err, db.ErrNotFound),
				errors.Is(err, db.ErrSnapshotJobActive), errors.Is(err, db.ErrConflict):
			default:
				r.log.Warn("snapshot orphaned-save record failed", "snapshot", row.ID, "reason", "store error")
				failed = true
			}
		}
		if next == nil {
			if failed {
				return errSnapshotReconcile
			}
			return nil
		}
		after = next
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// releasePins walks the eligible-pin keyset cursor in bounded pages. A bad
// row is skipped by the DB, so it cannot starve later rows.
func (r *Reconciler) releasePins(ctx context.Context) error {
	var after *db.SnapshotPinCursor
	for {
		_, next, err := r.store.ReleaseEligibleSnapshotPins(ctx, after, r.pageSize)
		if err != nil {
			return err
		}
		if next == nil {
			return nil
		}
		after = next
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// enqueuePurgeable walks purgeable rows in bounded pages, including rows whose
// first page already has an active cleanup job.
func (r *Reconciler) enqueuePurgeable(ctx context.Context) error {
	var (
		after  *db.SnapshotCleanupCursor
		failed bool
	)
	for {
		rows, next, err := r.store.PurgeableSnapshots(ctx, after, r.pageSize)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if _, err := r.store.EnqueueSnapshotDelete(ctx, row.OwnerID, row.ID); err != nil &&
				!errors.Is(err, db.ErrNotFound) {
				r.log.Warn("snapshot cleanup enqueue failed", "snapshot", row.ID, "reason", "store error")
				failed = true
			}
		}
		if next == nil {
			if failed {
				return errSnapshotReconcile
			}
			return nil
		}
		after = next
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// reconcileNodes compares each node's Info snapshot inventory with every known
// terminal DB generation, including purged rows. Unknown artifacts are only
// logged and preserved. A per-row repair failure marks the round incomplete.
func (r *Reconciler) reconcileNodes(ctx context.Context) error {
	nodes, err := r.store.NodeIDs(ctx)
	if err != nil {
		return err
	}
	var failed bool
	for _, nodeID := range nodes {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		info, err := r.nodeInfo(ctx, nodeID)
		if err != nil {
			r.log.Warn("snapshot node inventory failed", "node", nodeID, "reason", nodeReason(err))
			failed = true
			continue
		}
		terminal, err := r.terminalArtifacts(ctx, nodeID)
		if err != nil {
			r.log.Warn("snapshot artifact walk failed", "node", nodeID, "reason", "store error")
			failed = true
			continue
		}
		for _, snap := range info.GetSnapshots() {
			id := snap.GetSnapshotId()
			if id == "" {
				continue
			}
			// Node-supplied ids are only trusted as inventory keys after
			// the server-generated shape check; an invalid one is logged
			// as a fixed classification, never echoed.
			if !db.ValidSnapshotArtifactID(id) {
				r.log.Warn("snapshot node reported an invalid artifact id", "node", nodeID)
				failed = true
				continue
			}
			row, known := terminal[id]
			if known {
				if snap.GetSourceSandboxId() != "" && snap.GetSourceSandboxId() != row.SandboxID {
					r.log.Warn("snapshot artifact source mismatch preserved", "node", nodeID, "snapshot", row.ID)
					continue
				}
				if row.PurgedAt != nil {
					_, changed, err := r.store.ReopenSnapshotCleanup(ctx, row.OwnerID, row.ID, row.ArtifactID)
					if err != nil {
						r.log.Warn("snapshot reopen failed", "node", nodeID, "snapshot", row.ID, "reason", "store error")
						failed = true
						continue
					}
					if !changed {
						continue
					}
				}
				if _, err := r.store.EnqueueSnapshotDelete(ctx, row.OwnerID, row.ID); err != nil &&
					!errors.Is(err, db.ErrNotFound) {
					r.log.Warn("snapshot reappeared enqueue failed", "node", nodeID, "snapshot", row.ID, "reason", "store error")
					failed = true
				}
				continue
			}
			if _, ok, err := r.store.SavedSnapshotByArtifact(ctx, id); err != nil {
				r.log.Warn("snapshot artifact lookup failed", "node", nodeID, "reason", "store error")
				failed = true
			} else if ok {
				// A live/current generation: node inventory is expected.
				continue
			} else {
				// Unknown but well-formed: preserve it. The id is not
				// echoed because the node supplied it.
				r.log.Warn("unknown snapshot artifact preserved", "node", nodeID)
			}
		}
	}
	if failed {
		return errSnapshotReconcile
	}
	return nil
}

// terminalArtifacts pages all known terminal generations recorded on nodeID.
func (r *Reconciler) terminalArtifacts(ctx context.Context, nodeID string) (map[string]db.SavedSnapshot, error) {
	out := make(map[string]db.SavedSnapshot)
	var after *db.SnapshotCleanupCursor
	for {
		rows, next, err := r.store.CleanupArtifactsOnNode(ctx, nodeID, after, r.pageSize)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			out[row.ArtifactID] = row
		}
		if next == nil {
			return out, nil
		}
		after = next
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
}

// nodeInfo asks one gateway-bound node for Info under a bounded context. A nil
// client or closer must not panic and a closer is always closed once.
func (r *Reconciler) nodeInfo(ctx context.Context, nodeID string) (*nodev1.InfoResponse, error) {
	client, closeClient, err := dialNode(r.dial, nodeID)
	if err != nil {
		return nil, err
	}
	defer closeClient()
	callCtx, cancel := context.WithTimeout(ctx, r.inventoryTimeout)
	defer cancel()
	info, err := client.Info(callCtx, &nodev1.InfoRequest{})
	if err != nil {
		return nil, err
	}
	if info.GetNodeId() != nodeID {
		return nil, errors.New("snapshot inventory came from a different node")
	}
	return info, nil
}

// nodeReason maps a node/gateway failure to a fixed log classification.
func nodeReason(err error) string {
	switch status.Code(err) {
	case codes.Unavailable:
		return "node unavailable"
	case codes.DeadlineExceeded:
		return "node timeout"
	case codes.Unauthenticated, codes.PermissionDenied:
		return "gateway unauthorized"
	case codes.NotFound:
		return "node missing"
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "inventory timeout"
	case errors.Is(err, context.Canceled):
		return "inventory cancelled"
	default:
		return "inventory failed"
	}
}
