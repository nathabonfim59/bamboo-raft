package lifecycle

// Manifest restores on several nodes: a node that refuses a
// from_manifest boot with FAILED_PRECONDITION is marked without an object
// store (when it has the manifest's image) and the boot moves to another
// placed node, keeping the sandbox id and the job's source generation.

import (
	"context"
	"errors"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// errNoReadyMachine ends a manifest restore that no node can take: the
// refusing node was the only candidate, or no Placer is configured. It is
// terminal (sandbox error no_ready_machine), never retried.
var errNoReadyMachine = errors.New("lifecycle: no node can restore the manifest")

// bootFailureReason is the fixed sandbox error_reason of a terminal boot
// failure. A manifest restore's NOT_FOUND means the manifest is gone and
// INVALID_ARGUMENT that the node rejected the restore request (the disk
// smaller than the manifest's, an image mismatch).
func bootFailureReason(sbx db.Sandbox, err error) string {
	if errors.Is(err, errNoReadyMachine) {
		return "no_ready_machine"
	}
	if sbx.FromManifestKey != "" {
		switch status.Code(err) {
		case codes.NotFound:
			return "snapshot_not_found"
		case codes.InvalidArgument:
			return "restore_rejected"
		}
	}
	return terminalProvisionReason(err)
}

// manifestRefused handles FAILED_PRECONDITION from a from_manifest CreateVM:
// the node either has no object store or lacks the manifest's image. Only the
// first marks the node (its live Info still lists the image). Either way the
// boot is placed again elsewhere: the partial VM is deleted under the guard,
// then [Store.ReadmitSandbox] moves the reservation, row and a new provision
// job to the first candidate that still has room, and settles this job in
// the same fenced transaction. No candidate is a terminal no_ready_machine.
func (w *Worker) manifestRefused(ctx context.Context, job db.Job, lease *Lease, guard *db.SandboxGuard,
	opCtx context.Context, sbx db.Sandbox, p db.ProvisionPayload, client NodeClient,
) error {
	node := derefNode(sbx.NodeID)
	if w.nodeHasImage(opCtx, client, p.Image) {
		w.log.Warn("node cannot restore from the object store", "node", node, "sandbox", sbx.ID)
		if err := w.store.MarkNodeObjectStore(ctx, node, false); err != nil {
			w.log.Warn("node object store mark failed", "node", node, "reason", classify(err))
		}
	}
	if w.placer == nil {
		return w.bootFailed(ctx, job, lease, guard, opCtx, sbx, errNoReadyMachine)
	}
	// CreateVM refused before booting, but a redelivered attempt may have
	// left a partial VM: the reservation only moves once the node confirmed
	// none is left.
	if err := w.deleteVMUnderGuard(opCtx, guard, sbx); err != nil {
		if guard.Err() != nil {
			return w.afterGuardLoss(ctx, lease, guard, sbx)
		}
		return retry("refused boot cleanup failed", w.backoff(job.Attempts))
	}
	cands, err := w.placer.Place(opCtx, scheduler.PlaceRequest{
		Reserve:         sbx.Reserved,
		Image:           p.Image,
		Exclude:         []string{node},
		NeedObjectStore: true,
	})
	switch {
	case errors.Is(err, scheduler.ErrNoCapacity), errors.Is(err, scheduler.ErrNoNode):
		if lease.Lost() || ctx.Err() != nil {
			return nil
		}
		return w.bootFailed(ctx, job, lease, guard, opCtx, sbx, errNoReadyMachine)
	case err != nil:
		return retry("placement failed", w.backoff(job.Attempts))
	}
	for _, c := range cands {
		if guard.Err() != nil {
			return w.afterGuardLoss(ctx, lease, guard, sbx)
		}
		moved, next, err := w.store.ReadmitSandbox(ctx, job.ID, w.workerID, job.LeaseToken, db.ReadmitParams{
			Owner:           job.OwnerID,
			SandboxID:       sbx.ID,
			ExpectedVersion: sbx.Version,
			FromNode:        node,
			ToNode:          c.Node.ID,
		})
		switch {
		case err == nil:
			w.log.Info("manifest restore placed again", "sandbox", moved.ID, "from", node, "to", c.Node.ID, "job", next.ID)
			return nil
		case errors.Is(err, db.ErrNoCapacity):
			continue // another create took the room; try the next candidate
		case errors.Is(err, db.ErrNotFound):
			// Either the candidate's node row vanished or the sandbox was
			// deleted meanwhile; the reread tells them apart.
			if cur, rerr := w.store.SandboxAny(ctx, job.OwnerID, sbx.ID); rerr == nil && cur.DeletedAt != nil {
				return w.compensateDeleted(opCtx, cur)
			}
			continue
		case errors.Is(err, db.ErrJobNotLeased):
			return nil
		case errors.Is(err, db.ErrConflict):
			// A stop or delete moved the row while this attempt placed it:
			// the redelivered attempt rereads and decides again.
			return retry("sandbox changed during placement", w.backoff(job.Attempts))
		default:
			return retry("readmit failed", w.backoff(job.Attempts))
		}
	}
	return w.bootFailed(ctx, job, lease, guard, opCtx, sbx, errNoReadyMachine)
}

// nodeHasImage asks the node's live inventory whether it lists image. An
// unanswered probe proves nothing and reports false, so a node is never
// marked without an object store on a guess.
func (w *Worker) nodeHasImage(ctx context.Context, client NodeClient, image string) bool {
	if image == "" {
		return false
	}
	ictx, cancel := context.WithTimeout(ctx, w.cleanupTimeout)
	defer cancel()
	info, err := client.Info(ictx, &nodev1.InfoRequest{})
	if err != nil {
		return false
	}
	return slices.ContainsFunc(info.GetImages(), func(img *nodev1.Image) bool { return img.GetId() == image })
}

// markObjectStore records, best effort, that the sandbox's node just
// restored a manifest from the object store, so placement stops skipping it.
func (w *Worker) markObjectStore(ctx context.Context, sbx db.Sandbox) {
	if sbx.FromManifestKey != "" {
		w.markNodeObjectStoreOK(ctx, derefNode(sbx.NodeID))
	}
}

// markNodeObjectStoreOK records, best effort, that nodeID just read or wrote
// the object store (a manifest restore or a SnapshotVM succeeded).
func (w *Worker) markNodeObjectStoreOK(ctx context.Context, nodeID string) {
	if err := w.store.MarkNodeObjectStore(ctx, nodeID, true); err != nil && !errors.Is(err, db.ErrNotFound) {
		w.log.Warn("node object store mark failed", "node", nodeID, "reason", classify(err))
	}
}
