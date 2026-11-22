package lifecycle

// Node loss. With an object store configured, a node not observed connected
// for the grace (Options.NodeLostAfter) is lost, and what it held continues
// elsewhere from the object store:
//
//   - a ready-ish sandbox with a completed generation is re-placed on another
//     node, and the cross-node resume job restores that generation there;
//   - a ready-ish sandbox with nothing to restore from becomes error
//     node_lost;
//   - an archived sandbox whose disk is on the node, and a stop in flight on
//     it, keep only their last generation in the store (none: left as is).
//
// Three guards keep a short outage, a restart or a blind observer from
// moving anything: the round does nothing until this worker has run for the
// grace (nodes reconnect after a server restart), a node counts as lost only
// while another node was observed within the grace (the store's check: with
// every node stale the inventory poller itself may be down), and a node that
// still answers a live Info is never lost. Every decision is taken under the
// sandbox guard, so no snapshot, stop or resume call is in flight for the
// sandbox, and committed by a store transaction that rechecks the row and the
// node. The copy on the lost node is recorded as an eviction, which VM
// reconciliation deletes first when the node returns.

import (
	"context"
	"errors"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// nodeLostProbeTimeout bounds the live Info that can clear a node the
// inventory reports as gone.
const nodeLostProbeTimeout = 3 * time.Second

// reapLostNodes runs node-loss detection for one reaper round. Without an
// object store it does nothing: nothing can continue elsewhere, and the
// node's disk is the only copy of every sandbox on it.
func (w *Worker) reapLostNodes(ctx context.Context) {
	if w.objectStore == nil || w.nodeLostAfter <= 0 || time.Since(w.started) < w.nodeLostAfter {
		return
	}
	cutoff := time.Now().Add(-w.nodeLostAfter)
	nodes, err := w.store.LostNodes(ctx, cutoff)
	if err != nil {
		w.log.Warn("lost node scan failed", "reason", classify(err))
		return
	}
	for _, nodeID := range nodes {
		if ctx.Err() != nil {
			return
		}
		rows, err := w.store.SandboxesOnLostNode(ctx, nodeID)
		if err != nil {
			w.log.Warn("lost node sandbox scan failed", "node", nodeID, "reason", classify(err))
			continue
		}
		if len(rows) == 0 {
			continue // a node that holds nothing needs no probe
		}
		if w.nodeAnswers(ctx, nodeID) {
			w.log.Warn("node inventory is stale but the node answers; not treated as lost", "node", nodeID)
			continue
		}
		for _, sbx := range rows {
			if ctx.Err() != nil {
				return
			}
			w.reapLostSandbox(ctx, nodeID, cutoff, sbx)
		}
	}
}

// nodeAnswers reports whether nodeID answers a live Info as itself.
func (w *Worker) nodeAnswers(ctx context.Context, nodeID string) bool {
	client, closer, err := w.dial(nodeID)
	if err != nil {
		return false
	}
	defer closer.Close()
	pctx, cancel := context.WithTimeout(ctx, nodeLostProbeTimeout)
	defer cancel()
	info, err := client.Info(pctx, &nodev1.InfoRequest{})
	return err == nil && info.GetNodeId() == nodeID
}

// reapLostSandbox decides one sandbox of the lost node under its guard. A
// busy guard leaves the sandbox for the next round.
func (w *Worker) reapLostSandbox(ctx context.Context, nodeID string, cutoff time.Time, listed db.Sandbox) {
	guard, opCtx, stop, err := w.acquireReapGuard(ctx, listed.OwnerID, listed.ID)
	if err != nil {
		return
	}
	defer func() {
		stop()
		guard.Release(ctx)
	}()
	sbx, err := w.store.SandboxAny(ctx, listed.OwnerID, listed.ID)
	if err != nil || sbx.DeletedAt != nil {
		return
	}
	var (
		gen    db.Generation
		hasGen bool
	)
	if sbx.SnapshotsEnabled && sbx.DataExpiredAt == nil {
		gen, hasGen, err = w.store.LatestGeneration(ctx, sbx.OwnerID, sbx.ID)
		if err != nil {
			w.log.Warn("lost node generation read failed", "sandbox", sbx.ID, "reason", classify(err))
			return
		}
	}
	p := db.NodeLossParams{
		Owner: sbx.OwnerID, SandboxID: sbx.ID, LostNode: nodeID, LostBefore: cutoff, Version: sbx.Version,
	}
	onNode := derefNode(sbx.NodeID) == nodeID
	switch {
	case readyState(sbx.State) && onNode && hasGen:
		w.moveLostSandbox(ctx, opCtx, sbx, gen, p)
	case readyState(sbx.State) && onNode:
		_, err = w.store.FailLostSandbox(ctx, p)
		w.lostOutcome(sbx, nodeID, "sandbox of a lost node failed: nothing to restore it from", err)
	case sbx.State == db.StateArchiving && onNode && hasGen:
		_, _, err = w.store.FinishLostStop(ctx, p)
		w.lostOutcome(sbx, nodeID, "stop on a lost node finished from the last generation", err)
	case sbx.State == db.StateArchived && derefNode(sbx.DiskNodeID) == nodeID && hasGen:
		_, err = w.store.EvictLostDisk(ctx, p)
		w.lostOutcome(sbx, nodeID, "disk on a lost node given up; the sandbox resumes from its last generation", err)
	case sbx.State == db.StateArchiving && onNode,
		sbx.State == db.StateArchived && derefNode(sbx.DiskNodeID) == nodeID:
		// No generation: the lost node's disk is the only copy, so the
		// sandbox waits for that node.
		if w.lostWarned.mark(sbx.ID, nodeID) {
			w.log.Warn("sandbox on a lost node has no generation; it waits for the node", "sandbox", sbx.ID, "node", nodeID)
		}
	}
}

// moveLostSandbox places the sandbox elsewhere (never the lost node nor a
// node holding an evicted copy; the generation's image, a usable object
// store, a disk never smaller than the generation's) and commits the move on
// the first candidate that still has room. No candidate leaves the sandbox
// for a later round.
func (w *Worker) moveLostSandbox(ctx, opCtx context.Context, sbx db.Sandbox, gen db.Generation, p db.NodeLossParams) {
	if w.placer == nil {
		if w.lostWarned.mark(sbx.ID, p.LostNode) {
			w.log.Warn("sandbox on a lost node cannot move: no placer configured", "sandbox", sbx.ID, "node", p.LostNode)
		}
		return
	}
	exclude, err := w.store.OpenEvictionNodes(ctx, sbx.OwnerID, sbx.ID)
	if err != nil {
		w.log.Warn("lost node eviction read failed", "sandbox", sbx.ID, "reason", classify(err))
		return
	}
	reserve := sbx.Desired
	reserve.DiskGiB = max(reserve.DiskGiB, db.DiskGiBFor(gen.DiskBytes))
	cands, err := w.placer.Place(opCtx, scheduler.PlaceRequest{
		Reserve:         reserve,
		Image:           gen.Image,
		Exclude:         append(exclude, p.LostNode),
		NeedObjectStore: true,
	})
	if err != nil {
		w.noMoveTarget(sbx, p.LostNode, err)
		return
	}
	for _, c := range cands {
		_, _, err := w.store.MoveLostSandbox(ctx, p, db.NodeLossMove{
			ToNode: c.Node.ID, GenerationID: gen.ID, ManifestKey: gen.ManifestKey,
		})
		switch {
		case err == nil:
			w.lostWarned.forget(sbx.ID)
			w.log.Info("sandbox of a lost node moved", "sandbox", sbx.ID, "from", p.LostNode, "to", c.Node.ID)
			return
		case errors.Is(err, db.ErrNoCapacity), errors.Is(err, db.ErrNodeUnavailable):
			continue // the candidate filled up or vanished
		default:
			w.lostOutcome(sbx, p.LostNode, "", err)
			return
		}
	}
	w.noMoveTarget(sbx, p.LostNode, scheduler.ErrNoCapacity)
}

// noMoveTarget logs once per sandbox that no node can take it now.
func (w *Worker) noMoveTarget(sbx db.Sandbox, lostNode string, err error) {
	if w.lostWarned.mark(sbx.ID, lostNode) {
		w.log.Warn("no node can take the sandbox of a lost node; retrying next round",
			"sandbox", sbx.ID, "node", lostNode, "reason", classify(err))
	}
}

// lostOutcome logs one node-loss transaction result. A node that came back,
// a sandbox that moved on or was deleted, and the store's own refusals
// (another worker decided first) are quiet: the next round rereads.
func (w *Worker) lostOutcome(sbx db.Sandbox, lostNode, done string, err error) {
	switch {
	case err == nil:
		w.lostWarned.forget(sbx.ID)
		if done != "" {
			w.log.Warn(done, "sandbox", sbx.ID, "node", lostNode)
		}
	case errors.Is(err, db.ErrNodeNotLost), errors.Is(err, db.ErrConflict),
		errors.Is(err, db.ErrNotFound), errors.Is(err, db.ErrNoRestorableData):
	default:
		w.log.Warn("node loss transition failed", "sandbox", sbx.ID, "node", lostNode, "reason", classify(err))
	}
}
