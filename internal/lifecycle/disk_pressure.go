package lifecycle

// Disk pressure. With an object store configured and a nonzero
// -lifecycle.disk-low-percent, each reaper round lists the nodes whose
// reported free disk (total minus reserved) is below that share of their
// total, then gives up the retained disks of archived sandboxes on them —
// least recently stopped first — until the node is back above the
// threshold, estimated from the released reservations. The next inventory
// poll corrects the estimate.
//
// A candidate's kept disk equals the stop's final generation: the store's
// listing requires a completed generation cut at or after the stop was
// requested, so evicting the disk loses nothing and a later resume
// restores it from the object store, possibly on the same node. Every
// candidate is decided under its sandbox guard and the eviction commits
// before the copy is touched — the transaction drops the node's disk
// pointer and opens a vm_evictions row, so a resume admitted at any later
// point already restores from the store. The copy is then deleted with
// DeleteVM and the row closed; a failed delete leaves the open row for VM
// reconciliation, which removes the copy before anything else uses the
// node's inventory.

import (
	"context"
	"errors"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// diskPressurePageSize bounds one candidate listing of a low node.
const diskPressurePageSize = 100

// reapDiskPressure runs the disk-pressure step of a reaper round. Without
// an object store or with the percent off it does nothing: the retained
// disks would be the only copies.
func (w *Worker) reapDiskPressure(ctx context.Context) {
	if w.objectStore == nil || w.diskLowPercent <= 0 {
		return
	}
	nodes, err := w.store.LowDiskNodes(ctx, w.diskLowPercent)
	if err != nil {
		w.log.Warn("low disk node scan failed", "reason", classify(err))
		return
	}
	for _, n := range nodes {
		if ctx.Err() != nil {
			return
		}
		w.relieveNodeDisk(ctx, n)
	}
}

// relieveNodeDisk evicts retained disks off one low node until its free
// estimate reaches the threshold or the candidates run out.
func (w *Worker) relieveNodeDisk(ctx context.Context, n db.Node) {
	free := n.Free().DiskGiB
	low := func() bool {
		return free*100 < n.Total.DiskGiB*uint64(w.diskLowPercent)
	}
	for low() {
		if ctx.Err() != nil {
			return
		}
		candidates, err := w.store.DiskPressureCandidates(ctx, n.ID, diskPressurePageSize)
		if err != nil {
			w.log.Warn("disk pressure candidate scan failed", "node", n.ID, "reason", classify(err))
			return
		}
		if len(candidates) == 0 {
			if w.lowWarned.mark(n.ID, "no-candidates") {
				w.log.Warn("node low on disk with no evictable stopped disks",
					"node", n.ID, "free_gib", free, "total_gib", n.Total.DiskGiB)
			}
			return
		}
		evicted := 0
		for _, sbx := range candidates {
			if ctx.Err() != nil {
				return
			}
			if !low() {
				return
			}
			if w.evictPressuredDisk(ctx, n.ID, sbx) {
				evicted++
				free += sbx.Reserved.DiskGiB
			}
		}
		if evicted == 0 || len(candidates) < diskPressurePageSize {
			return
		}
	}
}

// evictPressuredDisk gives up one candidate's retained disk on nodeID under
// its sandbox guard: it re-reads the row, commits the eviction — the row
// drops node and disk pointers and opens a vm_evictions row — and only then
// deletes the node's copy and closes the row. It reports whether the
// eviction committed: the released reservation counts toward the node's
// free estimate however the delete went, and a failed or skipped delete
// leaves the open row for VM reconciliation.
func (w *Worker) evictPressuredDisk(ctx context.Context, nodeID string, listed db.Sandbox) bool {
	guard, opCtx, stop, err := w.acquireReapGuard(ctx, listed.OwnerID, listed.ID)
	if err != nil {
		return false
	}
	defer func() {
		stop()
		guard.Release(ctx)
	}()
	sbx, err := w.store.SandboxAny(ctx, listed.OwnerID, listed.ID)
	if err != nil || sbx.DeletedAt != nil || sbx.State != db.StateArchived ||
		derefNode(sbx.DiskNodeID) != nodeID || sbx.DataExpiredAt != nil {
		return false
	}
	_, err = w.store.EvictSandboxDisk(ctx, db.DiskPressureParams{
		Owner: sbx.OwnerID, SandboxID: sbx.ID, Node: nodeID, Version: sbx.Version,
	})
	switch {
	case err == nil:
		w.log.Info("stopped disk evicted under disk pressure", "sandbox", sbx.ID, "node", nodeID)
	case errors.Is(err, db.ErrConflict), errors.Is(err, db.ErrNotFound), errors.Is(err, db.ErrNoRestorableData):
		return false // resumed, deleted or expired meanwhile; the next round rereads
	default:
		w.log.Warn("disk pressure eviction failed", "sandbox", sbx.ID, "node", nodeID, "reason", classify(err))
		return false
	}
	if opCtx.Err() != nil {
		return true
	}
	if err := w.deleteVMOnNode(opCtx, nodeID, sbx.ID); err != nil {
		w.log.Warn("evicted copy delete pending; reconciliation retries",
			"sandbox", sbx.ID, "node", nodeID, "reason", classify(err))
		return true
	}
	if err := w.store.MarkEvictionDone(ctx, nodeID, sbx.ID); err != nil {
		w.log.Warn("eviction close failed; reconciliation retries",
			"sandbox", sbx.ID, "node", nodeID, "reason", classify(err))
	}
	return true
}
