package lifecycle

// Data retention for stopped sandboxes. An archived sandbox keeps only its
// disk; after the data-retention window that stored data expires too: the
// local copy is recorded as an eviction for VM reconciliation, the retained
// disk reservation is released and data_expired_at is set. The sandbox row
// and the saved snapshots taken from it stay: only an explicit deletion
// removes the row, and a saved snapshot is restorable long after its source
// expired.

import (
	"context"
	"errors"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// reapDataExpiry expires the stored data of every archived sandbox stopped
// longer than the configured retention. Each candidate is accepted in its own
// transaction, which rechecks state, stop time, expiry and concurrent stop or
// resume work under the row lock, so an old candidate can never act on a
// sandbox that resumed or was deleted meanwhile. The node's local copy is left
// to VM reconciliation, which deletes open evictions before anything else
// looks at that node. A zero retention (explicitly off, or no object store
// configured) disables the scan.
func (w *Worker) reapDataExpiry(ctx context.Context) {
	if w.dataRetention <= 0 {
		return
	}
	for {
		if ctx.Err() != nil {
			return
		}
		cutoff := time.Now().Add(-w.dataRetention)
		due, err := w.store.StoppedSandboxesWithDataBefore(ctx, cutoff, deletionReaperPageSize)
		if err != nil {
			w.log.Warn("data retention scan failed", "reason", classify(err))
			return
		}
		expired := 0
		for _, sbx := range due {
			if ctx.Err() != nil {
				return
			}
			_, ok, err := w.store.AcceptDataExpiry(ctx, sbx.OwnerID, sbx.ID, cutoff)
			switch {
			case errors.Is(err, db.ErrNotFound):
				// Deleted meanwhile: nothing left to expire.
			case err != nil:
				w.log.Warn("data expiry acceptance failed", "sandbox", sbx.ID, "reason", classify(err))
			case ok:
				expired++
			}
		}
		if len(due) < deletionReaperPageSize {
			return
		}
		if expired == 0 {
			// A full page with no accepted expiry cannot make progress by
			// re-reading the same rows; leave it for the next round.
			w.log.Warn("data retention scan made no progress", "page", len(due))
			return
		}
	}
}
