package server

// Late binding of the saved-snapshot plugin to the lifecycle worker: the
// plugin must exist before the worker (the worker takes the plugin's frozen
// handler map) while a generation-backed save needs the worker's
// CutGeneration. The plugin holds this small adapter; the role assigns the
// worker before starting either.

import (
	"context"
	"errors"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
)

// snapshotCutter implements snapshots.GenerationCutter over the worker.
type snapshotCutter struct {
	worker *lifecycle.Worker
}

// CutGeneration cuts one generation of a ready sandbox under its guard.
func (c *snapshotCutter) CutGeneration(ctx context.Context, lease *lifecycle.Lease,
	owner uuid.UUID, sandboxID string,
) (db.Generation, error) {
	if c.worker == nil {
		return db.Generation{}, errors.New("server: lifecycle worker is not initialized")
	}
	return c.worker.CutGeneration(ctx, lease, owner, sandboxID)
}
