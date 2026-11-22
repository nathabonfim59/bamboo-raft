package db

// VM evictions. A node's local copy of a sandbox that the
// control plane stopped using — the sandbox resumed on another node, and later
// node loss, data expiry or disk pressure — is recorded in vm_evictions in the
// same transaction that orphans it, together with the release of its
// reservation. The copy is then deleted with DeleteVM on that node: right away
// by the operation when the node is reachable, otherwise by VM reconciliation
// when the node comes back, before anything else looks at that node's
// inventory.
//
// While a row is open, no admission may point the sandbox at that node again:
// CreateVM is idempotent per sandbox id on a node, so a new boot there would
// get the stale copy back instead of a fresh restore.

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// EvictionReasonResumedElsewhere is the reason of an eviction written by a
// resume that restored the sandbox on another node.
const EvictionReasonResumedElsewhere = "resumed_elsewhere"

// EvictionReasonDataExpired is the reason of an eviction written when a
// stopped sandbox's retained data expired: the disk it left on its node must
// be deleted, not restored.
const EvictionReasonDataExpired = "data_expired"

// EvictionReasonDiskPressure is the reason of an eviction written when a
// node under disk pressure gives up an archived sandbox's retained disk.
const EvictionReasonDiskPressure = "disk_pressure"

// Eviction is one open vm_evictions row as reconciliation reads it.
type Eviction struct {
	ID        uuid.UUID
	NodeID    string
	SandboxID string
	OwnerID   uuid.UUID
	Reason    string
	CreatedAt time.Time
	// Current is true when the sandbox is live and its node_id is NodeID:
	// the copy on the node is the sandbox's current one and must not be
	// deleted (admission prevents this; reconciliation checks it anyway).
	Current bool
}

// insertEvictionTx records that nodeID's copy of sandboxID is orphaned. An
// open row for the same copy is kept as is.
func insertEvictionTx(ctx context.Context, tx pgx.Tx, nodeID, sandboxID, reason string) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO vm_evictions (id, node_id, sandbox_id, reason)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (node_id, sandbox_id) WHERE done_at IS NULL DO NOTHING`,
		uuid.NewV7(), nodeID, sandboxID, reason); err != nil {
		return fmtError("insert eviction", err)
	}
	return nil
}

// openEvictionTx reports whether nodeID still holds an evicted copy of
// sandboxID.
func openEvictionTx(ctx context.Context, tx pgx.Tx, nodeID, sandboxID string) (bool, error) {
	var open bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM vm_evictions
			WHERE node_id = $1 AND sandbox_id = $2 AND done_at IS NULL)`,
		nodeID, sandboxID).Scan(&open); err != nil {
		return false, fmtError("check eviction", err)
	}
	return open, nil
}

// OpenEvictions lists nodeID's open evictions, oldest first, with the owner
// and current node of each sandbox (deleted sandboxes included: their copies
// must go too).
func (s *Store) OpenEvictions(ctx context.Context, nodeID string) ([]Eviction, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT e.id, e.node_id, e.sandbox_id, s.owner_id, e.reason, e.created_at,
			(s.deleted_at IS NULL AND s.node_id IS NOT DISTINCT FROM e.node_id)
		FROM vm_evictions e JOIN sandboxes s ON s.id = e.sandbox_id
		WHERE e.node_id = $1 AND e.done_at IS NULL
		ORDER BY e.created_at, e.id`, nodeID)
	if err != nil {
		return nil, fmtError("list evictions", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Eviction, error) {
		var e Eviction
		err := row.Scan(&e.ID, &e.NodeID, &e.SandboxID, &e.OwnerID, &e.Reason, &e.CreatedAt, &e.Current)
		return e, err
	})
	if err != nil {
		return nil, fmtError("list evictions", err)
	}
	return out, nil
}

// OpenEvictionNodes lists the nodes that still hold an evicted copy of one
// owner-scoped sandbox; placement excludes them.
func (s *Store) OpenEvictionNodes(ctx context.Context, owner uuid.UUID, sandboxID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT e.node_id FROM vm_evictions e JOIN sandboxes s ON s.id = e.sandbox_id
		WHERE e.sandbox_id = $1 AND s.owner_id = $2 AND e.done_at IS NULL
		ORDER BY e.node_id`, sandboxID, owner)
	if err != nil {
		return nil, fmtError("list eviction nodes", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmtError("list eviction nodes", err)
	}
	return out, nil
}

// MarkEvictionDone closes nodeID's open eviction of sandboxID after DeleteVM
// confirmed the copy is gone. It is idempotent: no open row is not an error.
func (s *Store) MarkEvictionDone(ctx context.Context, nodeID, sandboxID string) error {
	if _, err := s.pool.Exec(ctx, `
		UPDATE vm_evictions SET done_at = now()
		WHERE node_id = $1 AND sandbox_id = $2 AND done_at IS NULL`,
		nodeID, sandboxID); err != nil {
		return fmtError("mark eviction done", err)
	}
	return nil
}
