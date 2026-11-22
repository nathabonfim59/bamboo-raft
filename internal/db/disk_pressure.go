package db

// Disk-pressure eviction of stopped disks. When a node reports free disk
// below the configured share of its total, the lifecycle role gives up the
// retained disks of archived sandboxes kept on it, least recently stopped
// first, until the node is back above the threshold. A candidate's kept
// disk equals its chain's latest completed generation — the stop's final
// generation, cut at or after the stop was requested — so giving it up
// loses nothing: the disk is then only in the object store, and a later
// resume restores it, possibly on the same node. A sandbox without such a
// generation is never listed: its disk is the only full copy.
//
// The eviction commits first: the transaction below rechecks the row under
// its lock (still archived, disk still on the node, same version), drops the
// node and disk pointers, releases the reservation and records the copy as
// an open vm_evictions row. A resume admitted after the commit already sees
// no local disk and restores from the store; the worker's DeleteVM then
// removes the copy and closes the row, and VM reconciliation finishes the
// delete whenever the first attempt could not.

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// DiskPressureReason is the reason recorded on a sandbox.disk_evicted
// event when a retained disk is given up because its node ran low on disk.
const DiskPressureReason = "disk_pressure"

// LowDiskNodes lists the nodes whose free disk — total minus reserved, the
// node's last reported free — is below percent of their total disk, by id.
// percent is 1..100; the caller's configuration bounds it.
func (s *Store) LowDiskNodes(ctx context.Context, percent int) ([]Node, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+nodeCols+` FROM nodes
		WHERE total_disk_gib > 0
		  AND (total_disk_gib - reserved_disk_gib) * 100 < total_disk_gib * $1
		ORDER BY id`, percent)
	if err != nil {
		return nil, fmtError("list low disk nodes", err)
	}
	defer rows.Close()
	var out []Node
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, fmtError("scan low disk node", err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("list low disk nodes", err)
	}
	return out, nil
}

// DiskPressureCandidates lists up to limit live archived sandboxes whose
// retained disk a low-disk node may give up: the disk is local to nodeID,
// the stored data has not expired, and the chain has a completed, unpurged
// generation cut at or after the sandbox's last stop was requested — the
// stop's final generation, which the kept disk equals exactly. Candidates
// come least recently stopped first. Every one is rechecked under its row
// lock by [Store.EvictSandboxDisk] before anything changes.
func (s *Store) DiskPressureCandidates(ctx context.Context, nodeID string, limit int) ([]Sandbox, error) {
	limit = clamp(limit, 1, 500, 100)
	// The lateral join exposes only the stop's requested_at, so the
	// unqualified sandbox column list stays unambiguous.
	rows, err := s.pool.Query(ctx, `SELECT `+sandboxCols+` FROM sandboxes s
		JOIN LATERAL (SELECT o.requested_at FROM stop_operations o
			WHERE o.id = s.stop_operation_id) op ON true
		WHERE s.deleted_at IS NULL
		  AND s.state = 'archived'
		  AND s.disk_node_id = $1
		  AND s.data_expired_at IS NULL
		  AND EXISTS (
			SELECT 1 FROM snapshots g
			WHERE g.sandbox_id = s.id AND g.chain_id = s.chain_id
			  AND g.status = 'completed' AND g.purged_at IS NULL
			  AND g.cut_at >= op.requested_at)
		ORDER BY op.requested_at, s.id
		LIMIT $2`, nodeID, limit)
	if err != nil {
		return nil, fmtError("list disk pressure candidates", err)
	}
	defer rows.Close()
	var out []Sandbox
	for rows.Next() {
		sbx, err := scanSandbox(rows)
		if err != nil {
			return nil, fmtError("scan disk pressure candidate", err)
		}
		out = append(out, sbx)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("list disk pressure candidates", err)
	}
	return out, nil
}

// DiskPressureParams names one archived sandbox whose retained disk Node
// may give up. Version is the row version the caller read (0 skips the
// check).
type DiskPressureParams struct {
	Owner     uuid.UUID
	SandboxID string
	Node      string
	Version   int64
}

// EvictSandboxDisk gives up Node's local disk of an archived sandbox whose
// kept copy a completed generation reproduces. In one transaction it locks
// the sandbox, rechecks that it is still archived with its disk on Node at
// the expected version and that its chain still holds a completed
// generation of unexpired data, releases the retained reservation, clears
// node_id and disk_node_id, records the copy as an open vm_evictions row
// ([EvictionReasonDiskPressure]) and records [EventSandboxDiskEvicted] with
// reason [DiskPressureReason]. The disk then exists only in the object
// store and a later resume restores it there; the copy is deleted
// afterwards by the caller's DeleteVM or, when that fails, by VM
// reconciliation, which closes the row.
//
// A deleted or foreign sandbox answers [ErrNotFound]; a sandbox no longer
// archived with its disk on Node, or a newer version, [ErrConflict]; data
// that expired or has no completed generation left [ErrNoRestorableData]
// (the disk would be the only full copy).
func (s *Store) EvictSandboxDisk(ctx context.Context, p DiskPressureParams) (Sandbox, error) {
	if p.Owner == uuid.Nil() {
		return Sandbox{}, ErrNoOwner
	}
	if p.SandboxID == "" || p.Node == "" {
		return Sandbox{}, errors.New("db: disk eviction needs a sandbox and a node")
	}
	var out Sandbox
	err := s.runTx(ctx, "evict sandbox disk", func(ctx context.Context, tx pgx.Tx) error {
		sbx, err := sandboxRowForUpdateTx(ctx, tx, p.Owner, p.SandboxID)
		if err != nil {
			return err
		}
		if p.Version != 0 && sbx.Version != p.Version {
			return ErrConflict
		}
		if sbx.State != StateArchived || derefString(sbx.DiskNodeID) != p.Node {
			return ErrConflict
		}
		if ok, err := hasCompletedGenerationTx(ctx, tx, sbx); err != nil {
			return err
		} else if !ok || sbx.DataExpiredAt != nil {
			return ErrNoRestorableData
		}
		if err := releaseNodeTx(ctx, tx, sbx.NodeID, sbx.Reserved); err != nil {
			return err
		}
		out, err = scanSandbox(tx.QueryRow(ctx, `
			UPDATE sandboxes SET
				node_id = NULL, disk_node_id = NULL,
				reserved_vcpus = 0, reserved_memory_mib = 0, reserved_disk_gib = 0,
				version = version + 1, updated_at = now()
			WHERE id = $1 AND version = $2
			RETURNING `+sandboxCols, sbx.ID, sbx.Version))
		if err != nil {
			return fmtError("evict sandbox disk", err)
		}
		if err := insertEvictionTx(ctx, tx, p.Node, sbx.ID, EvictionReasonDiskPressure); err != nil {
			return err
		}
		return insertEventTx(ctx, tx, out, EventInput{
			Type: EventSandboxDiskEvicted,
			Data: mustJSON(map[string]any{"reason": DiskPressureReason}),
		})
	})
	if err != nil {
		return Sandbox{}, err
	}
	return out, nil
}
