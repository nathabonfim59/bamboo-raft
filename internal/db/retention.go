package db

// Stopped-retention persistence. An archived sandbox keeps only its disk, so
// the reaper has two independent windows: after the data-retention window its
// stored data expires while the row stays, and after the (usually longer, or
// disabled) stopped-retention window the row itself is deleted through the
// normal deletion path. Both list archived sandboxes by their stopped
// timestamp and both accept a candidate under the row lock, rechecking state,
// stopped time and concurrent work, so an old candidate can never act on a
// sandbox that resumed or was extended in the meantime. The default durations
// and their disabled (never) values are worker/config concerns, not schema
// defaults.

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// StoppedSandboxesBefore lists up to limit live archived sandboxes stopped at
// or before the cutoff, oldest stop first. These are retention candidates;
// acceptance rechecks every one of them transactionally.
func (s *Store) StoppedSandboxesBefore(ctx context.Context, before time.Time, limit int) ([]Sandbox, error) {
	limit = clamp(limit, 1, 500, 100)
	rows, err := s.pool.Query(ctx, `SELECT `+sandboxCols+` FROM sandboxes
		WHERE deleted_at IS NULL AND state = 'archived' AND stopped_at IS NOT NULL
		  AND stopped_at <= $1
		ORDER BY stopped_at, id LIMIT $2`,
		before, limit)
	if err != nil {
		return nil, fmtError("list stopped sandboxes", err)
	}
	defer rows.Close()
	var out []Sandbox
	for rows.Next() {
		sbx, err := scanSandbox(rows)
		if err != nil {
			return nil, fmtError("scan stopped sandbox", err)
		}
		out = append(out, sbx)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("list stopped sandboxes", err)
	}
	return out, nil
}

// AcceptStoppedRetentionDeletion atomically accepts the retention deletion of
// one archived sandbox through the normal deletion path after rechecking,
// under its row lock, that it is still live, still archived and still stopped
// at or before stoppedBefore, and that no stop or resume job is queued or
// running. accepted is false with a nil error when the candidate is no longer
// eligible (it resumed or otherwise left archived, was deleted, or was
// stopped after the cutoff), so a reaper can skip it safely; unknown and
// foreign sandboxes answer [ErrNotFound]. Changing an archived sandbox's TTL
// alone does not change its stopped_at and therefore does not extend
// stopped-disk retention; only a state change, the stop time, deletion or
// concurrent stop/resume work invalidates a candidate.
//
// The deletion reuses the same soft delete, de-duplicated operation, event,
// work cancellation and node cleanup job as an explicit delete. A stopped
// sandbox's reservation is disk-only, so capacity is released exactly once.
func (s *Store) AcceptStoppedRetentionDeletion(ctx context.Context, owner uuid.UUID, sandboxID string, stoppedBefore time.Time, reason DeletionReason) (DeletionOperation, Sandbox, bool, error) {
	if owner == uuid.Nil() {
		return DeletionOperation{}, Sandbox{}, false, ErrNoOwner
	}
	var (
		op       DeletionOperation
		sbx      Sandbox
		accepted bool
	)
	err := s.runTx(ctx, "accept stopped retention deletion", func(ctx context.Context, tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			SELECT `+sandboxCols+` FROM sandboxes
			WHERE id = $1 AND owner_id = $2 FOR UPDATE`,
			sandboxID, owner)
		var err error
		sbx, err = scanSandbox(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmtError("lock retention candidate", err)
		}
		if sbx.DeletedAt != nil || sbx.State != StateArchived || sbx.StoppedAt == nil ||
			sbx.StoppedAt.After(stoppedBefore) {
			return nil
		}
		var active bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM jobs
				WHERE owner_id = $1 AND sandbox_id = $2
				  AND kind IN ('sandbox.stop', 'sandbox.resume')
				  AND state IN ('queued', 'running'))`,
			owner, sandboxID).Scan(&active); err != nil {
			return fmtError("check retention active jobs", err)
		}
		if active {
			return nil
		}
		op, sbx, err = s.acceptSandboxDeletionLockedTx(ctx, tx, sbx, reason)
		if err != nil {
			return err
		}
		accepted = true
		return nil
	})
	if err != nil {
		return DeletionOperation{}, Sandbox{}, false, err
	}
	return op, sbx, accepted, nil
}

// EventSandboxDataExpired is recorded when a stopped sandbox's stored data
// expires: its disk is gone everywhere, though the row and the saved snapshots
// taken from it stay.
const EventSandboxDataExpired = "sandbox.data_expired"

// StoppedSandboxesWithDataBefore lists up to limit live archived sandboxes
// whose stored data has not expired yet and that were stopped at or before the
// cutoff, oldest stop first. These are data-expiry candidates; acceptance
// rechecks every one of them transactionally.
func (s *Store) StoppedSandboxesWithDataBefore(ctx context.Context, before time.Time, limit int) ([]Sandbox, error) {
	limit = clamp(limit, 1, 500, 100)
	rows, err := s.pool.Query(ctx, `SELECT `+sandboxCols+` FROM sandboxes
		WHERE deleted_at IS NULL AND state = 'archived' AND stopped_at IS NOT NULL
		  AND stopped_at <= $1 AND data_expired_at IS NULL
		ORDER BY stopped_at, id LIMIT $2`,
		before, limit)
	if err != nil {
		return nil, fmtError("list expirable sandboxes", err)
	}
	defer rows.Close()
	var out []Sandbox
	for rows.Next() {
		sbx, err := scanSandbox(rows)
		if err != nil {
			return nil, fmtError("scan expirable sandbox", err)
		}
		out = append(out, sbx)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("list expirable sandboxes", err)
	}
	return out, nil
}

// AcceptDataExpiry atomically expires the stored data of one archived sandbox
// after rechecking, under its row lock, that it is still live, still archived,
// stopped at or before stoppedBefore, has not expired yet and has no stop or
// resume job queued or running. accepted is false with a nil error when the
// candidate is no longer eligible, so a reaper can skip it safely; unknown,
// deleted and foreign sandboxes answer [ErrNotFound].
//
// It sets data_expired_at, releases the retained disk reservation exactly once,
// clears disk_node_id and node_id (nothing local is left, so the sandbox is
// never described as sitting on the node that held the disk), records an
// eviction of that node's copy for VM reconciliation and appends
// [EventSandboxDataExpired]. The sandbox row itself stays; only an explicit
// deletion removes it. Saved snapshots taken from the sandbox are independent
// artifacts and are not touched.
func (s *Store) AcceptDataExpiry(ctx context.Context, owner uuid.UUID, sandboxID string, stoppedBefore time.Time) (Sandbox, bool, error) {
	if owner == uuid.Nil() {
		return Sandbox{}, false, ErrNoOwner
	}
	var (
		sbx     Sandbox
		expired bool
	)
	err := s.runTx(ctx, "accept data expiry", func(ctx context.Context, tx pgx.Tx) error {
		row, err := sandboxRowForUpdateTx(ctx, tx, owner, sandboxID)
		if err != nil {
			return err
		}
		if row.State != StateArchived || row.StoppedAt == nil ||
			row.StoppedAt.After(stoppedBefore) || row.DataExpiredAt != nil {
			return nil
		}
		var active bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM jobs
				WHERE owner_id = $1 AND sandbox_id = $2
				  AND kind IN ('sandbox.stop', 'sandbox.resume')
				  AND state IN ('queued', 'running'))`,
			owner, sandboxID).Scan(&active); err != nil {
			return fmtError("check data expiry active jobs", err)
		}
		if active {
			return nil
		}
		diskNode := derefString(row.DiskNodeID)
		if err := releaseNodeTx(ctx, tx, row.NodeID, row.Reserved); err != nil {
			return err
		}
		sbx, err = scanSandbox(tx.QueryRow(ctx, `
			UPDATE sandboxes SET
				data_expired_at = now(),
				disk_node_id = NULL,
				node_id = NULL,
				reserved_vcpus = 0, reserved_memory_mib = 0, reserved_disk_gib = 0,
				version = version + 1, updated_at = now()
			WHERE id = $1 AND owner_id = $2
			RETURNING `+sandboxCols, sandboxID, owner))
		if err != nil {
			return fmtError("expire sandbox data", err)
		}
		if diskNode != "" {
			if err := insertEvictionTx(ctx, tx, diskNode, sandboxID, EvictionReasonDataExpired); err != nil {
				return err
			}
		}
		expired = true
		return insertEventTx(ctx, tx, sbx, EventInput{Type: EventSandboxDataExpired})
	})
	if err != nil {
		return Sandbox{}, false, err
	}
	return sbx, expired, nil
}
