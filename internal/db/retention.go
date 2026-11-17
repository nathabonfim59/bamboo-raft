package db

// Stopped-retention deletion (B1.1 persistence half). The reaper lists
// archived sandboxes by their stopped timestamp and asks for a deletion; the
// acceptance rechecks state, stopped time and concurrent work under the row
// lock before using the normal deletion path, so an old candidate can never
// delete a sandbox that resumed or was extended in the meantime. The default
// retention duration and its disabled (never) value are worker/config
// concerns, not schema defaults.

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
