package db

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// RecoverCooldown is the minimum interval between accepted recovery attempts
// for the same sandbox.
const RecoverCooldown = 10 * time.Minute

// ErrTooSoon means a recovery was already accepted for this sandbox within
// [RecoverCooldown].
var ErrTooSoon = errors.New("db: sandbox was recovered too recently")

// AcceptRecover atomically accepts a recovery for an owner-scoped sandbox in
// one transaction: it locks the sandbox row, verifies the sandbox is in a
// ready-ish state with an assigned node and outside [RecoverCooldown], updates
// last_recover_at to now, records a sandbox.recovering event and enqueues one
// [JobRecover] job. Concurrent calls serialize on the row lock so a second
// caller receives [ErrTooSoon]. A missing, foreign or deleted sandbox answers
// [ErrNotFound]; a sandbox without a running machine answers [ErrInvalidState].
func (s *Store) AcceptRecover(ctx context.Context, owner uuid.UUID, sandboxID string, now time.Time) (Sandbox, error) {
	if owner == uuid.Nil() {
		return Sandbox{}, ErrNoOwner
	}
	if now.IsZero() {
		return Sandbox{}, errors.New("db: accept recover requires a non-zero timestamp")
	}
	now = now.UTC().Truncate(time.Microsecond)

	var out Sandbox
	err := s.runTx(ctx, "accept recover", func(ctx context.Context, tx pgx.Tx) error {
		sbx, err := sandboxRowForUpdateTx(ctx, tx, owner, sandboxID)
		if err != nil {
			return err
		}
		if !recoverEligibleState(sbx.State) || sbx.NodeID == nil || *sbx.NodeID == "" {
			return ErrInvalidState
		}
		if sbx.LastRecoverAt != nil && now.Sub(*sbx.LastRecoverAt) < RecoverCooldown {
			return ErrTooSoon
		}

		row := tx.QueryRow(ctx, `
			UPDATE sandboxes SET last_recover_at = $3, updated_at = now()
			WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL
			RETURNING `+sandboxCols,
			sandboxID, owner, now)
		out, err = scanSandbox(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmtError("accept recover update", err)
		}

		if err := insertEventTx(ctx, tx, out, EventInput{Type: "sandbox.recovering"}); err != nil {
			return err
		}

		payload, err := marshalJobPayload(RecoverPayload{
			NodeID:     *out.NodeID,
			Generation: out.LifecycleGeneration,
		})
		if err != nil {
			return err
		}
		if _, err := enqueueJobTx(ctx, tx, JobInput{
			Kind:      JobRecover,
			OwnerID:   owner,
			SandboxID: &sandboxID,
			Payload:   payload,
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return Sandbox{}, err
	}
	return out, nil
}

// RecordRecoverEvent appends one recovery outcome event for a live
// owner-scoped sandbox only while jobID is still running under worker and
// token with a live [JobRecover] lease. A lost claim answers [ErrJobNotLeased]
// and writes nothing; a missing, foreign or deleted sandbox answers
// [ErrNotFound].
func (s *Store) RecordRecoverEvent(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, owner uuid.UUID, sandboxID string, e EventInput) error {
	if owner == uuid.Nil() {
		return ErrNoOwner
	}
	if e.Data == "" {
		e.Data = "{}"
	}
	return s.runTx(ctx, "record recover event", func(ctx context.Context, tx pgx.Tx) error {
		if _, err := lockJobKindFenceTx(ctx, tx, jobID, worker, token, owner, sandboxID, JobRecover); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO sandbox_events (sandbox_id, owner_id, type, data)
			SELECT s.id, s.owner_id, $3, $4::jsonb
			FROM sandboxes s
			WHERE s.id = $1 AND s.owner_id = $2 AND s.deleted_at IS NULL`,
			sandboxID, owner, e.Type, e.Data)
		if err != nil {
			return fmtError("record recover event", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// recoverEligibleState reports whether a sandbox state represents a running
// machine eligible for recovery.
func recoverEligibleState(state SandboxState) bool {
	switch state {
	case StateReady, StateIdle, StateRunning:
		return true
	default:
		return false
	}
}
