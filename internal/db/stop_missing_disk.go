package db

// Terminal missing-disk stop (B1.2, orchestrator-approved option c). When a
// stop's VM and its retained disk are confirmed gone from the node, the stop
// can never archive a kept disk. The worker first calls DeleteVM — the frozen
// fence that aborts and joins any ambiguous earlier CreateVM/StartVM — and
// then this one transaction records the canonical unrecoverable outcome:
// the sandbox becomes a terminal error, the canonical stop operation stays
// failing with the fixed safe error vm_missing (so GET still exposes it), and
// every remaining reservation is released exactly once. Replays are
// idempotent: an already-recorded outcome changes nothing and reports
// applied=false, so callers never warn or release twice. A normal DELETE
// still works afterwards and supersedes the failing operation.

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// MissingDiskStopError is the fixed, safe error recorded on the canonical
// stop operation (and as the sandbox error reason) when the stop target's
// disk is confirmed missing. It is a classification, never node text.
const MissingDiskStopError = "vm_missing"

// FinalizeSandboxStopMissingDisk ends a stop whose VM and retained disk the
// node confirmed missing, after the worker's DeleteVM fenced any ambiguous
// earlier boot. In one transaction it moves the archiving sandbox to the
// terminal error state with the fixed reason [MissingDiskStopError], records
// the same safe error on the canonical stop operation (which stays failing so
// GET keeps exposing the outcome), and releases every remaining reservation —
// compute and retained disk — exactly once.
//
// It is fenced by the live [JobStop] claim, the operation id from its
// payload, the sandbox's current stop pointer, the expected row version and
// the archiving state, and fails closed on a newer intent: a deleted or
// foreign sandbox answers [ErrNotFound]; a superseded operation
// [ErrStopSuperseded]; a completed operation, a stale version, a row that
// left archiving or a payload mismatch [ErrConflict] or [ErrInvalidJobPayload];
// a lost claim [ErrJobNotLeased]. A replay of the already-recorded outcome
// returns the canonical rows with applied=false and no new effects.
func (s *Store) FinalizeSandboxStopMissingDisk(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p FinalizeStopParams) (Sandbox, StopOperation, bool, error) {
	if p.Owner == uuid.Nil() {
		return Sandbox{}, StopOperation{}, false, ErrNoOwner
	}
	var (
		sbx     Sandbox
		op      StopOperation
		applied bool
	)
	err := s.runTx(ctx, "finalize sandbox stop missing disk", func(ctx context.Context, tx pgx.Tx) error {
		payload, err := lockJobKindFenceTx(ctx, tx, jobID, worker, token, p.Owner, p.SandboxID, JobStop)
		if err != nil {
			return err
		}
		claimed, err := decodeStopPayload(payload)
		if err != nil {
			return err
		}
		if claimed.OperationID != p.StopID || claimed.VMID != p.SandboxID {
			return ErrInvalidJobPayload
		}
		sbx, err = sandboxRowForUpdateTx(ctx, tx, p.Owner, p.SandboxID)
		if err != nil {
			return err
		}
		op, err = stopForUpdateTx(ctx, tx, p.Owner, p.SandboxID, p.StopID)
		if err != nil {
			return err
		}
		if sbx.DeletedAt != nil {
			return ErrNotFound
		}
		if sbx.StopOperationID == nil || *sbx.StopOperationID != op.ID {
			return ErrConflict // a newer stop intent owns the machine
		}
		// The replay marker is the COMPLETE canonical outcome, and only that:
		// the error row carrying the fixed reason, this very operation still
		// failing with the same error, and nothing left reserved. Any lesser
		// combination (for example an operation error alone while the row is
		// still archiving with reservations held — the safe transient retry
		// path) fails closed below instead of replaying.
		if sbx.State == StateError && sbx.ErrorReason == MissingDiskStopError &&
			op.Status == StopFailing && op.Error == MissingDiskStopError &&
			sbx.Reserved == (Resources{}) {
			return nil
		}
		if op.Status == StopSuperseded {
			return ErrStopSuperseded
		}
		if op.Status == StopCompleted {
			return ErrConflict
		}
		if sbx.State != StateArchiving {
			return ErrConflict
		}
		if p.ExpectedVersion != 0 && sbx.Version != p.ExpectedVersion {
			return ErrConflict
		}
		if err := s.releaseSandboxReservationTx(ctx, tx, p.Owner, p.SandboxID); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			UPDATE sandboxes SET
				state = 'error', version = version + 1,
				reserved_vcpus = 0, reserved_memory_mib = 0, reserved_disk_gib = 0,
				error_reason = $3, ssh_endpoint = NULL, ssh_host_key = NULL,
				updated_at = now()
			WHERE id = $1 AND owner_id = $2 AND version = $4
			  AND state = 'archiving' AND deleted_at IS NULL
			RETURNING `+sandboxCols,
			p.SandboxID, p.Owner, MissingDiskStopError, sbx.Version)
		sbx, err = scanSandbox(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return fmtError("finalize stop missing disk transition", err)
		}
		op, err = markStopMissingDiskTx(ctx, tx, p.Owner, p.SandboxID, p.StopID)
		if err != nil {
			return err
		}
		applied = true
		return insertEventTx(ctx, tx, sbx, EventInput{
			Type: "stop.missing_disk",
			Data: mustJSON(map[string]any{"operationId": op.ID}),
		})
	})
	if err != nil {
		return Sandbox{}, StopOperation{}, false, err
	}
	return sbx, op, applied, nil
}

// markStopMissingDiskTx records the terminal failing attempt on the canonical
// stop operation with the fixed safe error.
func markStopMissingDiskTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, sandboxID, stopID string) (StopOperation, error) {
	row := tx.QueryRow(ctx, `
		UPDATE stop_operations SET
			status = 'failing', last_attempt_at = now(), error = $4, updated_at = now()
		WHERE id = $1 AND owner_id = $2 AND sandbox_id = $3
		  AND status IN ('pending', 'failing')
		RETURNING `+stopCols,
		stopID, owner, sandboxID, MissingDiskStopError)
	op, err := scanStop(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return StopOperation{}, ErrConflict
	}
	if err != nil {
		return StopOperation{}, fmtError("mark stop missing disk", err)
	}
	return op, nil
}
