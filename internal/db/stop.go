package db

// Durable stop operations (B1.1). A stop is accepted atomically with the
// sandbox moving to archiving and a [JobStop] enqueued; the operation is the
// canonical record of force escalation and retry state, so workers reread it
// instead of trusting a stale job payload. Finalization is fenced by the
// actual job claim, the sandbox version and the operation row, and releases
// only the compute part of the reservation: the disk and the desired machine
// shape stay for a later resume or deletion.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// StopStatus is the pinned StopOperation lifecycle.
type StopStatus string

const (
	StopPending    StopStatus = "pending"
	StopFailing    StopStatus = "failing"
	StopCompleted  StopStatus = "completed"
	StopSuperseded StopStatus = "superseded"
)

// StopOperation mirrors the API StopOperation schema plus the internal
// node/force bookkeeping. RequestedAt is kept after the stop ends even though
// the API hides it on the sandbox once ended. ForceRequested records a live
// force escalation; Forced records that force actually ended this stop.
type StopOperation struct {
	ID             string
	OwnerID        uuid.UUID
	SandboxID      string
	Status         StopStatus
	RequestedAt    time.Time
	LastAttemptAt  *time.Time
	Error          string
	EndedAt        *time.Time
	Forced         bool
	ForceRequested bool
	NodeID         string
}

const stopCols = `id, owner_id, sandbox_id, status, requested_at, last_attempt_at,
	error, ended_at, forced, force_requested, node_id`

// stopColsQualified is [stopCols] for reads that join another owner-bearing
// table, where id/owner_id/sandbox_id would be ambiguous.
const stopColsQualified = `o.id, o.owner_id, o.sandbox_id, o.status, o.requested_at,
	o.last_attempt_at, o.error, o.ended_at, o.forced, o.force_requested, o.node_id`

func scanStop(row scanner) (StopOperation, error) {
	var op StopOperation
	err := row.Scan(&op.ID, &op.OwnerID, &op.SandboxID, &op.Status, &op.RequestedAt,
		&op.LastAttemptAt, &op.Error, &op.EndedAt, &op.Forced, &op.ForceRequested, &op.NodeID)
	return op, err
}

// newStopID mints a stop_ identifier: 8 crypto-random bytes, hex, matching
// the pinned example shape.
func newStopID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("db: stop id: %w", err)
	}
	return "stop_" + hex.EncodeToString(b[:]), nil
}

// AcceptSandboxStop atomically accepts a stop for an active sandbox: the
// sandbox moves to archiving, a pending durable StopOperation is written and
// one [JobStop] is enqueued. Accepting again while pending or failing returns
// the same operation without new effects; a force=true repeat persists the
// escalation on the canonical operation and its queued job. Stopping an
// already archived sandbox returns its latest completed operation unchanged.
// A deleted or foreign sandbox answers [ErrNotFound]; an unsupported state
// answers [ErrInvalidState].
//
// The initial provision job, when the stop races the first boot, is left
// queued or running on purpose: the worker must complete the first disk under
// the shared sandbox guard before stopping it.
func (s *Store) AcceptSandboxStop(ctx context.Context, owner uuid.UUID, sandboxID string, force bool) (StopOperation, Sandbox, Job, bool, error) {
	return s.acceptSandboxStop(ctx, owner, sandboxID, 0, nil, force)
}

// AcceptSandboxExpiryStop accepts an auto-stop only while the reaper's
// candidate is still current: under the sandbox row lock it requires the
// expected version and an expires_at at or before deadline, so a concurrent
// PATCH extension or manual stop wins instead of being overwritten.
func (s *Store) AcceptSandboxExpiryStop(ctx context.Context, owner uuid.UUID, sandboxID string, expectedVersion int64, deadline time.Time, force bool) (StopOperation, Sandbox, Job, bool, error) {
	return s.acceptSandboxStop(ctx, owner, sandboxID, expectedVersion, &deadline, force)
}

func (s *Store) acceptSandboxStop(ctx context.Context, owner uuid.UUID, sandboxID string, expectedVersion int64, expiryDeadline *time.Time, force bool) (StopOperation, Sandbox, Job, bool, error) {
	if owner == uuid.Nil() {
		return StopOperation{}, Sandbox{}, Job{}, false, ErrNoOwner
	}
	var (
		op      StopOperation
		sbx     Sandbox
		job     Job
		created bool
	)
	err := s.runTx(ctx, "accept sandbox stop", func(ctx context.Context, tx pgx.Tx) error {
		var err error
		sbx, err = sandboxRowForUpdateTx(ctx, tx, owner, sandboxID)
		if err != nil {
			return err
		}
		if expectedVersion != 0 && sbx.Version != expectedVersion {
			return ErrConflict
		}
		if expiryDeadline != nil {
			if sbx.ExpiresAt == nil || sbx.ExpiresAt.After(*expiryDeadline) {
				return ErrConflict
			}
		}
		switch sbx.State {
		case StateArchived:
			op, err = latestStopTx(ctx, tx, owner, sandboxID)
			return err
		case StateArchiving:
			op, err = activeStopTx(ctx, tx, owner, sandboxID)
			if err != nil {
				return err
			}
			if force {
				op, err = escalateStopForceTx(ctx, tx, owner, op)
			}
			return err
		}
		if !slices.Contains(ActiveStates(), sbx.State) {
			return ErrInvalidState
		}

		op, err = insertStopTx(ctx, tx, owner, sbx, force)
		if err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			UPDATE sandboxes SET
				state = $3, version = version + 1, stop_operation_id = $4,
				error_reason = '', ssh_endpoint = NULL, ssh_host_key = NULL,
				updated_at = now()
			WHERE id = $1 AND owner_id = $2 AND version = $5 AND state = ANY($6)
			  AND deleted_at IS NULL
			RETURNING `+sandboxCols,
			sandboxID, owner, StateArchiving, op.ID, sbx.Version, statesToStrings(ActiveStates()))
		sbx, err = scanSandbox(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return fmtError("accept stop transition", err)
		}
		if err := insertEventTx(ctx, tx, sbx, EventInput{
			Type: "stop.requested",
			Data: mustJSON(map[string]any{"operationId": op.ID, "force": force}),
		}); err != nil {
			return err
		}
		// A queued resume can no longer be the current intent. The initial
		// provision job is deliberately not cancelled: archiving needs its
		// disk first.
		if _, err := tx.Exec(ctx, `
			UPDATE jobs SET state = 'cancelled', updated_at = now()
			WHERE sandbox_id = $1 AND owner_id = $2 AND state = 'queued'
			  AND kind = 'sandbox.resume'`, sandboxID, owner); err != nil {
			return fmtError("cancel resume jobs", err)
		}
		payload, err := marshalJobPayload(StopPayload{
			OperationID: op.ID,
			NodeID:      derefString(sbx.NodeID),
			VMID:        sbx.ID,
			Force:       force,
		})
		if err != nil {
			return err
		}
		job, err = enqueueJobTx(ctx, tx, JobInput{
			Kind:      JobStop,
			OwnerID:   owner,
			SandboxID: &sandboxID,
			Payload:   payload,
		})
		if err != nil {
			return err
		}
		created = true
		return nil
	})
	if err != nil {
		return StopOperation{}, Sandbox{}, Job{}, false, err
	}
	return op, sbx, job, created, nil
}

// insertStopTx writes the first StopOperation of a stop request.
func insertStopTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, sbx Sandbox, force bool) (StopOperation, error) {
	id, err := newStopID()
	if err != nil {
		return StopOperation{}, err
	}
	row := tx.QueryRow(ctx, `
		INSERT INTO stop_operations
			(id, owner_id, sandbox_id, status, node_id, force_requested, requested_at)
		VALUES ($1, $2, $3, 'pending', $4, $5, now())
		RETURNING `+stopCols,
		id, owner, sbx.ID, derefString(sbx.NodeID), force)
	op, err := scanStop(row)
	if err != nil {
		return StopOperation{}, fmtError("insert stop operation", err)
	}
	return op, nil
}

// escalateStopForceTx persists force on the canonical repeatable operation
// and on its still-queued job. A running worker rereads the operation, so a
// stale payload cannot hide the escalation.
func escalateStopForceTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, op StopOperation) (StopOperation, error) {
	if op.ForceRequested {
		return op, nil
	}
	row := tx.QueryRow(ctx, `
		UPDATE stop_operations SET force_requested = true, updated_at = now()
		WHERE id = $1 AND owner_id = $2 AND status IN ('pending', 'failing')
		RETURNING `+stopCols,
		op.ID, owner)
	escalated, err := scanStop(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return latestStopTx(ctx, tx, owner, op.SandboxID)
	}
	if err != nil {
		return StopOperation{}, fmtError("escalate stop force", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE jobs SET payload = payload || '{"Force": true}'::jsonb, updated_at = now()
		WHERE id = (
			SELECT id FROM jobs
			WHERE owner_id = $1 AND sandbox_id = $2 AND kind = 'sandbox.stop' AND state = 'queued'
			ORDER BY created_at DESC, id DESC LIMIT 1
		)`, owner, op.SandboxID); err != nil {
		return StopOperation{}, fmtError("escalate stop job force", err)
	}
	return escalated, nil
}

// StopOperation returns one stop request by owner, sandbox and id. The
// sandbox predicate is part of the identity: a stop id that belongs to
// another same-owner sandbox answers [ErrNotFound], never that other row.
func (s *Store) StopOperation(ctx context.Context, owner uuid.UUID, sandboxID, stopID string) (StopOperation, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+stopCols+` FROM stop_operations
		WHERE id = $1 AND owner_id = $2 AND sandbox_id = $3`,
		stopID, owner, sandboxID)
	op, err := scanStop(row)
	if err != nil {
		return StopOperation{}, errNoRows("select stop operation", err)
	}
	return op, nil
}

// LatestSandboxStop returns a live owner-scoped sandbox's most recent stop
// operation. The join requires the live sandbox in the same statement, so a
// deleted or foreign sandbox never exposes another row's stop metadata. A
// sandbox that was never stopped answers [ErrNotFound].
func (s *Store) LatestSandboxStop(ctx context.Context, owner uuid.UUID, sandboxID string) (StopOperation, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+stopColsQualified+` FROM stop_operations o
		JOIN sandboxes s ON s.id = o.sandbox_id AND s.owner_id = o.owner_id
		WHERE o.owner_id = $1 AND o.sandbox_id = $2 AND s.deleted_at IS NULL
		ORDER BY o.requested_at DESC, o.id DESC LIMIT 1`,
		owner, sandboxID)
	op, err := scanStop(row)
	if err != nil {
		return StopOperation{}, errNoRows("latest stop operation", err)
	}
	return op, nil
}

// latestStopTx reads the newest stop of an owner-scoped sandbox, if any.
func latestStopTx(ctx context.Context, q rowQuerier, owner uuid.UUID, sandboxID string) (StopOperation, error) {
	row := q.QueryRow(ctx, `
		SELECT `+stopCols+` FROM stop_operations
		WHERE sandbox_id = $1 AND owner_id = $2
		ORDER BY requested_at DESC, id DESC LIMIT 1`,
		sandboxID, owner)
	op, err := scanStop(row)
	if err != nil {
		return StopOperation{}, errNoRows("latest stop operation", err)
	}
	return op, nil
}

// activeStopTx reads the one pending or failing stop under the sandbox row
// lock held by the caller, so acceptance and finalization serialize.
func activeStopTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, sandboxID string) (StopOperation, error) {
	row := tx.QueryRow(ctx, `
		SELECT `+stopCols+` FROM stop_operations
		WHERE sandbox_id = $1 AND owner_id = $2 AND status IN ('pending', 'failing')
		FOR UPDATE`,
		sandboxID, owner)
	op, err := scanStop(row)
	if err != nil {
		return StopOperation{}, errNoRows("active stop operation", err)
	}
	return op, nil
}

// FailSandboxStopAttempt records a failed archival attempt under this
// worker's live job claim: the operation becomes failing with the attempt
// time and a safe reason, and is retried automatically. A completed or
// superseded operation is returned unchanged (a stale failure must not
// rewrite terminal state); a lost claim answers [ErrJobNotLeased].
func (s *Store) FailSandboxStopAttempt(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, owner uuid.UUID, stopID, reason string) (StopOperation, error) {
	var out StopOperation
	err := s.runTx(ctx, "fail sandbox stop attempt", func(ctx context.Context, tx pgx.Tx) error {
		sandboxID, payload, err := lockJobKindOwnerFenceTx(ctx, tx, jobID, worker, token, owner, JobStop)
		if err != nil {
			return err
		}
		if sandboxID == nil {
			return ErrInvalidJobPayload
		}
		claimed, err := decodeStopPayload(payload)
		if err != nil {
			return err
		}
		if claimed.OperationID != stopID || claimed.VMID != *sandboxID {
			return ErrInvalidJobPayload
		}
		row := tx.QueryRow(ctx, `
			UPDATE stop_operations SET
				status = 'failing', last_attempt_at = now(), error = $3, updated_at = now()
			WHERE id = $1 AND owner_id = $2
			  AND sandbox_id IS NOT DISTINCT FROM $4
			  AND status IN ('pending', 'failing')
			RETURNING `+stopCols,
			stopID, owner, reason, sandboxID)
		out, err = scanStop(row)
		if err == nil {
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmtError("fail stop attempt", err)
		}
		// Terminal (completed/superseded) or mismatched: re-read by the
		// canonical identity so the caller can observe terminal state.
		out, err = stopBySandboxTx(ctx, tx, owner, *sandboxID, stopID)
		return err
	})
	if err != nil {
		return StopOperation{}, err
	}
	return out, nil
}

// FinalizeStopParams identifies the sandbox and operation one archival
// attempt finalizes. ExpectedVersion is the version the worker loaded when
// it started; 0 skips the version check (the operation status still fences).
type FinalizeStopParams struct {
	Owner           uuid.UUID
	SandboxID       string
	StopID          string
	ExpectedVersion int64
}

// FinalizeSandboxStop completes an owner-scoped stop after the node
// confirmed the VM is off: it is fenced by the actual [JobStop] claim, the
// sandbox version and the operation row, releases the vCPU/memory
// reservation exactly once, keeps the disk and desired shape, marks the
// sandbox archived with its stopped timestamp and the operation completed.
// Duplicate delivery of an already completed stop returns the canonical rows
// unchanged. A superseded operation answers [ErrStopSuperseded]; a deleted or
// foreign sandbox [ErrNotFound]; a state or version mismatch [ErrConflict].
func (s *Store) FinalizeSandboxStop(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p FinalizeStopParams) (Sandbox, StopOperation, error) {
	if p.Owner == uuid.Nil() {
		return Sandbox{}, StopOperation{}, ErrNoOwner
	}
	var (
		sbx Sandbox
		op  StopOperation
	)
	err := s.runTx(ctx, "finalize sandbox stop", func(ctx context.Context, tx pgx.Tx) error {
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
		if sbx.StopOperationID == nil || *sbx.StopOperationID != op.ID {
			return ErrConflict
		}
		switch op.Status {
		case StopCompleted:
			if sbx.State != StateArchived {
				return ErrConflict
			}
			return nil // duplicate delivery: canonical rows already final
		case StopSuperseded:
			return ErrStopSuperseded
		}
		if sbx.State != StateArchiving {
			return ErrConflict
		}
		if p.ExpectedVersion != 0 && sbx.Version != p.ExpectedVersion {
			return ErrConflict
		}
		if err := releaseSandboxComputeTx(ctx, tx, sbx); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			UPDATE sandboxes SET
				state = 'archived', version = version + 1,
				reserved_vcpus = 0, reserved_memory_mib = 0,
				stopped_at = now(), error_reason = '',
				ssh_endpoint = NULL, ssh_host_key = NULL, updated_at = now()
			WHERE id = $1 AND owner_id = $2 AND version = $3 AND state = 'archiving'
			  AND deleted_at IS NULL
			RETURNING `+sandboxCols,
			p.SandboxID, p.Owner, sbx.Version)
		sbx, err = scanSandbox(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return fmtError("finalize stop transition", err)
		}
		op, err = completeStopTx(ctx, tx, p.Owner, p.SandboxID, p.StopID)
		if err != nil {
			return err
		}
		return insertEventTx(ctx, tx, sbx, EventInput{
			Type: "stop.completed",
			Data: mustJSON(map[string]any{"operationId": op.ID, "forced": op.Forced}),
		})
	})
	if err != nil {
		return Sandbox{}, StopOperation{}, err
	}
	return sbx, op, nil
}

// completeStopTx turns a pending/failing operation into its terminal
// completed form, carrying the live force escalation into the historical
// forced flag.
func completeStopTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, sandboxID, stopID string) (StopOperation, error) {
	row := tx.QueryRow(ctx, `
		UPDATE stop_operations SET
			status = 'completed', ended_at = now(), last_attempt_at = now(),
			error = '', forced = force_requested, updated_at = now()
		WHERE id = $1 AND owner_id = $2 AND sandbox_id = $3
		  AND status IN ('pending', 'failing')
		RETURNING `+stopCols,
		stopID, owner, sandboxID)
	op, err := scanStop(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return StopOperation{}, ErrConflict
	}
	if err != nil {
		return StopOperation{}, fmtError("complete stop operation", err)
	}
	return op, nil
}

// releaseSandboxComputeTx subtracts the sandbox's live vCPU/memory
// reservation from its node. The caller holds the sandbox row lock and must
// zero the sandbox counters in the same transaction; the disk reservation is
// never touched here.
func releaseSandboxComputeTx(ctx context.Context, tx pgx.Tx, sbx Sandbox) error {
	if sbx.Reserved.VCPUs == 0 && sbx.Reserved.MemoryMiB == 0 {
		return nil
	}
	if sbx.NodeID == nil {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE nodes SET
			reserved_vcpus = GREATEST(reserved_vcpus - $2, 0),
			reserved_memory_mib = GREATEST(reserved_memory_mib - $3, 0),
			updated_at = now()
		WHERE id = $1`,
		*sbx.NodeID, sbx.Reserved.VCPUs, sbx.Reserved.MemoryMiB); err != nil {
		return fmtError("release sandbox compute", err)
	}
	return nil
}

// sandboxRowForUpdateTx locks and reads one live owner-scoped sandbox.
func sandboxRowForUpdateTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, sandboxID string) (Sandbox, error) {
	row := tx.QueryRow(ctx, `
		SELECT `+sandboxCols+` FROM sandboxes
		WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL FOR UPDATE`,
		sandboxID, owner)
	sbx, err := scanSandbox(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Sandbox{}, ErrNotFound
	}
	if err != nil {
		return Sandbox{}, fmtError("lock sandbox", err)
	}
	return sbx, nil
}

// stopForUpdateTx locks one operation that must match the given sandbox.
func stopForUpdateTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, sandboxID, stopID string) (StopOperation, error) {
	row := tx.QueryRow(ctx, `
		SELECT `+stopCols+` FROM stop_operations
		WHERE id = $1 AND owner_id = $2 AND sandbox_id = $3 FOR UPDATE`,
		stopID, owner, sandboxID)
	op, err := scanStop(row)
	if err != nil {
		return StopOperation{}, errNoRows("lock stop operation", err)
	}
	return op, nil
}

// stopBySandboxTx reads one operation by owner, sandbox and id.
func stopBySandboxTx(ctx context.Context, q rowQuerier, owner uuid.UUID, sandboxID, stopID string) (StopOperation, error) {
	row := q.QueryRow(ctx, `
		SELECT `+stopCols+` FROM stop_operations
		WHERE id = $1 AND owner_id = $2 AND sandbox_id = $3`,
		stopID, owner, sandboxID)
	op, err := scanStop(row)
	if err != nil {
		return StopOperation{}, errNoRows("select sandbox stop", err)
	}
	return op, nil
}

// lockJobKindFenceTx takes a shared lock on the caller's live job claim and
// verifies its kind, owner and sandbox, returning the claimed payload so the
// caller can bind it to the operation it is about to mutate. A valid lease
// for another job, another kind or another payload must not authorize this
// operation.
func lockJobKindFenceTx(ctx context.Context, tx pgx.Tx, jobID uuid.UUID, worker string, token uuid.UUID, owner uuid.UUID, sandboxID string, kind JobKind) ([]byte, error) {
	var payload []byte
	err := tx.QueryRow(ctx, `
		SELECT payload FROM jobs
		WHERE id = $1 AND `+jobFence+` AND owner_id = $4 AND sandbox_id = $5 AND kind = $6
		FOR SHARE`, jobID, worker, token, owner, sandboxID, kind).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrJobNotLeased
	}
	if err != nil {
		return nil, fmtError("lock job kind fence", err)
	}
	return payload, nil
}

// lockJobKindOwnerFenceTx is [lockJobKindFenceTx] for callers that do not
// know the sandbox yet: it verifies the claim's kind and owner and returns
// the job's sandbox id plus payload for the follow-up owner-scoped write.
func lockJobKindOwnerFenceTx(ctx context.Context, tx pgx.Tx, jobID uuid.UUID, worker string, token uuid.UUID, owner uuid.UUID, kind JobKind) (*string, []byte, error) {
	var (
		sandboxID *string
		payload   []byte
	)
	err := tx.QueryRow(ctx, `
		SELECT sandbox_id, payload FROM jobs
		WHERE id = $1 AND `+jobFence+` AND owner_id = $4 AND kind = $5
		FOR SHARE`, jobID, worker, token, owner, kind).Scan(&sandboxID, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, ErrJobNotLeased
	}
	if err != nil {
		return nil, nil, fmtError("lock job kind owner fence", err)
	}
	return sandboxID, payload, nil
}

// decodeStopPayload decodes a claimed [JobStop] payload, failing closed on
// malformed content.
func decodeStopPayload(payload []byte) (StopPayload, error) {
	var p StopPayload
	if err := jsonUnmarshal(payload, &p); err != nil {
		return StopPayload{}, ErrInvalidJobPayload
	}
	return p, nil
}
