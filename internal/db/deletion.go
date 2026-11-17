package db

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// DeletionKind says what a deletion operation removes.
type DeletionKind string

const (
	DeletionSandbox  DeletionKind = "sandbox"
	DeletionSnapshot DeletionKind = "snapshot"
)

// DeletionReason is why the deletion was accepted.
type DeletionReason string

const (
	// DeletionExplicit covers API DELETE calls. Phase 0.5 TTL expiry also
	// lands here; the compat docs record the mapping.
	DeletionExplicit DeletionReason = "explicit"
	// DeletionZDR covers zero-data-loss retention deletions.
	DeletionZDR DeletionReason = "zdr"
	// DeletionAccount covers account closure deletions.
	DeletionAccount DeletionReason = "account"
)

// DeletionStatus is the operation lifecycle.
type DeletionStatus string

const (
	DeletionPending    DeletionStatus = "pending"
	DeletionProcessing DeletionStatus = "processing"
	DeletionBlocked    DeletionStatus = "blocked"
	DeletionCompleted  DeletionStatus = "completed"
)

// DeletionStage is the optional background-purge stage.
type DeletionStage string

const (
	StageRemoving       DeletionStage = "removing"
	StageWaitingUploads DeletionStage = "waiting_for_uploads"
	StageRetrying       DeletionStage = "retrying"
	StageCompleted      DeletionStage = "completed"
)

// DeletionOperation mirrors the API DeletionOperation schema: a durable,
// owner-visible record of an accepted irreversible deletion. IDs match
// ^bdop_[a-f0-9]{32}$.
type DeletionOperation struct {
	ID           string
	OwnerID      uuid.UUID
	Kind         DeletionKind
	TargetID     string
	Reason       DeletionReason
	Status       DeletionStatus
	Stage        DeletionStage // "" means unset
	ExpectedBy   *time.Time
	AttemptCount int
	RequestedAt  time.Time
	CompletedAt  *time.Time
}

const deletionCols = `id, owner_id, kind, target_id, reason, status, stage, expected_by,
	attempt_count, requested_at, completed_at`

func scanDeletion(row scanner) (DeletionOperation, error) {
	var (
		op    DeletionOperation
		stage *string
	)
	err := row.Scan(&op.ID, &op.OwnerID, &op.Kind, &op.TargetID, &op.Reason, &op.Status,
		&stage, &op.ExpectedBy, &op.AttemptCount, &op.RequestedAt, &op.CompletedAt)
	if err == nil && stage != nil {
		op.Stage = DeletionStage(*stage)
	}
	return op, err
}

// newDeletionID mints a bdop_ identifier: 16 crypto-random bytes, hex.
func newDeletionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("db: deletion id: %w", err)
	}
	return "bdop_" + hex.EncodeToString(b[:]), nil
}

// AcceptSandboxDeletion atomically accepts an irreversible sandbox deletion:
// it soft-deletes the sandbox (gone from every owner-visible list and lookup
// immediately), writes a pending DeletionOperation, records an event,
// supersedes an in-flight stop, cancels queued provision/stop/resume work and
// enqueues the node deletion job. Accepting twice returns the existing active
// operation without new side effects. The operation keeps saved snapshots
// intact.
func (s *Store) AcceptSandboxDeletion(ctx context.Context, owner uuid.UUID, sandboxID string, reason DeletionReason) (DeletionOperation, Sandbox, error) {
	var (
		op  DeletionOperation
		sbx Sandbox
	)
	err := s.runTx(ctx, "accept sandbox deletion", func(ctx context.Context, tx pgx.Tx) error {
		// Take the sandbox row lock first so a concurrent delete, stop or
		// resume serializes against this acceptance.
		row := tx.QueryRow(ctx, `
			SELECT `+sandboxCols+` FROM sandboxes
			WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL FOR UPDATE`,
			sandboxID, owner)
		var err error
		sbx, err = scanSandbox(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return deletionAcceptError(ctx, tx, owner, sandboxID, &op)
		}
		if err != nil {
			return fmtError("accept deletion lock", err)
		}
		op, sbx, err = s.acceptSandboxDeletionLockedTx(ctx, tx, sbx, reason)
		return err
	})
	if err != nil {
		return DeletionOperation{}, Sandbox{}, err
	}
	return op, sbx, nil
}

// acceptSandboxDeletionLockedTx performs the irreversible side effects of an
// accepted deletion for a sandbox whose row the caller already locked: soft
// delete it, write or replay the one active operation, record the event,
// supersede an in-flight stop and cancel queued provision/stop/resume work,
// then enqueue the node cleanup with exactly the reservation that remains.
// A stopped sandbox's reservation is disk-only here, so deletion releases the
// disk once and never compute twice.
func (s *Store) acceptSandboxDeletionLockedTx(ctx context.Context, tx pgx.Tx, locked Sandbox, reason DeletionReason) (DeletionOperation, Sandbox, error) {
	row := tx.QueryRow(ctx, `
		UPDATE sandboxes SET deleted_at = now(), updated_at = now()
		WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL
		RETURNING `+sandboxCols,
		locked.ID, locked.OwnerID)
	sbx, err := scanSandbox(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return DeletionOperation{}, Sandbox{}, ErrNotFound
	}
	if err != nil {
		return DeletionOperation{}, Sandbox{}, fmtError("accept deletion soft delete", err)
	}

	var active string
	err = tx.QueryRow(ctx, `
		SELECT id FROM deletion_operations
		WHERE target_id = $1 AND owner_id = $2 AND status <> 'completed'`,
		sbx.ID, sbx.OwnerID).Scan(&active)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// First acceptance: create the operation below.
	case err != nil:
		return DeletionOperation{}, Sandbox{}, fmtError("accept deletion active lookup", err)
	default:
		// Someone already accepted; surface their operation idempotently.
		existing, err := s.deletionRowTx(ctx, tx, sbx.OwnerID, active)
		if err != nil {
			return DeletionOperation{}, Sandbox{}, err
		}
		return existing, sbx, nil
	}

	id, idErr := newDeletionID()
	if idErr != nil {
		return DeletionOperation{}, Sandbox{}, idErr
	}
	op := DeletionOperation{
		ID:       id,
		OwnerID:  sbx.OwnerID,
		Kind:     DeletionSandbox,
		TargetID: sbx.ID,
		Reason:   reason,
		Status:   DeletionPending,
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO deletion_operations (id, owner_id, kind, target_id, reason, status, requested_at)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		RETURNING requested_at`,
		op.ID, sbx.OwnerID, op.Kind, op.TargetID, op.Reason, op.Status).Scan(&op.RequestedAt)
	if err != nil {
		return DeletionOperation{}, Sandbox{}, fmtError("insert deletion operation", err)
	}

	if err := insertEventTx(ctx, tx, sbx, EventInput{
		Type: "delete.requested",
		Data: mustJSON(map[string]string{"operationId": op.ID, "reason": string(reason)}),
	}); err != nil {
		return DeletionOperation{}, Sandbox{}, err
	}
	// An accepted deletion ends any in-flight stop durably and cancels the
	// queued work that can no longer run. A running attempt notices the
	// deleted row and fails closed.
	if _, err := tx.Exec(ctx, `
		UPDATE stop_operations SET status = 'superseded', ended_at = now(), updated_at = now()
		WHERE sandbox_id = $1 AND owner_id = $2 AND status IN ('pending', 'failing')`,
		sbx.ID, sbx.OwnerID); err != nil {
		return DeletionOperation{}, Sandbox{}, fmtError("supersede stop on delete", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE jobs SET state = 'cancelled', updated_at = now()
		WHERE sandbox_id = $1 AND owner_id = $2 AND state = 'queued'
		  AND kind IN ('sandbox.provision', 'sandbox.stop', 'sandbox.resume')`,
		sbx.ID, sbx.OwnerID); err != nil {
		return DeletionOperation{}, Sandbox{}, fmtError("cancel sandbox jobs", err)
	}

	payload, err := marshalJobPayload(DeleteVMPayload{
		NodeID:      derefString(sbx.NodeID),
		VMID:        sbx.ID,
		Resources:   sbx.Reserved,
		OperationID: op.ID,
	})
	if err != nil {
		return DeletionOperation{}, Sandbox{}, err
	}
	if _, err := enqueueJobTx(ctx, tx, JobInput{
		Kind:      JobDeleteVM,
		OwnerID:   sbx.OwnerID,
		SandboxID: &sbx.ID,
		Payload:   payload,
	}); err != nil {
		return DeletionOperation{}, Sandbox{}, err
	}
	return op, sbx, nil
}

// deletionAcceptError explains a failed soft delete. When the sandbox was
// already deleted by this owner it fills out with the existing operation so
// acceptance stays idempotent; otherwise the sandbox is unknown or foreign.
// The owner predicate is part of the read, so a foreign row stays a safe
// not-found.
func deletionAcceptError(ctx context.Context, tx pgx.Tx, owner uuid.UUID, sandboxID string, out *DeletionOperation) error {
	var deleted *time.Time
	err := tx.QueryRow(ctx,
		`SELECT deleted_at FROM sandboxes WHERE id = $1 AND owner_id = $2`,
		sandboxID, owner).Scan(&deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmtError("accept deletion recheck", err)
	}
	if deleted == nil {
		return ErrNotFound // unreachable: the update above matched no row
	}
	// Already deleted: surface the recorded operation, completed or not.
	row := tx.QueryRow(ctx, `
		SELECT `+deletionCols+` FROM deletion_operations
		WHERE target_id = $1 AND owner_id = $2
		ORDER BY requested_at DESC LIMIT 1`, sandboxID, owner)
	existing, err := scanDeletion(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmtError("accept deletion operation lookup", err)
	}
	*out = existing
	return nil
}

// deletionRowTx reads one owner-scoped operation inside a transaction.
func (s *Store) deletionRowTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, id string) (DeletionOperation, error) {
	row := tx.QueryRow(ctx,
		`SELECT `+deletionCols+` FROM deletion_operations WHERE id = $1 AND owner_id = $2`,
		id, owner)
	op, err := scanDeletion(row)
	if err != nil {
		return DeletionOperation{}, errNoRows("select deletion operation", err)
	}
	return op, nil
}

// DeletionOperation returns one operation owned by owner. Unknown and
// foreign operations answer ErrNotFound.
func (s *Store) DeletionOperation(ctx context.Context, owner uuid.UUID, opID string) (DeletionOperation, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+deletionCols+` FROM deletion_operations WHERE id = $1 AND owner_id = $2`,
		opID, owner)
	op, err := scanDeletion(row)
	if err != nil {
		return DeletionOperation{}, errNoRows("select deletion operation", err)
	}
	return op, nil
}

// BeginDeletionAttempt marks an owner-scoped operation processing for one
// worker run and counts the attempt. It is called when the delete job
// starts, before any node RPC.
func (s *Store) BeginDeletionAttempt(ctx context.Context, owner uuid.UUID, opID string) (DeletionOperation, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE deletion_operations SET
			status = 'processing',
			stage = 'removing',
			attempt_count = attempt_count + 1,
			updated_at = now()
		WHERE id = $1 AND owner_id = $2 AND status IN ('pending', 'processing', 'blocked')
		RETURNING `+deletionCols,
		opID, owner)
	op, err := scanDeletion(row)
	if err != nil {
		return DeletionOperation{}, errNoRows("begin deletion attempt", err)
	}
	return op, nil
}

// CompleteDeletion finishes an owner-scoped operation after the node
// confirmed cleanup: in one transaction it marks the operation completed
// (stage completed, completed_at set) and releases the sandbox's reservation
// through the exactly-once zero-and-clear path, so capacity returns exactly
// when the durable record says the VM is gone and no later delivery can
// double-release. Completing an already completed operation returns it
// unchanged (idempotent duplicate delivery).
func (s *Store) CompleteDeletion(ctx context.Context, owner uuid.UUID, opID string) (DeletionOperation, error) {
	var op DeletionOperation
	err := s.runTx(ctx, "complete deletion", func(ctx context.Context, tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			UPDATE deletion_operations SET
				status = 'completed',
				stage = 'completed',
				completed_at = now(),
				updated_at = now()
			WHERE id = $1 AND owner_id = $2 AND status <> 'completed'
			RETURNING `+deletionCols,
			opID, owner)
		var err error
		op, err = scanDeletion(row)
		if errors.Is(err, pgx.ErrNoRows) {
			// Either duplicate delivery of a completed operation
			// (return it) or a foreign/unknown one (NotFound).
			existing, rerr := s.deletionRowTx(ctx, tx, owner, opID)
			if rerr != nil {
				return rerr
			}
			if existing.Status != DeletionCompleted {
				return ErrConflict
			}
			op = existing
			return nil
		}
		if err != nil {
			return fmtError("complete deletion", err)
		}
		if op.Kind == DeletionSandbox {
			if err := s.releaseSandboxReservationTx(ctx, tx, owner, op.TargetID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return DeletionOperation{}, err
	}
	return op, nil
}

// releaseSandboxReservationTx returns a sandbox's reservation exactly once
// inside an open transaction: lock the row, subtract from the node, zero
// the sandbox counters.
func (s *Store) releaseSandboxReservationTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, sandboxID string) error {
	var (
		nodeID     *string
		vcpus, mem int64
		disk       int64
	)
	err := tx.QueryRow(ctx, `
		SELECT node_id, reserved_vcpus, reserved_memory_mib, reserved_disk_gib
		FROM sandboxes WHERE id = $1 AND owner_id = $2 FOR UPDATE`,
		sandboxID, owner).Scan(&nodeID, &vcpus, &mem, &disk)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmtError("release reservation select", err)
	}
	if vcpus == 0 && mem == 0 && disk == 0 {
		return nil // already released
	}
	if nodeID != nil {
		if _, err := tx.Exec(ctx, `
			UPDATE nodes SET
				reserved_vcpus = GREATEST(reserved_vcpus - $2, 0),
				reserved_memory_mib = GREATEST(reserved_memory_mib - $3, 0),
				reserved_disk_gib = GREATEST(reserved_disk_gib - $4, 0),
				updated_at = now()
			WHERE id = $1`, *nodeID, vcpus, mem, disk); err != nil {
			return fmtError("release reservation node", err)
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sandboxes SET reserved_vcpus = 0, reserved_memory_mib = 0,
			reserved_disk_gib = 0, updated_at = now()
		WHERE id = $1`, sandboxID); err != nil {
		return fmtError("release reservation clear", err)
	}
	return nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func mustJSON(v any) string {
	b, err := jsonMarshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
