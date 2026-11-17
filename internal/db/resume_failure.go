package db

// Terminal resume failure finalization (B1.2). A resume boots a retained disk
// back to READY; when that boot fails for good, the durable end state is
// designed explicitly so the initial-create cleanup path can never run against
// the retained disk:
//
//   - after StopVM confirmed the VM is off and the disk kept, the sandbox
//     returns to archived at the same lifecycle generation with a safe error,
//     and only the compute part of the reservation is released;
//   - after the node confirmed the retained disk is gone and DeleteVM fenced
//     any ambiguous earlier start, every remaining reservation is released and
//     the sandbox becomes a terminal error.
//
// Both are fenced by the live [JobResume] claim and the lifecycle generation
// of its payload, and fail closed on a newer stop, resume or delete intent.

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// FinalizeResumeFailureParams identifies the resume attempt one worker
// finalizes after a failed boot. Generation must be the generation from the
// [ResumePayload]; Version is the row version the worker loaded (0 skips the
// check).
type FinalizeResumeFailureParams struct {
	Owner      uuid.UUID
	SandboxID  string
	Generation int64
	Version    int64
}

// lockResumeFailureTx validates the failing worker's claim and returns the
// locked sandbox row a failure finalization may mutate: the job must still be
// running under worker and token with a live lease, belong to the same owner
// and sandbox, carry exactly the payload generation the caller names, and the
// row must still be provisioning at that generation. Anything else fails
// closed without touching capacity or state.
func (s *Store) lockResumeFailureTx(ctx context.Context, tx pgx.Tx, jobID uuid.UUID, worker string, token uuid.UUID, p FinalizeResumeFailureParams) (Sandbox, error) {
	payload, err := lockJobKindFenceTx(ctx, tx, jobID, worker, token, p.Owner, p.SandboxID, JobResume)
	if err != nil {
		return Sandbox{}, err
	}
	if p.Generation <= 0 {
		return Sandbox{}, ErrConflict
	}
	claimed, err := decodeResumePayload(payload)
	if err != nil {
		return Sandbox{}, err
	}
	if claimed.Generation != p.Generation || claimed.VMID != p.SandboxID {
		return Sandbox{}, ErrInvalidJobPayload
	}
	sbx, err := sandboxRowForUpdateTx(ctx, tx, p.Owner, p.SandboxID)
	if err != nil {
		return Sandbox{}, err
	}
	if sbx.LifecycleGeneration != p.Generation {
		return Sandbox{}, ErrConflict
	}
	if sbx.State != StateProvisioning {
		return Sandbox{}, ErrConflict
	}
	if p.Version != 0 && sbx.Version != p.Version {
		return Sandbox{}, ErrConflict
	}
	return sbx, nil
}

// FinalizeSandboxResumeFailure ends one resume attempt whose boot failed for
// good after the node confirmed the VM is stopped and its disk retained: the
// sandbox returns from provisioning to archived at the same lifecycle
// generation, records its stopped timestamp and a safe error reason, and the
// compute part of its reservation is released exactly once. The disk
// reservation and the desired machine shape survive for a later resume or
// deletion.
//
// It is fenced by the live [JobResume] claim, the generation from its
// payload, the row version and the provisioning state, and fails closed on a
// newer intent: a deleted or foreign sandbox answers [ErrNotFound]; a newer
// resume (generation bumped), a newer stop (state left provisioning) or a
// version mismatch answers [ErrConflict]; a lost claim [ErrJobNotLeased].
func (s *Store) FinalizeSandboxResumeFailure(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p FinalizeResumeFailureParams, reason string) (Sandbox, error) {
	if p.Owner == uuid.Nil() {
		return Sandbox{}, ErrNoOwner
	}
	var out Sandbox
	err := s.runTx(ctx, "finalize sandbox resume failure", func(ctx context.Context, tx pgx.Tx) error {
		sbx, err := s.lockResumeFailureTx(ctx, tx, jobID, worker, token, p)
		if err != nil {
			return err
		}
		if err := releaseSandboxComputeTx(ctx, tx, sbx); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			UPDATE sandboxes SET
				state = 'archived', version = version + 1,
				reserved_vcpus = 0, reserved_memory_mib = 0,
				stopped_at = now(), error_reason = $3,
				ssh_endpoint = NULL, ssh_host_key = NULL, updated_at = now()
			WHERE id = $1 AND owner_id = $2 AND version = $4
			  AND state = 'provisioning' AND lifecycle_generation = $5
			  AND deleted_at IS NULL
			RETURNING `+sandboxCols,
			p.SandboxID, p.Owner, reason, sbx.Version, p.Generation)
		out, err = scanSandbox(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return fmtError("finalize resume failure transition", err)
		}
		return insertEventTx(ctx, tx, out, EventInput{
			Type: "resume.failed",
			Data: mustJSON(map[string]any{"generation": p.Generation, "reason": reason}),
		})
	})
	if err != nil {
		return Sandbox{}, err
	}
	return out, nil
}

// FinalizeSandboxResumeMissingDisk ends a resume whose retained disk the node
// confirmed missing, after DeleteVM fenced any ambiguous earlier start: there
// is nothing left to boot or retain, so the provisioning generation becomes a
// terminal error (reason vm_missing) and every remaining reservation —
// compute and retained disk — is released exactly once. The fencing and
// fail-closed rules match [FinalizeSandboxResumeFailure]; a deleted or
// foreign sandbox answers [ErrNotFound] and any newer intent [ErrConflict].
func (s *Store) FinalizeSandboxResumeMissingDisk(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p FinalizeResumeFailureParams) (Sandbox, error) {
	if p.Owner == uuid.Nil() {
		return Sandbox{}, ErrNoOwner
	}
	var out Sandbox
	err := s.runTx(ctx, "finalize sandbox resume missing disk", func(ctx context.Context, tx pgx.Tx) error {
		sbx, err := s.lockResumeFailureTx(ctx, tx, jobID, worker, token, p)
		if err != nil {
			return err
		}
		if err := s.releaseSandboxReservationTx(ctx, tx, p.Owner, p.SandboxID); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			UPDATE sandboxes SET
				state = 'error', version = version + 1,
				reserved_vcpus = 0, reserved_memory_mib = 0, reserved_disk_gib = 0,
				error_reason = 'vm_missing', ssh_endpoint = NULL, ssh_host_key = NULL,
				updated_at = now()
			WHERE id = $1 AND owner_id = $2 AND version = $3
			  AND state = 'provisioning' AND lifecycle_generation = $4
			  AND deleted_at IS NULL
			RETURNING `+sandboxCols,
			p.SandboxID, p.Owner, sbx.Version, p.Generation)
		out, err = scanSandbox(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return fmtError("finalize resume missing disk transition", err)
		}
		return insertEventTx(ctx, tx, out, EventInput{
			Type: "resume.missing_disk",
			Data: mustJSON(map[string]any{"generation": p.Generation}),
		})
	})
	if err != nil {
		return Sandbox{}, err
	}
	return out, nil
}
