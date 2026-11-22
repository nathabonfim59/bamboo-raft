package db

// Same-node resume (B1.1). A stopped sandbox keeps its disk and desired
// machine shape; resume reserves the compute again atomically on the node
// that holds the disk, bumps the lifecycle generation and enqueues one
// [JobResume]. A resume that arrives while the stop is still archiving
// supersedes that durable operation instead of reserving a second time: the
// full reservation stays until StartVM returns.

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// ResumeParams is one owner-scoped resume request. ExpectedVersion is the
// version the caller loaded; 0 skips the check. Resources and MachineType are
// either both empty (keep the stored shape) or both set (a new shape).
// SupersedeStopID must name the active stop when the sandbox is archiving; it
// is ignored for an archived sandbox.
type ResumeParams struct {
	Owner           uuid.UUID
	SandboxID       string
	ExpectedVersion int64
	MachineType     string
	Resources       Resources
	Env             EnvChange
	TTL             TTLOption
	SupersedeStopID string
	MaxAttempts     int
}

// ResumeAcceptance is what one accepted resume produced. Superseded is set
// when the resume ended an in-flight stop.
type ResumeAcceptance struct {
	Sandbox    Sandbox
	Job        Job
	Superseded *StopOperation
}

// AcceptSandboxResume atomically accepts a resume for an archived sandbox:
// it re-reserves the desired vCPU/memory (plus any positive disk growth) on
// the same node under the node row lock, restores reserved_* from desired_*,
// applies the env and TTL options, moves the sandbox to provisioning with a
// fresh lifecycle generation and enqueues [JobResume]. An explicit null TTL
// disables auto-stop; an omitted TTL keeps the configured duration and resets
// the deadline. An omitted env keeps the sealed env; an explicit empty env
// replaces it with none.
//
// For a sandbox still archiving, SupersedeStopID must match the active stop:
// the stop becomes superseded, its queued job is cancelled and the existing
// full reservation is retained (running the resume on the same machine), so
// neither compute nor disk is reserved twice. Before the first boot has
// completed, there is no disk to start: while the canonical initial provision
// job is still queued or running, a resume during archiving answers
// [ErrConflict] instead of abandoning the disk creation. B1.3 maps that
// conflict to the pinned sandbox_stopping response.
//
// A deleted or foreign sandbox answers [ErrNotFound]; capacity refusal
// [ErrNoCapacity]; a missing node row [ErrNodeUnavailable]; a smaller disk
// than the retained one [ErrTypeTooSmall]; an inconsistent shape
// [ErrInvalidMachine]; another state [ErrInvalidState].
func (s *Store) AcceptSandboxResume(ctx context.Context, p ResumeParams) (ResumeAcceptance, error) {
	if p.Owner == uuid.Nil() {
		return ResumeAcceptance{}, ErrNoOwner
	}
	var out ResumeAcceptance
	err := s.runTx(ctx, "accept sandbox resume", func(ctx context.Context, tx pgx.Tx) error {
		sbx, err := sandboxRowForUpdateTx(ctx, tx, p.Owner, p.SandboxID)
		if err != nil {
			return err
		}
		if p.ExpectedVersion != 0 && sbx.Version != p.ExpectedVersion {
			return ErrConflict
		}
		desired, machineType, err := resumeShape(sbx, p)
		if err != nil {
			return err
		}
		if sbx.NodeID == nil || *sbx.NodeID == "" {
			return ErrNodeUnavailable
		}
		nodeID := *sbx.NodeID
		if p.TTL.Set && p.TTL.Seconds != nil {
			if err := ValidateTTLSeconds(p.TTL.Seconds); err != nil {
				return err
			}
		}
		var envSealed []byte
		if p.Env.Set {
			envSealed, err = s.sealEnv(p.Env.Values, p.Owner, sbx.ID)
			if err != nil {
				return err
			}
		}

		switch sbx.State {
		case StateArchived:
			add := Resources{VCPUs: desired.VCPUs, MemoryMiB: desired.MemoryMiB}
			if desired.DiskGiB > sbx.Reserved.DiskGiB {
				add.DiskGiB = desired.DiskGiB - sbx.Reserved.DiskGiB
			}
			if add != (Resources{}) {
				if err := reserveResumeCapacityTx(ctx, tx, nodeID, add); err != nil {
					return err
				}
			}
			out.Sandbox, err = transitionResumeTx(ctx, tx, sbx, p, envSealed, desired, machineType, nodeID)
			if err != nil {
				return err
			}
		case StateArchiving:
			if p.SupersedeStopID == "" {
				return ErrInvalidState
			}
			// Before the first boot completed there is no disk to start:
			// superseding the stop would abandon the initial provision.
			// Refuse while its canonical provision job is still queued or
			// running; B1.3 maps this conflict to sandbox_stopping.
			if sbx.LifecycleGeneration == 0 {
				boot, err := latestProvisionStateTx(ctx, tx, p.Owner, p.SandboxID)
				if err != nil {
					return err
				}
				if boot == JobQueued || boot == JobRunning {
					return ErrConflict
				}
			}
			op, err := activeStopTx(ctx, tx, p.Owner, p.SandboxID)
			if err != nil {
				return err
			}
			if op.ID != p.SupersedeStopID {
				return ErrConflict
			}
			// The node may already have stopped the VM while DB
			// finalization was superseded. The full reservation is
			// still held here: never add a second one. A shape change
			// during the stop returns a conflict for the API to refuse
			// rather than double-reserving.
			if p.Resources != (Resources{}) && p.Resources != sbx.Desired {
				return ErrConflict
			}
			superseded, err := supersedeStopTx(ctx, tx, p.Owner, op)
			if err != nil {
				return err
			}
			out.Superseded = &superseded
			out.Sandbox, err = transitionResumeTx(ctx, tx, sbx, p, envSealed, sbx.Desired, sbx.MachineType, nodeID)
			if err != nil {
				return err
			}
		default:
			return ErrInvalidState
		}

		payload, err := marshalJobPayload(ResumePayload{
			NodeID:      nodeID,
			VMID:        out.Sandbox.ID,
			MachineType: out.Sandbox.MachineType,
			Resources:   out.Sandbox.Desired,
			FromStopID:  derefString(out.Sandbox.StopOperationID),
			Generation:  out.Sandbox.LifecycleGeneration,
		})
		if err != nil {
			return err
		}
		attempts := p.MaxAttempts
		out.Job, err = enqueueJobTx(ctx, tx, JobInput{
			Kind:        JobResume,
			OwnerID:     p.Owner,
			SandboxID:   &p.SandboxID,
			Payload:     payload,
			MaxAttempts: attempts,
		})
		if err != nil {
			return err
		}
		return insertEventTx(ctx, tx, out.Sandbox, EventInput{
			Type: "resume.accepted",
			Data: mustJSON(map[string]any{"generation": out.Sandbox.LifecycleGeneration}),
		})
	})
	if err != nil {
		return ResumeAcceptance{}, err
	}
	return out, nil
}

// resumeShape resolves and validates the resume's machine shape.
func resumeShape(sbx Sandbox, p ResumeParams) (Resources, string, error) {
	switch {
	case p.Resources != (Resources{}):
		if p.Resources.VCPUs == 0 || p.Resources.MemoryMiB == 0 || p.Resources.DiskGiB == 0 || p.MachineType == "" {
			return Resources{}, "", ErrInvalidMachine
		}
		if p.Resources.DiskGiB < sbx.Reserved.DiskGiB {
			return Resources{}, "", ErrTypeTooSmall
		}
		return p.Resources, p.MachineType, nil
	case p.MachineType != "" && p.MachineType != sbx.MachineType:
		return Resources{}, "", ErrInvalidMachine
	default:
		return sbx.Desired, sbx.MachineType, nil
	}
}

// transitionResumeTx moves an archived or archiving sandbox to provisioning
// and applies the shape, reservation, env and TTL, bumping the lifecycle
// generation. For an archived sandbox reserved_* is rebuilt from desired_*;
// for an archiving sandbox the caller already holds the full reservation and
// the update leaves it untouched.
func transitionResumeTx(ctx context.Context, tx pgx.Tx, sbx Sandbox, p ResumeParams, envSealed []byte, desired Resources, machineType, nodeID string) (Sandbox, error) {
	var ttlSeconds any
	if p.TTL.Set && p.TTL.Seconds != nil {
		ttlSeconds = *p.TTL.Seconds
	}
	row := tx.QueryRow(ctx, `
		UPDATE sandboxes SET
			state = 'provisioning', version = version + 1,
			lifecycle_generation = lifecycle_generation + 1,
			machine_type = $3,
			desired_vcpus = $4, desired_memory_mib = $5, desired_disk_gib = $6,
			reserved_vcpus = CASE WHEN $7 THEN $4 ELSE reserved_vcpus END,
			reserved_memory_mib = CASE WHEN $7 THEN $5 ELSE reserved_memory_mib END,
			reserved_disk_gib = CASE WHEN $7 THEN $6 ELSE reserved_disk_gib END,
			node_id = $8,
			stopped_at = NULL, error_reason = '',
			ssh_endpoint = NULL, ssh_host_key = NULL,
			env_sealed = CASE WHEN $9 THEN $10 ELSE env_sealed END,
			ttl_seconds = CASE WHEN $11 THEN $12::integer ELSE ttl_seconds END,
			expires_at = CASE
				WHEN $11 THEN (
					CASE WHEN $12::integer IS NULL THEN NULL
					     ELSE now() + make_interval(secs => $12::integer) END)
				WHEN ttl_seconds IS NULL THEN NULL
				ELSE now() + make_interval(secs => ttl_seconds) END,
			updated_at = now()
		WHERE id = $1 AND owner_id = $2 AND version = $13
		  AND state IN ('archived', 'archiving') AND deleted_at IS NULL
		RETURNING `+sandboxCols,
		sbx.ID, sbx.OwnerID, machineType,
		desired.VCPUs, desired.MemoryMiB, desired.DiskGiB,
		sbx.State == StateArchived, nodeID,
		p.Env.Set, envSealed,
		p.TTL.Set, ttlSeconds,
		sbx.Version)
	out, err := scanSandbox(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Sandbox{}, ErrConflict
	}
	if err != nil {
		return Sandbox{}, fmtError("accept resume transition", err)
	}
	return out, nil
}

// reserveResumeCapacityTx reserves add on the node under the same guarded
// UPDATE as create placement; a missing node row is reported separately from
// a full one so a resume with a lost node fails cleanly.
func reserveResumeCapacityTx(ctx context.Context, tx pgx.Tx, nodeID string, add Resources) error {
	tag, err := tx.Exec(ctx, `
		UPDATE nodes SET
			reserved_vcpus = reserved_vcpus + $2,
			reserved_memory_mib = reserved_memory_mib + $3,
			reserved_disk_gib = reserved_disk_gib + $4,
			updated_at = now()
		WHERE id = $1
		  AND reserved_vcpus + $2 <= total_vcpus
		  AND reserved_memory_mib + $3 <= total_memory_mib
		  AND reserved_disk_gib + $4 <= total_disk_gib`,
		nodeID, add.VCPUs, add.MemoryMiB, add.DiskGiB)
	if err != nil {
		return fmtError("resume reserve", err)
	}
	if tag.RowsAffected() == 0 {
		if err := noCapacityOrMissing(ctx, tx, nodeID); errors.Is(err, ErrNotFound) {
			return ErrNodeUnavailable
		} else {
			return err
		}
	}
	return nil
}

// supersedeStopTx ends the active stop because a resume took over the
// machine, cancels its still-queued job and returns the canonical updated
// operation.
func supersedeStopTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, op StopOperation) (StopOperation, error) {
	row := tx.QueryRow(ctx, `
		UPDATE stop_operations SET
			status = 'superseded', ended_at = now(), updated_at = now()
		WHERE id = $1 AND owner_id = $2 AND sandbox_id = $3
		  AND status IN ('pending', 'failing')
		RETURNING `+stopCols,
		op.ID, owner, op.SandboxID)
	superseded, err := scanStop(row)
	if err != nil {
		return StopOperation{}, errNoRows("supersede stop", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE jobs SET state = 'cancelled', updated_at = now()
		WHERE owner_id = $1 AND sandbox_id = $2 AND kind = 'sandbox.stop' AND state = 'queued'`,
		owner, op.SandboxID); err != nil {
		return StopOperation{}, fmtError("cancel stop jobs", err)
	}
	return superseded, nil
}

// latestProvisionStateTx returns the state of the canonical initial provision
// job, or "" when the sandbox never had one.
func latestProvisionStateTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, sandboxID string) (JobState, error) {
	var state JobState
	err := tx.QueryRow(ctx, `
		SELECT state FROM jobs
		WHERE owner_id = $1 AND sandbox_id = $2 AND kind = 'sandbox.provision'
		ORDER BY created_at DESC, id DESC LIMIT 1`,
		owner, sandboxID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmtError("latest provision state", err)
	}
	return state, nil
}

// FinalizeResumeParams identifies the resume attempt one worker finalizes.
// Generation must be the generation from the [ResumePayload]; Version is the
// row version the worker loaded (0 skips the check).
type FinalizeResumeParams struct {
	Owner      uuid.UUID
	SandboxID  string
	Generation int64
	Version    int64
}

// FinalizeSandboxResume completes a resume after StartVM succeeded, fenced by
// the actual [JobResume] claim, the lifecycle generation from its payload and
// the row version. It moves provisioning to ready and clears the stop
// timestamp. For a cross-node resume (payload FromManifest set) it also sets
// disk_node_id to the node the disk was restored on, hydrated_at and the
// resume job's source generation as the restore source (the chunk collector
// keeps it), and records [EventSandboxHydrated]. A stale generation, wrong state or version mismatch answers
// [ErrConflict]; a lost claim [ErrJobNotLeased]; a deleted or foreign sandbox
// [ErrNotFound].
func (s *Store) FinalizeSandboxResume(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p FinalizeResumeParams) (Sandbox, error) {
	if p.Owner == uuid.Nil() {
		return Sandbox{}, ErrNoOwner
	}
	var out Sandbox
	err := s.runTx(ctx, "finalize sandbox resume", func(ctx context.Context, tx pgx.Tx) error {
		payload, err := lockJobKindFenceTx(ctx, tx, jobID, worker, token, p.Owner, p.SandboxID, JobResume)
		if err != nil {
			return err
		}
		if p.Generation <= 0 {
			return ErrConflict
		}
		claimed, err := decodeResumePayload(payload)
		if err != nil {
			return err
		}
		if claimed.Generation != p.Generation || claimed.VMID != p.SandboxID {
			return ErrInvalidJobPayload
		}
		sbx, err := sandboxRowForUpdateTx(ctx, tx, p.Owner, p.SandboxID)
		if err != nil {
			return err
		}
		if sbx.LifecycleGeneration != p.Generation {
			return ErrConflict
		}
		if sbx.State != StateProvisioning {
			return ErrConflict
		}
		if p.Version != 0 && sbx.Version != p.Version {
			return ErrConflict
		}
		// A cross-node resume restored the disk on the payload's
		// node: it is local there now, and fully hydrated (restores are
		// eager).
		elsewhere := claimed.FromManifest != ""
		if elsewhere && derefString(sbx.NodeID) != claimed.NodeID {
			return ErrConflict
		}
		row := tx.QueryRow(ctx, `
			UPDATE sandboxes SET
				state = 'ready', version = version + 1,
				stopped_at = NULL, error_reason = '',
				disk_node_id = CASE WHEN $5 THEN node_id ELSE disk_node_id END,
				hydrated_at = CASE WHEN $5 THEN now() ELSE hydrated_at END,
				restore_source_generation_id = CASE WHEN $5
					THEN (SELECT source_generation_id FROM jobs WHERE id = $6)
					ELSE restore_source_generation_id END,
				ssh_endpoint = NULL, ssh_host_key = NULL, updated_at = now()
			WHERE id = $1 AND owner_id = $2 AND version = $3
			  AND state = 'provisioning' AND lifecycle_generation = $4
			  AND deleted_at IS NULL
			RETURNING `+sandboxCols,
			p.SandboxID, p.Owner, sbx.Version, p.Generation, elsewhere, jobID)
		out, err = scanSandbox(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return fmtError("finalize resume transition", err)
		}
		if err := insertEventTx(ctx, tx, out, EventInput{Type: EventStatePrefix + string(StateReady)}); err != nil {
			return err
		}
		if elsewhere {
			return insertEventTx(ctx, tx, out, EventInput{Type: EventSandboxHydrated})
		}
		return nil
	})
	if err != nil {
		return Sandbox{}, err
	}
	return out, nil
}

// decodeResumePayload decodes a claimed [JobResume] payload, failing closed
// on malformed content.
func decodeResumePayload(payload []byte) (ResumePayload, error) {
	var p ResumePayload
	if err := jsonUnmarshal(payload, &p); err != nil {
		return ResumePayload{}, ErrInvalidJobPayload
	}
	return p, nil
}
