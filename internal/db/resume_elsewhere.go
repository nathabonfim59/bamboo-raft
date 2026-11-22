package db

// Resume on another node. When the node that keeps an
// archived sandbox's disk cannot take the resume (full, disconnected, or the
// disk only exists in the object store), the resume restores the chain's last
// completed generation on another node with CreateVM from_manifest. The chain
// continues: chain_id is unchanged and the next generation number follows the
// last one.
//
// Admission is one transaction: the compute and disk are reserved on the new
// node, the retained disk's reservation on the old node is released, the old
// node's copy is recorded as an eviction, node_id moves to the new node and
// disk_node_id becomes NULL (the disk is only in the object store until the
// restore is READY), and the resume job records its source generation so the
// chunk collector keeps it.

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// Errors of resume admission on another node.
var (
	// ErrSnapshotDataExpired means the sandbox's stored data expired: there
	// is nothing to restore it from (409 snapshot_data_expired).
	ErrSnapshotDataExpired = errors.New("db: sandbox snapshot data expired")

	// ErrNoRestorableData means a stopped sandbox has neither a local disk
	// nor a completed generation to resume from (409 resume_failed).
	ErrNoRestorableData = errors.New("db: sandbox has no disk and no snapshot to resume from")
)

// ResumeElsewhereParams is one resume restored on ToNode from the completed
// generation GenerationID (whose manifest is ManifestKey). The embedded
// [ResumeParams] keep their meaning; SupersedeStopID is ignored, since only an
// archived sandbox resumes elsewhere.
type ResumeElsewhereParams struct {
	ResumeParams
	ToNode       string
	GenerationID uuid.UUID
	ManifestKey  string
}

// AcceptSandboxResumeElsewhere atomically accepts a resume of an archived
// sandbox on ToNode, restoring the disk from one of its own completed
// generations. In one transaction it locks the sandbox and the generation
// (FOR SHARE: it cannot be purged while this commits), grows the disk of the
// resolved machine shape to the generation's disk, releases the sandbox's
// old reservation, reserves the full shape on ToNode, moves the sandbox to
// provisioning on ToNode with a fresh lifecycle generation and no local disk,
// records the old disk node's copy as an eviction and enqueues a [JobResume]
// whose payload carries the manifest and whose source_generation_id is the
// generation.
//
// A deleted or foreign sandbox answers [ErrNotFound]; a state other than
// archived [ErrInvalidState]; snapshots off [ErrSnapshotsDisabled]; expired
// data [ErrSnapshotDataExpired]; a generation that is not this sandbox's
// current chain's, not completed, purged or with another manifest
// [ErrSnapshotNotFound]; ToNode being the disk node or holding an open
// eviction of the sandbox [ErrConflict]; a full ToNode [ErrNoCapacity]; an
// unknown ToNode [ErrNodeUnavailable]; a shape smaller than the retained disk
// [ErrTypeTooSmall].
func (s *Store) AcceptSandboxResumeElsewhere(ctx context.Context, p ResumeElsewhereParams) (ResumeAcceptance, error) {
	if p.Owner == uuid.Nil() {
		return ResumeAcceptance{}, ErrNoOwner
	}
	if p.ToNode == "" || p.ManifestKey == "" || p.GenerationID == uuid.Nil() {
		return ResumeAcceptance{}, errors.New("db: resume elsewhere needs a node and a generation")
	}
	var out ResumeAcceptance
	err := s.runTx(ctx, "accept sandbox resume elsewhere", func(ctx context.Context, tx pgx.Tx) error {
		sbx, err := sandboxRowForUpdateTx(ctx, tx, p.Owner, p.SandboxID)
		if err != nil {
			return err
		}
		if p.ExpectedVersion != 0 && sbx.Version != p.ExpectedVersion {
			return ErrConflict
		}
		switch {
		case sbx.State != StateArchived:
			return ErrInvalidState
		case !sbx.SnapshotsEnabled:
			return ErrSnapshotsDisabled
		case sbx.DataExpiredAt != nil:
			return ErrSnapshotDataExpired
		}
		desired, machineType, err := resumeShape(sbx, p.ResumeParams)
		if err != nil {
			return err
		}
		image, diskBytes, err := lockResumeGenerationTx(ctx, tx, p, sbx.ChainID)
		if err != nil {
			return err
		}
		desired.DiskGiB = max(desired.DiskGiB, DiskGiBFor(diskBytes))

		oldDisk := derefString(sbx.DiskNodeID)
		if p.ToNode == oldDisk {
			return ErrConflict // the disk node resumes in place
		}
		if open, err := openEvictionTx(ctx, tx, p.ToNode, sbx.ID); err != nil {
			return err
		} else if open {
			return ErrConflict
		}
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

		// Release first: ToNode may be the sandbox's old node_id when the
		// disk there is already gone.
		if err := releaseNodeTx(ctx, tx, sbx.NodeID, sbx.Reserved); err != nil {
			return err
		}
		if err := reserveResumeCapacityTx(ctx, tx, p.ToNode, desired); err != nil {
			return err
		}
		if _, err := transitionResumeTx(ctx, tx, sbx, p.ResumeParams, envSealed, desired, machineType, p.ToNode); err != nil {
			return err
		}
		out.Sandbox, err = scanSandbox(tx.QueryRow(ctx, `
			UPDATE sandboxes SET disk_node_id = NULL WHERE id = $1
			RETURNING `+sandboxCols, sbx.ID))
		if err != nil {
			return fmtError("resume elsewhere disk node", err)
		}
		if oldDisk != "" {
			if err := insertEvictionTx(ctx, tx, oldDisk, sbx.ID, EvictionReasonResumedElsewhere); err != nil {
				return err
			}
		}

		payload, err := marshalJobPayload(ResumePayload{
			NodeID:       p.ToNode,
			VMID:         out.Sandbox.ID,
			MachineType:  out.Sandbox.MachineType,
			Resources:    out.Sandbox.Desired,
			FromStopID:   derefString(out.Sandbox.StopOperationID),
			Generation:   out.Sandbox.LifecycleGeneration,
			FromManifest: p.ManifestKey,
			Image:        image,
			EvictNode:    oldDisk,
		})
		if err != nil {
			return err
		}
		out.Job, err = enqueueJobTx(ctx, tx, JobInput{
			Kind:               JobResume,
			OwnerID:            p.Owner,
			SandboxID:          &p.SandboxID,
			Payload:            payload,
			MaxAttempts:        p.MaxAttempts,
			SourceGenerationID: new(p.GenerationID),
		})
		if err != nil {
			return err
		}
		return insertEventTx(ctx, tx, out.Sandbox, EventInput{
			Type: "resume.accepted",
			Data: mustJSON(map[string]any{
				"generation": out.Sandbox.LifecycleGeneration,
				"restore":    "manifest",
			}),
		})
	})
	if err != nil {
		return ResumeAcceptance{}, err
	}
	return out, nil
}

// lockResumeGenerationTx locks the resume's source generation FOR SHARE and
// returns its image and disk size. It must be a completed, unpurged
// generation of this owner's sandbox on chain, with the expected manifest.
func lockResumeGenerationTx(ctx context.Context, tx pgx.Tx, p ResumeElsewhereParams, chain uuid.UUID) (string, uint64, error) {
	var (
		key   string
		image *string
		disk  *int64
	)
	err := tx.QueryRow(ctx, `
		SELECT manifest_key, image, disk_bytes FROM snapshots
		WHERE id = $1 AND owner_id = $2 AND sandbox_id = $3 AND chain_id = $4
		  AND status = 'completed' AND purged_at IS NULL
		FOR SHARE`, p.GenerationID, p.Owner, p.SandboxID, chain).Scan(&key, &image, &disk)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, ErrSnapshotNotFound
	}
	if err != nil {
		return "", 0, fmtError("lock resume generation", err)
	}
	// A completed row always has image and disk_bytes (schema check).
	if key != p.ManifestKey || image == nil || disk == nil || *disk < 0 {
		return "", 0, ErrSnapshotNotFound
	}
	return *image, uint64(*disk), nil
}

// releaseNodeTx returns r to nodeID's free capacity (nothing for a nil node
// or an empty r). The caller updates the sandbox row's reserved_* itself.
func releaseNodeTx(ctx context.Context, tx pgx.Tx, nodeID *string, r Resources) error {
	if nodeID == nil || *nodeID == "" || r == (Resources{}) {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE nodes SET
			reserved_vcpus = GREATEST(reserved_vcpus - $2, 0),
			reserved_memory_mib = GREATEST(reserved_memory_mib - $3, 0),
			reserved_disk_gib = GREATEST(reserved_disk_gib - $4, 0),
			updated_at = now()
		WHERE id = $1`, *nodeID, r.VCPUs, r.MemoryMiB, r.DiskGiB); err != nil {
		return fmtError("release node capacity", err)
	}
	return nil
}

// lockResumeElsewhereTx is [Store.lockResumeFailureTx] for a cross-node
// resume: the claimed payload must carry a manifest, and the row must still be
// on the payload's node.
func (s *Store) lockResumeElsewhereTx(ctx context.Context, tx pgx.Tx, jobID uuid.UUID, worker string, token uuid.UUID, p FinalizeResumeFailureParams) (Sandbox, ResumePayload, error) {
	sbx, err := s.lockResumeFailureTx(ctx, tx, jobID, worker, token, p)
	if err != nil {
		return Sandbox{}, ResumePayload{}, err
	}
	var raw []byte
	if err := tx.QueryRow(ctx, `SELECT payload FROM jobs WHERE id = $1`, jobID).Scan(&raw); err != nil {
		return Sandbox{}, ResumePayload{}, fmtError("resume elsewhere payload", err)
	}
	claimed, err := decodeResumePayload(raw)
	if err != nil {
		return Sandbox{}, ResumePayload{}, err
	}
	if claimed.FromManifest == "" {
		return Sandbox{}, ResumePayload{}, ErrInvalidJobPayload
	}
	if derefString(sbx.NodeID) != claimed.NodeID {
		return Sandbox{}, ResumePayload{}, ErrConflict
	}
	return sbx, claimed, nil
}

// FinalizeSandboxResumeElsewhereFailure ends a cross-node resume whose
// restore failed for good, after the worker's DeleteVM on the new node fenced
// any boot there. The sandbox goes back to archived at the same lifecycle
// generation with a safe error reason and no local disk (disk_node_id NULL),
// and every reservation it held on the new node is released: the disk only
// exists in the object store now, so the next resume places it again and
// restores the same chain. Fencing matches [Store.FinalizeSandboxResumeFailure];
// a claim whose payload is not a cross-node resume answers
// [ErrInvalidJobPayload].
func (s *Store) FinalizeSandboxResumeElsewhereFailure(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p FinalizeResumeFailureParams, reason string) (Sandbox, error) {
	if p.Owner == uuid.Nil() {
		return Sandbox{}, ErrNoOwner
	}
	var out Sandbox
	err := s.runTx(ctx, "finalize sandbox resume elsewhere failure", func(ctx context.Context, tx pgx.Tx) error {
		sbx, _, err := s.lockResumeElsewhereTx(ctx, tx, jobID, worker, token, p)
		if err != nil {
			return err
		}
		if err := releaseNodeTx(ctx, tx, sbx.NodeID, sbx.Reserved); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			UPDATE sandboxes SET
				state = 'archived', version = version + 1,
				reserved_vcpus = 0, reserved_memory_mib = 0, reserved_disk_gib = 0,
				disk_node_id = NULL,
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
			return fmtError("finalize resume elsewhere failure transition", err)
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

// MoveResumeParams moves a cross-node resume from FromNode, which refused the
// restore (no usable object store), to the placed ToNode. Generation and
// Version fence it like [FinalizeResumeFailureParams].
type MoveResumeParams struct {
	Owner      uuid.UUID
	SandboxID  string
	Generation int64
	Version    int64
	FromNode   string
	ToNode     string
}

// MoveResume moves a cross-node resume to another node in one transaction
// fenced by the live claim of the resume job jobID, the payload's lifecycle
// generation, the row version and the provisioning state: it reserves the
// sandbox's reservation on ToNode, releases it on FromNode, points node_id at
// ToNode, records [EventSandboxRescheduled], enqueues a new [JobResume] for
// ToNode with the same manifest, eviction and source generation, and marks
// jobID done. ToNode must differ from FromNode and from the payload's evicted
// node and hold no open eviction of the sandbox, else [ErrConflict]; a full
// ToNode answers [ErrNoCapacity], an unknown one [ErrNotFound]; a lapsed
// claim [ErrJobNotLeased].
func (s *Store) MoveResume(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p MoveResumeParams) (Sandbox, Job, error) {
	if p.Owner == uuid.Nil() {
		return Sandbox{}, Job{}, ErrNoOwner
	}
	if p.FromNode == "" || p.ToNode == "" || p.FromNode == p.ToNode {
		return Sandbox{}, Job{}, errors.New("db: move resume needs two different nodes")
	}
	var (
		out    Sandbox
		newJob Job
	)
	err := s.runTx(ctx, "move resume", func(ctx context.Context, tx pgx.Tx) error {
		sbx, claimed, err := s.lockResumeElsewhereTx(ctx, tx, jobID, worker, token, FinalizeResumeFailureParams{
			Owner: p.Owner, SandboxID: p.SandboxID, Generation: p.Generation, Version: p.Version,
		})
		if err != nil {
			return err
		}
		if claimed.NodeID != p.FromNode || p.ToNode == claimed.EvictNode {
			return ErrConflict
		}
		if open, err := openEvictionTx(ctx, tx, p.ToNode, sbx.ID); err != nil {
			return err
		} else if open {
			return ErrConflict
		}
		var (
			attempts int
			source   *uuid.UUID
		)
		if err := tx.QueryRow(ctx, `SELECT max_attempts, source_generation_id FROM jobs WHERE id = $1`,
			jobID).Scan(&attempts, &source); err != nil {
			return fmtError("move resume job read", err)
		}

		r := sbx.Reserved
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
			p.ToNode, r.VCPUs, r.MemoryMiB, r.DiskGiB)
		if err != nil {
			return fmtError("move resume reserve", err)
		}
		if tag.RowsAffected() == 0 {
			return noCapacityOrMissing(ctx, tx, p.ToNode)
		}
		if err := releaseNodeTx(ctx, tx, &p.FromNode, r); err != nil {
			return err
		}
		out, err = scanSandbox(tx.QueryRow(ctx, `
			UPDATE sandboxes SET node_id = $2, version = version + 1, updated_at = now()
			WHERE id = $1
			RETURNING `+sandboxCols, p.SandboxID, p.ToNode))
		if err != nil {
			return fmtError("move resume sandbox", err)
		}
		if err := insertEventTx(ctx, tx, out, EventInput{
			Type: EventSandboxRescheduled,
			Data: `{"reason":"node_cannot_restore"}`,
		}); err != nil {
			return err
		}

		next := claimed
		next.NodeID = p.ToNode
		payload, err := marshalJobPayload(next)
		if err != nil {
			return err
		}
		newJob, err = enqueueJobTx(ctx, tx, JobInput{
			Kind:               JobResume,
			OwnerID:            p.Owner,
			SandboxID:          &p.SandboxID,
			Payload:            payload,
			MaxAttempts:        attempts,
			SourceGenerationID: source,
		})
		if err != nil {
			return err
		}
		tag, err = tx.Exec(ctx, `
			UPDATE jobs SET state = 'done', leased_by = '', leased_until = NULL, lease_token = NULL,
				updated_at = now()
			WHERE id = $1 AND `+jobFence, jobID, worker, token)
		if err != nil {
			return fmtError("move resume complete job", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrJobNotLeased
		}
		return nil
	})
	if err != nil {
		return Sandbox{}, Job{}, err
	}
	return out, newJob, nil
}
