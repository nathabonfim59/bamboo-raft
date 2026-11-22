package db

// Multi-node placement: sandboxes created from an object-store
// generation, batch creates and moving a boot that a node refused to another
// node.

import (
	"bytes"
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// EventSandboxRescheduled is recorded when a sandbox's boot moves to another
// node before it ever became ready (the first node could not restore it).
const EventSandboxRescheduled = "sandbox.rescheduled"

// gibBytes is one GiB in bytes.
const gibBytes = 1 << 30

// DiskGiBFor is the smallest whole number of GiB that holds diskBytes.
func DiskGiBFor(diskBytes uint64) uint64 {
	return diskBytes/gibBytes + min(diskBytes%gibBytes, 1)
}

// lockGenerationSourceTx locks the generation a create restores from FOR
// SHARE, so it cannot be purged while the transaction commits, and returns its
// image and owner. The row must be completed, unpurged and carry manifestKey,
// else [ErrSnapshotNotFound]; reserve must hold its disk, else
// [ErrSnapshotDiskTooSmall]. A cross-account generation is authorized by the
// caller with a share code checked in the same transaction.
func lockGenerationSourceTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, manifestKey string, reserve Resources) (string, uuid.UUID, error) {
	var (
		key   string
		image *string
		disk  *int64
		owner uuid.UUID
	)
	err := tx.QueryRow(ctx, `
		SELECT manifest_key, image, disk_bytes, owner_id FROM snapshots
		WHERE id = $1 AND status = 'completed' AND purged_at IS NULL
		FOR SHARE`, id).Scan(&key, &image, &disk, &owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", uuid.Nil(), ErrSnapshotNotFound
	}
	if err != nil {
		return "", uuid.Nil(), fmtError("lock generation source", err)
	}
	// A completed row always has image and disk_bytes (schema check).
	if key != manifestKey || image == nil || disk == nil {
		return "", uuid.Nil(), ErrSnapshotNotFound
	}
	if reserve.DiskGiB < DiskGiBFor(uint64(*disk)) {
		return "", uuid.Nil(), ErrSnapshotDiskTooSmall
	}
	return *image, owner, nil
}

// CreateSandboxes creates a batch of sandboxes (a create with count) in ONE
// transaction: if any reservation or source check fails, nothing is created.
// Every element follows [Store.CreateSandbox]'s rules and must belong to the
// same owner. Only the first element's IdempotencyKey/RequestSHA256 is used
// (the others must leave the key empty): it keys the whole batch, and a
// replay returns every sandbox of it in creation order. The result is in the
// order of ps.
func (s *Store) CreateSandboxes(ctx context.Context, ps []CreateSandboxParams) ([]CreateSandboxResult, error) {
	if len(ps) == 0 {
		return nil, errors.New("db: create sandboxes needs at least one sandbox")
	}
	preps := make([]preparedCreate, len(ps))
	for i, p := range ps {
		if p.Owner != ps[0].Owner {
			return nil, errors.New("db: a create batch belongs to one owner")
		}
		if i > 0 && p.IdempotencyKey != "" {
			return nil, errors.New("db: only the first sandbox of a create batch carries the idempotency key")
		}
		prep, err := s.prepareCreate(p)
		if err != nil {
			return nil, err
		}
		preps[i] = prep
	}
	first := preps[0].p
	var out []CreateSandboxResult
	err := s.runTx(ctx, "create sandboxes", func(ctx context.Context, tx pgx.Tx) error {
		if first.IdempotencyKey != "" {
			if err := lockCreateKeyTx(ctx, tx, first.Owner, first.IdempotencyKey); err != nil {
				return err
			}
			res, found, err := s.replayCreateBatchTx(ctx, tx, first.Owner, first.IdempotencyKey, first.RequestSHA256)
			if err != nil {
				return err
			}
			if found {
				out = res
				return nil
			}
		}
		out = make([]CreateSandboxResult, 0, len(preps))
		ids := make([]string, 0, len(preps))
		for _, prep := range preps {
			res, err := createSandboxTx(ctx, tx, prep)
			if err != nil {
				return err
			}
			out = append(out, res)
			ids = append(ids, prep.p.ID)
		}
		if first.IdempotencyKey != "" {
			return insertCreateKeyTx(ctx, tx, first, ids)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ReplayCreateSandboxBatch answers the batch create stored on an idempotency
// key, in creation order: the admission fast path of a create with count. A
// key stored by a single create replays as a batch of one. It serializes with
// the create on the owner+key advisory lock like [Store.ReplayCreateSandbox];
// a different request hash answers [ErrIdempotencyReused]; expired or absent
// keys report found=false.
func (s *Store) ReplayCreateSandboxBatch(ctx context.Context, owner uuid.UUID, key string, requestSHA256 []byte) ([]CreateSandboxResult, bool, error) {
	if owner == uuid.Nil() || key == "" {
		return nil, false, nil
	}
	var (
		out   []CreateSandboxResult
		found bool
	)
	err := s.runTx(ctx, "replay create batch", func(ctx context.Context, tx pgx.Tx) error {
		if err := lockCreateKeyTx(ctx, tx, owner, key); err != nil {
			return err
		}
		var err error
		out, found, err = s.replayCreateBatchTx(ctx, tx, owner, key, requestSHA256)
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return out, found, nil
}

// replayCreateBatchTx reads the sandboxes stored on an unexpired key, in
// batch order, inside the caller's transaction.
func (s *Store) replayCreateBatchTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, key string, requestSHA256 []byte) ([]CreateSandboxResult, bool, error) {
	var (
		hash      []byte
		sandboxID string
		batch     []string
	)
	err := tx.QueryRow(ctx,
		`SELECT request_sha256, sandbox_id, batch_sandbox_ids FROM idempotency_keys
		 WHERE owner_id = $1 AND key = $2 AND expires_at > now()`,
		owner, key).Scan(&hash, &sandboxID, &batch)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmtError("idempotency batch lookup", err)
	}
	if !bytes.Equal(hash, requestSHA256) {
		return nil, true, ErrIdempotencyReused
	}
	if len(batch) == 0 {
		batch = []string{sandboxID}
	}
	out := make([]CreateSandboxResult, 0, len(batch))
	for _, id := range batch {
		sbx, err := sandboxRow(ctx, tx,
			`SELECT `+sandboxCols+` FROM sandboxes WHERE id = $1 AND owner_id = $2`, id, owner)
		if err != nil {
			return nil, true, err
		}
		job, err := latestJob(ctx, tx, owner, id, JobProvision)
		if err != nil {
			return nil, true, err
		}
		out = append(out, CreateSandboxResult{Sandbox: sbx, Job: job, Replayed: true})
	}
	return out, true, nil
}

// ReadmitParams moves one manifest restore's first boot to another node.
// ExpectedVersion is the row version the worker read under its guard;
// FromNode the node that refused the restore; ToNode the placed replacement.
type ReadmitParams struct {
	Owner           uuid.UUID
	SandboxID       string
	ExpectedVersion int64
	FromNode        string
	ToNode          string
}

// ReadmitSandbox moves the initial boot of a sandbox restored from a manifest
// from FromNode to ToNode in one transaction fenced by the live claim of the
// provision job jobID: it reserves the sandbox's reservation on ToNode
// ([ErrNoCapacity] when full, [ErrNotFound] when the node row is gone),
// releases it on FromNode, points node_id and disk_node_id at ToNode, records
// [EventSandboxRescheduled], enqueues a new provision job for ToNode that
// carries the same source generation, and marks jobID done. The sandbox keeps
// its id and lifecycle generation zero, so it is still an initial provision
// to stop, resume and failure reconciliation. The row must be in
// provisioning (or archiving: a stop accepted during the boot still needs the
// disk), at ExpectedVersion, generation zero, on FromNode and from a
// manifest, else [ErrConflict]; a deleted row answers [ErrNotFound] and a
// lapsed claim [ErrJobNotLeased].
func (s *Store) ReadmitSandbox(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p ReadmitParams) (Sandbox, Job, error) {
	if p.Owner == uuid.Nil() {
		return Sandbox{}, Job{}, ErrNoOwner
	}
	if p.FromNode == "" || p.ToNode == "" || p.FromNode == p.ToNode {
		return Sandbox{}, Job{}, errors.New("db: readmit needs two different nodes")
	}
	var (
		out    Sandbox
		newJob Job
	)
	err := s.runTx(ctx, "readmit sandbox", func(ctx context.Context, tx pgx.Tx) error {
		if err := lockJobFenceTx(ctx, tx, jobID, worker, token, p.Owner, p.SandboxID); err != nil {
			return err
		}
		var (
			kind     JobKind
			payload  []byte
			attempts int
			source   *uuid.UUID
		)
		if err := tx.QueryRow(ctx,
			`SELECT kind, payload, max_attempts, source_generation_id FROM jobs WHERE id = $1`,
			jobID).Scan(&kind, &payload, &attempts, &source); err != nil {
			return fmtError("readmit job read", err)
		}
		var old ProvisionPayload
		if kind != JobProvision || jsonUnmarshal(payload, &old) != nil {
			return ErrInvalidJobPayload
		}

		sbx, err := scanSandbox(tx.QueryRow(ctx, `SELECT `+sandboxCols+` FROM sandboxes
			WHERE id = $1 AND owner_id = $2 FOR UPDATE`, p.SandboxID, p.Owner))
		if err != nil {
			return errNoRows("readmit sandbox read", err)
		}
		switch {
		case sbx.DeletedAt != nil:
			return ErrNotFound
		case sbx.Version != p.ExpectedVersion, sbx.LifecycleGeneration != 0,
			sbx.State != StateProvisioning && sbx.State != StateArchiving,
			sbx.NodeID == nil || *sbx.NodeID != p.FromNode,
			sbx.FromManifestKey == "" || old.FromManifest != sbx.FromManifestKey:
			return ErrConflict
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
			return fmtError("readmit reserve", err)
		}
		if tag.RowsAffected() == 0 {
			return noCapacityOrMissing(ctx, tx, p.ToNode)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE nodes SET
				reserved_vcpus = GREATEST(reserved_vcpus - $2, 0),
				reserved_memory_mib = GREATEST(reserved_memory_mib - $3, 0),
				reserved_disk_gib = GREATEST(reserved_disk_gib - $4, 0),
				updated_at = now()
			WHERE id = $1`, p.FromNode, r.VCPUs, r.MemoryMiB, r.DiskGiB); err != nil {
			return fmtError("readmit release", err)
		}

		out, err = scanSandbox(tx.QueryRow(ctx, `
			UPDATE sandboxes SET node_id = $2, disk_node_id = $2, version = version + 1, updated_at = now()
			WHERE id = $1
			RETURNING `+sandboxCols, p.SandboxID, p.ToNode))
		if err != nil {
			return fmtError("readmit sandbox move", err)
		}
		if err := insertEventTx(ctx, tx, out, EventInput{
			Type: EventSandboxRescheduled,
			Data: `{"reason":"node_cannot_restore"}`,
		}); err != nil {
			return err
		}

		next, err := marshalJobPayload(ProvisionPayload{
			NodeID:       p.ToNode,
			Resources:    out.Reserved,
			FromManifest: out.FromManifestKey,
			Image:        old.Image,
		})
		if err != nil {
			return err
		}
		newJob, err = enqueueJobTx(ctx, tx, JobInput{
			Kind:               JobProvision,
			OwnerID:            p.Owner,
			SandboxID:          &p.SandboxID,
			Payload:            next,
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
			return fmtError("readmit complete job", err)
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
