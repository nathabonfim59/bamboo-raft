package db

// Forking: a fork is a new sandbox on a new chain whose disk is
// restored from one generation of the source's chain. The API role cannot cut
// that generation itself (it needs the source's guard and a node call), so the
// fork is admitted here with a [JobFork] and handed to its provision job by
// [Store.StartFork] once the lifecycle role has the generation.

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// ErrNodeGone means the node row a reservation was made against does not
// exist. Unlike [ErrNotFound] it does not name an owner-scoped row, so
// placement may retry another candidate.
var ErrNodeGone = errors.New("db: node row is gone")

// ForkAdmitParams is the input of [Store.AdmitFork]. The caller generated ID
// (ids.Sandbox) and chose NodeID through placement; the reservation is
// re-verified inside the transaction.
type ForkAdmitParams struct {
	ID          string
	Owner       uuid.UUID
	SourceID    string
	MachineType string
	Name        string
	Labels      map[string]string
	// Env is the plaintext env the fork request supplied. When InheritEnv is
	// true it is ignored and the source's sealed env is copied inside the
	// transaction, so the source env never leaves the database.
	Env         map[string]string
	InheritEnv  bool
	TTLSeconds  *int
	NodeID      string
	Reserve     Resources
	MaxAttempts int
	// IdempotencyKey and RequestSHA256 key the fork exactly once, like a
	// create.
	IdempotencyKey string
	RequestSHA256  []byte
	// SourceGenerationID pins the source generation the fork job must use.
	// It is set for a stopped (archived) source, whose chain sees no new
	// generation, and nil for a ready source the job cuts a fresh one from.
	SourceGenerationID *uuid.UUID
}

// AdmitFork writes one accepted fork in a single transaction: it rechecks the
// owner-scoped source (flags, expiry, state), pins the source generation when
// one is given, reserves the fork sandbox on NodeID, inserts the sandbox on a
// new chain in provisioning with a fresh state event, and enqueues the
// [JobFork] that hands it to its provision job. The idempotency key is
// honoured exactly like a create, in its own namespace (a create key never
// replays a fork and vice versa): a replay returns the original fork sandbox
// and its job. A repeated key with a different request hash answers
// [ErrIdempotencyReused]; a concurrent same-key write answers
// [ErrIdempotencyInProgress].
//
// An inherited env is opened with the source id's AAD and sealed again for the
// fork, because env ciphertext is bound to its sandbox id; the plaintext never
// leaves this package.
func (s *Store) AdmitFork(ctx context.Context, p ForkAdmitParams) (CreateSandboxResult, error) {
	if p.Owner == uuid.Nil() {
		return CreateSandboxResult{}, ErrNoOwner
	}
	if p.ID == "" || p.SourceID == "" || p.NodeID == "" {
		return CreateSandboxResult{}, errors.New("db: fork admission needs a sandbox id, a source and a node")
	}
	if err := ValidateTTLSeconds(p.TTLSeconds); err != nil {
		return CreateSandboxResult{}, err
	}
	labels, err := marshalStringMap(p.Labels)
	if err != nil {
		return CreateSandboxResult{}, err
	}
	// The request's env is sealed for the fork id. An inherited env is opened
	// with the source id's AAD and sealed again for the fork inside the
	// transaction; its plaintext never leaves this package.
	var requestEnv []byte
	if !p.InheritEnv {
		if requestEnv, err = s.sealEnv(p.Env, p.Owner, p.ID); err != nil {
			return CreateSandboxResult{}, err
		}
	}
	if p.MachineType == "" {
		p.MachineType = "default"
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = DefaultMaxAttempts
	}

	// A fork key lives in its own namespace: the same value sent to a create
	// must not replay the other operation's sandbox.
	key := ""
	if p.IdempotencyKey != "" {
		key = "fork:" + p.IdempotencyKey
	}
	var out CreateSandboxResult
	err = s.runTx(ctx, "admit fork", func(ctx context.Context, tx pgx.Tx) error {
		if key != "" {
			if err := lockCreateKeyTx(ctx, tx, p.Owner, key); err != nil {
				return err
			}
			res, found, err := s.replayForkTx(ctx, tx, p.Owner, key, p.RequestSHA256)
			if err != nil {
				return err
			}
			if found {
				out = res
				return nil
			}
		}

		source, err := forkSourceTx(ctx, tx, p.Owner, p.SourceID)
		if err != nil {
			return err
		}
		forkEnv := requestEnv
		if p.InheritEnv {
			if forkEnv, err = s.forkEnv(source.envSealed, p.Owner, p.SourceID, p.ID); err != nil {
				return err
			}
		}
		if p.SourceGenerationID != nil {
			if _, err := lockForkGenerationTx(ctx, tx, p.Owner, p.SourceID, *p.SourceGenerationID, p.Reserve); err != nil {
				return err
			}
		}
		if err := reserveNodeTx(ctx, tx, p.NodeID, p.Reserve); err != nil {
			return err
		}

		row := tx.QueryRow(ctx, `
			INSERT INTO sandboxes (id, owner_id, state, machine_type, name, labels, env_sealed,
				node_id, reserved_vcpus, reserved_memory_mib, reserved_disk_gib,
				desired_vcpus, desired_memory_mib, desired_disk_gib,
				expires_at, ttl_seconds,
				chain_id, snapshots_enabled, disk_node_id, created_at)
			VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::bytea,
				$8, $9, $10, $11, $9, $10, $11,
				CASE WHEN $12::integer IS NULL THEN NULL
				     ELSE now() + make_interval(secs => $12::integer) END,
				$12, $13, true, $8, now())
			RETURNING `+sandboxCols,
			p.ID, p.Owner, StateProvisioning, p.MachineType, p.Name, labels, forkEnv,
			p.NodeID, p.Reserve.VCPUs, p.Reserve.MemoryMiB, p.Reserve.DiskGiB,
			p.TTLSeconds, uuid.NewV7())
		out.Sandbox, err = scanSandbox(row)
		if err != nil {
			return fmtError("admit fork insert", err)
		}
		if err := insertEventTx(ctx, tx, out.Sandbox, EventInput{
			Type: EventStatePrefix + string(StateProvisioning),
		}); err != nil {
			return err
		}
		payload, err := marshalJobPayload(ForkPayload{SourceID: p.SourceID, NodeID: p.NodeID})
		if err != nil {
			return err
		}
		out.Job, err = enqueueJobTx(ctx, tx, JobInput{
			Kind:               JobFork,
			OwnerID:            p.Owner,
			SandboxID:          &p.ID,
			Payload:            payload,
			MaxAttempts:        p.MaxAttempts,
			SourceGenerationID: p.SourceGenerationID,
		})
		if err != nil {
			return err
		}
		if key != "" {
			return insertCreateKeyTx(ctx, tx, CreateSandboxParams{
				ID: p.ID, Owner: p.Owner, IdempotencyKey: key,
				RequestSHA256: p.RequestSHA256,
			}, nil)
		}
		return nil
	})
	if err != nil {
		return CreateSandboxResult{}, err
	}
	return out, nil
}

// forkSource is the source state [Store.AdmitFork] re-reads under its row
// lock before reserving anything.
type forkSource struct {
	envSealed []byte
}

// forkEnv re-seals the source's env for the fork sandbox. Env ciphertext is
// bound to the owner and sandbox id, so a copy would not decrypt for the fork:
// the source's plaintext is opened with the source id's AAD and sealed again
// for the fork id, all inside internal/db.
func (s *Store) forkEnv(sealed []byte, owner uuid.UUID, sourceID, forkID string) ([]byte, error) {
	if sealed == nil {
		return nil, nil
	}
	if s.env.gcm == nil {
		return nil, errStoreNotMigrated
	}
	plain, err := s.env.open(sealed, aad(owner, sourceID, "env"))
	if err != nil {
		return nil, err
	}
	env, err := decodeEnv(plain)
	if err != nil {
		return nil, err
	}
	return s.sealEnv(env, owner, forkID)
}

// forkSourceTx locks the owner's live source sandbox FOR SHARE and refuses it
// with the codes the v1 API defines: a sandbox created with snapshots off
// ([ErrSnapshotsDisabled]) and one whose data expired
// ([ErrSnapshotDataExpired]) can never be forked. The row is locked so the
// sealed env copy cannot race a resume that replaces it.
func forkSourceTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, sourceID string) (forkSource, error) {
	var (
		source  forkSource
		enabled bool
		expired *time.Time
		sealed  *[]byte
	)
	err := tx.QueryRow(ctx, `
		SELECT snapshots_enabled, data_expired_at, env_sealed
		FROM sandboxes
		WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL
		FOR SHARE`, sourceID, owner).Scan(&enabled, &expired, &sealed)
	if errors.Is(err, pgx.ErrNoRows) {
		return forkSource{}, ErrNotFound
	}
	if err != nil {
		return forkSource{}, fmtError("lock fork source", err)
	}
	if !enabled {
		return forkSource{}, ErrSnapshotsDisabled
	}
	if expired != nil {
		return forkSource{}, ErrSnapshotDataExpired
	}
	if sealed != nil {
		source.envSealed = *sealed
	}
	return source, nil
}

// lockForkGenerationTx locks one completed, unpurged generation of the owner's
// source sandbox FOR SHARE and checks that reserve holds its disk. It is the
// admission-time pin for a stopped source; [Store.StartFork] validates the
// same row again when it hands the fork to its provision job.
func lockForkGenerationTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, sourceID string, id uuid.UUID, reserve Resources) (Generation, error) {
	gen, err := scanGeneration(tx.QueryRow(ctx, `
		SELECT `+generationCols+` FROM snapshots
		WHERE id = $1 AND owner_id = $2 AND sandbox_id = $3
		  AND status = 'completed' AND purged_at IS NULL
		FOR SHARE`, id, owner, sourceID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Generation{}, ErrSnapshotNotFound
	}
	if err != nil {
		return Generation{}, fmtError("lock fork generation", err)
	}
	if reserve.DiskGiB < DiskGiBFor(gen.DiskBytes) {
		return Generation{}, ErrSnapshotDiskTooSmall
	}
	return gen, nil
}

// reserveNodeTx makes the fork sandbox's reservation on nodeID, refusing with
// [ErrNoCapacity] when the node is full and [ErrNodeGone] when its row is
// gone.
func reserveNodeTx(ctx context.Context, tx pgx.Tx, nodeID string, r Resources) error {
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
		nodeID, r.VCPUs, r.MemoryMiB, r.DiskGiB)
	if err != nil {
		return fmtError("admit fork reserve", err)
	}
	if tag.RowsAffected() == 0 {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM nodes WHERE id = $1)`, nodeID).
			Scan(&exists); err != nil {
			return fmtError("admit fork check node", err)
		}
		if !exists {
			return ErrNodeGone
		}
		return ErrNoCapacity
	}
	return nil
}

// replayForkTx returns the fork stored on an existing unexpired idempotency
// key inside the caller's transaction. found is false when the key is absent
// or expired and the fork should proceed.
func (s *Store) replayForkTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, key string, requestSHA256 []byte) (CreateSandboxResult, bool, error) {
	var (
		hash      []byte
		sandboxID string
	)
	err := tx.QueryRow(ctx,
		`SELECT request_sha256, sandbox_id FROM idempotency_keys
		 WHERE owner_id = $1 AND key = $2 AND expires_at > now()`,
		owner, key).Scan(&hash, &sandboxID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CreateSandboxResult{}, false, nil // fresh or expired key
		}
		return CreateSandboxResult{}, false, fmtError("fork idempotency lookup", err)
	}
	if !bytes.Equal(hash, requestSHA256) {
		return CreateSandboxResult{}, true, ErrIdempotencyReused
	}
	sandbox, err := sandboxRow(ctx, tx,
		`SELECT `+sandboxCols+` FROM sandboxes WHERE id = $1 AND owner_id = $2`, sandboxID, owner)
	if err != nil {
		return CreateSandboxResult{}, true, err
	}
	job, err := latestJob(ctx, tx, owner, sandboxID, JobFork)
	if err != nil {
		return CreateSandboxResult{}, true, err
	}
	return CreateSandboxResult{Sandbox: sandbox, Job: job, Replayed: true}, true, nil
}

// ReplayFork answers the fork stored on an idempotency key without placing
// anything: the admission fast path, and the final recheck after placement
// failed, so a client retry of a fork that was already accepted never loses it
// to a capacity refusal. The key lives in the fork namespace, so a create key
// with the same value does not match. A different request hash answers
// [ErrIdempotencyReused]; an absent, expired or empty key reports found=false.
func (s *Store) ReplayFork(ctx context.Context, owner uuid.UUID, key string, requestSHA256 []byte) (CreateSandboxResult, bool, error) {
	if owner == uuid.Nil() || key == "" {
		return CreateSandboxResult{}, false, nil
	}
	namespaced := "fork:" + key
	var (
		out   CreateSandboxResult
		found bool
	)
	err := s.runTx(ctx, "replay fork", func(ctx context.Context, tx pgx.Tx) error {
		if err := lockCreateKeyTx(ctx, tx, owner, namespaced); err != nil {
			return err
		}
		var err error
		out, found, err = s.replayForkTx(ctx, tx, owner, namespaced, requestSHA256)
		return err
	})
	if err != nil {
		return CreateSandboxResult{}, false, err
	}
	return out, found, nil
}

// StartForkParams binds the resolved source generation to a fork sandbox. The
// values come from the claimed [JobFork] and the canonical fork row, never
// from a client.
type StartForkParams struct {
	ForkID       string
	Owner        uuid.UUID
	SourceID     string
	NodeID       string
	GenerationID uuid.UUID
}

// StartFork hands a fork sandbox to its provision job in one transaction
// fenced by the live claim of the [JobFork] jobID: it re-validates that the
// generation is one completed, unpurged generation of the source that fits the
// fork's reservation, records it as the fork's manifest, enqueues the
// provision job that boots CreateVM from_manifest (carrying the generation so
// the chunk collector keeps it), and marks the fork job done. The hand-off is
// idempotent: a fork that already has a provision job returns it without
// enqueuing a second one, so a retried or redelivered attempt can never boot
// the same fork twice. A deleted fork answers [ErrNotFound]; a lapsed claim
// [ErrJobNotLeased]; a fork whose state no longer accepts a hand-off (ready,
// archived, error) returns its latest provision job and writes nothing.
func (s *Store) StartFork(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p StartForkParams) (Job, error) {
	if p.Owner == uuid.Nil() {
		return Job{}, ErrNoOwner
	}
	if p.ForkID == "" || p.SourceID == "" || p.NodeID == "" || p.GenerationID == uuid.Nil() {
		return Job{}, errors.New("db: start fork needs the fork, its source, its node and a generation")
	}
	var out Job
	err := s.runTx(ctx, "start fork", func(ctx context.Context, tx pgx.Tx) error {
		if err := lockJobFenceTx(ctx, tx, jobID, worker, token, p.Owner, p.ForkID); err != nil {
			return err
		}
		var (
			kind         JobKind
			claimPayload []byte
		)
		if err := tx.QueryRow(ctx,
			`SELECT kind, payload FROM jobs WHERE id = $1`, jobID).Scan(&kind, &claimPayload); err != nil {
			return fmtError("start fork job read", err)
		}
		var jobPayload ForkPayload
		if kind != JobFork || jsonUnmarshal(claimPayload, &jobPayload) != nil ||
			jobPayload.SourceID != p.SourceID || jobPayload.NodeID != p.NodeID {
			return ErrInvalidJobPayload
		}

		fork, err := scanSandbox(tx.QueryRow(ctx, `SELECT `+sandboxCols+` FROM sandboxes
			WHERE id = $1 AND owner_id = $2 FOR UPDATE`, p.ForkID, p.Owner))
		if err != nil {
			return errNoRows("start fork sandbox read", err)
		}
		if fork.DeletedAt != nil {
			return ErrNotFound
		}
		if fork.NodeID == nil || *fork.NodeID != p.NodeID {
			return ErrConflict
		}
		// A stop accepted during the first boot still needs the disk this
		// hand-off creates, so archiving at generation zero is allowed.
		handOff := fork.State == StateProvisioning ||
			(fork.State == StateArchiving && fork.LifecycleGeneration == 0)
		existing, err := latestJob(ctx, tx, p.Owner, p.ForkID, JobProvision)
		if err != nil {
			return err
		}
		if !handOff || existing.ID != uuid.Nil() {
			out = existing
			return completeForkJobTx(ctx, tx, jobID, worker, token)
		}

		gen, err := lockForkGenerationTx(ctx, tx, p.Owner, p.SourceID, p.GenerationID, fork.Reserved)
		if err != nil {
			return err
		}
		if gen.Image == "" || gen.ManifestKey == "" {
			return ErrSnapshotNotFound
		}
		if _, err := tx.Exec(ctx,
			`UPDATE sandboxes SET from_manifest_key = $2, updated_at = now() WHERE id = $1`,
			p.ForkID, gen.ManifestKey); err != nil {
			return fmtError("start fork manifest", err)
		}
		provisionPayload, err := marshalJobPayload(ProvisionPayload{
			NodeID:       p.NodeID,
			Resources:    fork.Reserved,
			FromManifest: gen.ManifestKey,
			Image:        gen.Image,
		})
		if err != nil {
			return err
		}
		out, err = enqueueJobTx(ctx, tx, JobInput{
			Kind:               JobProvision,
			OwnerID:            p.Owner,
			SandboxID:          &p.ForkID,
			Payload:            provisionPayload,
			SourceGenerationID: &gen.ID,
		})
		if err != nil {
			return err
		}
		return completeForkJobTx(ctx, tx, jobID, worker, token)
	})
	if err != nil {
		return Job{}, err
	}
	return out, nil
}

// completeForkJobTx marks the fork job done under the caller's live claim.
func completeForkJobTx(ctx context.Context, tx pgx.Tx, jobID uuid.UUID, worker string, token uuid.UUID) error {
	tag, err := tx.Exec(ctx, `
		UPDATE jobs SET state = 'done', leased_by = '', leased_until = NULL, lease_token = NULL,
			updated_at = now()
		WHERE id = $1 AND `+jobFence, jobID, worker, token)
	if err != nil {
		return fmtError("complete fork job", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotLeased
	}
	return nil
}

// GenerationByID returns one completed, unpurged generation of the owner's
// source sandbox. It is how the [JobFork] handler reads the generation
// admission pinned for a stopped source; a foreign, purged, failed or unknown
// row answers [ErrNotFound].
func (s *Store) GenerationByID(ctx context.Context, owner uuid.UUID, sourceID string, id uuid.UUID) (Generation, error) {
	gen, err := scanGeneration(s.pool.QueryRow(ctx, `
		SELECT `+generationCols+` FROM snapshots
		WHERE id = $1 AND owner_id = $2 AND sandbox_id = $3
		  AND status = 'completed' AND purged_at IS NULL`, id, owner, sourceID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Generation{}, ErrNotFound
	}
	if err != nil {
		return Generation{}, fmtError("read generation", err)
	}
	return gen, nil
}
