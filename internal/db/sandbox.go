package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// SandboxState values are the API Sandbox.state enum. Phase
// 0.5 uses Provisioning, Ready, Error and Cancelled; the rest exist so the
// column constraint covers the full wire enum.
type SandboxState string

const (
	StateInit         SandboxState = "init"
	StateProvisioning SandboxState = "provisioning"
	StateProvisioned  SandboxState = "provisioned"
	StateCloning      SandboxState = "cloning"
	StateReady        SandboxState = "ready"
	StateIdle         SandboxState = "idle"
	StateRunning      SandboxState = "running"
	StateArchiving    SandboxState = "archiving"
	StateArchived     SandboxState = "archived"
	StateError        SandboxState = "error"
	StateCancelled    SandboxState = "cancelled"
)

// ActiveStates returns the states in which a sandbox still holds a machine
// or is booting one; TTL expiry applies to these. A fresh slice every call:
// no package-level mutable state.
func ActiveStates() []SandboxState {
	return []SandboxState{
		StateProvisioning, StateProvisioned, StateCloning, StateReady, StateIdle, StateRunning,
	}
}

// Sandbox is one sandbox row. Plaintext env is not part of the row: it is
// sealed at rest (env_sealed) and only [Store.DecryptSandboxEnvForNode]
// opens it, so no ordinary read, log line or error can leak it.
type Sandbox struct {
	ID          string
	OwnerID     uuid.UUID
	State       SandboxState
	MachineType string
	Name        string
	Labels      map[string]string
	NodeID      *string
	Reserved    Resources
	// Desired is the machine shape the sandbox was created, resumed or
	// resized to. It survives a stop, when only the compute part of
	// Reserved is released, and is what a same-node resume reserves again.
	Desired Resources
	// TTLSeconds is the configured auto-stop duration, independent of the
	// absolute deadline in ExpiresAt. A resume resets ExpiresAt from it.
	TTLSeconds *int
	// Subdomain is the sandbox's globally unique public label, or nil
	// before one is assigned.
	Subdomain *string
	// StoppedAt is when the sandbox reached archived, nil while running.
	StoppedAt *time.Time
	// StopOperationID points at the latest durable stop request, kept even
	// after that stop ends.
	StopOperationID *string
	// LifecycleGeneration identifies the current boot attempt. Each
	// accepted resume increments it; resume and provision workers may only
	// act for the generation in their payload.
	LifecycleGeneration int64
	Version             int64
	ExpiresAt           *time.Time
	ErrorReason         string
	// SnapshotID pins the immutable saved-snapshot generation a restore was
	// accepted from. SnapshotPin stays true until the restore is confirmed
	// ready or its reservation is released after a confirmed DeleteVM.
	SnapshotID     *uuid.UUID
	SnapshotPin    bool
	FromSnapshotID string
	// SSH is the endpoint+host-key pair a successful AddSSHKey produced for
	// the current boot. It is nil until the API stores one and is cleared by
	// every state change that leaves the ready-ish states.
	SSH       *SSHAccess
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
}

// String renders the row without payload contents.
func (s Sandbox) String() string {
	var b strings.Builder
	b.WriteString(s.ID)
	b.WriteString(" state=")
	b.WriteString(string(s.State))
	if s.NodeID != nil {
		b.WriteString(" node=")
		b.WriteString(*s.NodeID)
	}
	if s.DeletedAt != nil {
		b.WriteString(" deleted")
	}
	return b.String()
}

const sandboxCols = `id, owner_id, state, machine_type, name, labels, node_id,
	reserved_vcpus, reserved_memory_mib, reserved_disk_gib,
	desired_vcpus, desired_memory_mib, desired_disk_gib,
	ttl_seconds, subdomain, stopped_at, stop_operation_id, lifecycle_generation,
	version, expires_at, error_reason, snapshot_id, snapshot_pin, from_snapshot_id,
	created_at, updated_at, deleted_at, ssh_endpoint, ssh_host_key`

func scanSandbox(row scanner) (Sandbox, error) {
	var (
		s            Sandbox
		labels       []byte
		reservedVCPU int64
		reservedMem  int64
		reservedDisk int64
		desiredVCPU  int64
		desiredMem   int64
		desiredDisk  int64
		sshEndpoint  *string
		sshHostKey   *string
	)
	err := row.Scan(&s.ID, &s.OwnerID, &s.State, &s.MachineType, &s.Name,
		&labels, &s.NodeID,
		&reservedVCPU, &reservedMem, &reservedDisk,
		&desiredVCPU, &desiredMem, &desiredDisk,
		&s.TTLSeconds, &s.Subdomain, &s.StoppedAt, &s.StopOperationID,
		&s.LifecycleGeneration,
		&s.Version, &s.ExpiresAt, &s.ErrorReason,
		&s.SnapshotID, &s.SnapshotPin, &s.FromSnapshotID,
		&s.CreatedAt, &s.UpdatedAt, &s.DeletedAt,
		&sshEndpoint, &sshHostKey)
	if err != nil {
		return Sandbox{}, err
	}
	s.Reserved = Resources{VCPUs: uint32(reservedVCPU), MemoryMiB: uint64(reservedMem), DiskGiB: uint64(reservedDisk)}
	s.Desired = Resources{VCPUs: uint32(desiredVCPU), MemoryMiB: uint64(desiredMem), DiskGiB: uint64(desiredDisk)}
	if s.Labels, err = unmarshalStringMap(labels); err != nil {
		return Sandbox{}, err
	}
	if sshEndpoint != nil || sshHostKey != nil {
		// The schema keeps the pair together; fail closed rather than
		// exposing a host key without its endpoint.
		if sshEndpoint == nil || sshHostKey == nil {
			return Sandbox{}, errors.New("db: corrupt sandbox ssh metadata")
		}
		s.SSH = &SSHAccess{Endpoint: *sshEndpoint, HostKey: *sshHostKey}
	}
	return s, nil
}

// CreateSandboxParams is the input of the atomic sandbox creation. The
// caller generates ID (ids.Sandbox) and chooses NodeID from candidate nodes;
// the reservation is re-verified inside the transaction. Env is plaintext
// input only: the store seals it before persisting, and it never reaches
// jobs, events or errors.
//
// TTLSeconds and ExpiresAt are mutually exclusive: TTLSeconds is the
// API-facing duration captured at admission, and the database computes
// expires_at from the same now() as created_at. ExpiresAt remains for legacy
// callers that already carry an absolute deadline.
type CreateSandboxParams struct {
	ID          string
	Owner       uuid.UUID
	MachineType string
	Name        string
	Labels      map[string]string
	Env         map[string]string
	ExpiresAt   *time.Time
	TTLSeconds  *int
	NodeID      string
	Reserve     Resources
	MaxAttempts int

	// IdempotencyKey and RequestSHA256 together opt into exactly-once
	// create. RequestSHA256 is the hash of the raw request body.
	IdempotencyKey string
	RequestSHA256  []byte

	// Restore, when set, creates the sandbox from one immutable saved
	// snapshot generation. The transaction locks and revalidates the row
	// (owner, ready, not retired or deleted, same node and artifact id) and
	// increments its pin counter before the reservation. NodeID must be the
	// artifact's recorded node.
	Restore *SnapshotSource
}

// MaxTTLSeconds is the Phase 0.5 upper bound for create TTLSeconds (30
// days). The API layer may apply a tighter configured limit; the
// store still refuses anything outside this absolute range.
const MaxTTLSeconds = 30 * 24 * 60 * 60

// ErrInvalidTTL rejects TTL inputs that are non-positive, out of range or
// supplied together with a legacy absolute ExpiresAt.
var ErrInvalidTTL = errors.New("db: invalid sandbox ttl")

// ValidateTTLSeconds checks the API-facing duration source. Callers that
// also accept ExpiresAt must reject the simultaneous form themselves (the
// store does it too).
func ValidateTTLSeconds(ttl *int) error {
	if ttl == nil {
		return nil
	}
	if *ttl <= 0 || *ttl > MaxTTLSeconds {
		return ErrInvalidTTL
	}
	return nil
}

// validateTTL rejects a create that mixes duration and absolute expiry, or
// carries an out-of-range duration.
func validateTTL(expiresAt *time.Time, ttl *int) error {
	if ttl == nil {
		return nil
	}
	if expiresAt != nil {
		return ErrInvalidTTL
	}
	return ValidateTTLSeconds(ttl)
}

// CreateSandboxResult is what one accepted create produced. Replayed is true
// when the idempotency key already existed: no new rows were written and
// Sandbox is the previously created sandbox.
type CreateSandboxResult struct {
	Sandbox  Sandbox
	Job      Job
	Replayed bool
}

// idempotencyRetention is how long create keys are kept (v1 API: 24 hours).
const idempotencyRetention = 24 * time.Hour

// CreateSandbox atomically reserves capacity on the chosen node, writes the
// sandbox, its first event and the provision job, and (when requested) the
// idempotency key. Creates with the same owner and key serialize on a
// transaction-scoped advisory lock before capacity is touched, so a duplicate
// racing the create that consumed the last slot replays the winner instead of
// answering no-capacity. A repeated key with the same request hash replays the
// original sandbox; a repeated key with a different hash answers
// [ErrIdempotencyReused]; the unique-key guard still answers
// [ErrIdempotencyInProgress] if an unserialized writer wins the insert (map it
// to API 409 idempotency_in_progress).
func (s *Store) CreateSandbox(ctx context.Context, p CreateSandboxParams) (CreateSandboxResult, error) {
	if err := validateTTL(p.ExpiresAt, p.TTLSeconds); err != nil {
		return CreateSandboxResult{}, err
	}
	labels, err := marshalStringMap(p.Labels)
	if err != nil {
		return CreateSandboxResult{}, err
	}
	envSealed, err := s.sealEnv(p.Env, p.Owner, p.ID)
	if err != nil {
		return CreateSandboxResult{}, err
	}
	if p.MachineType == "" {
		p.MachineType = "default"
	}

	var (
		out     CreateSandboxResult
		payload string
	)
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = DefaultMaxAttempts
	}

	err = s.runTx(ctx, "create sandbox", func(ctx context.Context, tx pgx.Tx) error {
		// Replay lives inside the capacity transaction, after a per
		// owner+key advisory lock: a racing duplicate waits here and
		// then replays, never competing for the slot the winner took.
		if p.IdempotencyKey != "" {
			if err := lockCreateKeyTx(ctx, tx, p.Owner, p.IdempotencyKey); err != nil {
				return err
			}
			res, found, err := s.replayCreateTx(ctx, tx, p)
			if err != nil {
				return err
			}
			if found {
				out = res
				return nil
			}
		}

		// A restore pins its exact generation before anything else so
		// the artifact cannot be retired and cleaned up while this
		// transaction is still reserving capacity.
		var (
			snapshotRowID  *uuid.UUID
			fromSnapshotID string
		)
		if p.Restore != nil {
			snapshot, err := lockRestoreSourceTx(ctx, tx, p.Owner, p.NodeID, p.Reserve, *p.Restore)
			if err != nil {
				return err
			}
			rowID := snapshot.ID
			snapshotRowID = &rowID
			fromSnapshotID = snapshot.ArtifactID
		}

		payload, err = marshalJobPayload(ProvisionPayload{
			NodeID:       p.NodeID,
			Resources:    p.Reserve,
			FromSnapshot: fromSnapshotID,
		})
		if err != nil {
			return err
		}

		// Capacity next: the node row lock serializes concurrent
		// reservations for the same node.
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
			p.NodeID, p.Reserve.VCPUs, p.Reserve.MemoryMiB, p.Reserve.DiskGiB)
		if err != nil {
			return fmtError("create sandbox reserve", err)
		}
		if tag.RowsAffected() == 0 {
			return noCapacityOrMissing(ctx, tx, p.NodeID)
		}

		var expires any
		if p.ExpiresAt != nil {
			expires = *p.ExpiresAt
		}
		var ttl any
		if p.TTLSeconds != nil {
			ttl = *p.TTLSeconds
		}
		var snapshotArg any
		if snapshotRowID != nil {
			snapshotArg = *snapshotRowID
		}
		row := tx.QueryRow(ctx, `
			INSERT INTO sandboxes (id, owner_id, state, machine_type, name, labels, env_sealed,
				node_id, reserved_vcpus, reserved_memory_mib, reserved_disk_gib,
				desired_vcpus, desired_memory_mib, desired_disk_gib,
				expires_at, ttl_seconds,
				snapshot_id, snapshot_pin, from_snapshot_id, created_at)
			VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10, $11,
				$9, $10, $11,
				CASE WHEN $12::integer IS NULL THEN $13 ELSE now() + make_interval(secs => $12::integer) END,
				$12,
				$14, $15, $16, now())
			RETURNING `+sandboxCols,
			p.ID, p.Owner, StateProvisioning, p.MachineType, p.Name, labels, envSealed,
			p.NodeID, p.Reserve.VCPUs, p.Reserve.MemoryMiB, p.Reserve.DiskGiB, ttl, expires,
			snapshotArg, p.Restore != nil, fromSnapshotID)
		out.Sandbox, err = scanSandbox(row)
		if err != nil {
			return fmtError("create sandbox insert", err)
		}

		if err := insertEventTx(ctx, tx, out.Sandbox, EventInput{Type: EventStatePrefix + string(StateProvisioning)}); err != nil {
			return err
		}
		job, err := enqueueJobTx(ctx, tx, JobInput{
			Kind:        JobProvision,
			OwnerID:     p.Owner,
			SandboxID:   &p.ID,
			Payload:     payload,
			MaxAttempts: p.MaxAttempts,
		})
		if err != nil {
			return err
		}
		out.Job = job

		if p.IdempotencyKey != "" {
			// An expired key no longer replays: remove it so the
			// primary key never blocks reuse of the key forever.
			// The delete races nobody: only this transaction can
			// insert the owner+key pair while holding it.
			if _, err := tx.Exec(ctx, `
				DELETE FROM idempotency_keys
				WHERE owner_id = $1 AND key = $2 AND expires_at <= now()`,
				p.Owner, p.IdempotencyKey); err != nil {
				return fmtError("create sandbox idempotency purge", err)
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO idempotency_keys (owner_id, key, request_sha256, sandbox_id, expires_at)
				VALUES ($1, $2, $3, $4, now() + make_interval(secs => $5::float8))`,
				p.Owner, p.IdempotencyKey, p.RequestSHA256, p.ID,
				idempotencyRetention.Seconds())
			if isUniqueViolation(err, "idempotency_keys_pkey") {
				// A concurrent create with the same key committed
				// first; the client retries and replays.
				return ErrIdempotencyInProgress
			}
			if err != nil {
				return fmtError("create sandbox idempotency insert", err)
			}
		}
		return nil
	})
	if err != nil {
		return CreateSandboxResult{}, err
	}
	return out, nil
}

// lockCreateKeyTx serializes creates for one owner and idempotency key for
// the rest of the transaction. The advisory lock is transaction-scoped, so it
// is released on commit or rollback; a hash collision only serializes two
// unrelated creates.
func lockCreateKeyTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, key string) error {
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"bbraft-create:"+owner.String()+":"+key); err != nil {
		return fmtError("lock create key", err)
	}
	return nil
}

// replayCreateTx returns the stored create for an existing unexpired
// idempotency key inside the caller's transaction. found is false when the
// key is absent or expired and the create should proceed.
func (s *Store) replayCreateTx(ctx context.Context, tx pgx.Tx, p CreateSandboxParams) (CreateSandboxResult, bool, error) {
	var (
		hash      []byte
		sandboxID string
	)
	err := tx.QueryRow(ctx,
		`SELECT request_sha256, sandbox_id FROM idempotency_keys
		 WHERE owner_id = $1 AND key = $2 AND expires_at > now()`,
		p.Owner, p.IdempotencyKey).Scan(&hash, &sandboxID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return CreateSandboxResult{}, false, nil // fresh or expired key
		}
		return CreateSandboxResult{}, false, fmtError("idempotency lookup", err)
	}
	if !bytes.Equal(hash, p.RequestSHA256) {
		return CreateSandboxResult{}, true, ErrIdempotencyReused
	}
	sandbox, err := sandboxRow(ctx, tx,
		`SELECT `+sandboxCols+` FROM sandboxes WHERE id = $1 AND owner_id = $2`, sandboxID, p.Owner)
	if err != nil {
		return CreateSandboxResult{}, true, err
	}
	job, err := latestJob(ctx, tx, p.Owner, sandboxID, JobProvision)
	if err != nil {
		return CreateSandboxResult{}, true, err
	}
	return CreateSandboxResult{Sandbox: sandbox, Job: job, Replayed: true}, true, nil
}

// SetIdempotencyResponse stores the success payload of a create on its
// idempotency row. The column is jsonb, so PostgreSQL normalizes whitespace
// and key order: callers must not rely on byte-identical text. This package
// exposes no getter for it yet; the API layer owns the response
// representation. Missing rows answer ErrNotFound; the create itself is
// already durable without it.
func (s *Store) SetIdempotencyResponse(ctx context.Context, owner uuid.UUID, key string, response []byte) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE idempotency_keys SET response = $3::jsonb WHERE owner_id = $1 AND key = $2`,
		owner, key, string(response))
	if err != nil {
		return fmtError("store idempotency response", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// noCapacityOrMissing distinguishes a full node from an unknown one after a
// failed in-transaction reservation.
func noCapacityOrMissing(ctx context.Context, tx pgx.Tx, nodeID string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM nodes WHERE id = $1)`, nodeID).Scan(&exists); err != nil {
		return fmtError("check node", err)
	}
	if !exists {
		return ErrNotFound
	}
	return ErrNoCapacity
}

// sandboxRow scans a single sandbox from a prepared one-row query.
func sandboxRow(ctx context.Context, q rowQuerier, sql string, args ...any) (Sandbox, error) {
	sbx, err := scanSandbox(q.QueryRow(ctx, sql, args...))
	if err != nil {
		return Sandbox{}, errNoRows("select sandbox", err)
	}
	return sbx, nil
}

// Sandbox returns one live sandbox owned by owner. Deleted, unknown and
// foreign sandboxes all answer ErrNotFound.
func (s *Store) Sandbox(ctx context.Context, owner uuid.UUID, id string) (Sandbox, error) {
	return sandboxRow(ctx, s.pool,
		`SELECT `+sandboxCols+` FROM sandboxes WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL`,
		id, owner)
}

// TransitionParams describes one optimistic lifecycle move. The move only
// commits when the sandbox is still in FromStates at ExpectedVersion, is
// owned by Owner and has not been deleted; it then writes the new state, an
// event and an optional follow-up job in the same transaction.
type TransitionParams struct {
	SandboxID string
	// Owner scopes the move to one account. There is no internal bypass:
	// job workers and the TTL reaper carry the job's owner explicitly.
	Owner           uuid.UUID
	FromStates      []SandboxState
	ExpectedVersion int64
	To              SandboxState
	ErrorReason     string
	Event           EventInput
	Job             *JobInput
}

// ErrNoOwner rejects writes that would operate without an explicit owner.
var ErrNoOwner = errors.New("db: operation requires an explicit owner")

// TransitionSandbox applies one lifecycle move. A zero Owner answers
// [ErrNoOwner] without touching the row. Version or from-state mismatches
// answer ErrConflict; an unknown, foreign or deleted sandbox answers
// ErrNotFound.
func (s *Store) TransitionSandbox(ctx context.Context, p TransitionParams) (Sandbox, error) {
	if p.Owner == uuid.Nil() {
		return Sandbox{}, ErrNoOwner
	}
	if len(p.FromStates) == 0 {
		p.FromStates = []SandboxState{p.To}
	}
	if p.Event.Type == "" {
		p.Event.Type = EventStatePrefix + string(p.To)
	}
	var out Sandbox
	err := s.runTx(ctx, "transition sandbox", func(ctx context.Context, tx pgx.Tx) error {
		if err := validateFollowUpJob(&p); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			UPDATE sandboxes SET
				state = $3, version = version + 1, error_reason = $4,
				ssh_endpoint = CASE WHEN $3 IN ('ready', 'idle', 'running') THEN ssh_endpoint ELSE NULL END,
				ssh_host_key = CASE WHEN $3 IN ('ready', 'idle', 'running') THEN ssh_host_key ELSE NULL END,
				updated_at = now()
			WHERE id = $1 AND owner_id = $6 AND version = $2 AND state = ANY($5) AND deleted_at IS NULL
			RETURNING `+sandboxCols,
			p.SandboxID, p.ExpectedVersion, p.To, p.ErrorReason, statesToStrings(p.FromStates), p.Owner)
		var err error
		out, err = scanSandbox(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return transitionError(ctx, tx, p)
		}
		if err != nil {
			// A genuine SQL or cancellation failure: report it, do
			// not re-classify it as a guard mismatch.
			return fmtError("transition sandbox", err)
		}
		if err := insertEventTx(ctx, tx, out, p.Event); err != nil {
			return err
		}
		if p.Job != nil {
			if _, err := enqueueJobTx(ctx, tx, *p.Job); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return Sandbox{}, err
	}
	return out, nil
}

// validateFollowUpJob keeps an optional follow-up job inside the move it
// belongs to: same owner and the same sandbox. A nil job sandbox defaults to
// the parent; an explicit different sandbox is rejected.
func validateFollowUpJob(p *TransitionParams) error {
	if p.Job == nil {
		return nil
	}
	if p.Job.OwnerID != p.Owner {
		return fmt.Errorf("db: transition %s follow-up job owner %s does not match %s",
			p.SandboxID, p.Job.OwnerID, p.Owner)
	}
	if p.Job.SandboxID == nil {
		p.Job.SandboxID = &p.SandboxID
		return nil
	}
	if *p.Job.SandboxID != p.SandboxID {
		return fmt.Errorf("db: transition %s follow-up job targets %s", p.SandboxID, *p.Job.SandboxID)
	}
	return nil
}

// transitionError explains a zero-row UPDATE by re-reading the row. The
// owner predicate is part of the read too, so a foreign row stays a safe
// not-found just like in the guarded update.
func transitionError(ctx context.Context, tx pgx.Tx, p TransitionParams) error {
	var (
		version int64
		state   string
		deleted *time.Time
	)
	err := tx.QueryRow(ctx,
		`SELECT version, state, deleted_at FROM sandboxes WHERE id = $1 AND owner_id = $2`,
		p.SandboxID, p.Owner).Scan(&version, &state, &deleted)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmtError("transition sandbox recheck", err)
	}
	if deleted != nil {
		return ErrNotFound
	}
	return ErrConflict
}

// PageCursor is the keyset position after the last row of a page: creation
// time at PostgreSQL microsecond precision plus the row id as tie-break.
// The API layer encodes it opaquely; the owner filter is always applied
// independently of the cursor.
type PageCursor struct {
	CreatedAt time.Time
	ID        string
}

// truncate ensures cursor times carry no sub-microsecond digits a SQL
// comparison would never reproduce.
func (c PageCursor) truncate() PageCursor {
	return PageCursor{CreatedAt: c.CreatedAt.Truncate(time.Microsecond), ID: c.ID}
}

// SandboxListQuery is an owner-scoped, cursor-paginated sandbox listing.
// Filters combine with AND; Labels requires every pair to match. Name is a
// pointer so filtering for the empty name stays distinct from no name
// filter.
type SandboxListQuery struct {
	Owner      uuid.UUID
	States     []SandboxState
	Name       *string
	NamePrefix string
	Labels     map[string]string
	Limit      int  // 1..200; values out of range are clamped
	Ascending  bool // default is newest first
	After      *PageCursor
}

// ListSandboxes returns one page of live owner-visible sandboxes and the
// cursor for the next page, or a nil cursor at the end.
func (s *Store) ListSandboxes(ctx context.Context, q SandboxListQuery) ([]Sandbox, *PageCursor, error) {
	q.Limit = clamp(q.Limit, 1, 200, 100)
	where := []string{"owner_id = $1", "deleted_at IS NULL"}
	args := []any{q.Owner}
	add := func(fragment string, arg any) {
		args = append(args, arg)
		where = append(where, strings.Replace(fragment, "$n", "$"+strconv.Itoa(len(args)), 1))
	}
	if len(q.States) > 0 {
		add(`state = ANY($n)`, statesToStrings(q.States))
	}
	if q.Name != nil {
		add(`name = $n`, *q.Name)
	}
	if q.NamePrefix != "" {
		add(`name LIKE $n`, escapeLike(q.NamePrefix)+"%")
	}
	if len(q.Labels) > 0 {
		m, err := marshalStringMap(q.Labels)
		if err != nil {
			return nil, nil, err
		}
		add(`labels @> $n::jsonb`, m)
	}
	if q.After != nil {
		c := q.After.truncate()
		op := ">"
		if !q.Ascending {
			op = "<"
		}
		args = append(args, c.CreatedAt, c.ID)
		where = append(where, fmt.Sprintf("(created_at, id) %s ($%d, $%d)",
			op, len(args)-1, len(args)))
	}
	order := `created_at DESC, id DESC`
	if q.Ascending {
		order = `created_at ASC, id ASC`
	}
	sql := `SELECT ` + sandboxCols + ` FROM sandboxes WHERE ` +
		strings.Join(where, " AND ") +
		` ORDER BY ` + order + ` LIMIT ` + strconv.Itoa(q.Limit+1)
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, fmtError("list sandboxes", err)
	}
	defer rows.Close()
	var page []Sandbox
	for rows.Next() {
		sbx, err := scanSandbox(rows)
		if err != nil {
			return nil, nil, fmtError("list sandboxes scan", err)
		}
		page = append(page, sbx)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmtError("list sandboxes", err)
	}
	if len(page) > q.Limit {
		last := page[q.Limit-1]
		page = page[:q.Limit]
		next := PageCursor{CreatedAt: last.CreatedAt, ID: last.ID}
		return page, &next, nil
	}
	return page, nil, nil
}

// SandboxesExpiring returns up to limit live sandboxes in an active state
// whose TTL passed before the given time, oldest deadline first. The TTL
// reaper turns each into an accepted deletion.
func (s *Store) SandboxesExpiring(ctx context.Context, before time.Time, limit int) ([]Sandbox, error) {
	limit = clamp(limit, 1, 500, 100)
	rows, err := s.pool.Query(ctx, `SELECT `+sandboxCols+` FROM sandboxes
		WHERE deleted_at IS NULL AND expires_at IS NOT NULL AND expires_at <= $1
		  AND state = ANY($2)
		ORDER BY expires_at, id LIMIT $3`,
		before, statesToStrings(ActiveStates()), limit)
	if err != nil {
		return nil, fmtError("list expiring", err)
	}
	defer rows.Close()
	var out []Sandbox
	for rows.Next() {
		sbx, err := scanSandbox(rows)
		if err != nil {
			return nil, fmtError("list expiring scan", err)
		}
		out = append(out, sbx)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("list expiring", err)
	}
	return out, nil
}

// latestJob returns the most recent job of kind for an owner's sandbox, if
// any. The owner predicate is part of the read.
func latestJob(ctx context.Context, q rowQuerier, owner uuid.UUID, sandboxID string, kind JobKind) (Job, error) {
	row := q.QueryRow(ctx, `SELECT `+jobCols+` FROM jobs
		WHERE sandbox_id = $1 AND owner_id = $2 AND kind = $3
		ORDER BY created_at DESC, id DESC LIMIT 1`,
		sandboxID, owner, kind)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, nil
	}
	if err != nil {
		return Job{}, fmtError("latest job", err)
	}
	return j, nil
}
