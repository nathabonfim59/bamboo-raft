package db

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// JobKind names one durable work type.
type JobKind string

const (
	// JobProvision boots a sandbox VM on its reserved node.
	JobProvision JobKind = "sandbox.provision"
	// JobStop archives a sandbox: last snapshot, StopVM, then the durable
	// stop operation completes. The disk stays on its node.
	JobStop JobKind = "sandbox.stop"
	// JobResume starts a previously stopped sandbox again on the same
	// node whose disk it retained.
	JobResume JobKind = "sandbox.resume"
	// JobDeleteVM removes a sandbox's VM after an accepted deletion.
	JobDeleteVM JobKind = "sandbox.delete"
	// JobSaveSnapshot saves a sandbox into a named snapshot artifact.
	JobSaveSnapshot JobKind = "snapshot.save"
	// JobDeleteSnapshot removes one retired or deleted snapshot artifact
	// from its recorded node. DeleteSnapshot is idempotent.
	JobDeleteSnapshot JobKind = "snapshot.delete"
)

// JobState is the queue lifecycle of one job.
type JobState string

const (
	JobQueued    JobState = "queued"
	JobRunning   JobState = "running"
	JobDone      JobState = "done"
	JobFailed    JobState = "failed"
	JobCancelled JobState = "cancelled"
)

// DefaultMaxAttempts bounds retries before a job fails terminally.
const DefaultMaxAttempts = 5

// JobsChannel is the LISTEN/NOTIFY channel used to wake workers.
const JobsChannel = "bbraft_jobs"

// Job is one queue row. Payload carries the typed payload of its Kind.
// LeaseToken fences the current claim: CompleteJob, RetryJob and
// RenewJobLease must present it while the lease is still live, so a stale
// attempt can never finish work that was reclaimed elsewhere.
type Job struct {
	ID          uuid.UUID
	Kind        JobKind
	OwnerID     uuid.UUID
	SandboxID   *string
	SnapshotID  *uuid.UUID // saved snapshot generation, when the job belongs to one
	Payload     string     // JSON object
	State       JobState
	Attempts    int
	MaxAttempts int
	RunAfter    time.Time
	LeasedBy    string
	LeasedUntil *time.Time
	LeaseToken  uuid.UUID // zero unless running
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ProvisionPayload is the payload of [JobProvision].
type ProvisionPayload struct {
	NodeID    string
	Resources Resources
	// FromSnapshot is the immutable artifact id a restore boots from. Empty
	// for a normal create. The job handler must still validate it against
	// the canonical sandbox row.
	FromSnapshot string
	// Generation is the sandbox lifecycle generation this boot belongs to.
	// The initial create writes 0; a resume increments it. A provision
	// worker must never complete a boot for a different generation.
	Generation int64
}

// StopPayload is the payload of [JobStop]. Force is only a hint captured at
// acceptance: a later force escalation updates the canonical StopOperation,
// which workers must reread instead of trusting this field. OperationID
// binds the job to the operation that owns the archiving state.
type StopPayload struct {
	OperationID string
	NodeID      string
	VMID        string
	Force       bool
}

// ResumePayload is the payload of [JobResume]. Generation binds the attempt
// to the accepted resume: the worker may only finalize while the sandbox
// still carries this lifecycle generation, so a stale resume can never move
// a later attempt.
type ResumePayload struct {
	NodeID      string
	VMID        string
	MachineType string
	Resources   Resources
	FromStopID  string
	Generation  int64
}

// DeleteVMPayload is the payload of [JobDeleteVM].
type DeleteVMPayload struct {
	NodeID      string
	VMID        string
	Resources   Resources
	OperationID string
}

// SaveSnapshotPayload is the payload of [JobSaveSnapshot]. SnapshotID is the
// immutable saved_snapshots row of this generation; NodeID, VMID, ArtifactID
// and Generation are a duplicate copy the handler validates against the row.
type SaveSnapshotPayload struct {
	SnapshotID uuid.UUID
	NodeID     string
	VMID       string
	ArtifactID string
	Generation int64
}

// DeleteSnapshotPayload is the payload of [JobDeleteSnapshot]. The handler
// resolves the canonical row by SnapshotID and only deletes on its recorded
// node; the copied fields exist so a stale payload cannot redirect cleanup.
type DeleteSnapshotPayload struct {
	SnapshotID uuid.UUID
	NodeID     string
	ArtifactID string
}

// JobInput describes a job to enqueue inside a Store operation.
type JobInput struct {
	Kind        JobKind
	OwnerID     uuid.UUID
	SandboxID   *string
	SnapshotID  *uuid.UUID
	Payload     string // JSON object from one of the payload types
	MaxAttempts int    // <= 0 means DefaultMaxAttempts
}

const jobCols = `id, kind, owner_id, sandbox_id, snapshot_id, payload, state, attempts, max_attempts,
	run_after, leased_by, leased_until, lease_token, last_error, created_at, updated_at`

func scanJob(row scanner) (Job, error) {
	var (
		j       Job
		payload []byte
		token   *uuid.UUID
	)
	err := row.Scan(&j.ID, &j.Kind, &j.OwnerID, &j.SandboxID, &j.SnapshotID, &payload, &j.State,
		&j.Attempts, &j.MaxAttempts, &j.RunAfter, &j.LeasedBy, &j.LeasedUntil, &token,
		&j.LastError, &j.CreatedAt, &j.UpdatedAt)
	if err == nil {
		j.Payload = string(payload)
		if token != nil {
			j.LeaseToken = *token
		}
	}
	return j, err
}

// marshalJobPayload encodes one typed payload for a JobInput.
func marshalJobPayload(v any) (string, error) {
	b, err := jsonMarshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// EnqueueJob writes one queued job and notifies a worker. Use this for jobs
// born outside other Store operations; operations that already hold a
// transaction enqueue internally.
func (s *Store) EnqueueJob(ctx context.Context, in JobInput) (Job, error) {
	var job Job
	err := s.runTx(ctx, "enqueue job", func(ctx context.Context, tx pgx.Tx) error {
		var err error
		job, err = enqueueJobTx(ctx, tx, in)
		return err
	})
	if err != nil {
		return Job{}, err
	}
	return job, nil
}

// LatestSandboxJob returns the most recent owner-scoped job of kind for a
// sandbox, including its payload and state. Stop acceptance during initial
// provisioning must leave the boot job available for the worker to inspect,
// so this is how a stop/resume worker finds the canonical provision attempt
// instead of trusting a job payload. Unknown or absent rows answer
// [ErrNotFound].
func (s *Store) LatestSandboxJob(ctx context.Context, owner uuid.UUID, sandboxID string, kind JobKind) (Job, error) {
	job, err := latestJob(ctx, s.pool, owner, sandboxID, kind)
	if err != nil {
		return Job{}, err
	}
	if job.ID == uuid.Nil() {
		return Job{}, ErrNotFound
	}
	return job, nil
}

// enqueueJobTx inserts the job and sends the wake-up notification in the
// caller's transaction, so a rolled-back enqueue never wakes anybody.
func enqueueJobTx(ctx context.Context, tx pgx.Tx, in JobInput) (Job, error) {
	if in.Payload == "" {
		in.Payload = "{}"
	}
	if in.MaxAttempts <= 0 {
		in.MaxAttempts = DefaultMaxAttempts
	}
	row := tx.QueryRow(ctx, `
		INSERT INTO jobs (id, kind, owner_id, sandbox_id, snapshot_id, payload, max_attempts)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
		RETURNING `+jobCols,
		uuid.NewV7(), in.Kind, in.OwnerID, in.SandboxID, in.SnapshotID, in.Payload, in.MaxAttempts)
	j, err := scanJob(row)
	if err != nil {
		return Job{}, fmtError("enqueue job", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, JobsChannel, string(in.Kind)); err != nil {
		return Job{}, fmtError("enqueue job notify", err)
	}
	return j, nil
}

// ClaimJob atomically takes the next runnable job for worker with a fresh
// lease token. A job is runnable when it is queued, due and has attempts
// left, or when its lease expired (worker crash) and attempts remain;
// claiming always increments attempts. An empty queue answers [ErrNoJobs].
// worker must be non-empty and lease positive.
func (s *Store) ClaimJob(ctx context.Context, worker string, lease time.Duration) (Job, error) {
	if worker == "" {
		return Job{}, errors.New("db: claim requires a non-empty worker id")
	}
	if lease <= 0 {
		return Job{}, errors.New("db: claim requires a positive lease")
	}
	row := s.pool.QueryRow(ctx, `
		UPDATE jobs SET
			state = 'running',
			attempts = attempts + 1,
			leased_by = $1,
			leased_until = now() + make_interval(secs => $2::float8),
			lease_token = $3,
			updated_at = now()
		WHERE id = (
			SELECT id FROM jobs
			WHERE attempts < max_attempts
			  AND (
				(state = 'queued' AND run_after <= now())
				OR (state = 'running' AND leased_until < now())
			  )
			ORDER BY run_after, id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING `+jobCols,
		worker, secondsOf(lease), uuid.NewV7())
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoJobs
	}
	if err != nil {
		return Job{}, fmtError("claim job", err)
	}
	return j, nil
}

// fenced guards a mutation of a running job: the caller must present the
// claim's lease token, the worker id, and the lease must still be live.
const jobFence = `state = 'running' AND leased_by = $2 AND lease_token = $3 AND leased_until > now()`

// RenewJobLease extends the lease of a job this worker still holds; the
// lease must be positive. [ErrJobNotLeased] means the lease lapsed and the
// job was reclaimed or finished elsewhere.
func (s *Store) RenewJobLease(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, lease time.Duration) error {
	if lease <= 0 {
		return errors.New("db: renew requires a positive lease")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET leased_until = now() + make_interval(secs => $4::float8), updated_at = now()
		WHERE id = $1 AND `+jobFence,
		jobID, worker, token, secondsOf(lease))
	if err != nil {
		return fmtError("renew job lease", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotLeased
	}
	return nil
}

// CompleteJob marks a job done. [ErrJobNotLeased] means the lease lapsed and
// another worker redelivered it; the side effects must already be
// idempotent, so the caller logs and moves on.
func (s *Store) CompleteJob(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET state = 'done', leased_by = '', leased_until = NULL, lease_token = NULL,
			updated_at = now()
		WHERE id = $1 AND `+jobFence,
		jobID, worker, token)
	if err != nil {
		return fmtError("complete job", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotLeased
	}
	return nil
}

// RetryJob requeues a failed attempt with backoff, or fails the job
// terminally once attempts reached MaxAttempts. cause is a safe, non-secret
// reason string stored on the row. Only the current fenced claim can retry.
func (s *Store) RetryJob(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, backoff time.Duration, cause string) error {
	if backoff < 0 {
		return errors.New("db: retry backoff must not be negative")
	}
	return s.runTx(ctx, "retry job", func(ctx context.Context, tx pgx.Tx) error {
		var state JobState
		err := tx.QueryRow(ctx, `
			UPDATE jobs SET
				state = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
				run_after = now() + make_interval(secs => $4::float8),
				last_error = $5,
				leased_by = '',
				leased_until = NULL,
				lease_token = NULL,
				updated_at = now()
			WHERE id = $1 AND `+jobFence+`
			RETURNING state`,
			jobID, worker, token, secondsOf(backoff), cause).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrJobNotLeased
		}
		if err != nil {
			return fmtError("retry job", err)
		}
		if state == JobQueued {
			// Wake a worker for the retried run; terminal failures
			// notify nobody.
			if _, err := tx.Exec(ctx, `SELECT pg_notify($1, 'retry')`, JobsChannel); err != nil {
				return fmtError("retry job notify", err)
			}
		}
		return nil
	})
}

// CancelSandboxJobs cancels every still-queued job of the given kinds for an
// owner's sandbox. Running jobs are left alone: their effects are idempotent
// and their completion is guarded by state checks. The owner predicate is
// part of the write, so a foreign sandbox id cancels nothing. Returns the
// number cancelled.
func (s *Store) CancelSandboxJobs(ctx context.Context, owner uuid.UUID, sandboxID string, kinds ...JobKind) (int64, error) {
	if len(kinds) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET state = 'cancelled', updated_at = now()
		WHERE owner_id = $1 AND sandbox_id = $2 AND state = 'queued' AND kind = ANY($3::text[])`,
		owner, sandboxID, kindsToStrings(kinds))
	if err != nil {
		return 0, fmtError("cancel sandbox jobs", err)
	}
	return tag.RowsAffected(), nil
}

// FailStaleJobs terminally fails running jobs whose lease expired with no
// attempts left, so a crashed worker cannot strand work forever. Returns the
// number failed. The reaper calls this periodically; after it runs,
// FailedJobs reports the jobs whose target sandbox may be stuck mid-state.
func (s *Store) FailStaleJobs(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET state = 'failed', last_error = 'lease expired', updated_at = now()
		WHERE state = 'running' AND leased_until < now() AND attempts >= max_attempts`)
	if err != nil {
		return 0, fmtError("fail stale jobs", err)
	}
	return tag.RowsAffected(), nil
}

// FailedJobCursor is the keyset position after the last failed job of a
// page: the job's update time at PostgreSQL microsecond precision plus its
// id as tie-break.
type FailedJobCursor struct {
	UpdatedAt time.Time
	ID        uuid.UUID
}

// FailedJobs walks terminally failed jobs in (updated_at, id) order after
// the given cursor, oldest first, and returns the cursor for the next page
// (nil at the end). Lifecycle reconciles failures by walking pages to
// completion and recording which ones it handled: the store keeps no
// acknowledgement state, and an oldest-first keyset walk cannot starve an
// unreconciled failure behind newer ones. Re-running a walk re-reads rows by
// design. No worker ships here.
func (s *Store) FailedJobs(ctx context.Context, after *FailedJobCursor, limit int) ([]Job, *FailedJobCursor, error) {
	limit = clamp(limit, 1, 500, 100)
	where := `state = 'failed'`
	args := []any{}
	if after != nil {
		args = append(args, after.UpdatedAt.Truncate(time.Microsecond), after.ID)
		where += ` AND (updated_at, id) > ($1, $2)`
	}
	args = append(args, limit+1)
	sql := `SELECT ` + jobCols + ` FROM jobs WHERE ` + where +
		` ORDER BY updated_at, id LIMIT $` + strconv.Itoa(len(args))
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, fmtError("list failed jobs", err)
	}
	defer rows.Close()
	var page []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, nil, fmtError("list failed jobs scan", err)
		}
		page = append(page, j)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmtError("list failed jobs", err)
	}
	if len(page) > limit {
		last := page[limit-1]
		page = page[:limit]
		return page, &FailedJobCursor{UpdatedAt: last.UpdatedAt, ID: last.ID}, nil
	}
	return page, nil, nil
}

// secondsOf converts a duration to seconds for make_interval, keeping lease
// and backoff arithmetic on the database clock.
func secondsOf(d time.Duration) float64 { return d.Seconds() }

// kindsToStrings converts job kinds to a plain text array parameter.
func kindsToStrings(kinds []JobKind) []string {
	out := make([]string, len(kinds))
	for i, k := range kinds {
		out[i] = string(k)
	}
	return out
}

// statesToStrings converts sandbox states the same way.
func statesToStrings(states []SandboxState) []string {
	out := make([]string, len(states))
	for i, st := range states {
		out[i] = string(st)
	}
	return out
}
