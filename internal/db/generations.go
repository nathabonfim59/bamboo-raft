package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// GenerationStatus is the state of one snapshot attempt.
type GenerationStatus string

// Generation statuses. Only completed rows are ever shown through the API.
const (
	// GenerationPending is an attempt in flight; at most one per sandbox.
	GenerationPending GenerationStatus = "pending"
	// GenerationCompleted is a generation whose manifest is in the object
	// store and whose sizes the node reported.
	GenerationCompleted GenerationStatus = "completed"
	// GenerationFailed is an attempt that ended without a generation. Its
	// number is never reused.
	GenerationFailed GenerationStatus = "failed"
)

// Errors of the generation store.
var (
	// ErrGCSweeping means the chunk collector is sweeping: no snapshot attempt
	// may begin until it finishes, because the sweep could delete chunks the
	// new generation is about to reference.
	ErrGCSweeping = errors.New("db: chunk GC sweep in progress")

	// ErrSnapshotsDisabled means the sandbox was created with snapshots off.
	ErrSnapshotsDisabled = errors.New("db: snapshots disabled for this sandbox")
)

// Generation is one snapshots row: one attempt at one position of a sandbox's
// chain.
type Generation struct {
	ID        uuid.UUID
	OwnerID   uuid.UUID
	SandboxID string
	ChainID   uuid.UUID
	// Number is the chain position; 0 is the base generation.
	Number uint32
	Status GenerationStatus
	// ManifestKey is the object-store key of the generation's manifest.
	ManifestKey string
	NodeID      string
	Image       string
	// DiskBytes, Chunks, ChunksUploaded, BytesUploaded and ContentBytes are the
	// node's report; zero until the generation completed.
	DiskBytes, Chunks, ChunksUploaded, BytesUploaded, ContentBytes uint64
	Frozen                                                         bool
	// Error is the short failure code of a failed attempt, never a secret.
	Error string
	// CutAt is the node's creation time of the generation; nil until completed.
	CutAt       *time.Time
	StartedAt   time.Time
	CompletedAt *time.Time
	DeadlineAt  time.Time
	PurgedAt    *time.Time
	// JobID and LeaseToken are the lifecycle job and the claim that own a
	// pending attempt. Both are zero on completed and failed rows and on
	// rows written before the owner columns existed.
	JobID      uuid.UUID
	LeaseToken uuid.UUID
}

// Kind is "base" for generation 0 and "incremental" for every later one.
func (g Generation) Kind() string {
	if g.Number == 0 {
		return "base"
	}
	return "incremental"
}

// GenerationResult is what the node's Generation message reports for a
// finished snapshot.
type GenerationResult struct {
	Image string
	// DiskBytes, Chunks, ChunksUploaded, BytesUploaded and ContentBytes are the
	// sizes the node reported.
	DiskBytes, Chunks, ChunksUploaded, BytesUploaded, ContentBytes uint64
	Frozen                                                         bool
	// CutAt is the node's creation time of the generation.
	CutAt time.Time
}

const generationCols = `id, owner_id, sandbox_id, chain_id, generation, status, manifest_key, node_id,
	image, disk_bytes, chunks, chunks_uploaded, bytes_uploaded, content_bytes, frozen,
	error, cut_at, started_at, completed_at, deadline_at, purged_at, job_id, lease_token`

func scanGeneration(row scanner) (Generation, error) {
	var (
		g        Generation
		number   int64
		image    *string
		disk     *int64
		chunks   *int64
		uploaded *int64
		bytesUp  *int64
		content  *int64
		frozen   *bool
		jobID    *uuid.UUID
		token    *uuid.UUID
	)
	err := row.Scan(&g.ID, &g.OwnerID, &g.SandboxID, &g.ChainID, &number, &g.Status,
		&g.ManifestKey, &g.NodeID, &image, &disk, &chunks, &uploaded, &bytesUp, &content,
		&frozen, &g.Error, &g.CutAt, &g.StartedAt, &g.CompletedAt, &g.DeadlineAt, &g.PurgedAt,
		&jobID, &token)
	if err != nil {
		return Generation{}, err
	}
	if jobID != nil {
		g.JobID = *jobID
	}
	if token != nil {
		g.LeaseToken = *token
	}
	// The schema bounds generation to uint32 and every size to >= 0.
	g.Number = uint32(number)
	if image != nil {
		g.Image = *image
	}
	for _, f := range []struct {
		dst *uint64
		src *int64
	}{
		{&g.DiskBytes, disk}, {&g.Chunks, chunks}, {&g.ChunksUploaded, uploaded},
		{&g.BytesUploaded, bytesUp}, {&g.ContentBytes, content},
	} {
		if f.src != nil {
			*f.dst = uint64(*f.src)
		}
	}
	g.Frozen = frozen != nil && *frozen
	return g, nil
}

// sizeArg converts a node-reported size for a bigint column.
func sizeArg(name string, v uint64) (int64, error) {
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("db: generation %s %d out of range", name, v)
	}
	return int64(v), nil
}

// BeginGenerationParams describes the attempt to begin and owns it: the
// sandbox whose chain gets the row, and the lifecycle job and claim that are
// cutting it.
type BeginGenerationParams struct {
	// Owner is the sandbox's account.
	Owner uuid.UUID
	// SandboxID is the owner's live sandbox.
	SandboxID string
	// Timeout bounds the attempt and sets the row's deadline.
	Timeout time.Duration
	// JobID is the lifecycle job taking the generation; required.
	JobID uuid.UUID
	// LeaseToken is that job's current claim token; required.
	LeaseToken uuid.UUID
}

// BeginGeneration inserts a pending row for the sandbox's current chain with
// number = max(number)+1 over ALL rows of the chain (0 for an empty chain),
// node = the sandbox's node, deadline = now+timeout and the owning job and
// lease token. It runs in one transaction, so concurrent calls for one
// sandbox serialize. The lock order is gc_state FOR SHARE, the sandbox row
// FOR NO KEY UPDATE, the pending snapshot row FOR UPDATE and its owning job
// FOR SHARE — the same order finishGeneration uses, so a takeover and a
// normal completion of one sandbox's attempt cannot deadlock.
//
// A pending row whose owner is gone does not block: when its job is not
// running, holds another lease token or let its lease expire (or, for a row
// written before the owner columns, when its deadline passed) the old row
// becomes failed with the fixed error "abandoned" and the new attempt takes
// the next number. A pending row whose owner still holds a live claim answers
// [ErrConflict].
//
// A missing, foreign or deleted sandbox answers [ErrNotFound]; a sandbox with
// snapshots off [ErrSnapshotsDisabled]; a sandbox without a node
// [ErrInvalidState]. A number above math.MaxUint32 is an error: it never
// wraps.
func (s *Store) BeginGeneration(ctx context.Context, p BeginGenerationParams) (Generation, error) {
	if p.Timeout <= 0 {
		return Generation{}, errors.New("db: begin generation requires a positive timeout")
	}
	if p.JobID == uuid.Nil() || p.LeaseToken == uuid.Nil() {
		return Generation{}, errors.New("db: begin generation requires the owning job and lease token")
	}
	var out Generation
	err := s.runTx(ctx, "begin generation", func(ctx context.Context, tx pgx.Tx) error {
		var sweeping bool
		if err := tx.QueryRow(ctx, `SELECT sweeping FROM gc_state WHERE id FOR SHARE`).Scan(&sweeping); err != nil {
			return fmtError("begin generation gc state", err)
		}
		if sweeping {
			return ErrGCSweeping
		}

		var (
			chain   uuid.UUID
			node    *string
			enabled bool
		)
		err := tx.QueryRow(ctx, `
			SELECT chain_id, node_id, snapshots_enabled FROM sandboxes
			WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL
			FOR NO KEY UPDATE`, p.SandboxID, p.Owner).Scan(&chain, &node, &enabled)
		if err != nil {
			return errNoRows("begin generation sandbox", err)
		}
		if !enabled {
			return ErrSnapshotsDisabled
		}
		if node == nil || *node == "" {
			return ErrInvalidState
		}

		// The pending row (at most one, snapshots_pending_uk) and its
		// owning job are locked so two Begins cannot both take it over.
		pending, err := scanGeneration(tx.QueryRow(ctx, `SELECT `+generationCols+`
			FROM snapshots WHERE sandbox_id = $1 AND status = 'pending' FOR UPDATE`,
			p.SandboxID))
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// Nothing in flight.
		case err != nil:
			return fmtError("begin generation pending", err)
		default:
			live, err := generationAttemptLive(ctx, tx, pending)
			if err != nil {
				return err
			}
			if live {
				return ErrConflict
			}
			if _, err := tx.Exec(ctx,
				`UPDATE snapshots SET status = 'failed', error = 'abandoned' WHERE id = $1`,
				pending.ID); err != nil {
				return fmtError("begin generation abandon", err)
			}
		}

		var highest *int64
		if err := tx.QueryRow(ctx, `SELECT max(generation) FROM snapshots WHERE chain_id = $1`,
			chain).Scan(&highest); err != nil {
			return fmtError("begin generation number", err)
		}
		var number int64
		if highest != nil {
			number = *highest + 1
		}
		if number > math.MaxUint32 {
			return fmt.Errorf("db: sandbox %s chain %s has no generation numbers left", p.SandboxID, chain)
		}
		kind := "incremental"
		if number == 0 {
			kind = "base"
		}

		row := tx.QueryRow(ctx, `
			INSERT INTO snapshots (id, owner_id, sandbox_id, chain_id, generation, kind, status,
				manifest_key, node_id, job_id, lease_token, deadline_at)
			VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8, $9, $10,
				now() + make_interval(secs => $11::float8))
			RETURNING `+generationCols,
			uuid.NewV7(), p.Owner, p.SandboxID, chain, number, kind,
			snapformat.ManifestKey(chain.String(), uint32(number)), *node, p.JobID, p.LeaseToken,
			secondsOf(p.Timeout))
		out, err = scanGeneration(row)
		if isUniqueViolation(err, "snapshots_pending_uk") || isUniqueViolation(err, "snapshots_chain_generation_uk") {
			return ErrConflict
		}
		if err != nil {
			return fmtError("begin generation insert", err)
		}
		return nil
	})
	if err != nil {
		return Generation{}, err
	}
	return out, nil
}

// generationAttemptLive reports whether a pending row's owner can still
// finish it: its job must be running, present the row's lease token and hold
// an unexpired lease. The job row is read FOR SHARE so a concurrent
// completion or reclaim waits until this transaction decides. A row written
// before the owner columns has no job to ask; it stays live until its
// deadline passes.
func generationAttemptLive(ctx context.Context, tx pgx.Tx, g Generation) (bool, error) {
	if g.JobID == uuid.Nil() || g.LeaseToken == uuid.Nil() {
		var expired bool
		if err := tx.QueryRow(ctx,
			`SELECT deadline_at < now() FROM snapshots WHERE id = $1`, g.ID).Scan(&expired); err != nil {
			return false, fmtError("begin generation deadline", err)
		}
		return !expired, nil
	}
	var live bool
	err := tx.QueryRow(ctx, `
		SELECT COALESCE(state = 'running' AND lease_token = $2 AND leased_until > now(), false)
		FROM jobs WHERE id = $1
		FOR SHARE`, g.JobID, g.LeaseToken).Scan(&live)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // the job row is gone: nobody owns the row
	}
	if err != nil {
		return false, fmtError("begin generation owner", err)
	}
	return live, nil
}

// CompleteGeneration marks a pending row completed with the node's result and
// sets the sandbox's next_snapshot_at to next. Repeating it for a completed
// row returns that row unchanged; a failed row answers [ErrConflict] and an
// unknown id [ErrNotFound].
func (s *Store) CompleteGeneration(ctx context.Context, id uuid.UUID, r GenerationResult, next time.Time) (Generation, error) {
	if r.CutAt.IsZero() {
		return Generation{}, errors.New("db: complete generation requires the cut time")
	}
	var sizes [5]int64
	for i, f := range []struct {
		name string
		v    uint64
	}{
		{"disk bytes", r.DiskBytes}, {"chunks", r.Chunks}, {"chunks uploaded", r.ChunksUploaded},
		{"bytes uploaded", r.BytesUploaded}, {"content bytes", r.ContentBytes},
	} {
		v, err := sizeArg(f.name, f.v)
		if err != nil {
			return Generation{}, err
		}
		sizes[i] = v
	}
	return s.finishGeneration(ctx, "complete generation", id, GenerationCompleted, next, func(ctx context.Context, tx pgx.Tx) (Generation, error) {
		return scanGeneration(tx.QueryRow(ctx, `
			UPDATE snapshots SET status = 'completed', image = $2,
				disk_bytes = $3, chunks = $4, chunks_uploaded = $5, bytes_uploaded = $6,
				content_bytes = $7, frozen = $8, cut_at = $9, completed_at = now(), error = ''
			WHERE id = $1
			RETURNING `+generationCols,
			id, r.Image, sizes[0], sizes[1], sizes[2], sizes[3], sizes[4], r.Frozen, r.CutAt))
	})
}

// FailGeneration marks a pending row failed and sets the sandbox's
// next_snapshot_at to next. reason is a short code and is stored as is: never
// pass a secret. Repeating it for a failed row returns that row unchanged; a
// completed row answers [ErrConflict] and an unknown id [ErrNotFound].
func (s *Store) FailGeneration(ctx context.Context, id uuid.UUID, reason string, next time.Time) (Generation, error) {
	return s.finishGeneration(ctx, "fail generation", id, GenerationFailed, next, func(ctx context.Context, tx pgx.Tx) (Generation, error) {
		return scanGeneration(tx.QueryRow(ctx, `
			UPDATE snapshots SET status = 'failed', error = $2
			WHERE id = $1
			RETURNING `+generationCols, id, reason))
	})
}

// finishGeneration is the shared state machine of Complete and Fail: lock the
// sandbox row first, then the snapshot row, accept a repeat of the same
// outcome, refuse the opposite one, otherwise run apply and schedule the
// sandbox's next attempt. The lock order is the one BeginGeneration uses too
// — gc_state, the sandbox row, the snapshot row, the job row — so a takeover
// and a normal completion of one sandbox's attempt cannot deadlock.
func (s *Store) finishGeneration(ctx context.Context, op string, id uuid.UUID, to GenerationStatus, next time.Time,
	apply func(context.Context, pgx.Tx) (Generation, error),
) (Generation, error) {
	var out Generation
	err := s.runTx(ctx, op, func(ctx context.Context, tx pgx.Tx) error {
		// The sandbox lock comes before the snapshot row's: a completion on
		// the pending row and a Begin taking that row over must not wait for
		// each other in opposite orders.
		var sandboxID string
		err := tx.QueryRow(ctx, `
			SELECT id FROM sandboxes
			WHERE id = (SELECT sandbox_id FROM snapshots WHERE id = $1)
			FOR NO KEY UPDATE`, id).Scan(&sandboxID)
		if err != nil {
			return errNoRows(op+" sandbox", err)
		}
		cur, err := scanGeneration(tx.QueryRow(ctx,
			`SELECT `+generationCols+` FROM snapshots WHERE id = $1 FOR UPDATE`, id))
		if err != nil {
			return errNoRows(op+" read", err)
		}
		switch cur.Status {
		case to:
			out = cur
			return nil
		case GenerationPending:
		default:
			return ErrConflict
		}
		if out, err = apply(ctx, tx); err != nil {
			return fmtError(op, err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE sandboxes SET next_snapshot_at = $2 WHERE id = $1`,
			out.SandboxID, next); err != nil {
			return fmtError(op+" schedule", err)
		}
		return nil
	})
	if err != nil {
		return Generation{}, err
	}
	return out, nil
}

// LatestGeneration is the sandbox's newest completed, unpurged generation
// across all of its chains, ordered by cut time then number; ok is false when
// there is none or the sandbox is missing, foreign or deleted.
func (s *Store) LatestGeneration(ctx context.Context, owner uuid.UUID, sandboxID string) (Generation, bool, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+generationCols+` FROM snapshots
		WHERE owner_id = $1 AND sandbox_id = $2 AND status = 'completed' AND purged_at IS NULL
		  AND sandbox_id IN (SELECT id FROM sandboxes WHERE id = $2 AND owner_id = $1 AND deleted_at IS NULL)
		ORDER BY cut_at DESC, generation DESC, id DESC
		LIMIT 1`, owner, sandboxID)
	g, err := scanGeneration(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Generation{}, false, nil
	}
	if err != nil {
		return Generation{}, false, fmtError("latest generation", err)
	}
	return g, true, nil
}

// GenerationCursor is the keyset position after the last row of a page: the
// cut time at PostgreSQL microsecond precision plus the row id as tie-break.
type GenerationCursor struct {
	At time.Time
	ID uuid.UUID
}

// GenerationListQuery is an owner-scoped, cursor-paginated generation listing.
type GenerationListQuery struct {
	Owner uuid.UUID
	// SandboxID restricts the listing to one sandbox; empty lists every
	// non-deleted sandbox of the owner. A missing or deleted sandbox lists
	// nothing: callers that need a 404 look the sandbox up first.
	SandboxID string
	Limit     int  // 1..200, default 50; out-of-range values are clamped
	Ascending bool // default is newest first
	After     *GenerationCursor
}

// ListGenerations lists completed, unpurged generations by (cut time, id),
// newest first unless Ascending, and the cursor of the next page (nil at the
// end). Pending, failed and purged rows are never listed.
func (s *Store) ListGenerations(ctx context.Context, q GenerationListQuery) ([]Generation, *GenerationCursor, error) {
	limit := clamp(q.Limit, 1, 200, 50)
	args := []any{q.Owner}
	owned := `SELECT id FROM sandboxes WHERE owner_id = $1 AND deleted_at IS NULL`
	if q.SandboxID != "" {
		args = append(args, q.SandboxID)
		owned += ` AND id = $2`
	}
	where := []string{
		"owner_id = $1", "status = 'completed'", "purged_at IS NULL",
		"sandbox_id IN (" + owned + ")",
	}
	order := `cut_at DESC, id DESC`
	if q.Ascending {
		order = `cut_at ASC, id ASC`
	}
	if q.After != nil {
		op := "<"
		if q.Ascending {
			op = ">"
		}
		args = append(args, q.After.At.Truncate(time.Microsecond), q.After.ID)
		where = append(where, fmt.Sprintf("(cut_at, id) %s ($%d, $%d)", op, len(args)-1, len(args)))
	}
	rows, err := s.pool.Query(ctx, `SELECT `+generationCols+` FROM snapshots WHERE `+
		strings.Join(where, " AND ")+` ORDER BY `+order+` LIMIT `+strconv.Itoa(limit+1), args...)
	if err != nil {
		return nil, nil, fmtError("list generations", err)
	}
	defer rows.Close()
	var page []Generation
	for rows.Next() {
		g, err := scanGeneration(rows)
		if err != nil {
			return nil, nil, fmtError("list generations scan", err)
		}
		page = append(page, g)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmtError("list generations", err)
	}
	if len(page) > limit {
		page = page[:limit]
		last := page[limit-1]
		return page, &GenerationCursor{At: *last.CutAt, ID: last.ID}, nil
	}
	return page, nil, nil
}

// DueSnapshotSandboxes lists live sandboxes in a ready-ish state (ready, idle
// or running) with snapshots enabled whose next_snapshot_at is NULL or not
// after now and that have no queued or running sandbox.snapshot job, oldest
// due first (never-snapshotted sandboxes first). limit is clamped to 1..500,
// default 100.
func (s *Store) DueSnapshotSandboxes(ctx context.Context, now time.Time, limit int) ([]Sandbox, error) {
	limit = clamp(limit, 1, 500, 100)
	rows, err := s.pool.Query(ctx, `SELECT `+sandboxCols+` FROM sandboxes
		WHERE deleted_at IS NULL AND snapshots_enabled
		  AND state IN ('ready', 'idle', 'running')
		  AND (next_snapshot_at IS NULL OR next_snapshot_at <= $1)
		  AND NOT EXISTS (
			SELECT 1 FROM jobs
			WHERE jobs.sandbox_id = sandboxes.id AND jobs.kind = $2
			  AND jobs.state IN ('queued', 'running'))
		ORDER BY next_snapshot_at NULLS FIRST, id
		LIMIT $3`, now, JobSnapshot, limit)
	if err != nil {
		return nil, fmtError("list due snapshot sandboxes", err)
	}
	defer rows.Close()
	var out []Sandbox
	for rows.Next() {
		sbx, err := scanSandbox(rows)
		if err != nil {
			return nil, fmtError("list due snapshot sandboxes scan", err)
		}
		out = append(out, sbx)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("list due snapshot sandboxes", err)
	}
	return out, nil
}

// EnqueueSnapshotJob enqueues one sandbox.snapshot job for the owner's live
// sandbox unless one is already queued or running for it, and reports whether
// it enqueued. The dedupe runs under a per-sandbox advisory lock in the
// enqueue transaction, so concurrent callers enqueue exactly one. A missing,
// foreign or deleted sandbox answers [ErrNotFound].
func (s *Store) EnqueueSnapshotJob(ctx context.Context, owner uuid.UUID, sandboxID string) (bool, error) {
	var enqueued bool
	err := s.runTx(ctx, "enqueue snapshot job", func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
			"bbraft-snapshot-job:"+sandboxID); err != nil {
			return fmtError("lock snapshot job", err)
		}
		var live bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM sandboxes
				WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL)`,
			sandboxID, owner).Scan(&live); err != nil {
			return fmtError("check snapshot job sandbox", err)
		}
		if !live {
			return ErrNotFound
		}
		var active bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM jobs
				WHERE sandbox_id = $1 AND kind = $2 AND state IN ('queued', 'running'))`,
			sandboxID, JobSnapshot).Scan(&active); err != nil {
			return fmtError("check active snapshot job", err)
		}
		if active {
			return nil
		}
		if _, err := enqueueJobTx(ctx, tx, JobInput{
			Kind:      JobSnapshot,
			OwnerID:   owner,
			SandboxID: &sandboxID,
		}); err != nil {
			return err
		}
		enqueued = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return enqueued, nil
}

// SetSnapshotSchedule sets the owner's live sandbox's next_snapshot_at (the
// back-off after a failed or refused attempt). A missing, foreign or deleted
// sandbox answers [ErrNotFound].
func (s *Store) SetSnapshotSchedule(ctx context.Context, owner uuid.UUID, sandboxID string, next time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE sandboxes SET next_snapshot_at = $3
		WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL`,
		sandboxID, owner, next)
	if err != nil {
		return fmtError("set snapshot schedule", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// PendingGenerations lists pending rows of every owner, oldest first, for the
// GC fence and reconciliation. limit is clamped to 1..500, default 100.
func (s *Store) PendingGenerations(ctx context.Context, limit int) ([]Generation, error) {
	limit = clamp(limit, 1, 500, 100)
	rows, err := s.pool.Query(ctx, `SELECT `+generationCols+` FROM snapshots
		WHERE status = 'pending' ORDER BY started_at, id LIMIT $1`, limit)
	if err != nil {
		return nil, fmtError("list pending generations", err)
	}
	defer rows.Close()
	var out []Generation
	for rows.Next() {
		g, err := scanGeneration(rows)
		if err != nil {
			return nil, fmtError("list pending generations scan", err)
		}
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("list pending generations", err)
	}
	return out, nil
}
