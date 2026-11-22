package db

// Lifecycle-facing Store extensions (B1.4). They stay in this file so the
// accepted schema, core CRUD and queue methods remain untouched: scheduler
// admission needs a replay read before placement, lifecycle workers need
// lease-fenced sandbox transitions and canonical rows that still exist after
// an accepted deletion, and the reaper needs idempotent deletion-operation
// and job-reconciliation writes.

import (
	"context"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// ReplayCreateSandbox answers the create associated with an idempotency key,
// if any. It is the admission fast path: callers check it before placement so
// a duplicate request replays even when every node is full or disconnected.
// It serializes with [Store.CreateSandbox] on the same owner+key advisory
// lock, so it either observes the winner's committed rows or reports no
// replay. A key reused with a different request hash answers
// [ErrIdempotencyReused]. Expired keys report no replay.
func (s *Store) ReplayCreateSandbox(ctx context.Context, owner uuid.UUID, key string, requestSHA256 []byte) (CreateSandboxResult, bool, error) {
	if owner == uuid.Nil() || key == "" {
		return CreateSandboxResult{}, false, nil
	}
	var (
		out   CreateSandboxResult
		found bool
	)
	err := s.runTx(ctx, "replay create", func(ctx context.Context, tx pgx.Tx) error {
		if err := lockCreateKeyTx(ctx, tx, owner, key); err != nil {
			return err
		}
		var err error
		out, found, err = s.replayCreateTx(ctx, tx, CreateSandboxParams{
			Owner:          owner,
			IdempotencyKey: key,
			RequestSHA256:  requestSHA256,
		})
		return err
	})
	if err != nil {
		return CreateSandboxResult{}, false, err
	}
	return out, found, nil
}

// SandboxAny returns one owner-scoped sandbox including rows whose deletion
// was accepted. Jobs and reconciliation must still see the canonical row
// (node, reservation, owner, version) after the sandbox disappears from
// owner-visible reads, so they can clean up the VM and never trust a job
// payload for those facts. Unknown and foreign rows answer [ErrNotFound].
func (s *Store) SandboxAny(ctx context.Context, owner uuid.UUID, id string) (Sandbox, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+sandboxCols+` FROM sandboxes WHERE id = $1 AND owner_id = $2`,
		id, owner)
	sbx, err := scanSandbox(row)
	if err != nil {
		return Sandbox{}, errNoRows("select sandbox any state", err)
	}
	return sbx, nil
}

// SandboxesOnNode lists every sandbox row currently associated with nodeID,
// including deleted rows, oldest first. Reconciliation compares this durable
// ownership set with the node's ListVMs answer; anything it does not know is
// preserved, never deleted on a guess.
func (s *Store) SandboxesOnNode(ctx context.Context, nodeID string) ([]Sandbox, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+sandboxCols+` FROM sandboxes WHERE node_id = $1 ORDER BY created_at, id`,
		nodeID)
	if err != nil {
		return nil, fmtError("list node sandboxes", err)
	}
	defer rows.Close()
	var out []Sandbox
	for rows.Next() {
		sbx, err := scanSandbox(rows)
		if err != nil {
			return nil, fmtError("scan node sandbox", err)
		}
		out = append(out, sbx)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("list node sandboxes", err)
	}
	return out, nil
}

// NodeIDs lists node ids that either have inventory or still own sandbox
// rows. Reconciliation visits both: a replaced node keeps its old id on the
// sandboxes it hosts until cleanup finishes, and a newly dialed node may not
// have inventory yet.
func (s *Store) NodeIDs(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM nodes
		UNION
		SELECT node_id FROM sandboxes WHERE node_id IS NOT NULL
		ORDER BY 1`)
	if err != nil {
		return nil, fmtError("list node ids", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmtError("scan node id", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("list node ids", err)
	}
	return out, nil
}

// TransitionSandboxJob applies [TransitionParams] only while jobID is still
// running under worker and token with a live lease, and while the sandbox
// still matches the expected version and from-states. The job row is locked
// FOR SHARE for the transaction, so a concurrent claim cannot retire the
// lease mid-move. A Hydrated move also records the job's source generation as
// the sandbox's restore source in the same transaction as hydrated_at, so the
// chunk collector keeps that generation until the sandbox has data of its
// own. A sandbox guard mismatch answers ErrConflict/ErrNotFound; a
// lost claim answers [ErrJobNotLeased]. A stale boot completion therefore
// cannot move a deleted or re-owned sandbox.
func (s *Store) TransitionSandboxJob(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p TransitionParams) (Sandbox, error) {
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
	err := s.runTx(ctx, "transition sandbox for job", func(ctx context.Context, tx pgx.Tx) error {
		if err := validateFollowUpJob(&p); err != nil {
			return err
		}
		if err := lockJobFenceTx(ctx, tx, jobID, worker, token, p.Owner, p.SandboxID); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			UPDATE sandboxes SET
				state = $3, version = version + 1, error_reason = $4,
				ssh_endpoint = CASE WHEN $3 IN ('ready', 'idle', 'running') THEN ssh_endpoint ELSE NULL END,
				ssh_host_key = CASE WHEN $3 IN ('ready', 'idle', 'running') THEN ssh_host_key ELSE NULL END,
				hydrated_at = CASE WHEN $7 THEN now() ELSE hydrated_at END,
				restore_source_generation_id = CASE WHEN $7
					THEN (SELECT source_generation_id FROM jobs WHERE id = $8)
					ELSE restore_source_generation_id END,
				updated_at = now()
			WHERE id = $1 AND owner_id = $6 AND version = $2 AND state = ANY($5) AND deleted_at IS NULL
			RETURNING `+sandboxCols,
			p.SandboxID, p.ExpectedVersion, p.To, p.ErrorReason, statesToStrings(p.FromStates), p.Owner, p.Hydrated, jobID)
		var err error
		out, err = scanSandbox(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return transitionError(ctx, tx, p)
		}
		if err != nil {
			return fmtError("transition sandbox for job", err)
		}
		if err := insertEventTx(ctx, tx, out, p.Event); err != nil {
			return err
		}
		if p.Hydrated {
			if err := insertEventTx(ctx, tx, out, EventInput{Type: EventSandboxHydrated}); err != nil {
				return err
			}
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

// lockJobFenceTx takes a shared row lock on a job that this worker still
// holds with a live lease AND that belongs to the same owner and sandbox as
// the transition. A valid token for an unrelated job must not authorize a
// move on this row. A concurrent claim (which updates the row) waits for the
// transaction, so either the move commits first or the claim wins and the
// move answers [ErrJobNotLeased].
func lockJobFenceTx(ctx context.Context, tx pgx.Tx, jobID uuid.UUID, worker string, token uuid.UUID, owner uuid.UUID, sandboxID string) error {
	var one int
	err := tx.QueryRow(ctx, `
		SELECT 1 FROM jobs
		WHERE id = $1 AND `+jobFence+` AND owner_id = $4 AND sandbox_id = $5
		FOR SHARE`, jobID, worker, token, owner, sandboxID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrJobNotLeased
	}
	if err != nil {
		return fmtError("lock job fence", err)
	}
	return nil
}

// FailJob terminally fails a running job under this worker's live claim.
// Handlers use it for non-retryable failures (payload/target validation), so
// max_attempts does not force pointless retries. reason is safe and
// non-secret. [ErrJobNotLeased] means the claim is gone.
func (s *Store) FailJob(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, reason string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET state = 'failed', last_error = $4,
			leased_by = '', leased_until = NULL, lease_token = NULL, updated_at = now()
		WHERE id = $1 AND `+jobFence,
		jobID, worker, token, reason)
	if err != nil {
		return fmtError("fail job", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotLeased
	}
	return nil
}

// CancelJob terminally cancels a running job under this worker's live claim.
// Provisioning uses it when the sandbox was deleted while the boot was in
// flight and compensation already ran. [ErrJobNotLeased] means the claim is
// gone.
func (s *Store) CancelJob(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET state = 'cancelled', leased_by = '', leased_until = NULL,
			lease_token = NULL, updated_at = now()
		WHERE id = $1 AND `+jobFence,
		jobID, worker, token)
	if err != nil {
		return fmtError("cancel job", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJobNotLeased
	}
	return nil
}

// BlockDeletionOperation marks an owner-scoped operation blocked for retry
// (status blocked, stage retrying) without touching a completed terminal
// record. Reconciliation calls it after a delete job exhausts its attempts;
// it is idempotent. Unknown and foreign operations answer [ErrNotFound].
func (s *Store) BlockDeletionOperation(ctx context.Context, owner uuid.UUID, opID string) (DeletionOperation, error) {
	var op DeletionOperation
	err := s.runTx(ctx, "block deletion operation", func(ctx context.Context, tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			UPDATE deletion_operations SET status = 'blocked', stage = 'retrying', updated_at = now()
			WHERE id = $1 AND owner_id = $2 AND status <> 'completed'
			RETURNING `+deletionCols,
			opID, owner)
		var err error
		op, err = scanDeletion(row)
		if errors.Is(err, pgx.ErrNoRows) {
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
		return err
	})
	if err != nil {
		return DeletionOperation{}, err
	}
	return op, nil
}

// ActiveSandboxDeletion returns the owner-scoped deletion operation for
// sandboxID that has not completed yet. The reaper uses it instead of
// trusting a failed job payload for the operation identity. Completed or
// missing operations answer [ErrNotFound].
func (s *Store) ActiveSandboxDeletion(ctx context.Context, owner uuid.UUID, sandboxID string) (DeletionOperation, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+deletionCols+` FROM deletion_operations
		WHERE owner_id = $1 AND target_id = $2 AND status <> 'completed'
		ORDER BY requested_at DESC, id DESC LIMIT 1`,
		owner, sandboxID)
	op, err := scanDeletion(row)
	if err != nil {
		return DeletionOperation{}, errNoRows("select active deletion operation", err)
	}
	return op, nil
}

// SandboxGuard serializes side-effecting node work for one owner+sandbox
// across processes. It is a PostgreSQL session-level advisory lock on a
// dedicated connection outside the Store pool: no SQL transaction is held
// across an RPC. Provisioning holds it from the canonical recheck through
// CreateVM and any compensation; deletion acquires it before final cleanup
// and CompleteDeletion. A lost connection releases the lock server-side and
// closes Done, so the attempt cancels instead of acting without the guard.
//
// The guard is not a substitute for job lease fencing: every state change
// still presents the claim's lease token. It closes the window in which an
// expired lease could otherwise let deletion release capacity while the old
// boot RPC is still able to create a VM.
type SandboxGuard struct {
	conn *pgx.Conn
	key  string

	done     chan struct{}
	stop     chan struct{}
	monitor  sync.WaitGroup
	release  sync.Once
	lossOnce sync.Once
	lost     atomic.Pointer[guardLoss]
}

// guardLoss carries the sentinel without exposing driver details.
type guardLoss struct{ err error }

// ErrSandboxGuardLost means the guard's dedicated connection died while the
// operation was running. The holder must stop side effects, cancel the RPC
// and treat the attempt as abandoned; another worker may now own the guard.
var ErrSandboxGuardLost = errors.New("db: sandbox guard connection lost")

// Guard keepalive bounds: the monitor detects a dead dedicated connection,
// and release never inherits an already-cancelled attempt context.
const (
	guardKeepaliveInterval = time.Second
	guardKeepaliveTimeout  = 3 * time.Second
	guardReleaseTimeout    = 3 * time.Second
)

// AcquireSandboxGuard opens a dedicated connection from the Store's
// parameters, takes the session advisory lock for owner+sandbox (blocking
// until available or ctx ends) and starts a keepalive monitor. The raw
// connection is never a pool slot, so a guarded long boot cannot starve
// queries or the job listener. Callers must always Release.
func (s *Store) AcquireSandboxGuard(ctx context.Context, owner uuid.UUID, sandboxID string) (*SandboxGuard, error) {
	if owner == uuid.Nil() || sandboxID == "" {
		return nil, errors.New("db: sandbox guard requires owner and sandbox id")
	}
	cfg := *s.pool.Config().ConnConfig
	cfg.RuntimeParams = maps.Clone(cfg.RuntimeParams)
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	// A distinct application name lets tests and operators identify only
	// guard connections without touching the pool.
	cfg.RuntimeParams["application_name"] = cfg.RuntimeParams["application_name"] + "_guard"
	conn, err := pgx.ConnectConfig(ctx, &cfg)
	if err != nil {
		// Never wrap the driver error: connection errors can echo the
		// connection string. The caller only needs "guard unavailable".
		return nil, errors.New("db: sandbox guard connect failed")
	}
	key := "bbraft-guard:" + owner.String() + ":" + sandboxID
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, key); err != nil {
		closeGuardConn(conn)
		return nil, fmtError("sandbox guard lock", err)
	}
	g := &SandboxGuard{
		conn: conn,
		key:  key,
		done: make(chan struct{}),
		stop: make(chan struct{}),
	}
	g.monitor.Go(func() { g.watch() })
	return g, nil
}

// Done is closed when the guard is lost or released. A loss closes it before
// any compensation; callers should inspect Err and cancel their RPC context.
func (g *SandboxGuard) Done() <-chan struct{} { return g.done }

// Err reports [ErrSandboxGuardLost] after a lost connection and nil for a
// normal release.
func (g *SandboxGuard) Err() error {
	if loss := g.lost.Load(); loss != nil {
		return loss.err
	}
	return nil
}

// watch pings the dedicated connection until release or failure. The ping is
// the only statement running on the connection after lock acquisition, so
// pgx.Conn's single-user rule holds. A failed ping means PostgreSQL has
// already released the session lock; close the local socket promptly.
func (g *SandboxGuard) watch() {
	ticker := time.NewTicker(guardKeepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), guardKeepaliveTimeout)
			_, err := g.conn.Exec(ctx, "SELECT 1")
			cancel()
			if err != nil {
				g.fail(ErrSandboxGuardLost)
				// PostgreSQL already released the session lock when
				// the connection died; drop the local socket promptly
				// so nothing lingers half-dead.
				closeGuardConn(g.conn)
				return
			}
		}
	}
}

// fail records the loss once and closes the observable channel.
func (g *SandboxGuard) fail(err error) {
	g.lossOnce.Do(func() {
		g.lost.Store(&guardLoss{err: err})
		close(g.done)
	})
}

// Release stops the monitor, unlocks the session advisory lock when the
// connection is still healthy, and closes the dedicated connection. It is
// idempotent, never blocks on a cancelled attempt context and closes Done.
func (g *SandboxGuard) Release(_ context.Context) {
	g.release.Do(func() {
		close(g.stop)
		g.monitor.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), guardReleaseTimeout)
		defer cancel()
		if g.Err() == nil {
			_, _ = g.conn.Exec(ctx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, g.key) //nolint:errcheck // closing the connection releases it too
		}
		closeGuardConn(g.conn)
		// Done also closes for a normal release; Err stays nil.
		g.lossOnce.Do(func() { close(g.done) })
	})
}

// closeGuardConn releases a dedicated connection under a bounded context.
func closeGuardConn(conn *pgx.Conn) {
	if conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), guardReleaseTimeout)
	defer cancel()
	_ = conn.Close(ctx) //nolint:errcheck // socket drop is the remaining cleanup
}

// RetrySandboxDeletion atomically decides whether an exhausted delete job
// needs a retry. Under the operation row lock and the same dedupe key as
// [Store.EnqueueJobIfAbsent], it leaves the current operation status unchanged
// when a canonical delete job is already queued or running (an old failure
// must not flip a newer attempt to blocked), and otherwise marks the
// operation blocked/retrying and enqueues exactly one retry in the same
// transaction. It reports whether a job was enqueued.
func (s *Store) RetrySandboxDeletion(ctx context.Context, owner uuid.UUID, opID, sandboxID string, in JobInput) (DeletionOperation, bool, error) {
	if in.Kind != JobDeleteVM || in.OwnerID != owner || in.SandboxID == nil || *in.SandboxID != sandboxID {
		return DeletionOperation{}, false, errors.New("db: retry deletion job does not match operation target")
	}
	var (
		op    DeletionOperation
		added bool
	)
	err := s.runTx(ctx, "retry sandbox deletion", func(ctx context.Context, tx pgx.Tx) error {
		if err := lockJobDedupeTx(ctx, tx, in); err != nil {
			return err
		}
		row := tx.QueryRow(ctx,
			`SELECT `+deletionCols+` FROM deletion_operations
			 WHERE id = $1 AND owner_id = $2 FOR UPDATE`,
			opID, owner)
		var err error
		op, err = scanDeletion(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmtError("lock deletion operation", err)
		}
		if op.Kind != DeletionSandbox || op.TargetID != sandboxID {
			return errors.New("db: deletion operation does not match target")
		}
		if op.Status == DeletionCompleted {
			return nil
		}
		active, err := activeJobTx(ctx, tx, in)
		if err != nil {
			return err
		}
		if active {
			// A newer attempt owns cleanup; an old failed row must not
			// rewrite its operation status.
			return nil
		}
		row = tx.QueryRow(ctx, `
			UPDATE deletion_operations SET status = 'blocked', stage = 'retrying', updated_at = now()
			WHERE id = $1 AND owner_id = $2 AND status <> 'completed'
			RETURNING `+deletionCols,
			opID, owner)
		op, err = scanDeletion(row)
		if err != nil {
			return fmtError("block deletion operation", err)
		}
		if _, err := enqueueJobTx(ctx, tx, in); err != nil {
			return err
		}
		added = true
		return nil
	})
	if err != nil {
		return DeletionOperation{}, false, err
	}
	return op, added, nil
}

// lockJobDedupeTx serializes retry decisions for one owner+sandbox+kind.
func lockJobDedupeTx(ctx context.Context, tx pgx.Tx, in JobInput) error {
	key := "bbraft-job:" + in.OwnerID.String() + ":" + derefString(in.SandboxID) + ":" + string(in.Kind)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return fmtError("lock job dedupe", err)
	}
	return nil
}

// activeJobTx reports whether a queued or running job of this kind already
// targets the same owner-scoped sandbox.
func activeJobTx(ctx context.Context, tx pgx.Tx, in JobInput) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM jobs
			WHERE owner_id = $1 AND sandbox_id IS NOT DISTINCT FROM $2
			  AND kind = $3 AND state IN ('queued', 'running'))`,
		in.OwnerID, in.SandboxID, in.Kind).Scan(&exists)
	if err != nil {
		return false, fmtError("check active job", err)
	}
	return exists, nil
}

// EnqueueJobIfAbsent writes a queued job unless an owner-scoped queued or
// running job of the same kind already targets the same sandbox. The check
// and insert run in one transaction under an advisory lock, so two
// reconcilers cannot enqueue duplicate cleanup jobs. It reports whether a
// job was written.
func (s *Store) EnqueueJobIfAbsent(ctx context.Context, in JobInput) (Job, bool, error) {
	var (
		job   Job
		added bool
	)
	err := s.runTx(ctx, "enqueue job if absent", func(ctx context.Context, tx pgx.Tx) error {
		if err := lockJobDedupeTx(ctx, tx, in); err != nil {
			return err
		}
		exists, err := activeJobTx(ctx, tx, in)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		job, err = enqueueJobTx(ctx, tx, in)
		if err != nil {
			return err
		}
		added = true
		return nil
	})
	if err != nil {
		return Job{}, false, err
	}
	return job, added, nil
}
