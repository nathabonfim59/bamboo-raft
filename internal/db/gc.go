package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// gcLiveSQL selects the id of every generation the chunk collector must keep
// readable. $1 is the history cutoff: completed generations cut after it stay
// live. A generation is live when it is pending (an attempt may still be
// writing it, for every sandbox) or purge-free and one of:
//
//  1. on a sandbox that is neither deleted nor expired: its chain's latest
//     completed generation, or a completed one cut after the history cutoff;
//  2. the restore source of such a sandbox, until the sandbox has a completed
//     generation of its own on its current chain cut after the restore;
//  3. on such a sandbox, newer than the chain's latest completed generation in
//     any status (a failed or abandoned attempt may have written a manifest
//     the next attempt skipped chunks for);
//  4. pinned by a live saved snapshot, or by a queued or running job.
const gcLiveSQL = `
	SELECT g.id FROM snapshots g
	WHERE g.status = 'pending' OR (g.purged_at IS NULL AND (
		EXISTS (
			SELECT 1 FROM sandboxes s
			WHERE s.id = g.sandbox_id AND s.chain_id = g.chain_id
			  AND s.deleted_at IS NULL AND s.data_expired_at IS NULL
			  AND CASE WHEN g.status = 'completed'
				THEN g.cut_at > $1 OR g.generation = (
					SELECT max(c.generation) FROM snapshots c
					WHERE c.chain_id = g.chain_id AND c.status = 'completed')
				ELSE g.generation > COALESCE((
					SELECT max(c.generation) FROM snapshots c
					WHERE c.chain_id = g.chain_id AND c.status = 'completed'), -1)
			  END)
		OR EXISTS (
			SELECT 1 FROM sandboxes s
			WHERE s.restore_source_generation_id = g.id
			  AND s.deleted_at IS NULL AND s.data_expired_at IS NULL
			  AND NOT EXISTS (
				SELECT 1 FROM snapshots n
				WHERE n.sandbox_id = s.id AND n.chain_id = s.chain_id
				  AND n.status = 'completed' AND n.cut_at > s.hydrated_at))
		OR EXISTS (
			SELECT 1 FROM saved_snapshots v
			WHERE v.generation_id = g.id AND v.deleted_at IS NULL
			  AND v.retired_at IS NULL AND v.status IN ('saving', 'ready'))
		OR EXISTS (
			SELECT 1 FROM jobs j
			WHERE j.source_generation_id = g.id AND j.state IN ('queued', 'running'))))`

// GCLiveGeneration is one generation the collector must keep.
type GCLiveGeneration struct {
	// ManifestKey is the generation's manifest object key. The object may not
	// exist (a pending or failed attempt that never wrote it).
	ManifestKey string
	Status      GenerationStatus
}

// GCLiveGenerations lists the generations whose manifests and chunks must be
// kept, for every owner: the rules of the collector's liveness (see the
// chunk collector's documentation). historyCutoff is the oldest cut time at
// which a completed generation that is not the latest of its chain is still
// kept.
func (s *Store) GCLiveGenerations(ctx context.Context, historyCutoff time.Time) ([]GCLiveGeneration, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT manifest_key, status FROM snapshots WHERE id IN (`+gcLiveSQL+`)`, historyCutoff)
	if err != nil {
		return nil, fmtError("gc live generations", err)
	}
	defer rows.Close()
	var out []GCLiveGeneration
	for rows.Next() {
		var g GCLiveGeneration
		if err := rows.Scan(&g.ManifestKey, &g.Status); err != nil {
			return nil, fmtError("gc live generations scan", err)
		}
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("gc live generations", err)
	}
	return out, nil
}

// GCBeginSweep claims the collector's sweep fence: it sets gc_state.sweeping,
// which serializes with [Store.BeginGeneration] (that reads the row FOR SHARE,
// so the update waits for attempts that are beginning and later attempts see
// the flag and are refused with [ErrGCSweeping]). It reports false, changing
// nothing, when another sweep holds the flag and started less than staleAfter
// ago: a flag older than that is a crashed sweep's and is taken over.
func (s *Store) GCBeginSweep(ctx context.Context, staleAfter time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE gc_state SET sweeping = true, sweep_started_at = now()
		WHERE id AND (NOT sweeping
			OR COALESCE(sweep_started_at, '-infinity'::timestamptz) < now() - make_interval(secs => $1::float8))`,
		secondsOf(staleAfter))
	if err != nil {
		return false, fmtError("gc begin sweep", err)
	}
	return tag.RowsAffected() == 1, nil
}

// GCEndSweep clears the sweep fence and records the end of a mark in
// last_mark_at.
func (s *Store) GCEndSweep(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `
		UPDATE gc_state SET sweeping = false, sweep_started_at = NULL, last_mark_at = now()
		WHERE id`); err != nil {
		return fmtError("gc end sweep", err)
	}
	return nil
}

// GCRecoverStaleSweep clears a sweep fence left set by a crashed collector:
// one whose sweep started more than staleAfter ago. It reports whether it
// cleared one.
func (s *Store) GCRecoverStaleSweep(ctx context.Context, staleAfter time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE gc_state SET sweeping = false, sweep_started_at = NULL
		WHERE id AND sweeping
		  AND COALESCE(sweep_started_at, '-infinity'::timestamptz) < now() - make_interval(secs => $1::float8)`,
		secondsOf(staleAfter))
	if err != nil {
		return false, fmtError("gc recover stale sweep", err)
	}
	return tag.RowsAffected() == 1, nil
}

// GCInFlightGeneration is an attempt that is not completed and whose owner
// may still be writing chunks.
type GCInFlightGeneration struct {
	ID         uuid.UUID
	SandboxID  string
	Number     uint32
	DeadlineAt time.Time
}

// GCInFlightGenerations lists the attempts that are not completed and whose
// deadline plus margin has not passed yet, oldest deadline first: the attempts
// the sweep fence waits for. A pending attempt is listed, and so is a failed
// one: a lost lease does not prove the attempt's call ended, only its deadline
// does, so a takeover or a recorded failure holds the fence until then. Older
// rows are dead (their owner cannot still be writing) and are not listed.
func (s *Store) GCInFlightGenerations(ctx context.Context, margin time.Duration) ([]GCInFlightGeneration, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, sandbox_id, generation, deadline_at FROM snapshots
		WHERE status <> 'completed' AND deadline_at + make_interval(secs => $1::float8) > now()
		ORDER BY deadline_at, id`, secondsOf(margin))
	if err != nil {
		return nil, fmtError("gc in-flight generations", err)
	}
	defer rows.Close()
	var out []GCInFlightGeneration
	for rows.Next() {
		var (
			g      GCInFlightGeneration
			number int64
		)
		if err := rows.Scan(&g.ID, &g.SandboxID, &number, &g.DeadlineAt); err != nil {
			return nil, fmtError("gc in-flight generations scan", err)
		}
		g.Number = uint32(number) // the schema bounds generation to uint32
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmtError("gc in-flight generations", err)
	}
	return out, nil
}

// GCPurgeGenerations marks the generations behind manifestKeys purged unless
// one is live again: it locks the rows (a fork or saved-snapshot admission
// holds the generation row FOR SHARE, so it either commits first or sees the
// purge and is refused), re-evaluates liveness on a fresh statement and
// updates only rows that are still dead. It returns the manifest keys whose
// row stayed unpurged: those are live and the caller must keep their chunks.
// A key without a row, or whose row was purged already, counts as purged.
// historyCutoff is as for [Store.GCLiveGenerations]; keys must be manifest
// object keys.
func (s *Store) GCPurgeGenerations(ctx context.Context, historyCutoff time.Time, manifestKeys []string) ([]string, error) {
	chains := make([]string, 0, len(manifestKeys))
	numbers := make([]int64, 0, len(manifestKeys))
	for _, key := range manifestKeys {
		chain, generation, err := snapformat.ParseManifestKey(key)
		if err != nil {
			return nil, err
		}
		chains = append(chains, chain)
		numbers = append(numbers, int64(generation))
	}
	if len(chains) == 0 {
		return nil, nil
	}
	// The row filter twice: with the chains and numbers as $1/$2 for the
	// statements that do not take the history cutoff, and as $2/$3 after it.
	const (
		rowsOf = `(chain_id, generation) IN (
			SELECT c::uuid, g FROM unnest($1::text[], $2::bigint[]) AS t(c, g))`
		rowsAfterCutoff = `(chain_id, generation) IN (
			SELECT c::uuid, g FROM unnest($2::text[], $3::bigint[]) AS t(c, g))`
	)
	var declined []string
	err := s.runTx(ctx, "gc purge generations", func(ctx context.Context, tx pgx.Tx) error {
		// The lock statement ends before the guarded update starts, so the
		// update's liveness check sees every admission that committed while
		// this transaction waited for a row.
		if _, err := tx.Exec(ctx, `
			SELECT id FROM snapshots WHERE `+rowsOf+` AND purged_at IS NULL
			ORDER BY id FOR UPDATE`, chains, numbers); err != nil {
			return fmtError("gc purge lock", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE snapshots SET purged_at = now()
			WHERE `+rowsAfterCutoff+` AND purged_at IS NULL AND status <> 'pending'
			  AND id NOT IN (`+gcLiveSQL+`)`, historyCutoff, chains, numbers); err != nil {
			return fmtError("gc purge update", err)
		}
		rows, err := tx.Query(ctx, `
			SELECT manifest_key FROM snapshots WHERE `+rowsOf+` AND purged_at IS NULL`,
			chains, numbers)
		if err != nil {
			return fmtError("gc purge declined", err)
		}
		defer rows.Close()
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				return fmtError("gc purge declined scan", err)
			}
			declined = append(declined, key)
		}
		if err := rows.Err(); err != nil {
			return fmtError("gc purge declined", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return declined, nil
}

// GCSweeping reports the fence flag (tests and diagnostics).
func (s *Store) GCSweeping(ctx context.Context) (bool, error) {
	var sweeping bool
	if err := s.pool.QueryRow(ctx, `SELECT sweeping FROM gc_state WHERE id`).Scan(&sweeping); err != nil {
		return false, fmtError("gc sweeping", err)
	}
	return sweeping, nil
}
