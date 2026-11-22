package db_test

// PostgreSQL tests of the generation store. They skip unless
// BBRAFT_TEST_DATABASE_URL is set (BBRAFT_TEST_REQUIRE_ALL=1 turns the skip
// into a failure); every test creates and drops its own schema.

import (
	"context"
	"errors"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// phase2LatestMigration is the newest goose version of the embedded set, so
// tests that re-apply every migration do not hardcode a version number.
func phase2LatestMigration(t *testing.T) int64 {
	t.Helper()
	entries, err := os.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	var latest int64
	for _, e := range entries {
		num, _, ok := strings.Cut(e.Name(), "_")
		if !ok {
			continue
		}
		v, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			continue
		}
		latest = max(latest, v)
	}
	if latest == 0 {
		t.Fatal("no migrations found")
	}
	return latest
}

// genStore opens a store with one roomy node and returns it with a fresh owner.
func genStore(t *testing.T) (*db.Store, uuid.UUID) {
	t.Helper()
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 65536, DiskGiB: 1000})
	return s, owner(t, s)
}

// genSandbox creates a sandbox for own and moves it to state (default ready).
func genSandbox(t *testing.T, s *db.Store, own uuid.UUID, state db.SandboxState) db.Sandbox {
	t.Helper()
	res := create(t, s, db.CreateSandboxParams{Owner: own})
	if state == "" {
		state = db.StateReady
	}
	genExec(t, s, `UPDATE sandboxes SET state = $2 WHERE id = $1`, res.Sandbox.ID, string(state))
	sbx, err := s.Sandbox(t.Context(), own, res.Sandbox.ID)
	if err != nil {
		t.Fatalf("reload sandbox: %v", err)
	}
	return sbx
}

func genExec(t *testing.T, s *db.Store, sql string, args ...any) {
	t.Helper()
	if _, err := s.Pool().Exec(t.Context(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// genResult is a completed-generation report cut at the given time.
func genResult(cut time.Time) db.GenerationResult {
	return db.GenerationResult{
		Image: "img-1", DiskBytes: 10 << 30, Chunks: 7, ChunksUploaded: 3,
		BytesUploaded: 3 << 20, ContentBytes: 5 << 20, Frozen: true, CutAt: cut,
	}
}

// genJob inserts a running lifecycle job with a live claim: the owner a
// pending generation row records. The row is real so the foreign key holds;
// only its id and lease token are used.
func genJob(t *testing.T, s *db.Store, own uuid.UUID, sandboxID string) db.Job {
	t.Helper()
	job := db.Job{ID: uuid.NewV7(), LeaseToken: uuid.NewV7()}
	genExec(t, s, `
		INSERT INTO jobs (id, kind, owner_id, sandbox_id, state, attempts, leased_by,
			leased_until, lease_token)
		VALUES ($1, 'sandbox.snapshot', $2, $3, 'running', 1, 'test-worker',
			now() + interval '1 hour', $4)`,
		job.ID, own, sandboxID, job.LeaseToken)
	return job
}

// genBegin begins a generation under a fresh live job and fails the test on
// error.
func genBegin(t *testing.T, s *db.Store, own uuid.UUID, sandboxID string) db.Generation {
	t.Helper()
	return genBeginAs(t, s, own, sandboxID, genJob(t, s, own, sandboxID))
}

// genBeginAs begins a generation owned by job and fails the test on error.
func genBeginAs(t *testing.T, s *db.Store, own uuid.UUID, sandboxID string, job db.Job) db.Generation {
	t.Helper()
	g, err := s.BeginGeneration(t.Context(), db.BeginGenerationParams{
		Owner: own, SandboxID: sandboxID, Timeout: time.Minute,
		JobID: job.ID, LeaseToken: job.LeaseToken,
	})
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	return g
}

// genComplete begins and completes one generation cut at cut.
func genComplete(t *testing.T, s *db.Store, own uuid.UUID, sandboxID string, cut time.Time) db.Generation {
	t.Helper()
	g := genBegin(t, s, own, sandboxID)
	done, err := s.CompleteGeneration(t.Context(), g.ID, genResult(cut), cut.Add(time.Hour))
	if err != nil {
		t.Fatalf("complete generation: %v", err)
	}
	return done
}

// TestPhase2Migration covers up, the guarded down and up again, plus the
// backfill of rows that predate the migration.
func TestPhase2Migration(t *testing.T) {
	s, own := genStore(t)
	provider := phase1Provider(t, s)
	ctx := t.Context()

	if _, err := provider.DownTo(ctx, 6); err != nil {
		t.Fatalf("down to 6 (empty): %v", err)
	}
	for _, table := range []string{"snapshots", "gc_state"} {
		if n := count(t, s, `SELECT count(*) FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = $1`, table); n != 0 {
			t.Fatalf("table %s survived the down migration", table)
		}
	}

	// Rows written under the previous schema: a live one with a node, an
	// archived one and an errored one.
	base := time.Now().UTC().Truncate(time.Microsecond)
	for _, f := range []struct{ id, state string }{
		{"bx_legacy01", "ready"}, {"bx_legacy02", "archived"}, {"bx_legacy03", "error"},
	} {
		genExec(t, s, `
			INSERT INTO sandboxes (id, owner_id, state, machine_type, name, node_id, created_at)
			VALUES ($1, $2, $3, 'default', $1, 'node-a', $4)`, f.id, own, f.state, base)
	}
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("up: %v", err)
	}
	for _, f := range []struct {
		id       string
		wantDisk bool
	}{{"bx_legacy01", true}, {"bx_legacy02", true}, {"bx_legacy03", false}} {
		var (
			chain   uuid.UUID
			enabled bool
			disk    *string
		)
		if err := s.Pool().QueryRow(ctx, `SELECT chain_id, snapshots_enabled, disk_node_id
			FROM sandboxes WHERE id = $1`, f.id).Scan(&chain, &enabled, &disk); err != nil {
			t.Fatalf("backfill %s: %v", f.id, err)
		}
		if chain == uuid.Nil() || !enabled {
			t.Fatalf("%s backfill: chain=%v enabled=%v", f.id, chain, enabled)
		}
		if f.wantDisk != (disk != nil && *disk == "node-a") {
			t.Fatalf("%s disk_node_id = %v, want node-a: %v", f.id, disk, f.wantDisk)
		}
	}
	if n := count(t, s, `SELECT count(DISTINCT chain_id) FROM sandboxes`); n != 3 {
		t.Fatalf("distinct backfilled chains = %d, want 3", n)
	}
	if n := count(t, s, `SELECT count(*) FROM gc_state WHERE id AND NOT sweeping`); n != 1 {
		t.Fatalf("gc_state rows = %d, want the one idle singleton", n)
	}

	// A generation row blocks the rollback; removing it lets it through.
	sbx := genSandbox(t, s, own, db.StateReady)
	g := genBegin(t, s, own, sbx.ID)
	if _, err := provider.DownTo(ctx, 6); err == nil {
		t.Fatal("down with a generation row succeeded")
	}
	genExec(t, s, `DELETE FROM snapshots WHERE id = $1`, g.ID)

	// So does a snapshot job.
	if _, err := s.EnqueueSnapshotJob(ctx, own, sbx.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(ctx, 6); err == nil {
		t.Fatal("down with a snapshot job succeeded")
	}
	genExec(t, s, `DELETE FROM jobs WHERE kind = 'sandbox.snapshot'`)

	if _, err := provider.DownTo(ctx, 6); err != nil {
		t.Fatalf("down after cleanup: %v", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("up again: %v", err)
	}
}

// TestCreateSandboxChain proves create assigns a fresh chain per sandbox and
// stores the snapshots flag (nil means on).
func TestCreateSandboxChain(t *testing.T) {
	s, own := genStore(t)
	off := false
	on := true
	seen := map[uuid.UUID]bool{}
	for _, tc := range []struct {
		name string
		in   *bool
		want bool
	}{{"default", nil, true}, {"explicit on", &on, true}, {"off", &off, false}} {
		res := create(t, s, db.CreateSandboxParams{Owner: own, SnapshotsEnabled: tc.in})
		got, err := s.Sandbox(t.Context(), own, res.Sandbox.ID)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got.SnapshotsEnabled != tc.want {
			t.Errorf("%s: SnapshotsEnabled = %v, want %v", tc.name, got.SnapshotsEnabled, tc.want)
		}
		if got.ChainID == uuid.Nil() || seen[got.ChainID] {
			t.Errorf("%s: chain id %v is zero or reused", tc.name, got.ChainID)
		}
		seen[got.ChainID] = true
		if res.Sandbox.ChainID != got.ChainID {
			t.Errorf("%s: create returned chain %v, stored %v", tc.name, res.Sandbox.ChainID, got.ChainID)
		}
		if got.DiskNodeID == nil || *got.DiskNodeID != "node-a" {
			t.Errorf("%s: DiskNodeID = %v, want node-a", tc.name, got.DiskNodeID)
		}
		if got.NextSnapshotAt != nil || got.HydratedAt != nil || got.DataExpiredAt != nil || got.LastRecoverAt != nil {
			t.Errorf("%s: unset timestamps are not nil: %+v", tc.name, got)
		}
	}
}

// TestBeginGenerationNumbering proves numbers go 0, 1, 2 over every row of the
// chain: a failed row keeps its number and a pending row blocks a second begin.
func TestBeginGenerationNumbering(t *testing.T) {
	s, own := genStore(t)
	sbx := genSandbox(t, s, own, "")
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Microsecond)

	job := genJob(t, s, own, sbx.ID)
	g0 := genBeginAs(t, s, own, sbx.ID, job)
	if g0.Number != 0 || g0.Kind() != "base" || g0.Status != db.GenerationPending {
		t.Fatalf("first generation = %+v", g0)
	}
	if g0.JobID != job.ID || g0.LeaseToken != job.LeaseToken {
		t.Fatalf("first generation owner = %v/%v, want %v/%v", g0.JobID, g0.LeaseToken, job.ID, job.LeaseToken)
	}
	var storedJob, storedToken *uuid.UUID
	if err := s.Pool().QueryRow(ctx,
		`SELECT job_id, lease_token FROM snapshots WHERE id = $1`, g0.ID).Scan(&storedJob, &storedToken); err != nil {
		t.Fatal(err)
	}
	if storedJob == nil || *storedJob != job.ID || storedToken == nil || *storedToken != job.LeaseToken {
		t.Fatalf("stored owner = %v/%v, want %v/%v", storedJob, storedToken, job.ID, job.LeaseToken)
	}
	if g0.ChainID != sbx.ChainID || g0.OwnerID != own || g0.SandboxID != sbx.ID || g0.NodeID != "node-a" {
		t.Fatalf("first generation identity = %+v", g0)
	}
	if want := snapformat.ManifestKey(sbx.ChainID.String(), 0); g0.ManifestKey != want {
		t.Fatalf("manifest key = %q, want %q", g0.ManifestKey, want)
	}
	if !g0.DeadlineAt.After(g0.StartedAt) || g0.CutAt != nil || g0.CompletedAt != nil {
		t.Fatalf("pending timestamps = %+v", g0)
	}

	if _, err := s.BeginGeneration(ctx, db.BeginGenerationParams{
		Owner: own, SandboxID: sbx.ID, Timeout: time.Minute,
		JobID: job.ID, LeaseToken: job.LeaseToken,
	}); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("begin while pending = %v, want ErrConflict", err)
	}
	done, err := s.CompleteGeneration(ctx, g0.ID, genResult(now), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != db.GenerationCompleted || done.Image != "img-1" || done.DiskBytes != 10<<30 ||
		done.Chunks != 7 || done.ChunksUploaded != 3 || done.BytesUploaded != 3<<20 ||
		done.ContentBytes != 5<<20 || !done.Frozen || done.CutAt == nil || !done.CutAt.Equal(now) ||
		done.CompletedAt == nil {
		t.Fatalf("completed generation = %+v", done)
	}

	g1 := genBegin(t, s, own, sbx.ID)
	if g1.Number != 1 || g1.Kind() != "incremental" {
		t.Fatalf("second generation = %+v", g1)
	}
	failed, err := s.FailGeneration(ctx, g1.ID, "node_unreachable", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != db.GenerationFailed || failed.Error != "node_unreachable" || failed.CompletedAt != nil {
		t.Fatalf("failed generation = %+v", failed)
	}

	// The failed number 1 is consumed: the next attempt is 2.
	if g2 := genBegin(t, s, own, sbx.ID); g2.Number != 2 {
		t.Fatalf("after a failed row number = %d, want 2", g2.Number)
	}

	// Completing and failing scheduled the sandbox.
	got, err := s.Sandbox(ctx, own, sbx.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.NextSnapshotAt == nil || !got.NextSnapshotAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("next_snapshot_at = %v, want the failure back-off %v", got.NextSnapshotAt, now.Add(time.Minute))
	}
}

// TestBeginGenerationNumberLimit proves the chain number never wraps past
// MaxUint32.
func TestBeginGenerationNumberLimit(t *testing.T) {
	s, own := genStore(t)
	sbx := genSandbox(t, s, own, "")
	genExec(t, s, `
		INSERT INTO snapshots (id, owner_id, sandbox_id, chain_id, generation, kind, status,
			manifest_key, node_id, deadline_at)
		VALUES ($1, $2, $3, $4, 4294967295, 'incremental', 'failed', 'k', 'node-a', now())`,
		uuid.NewV7(), own, sbx.ID, sbx.ChainID)
	job := genJob(t, s, own, sbx.ID)
	_, err := s.BeginGeneration(t.Context(), db.BeginGenerationParams{
		Owner: own, SandboxID: sbx.ID, Timeout: time.Minute,
		JobID: job.ID, LeaseToken: job.LeaseToken,
	})
	if err == nil || errors.Is(err, db.ErrConflict) {
		t.Fatalf("begin past MaxUint32 = %v, want a non-conflict error", err)
	}
	if n := count(t, s, `SELECT count(*) FROM snapshots WHERE sandbox_id = $1`, sbx.ID); n != 1 {
		t.Fatalf("snapshot rows = %d, want only the fixture", n)
	}
}

// TestBeginGenerationConcurrent proves exactly one of many simultaneous
// begins for one sandbox wins and the rest see ErrConflict.
func TestBeginGenerationConcurrent(t *testing.T) {
	s, own := genStore(t)
	sbx := genSandbox(t, s, own, "")
	const workers = 8
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		wins int
		errs []error
	)
	for range workers {
		job := genJob(t, s, own, sbx.ID)
		wg.Go(func() {
			_, err := s.BeginGeneration(t.Context(), db.BeginGenerationParams{
				Owner: own, SandboxID: sbx.ID, Timeout: time.Minute,
				JobID: job.ID, LeaseToken: job.LeaseToken,
			})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				wins++
			} else {
				errs = append(errs, err)
			}
		})
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("winners = %d, want exactly 1 (errors: %v)", wins, errs)
	}
	for _, err := range errs {
		if !errors.Is(err, db.ErrConflict) {
			t.Fatalf("loser error = %v, want ErrConflict", err)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM snapshots WHERE sandbox_id = $1`, sbx.ID); n != 1 {
		t.Fatalf("snapshot rows = %d, want 1", n)
	}
}

// TestBeginGenerationLiveOwnerBlocks proves a pending row whose job still
// holds the claim it recorded blocks the next attempt, whichever claim asks,
// and stays pending.
func TestBeginGenerationLiveOwnerBlocks(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	sbx := genSandbox(t, s, own, "")
	job := genJob(t, s, own, sbx.ID)
	g := genBeginAs(t, s, own, sbx.ID, job)

	other := genJob(t, s, own, sbx.ID)
	for _, tc := range []struct {
		name string
		job  db.Job
	}{
		{"the same claim", job},
		{"another live claim", other},
	} {
		_, err := s.BeginGeneration(ctx, db.BeginGenerationParams{
			Owner: own, SandboxID: sbx.ID, Timeout: time.Minute,
			JobID: tc.job.ID, LeaseToken: tc.job.LeaseToken,
		})
		if !errors.Is(err, db.ErrConflict) {
			t.Fatalf("%s: begin = %v, want ErrConflict", tc.name, err)
		}
	}
	var status db.GenerationStatus
	var reason string
	if err := s.Pool().QueryRow(ctx,
		`SELECT status, error FROM snapshots WHERE id = $1`, g.ID).Scan(&status, &reason); err != nil {
		t.Fatal(err)
	}
	if status != db.GenerationPending || reason != "" {
		t.Fatalf("pending row after a refused begin = %s %q, want pending unchanged", status, reason)
	}
	if n := count(t, s, `SELECT count(*) FROM snapshots WHERE sandbox_id = $1`, sbx.ID); n != 1 {
		t.Fatalf("snapshot rows = %d, want only the pending one", n)
	}
}

// TestBeginGenerationTakeover proves a pending row whose owning attempt died
// no longer blocks: the row becomes failed "abandoned" and the new attempt
// takes the next number. A dead owner is a finished, failed or requeued job,
// an expired lease, or the same job reclaimed under a new token.
func TestBeginGenerationTakeover(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	tests := []struct {
		name string
		kill func(t *testing.T, job uuid.UUID)
	}{
		{"job done", func(t *testing.T, id uuid.UUID) {
			genExec(t, s, `UPDATE jobs SET state = 'done', leased_until = NULL, lease_token = NULL
				WHERE id = $1`, id)
		}},
		{"job failed", func(t *testing.T, id uuid.UUID) {
			genExec(t, s, `UPDATE jobs SET state = 'failed', leased_until = NULL, lease_token = NULL
				WHERE id = $1`, id)
		}},
		{"job requeued", func(t *testing.T, id uuid.UUID) {
			genExec(t, s, `UPDATE jobs SET state = 'queued', leased_until = NULL, lease_token = NULL
				WHERE id = $1`, id)
		}},
		{"lease expired", func(t *testing.T, id uuid.UUID) {
			genExec(t, s, `UPDATE jobs SET leased_until = now() - interval '1 second' WHERE id = $1`, id)
		}},
		{"reclaimed under a new token", func(t *testing.T, id uuid.UUID) {
			genExec(t, s, `UPDATE jobs SET lease_token = $2, leased_until = now() + interval '1 hour'
				WHERE id = $1`, id, uuid.NewV7())
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sbx := genSandbox(t, s, own, "")
			old := genJob(t, s, own, sbx.ID)
			first := genBeginAs(t, s, own, sbx.ID, old)
			if first.Number != 0 {
				t.Fatalf("first generation = %d, want 0", first.Number)
			}
			test.kill(t, old.ID)

			next := genBegin(t, s, own, sbx.ID)
			if next.Number != 1 {
				t.Fatalf("number after taking over a dead attempt = %d, want 1", next.Number)
			}
			if next.Status != db.GenerationPending || next.JobID == old.ID {
				t.Fatalf("new attempt = %+v, want a pending row owned by the new job", next)
			}
			var status db.GenerationStatus
			var reason string
			if err := s.Pool().QueryRow(ctx,
				`SELECT status, error FROM snapshots WHERE id = $1`, first.ID).Scan(&status, &reason); err != nil {
				t.Fatal(err)
			}
			if status != db.GenerationFailed || reason != "abandoned" {
				t.Fatalf("taken-over row = %s %q, want failed abandoned", status, reason)
			}
		})
	}
}

// TestBeginGenerationLegacyPending covers a pending row written before the
// owner columns: before its deadline it still blocks; once the deadline
// passed it is taken over.
func TestBeginGenerationLegacyPending(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	sbx := genSandbox(t, s, own, "")
	legacy := uuid.NewV7()
	genExec(t, s, `
		INSERT INTO snapshots (id, owner_id, sandbox_id, chain_id, generation, kind, status,
			manifest_key, node_id, deadline_at)
		VALUES ($1, $2, $3, $4, 0, 'base', 'pending', 'legacy', 'node-a',
			now() + interval '1 hour')`, legacy, own, sbx.ID, sbx.ChainID)
	job := genJob(t, s, own, sbx.ID)
	begin := func() (db.Generation, error) {
		return s.BeginGeneration(ctx, db.BeginGenerationParams{
			Owner: own, SandboxID: sbx.ID, Timeout: time.Minute,
			JobID: job.ID, LeaseToken: job.LeaseToken,
		})
	}

	if _, err := begin(); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("begin over a legacy row before its deadline = %v, want ErrConflict", err)
	}
	genExec(t, s, `UPDATE snapshots SET deadline_at = now() - interval '1 second' WHERE id = $1`, legacy)
	next, err := begin()
	if err != nil {
		t.Fatalf("begin over a legacy row past its deadline: %v", err)
	}
	if next.Number != 1 {
		t.Fatalf("number after taking over the legacy row = %d, want 1", next.Number)
	}
	var status db.GenerationStatus
	var reason string
	if err := s.Pool().QueryRow(ctx,
		`SELECT status, error FROM snapshots WHERE id = $1`, legacy).Scan(&status, &reason); err != nil {
		t.Fatal(err)
	}
	if status != db.GenerationFailed || reason != "abandoned" {
		t.Fatalf("legacy row after a takeover = %s %q, want failed abandoned", status, reason)
	}
}

// TestBeginGenerationTakeoverConcurrent proves two simultaneous Begins over
// one dead row cannot both take it over: exactly one wins and the other sees
// the winner's live pending row.
func TestBeginGenerationTakeoverConcurrent(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	sbx := genSandbox(t, s, own, "")
	dead := genJob(t, s, own, sbx.ID)
	genBeginAs(t, s, own, sbx.ID, dead)
	genExec(t, s, `UPDATE jobs SET leased_until = now() - interval '1 second' WHERE id = $1`, dead.ID)

	jobs := []db.Job{genJob(t, s, own, sbx.ID), genJob(t, s, own, sbx.ID)}
	start := make(chan struct{})
	errs := make([]error, len(jobs))
	var wg sync.WaitGroup
	for i, job := range jobs {
		wg.Go(func() {
			<-start
			_, errs[i] = s.BeginGeneration(ctx, db.BeginGenerationParams{
				Owner: own, SandboxID: sbx.ID, Timeout: time.Minute,
				JobID: job.ID, LeaseToken: job.LeaseToken,
			})
		})
	}
	close(start)
	wg.Wait()

	wins := 0
	for i, err := range errs {
		switch {
		case err == nil:
			wins++
		case !errors.Is(err, db.ErrConflict):
			t.Fatalf("begin %d = %v, want success or ErrConflict", i, err)
		}
	}
	if wins != 1 {
		t.Fatalf("winners = %d, want exactly 1 (errors: %v)", wins, errs)
	}
	if n := count(t, s, `SELECT count(*) FROM snapshots WHERE sandbox_id = $1
		AND status = 'failed' AND error = 'abandoned'`, sbx.ID); n != 1 {
		t.Fatalf("abandoned rows = %d, want 1", n)
	}
	if n := count(t, s, `SELECT count(*) FROM snapshots WHERE sandbox_id = $1
		AND status = 'pending'`, sbx.ID); n != 1 {
		t.Fatalf("pending rows = %d, want 1", n)
	}
}

// TestTakenOverAttemptCannotComplete is the losing interleaving of a
// takeover: the dead worker wakes up after its row was failed and must not
// resurrect it, while the new attempt still completes normally.
func TestTakenOverAttemptCannotComplete(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Microsecond)
	sbx := genSandbox(t, s, own, "")
	dead := genJob(t, s, own, sbx.ID)
	old := genBeginAs(t, s, own, sbx.ID, dead)
	genExec(t, s, `UPDATE jobs SET leased_until = now() - interval '1 second' WHERE id = $1`, dead.ID)
	next := genBegin(t, s, own, sbx.ID)

	if _, err := s.CompleteGeneration(ctx, old.ID, genResult(now), now); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("complete of a taken-over row = %v, want ErrConflict", err)
	}
	late, err := s.FailGeneration(ctx, old.ID, "timeout", now)
	if err != nil || late.Status != db.GenerationFailed || late.Error != "abandoned" {
		t.Fatalf("late fail of a taken-over row = %+v, %v; want failed abandoned unchanged", late, err)
	}
	done, err := s.CompleteGeneration(ctx, next.ID, genResult(now), now)
	if err != nil || done.Status != db.GenerationCompleted {
		t.Fatalf("the new attempt's complete = %+v, %v", done, err)
	}
}

// genBackends dumps the database's other backends and what they wait for, so
// a failed lock-order wait shows why the expected blocker was not found.
func genBackends(t *testing.T, tx pgx.Tx) string {
	t.Helper()
	var dump string
	if err := tx.QueryRow(t.Context(), `
		SELECT coalesce(string_agg(
			pid::text || ' ' || coalesce(state, '') || ' ' ||
			coalesce(wait_event_type, '-') || '/' || coalesce(wait_event, '-') || ' ' ||
			coalesce(left(query, 60), ''), ' | '), '')
		FROM pg_stat_activity
		WHERE datname = current_database() AND pid <> pg_backend_pid()`).Scan(&dump); err != nil {
		return "unavailable: " + err.Error()
	}
	return dump
}

// TestGenerationLocksTakeTheSandboxFirst proves Complete and Fail take the
// sandbox row before the snapshot row — the order BeginGeneration's takeover
// uses. A completion that takes the snapshot row first deadlocks a takeover
// holding the sandbox row.
//
// The test holds the snapshot row and starts a completion. With the right
// order the completion takes the sandbox row and waits for the snapshot row,
// so a probe of the sandbox row with a short lock_timeout must WAIT; with the
// old order the completion waits for the snapshot row without ever taking the
// sandbox row, so every probe is granted and the test fails. No stats view or
// fixed sleep is involved.
func TestGenerationLocksTakeTheSandboxFirst(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Microsecond)
	sbx := genSandbox(t, s, own, "")
	g := genBegin(t, s, own, sbx.ID)

	// Hold the snapshot row: the completion cannot get past it.
	hold, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hold.Rollback(context.WithoutCancel(ctx)) })
	var held string
	if err := hold.QueryRow(ctx,
		`SELECT id FROM snapshots WHERE id = $1 FOR UPDATE`, g.ID).Scan(&held); err != nil {
		t.Fatalf("hold the snapshot row: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := s.CompleteGeneration(ctx, g.ID, genResult(now), now)
		done <- err
	}()

	start := time.Now()
	deadline := start.Add(5 * time.Second)
	waited := false
	for !waited {
		select {
		case err := <-done:
			t.Fatalf("the completion returned instead of waiting for the snapshot row: %v", err)
		default:
		}
		// A conflicting lock request on the sandbox row: granted while the
		// completion has not taken that row, timed out once it has.
		probe, err := s.Pool().Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := probe.Exec(ctx, `SET LOCAL lock_timeout = '100ms'`); err != nil {
			_ = probe.Rollback(context.WithoutCancel(ctx))
			t.Fatalf("probe lock timeout setup: %v", err)
		}
		_, lockErr := probe.Exec(ctx,
			`SELECT id FROM sandboxes WHERE id = $1 FOR NO KEY UPDATE`, sbx.ID)
		_ = probe.Rollback(context.WithoutCancel(ctx))
		if pgErr, ok := errors.AsType[*pgconn.PgError](lockErr); ok && pgErr.Code == "55P03" {
			waited = true
		} else if lockErr != nil {
			t.Fatalf("probe the sandbox row: %v", lockErr)
		}
		if !waited {
			if time.Now().After(deadline) {
				t.Fatalf("the completion held the snapshot row and never took the sandbox row "+
					"(after %v; %s)", time.Since(start).Round(time.Millisecond), genBackends(t, hold))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// The completion holds the sandbox row and waits for the snapshot row:
	// releasing it lets the completion finish.
	if err := hold.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("complete generation: %v", err)
	}
}

// TestBeginGenerationRefusals covers the GC fence, disabled sandboxes, owner
// scoping, deleted and node-less sandboxes, a bad timeout and a missing
// owning job or lease token.
func TestBeginGenerationRefusals(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	sbx := genSandbox(t, s, own, "")
	// The job row only has to exist for the pending row's foreign key; used
	// for begins whose target sandbox is refused or missing.
	anchor := genSandbox(t, s, own, "")
	// begin starts under a live job of the sandbox's owner.
	begin := func(owner uuid.UUID, sandboxID string, timeout time.Duration) error {
		t.Helper()
		job := genJob(t, s, owner, anchor.ID)
		_, err := s.BeginGeneration(ctx, db.BeginGenerationParams{
			Owner: owner, SandboxID: sandboxID, Timeout: timeout,
			JobID: job.ID, LeaseToken: job.LeaseToken,
		})
		return err
	}

	genExec(t, s, `UPDATE gc_state SET sweeping = true`)
	if err := begin(own, sbx.ID, time.Minute); !errors.Is(err, db.ErrGCSweeping) {
		t.Fatalf("begin while sweeping = %v, want ErrGCSweeping", err)
	}
	genExec(t, s, `UPDATE gc_state SET sweeping = false`)
	if n := count(t, s, `SELECT count(*) FROM snapshots`); n != 0 {
		t.Fatalf("a refused begin left %d rows", n)
	}

	off := false
	disabled := create(t, s, db.CreateSandboxParams{Owner: own, SnapshotsEnabled: &off}).Sandbox
	if err := begin(own, disabled.ID, time.Minute); !errors.Is(err, db.ErrSnapshotsDisabled) {
		t.Fatalf("begin on a disabled sandbox = %v, want ErrSnapshotsDisabled", err)
	}

	if err := begin(owner(t, s), sbx.ID, time.Minute); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("begin as a foreign owner = %v, want ErrNotFound", err)
	}
	if err := begin(own, "bx_missing01", time.Minute); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("begin on a missing sandbox = %v, want ErrNotFound", err)
	}
	if err := begin(own, sbx.ID, 0); err == nil {
		t.Fatal("begin with a zero timeout succeeded")
	}

	job := genJob(t, s, own, sbx.ID)
	if _, err := s.BeginGeneration(ctx, db.BeginGenerationParams{
		Owner: own, SandboxID: sbx.ID, Timeout: time.Minute, LeaseToken: job.LeaseToken,
	}); err == nil {
		t.Fatal("begin without the owning job succeeded")
	}
	if _, err := s.BeginGeneration(ctx, db.BeginGenerationParams{
		Owner: own, SandboxID: sbx.ID, Timeout: time.Minute, JobID: job.ID,
	}); err == nil {
		t.Fatal("begin without a lease token succeeded")
	}

	nodeless := genSandbox(t, s, own, "")
	genExec(t, s, `UPDATE sandboxes SET node_id = NULL WHERE id = $1`, nodeless.ID)
	if err := begin(own, nodeless.ID, time.Minute); !errors.Is(err, db.ErrInvalidState) {
		t.Fatalf("begin on a node-less sandbox = %v, want ErrInvalidState", err)
	}

	genExec(t, s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, sbx.ID)
	if err := begin(own, sbx.ID, time.Minute); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("begin on a deleted sandbox = %v, want ErrNotFound", err)
	}
}

// TestGenerationStateMachine covers Complete and Fail transitions and their
// idempotency.
func TestGenerationStateMachine(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Microsecond)
	sbx := genSandbox(t, s, own, "")

	completed := genComplete(t, s, own, sbx.ID, now)
	// Failing a completed row is a conflict and changes nothing.
	if _, err := s.FailGeneration(ctx, completed.ID, "late", now); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("fail after complete = %v, want ErrConflict", err)
	}
	// Repeating Complete returns the same row, even with a different result
	// and schedule.
	other := genResult(now.Add(time.Hour))
	other.Image = "other"
	again, err := s.CompleteGeneration(ctx, completed.ID, other, now.Add(48*time.Hour))
	if err != nil {
		t.Fatalf("repeat complete: %v", err)
	}
	if again.Image != completed.Image || !again.CutAt.Equal(*completed.CutAt) || again.Status != db.GenerationCompleted {
		t.Fatalf("repeat complete changed the row: %+v vs %+v", again, completed)
	}
	got, err := s.Sandbox(ctx, own, sbx.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.NextSnapshotAt == nil || !got.NextSnapshotAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("repeat complete moved next_snapshot_at to %v", got.NextSnapshotAt)
	}

	// A failed row cannot be completed; failing again is a no-op.
	pending := genBegin(t, s, own, sbx.ID)
	failed, err := s.FailGeneration(ctx, pending.ID, "first", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteGeneration(ctx, pending.ID, genResult(now), now); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("complete after fail = %v, want ErrConflict", err)
	}
	repeat, err := s.FailGeneration(ctx, pending.ID, "second", now)
	if err != nil || repeat.Error != failed.Error {
		t.Fatalf("repeat fail = %+v, %v; want the first reason kept", repeat, err)
	}

	for name, call := range map[string]func() error{
		"complete": func() error {
			_, err := s.CompleteGeneration(ctx, uuid.NewV7(), genResult(now), now)
			return err
		},
		"fail": func() error {
			_, err := s.FailGeneration(ctx, uuid.NewV7(), "x", now)
			return err
		},
	} {
		if err := call(); !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("%s of an unknown id = %v, want ErrNotFound", name, err)
		}
	}
	if _, err := s.CompleteGeneration(ctx, pending.ID, db.GenerationResult{}, now); err == nil {
		t.Fatal("complete without a cut time succeeded")
	}
}

// TestListGenerations covers scoping, filters, ordering, exclusions, limit
// clamping and a keyset walk across equal timestamps.
func TestListGenerations(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	cut := time.Now().UTC().Truncate(time.Second)

	a := genSandbox(t, s, own, "")
	b := genSandbox(t, s, own, "")
	foreignOwner := owner(t, s)
	foreign := genSandbox(t, s, foreignOwner, "")
	deleted := genSandbox(t, s, own, "")

	// Four rows share one cut time across sandboxes; the rest are distinct.
	want := map[uuid.UUID]bool{}
	for i, sandboxID := range []string{a.ID, a.ID, a.ID, b.ID, b.ID, b.ID, b.ID} {
		at := cut
		if i >= 4 {
			at = cut.Add(time.Duration(i) * time.Minute)
		}
		want[genComplete(t, s, own, sandboxID, at).ID] = true
	}
	foreignGen := genComplete(t, s, foreignOwner, foreign.ID, cut)
	genComplete(t, s, own, deleted.ID, cut)
	genExec(t, s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, deleted.ID)

	// Excluded: pending, failed and purged rows.
	pending := genBegin(t, s, own, a.ID)
	failedRow := genBegin(t, s, own, b.ID)
	if _, err := s.FailGeneration(ctx, failedRow.ID, "x", cut); err != nil {
		t.Fatal(err)
	}
	purged := genComplete(t, s, own, b.ID, cut.Add(time.Hour))
	genExec(t, s, `UPDATE snapshots SET purged_at = now() WHERE id = $1`, purged.ID)

	all, next, err := s.ListGenerations(ctx, db.GenerationListQuery{Owner: own})
	if err != nil || next != nil {
		t.Fatalf("list all = %d rows, cursor %v, %v", len(all), next, err)
	}
	if len(all) != len(want) {
		t.Fatalf("list all returned %d rows, want %d", len(all), len(want))
	}
	for _, g := range all {
		if !want[g.ID] || g.OwnerID != own || g.Status != db.GenerationCompleted || g.PurgedAt != nil {
			t.Fatalf("unexpected listed row %+v (pending %v, foreign %v)", g, pending.ID, foreignGen.ID)
		}
	}
	isDesc := func(rows []db.Generation) bool {
		return slices.IsSortedFunc(rows, func(x, y db.Generation) int {
			if c := y.CutAt.Compare(*x.CutAt); c != 0 {
				return c
			}
			return slices.Compare(y.ID[:], x.ID[:])
		})
	}
	if !isDesc(all) {
		t.Fatal("default order is not (cut_at, id) descending")
	}

	// Ascending is the exact reverse.
	asc, _, err := s.ListGenerations(ctx, db.GenerationListQuery{Owner: own, Ascending: true})
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(asc)
	if !slices.EqualFunc(asc, all, func(x, y db.Generation) bool { return x.ID == y.ID }) {
		t.Fatal("ascending is not the reverse of descending")
	}

	// Sandbox filter, including a foreign and a deleted sandbox.
	onlyA, _, err := s.ListGenerations(ctx, db.GenerationListQuery{Owner: own, SandboxID: a.ID})
	if err != nil || len(onlyA) != 3 {
		t.Fatalf("list sandbox a = %d rows, %v; want 3", len(onlyA), err)
	}
	for _, g := range onlyA {
		if g.SandboxID != a.ID {
			t.Fatalf("sandbox filter leaked %+v", g)
		}
	}
	for name, id := range map[string]string{"foreign": foreign.ID, "deleted": deleted.ID, "missing": "bx_missing01"} {
		rows, _, err := s.ListGenerations(ctx, db.GenerationListQuery{Owner: own, SandboxID: id})
		if err != nil || len(rows) != 0 {
			t.Fatalf("list %s sandbox = %d rows, %v; want none", name, len(rows), err)
		}
	}
	if rows, _, err := s.ListGenerations(ctx, db.GenerationListQuery{Owner: foreignOwner}); err != nil || len(rows) != 1 || rows[0].ID != foreignGen.ID {
		t.Fatalf("foreign owner list = %+v, %v", rows, err)
	}

	// A keyset walk visits every row exactly once in both directions, for
	// several page sizes, with equal timestamps in the middle.
	for _, ascending := range []bool{false, true} {
		for _, limit := range []int{1, 2, 3, 7} {
			var (
				seen   []uuid.UUID
				cursor *db.GenerationCursor
			)
			for pages := 0; ; pages++ {
				if pages > len(want)+1 {
					t.Fatalf("walk (asc=%v limit=%d) did not terminate", ascending, limit)
				}
				rows, nextCursor, err := s.ListGenerations(ctx, db.GenerationListQuery{
					Owner: own, Limit: limit, Ascending: ascending, After: cursor,
				})
				if err != nil {
					t.Fatal(err)
				}
				for _, g := range rows {
					seen = append(seen, g.ID)
				}
				if nextCursor == nil {
					break
				}
				if len(rows) != limit {
					t.Fatalf("a page with a next cursor has %d rows, want %d", len(rows), limit)
				}
				cursor = nextCursor
			}
			wantOrder := make([]uuid.UUID, len(all))
			for i, g := range all {
				wantOrder[i] = g.ID
			}
			if ascending {
				slices.Reverse(wantOrder)
			}
			if !slices.Equal(seen, wantOrder) {
				t.Fatalf("walk (asc=%v limit=%d) saw %v, want %v", ascending, limit, seen, wantOrder)
			}
		}
	}

	// Limits clamp: a negative one to a single row, an oversized one to the
	// maximum (all rows here).
	one, next, err := s.ListGenerations(ctx, db.GenerationListQuery{Owner: own, Limit: -5})
	if err != nil || len(one) != 1 || next == nil {
		t.Fatalf("limit -5 = %d rows, cursor %v, %v; want 1 row and a cursor", len(one), next, err)
	}
	if rows, next, err := s.ListGenerations(ctx, db.GenerationListQuery{Owner: own, Limit: 100000}); err != nil || len(rows) != len(want) || next != nil {
		t.Fatalf("limit 100000 = %d rows, cursor %v, %v", len(rows), next, err)
	}
}

// TestLatestGeneration proves it returns the newest completed, unpurged
// generation of the owner's own live sandbox.
func TestLatestGeneration(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	cut := time.Now().UTC().Truncate(time.Second)
	sbx := genSandbox(t, s, own, "")

	if _, ok, err := s.LatestGeneration(ctx, own, sbx.ID); err != nil || ok {
		t.Fatalf("latest of an empty chain = ok %v, %v", ok, err)
	}
	g0 := genComplete(t, s, own, sbx.ID, cut)
	if got, ok, err := s.LatestGeneration(ctx, own, sbx.ID); err != nil || !ok || got.ID != g0.ID {
		t.Fatalf("latest = %+v ok %v, %v; want %v", got, ok, err, g0.ID)
	}
	g1 := genComplete(t, s, own, sbx.ID, cut.Add(time.Minute))
	// A pending and a failed row never count.
	genBegin(t, s, own, sbx.ID)
	if got, ok, err := s.LatestGeneration(ctx, own, sbx.ID); err != nil || !ok || got.ID != g1.ID {
		t.Fatalf("latest with a pending row = %+v ok %v, %v; want %v", got, ok, err, g1.ID)
	}
	// Purged rows are skipped.
	genExec(t, s, `UPDATE snapshots SET purged_at = now() WHERE id = $1`, g1.ID)
	if got, ok, err := s.LatestGeneration(ctx, own, sbx.ID); err != nil || !ok || got.ID != g0.ID {
		t.Fatalf("latest with g1 purged = %+v ok %v, %v; want %v", got, ok, err, g0.ID)
	}
	// Another chain of the same sandbox counts by cut time.
	otherChain := uuid.NewV7()
	genExec(t, s, `
		INSERT INTO snapshots (id, owner_id, sandbox_id, chain_id, generation, kind, status,
			manifest_key, node_id, image, disk_bytes, chunks, chunks_uploaded, bytes_uploaded,
			content_bytes, frozen, cut_at, completed_at, deadline_at)
		VALUES ($1, $2, $3, $4, 0, 'base', 'completed', 'k', 'node-a', 'img', 1, 1, 1, 1, 1, false,
			$5, now(), now())`, uuid.NewV7(), own, sbx.ID, otherChain, cut.Add(time.Hour))
	if got, ok, err := s.LatestGeneration(ctx, own, sbx.ID); err != nil || !ok || got.ChainID != otherChain {
		t.Fatalf("latest across chains = %+v ok %v, %v; want chain %v", got, ok, err, otherChain)
	}

	if _, ok, err := s.LatestGeneration(ctx, owner(t, s), sbx.ID); err != nil || ok {
		t.Fatalf("latest as a foreign owner = ok %v, %v", ok, err)
	}
	genExec(t, s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, sbx.ID)
	if _, ok, err := s.LatestGeneration(ctx, own, sbx.ID); err != nil || ok {
		t.Fatalf("latest of a deleted sandbox = ok %v, %v", ok, err)
	}
}

// TestDueSnapshotSandboxes covers every filter of the worker's scan and its
// ordering.
func TestDueSnapshotSandboxes(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Microsecond)

	never := genSandbox(t, s, own, db.StateReady)
	pastOld := genSandbox(t, s, own, db.StateRunning)
	pastNew := genSandbox(t, s, own, db.StateIdle)
	exactly := genSandbox(t, s, own, db.StateReady)
	future := genSandbox(t, s, own, db.StateReady)
	genSandbox(t, s, own, db.StateProvisioning)
	genSandbox(t, s, own, db.StateArchived)
	disabled := genSandbox(t, s, own, db.StateReady)
	deleted := genSandbox(t, s, own, db.StateReady)
	busy := genSandbox(t, s, own, db.StateReady)
	foreign := genSandbox(t, s, owner(t, s), db.StateReady)

	for id, at := range map[string]time.Time{
		pastOld.ID: now.Add(-2 * time.Hour), pastNew.ID: now.Add(-time.Hour),
		exactly.ID: now, future.ID: now.Add(time.Minute),
	} {
		if err := s.SetSnapshotSchedule(ctx, own, id, at); err != nil {
			t.Fatal(err)
		}
	}
	genExec(t, s, `UPDATE sandboxes SET snapshots_enabled = false WHERE id = $1`, disabled.ID)
	genExec(t, s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, deleted.ID)
	if ok, err := s.EnqueueSnapshotJob(ctx, own, busy.ID); err != nil || !ok {
		t.Fatalf("enqueue for busy = %v, %v", ok, err)
	}

	ids := func(rows []db.Sandbox) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = r.ID
		}
		return out
	}
	got, err := s.DueSnapshotSandboxes(ctx, now, 100)
	if err != nil {
		t.Fatal(err)
	}
	// Never-snapshotted sandboxes come first (their relative order is the
	// collation's business), then the oldest due; the foreign owner's sandbox
	// is listed too because the worker is not owner-scoped.
	nulls := []string{never.ID, foreign.ID}
	slices.Sort(nulls)
	checkHead := func(rows []db.Sandbox) {
		t.Helper()
		head := ids(rows)[:2]
		slices.Sort(head)
		if !slices.Equal(head, nulls) {
			t.Fatalf("due head = %v, want the never-snapshotted %v", head, nulls)
		}
	}
	if len(got) != 5 {
		t.Fatalf("due = %v, want 5 rows", ids(got))
	}
	checkHead(got)
	if tail := ids(got)[2:]; !slices.Equal(tail, []string{pastOld.ID, pastNew.ID, exactly.ID}) {
		t.Fatalf("due tail = %v, want %v", tail, []string{pastOld.ID, pastNew.ID, exactly.ID})
	}
	// Limit applies after the ordering.
	got, err = s.DueSnapshotSandboxes(ctx, now, 3)
	if err != nil || len(got) != 3 || got[2].ID != pastOld.ID {
		t.Fatalf("due limit 3 = %v, %v; want the two never-snapshotted then %s", ids(got), err, pastOld.ID)
	}
	checkHead(got)
	// A finished job makes the sandbox due again.
	genExec(t, s, `UPDATE jobs SET state = 'done' WHERE sandbox_id = $1`, busy.ID)
	got, err = s.DueSnapshotSandboxes(ctx, now, 100)
	if err != nil || !slices.Contains(ids(got), busy.ID) {
		t.Fatalf("due after the job finished = %v, %v; want %s included", ids(got), err, busy.ID)
	}
}

// TestEnqueueSnapshotJob proves the dedupe under concurrency, owner scoping
// and re-enqueue once the job finished.
func TestEnqueueSnapshotJob(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	sbx := genSandbox(t, s, own, "")

	const workers = 8
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		enqueued int
		failures []error
	)
	for range workers {
		wg.Go(func() {
			ok, err := s.EnqueueSnapshotJob(ctx, own, sbx.ID)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				failures = append(failures, err)
			case ok:
				enqueued++
			}
		})
	}
	wg.Wait()
	if len(failures) > 0 || enqueued != 1 {
		t.Fatalf("enqueued = %d, errors = %v; want exactly one job", enqueued, failures)
	}
	q := `SELECT count(*) FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.snapshot'`
	if n := count(t, s, q, sbx.ID); n != 1 {
		t.Fatalf("snapshot jobs = %d, want 1", n)
	}
	var ownerID uuid.UUID
	if err := s.Pool().QueryRow(ctx, `SELECT owner_id FROM jobs WHERE sandbox_id = $1`, sbx.ID).Scan(&ownerID); err != nil || ownerID != own {
		t.Fatalf("job owner = %v, %v; want %v", ownerID, err, own)
	}

	// A running job still dedupes; a finished one does not.
	genExec(t, s, `UPDATE jobs SET state = 'running' WHERE sandbox_id = $1`, sbx.ID)
	if ok, err := s.EnqueueSnapshotJob(ctx, own, sbx.ID); err != nil || ok {
		t.Fatalf("enqueue while running = %v, %v; want false", ok, err)
	}
	genExec(t, s, `UPDATE jobs SET state = 'done' WHERE sandbox_id = $1`, sbx.ID)
	if ok, err := s.EnqueueSnapshotJob(ctx, own, sbx.ID); err != nil || !ok {
		t.Fatalf("enqueue after done = %v, %v; want true", ok, err)
	}
	if n := count(t, s, q, sbx.ID); n != 2 {
		t.Fatalf("snapshot jobs = %d, want 2", n)
	}

	if _, err := s.EnqueueSnapshotJob(ctx, owner(t, s), sbx.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("enqueue as a foreign owner = %v, want ErrNotFound", err)
	}
	if _, err := s.EnqueueSnapshotJob(ctx, own, "bx_missing01"); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("enqueue for a missing sandbox = %v, want ErrNotFound", err)
	}
}

// TestSetSnapshotSchedule proves the back-off write is owner scoped.
func TestSetSnapshotSchedule(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	sbx := genSandbox(t, s, own, "")
	next := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)

	if err := s.SetSnapshotSchedule(ctx, own, sbx.ID, next); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Sandbox(ctx, own, sbx.ID); err != nil || got.NextSnapshotAt == nil || !got.NextSnapshotAt.Equal(next) {
		t.Fatalf("schedule = %v, %v; want %v", got.NextSnapshotAt, err, next)
	}
	if err := s.SetSnapshotSchedule(ctx, owner(t, s), sbx.ID, next.Add(time.Hour)); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign schedule = %v, want ErrNotFound", err)
	}
	if got, _ := s.Sandbox(ctx, own, sbx.ID); !got.NextSnapshotAt.Equal(next) {
		t.Fatalf("a foreign write moved the schedule to %v", got.NextSnapshotAt)
	}
	genExec(t, s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, sbx.ID)
	if err := s.SetSnapshotSchedule(ctx, own, sbx.ID, next); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("schedule of a deleted sandbox = %v, want ErrNotFound", err)
	}
}

// TestPendingGenerations proves the fence listing: pending rows of every
// owner, oldest first, nothing else.
func TestPendingGenerations(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	now := time.Now().UTC()

	a := genSandbox(t, s, own, "")
	b := genSandbox(t, s, owner(t, s), "")
	c := genSandbox(t, s, own, "")
	done := genSandbox(t, s, own, "")

	first := genBegin(t, s, own, a.ID)
	second := genBeginAs(t, s, b.OwnerID, b.ID, genJob(t, s, b.OwnerID, b.ID))
	third := genBegin(t, s, own, c.ID)
	genComplete(t, s, own, done.ID, now)
	failed := genBegin(t, s, own, done.ID)
	if _, err := s.FailGeneration(ctx, failed.ID, "x", now); err != nil {
		t.Fatal(err)
	}
	genExec(t, s, `UPDATE snapshots SET started_at = now() - interval '3 minutes' WHERE id = $1`, second.ID)
	genExec(t, s, `UPDATE snapshots SET started_at = now() - interval '2 minutes' WHERE id = $1`, first.ID)

	got, err := s.PendingGenerations(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	var order []uuid.UUID
	for _, g := range got {
		if g.Status != db.GenerationPending {
			t.Fatalf("listed a %s row", g.Status)
		}
		order = append(order, g.ID)
	}
	if want := []uuid.UUID{second.ID, first.ID, third.ID}; !slices.Equal(order, want) {
		t.Fatalf("pending = %v, want %v", order, want)
	}
	if got, err := s.PendingGenerations(ctx, 1); err != nil || len(got) != 1 || got[0].ID != second.ID {
		t.Fatalf("pending limit 1 = %+v, %v", got, err)
	}
}

// TestJobSourceGeneration proves a job records the generation it reads from,
// the column survives a claim and the foreign key rejects an unknown id.
func TestJobSourceGeneration(t *testing.T) {
	s, own := genStore(t)
	ctx := t.Context()
	sbx := genSandbox(t, s, own, "")
	gen := genComplete(t, s, own, sbx.ID, time.Now().UTC().Truncate(time.Second))

	// CreateSandbox enqueued a provision job that would be claimed first.
	if _, err := s.CancelSandboxJobs(ctx, own, sbx.ID, db.JobProvision); err != nil {
		t.Fatal(err)
	}
	queued, err := s.EnqueueJob(ctx, db.JobInput{
		Kind: db.JobResume, OwnerID: own, SandboxID: &sbx.ID, SourceGenerationID: &gen.ID,
	})
	if err != nil {
		t.Fatalf("enqueue with a source generation: %v", err)
	}
	if queued.SourceGenerationID == nil || *queued.SourceGenerationID != gen.ID {
		t.Fatalf("enqueued SourceGenerationID = %v, want %v", queued.SourceGenerationID, gen.ID)
	}
	claimed, err := s.ClaimJob(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != queued.ID || claimed.SourceGenerationID == nil || *claimed.SourceGenerationID != gen.ID {
		t.Fatalf("claimed job = %+v, want source generation %v", claimed, gen.ID)
	}

	plain, err := s.EnqueueJob(ctx, db.JobInput{Kind: db.JobStop, OwnerID: own, SandboxID: &sbx.ID})
	if err != nil || plain.SourceGenerationID != nil {
		t.Fatalf("plain job SourceGenerationID = %v, %v; want nil", plain.SourceGenerationID, err)
	}

	missing := uuid.NewV7()
	if _, err := s.EnqueueJob(ctx, db.JobInput{
		Kind: db.JobResume, OwnerID: own, SandboxID: &sbx.ID, SourceGenerationID: &missing,
	}); err == nil {
		t.Fatal("enqueue with an unknown source generation succeeded")
	}
}
