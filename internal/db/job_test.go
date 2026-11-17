package db_test

// Durable queue behavior: claim exclusivity under concurrency, lease
// expiry and reclamation, fenced mutations, bounded retries, stale-job
// failure, notification loss and the poll fallback.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

func enqueue(t *testing.T, s *db.Store, own uuid.UUID, maxAttempts int) db.Job {
	t.Helper()
	j, err := s.EnqueueJob(t.Context(), db.JobInput{
		Kind: db.JobProvision, OwnerID: own, MaxAttempts: maxAttempts,
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return j
}

func TestEnqueueClaimComplete(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	j := enqueue(t, s, own, 0)

	if j.State != db.JobQueued || j.Attempts != 0 || j.MaxAttempts != db.DefaultMaxAttempts {
		t.Fatalf("fresh job: %+v", j)
	}
	got, err := s.ClaimJob(t.Context(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if got.ID != j.ID || got.State != db.JobRunning || got.Attempts != 1 {
		t.Fatalf("claimed: %+v", got)
	}
	if got.LeaseToken == uuid.Nil() {
		t.Fatal("claim did not mint a lease token")
	}
	if err := s.CompleteJob(t.Context(), got.ID, "w1", got.LeaseToken); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, err := s.ClaimJob(t.Context(), "w1", time.Minute); !errors.Is(err, db.ErrNoJobs) {
		t.Fatalf("claim after done: %v", err)
	}
}

func TestClaimValidatesWorkerAndLease(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	enqueue(t, s, own, 0)
	if _, err := s.ClaimJob(t.Context(), "", time.Minute); err == nil {
		t.Fatal("empty worker accepted")
	}
	if _, err := s.ClaimJob(t.Context(), "w1", 0); err == nil {
		t.Fatal("zero lease accepted")
	}
}

func TestConcurrentClaimsExactlyOnce(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	const jobs, workers = 24, 6
	for range jobs {
		enqueue(t, s, own, 0)
	}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		seen  = map[uuid.UUID]int{}
		dupes int
	)
	for w := range workers {
		wg.Go(func() {
			for {
				j, err := s.ClaimJob(t.Context(), "w"+string(rune('0'+w)), time.Minute)
				if errors.Is(err, db.ErrNoJobs) {
					return
				}
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				mu.Lock()
				seen[j.ID]++
				if seen[j.ID] > 1 {
					dupes++
				}
				mu.Unlock()
				if err := s.CompleteJob(t.Context(), j.ID, "w"+string(rune('0'+w)), j.LeaseToken); err != nil {
					t.Errorf("complete: %v", err)
				}
			}
		})
	}
	wg.Wait()
	if dupes != 0 {
		t.Fatalf("%d jobs delivered twice", dupes)
	}
	if len(seen) != jobs {
		t.Fatalf("distinct jobs claimed: %d of %d", len(seen), jobs)
	}
}

func TestExpiredLeaseReclaimedWithFencing(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	j := enqueue(t, s, own, 3)

	first, err := s.ClaimJob(t.Context(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	// While the lease is live the job is not re-deliverable and the
	// holder can act.
	if _, err := s.ClaimJob(t.Context(), "w2", time.Minute); !errors.Is(err, db.ErrNoJobs) {
		t.Fatalf("claim during live lease: %v", err)
	}
	if err := s.RenewJobLease(t.Context(), first.ID, "w1", first.LeaseToken, time.Minute); err != nil {
		t.Fatalf("renew: %v", err)
	}

	// Expire the lease on the database clock (worker crash) without
	// waiting on wall time. Before any reclaim, every mutation with the
	// stale token is rejected: Complete, Renew and Retry.
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE jobs SET leased_until = now() - interval '1 second' WHERE id = $1`, first.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteJob(t.Context(), first.ID, "w1", first.LeaseToken); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("complete after expiry, before reclaim: %v", err)
	}
	if err := s.RenewJobLease(t.Context(), first.ID, "w1", first.LeaseToken, time.Minute); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("renew after expiry, before reclaim: %v", err)
	}
	if err := s.RetryJob(t.Context(), first.ID, "w1", first.LeaseToken, time.Second, "stale"); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("retry after expiry, before reclaim: %v", err)
	}

	// Another worker — here even reusing the crashed name — reclaims the
	// job with a fresh token and bumped attempts.
	second, err := s.ClaimJob(t.Context(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if second.ID != j.ID || second.Attempts != 2 {
		t.Fatalf("reclaimed: %+v", second)
	}
	if second.LeaseToken == first.LeaseToken {
		t.Fatal("reclaim reused the lease token")
	}
	// The stale token still fences nothing.
	if err := s.CompleteJob(t.Context(), first.ID, "w1", first.LeaseToken); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("stale complete after reclaim: %v", err)
	}
	if err := s.RenewJobLease(t.Context(), first.ID, "w1", first.LeaseToken, time.Minute); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("stale renew after reclaim: %v", err)
	}
	if err := s.RetryJob(t.Context(), first.ID, "w1", first.LeaseToken, time.Second, "stale"); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("stale retry after reclaim: %v", err)
	}
	if err := s.CompleteJob(t.Context(), second.ID, "w1", second.LeaseToken); err != nil {
		t.Fatalf("fresh complete: %v", err)
	}
}

func TestRenewRejectsNonPositiveLease(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	j := enqueue(t, s, own, 0)
	claimed, err := s.ClaimJob(t.Context(), "w1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RenewJobLease(t.Context(), claimed.ID, "w1", claimed.LeaseToken, 0); err == nil {
		t.Fatal("zero lease renew accepted")
	}
	if err := s.CompleteJob(t.Context(), claimed.ID, "w1", claimed.LeaseToken); err != nil {
		t.Fatal(err)
	}
	_ = j
}

func TestRetryBackoffAndTerminalFailure(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	enqueue(t, s, own, 2)

	j, err := s.ClaimJob(t.Context(), "w1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// A future backoff: the job must not be claimable yet.
	if err := s.RetryJob(t.Context(), j.ID, "w1", j.LeaseToken, time.Hour, "node unreachable"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, err := s.ClaimJob(t.Context(), "w1", time.Minute); !errors.Is(err, db.ErrNoJobs) {
		t.Fatalf("claim before backoff elapses: %v", err)
	}
	// Make the backoff due on the database clock instead of sleeping.
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE jobs SET run_after = now() - interval '1 second' WHERE id = $1`, j.ID); err != nil {
		t.Fatal(err)
	}
	j, err = s.ClaimJob(t.Context(), "w1", time.Minute)
	if err != nil || j.Attempts != 2 {
		t.Fatalf("second claim: %v %+v", err, j)
	}
	if err := s.RetryJob(t.Context(), j.ID, "w1", j.LeaseToken, time.Second, "node unreachable"); err != nil {
		t.Fatalf("terminal retry: %v", err)
	}
	var state string
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT state FROM jobs WHERE id = $1`, j.ID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "failed" {
		t.Fatalf("state after exhausted attempts: %s", state)
	}
	if _, err := s.ClaimJob(t.Context(), "w1", time.Minute); !errors.Is(err, db.ErrNoJobs) {
		t.Fatalf("failed job re-claimed: %v", err)
	}

	failed, next, err := s.FailedJobs(t.Context(), nil, 10)
	if err != nil || len(failed) != 1 || failed[0].LastError != "node unreachable" || next != nil {
		t.Fatalf("failed jobs: %v %+v next=%v", err, failed, next)
	}
}

func TestFailedJobsCursorWalksAllFailures(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	// Seed three failed rows with a controlled order, as later
	// reconciliation would find them.
	for i := range 3 {
		if _, err := s.Pool().Exec(t.Context(), `
			INSERT INTO jobs (id, kind, owner_id, payload, state, attempts, max_attempts, updated_at)
			VALUES ($1, 'sandbox.provision', $2, '{}'::jsonb, 'failed', 1, 1,
			        now() - make_interval(secs => $3::float8))`,
			uuid.NewV7(), own, (3-i)*60); err != nil {
			t.Fatal(err)
		}
	}
	var got []uuid.UUID
	var after *db.FailedJobCursor
	for range 10 {
		page, next, err := s.FailedJobs(t.Context(), after, 1)
		if err != nil {
			t.Fatalf("failed jobs page: %v", err)
		}
		for _, j := range page {
			got = append(got, j.ID)
		}
		if next == nil {
			break
		}
		after = next
	}
	if len(got) != 3 {
		t.Fatalf("walked %d failed jobs, want 3", len(got))
	}
	seen := map[uuid.UUID]bool{}
	for _, id := range got {
		if seen[id] {
			t.Fatalf("duplicate failed job in walk: %s", id)
		}
		seen[id] = true
	}
}

func TestFailStaleJobsAndClaimAttemptsBound(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	enqueue(t, s, own, 1)

	j, err := s.ClaimJob(t.Context(), "w1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if j.Attempts != 1 || j.MaxAttempts != 1 {
		t.Fatalf("claim: %+v", j)
	}
	// Expire the only allowed attempt on the database clock.
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE jobs SET leased_until = now() - interval '1 second' WHERE id = $1`, j.ID); err != nil {
		t.Fatal(err)
	}
	// attempts == max: reclaim refuses; the sweep fails it terminally so
	// the crash cannot strand the job forever.
	if _, err := s.ClaimJob(t.Context(), "w2", time.Minute); !errors.Is(err, db.ErrNoJobs) {
		t.Fatalf("exhausted job reclaimed: %v", err)
	}
	n, err := s.FailStaleJobs(t.Context())
	if err != nil || n != 1 {
		t.Fatalf("fail stale: %d %v", n, err)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE state = 'failed'`); n != 1 {
		t.Fatalf("failed rows: %d", n)
	}
	// Reconciliation can find the failure after the sweep.
	failed, next, err := s.FailedJobs(t.Context(), nil, 10)
	if err != nil || len(failed) != 1 || failed[0].ID != j.ID || next != nil {
		t.Fatalf("reconcilable failures: %v %+v next=%v", err, failed, next)
	}
}

func TestNotificationLossCoveredByClaim(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	// A job written without NOTIFY (foreign tooling, lost packet) is
	// still claimable: claiming never depends on notifications.
	if _, err := s.Pool().Exec(t.Context(), `
		INSERT INTO jobs (id, kind, owner_id, payload)
		VALUES ($1, 'sandbox.provision', $2, '{}'::jsonb)`,
		uuid.NewV7(), own); err != nil {
		t.Fatal(err)
	}
	j, err := s.ClaimJob(t.Context(), "w1", time.Minute)
	if err != nil {
		t.Fatalf("claim unnotified job: %v", err)
	}
	if j.Kind != db.JobProvision {
		t.Fatalf("claimed kind: %s", j.Kind)
	}
}

func TestCancelSandboxJobs(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 65536, DiskGiB: 1000})
	own := owner(t, s)
	res := create(t, s, db.CreateSandboxParams{Owner: own})

	// A foreign owner cannot cancel this sandbox's work.
	if n, err := s.CancelSandboxJobs(t.Context(), owner(t, s), res.Sandbox.ID, db.JobProvision); err != nil || n != 0 {
		t.Fatalf("foreign cancel: %d %v", n, err)
	}
	n, err := s.CancelSandboxJobs(t.Context(), own, res.Sandbox.ID, db.JobProvision)
	if err != nil || n != 1 {
		t.Fatalf("cancel: %d %v", n, err)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE state = 'cancelled'`); n != 1 {
		t.Fatalf("cancelled rows: %d", n)
	}
	// Cancelled work is not claimable.
	if _, err := s.ClaimJob(t.Context(), "w1", time.Minute); !errors.Is(err, db.ErrNoJobs) {
		t.Fatalf("claim cancelled: %v", err)
	}
}

func TestJobListenerPollFallback(t *testing.T) {
	s := dbtestOpen(t)
	l, err := s.ListenJobs(t.Context(), 60*time.Millisecond)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	// No notification was sent: the poll fallback must still wake.
	wake := make(chan struct{})
	go func() {
		_ = l.Await(t.Context())
		close(wake)
	}()
	select {
	case <-wake:
	case <-time.After(2 * time.Second):
		t.Fatal("poll fallback did not wake within 2s")
	}
}

func TestJobListenerNotifiedFast(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	// Long poll: only a notification can explain a fast wake.
	l, err := s.ListenJobs(t.Context(), 10*time.Second)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	wake := make(chan struct{})
	go func() {
		_ = l.Await(t.Context())
		close(wake)
	}()
	time.Sleep(100 * time.Millisecond) // let LISTEN register
	enqueue(t, s, own, 0)
	select {
	case <-wake:
	case <-time.After(3 * time.Second):
		t.Fatal("notification did not wake the listener within 3s")
	}
}

func TestJobListenerCloseAndParentCancel(t *testing.T) {
	s := dbtestOpen(t)

	// Close joins the receiver goroutine promptly.
	l, err := s.ListenJobs(t.Context(), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { l.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return")
	}

	// Cancelling the parent context stops the listener by itself.
	ctx, cancel := context.WithCancel(t.Context())
	l2, err := s.ListenJobs(ctx, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	cancel()
	if err := l2.Await(t.Context()); err == nil {
		t.Fatal("Await after parent cancel returned nil")
	}
	// Close still joins and is idempotent.
	l2.Close()
	l2.Close()
}

// listenerPIDs lists only this store's LISTEN backends: same database, the
// store's unique application_name, and the exact LISTEN statement. It never
// matches another schema, database or test process.
func listenerPIDs(t *testing.T, s *db.Store) []int32 {
	t.Helper()
	rows, err := s.Pool().Query(t.Context(), `
		SELECT pid FROM pg_stat_activity
		WHERE datname = current_database()
		  AND application_name = current_setting('application_name')
		  AND query = 'LISTEN ' || $1
		  AND pid <> pg_backend_pid()
		ORDER BY pid`, db.JobsChannel)
	if err != nil {
		t.Fatalf("list listener backends: %v", err)
	}
	defer rows.Close()
	var pids []int32
	for rows.Next() {
		var pid int32
		if err := rows.Scan(&pid); err != nil {
			t.Fatalf("scan listener pid: %v", err)
		}
		pids = append(pids, pid)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("list listener backends: %v", err)
	}
	return pids
}

// waitForListenerPID waits for a LISTEN backend other than exclude, so a fast
// reconnect is observed by its new PID rather than by a transient empty gap.
func waitForListenerPID(t *testing.T, s *db.Store, exclude int32, what string) int32 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, pid := range listenerPIDs(t, s) {
			if pid != exclude {
				return pid
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// waitForListenerPIDGone fails if pid survives past the listener's Close.
func waitForListenerPIDGone(t *testing.T, s *db.Store, pid int32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		found := false
		for _, p := range listenerPIDs(t, s) {
			if p == pid {
				found = true
			}
		}
		if !found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("listener backend %d still present after Close", pid)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestJobListenerReconnectsAfterBackendLoss(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)

	l, err := s.ListenJobs(t.Context(), 60*time.Millisecond)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()

	original := waitForListenerPID(t, s, 0, "initial LISTEN")
	if _, err := s.Pool().Exec(t.Context(), `SELECT pg_terminate_backend($1)`, original); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	// Observe the replacement PID directly: a fast reconnect can skip any
	// count-zero gap in pg_stat_activity.
	replacement := waitForListenerPID(t, s, original, "listener to reconnect with a new backend")
	if replacement == original {
		t.Fatalf("listener reused backend %d after termination", original)
	}

	// The reconnected subscription still delivers wake-ups.
	wake := make(chan struct{})
	go func() {
		_ = l.Await(t.Context())
		close(wake)
	}()
	enqueue(t, s, own, 0)
	select {
	case <-wake:
	case <-time.After(3 * time.Second):
		t.Fatal("listener did not wake after backend loss and reconnect")
	}

	// Close must release the replacement connection. A deferred close
	// that captured the original connection leaks the replacement and
	// fails here.
	l.Close()
	waitForListenerPIDGone(t, s, replacement)
}
