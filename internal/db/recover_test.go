package db_test

// Real-Postgres tests for sandbox recovery persistence: AcceptRecover
// (transaction atomicity, cooldown boundary, concurrency, owner scoping, state
// checks) and RecordRecoverEvent (lease fencing). Gated by
// BBRAFT_TEST_DATABASE_URL through dbtestOpen.

import (
	"encoding/json/v2"
	"errors"
	"sync"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

func TestAcceptRecoverRejectsZeroTime(t *testing.T) {
	var s *db.Store
	if _, err := s.AcceptRecover(t.Context(), uuid.NewV7(), ids.Sandbox(), time.Time{}); err == nil {
		t.Fatal("AcceptRecover accepted a zero timestamp")
	}
}

func TestAcceptRecoverTransactionAndCooldown(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	other := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})

	// Provisioning sandbox is not eligible.
	prov := create(t, s, db.CreateSandboxParams{Owner: own})
	t0 := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	if _, err := s.AcceptRecover(t.Context(), own, prov.Sandbox.ID, t0); !errors.Is(err, db.ErrInvalidState) {
		t.Fatalf("provisioning recover: %v, want ErrInvalidState", err)
	}

	// Ready sandbox succeeds and writes last_recover_at, event and job in one tx.
	sbx := phase1Ready(t, s, own, prov.Sandbox)
	accepted, err := s.AcceptRecover(t.Context(), own, sbx.ID, t0)
	if err != nil {
		t.Fatalf("AcceptRecover: %v", err)
	}
	if accepted.LastRecoverAt == nil || !accepted.LastRecoverAt.Equal(t0) {
		t.Fatalf("LastRecoverAt = %v, want %v", accepted.LastRecoverAt, t0)
	}
	if n := count(t, s, `SELECT count(*) FROM sandbox_events WHERE sandbox_id = $1 AND type = 'sandbox.recovering'`, sbx.ID); n != 1 {
		t.Fatalf("recovering events = %d, want 1", n)
	}
	job, err := s.LatestSandboxJob(t.Context(), own, sbx.ID, db.JobRecover)
	if err != nil {
		t.Fatalf("LatestSandboxJob: %v", err)
	}
	var payload db.RecoverPayload
	if err := json.Unmarshal([]byte(job.Payload), &payload); err != nil {
		t.Fatalf("unmarshal recover payload: %v", err)
	}
	if payload.NodeID != "node-a" || payload.Generation != sbx.LifecycleGeneration {
		t.Fatalf("payload = %+v, want node-a gen %d", payload, sbx.LifecycleGeneration)
	}

	// Cooldown: 9m59s is refused with ErrTooSoon and writes nothing.
	if _, err := s.AcceptRecover(t.Context(), own, sbx.ID, t0.Add(9*time.Minute+59*time.Second)); !errors.Is(err, db.ErrTooSoon) {
		t.Fatalf("9m59s recover: %v, want ErrTooSoon", err)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.recover'`, sbx.ID); n != 1 {
		t.Fatalf("jobs after too_soon = %d, want 1", n)
	}

	// Cooldown boundary: 10m00s is accepted.
	t10 := t0.Add(10 * time.Minute)
	accepted2, err := s.AcceptRecover(t.Context(), own, sbx.ID, t10)
	if err != nil {
		t.Fatalf("10m00s recover: %v", err)
	}
	if accepted2.LastRecoverAt == nil || !accepted2.LastRecoverAt.Equal(t10) {
		t.Fatalf("LastRecoverAt = %v, want %v", accepted2.LastRecoverAt, t10)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.recover'`, sbx.ID); n != 2 {
		t.Fatalf("jobs after 10m = %d, want 2", n)
	}

	// Owner scoping and missing sandbox.
	if _, err := s.AcceptRecover(t.Context(), other, sbx.ID, t10.Add(time.Hour)); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign recover: %v, want ErrNotFound", err)
	}
	if _, err := s.AcceptRecover(t.Context(), own, ids.Sandbox(), t10.Add(time.Hour)); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("unknown recover: %v, want ErrNotFound", err)
	}

	// Archived sandbox is refused with ErrInvalidState.
	stopped, _ := phase1Stopped(t, s, own, accepted2)
	if _, err := s.AcceptRecover(t.Context(), own, stopped.ID, t10.Add(time.Hour)); !errors.Is(err, db.ErrInvalidState) {
		t.Fatalf("archived recover: %v, want ErrInvalidState", err)
	}

	// Deleted sandbox is ErrNotFound.
	if _, _, err := s.AcceptSandboxDeletion(t.Context(), own, stopped.ID, db.DeletionExplicit); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcceptRecover(t.Context(), own, stopped.ID, t10.Add(time.Hour)); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("deleted recover: %v, want ErrNotFound", err)
	}
}

func TestAcceptRecoverConcurrent(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		wins    int
		tooSoon int
		others  []error
	)
	start := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			<-start
			_, err := s.AcceptRecover(t.Context(), own, sbx.ID, now)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins++
			case errors.Is(err, db.ErrTooSoon):
				tooSoon++
			default:
				others = append(others, err)
			}
		})
	}
	close(start)
	wg.Wait()

	if len(others) != 0 {
		t.Fatalf("unexpected errors: %v", others)
	}
	if wins != 1 || tooSoon != 3 {
		t.Fatalf("wins=%d tooSoon=%d, want 1 and 3", wins, tooSoon)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.recover'`, sbx.ID); n != 1 {
		t.Fatalf("recover jobs = %d, want 1", n)
	}
	if n := count(t, s, `SELECT count(*) FROM sandbox_events WHERE sandbox_id = $1 AND type = 'sandbox.recovering'`, sbx.ID); n != 1 {
		t.Fatalf("recovering events = %d, want 1", n)
	}
}

func TestRecordRecoverEventLeaseFencing(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	other := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	phase1BootDone(t, s, "boot-worker")

	if _, err := s.AcceptRecover(t.Context(), own, sbx.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	job := phase1ClaimKind(t, s, "recover-worker", db.JobRecover)

	// Wrong token or wrong worker is fenced off.
	if err := s.RecordRecoverEvent(t.Context(), job.ID, "recover-worker", uuid.NewV7(), own, sbx.ID, db.EventInput{Type: "sandbox.recovered"}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("wrong token: %v, want ErrJobNotLeased", err)
	}
	if err := s.RecordRecoverEvent(t.Context(), job.ID, "other-worker", job.LeaseToken, own, sbx.ID, db.EventInput{Type: "sandbox.recovered"}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("wrong worker: %v, want ErrJobNotLeased", err)
	}
	if err := s.RecordRecoverEvent(t.Context(), job.ID, "recover-worker", job.LeaseToken, other, sbx.ID, db.EventInput{Type: "sandbox.recovered"}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("foreign owner: %v, want ErrJobNotLeased", err)
	}

	// Live claim writes the event.
	if err := s.RecordRecoverEvent(t.Context(), job.ID, "recover-worker", job.LeaseToken, own, sbx.ID, db.EventInput{Type: "sandbox.recovered"}); err != nil {
		t.Fatalf("RecordRecoverEvent: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM sandbox_events WHERE sandbox_id = $1 AND type = 'sandbox.recovered'`, sbx.ID); n != 1 {
		t.Fatalf("recovered events = %d, want 1", n)
	}

	// Expired lease writes nothing.
	if _, err := s.Pool().Exec(t.Context(), `UPDATE jobs SET leased_until = now() - interval '1 second' WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordRecoverEvent(t.Context(), job.ID, "recover-worker", job.LeaseToken, own, sbx.ID, db.EventInput{Type: "sandbox.recovered"}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("expired lease: %v, want ErrJobNotLeased", err)
	}
	if n := count(t, s, `SELECT count(*) FROM sandbox_events WHERE sandbox_id = $1 AND type = 'sandbox.recovered'`, sbx.ID); n != 1 {
		t.Fatalf("recovered events after expired lease = %d, want 1", n)
	}
}
