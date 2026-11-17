package db_test

// Sandbox lifecycle SQL: atomic create with idempotency and reservation,
// optimistic transitions, owner isolation, cursor pagination, expiry, and
// the sealed-env guarantees.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

func TestCreateSandboxAtomic(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 100})
	own := owner(t, s)
	res := create(t, s, db.CreateSandboxParams{
		Owner: own, Name: "alpha", Labels: map[string]string{"env": "prod"},
		Env: map[string]string{"SECRET_TOKEN": "hunter2-value"},
	})

	if res.Sandbox.State != db.StateProvisioning {
		t.Fatalf("state: %s", res.Sandbox.State)
	}
	if res.Sandbox.Version != 1 {
		t.Fatalf("version: %d", res.Sandbox.Version)
	}
	if res.Sandbox.NodeID == nil || *res.Sandbox.NodeID != "node-a" {
		t.Fatalf("node: %v", res.Sandbox.NodeID)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 1 || got.MemoryMiB != 1024 || got.DiskGiB != 10 {
		t.Fatalf("reservation: %+v", got)
	}
	if res.Job.Kind != db.JobProvision || res.Job.State != db.JobQueued {
		t.Fatalf("job: %+v", res.Job)
	}
	if n := count(t, s, `SELECT count(*) FROM sandbox_events WHERE sandbox_id = $1`, res.Sandbox.ID); n != 1 {
		t.Fatalf("events: %d", n)
	}
	// Labels survive; env plaintext is nowhere in the table.
	var labelEnv string
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT labels->>'env' FROM sandboxes WHERE id = $1`, res.Sandbox.ID).Scan(&labelEnv); err != nil {
		t.Fatal(err)
	}
	if labelEnv != "prod" {
		t.Fatalf("labels: %q", labelEnv)
	}
}

func TestCreateSandboxEnvSealedAtRest(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 100})
	own := owner(t, s)
	const sentinel = "hunter2-value-c19f4a7e5b2d48f0"
	res := create(t, s, db.CreateSandboxParams{
		Owner: own, Env: map[string]string{"SECRET_TOKEN": sentinel, "A": "b"},
	})

	var sealed []byte
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT env_sealed FROM sandboxes WHERE id = $1`, res.Sandbox.ID).Scan(&sealed); err != nil {
		t.Fatal(err)
	}
	if len(sealed) < 12+16 {
		t.Fatalf("sealed env too short: %d bytes", len(sealed))
	}
	if strings.Contains(string(sealed), sentinel) {
		t.Fatal("plaintext env leaked into env_sealed")
	}

	// The single decrypt path returns exactly the input.
	env, err := s.DecryptSandboxEnvForNode(t.Context(), own, res.Sandbox.ID)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if env["SECRET_TOKEN"] != sentinel {
		t.Fatal("decrypted env: SECRET_TOKEN mismatch")
	}
	if env["A"] != "b" {
		t.Fatal("decrypted env: A mismatch")
	}
	if len(env) != 2 {
		t.Fatalf("decrypted env: %d keys, want 2", len(env))
	}

	// Empty env stores NULL and decrypts to an empty map.
	plain := create(t, s, db.CreateSandboxParams{Owner: own})
	var nullEnv *int32
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT length(env_sealed) FROM sandboxes WHERE id = $1`, plain.Sandbox.ID).Scan(&nullEnv); err != nil {
		t.Fatal(err)
	}
	if nullEnv != nil {
		t.Fatalf("empty env stored sealed: len %d", *nullEnv)
	}
	env, err = s.DecryptSandboxEnvForNode(t.Context(), own, plain.Sandbox.ID)
	if err != nil || len(env) != 0 {
		t.Fatalf("empty env decrypt: %d keys, err=%v", len(env), err)
	}
}

func TestCreateSandboxWrongMasterKeyFails(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 100})
	own := owner(t, s)
	res := create(t, s, db.CreateSandboxParams{Owner: own, Env: map[string]string{"K": "v"}})

	// A second store over the same schema with a foreign master key must
	// refuse to run: authenticated unwrap fails and nothing is replaced
	// or rewrapped.
	otherMaster, err := db.NewSecrets([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := openSameSchema(t, s, otherMaster)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.Migrate(t.Context()); !errors.Is(err, db.ErrMasterKeyMismatch) {
		t.Fatalf("migrate with wrong master: %v", err)
	}
	// The stored install key survived untouched; the right master still
	// decrypts.
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatalf("migrate with right master after mismatch: %v", err)
	}
	if env, err := s.DecryptSandboxEnvForNode(t.Context(), own, res.Sandbox.ID); err != nil {
		t.Fatalf("decrypt after mismatch episode: %d keys, err=%v", len(env), err)
	} else if env["K"] != "v" {
		t.Fatal("decrypt after mismatch episode: K mismatch")
	}

	// Tampered ciphertext fails closed, never falls back to plaintext.
	var sealed []byte
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT env_sealed FROM sandboxes WHERE id = $1`, res.Sandbox.ID).Scan(&sealed); err != nil {
		t.Fatal(err)
	}
	sealed[20] ^= 0xFF
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE sandboxes SET env_sealed = $2 WHERE id = $1`, res.Sandbox.ID, sealed); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DecryptSandboxEnvForNode(t.Context(), own, res.Sandbox.ID); err == nil {
		t.Fatal("tampered env decrypted, want failure")
	}
}

func TestConcurrentMigrateAndKeyBootstrap(t *testing.T) {
	// A brand-new schema: several stores with the SAME master migrate it
	// at once. Goose advisory locks serialize the schema work and the
	// install-key bootstrap resolves to a single durable winner; all
	// stores must end up able to seal and open the same env.
	schema, _ := freshSchema(t)
	master := make([]byte, 32)
	for i := range master {
		master[i] = byte(i)
	}
	secrets, err := db.NewSecrets(master)
	if err != nil {
		t.Fatal(err)
	}

	const racers = 4
	stores := make([]*db.Store, racers)
	errs := make([]error, racers)
	var wg sync.WaitGroup
	for i := range racers {
		wg.Go(func() {
			cfg := freshPoolConfig(t)
			cfg.ConnConfig.RuntimeParams["search_path"] = schema
			st, err := db.OpenPool(t.Context(), cfg, secrets)
			stores[i], errs[i] = st, err
			if err != nil {
				return
			}
			errs[i] = st.Migrate(t.Context())
		})
	}
	wg.Wait()
	for i := range racers {
		if stores[i] != nil {
			defer stores[i].Close()
		}
		if errs[i] != nil {
			t.Fatalf("concurrent migrate %d: %v", i, errs[i])
		}
	}
	if n := count(t, stores[0], `SELECT count(*) FROM install_keys`); n != 1 {
		t.Fatalf("install key rows after concurrent bootstrap: %d", n)
	}

	// One store seals env a sandbox owns; another opens it: identical
	// data key under the shared master.
	node(t, stores[0], "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	acct, err := stores[0].EnsureAccount(t.Context(), "concurrent-migrate")
	if err != nil {
		t.Fatal(err)
	}
	own := acct.ID
	res := create(t, stores[0], db.CreateSandboxParams{
		Owner: own, Env: map[string]string{"SHARED": "secret"},
	})
	env, err := stores[1].DecryptSandboxEnvForNode(t.Context(), own, res.Sandbox.ID)
	if err != nil {
		t.Fatalf("cross-store decrypt: %d keys, err=%v", len(env), err)
	}
	if env["SHARED"] != "secret" {
		t.Fatal("cross-store decrypt: SHARED mismatch")
	}
}

func TestCreateSandboxIdempotency(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 100})
	own := owner(t, s)
	hash := []byte("0123456789abcdef0123456789abcdef")

	first := create(t, s, db.CreateSandboxParams{
		Owner: own, Name: "idem", IdempotencyKey: "k1", RequestSHA256: hash,
	})
	replay := create(t, s, db.CreateSandboxParams{
		Owner: own, Name: "idem", IdempotencyKey: "k1", RequestSHA256: hash,
	})
	if !replay.Replayed || replay.Sandbox.ID != first.Sandbox.ID {
		t.Fatalf("replay: replayed=%v id=%s", replay.Replayed, replay.Sandbox.ID)
	}
	if n := count(t, s, `SELECT count(*) FROM sandboxes WHERE owner_id = $1`, own); n != 1 {
		t.Fatalf("sandboxes after replay: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.provision'`); n != 1 {
		t.Fatalf("provision jobs after replay: %d", n)
	}

	// Same key, different body: refused, nothing new.
	_, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: "node-a",
		Reserve:        db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10},
		IdempotencyKey: "k1",
		RequestSHA256:  []byte("ffffffffffffffffffffffffffffffff"),
	})
	if !errors.Is(err, db.ErrIdempotencyReused) {
		t.Fatalf("reuse: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM sandboxes WHERE owner_id = $1`, own); n != 1 {
		t.Fatalf("sandboxes after reuse: %d", n)
	}

	// Keys are owner-scoped: another account may use the same key and
	// gets its own sandbox.
	other := owner(t, s)
	otherRes := create(t, s, db.CreateSandboxParams{
		Owner: other, IdempotencyKey: "k1", RequestSHA256: hash,
	})
	if otherRes.Replayed || otherRes.Sandbox.ID == first.Sandbox.ID {
		t.Fatal("another owner replayed a foreign idempotency key")
	}
}

func TestCreateSandboxIdempotencyConcurrent(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	own := owner(t, s)
	hash := []byte("0123456789abcdef0123456789abcdef")

	const racers = 4
	results := make([]db.CreateSandboxResult, racers)
	errs := make([]error, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range racers {
		wg.Go(func() {
			<-start
			results[i], errs[i] = s.CreateSandbox(t.Context(), db.CreateSandboxParams{
				ID: ids.Sandbox(), Owner: own, NodeID: "node-a",
				Reserve:        db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10},
				IdempotencyKey: "race", RequestSHA256: hash,
			})
		})
	}
	close(start)
	wg.Wait()

	var winners, replays, inProgress, failures int
	for i := range racers {
		switch {
		case errs[i] == nil && !results[i].Replayed:
			winners++
		case errs[i] == nil && results[i].Replayed:
			replays++
		case errors.Is(errs[i], db.ErrIdempotencyInProgress):
			inProgress++
		default:
			failures++
			t.Errorf("racer %d: %v", i, errs[i])
		}
	}
	if failures != 0 {
		t.Fatalf("%d racers failed unexpectedly", failures)
	}
	if winners != 1 {
		t.Fatalf("exactly one winner expected, got %d (replays=%d inProgress=%d)",
			winners, replays, inProgress)
	}
	if winners+replays+inProgress != racers {
		t.Fatalf("outcome accounting: %d+%d+%d != %d", winners, replays, inProgress, racers)
	}
	if n := count(t, s, `SELECT count(*) FROM sandboxes WHERE owner_id = $1`, own); n != 1 {
		t.Fatalf("sandboxes after race: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.provision'`); n != 1 {
		t.Fatalf("provision jobs after race: %d", n)
	}
}

// createLockWaiters counts this test store's own backends currently blocked
// on a PostgreSQL lock: same database, unique application_name and role.
func createLockWaiters(t *testing.T, s *db.Store) int64 {
	t.Helper()
	var n int64
	if err := s.Pool().QueryRow(t.Context(), `
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database()
		  AND application_name = current_setting('application_name')
		  AND usename = current_user
		  AND wait_event_type = 'Lock'
		  AND pid <> pg_backend_pid()`).Scan(&n); err != nil {
		t.Fatalf("count lock waiters: %v", err)
	}
	return n
}

// waitForCreateLockWaiters blocks until want create transactions are waiting
// on a lock. The wait is bounded and condition-based, never a fixed sleep.
func waitForCreateLockWaiters(t *testing.T, s *db.Store, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got := createLockWaiters(t, s); got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("create transactions did not both reach lock waits (want %d, saw %d)",
				want, createLockWaiters(t, s))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCreateSandboxConcurrentSameKeyLastSlot is the deterministic ordering
// regression: two same-key creates race for the only free slot. Both must end
// in one VM/job and either a replay or idempotency_in_progress — never
// no_capacity, which would mean the loser checked capacity before the replay.
//
// The test holds the node row lock so both creates are forced past their
// replay decision and leave the outside-transaction replay path before either
// can reserve: the old code waits twice on the node row and then fails the
// loser with no_capacity; the fixed code waits once on the node row and once
// on the per-key advisory lock, and the loser replays.
func TestCreateSandboxConcurrentSameKeyLastSlot(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10})
	own := owner(t, s)
	hash := []byte("0123456789abcdef0123456789abcdef")
	shape := db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10}

	// Externally hold the node row before either create starts.
	holdCtx, cancelHold := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelHold()
	hold, err := s.Pool().Begin(holdCtx)
	if err != nil {
		t.Fatalf("hold node lock: %v", err)
	}
	defer func() { _ = hold.Rollback(context.WithoutCancel(t.Context())) }()
	if _, err := hold.Exec(holdCtx,
		`SELECT 1 FROM nodes WHERE id = 'node-a' FOR UPDATE`); err != nil {
		t.Fatalf("hold node lock row: %v", err)
	}

	const racers = 2
	results := make([]db.CreateSandboxResult, racers)
	errs := make([]error, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range racers {
		wg.Go(func() {
			<-start
			results[i], errs[i] = s.CreateSandbox(t.Context(), db.CreateSandboxParams{
				ID: ids.Sandbox(), Owner: own, NodeID: "node-a", Reserve: shape,
				IdempotencyKey: "last-slot", RequestSHA256: hash,
			})
		})
	}
	close(start)

	// Both replay lookups have missed by now, and both creates are
	// blocked: one (fixed) on the advisory lock, both (old) on the node.
	waitForCreateLockWaiters(t, s, int64(racers))
	if err := hold.Rollback(context.WithoutCancel(t.Context())); err != nil {
		t.Fatalf("release node lock: %v", err)
	}
	wg.Wait()

	var winners, replays, inProgress int
	for i := range racers {
		switch {
		case errs[i] == nil && !results[i].Replayed:
			winners++
		case errs[i] == nil && results[i].Replayed:
			replays++
		case errors.Is(errs[i], db.ErrIdempotencyInProgress):
			inProgress++
		case errors.Is(errs[i], db.ErrNoCapacity):
			t.Fatalf("racer %d got no_capacity instead of replay/in_progress", i)
		default:
			t.Fatalf("racer %d: %v", i, errs[i])
		}
	}
	if winners != 1 || replays+inProgress != racers-1 {
		t.Fatalf("outcomes: winners=%d replays=%d inProgress=%d", winners, replays, inProgress)
	}
	if n := count(t, s, `SELECT count(*) FROM sandboxes WHERE owner_id = $1`, own); n != 1 {
		t.Fatalf("sandboxes after last-slot race: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE owner_id = $1 AND kind = 'sandbox.provision'`, own); n != 1 {
		t.Fatalf("provision jobs after last-slot race: %d", n)
	}
	// A body mismatch still reuses the key even though the node is full.
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: "node-a", Reserve: shape,
		IdempotencyKey: "last-slot",
		RequestSHA256:  []byte("ffffffffffffffffffffffffffffffff"),
	}); !errors.Is(err, db.ErrIdempotencyReused) {
		t.Fatalf("different body on full node: %v", err)
	}
}

func TestCreateSandboxExpiredKeyReused(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 100})
	own := owner(t, s)
	hash := []byte("0123456789abcdef0123456789abcdef")

	first := create(t, s, db.CreateSandboxParams{
		Owner: own, IdempotencyKey: "old", RequestSHA256: hash,
	})
	// Age the key past its 24h retention.
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE idempotency_keys SET expires_at = now() - interval '1 hour' WHERE key = 'old'`); err != nil {
		t.Fatal(err)
	}
	again := create(t, s, db.CreateSandboxParams{
		Owner: own, IdempotencyKey: "old", RequestSHA256: hash,
	})
	if again.Replayed || again.Sandbox.ID == first.Sandbox.ID {
		t.Fatalf("expired key replayed: %+v", again.Sandbox)
	}
	if n := count(t, s, `SELECT count(*) FROM sandboxes WHERE owner_id = $1`, own); n != 2 {
		t.Fatalf("sandboxes after expired reuse: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM idempotency_keys WHERE key = 'old'`); n != 1 {
		t.Fatalf("idempotency rows for 'old': %d", n)
	}
}

func TestCreateSandboxRollbackLeavesNothing(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10})
	own := owner(t, s)

	// Unknown node: the whole transaction rolls back.
	_, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: "node-missing",
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10},
	})
	if !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("unknown node: %v", err)
	}
	assertOwnerClean(t, s, own)

	// Full node: same rollback story.
	createOk := create(t, s, db.CreateSandboxParams{Owner: own})
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: "node-a",
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10},
	}); !errors.Is(err, db.ErrNoCapacity) {
		t.Fatalf("full node: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM sandboxes WHERE owner_id = $1`, own); n != 1 {
		t.Fatalf("sandboxes after no-capacity: %d", n)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 1 {
		t.Fatalf("reserved after no-capacity: %+v", got)
	}
	_ = createOk
}

func assertOwnerClean(t *testing.T, s *db.Store, own uuid.UUID) {
	t.Helper()
	if n := count(t, s, `SELECT count(*) FROM sandboxes WHERE owner_id = $1`, own); n != 0 {
		t.Fatalf("sandboxes left behind: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE owner_id = $1`, own); n != 0 {
		t.Fatalf("jobs left behind: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM sandbox_events WHERE owner_id = $1`, own); n != 0 {
		t.Fatalf("events left behind: %d", n)
	}
}

func TestSandboxOwnerIsolation(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	a, b := owner(t, s), owner(t, s)
	res := create(t, s, db.CreateSandboxParams{Owner: a, Env: map[string]string{"K": "v"}})

	if _, err := s.Sandbox(t.Context(), b, res.Sandbox.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign get: %v", err)
	}
	if _, err := s.DecryptSandboxEnvForNode(t.Context(), b, res.Sandbox.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign decrypt: %v", err)
	}
	if _, err := s.SandboxEvents(t.Context(), b, res.Sandbox.ID, 10); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign events: %v", err)
	}
	if _, _, err := s.AcceptSandboxDeletion(t.Context(), b, res.Sandbox.ID, db.DeletionExplicit); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign delete: %v", err)
	}
	got, _, err := s.ListSandboxes(t.Context(), db.SandboxListQuery{Owner: b})
	if err != nil || len(got) != 0 {
		t.Fatalf("foreign list: %v %d", err, len(got))
	}
	if _, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: b, FromStates: []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: 1, To: db.StateReady,
	}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign transition: %v", err)
	}
	if sb, err := s.Sandbox(t.Context(), a, res.Sandbox.ID); err != nil || sb.ID != res.Sandbox.ID {
		t.Fatalf("own get: %v", err)
	}
}

func TestTransitionSandbox(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	own := owner(t, s)
	res := create(t, s, db.CreateSandboxParams{Owner: own})

	// Zero owner is rejected outright (no internal bypass).
	if _, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: res.Sandbox.ID, FromStates: []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: 1, To: db.StateReady,
	}); !errors.Is(err, db.ErrNoOwner) {
		t.Fatalf("zero owner: %v", err)
	}

	// Happy path with follow-up job; version and event recorded.
	sb, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: 1, To: db.StateReady,
		Job: &db.JobInput{Kind: db.JobProvision, OwnerID: own},
	})
	if err != nil {
		t.Fatalf("transition: %v", err)
	}
	if sb.State != db.StateReady || sb.Version != 2 {
		t.Fatalf("after transition: %s v%d", sb.State, sb.Version)
	}
	if n := count(t, s, `SELECT count(*) FROM sandbox_events WHERE sandbox_id = $1 AND type = 'state.ready'`, res.Sandbox.ID); n != 1 {
		t.Fatalf("state.ready events: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE sandbox_id = $1`, res.Sandbox.ID); n != 2 {
		t.Fatalf("jobs after transition: %d", n)
	}
	// A nil follow-up sandbox defaults to the parent, so both jobs are
	// linked to this sandbox.
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.provision'`, res.Sandbox.ID); n != 2 {
		t.Fatalf("linked provision jobs after transition: %d", n)
	}

	// Stale version conflicts.
	if _, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: 1, To: db.StateError, ErrorReason: "stale",
	}); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("stale version: %v", err)
	}
	// Wrong from-state conflicts.
	if _, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: 2, To: db.StateError,
	}); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("wrong from-state: %v", err)
	}

	// Follow-up job must belong to the same owner and sandbox.
	if _, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateReady},
		ExpectedVersion: 2, To: db.StateIdle,
		Job: &db.JobInput{Kind: db.JobProvision, OwnerID: owner(t, s)},
	}); err == nil {
		t.Fatal("cross-owner follow-up job accepted")
	}
	otherRes := create(t, s, db.CreateSandboxParams{Owner: own})
	if _, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateReady},
		ExpectedVersion: 2, To: db.StateIdle,
		Job: &db.JobInput{Kind: db.JobProvision, OwnerID: own, SandboxID: &otherRes.Sandbox.ID},
	}); err == nil {
		t.Fatal("cross-sandbox follow-up job accepted")
	}

	// Deletion guards any further transition.
	if _, _, err := s.AcceptSandboxDeletion(t.Context(), own, res.Sandbox.ID, db.DeletionExplicit); err != nil {
		t.Fatalf("accept deletion: %v", err)
	}
	if _, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateIdle},
		ExpectedVersion: 2, To: db.StateReady,
	}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("transition after delete: %v", err)
	}
}

func TestListSandboxesPaginationAndFilters(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 65536, DiskGiB: 1000})
	own := owner(t, s)

	created := make([]db.Sandbox, 12)
	for i := range created {
		name := "sb-"
		switch i {
		case 0:
			name = "lit%eral"
		case 1:
			name = "litGeneral"
		}
		res := create(t, s, db.CreateSandboxParams{
			Owner: own, Name: name,
			Labels: map[string]string{"team": "core", "n": string(rune('a' + i))},
		})
		created[i] = res.Sandbox
	}
	// Force one shared microsecond created_at: the id tie-break alone must
	// keep the walk complete and the asc/desc reversal exact.
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes
		SET created_at = (SELECT min(created_at) FROM sandboxes WHERE owner_id = $1)
		WHERE owner_id = $1`, own); err != nil {
		t.Fatal(err)
	}

	walk := func(asc bool) []string {
		var ids []string
		var after *db.PageCursor
		for range 10 {
			page, next, err := s.ListSandboxes(t.Context(), db.SandboxListQuery{
				Owner: own, Limit: 5, Ascending: asc, After: after,
			})
			if err != nil {
				t.Fatalf("list page: %v", err)
			}
			for _, sb := range page {
				ids = append(ids, sb.ID)
			}
			if next == nil {
				return ids
			}
			after = next
		}
		t.Fatal("pagination did not terminate")
		return nil
	}

	desc := walk(false)
	if len(desc) != 12 {
		t.Fatalf("desc walk: %d", len(desc))
	}
	seen := map[string]bool{}
	for _, id := range desc {
		if seen[id] {
			t.Fatalf("duplicate id in walk: %s", id)
		}
		seen[id] = true
	}
	asc := walk(true)
	for i := range asc {
		if asc[i] != desc[len(desc)-1-i] {
			t.Fatalf("asc/desc disagree at %d", i)
		}
	}

	// Name filter: explicit empty name matches nothing here; exact name
	// matches one.
	empty := ""
	got, _, err := s.ListSandboxes(t.Context(), db.SandboxListQuery{Owner: own, Name: &empty})
	if err != nil || len(got) != 0 {
		t.Fatalf("empty name filter: %v %d", err, len(got))
	}
	got, _, err = s.ListSandboxes(t.Context(), db.SandboxListQuery{Owner: own, Name: ptr("lit%eral")})
	if err != nil || len(got) != 1 {
		t.Fatalf("exact name filter: %v %d", err, len(got))
	}
	// NamePrefix is a literal match: "lit%" does not match "litGeneral".
	got, _, err = s.ListSandboxes(t.Context(), db.SandboxListQuery{Owner: own, NamePrefix: "lit%"})
	if err != nil || len(got) != 1 {
		t.Fatalf("literal prefix filter: %v %d", err, len(got))
	}
	got, _, err = s.ListSandboxes(t.Context(), db.SandboxListQuery{Owner: own, NamePrefix: "lit"})
	if err != nil || len(got) != 2 {
		t.Fatalf("plain prefix filter: %v %d", err, len(got))
	}
	// Labels: single and compound subset matches; deleted rows excluded.
	got, _, err = s.ListSandboxes(t.Context(), db.SandboxListQuery{Owner: own, Labels: map[string]string{"team": "core"}})
	if err != nil || len(got) != 12 {
		t.Fatalf("label filter: %v %d", err, len(got))
	}
	got, _, err = s.ListSandboxes(t.Context(), db.SandboxListQuery{
		Owner: own, Labels: map[string]string{"team": "core", "n": "c"}, States: []db.SandboxState{db.StateProvisioning},
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("compound filter: %v %d", err, len(got))
	}

	if _, _, err := s.AcceptSandboxDeletion(t.Context(), own, created[0].ID, db.DeletionExplicit); err != nil {
		t.Fatal(err)
	}
	got, _, err = s.ListSandboxes(t.Context(), db.SandboxListQuery{Owner: own})
	if err != nil || len(got) != 11 {
		t.Fatalf("after delete list: %v %d", err, len(got))
	}
}

func TestSandboxesExpiring(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 65536, DiskGiB: 1000})
	own := owner(t, s)

	past := create(t, s, db.CreateSandboxParams{Owner: own, ExpiresAt: ptrTime(time.Now().Add(-time.Minute))})
	future := create(t, s, db.CreateSandboxParams{Owner: own, ExpiresAt: ptrTime(time.Now().Add(time.Hour))})
	none := create(t, s, db.CreateSandboxParams{Owner: own})

	due, err := s.SandboxesExpiring(t.Context(), time.Now(), 10)
	if err != nil {
		t.Fatalf("expiring: %v", err)
	}
	if len(due) != 1 || due[0].ID != past.Sandbox.ID {
		t.Fatalf("due: %+v", due)
	}

	// Terminal state stops expiry; deletion stops expiry.
	if _, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: past.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: 1, To: db.StateError, ErrorReason: "boot failed",
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AcceptSandboxDeletion(t.Context(), own, future.Sandbox.ID, db.DeletionExplicit); err != nil {
		t.Fatal(err)
	}
	due, err = s.SandboxesExpiring(t.Context(), time.Now(), 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("due after terminal/delete: %v %+v", err, due)
	}
	_ = none
}

func ptr[T any](v T) *T { return &v }

func ptrTime(t time.Time) *time.Time { return &t }
