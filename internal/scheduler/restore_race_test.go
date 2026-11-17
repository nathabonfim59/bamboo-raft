package scheduler_test

// Create-from race regression with the real PostgreSQL store and its
// idempotency keys. A deterministic resolver barrier pauses the loser after
// its first keyed replay missed, lets a same-owner/key/hash create commit and
// the name be deleted, then releases the resolver: the loser must replay the
// winner, never answer 404 or no-capacity. Requires BBRAFT_TEST_DATABASE_URL
// (skips without it; BBRAFT_TEST_REQUIRE_ALL=1 still fails).

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// snapshotFlowBarrierResolver pauses source resolution until release, then
// reads the live name so a deletion/replacement during the pause is observed.
type snapshotFlowBarrierResolver struct {
	store   *db.Store
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int64
}

func (r *snapshotFlowBarrierResolver) ResolveRestore(ctx context.Context, owner uuid.UUID,
	name string, typeExplicit bool, requestedType string,
) (db.SnapshotSource, string, db.Resources, error) {
	r.calls.Add(1)
	select {
	case r.entered <- struct{}{}:
	default:
	}
	select {
	case <-r.release:
	case <-ctx.Done():
		return db.SnapshotSource{}, "", db.Resources{}, ctx.Err()
	}
	target, apiErr := api.ResolveSnapshotRestore(ctx, r.store, owner, name, typeExplicit, requestedType)
	if apiErr != nil {
		return db.SnapshotSource{}, "", db.Resources{}, apiErr
	}
	return target.Source, target.MachineType, target.Resources, nil
}

// snapshotFlowCancelResolver blocks until its context ends and reports the
// caller's cancellation unchanged.
type snapshotFlowCancelResolver struct {
	entered chan struct{}
	calls   atomic.Int64
}

func (r *snapshotFlowCancelResolver) ResolveRestore(ctx context.Context, _ uuid.UUID,
	_ string, _ bool, _ string,
) (db.SnapshotSource, string, db.Resources, error) {
	r.calls.Add(1)
	select {
	case r.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return db.SnapshotSource{}, "", db.Resources{}, ctx.Err()
}

// snapshotFlowWaitingResolver records the admission deadline and waits for it,
// so a test can prove the whole failFast budget covers resolution.
type snapshotFlowWaitingResolver struct {
	entered  chan struct{}
	deadline atomic.Value // time.Time
	calls    atomic.Int64
}

func (r *snapshotFlowWaitingResolver) ResolveRestore(ctx context.Context, _ uuid.UUID,
	_ string, _ bool, _ string,
) (db.SnapshotSource, string, db.Resources, error) {
	r.calls.Add(1)
	deadline, _ := ctx.Deadline()
	r.deadline.Store(deadline)
	select {
	case r.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		return db.SnapshotSource{}, "", db.Resources{}, errors.New("resolver context never ended")
	}
	return db.SnapshotSource{}, "", db.Resources{}, ctx.Err()
}

// snapshotFlowStoreResolver adapts the production create-from policy exactly
// as the server does: api.ResolveSnapshotRestore owns type/disk semantics.
type snapshotFlowStoreResolver struct{ store *db.Store }

func (r snapshotFlowStoreResolver) ResolveRestore(ctx context.Context, owner uuid.UUID,
	name string, typeExplicit bool, requestedType string,
) (db.SnapshotSource, string, db.Resources, error) {
	target, apiErr := api.ResolveSnapshotRestore(ctx, r.store, owner, name, typeExplicit, requestedType)
	if apiErr != nil {
		return db.SnapshotSource{}, "", db.Resources{}, apiErr
	}
	return target.Source, target.MachineType, target.Resources, nil
}

// snapshotFlowForbiddenResolver proves a reused key never reaches source
// resolution.
type snapshotFlowForbiddenResolver struct{ calls atomic.Int64 }

func (r *snapshotFlowForbiddenResolver) ResolveRestore(context.Context, uuid.UUID,
	string, bool, string,
) (db.SnapshotSource, string, db.Resources, error) {
	r.calls.Add(1)
	return db.SnapshotSource{}, "", db.Resources{}, errors.New("resolver must not run for a reused key")
}

// snapshotFlowSavedSource inserts one ready generation with a caller-chosen
// artifact id, so two owners can keep independent names.
func snapshotFlowSavedSource(t *testing.T, store *db.Store, owner uuid.UUID,
	source db.Sandbox, nodeID, artifact string,
) db.SnapshotSource {
	t.Helper()
	if _, err := store.Pool().Exec(t.Context(), `
		INSERT INTO saved_snapshots
			(id, owner_id, name, sandbox_id, status, artifact_id, generation,
			 machine_type, disk_gib, node_id, is_current, created_at, updated_at, ready_at)
		VALUES ($1, $2, 'base', $3, 'ready', $4, 1, 'default', 51, $5, true, now(), now(), now())`,
		uuid.NewV7(), owner, source.ID, artifact, nodeID); err != nil {
		t.Fatalf("insert saved source: %v", err)
	}
	resolved, err := store.SnapshotRestoreSource(t.Context(), owner, "base")
	if err != nil {
		t.Fatalf("resolve saved source: %v", err)
	}
	return resolved
}

func snapshotFlowRestoreReserve() db.Resources {
	return db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 51}
}

// TestSnapshotFlowRestoreRaceLoserReplaysWinner pauses a create-from resolver,
// lets another same-owner/key/hash create commit and delete the name, then
// releases the resolver: the loser must replay the winner.
func TestSnapshotFlowRestoreRaceLoserReplaysWinner(t *testing.T) {
	env := newSchedEnv(t)
	env.registerNode(t, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 256 << 10, DiskGiB: 1000})
	owner := env.owner(t)
	source := schedReadySource(t, env.store, owner, "node-a")
	src := schedSavedSource(t, env.store, owner, source, "node-a")

	key := "restore-race"
	hash := sha256.Sum256([]byte(`{"from":"base"}`))
	resolver := &snapshotFlowBarrierResolver{
		store: env.store, entered: make(chan struct{}, 1), release: make(chan struct{}),
	}
	admit := env.scheduler(scheduler.Options{Restore: resolver})
	req := schedAdmitRequest(ids.Sandbox(), owner)
	req.From = "base"
	req.IdempotencyKey = key
	req.RequestSHA256 = hash[:]

	type outcome struct {
		result db.CreateSandboxResult
		err    error
	}
	loser := make(chan outcome, 1)
	finished := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resolver.release) }) }
	go func() {
		defer close(finished)
		result, err := admit.Admit(t.Context(), req)
		loser <- outcome{result, err}
	}()
	t.Cleanup(func() { release(); <-finished })

	select {
	case <-resolver.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("loser never reached source resolution")
	}

	// A duplicate restore commits with the same owner/key/hash from the
	// pre-resolved immutable source, then the name is deleted before the
	// loser's resolver returns.
	winnerID := ids.Sandbox()
	winner, err := env.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: winnerID, Owner: owner, MachineType: "default", Name: "winner",
		NodeID: "node-a", Reserve: snapshotFlowRestoreReserve(),
		Restore: &src, IdempotencyKey: key, RequestSHA256: hash[:],
	})
	if err != nil {
		t.Fatalf("winner restore create: %v", err)
	}
	if _, err := env.store.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: owner, Name: "base",
	}); err != nil {
		t.Fatalf("delete name during resolution: %v", err)
	}
	release()

	got := <-loser
	if got.err != nil {
		t.Fatalf("loser returned %v, want the replayed winner", got.err)
	}
	if !got.result.Replayed || got.result.Sandbox.ID != winner.Sandbox.ID {
		t.Fatalf("loser result replayed=%v id=%q, want %q",
			got.result.Replayed, got.result.Sandbox.ID, winner.Sandbox.ID)
	}
	if got.result.Sandbox.FromSnapshotID != src.ArtifactID || !got.result.Sandbox.SnapshotPin {
		t.Fatalf("replayed winner from=%q pin=%v, want the immutable artifact and held pin",
			got.result.Sandbox.FromSnapshotID, got.result.Sandbox.SnapshotPin)
	}
	if resolver.calls.Load() != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolver.calls.Load())
	}
	var pins int
	if err := env.store.Pool().QueryRow(t.Context(), `
		SELECT pins FROM saved_snapshots WHERE id = $1`, src.RowID).Scan(&pins); err != nil {
		t.Fatalf("read replayed winner pin: %v", err)
	}
	if pins != 1 {
		t.Fatalf("replayed winner pins = %d, want 1", pins)
	}
	var created int
	if err := env.store.Pool().QueryRow(t.Context(), `
		SELECT count(*) FROM sandboxes WHERE id = $1`, req.ID).Scan(&created); err != nil {
		t.Fatalf("count loser sandbox: %v", err)
	}
	if created != 0 {
		t.Fatalf("loser created %d sandboxes after replaying the winner", created)
	}
}

// TestSnapshotFlowRestoreRaceChangedHash proves a changed request hash under
// the same key is a reuse conflict, never a second sandbox.
func TestSnapshotFlowRestoreRaceChangedHash(t *testing.T) {
	env := newSchedEnv(t)
	env.registerNode(t, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 256 << 10, DiskGiB: 1000})
	owner := env.owner(t)
	key := "changed-hash"
	firstHash := sha256.Sum256([]byte(`{"type":"default"}`))
	secondHash := sha256.Sum256([]byte(`{"type":"large"}`))
	if _, err := env.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: owner, MachineType: "default", Name: "winner",
		NodeID: "node-a", Reserve: snapshotFlowRestoreReserve(),
		IdempotencyKey: key, RequestSHA256: firstHash[:],
	}); err != nil {
		t.Fatalf("winner create: %v", err)
	}

	resolver := &snapshotFlowForbiddenResolver{}
	req := schedAdmitRequest(ids.Sandbox(), owner)
	req.From = "base"
	req.IdempotencyKey = key
	req.RequestSHA256 = secondHash[:]
	if _, err := env.scheduler(scheduler.Options{Restore: resolver}).Admit(t.Context(), req); !errors.Is(err, db.ErrIdempotencyReused) {
		t.Fatalf("changed hash error = %v, want ErrIdempotencyReused", err)
	}
	if got := resolver.calls.Load(); got != 0 {
		t.Fatalf("resolver ran %d times for a reused key", got)
	}
	if n := schedCount(t, env.store, `SELECT count(*) FROM sandboxes`); n != 1 {
		t.Fatalf("sandboxes = %d, want only the winner", n)
	}
}

// TestSnapshotFlowRestoreRaceCrossOwner proves idempotency keys are owner
// scoped: the same key and hash create independently for two accounts.
func TestSnapshotFlowRestoreRaceCrossOwner(t *testing.T) {
	env := newSchedEnv(t)
	env.startNode(t, "node-a")
	env.registerNode(t, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 256 << 10, DiskGiB: 1000})
	ownerA := env.owner(t)
	ownerB := env.owner(t)
	sourceA := schedReadySource(t, env.store, ownerA, "node-a")
	sourceB := schedReadySource(t, env.store, ownerB, "node-a")
	srcA := snapshotFlowSavedSource(t, env.store, ownerA, sourceA, "node-a", "snap-"+strings.Repeat("c", 32))
	srcB := snapshotFlowSavedSource(t, env.store, ownerB, sourceB, "node-a", "snap-"+strings.Repeat("d", 32))

	key := "shared-key"
	hash := sha256.Sum256([]byte(`{"from":"base"}`))
	admit := env.scheduler(scheduler.Options{})
	request := func(owner uuid.UUID, source db.SnapshotSource) scheduler.AdmitRequest {
		req := schedAdmitRequest(ids.Sandbox(), owner)
		req.MachineType = "default"
		req.Reserve = snapshotFlowRestoreReserve()
		req.Restore = &source
		req.IdempotencyKey = key
		req.RequestSHA256 = hash[:]
		return req
	}
	first, err := admit.Admit(t.Context(), request(ownerA, srcA))
	if err != nil {
		t.Fatalf("owner A admit: %v", err)
	}
	second, err := admit.Admit(t.Context(), request(ownerB, srcB))
	if err != nil {
		t.Fatalf("owner B admit: %v", err)
	}
	if first.Replayed || second.Replayed || first.Sandbox.ID == second.Sandbox.ID {
		t.Fatalf("cross-owner creates interfered: a_replayed=%v b_replayed=%v distinct_ids=%v",
			first.Replayed, second.Replayed, first.Sandbox.ID != second.Sandbox.ID)
	}
	if first.Sandbox.OwnerID != ownerA || second.Sandbox.OwnerID != ownerB {
		t.Fatal("cross-owner creates landed under the wrong account")
	}
	if n := schedCount(t, env.store, `SELECT count(*) FROM sandboxes`); n != 4 {
		t.Fatalf("sandboxes = %d, want two sources and two restores", n)
	}
}

// TestSnapshotFlowRestoreRaceCallerCancellation proves the caller's
// cancellation survives the final keyed replay instead of being replaced by a
// replay or a capacity answer.
func TestSnapshotFlowRestoreRaceCallerCancellation(t *testing.T) {
	env := newSchedEnv(t)
	env.registerNode(t, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 256 << 10, DiskGiB: 1000})
	owner := env.owner(t)
	source := schedReadySource(t, env.store, owner, "node-a")
	schedSavedSource(t, env.store, owner, source, "node-a")

	key := "cancel-key"
	hash := sha256.Sum256([]byte(`{"from":"base"}`))
	resolver := &snapshotFlowCancelResolver{entered: make(chan struct{}, 1)}
	admit := env.scheduler(scheduler.Options{Restore: resolver})
	req := schedAdmitRequest(ids.Sandbox(), owner)
	req.From = "base"
	req.IdempotencyKey = key
	req.RequestSHA256 = hash[:]

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, err := admit.Admit(ctx, req)
		done <- err
	}()
	t.Cleanup(func() { cancel(); <-finished })
	select {
	case <-resolver.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("admit never reached source resolution")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled admit error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled admit did not return")
	}
	var created int
	if err := env.store.Pool().QueryRow(t.Context(), `
		SELECT count(*) FROM sandboxes WHERE id = $1`, req.ID).Scan(&created); err != nil {
		t.Fatalf("count canceled sandbox: %v", err)
	}
	if created != 0 {
		t.Fatalf("canceled admit created %d sandboxes", created)
	}
}

// TestSnapshotFlowRestoreFailFastBudgetCoversResolution proves source
// resolution runs inside the same failFast budget as replay and placement, and
// an expired budget maps to the retryable no-ready-machine family.
func TestSnapshotFlowRestoreFailFastBudgetCoversResolution(t *testing.T) {
	env := newSchedEnv(t)
	env.registerNode(t, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 256 << 10, DiskGiB: 1000})
	owner := env.owner(t)
	source := schedReadySource(t, env.store, owner, "node-a")
	schedSavedSource(t, env.store, owner, source, "node-a")

	resolver := &snapshotFlowWaitingResolver{entered: make(chan struct{}, 1)}
	const budget = 150 * time.Millisecond
	admit := env.scheduler(scheduler.Options{Restore: resolver, FailFastBudget: budget})
	req := schedAdmitRequest(ids.Sandbox(), owner)
	req.From = "base"
	req.FailFast = true

	start := time.Now()
	_, err := admit.Admit(t.Context(), req)
	elapsed := time.Since(start)
	if !errors.Is(err, scheduler.ErrNoNode) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("budgeted resolution error = %v, want ErrNoNode + DeadlineExceeded", err)
	}
	if elapsed >= time.Second {
		t.Fatalf("budget elapsed = %s, want well below a 1s fallback", elapsed)
	}
	raw := resolver.deadline.Load()
	deadline, ok := raw.(time.Time)
	if !ok || deadline.IsZero() {
		t.Fatal("resolver context carried no admission deadline")
	}
	if until := deadline.Sub(start); until <= 0 || until > budget+50*time.Millisecond {
		t.Fatalf("resolver deadline %s is outside the %s budget", until, budget)
	}
	var created int
	if err := env.store.Pool().QueryRow(t.Context(), `
		SELECT count(*) FROM sandboxes WHERE id = $1`, req.ID).Scan(&created); err != nil {
		t.Fatalf("count budgeted sandbox: %v", err)
	}
	if created != 0 {
		t.Fatalf("budgeted admit created %d sandboxes", created)
	}
}

// TestSnapshotFlowRestoreResolvedSticksToRecordedNode proves a create-from that
// resolves a name is placed only on the artifact's recorded node, never a
// fresher fallback.
func TestSnapshotFlowRestoreResolvedSticksToRecordedNode(t *testing.T) {
	env := newSchedEnv(t)
	env.startNode(t, "node-a")
	env.startNode(t, "node-b")
	env.registerNode(t, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 256 << 10, DiskGiB: 1000})
	env.registerNode(t, "node-b", db.Resources{VCPUs: 64, MemoryMiB: 256 << 10, DiskGiB: 1000})
	owner := env.owner(t)
	source := schedReadySource(t, env.store, owner, "node-b")
	saved := schedSavedSource(t, env.store, owner, source, "node-b")

	admit := env.scheduler(scheduler.Options{Restore: snapshotFlowStoreResolver{store: env.store}})
	req := schedAdmitRequest(ids.Sandbox(), owner)
	req.From = "base"
	req.TypeExplicit = false
	result, err := admit.Admit(t.Context(), req)
	if err != nil {
		t.Fatalf("resolved restore: %v", err)
	}
	if result.Sandbox.NodeID == nil || *result.Sandbox.NodeID != "node-b" {
		t.Fatalf("restore node = %v, want the recorded node-b", result.Sandbox.NodeID)
	}
	if result.Sandbox.FromSnapshotID != saved.ArtifactID || !result.Sandbox.SnapshotPin {
		t.Fatalf("restore row from=%q pin=%v", result.Sandbox.FromSnapshotID, result.Sandbox.SnapshotPin)
	}
}
