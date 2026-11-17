package db_test

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"sync"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

func TestLifecycleReplayBeforeCapacity(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 65536, DiskGiB: 1000})
	key := "replay-key"
	hash := sha256.Sum256([]byte("request"))
	res := create(t, s, db.CreateSandboxParams{
		Owner: own, IdempotencyKey: key, RequestSHA256: hash[:],
	})

	got, found, err := s.ReplayCreateSandbox(t.Context(), own, key, hash[:])
	if err != nil || !found || got.Sandbox.ID != res.Sandbox.ID || !got.Replayed {
		t.Fatalf("replay: %+v found=%v err=%v", got, found, err)
	}
	other := sha256.Sum256([]byte("different"))
	if _, _, err := s.ReplayCreateSandbox(t.Context(), own, key, other[:]); !errors.Is(err, db.ErrIdempotencyReused) {
		t.Fatalf("different hash: %v", err)
	}
	if _, found, err := s.ReplayCreateSandbox(t.Context(), own, "unknown", hash[:]); err != nil || found {
		t.Fatalf("unknown key: found=%v err=%v", found, err)
	}
	// Expiry removes the replay without deleting the sandbox.
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE idempotency_keys SET expires_at = now() - interval '1 second' WHERE owner_id = $1 AND key = $2`,
		own, key); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.ReplayCreateSandbox(t.Context(), own, key, hash[:]); err != nil || found {
		t.Fatalf("expired key: found=%v err=%v", found, err)
	}
}

func TestLifecycleSandboxAnyAndNodeOwnership(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	create(t, s, db.CreateSandboxParams{Owner: own, NodeID: "node-a"})
	gone := create(t, s, db.CreateSandboxParams{Owner: own, NodeID: "node-a"})
	if _, _, err := s.AcceptSandboxDeletion(t.Context(), own, gone.Sandbox.ID, db.DeletionExplicit); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sandbox(t.Context(), own, gone.Sandbox.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("visible read of deleted: %v", err)
	}
	sbx, err := s.SandboxAny(t.Context(), own, gone.Sandbox.ID)
	if err != nil || sbx.DeletedAt == nil {
		t.Fatalf("canonical deleted row: %+v err=%v", sbx, err)
	}
	rows, err := s.SandboxesOnNode(t.Context(), "node-a")
	if err != nil || len(rows) != 2 {
		t.Fatalf("sandboxes on node: %d err=%v", len(rows), err)
	}
	ids, err := s.NodeIDs(t.Context())
	if err != nil || len(ids) != 1 || ids[0] != "node-a" {
		t.Fatalf("node ids: %v err=%v", ids, err)
	}
}

func TestLifecycleTransitionSandboxJobFencing(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	res := create(t, s, db.CreateSandboxParams{Owner: own, NodeID: "node-a"})
	job, err := s.ClaimJob(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	params := db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: res.Sandbox.Version,
		To:              db.StateReady,
	}
	out, err := s.TransitionSandboxJob(t.Context(), job.ID, "worker", job.LeaseToken, params)
	if err != nil || out.State != db.StateReady {
		t.Fatalf("fenced transition: %+v err=%v", out, err)
	}
	// Wrong token cannot move the row.
	badToken := uuid.NewV7()
	params.To = db.StateIdle
	params.FromStates = []db.SandboxState{db.StateReady}
	params.ExpectedVersion = out.Version
	if _, err := s.TransitionSandboxJob(t.Context(), job.ID, "worker", badToken, params); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("wrong token: %v", err)
	}
	// A stale version is a conflict, not a lost lease.
	params.ExpectedVersion = out.Version - 1
	if _, err := s.TransitionSandboxJob(t.Context(), job.ID, "worker", job.LeaseToken, params); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("stale version: %v", err)
	}
	if err := s.CompleteJob(t.Context(), job.ID, "worker", job.LeaseToken); err != nil {
		t.Fatal(err)
	}
	params.ExpectedVersion = out.Version
	if _, err := s.TransitionSandboxJob(t.Context(), job.ID, "worker", job.LeaseToken, params); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("completed job fence: %v", err)
	}
}

func TestLifecycleTransitionSandboxJobRejectsForeignTarget(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	first := create(t, s, db.CreateSandboxParams{Owner: own, NodeID: "node-a"})
	second := create(t, s, db.CreateSandboxParams{Owner: own, NodeID: "node-a"})

	jobA, err := s.ClaimJob(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteJob(t.Context(), jobA.ID, "worker", jobA.LeaseToken); err != nil {
		t.Fatal(err)
	}
	jobB, err := s.ClaimJob(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if *jobB.SandboxID != second.Sandbox.ID {
		t.Fatalf("claimed job for %v, want %s", jobB.SandboxID, second.Sandbox.ID)
	}
	// A valid token for another sandbox's job must not move this row.
	_, err = s.TransitionSandboxJob(t.Context(), jobB.ID, "worker", jobB.LeaseToken, db.TransitionParams{
		SandboxID: first.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: first.Sandbox.Version,
		To:              db.StateReady,
	})
	if !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("foreign job token moved another sandbox: %v", err)
	}
	// The same token still works for its own target.
	if _, err := s.TransitionSandboxJob(t.Context(), jobB.ID, "worker", jobB.LeaseToken, db.TransitionParams{
		SandboxID: second.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: second.Sandbox.Version,
		To:              db.StateReady,
	}); err != nil {
		t.Fatalf("own target: %v", err)
	}
}

func TestLifecycleFailAndCancelJobAreFenced(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	first := enqueue(t, s, own, 3)
	job, err := s.ClaimJob(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != first.ID {
		t.Fatalf("claimed %s want %s", job.ID, first.ID)
	}
	if err := s.FailJob(t.Context(), job.ID, "worker", uuid.NewV7(), "bad"); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("fail with wrong token: %v", err)
	}
	if err := s.FailJob(t.Context(), job.ID, "worker", job.LeaseToken, "invalid payload"); err != nil {
		t.Fatal(err)
	}
	var state db.JobState
	if err := s.Pool().QueryRow(t.Context(), `SELECT state FROM jobs WHERE id = $1`, job.ID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != db.JobFailed {
		t.Fatalf("state after fail: %s", state)
	}

	second := enqueue(t, s, own, 3)
	cancelJob, err := s.ClaimJob(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if cancelJob.ID != second.ID {
		t.Fatalf("claimed %s want %s", cancelJob.ID, second.ID)
	}
	if err := s.CancelJob(t.Context(), cancelJob.ID, "worker", cancelJob.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if state := lifeJobState(t, s, cancelJob.ID); state != db.JobCancelled {
		t.Fatalf("state after cancel: %s", state)
	}
}

func TestLifecycleEnqueueJobIfAbsent(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	res := create(t, s, db.CreateSandboxParams{Owner: own, NodeID: "node-a"})
	sandboxID := res.Sandbox.ID
	in := db.JobInput{Kind: db.JobDeleteVM, OwnerID: own, SandboxID: &sandboxID, Payload: `{"NodeID":"node-a"}`}

	first, added, err := s.EnqueueJobIfAbsent(t.Context(), in)
	if err != nil || !added {
		t.Fatalf("first enqueue: added=%v err=%v", added, err)
	}
	if _, added, err := s.EnqueueJobIfAbsent(t.Context(), in); err != nil || added {
		t.Fatalf("second enqueue: added=%v err=%v", added, err)
	}
	job, err := s.ClaimJob(t.Context(), "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.Kind == db.JobProvision {
		// The create fixtures also queued a provision job; finish it so
		// the deduplicated delete job is next.
		if err := s.CompleteJob(t.Context(), job.ID, "worker", job.LeaseToken); err != nil {
			t.Fatal(err)
		}
		job, err = s.ClaimJob(t.Context(), "worker", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
	}
	if job.ID != first.ID {
		t.Fatalf("claimed %s, want deduplicated job %s", job.ID, first.ID)
	}
	if err := s.CompleteJob(t.Context(), job.ID, "worker", job.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, added, err := s.EnqueueJobIfAbsent(t.Context(), in); err != nil || !added {
		t.Fatalf("after completion: added=%v err=%v", added, err)
	}
}

func TestLifecycleRetrySandboxDeletionLeavesNewAttemptStatus(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	res := create(t, s, db.CreateSandboxParams{Owner: own, NodeID: "node-a"})
	op, sbx, err := s.AcceptSandboxDeletion(t.Context(), own, res.Sandbox.ID, db.DeletionExplicit)
	if err != nil {
		t.Fatal(err)
	}
	payload := lifeDeletionPayload(t, sbx, op.ID)
	input := db.JobInput{Kind: db.JobDeleteVM, OwnerID: own, SandboxID: &res.Sandbox.ID, Payload: payload}

	old, err := s.ClaimJob(t.Context(), "old", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FailJob(t.Context(), old.ID, "old", old.LeaseToken, "node unreachable"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnqueueJob(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	newJob, err := s.ClaimJob(t.Context(), "new", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginDeletionAttempt(t.Context(), own, op.ID); err != nil {
		t.Fatal(err)
	}

	got, added, err := s.RetrySandboxDeletion(t.Context(), own, op.ID, res.Sandbox.ID, input)
	if err != nil {
		t.Fatal(err)
	}
	if added {
		t.Fatal("old failure enqueued a retry over a running attempt")
	}
	if got.Status != db.DeletionProcessing {
		t.Fatalf("status changed to %s while a new attempt was processing", got.Status)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.delete' AND state IN ('queued','running')`); n != 1 {
		t.Fatalf("active delete jobs: %d, want only the running attempt", n)
	}
	_ = newJob

	// Concurrent reapers over an exhausted job produce exactly one retry.
	other := create(t, s, db.CreateSandboxParams{Owner: own, NodeID: "node-a"})
	op2, sbx2, err := s.AcceptSandboxDeletion(t.Context(), own, other.Sandbox.ID, db.DeletionExplicit)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := s.ClaimJob(t.Context(), "old", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FailJob(t.Context(), failed.ID, "old", failed.LeaseToken, "node unreachable"); err != nil {
		t.Fatal(err)
	}
	input2 := db.JobInput{Kind: db.JobDeleteVM, OwnerID: own, SandboxID: &other.Sandbox.ID, Payload: lifeDeletionPayload(t, sbx2, op2.ID)}
	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		addedCount int
	)
	for range 2 {
		wg.Go(func() {
			_, added, err := s.RetrySandboxDeletion(t.Context(), own, op2.ID, other.Sandbox.ID, input2)
			if err != nil {
				t.Errorf("retry deletion: %v", err)
				return
			}
			if added {
				mu.Lock()
				addedCount++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if addedCount != 1 {
		t.Fatalf("concurrent reapers enqueued %d retries, want 1", addedCount)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.delete' AND state = 'queued'`, other.Sandbox.ID); n != 1 {
		t.Fatalf("queued retries: %d, want 1", n)
	}
	got2, err := s.DeletionOperation(t.Context(), own, op2.ID)
	if err != nil || got2.Status != db.DeletionBlocked || got2.Stage != db.StageRetrying {
		t.Fatalf("operation after retry: %+v err=%v", got2, err)
	}
}

func lifeDeletionPayload(t *testing.T, sbx db.Sandbox, opID string) string {
	t.Helper()
	b, err := json.Marshal(db.DeleteVMPayload{
		NodeID: derefLifeNode(sbx.NodeID), VMID: sbx.ID, Resources: sbx.Reserved, OperationID: opID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func derefLifeNode(nodeID *string) string {
	if nodeID == nil {
		return ""
	}
	return *nodeID
}

func TestLifecycleGuardSerializesAndDetectsLoss(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	res := create(t, s, db.CreateSandboxParams{Owner: own, NodeID: "node-a"})

	guard, err := s.AcquireSandboxGuard(t.Context(), own, res.Sandbox.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { guard.Release(t.Context()) })
	acquireCtx, cancelAcquire := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancelAcquire)
	acquired := make(chan *db.SandboxGuard, 1)
	secondFinished := make(chan struct{})
	go func() {
		defer close(secondFinished)
		g, err := s.AcquireSandboxGuard(acquireCtx, own, res.Sandbox.ID)
		if err != nil {
			acquired <- nil
			return
		}
		acquired <- g
	}()
	t.Cleanup(func() {
		cancelAcquire()
		<-secondFinished
		select {
		case g := <-acquired:
			if g != nil {
				g.Release(t.Context())
			}
		default:
		}
	})
	select {
	case g := <-acquired:
		if g != nil {
			g.Release(t.Context())
		}
		t.Fatal("second guard acquired while the first was held")
	case <-time.After(250 * time.Millisecond):
	}
	guard.Release(t.Context())
	select {
	case g := <-acquired:
		if g == nil {
			t.Fatal("second guard failed after release")
		}
		g.Release(t.Context())
	case <-time.After(5 * time.Second):
		t.Fatal("second guard did not acquire after release")
	}

	// A killed dedicated backend releases the lock and cancels the attempt.
	guard, err = s.AcquireSandboxGuard(t.Context(), own, res.Sandbox.ID)
	if err != nil {
		t.Fatal(err)
	}
	granted := guard
	t.Cleanup(func() { granted.Release(t.Context()) })
	var pid int32
	if err := s.Pool().QueryRow(t.Context(), `
		SELECT pid FROM pg_stat_activity
		WHERE datname = current_database()
		  AND application_name = current_setting('application_name') || '_guard'
		  AND pid <> pg_backend_pid()
		LIMIT 1`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(t.Context(), `SELECT pg_terminate_backend($1)`, pid); err != nil {
		t.Fatal(err)
	}
	select {
	case <-guard.Done():
		if !errors.Is(guard.Err(), db.ErrSandboxGuardLost) {
			t.Fatalf("guard error: %v", guard.Err())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("guard loss was not detected")
	}
	guard.Release(t.Context())
}

func TestLifecycleActiveDeletionAndBlock(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	res := create(t, s, db.CreateSandboxParams{Owner: own, NodeID: "node-a"})
	op, _, err := s.AcceptSandboxDeletion(t.Context(), own, res.Sandbox.ID, db.DeletionExplicit)
	if err != nil {
		t.Fatal(err)
	}
	active, err := s.ActiveSandboxDeletion(t.Context(), own, res.Sandbox.ID)
	if err != nil || active.ID != op.ID {
		t.Fatalf("active deletion: %+v err=%v", active, err)
	}
	blocked, err := s.BlockDeletionOperation(t.Context(), own, op.ID)
	if err != nil || blocked.Status != db.DeletionBlocked || blocked.Stage != db.StageRetrying {
		t.Fatalf("blocked: %+v err=%v", blocked, err)
	}
	if _, err := s.CompleteDeletion(t.Context(), own, op.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ActiveSandboxDeletion(t.Context(), own, res.Sandbox.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("completed operation still active: %v", err)
	}
}

func lifeJobState(t *testing.T, s *db.Store, id uuid.UUID) db.JobState {
	t.Helper()
	var state db.JobState
	if err := s.Pool().QueryRow(t.Context(), `SELECT state FROM jobs WHERE id = $1`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}
