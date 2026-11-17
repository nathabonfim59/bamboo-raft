package db_test

// Deletion acceptance: the bdop_ operation, soft deletion, queued node
// cleanup, idempotent acceptance and completion, and capacity return.

import (
	"errors"
	"testing"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

func acceptDelete(t *testing.T, s *db.Store, own uuid.UUID, sandboxID string) db.DeletionOperation {
	t.Helper()
	op, _, err := s.AcceptSandboxDeletion(t.Context(), own, sandboxID, db.DeletionExplicit)
	if err != nil {
		t.Fatalf("accept deletion: %v", err)
	}
	return op
}

func TestDeletionAcceptanceAtomic(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	own := owner(t, s)
	res := create(t, s, db.CreateSandboxParams{Owner: own, Name: "going"})

	op := acceptDelete(t, s, own, res.Sandbox.ID)
	if len(op.ID) != 37 || op.ID[:5] != "bdop_" {
		t.Fatalf("operation id: %s", op.ID)
	}
	if op.Kind != db.DeletionSandbox || op.Status != db.DeletionPending || op.Reason != db.DeletionExplicit {
		t.Fatalf("operation: %+v", op)
	}
	// Soft deletion is immediate: gone from get and list.
	if _, err := s.Sandbox(t.Context(), own, res.Sandbox.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
	got, _, err := s.ListSandboxes(t.Context(), db.SandboxListQuery{Owner: own})
	if err != nil || len(got) != 0 {
		t.Fatalf("list after delete: %v %d", err, len(got))
	}
	// Provision job cancelled, delete job queued, event recorded.
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE state = 'cancelled'`); n != 1 {
		t.Fatalf("cancelled jobs: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.delete' AND state = 'queued'`); n != 1 {
		t.Fatalf("delete jobs: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM sandbox_events WHERE type = 'delete.requested'`); n != 1 {
		t.Fatalf("delete events: %d", n)
	}
	// Saved snapshots stay untouched: none existed here, but the schema
	// keeps artifacts; acceptance only cancels sandbox work.
	if n := count(t, s, `SELECT count(*) FROM saved_snapshots`); n != 0 {
		t.Fatalf("snapshots touched: %d", n)
	}

	// Owner-scoped GET.
	fetched, err := s.DeletionOperation(t.Context(), own, op.ID)
	if err != nil || fetched.ID != op.ID {
		t.Fatalf("get operation: %v", err)
	}
	other := owner(t, s)
	if _, err := s.DeletionOperation(t.Context(), other, op.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign operation get: %v", err)
	}

	// Re-accept returns the same active operation, no extra jobs.
	again := acceptDelete(t, s, own, res.Sandbox.ID)
	if again.ID != op.ID {
		t.Fatalf("re-accept minted %s", again.ID)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.delete'`); n != 1 {
		t.Fatalf("delete jobs after re-accept: %d", n)
	}
}

func TestDeletionWorkerLifecycle(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	own := owner(t, s)
	res := create(t, s, db.CreateSandboxParams{Owner: own})
	op := acceptDelete(t, s, own, res.Sandbox.ID)
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 1 {
		t.Fatalf("reserved before worker: %+v", got)
	}

	// Worker claims the delete job and marks the attempt, owner-scoped.
	begun, err := s.BeginDeletionAttempt(t.Context(), own, op.ID)
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	if begun.Status != db.DeletionProcessing || begun.Stage != db.StageRemoving || begun.AttemptCount != 1 {
		t.Fatalf("begun: %+v", begun)
	}
	other := owner(t, s)
	if _, err := s.BeginDeletionAttempt(t.Context(), other, op.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign begin: %v", err)
	}
	// Attempts accumulate per delivery.
	if _, err := s.BeginDeletionAttempt(t.Context(), own, op.ID); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, `SELECT attempt_count FROM deletion_operations WHERE id = $1`, op.ID); n != 2 {
		t.Fatalf("attempt count: %d", n)
	}

	// Confirmed node cleanup: completed, capacity back, exactly once.
	done, err := s.CompleteDeletion(t.Context(), own, op.ID)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if done.Status != db.DeletionCompleted || done.CompletedAt == nil || done.Stage != db.StageCompleted {
		t.Fatalf("done: %+v", done)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 0 || got.MemoryMiB != 0 || got.DiskGiB != 0 {
		t.Fatalf("reserved after complete: %+v", got)
	}
	if n := count(t, s, `SELECT reserved_vcpus + reserved_memory_mib + reserved_disk_gib FROM sandboxes WHERE id = $1`, res.Sandbox.ID); n != 0 {
		t.Fatalf("sandbox reservation not cleared: %d", n)
	}
	// Duplicate delivery: idempotent completion, no double release.
	repeat, err := s.CompleteDeletion(t.Context(), own, op.ID)
	if err != nil || repeat.Status != db.DeletionCompleted {
		t.Fatalf("repeat complete: %v %+v", err, repeat)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 0 {
		t.Fatalf("double release: %+v", got)
	}
	if _, err := s.CompleteDeletion(t.Context(), other, op.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign complete: %v", err)
	}
}

func TestDeletionAfterCompletionReplaysOperation(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	own := owner(t, s)
	res := create(t, s, db.CreateSandboxParams{Owner: own})
	op := acceptDelete(t, s, own, res.Sandbox.ID)
	if _, err := s.CompleteDeletion(t.Context(), own, op.ID); err != nil {
		t.Fatal(err)
	}
	// A late DELETE returns the completed operation, not an error and not
	// a second one.
	late := acceptDelete(t, s, own, res.Sandbox.ID)
	if late.ID != op.ID || late.Status != db.DeletionCompleted {
		t.Fatalf("late accept: %+v", late)
	}
	if n := count(t, s, `SELECT count(*) FROM deletion_operations`); n != 1 {
		t.Fatalf("operations: %d", n)
	}
	// Unknown sandbox stays not-found.
	if _, _, err := s.AcceptSandboxDeletion(t.Context(), own, "bx_missing1", db.DeletionExplicit); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("unknown sandbox: %v", err)
	}
}
