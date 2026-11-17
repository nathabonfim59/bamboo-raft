package db_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

func TestStoppedSandboxesBeforeAndRetentionAcceptance(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	stopped, _ := phase1Stopped(t, s, own, sbx)
	if stopped.StoppedAt == nil {
		t.Fatal("stopped sandbox has no stopped_at")
	}

	candidates, err := s.StoppedSandboxesBefore(t.Context(), stopped.StoppedAt.Add(-time.Hour), 100)
	if err != nil || len(candidates) != 0 {
		t.Fatalf("candidates before stop: %d err=%v", len(candidates), err)
	}
	candidates, err = s.StoppedSandboxesBefore(t.Context(), stopped.StoppedAt.Add(time.Second), 100)
	if err != nil || len(candidates) != 1 || candidates[0].ID != sbx.ID {
		t.Fatalf("candidates after stop: %+v err=%v", candidates, err)
	}

	// A stale cutoff (the candidate was stopped later than it) changes
	// nothing and does not create an operation.
	op, _, accepted, err := s.AcceptStoppedRetentionDeletion(t.Context(), own, sbx.ID, stopped.StoppedAt.Add(-time.Hour), db.DeletionExplicit)
	if err != nil || accepted {
		t.Fatalf("stale cutoff: accepted=%v op=%+v err=%v", accepted, op, err)
	}
	if n := count(t, s, `SELECT count(*) FROM deletion_operations`); n != 0 {
		t.Fatalf("stale cutoff wrote operations: %d", n)
	}

	op, deleted, accepted, err := s.AcceptStoppedRetentionDeletion(t.Context(), own, sbx.ID, stopped.StoppedAt.Add(time.Second), db.DeletionExplicit)
	if err != nil || !accepted {
		t.Fatalf("retention accept: accepted=%v err=%v", accepted, err)
	}
	if op.Kind != db.DeletionSandbox || op.TargetID != sbx.ID || deleted.DeletedAt == nil {
		t.Fatalf("deletion: %+v sandbox=%+v", op, deleted)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 0 || got.MemoryMiB != 0 || got.DiskGiB != 10 {
		t.Fatalf("retention acceptance released disk early: %+v", got)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.delete' AND state = 'queued' AND sandbox_id = $1`, sbx.ID); n != 1 {
		t.Fatalf("delete jobs: %d", n)
	}
	if _, err := s.CompleteDeletion(t.Context(), own, op.ID); err != nil {
		t.Fatal(err)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 0 || got.DiskGiB != 0 {
		t.Fatalf("node after retention deletion: %+v", got)
	}

	// Repeated scans see the deleted row as stale, never a second delete.
	if _, _, accepted, err := s.AcceptStoppedRetentionDeletion(t.Context(), own, sbx.ID, stopped.StoppedAt.Add(time.Second), db.DeletionExplicit); err != nil || accepted {
		t.Fatalf("repeat retention: accepted=%v err=%v", accepted, err)
	}
	if n := count(t, s, `SELECT count(*) FROM deletion_operations`); n != 1 {
		t.Fatalf("operations after repeat: %d", n)
	}
	if _, _, _, err := s.AcceptStoppedRetentionDeletion(t.Context(), owner(t, s), sbx.ID, time.Now().Add(time.Hour), db.DeletionExplicit); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign retention: %v", err)
	}
	if _, _, _, err := s.AcceptStoppedRetentionDeletion(t.Context(), own, "bx_missing1", time.Now(), db.DeletionExplicit); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("unknown retention: %v", err)
	}
}

// TestRetentionRechecksConcurrentResume proves an old retention candidate can
// never delete a sandbox that a resume (or its own completion) moved first.
func TestRetentionRechecksConcurrentResume(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})

	resumedFirst := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	stopped, _ := phase1Stopped(t, s, own, resumedFirst)
	if _, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: resumedFirst.ID}); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if _, _, accepted, err := s.AcceptStoppedRetentionDeletion(t.Context(), own, resumedFirst.ID, stopped.StoppedAt.Add(time.Second), db.DeletionExplicit); err != nil || accepted {
		t.Fatalf("retention after resume: accepted=%v err=%v", accepted, err)
	}
	if n := count(t, s, `SELECT count(*) FROM deletion_operations WHERE target_id = $1`, resumedFirst.ID); n != 0 {
		t.Fatalf("retention deleted a resumed sandbox: %d", n)
	}

	// Concurrently: exactly one side wins, and the invariant holds either
	// way.
	racing := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	racingStopped, _ := phase1Stopped(t, s, own, racing)
	var (
		wg          sync.WaitGroup
		resumeErr   error
		accepted    bool
		retentionEA error
	)
	wg.Go(func() {
		_, resumeErr = s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: racing.ID})
	})
	wg.Go(func() {
		_, _, accepted, retentionEA = s.AcceptStoppedRetentionDeletion(t.Context(), own, racing.ID, racingStopped.StoppedAt.Add(time.Second), db.DeletionExplicit)
	})
	wg.Wait()

	switch {
	case resumeErr == nil:
		if accepted || retentionEA != nil {
			t.Fatalf("resume won but retention accepted=%v err=%v", accepted, retentionEA)
		}
		if got, err := s.Sandbox(t.Context(), own, racing.ID); err != nil || got.State != db.StateProvisioning {
			t.Fatalf("resumed winner: %+v err=%v", got, err)
		}
		if n := count(t, s, `SELECT count(*) FROM deletion_operations WHERE target_id = $1`, racing.ID); n != 0 {
			t.Fatalf("deletion operation for resumed winner: %d", n)
		}
	case accepted && retentionEA == nil:
		if !errors.Is(resumeErr, db.ErrNotFound) {
			t.Fatalf("retention won but resume error: %v", resumeErr)
		}
		if _, err := s.Sandbox(t.Context(), own, racing.ID); !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("deleted sandbox still visible: %v", err)
		}
		if n := count(t, s, `SELECT count(*) FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.resume' AND state = 'queued'`, racing.ID); n != 0 {
			t.Fatalf("resume job for deleted sandbox: %d", n)
		}
	default:
		t.Fatalf("race outcome: resumeErr=%v accepted=%v retentionErr=%v", resumeErr, accepted, retentionEA)
	}
}
