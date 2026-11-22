package db_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
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

// dataExpiryStopAgo rewrites a sandbox's stopped_at to the past.
func dataExpiryStopAgo(t *testing.T, s *db.Store, sandboxID string, d time.Duration) {
	t.Helper()
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET stopped_at = now() - make_interval(secs => $2::float8) WHERE id = $1`,
		sandboxID, d.Seconds()); err != nil {
		t.Fatalf("backdate stop: %v", err)
	}
}

// TestDataExpiryExpiresStoppedData proves one expiry releases the retained disk
// exactly once, keeps the row, records the node copy as an eviction and the
// event, and never accepts the same candidate twice.
func TestDataExpiryExpiresStoppedData(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	stopped, _ := phase1Stopped(t, s, own, sbx)
	if stopped.DiskNodeID == nil || *stopped.DiskNodeID != "node-a" {
		t.Fatalf("stopped sandbox disk node: %+v", stopped.DiskNodeID)
	}
	dataExpiryStopAgo(t, s, sbx.ID, 2*time.Hour)
	cutoff := time.Now().Add(-time.Hour)

	candidates, err := s.StoppedSandboxesWithDataBefore(t.Context(), cutoff, 100)
	if err != nil || len(candidates) != 1 || candidates[0].ID != sbx.ID {
		t.Fatalf("candidates: %+v err=%v", candidates, err)
	}

	expired, ok, err := s.AcceptDataExpiry(t.Context(), own, sbx.ID, cutoff)
	if err != nil || !ok {
		t.Fatalf("accept expiry: ok=%v err=%v", ok, err)
	}
	if expired.DataExpiredAt == nil || expired.DiskNodeID != nil || expired.NodeID != nil {
		t.Fatalf("expired row: %+v", expired)
	}
	if expired.State != db.StateArchived || expired.DeletedAt != nil {
		t.Fatalf("expiry changed the row's lifecycle: %+v", expired)
	}
	if expired.Reserved != (db.Resources{}) {
		t.Fatalf("expiry kept a reservation: %+v", expired.Reserved)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{}) {
		t.Fatalf("node after expiry: %+v", got)
	}
	if n := count(t, s, `SELECT count(*) FROM vm_evictions
		WHERE node_id = 'node-a' AND sandbox_id = $1 AND reason = 'data_expired' AND done_at IS NULL`, sbx.ID); n != 1 {
		t.Fatalf("eviction rows: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM sandbox_events
		WHERE sandbox_id = $1 AND type = 'sandbox.data_expired'`, sbx.ID); n != 1 {
		t.Fatalf("data expiry events: %d", n)
	}

	// The row no longer lists as a candidate and a repeat changes nothing.
	candidates, err = s.StoppedSandboxesWithDataBefore(t.Context(), cutoff, 100)
	if err != nil || len(candidates) != 0 {
		t.Fatalf("candidates after expiry: %+v err=%v", candidates, err)
	}
	if _, ok, err := s.AcceptDataExpiry(t.Context(), own, sbx.ID, cutoff); err != nil || ok {
		t.Fatalf("repeat expiry: ok=%v err=%v", ok, err)
	}
	if n := count(t, s, `SELECT count(*) FROM vm_evictions WHERE sandbox_id = $1`, sbx.ID); n != 1 {
		t.Fatalf("eviction rows after repeat: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM sandbox_events
		WHERE sandbox_id = $1 AND type = 'sandbox.data_expired'`, sbx.ID); n != 1 {
		t.Fatalf("data expiry events after repeat: %d", n)
	}

	// Unknown, foreign and deleted sandboxes are never expired by a reaper.
	if _, _, err := s.AcceptDataExpiry(t.Context(), own, "bx_missing1", cutoff); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("unknown sandbox: %v", err)
	}
	if _, _, err := s.AcceptDataExpiry(t.Context(), owner(t, s), sbx.ID, cutoff); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign sandbox: %v", err)
	}
	if _, _, err := s.AcceptDataExpiry(t.Context(), uuid.Nil(), sbx.ID, cutoff); !errors.Is(err, db.ErrNoOwner) {
		t.Fatalf("ownerless expiry: %v", err)
	}
}

// TestDataExpirySkipsNewerResumedAndDeleted proves a candidate that is no
// longer eligible is left untouched: a fresh stop, a resume in flight and a
// deleted row.
func TestDataExpirySkipsNewerResumedAndDeleted(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	cutoff := time.Now().Add(-time.Hour)

	fresh := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	phase1Stopped(t, s, own, fresh)
	if _, ok, err := s.AcceptDataExpiry(t.Context(), own, fresh.ID, cutoff); err != nil || ok {
		t.Fatalf("fresh stop expired: ok=%v err=%v", ok, err)
	}

	resuming := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	phase1Stopped(t, s, own, resuming)
	dataExpiryStopAgo(t, s, resuming.ID, 2*time.Hour)
	if _, _, err := s.EnqueueJobIfAbsent(t.Context(), db.JobInput{
		Kind: db.JobResume, OwnerID: own, SandboxID: &resuming.ID, Payload: "{}",
	}); err != nil {
		t.Fatalf("queue resume: %v", err)
	}
	if _, ok, err := s.AcceptDataExpiry(t.Context(), own, resuming.ID, cutoff); err != nil || ok {
		t.Fatalf("sandbox with a resume in flight expired: ok=%v err=%v", ok, err)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.resume' AND state = 'queued'`); n != 1 {
		t.Fatalf("the refusal changed the queue: %d", n)
	}

	deleted := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	phase1Stopped(t, s, own, deleted)
	dataExpiryStopAgo(t, s, deleted.ID, 2*time.Hour)
	if _, _, err := s.AcceptSandboxDeletion(t.Context(), own, deleted.ID, db.DeletionExplicit); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, _, err := s.AcceptDataExpiry(t.Context(), own, deleted.ID, cutoff); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("deleted sandbox: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM sandbox_events
		WHERE sandbox_id = ANY($1::text[]) AND type = 'sandbox.data_expired'`,
		[]string{fresh.ID, resuming.ID, deleted.ID}); n != 0 {
		t.Fatalf("skipped candidates were expired: %d", n)
	}
}

// TestDataExpiryConcurrentAccept proves two reapers racing one candidate
// expire it exactly once.
func TestDataExpiryConcurrentAccept(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	phase1Stopped(t, s, own, sbx)
	dataExpiryStopAgo(t, s, sbx.ID, 2*time.Hour)
	cutoff := time.Now().Add(-time.Hour)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		accepted int
		errs     []error
	)
	for range 2 {
		wg.Go(func() {
			_, ok, err := s.AcceptDataExpiry(t.Context(), own, sbx.ID, cutoff)
			mu.Lock()
			defer mu.Unlock()
			if ok {
				accepted++
			}
			if err != nil {
				errs = append(errs, err)
			}
		})
	}
	wg.Wait()
	if len(errs) != 0 || accepted != 1 {
		t.Fatalf("race: accepted=%d errs=%v", accepted, errs)
	}
	if n := count(t, s, `SELECT count(*) FROM sandbox_events
		WHERE sandbox_id = $1 AND type = 'sandbox.data_expired'`, sbx.ID); n != 1 {
		t.Fatalf("data expiry events: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM vm_evictions WHERE sandbox_id = $1`, sbx.ID); n != 1 {
		t.Fatalf("eviction rows: %d", n)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{}) {
		t.Fatalf("node after the race: %+v", got)
	}
}

// TestSavedSnapshotSurvivesSourceDataExpiry proves a saved generation stays
// restorable after its source's data expired, while a new save from the
// expired source is refused.
func TestSavedSnapshotSurvivesSourceDataExpiry(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "base")
	snap = snapshotCompleteSave(t, s, snap, 4096)

	phase1Stopped(t, s, own, source)
	dataExpiryStopAgo(t, s, source.ID, 30*24*time.Hour)
	cutoff := time.Now().Add(-14 * 24 * time.Hour)
	expired, ok, err := s.AcceptDataExpiry(t.Context(), own, source.ID, cutoff)
	if err != nil || !ok || expired.DataExpiredAt == nil {
		t.Fatalf("expire source: ok=%v row=%+v err=%v", ok, expired, err)
	}

	if _, err := s.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: own, SandboxID: source.ID, Name: "after",
	}); !errors.Is(err, db.ErrSnapshotDataExpired) {
		t.Fatalf("save from an expired source: %v", err)
	}

	src, err := s.SnapshotRestoreSource(t.Context(), own, "base")
	if err != nil || src.ArtifactID != snap.ArtifactID {
		t.Fatalf("restore source after expiry: %+v err=%v", src, err)
	}
	restored, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID:          ids.Sandbox(),
		Owner:       own,
		NodeID:      nodeID,
		Name:        "restored",
		MachineType: source.MachineType,
		Reserve:     db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: source.Reserved.DiskGiB},
		Restore:     &src,
	})
	if err != nil {
		t.Fatalf("create from the saved snapshot after expiry: %v", err)
	}
	if !restored.Sandbox.SnapshotPin || restored.Sandbox.FromSnapshotID != snap.ArtifactID {
		t.Fatalf("restored sandbox: %+v", restored.Sandbox)
	}
}
