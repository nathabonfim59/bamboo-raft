package db_test

// Saved-snapshot generation, restore-pin and cleanup contracts. Every test
// opens its own PostgreSQL schema through dbtest; without
// BBRAFT_TEST_DATABASE_URL they skip (BBRAFT_TEST_REQUIRE_ALL=1 still fails,
// never weakens the gate).

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"encoding/json/v2"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

func snapshotNode(t *testing.T, s *db.Store) string {
	t.Helper()
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 200})
	return "node-a"
}

// snapshotReadySource creates a sandbox and moves it to ready, the only states a save
// may start from. Its provision job is finished so save claims are
// deterministic.
func snapshotReadySource(t *testing.T, s *db.Store, own uuid.UUID, nodeID string) db.Sandbox {
	t.Helper()
	res := create(t, s, db.CreateSandboxParams{
		Owner: own, NodeID: nodeID, MachineType: "default",
		Reserve: db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 51},
	})
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET state = 'ready' WHERE id = $1`, res.Sandbox.ID); err != nil {
		t.Fatalf("mark source ready: %v", err)
	}
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE jobs SET state = 'done'
		WHERE sandbox_id = $1 AND kind = 'sandbox.provision'`, res.Sandbox.ID); err != nil {
		t.Fatalf("finish provision job: %v", err)
	}
	sbx, err := s.Sandbox(t.Context(), own, res.Sandbox.ID)
	if err != nil {
		t.Fatalf("read ready source: %v", err)
	}
	return sbx
}

func snapshotAcceptSave(t *testing.T, s *db.Store, own uuid.UUID, sandboxID, name string) db.SavedSnapshot {
	t.Helper()
	snap, err := s.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: own, SandboxID: sandboxID, Name: name,
	})
	if err != nil {
		t.Fatalf("accept save %q: %v", name, err)
	}
	return snap
}

// snapshotClaimSave leases the exact save job for snap, leaving cleanup jobs
// untouched. It models one worker claim with a fresh token; a test that wants
// to observe cleanup jobs can still inspect them afterwards.
func snapshotClaimSave(t *testing.T, s *db.Store, snap db.SavedSnapshot) db.Job {
	t.Helper()
	var jobID uuid.UUID
	if err := s.Pool().QueryRow(t.Context(), `
		SELECT id FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'
		  AND (state = 'queued' OR (state = 'running' AND leased_until < now()))
		ORDER BY created_at, id LIMIT 1`,
		snap.OwnerID, snap.ID).Scan(&jobID); err != nil {
		t.Fatalf("save job lookup: %v", err)
	}
	token := uuid.NewV7()
	var attempts int
	if err := s.Pool().QueryRow(t.Context(), `
		UPDATE jobs SET
			state = 'running', attempts = attempts + 1, leased_by = 'snap-worker',
			leased_until = now() + interval '1 hour', lease_token = $2, updated_at = now()
		WHERE id = $1
		  AND (state = 'queued' OR (state = 'running' AND leased_until < now()))
		RETURNING attempts`, jobID, token).Scan(&attempts); err != nil {
		t.Fatalf("claim save job: %v", err)
	}
	return db.Job{
		ID: jobID, Kind: db.JobSaveSnapshot, OwnerID: snap.OwnerID,
		SnapshotID: &snap.ID, LeaseToken: token, Attempts: attempts,
	}
}

// snapshotLeaseStaleSave forces a save job of snap into a live-looking claimed state
// with a fresh token, simulating a delayed worker that is no longer the row's
// current generation.
func snapshotLeaseStaleSave(t *testing.T, s *db.Store, snap db.SavedSnapshot) db.Job {
	t.Helper()
	var jobID uuid.UUID
	if err := s.Pool().QueryRow(t.Context(), `
		SELECT id FROM jobs WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'
		ORDER BY created_at DESC, id DESC LIMIT 1`, snap.OwnerID, snap.ID).Scan(&jobID); err != nil {
		t.Fatalf("stale save job lookup: %v", err)
	}
	token := uuid.NewV7()
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE jobs SET state = 'running', leased_by = 'stale-worker',
			leased_until = now() + interval '1 hour', lease_token = $2
		WHERE id = $1`, jobID, token); err != nil {
		t.Fatalf("lease stale save: %v", err)
	}
	return db.Job{ID: jobID, LeaseToken: token, SnapshotID: &snap.ID}
}

func snapshotCompleteSave(t *testing.T, s *db.Store, snap db.SavedSnapshot, size int64) db.SavedSnapshot {
	t.Helper()
	job := snapshotClaimSave(t, s, snap)
	return snapshotCompleteSaveClaim(t, s, snap, size, job)
}

func snapshotCompleteSaveClaim(t *testing.T, s *db.Store, snap db.SavedSnapshot, size int64, job db.Job) db.SavedSnapshot {
	t.Helper()
	if snap.NodeID == nil {
		t.Fatalf("snapshot %s has no node", snap.ID)
	}
	out, changed, err := s.CompleteSnapshotSave(t.Context(), db.CompleteSnapshotSaveParams{
		Owner:      snap.OwnerID,
		SnapshotID: snap.ID,
		ArtifactID: snap.ArtifactID,
		Generation: snap.Generation,
		NodeID:     *snap.NodeID,
		SizeBytes:  &size,
		JobID:      job.ID,
		Worker:     "snap-worker",
		Token:      job.LeaseToken,
	})
	if err != nil {
		t.Fatalf("complete save: %v", err)
	}
	if !changed {
		t.Fatalf("complete save reported no change")
	}
	if err := s.CompleteJob(t.Context(), job.ID, "snap-worker", job.LeaseToken); err != nil {
		t.Fatalf("finish save job: %v", err)
	}
	return out
}

func snapshotFailSave(t *testing.T, s *db.Store, snap db.SavedSnapshot, reason string) db.SavedSnapshot {
	t.Helper()
	job := snapshotClaimSave(t, s, snap)
	return snapshotFailSaveClaim(t, s, snap, reason, job)
}

func snapshotFailSaveClaim(t *testing.T, s *db.Store, snap db.SavedSnapshot, reason string, job db.Job) db.SavedSnapshot {
	t.Helper()
	out, changed, err := s.FailSnapshotSave(t.Context(), db.FailSnapshotSaveParams{
		Owner: snap.OwnerID, SnapshotID: snap.ID, ArtifactID: snap.ArtifactID,
		Generation: snap.Generation, Reason: reason,
		JobID: job.ID, Worker: "snap-worker", Token: job.LeaseToken,
	})
	if err != nil || !changed {
		t.Fatalf("fail save: %v %v", err, changed)
	}
	if err := s.FailJob(t.Context(), job.ID, "snap-worker", job.LeaseToken, reason); err != nil {
		t.Fatalf("finish failed save job: %v", err)
	}
	return out
}

// snapshotFinishJobs marks queued/running snapshot jobs terminal so a
// rollback preflight is testing the state under test, not test leftovers.
func snapshotFinishJobs(t *testing.T, s *db.Store) {
	t.Helper()
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE jobs SET state = 'done'
		WHERE kind IN ('snapshot.save', 'snapshot.delete')
		  AND state IN ('queued', 'running')`); err != nil {
		t.Fatalf("finish snapshot jobs: %v", err)
	}
}

func TestSnapshotArtifactAndNameShape(t *testing.T) {
	for range 20 {
		id, err := db.NewSnapshotArtifactID()
		if err != nil {
			t.Fatal(err)
		}
		if !db.ValidSnapshotArtifactID(id) || len(id) != 37 {
			t.Fatalf("artifact %q", id)
		}
	}
	for _, name := range []string{"a", "web-stack", "0", strings.Repeat("a", 63)} {
		if !db.ValidSnapshotName(name) {
			t.Errorf("name %q rejected", name)
		}
	}
	for _, name := range []string{"", "-a", "A", "a_b", strings.Repeat("a", 64), "a/b"} {
		if db.ValidSnapshotName(name) {
			t.Errorf("name %q accepted", name)
		}
	}
}

func TestAcceptSnapshotSaveValidation(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)

	if _, err := s.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: own, SandboxID: source.ID, Name: "Bad_Name",
	}); !errors.Is(err, db.ErrInvalidSnapshotName) {
		t.Fatalf("bad name: %v", err)
	}

	provisioning := create(t, s, db.CreateSandboxParams{Owner: own, NodeID: nodeID})
	if _, err := s.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: own, SandboxID: provisioning.Sandbox.ID, Name: "early",
	}); !errors.Is(err, db.ErrSnapshotSourceNotReady) {
		t.Fatalf("provisioning source: %v", err)
	}

	other := owner(t, s)
	if _, err := s.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: other, SandboxID: source.ID, Name: "foreign",
	}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign source: %v", err)
	}

	snap := snapshotAcceptSave(t, s, own, source.ID, "base")
	if snap.Status != db.SnapshotSaving || snap.Generation != 1 || !snap.IsCurrent {
		t.Fatalf("accepted row: %+v", snap)
	}
	if !db.ValidSnapshotArtifactID(snap.ArtifactID) || snap.NodeID == nil || *snap.NodeID != nodeID {
		t.Fatalf("artifact/node: %+v", snap)
	}
	if snap.MachineType != source.MachineType {
		t.Fatalf("machine type %q, want %q", snap.MachineType, source.MachineType)
	}
	if snap.DiskGiB != source.Reserved.DiskGiB {
		t.Fatalf("disk %d, want %d", snap.DiskGiB, source.Reserved.DiskGiB)
	}
	if snap.Pins != 0 {
		t.Fatalf("pins = %d", snap.Pins)
	}

	var payload []byte
	var jobSnapshot uuid.UUID
	if err := s.Pool().QueryRow(t.Context(), `
		SELECT payload, snapshot_id FROM jobs
		WHERE owner_id = $1 AND kind = 'snapshot.save' AND snapshot_id = $2`,
		own, snap.ID).Scan(&payload, &jobSnapshot); err != nil {
		t.Fatalf("save job: %v", err)
	}
	var p db.SaveSnapshotPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		t.Fatalf("save payload: %v", err)
	}
	if p.SnapshotID != snap.ID || p.ArtifactID != snap.ArtifactID || p.Generation != 1 ||
		p.NodeID != nodeID || p.VMID != source.ID {
		t.Fatalf("save payload: %+v", p)
	}
	if jobSnapshot != snap.ID {
		t.Fatalf("job snapshot_id %s, want %s", jobSnapshot, snap.ID)
	}

	got, err := s.SavedSnapshotByName(t.Context(), own, "base")
	if err != nil || got.ID != snap.ID {
		t.Fatalf("by name: %v %+v", err, got)
	}
	if _, err := s.SavedSnapshotByName(t.Context(), other, "base"); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign name: %v", err)
	}
}

func TestSnapshotSaveSameNameConflict(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)

	const racers = 6
	results := make([]db.SavedSnapshot, racers)
	errs := make([]error, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range racers {
		wg.Go(func() {
			<-start
			results[i], errs[i] = s.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
				Owner: own, SandboxID: source.ID, Name: "cache",
			})
		})
	}
	close(start)
	wg.Wait()
	var ok, inProgress int
	for i, err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, db.ErrSnapshotSaveInProgress):
			inProgress++
		default:
			t.Fatalf("racer %d: %v", i, err)
		}
	}
	if ok != 1 || inProgress != racers-1 {
		t.Fatalf("ok=%d in_progress=%d", ok, inProgress)
	}
	if n := count(t, s, `SELECT count(*) FROM saved_snapshots WHERE owner_id = $1 AND name = 'cache'`, own); n != 1 {
		t.Fatalf("generations after race: %d", n)
	}

	// A delete while the winner is still saving is refused.
	if _, err := s.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: own, Name: "cache",
	}); !errors.Is(err, db.ErrSnapshotSaveInProgress) {
		t.Fatalf("delete while saving: %v", err)
	}
}

func TestSnapshotReplacementPreservesPriorReady(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)

	first := snapshotAcceptSave(t, s, own, source.ID, "base")
	first = snapshotCompleteSave(t, s, first, 4096)
	if first.Status != db.SnapshotReady || first.ReadyAt == nil {
		t.Fatalf("first ready: %+v", first)
	}

	// A replacement becomes current saving; the old ready generation is
	// still deployable and not yet retired.
	second := snapshotAcceptSave(t, s, own, source.ID, "base")
	if second.Generation != 2 || second.Status != db.SnapshotSaving {
		t.Fatalf("second: %+v", second)
	}
	old, err := s.SavedSnapshot(t.Context(), own, first.ID)
	if err != nil || old.Status != db.SnapshotReady || old.IsCurrent || old.RetiredAt != nil {
		t.Fatalf("old during replacement: %v %+v", err, old)
	}
	src, err := s.SnapshotRestoreSource(t.Context(), own, "base")
	if err != nil || src.RowID != first.ID {
		t.Fatalf("restore during replacement: %v %+v", err, src)
	}
	byName, err := s.SavedSnapshotByName(t.Context(), own, "base")
	if err != nil || byName.ID != second.ID || byName.Status != db.SnapshotSaving {
		t.Fatalf("name during replacement: %v %+v", err, byName)
	}

	// A failed replacement leaves the name failed but the old ready
	// generation deployable.
	failed := snapshotFailSave(t, s, second, "save interrupted")
	if failed.Status != db.SnapshotFailed {
		t.Fatalf("fail second: %+v", failed)
	}
	byName, err = s.SavedSnapshotByName(t.Context(), own, "base")
	if err != nil || byName.Status != db.SnapshotFailed || byName.ErrorReason != "save interrupted" {
		t.Fatalf("name after failure: %v %+v", err, byName)
	}
	src, err = s.SnapshotRestoreSource(t.Context(), own, "base")
	if err != nil || src.RowID != first.ID {
		t.Fatalf("restore after failure: %v %+v", err, src)
	}

	// A successful third generation retires the older ready one and keeps
	// the old artifact until cleanup.
	third := snapshotAcceptSave(t, s, own, source.ID, "base")
	if third.Generation != 3 {
		t.Fatalf("third generation %d", third.Generation)
	}
	third = snapshotCompleteSave(t, s, third, 8192)
	src, err = s.SnapshotRestoreSource(t.Context(), own, "base")
	if err != nil || src.RowID != third.ID {
		t.Fatalf("restore after success: %v %+v", err, src)
	}
	retired, err := s.SavedSnapshot(t.Context(), own, first.ID)
	if err != nil || retired.RetiredAt == nil || retired.IsCurrent {
		t.Fatalf("first not retired: %v %+v", err, retired)
	}
	if n := count(t, s, `SELECT count(*) FROM saved_snapshots
		WHERE owner_id = $1 AND name = 'base' AND deleted_at IS NULL AND is_current`, own); n != 1 {
		t.Fatalf("current rows: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.delete'
		  AND state IN ('queued', 'running')`, own, first.ID); n != 1 {
		t.Fatalf("cleanup jobs for retired first: %d", n)
	}
	if added, err := s.EnqueueSnapshotDelete(t.Context(), own, first.ID); err != nil || added {
		t.Fatalf("duplicate cleanup enqueue: %v %v", added, err)
	}
}

func TestSnapshotStaleCompletionCannotSwitchCurrent(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)

	first := snapshotAcceptSave(t, s, own, source.ID, "race")
	snapshotFailSave(t, s, first, "retry later")
	// Finish the first claim so the second save can be claimed normally.
	snapshotFinishJobs(t, s)
	second := snapshotAcceptSave(t, s, own, source.ID, "race")
	if second.Generation != 2 {
		t.Fatalf("second generation %d", second.Generation)
	}
	// Simulate a delayed worker from the first generation: its row is
	// saving again but is no longer current, and its old job is still a
	// live claim.
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE saved_snapshots SET status = 'saving', is_current = false, retired_at = NULL
		WHERE id = $1`, first.ID); err != nil {
		t.Fatal(err)
	}
	staleJob := snapshotLeaseStaleSave(t, s, first)
	staleSize := int64(1234)
	out, changed, err := s.CompleteSnapshotSave(t.Context(), db.CompleteSnapshotSaveParams{
		Owner: own, SnapshotID: first.ID, ArtifactID: first.ArtifactID,
		Generation: first.Generation, NodeID: *first.NodeID, SizeBytes: &staleSize,
		JobID: staleJob.ID, Worker: "stale-worker", Token: staleJob.LeaseToken,
	})
	if err != nil || !changed {
		t.Fatalf("stale completion: %v %v", err, changed)
	}
	if out.IsCurrent || out.RetiredAt == nil {
		t.Fatalf("stale completion not retired: %+v", out)
	}
	if err := s.CompleteJob(t.Context(), staleJob.ID, "stale-worker", staleJob.LeaseToken); err != nil {
		t.Fatalf("finish stale save job: %v", err)
	}
	cur, err := s.SavedSnapshotByName(t.Context(), own, "race")
	if err != nil || cur.ID != second.ID || cur.Status != db.SnapshotSaving || !cur.IsCurrent {
		t.Fatalf("current after stale completion: %v %+v", err, cur)
	}
}

func TestSnapshotDeletedNameNotResurrected(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)

	snap := snapshotAcceptSave(t, s, own, source.ID, "gone")
	job := snapshotClaimSave(t, s, snap)
	// Simulate deletion accepted while the save RPC was still running.
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE saved_snapshots SET deleted_at = now(), retired_at = now(), is_current = false
		WHERE id = $1`, snap.ID); err != nil {
		t.Fatal(err)
	}
	deletedSize := int64(99)
	out, changed, err := s.CompleteSnapshotSave(t.Context(), db.CompleteSnapshotSaveParams{
		Owner: own, SnapshotID: snap.ID, ArtifactID: snap.ArtifactID,
		Generation: snap.Generation, NodeID: *snap.NodeID, SizeBytes: &deletedSize,
		JobID: job.ID, Worker: "snap-worker", Token: job.LeaseToken,
	})
	if err != nil || !changed {
		t.Fatalf("deleted completion: %v %v", err, changed)
	}
	if out.DeletedAt == nil || out.IsCurrent {
		t.Fatalf("deleted completion resurrected row: %+v", out)
	}
	if _, err := s.SavedSnapshotByName(t.Context(), own, "gone"); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("deleted name visible: %v", err)
	}
}

func TestSnapshotSaveCompletionRequiresLiveClaim(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "claim")
	job := snapshotClaimSave(t, s, snap)
	size := int64(1)

	// Wrong worker, then wrong job id: neither may complete the generation.
	if _, _, err := s.CompleteSnapshotSave(t.Context(), db.CompleteSnapshotSaveParams{
		Owner: own, SnapshotID: snap.ID, ArtifactID: snap.ArtifactID,
		Generation: snap.Generation, NodeID: *snap.NodeID, SizeBytes: &size,
		JobID: job.ID, Worker: "other-worker", Token: job.LeaseToken,
	}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("wrong worker: %v", err)
	}
	otherJob := uuid.NewV7()
	if _, _, err := s.CompleteSnapshotSave(t.Context(), db.CompleteSnapshotSaveParams{
		Owner: own, SnapshotID: snap.ID, ArtifactID: snap.ArtifactID,
		Generation: snap.Generation, NodeID: *snap.NodeID, SizeBytes: &size,
		JobID: otherJob, Worker: "snap-worker", Token: job.LeaseToken,
	}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("foreign job id: %v", err)
	}

	// Expire the claim: the same token must not write.
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE jobs SET leased_until = now() - interval '1 minute' WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CompleteSnapshotSave(t.Context(), db.CompleteSnapshotSaveParams{
		Owner: own, SnapshotID: snap.ID, ArtifactID: snap.ArtifactID,
		Generation: snap.Generation, NodeID: *snap.NodeID, SizeBytes: &size,
		JobID: job.ID, Worker: "snap-worker", Token: job.LeaseToken,
	}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("expired claim: %v", err)
	}
	got, err := s.SavedSnapshot(t.Context(), own, snap.ID)
	if err != nil || got.Status != db.SnapshotSaving {
		t.Fatalf("row changed without claim: %v %+v", err, got)
	}

	// A reclaimed claim with a fresh token may finish it.
	reclaimed, err := s.ClaimJob(t.Context(), "snap-worker", time.Minute)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if reclaimed.ID != job.ID {
		t.Fatalf("reclaim job %s, want %s", reclaimed.ID, job.ID)
	}
	snapshotCompleteSaveClaim(t, s, snap, 77, reclaimed)
}

func TestSnapshotSaveForeignJobCannotCompleteAnotherArtifact(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	first := snapshotAcceptSave(t, s, own, source.ID, "first")
	firstJob := snapshotClaimSave(t, s, first)
	second := snapshotAcceptSave(t, s, own, source.ID, "second")
	secondJob := snapshotClaimSave(t, s, second)

	size := int64(1)
	if _, _, err := s.CompleteSnapshotSave(t.Context(), db.CompleteSnapshotSaveParams{
		Owner: own, SnapshotID: first.ID, ArtifactID: first.ArtifactID,
		Generation: first.Generation, NodeID: *first.NodeID, SizeBytes: &size,
		JobID: secondJob.ID, Worker: "snap-worker", Token: secondJob.LeaseToken,
	}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("foreign artifact job: %v", err)
	}
	snapshotCompleteSaveClaim(t, s, first, 5, firstJob)
	snapshotFailSaveClaim(t, s, second, "test cleanup", secondJob)
}

func TestReconcileSnapshotSaveFailureProvesJobTerminal(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "reconcile")

	params := db.ReconcileSnapshotSaveFailureParams{
		Owner: own, SnapshotID: snap.ID, ArtifactID: snap.ArtifactID,
		Generation: snap.Generation, Reason: "save interrupted",
	}
	// A queued job may be claimed at any moment: the reconciler must not
	// decide it.
	if _, _, err := s.ReconcileSnapshotSaveFailure(t.Context(), params); !errors.Is(err, db.ErrSnapshotJobActive) {
		t.Fatalf("queued job: %v", err)
	}
	job := snapshotClaimSave(t, s, snap)
	if _, _, err := s.ReconcileSnapshotSaveFailure(t.Context(), params); !errors.Is(err, db.ErrSnapshotJobActive) {
		t.Fatalf("running job: %v", err)
	}
	if err := s.FailJob(t.Context(), job.ID, "snap-worker", job.LeaseToken, "node timeout"); err != nil {
		t.Fatalf("fail job: %v", err)
	}
	out, changed, err := s.ReconcileSnapshotSaveFailure(t.Context(), params)
	if err != nil || !changed || out.Status != db.SnapshotFailed {
		t.Fatalf("terminal job reconcile: %v %+v", err, out)
	}

	// A missing job is an orphaned generation and may be failed too.
	orphan := snapshotAcceptSave(t, s, own, source.ID, "orphan")
	if _, err := s.Pool().Exec(t.Context(),
		`DELETE FROM jobs WHERE owner_id = $1 AND snapshot_id = $2`, own, orphan.ID); err != nil {
		t.Fatal(err)
	}
	out, changed, err = s.ReconcileSnapshotSaveFailure(t.Context(), db.ReconcileSnapshotSaveFailureParams{
		Owner: own, SnapshotID: orphan.ID, ArtifactID: orphan.ArtifactID,
		Generation: orphan.Generation, Reason: "save job missing",
	})
	if err != nil || !changed || out.Status != db.SnapshotFailed {
		t.Fatalf("missing job reconcile: %v %+v", err, out)
	}
}

func TestSnapshotSaveFailRequiresLiveClaim(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "fail-claim")
	job := snapshotClaimSave(t, s, snap)
	if _, _, err := s.FailSnapshotSave(t.Context(), db.FailSnapshotSaveParams{
		Owner: own, SnapshotID: snap.ID, ArtifactID: snap.ArtifactID,
		Generation: snap.Generation, Reason: "nope",
		JobID: job.ID, Worker: "snap-worker", Token: uuid.NewV7(),
	}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("foreign fail token: %v", err)
	}
	got, err := s.SavedSnapshot(t.Context(), own, snap.ID)
	if err != nil || got.Status != db.SnapshotSaving {
		t.Fatalf("row failed without claim: %v %+v", err, got)
	}
	snapshotFailSaveClaim(t, s, snap, "test cleanup", job)
}

func TestSnapshotDeleteAndReuse(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)

	snap := snapshotAcceptSave(t, s, own, source.ID, "web")
	snap = snapshotCompleteSave(t, s, snap, 100)
	if _, err := s.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: own, Name: "web",
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.SavedSnapshotByName(t.Context(), own, "web"); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("deleted name: %v", err)
	}
	if _, err := s.SnapshotRestoreSource(t.Context(), own, "web"); !errors.Is(err, db.ErrSnapshotNotFound) {
		t.Fatalf("deleted restore source: %v", err)
	}
	if _, err := s.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: own, Name: "web",
	}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.delete'
		  AND state IN ('queued', 'running')`, own, snap.ID); n != 1 {
		t.Fatalf("cleanup job after delete: %d", n)
	}
	// The name is immediately reusable and generations stay monotonic, so a
	// stale job can never alias the new generation.
	again := snapshotAcceptSave(t, s, own, source.ID, "web")
	if again.Generation != snap.Generation+1 || again.ID == snap.ID {
		t.Fatalf("reused name: %+v", again)
	}
}

func TestSnapshotRestorePinLifecycle(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)

	snap := snapshotAcceptSave(t, s, own, source.ID, "base")
	snap = snapshotCompleteSave(t, s, snap, 555)
	src, err := s.SnapshotRestoreSource(t.Context(), own, "base")
	if err != nil {
		t.Fatalf("restore source: %v", err)
	}
	if src.ArtifactID != snap.ArtifactID || src.NodeID != nodeID || src.MachineType != source.MachineType {
		t.Fatalf("restore source: %+v", src)
	}

	res, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: nodeID,
		MachineType: source.MachineType, Name: "restored",
		Reserve: db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 51},
		Restore: &src,
	})
	if err != nil {
		t.Fatalf("restore create: %v", err)
	}
	if !res.Sandbox.SnapshotPin || res.Sandbox.SnapshotID == nil || *res.Sandbox.SnapshotID != snap.ID ||
		res.Sandbox.FromSnapshotID != snap.ArtifactID {
		t.Fatalf("restored sandbox: %+v", res.Sandbox)
	}
	var pinned db.SavedSnapshot
	pinned, err = s.SavedSnapshot(t.Context(), own, snap.ID)
	if err != nil || pinned.Pins != 1 {
		t.Fatalf("pinned artifact: %v %+v", err, pinned)
	}

	var payload []byte
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT payload FROM jobs WHERE id = $1`, res.Job.ID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var pp db.ProvisionPayload
	if err := json.Unmarshal(payload, &pp); err != nil {
		t.Fatalf("provision payload: %v", err)
	}
	if pp.FromSnapshot != snap.ArtifactID || pp.NodeID != nodeID {
		t.Fatalf("provision payload: %+v", pp)
	}

	// Delete the name while the restore pin is held: the artifact is
	// retired but not purgeable.
	if _, err := s.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: own, Name: "base",
	}); err != nil {
		t.Fatalf("delete pinned name: %v", err)
	}
	if _, err := s.SnapshotRestoreSource(t.Context(), own, "base"); !errors.Is(err, db.ErrSnapshotNotFound) {
		t.Fatalf("deleted restore source: %v", err)
	}
	purgeable, _, err := s.PurgeableSnapshots(t.Context(), nil, 10)
	if err != nil {
		t.Fatalf("purgeable: %v", err)
	}
	for _, row := range purgeable {
		if row.ID == snap.ID {
			t.Fatalf("pinned artifact reported purgeable")
		}
	}
	if _, err := s.MarkSnapshotPurged(t.Context(), own, snap.ID, snap.ArtifactID); !errors.Is(err, db.ErrSnapshotNotPurgeable) {
		t.Fatalf("purge pinned: %v", err)
	}

	// A provisioning restore with a live reservation is not confirmed yet.
	if err := s.ReleaseSnapshotPin(t.Context(), own, res.Sandbox.ID); !errors.Is(err, db.ErrSnapshotPinNotReleasable) {
		t.Fatalf("premature release: %v", err)
	}
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE sandboxes SET state = 'ready' WHERE id = $1`, res.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseSnapshotPin(t.Context(), own, res.Sandbox.ID); err != nil {
		t.Fatalf("release pin: %v", err)
	}
	got, err := s.Sandbox(t.Context(), own, res.Sandbox.ID)
	if err != nil || got.SnapshotPin {
		t.Fatalf("pin flag after release: %v %+v", err, got)
	}
	pinned, err = s.SavedSnapshot(t.Context(), own, snap.ID)
	if err != nil || pinned.Pins != 0 {
		t.Fatalf("pins after release: %v %+v", err, pinned)
	}
	if err := s.ReleaseSnapshotPin(t.Context(), own, res.Sandbox.ID); err != nil {
		t.Fatalf("second release: %v", err)
	}
	purgeable, _, err = s.PurgeableSnapshots(t.Context(), nil, 10)
	if err != nil {
		t.Fatalf("purgeable after release: %v", err)
	}
	found := false
	for _, row := range purgeable {
		found = found || row.ID == snap.ID
	}
	if !found {
		t.Fatalf("released artifact not purgeable")
	}
	if _, err := s.MarkSnapshotPurged(t.Context(), own, snap.ID, snap.ArtifactID); err != nil {
		t.Fatalf("mark purged: %v", err)
	}
	if _, err := s.MarkSnapshotPurged(t.Context(), own, snap.ID, snap.ArtifactID); err != nil {
		t.Fatalf("idempotent mark purged: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM saved_snapshots WHERE id = $1 AND purged_at IS NOT NULL`, snap.ID); n != 1 {
		t.Fatalf("purged row: %d", n)
	}
}

func TestSnapshotRestorePinRejectsWrongOwnerOrNode(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "base")
	snap = snapshotCompleteSave(t, s, snap, 1)
	src, err := s.SnapshotRestoreSource(t.Context(), own, "base")
	if err != nil {
		t.Fatal(err)
	}

	other := owner(t, s)
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: other, NodeID: nodeID, Name: "foreign",
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 51}, Restore: &src,
	}); !errors.Is(err, db.ErrSnapshotNotFound) {
		t.Fatalf("foreign restore: %v", err)
	}
	node(t, s, "node-b", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 200})
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: "node-b", Name: "wrong-node",
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 51}, Restore: &src,
	}); !errors.Is(err, db.ErrSnapshotNotFound) {
		t.Fatalf("wrong node restore: %v", err)
	}
	got, err := s.SavedSnapshot(t.Context(), own, snap.ID)
	if err != nil || got.Pins != 0 {
		t.Fatalf("failed restores pinned: %v %+v", err, got)
	}
}

func TestSnapshotPinMismatchIsNotClamped(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "base")
	snap = snapshotCompleteSave(t, s, snap, 1)
	res := create(t, s, db.CreateSandboxParams{Owner: own, NodeID: nodeID})
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET snapshot_id = $2, snapshot_pin = true, state = 'ready' WHERE id = $1`,
		res.Sandbox.ID, snap.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseSnapshotPin(t.Context(), own, res.Sandbox.ID); !errors.Is(err, db.ErrSnapshotPinMismatch) {
		t.Fatalf("mismatched pin release: %v", err)
	}
}

func TestSnapshotRestoreAfterSourceSandboxDeleted(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "base")
	snap = snapshotCompleteSave(t, s, snap, 1)

	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET deleted_at = now(), state = 'cancelled' WHERE id = $1`,
		source.ID); err != nil {
		t.Fatal(err)
	}
	src, err := s.SnapshotRestoreSource(t.Context(), own, "base")
	if err != nil || src.RowID != snap.ID || src.NodeID != nodeID {
		t.Fatalf("restore source after source deletion: %v %+v", err, src)
	}
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: nodeID, Name: "restored",
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 51}, Restore: &src,
	}); err != nil {
		t.Fatalf("restore create: %v", err)
	}
}

func TestSnapshotCleanupNeverGuessesUnknownNode(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "base")
	snap = snapshotCompleteSave(t, s, snap, 1)
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE saved_snapshots SET retired_at = now(), node_id = NULL WHERE id = $1`, snap.ID); err != nil {
		t.Fatal(err)
	}
	purgeable, _, err := s.PurgeableSnapshots(t.Context(), nil, 10)
	if err != nil {
		t.Fatalf("purgeable: %v", err)
	}
	for _, row := range purgeable {
		if row.ID == snap.ID {
			t.Fatalf("node-less artifact reported purgeable")
		}
	}
	if added, err := s.EnqueueSnapshotDelete(t.Context(), own, snap.ID); err != nil || added {
		t.Fatalf("node-less cleanup enqueue: %v %v", added, err)
	}
	if _, err := s.MarkSnapshotPurged(t.Context(), own, snap.ID, snap.ArtifactID); !errors.Is(err, db.ErrSnapshotNotPurgeable) {
		t.Fatalf("node-less purge: %v", err)
	}
}

func TestSnapshotCleanupEnqueueRace(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "race-cleanup")
	snap = snapshotCompleteSave(t, s, snap, 1)
	if _, err := s.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: own, Name: "race-cleanup",
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Clear the deletion's own job so the race starts from no active job.
	snapshotFinishJobs(t, s)

	const racers = 8
	added := make([]bool, racers)
	errs := make([]error, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range racers {
		wg.Go(func() {
			<-start
			added[i], errs[i] = s.EnqueueSnapshotDelete(t.Context(), own, snap.ID)
		})
	}
	close(start)
	wg.Wait()
	wins := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("racer %d: %v", i, err)
		}
		if added[i] {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("cleanup enqueues = %d, want 1", wins)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.delete'
		  AND state IN ('queued', 'running')`, own, snap.ID); n != 1 {
		t.Fatalf("active cleanup jobs = %d, want 1", n)
	}
}

func TestReleaseEligibleSnapshotPins(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "base")
	snap = snapshotCompleteSave(t, s, snap, 1)
	src, err := s.SnapshotRestoreSource(t.Context(), own, "base")
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: nodeID, Name: "restored",
		MachineType: source.MachineType,
		Reserve:     db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 51},
		Restore:     &src,
	})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	// Provisioning with a live reservation is not proof of a completed
	// boot, so the pin must stay.
	if n, _, err := s.ReleaseEligibleSnapshotPins(t.Context(), nil, 10); err != nil || n != 0 {
		t.Fatalf("premature eligible release: %v %d", n, err)
	}
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE sandboxes SET state = 'ready' WHERE id = $1`, res.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	if n, _, err := s.ReleaseEligibleSnapshotPins(t.Context(), nil, 10); err != nil || n != 1 {
		t.Fatalf("ready eligible release: %v %d", n, err)
	}
	got, err := s.SavedSnapshot(t.Context(), own, snap.ID)
	if err != nil || got.Pins != 0 {
		t.Fatalf("pins after eligible release: %v %+v", err, got)
	}
}

func TestSnapshotRestoreSourceIdentityAndDisk(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	// Deliberately raise capacity: this test keeps several successful restore
	// fixtures alive at once (source 2048 MiB + grow 4096 + equal 4096).
	node(t, s, "node-a", db.Resources{VCPUs: 32, MemoryMiB: 65536, DiskGiB: 500})
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "identity")
	snapshotCompleteSave(t, s, snap, 1)
	src, err := s.SnapshotRestoreSource(t.Context(), own, "identity")
	if err != nil {
		t.Fatal(err)
	}
	if src.DiskGiB != source.Reserved.DiskGiB {
		t.Fatalf("source disk %d, want %d", src.DiskGiB, source.Reserved.DiskGiB)
	}

	// Every immutable field is part of the admission decision.
	tampered := []struct {
		name   string
		mutate func(*db.SnapshotSource)
	}{
		{"owner", func(x *db.SnapshotSource) { x.OwnerID = uuid.NewV7() }},
		{"name", func(x *db.SnapshotSource) { x.Name = "other" }},
		{"sandbox", func(x *db.SnapshotSource) { x.SourceSandboxID = "bx_other" }},
		{"generation", func(x *db.SnapshotSource) { x.Generation++ }},
		{"machine", func(x *db.SnapshotSource) { x.MachineType = "large" }},
		{"disk", func(x *db.SnapshotSource) { x.DiskGiB++ }},
		{"node", func(x *db.SnapshotSource) { x.NodeID = "node-b" }},
	}
	for _, tc := range tampered {
		bad := src
		tc.mutate(&bad)
		if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
			ID: ids.Sandbox(), Owner: own, NodeID: nodeID, Name: tc.name,
			MachineType: source.MachineType,
			Reserve:     db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 51},
			Restore:     &bad,
		}); !errors.Is(err, db.ErrSnapshotNotFound) {
			t.Fatalf("tampered %s: %v", tc.name, err)
		}
	}

	// Disk is grow-only; CPU/RAM are not restricted to the snapshot type.
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: nodeID, Name: "small",
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 25},
		Restore: &src,
	}); !errors.Is(err, db.ErrSnapshotDiskTooSmall) {
		t.Fatalf("small disk: %v", err)
	}
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: nodeID, Name: "big",
		MachineType: "large",
		Reserve:     db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 60},
		Restore:     &src,
	}); err != nil {
		t.Fatalf("larger disk and cpu/ram: %v", err)
	}
	// Equal disk permits a CPU/RAM resize (default -> small here).
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: nodeID, Name: "equal",
		MachineType: "small",
		Reserve:     db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51},
		Restore:     &src,
	}); err != nil {
		t.Fatalf("equal disk cpu/ram resize: %v", err)
	}
}

func TestSnapshotPurgeCursorWalksPastBlockedPage(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	const total = 205
	for i := range total {
		if _, err := s.Pool().Exec(t.Context(), `
			INSERT INTO saved_snapshots
				(id, owner_id, name, sandbox_id, status, artifact_id, generation,
				 machine_type, node_id, is_current, retired_at, updated_at)
			VALUES ($1, $2, $3, $4, 'ready', $5, 1, 'default', $6, false, now(),
				now() - make_interval(secs => $7))`,
			uuid.NewV7(), own, fmt.Sprintf("blocked%03d", i), source.ID,
			fmt.Sprintf("snap-%032x", 1000+i), nodeID, total-i); err != nil {
			t.Fatalf("insert purgeable %d: %v", i, err)
		}
	}
	// The first page already has an active cleanup job for every row.
	for i := range 100 {
		row, ok, err := s.SavedSnapshotByArtifact(t.Context(), fmt.Sprintf("snap-%032x", 1000+i))
		if err != nil || !ok {
			t.Fatalf("lookup blocked %d: %v %v", i, ok, err)
		}
		if added, err := s.EnqueueSnapshotDelete(t.Context(), own, row.ID); err != nil || !added {
			t.Fatalf("prime blocked %d: %v %v", i, added, err)
		}
	}

	var (
		seen  int
		after *db.SnapshotCleanupCursor
	)
	for range 5 {
		page, next, err := s.PurgeableSnapshots(t.Context(), after, 100)
		if err != nil {
			t.Fatalf("purge page: %v", err)
		}
		seen += len(page)
		for _, row := range page {
			if row.Name == "blocked000" || row.Name == "blocked099" {
				if added, err := s.EnqueueSnapshotDelete(t.Context(), own, row.ID); err != nil || added {
					t.Fatalf("blocked row re-enqueued: %v %v", added, err)
				}
			}
		}
		if after == nil && len(page) > 0 {
			// First page: the oldest rows are the blocked batch.
			for _, row := range page {
				if added, err := s.EnqueueSnapshotDelete(t.Context(), own, row.ID); err != nil || added {
					t.Fatalf("first page not blocked: %v %v", added, err)
				}
			}
		}
		if after != nil && len(page) > 0 {
			if added, err := s.EnqueueSnapshotDelete(t.Context(), own, page[0].ID); err != nil || !added {
				t.Fatalf("second page progress: %v %v", added, err)
			}
		}
		after = next
		if after == nil {
			break
		}
	}
	if seen != total {
		t.Fatalf("cursor saw %d purgeable rows, want %d", seen, total)
	}
}

func TestSnapshotPinCursorWalksPastIntegrityFailure(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	bad := snapshotAcceptSave(t, s, own, source.ID, "bad-pin")
	snapshotCompleteSave(t, s, bad, 1)
	good := snapshotAcceptSave(t, s, own, source.ID, "pins")
	good = snapshotCompleteSave(t, s, good, 1)
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE saved_snapshots SET pins = 204 WHERE id = $1`, good.ID); err != nil {
		t.Fatal(err)
	}
	const total = 205
	for i := range total {
		snapshotID := good.ID
		if i == 0 {
			// A flag without any counter is an integrity failure; it must
			// be skipped, never clamped, and must not starve rows 100+.
			snapshotID = bad.ID
		}
		if _, err := s.Pool().Exec(t.Context(), `
			INSERT INTO sandboxes
				(id, owner_id, state, machine_type, name, node_id,
				 snapshot_id, snapshot_pin, created_at, updated_at)
			VALUES ($1, $2, 'ready', 'default', $3, $4, $5, true, now(),
				now() - make_interval(secs => $6))`,
			fmt.Sprintf("bx_pin_%03d", i), own, fmt.Sprintf("pin-%03d", i), nodeID,
			snapshotID, total-i); err != nil {
			t.Fatalf("insert pinned sandbox %d: %v", i, err)
		}
	}
	var (
		released int
		after    *db.SnapshotPinCursor
		pages    int
	)
	for range 5 {
		n, next, err := s.ReleaseEligibleSnapshotPins(t.Context(), after, 100)
		if err != nil {
			t.Fatalf("pin page: %v", err)
		}
		released += n
		pages++
		after = next
		if after == nil {
			break
		}
	}
	if pages < 2 {
		t.Fatalf("pin cursor did not advance: pages=%d", pages)
	}
	if released != total-1 {
		t.Fatalf("released %d pins, want %d", released, total-1)
	}
	got, err := s.SavedSnapshot(t.Context(), own, good.ID)
	if err != nil || got.Pins != 0 {
		t.Fatalf("pins after walks: %v %+v", err, got)
	}
	var mismatchPinned bool
	if err := s.Pool().QueryRow(t.Context(), `
		SELECT snapshot_pin FROM sandboxes WHERE id = 'bx_pin_000'`).Scan(&mismatchPinned); err != nil {
		t.Fatal(err)
	}
	if !mismatchPinned {
		t.Fatal("integrity-failed pin was silently cleared")
	}
}

func TestSnapshotCleanupReopensReappearedArtifact(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "reappear")
	snap = snapshotCompleteSave(t, s, snap, 1)
	if _, err := s.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: own, Name: "reappear",
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	snapshotFinishJobs(t, s)
	if _, err := s.MarkSnapshotPurged(t.Context(), own, snap.ID, snap.ArtifactID); err != nil {
		t.Fatalf("mark purged: %v", err)
	}

	// The node still lists the artifact (for example a late SaveSnapshot
	// finished after cleanup). The inventory reconciler walks terminal rows,
	// including purged ones, and must be able to re-arm cleanup.
	rows, next, err := s.CleanupArtifactsOnNode(t.Context(), nodeID, nil, 10)
	if err != nil {
		t.Fatalf("cleanup artifacts: %v", err)
	}
	if next != nil {
		t.Fatalf("unexpected cleanup cursor")
	}
	found := false
	for _, row := range rows {
		found = found || row.ID == snap.ID
	}
	if !found {
		t.Fatal("purged artifact missing from node cleanup walk")
	}
	known, ok, err := s.SavedSnapshotByArtifact(t.Context(), snap.ArtifactID)
	if err != nil || !ok || known.ID != snap.ID {
		t.Fatalf("artifact lookup: %v %v %+v", ok, err, known)
	}
	if _, ok, err := s.SavedSnapshotByArtifact(t.Context(), "snap-"+strings.Repeat("0", 32)); err != nil || ok {
		t.Fatalf("unknown artifact accepted: %v %v", ok, err)
	}

	reopened, changed, err := s.ReopenSnapshotCleanup(t.Context(), own, snap.ID, snap.ArtifactID)
	if err != nil || !changed || reopened.PurgedAt != nil {
		t.Fatalf("reopen: %v %v %+v", changed, err, reopened)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.delete'
		  AND state IN ('queued', 'running')`, own, snap.ID); n != 1 {
		t.Fatalf("reopened cleanup jobs: %d", n)
	}
}

func TestSnapshotLegacyLargeDiskSurvivesSourceDeletion(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	// A large restore needs large capacity; snapshotNode is intentionally
	// small for the other tests.
	node(t, s, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 65536, DiskGiB: 500})
	own := owner(t, s)
	source := create(t, s, db.CreateSandboxParams{
		Owner: own, NodeID: nodeID, MachineType: "large",
		Reserve: db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 111},
	})
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE sandboxes SET state = 'ready' WHERE id = $1`,
		source.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	artifact := "snap-" + strings.Repeat("e", 32)
	snapID := uuid.NewV7()
	if _, err := s.Pool().Exec(t.Context(), `
		INSERT INTO saved_snapshots
			(id, owner_id, name, sandbox_id, status, artifact_id, generation,
			 machine_type, disk_gib, node_id, is_current, ready_at)
		VALUES ($1, $2, 'legacy-large', $3, 'ready', $4, 1, 'large', 0, $5, true, now())`,
		snapID, own, source.Sandbox.ID, artifact, nodeID); err != nil {
		t.Fatalf("insert legacy snapshot: %v", err)
	}
	// The source was deleted and its reservation released, as happens to a
	// legacy row before the new migration runs.
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET deleted_at = now(), state = 'cancelled',
			reserved_vcpus = 0, reserved_memory_mib = 0, reserved_disk_gib = 0
		WHERE id = $1`, source.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	if err := snapshotMigrateDownTo(t, s, 2); err != nil {
		t.Fatalf("down: %v", err)
	}
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatalf("re-migrate: %v", err)
	}
	row, err := s.SavedSnapshot(t.Context(), own, snapID)
	if err != nil || row.DiskGiB != 111 {
		t.Fatalf("legacy disk backfill: %v %+v", err, row)
	}
	src, err := s.SnapshotRestoreSource(t.Context(), own, "legacy-large")
	if err != nil || src.DiskGiB != 111 {
		t.Fatalf("legacy restore source: %v %+v", err, src)
	}
	// A default/small 51 GiB target must not be accepted for the 111 GiB
	// snapshot just because the source reservation is gone.
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: nodeID, Name: "shrink",
		MachineType: "default",
		Reserve:     db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51},
		Restore:     &src,
	}); !errors.Is(err, db.ErrSnapshotDiskTooSmall) {
		t.Fatalf("legacy shrink: %v", err)
	}
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: nodeID, Name: "equal",
		MachineType: "large",
		Reserve:     db.Resources{VCPUs: 8, MemoryMiB: 16384, DiskGiB: 111},
		Restore:     &src,
	}); err != nil {
		t.Fatalf("legacy equal disk: %v", err)
	}
}

func TestSnapshotRestoreUnknownDiskRejected(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	if _, err := s.Pool().Exec(t.Context(), `
		INSERT INTO saved_snapshots
			(id, owner_id, name, sandbox_id, status, artifact_id, generation,
			 machine_type, disk_gib, node_id, is_current, ready_at)
		VALUES ($1, $2, 'unknown', $3, 'ready', $4, 1, '', 0, $5, true, now())`,
		uuid.NewV7(), own, source.ID, "snap-"+strings.Repeat("f", 32), nodeID); err != nil {
		t.Fatalf("insert unknown disk: %v", err)
	}
	if _, err := s.SnapshotRestoreSource(t.Context(), own, "unknown"); !errors.Is(err, db.ErrSnapshotDiskUnknown) {
		t.Fatalf("unknown disk source: %v", err)
	}
}

func TestSnapshotMigrationDownRefusesPopulatedGenerations(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	first := snapshotAcceptSave(t, s, own, source.ID, "base")
	snapshotCompleteSave(t, s, first, 1)
	second := snapshotAcceptSave(t, s, own, source.ID, "base")
	snapshotCompleteSave(t, s, second, 2)
	snapshotFinishJobs(t, s)

	err := snapshotMigrateDownTo(t, s, 2)
	if err == nil || !strings.Contains(err.Error(), "multiple live generations") {
		t.Fatalf("down with two live generations: %v", err)
	}
	// The preflight runs before mutations: both generations and the new
	// columns must still be there.
	if n := count(t, s, `SELECT count(*) FROM saved_snapshots WHERE owner_id = $1`, own); n != 2 {
		t.Fatalf("generations after refused down: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'saved_snapshots'
		  AND column_name = 'is_current'`); n != 1 {
		t.Fatalf("new column lost after refused down: %d", n)
	}
}

func TestSnapshotMigrationDownRefusesActivePin(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "base")
	snap = snapshotCompleteSave(t, s, snap, 1)
	snapshotFinishJobs(t, s)
	src, err := s.SnapshotRestoreSource(t.Context(), own, "base")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: nodeID, Name: "pinned",
		MachineType: source.MachineType,
		Reserve:     db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 51},
		Restore:     &src,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if err := snapshotMigrateDownTo(t, s, 2); err == nil || !strings.Contains(err.Error(), "restore pin") {
		t.Fatalf("down with active pin: %v", err)
	}
	// The refused Down still rolled the later migrations back before 00003
	// refused; migrate up again before reading through the modern store.
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatalf("migrate up after refused down: %v", err)
	}
	got, err := s.SavedSnapshot(t.Context(), own, snap.ID)
	if err != nil || got.Pins != 1 {
		t.Fatalf("pin lost after refused down: %v %+v", err, got)
	}
}

func TestSnapshotMigrationDownRefusesActiveSnapshotJob(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snapshotAcceptSave(t, s, own, source.ID, "in-flight")
	if err := snapshotMigrateDownTo(t, s, 2); err == nil || !strings.Contains(err.Error(), "snapshot job") {
		t.Fatalf("down with active save job: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'saved_snapshots'
		  AND column_name = 'is_current'`); n != 1 {
		t.Fatalf("new column lost after refused down: %d", n)
	}
}

func TestSnapshotMigrationDownPreservesRepresentableHistory(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, "history")
	snap = snapshotCompleteSave(t, s, snap, 1)
	if _, err := s.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: own, Name: "history",
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	snapshotFinishJobs(t, s)

	if err := snapshotMigrateDownTo(t, s, 2); err != nil {
		t.Fatalf("down representable history: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM saved_snapshots WHERE id = $1 AND deleted_at IS NOT NULL`, snap.ID); n != 1 {
		t.Fatalf("historical row not preserved: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'sandboxes'
		  AND column_name = 'snapshot_pin'`); n != 0 {
		t.Fatalf("sandbox pin column survived down: %d", n)
	}
	// Re-applying the migration must adopt the preserved row safely.
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatalf("re-migrate: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM saved_snapshots WHERE id = $1`, snap.ID); n != 1 {
		t.Fatalf("row lost after re-migrate: %d", n)
	}
}

func TestAllSavedSnapshotsWalksEveryPage(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	source := snapshotReadySource(t, s, own, nodeID)
	const total = 205
	for i := range total {
		if _, err := s.Pool().Exec(t.Context(), `
			INSERT INTO saved_snapshots
				(id, owner_id, name, sandbox_id, status, artifact_id, generation,
				 machine_type, node_id, is_current, ready_at)
			VALUES ($1, $2, $3, $4, 'ready', $5, 1, 'default', $6, true, now())`,
			uuid.NewV7(), own, fmt.Sprintf("n%03d", i), source.ID,
			fmt.Sprintf("snap-%032x", i), nodeID); err != nil {
			t.Fatalf("insert snapshot %d: %v", i, err)
		}
	}
	all, err := s.AllSavedSnapshots(t.Context(), db.SnapshotListQuery{Owner: own})
	if err != nil {
		t.Fatalf("all snapshots: %v", err)
	}
	if len(all) != total {
		t.Fatalf("all snapshots = %d, want %d", len(all), total)
	}
	prefix, err := s.AllSavedSnapshots(t.Context(), db.SnapshotListQuery{Owner: own, NamePrefix: "n1"})
	if err != nil {
		t.Fatalf("prefix snapshots: %v", err)
	}
	if len(prefix) != 100 {
		t.Fatalf("prefix snapshots = %d, want 100", len(prefix))
	}
	exact, err := s.AllSavedSnapshots(t.Context(), db.SnapshotListQuery{Owner: own, Name: "n204"})
	if err != nil || len(exact) != 1 || exact[0].Name != "n204" {
		t.Fatalf("exact snapshot: %v %+v", err, exact)
	}
}
