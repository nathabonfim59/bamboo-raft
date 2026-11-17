package db_test

// Real-Postgres regression for the M1 stopped-restore snapshot-pin gap: a
// restore whose initial boot is stopped reaches archived through the canonical
// stop finalization while its saved-snapshot artifact pin is still held.
// Archived proves StopVM confirmed the VM off with its disk kept, so the disk
// no longer depends on the artifact and the pin may release. Archiving, a live
// initial boot and a live reservation must still keep it. Gated by
// BBRAFT_TEST_DATABASE_URL through dbtest.Open.

import (
	"errors"
	"testing"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// snapshotPinsRestore creates one restored sandbox from a completed saved
// generation named name and returns it with its artifact row. The returned
// row is provisioning with a live reservation and an exactly-once pin.
func snapshotPinsRestore(t *testing.T, s *db.Store, own uuid.UUID, nodeID, name string) (db.Sandbox, db.SavedSnapshot) {
	t.Helper()
	source := snapshotReadySource(t, s, own, nodeID)
	snap := snapshotAcceptSave(t, s, own, source.ID, name)
	snap = snapshotCompleteSave(t, s, snap, 1)
	src, err := s.SnapshotRestoreSource(t.Context(), own, name)
	if err != nil {
		t.Fatalf("restore source: %v", err)
	}
	res, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID:          ids.Sandbox(),
		Owner:       own,
		NodeID:      nodeID,
		Name:        "restored",
		MachineType: source.MachineType,
		Reserve:     db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: source.Reserved.DiskGiB},
		Restore:     &src,
	})
	if err != nil {
		t.Fatalf("restore create: %v", err)
	}
	if !res.Sandbox.SnapshotPin || res.Sandbox.SnapshotID == nil || *res.Sandbox.SnapshotID != snap.ID {
		t.Fatalf("restore is not pinned: %+v", res.Sandbox)
	}
	return res.Sandbox, snap
}

// snapshotPinsCheck asserts the sandbox flag and artifact counter together,
// because exactly-once release is the pair, never one side alone.
func snapshotPinsCheck(t *testing.T, s *db.Store, own uuid.UUID, sandboxID string, snap db.SavedSnapshot, wantPin bool, wantPins int) {
	t.Helper()
	sbx, err := s.Sandbox(t.Context(), own, sandboxID)
	if err != nil {
		t.Fatalf("read pinned sandbox: %v", err)
	}
	if sbx.SnapshotPin != wantPin {
		t.Fatalf("sandbox snapshot_pin = %v, want %v", sbx.SnapshotPin, wantPin)
	}
	row, err := s.SavedSnapshot(t.Context(), own, snap.ID)
	if err != nil {
		t.Fatalf("read pinned artifact: %v", err)
	}
	if row.Pins != wantPins {
		t.Fatalf("artifact pins = %d, want %d", row.Pins, wantPins)
	}
}

func TestStoppedRestorePinReleasesAfterArchivedFinalize(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	sbx, snap := snapshotPinsRestore(t, s, own, nodeID, "base")
	snapshotPinsCheck(t, s, own, sbx.ID, snap, true, 1)

	// The stop accepted mid-boot keeps the archiving row and its full
	// reservation: no outcome is confirmed yet, so neither the direct call
	// nor the reconciler scan may release the artifact.
	op, archiving, _, created, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil || !created || archiving.State != db.StateArchiving {
		t.Fatalf("accept stop: created=%v state=%s err=%v", created, archiving.State, err)
	}
	if err := s.ReleaseSnapshotPin(t.Context(), own, sbx.ID); !errors.Is(err, db.ErrSnapshotPinNotReleasable) {
		t.Fatalf("archiving release: %v, want ErrSnapshotPinNotReleasable", err)
	}
	if n, _, err := s.ReleaseEligibleSnapshotPins(t.Context(), nil, 10); err != nil || n != 0 {
		t.Fatalf("archiving eligible scan: n=%d err=%v, want 0", n, err)
	}
	snapshotPinsCheck(t, s, own, sbx.ID, snap, true, 1)

	// Delete the saved name while the restore is still archiving: the artifact
	// is retired but must stay pinned and unpurgeable until the outcome is
	// confirmed.
	if _, err := s.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: own, Name: "base",
	}); err != nil {
		t.Fatalf("delete pinned name: %v", err)
	}
	if page, _, err := s.PurgeableSnapshots(t.Context(), nil, 10); err != nil {
		t.Fatalf("purgeable while archiving: %v", err)
	} else {
		for _, row := range page {
			if row.ID == snap.ID {
				t.Fatal("archiving restore's artifact reported purgeable")
			}
		}
	}

	// Finalize through the real stop claim: the node confirmed the VM off,
	// only compute is released and the retained disk stays reserved.
	job := phase1ClaimKind(t, s, "restore-pin-worker", db.JobStop)
	archived, final, err := s.FinalizeSandboxStop(t.Context(), job.ID, "restore-pin-worker", job.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
	})
	if err != nil {
		t.Fatalf("finalize stop: %v", err)
	}
	if archived.State != db.StateArchived || final.Status != db.StopCompleted {
		t.Fatalf("archived=%+v operation=%+v", archived, final)
	}
	if archived.Reserved.VCPUs != 0 || archived.Reserved.MemoryMiB != 0 || archived.Reserved.DiskGiB != sbx.Reserved.DiskGiB {
		t.Fatalf("archived reservation = %+v, want compute released and disk %d kept", archived.Reserved, sbx.Reserved.DiskGiB)
	}
	// The pin is still held here: this is the gap the regression fixes. The
	// reconciler must observe the confirmed archived outcome.
	snapshotPinsCheck(t, s, own, sbx.ID, snap, true, 1)

	if n, _, err := s.ReleaseEligibleSnapshotPins(t.Context(), nil, 10); err != nil || n != 1 {
		t.Fatalf("archived eligible scan: n=%d err=%v, want 1", n, err)
	}
	snapshotPinsCheck(t, s, own, sbx.ID, snap, false, 0)

	// Artifact cleanup is enqueued exactly once and the retired row becomes
	// purgeable; replaying the release changes neither side.
	if n := count(t, s, `SELECT count(*) FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.delete'
		  AND state IN ('queued', 'running')`, own, snap.ID); n != 1 {
		t.Fatalf("active cleanup jobs = %d, want 1", n)
	}
	if page, _, err := s.PurgeableSnapshots(t.Context(), nil, 10); err != nil {
		t.Fatalf("purgeable after release: %v", err)
	} else {
		found := false
		for _, row := range page {
			found = found || row.ID == snap.ID
		}
		if !found {
			t.Fatal("released artifact is not purgeable")
		}
	}
	if added, err := s.EnqueueSnapshotDelete(t.Context(), own, snap.ID); err != nil || added {
		t.Fatalf("duplicate cleanup enqueue: added=%v err=%v", added, err)
	}
	if err := s.ReleaseSnapshotPin(t.Context(), own, sbx.ID); err != nil {
		t.Fatalf("replayed release: %v", err)
	}
	snapshotPinsCheck(t, s, own, sbx.ID, snap, false, 0)
	if n, _, err := s.ReleaseEligibleSnapshotPins(t.Context(), nil, 10); err != nil || n != 0 {
		t.Fatalf("replayed eligible scan: n=%d err=%v, want 0", n, err)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.delete'
		  AND state IN ('queued', 'running')`, own, snap.ID); n != 1 {
		t.Fatalf("active cleanup jobs after replay = %d, want 1", n)
	}
	if _, err := s.MarkSnapshotPurged(t.Context(), own, snap.ID, snap.ArtifactID); err != nil {
		t.Fatalf("mark purgeable artifact purged: %v", err)
	}

	// Tenant isolation: another owner cannot release this sandbox or observe
	// the artifact through an owner-scoped lookup.
	other := owner(t, s)
	if err := s.ReleaseSnapshotPin(t.Context(), other, sbx.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign pin release: %v, want ErrNotFound", err)
	}
	if _, err := s.SavedSnapshot(t.Context(), other, snap.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign artifact read: %v, want ErrNotFound", err)
	}
}

func TestResumeFailureArchivedRestorePinReleases(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	sbx, snap := snapshotPinsRestore(t, s, own, nodeID, "base")

	// Stop and finalize the initial restore, but do not run the reconciler
	// yet: the pin is still held when the sandbox is resumed.
	op, archiving, _, _, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil {
		t.Fatalf("accept stop: %v", err)
	}
	stopJob := phase1ClaimKind(t, s, "restore-pin-worker", db.JobStop)
	if _, _, err := s.FinalizeSandboxStop(t.Context(), stopJob.ID, "restore-pin-worker", stopJob.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
	}); err != nil {
		t.Fatalf("finalize stop: %v", err)
	}
	snapshotPinsCheck(t, s, own, sbx.ID, snap, true, 1)

	// Resume the retained disk and end it through the designed terminal
	// failure path: StopVM confirmed the disk kept, the row returns to
	// archived at a newer lifecycle generation and only compute is released.
	resumed, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: sbx.ID})
	if err != nil {
		t.Fatalf("accept resume: %v", err)
	}
	if resumed.Sandbox.State != db.StateProvisioning || resumed.Sandbox.LifecycleGeneration != 1 {
		t.Fatalf("resumed sandbox: %+v", resumed.Sandbox)
	}
	resumeJob := phase1ClaimKind(t, s, "resume-pin-worker", db.JobResume)
	failed, err := s.FinalizeSandboxResumeFailure(t.Context(), resumeJob.ID, "resume-pin-worker", resumeJob.LeaseToken,
		db.FinalizeResumeFailureParams{
			Owner: own, SandboxID: sbx.ID,
			Generation: resumed.Sandbox.LifecycleGeneration, Version: resumed.Sandbox.Version,
		}, "boot_failed")
	if err != nil {
		t.Fatalf("finalize resume failure: %v", err)
	}
	if failed.State != db.StateArchived || failed.LifecycleGeneration != 1 || failed.ErrorReason != "boot_failed" {
		t.Fatalf("resume failure outcome: %+v", failed)
	}
	if failed.Reserved.VCPUs != 0 || failed.Reserved.MemoryMiB != 0 || failed.Reserved.DiskGiB != sbx.Reserved.DiskGiB {
		t.Fatalf("resume failure reservation = %+v", failed.Reserved)
	}
	snapshotPinsCheck(t, s, own, sbx.ID, snap, true, 1)

	// Both paths into archived prove the disk is kept, so the same single
	// eligibility rule releases the pin exactly once and keeps the disk.
	if n, _, err := s.ReleaseEligibleSnapshotPins(t.Context(), nil, 10); err != nil || n != 1 {
		t.Fatalf("resume-failure archived eligible scan: n=%d err=%v, want 1", n, err)
	}
	snapshotPinsCheck(t, s, own, sbx.ID, snap, false, 0)
	if n, _, err := s.ReleaseEligibleSnapshotPins(t.Context(), nil, 10); err != nil || n != 0 {
		t.Fatalf("replayed eligible scan: n=%d err=%v, want 0", n, err)
	}
	if got := nodeReserved(t, s, "node-a"); got.DiskGiB != 2*sbx.Reserved.DiskGiB {
		t.Fatalf("disk reservation after release = %+v", got)
	}
}

func TestMissingDiskStopRestorePinStaysEligible(t *testing.T) {
	s := dbtestOpen(t)
	nodeID := snapshotNode(t, s)
	own := owner(t, s)
	sbx, snap := snapshotPinsRestore(t, s, own, nodeID, "base")

	// Archiving with the reservation still held is never eligible.
	op, archiving, _, _, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil {
		t.Fatalf("accept stop: %v", err)
	}
	if n, _, err := s.ReleaseEligibleSnapshotPins(t.Context(), nil, 10); err != nil || n != 0 {
		t.Fatalf("archiving eligible scan: n=%d err=%v, want 0", n, err)
	}

	// The confirmed missing disk is the other existing proof: DeleteVM fenced
	// any earlier boot and one transaction released every reservation, so the
	// zero-reservation rule still makes the artifact safe to release.
	job := phase1ClaimKind(t, s, "restore-pin-worker", db.JobStop)
	failed, final, applied, err := s.FinalizeSandboxStopMissingDisk(t.Context(), job.ID, "restore-pin-worker", job.LeaseToken,
		db.FinalizeStopParams{
			Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
		})
	if err != nil || !applied {
		t.Fatalf("finalize missing disk: applied=%v err=%v", applied, err)
	}
	if failed.State != db.StateError || failed.ErrorReason != db.MissingDiskStopError ||
		failed.Reserved != (db.Resources{}) || final.Status != db.StopFailing {
		t.Fatalf("missing disk outcome: sandbox=%+v operation=%+v", failed, final)
	}
	if n, _, err := s.ReleaseEligibleSnapshotPins(t.Context(), nil, 10); err != nil || n != 1 {
		t.Fatalf("missing-disk eligible scan: n=%d err=%v, want 1", n, err)
	}
	snapshotPinsCheck(t, s, own, sbx.ID, snap, false, 0)
}
