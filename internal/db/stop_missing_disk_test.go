package db_test

// Real-Postgres tests for the terminal missing-disk stop outcome (B1.2):
// after the worker's DeleteVM fence, one transaction fails the sandbox with
// the fixed vm_missing classification and releases every remaining
// reservation exactly once, replays change nothing, newer intents fail
// closed, and the row stays queryable with a working normal DELETE. Gated by
// BBRAFT_TEST_DATABASE_URL through dbtest.Open.

import (
	"errors"
	"testing"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// phase1ArchivingStop accepts a stop for a ready sandbox and claims its job.
func phase1ArchivingStop(t *testing.T, s *db.Store, own uuid.UUID, sbx db.Sandbox, worker string) (db.Sandbox, db.StopOperation, db.Job) {
	t.Helper()
	op, archiving, _, created, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil || !created {
		t.Fatalf("accept stop: created=%v err=%v", created, err)
	}
	job := phase1ClaimKind(t, s, worker, db.JobStop)
	return archiving, op, job
}

func TestFinalizeSandboxStopMissingDiskFailsOnceAndStaysQueryable(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	full := sbx.Reserved

	archiving, op, job := phase1ArchivingStop(t, s, own, sbx, "stop-worker")
	if got := nodeReserved(t, s, "node-a"); got != full {
		t.Fatalf("reservation before finalize: %+v", got)
	}

	p := db.FinalizeStopParams{Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version}
	cur, final, applied, err := s.FinalizeSandboxStopMissingDisk(t.Context(), job.ID, "stop-worker", job.LeaseToken, p)
	if err != nil {
		t.Fatalf("finalize missing disk: %v", err)
	}
	if !applied {
		t.Fatal("first finalize was not applied")
	}
	if cur.State != db.StateError || cur.ErrorReason != db.MissingDiskStopError {
		t.Fatalf("sandbox after missing disk: state=%s reason=%q", cur.State, cur.ErrorReason)
	}
	if cur.Reserved != (db.Resources{}) {
		t.Fatalf("reservation not released: %+v", cur.Reserved)
	}
	if final.Status != db.StopFailing || final.Error != db.MissingDiskStopError || final.EndedAt != nil {
		t.Fatalf("operation after missing disk: %+v", final)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{}) {
		t.Fatalf("node after missing disk: %+v", got)
	}

	// GET data stays queryable: the owner-visible read still answers the
	// error row with its stop metadata.
	visible, err := s.Sandbox(t.Context(), own, sbx.ID)
	if err != nil {
		t.Fatalf("owner-visible read after outcome: %v", err)
	}
	if visible.State != db.StateError || visible.StopOperationID == nil || *visible.StopOperationID != op.ID {
		t.Fatalf("visible row after outcome: %+v", visible)
	}
	if latest, err := s.LatestSandboxStop(t.Context(), own, sbx.ID); err != nil || latest.ID != op.ID ||
		latest.Status != db.StopFailing || latest.Error != db.MissingDiskStopError {
		t.Fatalf("latest stop after outcome: %+v (%v)", latest, err)
	}

	// Replay under the still-live claim: applied=false, nothing changes, and
	// the node counters stay zero rather than going negative.
	_, _, applied, err = s.FinalizeSandboxStopMissingDisk(t.Context(), job.ID, "stop-worker", job.LeaseToken, p)
	if err != nil {
		t.Fatalf("replay finalize: %v", err)
	}
	if applied {
		t.Fatal("replay was applied twice")
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{}) {
		t.Fatalf("node after replay: %+v", got)
	}

	// The replay marker must be the COMPLETE canonical outcome: an error row
	// whose current stop pointer moved elsewhere fails closed instead of
	// replaying, and so does a marker row that somehow still holds a
	// reservation.
	if _, err := s.Pool().Exec(t.Context(), `
		INSERT INTO stop_operations (id, owner_id, sandbox_id, status, ended_at)
		VALUES ('stop_aaaaaaaaaaaaaaaa', $1, $2, 'completed', now())`, own, sbx.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET stop_operation_id = 'stop_aaaaaaaaaaaaaaaa' WHERE id = $1`, sbx.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := s.FinalizeSandboxStopMissingDisk(t.Context(), job.ID, "stop-worker", job.LeaseToken, p); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("replay with moved stop pointer: %v, want conflict", err)
	}
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET stop_operation_id = $2, reserved_vcpus = 1 WHERE id = $1`,
		sbx.ID, op.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := s.FinalizeSandboxStopMissingDisk(t.Context(), job.ID, "stop-worker", job.LeaseToken, p); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("replay with leftover reservation: %v, want conflict", err)
	}
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET reserved_vcpus = 0 WHERE id = $1`, sbx.ID); err != nil {
		t.Fatal(err)
	}
	var diskLeft int64
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT reserved_disk_gib FROM nodes WHERE id = 'node-a'`).Scan(&diskLeft); err != nil {
		t.Fatal(err)
	}
	if diskLeft != 0 {
		t.Fatalf("node disk counter after replay: %d", diskLeft)
	}

	// A normal DELETE completes afterwards: acceptance supersedes the failing
	// stop and the idempotent release path finds nothing left to free.
	delOp, _, err := s.AcceptSandboxDeletion(t.Context(), own, sbx.ID, db.DeletionExplicit)
	if err != nil {
		t.Fatalf("delete after missing disk: %v", err)
	}
	if finalAfter, err := s.StopOperation(t.Context(), own, sbx.ID, op.ID); err != nil || finalAfter.Status != db.StopSuperseded {
		t.Fatalf("stop after deletion: %+v (%v)", finalAfter, err)
	}
	deleteJob := phase1ClaimKind(t, s, "delete-worker", db.JobDeleteVM)
	if err := s.CompleteJob(t.Context(), deleteJob.ID, "delete-worker", deleteJob.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteDeletion(t.Context(), own, delOp.ID); err != nil {
		t.Fatalf("complete deletion: %v", err)
	}
	if _, err := s.CompleteDeletion(t.Context(), own, delOp.ID); err != nil {
		t.Fatalf("repeat complete deletion: %v", err)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{}) {
		t.Fatalf("node after deletion: %+v", got)
	}
}

func TestFinalizeSandboxStopMissingDiskFences(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	foreign := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	full := sbx.Reserved

	archiving, op, job := phase1ArchivingStop(t, s, own, sbx, "stop-worker")
	p := db.FinalizeStopParams{Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version}

	check := func(err error, want error, what string) {
		t.Helper()
		if !errors.Is(err, want) {
			t.Fatalf("%s: %v, want %v", what, err, want)
		}
		if got := nodeReserved(t, s, "node-a"); got != full {
			t.Fatalf("%s released the reservation: %+v", what, got)
		}
		row, rerr := s.SandboxAny(t.Context(), own, sbx.ID)
		if rerr != nil || row.State != db.StateArchiving {
			t.Fatalf("%s changed the row: %+v (%v)", what, row, rerr)
		}
		latest, lerr := s.LatestSandboxStop(t.Context(), own, sbx.ID)
		if lerr != nil || latest.Error != "" {
			t.Fatalf("%s changed the operation: %+v (%v)", what, latest, lerr)
		}
	}

	check(func() error {
		_, _, _, err := s.FinalizeSandboxStopMissingDisk(t.Context(), job.ID, "stop-worker", uuid.NewV7(), p)
		return err
	}(), db.ErrJobNotLeased, "wrong token")
	check(func() error {
		_, _, _, err := s.FinalizeSandboxStopMissingDisk(t.Context(), job.ID, "other-worker", job.LeaseToken, p)
		return err
	}(), db.ErrJobNotLeased, "wrong worker")
	check(func() error {
		_, _, _, err := s.FinalizeSandboxStopMissingDisk(t.Context(), job.ID, "stop-worker", job.LeaseToken,
			db.FinalizeStopParams{Owner: own, SandboxID: sbx.ID, StopID: "stop_ffffffffffffffff", ExpectedVersion: archiving.Version})
		return err
	}(), db.ErrInvalidJobPayload, "mismatched stop id")
	check(func() error {
		_, _, _, err := s.FinalizeSandboxStopMissingDisk(t.Context(), job.ID, "stop-worker", job.LeaseToken,
			db.FinalizeStopParams{Owner: foreign, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version})
		return err
	}(), db.ErrJobNotLeased, "foreign owner")
	check(func() error {
		_, _, _, err := s.FinalizeSandboxStopMissingDisk(t.Context(), job.ID, "stop-worker", job.LeaseToken,
			db.FinalizeStopParams{Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version + 1})
		return err
	}(), db.ErrConflict, "stale version")

	// A superseded operation (a resume took over the machine mid-stop) must
	// fail closed as superseded, not release anything.
	s2 := dbtestOpen(t)
	own2 := owner(t, s2)
	node(t, s2, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx2 := phase1CreateReady(t, s2, own2, db.CreateSandboxParams{})
	archiving2, op2, job2 := phase1ArchivingStop(t, s2, own2, sbx2, "stop-worker")
	if _, err := s2.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own2, SandboxID: sbx2.ID, SupersedeStopID: op2.ID,
	}); err != nil {
		t.Fatalf("supersede stop: %v", err)
	}
	if _, _, _, err := s2.FinalizeSandboxStopMissingDisk(t.Context(), job2.ID, "stop-worker", job2.LeaseToken,
		db.FinalizeStopParams{Owner: own2, SandboxID: sbx2.ID, StopID: op2.ID, ExpectedVersion: archiving2.Version}); !errors.Is(err, db.ErrStopSuperseded) {
		t.Fatalf("superseded stop missing disk: %v", err)
	}

	// A completed stop (archived sandbox) is closed for the outcome too: the
	// claim is still live, so the completed operation itself is the fence.
	s3 := dbtestOpen(t)
	own3 := owner(t, s3)
	node(t, s3, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx3 := phase1CreateReady(t, s3, own3, db.CreateSandboxParams{})
	archiving3, op3, job3 := phase1ArchivingStop(t, s3, own3, sbx3, "stop-worker")
	if _, _, err := s3.FinalizeSandboxStop(t.Context(), job3.ID, "stop-worker", job3.LeaseToken, db.FinalizeStopParams{
		Owner: own3, SandboxID: sbx3.ID, StopID: op3.ID, ExpectedVersion: archiving3.Version,
	}); err != nil {
		t.Fatalf("complete the stop first: %v", err)
	}
	if _, _, _, err := s3.FinalizeSandboxStopMissingDisk(t.Context(), job3.ID, "stop-worker", job3.LeaseToken,
		db.FinalizeStopParams{Owner: own3, SandboxID: sbx3.ID, StopID: op3.ID, ExpectedVersion: archiving3.Version}); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("completed stop missing disk: %v", err)
	}
	if got := nodeReserved(t, s3, "node-a"); got.VCPUs != 0 || got.MemoryMiB != 0 || got.DiskGiB != sbx3.Desired.DiskGiB {
		t.Fatalf("node after completed-stop fence: %+v", got)
	}
}
