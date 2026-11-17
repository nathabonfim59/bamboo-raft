package db_test

// Real-Postgres tests for terminal resume failure finalization (B1.2): a
// failed boot returns to archived with compute-only release after StopVM
// confirmed the retained disk, and a confirmed missing disk releases
// everything only through the fenced store method. Gated by
// BBRAFT_TEST_DATABASE_URL through dbtest.Open.

import (
	"errors"
	"testing"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// phase1AcceptedResume stops a ready sandbox and accepts one resume, claiming
// its job for worker.
func phase1AcceptedResume(t *testing.T, s *db.Store, own uuid.UUID, sbx db.Sandbox, worker string, p db.ResumeParams) (db.Sandbox, db.Job) {
	t.Helper()
	_, _ = phase1Stopped(t, s, own, sbx)
	accepted, err := s.AcceptSandboxResume(t.Context(), p)
	if err != nil {
		t.Fatalf("accept resume: %v", err)
	}
	if accepted.Sandbox.LifecycleGeneration == 0 {
		t.Fatal("resume did not advance the generation")
	}
	job := phase1ClaimKind(t, s, worker, db.JobResume)
	return accepted.Sandbox, job
}

func TestFinalizeSandboxResumeFailureArchivesAndReleasesComputeOnly(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	desired := sbx.Desired

	resumed, job := phase1AcceptedResume(t, s, own, sbx, "fail-worker", db.ResumeParams{
		Owner: own, SandboxID: sbx.ID,
	})

	out, err := s.FinalizeSandboxResumeFailure(t.Context(), job.ID, "fail-worker", job.LeaseToken,
		db.FinalizeResumeFailureParams{
			Owner: own, SandboxID: sbx.ID,
			Generation: resumed.LifecycleGeneration, Version: resumed.Version,
		}, "boot_failed")
	if err != nil {
		t.Fatalf("finalize resume failure: %v", err)
	}
	if out.State != db.StateArchived {
		t.Fatalf("state: %s, want archived", out.State)
	}
	if out.ErrorReason != "boot_failed" {
		t.Fatalf("error reason: %q", out.ErrorReason)
	}
	if out.StoppedAt == nil {
		t.Fatal("stopped_at not recorded")
	}
	if out.LifecycleGeneration != resumed.LifecycleGeneration {
		t.Fatalf("generation moved: %d", out.LifecycleGeneration)
	}
	if out.Desired != desired {
		t.Fatalf("desired shape not kept: %+v", out.Desired)
	}
	if out.Reserved.VCPUs != 0 || out.Reserved.MemoryMiB != 0 {
		t.Fatalf("compute not released: %+v", out.Reserved)
	}
	if out.Reserved.DiskGiB != desired.DiskGiB {
		t.Fatalf("retained disk not kept: %+v", out.Reserved)
	}
	// The node keeps exactly the disk part: compute left, disk retained once.
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 0 || got.MemoryMiB != 0 || got.DiskGiB != desired.DiskGiB {
		t.Fatalf("node reservation: %+v", got)
	}
	if err := s.CompleteJob(t.Context(), job.ID, "fail-worker", job.LeaseToken); err != nil {
		t.Fatal(err)
	}

	// A later resume can still accept and reserve the same retained disk.
	again, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: sbx.ID})
	if err != nil {
		t.Fatalf("resume after failed resume: %v", err)
	}
	if got := nodeReserved(t, s, "node-a"); got != again.Sandbox.Desired {
		t.Fatalf("node after re-resume: %+v, want %+v", got, again.Sandbox.Desired)
	}
}

func TestFinalizeSandboxResumeFailureFencing(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	foreign := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	desired := sbx.Desired

	resumed, job := phase1AcceptedResume(t, s, own, sbx, "fail-worker", db.ResumeParams{
		Owner: own, SandboxID: sbx.ID,
	})
	p := db.FinalizeResumeFailureParams{
		Owner: own, SandboxID: sbx.ID,
		Generation: resumed.LifecycleGeneration, Version: resumed.Version,
	}
	full := desired

	check := func(err error, want error, what string) {
		t.Helper()
		if !errors.Is(err, want) {
			t.Fatalf("%s: %v, want %v", what, err, want)
		}
		if got := nodeReserved(t, s, "node-a"); got != full {
			t.Fatalf("%s changed the reservation: %+v", what, got)
		}
		row, rerr := s.SandboxAny(t.Context(), own, sbx.ID)
		if rerr != nil || row.State != db.StateProvisioning {
			t.Fatalf("%s changed the row: %+v (%v)", what, row, rerr)
		}
	}

	check(func() error {
		_, err := s.FinalizeSandboxResumeFailure(t.Context(), job.ID, "fail-worker", uuid.NewV7(), p, "boot_failed")
		return err
	}(), db.ErrJobNotLeased, "wrong token")
	check(func() error {
		_, err := s.FinalizeSandboxResumeFailure(t.Context(), job.ID, "fail-worker", job.LeaseToken,
			db.FinalizeResumeFailureParams{Owner: own, SandboxID: sbx.ID, Generation: 99, Version: resumed.Version}, "boot_failed")
		return err
	}(), db.ErrInvalidJobPayload, "wrong generation")
	check(func() error {
		_, err := s.FinalizeSandboxResumeFailure(t.Context(), job.ID, "fail-worker", job.LeaseToken,
			db.FinalizeResumeFailureParams{Owner: own, SandboxID: sbx.ID, Generation: 0, Version: resumed.Version}, "boot_failed")
		return err
	}(), db.ErrConflict, "missing generation")
	check(func() error {
		_, err := s.FinalizeSandboxResumeFailure(t.Context(), job.ID, "fail-worker", job.LeaseToken,
			db.FinalizeResumeFailureParams{Owner: own, SandboxID: sbx.ID, Generation: resumed.LifecycleGeneration, Version: resumed.Version + 1}, "boot_failed")
		return err
	}(), db.ErrConflict, "stale version")
	check(func() error {
		_, err := s.FinalizeSandboxResumeFailure(t.Context(), job.ID, "fail-worker", job.LeaseToken,
			db.FinalizeResumeFailureParams{Owner: foreign, SandboxID: sbx.ID, Generation: resumed.LifecycleGeneration, Version: resumed.Version}, "boot_failed")
		return err
	}(), db.ErrJobNotLeased, "foreign owner")

	// A newer stop intent moved the row out of provisioning: fail closed and
	// leave the machine to the stop job (archiving keeps the full
	// reservation).
	if _, archiving, _, _, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false); err != nil {
		t.Fatalf("accept newer stop: %v", err)
	} else {
		if archiving.State != db.StateArchiving {
			t.Fatalf("newer stop left state %s", archiving.State)
		}
	}
	if _, err := s.FinalizeSandboxResumeFailure(t.Context(), job.ID, "fail-worker", job.LeaseToken, p, "boot_failed"); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("newer stop intent: %v, want conflict", err)
	}
	if row, rerr := s.SandboxAny(t.Context(), own, sbx.ID); rerr != nil || row.State != db.StateArchiving {
		t.Fatalf("failed finalize changed the row: %+v (%v)", row, rerr)
	}
	if got := nodeReserved(t, s, "node-a"); got != full {
		t.Fatalf("failed finalize changed the reservation: %+v", got)
	}

	// A deleted sandbox is a safe not-found, never a finalize.
	s2 := dbtestOpen(t)
	own2 := owner(t, s2)
	node(t, s2, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx2 := phase1CreateReady(t, s2, own2, db.CreateSandboxParams{})
	resumed2, job2 := phase1AcceptedResume(t, s2, own2, sbx2, "fail-worker", db.ResumeParams{
		Owner: own2, SandboxID: sbx2.ID,
	})
	if _, _, err := s2.AcceptSandboxDeletion(t.Context(), own2, sbx2.ID, db.DeletionExplicit); err != nil {
		t.Fatalf("accept deletion: %v", err)
	}
	if _, err := s2.FinalizeSandboxResumeFailure(t.Context(), job2.ID, "fail-worker", job2.LeaseToken,
		db.FinalizeResumeFailureParams{
			Owner: own2, SandboxID: sbx2.ID,
			Generation: resumed2.LifecycleGeneration, Version: resumed2.Version,
		}, "boot_failed"); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("deleted sandbox finalize: %v", err)
	}
}

func TestFinalizeSandboxResumeMissingDiskReleasesAllOnce(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	desired := sbx.Desired

	resumed, job := phase1AcceptedResume(t, s, own, sbx, "fail-worker", db.ResumeParams{
		Owner: own, SandboxID: sbx.ID,
	})
	if got := nodeReserved(t, s, "node-a"); got != desired {
		t.Fatalf("node before finalize: %+v", got)
	}

	out, err := s.FinalizeSandboxResumeMissingDisk(t.Context(), job.ID, "fail-worker", job.LeaseToken,
		db.FinalizeResumeFailureParams{
			Owner: own, SandboxID: sbx.ID,
			Generation: resumed.LifecycleGeneration, Version: resumed.Version,
		})
	if err != nil {
		t.Fatalf("finalize missing disk: %v", err)
	}
	if out.State != db.StateError || out.ErrorReason != "vm_missing" {
		t.Fatalf("row after missing disk: state=%s reason=%q", out.State, out.ErrorReason)
	}
	if out.Reserved != (db.Resources{}) {
		t.Fatalf("reservation not released: %+v", out.Reserved)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{}) {
		t.Fatalf("node after missing disk: %+v", got)
	}
	if err := s.CompleteJob(t.Context(), job.ID, "fail-worker", job.LeaseToken); err != nil {
		t.Fatal(err)
	}

	// Repeated delivery under the finished claim changes nothing; a live
	// claim on a moved row fails closed.
	if _, err := s.FinalizeSandboxResumeMissingDisk(t.Context(), job.ID, "fail-worker", job.LeaseToken,
		db.FinalizeResumeFailureParams{
			Owner: own, SandboxID: sbx.ID,
			Generation: resumed.LifecycleGeneration, Version: out.Version,
		}); !errors.Is(err, db.ErrJobNotLeased) && !errors.Is(err, db.ErrConflict) {
		t.Fatalf("duplicate missing-disk finalize: %v", err)
	}

	// A second sandbox proves compute+disk release is exactly once: two
	// missing disks on one node leave zero, not a negative counter.
	sbx2 := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	resumed2, job2 := phase1AcceptedResume(t, s, own, sbx2, "fail-worker", db.ResumeParams{
		Owner: own, SandboxID: sbx2.ID,
	})
	if _, err := s.FinalizeSandboxResumeMissingDisk(t.Context(), job2.ID, "fail-worker", job2.LeaseToken,
		db.FinalizeResumeFailureParams{
			Owner: own, SandboxID: sbx2.ID,
			Generation: resumed2.LifecycleGeneration, Version: resumed2.Version,
		}); err != nil {
		t.Fatalf("second missing disk: %v", err)
	}
	if err := s.CompleteJob(t.Context(), job2.ID, "fail-worker", job2.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{}) {
		t.Fatalf("node after two missing disks: %+v", got)
	}
	var diskLeft int64
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT reserved_disk_gib FROM nodes WHERE id = 'node-a'`).Scan(&diskLeft); err != nil {
		t.Fatal(err)
	}
	if diskLeft != 0 {
		t.Fatalf("node disk counter after releases: %d", diskLeft)
	}
}
