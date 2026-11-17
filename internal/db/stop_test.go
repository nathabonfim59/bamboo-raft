package db_test

// Real-Postgres tests for the Phase 1 lifecycle database package (B1.1):
// durable stop operations, same-node resume capacity, PATCH persistence and
// stopped-retention deletion. They are gated by BBRAFT_TEST_DATABASE_URL
// through dbtest.Open and never report a skip as a pass.

import (
	"encoding/json/v2"
	"errors"
	"sync"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// phase1Ready moves a freshly created sandbox from provisioning to ready.
func phase1Ready(t *testing.T, s *db.Store, own uuid.UUID, sbx db.Sandbox) db.Sandbox {
	t.Helper()
	out, err := s.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID:       sbx.ID,
		Owner:           own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: sbx.Version,
		To:              db.StateReady,
	})
	if err != nil {
		t.Fatalf("ready transition: %v", err)
	}
	return out
}

// phase1ClaimKind claims jobs until one of kind arrives, completing unrelated
// jobs (typically the create provision fixture) so ordering stays simple.
func phase1ClaimKind(t *testing.T, s *db.Store, worker string, kind db.JobKind) db.Job {
	t.Helper()
	for range 10 {
		job, err := s.ClaimJob(t.Context(), worker, time.Minute)
		if err != nil {
			t.Fatalf("claim %s: %v", kind, err)
		}
		if job.Kind == kind {
			return job
		}
		if err := s.CompleteJob(t.Context(), job.ID, worker, job.LeaseToken); err != nil {
			t.Fatalf("complete %s before %s: %v", job.Kind, kind, err)
		}
	}
	t.Fatalf("no %s job appeared", kind)
	return db.Job{}
}

// phase1BootDone claims and completes the create's provision job, standing in
// for a worker that finished the first boot.
func phase1BootDone(t *testing.T, s *db.Store, worker string) {
	t.Helper()
	job := phase1ClaimKind(t, s, worker, db.JobProvision)
	if err := s.CompleteJob(t.Context(), job.ID, worker, job.LeaseToken); err != nil {
		t.Fatalf("complete boot job: %v", err)
	}
}

// phase1JobState reads one job's state for fence assertions.
func phase1JobState(t *testing.T, s *db.Store, id uuid.UUID) db.JobState {
	t.Helper()
	var state db.JobState
	if err := s.Pool().QueryRow(t.Context(), `SELECT state FROM jobs WHERE id = $1`, id).Scan(&state); err != nil {
		t.Fatalf("job state: %v", err)
	}
	return state
}

// phase1Stopped stops and finalizes a ready sandbox through the durable path.
func phase1Stopped(t *testing.T, s *db.Store, own uuid.UUID, sbx db.Sandbox) (db.Sandbox, db.StopOperation) {
	t.Helper()
	op, archiving, _, created, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil || !created {
		t.Fatalf("accept stop: created=%v err=%v", created, err)
	}
	job := phase1ClaimKind(t, s, "stop-worker", db.JobStop)
	stopped, final, err := s.FinalizeSandboxStop(t.Context(), job.ID, "stop-worker", job.LeaseToken, db.FinalizeStopParams{
		Owner:           own,
		SandboxID:       sbx.ID,
		StopID:          op.ID,
		ExpectedVersion: archiving.Version,
	})
	if err != nil {
		t.Fatalf("finalize stop: %v", err)
	}
	if err := s.CompleteJob(t.Context(), job.ID, "stop-worker", job.LeaseToken); err != nil {
		t.Fatalf("complete stop job: %v", err)
	}
	return stopped, final
}

// phase1CreateReady creates a sandbox on node-a and moves it to ready.
func phase1CreateReady(t *testing.T, s *db.Store, own uuid.UUID, p db.CreateSandboxParams) db.Sandbox {
	t.Helper()
	p.Owner = own
	res := create(t, s, p)
	return phase1Ready(t, s, own, res.Sandbox)
}

func TestSandboxStopAcceptanceAndReplay(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})

	op, archiving, job, created, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil {
		t.Fatalf("accept stop: %v", err)
	}
	if !created || op.Status != db.StopPending || op.SandboxID != sbx.ID || op.OwnerID != own {
		t.Fatalf("operation: created=%v %+v", created, op)
	}
	if len(op.ID) != len("stop_")+16 || op.ID[:len("stop_")] != "stop_" {
		t.Fatalf("stop id shape: %q", op.ID)
	}
	if archiving.State != db.StateArchiving {
		t.Fatalf("state after stop: %s", archiving.State)
	}
	if archiving.StopOperationID == nil || *archiving.StopOperationID != op.ID {
		t.Fatalf("stop metadata on sandbox: %+v", archiving.StopOperationID)
	}
	if archiving.Version != sbx.Version+1 {
		t.Fatalf("version after stop: %d, want %d", archiving.Version, sbx.Version+1)
	}
	// Archiving still holds the full reservation: the worker releases it
	// only after the node confirms the VM is off.
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 1 || got.MemoryMiB != 1024 || got.DiskGiB != 10 {
		t.Fatalf("node reservation during archiving: %+v", got)
	}
	if job.Kind != db.JobStop {
		t.Fatalf("job kind: %s", job.Kind)
	}
	var payload db.StopPayload
	if err := json.Unmarshal([]byte(job.Payload), &payload); err != nil {
		t.Fatalf("stop payload: %v", err)
	}
	if payload.OperationID != op.ID || payload.VMID != sbx.ID || payload.NodeID != "node-a" || payload.Force {
		t.Fatalf("stop payload: %+v", payload)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.stop' AND state = 'queued'`); n != 1 {
		t.Fatalf("queued stop jobs: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM sandbox_events WHERE sandbox_id = $1 AND type = 'stop.requested'`, sbx.ID); n != 1 {
		t.Fatalf("stop events: %d", n)
	}

	// Replay while pending: same operation, no new job, no state change.
	again, archiving2, _, created2, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil || created2 || again.ID != op.ID || archiving2.Version != archiving.Version {
		t.Fatalf("replay: created=%v op=%+v version=%d err=%v", created2, again, archiving2.Version, err)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.stop'`); n != 1 {
		t.Fatalf("stop jobs after replay: %d", n)
	}

	// A repeat force persists escalation on the canonical operation and the
	// queued job, so a worker rereading either sees it.
	escalated, _, _, created3, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, true)
	if err != nil || created3 || !escalated.ForceRequested {
		t.Fatalf("force escalation: created=%v op=%+v err=%v", created3, escalated, err)
	}
	var escalatedPayload db.StopPayload
	queued := phase1ClaimKind(t, s, "peek", db.JobStop)
	if err := json.Unmarshal([]byte(queued.Payload), &escalatedPayload); err != nil {
		t.Fatalf("escalated payload: %v", err)
	}
	if !escalatedPayload.Force {
		t.Fatalf("queued stop payload did not carry force: %s", queued.Payload)
	}
	if err := s.CompleteJob(t.Context(), queued.ID, "peek", queued.LeaseToken); err != nil {
		t.Fatal(err)
	}

	// Owner scoping.
	other := owner(t, s)
	if _, _, _, _, err := s.AcceptSandboxStop(t.Context(), other, sbx.ID, false); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign accept: %v", err)
	}
	if _, err := s.StopOperation(t.Context(), other, sbx.ID, op.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign stop read: %v", err)
	}
	if _, err := s.StopOperation(t.Context(), own, ids.Sandbox(), op.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("wrong-sandbox stop read: %v", err)
	}
}

func TestSandboxStopFinalizeFencesAndReleasesComputeOnce(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})

	op, archiving, _, _, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	job := phase1ClaimKind(t, s, "worker", db.JobStop)

	// Wrong lease token cannot finalize or release anything.
	if _, _, err := s.FinalizeSandboxStop(t.Context(), job.ID, "worker", uuid.NewV7(), db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
	}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("wrong token: %v", err)
	}
	// Stale version is a conflict.
	if _, _, err := s.FinalizeSandboxStop(t.Context(), job.ID, "worker", job.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version - 1,
	}); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("stale version: %v", err)
	}
	// A different operation id on this claim is a payload mismatch: the
	// claim itself is still valid but is not an authorization for another
	// operation.
	if _, _, err := s.FinalizeSandboxStop(t.Context(), job.ID, "worker", job.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: "stop_0000000000000000", ExpectedVersion: archiving.Version,
	}); !errors.Is(err, db.ErrInvalidJobPayload) {
		t.Fatalf("mismatched operation: %v", err)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 1 || got.MemoryMiB != 1024 {
		t.Fatalf("failed finalizations released capacity: %+v", got)
	}

	// A valid lease for a different job kind cannot authorize this stop.
	if _, err := s.EnqueueJob(t.Context(), db.JobInput{
		Kind: db.JobSaveSnapshot, OwnerID: own, SandboxID: &sbx.ID, Payload: "{}",
	}); err != nil {
		t.Fatal(err)
	}
	wrongKind := phase1ClaimKind(t, s, "other-worker", db.JobSaveSnapshot)
	if _, _, err := s.FinalizeSandboxStop(t.Context(), wrongKind.ID, "other-worker", wrongKind.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
	}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("wrong job kind: %v", err)
	}

	// A same-kind claim whose payload names another operation cannot
	// finalize this one.
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE jobs SET payload = payload || '{"OperationID":"stop_0000000000000000"}'::jsonb
		WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.FinalizeSandboxStop(t.Context(), job.ID, "worker", job.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
	}); !errors.Is(err, db.ErrInvalidJobPayload) {
		t.Fatalf("mismatched stop payload: %v", err)
	}
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE jobs SET payload = $2::jsonb WHERE id = $1`, job.ID, job.Payload); err != nil {
		t.Fatal(err)
	}

	stopped, final, err := s.FinalizeSandboxStop(t.Context(), job.ID, "worker", job.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
	})
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if stopped.State != db.StateArchived || stopped.StoppedAt == nil {
		t.Fatalf("stopped sandbox: %+v", stopped)
	}
	if final.Status != db.StopCompleted || final.EndedAt == nil {
		t.Fatalf("final stop: %+v", final)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 0 || got.MemoryMiB != 0 || got.DiskGiB != 10 {
		t.Fatalf("node after stop: %+v", got)
	}
	if stopped.Reserved.VCPUs != 0 || stopped.Reserved.MemoryMiB != 0 || stopped.Reserved.DiskGiB != 10 {
		t.Fatalf("sandbox reservation after stop: %+v", stopped.Reserved)
	}
	if stopped.Desired.VCPUs != 1 || stopped.Desired.MemoryMiB != 1024 || stopped.Desired.DiskGiB != 10 {
		t.Fatalf("desired shape after stop: %+v", stopped.Desired)
	}

	// Duplicate delivery while the claim is live is idempotent.
	repeat, repeatOp, err := s.FinalizeSandboxStop(t.Context(), job.ID, "worker", job.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
	})
	if err != nil || repeat.State != db.StateArchived || repeatOp.ID != final.ID {
		t.Fatalf("repeat finalize: state=%s op=%s err=%v", repeat.State, repeatOp.ID, err)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 0 || got.DiskGiB != 10 {
		t.Fatalf("duplicate release: %+v", got)
	}
	if err := s.CompleteJob(t.Context(), job.ID, "worker", job.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.FinalizeSandboxStop(t.Context(), job.ID, "worker", job.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID,
	}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("finalize after job completion: %v", err)
	}

	if got, err := s.LatestSandboxStop(t.Context(), own, sbx.ID); err != nil || got.ID != op.ID || got.Status != db.StopCompleted {
		t.Fatalf("latest stop: %+v err=%v", got, err)
	}
}

func TestSandboxStopFailureAndRetry(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	op, archiving, _, _, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	job := phase1ClaimKind(t, s, "worker", db.JobStop)

	if _, err := s.FailSandboxStopAttempt(t.Context(), job.ID, "worker", uuid.NewV7(), own, op.ID, "nope"); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("failure with wrong token: %v", err)
	}
	// A claimed stop job whose payload names another operation cannot mark
	// this one failing.
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE jobs SET payload = payload || '{"OperationID":"stop_0000000000000000"}'::jsonb
		WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FailSandboxStopAttempt(t.Context(), job.ID, "worker", job.LeaseToken, own, op.ID, "nope"); !errors.Is(err, db.ErrInvalidJobPayload) {
		t.Fatalf("failure with mismatched payload: %v", err)
	}
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE jobs SET payload = $2::jsonb WHERE id = $1`, job.ID, job.Payload); err != nil {
		t.Fatal(err)
	}
	failing, err := s.FailSandboxStopAttempt(t.Context(), job.ID, "worker", job.LeaseToken, own, op.ID, "node unavailable")
	if err != nil {
		t.Fatalf("fail attempt: %v", err)
	}
	if failing.Status != db.StopFailing || failing.LastAttemptAt == nil || failing.Error != "node unavailable" {
		t.Fatalf("failing operation: %+v", failing)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 1 {
		t.Fatalf("failed attempt released compute: %+v", got)
	}

	if err := s.RetryJob(t.Context(), job.ID, "worker", job.LeaseToken, 0, "node unavailable"); err != nil {
		t.Fatal(err)
	}
	retried := phase1ClaimKind(t, s, "worker", db.JobStop)
	stopped, final, err := s.FinalizeSandboxStop(t.Context(), retried.ID, "worker", retried.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
	})
	if err != nil {
		t.Fatalf("retry finalize: %v", err)
	}
	if stopped.State != db.StateArchived || final.Status != db.StopCompleted || final.Error != "" {
		t.Fatalf("after retry: %+v %+v", stopped, final)
	}
	// A late failure from the old attempt must not rewrite terminal state.
	late, err := s.FailSandboxStopAttempt(t.Context(), retried.ID, "worker", retried.LeaseToken, own, op.ID, "late")
	if err != nil || late.Status != db.StopCompleted || late.Error != "" {
		t.Fatalf("late failure rewrote terminal state: %+v err=%v", late, err)
	}
}

func TestSandboxStopDuringProvisionKeepsBootJob(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	res := create(t, s, db.CreateSandboxParams{Owner: own})

	op, archiving, _, created, err := s.AcceptSandboxStop(t.Context(), own, res.Sandbox.ID, false)
	if err != nil || !created {
		t.Fatalf("stop during provision: created=%v err=%v", created, err)
	}
	if archiving.State != db.StateArchiving {
		t.Fatalf("state: %s", archiving.State)
	}
	// The initial boot must stay available so the worker can finish the
	// first disk under the shared guard before stopping it.
	boot, err := s.LatestSandboxJob(t.Context(), own, res.Sandbox.ID, db.JobProvision)
	if err != nil {
		t.Fatalf("latest provision job: %v", err)
	}
	if boot.State != db.JobQueued {
		t.Fatalf("boot job state: %s", boot.State)
	}
	var payload db.ProvisionPayload
	if err := json.Unmarshal([]byte(boot.Payload), &payload); err != nil {
		t.Fatalf("provision payload: %v", err)
	}
	if payload.Generation != 0 {
		t.Fatalf("initial generation: %d", payload.Generation)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.provision' AND state = 'cancelled'`, res.Sandbox.ID); n != 0 {
		t.Fatalf("initial boot cancelled at stop acceptance: %d", n)
	}
	if got, err := s.LatestSandboxStop(t.Context(), own, res.Sandbox.ID); err != nil || got.ID != op.ID {
		t.Fatalf("latest stop during provision: %+v err=%v", got, err)
	}
	if _, err := s.LatestSandboxStop(t.Context(), own, ids.Sandbox()); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("latest stop for unknown sandbox: %v", err)
	}
}

func TestSandboxExpiryStopRechecksDeadlineAndVersion(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	ttl := 60
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{TTLSeconds: &ttl})
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE sandboxes SET expires_at = now() - interval '1 second' WHERE id = $1`, sbx.ID); err != nil {
		t.Fatal(err)
	}
	expired, err := s.Sandbox(t.Context(), own, sbx.ID)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, _, _, err := s.AcceptSandboxExpiryStop(t.Context(), own, sbx.ID, expired.Version-1, time.Now(), false); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("stale version: %v", err)
	}
	if _, archiving, _, created, err := s.AcceptSandboxExpiryStop(t.Context(), own, sbx.ID, expired.Version, time.Now(), false); err != nil || !created || archiving.State != db.StateArchiving {
		t.Fatalf("expiry stop: created=%v state=%s err=%v", created, archiving.State, err)
	}

	// A deadline that already moved still wins over an old candidate.
	otherTTL := 600
	future := phase1CreateReady(t, s, own, db.CreateSandboxParams{TTLSeconds: &otherTTL})
	if _, _, _, _, err := s.AcceptSandboxExpiryStop(t.Context(), own, future.ID, future.Version, time.Now(), false); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("future expiry accepted: %v", err)
	}

	// A concurrent PATCH extension bumps the version; the old candidate
	// must not stop the sandbox.
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE sandboxes SET expires_at = now() - interval '1 second' WHERE id = $1`, future.ID); err != nil {
		t.Fatal(err)
	}
	stale, err := s.Sandbox(t.Context(), own, future.ID)
	if err != nil {
		t.Fatal(err)
	}
	extension := 3600
	if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{
		Owner: own, SandboxID: future.ID, TTL: db.TTLOption{Set: true, Seconds: &extension},
	}); err != nil {
		t.Fatalf("extend: %v", err)
	}
	if _, _, _, _, err := s.AcceptSandboxExpiryStop(t.Context(), own, future.ID, stale.Version, time.Now(), false); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("extended sandbox stopped: %v", err)
	}
	if live, err := s.Sandbox(t.Context(), own, future.ID); err != nil || live.State != db.StateReady {
		t.Fatalf("extended sandbox state: %+v err=%v", live, err)
	}

	// An unlimited sandbox is never an expiry candidate.
	unlimited := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	if _, _, _, _, err := s.AcceptSandboxExpiryStop(t.Context(), own, unlimited.ID, unlimited.Version, time.Now(), false); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("unlimited expiry stop: %v", err)
	}
}

// TestSandboxStopFinalizeAfterDeletion proves deletion accepted during
// archiving frees the remaining reservation exactly once and a late stop
// finalization cannot archive a deleted row.
func TestSandboxStopFinalizeAfterDeletion(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	phase1BootDone(t, s, "boot-worker")
	op, _, _, _, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	stopJob := phase1ClaimKind(t, s, "stop-worker", db.JobStop)

	del, _, err := s.AcceptSandboxDeletion(t.Context(), own, sbx.ID, db.DeletionExplicit)
	if err != nil {
		t.Fatalf("delete during archiving: %v", err)
	}
	if _, _, err := s.FinalizeSandboxStop(t.Context(), stopJob.ID, "stop-worker", stopJob.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID,
	}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("stop finalized a deleted row: %v", err)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10}) {
		t.Fatalf("late stop finalize changed capacity: %+v", got)
	}
	superseded, err := s.StopOperation(t.Context(), own, sbx.ID, op.ID)
	if err != nil || superseded.Status != db.StopSuperseded {
		t.Fatalf("stop operation after deletion: %+v err=%v", superseded, err)
	}

	if _, err := s.CompleteDeletion(t.Context(), own, del.ID); err != nil {
		t.Fatal(err)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{}) {
		t.Fatalf("node after deletion: %+v", got)
	}
	// Repeat completion and the stale stop job cannot release again.
	if _, err := s.CompleteDeletion(t.Context(), own, del.ID); err != nil {
		t.Fatal(err)
	}
	late, err := s.FailSandboxStopAttempt(t.Context(), stopJob.ID, "stop-worker", stopJob.LeaseToken, own, op.ID, "late")
	if err != nil || late.Status != db.StopSuperseded {
		t.Fatalf("late stop failure: %+v err=%v", late, err)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{}) {
		t.Fatalf("late stop work released capacity again: %+v", got)
	}
}

// TestSandboxFinalizeRejectsMalformedPayloads proves valid JSON of the wrong
// shape fails closed for both stop and resume claims.
func TestSandboxFinalizeRejectsMalformedPayloads(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	phase1BootDone(t, s, "boot-worker")

	op, archiving, _, _, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	stopJob := phase1ClaimKind(t, s, "stop-worker", db.JobStop)
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE jobs SET payload = '{"OperationID": 123}'::jsonb WHERE id = $1`, stopJob.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.FinalizeSandboxStop(t.Context(), stopJob.ID, "stop-worker", stopJob.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
	}); !errors.Is(err, db.ErrInvalidJobPayload) {
		t.Fatalf("malformed stop payload: %v", err)
	}
	// Restoring the claim's real payload lets the same attempt finish.
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE jobs SET payload = $2::jsonb WHERE id = $1`, stopJob.ID, stopJob.Payload); err != nil {
		t.Fatal(err)
	}
	stopped, _, err := s.FinalizeSandboxStop(t.Context(), stopJob.ID, "stop-worker", stopJob.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version,
	})
	if err != nil {
		t.Fatalf("finalize after malformed payload: %v", err)
	}
	if err := s.CompleteJob(t.Context(), stopJob.ID, "stop-worker", stopJob.LeaseToken); err != nil {
		t.Fatal(err)
	}

	// The same sandbox can still resume with a fresh claim.
	accepted, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: stopped.ID})
	if err != nil {
		t.Fatal(err)
	}
	resumeJob := phase1ClaimKind(t, s, "resume-worker", db.JobResume)
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE jobs SET payload = '{"Generation": "one"}'::jsonb WHERE id = $1`, resumeJob.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FinalizeSandboxResume(t.Context(), resumeJob.ID, "resume-worker", resumeJob.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: stopped.ID, Generation: accepted.Sandbox.LifecycleGeneration, Version: accepted.Sandbox.Version,
	}); !errors.Is(err, db.ErrInvalidJobPayload) {
		t.Fatalf("malformed resume payload: %v", err)
	}
}

// TestSandboxStopAndDeleteReleaseOnlyRemainingCapacity proves a stopped
// sandbox's later deletion frees the retained disk once and never compute
// twice.
func TestSandboxStopAndDeleteReleaseOnlyRemainingCapacity(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	_, stopOp := phase1Stopped(t, s, own, sbx)

	// Stopping an already archived sandbox replays its completed operation
	// and does not mint a new one, even with force.
	replay, archived, _, created, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, true)
	if err != nil || created || replay.ID != stopOp.ID || replay.Status != db.StopCompleted ||
		archived.State != db.StateArchived {
		t.Fatalf("archived replay: created=%v op=%+v state=%s err=%v", created, replay, archived.State, err)
	}
	if n := count(t, s, `SELECT count(*) FROM stop_operations WHERE sandbox_id = $1`, sbx.ID); n != 1 {
		t.Fatalf("archived replay minted operations: %d", n)
	}

	op, _, err := s.AcceptSandboxDeletion(t.Context(), own, sbx.ID, db.DeletionExplicit)
	if err != nil {
		t.Fatalf("delete stopped sandbox: %v", err)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 0 || got.DiskGiB != 10 {
		t.Fatalf("delete acceptance released disk early: %+v", got)
	}
	if _, err := s.CompleteDeletion(t.Context(), own, op.ID); err != nil {
		t.Fatal(err)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 0 || got.MemoryMiB != 0 || got.DiskGiB != 0 {
		t.Fatalf("node after delete: %+v", got)
	}
	// Duplicate completion is idempotent.
	if _, err := s.CompleteDeletion(t.Context(), own, op.ID); err != nil {
		t.Fatal(err)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 0 || got.DiskGiB != 0 {
		t.Fatalf("double release after stop: %+v", got)
	}
}

// TestSandboxConcurrentStopAcceptance proves two stops for one sandbox
// serialize into one durable operation and one job.
func TestSandboxConcurrentStopAcceptance(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		idsSeen = map[string]struct{}{}
		created int
		errs    []string
	)
	for range 4 {
		wg.Go(func() {
			op, _, _, isNew, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err.Error())
				return
			}
			idsSeen[op.ID] = struct{}{}
			if isNew {
				created++
			}
		})
	}
	wg.Wait()
	if len(errs) != 0 {
		t.Fatalf("concurrent accepts: %v", errs)
	}
	if created != 1 || len(idsSeen) != 1 {
		t.Fatalf("created=%d distinct operations=%v", created, idsSeen)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.stop'`); n != 1 {
		t.Fatalf("stop jobs: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM stop_operations WHERE sandbox_id = $1 AND status IN ('pending','failing')`, sbx.ID); n != 1 {
		t.Fatalf("active operations: %d", n)
	}
}
