package db_test

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

func TestSandboxResumeRearmsComputeAndGeneration(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	ttl := 123
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{
		TTLSeconds: &ttl,
		Env:        map[string]string{"A": "1"},
	})
	var sealedBefore []byte
	if err := s.Pool().QueryRow(t.Context(), `SELECT env_sealed FROM sandboxes WHERE id = $1`, sbx.ID).Scan(&sealedBefore); err != nil {
		t.Fatal(err)
	}
	stopped, stopOp := phase1Stopped(t, s, own, sbx)
	if stopped.State != db.StateArchived {
		t.Fatalf("stopped state: %s", stopped.State)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 0 || got.MemoryMiB != 0 || got.DiskGiB != 10 {
		t.Fatalf("node after stop: %+v", got)
	}

	accepted, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner:     own,
		SandboxID: sbx.ID,
	})
	if err != nil {
		t.Fatalf("accept resume: %v", err)
	}
	resumed := accepted.Sandbox
	if accepted.Superseded != nil {
		t.Fatalf("archived resume superseded %+v", accepted.Superseded)
	}
	if resumed.State != db.StateProvisioning {
		t.Fatalf("resume state: %s", resumed.State)
	}
	if resumed.LifecycleGeneration != 1 {
		t.Fatalf("generation: %d", resumed.LifecycleGeneration)
	}
	if resumed.Version != stopped.Version+1 {
		t.Fatalf("version: %d, want %d", resumed.Version, stopped.Version+1)
	}
	if resumed.Desired != (db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10}) {
		t.Fatalf("desired: %+v", resumed.Desired)
	}
	if resumed.Reserved != resumed.Desired {
		t.Fatalf("reserved: %+v", resumed.Reserved)
	}
	if got := nodeReserved(t, s, "node-a"); got != resumed.Desired {
		t.Fatalf("node after resume: %+v, want %+v", got, resumed.Desired)
	}
	if resumed.StoppedAt != nil {
		t.Fatalf("stopped_at after resume: %s", resumed.StoppedAt)
	}
	if resumed.TTLSeconds == nil || *resumed.TTLSeconds != ttl {
		t.Fatalf("configured ttl after resume: %v", resumed.TTLSeconds)
	}
	if resumed.ExpiresAt == nil {
		t.Fatal("resume did not reset the deadline")
	}
	if d := time.Until(*resumed.ExpiresAt); d < 110*time.Second || d > 130*time.Second {
		t.Fatalf("resumed deadline in %s, want about %ds", d, ttl)
	}
	var sealedAfter []byte
	if err := s.Pool().QueryRow(t.Context(), `SELECT env_sealed FROM sandboxes WHERE id = $1`, sbx.ID).Scan(&sealedAfter); err != nil {
		t.Fatal(err)
	}
	if string(sealedBefore) != string(sealedAfter) {
		t.Fatal("omitted resume env changed the sealed env")
	}
	if accepted.Job.Kind != db.JobResume {
		t.Fatalf("resume job kind: %s", accepted.Job.Kind)
	}
	var payload db.ResumePayload
	if err := json.Unmarshal([]byte(accepted.Job.Payload), &payload); err != nil {
		t.Fatalf("resume payload: %v", err)
	}
	if payload.Generation != 1 || payload.VMID != sbx.ID || payload.NodeID != "node-a" ||
		payload.FromStopID != stopOp.ID || payload.Resources != resumed.Desired {
		t.Fatalf("resume payload: %+v", payload)
	}

	job := phase1ClaimKind(t, s, "resume-worker", db.JobResume)
	// A same-kind claim for another generation cannot finalize this one.
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE jobs SET payload = payload || '{"Generation":999}'::jsonb
		WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FinalizeSandboxResume(t.Context(), job.ID, "resume-worker", job.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID, Generation: 1, Version: resumed.Version,
	}); !errors.Is(err, db.ErrInvalidJobPayload) {
		t.Fatalf("mismatched resume generation: %v", err)
	}
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE jobs SET payload = $2::jsonb WHERE id = $1`, job.ID, job.Payload); err != nil {
		t.Fatal(err)
	}
	// A valid lease for a different job kind cannot finalize this resume
	// either.
	if _, err := s.EnqueueJob(t.Context(), db.JobInput{
		Kind: db.JobSaveSnapshot, OwnerID: own, SandboxID: &sbx.ID, Payload: "{}",
	}); err != nil {
		t.Fatal(err)
	}
	wrongKind := phase1ClaimKind(t, s, "other-worker", db.JobSaveSnapshot)
	if _, err := s.FinalizeSandboxResume(t.Context(), wrongKind.ID, "other-worker", wrongKind.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID, Generation: 1, Version: resumed.Version,
	}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("resume with wrong job kind: %v", err)
	}
	if _, err := s.FinalizeSandboxResume(t.Context(), job.ID, "resume-worker", job.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID, Generation: 0, Version: resumed.Version,
	}); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("finalize without generation: %v", err)
	}
	if _, err := s.FinalizeSandboxResume(t.Context(), job.ID, "resume-worker", uuid.NewV7(), db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID, Generation: 1, Version: resumed.Version,
	}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("finalize with wrong token: %v", err)
	}
	if _, err := s.FinalizeSandboxResume(t.Context(), job.ID, "resume-worker", job.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID, Generation: 1, Version: resumed.Version - 1,
	}); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("finalize with stale version: %v", err)
	}
	ready, err := s.FinalizeSandboxResume(t.Context(), job.ID, "resume-worker", job.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID, Generation: 1, Version: resumed.Version,
	})
	if err != nil {
		t.Fatalf("finalize resume: %v", err)
	}
	if ready.State != db.StateReady || ready.StoppedAt != nil || ready.LifecycleGeneration != 1 {
		t.Fatalf("resumed sandbox: %+v", ready)
	}
	if err := s.CompleteJob(t.Context(), job.ID, "resume-worker", job.LeaseToken); err != nil {
		t.Fatal(err)
	}
	// A later resume generation only: the same generation cannot finalize
	// twice once the job is done.
	if _, err := s.FinalizeSandboxResume(t.Context(), job.ID, "resume-worker", job.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID, Generation: 1, Version: ready.Version,
	}); !errors.Is(err, db.ErrJobNotLeased) {
		t.Fatalf("finalize after completion: %v", err)
	}
}

func TestSandboxResumeCapacityExhaustionAndMissingNode(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	a := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	b := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	c := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	for _, sbx := range []db.Sandbox{a, b, c} {
		phase1Stopped(t, s, own, sbx)
	}

	// A stopped sandbox whose node row is gone cannot resume: its disk is
	// stranded rather than silently replaced.
	if _, err := s.Pool().Exec(t.Context(), `UPDATE sandboxes SET node_id = NULL WHERE id = $1`, c.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: c.ID}); !errors.Is(err, db.ErrNodeUnavailable) {
		t.Fatalf("resume with missing node: %v", err)
	}

	// A smaller disk than the retained one is refused before capacity.
	if _, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: b.ID, MachineType: "small",
		Resources: db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 5},
	}); !errors.Is(err, db.ErrTypeTooSmall) {
		t.Fatalf("shrink resume: %v", err)
	}

	// Tighten totals so only one of a and b can take its compute back. The
	// retained disk totals stay reserved.
	if err := s.UpsertNodeInfo(t.Context(), db.NodeInfo{
		ID: "node-a", Capacity: db.Resources{VCPUs: 1, MemoryMiB: 1024},
		DiskFreeGiB: 70, // 100 GiB total minus the three retained 10 GiB disks
		SeenAt:      time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
		failures  []error
	)
	for _, id := range []string{a.ID, b.ID} {
		wg.Go(func() {
			_, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: id})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failures = append(failures, err)
				return
			}
			successes++
		})
	}
	wg.Wait()
	if successes != 1 {
		t.Fatalf("concurrent resumes: %d successes, failures %v", successes, failures)
	}
	for _, err := range failures {
		if !errors.Is(err, db.ErrNoCapacity) {
			t.Fatalf("loser error: %v", err)
		}
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 1 || got.MemoryMiB != 1024 || got.DiskGiB != 30 {
		t.Fatalf("node after the race: %+v", got)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.resume' AND state = 'queued'`); n != 1 {
		t.Fatalf("queued resumes: %d, want only the winner", n)
	}
	// The loser rolled back completely: still archived, still disk-only.
	if n := count(t, s, `SELECT count(*) FROM sandboxes WHERE id IN ($1, $2) AND state = 'archived' AND reserved_vcpus = 0 AND reserved_memory_mib = 0`, a.ID, b.ID); n != 1 {
		t.Fatalf("archived losers: %d, want exactly 1", n)
	}
}

// TestSandboxResumeAndCreateRaceForLastSlot proves resume admission shares the
// same node reservation as create admission: one of them takes the last slot
// and the other answers no capacity without partial rows or reservations.
func TestSandboxResumeAndCreateRaceForLastSlot(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	victim := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	phase1Stopped(t, s, own, victim)
	if err := s.UpsertNodeInfo(t.Context(), db.NodeInfo{
		ID: "node-a", Capacity: db.Resources{VCPUs: 1, MemoryMiB: 1024},
		DiskFreeGiB: 90, // 100 GiB total minus the victim's retained 10 GiB disk
		SeenAt:      time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		createErr error
		resumeErr error
		successes int
	)
	wg.Go(func() {
		_, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
			ID: ids.Sandbox(), Owner: own, NodeID: "node-a",
			Reserve: db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10},
		})
		mu.Lock()
		defer mu.Unlock()
		createErr = err
		if err == nil {
			successes++
		}
	})
	wg.Go(func() {
		_, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: victim.ID})
		mu.Lock()
		defer mu.Unlock()
		resumeErr = err
		if err == nil {
			successes++
		}
	})
	wg.Wait()
	if successes != 1 {
		t.Fatalf("admission race: %d successes, create=%v resume=%v", successes, createErr, resumeErr)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 1 || got.MemoryMiB != 1024 {
		t.Fatalf("node reservation after admission race: %+v", got)
	}
	switch {
	case createErr == nil:
		if !errors.Is(resumeErr, db.ErrNoCapacity) {
			t.Fatalf("create won but resume error: %v", resumeErr)
		}
		if got, err := s.Sandbox(t.Context(), own, victim.ID); err != nil || got.State != db.StateArchived || got.Reserved.VCPUs != 0 {
			t.Fatalf("victim after lost resume: %+v err=%v", got, err)
		}
	case resumeErr == nil:
		if !errors.Is(createErr, db.ErrNoCapacity) {
			t.Fatalf("resume won but create error: %v", createErr)
		}
		if n := count(t, s, `SELECT count(*) FROM sandboxes WHERE owner_id = $1`, own); n != 1 {
			t.Fatalf("losing create left a sandbox: %d sandboxes", n)
		}
	}
}

func TestSandboxResumeSupersedesArchivingStop(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	phase1BootDone(t, s, "boot-worker")

	// Queued stop (no worker claim yet): resume supersedes it and cancels
	// the queued job.
	op, archiving, _, _, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: sbx.ID}); !errors.Is(err, db.ErrInvalidState) {
		t.Fatalf("resume during archiving without stop id: %v", err)
	}
	if _, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: sbx.ID, SupersedeStopID: "stop_0000000000000000",
	}); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("resume with wrong stop id: %v", err)
	}
	if _, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: sbx.ID, SupersedeStopID: op.ID, MachineType: "large",
		Resources: db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 50},
	}); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("shape change during archiving: %v", err)
	}
	accepted, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: sbx.ID, SupersedeStopID: op.ID,
	})
	if err != nil {
		t.Fatalf("supersede resume: %v", err)
	}
	if accepted.Superseded == nil || accepted.Superseded.ID != op.ID ||
		accepted.Superseded.Status != db.StopSuperseded || accepted.Superseded.EndedAt == nil {
		t.Fatalf("superseded operation: %+v", accepted.Superseded)
	}
	if accepted.Sandbox.State != db.StateProvisioning || accepted.Sandbox.LifecycleGeneration != 1 {
		t.Fatalf("resumed during stop: %+v", accepted.Sandbox)
	}
	if accepted.Sandbox.Version != archiving.Version+1 {
		t.Fatalf("version after supersede: %d, want %d", accepted.Sandbox.Version, archiving.Version+1)
	}
	// The full reservation was already held while archiving: no second
	// one is added, even though the stop RPC may have completed on the node.
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10}) {
		t.Fatalf("node after supersede: %+v", got)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.stop' AND state = 'queued'`); n != 0 {
		t.Fatalf("queued stop jobs after supersede: %d", n)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.resume' AND state = 'queued'`); n != 1 {
		t.Fatalf("resume jobs after supersede: %d", n)
	}

	// A running stop attempt that lost the operation must fail closed and
	// release nothing.
	second := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	phase1BootDone(t, s, "boot-worker")
	op2, archiving2, _, _, err := s.AcceptSandboxStop(t.Context(), own, second.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	stopJob := phase1ClaimKind(t, s, "stop-worker", db.JobStop)
	if _, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: second.ID, SupersedeStopID: op2.ID,
	}); err != nil {
		t.Fatalf("supersede running stop: %v", err)
	}
	if _, _, err := s.FinalizeSandboxStop(t.Context(), stopJob.ID, "stop-worker", stopJob.LeaseToken, db.FinalizeStopParams{
		Owner: own, SandboxID: second.ID, StopID: op2.ID, ExpectedVersion: archiving2.Version,
	}); !errors.Is(err, db.ErrStopSuperseded) {
		t.Fatalf("stale stop finalize: %v", err)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 20}) {
		t.Fatalf("node after stale stop finalize: %+v", got)
	}
	if err := s.CompleteJob(t.Context(), stopJob.ID, "stop-worker", stopJob.LeaseToken); err != nil {
		t.Fatal(err)
	}
}

// TestSandboxResumeDuringFirstBootRefused proves a resume during archiving
// cannot abandon the initial disk creation before its first boot completed.
func TestSandboxResumeDuringFirstBootRefused(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	res := create(t, s, db.CreateSandboxParams{Owner: own}) // still provisioning, generation 0

	op, archiving, _, _, err := s.AcceptSandboxStop(t.Context(), own, res.Sandbox.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: res.Sandbox.ID, SupersedeStopID: op.ID,
	}); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("resume before first boot: %v", err)
	}
	if got, err := s.Sandbox(t.Context(), own, res.Sandbox.ID); err != nil || got.State != db.StateArchiving {
		t.Fatalf("sandbox after refused resume: %+v err=%v", got, err)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 1 {
		t.Fatalf("refused resume changed capacity: %+v", got)
	}

	// Once the initial boot job completed, the same resume supersedes the
	// stop and reuses the disk and reservation.
	phase1BootDone(t, s, "boot-worker")
	accepted, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: res.Sandbox.ID, SupersedeStopID: op.ID,
	})
	if err != nil {
		t.Fatalf("resume after first boot: %v", err)
	}
	if accepted.Sandbox.State != db.StateProvisioning || accepted.Sandbox.LifecycleGeneration != 1 {
		t.Fatalf("resumed sandbox: %+v", accepted.Sandbox)
	}
	if accepted.Superseded == nil || accepted.Superseded.Status != db.StopSuperseded || accepted.Superseded.EndedAt == nil {
		t.Fatalf("canonical superseded operation: %+v", accepted.Superseded)
	}
	if accepted.Superseded.ID != op.ID {
		t.Fatalf("superseded %s, want %s", accepted.Superseded.ID, op.ID)
	}
	if archiving.Version+1 != accepted.Sandbox.Version {
		t.Fatalf("version after supersede: %d, want %d", accepted.Sandbox.Version, archiving.Version+1)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10}) {
		t.Fatalf("node after supersede: %+v", got)
	}
}

// TestSandboxResumeStaleGenerationGuard proves a stale NONZERO generation
// reaches the sandbox generation guard with a valid old JobResume claim and
// fails without touching capacity or state.
func TestSandboxResumeStaleGenerationGuard(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	phase1BootDone(t, s, "boot-worker")
	phase1Stopped(t, s, own, sbx)

	first, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: sbx.ID})
	if err != nil || first.Sandbox.LifecycleGeneration != 1 {
		t.Fatalf("first resume: gen=%d err=%v", first.Sandbox.LifecycleGeneration, err)
	}
	oldJob := phase1ClaimKind(t, s, "old-worker", db.JobResume)

	// Stop the provisioning sandbox and resume again, producing generation
	// 2 while the old generation-1 claim is still live.
	stopOp, _, _, created, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false)
	if err != nil || !created {
		t.Fatalf("stop during provisioning: created=%v err=%v", created, err)
	}
	second, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: sbx.ID, SupersedeStopID: stopOp.ID,
	})
	if err != nil || second.Sandbox.LifecycleGeneration != 2 {
		t.Fatalf("second resume: gen=%d err=%v", second.Sandbox.LifecycleGeneration, err)
	}
	nodeBefore := nodeReserved(t, s, "node-a")

	// The old claim's payload names generation 1 and the bind passes; the
	// sandbox generation guard is what rejects it. Use the CURRENT version
	// so only that guard can explain the conflict.
	if _, err := s.FinalizeSandboxResume(t.Context(), oldJob.ID, "old-worker", oldJob.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID, Generation: 1, Version: second.Sandbox.Version,
	}); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("stale nonzero generation: %v", err)
	}
	after, err := s.Sandbox(t.Context(), own, sbx.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != db.StateProvisioning || after.LifecycleGeneration != 2 || after.Version != second.Sandbox.Version {
		t.Fatalf("state changed by stale finalize: %+v", after)
	}
	if got := nodeReserved(t, s, "node-a"); got != nodeBefore {
		t.Fatalf("capacity changed by stale finalize: %+v, want %+v", got, nodeBefore)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.resume' AND state = 'queued'`, sbx.ID); n != 1 {
		t.Fatalf("queued generation-2 resume: %d", n)
	}
	if state := phase1JobState(t, s, oldJob.ID); state != db.JobRunning {
		t.Fatalf("old claim state after stale finalize: %s", state)
	}
}

// TestSandboxResumeDiskGrowthAndRepeatedCycles proves only the positive disk
// delta is reserved on growth and the larger disk is retained exactly once
// across a following stop/resume cycle.
func TestSandboxResumeDiskGrowthAndRepeatedCycles(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	phase1BootDone(t, s, "boot-worker")
	phase1Stopped(t, s, own, sbx)

	grown, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: sbx.ID, MachineType: "large",
		Resources: db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 50},
	})
	if err != nil {
		t.Fatalf("grow resume: %v", err)
	}
	if grown.Sandbox.Desired != (db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 50}) ||
		grown.Sandbox.Reserved != grown.Sandbox.Desired {
		t.Fatalf("grown sandbox: desired=%+v reserved=%+v", grown.Sandbox.Desired, grown.Sandbox.Reserved)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 50}) {
		t.Fatalf("node after growth: %+v", got)
	}
	job := phase1ClaimKind(t, s, "resume-worker", db.JobResume)
	ready, err := s.FinalizeSandboxResume(t.Context(), job.ID, "resume-worker", job.LeaseToken, db.FinalizeResumeParams{
		Owner: own, SandboxID: sbx.ID, Generation: grown.Sandbox.LifecycleGeneration, Version: grown.Sandbox.Version,
	})
	if err != nil {
		t.Fatalf("finalize growth: %v", err)
	}
	if err := s.CompleteJob(t.Context(), job.ID, "resume-worker", job.LeaseToken); err != nil {
		t.Fatal(err)
	}

	// Stop the grown sandbox: compute leaves, the 50 GiB disk stays once.
	stopped, _ := phase1Stopped(t, s, own, ready)
	if stopped.Desired != (db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 50}) ||
		stopped.Reserved != (db.Resources{DiskGiB: 50}) {
		t.Fatalf("stopped grown sandbox: desired=%+v reserved=%+v", stopped.Desired, stopped.Reserved)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{DiskGiB: 50}) {
		t.Fatalf("node after grown stop: %+v", got)
	}

	// Resume with the stored shape: no disk reserve is repeated.
	again, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: sbx.ID})
	if err != nil {
		t.Fatalf("second resume: %v", err)
	}
	if again.Sandbox.LifecycleGeneration != 2 {
		t.Fatalf("second generation: %d", again.Sandbox.LifecycleGeneration)
	}
	if got := nodeReserved(t, s, "node-a"); got != (db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 50}) {
		t.Fatalf("node after second resume: %+v", got)
	}
	if again.Sandbox.Reserved.DiskGiB != 50 {
		t.Fatalf("disk reserved twice: %+v", again.Sandbox.Reserved)
	}
}

// TestSandboxResumeOwnerIsolation proves a foreign resume changes nothing:
// the row stays archived with its disk-only reservation and no job appears.
func TestSandboxResumeOwnerIsolation(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	other := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	phase1BootDone(t, s, "boot-worker")
	stopped, _ := phase1Stopped(t, s, own, sbx)
	before := nodeReserved(t, s, "node-a")

	if _, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: other, SandboxID: sbx.ID}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign resume: %v", err)
	}
	after, err := s.Sandbox(t.Context(), own, sbx.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != db.StateArchived || after.Version != stopped.Version || after.Reserved != stopped.Reserved {
		t.Fatalf("foreign resume changed the row: %+v", after)
	}
	if got := nodeReserved(t, s, "node-a"); got != before {
		t.Fatalf("foreign resume changed capacity: %+v, want %+v", got, before)
	}
	if n := count(t, s, `SELECT count(*) FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.resume'`, sbx.ID); n != 0 {
		t.Fatalf("foreign resume enqueued jobs: %d", n)
	}
}

func TestSandboxResumeTTLAndEnvOptions(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 16, MemoryMiB: 16384, DiskGiB: 200})

	// Omitted TTL keeps the configured duration and resets the deadline.
	ttl := 123
	keep := phase1CreateReady(t, s, own, db.CreateSandboxParams{TTLSeconds: &ttl})
	phase1Stopped(t, s, own, keep)
	accepted, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: keep.ID})
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Sandbox.TTLSeconds == nil || *accepted.Sandbox.TTLSeconds != ttl || accepted.Sandbox.ExpiresAt == nil {
		t.Fatalf("omitted ttl: %v %v", accepted.Sandbox.TTLSeconds, accepted.Sandbox.ExpiresAt)
	}
	if d := time.Until(*accepted.Sandbox.ExpiresAt); d < 110*time.Second || d > 130*time.Second {
		t.Fatalf("omitted ttl deadline in %s", d)
	}

	// Explicit null disables auto-stop for this sandbox.
	cleared := phase1CreateReady(t, s, own, db.CreateSandboxParams{TTLSeconds: &ttl})
	phase1Stopped(t, s, own, cleared)
	accepted, err = s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: cleared.ID,
		TTL: db.TTLOption{Set: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Sandbox.TTLSeconds != nil || accepted.Sandbox.ExpiresAt != nil {
		t.Fatalf("null ttl: %v %v", accepted.Sandbox.TTLSeconds, accepted.Sandbox.ExpiresAt)
	}

	// A value replaces both.
	value := 456
	replaced := phase1CreateReady(t, s, own, db.CreateSandboxParams{TTLSeconds: &ttl})
	phase1Stopped(t, s, own, replaced)
	accepted, err = s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: replaced.ID,
		TTL: db.TTLOption{Set: true, Seconds: &value},
	})
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Sandbox.TTLSeconds == nil || *accepted.Sandbox.TTLSeconds != value || accepted.Sandbox.ExpiresAt == nil {
		t.Fatalf("value ttl: %v %v", accepted.Sandbox.TTLSeconds, accepted.Sandbox.ExpiresAt)
	}
	if d := time.Until(*accepted.Sandbox.ExpiresAt); d < 445*time.Second || d > 465*time.Second {
		t.Fatalf("value ttl deadline in %s", d)
	}
	if _, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: keep.ID,
		TTL: db.TTLOption{Set: true, Seconds: new(0)},
	}); !errors.Is(err, db.ErrInvalidTTL) {
		t.Fatalf("invalid resume ttl: %v", err)
	}

	// Env: omitted keeps, explicit empty replaces with none, explicit
	// values replace.
	envKeep := phase1CreateReady(t, s, own, db.CreateSandboxParams{Env: map[string]string{"A": "1"}})
	phase1Stopped(t, s, own, envKeep)
	accepted, err = s.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: own, SandboxID: envKeep.ID})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.DecryptSandboxEnvForNode(t.Context(), own, envKeep.ID); err != nil || got["A"] != "1" {
		t.Fatalf("omitted env: %v %v", got, err)
	}
	if accepted.Sandbox.State != db.StateProvisioning {
		t.Fatalf("state: %s", accepted.Sandbox.State)
	}

	envClear := phase1CreateReady(t, s, own, db.CreateSandboxParams{Env: map[string]string{"A": "1"}})
	phase1Stopped(t, s, own, envClear)
	if _, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: envClear.ID,
		Env: db.EnvChange{Set: true},
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.DecryptSandboxEnvForNode(t.Context(), own, envClear.ID); err != nil || len(got) != 0 {
		t.Fatalf("explicit empty env: %v %v", got, err)
	}

	envSet := phase1CreateReady(t, s, own, db.CreateSandboxParams{Env: map[string]string{"A": "1"}})
	phase1Stopped(t, s, own, envSet)
	if _, err := s.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: envSet.ID,
		Env: db.EnvChange{Set: true, Values: map[string]string{"B": "2"}},
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.DecryptSandboxEnvForNode(t.Context(), own, envSet.ID); err != nil || got["B"] != "2" || len(got) != 1 {
		t.Fatalf("replaced env: %v %v", got, err)
	}
}
