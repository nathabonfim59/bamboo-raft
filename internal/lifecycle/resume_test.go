package lifecycle_test

// Durable resume worker tests (B1.2) against real PostgreSQL and nodefake
// through the gateway: retained-disk boots, retries that never clean up the
// disk, lease loss and redelivery, terminal failure preservation and the
// missing-disk outcome without CreateVM fallback. Gated by
// BBRAFT_TEST_DATABASE_URL.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// provisionReady drives a fresh sandbox's boot to a confirmed outcome: the
// row ready AND the provision job durably done. A single ProcessNext pass
// proves nothing — a transiently retried boot leaves the job queued — and
// every stop/resume test needs this precondition before it can assert
// anything about the paths it actually targets.
func provisionReady(t *testing.T, e *lifecycleEnv, own uuid.UUID, res db.CreateSandboxResult) *lifecycle.Worker {
	t.Helper()
	w := e.worker(t, lifecycle.Options{})
	deadline := time.Now().Add(10 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateReady ||
		e.jobStateFor(t, db.JobProvision, res.Sandbox.ID) != db.JobDone {
		if time.Now().After(deadline) {
			t.Fatalf("initial boot did not settle; state=%s provision=%s",
				e.sandbox(t, own, res.Sandbox.ID).State,
				e.jobStateFor(t, db.JobProvision, res.Sandbox.ID))
		}
		_, _ = w.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	return w
}

// stopThroughWorker archives a ready sandbox through the durable stop path.
func stopThroughWorker(t *testing.T, e *lifecycleEnv, own uuid.UUID, res db.CreateSandboxResult, w *lifecycle.Worker) db.Sandbox {
	t.Helper()
	e.acceptStop(t, own, res.Sandbox.ID, false)
	if !mustProcess(t, w) {
		t.Fatal("stop job was not claimed")
	}
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateArchived {
		t.Fatalf("sandbox after stop: %s", sbx.State)
	}
	return sbx
}

func TestResumeBootsRetainedDiskReady(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	secret := "resume-env-secret"
	res := e.createSandboxEnv(t, own, "node-a", 3, map[string]string{"TOKEN": secret})
	w := provisionReady(t, e, own, res)
	stopped := stopThroughWorker(t, e, own, res, w)
	desired := stopped.Desired

	accepted, err := e.store.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: res.Sandbox.ID,
	})
	if err != nil {
		t.Fatalf("accept resume: %v", err)
	}
	if accepted.Sandbox.LifecycleGeneration != 1 {
		t.Fatalf("resume generation: %d", accepted.Sandbox.LifecycleGeneration)
	}
	if !mustProcess(t, w) {
		t.Fatal("resume job was not claimed")
	}
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateReady || sbx.LifecycleGeneration != 1 || sbx.StoppedAt != nil {
		t.Fatalf("sandbox after resume: %+v", sbx)
	}
	if r := e.nodeReserved(t, "node-a"); r != desired {
		t.Fatalf("node after resume: %+v, want %+v", r, desired)
	}
	starts := n.startCalls()
	if len(starts) != 1 {
		t.Fatalf("StartVM calls: %d, want 1", len(starts))
	}
	if starts[0].GetSandboxId() != res.Sandbox.ID {
		t.Fatalf("StartVM target: %s", starts[0].GetSandboxId())
	}
	if starts[0].GetEnv()["TOKEN"] != secret {
		t.Fatal("sealed env was not passed to StartVM")
	}
	if starts[0].GetResources().GetDiskGib() != desired.DiskGiB {
		t.Fatalf("StartVM resources: %+v", starts[0].GetResources())
	}
	if e.jobStateFor(t, db.JobResume, res.Sandbox.ID) != db.JobDone {
		t.Fatalf("resume job state: %s", e.jobStateFor(t, db.JobResume, res.Sandbox.ID))
	}

	// A second stop/resume cycle advances the generation and keeps exactly
	// one reservation.
	stopThroughWorker(t, e, own, res, w)
	if _, err := e.store.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: res.Sandbox.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if !mustProcess(t, w) {
		t.Fatal("second resume job was not claimed")
	}
	sbx = e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateReady || sbx.LifecycleGeneration != 2 {
		t.Fatalf("sandbox after second resume: %+v", sbx)
	}
	if r := e.nodeReserved(t, "node-a"); r != desired {
		t.Fatalf("node after second resume: %+v, want exactly one %+v", r, desired)
	}
	if got := len(n.startCalls()); got != 2 {
		t.Fatalf("StartVM calls after two resumes: %d", got)
	}
}

func TestResumeRetryNeverDeletesRetainedDisk(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)
	stopped := stopThroughWorker(t, e, own, res, w)
	desired := stopped.Desired

	var calls atomic.Int32
	n.setStartHook(func(ctx context.Context, req *nodev1.StartVMRequest) (*nodev1.VM, error) {
		if calls.Add(1) == 1 {
			return nil, status.Error(codes.Unavailable, "link down")
		}
		return n.Node.StartVM(ctx, req)
	})
	if _, err := e.store.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: res.Sandbox.ID, MaxAttempts: 2,
	}); err != nil {
		t.Fatal(err)
	}

	// First attempt fails; the retry must not clean anything up.
	if !mustProcess(t, w) {
		t.Fatal("first resume attempt not claimed")
	}
	if got := len(n.deleteCalls()); got != 0 {
		t.Fatalf("failed resume attempt deleted the retained disk: %d", got)
	}
	if r := e.nodeReserved(t, "node-a"); r != desired {
		t.Fatalf("failed resume attempt changed the reservation: %+v", r)
	}
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.State != db.StateProvisioning {
		t.Fatalf("state after failed attempt: %s", sbx.State)
	}
	// Second attempt boots the retained disk.
	deadline := time.Now().Add(10 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateReady {
		if time.Now().After(deadline) {
			t.Fatal("resume retry did not finish")
		}
		_, _ = w.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	if r := e.nodeReserved(t, "node-a"); r != desired {
		t.Fatalf("node after retry: %+v", r)
	}
	if got := len(n.deleteCalls()); got != 0 {
		t.Fatalf("resume path deleted the disk: %d", got)
	}
	if got := len(n.startCalls()); got != 2 {
		t.Fatalf("StartVM calls: %d, want 2", got)
	}
}

func TestResumeLeaseLossMidBootRedelivers(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	n.Node.BootDelay = 400 * time.Millisecond
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)
	stopThroughWorker(t, e, own, res, w)

	if _, err := e.store.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: res.Sandbox.ID, MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}
	wA := e.worker(t, lifecycle.Options{Lease: 300 * time.Millisecond})
	wB := e.worker(t, lifecycle.Options{})

	started := make(chan struct{})
	var entered atomic.Bool
	n.setStartHook(func(ctx context.Context, req *nodev1.StartVMRequest) (*nodev1.VM, error) {
		if entered.CompareAndSwap(false, true) {
			close(started)
		}
		return n.Node.StartVM(ctx, req)
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = wA.ProcessNext(t.Context())
	}()
	<-started
	if _, err := e.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET leased_until = now() - interval '1 second'
		WHERE kind = 'sandbox.resume' AND sandbox_id = $1`, res.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("first resume attempt did not stop after lease loss")
	}
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.State != db.StateProvisioning {
		t.Fatalf("lost-lease attempt moved the row: %s", sbx.State)
	}

	deadline := time.Now().Add(10 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateReady {
		if time.Now().After(deadline) {
			t.Fatal("redelivered resume did not finish")
		}
		_, _ = wB.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	if r := e.nodeReserved(t, "node-a"); r != e.sandbox(t, own, res.Sandbox.ID).Desired {
		t.Fatalf("node after redelivery: %+v", r)
	}
	if got := len(n.deleteCalls()); got != 0 {
		t.Fatalf("lease loss deleted the retained disk: %d", got)
	}
	if got := len(n.startCalls()); got != 2 {
		t.Fatalf("StartVM calls: %d, want 2", got)
	}
}

func TestResumeFailureKeepsDiskAndArchives(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)
	stopped := stopThroughWorker(t, e, own, res, w)
	desired := stopped.Desired

	// A definitive StartVM refusal (the shape the node will never accept).
	n.setStartHook(func(context.Context, *nodev1.StartVMRequest) (*nodev1.VM, error) {
		return nil, status.Error(codes.InvalidArgument, "definitive refusal")
	})
	if _, err := e.store.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: res.Sandbox.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if !mustProcess(t, w) {
		t.Fatal("resume job not claimed")
	}

	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateArchived || sbx.ErrorReason != "boot_failed" {
		t.Fatalf("sandbox after failed resume: state=%s reason=%q", sbx.State, sbx.ErrorReason)
	}
	if sbx.StoppedAt == nil {
		t.Fatal("failed resume did not record stopped_at")
	}
	if sbx.LifecycleGeneration != 1 {
		t.Fatalf("generation changed: %d", sbx.LifecycleGeneration)
	}
	// Compute released once; the retained disk survives with the desired
	// shape; the initial-create cleanup never ran.
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != desired.DiskGiB {
		t.Fatalf("node after failed resume: %+v", r)
	}
	if sbx.Reserved.VCPUs != 0 || sbx.Reserved.MemoryMiB != 0 || sbx.Reserved.DiskGiB != desired.DiskGiB {
		t.Fatalf("reservation after failed resume: %+v", sbx.Reserved)
	}
	if got := len(n.deleteCalls()); got != 0 {
		t.Fatalf("failed resume deleted the VM: %d", got)
	}
	// StopVM confirmed the VM off with the disk kept before the finalize:
	// once for the original durable stop, once for the failure finalization.
	if vm, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID}); err != nil ||
		vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("VM after failed resume: %+v (%v)", vm, err)
	}
	if stops := n.stopCalls(); len(stops) != 2 || stops[0].GetSandboxId() != res.Sandbox.ID || stops[1].GetSandboxId() != res.Sandbox.ID {
		t.Fatalf("StopVM calls: %+v", stops)
	}
	if state := e.jobStateFor(t, db.JobResume, res.Sandbox.ID); state != db.JobFailed {
		t.Fatalf("resume job state: %s", state)
	}
}

func TestResumeMissingDiskEndsWithoutCreateFallback(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)
	stopThroughWorker(t, e, own, res, w)

	// The retained disk vanishes from the node.
	if _, err := n.Node.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: res.Sandbox.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: res.Sandbox.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if !mustProcess(t, w) {
		t.Fatal("resume job not claimed")
	}

	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateError || sbx.ErrorReason != "vm_missing" {
		t.Fatalf("sandbox after missing disk: state=%s reason=%q", sbx.State, sbx.ErrorReason)
	}
	if sbx.Reserved != (db.Resources{}) {
		t.Fatalf("reservations after missing disk: %+v", sbx.Reserved)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("node after missing disk: %+v", r)
	}
	// Never a CreateVM fallback, and the fence (DeleteVM) ran once.
	if got := len(n.createCalls()); got != 1 {
		t.Fatalf("CreateVM calls: %d, want only the initial boot", got)
	}
	if deletes := n.deleteCalls(); len(deletes) != 1 || deletes[0] != res.Sandbox.ID {
		t.Fatalf("DeleteVM fence calls: %v", deletes)
	}
	if state := e.jobStateFor(t, db.JobResume, res.Sandbox.ID); state != db.JobFailed {
		t.Fatalf("resume job state: %s", state)
	}
}

func TestStaleResumeGenerationNeverBoots(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)
	stopThroughWorker(t, e, own, res, w)

	// Generation 1 is accepted, boots and is stopped again; generation 2 is
	// accepted afterwards.
	first, err := e.store.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: res.Sandbox.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	mustProcess(t, w)
	stopThroughWorker(t, e, own, res, w)
	if _, err := e.store.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: res.Sandbox.ID,
	}); err != nil {
		t.Fatal(err)
	}
	mustProcess(t, w)
	if got := len(n.startCalls()); got != 2 {
		t.Fatalf("StartVM calls: %d, want 2", got)
	}

	// A redelivered generation-1 job must not touch the later boot.
	if _, err := e.store.EnqueueJob(t.Context(), db.JobInput{
		Kind: db.JobResume, OwnerID: own, SandboxID: &res.Sandbox.ID,
		Payload: first.Job.Payload, MaxAttempts: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if !mustProcess(t, w) {
		t.Fatal("stale resume job not claimed")
	}
	if got := len(n.startCalls()); got != 2 {
		t.Fatalf("stale resume generation booted: %d StartVM calls", got)
	}
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.State != db.StateReady || sbx.LifecycleGeneration != 2 {
		t.Fatalf("sandbox after stale delivery: %+v", sbx)
	}
}

// patchRaceStore arms the deterministic regressions around a finalize
// conflict: the first FinalizeSandboxResume is preceded by a real PATCH (the
// row version moves between the worker's read and its finalize, exactly the
// racing-writer window), and the first post-conflict reread can fail, so the
// handler must retry rather than treat either the version conflict or the
// failed reread as decided work.
type patchRaceStore struct {
	*db.Store
	mu          sync.Mutex
	finalizes   int
	rereadFired bool
	failReread  bool
	owner       uuid.UUID
	sandboxID   string
}

func (s *patchRaceStore) FinalizeSandboxResume(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p db.FinalizeResumeParams) (db.Sandbox, error) {
	s.mu.Lock()
	first := s.finalizes == 0
	s.finalizes++
	s.mu.Unlock()
	if first {
		newName := "renamed-under-resume"
		if _, err := s.Store.PatchSandbox(ctx, db.PatchSandboxParams{
			Owner: s.owner, SandboxID: s.sandboxID, Name: &newName,
		}); err != nil {
			return db.Sandbox{}, err
		}
	}
	return s.Store.FinalizeSandboxResume(ctx, jobID, worker, token, p)
}

func (s *patchRaceStore) SandboxAny(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error) {
	s.mu.Lock()
	armed := s.failReread && s.finalizes >= 1 && !s.rereadFired
	if armed {
		s.rereadFired = true
	}
	s.mu.Unlock()
	if armed {
		return db.Sandbox{}, errors.New("reread failed sentinel")
	}
	return s.Store.SandboxAny(ctx, owner, id)
}

// TestResumeFinalizeConflictConverges proves a PATCH racing the finalize does
// not strand a provisioning row: the conflict (and, in the second variant, a
// failing reread) retries, the next attempt finalizes with the fresh version,
// and capacity is reserved exactly once.
func TestResumeFinalizeConflictConverges(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failReread bool
	}{
		{name: "patch race", failReread: false},
		{name: "patch race with failing reread", failReread: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newLifecycleEnv(t)
			e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
			n := e.startNode(t, "node-a")
			own := e.owner(t)
			res := e.createSandbox(t, own, "node-a", 5)
			w := provisionReady(t, e, own, res)
			stopped := stopThroughWorker(t, e, own, res, w)
			desired := stopped.Desired

			if _, err := e.store.AcceptSandboxResume(t.Context(), db.ResumeParams{
				Owner: own, SandboxID: res.Sandbox.ID, MaxAttempts: 5,
			}); err != nil {
				t.Fatal(err)
			}
			wrapped := &patchRaceStore{
				Store: e.store, failReread: tc.failReread,
				owner: own, sandboxID: res.Sandbox.ID,
			}
			wRace := e.workerWithStore(t, wrapped, lifecycle.Options{})

			deadline := time.Now().Add(10 * time.Second)
			for e.sandbox(t, own, res.Sandbox.ID).State != db.StateReady {
				if time.Now().After(deadline) {
					t.Fatalf("resume did not converge; state=%s job=%s",
						e.sandbox(t, own, res.Sandbox.ID).State, e.jobStateFor(t, db.JobResume, res.Sandbox.ID))
				}
				_, _ = wRace.ProcessNext(t.Context())
				time.Sleep(5 * time.Millisecond)
			}
			sbx := e.sandbox(t, own, res.Sandbox.ID)
			if sbx.LifecycleGeneration != 1 || sbx.Name != "renamed-under-resume" {
				t.Fatalf("converged row: %+v", sbx)
			}
			if r := e.nodeReserved(t, "node-a"); r != desired {
				t.Fatalf("capacity leaked or double-reserved: %+v, want %+v", r, desired)
			}
			if e.jobStateFor(t, db.JobResume, res.Sandbox.ID) != db.JobDone {
				t.Fatalf("resume job: %s", e.jobStateFor(t, db.JobResume, res.Sandbox.ID))
			}
			if got := len(n.startCalls()); got < 2 {
				t.Fatalf("StartVM calls: %d, want the retried attempt", got)
			}
		})
	}
}
