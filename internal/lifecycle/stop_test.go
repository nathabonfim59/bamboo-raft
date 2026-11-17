package lifecycle_test

// Durable stop worker tests (B1.2) against real PostgreSQL and nodefake
// through the gateway: archival, initial-boot ordering, retries, resume
// supersession mid-RPC, PATCH races, force escalation, and the terminal
// missing-disk outcome with its one-WARN boundary. Gated by
// BBRAFT_TEST_DATABASE_URL.

import (
	"context"
	"log/slog"
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

// captureHandler collects log records so tests can assert the one-WARN
// boundary of the terminal missing-disk outcome.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

func (h *captureHandler) count(message string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Message == message {
			n++
		}
	}
	return n
}

// acceptStop accepts a durable stop through the store (the API path) and
// waits for nothing: the caller drives the worker.
func (e *lifecycleEnv) acceptStop(t *testing.T, own uuid.UUID, sandboxID string, force bool) db.StopOperation {
	t.Helper()
	op, _, _, _, err := e.store.AcceptSandboxStop(t.Context(), own, sandboxID, force)
	if err != nil {
		t.Fatalf("accept stop: %v", err)
	}
	return op
}

func TestStopArchivesSandboxAndReleasesComputeOnce(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)
	w := e.worker(t, lifecycle.Options{})
	mustProcess(t, w) // initial boot

	op := e.acceptStop(t, own, res.Sandbox.ID, false)
	if !mustProcess(t, w) {
		t.Fatal("stop job was not claimed")
	}

	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateArchived || sbx.StoppedAt == nil {
		t.Fatalf("sandbox after stop: %+v", sbx)
	}
	desired := sbx.Desired
	if desired.DiskGiB == 0 {
		t.Fatal("fixture has no retained disk")
	}
	// Compute released exactly once, disk retained: one more idle pass
	// changes nothing.
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != desired.DiskGiB {
		t.Fatalf("node after stop: %+v", r)
	}
	if worked := mustProcess(t, w); worked {
		t.Fatal("an extra job ran after archival")
	}
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != desired.DiskGiB {
		t.Fatalf("node changed after idle pass: %+v", r)
	}
	// The VM is STOPPED with its disk on the node, and the operation is
	// completed.
	if vm, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID}); err != nil ||
		vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("VM after stop: %+v (%v)", vm, err)
	}
	if final, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID); err != nil ||
		final.ID != op.ID || final.Status != db.StopCompleted || final.EndedAt == nil {
		t.Fatalf("stop operation: %+v (%v)", final, err)
	}
	if e.jobStateFor(t, db.JobStop, res.Sandbox.ID) != db.JobDone {
		t.Fatalf("stop job state: %s", e.jobStateFor(t, db.JobStop, res.Sandbox.ID))
	}
}

// jobStateFor reads the newest job of one kind for a sandbox.
func (e *lifecycleEnv) jobStateFor(t *testing.T, kind db.JobKind, sandboxID string) db.JobState {
	t.Helper()
	var state db.JobState
	if err := e.store.Pool().QueryRow(t.Context(), `
		SELECT state FROM jobs WHERE kind = $1 AND sandbox_id = $2
		ORDER BY created_at DESC, id DESC LIMIT 1`, string(kind), sandboxID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestPendingStopDoesNotStarveInitialProvisionSingleWorker(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3) // provision job still queued

	// Force the stop job to claim first.
	op := e.acceptStop(t, own, res.Sandbox.ID, false)
	if _, err := e.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET run_after = now() + interval '1 hour'
		WHERE kind = 'sandbox.provision' AND sandbox_id = $1`, res.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	w := e.worker(t, lifecycle.Options{})
	if !mustProcess(t, w) {
		t.Fatal("stop job was not claimed first")
	}
	// The pending stop must retry WITHOUT touching the node or taking the
	// guard: no boot was stopped or skipped, and the job is requeued.
	if got := len(n.createCalls()); got != 0 {
		t.Fatalf("stop attempt reached CreateVM: %d", got)
	}
	if got := len(n.stopCalls()); got != 0 {
		t.Fatalf("stop attempt reached StopVM: %d", got)
	}
	if state := e.jobStateFor(t, db.JobStop, res.Sandbox.ID); state != db.JobQueued {
		t.Fatalf("pending stop job state: %s, want queued", state)
	}
	if op2, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID); err != nil || op2.ID != op.ID || op2.Status != db.StopPending {
		t.Fatalf("pending stop operation: %+v (%v)", op2, err)
	}

	// Release the boot: one worker still finishes both jobs in order.
	if _, err := e.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET run_after = now() - interval '1 second'
		WHERE kind = 'sandbox.provision' AND sandbox_id = $1`, res.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateArchived {
		if time.Now().After(deadline) {
			t.Fatalf("stop did not finish after the boot; state=%s job=%s",
				e.sandbox(t, own, res.Sandbox.ID).State, e.jobStateFor(t, db.JobStop, res.Sandbox.ID))
		}
		_, _ = w.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	// The disk the boot created was retained, not destroyed.
	if got := len(n.createCalls()); got != 1 {
		t.Fatalf("CreateVM calls: %d, want 1", got)
	}
	if stops := n.stopCalls(); len(stops) != 1 || stops[0].GetSandboxId() != res.Sandbox.ID {
		t.Fatalf("StopVM calls: %+v", stops)
	}
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != sbx.Desired.DiskGiB {
		t.Fatalf("node after delayed stop: %+v", r)
	}
}

func TestPendingStopDoesNotStarveInitialProvisionTwoWorkers(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	n.Node.BootDelay = 200 * time.Millisecond
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 5)
	e.acceptStop(t, own, res.Sandbox.ID, false)

	w := e.worker(t, lifecycle.Options{Workers: 2})
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	runDone := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		runDone <- w.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-finished
	})
	waitFor(t, "sandbox archived with its disk", func() bool {
		return e.sandbox(t, own, res.Sandbox.ID).State == db.StateArchived
	})
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not join")
	}
	// The boot happened (the stop found a real disk), exactly once each.
	if got := len(n.createCalls()); got != 1 {
		t.Fatalf("CreateVM calls: %d, want 1", got)
	}
	if got := len(n.stopCalls()); got != 1 {
		t.Fatalf("StopVM calls: %d, want 1", got)
	}
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != sbx.Desired.DiskGiB {
		t.Fatalf("node after stop: %+v", r)
	}
}

func TestStopRetriesAfterNodeDisconnect(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	// A transiently retried boot leaves the provision job queued; the stop
	// attempt would then legitimately take the preliminary "initial boot in
	// flight" retry instead of the StopVM retry this test targets.
	w := provisionReady(t, e, own, res)

	var calls atomic.Int32
	n.setStopHook(func(ctx context.Context, req *nodev1.StopVMRequest) (*nodev1.VM, error) {
		if calls.Add(1) == 1 {
			return nil, status.Error(codes.Unavailable, "link down")
		}
		return n.Node.StopVM(ctx, req)
	})
	e.acceptStop(t, own, res.Sandbox.ID, false)

	// The failed attempt is recorded on the canonical operation and
	// retried. The hook counter proves the StopVM path itself ran; the
	// retry cause classifies any preliminary retry instead. The boot phase
	// is over, so give the stop phase its own deadline.
	deadline := time.Now().Add(10 * time.Second)
	for {
		op, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID)
		if err != nil {
			t.Fatal(err)
		}
		if op.Status == db.StopFailing && op.LastAttemptAt != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stop attempt did not record its failure; op=%+v hook=%d provision=%s cause=%q",
				op, calls.Load(), e.jobStateFor(t, db.JobProvision, res.Sandbox.ID),
				e.stopJobLastError(t, res.Sandbox.ID))
		}
		_, _ = w.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("StopVM hook ran %d times before the recorded failure, want 1", got)
	}
	if e.sandbox(t, own, res.Sandbox.ID).State != db.StateArchiving {
		t.Fatal("failed attempt changed the state")
	}
	if r := e.nodeReserved(t, "node-a"); r != e.sandbox(t, own, res.Sandbox.ID).Reserved {
		t.Fatalf("failed attempt changed the reservation: %+v", r)
	}
	deadline = time.Now().Add(10 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateArchived {
		if time.Now().After(deadline) {
			t.Fatal("stop retry did not archive")
		}
		_, _ = w.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	if op, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID); err != nil || op.Status != db.StopCompleted {
		t.Fatalf("operation after retry: %+v (%v)", op, err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("StopVM hook ran %d times in total, want 2", got)
	}
}

func TestResumeWhileStopRPCInFlightSupersedesStop(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := e.worker(t, lifecycle.Options{})
	mustProcess(t, w) // boot

	inStop := make(chan struct{})
	release := make(chan struct{})
	var (
		enteredOnce sync.Once
		releaseOnce sync.Once
	)
	n.setStopHook(func(ctx context.Context, req *nodev1.StopVMRequest) (*nodev1.VM, error) {
		enteredOnce.Do(func() { close(inStop) })
		<-release
		return n.Node.StopVM(ctx, req)
	})
	op := e.acceptStop(t, own, res.Sandbox.ID, false)

	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		_, _ = w.ProcessNext(t.Context())
	}()
	<-inStop
	// The resume arrives while StopVM is in flight: it supersedes the stop
	// and keeps the full reservation for the same machine.
	accepted, err := e.store.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: res.Sandbox.ID, SupersedeStopID: op.ID,
	})
	if err != nil {
		t.Fatalf("accept resume during stop: %v", err)
	}
	if accepted.Superseded == nil || accepted.Superseded.ID != op.ID {
		t.Fatalf("resume did not supersede: %+v", accepted.Superseded)
	}
	releaseOnce.Do(func() { close(release) })
	<-stopDone

	// The stop worker observed the supersession without releasing anything.
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateProvisioning {
		t.Fatalf("state after superseded stop: %s", sbx.State)
	}
	if r := e.nodeReserved(t, "node-a"); r != sbx.Desired {
		t.Fatalf("reservation after superseded stop: %+v, want the full %+v", r, sbx.Desired)
	}
	if final, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID); err != nil || final.Status != db.StopSuperseded {
		t.Fatalf("stop operation: %+v (%v)", final, err)
	}

	// The resume then boots the retained disk on the same machine: exactly
	// one StartVM, no double reservation.
	mustProcess(t, w)
	sbx = e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateReady || sbx.LifecycleGeneration != 1 {
		t.Fatalf("sandbox after resume: %+v", sbx)
	}
	if r := e.nodeReserved(t, "node-a"); r != sbx.Desired {
		t.Fatalf("node after resume: %+v, want exactly one %+v", r, sbx.Desired)
	}
	if got := len(n.startCalls()); got != 1 {
		t.Fatalf("StartVM calls: %d, want 1", got)
	}
	if got := len(n.stopCalls()); got != 1 {
		t.Fatalf("StopVM calls: %d, want 1", got)
	}
}

func TestStopPATCHRaceStillArchives(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := e.worker(t, lifecycle.Options{})
	mustProcess(t, w) // boot

	patched := make(chan struct{})
	n.setStopHook(func(ctx context.Context, req *nodev1.StopVMRequest) (*nodev1.VM, error) {
		select {
		case <-patched:
		default:
			newName := "renamed-under-stop"
			if _, err := e.store.PatchSandbox(ctx, db.PatchSandboxParams{
				Owner: own, SandboxID: req.GetSandboxId(), Name: &newName,
			}); err != nil {
				return nil, status.Error(codes.Internal, "patch failed")
			}
			close(patched)
		}
		return n.Node.StopVM(ctx, req)
	})
	op := e.acceptStop(t, own, res.Sandbox.ID, false)

	// The PATCH bumps the row version during the RPC; the finalize conflict
	// costs one requeue and the next attempt archives.
	deadline := time.Now().Add(10 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateArchived {
		if time.Now().After(deadline) {
			t.Fatalf("PATCH race lost the archive; state=%s job=%s",
				e.sandbox(t, own, res.Sandbox.ID).State, e.jobStateFor(t, db.JobStop, res.Sandbox.ID))
		}
		_, _ = w.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	if final, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID); err != nil || final.ID != op.ID || final.Status != db.StopCompleted {
		t.Fatalf("stop operation: %+v (%v)", final, err)
	}
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.Name != "renamed-under-stop" {
		t.Fatalf("PATCH lost: %q", sbx.Name)
	}
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != e.sandbox(t, own, res.Sandbox.ID).Desired.DiskGiB {
		t.Fatalf("node after PATCH race: %+v", r)
	}
}

func TestStopForceEscalationHonoredByWorker(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := e.worker(t, lifecycle.Options{})
	mustProcess(t, w) // boot

	op := e.acceptStop(t, own, res.Sandbox.ID, false)
	// A repeated forced stop persists the escalation on the canonical
	// operation; the worker must honor it despite the stale payload.
	if escalated, _, _, _, err := e.store.AcceptSandboxStop(t.Context(), own, res.Sandbox.ID, true); err != nil ||
		!escalated.ForceRequested || escalated.ID != op.ID {
		t.Fatalf("force escalation: %+v (%v)", escalated, err)
	}
	if !mustProcess(t, w) {
		t.Fatal("stop job not claimed")
	}
	stops := n.stopCalls()
	if len(stops) != 1 || stops[0].GetGraceSeconds() != 1 {
		t.Fatalf("StopVM calls: %+v, want forced grace 1s", stops)
	}
	final, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID)
	if err != nil || final.Status != db.StopCompleted || !final.Forced {
		t.Fatalf("forced stop operation: %+v (%v)", final, err)
	}
	if e.sandbox(t, own, res.Sandbox.ID).State != db.StateArchived {
		t.Fatal("forced stop did not archive")
	}
}

func TestStopTargetMissingEndsInTerminalOutcome(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := e.worker(t, lifecycle.Options{})
	mustProcess(t, w) // boot

	// The VM and its disk vanish from the node (a rebuilt node, or an
	// operator's cleanup): remove them directly, outside the worker.
	if _, err := n.Node.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: res.Sandbox.ID}); err != nil {
		t.Fatal(err)
	}
	capture := &captureHandler{}
	wLog := e.worker(t, lifecycle.Options{Logger: slog.New(capture)})
	op := e.acceptStop(t, own, res.Sandbox.ID, false)

	if !mustProcess(t, wLog) {
		t.Fatal("stop job not claimed")
	}
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateError || sbx.ErrorReason != "vm_missing" {
		t.Fatalf("sandbox after missing target: state=%s reason=%q", sbx.State, sbx.ErrorReason)
	}
	if sbx.Reserved != (db.Resources{}) {
		t.Fatalf("reservations not released: %+v", sbx.Reserved)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("node after missing target: %+v", r)
	}
	final, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID)
	if err != nil || final.ID != op.ID || final.Status != db.StopFailing || final.Error != "vm_missing" {
		t.Fatalf("stop operation: %+v (%v)", final, err)
	}
	if state := e.jobStateFor(t, db.JobStop, res.Sandbox.ID); state != db.JobFailed {
		t.Fatalf("stop job after terminal outcome: %s", state)
	}
	if got := capture.count("stop target missing; sandbox failed"); got != 1 {
		t.Fatalf("terminal outcome WARN count: %d, want 1", got)
	}
	// The owner-visible row stays queryable with its stop metadata.
	if _, err := e.store.Sandbox(t.Context(), own, res.Sandbox.ID); err != nil {
		t.Fatalf("owner-visible read: %v", err)
	}

	// A repeated reaper round neither requeues the canonical outcome nor
	// warns again, and the reservations stay released exactly once.
	wLog.ReapOnce(t.Context())
	if got := capture.count("stop target missing; sandbox failed"); got != 1 {
		t.Fatalf("WARN after reaper: %d, want 1", got)
	}
	if e.count(t, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.stop' AND state = 'queued'`) != 0 {
		t.Fatal("reaper requeued the canonical missing-disk stop")
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("node after reaper: %+v", r)
	}

	// Duplicate delivery of the same stop work also stays silent: nothing is
	// released a second time and no new WARN appears.
	var payload string
	if err := e.store.Pool().QueryRow(t.Context(), `
		SELECT payload FROM jobs WHERE kind = 'sandbox.stop' AND sandbox_id = $1
		ORDER BY created_at DESC, id DESC LIMIT 1`, res.Sandbox.ID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.EnqueueJob(t.Context(), db.JobInput{
		Kind: db.JobStop, OwnerID: own, SandboxID: &res.Sandbox.ID, Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	if !mustProcess(t, wLog) {
		t.Fatal("duplicate stop delivery not claimed")
	}
	if got := capture.count("stop target missing; sandbox failed"); got != 1 {
		t.Fatalf("WARN after duplicate delivery: %d, want 1", got)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("node after duplicate delivery: %+v", r)
	}

	// A normal DELETE still completes for the failed sandbox.
	if _, _, err := e.store.AcceptSandboxDeletion(t.Context(), own, res.Sandbox.ID, db.DeletionExplicit); err != nil {
		t.Fatalf("delete after terminal stop: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var completed int
		if err := e.store.Pool().QueryRow(t.Context(),
			`SELECT count(*) FROM deletion_operations WHERE target_id = $1 AND status = 'completed'`,
			res.Sandbox.ID).Scan(&completed); err != nil {
			t.Fatal(err)
		}
		if completed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("deletion after terminal stop did not complete")
		}
		_, _ = wLog.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("node after deletion: %+v", r)
	}
}

// TestReconcileFailedStopStillRetriesTransientMissingDiskRecord is the
// regression for the reaper's skip condition: an operation carrying the
// vm_missing error while the sandbox is STILL ARCHIVING (the safe transient
// StopVM NOT_FOUND path) must keep retrying; only the complete terminal
// outcome (error row under the failing operation) is never requeued.
func TestReconcileFailedStopStillRetriesTransientMissingDiskRecord(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := e.worker(t, lifecycle.Options{})
	mustProcess(t, w) // boot: the VM exists and is READY

	op := e.acceptStop(t, own, res.Sandbox.ID, false)
	// Synthesize the transient record: claim the stop job, record a failed
	// attempt with the vm_missing reason, and terminally fail the job while
	// the row is still archiving with the VM present.
	job, err := e.store.ClaimJob(t.Context(), "synthetic", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.Kind != db.JobStop {
		t.Fatalf("claimed %s, want sandbox.stop", job.Kind)
	}
	if _, err := e.store.FailSandboxStopAttempt(t.Context(), job.ID, "synthetic", job.LeaseToken, own, op.ID, "vm_missing"); err != nil {
		t.Fatal(err)
	}
	if err := e.store.FailJob(t.Context(), job.ID, "synthetic", job.LeaseToken, "vm_missing"); err != nil {
		t.Fatal(err)
	}

	capture := &captureHandler{}
	wLog := e.worker(t, lifecycle.Options{Logger: slog.New(capture)})
	wLog.ReapOnce(t.Context())

	if e.count(t, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.stop' AND state = 'queued'`) != 1 {
		t.Fatal("reaper did not requeue the transient missing-disk stop record")
	}
	if got := capture.count("stop target missing; sandbox failed"); got != 0 {
		t.Fatalf("reaper warned: %d", got)
	}
	// The requeued stop resolves normally: the VM exists, so the stop
	// archives it with the retained disk.
	deadline := time.Now().Add(10 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateArchived {
		if time.Now().After(deadline) {
			t.Fatalf("requeued stop did not archive; state=%s", e.sandbox(t, own, res.Sandbox.ID).State)
		}
		_, _ = wLog.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != e.sandbox(t, own, res.Sandbox.ID).Desired.DiskGiB {
		t.Fatalf("node after resolved transient stop: %+v", r)
	}
	if got := len(n.stopCalls()); got != 1 {
		t.Fatalf("StopVM calls: %d, want 1", got)
	}
}

// stopJobLastError reads the newest stop job's retry cause for diagnosing
// which preliminary retry an attempt took.
func (e *lifecycleEnv) stopJobLastError(t *testing.T, sandboxID string) string {
	t.Helper()
	var cause string
	if err := e.store.Pool().QueryRow(t.Context(), `
		SELECT last_error FROM jobs WHERE kind = 'sandbox.stop' AND sandbox_id = $1
		ORDER BY created_at DESC, id DESC LIMIT 1`, sandboxID).Scan(&cause); err != nil {
		t.Fatal(err)
	}
	return cause
}

// TestStopRetryWaitsForRetriedInitialBoot reproduces the integrated-make-check
// flake deterministically: the first boot attempt fails with a transient
// error (so the provision job requeues and the sandbox stays provisioning),
// and with the naive single-ProcessNext boot step the stop attempt then takes
// the preliminary "initial boot in flight" retry — leaving the operation
// pending with no attempt recorded, exactly the observed failure. The robust
// version drives the boot to a confirmed ready + done-job precondition
// before exercising the StopVM retry path.
func TestStopRetryWaitsForRetriedInitialBoot(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 4)

	var boots atomic.Int32
	n.setCreateHook(func(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
		if boots.Add(1) == 1 {
			return nil, status.Error(codes.Unavailable, "boot link down")
		}
		return n.Node.CreateVM(ctx, req)
	})
	// The helper is the confirmed precondition: ready row AND durably done
	// provision job. A retried boot leaves the job queued, and the stop
	// attempt would then legitimately wait for it.
	w := provisionReady(t, e, own, res)
	if got := boots.Load(); got != 2 {
		t.Fatalf("boot attempts: %d, want the forced retry plus success", got)
	}

	// The stop's first attempt fails on the node; the canonical operation
	// must record it (failing + LastAttemptAt) — proving the StopVM retry
	// path was reached rather than a preliminary retry.
	var stops atomic.Int32
	n.setStopHook(func(ctx context.Context, req *nodev1.StopVMRequest) (*nodev1.VM, error) {
		if stops.Add(1) == 1 {
			return nil, status.Error(codes.Unavailable, "stop link down")
		}
		return n.Node.StopVM(ctx, req)
	})
	e.acceptStop(t, own, res.Sandbox.ID, false)

	deadline := time.Now().Add(10 * time.Second)
	for {
		op, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID)
		if err != nil {
			t.Fatal(err)
		}
		if op.Status == db.StopFailing {
			if op.LastAttemptAt == nil {
				t.Fatalf("failing operation without an attempt: %+v", op)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stop attempt did not record its failure; op=%+v provision=%s cause=%q",
				op, e.jobStateFor(t, db.JobProvision, res.Sandbox.ID),
				e.stopJobLastError(t, res.Sandbox.ID))
		}
		_, _ = w.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	if got := stops.Load(); got != 1 {
		t.Fatalf("StopVM hook ran %d times before the recorded failure, want 1", got)
	}
	if r := e.nodeReserved(t, "node-a"); r != res.Sandbox.Reserved {
		t.Fatalf("failed stop attempt changed the reservation: %+v", r)
	}

	// The retry archives the sandbox and completes the operation.
	deadline = time.Now().Add(10 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateArchived {
		if time.Now().After(deadline) {
			t.Fatalf("stop retry did not archive; state=%s cause=%q",
				e.sandbox(t, own, res.Sandbox.ID).State, e.stopJobLastError(t, res.Sandbox.ID))
		}
		_, _ = w.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	op, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID)
	if err != nil || op.Status != db.StopCompleted {
		t.Fatalf("stop operation after retry: %+v (%v)", op, err)
	}
	if got := stops.Load(); got != 2 {
		t.Fatalf("StopVM hook ran %d times in total, want 2", got)
	}
}
