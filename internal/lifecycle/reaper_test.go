package lifecycle_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// countingStore wraps the real store to observe every failed-job page read.
type countingStore struct {
	*db.Store
	mu    sync.Mutex
	seen  map[uuid.UUID]int
	walks int
}

func (c *countingStore) FailedJobs(ctx context.Context, after *db.FailedJobCursor, limit int) ([]db.Job, *db.FailedJobCursor, error) {
	jobs, next, err := c.Store.FailedJobs(ctx, after, limit)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.walks++
	for _, j := range jobs {
		c.seen[j.ID]++
	}
	return jobs, next, err
}

func (c *countingStore) counts() (walks int, seen map[uuid.UUID]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[uuid.UUID]int, len(c.seen))
	for id, n := range c.seen {
		out[id] = n
	}
	return c.walks, out
}

type failingAcceptStore struct {
	*db.Store
	mu    sync.Mutex
	calls int
}

func (s *failingAcceptStore) AcceptSandboxExpiryStop(context.Context, uuid.UUID, string, int64, time.Time, bool) (db.StopOperation, db.Sandbox, db.Job, bool, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return db.StopOperation{}, db.Sandbox{}, db.Job{}, false, errors.New("accept failed sentinel")
}

func (s *failingAcceptStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestTTLFullPageWithoutProgressDoesNotSpin(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 256, MemoryMiB: 1 << 20, DiskGiB: 4000})
	own := e.owner(t)
	for range 100 {
		e.createSandbox(t, own, "node-a", 2)
	}
	if _, err := e.store.Pool().Exec(t.Context(),
		`UPDATE sandboxes SET expires_at = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	fst := &failingAcceptStore{Store: e.store}
	w := e.workerWithStore(t, fst, lifecycle.Options{})
	done := make(chan struct{})
	go func() { w.ReapOnce(t.Context()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("TTL reaper spun on a full page with no progress")
	}
	if got := fst.callCount(); got != deletionReaperPageSizeForTest {
		t.Fatalf("auto-stop accept attempts: %d, want one page", got)
	}
	if e.count(t, `SELECT count(*) FROM stop_operations`) != 0 {
		t.Fatal("failed acceptance still recorded stops")
	}
}

// deletionReaperPageSizeForTest mirrors lifecycle's page size without
// exporting implementation detail.
const deletionReaperPageSizeForTest = 100

func TestFailedJobWalkCoversEveryPageAndRestarts(t *testing.T) {
	e := newLifecycleEnv(t)
	own := e.owner(t)
	const failed = 250
	for range failed {
		if _, err := e.store.Pool().Exec(t.Context(), `
			INSERT INTO jobs (id, kind, owner_id, payload, state, attempts, max_attempts, last_error)
			VALUES ($1, 'snapshot.save', $2, '{}'::jsonb, 'failed', 1, 1, 'not ours yet')`,
			uuid.NewV7(), own); err != nil {
			t.Fatal(err)
		}
	}
	counting := &countingStore{Store: e.store, seen: map[uuid.UUID]int{}}
	w := e.workerWithStore(t, counting, lifecycle.Options{})

	w.ReapOnce(t.Context())
	walks, seen := counting.counts()
	if len(seen) != failed {
		t.Fatalf("first walk saw %d failed jobs, want %d", len(seen), failed)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s seen %d times in one walk", id, n)
		}
	}
	if walks < 3 {
		t.Fatalf("walk used %d pages, want at least 3", walks)
	}

	// Every reconciliation round restarts at the oldest row, so no failure
	// can starve behind newer ones.
	w.ReapOnce(t.Context())
	_, seen = counting.counts()
	if len(seen) != failed {
		t.Fatalf("second walk saw %d jobs", len(seen))
	}
	for id, n := range seen {
		if n != 2 {
			t.Fatalf("job %s seen %d times after two rounds", id, n)
		}
	}
	if n := e.count(t, `SELECT count(*) FROM jobs WHERE state = 'failed'`); n != failed {
		t.Fatalf("failed rows: %d", n)
	}
}

func TestFailedProvisionReconciliationRecordsErrorAndReleases(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)
	job, err := e.store.ClaimJob(t.Context(), "crashed-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.FailJob(t.Context(), job.ID, "crashed-worker", job.LeaseToken, "crashed before commit"); err != nil {
		t.Fatal(err)
	}

	w := e.worker(t, lifecycle.Options{})
	w.ReapOnce(t.Context())

	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateError || sbx.ErrorReason != "provision_failed" {
		t.Fatalf("reconciled sandbox: state=%s reason=%q", sbx.State, sbx.ErrorReason)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("reservation: %+v", r)
	}
	if !slicesContain(n.deleteCalls(), res.Sandbox.ID) {
		t.Fatalf("DeleteVM calls: %v", n.deleteCalls())
	}
}

func TestFailedDeleteReconciliationBlocksAndRequeues(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)
	if _, _, err := e.store.AcceptSandboxDeletion(t.Context(), own, res.Sandbox.ID, db.DeletionExplicit); err != nil {
		t.Fatal(err)
	}
	op := mustActiveOp(t, e, own, res.Sandbox.ID)
	job, err := e.store.ClaimJob(t.Context(), "crashed-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job.Kind != db.JobDeleteVM {
		t.Fatalf("claimed %s", job.Kind)
	}
	if err := e.store.FailJob(t.Context(), job.ID, "crashed-worker", job.LeaseToken, "node unreachable"); err != nil {
		t.Fatal(err)
	}

	w := e.worker(t, lifecycle.Options{})
	w.ReapOnce(t.Context())

	blocked, err := e.store.DeletionOperation(t.Context(), own, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Status != db.DeletionBlocked || blocked.Stage != db.StageRetrying {
		t.Fatalf("operation after failed delete: status=%s stage=%s", blocked.Status, blocked.Stage)
	}
	if n := e.count(t, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.delete' AND state = 'queued'`); n != 1 {
		t.Fatalf("queued delete jobs: %d, want 1", n)
	}
	// The requeued cleanup finishes the operation.
	if !mustProcess(t, w) {
		t.Fatal("requeued delete job not claimed")
	}
	done, err := e.store.DeletionOperation(t.Context(), own, op.ID)
	if err != nil || done.Status != db.DeletionCompleted {
		t.Fatalf("operation completion: %+v err=%v", done, err)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("reservation: %+v", r)
	}
}

func TestReconcileMissingVMMarksReadySandboxError(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)
	if _, err := e.store.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: res.Sandbox.Version,
		To:              db.StateReady,
	}); err != nil {
		t.Fatal(err)
	}

	w := e.worker(t, lifecycle.Options{})
	w.ReapOnce(t.Context())

	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateError || sbx.ErrorReason != "vm_missing" {
		t.Fatalf("state=%s reason=%q, want error/vm_missing", sbx.State, sbx.ErrorReason)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("reservation: %+v", r)
	}
	if !slicesContain(n.deleteCalls(), res.Sandbox.ID) {
		t.Fatalf("DeleteVM calls: %v", n.deleteCalls())
	}
}

func TestReconcilePreservesUnknownAndCleansDeleted(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 40})
	n := e.startNode(t, "node-a")
	own := e.owner(t)

	live := e.createSandbox(t, own, "node-a", 2)
	if _, err := e.store.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: live.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: live.Sandbox.Version,
		To:              db.StateReady,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Node.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: live.Sandbox.ID,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 1},
	}); err != nil {
		t.Fatal(err)
	}

	deleted := e.createSandbox(t, own, "node-a", 2)
	if _, err := n.Node.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: deleted.Sandbox.ID,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.store.AcceptSandboxDeletion(t.Context(), own, deleted.Sandbox.ID, db.DeletionExplicit); err != nil {
		t.Fatal(err)
	}

	unknownID := "bx_unknown_vm"
	if _, err := n.Node.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: unknownID,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 1},
	}); err != nil {
		t.Fatal(err)
	}

	w := e.worker(t, lifecycle.Options{})
	w.ReapOnce(t.Context())

	vms := e.vmIDs(t, "node-a")
	if !vms[live.Sandbox.ID] {
		t.Fatal("live VM was removed")
	}
	if vms[deleted.Sandbox.ID] {
		t.Fatal("deleted VM survived reconciliation")
	}
	if !vms[unknownID] {
		t.Fatal("unknown VM was deleted; reconciliation must preserve it")
	}
}

type failingReleaseStore struct {
	*db.Store
	once sync.Once
}

func (s *failingReleaseStore) ReleaseSandboxReservation(ctx context.Context, owner uuid.UUID, sandboxID string) error {
	failed := false
	s.once.Do(func() { failed = true })
	if failed {
		return errors.New("release failed sentinel")
	}
	return s.Store.ReleaseSandboxReservation(ctx, owner, sandboxID)
}

func TestReconcileErrorRowReleasesAfterEmptyInventory(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)
	// The provision job is already done, as it would be after a prior
	// repair; only the error row and its reservation remain.
	job, err := e.store.ClaimJob(t.Context(), "probe", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.CompleteJob(t.Context(), job.ID, "probe", job.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: res.Sandbox.Version,
		To:              db.StateReady,
	}); err != nil {
		t.Fatal(err)
	}

	failing := &failingReleaseStore{Store: e.store}
	w := e.workerWithStore(t, failing, lifecycle.Options{})
	w.ReapOnce(t.Context())
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateError {
		t.Fatalf("state after missing VM: %s", sbx.State)
	}
	if r := e.nodeReserved(t, "node-a"); r == (db.Resources{}) {
		t.Fatal("reservation was released even though the first release failed")
	}
	if !slicesContain(n.deleteCalls(), res.Sandbox.ID) {
		t.Fatal("first repair did not call DeleteVM")
	}

	// The VM is already absent; the next empty inventory must still repair
	// the error row and release the reservation exactly once.
	w.ReapOnce(t.Context())
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("reservation after retry: %+v", r)
	}
	if got := len(n.deleteCalls()); got < 2 {
		t.Fatalf("idempotent DeleteVM calls: %d, want at least 2", got)
	}
}

func TestReconcileStaleListSnapshotPreservesRecoveredVM(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)
	if _, err := e.store.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: res.Sandbox.Version,
		To:              db.StateReady,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Node.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: res.Sandbox.ID,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 1},
	}); err != nil {
		t.Fatal(err)
	}

	// The list snapshot predates the successful boot: it says "no VMs".
	var once sync.Once
	n.setListHook(func(ctx context.Context, req *nodev1.ListVMsRequest) (*nodev1.ListVMsResponse, error) {
		stale := false
		once.Do(func() { stale = true })
		if stale {
			return &nodev1.ListVMsResponse{}, nil
		}
		return n.Node.ListVMs(ctx, req)
	})

	w := e.worker(t, lifecycle.Options{})
	w.ReapOnce(t.Context())

	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateReady {
		t.Fatalf("recovered sandbox was changed to %s", sbx.State)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}) {
		t.Fatalf("recovered sandbox lost its reservation: %+v", r)
	}
	if vms := e.vmIDs(t, "node-a"); !vms[res.Sandbox.ID] {
		t.Fatal("recovered VM was deleted from a stale ListVMs snapshot")
	}
	if len(n.deleteCalls()) != 0 {
		t.Fatalf("DeleteVM called for a recovered VM: %v", n.deleteCalls())
	}
}

func TestBlockedCleanupDoesNotStarveLaterSandbox(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a-slow", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	e.registerNode(t, "node-z-fast", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	slow := e.startNode(t, "node-a-slow")
	fast := e.startNode(t, "node-z-fast")
	own := e.owner(t)

	makeDeleted := func(nodeID string, fake *lifecycleNode) string {
		res := e.createSandbox(t, own, nodeID, 2)
		if _, err := fake.Node.CreateVM(t.Context(), &nodev1.CreateVMRequest{
			SandboxId: res.Sandbox.ID,
			Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 1},
		}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := e.store.AcceptSandboxDeletion(t.Context(), own, res.Sandbox.ID, db.DeletionExplicit); err != nil {
			t.Fatal(err)
		}
		return res.Sandbox.ID
	}
	slowID := makeDeleted("node-a-slow", slow)
	fastID := makeDeleted("node-z-fast", fast)
	slow.setDeleteHook(func(ctx context.Context, _ *nodev1.DeleteVMRequest) (*nodev1.DeleteVMResponse, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	})

	w := e.worker(t, lifecycle.Options{CleanupTimeout: 100 * time.Millisecond, ReapGuardWait: 100 * time.Millisecond})
	start := time.Now()
	w.ReapOnce(t.Context())
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("reconciliation was not per-operation bounded: %s", elapsed)
	}
	if vms := e.vmIDs(t, "node-a-slow"); !vms[slowID] {
		t.Fatal("slow node unexpectedly cleaned")
	}
	if vms := e.vmIDs(t, "node-z-fast"); vms[fastID] {
		t.Fatal("later sandbox was starved by the blocked cleanup")
	}
}

func TestLogsNeverEchoNodeErrorText(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)
	if _, err := e.store.TransitionSandbox(t.Context(), db.TransitionParams{
		SandboxID: res.Sandbox.ID, Owner: own,
		FromStates:      []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: res.Sandbox.Version,
		To:              db.StateReady,
	}); err != nil {
		t.Fatal(err)
	}
	const sentinel = "sentinel-password=do-not-log"
	n.setDeleteHook(func(context.Context, *nodev1.DeleteVMRequest) (*nodev1.DeleteVMResponse, error) {
		return nil, status.Error(codes.Internal, sentinel)
	})

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	w := e.worker(t, lifecycle.Options{Logger: logger})
	w.ReapOnce(t.Context())

	logged := buf.String()
	if strings.Contains(logged, sentinel) {
		t.Fatal("log echoed the raw node error sentinel")
	}
	if !strings.Contains(logged, "VM repair cleanup pending") || !strings.Contains(logged, "internal error") {
		t.Fatal("log missing the fixed cleanup classification")
	}
	if r := e.nodeReserved(t, "node-a"); r == (db.Resources{}) {
		t.Fatal("capacity released although cleanup was unconfirmed")
	}
}

func TestGuardLossCancelsBlockedAttemptThenRedeliverySucceeds(t *testing.T) {
	// Regression shape: with the guard monitor disabled (or a boot shorter
	// than the keepalive interval) this test hangs at "attempt did not stop"
	// or observes a ready sandbox, which is the false pass the old 600ms
	// variant allowed.
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	// Keep the node-side boot visibly in flight well past the guard
	// keepalive interval (1s), so a normal boot cannot mask a missed loss.
	const bootDelay = 5 * time.Second
	n.Node.BootDelay = bootDelay
	var (
		mu        sync.Mutex
		createErr []error
	)
	n.setCreateHook(func(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
		vm, err := n.Node.CreateVM(ctx, req)
		mu.Lock()
		createErr = append(createErr, err)
		mu.Unlock()
		return vm, err
	})
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := e.worker(t, lifecycle.Options{Lease: 30 * time.Second, GuardReacquire: 2 * time.Second})
	attemptCtx, cancelAttempt := context.WithCancel(t.Context())
	t.Cleanup(cancelAttempt)
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, err := w.ProcessNext(attemptCtx)
		done <- err
	}()
	t.Cleanup(func() {
		cancelAttempt()
		<-finished
	})
	waitFor(t, "visible booting VM", func() bool {
		vm, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID})
		return err == nil && vm.GetState() == nodev1.VMState_VM_STATE_BOOTING
	})
	start := time.Now()

	// Kill only the guard's dedicated backend. The lock is released and the
	// attempt must observe the lost guard and cancel the blocked client call
	// before the node-side boot reaches READY.
	var pid int32
	if err := e.store.Pool().QueryRow(t.Context(), `
		SELECT pid FROM pg_stat_activity
		WHERE datname = current_database()
		  AND application_name = current_setting('application_name') || '_guard'
		  AND pid <> pg_backend_pid()
		LIMIT 1`).Scan(&pid); err != nil {
		t.Fatal("guard backend was not registered")
	}
	if _, err := e.store.Pool().Exec(t.Context(), `SELECT pg_terminate_backend($1)`, pid); err != nil {
		t.Fatal("guard backend termination failed")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("attempt after guard loss returned an error")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("attempt did not stop after guard loss")
	}
	if elapsed := time.Since(start); elapsed >= bootDelay {
		t.Fatalf("guard loss was not observed before the normal boot completed: %s", elapsed)
	}
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.State != db.StateProvisioning {
		t.Fatalf("first attempt completed the sandbox instead of losing the guard: %s", sbx.State)
	}
	if e.jobState(t, res.Job.ID) == db.JobDone {
		t.Fatal("first attempt marked the provision job done")
	}
	// The client call can return before the remote handler records its
	// result; wait for the first CreateVM observation before inspecting it.
	waitFor(t, "first CreateVM result recorded", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(createErr) > 0
	})
	mu.Lock()
	firstErrors := append([]error(nil), createErr...)
	mu.Unlock()
	if len(firstErrors) == 0 || status.Code(firstErrors[0]) != codes.Canceled {
		t.Fatal("first CreateVM did not observe cancellation/lost guard")
	}

	// Redelivery must really call CreateVM again and finish the boot.
	deadline := time.Now().Add(15 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateReady {
		if time.Now().After(deadline) {
			t.Fatalf("redelivery did not finish; job=%s state=%s",
				e.jobState(t, res.Job.ID), e.sandbox(t, own, res.Sandbox.ID).State)
		}
		_, _ = w.ProcessNext(attemptCtx)
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	gotCreates := len(createErr)
	secondErr := error(nil)
	if gotCreates >= 2 {
		secondErr = createErr[1]
	}
	mu.Unlock()
	if gotCreates < 2 {
		t.Fatal("redelivery did not call CreateVM again")
	}
	if secondErr != nil {
		t.Fatal("redelivered CreateVM did not succeed")
	}
}

// TestFailedProvisionReconcileRechecksGenerationAfterGuardWait proves the
// post-guard generation fence: a failed initial-provision job over a
// generation-zero provisioning row (a crashed first boot) whose reconciler is
// waiting for the sandbox guard must not act after a stop and resume accepted
// while it waited — the later boot's reservation stays untouched and its
// retained disk is never deleted.
func TestFailedProvisionReconcileRechecksGenerationAfterGuardWait(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	desired := res.Sandbox.Reserved

	// A crashed first boot: the row is still provisioning at generation zero
	// while its provision job is terminally failed.
	if _, err := e.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET state = 'failed', last_error = 'lease expired'
		WHERE kind = 'sandbox.provision' AND sandbox_id = $1`, res.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	w := e.worker(t, lifecycle.Options{})

	// Hold the guard so the reconciler blocks inside its wait, exactly like
	// a live worker would.
	guard, err := e.store.AcquireSandboxGuard(t.Context(), own, res.Sandbox.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { guard.Release(t.Context()) })
	reapDone := make(chan struct{})
	go func() {
		w.ReapOnce(t.Context())
		close(reapDone)
	}()
	waitFor(t, "reconciler waiting on the guard", func() bool {
		var waiting int
		err := e.store.Pool().QueryRow(t.Context(), `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database()
			  AND application_name = current_setting('application_name') || '_guard'
			  AND wait_event_type = 'Lock'`).Scan(&waiting)
		return err == nil && waiting > 0
	})

	// While the reconciler waits, the sandbox stops (its initial boot job is
	// failed, so the stop settles at once) and resumes: the generation
	// advances and the reservation stays full.
	stopOp := e.acceptStop(t, own, res.Sandbox.ID, false)
	if _, err := e.store.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: res.Sandbox.ID, SupersedeStopID: stopOp.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.State != db.StateProvisioning || sbx.LifecycleGeneration != 1 {
		t.Fatalf("row while reconciler waits: %+v", sbx)
	}
	if r := e.nodeReserved(t, "node-a"); r != desired {
		t.Fatalf("reservation while reconciler waits: %+v, want %+v", r, desired)
	}
	guard.Release(t.Context())
	select {
	case <-reapDone:
	case <-time.After(10 * time.Second):
		t.Fatal("reconciler did not finish after the guard was released")
	}

	// The stale initial-create failure did nothing to the later boot: no
	// error transition, no release, no node cleanup.
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateProvisioning || sbx.LifecycleGeneration != 1 || sbx.ErrorReason != "" {
		t.Fatalf("reconciler mutated the later boot: %+v", sbx)
	}
	if r := e.nodeReserved(t, "node-a"); r != desired {
		t.Fatalf("reconciler changed the reservation: %+v, want %+v", r, desired)
	}
	if got := len(n.deleteCalls()); got != 0 {
		t.Fatalf("reconciler deleted the retained disk: %d", got)
	}
	if got := len(n.createCalls()); got != 0 {
		t.Fatalf("reconciler booted anything: %d", got)
	}
}
