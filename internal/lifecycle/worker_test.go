package lifecycle_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"strings"
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

func mustProcess(t *testing.T, w *lifecycle.Worker) bool {
	t.Helper()
	worked, err := w.ProcessNext(t.Context())
	if err != nil {
		t.Fatalf("process next: %v", err)
	}
	return worked
}

func TestNewRejectsNegativeWorkers(t *testing.T) {
	// A typed nil *db.Store satisfies the interface and is never touched:
	// New must reject the negative worker count before any use.
	var store *db.Store
	_, err := lifecycle.New(lifecycle.Options{
		Store:   store,
		Logger:  quiet(),
		Workers: -1,
		Dial: func(string) (lifecycle.NodeClient, io.Closer, error) {
			return nil, nil, errors.New("not used")
		},
	})
	if err == nil {
		t.Fatal("negative Workers accepted")
	}
	if _, err := lifecycle.New(lifecycle.Options{
		Store:  store,
		Logger: quiet(),
		Lease:  time.Millisecond,
		Dial: func(string) (lifecycle.NodeClient, io.Closer, error) {
			return nil, nil, errors.New("not used")
		},
	}); err == nil {
		t.Fatal("lease below the safe minimum accepted")
	}
}

func TestProvisionSuccessAndDuplicateDelivery(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	secret := "env-secret-value"
	res := e.createSandboxEnv(t, own, "node-a", 2, map[string]string{"TOKEN": secret})
	w := e.worker(t, lifecycle.Options{})

	if !mustProcess(t, w) {
		t.Fatal("provision job was not claimed")
	}
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateReady {
		t.Fatalf("state %s, want ready", sbx.State)
	}
	calls := n.createCalls()
	if len(calls) != 1 {
		t.Fatalf("CreateVM calls: %d", len(calls))
	}
	req := calls[0]
	if req.GetImage() != "" {
		t.Fatalf("image %q, want empty", req.GetImage())
	}
	if req.GetHostname() != "" {
		t.Fatalf("hostname %q, want empty (display names are not hostnames)", req.GetHostname())
	}
	if req.GetSandboxId() != res.Sandbox.ID || req.GetResources().GetVcpus() != 1 {
		t.Fatal("CreateVM sandbox id or resources mismatch")
	}
	if req.GetEnv()["TOKEN"] != secret {
		t.Fatal("env was not passed to CreateVM")
	}
	if strings.Contains(res.Job.Payload, secret) {
		t.Fatal("plaintext env leaked into the queued job")
	}
	if state := e.jobState(t, res.Job.ID); state != db.JobDone {
		t.Fatalf("job state: %s", state)
	}

	// Redelivery of the same provision job must observe ready and not boot
	// a second VM.
	if _, err := e.store.EnqueueJob(t.Context(), db.JobInput{
		Kind: db.JobProvision, OwnerID: own, SandboxID: &res.Sandbox.ID,
		Payload: res.Job.Payload, MaxAttempts: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if !mustProcess(t, w) {
		t.Fatal("duplicate job not claimed")
	}
	if got := len(n.createCalls()); got != 1 {
		t.Fatalf("CreateVM calls after duplicate: %d", got)
	}
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.State != db.StateReady {
		t.Fatalf("state after duplicate: %s", sbx.State)
	}
}

func TestTerminalProvisionFailureCleansAndReleases(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	n.setCreateHook(func(context.Context, *nodev1.CreateVMRequest) (*nodev1.VM, error) {
		return nil, status.Error(codes.InvalidArgument, "secret raw node text")
	})
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 1)
	w := e.worker(t, lifecycle.Options{})
	if !mustProcess(t, w) {
		t.Fatal("provision job was not claimed")
	}

	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateError || sbx.ErrorReason != "image_unavailable" {
		t.Fatalf("sandbox after failure: state=%s reason=%q", sbx.State, sbx.ErrorReason)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("reservation not released: %+v", r)
	}
	if !slicesContain(n.deleteCalls(), res.Sandbox.ID) {
		t.Fatalf("DeleteVM calls %v, want %s", n.deleteCalls(), res.Sandbox.ID)
	}
	var lastError string
	if err := e.store.Pool().QueryRow(t.Context(),
		`SELECT last_error FROM jobs WHERE id = $1`, res.Job.ID).Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(lastError, "secret raw node text") || lastError != "image_unavailable" {
		t.Fatal("job last_error did not match the fixed safe reason")
	}
	if e.jobState(t, res.Job.ID) != db.JobFailed {
		t.Fatalf("job state: %s", e.jobState(t, res.Job.ID))
	}
}

func TestRetryAfterDisconnectWithSideEffectLeavesOneVM(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	var calls atomic.Int32
	n.setCreateHook(func(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
		if calls.Add(1) == 1 {
			// The VM exists, then the connection dies before the
			// response reaches the server.
			if _, err := n.Node.CreateVM(ctx, req); err != nil {
				return nil, err
			}
			return nil, status.Error(codes.Unavailable, "connection lost")
		}
		return n.Node.CreateVM(ctx, req)
	})
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := e.worker(t, lifecycle.Options{})

	deadline := time.Now().Add(5 * time.Second)
	for e.jobState(t, res.Job.ID) == db.JobRunning || e.jobState(t, res.Job.ID) == db.JobQueued {
		if time.Now().After(deadline) {
			t.Fatalf("job did not settle: %s", e.jobState(t, res.Job.ID))
		}
		_, _ = w.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	if e.jobState(t, res.Job.ID) != db.JobDone {
		t.Fatalf("job state: %s", e.jobState(t, res.Job.ID))
	}
	if e.sandbox(t, own, res.Sandbox.ID).State != db.StateReady {
		t.Fatal("sandbox did not become ready")
	}
	if got := len(n.createCalls()); got != 2 {
		t.Fatalf("CreateVM attempts: %d, want 2", got)
	}
	if vms := e.vmIDs(t, "node-a"); len(vms) != 1 || !vms[res.Sandbox.ID] {
		t.Fatalf("VMs after retry: %v", vms)
	}
	if !slicesContain(n.deleteCalls(), res.Sandbox.ID) {
		t.Fatal("partial VM was not cleaned before retry")
	}
}

func TestLeaseExpiryCancelsBlockedBootThenAnotherWorkerFinishes(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	n.Node.BootDelay = 400 * time.Millisecond
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	wA := e.worker(t, lifecycle.Options{Lease: 300 * time.Millisecond})
	wB := e.worker(t, lifecycle.Options{Lease: 300 * time.Millisecond})

	doneA := make(chan error, 1)
	attemptCtx, cancelAttempt := context.WithCancel(t.Context())
	t.Cleanup(cancelAttempt)
	finishedA := make(chan struct{})
	go func() {
		defer close(finishedA)
		_, err := wA.ProcessNext(attemptCtx)
		doneA <- err
	}()
	t.Cleanup(func() {
		cancelAttempt()
		<-finishedA
	})
	waitFor(t, "visible booting VM", func() bool {
		vm, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID})
		return err == nil && vm.GetState() == nodev1.VMState_VM_STATE_BOOTING
	})
	if _, err := e.store.Pool().Exec(t.Context(),
		`UPDATE jobs SET leased_until = now() - interval '1 second' WHERE id = $1`, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-doneA:
		if err != nil {
			t.Fatalf("first attempt: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first attempt did not stop after lease loss")
	}
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.State != db.StateProvisioning {
		t.Fatalf("first attempt moved the row: %s", sbx.State)
	}

	deadline := time.Now().Add(5 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateReady {
		if time.Now().After(deadline) {
			t.Fatalf("second worker did not finish; job=%s", e.jobState(t, res.Job.ID))
		}
		_, _ = wB.ProcessNext(t.Context())
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(n.createCalls()); got != 2 {
		t.Fatalf("CreateVM calls: %d, want 2", got)
	}
	if vms := e.vmIDs(t, "node-a"); len(vms) != 1 {
		t.Fatalf("VMs: %v", vms)
	}
}

func TestRunCancelsBlockedBootAndJoins(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	n.Node.BootDelay = 400 * time.Millisecond
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)
	w := e.worker(t, lifecycle.Options{Workers: 1})
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
	waitFor(t, "visible booting VM", func() bool {
		vm, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID})
		return err == nil && vm.GetState() == nodev1.VMState_VM_STATE_BOOTING
	})
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not join after cancellation")
	}

	// The cancelled attempt must be reclaimable, not marked done.
	waitFor(t, "job requeued after shutdown", func() bool {
		return e.jobState(t, res.Job.ID) == db.JobQueued
	})
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}) {
		t.Fatalf("reservation released while provisioning: %+v", r)
	}
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.State != db.StateProvisioning {
		t.Fatalf("cancelled boot changed state: %s", sbx.State)
	}

	// A fresh worker finishes the reclaimed job.
	w2 := e.worker(t, lifecycle.Options{})
	deadline := time.Now().Add(5 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateReady {
		if time.Now().After(deadline) {
			t.Fatal("redelivered job did not finish")
		}
		_, _ = w2.ProcessNext(t.Context())
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCancelWhileWaitingForGuardLeavesReclaimableJob(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)

	// Hold the guard so the worker blocks before any node RPC.
	guard, err := e.store.AcquireSandboxGuard(t.Context(), own, res.Sandbox.ID)
	if err != nil {
		t.Fatal("manual guard acquisition failed")
	}
	t.Cleanup(func() { guard.Release(t.Context()) })
	w := e.worker(t, lifecycle.Options{Lease: 5 * time.Second})
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, err := w.ProcessNext(ctx)
		done <- err
	}()
	t.Cleanup(func() {
		cancel()
		<-finished
	})
	// Wait for the real claimed job and for the worker's dedicated guard
	// backend to be blocked on the advisory lock, not a wall-clock sleep.
	waitFor(t, "worker waiting on the owned guard", func() bool {
		if e.jobState(t, res.Job.ID) != db.JobRunning {
			return false
		}
		var waiting int
		err := e.store.Pool().QueryRow(t.Context(), `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database()
			  AND application_name = current_setting('application_name') || '_guard'
			  AND wait_event_type = 'Lock'`).Scan(&waiting)
		return err == nil && waiting > 0
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("attempt while waiting for guard returned an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop while waiting for the guard")
	}
	guard.Release(t.Context())

	waitFor(t, "job requeued", func() bool { return e.jobState(t, res.Job.ID) == db.JobQueued })
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}) {
		t.Fatalf("reservation released: %+v", r)
	}
	if len(n.createCalls()) != 0 {
		t.Fatal("cancelled attempt reached the node")
	}

	w2 := e.worker(t, lifecycle.Options{})
	deadline := time.Now().Add(5 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateReady {
		if time.Now().After(deadline) {
			t.Fatal("reclaimed job did not finish")
		}
		_, _ = w2.ProcessNext(t.Context())
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTamperedProvisionPayloadNeverReachesNode(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	e.registerNode(t, "node-b", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)

	payload, err := json.Marshal(db.ProvisionPayload{
		NodeID:    "node-b",
		Resources: db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Pool().Exec(t.Context(),
		`UPDATE jobs SET payload = $1::jsonb WHERE id = $2`, string(payload), res.Job.ID); err != nil {
		t.Fatal(err)
	}
	w := e.worker(t, lifecycle.Options{})
	if !mustProcess(t, w) {
		t.Fatal("tampered job not claimed")
	}
	if len(n.createCalls()) != 0 {
		t.Fatal("tampered payload reached the node")
	}
	if e.jobState(t, res.Job.ID) != db.JobFailed {
		t.Fatalf("job state: %s", e.jobState(t, res.Job.ID))
	}
}

func TestForeignOwnerJobNeverReachesNode(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	foreign := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)
	if _, err := e.store.Pool().Exec(t.Context(),
		`UPDATE jobs SET owner_id = $1 WHERE id = $2`, foreign, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	w := e.worker(t, lifecycle.Options{})
	if !mustProcess(t, w) {
		t.Fatal("foreign job not claimed")
	}
	if len(n.createCalls()) != 0 {
		t.Fatal("foreign-owned job reached the node")
	}
	if e.jobState(t, res.Job.ID) != db.JobFailed {
		t.Fatalf("job state: %s", e.jobState(t, res.Job.ID))
	}
}

func TestBlockedLeaseRenewalCancelsBootAndKeepsReservation(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	const bootDelay = 2 * time.Second
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
	blocking := &blockingRenewStore{Store: e.store, started: make(chan struct{})}
	w := e.workerWithStore(t, blocking, lifecycle.Options{Lease: 300 * time.Millisecond})
	attemptCtx, cancelAttempt := context.WithCancel(t.Context())
	t.Cleanup(cancelAttempt)
	done := make(chan error, 1)
	finished := make(chan struct{})
	start := time.Now()
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
	select {
	case <-blocking.started:
	case <-time.After(5 * time.Second):
		t.Fatal("renewal was not attempted")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("attempt after blocked renewal returned an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stalled renewal did not cancel the boot attempt")
	}
	if elapsed := time.Since(start); elapsed >= bootDelay/2 {
		// Replacing the bounded renewal with context.WithCancel leaves the
		// renewal blocked until the boot completes; this assertion and the
		// Canceled-result check below are the regression.
		t.Fatalf("renewal was not bounded: attempt lasted %s", elapsed)
	}
	// The client can return before the server records its result; wait for
	// the first CreateVM observation, which must be a real cancellation
	// while the node-side boot is still in flight.
	waitFor(t, "first CreateVM result recorded", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(createErr) > 0
	})
	mu.Lock()
	firstErr := createErr[0]
	mu.Unlock()
	if status.Code(firstErr) != codes.Canceled {
		t.Fatal("bounded renewal did not cancel CreateVM")
	}
	if vm, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID}); err != nil ||
		vm.GetState() != nodev1.VMState_VM_STATE_BOOTING {
		t.Fatal("node-side boot was no longer in flight when CreateVM was canceled")
	}
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.State != db.StateProvisioning {
		t.Fatalf("blocked renewal changed state to %s", sbx.State)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}) {
		t.Fatalf("reservation released after lost claim: %+v", r)
	}
	if state := e.jobState(t, res.Job.ID); state == db.JobDone {
		t.Fatal("lost-claim attempt was marked done")
	}
	// Join the fake boot so the test leaves no goroutine behind.
	_, _ = n.Node.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: res.Sandbox.ID})
}

type blockingRenewStore struct {
	*db.Store
	once    sync.Once
	started chan struct{}
}

func (s *blockingRenewStore) RenewJobLease(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, lease time.Duration) error {
	blocked := false
	s.once.Do(func() { blocked = true; close(s.started) })
	if blocked {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.Store.RenewJobLease(ctx, jobID, worker, token, lease)
}

func TestTwoWorkersProvisionEachSandboxOnce(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 16, MemoryMiB: 16384, DiskGiB: 100})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	const sandboxes = 6
	res := make([]db.CreateSandboxResult, 0, sandboxes)
	for range sandboxes {
		res = append(res, e.createSandbox(t, own, "node-a", 2))
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	barrier := &completeBarrierStore{Store: e.store, entered: make(chan struct{}), release: release}
	w := e.workerWithStore(t, barrier, lifecycle.Options{Workers: 2, Lease: 30 * time.Second})
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
	// Unblock the held completion before joining Run during cleanup.
	t.Cleanup(unblock)
	select {
	case <-barrier.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("no provision job reached finalization")
	}
	waitFor(t, "all sandboxes ready", func() bool {
		for _, r := range res {
			if e.sandbox(t, own, r.Sandbox.ID).State != db.StateReady {
				return false
			}
		}
		return true
	})
	if done := e.doneProvisionJobs(t); done >= sandboxes {
		t.Fatal("barrier did not hold a ready-before-finalize provision job")
	}
	unblock()
	// A ready row is not enough: shutdown between the ready transition and
	// CompleteJob legitimately requeues the provision job. Wait for both the
	// durable ready state and all six done jobs before cancelling.
	waitFor(t, "all sandboxes ready and all provision jobs finalized", func() bool {
		if e.doneProvisionJobs(t) != sandboxes {
			return false
		}
		for _, r := range res {
			if e.sandbox(t, own, r.Sandbox.ID).State != db.StateReady {
				return false
			}
		}
		return true
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
	if got := len(n.createCalls()); got != sandboxes {
		t.Fatalf("CreateVM calls: %d, want %d", got, sandboxes)
	}
	if vms := e.vmIDs(t, "node-a"); len(vms) != sandboxes {
		t.Fatalf("VMs: %d, want %d", len(vms), sandboxes)
	}
	var done int64
	if err := e.store.Pool().QueryRow(t.Context(),
		`SELECT count(*) FROM jobs WHERE kind = 'sandbox.provision' AND state = 'done'`).Scan(&done); err != nil {
		t.Fatal(err)
	}
	if done != sandboxes {
		t.Fatalf("done provision jobs: %d", done)
	}
}

// completeBarrierStore holds the first CompleteJob until release, making the
// ready-before-finalize window deterministic instead of timing-dependent.
type completeBarrierStore struct {
	*db.Store
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *completeBarrierStore) CompleteJob(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID) error {
	blocked := false
	s.once.Do(func() { blocked = true; close(s.entered) })
	if blocked {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Store.CompleteJob(ctx, jobID, worker, token)
}

// doneProvisionJobs counts durably completed provision jobs.
func (e *lifecycleEnv) doneProvisionJobs(t *testing.T) int64 {
	t.Helper()
	return e.count(t, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.provision' AND state = 'done'`)
}

func slicesContain(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// TestForgedPositiveProvisionGenerationNeverReachesNode proves the
// initial-create generation fence: a provision payload claiming a positive
// lifecycle generation is forged or stale (initial provisions are always
// generation zero) and must fail closed before any node call, even when the
// claimed generation happens to match a row that is provisioning again after
// a resume. The resumed boot and its reservation stay untouched.
func TestForgedPositiveProvisionGenerationNeverReachesNode(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)
	stopThroughWorker(t, e, own, res, w)

	if _, err := e.store.AcceptSandboxResume(t.Context(), db.ResumeParams{
		Owner: own, SandboxID: res.Sandbox.ID,
	}); err != nil {
		t.Fatal(err)
	}
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateProvisioning || sbx.LifecycleGeneration != 1 {
		t.Fatalf("resumed row: %+v", sbx)
	}
	// Hold the legitimate resume job back so the forged job claims first.
	if _, err := e.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET run_after = now() + interval '1 hour'
		WHERE kind = 'sandbox.resume' AND sandbox_id = $1`, res.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	createBefore := len(n.createCalls())
	deleteBefore := len(n.deleteCalls())

	payload, err := json.Marshal(db.ProvisionPayload{
		NodeID:     "node-a",
		Resources:  sbx.Reserved,
		Generation: sbx.LifecycleGeneration, // forged: matches the row, but not zero
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.EnqueueJob(t.Context(), db.JobInput{
		Kind: db.JobProvision, OwnerID: own, SandboxID: &res.Sandbox.ID,
		Payload: string(payload), MaxAttempts: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if !mustProcess(t, w) {
		t.Fatal("forged provision job not claimed")
	}

	if got := len(n.createCalls()); got != createBefore {
		t.Fatalf("forged provision payload reached CreateVM: %d -> %d", createBefore, got)
	}
	if got := len(n.deleteCalls()); got != deleteBefore {
		t.Fatalf("forged provision payload reached DeleteVM: %d -> %d", deleteBefore, got)
	}
	if state := e.jobStateFor(t, db.JobProvision, res.Sandbox.ID); state != db.JobFailed {
		t.Fatalf("forged provision job state: %s, want failed", state)
	}
	cur := e.sandbox(t, own, res.Sandbox.ID)
	if cur.State != db.StateProvisioning || cur.LifecycleGeneration != 1 || cur.ErrorReason != "" {
		t.Fatalf("forged job mutated the resumed row: %+v", cur)
	}
	if r := e.nodeReserved(t, "node-a"); r != sbx.Desired {
		t.Fatalf("forged job changed the reservation: %+v, want %+v", r, sbx.Desired)
	}
}

// TestTerminalBootFailureDuringAcceptedStopKeepsTheFirstDisk is the
// deterministic terminal-failure interleaving: the initial boot creates the
// first disk and fails terminally while, inside the same RPC window, a stop
// is accepted (the canonical row advances to archiving before bootFailed's
// reread). The stop owns the disk and the archival: the stale initial-create
// cleanup must neither delete the disk nor release anything, and the stop
// job then archives the sandbox with its disk retained.
func TestTerminalBootFailureDuringAcceptedStopKeepsTheFirstDisk(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)

	var calls atomic.Int32
	n.setCreateHook(func(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
		if calls.Add(1) != 1 {
			return n.Node.CreateVM(ctx, req)
		}
		// The first disk is created, then the attempt fails terminally —
		// with the canonical row advancing to archiving before bootFailed
		// rereads it.
		if _, err := n.Node.CreateVM(ctx, req); err != nil {
			return nil, err
		}
		e.acceptStop(t, own, req.GetSandboxId(), false)
		return nil, status.Error(codes.InvalidArgument, "terminal after create")
	})
	w := e.worker(t, lifecycle.Options{})
	if !mustProcess(t, w) {
		t.Fatal("initial boot attempt not claimed")
	}

	// The stale initial-create cleanup deleted nothing and released nothing.
	if got := len(n.deleteCalls()); got != 0 {
		t.Fatalf("terminal boot failure deleted the first disk: %v", n.deleteCalls())
	}
	if r := e.nodeReserved(t, "node-a"); r != res.Sandbox.Reserved {
		t.Fatalf("terminal boot failure released capacity: %+v, want %+v", r, res.Sandbox.Reserved)
	}
	if state := e.jobStateFor(t, db.JobProvision, res.Sandbox.ID); state != db.JobFailed {
		t.Fatalf("initial job state: %s", state)
	}
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.State != db.StateArchiving {
		t.Fatalf("row after stale terminal failure: %s", sbx.State)
	}

	// The stop owns the outcome: it archives the sandbox with the disk the
	// failed boot created, releasing compute exactly once.
	if !mustProcess(t, w) {
		t.Fatal("stop job not claimed")
	}
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateArchived || sbx.StoppedAt == nil {
		t.Fatalf("sandbox after stop: %+v", sbx)
	}
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != sbx.Desired.DiskGiB {
		t.Fatalf("node after stop: %+v", r)
	}
	if vm, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID}); err != nil ||
		vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("first disk after stop: %+v (%v)", vm, err)
	}
	if got := len(n.deleteCalls()); got != 0 {
		t.Fatalf("stop path deleted the retained disk: %v", n.deleteCalls())
	}
}

// TestTerminalBootFailureWithAdvancedGenerationKeepsTheDisk proves the
// generation fence on initial-create terminal cleanup: when the canonical
// boot has advanced past generation zero while a stale attempt's failure is
// still in flight (here constructed directly: the row's generation moves
// under the RPC), the stale attempt must not transition, delete the disk or
// release anything.
func TestTerminalBootFailureWithAdvancedGenerationKeepsTheDisk(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)

	var calls atomic.Int32
	n.setCreateHook(func(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
		if calls.Add(1) != 1 {
			return n.Node.CreateVM(ctx, req)
		}
		if _, err := n.Node.CreateVM(ctx, req); err != nil {
			return nil, err
		}
		// The canonical boot advanced while this stale attempt ran: a later
		// resume owns the row and its retained disk now.
		if _, err := e.store.Pool().Exec(ctx, `
			UPDATE sandboxes SET lifecycle_generation = 1 WHERE id = $1`, req.GetSandboxId()); err != nil {
			return nil, err
		}
		return nil, status.Error(codes.InvalidArgument, "terminal after create")
	})
	w := e.worker(t, lifecycle.Options{})
	if !mustProcess(t, w) {
		t.Fatal("initial boot attempt not claimed")
	}

	if got := len(n.deleteCalls()); got != 0 {
		t.Fatalf("stale terminal failure deleted the disk: %v", n.deleteCalls())
	}
	cur := e.sandbox(t, own, res.Sandbox.ID)
	if cur.State != db.StateProvisioning || cur.LifecycleGeneration != 1 || cur.ErrorReason != "" {
		t.Fatalf("stale terminal failure mutated the later boot: %+v", cur)
	}
	if r := e.nodeReserved(t, "node-a"); r != res.Sandbox.Reserved {
		t.Fatalf("stale terminal failure released capacity: %+v, want %+v", r, res.Sandbox.Reserved)
	}
	if state := e.jobStateFor(t, db.JobProvision, res.Sandbox.ID); state != db.JobFailed {
		t.Fatalf("initial job state: %s", state)
	}
	if _, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID}); err != nil {
		t.Fatalf("disk removed by stale failure: %v", err)
	}
}

// TestRetryableBootFailureDuringAcceptedStopKeepsTheFirstDisk is the
// retryable twin of the terminal interleaving: the initial boot creates the
// first disk and fails with a transport error while a stop is accepted
// inside the same RPC window. The retryable cleanup rereads the canonical
// row before deciding: the accepted stop owns the disk, so no DeleteVM runs
// and the retried boot settles under the archiving allowance, after which
// the stop archives the sandbox with its disk retained.
func TestRetryableBootFailureDuringAcceptedStopKeepsTheFirstDisk(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 4)

	var calls atomic.Int32
	n.setCreateHook(func(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
		if calls.Add(1) != 1 {
			return n.Node.CreateVM(ctx, req)
		}
		// The first disk is created, then the answer is lost — with the
		// canonical row advancing to archiving before bootFailed rereads it.
		if _, err := n.Node.CreateVM(ctx, req); err != nil {
			return nil, err
		}
		e.acceptStop(t, own, req.GetSandboxId(), false)
		return nil, status.Error(codes.Unavailable, "answer lost")
	})
	w := e.worker(t, lifecycle.Options{})
	if !mustProcess(t, w) {
		t.Fatal("initial boot attempt not claimed")
	}

	// The retryable cleanup on the stale pre-RPC snapshot must not have
	// deleted the disk the stop now owns.
	if got := len(n.deleteCalls()); got != 0 {
		t.Fatalf("retryable boot failure deleted the first disk: %v", n.deleteCalls())
	}
	if r := e.nodeReserved(t, "node-a"); r != res.Sandbox.Reserved {
		t.Fatalf("retryable boot failure released capacity: %+v", r)
	}
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.State != db.StateArchiving {
		t.Fatalf("row after retryable failure: %s", sbx.State)
	}

	// The retry settles the initial boot under the archiving allowance and
	// the stop then archives the sandbox with the retained disk.
	deadline := time.Now().Add(10 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateArchived {
		if time.Now().After(deadline) {
			t.Fatalf("stop did not finish; state=%s provision=%s stop=%s",
				e.sandbox(t, own, res.Sandbox.ID).State,
				e.jobStateFor(t, db.JobProvision, res.Sandbox.ID),
				e.jobStateFor(t, db.JobStop, res.Sandbox.ID))
		}
		_, _ = w.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != sbx.Desired.DiskGiB {
		t.Fatalf("node after stop: %+v", r)
	}
	if vm, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID}); err != nil ||
		vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("first disk after stop: %+v (%v)", vm, err)
	}
	if got := len(n.deleteCalls()); got != 0 {
		t.Fatalf("retry path deleted the retained disk: %v", n.deleteCalls())
	}
}

// TestRetryableBootFailureWithAdvancedGenerationKeepsTheDisk is the
// retryable twin of the advanced-generation terminal interleaving: the row's
// generation moves under the RPC, so a later resume owns the boot and its
// retained disk; the stale retryable attempt must neither delete nor release
// and settles its job at once.
func TestRetryableBootFailureWithAdvancedGenerationKeepsTheDisk(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 4)

	var calls atomic.Int32
	n.setCreateHook(func(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
		if calls.Add(1) != 1 {
			return n.Node.CreateVM(ctx, req)
		}
		if _, err := n.Node.CreateVM(ctx, req); err != nil {
			return nil, err
		}
		// The canonical boot advanced while this stale attempt ran: a later
		// resume owns the row and its retained disk now.
		if _, err := e.store.Pool().Exec(ctx, `
			UPDATE sandboxes SET lifecycle_generation = 1 WHERE id = $1`, req.GetSandboxId()); err != nil {
			return nil, err
		}
		return nil, status.Error(codes.Unavailable, "answer lost")
	})
	w := e.worker(t, lifecycle.Options{})
	if !mustProcess(t, w) {
		t.Fatal("initial boot attempt not claimed")
	}

	if got := len(n.deleteCalls()); got != 0 {
		t.Fatalf("stale retryable failure deleted the disk: %v", n.deleteCalls())
	}
	cur := e.sandbox(t, own, res.Sandbox.ID)
	if cur.State != db.StateProvisioning || cur.LifecycleGeneration != 1 || cur.ErrorReason != "" {
		t.Fatalf("stale retryable failure mutated the later boot: %+v", cur)
	}
	if r := e.nodeReserved(t, "node-a"); r != res.Sandbox.Reserved {
		t.Fatalf("stale retryable failure released capacity: %+v, want %+v", r, res.Sandbox.Reserved)
	}
	if state := e.jobStateFor(t, db.JobProvision, res.Sandbox.ID); state != db.JobFailed {
		t.Fatalf("initial job state: %s, want failed (settled)", state)
	}
	if _, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID}); err != nil {
		t.Fatalf("disk removed by stale retryable failure: %v", err)
	}
}
