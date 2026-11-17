package lifecycle_test

import (
	"context"
	"encoding/json/v2"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// TestTTLDuringBlockedBootCannotCompleteOrReleaseBeforeGuard proves the
// auto-stop fence with the updated node fake: the boot is visible and running
// on the node, the client cancellation does not prove it stopped, and the
// durable stop may neither archive nor release capacity until the first boot
// settles and StopVM confirms the VM is off with its disk kept. The first
// disk the boot creates is retained by the stop, never deleted.
func TestTTLDuringBlockedBootCannotCompleteOrReleaseBeforeGuard(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	n.Node.BootDelay = 1 * time.Second
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	full := res.Sandbox.Reserved

	provisionWorker := e.worker(t, lifecycle.Options{Lease: 300 * time.Millisecond})
	attemptCtx, cancelAttempt := context.WithCancel(t.Context())
	t.Cleanup(cancelAttempt)
	provisionDone := make(chan error, 1)
	provisionFinished := make(chan struct{})
	go func() {
		defer close(provisionFinished)
		_, err := provisionWorker.ProcessNext(attemptCtx)
		provisionDone <- err
	}()
	t.Cleanup(func() {
		cancelAttempt()
		<-provisionFinished
	})
	// The fake admits the boot immediately and reports it BOOTING for the
	// whole BootDelay, so it is visible well before the client call ends.
	waitFor(t, "visible booting VM", func() bool {
		vm, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID})
		return err == nil && vm.GetState() == nodev1.VMState_VM_STATE_BOOTING
	})

	// Expire the sandbox and accept the auto-stop through the real TTL
	// reaper.
	if _, err := e.store.Pool().Exec(t.Context(),
		`UPDATE sandboxes SET expires_at = now() - interval '1 second' WHERE id = $1`,
		res.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	provisionWorker.ReapOnce(t.Context())
	op, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID)
	if err != nil || op.Status != db.StopPending {
		t.Fatalf("TTL auto-stop acceptance: %+v (%v)", op, err)
	}

	// While the boot claim is still live, the stop worker retries without
	// the guard and nothing completes or releases: the full reservation
	// (compute and disk) stays on the node and the VM keeps booting.
	stopWorker := e.worker(t, lifecycle.Options{Lease: 300 * time.Millisecond})
	if !mustProcess(t, stopWorker) {
		t.Fatal("stop attempt was not claimed")
	}
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.State != db.StateArchiving {
		t.Fatalf("state after pending stop attempt: %s", sbx.State)
	}
	if r := e.nodeReserved(t, "node-a"); r != full {
		t.Fatalf("reservation changed while the boot was in flight: %+v, want %+v", r, full)
	}
	if op, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID); err != nil || op.Status != db.StopPending {
		t.Fatalf("stop operation moved before the boot settled: %+v (%v)", op, err)
	}
	if vm, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID}); err != nil ||
		vm.GetState() != nodev1.VMState_VM_STATE_BOOTING {
		t.Fatalf("boot was disturbed by the pending stop: %+v (%v)", vm, err)
	}

	// Force the provisioning claim to expire: its client call is cancelled,
	// but the node keeps booting. The redelivered boot creates the first
	// disk (the stop needs it) and only then does StopVM run.
	if _, err := e.store.Pool().Exec(t.Context(),
		`UPDATE jobs SET leased_until = now() - interval '1 second' WHERE id = $1`, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-provisionDone:
		if err != nil {
			t.Fatalf("provision attempt: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("provision attempt did not stop")
	}
	deadline := time.Now().Add(20 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateArchived {
		if time.Now().After(deadline) {
			var jobs string
			_ = e.store.Pool().QueryRow(t.Context(), `
				SELECT string_agg(kind || ':' || state, ', ' ORDER BY kind)
				FROM jobs WHERE sandbox_id = $1`, res.Sandbox.ID).Scan(&jobs)
			t.Fatalf("auto-stop completion timeout: state=%s jobs=%s",
				e.sandbox(t, own, res.Sandbox.ID).State, jobs)
		}
		_, _ = stopWorker.ProcessNext(t.Context())
		time.Sleep(10 * time.Millisecond)
	}

	// The first disk was created and retained; compute released exactly
	// once, and the VM is STOPPED with its disk on the node.
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.StoppedAt == nil {
		t.Fatal("archived sandbox has no stopped timestamp")
	}
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != sbx.Desired.DiskGiB {
		t.Fatalf("node after auto-stop: %+v, want disk-only %+d", r, sbx.Desired.DiskGiB)
	}
	if vm, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID}); err != nil ||
		vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("VM after auto-stop: %+v (%v)", vm, err)
	}
	if final, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID); err != nil || final.Status != db.StopCompleted {
		t.Fatalf("stop operation after completion: %+v (%v)", final, err)
	}
	if deletes := n.deleteCalls(); len(deletes) != 0 {
		t.Fatalf("auto-stop deleted the first disk: %v", deletes)
	}
}

// TestDeleteVMFencesEarlierCreateVMThroughGateway exercises the accepted
// foundation guarantee through the real hub, gateway, nodelink and nodefake:
// a visible in-flight boot is aborted and joined by DeleteVM, its original
// CreateVM answers ABORTED, and the VM never reappears afterwards.
func TestDeleteVMFencesEarlierCreateVMThroughGateway(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	n.Node.BootDelay = 400 * time.Millisecond
	client, err := e.factory.Dial("node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	type result struct {
		vm  *nodev1.VM
		err error
	}
	created := make(chan result, 1)
	createFinished := make(chan struct{})
	go func() {
		defer close(createFinished)
		vm, err := client.CreateVM(ctx, &nodev1.CreateVMRequest{
			SandboxId: "bx_fenced",
			Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 1},
		})
		created <- result{vm: vm, err: err}
	}()
	t.Cleanup(func() {
		cancel()
		<-createFinished
	})
	waitFor(t, "visible booting VM", func() bool {
		vm, err := client.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: "bx_fenced"})
		return err == nil && vm.GetState() == nodev1.VMState_VM_STATE_BOOTING
	})
	if _, err := client.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: "bx_fenced"}); err != nil {
		t.Fatalf("DeleteVM: %v", err)
	}
	select {
	case res := <-created:
		if status.Code(res.err) != codes.Aborted {
			t.Fatalf("earlier CreateVM: vm=%v err=%v, want ABORTED", res.vm, res.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("earlier CreateVM was not joined by DeleteVM")
	}
	if _, err := client.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: "bx_fenced"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetVM after fence: %v", err)
	}
	time.Sleep(600 * time.Millisecond)
	if _, err := client.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: "bx_fenced"}); status.Code(err) != codes.NotFound {
		t.Fatalf("VM reappeared after DeleteVM: %v", err)
	}
}

func TestDeleteIsIdempotentAndDuplicateDeliveryIsQuiet(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)
	if _, _, err := e.store.AcceptSandboxDeletion(t.Context(), own, res.Sandbox.ID, db.DeletionExplicit); err != nil {
		t.Fatal(err)
	}
	w := e.worker(t, lifecycle.Options{})
	active := mustActiveOp(t, e, own, res.Sandbox.ID)
	if !mustProcess(t, w) {
		t.Fatal("delete job not claimed")
	}
	op, err := e.store.DeletionOperation(t.Context(), own, active.ID)
	if err != nil {
		t.Fatal(err)
	}
	if op.Status != db.DeletionCompleted {
		t.Fatalf("operation status: %s", op.Status)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("reservation: %+v", r)
	}
	before := len(n.deleteCalls())

	payload, err := json.Marshal(db.DeleteVMPayload{
		NodeID: "node-a", VMID: res.Sandbox.ID, OperationID: op.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.EnqueueJob(t.Context(), db.JobInput{
		Kind: db.JobDeleteVM, OwnerID: own, SandboxID: &res.Sandbox.ID, Payload: string(payload),
	}); err != nil {
		t.Fatal(err)
	}
	if !mustProcess(t, w) {
		t.Fatal("duplicate delete job not claimed")
	}
	if got := len(n.deleteCalls()); got != before {
		t.Fatalf("duplicate delete called the node: %d -> %d", before, got)
	}
}

func TestDeletePayloadTamperNeverReachesNode(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)
	if _, _, err := e.store.AcceptSandboxDeletion(t.Context(), own, res.Sandbox.ID, db.DeletionExplicit); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(db.DeleteVMPayload{
		NodeID: "node-a", VMID: "bx_someone_else", OperationID: "bdop_does_not_matter",
	})
	if err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err := e.store.Pool().QueryRow(t.Context(),
		`SELECT EXISTS (SELECT 1 FROM jobs WHERE kind = 'sandbox.delete' AND sandbox_id = $1)`,
		res.Sandbox.ID).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("delete job missing")
	}
	if _, err := e.store.Pool().Exec(t.Context(),
		`UPDATE jobs SET payload = $1::jsonb WHERE sandbox_id = $2 AND kind = 'sandbox.delete'`,
		string(payload), res.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	w := e.worker(t, lifecycle.Options{})
	if !mustProcess(t, w) {
		t.Fatal("tampered delete job not claimed")
	}
	if len(n.deleteCalls()) != 0 {
		t.Fatal("tampered delete payload reached the node")
	}
}

func mustActiveOp(t *testing.T, e *lifecycleEnv, owner uuid.UUID, sandboxID string) db.DeletionOperation {
	t.Helper()
	op, err := e.store.ActiveSandboxDeletion(t.Context(), owner, sandboxID)
	if err != nil {
		t.Fatalf("active operation: %v", err)
	}
	return op
}
