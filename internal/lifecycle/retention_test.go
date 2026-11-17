package lifecycle_test

// Reaper tests (B1.2): TTL expiry accepts durable auto-stops, stopped-disk
// retention deletes through the normal deletion path, disabled retention
// never deletes, deletion retries work for stopped sandboxes, and stale
// initial-provision failures never act on later resume generations. Real
// PostgreSQL and nodefake through the gateway. Gated by
// BBRAFT_TEST_DATABASE_URL.

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// expiredTTL marks a sandbox's deadline as passed.
func expiredTTL(t *testing.T, e *lifecycleEnv, sandboxID string) {
	t.Helper()
	if _, err := e.store.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET expires_at = now() - interval '1 second' WHERE id = $1`, sandboxID); err != nil {
		t.Fatal(err)
	}
}

// stoppedAgo rewrites a sandbox's stopped_at.
func stoppedAgo(t *testing.T, e *lifecycleEnv, sandboxID string, d time.Duration) {
	t.Helper()
	if _, err := e.store.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET stopped_at = now() - make_interval(secs => $2::float8) WHERE id = $1`,
		sandboxID, d.Seconds()); err != nil {
		t.Fatal(err)
	}
}

func TestReapTTLAutoStopsAtDeadline(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	ttl := 60
	res := e.createSandboxTTL(t, own, "node-a", 3, &ttl)
	w := provisionReady(t, e, own, res)

	// A live deadline accepts nothing.
	w.ReapOnce(t.Context())
	if e.count(t, `SELECT count(*) FROM stop_operations`) != 0 {
		t.Fatal("reaper stopped a sandbox before its deadline")
	}

	expiredTTL(t, e, res.Sandbox.ID)
	w.ReapOnce(t.Context())
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateArchiving {
		t.Fatalf("state after TTL reap: %s", sbx.State)
	}
	op, err := e.store.LatestSandboxStop(t.Context(), own, res.Sandbox.ID)
	if err != nil || op.Status != db.StopPending {
		t.Fatalf("auto-stop operation: %+v (%v)", op, err)
	}
	if e.jobStateFor(t, db.JobStop, res.Sandbox.ID) != db.JobQueued {
		t.Fatal("auto-stop did not enqueue the stop job")
	}
	// The worker archives it and releases compute once.
	if !mustProcess(t, w) {
		t.Fatal("auto-stop job not claimed")
	}
	sbx = e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateArchived || sbx.StoppedAt == nil {
		t.Fatalf("sandbox after auto-stop: %+v", sbx)
	}
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != sbx.Desired.DiskGiB {
		t.Fatalf("node after auto-stop: %+v", r)
	}
	if vm, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: res.Sandbox.ID}); err != nil ||
		vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("VM after auto-stop: %+v (%v)", vm, err)
	}
	// An expired deadline that was extended before the reap accepts nothing:
	// the old deadline passes, the PATCH resets it into the future, and the
	// scan no longer sees the sandbox as due.
	extended := e.createSandboxTTL(t, own, "node-a", 3, &ttl)
	mustProcess(t, w) // boot
	expiredTTL(t, e, extended.Sandbox.ID)
	newName := "still-running"
	if _, err := e.store.PatchSandbox(t.Context(), db.PatchSandboxParams{
		Owner: own, SandboxID: extended.Sandbox.ID, Name: &newName, TTL: db.TTLOption{Set: true, Seconds: &ttl},
	}); err != nil {
		t.Fatal(err)
	}
	w.ReapOnce(t.Context())
	if sbx := e.sandbox(t, own, extended.Sandbox.ID); sbx.State != db.StateReady {
		t.Fatalf("extended sandbox was stopped: %s", sbx.State)
	}
	if e.count(t, `SELECT count(*) FROM stop_operations WHERE sandbox_id = $1`, extended.Sandbox.ID) != 0 {
		t.Fatal("reaper accepted a stop for an extended sandbox")
	}
}

func TestStoppedRetentionDeletesAfterExpiry(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)
	stopped := stopThroughWorker(t, e, own, res, w)
	desired := stopped.Desired
	stoppedAgo(t, e, res.Sandbox.ID, 2*time.Hour)

	hour := time.Hour
	retention := e.worker(t, lifecycle.Options{StoppedRetention: &hour})
	retention.ReapOnce(t.Context())

	// The acceptance recorded an ordinary explicit deletion and enqueued the
	// normal delete job; the retained disk is still reserved until the node
	// confirms cleanup.
	op, err := e.store.ActiveSandboxDeletion(t.Context(), own, res.Sandbox.ID)
	if err != nil {
		t.Fatalf("retention deletion: %v", err)
	}
	if op.Reason != db.DeletionExplicit {
		t.Fatalf("retention deletion reason: %s, want explicit", op.Reason)
	}
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != desired.DiskGiB {
		t.Fatalf("node before delete job ran: %+v", r)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var completed int
		if err := e.store.Pool().QueryRow(t.Context(), `
			SELECT count(*) FROM deletion_operations WHERE target_id = $1 AND status = 'completed'`,
			res.Sandbox.ID).Scan(&completed); err != nil {
			t.Fatal(err)
		}
		if completed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retention deletion did not complete")
		}
		_, _ = retention.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	// Compute was released once at the stop, the disk exactly once here.
	// Inventory reconciliation may also clean the deleted sandbox's VM
	// idempotently, so at least one DeleteVM for this sandbox ran and the
	// final state and capacity are exact.
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("node after retention deletion: %+v", r)
	}
	deletes := n.deleteCalls()
	if len(deletes) == 0 {
		t.Fatal("retention deletion never called DeleteVM")
	}
	for _, id := range deletes {
		if id != res.Sandbox.ID {
			t.Fatalf("DeleteVM called for %q, want %q", id, res.Sandbox.ID)
		}
	}
	if vms := e.vmIDs(t, "node-a"); len(vms) != 0 {
		t.Fatalf("VMs after retention deletion: %v", vms)
	}
	// A repeated scan is a no-op: the sandbox is gone.
	retention.ReapOnce(t.Context())
	if e.count(t, `SELECT count(*) FROM deletion_operations WHERE target_id = $1`, res.Sandbox.ID) != 1 {
		t.Fatal("repeated retention scan re-deleted the sandbox")
	}
}

func TestStoppedRetentionDisabledAndFreshStops(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)
	stopped := stopThroughWorker(t, e, own, res, w)
	stoppedAgo(t, e, res.Sandbox.ID, 30*24*time.Hour)
	// A second sandbox stopped moments ago: under any retention it is fresh.
	fresh := e.createSandbox(t, own, "node-a", 3)
	mustProcess(t, w) // boot the fresh sandbox
	stopThroughWorker(t, e, own, fresh, w)

	// Explicit zero disables retention deletion, even for the 30-day-old
	// stop.
	off := time.Duration(0)
	disabled := e.worker(t, lifecycle.Options{StoppedRetention: &off})
	disabled.ReapOnce(t.Context())
	if e.count(t, `SELECT count(*) FROM deletion_operations`) != 0 {
		t.Fatal("disabled retention deleted a sandbox")
	}
	if got := len(n.deleteCalls()); got != 0 {
		t.Fatalf("disabled retention deleted a VM: %d", got)
	}

	// The default seven-day retention deletes the 30-day-old stop through
	// the normal path and leaves the fresh one archived with its disk.
	week := 7 * 24 * time.Hour
	def := e.worker(t, lifecycle.Options{StoppedRetention: &week})
	def.ReapOnce(t.Context())
	if e.count(t, `SELECT count(*) FROM deletion_operations WHERE target_id = $1`, res.Sandbox.ID) != 1 {
		t.Fatal("default retention did not delete the 30-day-old stop")
	}
	if sbx := e.sandbox(t, own, fresh.Sandbox.ID); sbx.State != db.StateArchived {
		t.Fatalf("freshly stopped sandbox changed: %s", sbx.State)
	}
	// Acceptance alone releases nothing: both retained disks stay reserved
	// until the delete worker confirms cleanup.
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != 2*stopped.Desired.DiskGiB {
		t.Fatalf("node after retention acceptance: %+v, want both disks", r)
	}
	// Complete the pending deletion so the test leaves no queued work.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var completed int
		if err := e.store.Pool().QueryRow(t.Context(), `
			SELECT count(*) FROM deletion_operations WHERE target_id = $1 AND status = 'completed'`,
			res.Sandbox.ID).Scan(&completed); err != nil {
			t.Fatal(err)
		}
		if completed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retention deletion did not complete")
		}
		_, _ = def.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	if r := e.nodeReserved(t, "node-a"); r.VCPUs != 0 || r.MemoryMiB != 0 || r.DiskGiB != stopped.Desired.DiskGiB {
		t.Fatalf("node after completed deletion: %+v, want only the fresh disk", r)
	}
}

func TestRetentionDeletionRetriesAfterExhaustedDelete(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 2)
	w := provisionReady(t, e, own, res)
	stopThroughWorker(t, e, own, res, w)
	stoppedAgo(t, e, res.Sandbox.ID, 2*time.Hour)

	hour := time.Hour
	retention := e.worker(t, lifecycle.Options{StoppedRetention: &hour})
	blocked := true
	n.setDeleteHook(func(ctx context.Context, req *nodev1.DeleteVMRequest) (*nodev1.DeleteVMResponse, error) {
		if blocked {
			return nil, status.Error(codes.Unavailable, "node down")
		}
		return n.Node.DeleteVM(ctx, req)
	})
	retention.ReapOnce(t.Context())
	if _, err := e.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET max_attempts = 1 WHERE kind = 'sandbox.delete' AND sandbox_id = $1`,
		res.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	if !mustProcess(t, retention) {
		t.Fatal("delete job not claimed")
	}
	if state := e.jobStateFor(t, db.JobDeleteVM, res.Sandbox.ID); state != db.JobFailed {
		t.Fatalf("delete job after exhaustion: %s", state)
	}
	// The retained disk is still reserved: cleanup is unconfirmed.
	if r := e.nodeReserved(t, "node-a"); r.DiskGiB == 0 {
		t.Fatal("reservation released before confirmed cleanup")
	}

	// The reaper requeues the blocked deletion; once the node answers, it
	// completes and releases the disk exactly once.
	blocked = false
	retention.ReapOnce(t.Context())
	deadline := time.Now().Add(10 * time.Second)
	for {
		var completed int
		if err := e.store.Pool().QueryRow(t.Context(), `
			SELECT count(*) FROM deletion_operations WHERE target_id = $1 AND status = 'completed'`,
			res.Sandbox.ID).Scan(&completed); err != nil {
			t.Fatal(err)
		}
		if completed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("requeued retention deletion did not complete")
		}
		_, _ = retention.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("node after deletion retry: %+v", r)
	}
}

func TestReconcileStaleProvisionFailureSkipsLaterResumeGeneration(t *testing.T) {
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
	desired := sbx.Desired

	// The initial provision job terminally fails after the fact (a crashed
	// worker's leftover). The reaper must not run the initial-create cleanup
	// path against the resumed boot's retained disk.
	if _, err := e.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET state = 'failed', last_error = 'lease expired'
		WHERE kind = 'sandbox.provision' AND sandbox_id = $1`, res.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	w.ReapOnce(t.Context())

	sbx = e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateProvisioning || sbx.ErrorReason != "" {
		t.Fatalf("stale provision failure mutated the resumed row: %+v", sbx)
	}
	if r := e.nodeReserved(t, "node-a"); r != desired {
		t.Fatalf("stale provision failure changed the reservation: %+v", r)
	}
	if got := len(n.deleteCalls()); got != 0 {
		t.Fatalf("stale provision failure deleted the VM: %d", got)
	}

	// A terminally failed resume job is requeued for the same generation and
	// then finishes the boot.
	if _, err := e.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET state = 'failed', last_error = 'lease expired'
		WHERE kind = 'sandbox.resume' AND sandbox_id = $1`, res.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	w.ReapOnce(t.Context())
	if e.count(t, `SELECT count(*) FROM jobs WHERE kind = 'sandbox.resume' AND state = 'queued'`) != 1 {
		t.Fatal("reaper did not requeue the failed resume")
	}
	deadline := time.Now().Add(10 * time.Second)
	for e.sandbox(t, own, res.Sandbox.ID).State != db.StateReady {
		if time.Now().After(deadline) {
			t.Fatalf("requeued resume did not finish; state=%s", e.sandbox(t, own, res.Sandbox.ID).State)
		}
		_, _ = w.ProcessNext(t.Context())
		time.Sleep(5 * time.Millisecond)
	}
	if r := e.nodeReserved(t, "node-a"); r != desired {
		t.Fatalf("node after requeued resume: %+v", r)
	}
	if got := len(n.startCalls()); got != 1 {
		t.Fatalf("StartVM calls: %d, want 1", got)
	}
}
