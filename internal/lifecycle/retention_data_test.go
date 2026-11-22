package lifecycle_test

// Data retention tests: a stopped sandbox's stored data expires after the
// configured window (reservation released, disk copy evicted and deleted by
// reconciliation, row and event kept), while a disabled window and Phase 1
// mode never expire anything. Real PostgreSQL and nodefake through the
// gateway. Gated by BBRAFT_TEST_DATABASE_URL.

import (
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
)

// dataExpiryEventCount counts the recorded expiry events of one sandbox.
func dataExpiryEventCount(t *testing.T, e *lifecycleEnv, sandboxID string) int64 {
	t.Helper()
	return e.count(t, `SELECT count(*) FROM sandbox_events
		WHERE sandbox_id = $1 AND type = 'sandbox.data_expired'`, sandboxID)
}

func TestDataRetentionExpiresStoppedData(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	n := e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)
	stopped := stopThroughWorker(t, e, own, res, w)
	if stopped.DataExpiredAt != nil {
		t.Fatal("a freshly stopped sandbox is already expired")
	}
	stoppedAgo(t, e, res.Sandbox.ID, 24*time.Hour)

	// A candidate that is not yet past the window is left alone.
	day := 48 * time.Hour
	notYet := e.worker(t, lifecycle.Options{ObjectStore: memstore.New(), DataRetention: &day})
	notYet.ReapOnce(t.Context())
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.DataExpiredAt != nil {
		t.Fatal("data expired inside the retention window")
	}

	hour := time.Hour
	retention := e.worker(t, lifecycle.Options{ObjectStore: memstore.New(), DataRetention: &hour})
	retention.ReapOnce(t.Context())

	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateArchived || sbx.DataExpiredAt == nil {
		t.Fatalf("expired sandbox: %+v", sbx)
	}
	if sbx.DiskNodeID != nil || sbx.NodeID != nil {
		t.Fatalf("expired sandbox still names a disk node: %+v", sbx)
	}
	if sbx.Reserved != (db.Resources{}) {
		t.Fatalf("expired sandbox kept a reservation: %+v", sbx.Reserved)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("node after expiry: %+v", r)
	}
	// Reconciliation deletes the evicted copy in the same round and closes
	// the eviction row.
	if got := len(n.deleteCalls()); got == 0 {
		t.Fatal("the node's copy was never deleted")
	}
	if vms := e.vmIDs(t, "node-a"); len(vms) != 0 {
		t.Fatalf("VMs after expiry: %v", vms)
	}
	if e.count(t, `SELECT count(*) FROM vm_evictions
		WHERE node_id = 'node-a' AND sandbox_id = $1 AND reason = 'data_expired'
		  AND done_at IS NOT NULL`, res.Sandbox.ID) != 1 {
		t.Fatal("the eviction was not closed by reconciliation")
	}
	if got := dataExpiryEventCount(t, e, res.Sandbox.ID); got != 1 {
		t.Fatalf("expiry events: %d", got)
	}

	// Succeeds idempotently: a second round changes and records nothing.
	retention.ReapOnce(t.Context())
	if got := dataExpiryEventCount(t, e, res.Sandbox.ID); got != 1 {
		t.Fatalf("expiry events after a second round: %d", got)
	}
	if e.count(t, `SELECT count(*) FROM vm_evictions WHERE sandbox_id = $1`, res.Sandbox.ID) != 1 {
		t.Fatal("a second round recorded another eviction")
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("node after the second round: %+v", r)
	}
}

func TestDataRetentionOffNeverExpires(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)
	stopped := stopThroughWorker(t, e, own, res, w)
	// Past the data window but well inside the seven-day row retention.
	stoppedAgo(t, e, res.Sandbox.ID, 2*time.Hour)

	// An explicit zero with an object store never expires data.
	off := time.Duration(0)
	disabled := e.worker(t, lifecycle.Options{ObjectStore: memstore.New(), DataRetention: &off})
	disabled.ReapOnce(t.Context())
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.DataExpiredAt != nil {
		t.Fatal("disabled data retention expired a sandbox")
	}
	if r := e.nodeReserved(t, "node-a"); r != stopped.Reserved {
		t.Fatalf("disabled data retention changed the reservation: %+v", r)
	}

	// Without an object store the window is off whatever the flag says:
	// Phase 1 behaviour is unchanged.
	phase1 := e.worker(t, lifecycle.Options{DataRetention: new(time.Hour)})
	phase1.ReapOnce(t.Context())
	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.DataExpiredAt != nil {
		t.Fatal("phase 1 mode expired a sandbox")
	}
	if r := e.nodeReserved(t, "node-a"); r != stopped.Reserved {
		t.Fatalf("phase 1 mode changed the reservation: %+v", r)
	}
	if got := dataExpiryEventCount(t, e, res.Sandbox.ID); got != 0 {
		t.Fatalf("expiry events: %d", got)
	}
}

func TestDataRetentionTwoReapersExpireOnce(t *testing.T) {
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	e.startNode(t, "node-a")
	own := e.owner(t)
	res := e.createSandbox(t, own, "node-a", 3)
	w := provisionReady(t, e, own, res)
	stopThroughWorker(t, e, own, res, w)
	stoppedAgo(t, e, res.Sandbox.ID, 24*time.Hour)

	hour := time.Hour
	first := e.worker(t, lifecycle.Options{ObjectStore: memstore.New(), DataRetention: &hour})
	second := e.worker(t, lifecycle.Options{ObjectStore: memstore.New(), DataRetention: &hour})
	var wg sync.WaitGroup
	wg.Go(func() { first.ReapOnce(t.Context()) })
	wg.Go(func() { second.ReapOnce(t.Context()) })
	wg.Wait()

	if sbx := e.sandbox(t, own, res.Sandbox.ID); sbx.DataExpiredAt == nil {
		t.Fatal("racing reapers did not expire the sandbox")
	}
	if got := dataExpiryEventCount(t, e, res.Sandbox.ID); got != 1 {
		t.Fatalf("expiry events: %d", got)
	}
	if n := e.count(t, `SELECT count(*) FROM vm_evictions WHERE sandbox_id = $1`, res.Sandbox.ID); n != 1 {
		t.Fatalf("evictions: %d", n)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("node after the race: %+v", r)
	}
}
