package server

// Disk inventory accounting through the production inventoryWriter and a real
// store. It requires BBRAFT_TEST_DATABASE_URL (dbtest skips without it, and
// fails under BBRAFT_TEST_REQUIRE_ALL=1).

import (
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

const inventoryDiskNode = "node-disk"

// inventoryDiskInfo is what the real node's Manager.Info reports: vCPU and
// memory are the TOTAL offer, disk_free_mib is the disk still free for new
// sandboxes, net of the node's own outstanding reservations. The numbers come
// from the captured e2e run (94 GiB node, 51 GiB default VM: Info 94 GiB
// before the create, 43 GiB while the VM exists).
func inventoryDiskInfo(freeGiB uint64) *nodev1.InfoResponse {
	return &nodev1.InfoResponse{
		NodeId: inventoryDiskNode, Vcpus: 8, MemoryMib: 16384, DiskFreeMib: freeGiB << 10,
		CpuModel: "x86-64-v3", QemuVersion: "10.0", DefaultImage: "base",
	}
}

// inventoryDiskAvailable is the disk the scheduler's capacity query sees:
// total minus reserved, signed so a double subtraction shows as a negative.
func inventoryDiskAvailable(t *testing.T, store *db.Store) int64 {
	t.Helper()
	n, err := store.Node(t.Context(), inventoryDiskNode)
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	return int64(n.Total.DiskGiB) - int64(n.Reserved.DiskGiB)
}

// inventoryDiskFits reports whether NodesWithCapacity places a sandbox that
// needs diskGiB (CPU and memory trivial).
func inventoryDiskFits(t *testing.T, store *db.Store, diskGiB uint64) bool {
	t.Helper()
	nodes, err := store.NodesWithCapacity(t.Context(),
		db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: diskGiB}, time.Minute)
	if err != nil {
		t.Fatalf("NodesWithCapacity: %v", err)
	}
	return len(nodes) == 1
}

// TestInventoryDiskCreateReportDeleteCreate is the e2e capacity-503 bug: a
// report taken while a VM exists must not make the server subtract that VM's
// reservation a second time, and a delete must free the room immediately.
func TestInventoryDiskCreateReportDeleteCreate(t *testing.T) {
	store := dbtest.Open(t)
	own, err := store.EnsureAccount(t.Context(), "acct-disk-inventory")
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}
	writer := inventoryWriter{store: store, now: time.Now}
	report := func(freeGiB uint64) {
		t.Helper()
		if err := writer.WriteNode(t.Context(), nodegateway.NodeState{NodeID: inventoryDiskNode, Connected: true, Info: inventoryDiskInfo(freeGiB)}); err != nil {
			t.Fatalf("WriteNode(free %d GiB): %v", freeGiB, err)
		}
	}
	createVM := func(diskGiB uint64) string {
		t.Helper()
		res, err := store.CreateSandbox(t.Context(), db.CreateSandboxParams{
			ID: ids.Sandbox(), Owner: own.ID, NodeID: inventoryDiskNode,
			Reserve: db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: diskGiB},
		})
		if err != nil {
			t.Fatalf("CreateSandbox(%d GiB): %v", diskGiB, err)
		}
		return res.Sandbox.ID
	}

	// 1. Fresh node: the 94 GiB the node reports are all placeable.
	report(94)
	if got := inventoryDiskAvailable(t, store); got != 94 {
		t.Fatalf("available after first report = %d GiB, want 94", got)
	}

	// 2. One default VM (51 GiB) is created on the server side.
	vm := createVM(51)
	if got := inventoryDiskAvailable(t, store); got != 43 {
		t.Fatalf("available after create = %d GiB, want 43", got)
	}

	// 3. The next poll: the node now has the VM, so it reports 43 GiB free,
	// already net of that VM. The server must not subtract its 51 again.
	report(43)
	if got := inventoryDiskAvailable(t, store); got != 43 {
		t.Fatalf("available after report taken with the VM alive = %d GiB, want 43 (reservation counted twice)", got)
	}
	if !inventoryDiskFits(t, store, 43) || inventoryDiskFits(t, store, 44) {
		t.Fatalf("placement with 43 GiB free: fits(43)=%v fits(44)=%v, want true/false",
			inventoryDiskFits(t, store, 43), inventoryDiskFits(t, store, 44))
	}

	// 4. A second reservation after the report lowers availability exactly once.
	second := createVM(40)
	if got := inventoryDiskAvailable(t, store); got != 3 {
		t.Fatalf("available after post-report reserve of 40 = %d GiB, want 3", got)
	}

	// 5. Delete the second one: its room returns once, replays change nothing.
	for range 2 {
		if err := store.ReleaseSandboxReservation(t.Context(), own.ID, second); err != nil {
			t.Fatalf("release second: %v", err)
		}
		if got := inventoryDiskAvailable(t, store); got != 43 {
			t.Fatalf("available after releasing the 40 GiB VM (replayed) = %d GiB, want 43", got)
		}
	}

	// 6. Delete the first VM: the room is placeable at once, before any new
	// report, so an immediate create of another default VM is accepted.
	if err := store.ReleaseSandboxReservation(t.Context(), own.ID, vm); err != nil {
		t.Fatalf("release first: %v", err)
	}
	if got := inventoryDiskAvailable(t, store); got != 94 {
		t.Fatalf("available right after delete = %d GiB, want 94", got)
	}
	if !inventoryDiskFits(t, store, 51) {
		t.Fatal("node does not fit a 51 GiB sandbox right after the delete released it (create would 503 until the next poll)")
	}
	again := createVM(51)

	// 7. The node later reports idle (both VMs gone on its side too): the
	// baseline is the offer again, and replaying a finished release is a no-op.
	for _, id := range []string{again, vm} {
		if err := store.ReleaseSandboxReservation(t.Context(), own.ID, id); err != nil {
			t.Fatalf("final release %s: %v", id, err)
		}
	}
	report(94)
	if got := inventoryDiskAvailable(t, store); got != 94 {
		t.Fatalf("available after idle report = %d GiB, want 94", got)
	}
}
