package db_test

// Node capacity: concurrent reservation never oversubscribes, shrunk
// totals never steal reservations back, and the owner+sandbox-bound
// release cannot double-free.

import (
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

func TestConcurrentCapacityNeverOversubscribes(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	one := db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10}

	const racers = 16
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
	)
	for range racers {
		wg.Go(func() {
			err := s.ReserveCapacity(t.Context(), "node-a", one)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successes++
				return
			}
			if err != db.ErrNoCapacity {
				t.Errorf("reserve: %v", err)
			}
		})
	}
	wg.Wait()
	if successes != 8 {
		t.Fatalf("successful reservations: %d, want exactly the capacity 8", successes)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 8 || got.MemoryMiB != 8192 || got.DiskGiB != 80 {
		t.Fatalf("reserved after race: %+v", got)
	}
}

func TestReserveUnknownNode(t *testing.T) {
	s := dbtestOpen(t)
	err := s.ReserveCapacity(t.Context(), "ghost", db.Resources{VCPUs: 1})
	if err != db.ErrNoCapacity && err != db.ErrNotFound {
		t.Fatalf("unknown node: %v", err)
	}
}

func TestNodeShrinkKeepsReservations(t *testing.T) {
	s := dbtestOpen(t)
	// Two VMs of 2 vCPU on a 4-vCPU node; then the node reports 2.
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	own := owner(t, s)
	two := db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 20}
	vm1 := create(t, s, db.CreateSandboxParams{Owner: own, Reserve: two})
	vm2 := create(t, s, db.CreateSandboxParams{Owner: own, Reserve: two})
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 4 {
		t.Fatalf("reserved before shrink: %+v", got)
	}

	// Shrink totals below the live reservation: the counters must stay
	// at 4 — clamping them down would steal room from running VMs.
	if err := s.UpsertNodeInfo(t.Context(), db.NodeInfo{
		ID: "node-a", Capacity: db.Resources{VCPUs: 2, MemoryMiB: 2048},
		DiskFreeGiB: 0, // the node has no disk left for new sandboxes
		SeenAt:      time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 4 || got.MemoryMiB != 4096 || got.DiskGiB != 40 {
		t.Fatalf("reservation after shrink: %+v", got)
	}

	// No new placement fits while both reservations stand.
	if err := s.ReserveCapacity(t.Context(), "node-a", db.Resources{VCPUs: 1}); err != db.ErrNoCapacity {
		t.Fatalf("placement above shrunk totals: %v", err)
	}
	// Candidates exclude the over-subscribed node.
	if nodes, err := s.NodesWithCapacity(t.Context(), db.Resources{VCPUs: 1}, time.Minute); err != nil || len(nodes) != 0 {
		t.Fatalf("candidates while overbooked: %v %+v", err, nodes)
	}

	// Releasing ONE VM frees exactly its room; the other's stays.
	if _, _, err := s.AcceptSandboxDeletion(t.Context(), own, vm1.Sandbox.ID, db.DeletionExplicit); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseSandboxReservation(t.Context(), own, vm1.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 2 || got.MemoryMiB != 2048 || got.DiskGiB != 20 {
		t.Fatalf("reservation after one release: %+v", got)
	}
	if err := s.ReserveCapacity(t.Context(), "node-a", db.Resources{VCPUs: 1}); err != db.ErrNoCapacity {
		t.Fatalf("placement at capacity: %v", err)
	}
	// Releasing the second VM empties the node against the shrunk
	// totals.
	if _, _, err := s.AcceptSandboxDeletion(t.Context(), own, vm2.Sandbox.ID, db.DeletionExplicit); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseSandboxReservation(t.Context(), own, vm2.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 0 || got.MemoryMiB != 0 || got.DiskGiB != 0 {
		t.Fatalf("reservation after both releases: %+v", got)
	}
	if err := s.ReserveCapacity(t.Context(), "node-a", db.Resources{VCPUs: 2, MemoryMiB: 1024, DiskGiB: 10}); err != nil {
		t.Fatalf("reserve after full release: %v", err)
	}
}

func TestReleaseSandboxReservationExactlyOnce(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	own := owner(t, s)
	res := create(t, s, db.CreateSandboxParams{Owner: own})

	for range 3 {
		if err := s.ReleaseSandboxReservation(t.Context(), own, res.Sandbox.ID); err != nil {
			t.Fatalf("release: %v", err)
		}
	}
	if got := nodeReserved(t, s, "node-a"); got.VCPUs != 0 || got.MemoryMiB != 0 || got.DiskGiB != 0 {
		t.Fatalf("node reserved after repeated release: %+v", got)
	}
	// Foreign owner cannot release.
	other := owner(t, s)
	if err := s.ReleaseSandboxReservation(t.Context(), other, res.Sandbox.ID); err != db.ErrNotFound {
		t.Fatalf("foreign release: %v", err)
	}
}

func TestNodesWithCapacityFreshness(t *testing.T) {
	s := dbtestOpen(t)
	fresh := db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40}
	node(t, s, "fresh-node", fresh)
	node(t, s, "full-node", db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10})
	// A stale node: fresh once, then its clock drifts into the past.
	node(t, s, "stale-node", fresh)
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE nodes SET last_seen_at = now() - interval '10 minutes' WHERE id = 'stale-node'`); err != nil {
		t.Fatal(err)
	}

	need := db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 20}
	got, err := s.NodesWithCapacity(t.Context(), need, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "fresh-node" {
		t.Fatalf("candidates: %+v", got)
	}
}
