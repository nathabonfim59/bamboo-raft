package db_test

// Disk inventory baseline: a node reports the disk still free for NEW
// sandboxes, already net of the node's own reservations. The store keeps
// total = reported free + the reservations it holds at that moment, so
// total - reserved is exactly the reported free, and later server-side
// reservations and releases move it once each. Requires
// BBRAFT_TEST_DATABASE_URL (dbtestOpen skips without it).

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// diskReport writes one node report with the given free disk.
func diskReport(t *testing.T, s *db.Store, id string, freeGiB uint64) {
	t.Helper()
	err := s.UpsertNodeInfo(t.Context(), db.NodeInfo{
		ID: id, CPUModel: "test-cpu", QEMUVersion: "9.2", DefaultImage: "img",
		Capacity:    db.Resources{VCPUs: 8, MemoryMiB: 16384},
		DiskFreeGiB: freeGiB,
		SeenAt:      time.Now(),
	})
	if err != nil {
		t.Fatalf("report free %d GiB: %v", freeGiB, err)
	}
}

// diskAvail is total - reserved disk, signed so a double count is visible.
func diskAvail(t *testing.T, s *db.Store, id string) (avail int64, n db.Node) {
	t.Helper()
	n, err := s.Node(t.Context(), id)
	if err != nil {
		t.Fatalf("node %s: %v", id, err)
	}
	return int64(n.Total.DiskGiB) - int64(n.Reserved.DiskGiB), n
}

func TestUpsertNodeInfoDiskFreeIsNetOfReservations(t *testing.T) {
	s := dbtestOpen(t)
	diskReport(t, s, "node-a", 94)
	if avail, n := diskAvail(t, s, "node-a"); avail != 94 || n.Total.DiskGiB != 94 || n.Reserved.DiskGiB != 0 {
		t.Fatalf("fresh node total/reserved = %d/%d, want 94/0", n.Total.DiskGiB, n.Reserved.DiskGiB)
	}
	// The reservation the node will see: 51 GiB.
	if err := s.ReserveCapacity(t.Context(), "node-a", db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}); err != nil {
		t.Fatalf("reserve 51: %v", err)
	}
	// Next report: the node has the VM, free is 43 net of it.
	diskReport(t, s, "node-a", 43)
	avail, n := diskAvail(t, s, "node-a")
	if avail != 43 || n.Total.DiskGiB != 94 || n.Reserved.DiskGiB != 51 {
		t.Fatalf("after report free 43 total/reserved = %d/%d (avail %d), want 94/51 (avail 43)",
			n.Total.DiskGiB, n.Reserved.DiskGiB, avail)
	}
	// CPU and memory remain TOTAL offers, replaced by each report.
	if n.Total.VCPUs != 8 || n.Total.MemoryMiB != 16384 {
		t.Fatalf("cpu/memory total = %d/%d, want 8/16384", n.Total.VCPUs, n.Total.MemoryMiB)
	}
	// Placement admits exactly the free disk.
	if err := s.ReserveCapacity(t.Context(), "node-a", db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 44}); !errors.Is(err, db.ErrNoCapacity) {
		t.Fatalf("reserve 44 with 43 free: %v, want ErrNoCapacity", err)
	}
	if err := s.ReserveCapacity(t.Context(), "node-a", db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 43}); err != nil {
		t.Fatalf("reserve 43 with 43 free: %v", err)
	}
}

func TestDiskReportFullNodeKeepsReservations(t *testing.T) {
	s := dbtestOpen(t)
	diskReport(t, s, "node-a", 100)
	if err := s.ReserveCapacity(t.Context(), "node-a", db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 60}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// The node says it has nothing left: no room, but the 60 GiB reservation
	// is a live sandbox's and stays.
	diskReport(t, s, "node-a", 0)
	avail, n := diskAvail(t, s, "node-a")
	if avail != 0 || n.Reserved.DiskGiB != 60 || n.Total.DiskGiB != 60 {
		t.Fatalf("total/reserved = %d/%d (avail %d), want 60/60 (avail 0)", n.Total.DiskGiB, n.Reserved.DiskGiB, avail)
	}
	nodes, err := s.NodesWithCapacity(t.Context(), db.Resources{DiskGiB: 1}, time.Minute)
	if err != nil || len(nodes) != 0 {
		t.Fatalf("NodesWithCapacity(1 GiB) on a full node = %d nodes, err %v", len(nodes), err)
	}
}

func TestDiskReleaseAfterReportFreesOnceAndReplaysAreNoops(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	diskReport(t, s, "node-a", 94)
	res := create(t, s, db.CreateSandboxParams{
		Owner: own, NodeID: "node-a", Reserve: db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51},
	})
	diskReport(t, s, "node-a", 43)
	if avail, _ := diskAvail(t, s, "node-a"); avail != 43 {
		t.Fatalf("available before release = %d, want 43", avail)
	}
	for i := range 3 { // delivery 0 releases, 1 and 2 are replays
		if err := s.ReleaseSandboxReservation(t.Context(), own, res.Sandbox.ID); err != nil {
			t.Fatalf("release %d: %v", i, err)
		}
		if avail, n := diskAvail(t, s, "node-a"); avail != 94 || n.Reserved.DiskGiB != 0 {
			t.Fatalf("release %d: available/reserved = %d/%d, want 94/0", i, avail, n.Reserved.DiskGiB)
		}
	}
	// The node's report still includes the VM until its own delete finishes:
	// conservative for one poll, never inflated.
	diskReport(t, s, "node-a", 43)
	if avail, _ := diskAvail(t, s, "node-a"); avail != 43 {
		t.Fatalf("available after a report taken before the node finished deleting = %d, want 43", avail)
	}
	diskReport(t, s, "node-a", 94)
	if avail, _ := diskAvail(t, s, "node-a"); avail != 94 {
		t.Fatalf("available after the node finished deleting = %d, want 94", avail)
	}
}

// TestDiskReportRacingReserveHasOnlySerialOutcomes runs a report and a
// reservation at the same moment. The row lock serializes them, so exactly
// two outcomes are possible: the report first (it sees the old reservation,
// then the reserve lowers availability by R) or the reserve first (the report
// folds R into its baseline, so the report's stale free stands). Never a
// negative availability and never a lost reservation.
func TestDiskReportRacingReserveHasOnlySerialOutcomes(t *testing.T) {
	s := dbtestOpen(t)
	const free, reserve = 100, 30
	reportFirst, reserveFirst := 0, 0
	for i := range 40 {
		id := fmt.Sprintf("node-race-%02d", i)
		diskReport(t, s, id, free)
		start := make(chan struct{})
		var wg sync.WaitGroup
		var reportErr, reserveErr error
		wg.Go(func() {
			<-start
			reportErr = s.UpsertNodeInfo(t.Context(), db.NodeInfo{
				ID: id, CPUModel: "test-cpu", QEMUVersion: "9.2", DefaultImage: "img",
				Capacity:    db.Resources{VCPUs: 8, MemoryMiB: 16384},
				DiskFreeGiB: free, SeenAt: time.Now(),
			})
		})
		wg.Go(func() {
			<-start
			reserveErr = s.ReserveCapacity(t.Context(), id, db.Resources{DiskGiB: reserve})
		})
		close(start)
		wg.Wait()
		if reportErr != nil || reserveErr != nil {
			t.Fatalf("iteration %d: report %v, reserve %v", i, reportErr, reserveErr)
		}
		avail, n := diskAvail(t, s, id)
		if n.Reserved.DiskGiB != reserve {
			t.Fatalf("iteration %d: reserved = %d, want %d (lost update)", i, n.Reserved.DiskGiB, reserve)
		}
		switch avail {
		case free - reserve:
			reportFirst++
		case free:
			reserveFirst++
		default:
			t.Fatalf("iteration %d: available = %d, want %d (report first) or %d (reserve first)", i, avail, free-reserve, free)
		}
	}
	t.Logf("serial outcomes: report-first %d, reserve-first %d", reportFirst, reserveFirst)
}

// TestDiskDeterministicReserveBeforeReport pins the first serial order with
// no goroutines: a reservation that lands before the report is folded into
// that report's baseline.
func TestDiskDeterministicReserveBeforeReport(t *testing.T) {
	s := dbtestOpen(t)
	diskReport(t, s, "node-a", 100)
	if err := s.ReserveCapacity(t.Context(), "node-a", db.Resources{DiskGiB: 30}); err != nil {
		t.Fatal(err)
	}
	diskReport(t, s, "node-a", 100) // taken before the node saw the VM
	if avail, _ := diskAvail(t, s, "node-a"); avail != 100 {
		t.Fatalf("reserve then stale report: available %d, want 100 (bounded one-poll staleness)", avail)
	}
	diskReport(t, s, "node-a", 70) // the next poll corrects it
	if avail, _ := diskAvail(t, s, "node-a"); avail != 70 {
		t.Fatalf("after the correcting report: available %d, want 70", avail)
	}
}
