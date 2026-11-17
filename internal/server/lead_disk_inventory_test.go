package server

import (
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

// TestLeadDiskFreeReportSurvivesDeletion requires BBRAFT_TEST_DATABASE_URL.
// It reproduces the real-node 94/51 GiB sequence through the production
// inventory adapter and a migrated, isolated PostgreSQL schema.
func TestLeadDiskFreeReportSurvivesDeletion(t *testing.T) {
	s := dbtest.Open(t)
	ctx := t.Context()
	account, err := s.EnsureAccount(ctx, "lead-disk-report")
	if err != nil {
		t.Fatal(err)
	}
	writer := inventoryWriter{store: s, now: time.Now}
	report := func(free uint64) {
		t.Helper()
		if err := writer.WriteNode(ctx, nodegateway.NodeState{
			NodeID: "lead-disk-node", Connected: true,
			Info: &nodev1.InfoResponse{
				NodeId: "lead-disk-node", Vcpus: 7, MemoryMib: 14967,
				DiskFreeMib: free * 1024, CpuModel: "test", QemuVersion: "test",
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	params := func() db.CreateSandboxParams {
		return db.CreateSandboxParams{
			ID: ids.Sandbox(), Owner: account.ID, NodeID: "lead-disk-node",
			MachineType: "default", MaxAttempts: 3,
			Reserve: db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51},
		}
	}
	report(94)
	first, err := s.CreateSandbox(ctx, params())
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	report(43) // Already excludes the live VM's 51 GiB disk promise.
	op, _, err := s.AcceptSandboxDeletion(ctx, account.ID, first.Sandbox.ID, db.DeletionExplicit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteDeletion(ctx, account.ID, op.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSandbox(ctx, params()); err != nil {
		t.Fatalf("immediate create after completed deletion: %v; disk report was already net of the old reservation", err)
	}
	// Replaying the old completion must not release the new VM's promise.
	if _, err := s.CompleteDeletion(ctx, account.ID, op.ID); err != nil {
		t.Fatal(err)
	}
	row, err := s.Node(ctx, "lead-disk-node")
	if err != nil {
		t.Fatal(err)
	}
	if row.Reserved.DiskGiB != 51 || row.Total.DiskGiB-row.Reserved.DiskGiB != 43 {
		t.Fatalf("after replay: total=%d reserved=%d, want 94/51", row.Total.DiskGiB, row.Reserved.DiskGiB)
	}
}
