package db_test

// Disk-pressure eviction: LowDiskNodes finds nodes under their free-disk
// threshold, DiskPressureCandidates lists only archived sandboxes whose
// retained disk a completed generation reproduces, and EvictSandboxDisk
// commits each confirmed delete under the row lock's recheck.
//
// Integration tests: they skip unless BBRAFT_TEST_DATABASE_URL is set
// (see dbtest).

import (
	"errors"
	"strings"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// dpStopped creates a ready sandbox on nodeID, stops it through the durable
// path and returns the archived row (retained local disk) with its stop.
func dpStopped(t *testing.T, s *db.Store, own uuid.UUID, nodeID string) (db.Sandbox, db.StopOperation) {
	t.Helper()
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{NodeID: nodeID})
	return phase1Stopped(t, s, own, sbx)
}

// dpCandidate is an archived sandbox on nodeID whose chain has a completed
// generation cut genSkew from the stop's requested_at: positive is the
// stop's final generation, zero the boundary, negative a stale one.
func dpCandidate(t *testing.T, s *db.Store, own uuid.UUID, nodeID string, genSkew time.Duration) db.Sandbox {
	t.Helper()
	sbx, op := dpStopped(t, s, own, nodeID)
	genComplete(t, s, own, sbx.ID, op.RequestedAt.Add(genSkew))
	return sbx
}

func TestLowDiskNodes(t *testing.T) {
	s := dbtestOpen(t)
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		node(t, s, id, db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	}
	// free = total - reserved: node-a 9, node-b 10, node-c 100 of 100.
	rsmExec(t, s, `UPDATE nodes SET reserved_disk_gib = 91 WHERE id = 'node-a'`)
	rsmExec(t, s, `UPDATE nodes SET reserved_disk_gib = 90 WHERE id = 'node-b'`)

	got, err := s.LowDiskNodes(t.Context(), 10)
	if err != nil {
		t.Fatalf("low disk nodes: %v", err)
	}
	if len(got) != 1 || got[0].ID != "node-a" {
		t.Fatalf("at 10%% got %v, want [node-a] (node-b sits exactly on the threshold)", nodeIDs(got))
	}
	got, err = s.LowDiskNodes(t.Context(), 9)
	if err != nil {
		t.Fatalf("low disk nodes at 9: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("at 9%% got %v, want none (free 9 of 100 is not below)", nodeIDs(got))
	}
	got, err = s.LowDiskNodes(t.Context(), 50)
	if err != nil {
		t.Fatalf("low disk nodes at 50: %v", err)
	}
	if len(got) != 2 || got[0].ID != "node-a" || got[1].ID != "node-b" {
		t.Fatalf("at 50%% got %v, want [node-a node-b]", nodeIDs(got))
	}
}

func nodeIDs(ns []db.Node) []string {
	out := make([]string, 0, len(ns))
	for _, n := range ns {
		out = append(out, n.ID)
	}
	return out
}

func TestDiskPressureCandidates(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	node(t, s, "node-b", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})

	// Eligible: archived on node-a with the stop's final generation, one at
	// the boundary (cut exactly at requested_at).
	first := dpCandidate(t, s, own, "node-a", time.Second)
	second := dpCandidate(t, s, own, "node-a", 0)
	// Oldest stop first: make the earlier stop unmistakably older.
	rsmExec(t, s, `UPDATE stop_operations SET requested_at = requested_at - interval '1 minute'
		WHERE sandbox_id = $1`, first.ID)

	excluded := map[string]db.Sandbox{
		// Archived but nothing completed: the disk is the only copy.
		"no generation": mustStopped(t, s, own, "node-a"),
		// A completed generation cut before the stop was requested.
		"stale generation": dpCandidate(t, s, own, "node-a", -time.Second),
		// Same shape on another node.
		"other node": dpCandidate(t, s, own, "node-b", time.Second),
		// Still ready, generation or not.
		"not archived": func() db.Sandbox {
			sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{NodeID: "node-a"})
			genComplete(t, s, own, sbx.ID, time.Now())
			return sbx
		}(),
		// Archived with a final generation, data already expired.
		"data expired": dpCandidate(t, s, own, "node-a", time.Second),
		// Archived, disk already given up (nothing local to evict).
		"disk in store": mustStopped(t, s, own, "node-a"),
		// Archived with a final generation, deleted since.
		"deleted": dpCandidate(t, s, own, "node-a", time.Second),
	}
	rsmExec(t, s, `UPDATE sandboxes SET data_expired_at = now() WHERE id = $1`, excluded["data expired"].ID)
	rsmExec(t, s, `UPDATE sandboxes SET disk_node_id = NULL WHERE id = $1`, excluded["disk in store"].ID)
	rsmExec(t, s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, excluded["deleted"].ID)
	// A purged generation does not count either: retire the row, then purge it.
	purged := dpCandidate(t, s, own, "node-a", time.Second)
	rsmExec(t, s, `UPDATE snapshots SET purged_at = now() WHERE sandbox_id = $1`, purged.ID)
	excluded["purged generation"] = purged

	got, err := s.DiskPressureCandidates(t.Context(), "node-a", 10)
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(got) != 2 || got[0].ID != first.ID || got[1].ID != second.ID {
		t.Fatalf("candidates = %v, want oldest-stopped-first [%s %s]", sandboxIDs(got), first.ID, second.ID)
	}
	listed := make(map[string]bool, len(got))
	for _, sbx := range got {
		listed[sbx.ID] = true
	}
	for name, sbx := range excluded {
		if listed[sbx.ID] {
			t.Fatalf("%s case (%s) was listed", name, sbx.ID)
		}
	}
}

func sandboxIDs(ss []db.Sandbox) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		out = append(out, s.ID)
	}
	return out
}

func mustStopped(t *testing.T, s *db.Store, own uuid.UUID, nodeID string) db.Sandbox {
	t.Helper()
	sbx, _ := dpStopped(t, s, own, nodeID)
	return sbx
}

func TestEvictSandboxDisk(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	sbx := dpCandidate(t, s, own, "node-a", time.Second)
	before := nodeReserved(t, s, "node-a").DiskGiB
	if before != 10 {
		t.Fatalf("node reserved disk before = %d, want the stopped 10 GiB", before)
	}

	out, err := s.EvictSandboxDisk(t.Context(), db.DiskPressureParams{
		Owner: own, SandboxID: sbx.ID, Node: "node-a", Version: sbx.Version,
	})
	if err != nil {
		t.Fatalf("evict: %v", err)
	}
	if out.State != db.StateArchived || out.DiskNodeID != nil || out.NodeID != nil {
		t.Fatalf("after evict: state=%s node=%v disk=%v, want archived with no node or local disk",
			out.State, out.NodeID, out.DiskNodeID)
	}
	if out.Version != sbx.Version+1 {
		t.Fatalf("version = %d, want %d", out.Version, sbx.Version+1)
	}
	if out.Reserved != (db.Resources{}) {
		t.Fatalf("sandbox reserved = %+v, want released", out.Reserved)
	}
	if got := nodeReserved(t, s, "node-a").DiskGiB; got != before-10 {
		t.Fatalf("node reserved disk = %d, want %d after release", got, before-10)
	}
	if n := count(t, s, `SELECT count(*) FROM vm_evictions
		WHERE sandbox_id = $1 AND node_id = 'node-a' AND reason = 'disk_pressure' AND done_at IS NULL`,
		sbx.ID); n != 1 {
		t.Fatalf("open disk_pressure evictions = %d, want 1", n)
	}
	events, err := s.SandboxEvents(t.Context(), own, sbx.ID, 50)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var evicted *db.SandboxEvent
	for i := range events {
		if events[i].Type == db.EventSandboxDiskEvicted {
			evicted = &events[i]
		}
	}
	if evicted == nil {
		t.Fatal("no sandbox.disk_evicted event")
	}
	if !strings.Contains(evicted.Data, "disk_pressure") {
		t.Fatalf("event data = %s, want reason disk_pressure", evicted.Data)
	}
}

func TestEvictSandboxDiskRefusals(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	node(t, s, "node-b", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})

	t.Run("version moved on", func(t *testing.T) {
		sbx := dpCandidate(t, s, own, "node-a", time.Second)
		_, err := s.EvictSandboxDisk(t.Context(), db.DiskPressureParams{
			Owner: own, SandboxID: sbx.ID, Node: "node-a", Version: sbx.Version + 1,
		})
		if !errors.Is(err, db.ErrConflict) {
			t.Fatalf("evict = %v, want ErrConflict", err)
		}
		untouched := dpRow(t, s, own, sbx.ID)
		if untouched.DiskNodeID == nil || *untouched.DiskNodeID != "node-a" {
			t.Fatalf("row changed on refusal: %+v", untouched)
		}
	})
	t.Run("disk on another node", func(t *testing.T) {
		sbx := dpCandidate(t, s, own, "node-a", time.Second)
		_, err := s.EvictSandboxDisk(t.Context(), db.DiskPressureParams{
			Owner: own, SandboxID: sbx.ID, Node: "node-b", Version: sbx.Version,
		})
		if !errors.Is(err, db.ErrConflict) {
			t.Fatalf("evict = %v, want ErrConflict", err)
		}
	})
	t.Run("resumed meanwhile", func(t *testing.T) {
		sbx := dpCandidate(t, s, own, "node-a", time.Second)
		// A resume that slipped in between the listing and the transaction
		// moves the row off archived; the recheck refuses.
		rsmExec(t, s, `UPDATE sandboxes SET state = 'provisioning' WHERE id = $1`, sbx.ID)
		_, err := s.EvictSandboxDisk(t.Context(), db.DiskPressureParams{
			Owner: own, SandboxID: sbx.ID, Node: "node-a", Version: sbx.Version,
		})
		if !errors.Is(err, db.ErrConflict) {
			t.Fatalf("evict = %v, want ErrConflict", err)
		}
	})
	t.Run("no completed generation", func(t *testing.T) {
		sbx := mustStopped(t, s, own, "node-a")
		_, err := s.EvictSandboxDisk(t.Context(), db.DiskPressureParams{
			Owner: own, SandboxID: sbx.ID, Node: "node-a", Version: sbx.Version,
		})
		if !errors.Is(err, db.ErrNoRestorableData) {
			t.Fatalf("evict = %v, want ErrNoRestorableData", err)
		}
	})
	t.Run("data expired", func(t *testing.T) {
		sbx := dpCandidate(t, s, own, "node-a", time.Second)
		rsmExec(t, s, `UPDATE sandboxes SET data_expired_at = now() WHERE id = $1`, sbx.ID)
		_, err := s.EvictSandboxDisk(t.Context(), db.DiskPressureParams{
			Owner: own, SandboxID: sbx.ID, Node: "node-a", Version: sbx.Version,
		})
		if !errors.Is(err, db.ErrNoRestorableData) {
			t.Fatalf("evict = %v, want ErrNoRestorableData", err)
		}
	})
	t.Run("deleted meanwhile", func(t *testing.T) {
		sbx := dpCandidate(t, s, own, "node-a", time.Second)
		rsmExec(t, s, `UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, sbx.ID)
		_, err := s.EvictSandboxDisk(t.Context(), db.DiskPressureParams{
			Owner: own, SandboxID: sbx.ID, Node: "node-a", Version: sbx.Version,
		})
		if !errors.Is(err, db.ErrNotFound) {
			t.Fatalf("evict = %v, want ErrNotFound", err)
		}
	})
	t.Run("no owner", func(t *testing.T) {
		sbx := dpCandidate(t, s, own, "node-a", time.Second)
		_, err := s.EvictSandboxDisk(t.Context(), db.DiskPressureParams{
			SandboxID: sbx.ID, Node: "node-a", Version: sbx.Version,
		})
		if !errors.Is(err, db.ErrNoOwner) {
			t.Fatalf("evict = %v, want ErrNoOwner", err)
		}
	})
}

// dpRow rereads one sandbox for fixture assertions.
func dpRow(t *testing.T, s *db.Store, own uuid.UUID, id string) db.Sandbox {
	t.Helper()
	sbx, err := s.Sandbox(t.Context(), own, id)
	if err != nil {
		t.Fatalf("sandbox %s: %v", id, err)
	}
	return sbx
}
