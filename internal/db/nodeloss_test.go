package db_test

// Node loss: last_up_at bookkeeping, the lost-node scan with its observer
// guard, and the per-sandbox transactions with their rechecks. PostgreSQL
// only: the tests skip unless BBRAFT_TEST_DATABASE_URL is set (dbtest.Open).

import (
	"encoding/json/v2"
	"errors"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// nlFixture is a sandbox on node-a with node-b and node-c registered, node-a
// not observed for an hour and the others now.
type nlFixture struct {
	s      *db.Store
	own    uuid.UUID
	sbx    db.Sandbox
	cutoff time.Time
}

// nlSetup creates the fixture's ready sandbox on node-a and makes node-a
// lost.
func nlSetup(t *testing.T) nlFixture {
	t.Helper()
	s := dbtestOpen(t)
	own := owner(t, s)
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		node(t, s, id, db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	}
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	nlDownFor(t, s, "node-a", time.Hour)
	return nlFixture{s: s, own: own, sbx: sbx, cutoff: time.Now().Add(-time.Minute)}
}

// nlDownFor puts nodeID's last connected observation ago in the past.
func nlDownFor(t *testing.T, s *db.Store, nodeID string, ago time.Duration) {
	t.Helper()
	rsmExec(t, s, `UPDATE nodes SET last_up_at = now() - make_interval(secs => $2::float8) WHERE id = $1`,
		nodeID, ago.Seconds())
}

// nlGeneration completes one generation of the fixture's sandbox with a
// disk of diskGiB.
func (f nlFixture) nlGeneration(t *testing.T, diskGiB uint64) db.Generation {
	t.Helper()
	g := genBegin(t, f.s, f.own, f.sbx.ID)
	res := genResult(time.Now())
	res.DiskBytes = diskGiB << 30
	gen, err := f.s.CompleteGeneration(t.Context(), g.ID, res, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("complete generation: %v", err)
	}
	return gen
}

// params names the fixture's sandbox at its current version.
func (f nlFixture) params(t *testing.T) db.NodeLossParams {
	t.Helper()
	cur, err := f.s.SandboxAny(t.Context(), f.own, f.sbx.ID)
	if err != nil {
		t.Fatal(err)
	}
	return db.NodeLossParams{
		Owner: f.own, SandboxID: f.sbx.ID, LostNode: "node-a", LostBefore: f.cutoff, Version: cur.Version,
	}
}

func (f nlFixture) sandbox(t *testing.T) db.Sandbox {
	t.Helper()
	cur, err := f.s.SandboxAny(t.Context(), f.own, f.sbx.ID)
	if err != nil {
		t.Fatal(err)
	}
	return cur
}

// openEvictions lists the fixture sandbox's open evictions as node/reason.
func (f nlFixture) openEvictions(t *testing.T) []string {
	t.Helper()
	evs, err := f.s.OpenEvictions(t.Context(), "node-a")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range evs {
		if e.SandboxID == f.sbx.ID {
			out = append(out, e.NodeID+"/"+e.Reason)
		}
	}
	return out
}

func (f nlFixture) events(t *testing.T, kind string) int {
	t.Helper()
	evs, err := f.s.SandboxEvents(t.Context(), f.own, f.sbx.ID, 200)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range evs {
		if e.Type == kind {
			n++
		}
	}
	return n
}

// TestNodeLastUpAt: a connected observation sets last_up_at, the stale
// marker of a disconnected observation leaves it.
func TestNodeLastUpAt(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	seen := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	if err := s.UpsertNodeInfo(t.Context(), db.NodeInfo{
		ID: "node-a", CPUModel: "c", QEMUVersion: "9", DefaultImage: "img",
		Capacity: db.Resources{VCPUs: 8, MemoryMiB: 8192}, DiskFreeGiB: 100, SeenAt: seen,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.TouchNodeSeen(t.Context(), "node-a", time.Unix(0, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	var lastSeen, lastUp time.Time
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT last_seen_at, last_up_at FROM nodes WHERE id = 'node-a'`).Scan(&lastSeen, &lastUp); err != nil {
		t.Fatal(err)
	}
	if !lastSeen.Equal(time.Unix(0, 0)) {
		t.Fatalf("last_seen_at = %s, want the epoch", lastSeen)
	}
	if !lastUp.Equal(seen) {
		t.Fatalf("last_up_at = %s, want %s", lastUp, seen)
	}
}

// TestLostNodes lists only nodes not observed since the cutoff, and nothing
// while no other node was observed after it.
func TestLostNodes(t *testing.T) {
	s := dbtestOpen(t)
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		node(t, s, id, db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	}
	cutoff := time.Now().Add(-time.Minute)
	for _, tc := range []struct {
		name string
		down []string
		want []string
	}{
		{"all observed", nil, nil},
		{"one lost", []string{"node-a"}, []string{"node-a"}},
		{"two lost, one observer", []string{"node-a", "node-c"}, []string{"node-a", "node-c"}},
		{"no observer", []string{"node-a", "node-b", "node-c"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rsmExec(t, s, `UPDATE nodes SET last_up_at = now()`)
			for _, id := range tc.down {
				nlDownFor(t, s, id, time.Hour)
			}
			got, err := s.LostNodes(t.Context(), cutoff)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("lost = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("lost = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestSandboxesOnLostNode lists ready-ish and archiving sandboxes on the
// node and archived ones whose disk is there, nothing else.
func TestSandboxesOnLostNode(t *testing.T) {
	f := nlSetup(t)
	other := phase1CreateReady(t, f.s, f.own, db.CreateSandboxParams{NodeID: "node-b"})
	stopped, _ := phase1Stopped(t, f.s, f.own, phase1CreateReady(t, f.s, f.own, db.CreateSandboxParams{}))
	provisioning := create(t, f.s, db.CreateSandboxParams{Owner: f.own}).Sandbox
	got, err := f.s.SandboxesOnLostNode(t.Context(), "node-a")
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, sbx := range got {
		ids[sbx.ID] = true
	}
	if !ids[f.sbx.ID] || !ids[stopped.ID] || ids[other.ID] || ids[provisioning.ID] || len(ids) != 2 {
		t.Fatalf("listed %v; want the ready %s and the archived %s only", ids, f.sbx.ID, stopped.ID)
	}
}

// TestMoveLostSandbox moves a ready sandbox of node-a to node-b from a
// generation with a larger disk.
func TestMoveLostSandbox(t *testing.T) {
	f := nlSetup(t)
	gen := f.nlGeneration(t, 15)
	before := f.sandbox(t)

	moved, job, err := f.s.MoveLostSandbox(t.Context(), f.params(t), db.NodeLossMove{
		ToNode: "node-b", GenerationID: gen.ID, ManifestKey: gen.ManifestKey,
	})
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	want := before.Desired
	want.DiskGiB = 15
	switch {
	case moved.State != db.StateProvisioning || derefStr(moved.NodeID) != "node-b" || moved.DiskNodeID != nil:
		t.Fatalf("moved: %s on %v disk %v", moved.State, moved.NodeID, moved.DiskNodeID)
	case moved.LifecycleGeneration != before.LifecycleGeneration+1:
		t.Fatalf("lifecycle generation = %d, want %d", moved.LifecycleGeneration, before.LifecycleGeneration+1)
	case moved.Desired != want || moved.Reserved != want:
		t.Fatalf("desired=%+v reserved=%+v, want %+v", moved.Desired, moved.Reserved, want)
	}
	if r := nodeReserved(t, f.s, "node-a"); r != (db.Resources{}) {
		t.Fatalf("node-a reserved = %+v", r)
	}
	if r := nodeReserved(t, f.s, "node-b"); r != want {
		t.Fatalf("node-b reserved = %+v, want %+v", r, want)
	}
	if ev := f.openEvictions(t); len(ev) != 1 || ev[0] != "node-a/"+db.EvictionReasonNodeLost {
		t.Fatalf("open evictions = %v", ev)
	}
	var p db.ResumePayload
	if err := json.Unmarshal([]byte(job.Payload), &p); err != nil {
		t.Fatal(err)
	}
	switch {
	case job.Kind != db.JobResume:
		t.Fatalf("job kind = %s", job.Kind)
	case p.NodeID != "node-b" || p.FromManifest != gen.ManifestKey || p.EvictNode != "node-a" ||
		p.Generation != moved.LifecycleGeneration || p.Resources != want || p.Image != gen.Image:
		t.Fatalf("payload = %+v", p)
	case job.SourceGenerationID == nil || *job.SourceGenerationID != gen.ID:
		t.Fatalf("source generation = %v, want %s", job.SourceGenerationID, gen.ID)
	}
	if n := f.events(t, db.EventSandboxMoved); n != 1 {
		t.Fatalf("sandbox.moved events = %d", n)
	}

	// A second worker deciding on the old row finds it moved.
	stale := db.NodeLossParams{Owner: f.own, SandboxID: f.sbx.ID, LostNode: "node-a", LostBefore: f.cutoff}
	if _, _, err := f.s.MoveLostSandbox(t.Context(), stale, db.NodeLossMove{
		ToNode: "node-c", GenerationID: gen.ID, ManifestKey: gen.ManifestKey,
	}); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("second move = %v, want ErrConflict", err)
	}
	if n := count(t, f.s, `SELECT count(*) FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.resume'`, f.sbx.ID); n != 1 {
		t.Fatalf("resume jobs = %d, want 1", n)
	}
}

// TestMoveLostSandboxRefusals: every refusal leaves the row, the nodes and
// the evictions as they were.
func TestMoveLostSandboxRefusals(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, f nlFixture, p *db.NodeLossParams, m *db.NodeLossMove)
		want    error
	}{
		{"node back", func(t *testing.T, f nlFixture, _ *db.NodeLossParams, _ *db.NodeLossMove) {
			rsmExec(t, f.s, `UPDATE nodes SET last_up_at = now() WHERE id = 'node-a'`)
		}, db.ErrNodeNotLost},
		{"no observer", func(t *testing.T, f nlFixture, _ *db.NodeLossParams, _ *db.NodeLossMove) {
			nlDownFor(t, f.s, "node-b", time.Hour)
			nlDownFor(t, f.s, "node-c", time.Hour)
		}, db.ErrNodeNotLost},
		{"stale version", func(t *testing.T, _ nlFixture, p *db.NodeLossParams, _ *db.NodeLossMove) {
			p.Version--
		}, db.ErrConflict},
		{"to the lost node", func(t *testing.T, _ nlFixture, _ *db.NodeLossParams, m *db.NodeLossMove) {
			m.ToNode = "node-a"
		}, db.ErrConflict},
		{"to a node with an evicted copy", func(t *testing.T, f nlFixture, _ *db.NodeLossParams, _ *db.NodeLossMove) {
			rsmExec(t, f.s, `INSERT INTO vm_evictions (id, node_id, sandbox_id, reason) VALUES ($1, 'node-b', $2, 'test')`,
				uuid.NewV7(), f.sbx.ID)
		}, db.ErrConflict},
		{"full target", func(t *testing.T, f nlFixture, _ *db.NodeLossParams, _ *db.NodeLossMove) {
			rsmExec(t, f.s, `UPDATE nodes SET reserved_vcpus = total_vcpus WHERE id = 'node-b'`)
		}, db.ErrNoCapacity},
		{"unknown target", func(t *testing.T, _ nlFixture, _ *db.NodeLossParams, m *db.NodeLossMove) {
			m.ToNode = "node-z"
		}, db.ErrNodeUnavailable},
		{"wrong manifest", func(t *testing.T, _ nlFixture, _ *db.NodeLossParams, m *db.NodeLossMove) {
			m.ManifestKey = "other"
		}, db.ErrSnapshotNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := nlSetup(t)
			gen := f.nlGeneration(t, 10)
			p := f.params(t)
			m := db.NodeLossMove{ToNode: "node-b", GenerationID: gen.ID, ManifestKey: gen.ManifestKey}
			tc.prepare(t, f, &p, &m)
			before := f.sandbox(t)
			resA, resB := nodeReserved(t, f.s, "node-a"), nodeReserved(t, f.s, "node-b")
			if _, _, err := f.s.MoveLostSandbox(t.Context(), p, m); !errors.Is(err, tc.want) {
				t.Fatalf("move = %v, want %v", err, tc.want)
			}
			if after := f.sandbox(t); after.Version != before.Version || after.State != db.StateReady {
				t.Fatalf("row changed: %s v%d -> %s v%d", before.State, before.Version, after.State, after.Version)
			}
			if nodeReserved(t, f.s, "node-a") != resA || nodeReserved(t, f.s, "node-b") != resB {
				t.Fatal("reservations changed")
			}
			if ev := f.openEvictions(t); len(ev) != 0 {
				t.Fatalf("node-a evictions = %v", ev)
			}
		})
	}
}

// TestFailLostSandbox fails a ready sandbox without a generation and
// refuses one that has a generation (it is moved instead).
func TestFailLostSandbox(t *testing.T) {
	t.Run("no generation", func(t *testing.T) {
		f := nlSetup(t)
		got, err := f.s.FailLostSandbox(t.Context(), f.params(t))
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case got.State != db.StateError || got.ErrorReason != db.NodeLostReason:
			t.Fatalf("state=%s reason=%q", got.State, got.ErrorReason)
		case got.NodeID != nil || got.DiskNodeID != nil || got.Reserved != (db.Resources{}):
			t.Fatalf("node=%v disk=%v reserved=%+v", got.NodeID, got.DiskNodeID, got.Reserved)
		}
		if r := nodeReserved(t, f.s, "node-a"); r != (db.Resources{}) {
			t.Fatalf("node-a reserved = %+v", r)
		}
		if ev := f.openEvictions(t); len(ev) != 1 {
			t.Fatalf("open evictions = %v", ev)
		}
		// The eviction is not the sandbox's current VM: reconciliation
		// deletes it.
		evs, err := f.s.OpenEvictions(t.Context(), "node-a")
		if err != nil || len(evs) != 1 || evs[0].Current {
			t.Fatalf("evictions = %+v, %v", evs, err)
		}
	})
	t.Run("with a generation", func(t *testing.T) {
		f := nlSetup(t)
		f.nlGeneration(t, 10)
		if _, err := f.s.FailLostSandbox(t.Context(), f.params(t)); !errors.Is(err, db.ErrConflict) {
			t.Fatalf("fail = %v, want ErrConflict", err)
		}
		if got := f.sandbox(t); got.State != db.StateReady {
			t.Fatalf("state = %s", got.State)
		}
	})
}

// TestEvictLostDisk gives up an archived sandbox's disk on the lost node
// when a generation exists, and refuses without one.
func TestEvictLostDisk(t *testing.T) {
	t.Run("with a generation", func(t *testing.T) {
		f := nlSetup(t)
		f.nlGeneration(t, 10)
		phase1Stopped(t, f.s, f.own, f.sbx)
		got, err := f.s.EvictLostDisk(t.Context(), f.params(t))
		if err != nil {
			t.Fatal(err)
		}
		if got.State != db.StateArchived || got.NodeID != nil || got.DiskNodeID != nil || got.Reserved != (db.Resources{}) {
			t.Fatalf("got %s node=%v disk=%v reserved=%+v", got.State, got.NodeID, got.DiskNodeID, got.Reserved)
		}
		if r := nodeReserved(t, f.s, "node-a"); r != (db.Resources{}) {
			t.Fatalf("node-a reserved = %+v", r)
		}
		if ev := f.openEvictions(t); len(ev) != 1 {
			t.Fatalf("open evictions = %v", ev)
		}
		if n := f.events(t, db.EventSandboxDiskEvicted); n != 1 {
			t.Fatalf("disk evicted events = %d", n)
		}
	})
	t.Run("without a generation", func(t *testing.T) {
		f := nlSetup(t)
		stopped, _ := phase1Stopped(t, f.s, f.own, f.sbx)
		if _, err := f.s.EvictLostDisk(t.Context(), f.params(t)); !errors.Is(err, db.ErrNoRestorableData) {
			t.Fatalf("evict = %v, want ErrNoRestorableData", err)
		}
		if got := f.sandbox(t); got.Version != stopped.Version || derefStr(got.DiskNodeID) != "node-a" {
			t.Fatalf("row changed: v%d disk %v", got.Version, got.DiskNodeID)
		}
	})
}

// TestFinishLostStop archives a sandbox whose stop is in flight on the lost
// node from its generation, and refuses without one.
func TestFinishLostStop(t *testing.T) {
	t.Run("with a generation", func(t *testing.T) {
		f := nlSetup(t)
		f.nlGeneration(t, 10)
		op, _, _, _, err := f.s.AcceptSandboxStop(t.Context(), f.own, f.sbx.ID, false)
		if err != nil {
			t.Fatal(err)
		}
		got, final, err := f.s.FinishLostStop(t.Context(), f.params(t))
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case got.State != db.StateArchived || got.StoppedAt == nil:
			t.Fatalf("state=%s stopped=%v", got.State, got.StoppedAt)
		case got.NodeID != nil || got.DiskNodeID != nil || got.Reserved != (db.Resources{}):
			t.Fatalf("node=%v disk=%v reserved=%+v", got.NodeID, got.DiskNodeID, got.Reserved)
		case final.ID != op.ID || final.Status != db.StopCompleted:
			t.Fatalf("operation = %+v", final)
		}
		if r := nodeReserved(t, f.s, "node-a"); r != (db.Resources{}) {
			t.Fatalf("node-a reserved = %+v", r)
		}
		if ev := f.openEvictions(t); len(ev) != 1 {
			t.Fatalf("open evictions = %v", ev)
		}
	})
	t.Run("without a generation", func(t *testing.T) {
		f := nlSetup(t)
		if _, _, _, _, err := f.s.AcceptSandboxStop(t.Context(), f.own, f.sbx.ID, false); err != nil {
			t.Fatal(err)
		}
		if _, _, err := f.s.FinishLostStop(t.Context(), f.params(t)); !errors.Is(err, db.ErrNoRestorableData) {
			t.Fatalf("finish = %v, want ErrNoRestorableData", err)
		}
		if got := f.sandbox(t); got.State != db.StateArchiving {
			t.Fatalf("state = %s", got.State)
		}
	})
}

// TestFinalizeStopDiskInStore: a stop of a cross-node resume with nothing on
// the new node archives with the disk only in the store and nothing
// reserved; the recheck refuses a sandbox that keeps a local disk, and the
// option excludes DiskErased.
func TestFinalizeStopDiskInStore(t *testing.T) {
	r := rsmSetup(t, 10)
	acc, err := r.s.AcceptSandboxResumeElsewhere(t.Context(), r.params("node-b"))
	if err != nil {
		t.Fatal(err)
	}
	op, archiving, _, _, err := r.s.AcceptSandboxStop(t.Context(), r.own, acc.Sandbox.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	job := phase1ClaimKind(t, r.s, "stop-worker", db.JobStop)
	p := db.FinalizeStopParams{Owner: r.own, SandboxID: r.sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version}

	both := p
	both.DiskErased, both.DiskInStore = true, true
	if _, _, err := r.s.FinalizeSandboxStop(t.Context(), job.ID, "stop-worker", job.LeaseToken, both); err == nil {
		t.Fatal("DiskErased with DiskInStore accepted")
	}

	p.DiskInStore = true
	stopped, final, err := r.s.FinalizeSandboxStop(t.Context(), job.ID, "stop-worker", job.LeaseToken, p)
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	switch {
	case stopped.State != db.StateArchived || stopped.DiskNodeID != nil || stopped.Reserved != (db.Resources{}):
		t.Fatalf("stopped %s disk=%v reserved=%+v", stopped.State, stopped.DiskNodeID, stopped.Reserved)
	case final.Status != db.StopCompleted:
		t.Fatalf("operation = %+v", final)
	}
	if res := nodeReserved(t, r.s, "node-b"); res != (db.Resources{}) {
		t.Fatalf("node-b reserved = %+v", res)
	}

	t.Run("local disk refused", func(t *testing.T) {
		f := nlSetup(t)
		f.nlGeneration(t, 10)
		op, archiving, _, _, err := f.s.AcceptSandboxStop(t.Context(), f.own, f.sbx.ID, false)
		if err != nil {
			t.Fatal(err)
		}
		job := phase1ClaimKind(t, f.s, "stop-worker", db.JobStop)
		_, _, err = f.s.FinalizeSandboxStop(t.Context(), job.ID, "stop-worker", job.LeaseToken, db.FinalizeStopParams{
			Owner: f.own, SandboxID: f.sbx.ID, StopID: op.ID, ExpectedVersion: archiving.Version, DiskInStore: true,
		})
		if !errors.Is(err, db.ErrNoRestorableData) {
			t.Fatalf("finalize = %v, want ErrNoRestorableData", err)
		}
		if got := f.sandbox(t); got.State != db.StateArchiving {
			t.Fatalf("state = %s", got.State)
		}
	})
}

// TestNodeLossMigrationDownUp rolls the node-loss migration back and applies
// it again; last_up_at is filled for existing nodes.
func TestNodeLossMigrationDownUp(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	if err := snapshotMigrateDownTo(t, s, 11); err != nil {
		t.Fatalf("down: %v", err)
	}
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatalf("up again: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM nodes WHERE last_up_at > now() - interval '1 hour'`); n != 1 {
		t.Fatalf("nodes with a recent last_up_at = %d, want 1", n)
	}
}

// derefStr reads an optional string.
func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
