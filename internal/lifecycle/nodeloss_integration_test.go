package lifecycle_test

// Node loss: the sandboxes of a node not observed connected for the grace
// continue on another node from their last generation, become error
// node_lost when there is nothing to restore, or keep only their generation
// when stopped; a returning node has its stale copies deleted first. Also the
// stop that takes over a cross-node restore before anything exists on the new
// node. PostgreSQL plus nodefakes sharing one memstore through nodelink and
// the gateway; they skip unless BBRAFT_TEST_DATABASE_URL is set (dbtest.Open).
//
// No inventory poller runs here, so the tests write nodes.last_up_at
// themselves (nlUp, nlDown) to stand for the observations it would make.

import (
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
)

// nlGrace is the node-loss grace of the test workers.
const nlGrace = 500 * time.Millisecond

// nlWorker returns a Phase 2 worker with node loss on after grace, and waits
// until it has run for that grace (the startup guard).
func nlWorker(t *testing.T, r *rsmEnv, grace time.Duration, logs *genLogBuffer) *lifecycle.Worker {
	t.Helper()
	opts := lifecycle.Options{Placer: r.sched, NodeLostAfter: grace}
	if logs != nil {
		opts.Logger = slog.New(slog.NewTextHandler(logs, nil))
	}
	w := genWorker(t, r.lifecycleEnv, r.store, r.mem, opts)
	if grace < time.Minute {
		time.Sleep(grace + 50*time.Millisecond)
	}
	return w
}

// nlUp records a connected observation of the nodes now.
func nlUp(t *testing.T, r *rsmEnv, nodeIDs ...string) {
	t.Helper()
	for _, id := range nodeIDs {
		rsmExec(t, r, `UPDATE nodes SET last_up_at = now() WHERE id = $1`, id)
	}
}

// nlDown puts the node's last connected observation ago in the past.
func nlDown(t *testing.T, r *rsmEnv, nodeID string, ago time.Duration) {
	t.Helper()
	rsmExec(t, r, `UPDATE nodes SET last_up_at = now() - make_interval(secs => $2::float8) WHERE id = $1`,
		nodeID, ago.Seconds())
}

// nlGeneration cuts one generation of the ready sandbox and returns it.
func nlGeneration(t *testing.T, r *rsmEnv, sbx db.Sandbox) db.Generation {
	t.Helper()
	genMakeDue(t, r.lifecycleEnv, sbx)
	if genRound(t, r.w) != 1 {
		t.Fatal("no snapshot job")
	}
	gen, ok, err := r.store.LatestGeneration(t.Context(), r.own, sbx.ID)
	if err != nil || !ok {
		t.Fatalf("generation: %v, %v", ok, err)
	}
	return gen
}

// nlLoseA drops node-a's link and makes it lost past the grace, with node-b
// observed now.
func nlLoseA(t *testing.T, r *rsmEnv) {
	t.Helper()
	r.nodes["node-a"].disconnect(t)
	nlDown(t, r, "node-a", time.Hour)
	nlUp(t, r, "node-b")
}

// nlReturnA reconnects node-a and runs one reaper round with both nodes up.
func nlReturnA(t *testing.T, r *rsmEnv, w *lifecycle.Worker) {
	t.Helper()
	r.nodes["node-a"].connect(t)
	nlUp(t, r, "node-a", "node-b")
	w.ReapOnce(t.Context())
}

// TestNodeLossMovesReadySandbox is the node-loss exit: a ready sandbox on
// node-a with a completed generation, node-a's link gone. Before the grace
// nothing moves; after it the sandbox is provisioning on node-b (node-a's
// reservation released, an open node_lost eviction, sandbox.moved), and the
// resume job makes it ready there with the file written before the
// generation, continuing the chain. When node-a returns its copy is deleted
// first: never reported unknown, never snapshotted, never reserved.
func TestNodeLossMovesReadySandbox(t *testing.T) {
	r := newRsmEnv(t, "node-a", "node-b")
	a, b := r.nodes["node-a"], r.nodes["node-b"]
	logs := &genLogBuffer{}
	w := nlWorker(t, r, nlGrace, logs)
	sbx := r.bootWithNote(t, "node-a", "written before the loss")
	gen := nlGeneration(t, r, sbx)
	_, _, snapsBefore := a.counts()

	a.disconnect(t)
	nlUp(t, r, "node-a", "node-b") // the link is down, but within the grace
	w.ReapOnce(t.Context())
	if got := r.sandbox(t, r.own, sbx.ID); got.State != db.StateReady || derefString(got.NodeID) != "node-a" {
		t.Fatalf("within the grace: %s on %v, want ready on node-a", got.State, got.NodeID)
	}

	nlDown(t, r, "node-a", time.Hour)
	w.ReapOnce(t.Context())
	moved := r.sandbox(t, r.own, sbx.ID)
	switch {
	case moved.State != db.StateProvisioning || derefString(moved.NodeID) != "node-b":
		t.Fatalf("after the grace: %s on %v, want provisioning on node-b", moved.State, moved.NodeID)
	case moved.DiskNodeID != nil:
		t.Fatalf("disk node = %v, want none until the restore", *moved.DiskNodeID)
	case moved.LifecycleGeneration != sbx.LifecycleGeneration+1:
		t.Fatalf("lifecycle generation = %d, want %d", moved.LifecycleGeneration, sbx.LifecycleGeneration+1)
	}
	if res := r.nodeReserved(t, "node-a"); res != (db.Resources{}) {
		t.Fatalf("node-a still reserved: %+v", res)
	}
	if res := r.nodeReserved(t, "node-b"); res != moved.Reserved {
		t.Fatalf("node-b reserved = %+v, want %+v", res, moved.Reserved)
	}
	if ev := r.evictions(t, sbx.ID); len(ev) != 1 || ev[0].node != "node-a" || !ev[0].open ||
		ev[0].reason != db.EvictionReasonNodeLost {
		t.Fatalf("evictions = %+v, want one open node_lost row on node-a", ev)
	}
	if n := r.events(t, sbx.ID)[db.EventSandboxMoved]; n != 1 {
		t.Fatalf("sandbox.moved events = %d, want 1", n)
	}
	job, err := r.store.LatestSandboxJob(t.Context(), r.own, sbx.ID, db.JobResume)
	if err != nil {
		t.Fatal(err)
	}
	if p := r.payload(t, job); p.NodeID != "node-b" || p.FromManifest != gen.ManifestKey || p.EvictNode != "node-a" {
		t.Fatalf("resume payload = %+v, want node-b from %s evicting node-a", p, gen.ManifestKey)
	}
	if job.SourceGenerationID == nil || *job.SourceGenerationID != gen.ID {
		t.Fatalf("job source generation = %v, want %s", job.SourceGenerationID, gen.ID)
	}

	if !mustProcess(t, w) {
		t.Fatal("resume job not claimed")
	}
	got := r.sandbox(t, r.own, sbx.ID)
	switch {
	case got.State != db.StateReady:
		t.Fatalf("state = %s (%s)", got.State, got.ErrorReason)
	case derefString(got.NodeID) != "node-b", derefString(got.DiskNodeID) != "node-b":
		t.Fatalf("node=%v disk=%v, want node-b", got.NodeID, got.DiskNodeID)
	case got.ChainID != sbx.ChainID:
		t.Fatalf("chain = %s, want %s", got.ChainID, sbx.ChainID)
	}
	if note, err := os.ReadFile(rsmNotePath(b, sbx.ID)); err != nil || string(note) != "written before the loss" {
		t.Fatalf("note on node-b = %q, %v", note, err)
	}
	next := nlGeneration(t, r, got)
	if next.ChainID != gen.ChainID || next.Number != gen.Number+1 || next.NodeID != "node-b" {
		t.Fatalf("next generation chain=%s number=%d node=%s, want %s/%d/node-b",
			next.ChainID, next.Number, next.NodeID, gen.ChainID, gen.Number+1)
	}

	if !a.hasVM(t, sbx.ID) {
		t.Fatal("node-a lost its copy while away (test premise)")
	}
	nlReturnA(t, r, w)
	if a.hasVM(t, sbx.ID) {
		t.Fatal("reconciliation kept node-a's stale copy")
	}
	if ev := r.evictions(t, sbx.ID); len(ev) != 1 || ev[0].open {
		t.Fatalf("evictions after the return = %+v, want the row done", ev)
	}
	if n := logs.count("unknown VM preserved"); n != 0 {
		t.Fatalf("stale copy reported unknown %d times", n)
	}
	if res := r.nodeReserved(t, "node-a"); res != (db.Resources{}) {
		t.Fatalf("node-a reserved after the return: %+v", res)
	}
	nlGeneration(t, r, r.sandbox(t, r.own, sbx.ID))
	if _, _, snaps := a.counts(); snaps != snapsBefore {
		t.Fatalf("node-a SnapshotVM calls = %d after the loss, want %d", snaps, snapsBefore)
	}
	if after := r.sandbox(t, r.own, sbx.ID); after.State != db.StateReady || derefString(after.NodeID) != "node-b" {
		t.Fatalf("after the return: %s on %v", after.State, after.NodeID)
	}
}

// TestNodeLossBlipMovesNothing drops node-a's link for less than the grace:
// the round sees the link down but a recent observation, and nothing moves.
func TestNodeLossBlipMovesNothing(t *testing.T) {
	r := newRsmEnv(t, "node-a", "node-b")
	w := nlWorker(t, r, nlGrace, nil)
	sbx := r.bootWithNote(t, "node-a", "blip")
	nlGeneration(t, r, sbx)

	r.nodes["node-a"].disconnect(t)
	nlUp(t, r, "node-a", "node-b")
	w.ReapOnce(t.Context())
	nlReturnA(t, r, w)

	got := r.sandbox(t, r.own, sbx.ID)
	if got.State != db.StateReady || derefString(got.NodeID) != "node-a" || derefString(got.DiskNodeID) != "node-a" {
		t.Fatalf("after the blip: %s on %v (disk %v), want ready on node-a", got.State, got.NodeID, got.DiskNodeID)
	}
	if ev := r.evictions(t, sbx.ID); len(ev) != 0 {
		t.Fatalf("evictions = %+v, want none", ev)
	}
	if !r.nodes["node-a"].hasVM(t, sbx.ID) {
		t.Fatal("node-a's VM was deleted")
	}
}

// TestNodeLossWithoutGenerationFails: a ready sandbox with nothing to
// restore from (no generation yet, or snapshots off) becomes error
// node_lost with no node and nothing reserved; node-a's copy is deleted when
// it returns and the row stays as it was.
func TestNodeLossWithoutGenerationFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		boot func(t *testing.T, r *rsmEnv) db.Sandbox
	}{
		{"no generation yet", func(t *testing.T, r *rsmEnv) db.Sandbox {
			return r.bootWithNote(t, "node-a", "never snapshotted")
		}},
		{"snapshots off", func(t *testing.T, r *rsmEnv) db.Sandbox {
			return genCreate(t, r.lifecycleEnv, r.w, r.own, new(false))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRsmEnv(t, "node-a", "node-b")
			logs := &genLogBuffer{}
			w := nlWorker(t, r, nlGrace, logs)
			sbx := tc.boot(t, r)

			nlLoseA(t, r)
			w.ReapOnce(t.Context())
			got := r.sandbox(t, r.own, sbx.ID)
			switch {
			case got.State != db.StateError || got.ErrorReason != db.NodeLostReason:
				t.Fatalf("state=%s reason=%q, want error node_lost", got.State, got.ErrorReason)
			case got.NodeID != nil || got.DiskNodeID != nil:
				t.Fatalf("node=%v disk=%v, want none", got.NodeID, got.DiskNodeID)
			case got.Reserved != (db.Resources{}):
				t.Fatalf("reserved = %+v", got.Reserved)
			}
			if res := r.nodeReserved(t, "node-a"); res != (db.Resources{}) {
				t.Fatalf("node-a still reserved: %+v", res)
			}
			if ev := r.evictions(t, sbx.ID); len(ev) != 1 || ev[0].node != "node-a" || !ev[0].open {
				t.Fatalf("evictions = %+v, want one open node-a row", ev)
			}

			nlReturnA(t, r, w)
			if r.nodes["node-a"].hasVM(t, sbx.ID) {
				t.Fatal("node-a kept the failed sandbox's VM")
			}
			if n := logs.count("unknown VM preserved") + logs.count("evicted VM is the sandbox's current VM"); n != 0 {
				t.Fatalf("the copy was reported %d times instead of deleted", n)
			}
			if after := r.sandbox(t, r.own, sbx.ID); after.State != db.StateError || after.ErrorReason != db.NodeLostReason {
				t.Fatalf("after the return: %s (%s)", after.State, after.ErrorReason)
			}
		})
	}
}

// TestNodeLossArchivedSandbox: an archived sandbox whose disk is on the lost
// node keeps only its generation (no node, nothing reserved, an eviction)
// and later resumes on node-b with the file; without a generation the disk
// is the only copy and the sandbox is left alone.
func TestNodeLossArchivedSandbox(t *testing.T) {
	t.Run("with a generation", func(t *testing.T) {
		r := newRsmEnv(t, "node-a", "node-b")
		w := nlWorker(t, r, nlGrace, nil)
		sbx := r.bootWithNote(t, "node-a", "archived then lost")
		r.stop(t, sbx.ID) // final generation

		nlLoseA(t, r)
		w.ReapOnce(t.Context())
		got := r.sandbox(t, r.own, sbx.ID)
		switch {
		case got.State != db.StateArchived:
			t.Fatalf("state = %s", got.State)
		case got.NodeID != nil || got.DiskNodeID != nil || got.Reserved != (db.Resources{}):
			t.Fatalf("node=%v disk=%v reserved=%+v, want none", got.NodeID, got.DiskNodeID, got.Reserved)
		}
		if res := r.nodeReserved(t, "node-a"); res != (db.Resources{}) {
			t.Fatalf("node-a still reserved: %+v", res)
		}
		if ev := r.evictions(t, sbx.ID); len(ev) != 1 || ev[0].node != "node-a" || !ev[0].open {
			t.Fatalf("evictions = %+v, want one open node-a row", ev)
		}

		acc := r.resume(t, sbx.ID)
		if p := r.payload(t, acc.Job); p.NodeID != "node-b" || p.FromManifest == "" {
			t.Fatalf("resume payload = %+v, want a manifest restore on node-b", p)
		}
		if !mustProcess(t, w) {
			t.Fatal("resume job not claimed")
		}
		if after := r.sandbox(t, r.own, sbx.ID); after.State != db.StateReady || derefString(after.DiskNodeID) != "node-b" {
			t.Fatalf("after the resume: %s (disk %v)", after.State, after.DiskNodeID)
		}
		if note, err := os.ReadFile(rsmNotePath(r.nodes["node-b"], sbx.ID)); err != nil || string(note) != "archived then lost" {
			t.Fatalf("note on node-b = %q, %v", note, err)
		}
	})
	t.Run("without a generation", func(t *testing.T) {
		r := newRsmEnv(t, "node-a", "node-b")
		w := nlWorker(t, r, nlGrace, nil)
		sbx := r.bootWithNote(t, "node-a", "only copy")
		p1 := r.worker(t, lifecycle.Options{}) // stops without a final generation
		r.acceptStop(t, r.own, sbx.ID, false)
		if !mustProcess(t, p1) {
			t.Fatal("stop job not claimed")
		}
		before := r.sandbox(t, r.own, sbx.ID)

		nlLoseA(t, r)
		w.ReapOnce(t.Context())
		got := r.sandbox(t, r.own, sbx.ID)
		if got.Version != before.Version || derefString(got.DiskNodeID) != "node-a" || got.Reserved != before.Reserved {
			t.Fatalf("archived without a generation changed: %+v -> %+v", before, got)
		}
		if ev := r.evictions(t, sbx.ID); len(ev) != 0 {
			t.Fatalf("evictions = %+v, want none", ev)
		}
	})
}

// TestNodeLossArchivingSandbox: a stop in flight on the lost node finishes
// archived from the last generation (operation completed, no node, nothing
// reserved, an eviction); its queued stop job then has nothing to do and the
// sandbox resumes on node-b. Without a generation the stop is left to fail.
func TestNodeLossArchivingSandbox(t *testing.T) {
	t.Run("with a generation", func(t *testing.T) {
		r := newRsmEnv(t, "node-a", "node-b")
		w := nlWorker(t, r, nlGrace, nil)
		sbx := r.bootWithNote(t, "node-a", "stopping when lost")
		nlGeneration(t, r, sbx)
		op := r.acceptStop(t, r.own, sbx.ID, false)

		nlLoseA(t, r)
		w.ReapOnce(t.Context())
		got := r.sandbox(t, r.own, sbx.ID)
		switch {
		case got.State != db.StateArchived:
			t.Fatalf("state = %s (%s)", got.State, got.ErrorReason)
		case got.NodeID != nil || got.DiskNodeID != nil || got.Reserved != (db.Resources{}):
			t.Fatalf("node=%v disk=%v reserved=%+v, want none", got.NodeID, got.DiskNodeID, got.Reserved)
		case got.StoppedAt == nil:
			t.Fatal("stopped_at not set")
		}
		if final, err := r.store.StopOperation(t.Context(), r.own, sbx.ID, op.ID); err != nil || final.Status != db.StopCompleted {
			t.Fatalf("stop operation = %+v, %v; want completed", final, err)
		}
		if res := r.nodeReserved(t, "node-a"); res != (db.Resources{}) {
			t.Fatalf("node-a still reserved: %+v", res)
		}
		if ev := r.evictions(t, sbx.ID); len(ev) != 1 || ev[0].node != "node-a" || !ev[0].open {
			t.Fatalf("evictions = %+v", ev)
		}
		if !mustProcess(t, w) {
			t.Fatal("stop job not claimed")
		}
		if after := r.sandbox(t, r.own, sbx.ID); after.Version != got.Version {
			t.Fatalf("the stale stop job changed the row: %s v%d", after.State, after.Version)
		}

		r.resume(t, sbx.ID)
		if !mustProcess(t, w) {
			t.Fatal("resume job not claimed")
		}
		if note, err := os.ReadFile(rsmNotePath(r.nodes["node-b"], sbx.ID)); err != nil || string(note) != "stopping when lost" {
			t.Fatalf("note on node-b = %q, %v", note, err)
		}
	})
	t.Run("without a generation", func(t *testing.T) {
		r := newRsmEnv(t, "node-a", "node-b")
		w := nlWorker(t, r, nlGrace, nil)
		sbx := r.bootWithNote(t, "node-a", "no generation")
		op := r.acceptStop(t, r.own, sbx.ID, false)

		nlLoseA(t, r)
		w.ReapOnce(t.Context())
		got := r.sandbox(t, r.own, sbx.ID)
		if got.State != db.StateArchiving || derefString(got.NodeID) != "node-a" {
			t.Fatalf("state=%s node=%v, want archiving on node-a", got.State, got.NodeID)
		}
		if final, err := r.store.StopOperation(t.Context(), r.own, sbx.ID, op.ID); err != nil || final.Status == db.StopCompleted {
			t.Fatalf("stop operation = %+v, %v; want still open", final, err)
		}
		if ev := r.evictions(t, sbx.ID); len(ev) != 0 {
			t.Fatalf("evictions = %+v, want none", ev)
		}
	})
}

// TestNodeLossTwoWorkersMoveOnce runs the detector of two lifecycle workers
// at the same time: the sandbox moves exactly once.
func TestNodeLossTwoWorkersMoveOnce(t *testing.T) {
	r := newRsmEnv(t, "node-a", "node-b")
	w1 := nlWorker(t, r, nlGrace, nil)
	w2 := nlWorker(t, r, nlGrace, nil)
	sbx := r.bootWithNote(t, "node-a", "once")
	nlGeneration(t, r, sbx)

	nlLoseA(t, r)
	var wg sync.WaitGroup
	for _, w := range []*lifecycle.Worker{w1, w2} {
		wg.Go(func() { w.ReapOnce(t.Context()) })
	}
	wg.Wait()

	got := r.sandbox(t, r.own, sbx.ID)
	if got.State != db.StateProvisioning || got.LifecycleGeneration != sbx.LifecycleGeneration+1 {
		t.Fatalf("state=%s lifecycle generation=%d, want provisioning at %d",
			got.State, got.LifecycleGeneration, sbx.LifecycleGeneration+1)
	}
	if n := r.count(t, `SELECT count(*) FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.resume'`, sbx.ID); n != 1 {
		t.Fatalf("resume jobs = %d, want 1", n)
	}
	if n := r.events(t, sbx.ID)[db.EventSandboxMoved]; n != 1 {
		t.Fatalf("sandbox.moved events = %d, want 1", n)
	}
	if ev := r.evictions(t, sbx.ID); len(ev) != 1 {
		t.Fatalf("evictions = %+v, want 1", ev)
	}
	if res := r.nodeReserved(t, "node-b"); res != got.Reserved {
		t.Fatalf("node-b reserved = %+v, want %+v (one reservation)", res, got.Reserved)
	}
}

// TestNodeLossWaitsForRoom: with no other node that has room the sandbox
// stays where it is (its reservation kept, one warning however many rounds),
// and it moves on a later round once room appears.
func TestNodeLossWaitsForRoom(t *testing.T) {
	r := newRsmEnv(t, "node-a", "node-b")
	logs := &genLogBuffer{}
	w := nlWorker(t, r, nlGrace, logs)
	sbx := r.bootWithNote(t, "node-a", "waits")
	nlGeneration(t, r, sbx)
	r.fill(t, "node-b")

	nlLoseA(t, r)
	w.ReapOnce(t.Context())
	nlUp(t, r, "node-b")
	w.ReapOnce(t.Context())
	got := r.sandbox(t, r.own, sbx.ID)
	if got.State != db.StateReady || derefString(got.NodeID) != "node-a" || got.Reserved != sbx.Reserved {
		t.Fatalf("without room: %s on %v reserved %+v, want ready on node-a as before", got.State, got.NodeID, got.Reserved)
	}
	if n := logs.count("no node can take the sandbox of a lost node"); n != 1 {
		t.Fatalf("no-room warnings = %d, want 1", n)
	}

	rsmExec(t, r, `UPDATE nodes SET reserved_vcpus = 0 WHERE id = 'node-b'`)
	nlUp(t, r, "node-b")
	w.ReapOnce(t.Context())
	if got := r.sandbox(t, r.own, sbx.ID); got.State != db.StateProvisioning || derefString(got.NodeID) != "node-b" {
		t.Fatalf("with room: %s on %v, want provisioning on node-b", got.State, got.NodeID)
	}
}

// TestNodeLossGuards covers the guards: a worker that has not run for the
// grace yet, a round where no other node was observed within the grace, a
// worker without an object store (Phase 1 mode) and a node that still answers
// although its inventory is stale. None moves anything; the observer case
// moves once another node is observed again.
func TestNodeLossGuards(t *testing.T) {
	t.Run("worker younger than the grace", func(t *testing.T) {
		r := newRsmEnv(t, "node-a", "node-b")
		w := nlWorker(t, r, time.Hour, nil)
		sbx := r.bootWithNote(t, "node-a", "young worker")
		nlGeneration(t, r, sbx)
		r.nodes["node-a"].disconnect(t)
		nlDown(t, r, "node-a", 2*time.Hour)
		nlUp(t, r, "node-b")
		w.ReapOnce(t.Context())
		if got := r.sandbox(t, r.own, sbx.ID); got.State != db.StateReady || derefString(got.NodeID) != "node-a" {
			t.Fatalf("%s on %v, want ready on node-a", got.State, got.NodeID)
		}
	})
	t.Run("no other node observed", func(t *testing.T) {
		r := newRsmEnv(t, "node-a", "node-b")
		w := nlWorker(t, r, nlGrace, nil)
		sbx := r.bootWithNote(t, "node-a", "blind observer")
		nlGeneration(t, r, sbx)
		r.nodes["node-a"].disconnect(t)
		nlDown(t, r, "node-a", time.Hour)
		nlDown(t, r, "node-b", time.Hour)
		w.ReapOnce(t.Context())
		if got := r.sandbox(t, r.own, sbx.ID); got.State != db.StateReady || derefString(got.NodeID) != "node-a" {
			t.Fatalf("%s on %v, want ready on node-a", got.State, got.NodeID)
		}
		nlUp(t, r, "node-b")
		w.ReapOnce(t.Context())
		if got := r.sandbox(t, r.own, sbx.ID); derefString(got.NodeID) != "node-b" {
			t.Fatalf("with node-b observed: %s on %v, want node-b", got.State, got.NodeID)
		}
	})
	t.Run("no object store", func(t *testing.T) {
		r := newRsmEnv(t, "node-a", "node-b")
		w := r.worker(t, lifecycle.Options{Placer: r.sched, NodeLostAfter: nlGrace})
		time.Sleep(nlGrace + 50*time.Millisecond)
		sbx := r.bootWithNote(t, "node-a", "phase 1 mode")
		nlLoseA(t, r)
		w.ReapOnce(t.Context())
		if got := r.sandbox(t, r.own, sbx.ID); got.State != db.StateReady || derefString(got.NodeID) != "node-a" {
			t.Fatalf("%s on %v, want ready on node-a", got.State, got.NodeID)
		}
		if ev := r.evictions(t, sbx.ID); len(ev) != 0 {
			t.Fatalf("evictions = %+v, want none", ev)
		}
	})
	t.Run("node still answers", func(t *testing.T) {
		r := newRsmEnv(t, "node-a", "node-b")
		logs := &genLogBuffer{}
		w := nlWorker(t, r, nlGrace, logs)
		sbx := r.bootWithNote(t, "node-a", "stale inventory")
		nlGeneration(t, r, sbx)
		nlDown(t, r, "node-a", time.Hour) // inventory stale, link up
		nlUp(t, r, "node-b")
		w.ReapOnce(t.Context())
		if got := r.sandbox(t, r.own, sbx.ID); got.State != db.StateReady || derefString(got.NodeID) != "node-a" {
			t.Fatalf("%s on %v, want ready on node-a", got.State, got.NodeID)
		}
		if logs.count("the node answers") == 0 {
			t.Fatal("the answering node was not reported")
		}
	})
}

// TestStopDuringCrossNodeBootBeforeRestoreArchivesFromStore: a stop accepted
// while a cross-node resume is queued, before anything exists on node-b,
// finds no VM there. The disk was never local and the store has the
// generation, so the stop archives the sandbox with no local disk and
// nothing reserved (not error vm_missing), and the next resume restores it
// on node-b with the file.
func TestStopDuringCrossNodeBootBeforeRestoreArchivesFromStore(t *testing.T) {
	r := newRsmEnv(t, "node-a", "node-b")
	b := r.nodes["node-b"]
	sbx := r.bootWithNote(t, "node-a", "stopped before the restore")
	r.stop(t, sbx.ID)
	r.fill(t, "node-a")
	acc := r.resume(t, sbx.ID)
	if p := r.payload(t, acc.Job); p.NodeID != "node-b" || p.FromManifest == "" {
		t.Fatalf("resume payload = %+v, want a manifest restore on node-b", p)
	}
	if b.hasVM(t, sbx.ID) {
		t.Fatal("test premise: node-b already holds a VM")
	}

	stopped := r.stop(t, sbx.ID) // cancels the queued resume job; fails today with error vm_missing
	switch {
	case stopped.DiskNodeID != nil:
		t.Fatalf("disk node = %v, want none", *stopped.DiskNodeID)
	case stopped.Reserved != (db.Resources{}):
		t.Fatalf("reserved = %+v, want none", stopped.Reserved)
	case stopped.ErrorReason != "":
		t.Fatalf("error reason = %q", stopped.ErrorReason)
	}
	if res := r.nodeReserved(t, "node-b"); res != (db.Resources{}) {
		t.Fatalf("node-b still reserved: %+v", res)
	}

	acc = r.resume(t, sbx.ID)
	if p := r.payload(t, acc.Job); p.NodeID != "node-b" || p.FromManifest == "" {
		t.Fatalf("second resume payload = %+v, want a manifest restore on node-b", p)
	}
	if !mustProcess(t, r.w) {
		t.Fatal("resume job not claimed")
	}
	if got := r.sandbox(t, r.own, sbx.ID); got.State != db.StateReady || derefString(got.DiskNodeID) != "node-b" {
		t.Fatalf("after the resume: %s (%s) disk %v", got.State, got.ErrorReason, got.DiskNodeID)
	}
	if note, err := os.ReadFile(rsmNotePath(b, sbx.ID)); err != nil || string(note) != "stopped before the restore" {
		t.Fatalf("note on node-b = %q, %v", note, err)
	}
}
