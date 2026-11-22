package lifecycle_test

// Disk-pressure eviction: with an object store, a node whose reported free
// disk drops below the configured share of its total gives up the retained
// disks of its archived sandboxes — least recently stopped first — until it
// is back above it. Every evicted disk equals the stop's final generation,
// so a later resume restores it from the store; a stopped sandbox without
// that generation is the only copy and is never touched. PostgreSQL plus
// nodefakes sharing one memstore, reached through nodelink and the gateway.
// They skip unless BBRAFT_TEST_DATABASE_URL is set (dbtest.Open).

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// dpStartNode registers and connects one lifecycleNode — DeleteVM calls
// recorded, failure scriptable — on the shared object store.
func dpStartNode(t *testing.T, e *lifecycleEnv, id string, mem *memstore.Store) (*lifecycleNode, string) {
	t.Helper()
	e.registerNode(t, id, db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	root := t.TempDir()
	n := newLifecycleNode(id, root)
	n.Store = mem
	srv := grpc.NewServer(nodelink.ServerOptions()...)
	nodev1.RegisterNodeServer(srv, n)
	// Node link lifetime: joined by t.Cleanup, so not the test context.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = nodelink.Serve(ctx, nodelink.DialConfig{
			Addr: e.addr, NodeID: id, Token: "node-token", Log: quiet(),
			MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		}, srv)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })
	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()
	if _, err := e.hub.Wait(waitCtx, id); err != nil {
		t.Fatalf("node %s did not connect: %v", id, err)
	}
	return n, root
}

// dpEnv is several nodes on one memstore, a scheduler that resumes
// elsewhere, and a Phase 2 worker with disk-pressure eviction at percent.
type dpEnv struct {
	*lifecycleEnv
	nodes map[string]*lifecycleNode
	roots map[string]string
	mem   *memstore.Store
	w     *lifecycle.Worker
	sched *scheduler.Scheduler
	own   uuid.UUID
}

func newDPEnv(t *testing.T, percent int, nodeIDs ...string) *dpEnv {
	t.Helper()
	e := newLifecycleEnv(t)
	r := &dpEnv{
		lifecycleEnv: e, mem: memstore.New(),
		nodes: map[string]*lifecycleNode{}, roots: map[string]string{},
	}
	for _, id := range nodeIDs {
		n, root := dpStartNode(t, e, id, r.mem)
		r.nodes[id], r.roots[id] = n, root
	}
	dial := func(nodeID string) (scheduler.NodeClient, io.Closer, error) {
		c, err := e.factory.Dial(nodeID)
		if err != nil {
			return nil, nil, err
		}
		return c, c, nil
	}
	var err error
	r.sched, err = scheduler.New(scheduler.Options{
		Store: e.store, Log: quiet(), Dial: dial,
		FreshWithin: time.Minute, ProbeTimeout: 500 * time.Millisecond,
		ResumeElsewhere: e.store,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.w = genWorker(t, e, e.store, r.mem, lifecycle.Options{
		Placer:         r.sched,
		DiskLowPercent: percent,
	})
	r.own = e.owner(t)
	return r
}

// boot creates and boots a sandbox on nodeID and writes note.txt into its
// disk.
func (r *dpEnv) boot(t *testing.T, nodeID, note string) db.Sandbox {
	t.Helper()
	res, err := r.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: r.own, MachineType: "small", Name: "dp", NodeID: nodeID,
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}, MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create sandbox: %v", err)
	}
	if !mustProcess(t, r.w) {
		t.Fatal("provision job not claimed")
	}
	sbx := r.sandbox(t, r.own, res.Sandbox.ID)
	if sbx.State != db.StateReady {
		t.Fatalf("sandbox after boot: %s", sbx.State)
	}
	if err := r.store.SetSnapshotSchedule(t.Context(), r.own, sbx.ID, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	path := dpNotePath(r.roots[nodeID], sbx.ID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(note), 0o644); err != nil {
		t.Fatal(err)
	}
	return sbx
}

func dpNotePath(root, sandboxID string) string {
	return filepath.Join(root, "vms", sandboxID, "home", "user", "note.txt")
}

// dpHasVM reports whether the fake itself holds a VM for sandboxID.
func dpHasVM(t *testing.T, n *lifecycleNode, sandboxID string) bool {
	t.Helper()
	_, err := n.Node.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: sandboxID})
	if status.Code(err) == codes.NotFound {
		return false
	}
	if err != nil {
		t.Fatalf("GetVM: %v", err)
	}
	return true
}

// stop archives the sandbox through the Phase 2 worker (final generation).
func (r *dpEnv) stop(t *testing.T, sandboxID string) db.Sandbox {
	t.Helper()
	r.acceptStop(t, r.own, sandboxID, false)
	if !mustProcess(t, r.w) {
		t.Fatal("stop job not claimed")
	}
	return r.expectArchived(t, sandboxID)
}

// stopNoGen archives the sandbox through a Phase 1 worker: no final
// generation, so the kept disk is its only copy.
func (r *dpEnv) stopNoGen(t *testing.T, sandboxID string) db.Sandbox {
	t.Helper()
	p1 := r.worker(t, lifecycle.Options{})
	r.acceptStop(t, r.own, sandboxID, false)
	if !mustProcess(t, p1) {
		t.Fatal("stop job not claimed")
	}
	return r.expectArchived(t, sandboxID)
}

func (r *dpEnv) expectArchived(t *testing.T, sandboxID string) db.Sandbox {
	t.Helper()
	sbx := r.sandbox(t, r.own, sandboxID)
	if sbx.State != db.StateArchived {
		t.Fatalf("sandbox after stop: %s (%s)", sbx.State, sbx.ErrorReason)
	}
	return sbx
}

// lowDisk reports nodeID's free disk as total minus reservedGiB, below the
// configured line for the fixtures' 100 GiB nodes.
func (r *dpEnv) lowDisk(t *testing.T, nodeID string, reservedGiB int64) {
	t.Helper()
	if _, err := r.store.Pool().Exec(t.Context(),
		`UPDATE nodes SET reserved_disk_gib = $2 WHERE id = $1`, nodeID, reservedGiB); err != nil {
		t.Fatal(err)
	}
}

// olderStop pushes one sandbox's stop request back a minute so ordering by
// the request is deterministic.
func (r *dpEnv) olderStop(t *testing.T, sandboxID string) {
	t.Helper()
	if _, err := r.store.Pool().Exec(t.Context(),
		`UPDATE stop_operations SET requested_at = requested_at - interval '1 minute'
		 WHERE sandbox_id = $1`, sandboxID); err != nil {
		t.Fatal(err)
	}
}

// fill reserves all of a node's vCPUs in the inventory.
func (r *dpEnv) fill(t *testing.T, nodeID string) {
	t.Helper()
	if _, err := r.store.Pool().Exec(t.Context(),
		`UPDATE nodes SET reserved_vcpus = total_vcpus WHERE id = $1`, nodeID); err != nil {
		t.Fatal(err)
	}
}

func (r *dpEnv) resume(t *testing.T, sandboxID string) db.ResumeAcceptance {
	t.Helper()
	acc, err := r.sched.Resume(t.Context(), scheduler.ResumeRequest{Owner: r.own, SandboxID: sandboxID})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	return acc
}

func (r *dpEnv) events(t *testing.T, sandboxID string) map[string]int {
	t.Helper()
	events, err := r.store.SandboxEvents(t.Context(), r.own, sandboxID, 200)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int{}
	for _, ev := range events {
		out[ev.Type]++
	}
	return out
}

// evicted asserts the disk-pressure outcome of one archived sandbox whose
// copy delete already ran: no node or disk left, copy gone, the event.
func (r *dpEnv) evicted(t *testing.T, n *lifecycleNode, id string) {
	t.Helper()
	got := r.sandbox(t, r.own, id)
	if got.State != db.StateArchived || got.NodeID != nil || got.DiskNodeID != nil || got.Reserved != (db.Resources{}) {
		t.Fatalf("%s after evict: state=%s node=%v disk=%v reserved=%+v, want archived with the disk given up",
			id, got.State, got.NodeID, got.DiskNodeID, got.Reserved)
	}
	if dpHasVM(t, n, id) {
		t.Fatalf("%s still on the node after evict", id)
	}
	if n := r.events(t, id)[db.EventSandboxDiskEvicted]; n != 1 {
		t.Fatalf("%s disk_evicted events = %d, want 1", id, n)
	}
}

// dpEviction is one vm_evictions row; open is done_at IS NULL.
type dpEviction struct {
	node, reason string
	open         bool
}

func (r *dpEnv) evictions(t *testing.T, sandboxID string) []dpEviction {
	t.Helper()
	rows, err := r.store.Pool().Query(t.Context(), `
		SELECT node_id, reason, done_at IS NULL FROM vm_evictions
		WHERE sandbox_id = $1 ORDER BY created_at, id`, sandboxID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []dpEviction
	for rows.Next() {
		var ev dpEviction
		if err := rows.Scan(&ev.node, &ev.reason, &ev.open); err != nil {
			t.Fatal(err)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// keptDisk asserts the sandbox still holds its retained disk on nodeID.
func (r *dpEnv) keptDisk(t *testing.T, n *lifecycleNode, nodeID, id string) {
	t.Helper()
	got := r.sandbox(t, r.own, id)
	if got.State != db.StateArchived || derefString(got.DiskNodeID) != nodeID || got.Reserved.DiskGiB == 0 {
		t.Fatalf("%s: state=%s disk=%v reserved=%+v, want archived keeping its disk on %s",
			id, got.State, got.DiskNodeID, got.Reserved, nodeID)
	}
	if !dpHasVM(t, n, id) {
		t.Fatalf("%s gone from the node", id)
	}
}

// TestDiskPressureEvictsEligibleStops is the core scenario: three archived
// sandboxes on a low node — two with their stop's final generation and one
// without. The eligible two are deleted on the node in least-recently-
// stopped order and their rows give the disk up with a sandbox.disk_evicted
// event; the one whose disk is the only copy keeps everything.
func TestDiskPressureEvictsEligibleStops(t *testing.T) {
	r := newDPEnv(t, 10, "node-a")
	a := r.nodes["node-a"]

	oldest := r.boot(t, "node-a", "one")
	r.stop(t, oldest.ID)
	second := r.boot(t, "node-a", "two")
	r.stop(t, second.ID)
	onlyCopy := r.boot(t, "node-a", "three")
	stoppedNoGen := r.stopNoGen(t, onlyCopy.ID)
	r.olderStop(t, oldest.ID)

	r.lowDisk(t, "node-a", 95) // 5 GiB of 100 reported free
	r.w.ReapOnce(t.Context())

	if deletes := a.deleteCalls(); len(deletes) != 2 || deletes[0] != oldest.ID || deletes[1] != second.ID {
		t.Fatalf("DeleteVM calls = %v, want oldest-stopped-first [%s %s]", deletes, oldest.ID, second.ID)
	}
	r.evicted(t, a, oldest.ID)
	r.evicted(t, a, second.ID)
	// 95 less the two released GiB.
	if got := r.nodeReserved(t, "node-a").DiskGiB; got != 93 {
		t.Fatalf("node-a reserved disk = %d, want 93 after two releases", got)
	}
	got := r.sandbox(t, r.own, onlyCopy.ID)
	if got.Version != stoppedNoGen.Version {
		t.Fatalf("only-copy row changed: %+v -> %+v", stoppedNoGen, got)
	}
	r.keptDisk(t, a, "node-a", onlyCopy.ID)
	if n := r.events(t, onlyCopy.ID)[db.EventSandboxDiskEvicted]; n != 0 {
		t.Fatalf("only-copy evicted events = %d, want 0", n)
	}
}

// TestDiskPressureNodeAboveThreshold: a node with enough free disk loses
// nothing.
func TestDiskPressureNodeAboveThreshold(t *testing.T) {
	r := newDPEnv(t, 10, "node-a")
	a := r.nodes["node-a"]
	sbx := r.boot(t, "node-a", "kept")
	r.stop(t, sbx.ID)

	r.w.ReapOnce(t.Context())

	if deletes := a.deleteCalls(); len(deletes) != 0 {
		t.Fatalf("DeleteVM calls = %v, want none", deletes)
	}
	r.keptDisk(t, a, "node-a", sbx.ID)
}

// TestDiskPressureDisabled: a zero percent turns the eviction off even on a
// low node with an object store.
func TestDiskPressureDisabled(t *testing.T) {
	r := newDPEnv(t, 0, "node-a")
	a := r.nodes["node-a"]
	sbx := r.boot(t, "node-a", "kept")
	r.stop(t, sbx.ID)
	r.lowDisk(t, "node-a", 95)

	r.w.ReapOnce(t.Context())

	if deletes := a.deleteCalls(); len(deletes) != 0 {
		t.Fatalf("DeleteVM calls = %v, want none", deletes)
	}
	r.keptDisk(t, a, "node-a", sbx.ID)
}

// TestDiskPressureNoObjectStore: without an object store the kept disks
// are the only copies, so a low node loses nothing. The fixture's worker
// still has one for the stop's final generation; the Phase 1 worker running
// the round has none.
func TestDiskPressureNoObjectStore(t *testing.T) {
	r := newDPEnv(t, 10, "node-a")
	a := r.nodes["node-a"]
	sbx := r.boot(t, "node-a", "kept")
	r.stop(t, sbx.ID)
	r.lowDisk(t, "node-a", 95)

	p1 := r.worker(t, lifecycle.Options{DiskLowPercent: 10})
	p1.ReapOnce(t.Context())

	if deletes := a.deleteCalls(); len(deletes) != 0 {
		t.Fatalf("DeleteVM calls = %v, want none", deletes)
	}
	r.keptDisk(t, a, "node-a", sbx.ID)
}

// TestDiskPressureDeleteVMFailLeavesOpenEviction: a refused delete after
// the commit does not roll anything back — the disk is given up in the
// row, the node's copy is an open eviction, and the sandbox resumes from
// the store meanwhile. Once deletes work again the copy goes and the row
// closes.
func TestDiskPressureDeleteVMFailLeavesOpenEviction(t *testing.T) {
	r := newDPEnv(t, 10, "node-a", "node-b")
	a, b := r.nodes["node-a"], r.nodes["node-b"]
	sbx := r.boot(t, "node-a", "delete fails")
	r.stop(t, sbx.ID)
	r.lowDisk(t, "node-a", 95)

	a.setDeleteHook(func(context.Context, *nodev1.DeleteVMRequest) (*nodev1.DeleteVMResponse, error) {
		return nil, status.Error(codes.Unavailable, "node busy")
	})
	r.w.ReapOnce(t.Context())

	got := r.sandbox(t, r.own, sbx.ID)
	if got.State != db.StateArchived || got.NodeID != nil || got.DiskNodeID != nil || got.Reserved != (db.Resources{}) {
		t.Fatalf("after the commit: state=%s node=%v disk=%v reserved=%+v, want archived, nothing held",
			got.State, got.NodeID, got.DiskNodeID, got.Reserved)
	}
	if ev := r.evictions(t, sbx.ID); len(ev) != 1 || ev[0].node != "node-a" || !ev[0].open ||
		ev[0].reason != db.EvictionReasonDiskPressure {
		t.Fatalf("evictions = %+v, want one open node-a disk_pressure row", ev)
	}
	if !dpHasVM(t, a, sbx.ID) {
		t.Fatal("the refused delete removed the copy anyway")
	}

	// Resumable from the store while the copy is still there: the open
	// eviction keeps node-a out of placement, so it lands on node-b.
	a.setDeleteHook(nil)
	r.resume(t, sbx.ID)
	if !mustProcess(t, r.w) {
		t.Fatal("resume job not claimed")
	}
	got = r.sandbox(t, r.own, sbx.ID)
	if got.State != db.StateReady || derefString(got.DiskNodeID) != "node-b" {
		t.Fatalf("after the resume: %s (disk %v), want ready on node-b", got.State, got.DiskNodeID)
	}
	if note, err := os.ReadFile(dpNotePath(r.roots["node-b"], sbx.ID)); err != nil || string(note) != "delete fails" {
		t.Fatalf("note on node-b = %q, %v", note, err)
	}
	// The copy's delete succeeded during the resume or this round; either
	// way the open row closes and nothing of the sandbox stays on node-a.
	r.w.ReapOnce(t.Context())
	if dpHasVM(t, a, sbx.ID) {
		t.Fatal("node-a still holds the evicted copy")
	}
	if ev := r.evictions(t, sbx.ID); len(ev) != 1 || ev[0].open {
		t.Fatalf("evictions = %+v, want the disk_pressure row closed", ev)
	}
	if !dpHasVM(t, b, sbx.ID) {
		t.Fatal("node-b does not hold the restored VM")
	}
}

// dpRaceStore fires after inside EvictSandboxDisk once the eviction of
// resumeID has committed — the window where the row already dropped the
// disk but the node's copy still exists.
type dpRaceStore struct {
	*db.Store
	resumeID string
	after    func()
	once     sync.Once
}

func (s *dpRaceStore) EvictSandboxDisk(ctx context.Context, p db.DiskPressureParams) (db.Sandbox, error) {
	sbx, err := s.Store.EvictSandboxDisk(ctx, p)
	if err == nil && p.SandboxID == s.resumeID && s.after != nil {
		s.once.Do(s.after)
	}
	return sbx, err
}

// TestDiskPressureResumeAfterCommit is the race the commit order exists
// for: a resume admitted right after the eviction commit — before the
// node's copy is deleted — sees no local disk and restores from the store
// instead of starting a VM whose disk is already gone.
func TestDiskPressureResumeAfterCommit(t *testing.T) {
	r := newDPEnv(t, 10, "node-a", "node-b")
	a, b := r.nodes["node-a"], r.nodes["node-b"]
	sbx := r.boot(t, "node-a", "committed then resumed")
	r.stop(t, sbx.ID)
	r.lowDisk(t, "node-a", 95)

	var resumeErr error
	store := &dpRaceStore{Store: r.store, resumeID: sbx.ID}
	store.after = func() {
		_, resumeErr = r.sched.Resume(context.WithoutCancel(t.Context()),
			scheduler.ResumeRequest{Owner: r.own, SandboxID: sbx.ID})
	}
	w := genWorker(t, r.lifecycleEnv, store, r.mem, lifecycle.Options{
		Placer:         r.sched,
		DiskLowPercent: 10,
	})

	w.ReapOnce(t.Context())

	if resumeErr != nil {
		t.Fatalf("resume admitted after the eviction commit: %v", resumeErr)
	}
	if dpHasVM(t, a, sbx.ID) {
		t.Fatal("node-a still holds the evicted copy")
	}
	if ev := r.evictions(t, sbx.ID); len(ev) != 1 || ev[0].open || ev[0].reason != db.EvictionReasonDiskPressure {
		t.Fatalf("evictions = %+v, want the disk_pressure row closed", ev)
	}
	if !mustProcess(t, w) {
		t.Fatal("resume job not claimed")
	}
	got := r.sandbox(t, r.own, sbx.ID)
	if got.State != db.StateReady || derefString(got.DiskNodeID) != "node-b" {
		t.Fatalf("after the resume: %s (disk %v), want ready on node-b", got.State, got.DiskNodeID)
	}
	if note, err := os.ReadFile(dpNotePath(r.roots["node-b"], sbx.ID)); err != nil || string(note) != "committed then resumed" {
		t.Fatalf("note on node-b = %q, %v", note, err)
	}
	if !dpHasVM(t, b, sbx.ID) {
		t.Fatal("node-b does not hold the restored VM")
	}
}

// dpResumeStore slips one resume acceptance in between the reaper's
// candidate listing and its per-sandbox decision for resumeID.
type dpResumeStore struct {
	*db.Store
	resumeID  string
	do        func()
	once      sync.Once
	resumeErr error
}

func (s *dpResumeStore) SandboxAny(ctx context.Context, own uuid.UUID, id string) (db.Sandbox, error) {
	if id == s.resumeID && s.do != nil {
		s.once.Do(s.do)
	}
	return s.Store.SandboxAny(ctx, own, id)
}

// TestDiskPressureResumeWins: a sandbox resumed between the candidate
// listing and the eviction decision is left alone — the resume owns it, the
// local disk stays for its same-node start, and no eviction is committed.
func TestDiskPressureResumeWins(t *testing.T) {
	r := newDPEnv(t, 10, "node-a")
	a := r.nodes["node-a"]
	sbx := r.boot(t, "node-a", "resume wins")
	r.stop(t, sbx.ID)
	r.lowDisk(t, "node-a", 95)

	store := &dpResumeStore{Store: r.store, resumeID: sbx.ID}
	store.do = func() {
		_, err := r.sched.Resume(context.WithoutCancel(t.Context()),
			scheduler.ResumeRequest{Owner: r.own, SandboxID: sbx.ID})
		store.resumeErr = err
	}
	w := genWorker(t, r.lifecycleEnv, store, r.mem, lifecycle.Options{
		Placer:         r.sched,
		DiskLowPercent: 10,
	})

	w.ReapOnce(t.Context())

	if store.resumeErr != nil {
		t.Fatalf("injected resume: %v", store.resumeErr)
	}
	if deletes := a.deleteCalls(); len(deletes) != 0 {
		t.Fatalf("DeleteVM calls = %v, want none — the resumed disk must stay", deletes)
	}
	got := r.sandbox(t, r.own, sbx.ID)
	if got.State == db.StateArchived {
		t.Fatal("the injected resume did not take the row")
	}
	if n := r.events(t, sbx.ID)[db.EventSandboxDiskEvicted]; n != 0 {
		t.Fatalf("evicted events = %d, want 0", n)
	}
	if ev := r.evictions(t, sbx.ID); len(ev) != 0 {
		t.Fatalf("evictions = %+v, want none", ev)
	}

	// The accepted resume completes on node-a: the disk was never deleted.
	if !mustProcess(t, w) {
		t.Fatal("resume job not claimed")
	}
	got = r.sandbox(t, r.own, sbx.ID)
	if got.State != db.StateReady || derefString(got.DiskNodeID) != "node-a" {
		t.Fatalf("after the resume: %s (disk %v)", got.State, got.DiskNodeID)
	}
}

// TestDiskPressureEvictedResumesFromStore: a sandbox whose disk was evicted
// under disk pressure resumes from its last generation through the
// resume-elsewhere path, landing on another node with its files.
func TestDiskPressureEvictedResumesFromStore(t *testing.T) {
	r := newDPEnv(t, 10, "node-a", "node-b")
	a, b := r.nodes["node-a"], r.nodes["node-b"]
	sbx := r.boot(t, "node-a", "evicted then resumed")
	r.stop(t, sbx.ID)
	r.lowDisk(t, "node-a", 95)

	r.w.ReapOnce(t.Context())
	r.evicted(t, a, sbx.ID)
	if ev := r.evictions(t, sbx.ID); len(ev) != 1 || ev[0].open || ev[0].reason != db.EvictionReasonDiskPressure {
		t.Fatalf("evictions = %+v, want the disk_pressure row closed", ev)
	}

	r.fill(t, "node-a")
	acc := r.resume(t, sbx.ID)
	if !mustProcess(t, r.w) {
		t.Fatal("resume job not claimed")
	}
	got := r.sandbox(t, r.own, sbx.ID)
	switch {
	case got.State != db.StateReady:
		t.Fatalf("state = %s (%s)", got.State, got.ErrorReason)
	case derefString(got.NodeID) != "node-b" || derefString(got.DiskNodeID) != "node-b":
		t.Fatalf("node=%v disk=%v, want node-b", got.NodeID, got.DiskNodeID)
	case acc.Sandbox.ID != sbx.ID:
		t.Fatalf("resumed %s, want %s", acc.Sandbox.ID, sbx.ID)
	}
	if note, err := os.ReadFile(dpNotePath(r.roots["node-b"], sbx.ID)); err != nil || string(note) != "evicted then resumed" {
		t.Fatalf("note on node-b = %q, %v", note, err)
	}
	if !dpHasVM(t, b, sbx.ID) {
		t.Fatal("node-b does not hold the restored VM")
	}
}

// TestNewRejectsBadDiskLowPercent covers the worker option's bounds.
func TestNewRejectsBadDiskLowPercent(t *testing.T) {
	var store *db.Store // typed nil: New must reject the percent before use
	for _, bad := range []int{-1, 91} {
		if _, err := lifecycle.New(lifecycle.Options{
			Store:          store,
			Logger:         quiet(),
			DiskLowPercent: bad,
			Dial: func(string) (lifecycle.NodeClient, io.Closer, error) {
				return nil, nil, status.Error(codes.Internal, "not used")
			},
		}); err == nil {
			t.Fatalf("DiskLowPercent %d accepted", bad)
		}
	}
}
