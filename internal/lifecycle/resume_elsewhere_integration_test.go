package lifecycle_test

// Resume on another node: an archived sandbox whose disk node is full
// or disconnected restores its last completed generation on another node; the
// old copy is deleted, or recorded as an eviction that VM reconciliation
// deletes when the node returns. PostgreSQL plus nodefakes sharing one
// memstore, reached through nodelink and the gateway. They skip unless
// BBRAFT_TEST_DATABASE_URL is set (dbtest.Open).

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
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
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// rsmNode wraps nodefake to count CreateVM/StartVM/SnapshotVM calls, script
// CreateVM refusals and drop or restore its node link on demand.
type rsmNode struct {
	*nodefake.Node
	id   string
	root string
	e    *lifecycleEnv

	mu         sync.Mutex
	createHook func(*nodev1.CreateVMRequest) error // non-nil error: refuse
	creates    []*nodev1.CreateVMRequest
	starts     int
	snapshots  int
	cancel     context.CancelFunc
	done       chan struct{}
}

func (n *rsmNode) CreateVM(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
	n.mu.Lock()
	n.creates = append(n.creates, req)
	hook := n.createHook
	n.mu.Unlock()
	if hook != nil {
		if err := hook(req); err != nil {
			return nil, err
		}
	}
	return n.Node.CreateVM(ctx, req)
}

func (n *rsmNode) StartVM(ctx context.Context, req *nodev1.StartVMRequest) (*nodev1.VM, error) {
	n.mu.Lock()
	n.starts++
	n.mu.Unlock()
	return n.Node.StartVM(ctx, req)
}

func (n *rsmNode) SnapshotVM(ctx context.Context, req *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
	n.mu.Lock()
	n.snapshots++
	n.mu.Unlock()
	return n.Node.SnapshotVM(ctx, req)
}

func (n *rsmNode) setCreateHook(h func(*nodev1.CreateVMRequest) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.createHook = h
}

// counts reports manifest CreateVM calls, StartVM calls and SnapshotVM calls.
func (n *rsmNode) counts() (manifestCreates, starts, snapshots int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, c := range n.creates {
		if c.GetFromManifest() != "" {
			manifestCreates++
		}
	}
	return manifestCreates, n.starts, n.snapshots
}

// hasVM reports whether the fake itself holds a VM for sandboxID, whatever
// its link state.
func (n *rsmNode) hasVM(t *testing.T, sandboxID string) bool {
	t.Helper()
	_, err := n.Node.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: sandboxID})
	if status.Code(err) == codes.NotFound {
		return false
	}
	if err != nil {
		t.Fatalf("GetVM on %s: %v", n.id, err)
	}
	return true
}

// connect serves the node over a fresh nodelink connection and waits for the
// hub to see it.
func (n *rsmNode) connect(t *testing.T) {
	t.Helper()
	srv := grpc.NewServer(nodelink.ServerOptions()...)
	nodev1.RegisterNodeServer(srv, n)
	// Node link lifetime: ended by disconnect or t.Cleanup.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = nodelink.Serve(ctx, nodelink.DialConfig{
			Addr: n.e.addr, NodeID: n.id, Token: "node-token", Log: quiet(),
			MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		}, srv)
		close(done)
	}()
	n.mu.Lock()
	n.cancel, n.done = cancel, done
	n.mu.Unlock()
	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()
	if _, err := n.e.hub.Wait(waitCtx, n.id); err != nil {
		t.Fatalf("node %s did not connect: %v", n.id, err)
	}
}

// disconnect drops the node link; the fake keeps its VMs.
func (n *rsmNode) disconnect(t *testing.T) {
	t.Helper()
	n.mu.Lock()
	cancel, done := n.cancel, n.done
	n.cancel, n.done = nil, nil
	n.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
	waitFor(t, "node "+n.id+" gone from the gateway", func() bool {
		c, err := n.e.factory.Dial(n.id)
		if err != nil {
			return true
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		_, err = c.Info(ctx, &nodev1.InfoRequest{})
		return err != nil
	})
}

// rsmStartNode registers and connects one rsmNode on the shared store.
func rsmStartNode(t *testing.T, e *lifecycleEnv, id string, mem *memstore.Store) *rsmNode {
	t.Helper()
	e.registerNode(t, id, db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	root := t.TempDir()
	fake := nodefake.New(id, root)
	fake.VCPUs = 64
	fake.MemoryMiB = 256 << 10
	fake.Store = mem
	n := &rsmNode{Node: fake, id: id, root: root, e: e}
	n.connect(t)
	t.Cleanup(func() {
		n.mu.Lock()
		cancel, done := n.cancel, n.done
		n.mu.Unlock()
		if cancel != nil {
			cancel()
			<-done
		}
	})
	return n
}

// rsmEnv is several nodes on one memstore, a Phase 2 worker with a Placer,
// and a scheduler that resumes elsewhere.
type rsmEnv struct {
	*lifecycleEnv
	nodes map[string]*rsmNode
	mem   *memstore.Store
	w     *lifecycle.Worker
	sched *scheduler.Scheduler
	own   uuid.UUID
	logs  *genLogBuffer
}

func newRsmEnv(t *testing.T, nodeIDs ...string) *rsmEnv {
	t.Helper()
	e := newLifecycleEnv(t)
	r := &rsmEnv{lifecycleEnv: e, nodes: map[string]*rsmNode{}, mem: memstore.New(), logs: &genLogBuffer{}}
	for _, id := range nodeIDs {
		r.nodes[id] = rsmStartNode(t, e, id, r.mem)
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
		Placer: r.sched,
		Logger: slog.New(slog.NewTextHandler(r.logs, nil)),
	})
	r.own = e.owner(t)
	return r
}

// bootWithNote creates and boots a sandbox on nodeID and writes note.txt
// into its disk.
func (r *rsmEnv) bootWithNote(t *testing.T, nodeID, note string) db.Sandbox {
	t.Helper()
	res, err := r.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: r.own, MachineType: "small", Name: "rsm", NodeID: nodeID,
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
	r.writeNote(t, r.nodes[nodeID], sbx.ID, note)
	return sbx
}

func (r *rsmEnv) writeNote(t *testing.T, n *rsmNode, sandboxID, note string) {
	t.Helper()
	path := rsmNotePath(n, sandboxID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(note), 0o644); err != nil {
		t.Fatal(err)
	}
}

func rsmNotePath(n *rsmNode, sandboxID string) string {
	return filepath.Join(n.root, "vms", sandboxID, "home", "user", "note.txt")
}

// stop archives the sandbox through the worker (final generation included).
func (r *rsmEnv) stop(t *testing.T, sandboxID string) db.Sandbox {
	t.Helper()
	r.acceptStop(t, r.own, sandboxID, false)
	if !mustProcess(t, r.w) {
		t.Fatal("stop job not claimed")
	}
	sbx := r.sandbox(t, r.own, sandboxID)
	if sbx.State != db.StateArchived {
		t.Fatalf("sandbox after stop: %s (%s)", sbx.State, sbx.ErrorReason)
	}
	return sbx
}

// fill reserves all of a node's vCPUs in the inventory.
func (r *rsmEnv) fill(t *testing.T, nodeID string) {
	t.Helper()
	if _, err := r.store.Pool().Exec(t.Context(),
		`UPDATE nodes SET reserved_vcpus = total_vcpus WHERE id = $1`, nodeID); err != nil {
		t.Fatal(err)
	}
}

func (r *rsmEnv) resume(t *testing.T, sandboxID string) db.ResumeAcceptance {
	t.Helper()
	acc, err := r.sched.Resume(t.Context(), scheduler.ResumeRequest{Owner: r.own, SandboxID: sandboxID})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	return acc
}

func (r *rsmEnv) payload(t *testing.T, job db.Job) db.ResumePayload {
	t.Helper()
	var p db.ResumePayload
	if err := json.Unmarshal([]byte(job.Payload), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

// eviction is one vm_evictions row; open is done_at IS NULL.
type rsmEviction struct {
	node, reason string
	open         bool
}

func (r *rsmEnv) evictions(t *testing.T, sandboxID string) []rsmEviction {
	t.Helper()
	rows, err := r.store.Pool().Query(t.Context(), `
		SELECT node_id, reason, done_at IS NULL FROM vm_evictions
		WHERE sandbox_id = $1 ORDER BY created_at, id`, sandboxID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []rsmEviction
	for rows.Next() {
		var ev rsmEviction
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

func (r *rsmEnv) events(t *testing.T, sandboxID string) map[string]int {
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

// TestResumeElsewhereWhenDiskNodeFull stops a sandbox on node-a (final
// generation), fills node-a and resumes: the resume lands on node-b from the
// manifest with the file, node_id and disk_node_id move to node-b, the
// sandbox.hydrated event is recorded, the chain continues and node-a's copy
// is deleted with its reservation released.
func TestResumeElsewhereWhenDiskNodeFull(t *testing.T) {
	r := newRsmEnv(t, "node-a", "node-b")
	a, b := r.nodes["node-a"], r.nodes["node-b"]
	sbx := r.bootWithNote(t, "node-a", "written before the stop")
	stopped := r.stop(t, sbx.ID)
	last, ok, err := r.store.LatestGeneration(t.Context(), r.own, sbx.ID)
	if err != nil || !ok {
		t.Fatalf("final generation: %v, %v", ok, err)
	}
	r.fill(t, "node-a")
	diskA := r.nodeReserved(t, "node-a").DiskGiB

	acc := r.resume(t, sbx.ID)
	p := r.payload(t, acc.Job)
	if p.NodeID != "node-b" || p.FromManifest != last.ManifestKey || p.EvictNode != "node-a" {
		t.Fatalf("resume payload = %+v, want node-b from %s evicting node-a", p, last.ManifestKey)
	}
	if acc.Job.SourceGenerationID == nil || *acc.Job.SourceGenerationID != last.ID {
		t.Fatalf("job source generation = %v, want %s", acc.Job.SourceGenerationID, last.ID)
	}
	if acc.Sandbox.DiskNodeID != nil || derefString(acc.Sandbox.NodeID) != "node-b" {
		t.Fatalf("admitted row node=%v disk=%v", acc.Sandbox.NodeID, acc.Sandbox.DiskNodeID)
	}
	if got := r.nodeReserved(t, "node-a").DiskGiB; got != diskA-stopped.Reserved.DiskGiB {
		t.Fatalf("node-a disk reserved = %d, want %d", got, diskA-stopped.Reserved.DiskGiB)
	}
	if !mustProcess(t, r.w) {
		t.Fatal("resume job not claimed")
	}

	got := r.sandbox(t, r.own, sbx.ID)
	switch {
	case got.State != db.StateReady:
		t.Fatalf("state = %s (%s)", got.State, got.ErrorReason)
	case derefString(got.NodeID) != "node-b", derefString(got.DiskNodeID) != "node-b":
		t.Fatalf("node=%v disk=%v, want node-b", got.NodeID, got.DiskNodeID)
	case got.HydratedAt == nil:
		t.Fatal("hydrated_at not set")
	case got.ChainID != sbx.ChainID:
		t.Fatalf("chain = %s, want %s", got.ChainID, sbx.ChainID)
	}
	if n := r.events(t, sbx.ID)[db.EventSandboxHydrated]; n != 1 {
		t.Fatalf("sandbox.hydrated events = %d, want 1", n)
	}
	if note, err := os.ReadFile(rsmNotePath(b, sbx.ID)); err != nil || string(note) != "written before the stop" {
		t.Fatalf("note on node-b = %q, %v", note, err)
	}
	if a.hasVM(t, sbx.ID) {
		t.Fatal("node-a still holds the old copy")
	}
	if ev := r.evictions(t, sbx.ID); len(ev) != 1 || ev[0].node != "node-a" || ev[0].open ||
		ev[0].reason != db.EvictionReasonResumedElsewhere {
		t.Fatalf("evictions = %+v, want one closed node-a row", ev)
	}
	if res := r.nodeReserved(t, "node-b"); res != got.Desired {
		t.Fatalf("node-b reserved = %+v, want %+v", res, got.Desired)
	}
	if creates, starts, _ := b.counts(); creates != 1 || starts != 0 {
		t.Fatalf("node-b manifest creates=%d starts=%d, want 1/0", creates, starts)
	}

	// The chain continues on node-b.
	genMakeDue(t, r.lifecycleEnv, got)
	if genRound(t, r.w) != 1 {
		t.Fatal("no snapshot job after the resume")
	}
	next, ok, err := r.store.LatestGeneration(t.Context(), r.own, sbx.ID)
	switch {
	case err != nil || !ok:
		t.Fatalf("next generation: %v, %v", ok, err)
	case next.ChainID != last.ChainID || next.Number != last.Number+1 || next.NodeID != "node-b":
		t.Fatalf("next generation chain=%s number=%d node=%s, want chain %s number %d node-b",
			next.ChainID, next.Number, next.NodeID, last.ChainID, last.Number+1)
	}
}

// TestResumeElsewhereWhenDiskNodeDisconnected resumes while node-a is away:
// the sandbox lands on node-b and node-a's copy stays as an open eviction.
// When node-a reconnects, reconciliation deletes the copy first and closes
// the row; the VM is never reported unknown nor snapshotted.
func TestResumeElsewhereWhenDiskNodeDisconnected(t *testing.T) {
	r := newRsmEnv(t, "node-a", "node-b")
	a := r.nodes["node-a"]
	sbx := r.bootWithNote(t, "node-a", "kept through the move")
	r.stop(t, sbx.ID)
	_, _, snapsBefore := a.counts()

	a.disconnect(t)
	acc := r.resume(t, sbx.ID)
	if p := r.payload(t, acc.Job); p.NodeID != "node-b" || p.EvictNode != "node-a" {
		t.Fatalf("resume payload = %+v", p)
	}
	if !mustProcess(t, r.w) {
		t.Fatal("resume job not claimed")
	}
	got := r.sandbox(t, r.own, sbx.ID)
	if got.State != db.StateReady || derefString(got.DiskNodeID) != "node-b" {
		t.Fatalf("state=%s disk=%v", got.State, got.DiskNodeID)
	}
	if note, err := os.ReadFile(rsmNotePath(r.nodes["node-b"], sbx.ID)); err != nil || string(note) != "kept through the move" {
		t.Fatalf("note on node-b = %q, %v", note, err)
	}
	if ev := r.evictions(t, sbx.ID); len(ev) != 1 || ev[0].node != "node-a" || !ev[0].open {
		t.Fatalf("evictions = %+v, want one open node-a row", ev)
	}
	if !a.hasVM(t, sbx.ID) {
		t.Fatal("node-a lost its copy while away (test premise)")
	}

	a.connect(t)
	r.w.ReapOnce(t.Context())
	if a.hasVM(t, sbx.ID) {
		t.Fatal("reconciliation kept node-a's evicted copy")
	}
	if ev := r.evictions(t, sbx.ID); len(ev) != 1 || ev[0].open {
		t.Fatalf("evictions after reconcile = %+v, want the row done", ev)
	}
	if n := r.logs.count("unknown VM preserved"); n != 0 {
		t.Fatalf("evicted copy reported unknown %d times", n)
	}
	genMakeDue(t, r.lifecycleEnv, got)
	genRound(t, r.w)
	if _, _, snaps := a.counts(); snaps != snapsBefore {
		t.Fatalf("node-a SnapshotVM calls = %d after the move, want %d", snaps, snapsBefore)
	}
	if after := r.sandbox(t, r.own, sbx.ID); after.State != db.StateReady || derefString(after.NodeID) != "node-b" {
		t.Fatalf("sandbox after reconcile: %s on %v", after.State, after.NodeID)
	}
}

// TestResumeSameNodeStillStartsTheKeptDisk keeps the Phase 1 path when the
// disk node has room: StartVM there, no manifest CreateVM anywhere.
func TestResumeSameNodeStillStartsTheKeptDisk(t *testing.T) {
	r := newRsmEnv(t, "node-a", "node-b")
	a, b := r.nodes["node-a"], r.nodes["node-b"]
	sbx := r.bootWithNote(t, "node-a", "local")
	r.stop(t, sbx.ID)

	acc := r.resume(t, sbx.ID)
	if p := r.payload(t, acc.Job); p.FromManifest != "" || p.NodeID != "node-a" {
		t.Fatalf("resume payload = %+v, want same node", p)
	}
	if !mustProcess(t, r.w) {
		t.Fatal("resume job not claimed")
	}
	if got := r.sandbox(t, r.own, sbx.ID); got.State != db.StateReady || derefString(got.DiskNodeID) != "node-a" {
		t.Fatalf("state=%s disk=%v", got.State, got.DiskNodeID)
	}
	aCreates, aStarts, _ := a.counts()
	bCreates, bStarts, _ := b.counts()
	if aStarts != 1 || aCreates != 0 || bCreates != 0 || bStarts != 0 {
		t.Fatalf("calls: a creates=%d starts=%d, b creates=%d starts=%d; want only one StartVM on a",
			aCreates, aStarts, bCreates, bStarts)
	}
	if ev := r.evictions(t, sbx.ID); len(ev) != 0 {
		t.Fatalf("evictions = %+v, want none", ev)
	}
}

// rsmLeaseStore expires the claim right before the first resume
// finalization, as a lease lost after CreateVM returned would.
type rsmLeaseStore struct {
	*db.Store
	once sync.Once
}

func (s *rsmLeaseStore) FinalizeSandboxResume(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p db.FinalizeResumeParams) (db.Sandbox, error) {
	var err error
	s.once.Do(func() {
		_, err = s.Pool().Exec(ctx,
			`UPDATE jobs SET leased_until = now() - interval '1 second' WHERE id = $1`, jobID)
	})
	if err != nil {
		return db.Sandbox{}, err
	}
	return s.Store.FinalizeSandboxResume(ctx, jobID, worker, token, p)
}

// TestResumeElsewhereRetryReusesTheVM loses the claim after CreateVM: the
// redelivered attempt calls CreateVM again and gets the same VM back, so
// node-b ends with exactly one VM and the sandbox is ready.
func TestResumeElsewhereRetryReusesTheVM(t *testing.T) {
	r := newRsmEnv(t, "node-a", "node-b")
	b := r.nodes["node-b"]
	store := &rsmLeaseStore{Store: r.store}
	w := genWorker(t, r.lifecycleEnv, store, r.mem, lifecycle.Options{Placer: r.sched})
	sbx := r.bootWithNote(t, "node-a", "once")
	r.stop(t, sbx.ID)
	r.fill(t, "node-a")
	r.resume(t, sbx.ID)

	if !mustProcess(t, w) {
		t.Fatal("resume job not claimed")
	}
	if got := r.sandbox(t, r.own, sbx.ID); got.State != db.StateProvisioning {
		t.Fatalf("state after the lost claim = %s, want provisioning", got.State)
	}
	first, err := b.Node.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: sbx.ID})
	if err != nil {
		t.Fatalf("VM on node-b after the first attempt: %v", err)
	}
	if !mustProcess(t, w) {
		t.Fatal("redelivered resume job not claimed")
	}
	got := r.sandbox(t, r.own, sbx.ID)
	if got.State != db.StateReady || derefString(got.NodeID) != "node-b" {
		t.Fatalf("state=%s node=%v", got.State, got.NodeID)
	}
	second, err := b.Node.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: sbx.ID})
	if err != nil {
		t.Fatal(err)
	}
	if first.GetCreatedAtUnixMs() != second.GetCreatedAtUnixMs() {
		t.Fatal("the redelivery booted a second VM on node-b")
	}
	if creates, _, _ := b.counts(); creates != 2 {
		t.Fatalf("node-b manifest CreateVM calls = %d, want 2 (one per attempt)", creates)
	}
	vms, err := b.Node.ListVMs(t.Context(), &nodev1.ListVMsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(vms.GetVms()) != 1 {
		t.Fatalf("node-b VMs = %d, want 1", len(vms.GetVms()))
	}
}

// TestResumeElsewhereMovesOffNodeWithoutObjectStore: node-b answers the
// manifest restore with FAILED_PRECONDITION. It is marked without an object
// store and the resume moves to node-c, where it becomes ready with the file.
func TestResumeElsewhereMovesOffNodeWithoutObjectStore(t *testing.T) {
	r := newRsmEnv(t, "node-a", "node-b", "node-c")
	b, c := r.nodes["node-b"], r.nodes["node-c"]
	sbx := r.bootWithNote(t, "node-a", "moved twice")
	r.stop(t, sbx.ID)
	r.fill(t, "node-a")
	b.setCreateHook(refuseManifests)
	// node-c has less free memory, so placement ranks node-b first.
	if _, err := r.store.Pool().Exec(t.Context(),
		`UPDATE nodes SET reserved_memory_mib = 4096 WHERE id = 'node-c'`); err != nil {
		t.Fatal(err)
	}

	acc := r.resume(t, sbx.ID)
	if p := r.payload(t, acc.Job); p.NodeID != "node-b" {
		t.Fatalf("first placement = %s, want node-b", p.NodeID)
	}
	if !mustProcess(t, r.w) {
		t.Fatal("resume job not claimed")
	}
	if nb, err := r.store.Node(t.Context(), "node-b"); err != nil || nb.ObjectStoreOK {
		t.Fatal("node-b not marked without an object store")
	}
	moved := r.sandbox(t, r.own, sbx.ID)
	if derefString(moved.NodeID) != "node-c" || moved.State != db.StateProvisioning {
		t.Fatalf("after the refusal: %s on %v", moved.State, moved.NodeID)
	}
	if r.nodeReserved(t, "node-b") != (db.Resources{}) {
		t.Fatalf("node-b still reserved: %+v", r.nodeReserved(t, "node-b"))
	}
	if !mustProcess(t, r.w) {
		t.Fatal("moved resume job not claimed")
	}
	got := r.sandbox(t, r.own, sbx.ID)
	if got.State != db.StateReady || derefString(got.DiskNodeID) != "node-c" {
		t.Fatalf("state=%s disk=%v", got.State, got.DiskNodeID)
	}
	if note, err := os.ReadFile(rsmNotePath(c, sbx.ID)); err != nil || string(note) != "moved twice" {
		t.Fatalf("note on node-c = %q, %v", note, err)
	}
	if b.hasVM(t, sbx.ID) {
		t.Fatal("node-b kept a VM")
	}
	if n := r.events(t, sbx.ID)[db.EventSandboxRescheduled]; n != 1 {
		t.Fatalf("rescheduled events = %d, want 1", n)
	}
	if r.count(t, `SELECT count(*) FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.resume'
		AND source_generation_id IS NOT NULL`, sbx.ID) != 2 {
		t.Fatal("the moved resume job lost its source generation")
	}
}

// TestResumeElsewhereNoOtherNodeEndsWith503: the only other node refuses the
// manifest; the resume ends archived (no_ready_machine, no local disk,
// nothing reserved) and the next resume answers a clear no-node refusal.
func TestResumeElsewhereNoOtherNodeEndsWith503(t *testing.T) {
	r := newRsmEnv(t, "node-a", "node-b")
	r.nodes["node-b"].setCreateHook(refuseManifests)
	sbx := r.bootWithNote(t, "node-a", "nowhere")
	r.stop(t, sbx.ID)
	r.fill(t, "node-a")
	r.resume(t, sbx.ID)
	mustProcess(t, r.w)

	got := r.sandbox(t, r.own, sbx.ID)
	switch {
	case got.State != db.StateArchived || got.ErrorReason != "no_ready_machine":
		t.Fatalf("state=%s reason=%q", got.State, got.ErrorReason)
	case got.DiskNodeID != nil:
		t.Fatalf("disk node = %v, want none", *got.DiskNodeID)
	case got.Reserved != (db.Resources{}):
		t.Fatalf("reserved = %+v", got.Reserved)
	}
	_, err := r.sched.Resume(t.Context(), scheduler.ResumeRequest{Owner: r.own, SandboxID: sbx.ID})
	if !errors.Is(err, scheduler.ErrNoNode) && !errors.Is(err, scheduler.ErrNoCapacity) {
		t.Fatalf("second resume = %v, want a no-node/no-capacity refusal (503)", err)
	}
}

// TestResumeElsewhereTerminalFailureStaysResumable: node-b answers NOT_FOUND
// once (the manifest is unreadable); the resume ends archived with
// snapshot_not_found and no local disk, node-a's copy is still an open
// eviction that reconciliation deletes, and a second resume restores the same
// generation and becomes ready with the file.
func TestResumeElsewhereTerminalFailureStaysResumable(t *testing.T) {
	r := newRsmEnv(t, "node-a", "node-b")
	a, b := r.nodes["node-a"], r.nodes["node-b"]
	sbx := r.bootWithNote(t, "node-a", "second try")
	r.stop(t, sbx.ID)
	r.fill(t, "node-a")
	var refused sync.Once
	b.setCreateHook(func(req *nodev1.CreateVMRequest) error {
		var err error
		refused.Do(func() { err = status.Error(codes.NotFound, "manifest missing") })
		return err
	})
	r.resume(t, sbx.ID)
	mustProcess(t, r.w)

	failed := r.sandbox(t, r.own, sbx.ID)
	switch {
	case failed.State != db.StateArchived || failed.ErrorReason != "snapshot_not_found":
		t.Fatalf("state=%s reason=%q", failed.State, failed.ErrorReason)
	case failed.DiskNodeID != nil || failed.Reserved != (db.Resources{}):
		t.Fatalf("disk=%v reserved=%+v, want none", failed.DiskNodeID, failed.Reserved)
	}
	if r.nodeReserved(t, "node-b") != (db.Resources{}) {
		t.Fatalf("node-b still reserved: %+v", r.nodeReserved(t, "node-b"))
	}
	r.w.ReapOnce(t.Context())
	if a.hasVM(t, sbx.ID) {
		t.Fatal("node-a's evicted copy survived reconciliation")
	}

	acc := r.resume(t, sbx.ID)
	if p := r.payload(t, acc.Job); p.FromManifest == "" || p.EvictNode != "" {
		t.Fatalf("second resume payload = %+v, want a manifest restore with nothing to evict", p)
	}
	if !mustProcess(t, r.w) {
		t.Fatal("second resume job not claimed")
	}
	got := r.sandbox(t, r.own, sbx.ID)
	if got.State != db.StateReady || got.ErrorReason != "" {
		t.Fatalf("state=%s reason=%q", got.State, got.ErrorReason)
	}
	node := r.nodes[derefString(got.NodeID)]
	if note, err := os.ReadFile(rsmNotePath(node, sbx.ID)); err != nil || string(note) != "second try" {
		t.Fatalf("note after the second resume = %q, %v", note, err)
	}
}

// TestResumeRefusalsWithoutRestorableData covers the admission refusals: no
// generation and no local disk (resume_failed), no generation with the disk
// node away (the same-node 503), and expired data.
func TestResumeRefusalsWithoutRestorableData(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, r *rsmEnv, sandboxID string)
		want    error
	}{
		{
			name: "no generation and no disk",
			prepare: func(t *testing.T, r *rsmEnv, id string) {
				rsmExec(t, r, `UPDATE sandboxes SET disk_node_id = NULL WHERE id = $1`, id)
			},
			want: db.ErrNoRestorableData,
		},
		{
			name: "no generation, disk node away",
			prepare: func(t *testing.T, r *rsmEnv, id string) {
				r.nodes["node-a"].disconnect(t)
			},
			want: scheduler.ErrNoNode,
		},
		{
			name: "data expired",
			prepare: func(t *testing.T, r *rsmEnv, id string) {
				rsmExec(t, r, `UPDATE sandboxes SET data_expired_at = now(), disk_node_id = NULL WHERE id = $1`, id)
			},
			want: db.ErrSnapshotDataExpired,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRsmEnv(t, "node-a", "node-b")
			sbx := r.bootWithNote(t, "node-a", "none")
			// A Phase 1 worker stops without a final generation.
			p1 := r.worker(t, lifecycle.Options{})
			r.acceptStop(t, r.own, sbx.ID, false)
			if !mustProcess(t, p1) {
				t.Fatal("stop job not claimed")
			}
			if _, ok, _ := r.store.LatestGeneration(t.Context(), r.own, sbx.ID); ok {
				t.Fatal("test premise: a generation exists")
			}
			tc.prepare(t, r, sbx.ID)
			_, err := r.sched.Resume(t.Context(), scheduler.ResumeRequest{Owner: r.own, SandboxID: sbx.ID})
			if !errors.Is(err, tc.want) {
				t.Fatalf("resume = %v, want %v", err, tc.want)
			}
			if got := r.sandbox(t, r.own, sbx.ID); got.State != db.StateArchived {
				t.Fatalf("state after the refusal = %s", got.State)
			}
		})
	}
}

func rsmExec(t *testing.T, r *rsmEnv, sql string, args ...any) {
	t.Helper()
	if _, err := r.store.Pool().Exec(t.Context(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

// TestStopDuringCrossNodeBootKeepsDiskOnNewNode: a stop accepted while a
// cross-node resume's VM already exists on node-b stops it there; the
// archived row records node-b as the disk node, and the next resume starts
// the kept disk on node-b with StartVM instead of getting the stopped VM back
// from an idempotent CreateVM.
func TestStopDuringCrossNodeBootKeepsDiskOnNewNode(t *testing.T) {
	r := newRsmEnv(t, "node-a", "node-b")
	b := r.nodes["node-b"]
	sbx := r.bootWithNote(t, "node-a", "stopped mid-move")
	r.stop(t, sbx.ID)
	r.fill(t, "node-a")
	acc := r.resume(t, sbx.ID)
	p := r.payload(t, acc.Job)
	// The boot completed on node-b, but its job has not finalized yet.
	if _, err := b.Node.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: sbx.ID, Resources: &nodev1.Resources{
			Vcpus: p.Resources.VCPUs, MemoryMib: p.Resources.MemoryMiB, DiskGib: p.Resources.DiskGiB,
		},
		FromManifest: p.FromManifest,
	}); err != nil {
		t.Fatalf("restore on node-b: %v", err)
	}
	stopped := r.stop(t, sbx.ID) // cancels the queued resume job
	if derefString(stopped.DiskNodeID) != "node-b" || derefString(stopped.NodeID) != "node-b" {
		t.Fatalf("after the stop node=%v disk=%v, want node-b", stopped.NodeID, stopped.DiskNodeID)
	}
	_, startsBefore, _ := b.counts()

	acc = r.resume(t, sbx.ID)
	if p := r.payload(t, acc.Job); p.FromManifest != "" || p.NodeID != "node-b" {
		t.Fatalf("resume payload = %+v, want a same-node start on node-b", p)
	}
	if !mustProcess(t, r.w) {
		t.Fatal("resume job not claimed")
	}
	if got := r.sandbox(t, r.own, sbx.ID); got.State != db.StateReady {
		t.Fatalf("state = %s (%s)", got.State, got.ErrorReason)
	}
	if _, starts, _ := b.counts(); starts != startsBefore+1 {
		t.Fatalf("node-b StartVM calls = %d, want %d", starts, startsBefore+1)
	}
}

// derefString reads an optional string.
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
