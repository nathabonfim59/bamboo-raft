package lifecycle_test

// Snapshot generation integration tests: PostgreSQL plus nodefake through
// nodelink and the internal gateway, every fake sharing one memstore with the
// worker. They skip unless BBRAFT_TEST_DATABASE_URL is set (dbtest.Open).

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// genNode wraps nodefake to record and script SnapshotVM and to observe the
// order of StopVM and SnapshotVM, without editing the fake.
type genNode struct {
	*nodefake.Node
	root string

	mu          sync.Mutex
	snapHook    func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error)
	snapCalls   []*nodev1.SnapshotVMRequest
	order       []string
	inFlight    int
	maxInFlight int
}

func (n *genNode) setSnapHook(h func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapHook = h
}

func (n *genNode) SnapshotVM(ctx context.Context, req *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
	n.mu.Lock()
	n.snapCalls = append(n.snapCalls, req)
	n.order = append(n.order, "snapshot")
	n.inFlight++
	n.maxInFlight = max(n.maxInFlight, n.inFlight)
	hook := n.snapHook
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		n.inFlight--
		n.mu.Unlock()
	}()
	if hook != nil {
		return hook(ctx, req)
	}
	return n.Node.SnapshotVM(ctx, req)
}

func (n *genNode) StopVM(ctx context.Context, req *nodev1.StopVMRequest) (*nodev1.VM, error) {
	n.mu.Lock()
	n.order = append(n.order, "stop")
	n.mu.Unlock()
	return n.Node.StopVM(ctx, req)
}

func (n *genNode) snapshotCalls() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.snapCalls)
}

func (n *genNode) callOrder() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.order...)
}

// homeFile is the path of a file under /home/user of the fake VM's disk.
func (n *genNode) homeFile(sandboxID, name string) string {
	return filepath.Join(n.root, "vms", sandboxID, "home", "user", name)
}

// genStartNode connects a genNode to the env's hub, its disk store set to
// objects (nil: a node without an object store).
func genStartNode(t *testing.T, e *lifecycleEnv, id string, objects objstore.Store) *genNode {
	t.Helper()
	root := t.TempDir()
	fake := nodefake.New(id, root)
	fake.VCPUs = 64
	fake.MemoryMiB = 256 << 10
	fake.Store = objects
	n := &genNode{Node: fake, root: root}
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
	return n
}

// genSetup is one env with a registered node "node-a" on the shared store.
func genSetup(t *testing.T, nodeObjects bool) (*lifecycleEnv, *genNode, *memstore.Store) {
	t.Helper()
	e := newLifecycleEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 20})
	mem := memstore.New()
	var objects objstore.Store
	if nodeObjects {
		objects = mem
	}
	return e, genStartNode(t, e, "node-a", objects), mem
}

// genWorker is a Phase 2 worker on mem with a 1 s interval and no jitter
// (rand 0.5) unless opts says otherwise.
func genWorker(t *testing.T, e *lifecycleEnv, store lifecycle.Store, mem objstore.Store, opts lifecycle.Options) *lifecycle.Worker {
	t.Helper()
	opts.ObjectStore = mem
	if opts.SnapshotInterval == 0 {
		opts.SnapshotInterval = time.Second
	}
	if opts.SnapshotRand == nil {
		opts.SnapshotRand = func() float64 { return 0.5 }
	}
	return e.workerWithStore(t, store, opts)
}

// genCreate creates and boots one sandbox on node-a.
func genCreate(t *testing.T, e *lifecycleEnv, w *lifecycle.Worker, own uuid.UUID, enabled *bool) db.Sandbox {
	t.Helper()
	res, err := e.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, MachineType: "small", Name: "gen", NodeID: "node-a",
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}, MaxAttempts: 3,
		SnapshotsEnabled: enabled,
	})
	if err != nil {
		t.Fatalf("create sandbox: %v", err)
	}
	if !mustProcess(t, w) {
		t.Fatal("provision job not claimed")
	}
	sbx := e.sandbox(t, own, res.Sandbox.ID)
	if sbx.State != db.StateReady {
		t.Fatalf("sandbox after boot: %s", sbx.State)
	}
	return sbx
}

// genRound enqueues due snapshots and runs one job; it reports how many
// jobs were enqueued.
func genRound(t *testing.T, w *lifecycle.Worker) int {
	t.Helper()
	n, err := w.EnqueueDueSnapshots(t.Context())
	if err != nil {
		t.Fatalf("enqueue due snapshots: %v", err)
	}
	if n > 0 && !mustProcess(t, w) {
		t.Fatal("snapshot job not claimed")
	}
	return n
}

// genMakeDue puts the sandbox's next generation in the past.
func genMakeDue(t *testing.T, e *lifecycleEnv, sbx db.Sandbox) {
	t.Helper()
	if err := e.store.SetSnapshotSchedule(t.Context(), sbx.OwnerID, sbx.ID, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
}

// genRow is one snapshots row as the tests read it.
type genRow struct {
	number    uint32
	status    db.GenerationStatus
	reason    string
	chain     uuid.UUID
	key       string
	started   time.Time
	completed *time.Time
	deadline  time.Time
}

func genRows(t *testing.T, e *lifecycleEnv, sandboxID string) []genRow {
	t.Helper()
	rows, err := e.store.Pool().Query(t.Context(), `
		SELECT generation, status, error, chain_id, manifest_key, started_at, completed_at, deadline_at
		FROM snapshots WHERE sandbox_id = $1 ORDER BY generation`, sandboxID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []genRow
	for rows.Next() {
		var r genRow
		var n int64
		if err := rows.Scan(&n, &r.status, &r.reason, &r.chain, &r.key, &r.started, &r.completed, &r.deadline); err != nil {
			t.Fatal(err)
		}
		r.number = uint32(n)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func genSummary(rows []genRow) string {
	var parts []string
	for _, r := range rows {
		parts = append(parts, strings.TrimSpace(string(r.status)+" "+r.reason))
	}
	return strings.Join(parts, ", ")
}

// genLogBuffer collects log lines from concurrent writers.
type genLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *genLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *genLogBuffer) count(s string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Count(b.buf.String(), s)
}

// TestSnapshotWorkerGenerationPerInterval runs the full worker: a ready
// sandbox gets generations 0, 1, 2 on its chain, one per interval, each
// manifest in the shared store, and the next time carries the jitter.
func TestSnapshotWorkerGenerationPerInterval(t *testing.T) {
	e, _, mem := genSetup(t, true)
	own := e.owner(t)
	w := genWorker(t, e, e.store, mem, lifecycle.Options{
		SnapshotRand: func() float64 { return 0 }, // the low edge: interval - 10 %
	})
	res, err := e.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, MachineType: "small", Name: "gen", NodeID: "node-a",
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}, MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitFor(t, "three completed generations", func() bool {
		completed := 0
		for _, r := range genRows(t, e, res.Sandbox.ID) {
			if r.status == db.GenerationCompleted {
				completed++
			}
		}
		return completed >= 3
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}

	sbx := e.sandbox(t, own, res.Sandbox.ID)
	rows := genRows(t, e, res.Sandbox.ID)
	for i, r := range rows[:3] {
		if r.number != uint32(i) || r.status != db.GenerationCompleted || r.chain != sbx.ChainID {
			t.Fatalf("row %d = %+v, want completed generation %d on chain %s (%s)", i, r, i, sbx.ChainID, genSummary(rows))
		}
		if r.key != snapformat.ManifestKey(sbx.ChainID.String(), uint32(i)) {
			t.Errorf("row %d key %q", i, r.key)
		}
		if _, err := mem.Head(t.Context(), r.key); err != nil {
			t.Errorf("manifest %s not in the store: %v", r.key, err)
		}
		if i > 0 {
			// The next attempt starts no sooner than interval - 10 %
			// after the previous one completed (small clock slack).
			if gap := r.started.Sub(*rows[i-1].completed); gap < 800*time.Millisecond {
				t.Errorf("generation %d started %v after the previous one", i, gap)
			}
		}
	}
	// The schedule comes from the last finished row (a row the shutdown
	// interrupted stays pending and sets nothing).
	var last *genRow
	for i := range rows {
		if rows[i].status != db.GenerationPending {
			last = &rows[i]
		}
	}
	if last == nil || last.status != db.GenerationCompleted || sbx.NextSnapshotAt == nil {
		t.Fatalf("last finished row %+v, next %v", last, sbx.NextSnapshotAt)
	}
	if d := sbx.NextSnapshotAt.Sub(*last.completed); d < 700*time.Millisecond || d > 1100*time.Millisecond {
		t.Errorf("next snapshot %v after the last completion, want about 900ms (interval - 10 %%)", d)
	}
}

// TestSnapshotAlreadyExistsBumpsGeneration pre-puts generation 0's manifest:
// the node answers ALREADY_EXISTS, the row fails and generation 1 completes
// in the same job.
func TestSnapshotAlreadyExistsBumpsGeneration(t *testing.T) {
	e, n, mem := genSetup(t, true)
	own := e.owner(t)
	w := genWorker(t, e, e.store, mem, lifecycle.Options{})
	sbx := genCreate(t, e, w, own, nil)
	key := snapformat.ManifestKey(sbx.ChainID.String(), 0)
	if err := mem.PutIfAbsent(t.Context(), key, strings.NewReader("taken"), 5); err != nil {
		t.Fatal(err)
	}
	if genRound(t, w) != 1 {
		t.Fatal("no snapshot job enqueued for a due sandbox")
	}
	rows := genRows(t, e, sbx.ID)
	if len(rows) != 2 || rows[0].status != db.GenerationFailed || rows[0].reason != "exists" ||
		rows[1].number != 1 || rows[1].status != db.GenerationCompleted {
		t.Fatalf("rows = %s, want failed exists then completed 1", genSummary(rows))
	}
	if n.snapshotCalls() != 2 {
		t.Fatalf("SnapshotVM calls = %d, want 2", n.snapshotCalls())
	}
}

// TestSnapshotRefusedBacksOffAndWarnsOnce uses a node without an object
// store: every attempt fails FAILED_PRECONDITION, the sandbox stays ready,
// backs off ten intervals and the refusal is logged once.
func TestSnapshotRefusedBacksOffAndWarnsOnce(t *testing.T) {
	e, _, mem := genSetup(t, false)
	own := e.owner(t)
	logs := &genLogBuffer{}
	w := genWorker(t, e, e.store, mem, lifecycle.Options{Logger: slog.New(slog.NewTextHandler(logs, nil))})
	sbx := genCreate(t, e, w, own, nil)
	for round := range 3 {
		if round > 0 {
			genMakeDue(t, e, sbx)
		}
		before := time.Now()
		if genRound(t, w) != 1 {
			t.Fatalf("round %d: no snapshot job", round)
		}
		cur := e.sandbox(t, own, sbx.ID)
		if cur.State != db.StateReady {
			t.Fatalf("round %d: sandbox %s, want ready", round, cur.State)
		}
		if cur.NextSnapshotAt == nil || cur.NextSnapshotAt.Sub(before) < 9*time.Second {
			t.Fatalf("round %d: next snapshot %v, want about ten intervals ahead", round, cur.NextSnapshotAt)
		}
		if n, _ := w.EnqueueDueSnapshots(t.Context()); n != 0 {
			t.Fatalf("round %d: a backed-off sandbox was enqueued again", round)
		}
	}
	rows := genRows(t, e, sbx.ID)
	if len(rows) != 3 {
		t.Fatalf("rows = %s, want three failed attempts", genSummary(rows))
	}
	for _, r := range rows {
		if r.status != db.GenerationFailed || r.reason != "precondition" {
			t.Fatalf("rows = %s, want failed precondition", genSummary(rows))
		}
	}
	if got := logs.count("node cannot snapshot sandbox"); got != 1 {
		t.Fatalf("refusal warnings = %d, want 1", got)
	}
}

// TestSnapshotFailureThenSuccess fails one SnapshotVM: the row is failed
// with a safe code, the sandbox keeps its normal schedule, and the next
// attempt completes the next number.
func TestSnapshotFailureThenSuccess(t *testing.T) {
	e, n, mem := genSetup(t, true)
	own := e.owner(t)
	w := genWorker(t, e, e.store, mem, lifecycle.Options{})
	sbx := genCreate(t, e, w, own, nil)
	n.setSnapHook(func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
		return nil, status.Error(codes.Unavailable, "object store: put chunk: secret detail")
	})
	before := time.Now()
	genRound(t, w)
	cur := e.sandbox(t, own, sbx.ID)
	if cur.NextSnapshotAt == nil || cur.NextSnapshotAt.Sub(before) > 3*time.Second {
		t.Fatalf("next snapshot after a failure = %v, want the normal interval", cur.NextSnapshotAt)
	}
	n.setSnapHook(nil)
	genMakeDue(t, e, sbx)
	genRound(t, w)
	rows := genRows(t, e, sbx.ID)
	if len(rows) != 2 || rows[0].status != db.GenerationFailed || rows[0].reason != "node_unavailable" ||
		rows[1].number != 1 || rows[1].status != db.GenerationCompleted {
		t.Fatalf("rows = %s, want failed node_unavailable then completed 1", genSummary(rows))
	}
}

// TestStopFinalGenerationHoldsTheLastWrite writes a file just before the
// stop: the stop's final generation, cut after StopVM, holds it, and a second
// fake on the same store restores it from the manifest.
func TestStopFinalGenerationHoldsTheLastWrite(t *testing.T) {
	e, n, mem := genSetup(t, true)
	own := e.owner(t)
	w := genWorker(t, e, e.store, mem, lifecycle.Options{})
	sbx := genCreate(t, e, w, own, nil)
	genRound(t, w) // generation 0, before the write

	path := n.homeFile(sbx.ID, "note.txt")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("written just before stop"), 0o644); err != nil {
		t.Fatal(err)
	}
	e.acceptStop(t, own, sbx.ID, false)
	if !mustProcess(t, w) {
		t.Fatal("stop job not claimed")
	}
	if got := e.sandbox(t, own, sbx.ID); got.State != db.StateArchived {
		t.Fatalf("sandbox after stop: %s", got.State)
	}
	if order := n.callOrder(); strings.Join(order, ",") != "snapshot,stop,snapshot" {
		t.Fatalf("node calls = %v, want the final snapshot after StopVM", order)
	}
	latest, ok, err := e.store.LatestGeneration(t.Context(), own, sbx.ID)
	if err != nil || !ok || latest.Number != 1 {
		t.Fatalf("latest generation = %+v, %v, %v; want the final generation 1", latest, ok, err)
	}

	otherRoot := t.TempDir()
	other := nodefake.New("node-b", otherRoot)
	other.Store = mem
	restored := ids.Sandbox()
	if _, err := other.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: restored, FromManifest: latest.ManifestKey,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 1},
	}); err != nil {
		t.Fatalf("restore on a second fake: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(otherRoot, "vms", restored, "home", "user", "note.txt"))
	if err != nil || string(got) != "written just before stop" {
		t.Fatalf("restored file = %q, %v", got, err)
	}
}

// TestStopFailingFinalSnapshotThenForce makes every SnapshotVM fail: the
// stop goes failing and retries; a forced stop then completes and the
// sandbox keeps its previous generation.
func TestStopFailingFinalSnapshotThenForce(t *testing.T) {
	e, n, mem := genSetup(t, true)
	own := e.owner(t)
	w := genWorker(t, e, e.store, mem, lifecycle.Options{})
	sbx := genCreate(t, e, w, own, nil)
	genRound(t, w) // generation 0 succeeds

	n.setSnapHook(func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
		return nil, status.Error(codes.Unavailable, "store down")
	})
	op := e.acceptStop(t, own, sbx.ID, false)
	if !mustProcess(t, w) {
		t.Fatal("stop job not claimed")
	}
	failing, err := e.store.StopOperation(t.Context(), own, sbx.ID, op.ID)
	if err != nil || failing.Status != db.StopFailing {
		t.Fatalf("stop after a failed final snapshot = %+v (%v), want failing", failing, err)
	}
	if got := e.sandbox(t, own, sbx.ID); got.State != db.StateArchiving {
		t.Fatalf("sandbox = %s, want archiving while the stop retries", got.State)
	}

	if escalated, _, _, _, err := e.store.AcceptSandboxStop(t.Context(), own, sbx.ID, true); err != nil || !escalated.ForceRequested {
		t.Fatalf("force escalation: %+v (%v)", escalated, err)
	}
	waitFor(t, "forced stop to archive", func() bool {
		if _, err := w.ProcessNext(t.Context()); err != nil {
			t.Fatal(err)
		}
		return e.sandbox(t, own, sbx.ID).State == db.StateArchived
	})
	latest, ok, err := e.store.LatestGeneration(t.Context(), own, sbx.ID)
	if err != nil || !ok || latest.Number != 0 {
		t.Fatalf("latest generation = %+v, %v, %v; want the previous generation 0", latest, ok, err)
	}
	if final, err := e.store.StopOperation(t.Context(), own, sbx.ID, op.ID); err != nil || final.Status != db.StopCompleted {
		t.Fatalf("stop = %+v (%v), want completed", final, err)
	}
}

// TestStopSnapshotsOffErasesTheDisk: a sandbox created with snapshots off is
// never scheduled, uploads nothing, and its stop deletes the VM, releases the
// disk reservation and clears the disk's node.
func TestStopSnapshotsOffErasesTheDisk(t *testing.T) {
	e, n, mem := genSetup(t, true)
	own := e.owner(t)
	w := genWorker(t, e, e.store, mem, lifecycle.Options{})
	sbx := genCreate(t, e, w, own, new(false))
	if sbx.SnapshotsEnabled {
		t.Fatal("fixture has snapshots on")
	}
	if genRound(t, w) != 0 {
		t.Fatal("a sandbox with snapshots off was scheduled")
	}
	e.acceptStop(t, own, sbx.ID, false)
	if !mustProcess(t, w) {
		t.Fatal("stop job not claimed")
	}
	got := e.sandbox(t, own, sbx.ID)
	if got.State != db.StateArchived || got.DiskNodeID != nil || got.Reserved != (db.Resources{}) {
		t.Fatalf("sandbox after an erasing stop: state %s disk node %v reserved %+v", got.State, got.DiskNodeID, got.Reserved)
	}
	if r := e.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("node reservation after an erasing stop: %+v", r)
	}
	if _, err := n.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: sbx.ID}); status.Code(err) != codes.NotFound {
		t.Fatalf("VM after an erasing stop: %v, want NOT_FOUND", err)
	}
	if mem.Len() != 0 || n.snapshotCalls() != 0 {
		t.Fatalf("store holds %d objects, %d SnapshotVM calls; want none", mem.Len(), n.snapshotCalls())
	}
	if c := e.count(t, `SELECT count(*) FROM sandbox_events WHERE sandbox_id = $1
		AND type = 'stop.completed' AND data->>'erased' = 'true'`, sbx.ID); c != 1 {
		t.Fatalf("erased stop.completed events = %d, want 1", c)
	}
}

// TestStopSnapshotsOffWithVMAlreadyGone: a retried erasing stop whose VM an
// earlier attempt already deleted finishes archived, not vm_missing.
func TestStopSnapshotsOffWithVMAlreadyGone(t *testing.T) {
	e, n, mem := genSetup(t, true)
	own := e.owner(t)
	w := genWorker(t, e, e.store, mem, lifecycle.Options{})
	sbx := genCreate(t, e, w, own, new(false))
	if _, err := n.Node.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: sbx.ID}); err != nil {
		t.Fatal(err)
	}
	e.acceptStop(t, own, sbx.ID, false)
	if !mustProcess(t, w) {
		t.Fatal("stop job not claimed")
	}
	if got := e.sandbox(t, own, sbx.ID); got.State != db.StateArchived || got.ErrorReason != "" || got.DiskNodeID != nil {
		t.Fatalf("sandbox = %s %q disk node %v, want archived with the disk erased", got.State, got.ErrorReason, got.DiskNodeID)
	}
}

// TestPhase1ModeStopUnchanged runs without an object store: nothing is
// scheduled, the stop never calls SnapshotVM and keeps the local disk.
func TestPhase1ModeStopUnchanged(t *testing.T) {
	e, n, mem := genSetup(t, true)
	own := e.owner(t)
	w := e.worker(t, lifecycle.Options{}) // no ObjectStore
	sbx := genCreate(t, e, w, own, nil)
	if got, err := w.EnqueueDueSnapshots(t.Context()); got != 0 || err != nil {
		t.Fatalf("EnqueueDueSnapshots = %d, %v", got, err)
	}
	if c := e.count(t, `SELECT count(*) FROM jobs WHERE kind = $1`, string(db.JobSnapshot)); c != 0 {
		t.Fatalf("snapshot jobs = %d, want 0", c)
	}
	e.acceptStop(t, own, sbx.ID, false)
	if !mustProcess(t, w) {
		t.Fatal("stop job not claimed")
	}
	got := e.sandbox(t, own, sbx.ID)
	if got.State != db.StateArchived || got.DiskNodeID == nil || *got.DiskNodeID != "node-a" {
		t.Fatalf("sandbox after a Phase 1 stop: %s disk node %v", got.State, got.DiskNodeID)
	}
	if n.snapshotCalls() != 0 || mem.Len() != 0 {
		t.Fatalf("SnapshotVM calls %d, objects %d; want none", n.snapshotCalls(), mem.Len())
	}
	// A stray snapshot job is a no-op.
	if ok, err := e.store.EnqueueSnapshotJob(t.Context(), own, sbx.ID); err != nil || !ok {
		t.Fatalf("enqueue stray job: %v %v", ok, err)
	}
	mustProcess(t, w)
	if rows := genRows(t, e, sbx.ID); len(rows) != 0 {
		t.Fatalf("rows = %s, want none", genSummary(rows))
	}
}

// genRenewStore fails lease renewal on demand.
type genRenewStore struct {
	*db.Store
	fail atomic.Bool
}

func (s *genRenewStore) RenewJobLease(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, lease time.Duration) error {
	if s.fail.Load() {
		return errors.New("renewal refused by the test")
	}
	return s.Store.RenewJobLease(ctx, jobID, worker, token, lease)
}

// TestSnapshotLeaseLossLeavesPendingUntilDeadline loses the claim while
// SnapshotVM runs: the stale attempt neither completes nor fails the row;
// the reaper fails it once its deadline passed, and the next attempt takes
// the next number.
func TestSnapshotLeaseLossLeavesPendingUntilDeadline(t *testing.T) {
	e, n, mem := genSetup(t, true)
	own := e.owner(t)
	store := &genRenewStore{Store: e.store}
	w := genWorker(t, e, store, mem, lifecycle.Options{
		Lease: 300 * time.Millisecond, SnapshotTimeout: time.Second,
	})
	sbx := genCreate(t, e, w, own, nil)

	var cancelled atomic.Bool
	n.setSnapHook(func(ctx context.Context, _ *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
		store.fail.Store(true)
		<-ctx.Done()
		cancelled.Store(true)
		return nil, status.FromContextError(ctx.Err()).Err()
	})
	genRound(t, w)
	rows := genRows(t, e, sbx.ID)
	if len(rows) != 1 || rows[0].status != db.GenerationPending {
		t.Fatalf("rows after a lost lease = %s, want one pending", genSummary(rows))
	}
	waitFor(t, "the node call to be cancelled", cancelled.Load)

	store.fail.Store(false)
	n.setSnapHook(nil)
	waitFor(t, "the pending row's deadline and grace", func() bool {
		return time.Now().After(rows[0].deadline.Add(1100 * time.Millisecond))
	})
	w.ReapOnce(t.Context())
	rows = genRows(t, e, sbx.ID)
	if len(rows) != 1 || rows[0].status != db.GenerationFailed || rows[0].reason != "deadline" {
		t.Fatalf("rows after reconciliation = %s, want failed deadline", genSummary(rows))
	}
	waitFor(t, "a new snapshot after reconciliation", func() bool {
		if _, err := w.EnqueueDueSnapshots(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, err := w.ProcessNext(t.Context()); err != nil {
			t.Fatal(err)
		}
		rows = genRows(t, e, sbx.ID)
		return len(rows) == 2 && rows[1].status == db.GenerationCompleted
	})
	if rows[1].number != 1 {
		t.Fatalf("number after reconciliation = %d, want 1", rows[1].number)
	}
}

// TestStopTakesOverCrashedSnapshotAttempt is the crashed-attempt scenario: a periodic
// attempt begins a generation and its worker dies (the claim is lost and its
// job lease expires), then a stop is accepted. The dead attempt's pending row
// must not hold the stop's final generation until the row's deadline: the stop
// takes the row over and archives within a few seconds, though the snapshot
// timeout is a minute.
func TestStopTakesOverCrashedSnapshotAttempt(t *testing.T) {
	e, n, mem := genSetup(t, true)
	own := e.owner(t)
	store := &genRenewStore{Store: e.store}
	w := genWorker(t, e, store, mem, lifecycle.Options{
		Lease: 300 * time.Millisecond, SnapshotTimeout: time.Minute,
	})
	sbx := genCreate(t, e, w, own, nil)

	var cancelled atomic.Bool
	n.setSnapHook(func(ctx context.Context, _ *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
		store.fail.Store(true) // the next renewal fails: the worker crashed
		<-ctx.Done()
		cancelled.Store(true)
		return nil, status.FromContextError(ctx.Err()).Err()
	})
	genRound(t, w)
	waitFor(t, "the crashed attempt's node call to end", cancelled.Load)
	rows := genRows(t, e, sbx.ID)
	if len(rows) != 1 || rows[0].status != db.GenerationPending {
		t.Fatalf("rows after the crash = %s, want one pending", genSummary(rows))
	}
	// The dead worker's claim expires and has no attempts left, so nothing
	// redelivers it; its pending row keeps its (minute-long) deadline.
	if _, err := e.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET leased_until = now() - interval '1 minute', attempts = max_attempts
		WHERE sandbox_id = $1 AND kind = 'sandbox.snapshot'`, sbx.ID); err != nil {
		t.Fatal(err)
	}
	store.fail.Store(false)
	n.setSnapHook(nil)

	start := time.Now()
	e.acceptStop(t, own, sbx.ID, false)
	for time.Now().Before(start.Add(10 * time.Second)) {
		if _, err := w.ProcessNext(context.WithoutCancel(t.Context())); err != nil {
			t.Fatal(err)
		}
		if e.sandbox(t, own, sbx.ID).State == db.StateArchived {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := e.sandbox(t, own, sbx.ID); got.State != db.StateArchived {
		t.Fatalf("stop after a crashed snapshot attempt = %s after %v (rows %s), want archived; "+
			"the dead attempt's pending row blocked it until its deadline",
			got.State, time.Since(start).Round(time.Millisecond), genSummary(genRows(t, e, sbx.ID)))
	}
	if elapsed := time.Since(start); elapsed >= 10*time.Second {
		t.Fatalf("stop took %v, want well under the snapshot timeout", elapsed)
	}
	rows = genRows(t, e, sbx.ID)
	if len(rows) != 2 || rows[0].status != db.GenerationFailed || rows[0].reason != "abandoned" ||
		rows[1].number != 1 || rows[1].status != db.GenerationCompleted {
		t.Fatalf("rows = %s, want the dead attempt failed abandoned and the final generation completed",
			genSummary(rows))
	}
}

// TestSnapshotAndStopNeverOverlap blocks a periodic SnapshotVM while a stop
// arrives on a second worker: the stop waits for the guard, so the node
// never sees two SnapshotVM calls (or a StopVM) during the first one.
func TestSnapshotAndStopNeverOverlap(t *testing.T) {
	e, n, mem := genSetup(t, true)
	own := e.owner(t)
	w1 := genWorker(t, e, e.store, mem, lifecycle.Options{})
	w2 := genWorker(t, e, e.store, mem, lifecycle.Options{})
	sbx := genCreate(t, e, w1, own, nil)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	n.setSnapHook(func(ctx context.Context, req *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
		first := false
		once.Do(func() { first = true })
		if first {
			close(entered)
			<-release
		}
		return n.Node.SnapshotVM(ctx, req)
	})
	if got, err := w1.EnqueueDueSnapshots(t.Context()); err != nil || got != 1 {
		t.Fatalf("enqueue = %d, %v", got, err)
	}
	var wg sync.WaitGroup
	wg.Go(func() { _, _ = w1.ProcessNext(context.WithoutCancel(t.Context())) })
	<-entered
	e.acceptStop(t, own, sbx.ID, false)
	wg.Go(func() { _, _ = w2.ProcessNext(context.WithoutCancel(t.Context())) })
	time.Sleep(300 * time.Millisecond)
	if order := n.callOrder(); strings.Join(order, ",") != "snapshot" {
		t.Fatalf("node calls while the snapshot holds the guard = %v", order)
	}
	close(release)
	wg.Wait()
	if got := e.sandbox(t, own, sbx.ID); got.State != db.StateArchived {
		t.Fatalf("sandbox = %s, want archived", got.State)
	}
	n.mu.Lock()
	peak := n.maxInFlight
	n.mu.Unlock()
	if peak != 1 {
		t.Fatalf("concurrent SnapshotVM calls = %d, want 1", peak)
	}
	if order := n.callOrder(); strings.Join(order, ",") != "snapshot,stop,snapshot" {
		t.Fatalf("node calls = %v", order)
	}
	rows := genRows(t, e, sbx.ID)
	if len(rows) != 2 || rows[0].status != db.GenerationCompleted || rows[1].status != db.GenerationCompleted {
		t.Fatalf("rows = %s, want the periodic and the final generation", genSummary(rows))
	}
}
