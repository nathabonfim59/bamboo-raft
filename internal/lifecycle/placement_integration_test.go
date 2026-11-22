package lifecycle_test

// Multi-node placement integration tests: sandboxes created from an
// object-store generation on another node, a node without an object store
// refusing a manifest restore, and the boot moving elsewhere. PostgreSQL plus
// nodefakes sharing one memstore, reached through nodelink and the gateway.
// They skip unless BBRAFT_TEST_DATABASE_URL is set (dbtest.Open).

import (
	"context"
	"encoding/json/v2"
	"io"
	"os"
	"path/filepath"
	"slices"
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
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// placeNode wraps nodefake to script CreateVM and DeleteVM and to record the
// order of boots and stops, without editing the fake.
type placeNode struct {
	*nodefake.Node
	root string

	mu         sync.Mutex
	createHook func(*nodev1.CreateVMRequest) error // non-nil error: refuse
	deleteErr  error
	creates    []*nodev1.CreateVMRequest
	order      []string
}

func (n *placeNode) setCreateHook(h func(*nodev1.CreateVMRequest) error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.createHook = h
}

func (n *placeNode) setDeleteErr(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.deleteErr = err
}

func (n *placeNode) CreateVM(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
	n.mu.Lock()
	n.creates = append(n.creates, req)
	n.order = append(n.order, "create")
	hook := n.createHook
	n.mu.Unlock()
	if hook != nil {
		if err := hook(req); err != nil {
			return nil, err
		}
	}
	return n.Node.CreateVM(ctx, req)
}

func (n *placeNode) DeleteVM(ctx context.Context, req *nodev1.DeleteVMRequest) (*nodev1.DeleteVMResponse, error) {
	n.mu.Lock()
	err := n.deleteErr
	n.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return n.Node.DeleteVM(ctx, req)
}

func (n *placeNode) StopVM(ctx context.Context, req *nodev1.StopVMRequest) (*nodev1.VM, error) {
	n.mu.Lock()
	n.order = append(n.order, "stop")
	n.mu.Unlock()
	return n.Node.StopVM(ctx, req)
}

func (n *placeNode) manifestCreates() []*nodev1.CreateVMRequest {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []*nodev1.CreateVMRequest
	for _, c := range n.creates {
		if c.GetFromManifest() != "" {
			out = append(out, c)
		}
	}
	return out
}

func (n *placeNode) callOrder() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.order)
}

// refuseManifests is a node without an object store as the contract
// describes it: manifest restores answer FAILED_PRECONDITION.
func refuseManifests(req *nodev1.CreateVMRequest) error {
	if req.GetFromManifest() != "" {
		return status.Error(codes.FailedPrecondition, "this node has no object store")
	}
	return nil
}

// placeStartNode registers and connects one placeNode on objects.
func placeStartNode(t *testing.T, e *lifecycleEnv, id string, objects objstore.Store) *placeNode {
	t.Helper()
	e.registerNode(t, id, db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	root := t.TempDir()
	fake := nodefake.New(id, root)
	fake.VCPUs = 64
	fake.MemoryMiB = 256 << 10
	fake.Store = objects
	n := &placeNode{Node: fake, root: root}
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

// placeHomeFile is a file under /home/user of a fake VM's disk.
func placeHomeFile(n *placeNode, sandboxID, name string) string {
	return filepath.Join(n.root, "vms", sandboxID, "home", "user", name)
}

// placeScheduler is the lifecycle role's Placer over the env's gateway.
func placeScheduler(t *testing.T, e *lifecycleEnv) *scheduler.Scheduler {
	t.Helper()
	s, err := scheduler.New(scheduler.Options{
		Store: e.store,
		Log:   quiet(),
		Dial: func(nodeID string) (scheduler.NodeClient, io.Closer, error) {
			c, err := e.factory.Dial(nodeID)
			if err != nil {
				return nil, nil, err
			}
			return c, c, nil
		},
		FreshWithin: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// placeEnv is two nodes on one memstore, a worker with a Placer, and the
// scheduler admitting creates.
type placeEnv struct {
	*lifecycleEnv
	a, b  *placeNode
	mem   *memstore.Store
	w     *lifecycle.Worker
	sched *scheduler.Scheduler
	own   uuid.UUID
}

func newPlaceEnv(t *testing.T, nodes ...string) *placeEnv {
	t.Helper()
	e := newLifecycleEnv(t)
	p := &placeEnv{lifecycleEnv: e, mem: memstore.New()}
	p.a = placeStartNode(t, e, "node-a", p.mem)
	if !slices.Contains(nodes, "only-a") {
		p.b = placeStartNode(t, e, "node-b", p.mem)
	}
	p.sched = placeScheduler(t, e)
	p.w = genWorker(t, e, e.store, p.mem, lifecycle.Options{Placer: p.sched})
	p.own = e.owner(t)
	return p
}

// boot creates and boots a sandbox on nodeID, with snapshots far in the
// future so later snapshot rounds pick only the sandbox a test makes due.
func (p *placeEnv) boot(t *testing.T, nodeID string) db.Sandbox {
	t.Helper()
	res, err := p.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: p.own, MachineType: "small", Name: "src", NodeID: nodeID,
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}, MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create sandbox: %v", err)
	}
	if !mustProcess(t, p.w) {
		t.Fatal("provision job not claimed")
	}
	sbx := p.sandbox(t, p.own, res.Sandbox.ID)
	if sbx.State != db.StateReady {
		t.Fatalf("sandbox after boot: %s", sbx.State)
	}
	p.quiet(t, sbx.ID)
	return sbx
}

// quiet moves a sandbox's next snapshot an hour out.
func (p *placeEnv) quiet(t *testing.T, sandboxID string) {
	t.Helper()
	if err := p.store.SetSnapshotSchedule(t.Context(), p.own, sandboxID, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
}

// generation writes note.txt into the sandbox on n and takes a generation.
func (p *placeEnv) generation(t *testing.T, n *placeNode, sbx db.Sandbox, note string) db.Generation {
	t.Helper()
	path := placeHomeFile(n, sbx.ID, "note.txt")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(note), 0o644); err != nil {
		t.Fatal(err)
	}
	genMakeDue(t, p.lifecycleEnv, sbx)
	if genRound(t, p.w) != 1 {
		t.Fatal("no snapshot job enqueued")
	}
	gen, ok, err := p.store.LatestGeneration(t.Context(), p.own, sbx.ID)
	if err != nil || !ok {
		t.Fatalf("latest generation: %v, %v", ok, err)
	}
	p.quiet(t, sbx.ID)
	return gen
}

// admit creates a sandbox from gen through the scheduler.
func (p *placeEnv) admit(t *testing.T, gen db.Generation, maxAttempts int, prefer ...string) db.CreateSandboxResult {
	t.Helper()
	res, err := p.sched.Admit(t.Context(), scheduler.AdmitRequest{
		ID: ids.Sandbox(), Owner: p.own, MachineType: "small", Name: "restored",
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}, MaxAttempts: maxAttempts,
		Source: &scheduler.GenerationSource{
			GenerationID: gen.ID, ManifestKey: gen.ManifestKey, Image: gen.Image,
			DiskBytes: gen.DiskBytes, Prefer: prefer,
		},
	})
	if err != nil {
		t.Fatalf("admit from generation: %v", err)
	}
	return res
}

func (p *placeEnv) node(t *testing.T, id string) db.Node {
	t.Helper()
	n, err := p.store.Node(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func (p *placeEnv) eventTypes(t *testing.T, sandboxID string) []string {
	t.Helper()
	events, err := p.store.SandboxEvents(t.Context(), p.own, sandboxID, 100)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(events))
	for i, ev := range events {
		out[i] = ev.Type
	}
	return out
}

func (p *placeEnv) provisionJobs(t *testing.T, sandboxID string) []db.Job {
	t.Helper()
	var out []db.Job
	rows, err := p.store.Pool().Query(t.Context(), `
		SELECT id, state, payload, source_generation_id FROM jobs
		WHERE sandbox_id = $1 AND kind = $2 ORDER BY created_at, id`, sandboxID, db.JobProvision)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			j       db.Job
			payload []byte
		)
		if err := rows.Scan(&j.ID, &j.State, &payload, &j.SourceGenerationID); err != nil {
			t.Fatal(err)
		}
		j.Payload = string(payload)
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func placeReadFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// TestCreateFromGenerationOnAnotherNode snapshots a sandbox on node-a and
// creates a new sandbox from that generation preferring node-b: it boots on
// node-b from the manifest with the file, a disk no smaller than the
// manifest's, the source generation on its job, and is hydrated at READY.
func TestCreateFromGenerationOnAnotherNode(t *testing.T) {
	p := newPlaceEnv(t)
	src := p.boot(t, "node-a")
	gen := p.generation(t, p.a, src, "from node a")

	res := p.admit(t, gen, 3, "node-b")
	if !mustProcess(t, p.w) {
		t.Fatal("provision job not claimed")
	}
	got := p.sandbox(t, p.own, res.Sandbox.ID)
	if got.State != db.StateReady || *got.NodeID != "node-b" {
		t.Fatalf("restored sandbox = %s on %v, want ready on node-b", got.State, *got.NodeID)
	}
	if note := placeReadFile(t, placeHomeFile(p.b, got.ID, "note.txt")); note != "from node a" {
		t.Fatalf("restored note = %q", note)
	}
	if got.Reserved.DiskGiB < db.DiskGiBFor(gen.DiskBytes) {
		t.Fatalf("disk %d GiB is smaller than the manifest's %d bytes", got.Reserved.DiskGiB, gen.DiskBytes)
	}
	if got.HydratedAt == nil {
		t.Fatal("hydrated_at not set at READY")
	}
	if events := p.eventTypes(t, got.ID); !slices.Contains(events, db.EventSandboxHydrated) {
		t.Fatalf("events = %v, want %s", events, db.EventSandboxHydrated)
	}
	if got.ChainID == src.ChainID {
		t.Fatal("restored sandbox continues the source's chain")
	}
	jobs := p.provisionJobs(t, got.ID)
	if len(jobs) != 1 || jobs[0].SourceGenerationID == nil || *jobs[0].SourceGenerationID != gen.ID {
		t.Fatalf("provision jobs = %+v, want one with source generation %s", jobs, gen.ID)
	}
	creates := p.b.manifestCreates()
	if len(creates) != 1 || creates[0].GetFromManifest() != gen.ManifestKey || creates[0].GetImage() != "" ||
		creates[0].GetResources().GetDiskGib() != got.Reserved.DiskGiB {
		t.Fatalf("node-b CreateVM calls = %v", creates)
	}
	if len(p.a.manifestCreates()) != 0 {
		t.Fatal("node-a was asked to restore")
	}
	// A normal create is never marked hydrated.
	if src := p.sandbox(t, p.own, src.ID); src.HydratedAt != nil {
		t.Fatal("fresh-disk sandbox has hydrated_at")
	}
}

// TestManifestRefusedMovesToAnotherNode: node-a answers FAILED_PRECONDITION
// to manifest restores while listing the image. It is marked without an
// object store, the sandbox moves to node-b (same id, lifecycle generation
// zero, the job's source generation) and becomes READY there; later manifest
// placements skip node-a until a snapshot on it succeeds.
func TestManifestRefusedMovesToAnotherNode(t *testing.T) {
	p := newPlaceEnv(t)
	src := p.boot(t, "node-b")
	gen := p.generation(t, p.b, src, "from node b")
	p.a.setCreateHook(refuseManifests)
	baseB := p.nodeReserved(t, "node-b")

	res := p.admit(t, gen, 3, "node-a")
	if *res.Sandbox.NodeID != "node-a" {
		t.Fatalf("admitted on %s, want the preferred node-a", *res.Sandbox.NodeID)
	}
	if !mustProcess(t, p.w) {
		t.Fatal("provision job not claimed")
	}
	moved := p.sandbox(t, p.own, res.Sandbox.ID)
	if moved.State != db.StateProvisioning || *moved.NodeID != "node-b" || *moved.DiskNodeID != "node-b" ||
		moved.LifecycleGeneration != 0 {
		t.Fatalf("after refusal: %s on %v (disk %v, generation %d)", moved.State, *moved.NodeID, *moved.DiskNodeID, moved.LifecycleGeneration)
	}
	if n := p.node(t, "node-a"); n.ObjectStoreOK || n.ObjectStoreCheckedAt == nil {
		t.Fatalf("node-a after refusal = ok %v checked %v, want marked without an object store", n.ObjectStoreOK, n.ObjectStoreCheckedAt)
	}
	if r := p.nodeReserved(t, "node-a"); r != (db.Resources{}) {
		t.Fatalf("node-a still reserves %+v", r)
	}
	jobs := p.provisionJobs(t, moved.ID)
	if len(jobs) != 2 || jobs[0].State != db.JobDone || jobs[1].State != db.JobQueued {
		t.Fatalf("provision jobs = %+v, want the refused one done and a queued one", jobs)
	}
	var payload db.ProvisionPayload
	if err := json.Unmarshal([]byte(jobs[1].Payload), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.NodeID != "node-b" || payload.FromManifest != gen.ManifestKey || payload.Image != gen.Image ||
		payload.Generation != 0 || jobs[1].SourceGenerationID == nil || *jobs[1].SourceGenerationID != gen.ID {
		t.Fatalf("new provision job = %+v source %v", payload, jobs[1].SourceGenerationID)
	}
	if events := p.eventTypes(t, moved.ID); !slices.Contains(events, db.EventSandboxRescheduled) {
		t.Fatalf("events = %v, want %s", events, db.EventSandboxRescheduled)
	}

	if !mustProcess(t, p.w) {
		t.Fatal("moved provision job not claimed")
	}
	got := p.sandbox(t, p.own, moved.ID)
	if got.State != db.StateReady || *got.NodeID != "node-b" || got.HydratedAt == nil {
		t.Fatalf("moved sandbox = %s on %v hydrated %v", got.State, *got.NodeID, got.HydratedAt)
	}
	if note := placeReadFile(t, placeHomeFile(p.b, got.ID, "note.txt")); note != "from node b" {
		t.Fatalf("restored note = %q", note)
	}
	want := baseB
	want.VCPUs += got.Reserved.VCPUs
	want.MemoryMiB += got.Reserved.MemoryMiB
	want.DiskGiB += got.Reserved.DiskGiB
	if r := p.nodeReserved(t, "node-b"); r != want {
		t.Fatalf("node-b reserves %+v, want %+v", r, want)
	}

	// Later manifest placements skip node-a, even when preferred.
	cands, err := p.sched.Place(t.Context(), scheduler.PlaceRequest{
		Reserve: got.Reserved, Image: gen.Image, Prefer: []string{"node-a"}, NeedObjectStore: true,
	})
	if err != nil || len(cands) != 1 || cands[0].Node.ID != "node-b" {
		t.Fatalf("manifest placement after refusal = %v, %v; want node-b only", cands, err)
	}
	p.quiet(t, got.ID)

	// A successful snapshot on node-a marks it usable again.
	fresh := p.boot(t, "node-a")
	p.generation(t, p.a, fresh, "node a works")
	if n := p.node(t, "node-a"); !n.ObjectStoreOK {
		t.Fatal("node-a not marked usable after a successful snapshot")
	}
	cands, err = p.sched.Place(t.Context(), scheduler.PlaceRequest{
		Reserve: got.Reserved, Image: gen.Image, Prefer: []string{"node-a"}, NeedObjectStore: true,
	})
	if err != nil || len(cands) != 2 || cands[0].Node.ID != "node-a" {
		t.Fatalf("manifest placement after recovery = %v, %v; want node-a first", cands, err)
	}
}

// TestManifestRefusedWithoutAnotherNode: with no other candidate the boot
// fails terminally with no_ready_machine and the reservation is released.
func TestManifestRefusedWithoutAnotherNode(t *testing.T) {
	p := newPlaceEnv(t, "only-a")
	src := p.boot(t, "node-a")
	gen := p.generation(t, p.a, src, "only a")
	p.a.setCreateHook(refuseManifests)
	base := p.nodeReserved(t, "node-a")

	res := p.admit(t, gen, 3)
	if !mustProcess(t, p.w) {
		t.Fatal("provision job not claimed")
	}
	got := p.sandbox(t, p.own, res.Sandbox.ID)
	if got.State != db.StateError || got.ErrorReason != "no_ready_machine" {
		t.Fatalf("sandbox = %s (%s), want error no_ready_machine", got.State, got.ErrorReason)
	}
	if r := p.nodeReserved(t, "node-a"); r != base {
		t.Fatalf("node-a reserves %+v, want %+v (released)", r, base)
	}
	if jobs := p.provisionJobs(t, got.ID); len(jobs) != 1 || jobs[0].State != db.JobFailed {
		t.Fatalf("provision jobs = %+v, want one failed", jobs)
	}
}

// TestManifestRestoreTerminalReasons maps a missing manifest and a rejected
// restore to their sandbox error reasons without retrying.
func TestManifestRestoreTerminalReasons(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(t *testing.T, p *placeEnv, gen db.Generation)
		reason string
	}{
		{"manifest deleted", func(t *testing.T, p *placeEnv, gen db.Generation) {
			if err := p.mem.Delete(t.Context(), gen.ManifestKey); err != nil {
				t.Fatal(err)
			}
		}, "snapshot_not_found"},
		{"restore rejected", func(t *testing.T, p *placeEnv, gen db.Generation) {
			p.b.setCreateHook(func(req *nodev1.CreateVMRequest) error {
				if req.GetFromManifest() != "" {
					return status.Error(codes.InvalidArgument, "disk smaller than the manifest")
				}
				return nil
			})
		}, "restore_rejected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newPlaceEnv(t)
			src := p.boot(t, "node-a")
			gen := p.generation(t, p.a, src, "x")
			tc.break_(t, p, gen)
			res := p.admit(t, gen, 3, "node-b")
			if !mustProcess(t, p.w) {
				t.Fatal("provision job not claimed")
			}
			got := p.sandbox(t, p.own, res.Sandbox.ID)
			if got.State != db.StateError || got.ErrorReason != tc.reason {
				t.Fatalf("sandbox = %s (%s), want error %s", got.State, got.ErrorReason, tc.reason)
			}
			if n := len(p.b.manifestCreates()); n != 1 {
				t.Fatalf("node-b saw %d restore attempts, want 1 (terminal)", n)
			}
			if r := p.nodeReserved(t, "node-b"); r != (db.Resources{}) {
				t.Fatalf("node-b reserves %+v after the terminal failure", r)
			}
			if !p.node(t, "node-b").ObjectStoreOK {
				t.Fatal("a terminal restore failure marked node-b without an object store")
			}
		})
	}
}

// placeReadmitted runs a refused boot on node-a so the sandbox is moved to
// node-b with a queued provision job, and returns the sandbox and that job.
func placeReadmitted(t *testing.T, p *placeEnv) (db.Sandbox, db.Job) {
	t.Helper()
	src := p.boot(t, "node-b")
	gen := p.generation(t, p.b, src, "moved")
	p.a.setCreateHook(refuseManifests)
	res := p.admit(t, gen, 1, "node-a")
	if !mustProcess(t, p.w) {
		t.Fatal("provision job not claimed")
	}
	jobs := p.provisionJobs(t, res.Sandbox.ID)
	if len(jobs) != 2 || jobs[1].State != db.JobQueued {
		t.Fatalf("provision jobs = %+v, want a queued re-admitted boot", jobs)
	}
	return p.sandbox(t, p.own, res.Sandbox.ID), jobs[1]
}

// TestReadmittedBootIsAwaitedByStop: a stop accepted while the re-admitted
// boot is still queued waits for it (lifecycle generation zero keeps it the
// initial provision), then archives the disk the boot created on node-b.
func TestReadmittedBootIsAwaitedByStop(t *testing.T) {
	p := newPlaceEnv(t)
	sbx, boot := placeReadmitted(t, p)

	// Hold the boot back so the stop job is claimed first.
	if _, err := p.store.Pool().Exec(t.Context(),
		`UPDATE jobs SET run_after = now() + interval '1 hour' WHERE id = $1`, boot.ID); err != nil {
		t.Fatal(err)
	}
	p.acceptStop(t, p.own, sbx.ID, false)
	if !mustProcess(t, p.w) {
		t.Fatal("stop job not claimed")
	}
	if got := p.sandbox(t, p.own, sbx.ID); got.State != db.StateArchiving {
		t.Fatalf("sandbox while the boot is queued = %s, want archiving", got.State)
	}
	var stopState db.JobState
	var lastError string
	if err := p.store.Pool().QueryRow(t.Context(), `
		SELECT state, last_error FROM jobs WHERE sandbox_id = $1 AND kind = $2`,
		sbx.ID, db.JobStop).Scan(&stopState, &lastError); err != nil {
		t.Fatal(err)
	}
	if stopState != db.JobQueued || lastError != "initial boot in flight" {
		t.Fatalf("stop job = %s %q, want queued behind the boot", stopState, lastError)
	}
	if slices.Contains(p.b.callOrder(), "stop") {
		t.Fatal("StopVM ran before the re-admitted boot")
	}

	if _, err := p.store.Pool().Exec(t.Context(), `UPDATE jobs SET run_after = now() WHERE id = $1`, boot.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "archived", func() bool {
		mustProcess(t, p.w)
		return p.sandbox(t, p.own, sbx.ID).State == db.StateArchived
	})
	order := p.b.callOrder()
	create, stop := slices.Index(order, "create"), slices.Index(order, "stop")
	if create < 0 || stop < 0 || stop < create || len(p.b.manifestCreates()) != 1 {
		t.Fatalf("node-b calls = %v, want the manifest boot before StopVM", order)
	}
}

// TestReadmittedBootFailureIsReconciled: a re-admitted boot that fails
// terminally while its cleanup cannot reach the node keeps its reservation;
// failed-job reconciliation then cleans it up like any initial provision.
func TestReadmittedBootFailureIsReconciled(t *testing.T) {
	p := newPlaceEnv(t)
	sbx, _ := placeReadmitted(t, p)
	base := p.nodeReserved(t, "node-b")
	p.b.setCreateHook(func(req *nodev1.CreateVMRequest) error {
		if req.GetFromManifest() != "" {
			return status.Error(codes.InvalidArgument, "restore rejected")
		}
		return nil
	})
	p.b.setDeleteErr(status.Error(codes.Unavailable, "node unreachable"))

	if !mustProcess(t, p.w) {
		t.Fatal("re-admitted boot not claimed")
	}
	got := p.sandbox(t, p.own, sbx.ID)
	if got.State != db.StateError || got.ErrorReason != "restore_rejected" {
		t.Fatalf("sandbox = %s (%s), want error restore_rejected", got.State, got.ErrorReason)
	}
	if r := p.nodeReserved(t, "node-b"); r != base {
		t.Fatalf("node-b reserves %+v before reconciliation, want %+v (cleanup unconfirmed)", r, base)
	}

	p.b.setDeleteErr(nil)
	p.w.ReapOnce(t.Context())
	want := base
	want.VCPUs -= got.Reserved.VCPUs
	want.MemoryMiB -= got.Reserved.MemoryMiB
	want.DiskGiB -= got.Reserved.DiskGiB
	if r := p.nodeReserved(t, "node-b"); r != want {
		t.Fatalf("node-b reserves %+v after reconciliation, want %+v", r, want)
	}
}

// placeVMState is the fake's state of sandboxID's VM, or UNSPECIFIED when
// the node has none.
func placeVMState(t *testing.T, n *placeNode, sandboxID string) nodev1.VMState {
	t.Helper()
	vms, err := n.ListVMs(t.Context(), &nodev1.ListVMsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, vm := range vms.GetVms() {
		if vm.GetSandboxId() == sandboxID {
			return vm.GetState()
		}
	}
	return nodev1.VMState_VM_STATE_UNSPECIFIED
}

// TestStopBeforeReadmit: a stop accepted while the first boot of a manifest
// restore is still on node-a (lifecycle generation zero, archiving) survives
// the re-admit: the boot moves to node-b, the stop waits for it and then
// archives node-b's disk. Node-a keeps no VM and no reservation; node-b keeps
// the stopped VM and only the disk part of the reservation (a stop retains
// the disk). Two orders: the stop lands before the boot is claimed, or while
// node-a is refusing (the re-admit then conflicts on the row version and the
// retried boot re-admits).
func TestStopBeforeReadmit(t *testing.T) {
	for _, tc := range []struct {
		name         string
		stopInRefuse bool
	}{
		{"stop before the boot runs", false},
		{"stop while node-a refuses", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newPlaceEnv(t)
			src := p.boot(t, "node-b")
			gen := p.generation(t, p.b, src, "stopped")
			baseB := p.nodeReserved(t, "node-b")

			var (
				once    sync.Once
				pending = make(chan string, 1) // sandbox id to stop from the hook
			)
			p.a.setCreateHook(func(req *nodev1.CreateVMRequest) error {
				if req.GetFromManifest() == "" {
					return nil
				}
				if tc.stopInRefuse {
					once.Do(func() {
						id := <-pending
						if _, _, _, _, err := p.store.AcceptSandboxStop(context.Background(), p.own, id, false); err != nil {
							t.Errorf("accept stop inside the refusal: %v", err)
						}
					})
				}
				return status.Error(codes.FailedPrecondition, "this node has no object store")
			})
			res := p.admit(t, gen, 3, "node-a")
			id := res.Sandbox.ID
			if tc.stopInRefuse {
				pending <- id
			} else {
				p.acceptStop(t, p.own, id, false)
			}

			waitFor(t, "archived", func() bool {
				mustProcess(t, p.w)
				return p.sandbox(t, p.own, id).State == db.StateArchived
			})

			got := p.sandbox(t, p.own, id)
			if *got.NodeID != "node-b" || got.DiskNodeID == nil || *got.DiskNodeID != "node-b" || got.LifecycleGeneration != 0 {
				t.Fatalf("archived on %v (disk %v, generation %d), want node-b", *got.NodeID, got.DiskNodeID, got.LifecycleGeneration)
			}
			if r := p.nodeReserved(t, "node-a"); r != (db.Resources{}) {
				t.Fatalf("node-a reserves %+v, want nothing", r)
			}
			if st := placeVMState(t, p.a, id); st != nodev1.VMState_VM_STATE_UNSPECIFIED {
				t.Fatalf("node-a has a VM in %s", st)
			}
			if got.Reserved != (db.Resources{DiskGiB: got.Desired.DiskGiB}) {
				t.Fatalf("archived reservation %+v, want only the %d GiB disk", got.Reserved, got.Desired.DiskGiB)
			}
			want := baseB
			want.DiskGiB += got.Desired.DiskGiB
			if r := p.nodeReserved(t, "node-b"); r != want {
				t.Fatalf("node-b reserves %+v, want %+v (the source plus this disk)", r, want)
			}
			if st := placeVMState(t, p.b, id); st != nodev1.VMState_VM_STATE_STOPPED {
				t.Fatalf("node-b VM is %s, want STOPPED with the disk kept", st)
			}
			order := p.b.callOrder()
			if create, stop := slices.Index(order, "create"), slices.Index(order, "stop"); create < 0 || stop < create {
				t.Fatalf("node-b calls = %v, want the manifest boot before StopVM", order)
			}
			if n := len(p.b.manifestCreates()); n != 1 {
				t.Fatalf("node-b saw %d manifest boots, want 1", n)
			}
			if note := placeReadFile(t, placeHomeFile(p.b, id, "note.txt")); note != "stopped" {
				t.Fatalf("archived disk note = %q", note)
			}
			jobs := p.provisionJobs(t, id)
			if len(jobs) != 2 || jobs[0].State != db.JobDone || jobs[1].State != db.JobDone {
				t.Fatalf("provision jobs = %+v, want the refused and the moved boot done", jobs)
			}
		})
	}
}
