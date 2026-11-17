package lifecycle_test

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

const lifecycleGateToken = "lifecycle-internal-token"

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// lifecycleNode wraps nodefake with request recording and per-test hooks, so
// tests can observe exact CreateVM/DeleteVM/StopVM/StartVM/GetVM calls and
// force disconnects, blocked boots and failures without touching the fake.
type lifecycleNode struct {
	*nodefake.Node

	mu         sync.Mutex
	creates    []*nodev1.CreateVMRequest
	deletes    []string
	stops      []*nodev1.StopVMRequest
	starts     []*nodev1.StartVMRequest
	createHook func(context.Context, *nodev1.CreateVMRequest) (*nodev1.VM, error)
	deleteHook func(context.Context, *nodev1.DeleteVMRequest) (*nodev1.DeleteVMResponse, error)
	stopHook   func(context.Context, *nodev1.StopVMRequest) (*nodev1.VM, error)
	startHook  func(context.Context, *nodev1.StartVMRequest) (*nodev1.VM, error)
	listHook   func(context.Context, *nodev1.ListVMsRequest) (*nodev1.ListVMsResponse, error)
}

func newLifecycleNode(id, root string) *lifecycleNode {
	fake := nodefake.New(id, root)
	fake.VCPUs = 64
	fake.MemoryMiB = 256 << 10
	return &lifecycleNode{Node: fake}
}

func (n *lifecycleNode) setCreateHook(h func(context.Context, *nodev1.CreateVMRequest) (*nodev1.VM, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.createHook = h
}

func (n *lifecycleNode) setDeleteHook(h func(context.Context, *nodev1.DeleteVMRequest) (*nodev1.DeleteVMResponse, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.deleteHook = h
}

func (n *lifecycleNode) setStopHook(h func(context.Context, *nodev1.StopVMRequest) (*nodev1.VM, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stopHook = h
}

func (n *lifecycleNode) setStartHook(h func(context.Context, *nodev1.StartVMRequest) (*nodev1.VM, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.startHook = h
}

func (n *lifecycleNode) setListHook(h func(context.Context, *nodev1.ListVMsRequest) (*nodev1.ListVMsResponse, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.listHook = h
}

func (n *lifecycleNode) CreateVM(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
	n.mu.Lock()
	n.creates = append(n.creates, req)
	hook := n.createHook
	n.mu.Unlock()
	if hook != nil {
		return hook(ctx, req)
	}
	return n.Node.CreateVM(ctx, req)
}

func (n *lifecycleNode) DeleteVM(ctx context.Context, req *nodev1.DeleteVMRequest) (*nodev1.DeleteVMResponse, error) {
	n.mu.Lock()
	n.deletes = append(n.deletes, req.GetSandboxId())
	hook := n.deleteHook
	n.mu.Unlock()
	if hook != nil {
		return hook(ctx, req)
	}
	return n.Node.DeleteVM(ctx, req)
}

func (n *lifecycleNode) StopVM(ctx context.Context, req *nodev1.StopVMRequest) (*nodev1.VM, error) {
	n.mu.Lock()
	n.stops = append(n.stops, req)
	hook := n.stopHook
	n.mu.Unlock()
	if hook != nil {
		return hook(ctx, req)
	}
	return n.Node.StopVM(ctx, req)
}

func (n *lifecycleNode) StartVM(ctx context.Context, req *nodev1.StartVMRequest) (*nodev1.VM, error) {
	n.mu.Lock()
	n.starts = append(n.starts, req)
	hook := n.startHook
	n.mu.Unlock()
	if hook != nil {
		return hook(ctx, req)
	}
	return n.Node.StartVM(ctx, req)
}

func (n *lifecycleNode) ListVMs(ctx context.Context, req *nodev1.ListVMsRequest) (*nodev1.ListVMsResponse, error) {
	n.mu.Lock()
	hook := n.listHook
	n.mu.Unlock()
	if hook != nil {
		return hook(ctx, req)
	}
	return n.Node.ListVMs(ctx, req)
}

func (n *lifecycleNode) createCalls() []*nodev1.CreateVMRequest {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]*nodev1.CreateVMRequest(nil), n.creates...)
}

func (n *lifecycleNode) deleteCalls() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.deletes...)
}

func (n *lifecycleNode) stopCalls() []*nodev1.StopVMRequest {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]*nodev1.StopVMRequest(nil), n.stops...)
}

func (n *lifecycleNode) startCalls() []*nodev1.StartVMRequest {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]*nodev1.StartVMRequest(nil), n.starts...)
}

// lifecycleEnv wires PostgreSQL, a hub, the internal gateway and fake nodes
// reached exactly the way the lifecycle role reaches them: through the
// gateway client, never the Hub.
type lifecycleEnv struct {
	store   *db.Store
	hub     *nodelink.Hub
	factory *nodegateway.ClientFactory
	addr    string
	nodes   map[string]*lifecycleNode
	stops   map[string]func()
}

func newLifecycleEnv(t *testing.T) *lifecycleEnv {
	t.Helper()
	store := dbtest.Open(t)
	// Infrastructure lifetime: these servers must stay up for the whole
	// test and are explicitly canceled/joined by cleanup. Deriving them
	// from t.Context would cancel them at the same time as the cleanup
	// that joins them, which is harder to reason about.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	hubLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hub := nodelink.NewHub(func(_ context.Context, h nodelink.Hello) error {
		want := sha256.Sum256([]byte("node-token"))
		got := sha256.Sum256([]byte(h.Token))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			return errors.New("bad node token")
		}
		return nil
	}, quiet())
	hubDone := make(chan struct{})
	go func() { _ = hub.Serve(ctx, hubLn); close(hubDone) }()
	t.Cleanup(func() { cancel(); <-hubDone })

	gw, err := nodegateway.New(hub, lifecycleGateToken)
	if err != nil {
		t.Fatal(err)
	}
	gwLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gwDone := make(chan struct{})
	go func() { _ = gw.Serve(ctx, gwLn); close(gwDone) }()
	t.Cleanup(func() { cancel(); <-gwDone })

	return &lifecycleEnv{
		store:   store,
		hub:     hub,
		factory: nodegateway.NewClientFactory(gwLn.Addr().String(), lifecycleGateToken, nil),
		addr:    hubLn.Addr().String(),
		nodes:   map[string]*lifecycleNode{},
		stops:   map[string]func(){},
	}
}

func (e *lifecycleEnv) startNode(t *testing.T, id string) *lifecycleNode {
	t.Helper()
	n := newLifecycleNode(id, t.TempDir())
	srv := grpc.NewServer(nodelink.ServerOptions()...)
	nodev1.RegisterNodeServer(srv, n)
	// Node link lifetime: started here and joined by t.Cleanup, so use a
	// dedicated context rather than the test context.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = nodelink.Serve(ctx, nodelink.DialConfig{
			Addr: e.addr, NodeID: id, Token: "node-token", Log: quiet(),
			MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		}, srv)
		close(done)
	}()
	stop := func() { cancel(); <-done }
	e.nodes[id] = n
	e.stops[id] = stop
	t.Cleanup(func() {
		if _, ok := e.stops[id]; ok {
			stop()
		}
	})
	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()
	if _, err := e.hub.Wait(waitCtx, id); err != nil {
		t.Fatalf("node %s did not connect: %v", id, err)
	}
	return n
}

func (e *lifecycleEnv) registerNode(t *testing.T, id string, total db.Resources) {
	t.Helper()
	err := e.store.UpsertNodeInfo(t.Context(), db.NodeInfo{
		ID: id, CPUModel: "test", QEMUVersion: "9", DefaultImage: nodefake.Image,
		Capacity:    db.Resources{VCPUs: total.VCPUs, MemoryMiB: total.MemoryMiB},
		DiskFreeGiB: total.DiskGiB, // fresh row: no reservations, free == total
		SeenAt:      time.Now(),
	})
	if err != nil {
		t.Fatalf("register node: %v", err)
	}
}

func (e *lifecycleEnv) owner(t *testing.T) uuid.UUID {
	t.Helper()
	a, err := e.store.EnsureAccount(t.Context(), "acct-"+uuid.NewV7().String())
	if err != nil {
		t.Fatal(err)
	}
	return a.ID
}

// worker returns a Worker with short, deterministic test timings, the
// production lease unless a test overrides it, and the gateway-bound dial
// function. Callers override fields after this returns.
func (e *lifecycleEnv) worker(t *testing.T, opts lifecycle.Options) *lifecycle.Worker {
	t.Helper()
	return e.workerWithStore(t, e.store, opts)
}

func (e *lifecycleEnv) workerWithStore(t *testing.T, store lifecycle.Store, opts lifecycle.Options) *lifecycle.Worker {
	t.Helper()
	opts.Store = store
	if opts.Logger == nil {
		opts.Logger = quiet()
	}
	opts.Dial = func(nodeID string) (lifecycle.NodeClient, io.Closer, error) {
		c, err := e.factory.Dial(nodeID)
		if err != nil {
			return nil, nil, err
		}
		return c, c, nil
	}
	if opts.Workers == 0 {
		opts.Workers = 1
	}
	// Healthy fixtures use the production lease. A short 300ms fixture
	// lease renews every 100ms with a 100ms renewal deadline, so one slow
	// database round trip under a loaded full-suite run can abort an
	// otherwise healthy attempt as "lease lost" and redeliver it, adding an
	// incidental CreateVM attempt (and a third attempt in the injected-retry
	// tests). Tests that intentionally exercise lease expiry pass an
	// explicit short Lease and keep it. (B1.2)
	if opts.Lease == 0 {
		opts.Lease = lifecycle.DefaultLease
	}
	if opts.BootTimeout == 0 {
		opts.BootTimeout = 5 * time.Second
	}
	if opts.RetryBase == 0 {
		opts.RetryBase = 10 * time.Millisecond
	}
	if opts.MaxBackoff == 0 {
		opts.MaxBackoff = 100 * time.Millisecond
	}
	if opts.JobPoll == 0 {
		opts.JobPoll = 20 * time.Millisecond
	}
	if opts.ReaperInterval == 0 {
		opts.ReaperInterval = 20 * time.Millisecond
	}
	if opts.CleanupTimeout == 0 {
		opts.CleanupTimeout = 2 * time.Second
	}
	if opts.ReapGuardWait == 0 {
		opts.ReapGuardWait = 2 * time.Second
	}
	if opts.GuardReacquire == 0 {
		opts.GuardReacquire = 500 * time.Millisecond
	}
	w, err := lifecycle.New(opts)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	return w
}

func (e *lifecycleEnv) createSandbox(t *testing.T, owner uuid.UUID, nodeID string, maxAttempts int) db.CreateSandboxResult {
	t.Helper()
	return e.createSandboxEnv(t, owner, nodeID, maxAttempts, nil)
}

// createSandboxTTL creates a sandbox with a configured auto-stop duration.
func (e *lifecycleEnv) createSandboxTTL(t *testing.T, owner uuid.UUID, nodeID string, maxAttempts int, ttl *int) db.CreateSandboxResult {
	t.Helper()
	res, err := e.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID:          ids.Sandbox(),
		Owner:       owner,
		MachineType: "small",
		Name:        "ttl-test",
		TTLSeconds:  ttl,
		NodeID:      nodeID,
		Reserve:     db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1},
		MaxAttempts: maxAttempts,
	})
	if err != nil {
		t.Fatalf("create sandbox: %v", err)
	}
	return res
}

func (e *lifecycleEnv) createSandboxEnv(t *testing.T, owner uuid.UUID, nodeID string, maxAttempts int, env map[string]string) db.CreateSandboxResult {
	t.Helper()
	res, err := e.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID:          ids.Sandbox(),
		Owner:       owner,
		MachineType: "small",
		Name:        "test",
		Env:         env,
		NodeID:      nodeID,
		Reserve:     db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1},
		MaxAttempts: maxAttempts,
	})
	if err != nil {
		t.Fatalf("create sandbox: %v", err)
	}
	return res
}

func (e *lifecycleEnv) sandbox(t *testing.T, owner uuid.UUID, id string) db.Sandbox {
	t.Helper()
	sbx, err := e.store.SandboxAny(t.Context(), owner, id)
	if err != nil {
		t.Fatalf("sandbox %s: %v", id, err)
	}
	return sbx
}

func (e *lifecycleEnv) jobState(t *testing.T, jobID uuid.UUID) db.JobState {
	t.Helper()
	var state db.JobState
	if err := e.store.Pool().QueryRow(t.Context(),
		`SELECT state FROM jobs WHERE id = $1`, jobID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func (e *lifecycleEnv) vmIDs(t *testing.T, nodeID string) map[string]bool {
	t.Helper()
	n := e.nodes[nodeID]
	if n == nil {
		return nil
	}
	resp, err := n.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	_ = resp
	vms, err := n.ListVMs(t.Context(), &nodev1.ListVMsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, vm := range vms.GetVms() {
		out[vm.GetSandboxId()] = true
	}
	return out
}

func (e *lifecycleEnv) count(t *testing.T, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := e.store.Pool().QueryRow(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *lifecycleEnv) nodeReserved(t *testing.T, nodeID string) db.Resources {
	t.Helper()
	n, err := e.store.Node(t.Context(), nodeID)
	if err != nil {
		t.Fatal(err)
	}
	return n.Reserved
}

// waitFor polls cond until it returns true or the test deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
