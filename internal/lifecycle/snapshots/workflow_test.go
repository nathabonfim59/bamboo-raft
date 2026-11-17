package snapshots_test

// Fresh saved-snapshot workflow tests. Every acceptance flow runs the frozen
// plugin handlers through the real lifecycle worker over live PostgreSQL and
// reaches nodefake only through the authenticated internal gateway and a real
// nodelink connection. The suite needs BBRAFT_TEST_DATABASE_URL and skips
// without it (BBRAFT_TEST_REQUIRE_ALL=1 still fails, never weakens the gate).
//
// The existing plugin_integration_test.go keeps the direct-gRPC smoke flows;
// this file covers the remaining workflow, race and reconciliation gaps.

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle/snapshots"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

const (
	snapshotFlowNodeToken    = "snapshot-flow-node-token"
	snapshotFlowGatewayToken = "snapshot-flow-gateway-token"
)

func snapshotFlowQuiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// snapshotFlowEnv owns one migrated schema, one node link, the internal
// gateway and a gateway client factory. The serve contexts are
// Background-derived on purpose: t.Context is cancelled before cleanups run,
// and node-side cleanup (DeleteVM/DeleteSnapshot) still needs the link.
type snapshotFlowEnv struct {
	store    *db.Store
	hub      *nodelink.Hub
	hubAddr  string
	gwAddr   string
	factory  *nodegateway.ClientFactory
	owner    uuid.UUID
	nodeID   string
	node     nodev1.NodeServer
	account  db.Account
	nodeSeen time.Time
}

func snapshotFlowNewEnv(t *testing.T, nodeID string, node nodev1.NodeServer) *snapshotFlowEnv {
	t.Helper()
	store := dbtest.Open(t)
	hub, hubAddr := snapshotFlowHub(t)
	snapshotFlowStartNode(t, hubAddr, nodeID, node)
	snapshotFlowWaitLink(t, hub, nodeID)
	gwAddr := snapshotFlowGateway(t, hub)
	factory := nodegateway.NewClientFactory(gwAddr, snapshotFlowGatewayToken, nil)
	account, err := store.EnsureAccount(t.Context(), "snapflow-"+uuid.NewV7().String())
	if err != nil {
		t.Fatalf("account: %v", err)
	}
	env := &snapshotFlowEnv{
		store: store, hub: hub, hubAddr: hubAddr, gwAddr: gwAddr,
		factory: factory, owner: account.ID, nodeID: nodeID, node: node, account: account,
	}
	env.seedNode(t)
	return env
}

// seedNode registers a fresh node row with enough room for every flow.
func (e *snapshotFlowEnv) seedNode(t *testing.T) {
	t.Helper()
	e.nodeSeen = time.Now()
	if err := e.store.UpsertNodeInfo(t.Context(), db.NodeInfo{
		ID: e.nodeID, CPUModel: "fake", QEMUVersion: "none",
		Capacity:    db.Resources{VCPUs: 64, MemoryMiB: 262144},
		DiskFreeGiB: 4096,
		SeenAt:      e.nodeSeen,
	}); err != nil {
		t.Fatalf("node info: %v", err)
	}
}

// dial is the lifecycle gateway dialer for the env's factory.
func (e *snapshotFlowEnv) dial(nodeID string) (lifecycle.NodeClient, io.Closer, error) {
	client, err := e.factory.Dial(nodeID)
	if err != nil {
		return nil, nil, err
	}
	return client, client, nil
}

// snapshotFlowHub starts a nodelink hub that authenticates the fixed node
// token and returns its address.
func snapshotFlowHub(t *testing.T) (*nodelink.Hub, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("hub listen: %v", err)
	}
	want := sha256.Sum256([]byte(snapshotFlowNodeToken))
	hub := nodelink.NewHub(func(_ context.Context, hello nodelink.Hello) error {
		got := sha256.Sum256([]byte(hello.Token))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			return errors.New("bad node token")
		}
		return nil
	}, snapshotFlowQuiet())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = hub.Serve(ctx, ln); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return hub, ln.Addr().String()
}

// snapshotFlowStartNode serves node over a real nodelink connection.
func snapshotFlowStartNode(t *testing.T, hubAddr, nodeID string, node nodev1.NodeServer) {
	t.Helper()
	srv := grpc.NewServer(nodelink.ServerOptions()...)
	nodev1.RegisterNodeServer(srv, node)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = nodelink.Serve(ctx, nodelink.DialConfig{
			Addr: hubAddr, NodeID: nodeID, Token: snapshotFlowNodeToken,
			Log: snapshotFlowQuiet(), MinBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond,
		}, srv)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })
}

// snapshotFlowGateway serves the authenticated internal gateway.
func snapshotFlowGateway(t *testing.T, hub *nodelink.Hub) string {
	t.Helper()
	gw, err := nodegateway.New(hub, snapshotFlowGatewayToken)
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("gateway listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gw.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("gateway Serve: %v", err)
		}
	})
	return ln.Addr().String()
}

func snapshotFlowWaitLink(t *testing.T, hub *nodelink.Hub, nodeID string) *nodelink.Link {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	link, err := hub.Wait(ctx, nodeID)
	if err != nil {
		t.Fatalf("node %s never connected: %v", nodeID, err)
	}
	return link
}

// snapshotFlowReadySource creates a ready source sandbox on the env's node and
// completes its provision job so the save job is the only runnable work. An
// optional env is sealed on the source row.
func (e *snapshotFlowEnv) readySource(t *testing.T, machine, name string, res db.Resources, env ...map[string]string) db.Sandbox {
	t.Helper()
	var sealed map[string]string
	if len(env) > 0 {
		sealed = env[0]
	}
	created, err := e.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: e.owner, MachineType: machine, Name: name,
		Env: sealed, NodeID: e.nodeID, Reserve: res,
	})
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	if _, err := e.store.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET state = 'ready' WHERE id = $1`, created.Sandbox.ID); err != nil {
		t.Fatalf("mark source ready: %v", err)
	}
	if _, err := e.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET state = 'done' WHERE sandbox_id = $1 AND kind = 'sandbox.provision'`,
		created.Sandbox.ID); err != nil {
		t.Fatalf("finish provision job: %v", err)
	}
	sandbox, err := e.store.SandboxAny(t.Context(), e.owner, created.Sandbox.ID)
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	return sandbox
}

// fake returns the nodefake under the registered node server, if it is one.
func (e *snapshotFlowEnv) fake(t *testing.T) *nodefake.Node {
	t.Helper()
	switch node := e.node.(type) {
	case *nodefake.Node:
		return node
	case *snapshotFlowCaptureNode:
		return node.Node
	case *snapshotFlowGateNode:
		return node.Node
	case *snapshotFlowInfoNode:
		return node.Node
	case *snapshotFlowDeleteNode:
		return node.Node
	case *snapshotFlowFailOnceDeleteNode:
		return node.Node
	default:
		t.Fatalf("node server %T has no nodefake", e.node)
		return nil
	}
}

// createVM seeds the fake node's VM directly. This is test setup, not a
// control-plane side effect; every handler path still uses the gateway.
func (e *snapshotFlowEnv) createVM(t *testing.T, sandboxID string, res db.Resources) {
	t.Helper()
	if _, err := e.fake(t).CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: sandboxID,
		Resources: &nodev1.Resources{Vcpus: res.VCPUs, MemoryMib: res.MemoryMiB, DiskGib: res.DiskGiB},
	}); err != nil {
		t.Fatalf("create fake vm: %v", err)
	}
}

func snapshotFlowWorker(t *testing.T, store *db.Store, dial lifecycle.Dial, lease time.Duration) *lifecycle.Worker {
	t.Helper()
	return snapshotFlowWorkerWith(t, store, store, dial, lease)
}

// snapshotFlowWorkerWith builds a worker whose lifecycle store and snapshot
// plugin store may differ (a test wrapper). Both retry bases are explicit so
// redelivery barriers are short and deterministic.
func snapshotFlowWorkerWith(t *testing.T, store lifecycle.Store, pluginStore snapshots.Store,
	dial lifecycle.Dial, lease time.Duration,
) *lifecycle.Worker {
	t.Helper()
	return snapshotFlowWorkerWithLogger(t, store, pluginStore, dial, lease, snapshotFlowQuiet())
}

func snapshotFlowWorkerWithLogger(t *testing.T, store lifecycle.Store, pluginStore snapshots.Store,
	dial lifecycle.Dial, lease time.Duration, logger *slog.Logger,
) *lifecycle.Worker {
	t.Helper()
	plugin, err := snapshots.New(snapshots.Options{
		Store: pluginStore, Dial: dial, Logger: logger,
		RetryBase: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("snapshot plugin: %v", err)
	}
	worker, err := lifecycle.New(lifecycle.Options{
		Store: store, Dial: dial, Logger: logger,
		Lease: lease, BootTimeout: 10 * time.Second,
		RetryBase: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		Handlers: plugin.Handlers(),
	})
	if err != nil {
		t.Fatalf("lifecycle worker: %v", err)
	}
	return worker
}

// snapshotFlowDrain runs runnable jobs until the queue is quiet. stop is
// checked before each claim; nil means run until no job is runnable.
func snapshotFlowDrain(t *testing.T, worker *lifecycle.Worker, stop func() bool) {
	t.Helper()
	for range 50 {
		if stop != nil && stop() {
			return
		}
		worked, err := worker.ProcessNext(t.Context())
		if err != nil {
			t.Fatalf("worker: %v", err)
		}
		if !worked {
			return
		}
	}
	t.Fatal("jobs did not drain")
}

// snapshotFlowRunUntil polls cond and processes runnable jobs until it holds.
// Unlike a plain drain, it keeps going across a retry backoff, bounded by a
// wall deadline, so a redelivered attempt is actually proven to finish.
func snapshotFlowRunUntil(t *testing.T, worker *lifecycle.Worker, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		worked, err := worker.ProcessNext(t.Context())
		if err != nil {
			t.Fatalf("worker while waiting for %s: %v", what, err)
		}
		if !worked {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func snapshotFlowSnapshot(t *testing.T, store *db.Store, owner, id uuid.UUID) db.SavedSnapshot {
	t.Helper()
	snap, err := store.SavedSnapshot(t.Context(), owner, id)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	return snap
}

func snapshotFlowJobCount(t *testing.T, store *db.Store, owner uuid.UUID, snapshotID uuid.UUID, kind db.JobKind) int {
	t.Helper()
	var count int
	if err := store.Pool().QueryRow(t.Context(), `
		SELECT count(*) FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = $3`,
		owner, snapshotID, kind).Scan(&count); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	return count
}

// snapshotFlowCaptureNode records every CreateVM request while delegating.
type snapshotFlowCaptureNode struct {
	*nodefake.Node
	mu      sync.Mutex
	creates []*nodev1.CreateVMRequest
}

func (n *snapshotFlowCaptureNode) CreateVM(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
	clone := &nodev1.CreateVMRequest{
		SandboxId: req.GetSandboxId(), Image: req.GetImage(),
		FromSnapshot: req.GetFromSnapshot(), Hostname: req.GetHostname(),
		Resources: &nodev1.Resources{
			Vcpus: req.GetResources().GetVcpus(), MemoryMib: req.GetResources().GetMemoryMib(),
			DiskGib: req.GetResources().GetDiskGib(),
		},
	}
	for key, value := range req.GetEnv() {
		if clone.Env == nil {
			clone.Env = map[string]string{}
		}
		clone.Env[key] = value
	}
	n.mu.Lock()
	n.creates = append(n.creates, clone)
	n.mu.Unlock()
	return n.Node.CreateVM(ctx, req)
}

func (n *snapshotFlowCaptureNode) captured() []*nodev1.CreateVMRequest {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]*nodev1.CreateVMRequest(nil), n.creates...)
}

// snapshotFlowGateNode blocks SaveSnapshot or DeleteVM until ctx ends so a
// test can reclaim the claim or kill the guard backend mid-RPC.
type snapshotFlowGateNode struct {
	*nodefake.Node
	blockSave   atomic.Bool
	blockDelete atomic.Bool
	saveEntered chan struct{}
	saveCancel  chan struct{}
	delEntered  chan struct{}
	delCancel   chan struct{}
	saveOnce    sync.Once
	delOnce     sync.Once
}

func snapshotFlowNewGateNode(t *testing.T, nodeID string) *snapshotFlowGateNode {
	t.Helper()
	return &snapshotFlowGateNode{
		Node:        nodefake.New(nodeID, t.TempDir()),
		saveEntered: make(chan struct{}, 1),
		saveCancel:  make(chan struct{}, 1),
		delEntered:  make(chan struct{}, 1),
		delCancel:   make(chan struct{}, 1),
	}
}

func (n *snapshotFlowGateNode) SaveSnapshot(ctx context.Context, req *nodev1.SaveSnapshotRequest) (*nodev1.Snapshot, error) {
	if !n.blockSave.Load() {
		return n.Node.SaveSnapshot(ctx, req)
	}
	select {
	case n.saveEntered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	n.saveOnce.Do(func() { close(n.saveCancel) })
	return nil, status.FromContextError(ctx.Err()).Err()
}

func (n *snapshotFlowGateNode) DeleteVM(ctx context.Context, req *nodev1.DeleteVMRequest) (*nodev1.DeleteVMResponse, error) {
	if !n.blockDelete.Load() {
		return n.Node.DeleteVM(ctx, req)
	}
	select {
	case n.delEntered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	n.delOnce.Do(func() { close(n.delCancel) })
	return nil, status.FromContextError(ctx.Err()).Err()
}

// snapshotFlowInfoNode rewrites the Info node id, so the handler must reject
// inventory that identifies another node.
type snapshotFlowInfoNode struct {
	*nodefake.Node
	nodeID string
}

func (n *snapshotFlowInfoNode) Info(ctx context.Context, req *nodev1.InfoRequest) (*nodev1.InfoResponse, error) {
	info, err := n.Node.Info(ctx, req)
	if err != nil {
		return nil, err
	}
	info.NodeId = n.nodeID
	return info, nil
}

// snapshotFlowDeleteNode records DeleteSnapshot calls.
type snapshotFlowDeleteNode struct {
	*nodefake.Node
	mu      sync.Mutex
	deleted []string
}

func (n *snapshotFlowDeleteNode) DeleteSnapshot(ctx context.Context, req *nodev1.DeleteSnapshotRequest) (*nodev1.DeleteSnapshotResponse, error) {
	n.mu.Lock()
	n.deleted = append(n.deleted, req.GetSnapshotId())
	n.mu.Unlock()
	return n.Node.DeleteSnapshot(ctx, req)
}

func (n *snapshotFlowDeleteNode) deletes() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.deleted...)
}

// snapshotFlowFailOnceDeleteNode fails the first DeleteSnapshot with an
// Unavailable status carrying a sentinel, then delegates and records every
// successful delete.
type snapshotFlowFailOnceDeleteNode struct {
	*nodefake.Node
	sentinel string
	failOnce atomic.Bool
	mu       sync.Mutex
	calls    int
	deleted  []string
}

func (n *snapshotFlowFailOnceDeleteNode) DeleteSnapshot(ctx context.Context,
	req *nodev1.DeleteSnapshotRequest,
) (*nodev1.DeleteSnapshotResponse, error) {
	n.mu.Lock()
	n.calls++
	n.mu.Unlock()
	if n.failOnce.CompareAndSwap(true, false) {
		return nil, status.Error(codes.Unavailable, n.sentinel)
	}
	n.mu.Lock()
	n.deleted = append(n.deleted, req.GetSnapshotId())
	n.mu.Unlock()
	return n.Node.DeleteSnapshot(ctx, req)
}

func (n *snapshotFlowFailOnceDeleteNode) deleteCalls() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.calls
}

// snapshotFlowFailOncePurgeStore fails the first purge record after the node
// already confirmed the artifact is gone, modelling a driver fault between
// the idempotent delete and the durable purge marker.
type snapshotFlowFailOncePurgeStore struct {
	*db.Store
	sentinel string
	failOnce atomic.Bool
}

func (s *snapshotFlowFailOncePurgeStore) MarkSnapshotPurged(ctx context.Context,
	owner uuid.UUID, snapshotID uuid.UUID, artifactID string,
) (db.SavedSnapshot, error) {
	if s.failOnce.CompareAndSwap(true, false) {
		return db.SavedSnapshot{}, errors.New(s.sentinel)
	}
	return s.Store.MarkSnapshotPurged(ctx, owner, snapshotID, artifactID)
}

// snapshotFlowTrackingDial counts gateway dials so a malformed job can prove
// it never reached the node.
type snapshotFlowTrackingDial struct {
	inner lifecycle.Dial
	dials atomic.Int64
}

func (d *snapshotFlowTrackingDial) Dial(nodeID string) (lifecycle.NodeClient, io.Closer, error) {
	d.dials.Add(1)
	return d.inner(nodeID)
}

// snapshotFlowReclaimStore fakes successful renewals for one save claim until
// the test commits a real same-row ClaimJob reclaim, then delegates renewals
// so the original token is rejected and its blocked RPC cancels.
type snapshotFlowReclaimStore struct {
	*db.Store
	mu            sync.Mutex
	target        uuid.UUID
	originalToken uuid.UUID
	claimed       chan struct{}
	reclaimed     atomic.Bool
}

func (s *snapshotFlowReclaimStore) ClaimJob(ctx context.Context, worker string, lease time.Duration) (db.Job, error) {
	job, err := s.Store.ClaimJob(ctx, worker, lease)
	if err != nil || job.Kind != db.JobSaveSnapshot {
		return job, err
	}
	s.mu.Lock()
	if s.target == uuid.Nil() {
		s.target = job.ID
		s.originalToken = job.LeaseToken
		close(s.claimed)
	}
	s.mu.Unlock()
	return job, nil
}

func (s *snapshotFlowReclaimStore) RenewJobLease(ctx context.Context, jobID uuid.UUID,
	worker string, token uuid.UUID, lease time.Duration,
) error {
	s.mu.Lock()
	target := s.target
	s.mu.Unlock()
	if jobID == target && !s.reclaimed.Load() {
		return nil // fault injection: the original claim looks alive
	}
	return s.Store.RenewJobLease(ctx, jobID, worker, token, lease)
}

func (s *snapshotFlowReclaimStore) claimedJob() (uuid.UUID, uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.target, s.originalToken
}

// snapshotFlowLimitStore records the page size every purgeable scan received.
type snapshotFlowLimitStore struct {
	*db.Store
	mu     sync.Mutex
	limits []int
}

func (s *snapshotFlowLimitStore) PurgeableSnapshots(ctx context.Context,
	after *db.SnapshotCleanupCursor, limit int,
) ([]db.SavedSnapshot, *db.SnapshotCleanupCursor, error) {
	s.mu.Lock()
	s.limits = append(s.limits, limit)
	s.mu.Unlock()
	return s.Store.PurgeableSnapshots(ctx, after, limit)
}

func (s *snapshotFlowLimitStore) recordedLimits() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.limits...)
}

// TestSnapshotFlowSaveRestoreAndDelete proves the core workflow: a save becomes
// ready, deleting the source sandbox leaves the artifact usable, a restore
// boots the recorded immutable artifact (not the name) with an empty image and
// only the request env, and the final pin release purges the artifact.
func TestSnapshotFlowSaveRestoreAndDelete(t *testing.T) {
	node := &snapshotFlowCaptureNode{Node: nodefake.New("node-a", t.TempDir())}
	env := snapshotFlowNewEnv(t, "node-a", node)
	small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
	const sourceSentinel = "SOURCE_ONLY_SENTINEL"
	source := env.readySource(t, "small", "source", small, map[string]string{
		sourceSentinel: "source-only-" + strings.Repeat("s", 48),
	})
	env.createVM(t, source.ID, small)

	save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: env.owner, SandboxID: source.ID, Name: "base",
	})
	if err != nil {
		t.Fatalf("accept save: %v", err)
	}
	worker := snapshotFlowWorker(t, env.store, env.dial, 2*time.Second)
	snapshotFlowDrain(t, worker, nil)
	ready := snapshotFlowSnapshot(t, env.store, env.owner, save.ID)
	if ready.Status != db.SnapshotReady || ready.SizeBytes == nil {
		t.Fatalf("saved row status=%s size_present=%v", ready.Status, ready.SizeBytes != nil)
	}
	artifact := ready.ArtifactID

	// Deleting the source sandbox must run the real DeleteVM job to done,
	// proving the artifact survives actual node cleanup.
	if _, _, err := env.store.AcceptSandboxDeletion(t.Context(), env.owner, source.ID, db.DeletionExplicit); err != nil {
		t.Fatalf("accept source deletion: %v", err)
	}
	snapshotFlowRunUntil(t, worker, "source DeleteVM done", func() bool {
		var state db.JobState
		if err := env.store.Pool().QueryRow(t.Context(), `
			SELECT state FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.delete'
			ORDER BY created_at DESC LIMIT 1`, source.ID).Scan(&state); err != nil {
			return false
		}
		return state == db.JobDone
	})
	if _, err := env.fake(t).GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: source.ID}); status.Code(err) != codes.NotFound {
		t.Fatalf("source VM still present after DeleteVM: %s", status.Code(err))
	}

	resolved, err := env.store.SnapshotRestoreSource(t.Context(), env.owner, "base")
	if err != nil {
		t.Fatalf("resolve after source deletion: %v", err)
	}
	if resolved.ArtifactID != artifact || resolved.NodeID != env.nodeID {
		t.Fatalf("resolved artifact=%q node=%q", resolved.ArtifactID, resolved.NodeID)
	}

	restored, err := env.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: env.owner, MachineType: "small", Name: "restored",
		Env: map[string]string{"TOKEN": "value"}, NodeID: env.nodeID, Reserve: small,
		Restore: &resolved,
	})
	if err != nil {
		t.Fatalf("create restore: %v", err)
	}
	// Delete the name after the pin exists: the artifact must stay usable.
	if _, err := env.store.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: env.owner, Name: "base",
	}); err != nil {
		t.Fatalf("accept snapshot delete: %v", err)
	}

	snapshotFlowRunUntil(t, worker, "restore ready and artifact purged", func() bool {
		return snapshotFlowSnapshot(t, env.store, env.owner, save.ID).PurgedAt != nil
	})

	creates := node.captured()
	if len(creates) == 0 {
		t.Fatal("provision never called CreateVM")
	}
	var restoreCreate *nodev1.CreateVMRequest
	for _, req := range creates {
		if req.GetSandboxId() == restored.Sandbox.ID {
			restoreCreate = req
		}
	}
	if restoreCreate == nil {
		t.Fatalf("no CreateVM for restore %s", restored.Sandbox.ID)
	}
	if restoreCreate.GetImage() != "" {
		t.Fatalf("restore CreateVM image = %q, want empty", restoreCreate.GetImage())
	}
	if restoreCreate.GetFromSnapshot() != artifact {
		t.Fatalf("restore CreateVM from_snapshot = %q, want %q", restoreCreate.GetFromSnapshot(), artifact)
	}
	_, hasRequestEnv := restoreCreate.GetEnv()["TOKEN"]
	_, hasSourceEnv := restoreCreate.GetEnv()[sourceSentinel]
	if len(restoreCreate.GetEnv()) != 1 || !hasRequestEnv || hasSourceEnv {
		t.Fatalf("restore CreateVM env count=%d request_key=%v source_key=%v",
			len(restoreCreate.GetEnv()), hasRequestEnv, hasSourceEnv)
	}

	booted, err := env.store.SandboxAny(t.Context(), env.owner, restored.Sandbox.ID)
	if err != nil {
		t.Fatalf("read restored: %v", err)
	}
	if booted.State != db.StateReady {
		t.Fatalf("restored state = %s", booted.State)
	}
	if booted.FromSnapshotID != artifact || booted.SnapshotPin {
		t.Fatalf("restored from=%q pin=%v, want %q and released", booted.FromSnapshotID, booted.SnapshotPin, artifact)
	}

	purged := snapshotFlowSnapshot(t, env.store, env.owner, save.ID)
	if purged.PurgedAt == nil {
		t.Fatalf("artifact not purged after the pin released: status=%s pins=%d", purged.Status, purged.Pins)
	}
	info, err := env.fake(t).Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatalf("node info: %v", err)
	}
	for _, snap := range info.GetSnapshots() {
		if snap.GetSnapshotId() == artifact {
			t.Fatalf("purged artifact %s still on the node", artifact)
		}
	}
}

// TestSnapshotFlowReplacementKeepsPriorReady proves a pending or failed
// replacement never disturbs the prior ready generation, a successful
// replacement retires it, and its cleanup waits for the restore pin.
func TestSnapshotFlowReplacementKeepsPriorReady(t *testing.T) {
	node := &snapshotFlowDeleteNode{Node: nodefake.New("node-a", t.TempDir())}
	env := snapshotFlowNewEnv(t, "node-a", node)
	small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
	source := env.readySource(t, "small", "source", small)
	env.createVM(t, source.ID, small)
	worker := snapshotFlowWorker(t, env.store, env.dial, 2*time.Second)

	first, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: env.owner, SandboxID: source.ID, Name: "base",
	})
	if err != nil {
		t.Fatalf("accept first save: %v", err)
	}
	snapshotFlowDrain(t, worker, nil)
	gen1 := snapshotFlowSnapshot(t, env.store, env.owner, first.ID)
	if gen1.Status != db.SnapshotReady {
		t.Fatalf("first generation = %s", gen1.Status)
	}

	// Pending replacement: the prior ready generation stays resolvable.
	second, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: env.owner, SandboxID: source.ID, Name: "base",
	})
	if err != nil {
		t.Fatalf("accept pending save: %v", err)
	}
	pending, err := env.store.SnapshotRestoreSource(t.Context(), env.owner, "base")
	if err != nil || pending.ArtifactID != gen1.ArtifactID {
		t.Fatalf("pending replacement resolved %q err=%v, want %q", pending.ArtifactID, err, gen1.ArtifactID)
	}

	// Fail the pending replacement at the node.
	if _, err := env.fake(t).DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: source.ID}); err != nil {
		t.Fatalf("delete source vm: %v", err)
	}
	snapshotFlowDrain(t, worker, nil)
	failed := snapshotFlowSnapshot(t, env.store, env.owner, second.ID)
	if failed.Status != db.SnapshotFailed {
		t.Fatalf("replacement status = %s, want failed", failed.Status)
	}
	afterFail, err := env.store.SnapshotRestoreSource(t.Context(), env.owner, "base")
	if err != nil || afterFail.ArtifactID != gen1.ArtifactID {
		t.Fatalf("failed replacement resolved %q err=%v, want %q", afterFail.ArtifactID, err, gen1.ArtifactID)
	}

	// Pin the prior generation, then complete a successful replacement.
	if _, err := env.fake(t).CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: source.ID,
		Resources: &nodev1.Resources{Vcpus: small.VCPUs, MemoryMib: small.MemoryMiB, DiskGib: small.DiskGiB},
	}); err != nil {
		t.Fatalf("recreate source vm: %v", err)
	}
	restore, err := env.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: env.owner, MachineType: "small", Name: "restored",
		NodeID: env.nodeID, Reserve: small, Restore: &afterFail,
	})
	if err != nil {
		t.Fatalf("create pinned restore: %v", err)
	}
	// Durable barrier: push the restore's provision job into the future so
	// only the replacement save runs, and the pin is still held when it
	// completes.
	if _, err := env.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET run_after = now() + interval '1 hour'
		WHERE sandbox_id = $1 AND kind = 'sandbox.provision'`, restore.Sandbox.ID); err != nil {
		t.Fatalf("hold restore provision: %v", err)
	}
	third, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: env.owner, SandboxID: source.ID, Name: "base",
	})
	if err != nil {
		t.Fatalf("accept successful replacement: %v", err)
	}
	snapshotFlowDrain(t, worker, nil)
	gen3 := snapshotFlowSnapshot(t, env.store, env.owner, third.ID)
	if gen3.Status != db.SnapshotReady || !gen3.IsCurrent {
		t.Fatalf("replacement = %s current=%v", gen3.Status, gen3.IsCurrent)
	}
	retired := snapshotFlowSnapshot(t, env.store, env.owner, gen1.ID)
	if retired.RetiredAt == nil {
		t.Fatal("successful replacement did not retire the older ready generation")
	}
	if retired.Pins != 1 {
		t.Fatalf("older generation pins = %d, want the held restore pin", retired.Pins)
	}
	if snapshotFlowJobCount(t, env.store, env.owner, gen1.ID, db.JobDeleteSnapshot) != 0 {
		t.Fatal("pin-held generation got a cleanup job")
	}
	for _, artifact := range node.deletes() {
		if artifact == gen1.ArtifactID {
			t.Fatal("pin-held artifact was deleted on the node")
		}
	}

	// The confirmed restore releases the pin, then cleanup purges the artifact.
	if _, err := env.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET run_after = now()
		WHERE sandbox_id = $1 AND kind = 'sandbox.provision'`, restore.Sandbox.ID); err != nil {
		t.Fatalf("release restore provision barrier: %v", err)
	}
	snapshotFlowRunUntil(t, worker, "restore ready and retired artifact purged", func() bool {
		got := snapshotFlowSnapshot(t, env.store, env.owner, gen1.ID)
		return got.Pins == 0 && got.PurgedAt != nil
	})
	released := snapshotFlowSnapshot(t, env.store, env.owner, gen1.ID)
	if released.Pins != 0 || released.PurgedAt == nil {
		t.Fatalf("after pin release pins=%d purged=%v", released.Pins, released.PurgedAt != nil)
	}
	deletedGen1 := false
	for _, artifact := range node.deletes() {
		if artifact == gen1.ArtifactID {
			deletedGen1 = true
		}
	}
	if !deletedGen1 {
		t.Fatalf("cleanup did not delete the retired artifact; deletes=%v", node.deletes())
	}
	kept, err := env.store.SavedSnapshot(t.Context(), env.owner, gen3.ID)
	if err != nil || kept.PurgedAt != nil {
		t.Fatalf("current generation was cleaned: purged=%v err=%v", kept.PurgedAt != nil, err)
	}
	_ = restore
}

// TestSnapshotFlowRestorePolicy proves the create-from machine policy: an
// omitted type inherits the snapshot type, an explicit larger type grows disk,
// and a smaller disk is rejected before any placement.
func TestSnapshotFlowRestorePolicy(t *testing.T) {
	env := snapshotFlowNewEnv(t, "node-a", nodefake.New("node-a", t.TempDir()))
	small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
	large := db.Resources{VCPUs: 8, MemoryMiB: 16 << 10, DiskGiB: 111}

	smallSource := env.readySource(t, "small", "small-source", small)
	largeSource := env.readySource(t, "large", "large-source", large)
	for _, tc := range []struct {
		source db.Sandbox
		name   string
	}{{smallSource, "small-base"}, {largeSource, "large-base"}} {
		snap, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
			Owner: env.owner, SandboxID: tc.source.ID, Name: tc.name,
		})
		if err != nil {
			t.Fatalf("accept %s: %v", tc.name, err)
		}
		if _, err := env.store.Pool().Exec(t.Context(), `
			UPDATE saved_snapshots SET status = 'ready', ready_at = now() WHERE id = $1`,
			snap.ID); err != nil {
			t.Fatalf("mark %s ready: %v", tc.name, err)
		}
	}

	inherited, apiErr := api.ResolveSnapshotRestore(t.Context(), env.store, env.owner, "small-base", false, "")
	if apiErr != nil {
		t.Fatalf("inherited type: %v", apiErr)
	}
	if inherited.MachineType != "small" || inherited.Resources != small {
		t.Fatalf("inherited target machine=%q resources=%+v", inherited.MachineType, inherited.Resources)
	}

	grown, apiErr := api.ResolveSnapshotRestore(t.Context(), env.store, env.owner, "small-base", true, "large")
	if apiErr != nil {
		t.Fatalf("explicit larger type: %v", apiErr)
	}
	wantLarge, ok := machinetype.Lookup("large")
	if !ok {
		t.Fatal("large machine type is not registered")
	}
	if grown.MachineType != "large" || grown.Resources.DiskGiB != wantLarge.DiskGiB ||
		grown.Resources.VCPUs != wantLarge.VCPUs || grown.Resources.MemoryMiB != wantLarge.MemoryMiB {
		t.Fatalf("grown target = %+v, want %+v", grown.Resources, wantLarge)
	}

	if _, apiErr := api.ResolveSnapshotRestore(t.Context(), env.store, env.owner, "large-base", true, "small"); apiErr == nil ||
		apiErr.Code != apierr.CodeInvalidJSON {
		t.Fatalf("shrink policy error = %v, want invalid_json", apiErr)
	}

	if _, apiErr := api.ResolveSnapshotRestore(t.Context(), env.store, env.owner, "missing", false, ""); apiErr == nil ||
		apiErr.Code != apierr.CodeSnapshotNotFound {
		t.Fatalf("missing policy error = %v, want snapshot_not_found", apiErr)
	}
}

// TestSnapshotFlowPinReleasePaths proves both confirmed outcomes release the
// pin exactly once, while cancellation alone releases nothing.
func TestSnapshotFlowPinReleasePaths(t *testing.T) {
	node := snapshotFlowNewGateNode(t, "node-a")
	env := snapshotFlowNewEnv(t, "node-a", node)
	small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
	source := env.readySource(t, "small", "source", small)
	env.createVM(t, source.ID, small)
	worker := snapshotFlowWorker(t, env.store, env.dial, 2*time.Second)

	save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: env.owner, SandboxID: source.ID, Name: "base",
	})
	if err != nil {
		t.Fatalf("accept save: %v", err)
	}
	snapshotFlowDrain(t, worker, nil)
	ready := snapshotFlowSnapshot(t, env.store, env.owner, save.ID)
	if ready.Status != db.SnapshotReady {
		t.Fatalf("save status = %s", ready.Status)
	}
	resolved, err := env.store.SnapshotRestoreSource(t.Context(), env.owner, "base")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	// Path A: a confirmed ready restore releases exactly once.
	normal, err := env.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: env.owner, MachineType: "small", Name: "normal",
		NodeID: env.nodeID, Reserve: small, Restore: &resolved,
	})
	if err != nil {
		t.Fatalf("create normal restore: %v", err)
	}
	snapshotFlowDrain(t, worker, nil)
	if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.Pins != 0 {
		t.Fatalf("normal restore left pins = %d", got.Pins)
	}
	released, next, err := env.store.ReleaseEligibleSnapshotPins(t.Context(), nil, 100)
	if err != nil || released != 0 || next != nil {
		t.Fatalf("second pin release: released=%d next=%v err=%v", released, next, err)
	}
	row, err := env.store.SandboxAny(t.Context(), env.owner, normal.Sandbox.ID)
	if err != nil || row.SnapshotPin {
		t.Fatalf("normal sandbox pin=%v err=%v", row.SnapshotPin, err)
	}

	// Path B: cancellation alone never releases; confirmed DeleteVM does.
	cancelled, err := env.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: env.owner, MachineType: "small", Name: "cancelled",
		NodeID: env.nodeID, Reserve: small, Restore: &resolved,
	})
	if err != nil {
		t.Fatalf("create cancelled restore: %v", err)
	}
	if _, _, err := env.store.AcceptSandboxDeletion(t.Context(), env.owner, cancelled.Sandbox.ID, db.DeletionExplicit); err != nil {
		t.Fatalf("accept cancelled deletion: %v", err)
	}
	// Provision sees the deleted row and returns without side effects.
	snapshotFlowDrain(t, worker, func() bool {
		var state db.JobState
		_ = env.store.Pool().QueryRow(t.Context(), `
			SELECT state FROM jobs WHERE sandbox_id = $1 AND kind = 'sandbox.provision'
			ORDER BY created_at DESC LIMIT 1`, cancelled.Sandbox.ID).Scan(&state)
		return state == db.JobDone || state == db.JobCancelled
	})
	if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.Pins != 1 {
		t.Fatalf("deletion acceptance alone released the pin: %d", got.Pins)
	}

	node.blockDelete.Store(true)
	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, err := worker.ProcessNext(runCtx)
		done <- err
	}()
	t.Cleanup(func() { cancelRun(); <-finished })
	select {
	case <-node.delEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("delete never reached the node")
	}
	cancelRun()
	select {
	case <-node.delCancel:
	case <-time.After(5 * time.Second):
		t.Fatal("delete RPC was not cancelled")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cancelled delete attempt: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("delete attempt did not return after cancellation")
	}
	if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.Pins != 1 {
		t.Fatalf("cancellation alone released the pin: %d", got.Pins)
	}

	node.blockDelete.Store(false)
	snapshotFlowRunUntil(t, worker, "confirmed DeleteVM releases the pin", func() bool {
		return snapshotFlowSnapshot(t, env.store, env.owner, save.ID).Pins == 0
	})
	if err := env.store.ReleaseSnapshotPin(t.Context(), env.owner, cancelled.Sandbox.ID); err != nil {
		t.Fatalf("second release: %v", err)
	}
	if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.Pins != 0 {
		t.Fatalf("second release changed pins to %d", got.Pins)
	}
}

// TestSnapshotFlowBlockedSaveCancellation proves a save blocked at the node is
// cancelled when the guard backend dies or the claim is reclaimed, never
// commits ready, and recovers on redelivery.
func TestSnapshotFlowBlockedSaveCancellation(t *testing.T) {
	t.Run("guard backend terminated", func(t *testing.T) {
		node := snapshotFlowNewGateNode(t, "node-a")
		env := snapshotFlowNewEnv(t, "node-a", node)
		small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
		source := env.readySource(t, "small", "source", small)
		env.createVM(t, source.ID, small)
		save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
			Owner: env.owner, SandboxID: source.ID, Name: "base",
		})
		if err != nil {
			t.Fatalf("accept save: %v", err)
		}
		worker := snapshotFlowWorker(t, env.store, env.dial, 2*time.Second)
		node.blockSave.Store(true)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			_, err := worker.ProcessNext(ctx)
			done <- err
		}()
		t.Cleanup(func() { cancel(); <-finished })
		select {
		case <-node.saveEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("save never reached the node")
		}

		app := env.store.Pool().Config().ConnConfig.RuntimeParams["application_name"] + "_guard"
		rows, err := env.store.Pool().Query(t.Context(), `
			SELECT pid FROM pg_stat_activity
			WHERE datname = current_database() AND usename = current_user
			  AND application_name = $1`, app)
		if err != nil {
			t.Fatalf("list guard backends: %v", err)
		}
		var pids []int32
		for rows.Next() {
			var pid int32
			if err := rows.Scan(&pid); err != nil {
				rows.Close()
				t.Fatalf("scan guard pid: %v", err)
			}
			pids = append(pids, pid)
		}
		rows.Close()
		if len(pids) != 1 {
			t.Fatalf("owned guard backends = %d, want exactly 1", len(pids))
		}
		var terminated bool
		if err := env.store.Pool().QueryRow(t.Context(),
			`SELECT pg_terminate_backend($1)`, pids[0]).Scan(&terminated); err != nil {
			t.Fatalf("terminate guard backend: %v", err)
		}
		if !terminated {
			t.Fatal("owned guard backend was not terminated")
		}
		select {
		case <-node.saveCancel:
		case <-time.After(5 * time.Second):
			t.Fatal("save RPC was not cancelled after guard loss")
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("guard-loss attempt: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("guard-loss attempt did not return")
		}
		if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.Status != db.SnapshotSaving {
			t.Fatalf("guard loss committed status %s", got.Status)
		}
		if got, err := env.store.SandboxAny(t.Context(), env.owner, source.ID); err != nil || got.Reserved != small {
			t.Fatalf("guard loss changed source reservation: vcpus=%d mem=%d disk=%d err=%v",
				got.Reserved.VCPUs, got.Reserved.MemoryMiB, got.Reserved.DiskGiB, err)
		}

		node.blockSave.Store(false)
		snapshotFlowRunUntil(t, worker, "redelivered save ready", func() bool {
			return snapshotFlowSnapshot(t, env.store, env.owner, save.ID).Status == db.SnapshotReady
		})
	})

	t.Run("claim reclaimed", func(t *testing.T) {
		node := snapshotFlowNewGateNode(t, "node-a")
		env := snapshotFlowNewEnv(t, "node-a", node)
		small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
		source := env.readySource(t, "small", "source", small)
		env.createVM(t, source.ID, small)
		save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
			Owner: env.owner, SandboxID: source.ID, Name: "base",
		})
		if err != nil {
			t.Fatalf("accept save: %v", err)
		}

		// The wrapper keeps the original claim's renewal looking healthy
		// until a real same-row ClaimJob commits a new token.
		wrapped := &snapshotFlowReclaimStore{Store: env.store, claimed: make(chan struct{})}
		worker := snapshotFlowWorkerWith(t, wrapped, wrapped, env.dial, 150*time.Millisecond)
		node.blockSave.Store(true)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			_, err := worker.ProcessNext(ctx)
			done <- err
		}()
		t.Cleanup(func() { cancel(); <-finished })

		select {
		case <-wrapped.claimed:
		case <-time.After(5 * time.Second):
			t.Fatal("worker never claimed the save job")
		}
		target, originalToken := wrapped.claimedJob()
		select {
		case <-node.saveEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("save never reached the node")
		}
		// Expire the exact claimed row on the DB clock, then let a real
		// ClaimJob reclaim it with a new token.
		if _, err := env.store.Pool().Exec(t.Context(), `
			UPDATE jobs SET leased_until = now() - interval '1 second' WHERE id = $1`,
			target); err != nil {
			t.Fatalf("expire original claim: %v", err)
		}
		reclaimed, err := env.store.ClaimJob(t.Context(), "reclaimer", 100*time.Millisecond)
		if err != nil {
			t.Fatalf("reclaim save job: %v", err)
		}
		if reclaimed.ID != target {
			t.Fatalf("reclaimer claimed a different job")
		}
		if reclaimed.LeaseToken == originalToken {
			t.Fatal("reclaim did not change the lease token")
		}
		wrapped.reclaimed.Store(true)

		select {
		case <-node.saveCancel:
		case <-time.After(5 * time.Second):
			t.Fatal("save RPC was not cancelled after reclaim")
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("reclaimed attempt: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("reclaimed attempt did not return")
		}
		if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.Status != db.SnapshotSaving {
			t.Fatalf("reclaim committed status %s", got.Status)
		}
		if got, err := env.store.SandboxAny(t.Context(), env.owner, source.ID); err != nil || got.Reserved != small {
			t.Fatalf("reclaim changed source reservation: vcpus=%d mem=%d disk=%d err=%v",
				got.Reserved.VCPUs, got.Reserved.MemoryMiB, got.Reserved.DiskGiB, err)
		}

		// Expire the reclaimer's row on the DB clock; the original worker's
		// next claim is the real redelivery.
		if _, err := env.store.Pool().Exec(t.Context(), `
			UPDATE jobs SET leased_until = now() - interval '1 second' WHERE id = $1`,
			target); err != nil {
			t.Fatalf("expire reclaimer claim: %v", err)
		}
		node.blockSave.Store(false)
		snapshotFlowRunUntil(t, worker, "redelivered save ready", func() bool {
			return snapshotFlowSnapshot(t, env.store, env.owner, save.ID).Status == db.SnapshotReady
		})
	})
}

// snapshotFlowReadSavePayload reads the save job payload of one generation.
func snapshotFlowReadSavePayload(t *testing.T, store *db.Store, owner uuid.UUID, snapshotID uuid.UUID) db.SaveSnapshotPayload {
	t.Helper()
	var raw string
	if err := store.Pool().QueryRow(t.Context(), `
		SELECT payload::text FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'
		ORDER BY created_at DESC LIMIT 1`, owner, snapshotID).Scan(&raw); err != nil {
		t.Fatalf("read save payload: %v", err)
	}
	var payload db.SaveSnapshotPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode save payload: %v", err)
	}
	return payload
}

func snapshotFlowWriteSavePayload(t *testing.T, store *db.Store, owner uuid.UUID, snapshotID uuid.UUID, payload db.SaveSnapshotPayload) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode save payload: %v", err)
	}
	if _, err := store.Pool().Exec(t.Context(), `
		UPDATE jobs SET payload = $3::jsonb
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'`,
		owner, snapshotID, raw); err != nil {
		t.Fatalf("write save payload: %v", err)
	}
}

// snapshotFlowReadDeletePayload reads the cleanup payload of one generation.
func snapshotFlowReadDeletePayload(t *testing.T, store *db.Store, owner uuid.UUID, snapshotID uuid.UUID) db.DeleteSnapshotPayload {
	t.Helper()
	var raw string
	if err := store.Pool().QueryRow(t.Context(), `
		SELECT payload::text FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.delete'
		ORDER BY created_at DESC LIMIT 1`, owner, snapshotID).Scan(&raw); err != nil {
		t.Fatalf("read delete payload: %v", err)
	}
	var payload db.DeleteSnapshotPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode delete payload: %v", err)
	}
	return payload
}

func snapshotFlowWriteDeletePayload(t *testing.T, store *db.Store, owner uuid.UUID, snapshotID uuid.UUID, payload db.DeleteSnapshotPayload) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode delete payload: %v", err)
	}
	if _, err := store.Pool().Exec(t.Context(), `
		UPDATE jobs SET payload = $3::jsonb
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.delete'`,
		owner, snapshotID, raw); err != nil {
		t.Fatalf("write delete payload: %v", err)
	}
}

// snapshotFlowJobState reports one snapshot job's state.
func snapshotFlowJobState(t *testing.T, store *db.Store, owner uuid.UUID, snapshotID uuid.UUID, kind db.JobKind) db.JobState {
	t.Helper()
	var state db.JobState
	if err := store.Pool().QueryRow(t.Context(), `
		SELECT state FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = $3
		ORDER BY created_at DESC LIMIT 1`, owner, snapshotID, kind).Scan(&state); err != nil {
		t.Fatalf("read job state: %v", err)
	}
	return state
}

// snapshotFlowLastError reports the fixed classification recorded on the
// latest snapshot job of kind.
func snapshotFlowLastError(t *testing.T, store *db.Store, owner uuid.UUID, snapshotID uuid.UUID, kind db.JobKind) string {
	t.Helper()
	var lastError string
	if err := store.Pool().QueryRow(t.Context(), `
		SELECT last_error FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = $3
		ORDER BY created_at DESC LIMIT 1`, owner, snapshotID, kind).Scan(&lastError); err != nil {
		t.Fatalf("read job last_error: %v", err)
	}
	return lastError
}

// snapshotFlowMalformedStore wraps the real store and substitutes a tampered
// row for one generation. The database CHECK cannot store such a row; this
// models a future/driver fault and proves the handler rejects it before any
// node RPC.
type snapshotFlowMalformedStore struct {
	*db.Store
	snapshotID uuid.UUID
	row        db.SavedSnapshot
	sandboxID  string
	sandbox    db.Sandbox
}

func (s *snapshotFlowMalformedStore) SavedSnapshot(ctx context.Context, owner, id uuid.UUID) (db.SavedSnapshot, error) {
	if id == s.snapshotID {
		return s.row, nil
	}
	return s.Store.SavedSnapshot(ctx, owner, id)
}

func (s *snapshotFlowMalformedStore) SandboxAny(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error) {
	if id == s.sandboxID {
		return s.sandbox, nil
	}
	return s.Store.SandboxAny(ctx, owner, id)
}

// TestSnapshotFlowSaveBindingMatrix proves malformed save bindings never reach
// the node, including a canonical-unsafe artifact pair that the row and
// payload agree on.
func TestSnapshotFlowSaveBindingMatrix(t *testing.T) {
	node := nodefake.New("node-a", t.TempDir())
	env := snapshotFlowNewEnv(t, "node-a", node)
	small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}

	prepare := func(t *testing.T) (db.Sandbox, db.SavedSnapshot) {
		t.Helper()
		source := env.readySource(t, "small", "source-"+uuid.NewV7().String(), small)
		env.createVM(t, source.ID, small)
		save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
			Owner: env.owner, SandboxID: source.ID, Name: "base-" + strings.ReplaceAll(uuid.NewV7().String(), "-", "")[:20],
		})
		if err != nil {
			t.Fatalf("accept save: %v", err)
		}
		return source, save
	}

	payloadMutations := []struct {
		name   string
		mutate func(*db.SaveSnapshotPayload)
	}{
		{"payload snapshot id", func(p *db.SaveSnapshotPayload) { p.SnapshotID = uuid.NewV7() }},
		{"payload vmid", func(p *db.SaveSnapshotPayload) { p.VMID = "bx_other123" }},
		{"payload node", func(p *db.SaveSnapshotPayload) { p.NodeID = "node-b" }},
		{"payload artifact", func(p *db.SaveSnapshotPayload) { p.ArtifactID = "not-a-snapshot" }},
		{"payload generation", func(p *db.SaveSnapshotPayload) { p.Generation++ }},
	}
	for _, tc := range payloadMutations {
		t.Run(tc.name, func(t *testing.T) {
			_, save := prepare(t)
			payload := snapshotFlowReadSavePayload(t, env.store, env.owner, save.ID)
			tc.mutate(&payload)
			snapshotFlowWriteSavePayload(t, env.store, env.owner, save.ID, payload)

			tracked := &snapshotFlowTrackingDial{inner: env.dial}
			worker := snapshotFlowWorker(t, env.store, tracked.Dial, 2*time.Second)
			if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
				t.Fatalf("malformed save: worked=%v err=%v", worked, err)
			}
			if got := tracked.dials.Load(); got != 0 {
				t.Fatalf("malformed save dialed the node %d times", got)
			}
			if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.Status != db.SnapshotSaving {
				t.Fatalf("malformed save changed status to %s", got.Status)
			}
		})
	}

	t.Run("job owner mismatch", func(t *testing.T) {
		_, save := prepare(t)
		other, err := env.store.EnsureAccount(t.Context(), "other-"+uuid.NewV7().String())
		if err != nil {
			t.Fatalf("other account: %v", err)
		}
		if _, err := env.store.Pool().Exec(t.Context(), `
			UPDATE jobs SET owner_id = $3
			WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'`,
			env.owner, save.ID, other.ID); err != nil {
			t.Fatalf("tamper job owner: %v", err)
		}
		tracked := &snapshotFlowTrackingDial{inner: env.dial}
		worker := snapshotFlowWorker(t, env.store, tracked.Dial, 2*time.Second)
		if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
			t.Fatalf("owner-mismatch save: worked=%v err=%v", worked, err)
		}
		if got := tracked.dials.Load(); got != 0 {
			t.Fatalf("owner-mismatch save dialed the node %d times", got)
		}
		if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.Status != db.SnapshotSaving {
			t.Fatalf("owner-mismatch save changed status to %s", got.Status)
		}
	})

	t.Run("job without sandbox", func(t *testing.T) {
		_, save := prepare(t)
		if _, err := env.store.Pool().Exec(t.Context(), `
			UPDATE jobs SET sandbox_id = NULL
			WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'`,
			env.owner, save.ID); err != nil {
			t.Fatalf("clear job sandbox: %v", err)
		}
		tracked := &snapshotFlowTrackingDial{inner: env.dial}
		worker := snapshotFlowWorker(t, env.store, tracked.Dial, 2*time.Second)
		if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
			t.Fatalf("missing-sandbox save: worked=%v err=%v", worked, err)
		}
		if got := tracked.dials.Load(); got != 0 {
			t.Fatalf("missing-sandbox save dialed the node %d times", got)
		}
	})

	t.Run("malformed payload", func(t *testing.T) {
		_, save := prepare(t)
		if _, err := env.store.Pool().Exec(t.Context(), `
			UPDATE jobs SET payload = '"not-an-object"'::jsonb
			WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'`,
			env.owner, save.ID); err != nil {
			t.Fatalf("tamper payload type: %v", err)
		}
		tracked := &snapshotFlowTrackingDial{inner: env.dial}
		worker := snapshotFlowWorker(t, env.store, tracked.Dial, 2*time.Second)
		if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
			t.Fatalf("malformed-payload save: worked=%v err=%v", worked, err)
		}
		if got := tracked.dials.Load(); got != 0 {
			t.Fatalf("malformed-payload save dialed the node %d times", got)
		}
	})

	t.Run("row bindings and unsafe pair", func(t *testing.T) {
		source, save := prepare(t)
		payload := snapshotFlowReadSavePayload(t, env.store, env.owner, save.ID)
		row := snapshotFlowSnapshot(t, env.store, env.owner, save.ID)
		// The payload and row agree on a non-canonical artifact id: the
		// shape check must still stop the RPC.
		payload.ArtifactID = "not-a-snapshot"
		row.ArtifactID = payload.ArtifactID
		snapshotFlowWriteSavePayload(t, env.store, env.owner, save.ID, payload)
		tracked := &snapshotFlowTrackingDial{inner: env.dial}
		wrapped := &snapshotFlowMalformedStore{Store: env.store, snapshotID: save.ID, row: row}
		worker := snapshotFlowWorkerWith(t, wrapped, wrapped, tracked.Dial, 2*time.Second)
		if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
			t.Fatalf("unsafe pair: worked=%v err=%v", worked, err)
		}
		if got := tracked.dials.Load(); got != 0 {
			t.Fatalf("unsafe pair dialed the node %d times", got)
		}

		// A mismatched source row is rejected too, on a fresh job because
		// the unsafe-pair attempt already failed terminally.
		source2, save2 := prepare(t)
		badSandbox := source2
		badSandbox.NodeID = new("node-b")
		wrapped.snapshotID = save2.ID
		wrapped.row = snapshotFlowSnapshot(t, env.store, env.owner, save2.ID)
		wrapped.sandboxID = source2.ID
		wrapped.sandbox = badSandbox
		if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
			t.Fatalf("source mismatch: worked=%v err=%v", worked, err)
		}
		if got := tracked.dials.Load(); got != 0 {
			t.Fatalf("source mismatch dialed the node %d times", got)
		}
		_ = payload
		_ = source
	})
}

// TestSnapshotFlowDeleteBindingMatrix proves malformed cleanup bindings never
// delete an artifact and never mark it purged.
func TestSnapshotFlowDeleteBindingMatrix(t *testing.T) {
	node := nodefake.New("node-a", t.TempDir())
	env := snapshotFlowNewEnv(t, "node-a", node)
	small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}

	prepare := func(t *testing.T) (db.Sandbox, db.SavedSnapshot) {
		t.Helper()
		source := env.readySource(t, "small", "source-"+uuid.NewV7().String(), small)
		name := "base-" + strings.ReplaceAll(uuid.NewV7().String(), "-", "")[:20]
		save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
			Owner: env.owner, SandboxID: source.ID, Name: name,
		})
		if err != nil {
			t.Fatalf("accept save: %v", err)
		}
		if _, err := env.store.Pool().Exec(t.Context(), `
			UPDATE saved_snapshots SET status = 'ready', ready_at = now() WHERE id = $1`, save.ID); err != nil {
			t.Fatalf("mark ready: %v", err)
		}
		if _, err := env.store.Pool().Exec(t.Context(), `
			UPDATE jobs SET state = 'done'
			WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'`,
			env.owner, save.ID); err != nil {
			t.Fatalf("finish save job: %v", err)
		}
		if _, err := env.store.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
			Owner: env.owner, Name: name,
		}); err != nil {
			t.Fatalf("accept delete: %v", err)
		}
		return source, save
	}

	t.Run("payload node", func(t *testing.T) {
		_, save := prepare(t)
		payload := snapshotFlowReadDeletePayload(t, env.store, env.owner, save.ID)
		payload.NodeID = "node-b"
		snapshotFlowWriteDeletePayload(t, env.store, env.owner, save.ID, payload)
		tracked := &snapshotFlowTrackingDial{inner: env.dial}
		worker := snapshotFlowWorker(t, env.store, tracked.Dial, 2*time.Second)
		if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
			t.Fatalf("node-mismatch delete: worked=%v err=%v", worked, err)
		}
		if got := tracked.dials.Load(); got != 0 {
			t.Fatalf("node-mismatch delete dialed %d times", got)
		}
		if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.PurgedAt != nil {
			t.Fatal("node-mismatch delete purged the artifact")
		}
	})

	t.Run("payload artifact", func(t *testing.T) {
		_, save := prepare(t)
		payload := snapshotFlowReadDeletePayload(t, env.store, env.owner, save.ID)
		payload.ArtifactID = "not-a-snapshot"
		snapshotFlowWriteDeletePayload(t, env.store, env.owner, save.ID, payload)
		tracked := &snapshotFlowTrackingDial{inner: env.dial}
		worker := snapshotFlowWorker(t, env.store, tracked.Dial, 2*time.Second)
		if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
			t.Fatalf("artifact-mismatch delete: worked=%v err=%v", worked, err)
		}
		if got := tracked.dials.Load(); got != 0 {
			t.Fatalf("artifact-mismatch delete dialed %d times", got)
		}
		if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.PurgedAt != nil {
			t.Fatal("artifact-mismatch delete purged the artifact")
		}
	})

	t.Run("job owner", func(t *testing.T) {
		_, save := prepare(t)
		other, err := env.store.EnsureAccount(t.Context(), "other-"+uuid.NewV7().String())
		if err != nil {
			t.Fatalf("other account: %v", err)
		}
		if _, err := env.store.Pool().Exec(t.Context(), `
			UPDATE jobs SET owner_id = $3
			WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.delete'`,
			env.owner, save.ID, other.ID); err != nil {
			t.Fatalf("tamper delete owner: %v", err)
		}
		tracked := &snapshotFlowTrackingDial{inner: env.dial}
		worker := snapshotFlowWorker(t, env.store, tracked.Dial, 2*time.Second)
		if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
			t.Fatalf("owner-mismatch delete: worked=%v err=%v", worked, err)
		}
		if got := tracked.dials.Load(); got != 0 {
			t.Fatalf("owner-mismatch delete dialed %d times", got)
		}
		if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.PurgedAt != nil {
			t.Fatal("owner-mismatch delete purged the artifact")
		}
	})

	t.Run("row bindings", func(t *testing.T) {
		_, save := prepare(t)
		row := snapshotFlowSnapshot(t, env.store, env.owner, save.ID)
		row.NodeID = new("node-b")
		tracked := &snapshotFlowTrackingDial{inner: env.dial}
		wrapped := &snapshotFlowMalformedStore{Store: env.store, snapshotID: save.ID, row: row}
		worker := snapshotFlowWorkerWith(t, wrapped, wrapped, tracked.Dial, 2*time.Second)
		if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
			t.Fatalf("row-mismatch delete: worked=%v err=%v", worked, err)
		}
		if got := tracked.dials.Load(); got != 0 {
			t.Fatalf("row-mismatch delete dialed %d times", got)
		}
		if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.PurgedAt != nil {
			t.Fatal("row-mismatch delete purged the artifact")
		}
	})
}

// snapshotFlowReadyToDelete saves one source generation ready and accepts the
// name deletion, returning the ready generation whose cleanup job is queued.
func snapshotFlowReadyToDelete(t *testing.T, env *snapshotFlowEnv,
	worker *lifecycle.Worker, small db.Resources,
) db.SavedSnapshot {
	t.Helper()
	source := env.readySource(t, "small", "source", small)
	env.createVM(t, source.ID, small)
	save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: env.owner, SandboxID: source.ID, Name: "base",
	})
	if err != nil {
		t.Fatalf("accept save: %v", err)
	}
	snapshotFlowRunUntil(t, worker, "save ready", func() bool {
		return snapshotFlowSnapshot(t, env.store, env.owner, save.ID).Status == db.SnapshotReady
	})
	ready := snapshotFlowSnapshot(t, env.store, env.owner, save.ID)
	if _, err := env.store.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: env.owner, Name: "base",
	}); err != nil {
		t.Fatalf("accept delete: %v", err)
	}
	return ready
}

// snapshotFlowHasArtifact reports whether the node inventory still lists id.
func snapshotFlowHasArtifact(t *testing.T, node *nodefake.Node, id string) bool {
	t.Helper()
	info, err := node.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatalf("node info: %v", err)
	}
	for _, snap := range info.GetSnapshots() {
		if snap.GetSnapshotId() == id {
			return true
		}
	}
	return false
}

// TestSnapshotFlowDeleteSnapshotFailureRetries proves a live node
// DeleteSnapshot Unavailable is retried, never purged early, keeps the
// artifact on the node and records only the fixed classification.
func TestSnapshotFlowDeleteSnapshotFailureRetries(t *testing.T) {
	const sentinel = "SENTINEL-delete-node-unavailable"
	node := &snapshotFlowFailOnceDeleteNode{Node: nodefake.New("node-a", t.TempDir()), sentinel: sentinel}
	node.failOnce.Store(true)
	env := snapshotFlowNewEnv(t, "node-a", node)
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	worker := snapshotFlowWorkerWithLogger(t, env.store, env.store, env.dial, 2*time.Second, logger)
	ready := snapshotFlowReadyToDelete(t, env, worker, db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51})

	if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
		t.Fatalf("first delete attempt: worked=%v err=%v", worked, err)
	}
	if got := snapshotFlowSnapshot(t, env.store, env.owner, ready.ID); got.PurgedAt != nil {
		t.Fatal("failed DeleteSnapshot purged the artifact")
	}
	if state := snapshotFlowJobState(t, env.store, env.owner, ready.ID, db.JobDeleteSnapshot); state != db.JobQueued {
		t.Fatalf("failed delete job state = %s, want queued", state)
	}
	lastError := snapshotFlowLastError(t, env.store, env.owner, ready.ID, db.JobDeleteSnapshot)
	if strings.Contains(lastError, sentinel) {
		t.Fatal("job last_error echoed the node error")
	}
	if strings.Contains(logs.String(), sentinel) {
		t.Fatal("log echoed the node error")
	}
	if lastError != "node unavailable" {
		t.Fatalf("job last_error = %q, want the fixed node classification", lastError)
	}
	if !snapshotFlowHasArtifact(t, node.Node, ready.ArtifactID) {
		t.Fatal("failed DeleteSnapshot removed the artifact")
	}

	snapshotFlowRunUntil(t, worker, "redelivered delete purged", func() bool {
		return snapshotFlowSnapshot(t, env.store, env.owner, ready.ID).PurgedAt != nil
	})
	if state := snapshotFlowJobState(t, env.store, env.owner, ready.ID, db.JobDeleteSnapshot); state != db.JobDone {
		t.Fatalf("redelivered delete job state = %s, want done", state)
	}
	if node.deleteCalls() != 2 {
		t.Fatalf("DeleteSnapshot calls = %d, want the failed attempt plus one retry", node.deleteCalls())
	}
	if snapshotFlowHasArtifact(t, node.Node, ready.ArtifactID) {
		t.Fatal("purged artifact is still on the node")
	}
}

// TestSnapshotFlowPurgeRecordFailureRetries proves a purge-record fault after
// a confirmed node delete is retried, the idempotent DeleteSnapshot runs again
// and the durable purge finally commits.
func TestSnapshotFlowPurgeRecordFailureRetries(t *testing.T) {
	const sentinel = "SENTINEL-mark-purged-failure"
	node := &snapshotFlowDeleteNode{Node: nodefake.New("node-a", t.TempDir())}
	env := snapshotFlowNewEnv(t, "node-a", node)
	wrapped := &snapshotFlowFailOncePurgeStore{Store: env.store, sentinel: sentinel}
	wrapped.failOnce.Store(true)
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	worker := snapshotFlowWorkerWithLogger(t, wrapped, wrapped, env.dial, 2*time.Second, logger)
	ready := snapshotFlowReadyToDelete(t, env, worker, db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51})

	if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
		t.Fatalf("first purge attempt: worked=%v err=%v", worked, err)
	}
	if got := snapshotFlowSnapshot(t, env.store, env.owner, ready.ID); got.PurgedAt != nil {
		t.Fatal("failed purge record marked the artifact purged")
	}
	if state := snapshotFlowJobState(t, env.store, env.owner, ready.ID, db.JobDeleteSnapshot); state != db.JobQueued {
		t.Fatalf("failed purge job state = %s, want queued", state)
	}
	lastError := snapshotFlowLastError(t, env.store, env.owner, ready.ID, db.JobDeleteSnapshot)
	if strings.Contains(lastError, sentinel) {
		t.Fatal("job last_error echoed the store error")
	}
	if strings.Contains(logs.String(), sentinel) {
		t.Fatal("log echoed the store error")
	}
	if lastError != "snapshot purge record failed" {
		t.Fatalf("job last_error = %q, want the fixed purge classification", lastError)
	}
	if snapshotFlowHasArtifact(t, node.Node, ready.ArtifactID) {
		t.Fatal("confirmed node delete did not remove the artifact")
	}
	if got := len(node.deletes()); got != 1 {
		t.Fatalf("DeleteSnapshot calls = %d, want 1 before the retry", got)
	}

	snapshotFlowRunUntil(t, worker, "purge record retried and committed", func() bool {
		return snapshotFlowSnapshot(t, env.store, env.owner, ready.ID).PurgedAt != nil
	})
	if state := snapshotFlowJobState(t, env.store, env.owner, ready.ID, db.JobDeleteSnapshot); state != db.JobDone {
		t.Fatalf("retried purge job state = %s, want done", state)
	}
	if got := len(node.deletes()); got != 2 {
		t.Fatalf("DeleteSnapshot calls = %d, want the idempotent retry", got)
	}
}

// snapshotFlowCountingCloser counts closes without panicking on nil.
type snapshotFlowCountingCloser struct{ closes atomic.Int64 }

func (c *snapshotFlowCountingCloser) Close() error { c.closes.Add(1); return nil }

// TestSnapshotFlowNilDialSafety proves a nil client, a closer returned with an
// error and a nil closer never panic inside the real handler and always close
// a non-nil closer once.
func TestSnapshotFlowNilDialSafety(t *testing.T) {
	small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
	tests := []struct {
		name      string
		dial      func(*testing.T, *snapshotFlowEnv, *snapshotFlowCountingCloser) lifecycle.Dial
		wantClose int64
		wantState db.SnapshotStatus
		wantJob   db.JobState
	}{
		{
			name: "nil client closes closer",
			dial: func(_ *testing.T, _ *snapshotFlowEnv, closer *snapshotFlowCountingCloser) lifecycle.Dial {
				return func(string) (lifecycle.NodeClient, io.Closer, error) { return nil, closer, nil }
			},
			wantClose: 1, wantState: db.SnapshotSaving, wantJob: db.JobQueued,
		},
		{
			name: "error closes nonnil closer",
			dial: func(_ *testing.T, _ *snapshotFlowEnv, closer *snapshotFlowCountingCloser) lifecycle.Dial {
				return func(string) (lifecycle.NodeClient, io.Closer, error) {
					return nil, closer, errors.New("dial failed")
				}
			},
			wantClose: 1, wantState: db.SnapshotSaving, wantJob: db.JobQueued,
		},
		{
			name: "real client with nil closer succeeds",
			dial: func(t *testing.T, env *snapshotFlowEnv, _ *snapshotFlowCountingCloser) lifecycle.Dial {
				client, err := env.factory.Dial(env.nodeID)
				if err != nil {
					t.Fatalf("gateway client: %v", err)
				}
				t.Cleanup(func() { _ = client.Close() })
				return func(string) (lifecycle.NodeClient, io.Closer, error) { return client, nil, nil }
			},
			wantClose: 0, wantState: db.SnapshotReady, wantJob: db.JobDone,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A fresh env per case keeps retry queues isolated, so a later
			// case cannot be satisfied by an earlier case's queued job.
			env := snapshotFlowNewEnv(t, "node-a", nodefake.New("node-a", t.TempDir()))
			source := env.readySource(t, "small", "source", small)
			if tc.wantState == db.SnapshotReady {
				env.createVM(t, source.ID, small)
			}
			save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
				Owner: env.owner, SandboxID: source.ID, Name: "base",
			})
			if err != nil {
				t.Fatalf("accept save: %v", err)
			}
			closer := &snapshotFlowCountingCloser{}
			worker := snapshotFlowWorker(t, env.store, tc.dial(t, env, closer), 2*time.Second)
			if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
				t.Fatalf("nil-dial save: worked=%v err=%v", worked, err)
			}
			if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.Status != tc.wantState {
				t.Fatalf("nil-dial save status = %s, want %s", got.Status, tc.wantState)
			}
			if got := snapshotFlowJobState(t, env.store, env.owner, save.ID, db.JobSaveSnapshot); got != tc.wantJob {
				t.Fatalf("nil-dial job state = %s, want %s", got, tc.wantJob)
			}
			if got := closer.closes.Load(); got != tc.wantClose {
				t.Fatalf("closer closes = %d, want %d", got, tc.wantClose)
			}
		})
	}
}

// TestSnapshotFlowAlreadyExistsMatrix proves a known artifact+source retry
// succeeds, an unexplained collision fails safely, and mismatched Info does
// not become success.
func TestSnapshotFlowAlreadyExistsMatrix(t *testing.T) {
	t.Run("known artifact retries", func(t *testing.T) {
		node := &snapshotFlowInfoNode{Node: nodefake.New("node-a", t.TempDir()), nodeID: "node-a"}
		env := snapshotFlowNewEnv(t, "node-a", node)
		small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
		source := env.readySource(t, "small", "source", small)
		env.createVM(t, source.ID, small)
		artifact, err := db.NewSnapshotArtifactID()
		if err != nil {
			t.Fatalf("artifact id: %v", err)
		}
		if _, err := node.Node.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{
			SandboxId: source.ID, SnapshotId: artifact,
		}); err != nil {
			t.Fatalf("seed artifact: %v", err)
		}
		save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
			Owner: env.owner, SandboxID: source.ID, Name: "base", ArtifactID: artifact,
		})
		if err != nil {
			t.Fatalf("accept save: %v", err)
		}
		worker := snapshotFlowWorker(t, env.store, env.dial, 2*time.Second)
		snapshotFlowDrain(t, worker, nil)
		if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.Status != db.SnapshotReady {
			t.Fatalf("idempotent retry status = %s", got.Status)
		}
	})

	t.Run("unknown source collision fails", func(t *testing.T) {
		node := &snapshotFlowInfoNode{Node: nodefake.New("node-a", t.TempDir()), nodeID: "node-a"}
		env := snapshotFlowNewEnv(t, "node-a", node)
		small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
		source := env.readySource(t, "small", "source", small)
		other := env.readySource(t, "small", "other", small)
		env.createVM(t, source.ID, small)
		env.createVM(t, other.ID, small)
		artifact, err := db.NewSnapshotArtifactID()
		if err != nil {
			t.Fatalf("artifact id: %v", err)
		}
		if _, err := node.Node.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{
			SandboxId: other.ID, SnapshotId: artifact,
		}); err != nil {
			t.Fatalf("seed foreign artifact: %v", err)
		}
		save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
			Owner: env.owner, SandboxID: source.ID, Name: "base", ArtifactID: artifact,
		})
		if err != nil {
			t.Fatalf("accept save: %v", err)
		}
		worker := snapshotFlowWorker(t, env.store, env.dial, 2*time.Second)
		snapshotFlowDrain(t, worker, nil)
		if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.Status != db.SnapshotFailed {
			t.Fatalf("collision status = %s, want failed", got.Status)
		}
	})

	t.Run("info from another node is not success", func(t *testing.T) {
		node := &snapshotFlowInfoNode{Node: nodefake.New("node-a", t.TempDir()), nodeID: "node-b"}
		env := snapshotFlowNewEnv(t, "node-a", node)
		small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
		source := env.readySource(t, "small", "source", small)
		env.createVM(t, source.ID, small)
		artifact, err := db.NewSnapshotArtifactID()
		if err != nil {
			t.Fatalf("artifact id: %v", err)
		}
		if _, err := node.Node.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{
			SandboxId: source.ID, SnapshotId: artifact,
		}); err != nil {
			t.Fatalf("seed artifact: %v", err)
		}
		save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
			Owner: env.owner, SandboxID: source.ID, Name: "base", ArtifactID: artifact,
		})
		if err != nil {
			t.Fatalf("accept save: %v", err)
		}
		worker := snapshotFlowWorker(t, env.store, env.dial, 2*time.Second)
		snapshotFlowDrain(t, worker, nil)
		if got := snapshotFlowSnapshot(t, env.store, env.owner, save.ID); got.Status != db.SnapshotSaving {
			t.Fatalf("wrong-node Info status = %s, want saving", got.Status)
		}
	})
}

// TestSnapshotFlowReconcileLateSave proves a purged artifact that reappears in
// the node inventory is reopened, cleaned and durably purged again, while
// unknown artifacts are preserved.
func TestSnapshotFlowReconcileLateSave(t *testing.T) {
	node := nodefake.New("node-a", t.TempDir())
	env := snapshotFlowNewEnv(t, "node-a", node)
	small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
	source := env.readySource(t, "small", "source", small)
	env.createVM(t, source.ID, small)
	worker := snapshotFlowWorker(t, env.store, env.dial, 2*time.Second)

	save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: env.owner, SandboxID: source.ID, Name: "base",
	})
	if err != nil {
		t.Fatalf("accept save: %v", err)
	}
	snapshotFlowDrain(t, worker, nil)
	ready := snapshotFlowSnapshot(t, env.store, env.owner, save.ID)
	if ready.Status != db.SnapshotReady {
		t.Fatalf("save status = %s", ready.Status)
	}
	if _, err := env.store.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: env.owner, Name: "base",
	}); err != nil {
		t.Fatalf("accept delete: %v", err)
	}
	snapshotFlowDrain(t, worker, nil)
	purged := snapshotFlowSnapshot(t, env.store, env.owner, save.ID)
	if purged.PurgedAt == nil {
		t.Fatal("artifact was not purged before the late save")
	}

	// A late SaveSnapshot recreates the artifact on the node without any
	// server-side fence; only repeated inventory reconciliation converges.
	if _, err := node.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{
		SandboxId: source.ID, SnapshotId: ready.ArtifactID,
	}); err != nil {
		t.Fatalf("reappear artifact: %v", err)
	}
	reconciler, err := snapshots.NewReconciler(snapshots.ReconcilerOptions{
		Store: env.store, Dial: env.dial, Logger: snapshotFlowQuiet(), PageSize: 10,
	})
	if err != nil {
		t.Fatalf("reconciler: %v", err)
	}
	if err := reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatalf("reconcile late save: %v", err)
	}
	reopened := snapshotFlowSnapshot(t, env.store, env.owner, save.ID)
	if reopened.PurgedAt != nil {
		t.Fatal("reappeared artifact was not reopened")
	}
	if snapshotFlowJobCount(t, env.store, env.owner, save.ID, db.JobDeleteSnapshot) == 0 {
		t.Fatal("reappeared artifact got no cleanup job")
	}
	snapshotFlowDrain(t, worker, nil)
	again := snapshotFlowSnapshot(t, env.store, env.owner, save.ID)
	if again.PurgedAt == nil {
		t.Fatal("reappeared artifact was not purged again")
	}
	info, err := node.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatalf("node info: %v", err)
	}
	for _, snap := range info.GetSnapshots() {
		if snap.GetSnapshotId() == ready.ArtifactID {
			t.Fatalf("artifact %s still on the node", ready.ArtifactID)
		}
	}

	// An unknown but well-formed artifact is preserved, never deleted.
	unknown, err := db.NewSnapshotArtifactID()
	if err != nil {
		t.Fatalf("unknown artifact id: %v", err)
	}
	if _, err := node.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{
		SandboxId: source.ID, SnapshotId: unknown,
	}); err != nil {
		t.Fatalf("seed unknown artifact: %v", err)
	}
	if err := reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatalf("reconcile unknown artifact: %v", err)
	}
	info, err = node.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatalf("node info: %v", err)
	}
	found := false
	for _, snap := range info.GetSnapshots() {
		if snap.GetSnapshotId() == unknown {
			found = true
		}
	}
	if !found {
		t.Fatal("unknown artifact was deleted")
	}
}

// TestSnapshotFlowReconcileSaving proves missing/terminal save jobs fail their
// generation while queued/running saves are never failed, and small pages
// still make progress.
func TestSnapshotFlowReconcileSaving(t *testing.T) {
	env := snapshotFlowNewEnv(t, "node-a", nodefake.New("node-a", t.TempDir()))
	small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
	source := env.readySource(t, "small", "source", small)

	names := []string{"missing", "terminal", "queued", "running"}
	saves := make([]db.SavedSnapshot, len(names))
	for i, name := range names {
		save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
			Owner: env.owner, SandboxID: source.ID, Name: name,
		})
		if err != nil {
			t.Fatalf("accept %s: %v", name, err)
		}
		saves[i] = save
	}
	// missing: no save job at all.
	if _, err := env.store.Pool().Exec(t.Context(), `
		DELETE FROM jobs WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'`,
		env.owner, saves[0].ID); err != nil {
		t.Fatalf("delete save job: %v", err)
	}
	// terminal: a failed save job.
	if _, err := env.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET state = 'failed'
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'`,
		env.owner, saves[1].ID); err != nil {
		t.Fatalf("fail save job: %v", err)
	}
	// queued: reconciliation must leave an active queued save alone.
	if _, err := env.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET state = 'queued', run_after = now() + interval '1 hour'
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'`,
		env.owner, saves[2].ID); err != nil {
		t.Fatalf("queue save job: %v", err)
	}
	// running: reconciliation must leave an active running save alone.
	if _, err := env.store.Pool().Exec(t.Context(), `
		UPDATE jobs SET state = 'running', leased_by = 'worker', lease_token = $3,
			leased_until = now() + interval '1 hour'
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'`,
		env.owner, saves[3].ID, uuid.NewV7()); err != nil {
		t.Fatalf("run save job: %v", err)
	}

	reconciler, err := snapshots.NewReconciler(snapshots.ReconcilerOptions{
		Store: env.store, Dial: env.dial, Logger: snapshotFlowQuiet(), PageSize: 1,
	})
	if err != nil {
		t.Fatalf("reconciler: %v", err)
	}
	if err := reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatalf("reconcile saving: %v", err)
	}
	want := []db.SnapshotStatus{
		db.SnapshotFailed, db.SnapshotFailed, db.SnapshotSaving, db.SnapshotSaving,
	}
	for i, status := range want {
		if got := snapshotFlowSnapshot(t, env.store, env.owner, saves[i].ID); got.Status != status {
			t.Fatalf("%s status = %s, want %s", names[i], got.Status, status)
		}
	}
}

// TestSnapshotFlowReconcilePaging proves cleanup paging advances past a first
// page blocked by an active job even with a page size of one.
func TestSnapshotFlowReconcilePaging(t *testing.T) {
	env := snapshotFlowNewEnv(t, "node-a", nodefake.New("node-a", t.TempDir()))
	small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
	source := env.readySource(t, "small", "source", small)

	var saves []db.SavedSnapshot
	for _, name := range []string{"first", "second"} {
		save, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
			Owner: env.owner, SandboxID: source.ID, Name: name,
		})
		if err != nil {
			t.Fatalf("accept %s: %v", name, err)
		}
		if _, err := env.store.Pool().Exec(t.Context(), `
			UPDATE saved_snapshots SET status = 'ready', ready_at = now(),
				retired_at = now(), is_current = false WHERE id = $1`, save.ID); err != nil {
			t.Fatalf("retire %s: %v", name, err)
		}
		if _, err := env.store.Pool().Exec(t.Context(), `
			UPDATE jobs SET state = 'done'
			WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'`,
			env.owner, save.ID); err != nil {
			t.Fatalf("finish save job: %v", err)
		}
		saves = append(saves, save)
	}
	// Block the first page with an already active cleanup job.
	if _, err := env.store.EnqueueSnapshotDelete(t.Context(), env.owner, saves[0].ID); err != nil {
		t.Fatalf("block first cleanup: %v", err)
	}

	limited := &snapshotFlowLimitStore{Store: env.store}
	reconciler, err := snapshots.NewReconciler(snapshots.ReconcilerOptions{
		Store: limited, Dial: env.dial, Logger: snapshotFlowQuiet(), PageSize: 1,
	})
	if err != nil {
		t.Fatalf("reconciler: %v", err)
	}
	if err := reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatalf("reconcile paging: %v", err)
	}
	limits := limited.recordedLimits()
	if len(limits) < 2 {
		t.Fatalf("purgeable scans = %d, want multiple pages", len(limits))
	}
	for _, limit := range limits {
		if limit != 1 {
			t.Fatalf("purgeable scan limit = %d, want the configured 1", limit)
		}
	}
	if got := snapshotFlowJobCount(t, env.store, env.owner, saves[0].ID, db.JobDeleteSnapshot); got != 1 {
		t.Fatalf("blocked first page cleanup jobs = %d, want 1", got)
	}
	if got := snapshotFlowJobCount(t, env.store, env.owner, saves[1].ID, db.JobDeleteSnapshot); got == 0 {
		t.Fatal("second page cleanup was starved")
	}
}

// TestSnapshotFlowReconcileInventoryErrors proves a wrong-node inventory and
// an invalid artifact id are incomplete rounds with fixed logs that never
// echo the node-supplied value.
func TestSnapshotFlowReconcileInventoryErrors(t *testing.T) {
	t.Run("wrong node info", func(t *testing.T) {
		node := &snapshotFlowInfoNode{Node: nodefake.New("node-a", t.TempDir()), nodeID: "node-b"}
		env := snapshotFlowNewEnv(t, "node-a", node)
		var logs strings.Builder
		reconciler, err := snapshots.NewReconciler(snapshots.ReconcilerOptions{
			Store: env.store, Dial: env.dial,
			Logger: slog.New(slog.NewTextHandler(&logs, nil)), PageSize: 10,
		})
		if err != nil {
			t.Fatalf("reconciler: %v", err)
		}
		if err := reconciler.ReconcileOnce(t.Context()); err == nil {
			t.Fatal("wrong-node inventory reconciled as complete")
		}
		if !strings.Contains(logs.String(), "snapshot node inventory failed") {
			t.Fatal("missing fixed inventory log classification")
		}
	})

	t.Run("invalid artifact id", func(t *testing.T) {
		node := nodefake.New("node-a", t.TempDir())
		env := snapshotFlowNewEnv(t, "node-a", node)
		small := db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51}
		source := env.readySource(t, "small", "source", small)
		env.createVM(t, source.ID, small)
		const unsafe = "not-a-snapshot"
		if _, err := node.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{
			SandboxId: source.ID, SnapshotId: unsafe,
		}); err != nil {
			t.Fatalf("seed invalid artifact: %v", err)
		}
		var logs strings.Builder
		reconciler, err := snapshots.NewReconciler(snapshots.ReconcilerOptions{
			Store: env.store, Dial: env.dial,
			Logger: slog.New(slog.NewTextHandler(&logs, nil)), PageSize: 10,
		})
		if err != nil {
			t.Fatalf("reconciler: %v", err)
		}
		if err := reconciler.ReconcileOnce(t.Context()); err == nil {
			t.Fatal("invalid artifact id reconciled as complete")
		}
		if strings.Contains(logs.String(), unsafe) {
			t.Fatal("log echoed the node-supplied artifact id")
		}
		if !strings.Contains(logs.String(), "invalid artifact id") {
			t.Fatal("missing fixed invalid-id log classification")
		}
	})
}

// TestSnapshotFlowReconcileNodeErrorClassification proves a raw node error is
// classified, never echoed.
func TestSnapshotFlowReconcileNodeErrorClassification(t *testing.T) {
	const sentinel = "SENTINEL-raw-node-error"
	node := &snapshotFlowFailingInfoNode{
		Node: nodefake.New("node-a", t.TempDir()),
		err:  status.Error(codes.Unavailable, sentinel),
	}
	env := snapshotFlowNewEnv(t, "node-a", node)
	var logs strings.Builder
	reconciler, err := snapshots.NewReconciler(snapshots.ReconcilerOptions{
		Store: env.store, Dial: env.dial,
		Logger: slog.New(slog.NewTextHandler(&logs, nil)), PageSize: 10,
	})
	if err != nil {
		t.Fatalf("reconciler: %v", err)
	}
	if err := reconciler.ReconcileOnce(t.Context()); err == nil {
		t.Fatal("node Info failure reconciled as complete")
	}
	if strings.Contains(logs.String(), sentinel) {
		t.Fatal("log echoed the raw node error")
	}
	if !strings.Contains(logs.String(), "node unavailable") {
		t.Fatal("missing fixed node classification")
	}
}

// snapshotFlowFailingInfoNode always fails Info with a fixed status error.
type snapshotFlowFailingInfoNode struct {
	*nodefake.Node
	err error
}

func (n *snapshotFlowFailingInfoNode) Info(context.Context, *nodev1.InfoRequest) (*nodev1.InfoResponse, error) {
	return nil, n.err
}
