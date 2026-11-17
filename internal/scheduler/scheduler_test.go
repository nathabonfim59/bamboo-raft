package scheduler_test

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
	"testing"
	"time"

	"google.golang.org/grpc"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

const schedInternalToken = "scheduler-internal-token"

func schedQuiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// schedEnv wires a real PostgreSQL store, a node hub, the internal gateway and
// fake nodes that dial in through nodelink, so placement tests exercise the
// exact gateway client the server role will use.
type schedEnv struct {
	store   *db.Store
	hub     *nodelink.Hub
	factory *nodegateway.ClientFactory
	addr    string
	stops   map[string]func()
}

func newSchedEnv(t *testing.T) *schedEnv {
	t.Helper()
	store := dbtest.Open(t)
	// Infrastructure lifetime: the hub/gateway stay up for the whole test
	// and are explicitly canceled/joined by cleanup; t.Context would be
	// canceled together with the cleanup that joins them.
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
	}, schedQuiet())
	hubDone := make(chan struct{})
	go func() { _ = hub.Serve(ctx, hubLn); close(hubDone) }()
	t.Cleanup(func() { cancel(); <-hubDone })

	gw, err := nodegateway.New(hub, schedInternalToken)
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

	return &schedEnv{
		store:   store,
		hub:     hub,
		factory: nodegateway.NewClientFactory(gwLn.Addr().String(), schedInternalToken, nil),
		addr:    hubLn.Addr().String(),
		stops:   map[string]func(){},
	}
}

// startNode connects one fake node through nodelink.
func (e *schedEnv) startNode(t *testing.T, id string) *nodefake.Node {
	t.Helper()
	fake := nodefake.New(id, t.TempDir())
	fake.VCPUs = 64
	fake.MemoryMiB = 256 << 10
	srv := grpc.NewServer(nodelink.ServerOptions()...)
	fake.Register(srv)
	// Node link lifetime: started here and joined by cleanup.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = nodelink.Serve(ctx, nodelink.DialConfig{
			Addr: e.addr, NodeID: id, Token: "node-token", Log: schedQuiet(),
			MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		}, srv)
		close(done)
	}()
	stop := func() { cancel(); <-done }
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
	return fake
}

func (e *schedEnv) stopNode(t *testing.T, id string) {
	t.Helper()
	if stop, ok := e.stops[id]; ok {
		stop()
		delete(e.stops, id)
	}
}

func (e *schedEnv) registerNode(t *testing.T, id string, total db.Resources) {
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

func (e *schedEnv) owner(t *testing.T) uuid.UUID {
	t.Helper()
	a, err := e.store.EnsureAccount(t.Context(), "acct-"+uuid.NewV7().String())
	if err != nil {
		t.Fatal(err)
	}
	return a.ID
}

func (e *schedEnv) scheduler(opts scheduler.Options) *scheduler.Scheduler {
	if opts.Store == nil {
		opts.Store = e.store
	}
	opts.Log = schedQuiet()
	opts.Dial = func(nodeID string) (scheduler.NodeClient, io.Closer, error) {
		c, err := e.factory.Dial(nodeID)
		if err != nil {
			return nil, nil, err
		}
		return c, c, nil
	}
	if opts.FreshWithin == 0 {
		opts.FreshWithin = time.Minute
	}
	s, err := scheduler.New(opts)
	if err != nil {
		panic(err)
	}
	return s
}

func schedReserve() db.Resources { return db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 10} }

func schedAdmitRequest(id string, owner uuid.UUID) scheduler.AdmitRequest {
	return scheduler.AdmitRequest{
		ID: id, Owner: owner, MachineType: "small", Name: "test",
		Reserve: schedReserve(), MaxAttempts: 2,
	}
}

// delayingStore simulates slow placement between admission and insert.
type delayingStore struct {
	*db.Store
	delay time.Duration
}

func (d *delayingStore) NodesWithCapacity(ctx context.Context, need db.Resources, fresh time.Duration) ([]db.Node, error) {
	select {
	case <-time.After(d.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return d.Store.NodesWithCapacity(ctx, need, fresh)
}

func TestAdmitTTLSurvivesPlacementDelay(t *testing.T) {
	e := newSchedEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	e.startNode(t, "node-a")
	own := e.owner(t)
	ttl := 60
	req := schedAdmitRequest(ids.Sandbox(), own)
	req.TTLSeconds = &ttl
	const delay = 300 * time.Millisecond
	start := time.Now()
	res, err := e.scheduler(scheduler.Options{Store: &delayingStore{Store: e.store, delay: delay}}).Admit(t.Context(), req)
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if elapsed := time.Since(start); elapsed < delay {
		t.Fatalf("placement delay not exercised: %s", elapsed)
	}
	if res.Sandbox.ExpiresAt == nil {
		t.Fatal("TTL create has no expiry")
	}
	if got := res.Sandbox.ExpiresAt.Sub(res.Sandbox.CreatedAt); got != time.Duration(ttl)*time.Second {
		t.Fatalf("expires-created = %s, want %ds", got, ttl)
	}
	// A wall-clock deadline captured before placement would expire less
	// than ttl from now; the duration source must not.
	if res.Sandbox.ExpiresAt.Sub(start) < time.Duration(ttl)*time.Second+delay/2 {
		t.Fatalf("expiry %s was computed from pre-placement wall clock", res.Sandbox.ExpiresAt.Sub(start))
	}
}

func TestAdmitRejectsInvalidTTLBeforePlacement(t *testing.T) {
	e := newSchedEnv(t)
	own := e.owner(t)
	ttl := 60
	deadline := time.Now().Add(time.Hour)
	both := schedAdmitRequest(ids.Sandbox(), own)
	both.TTLSeconds = &ttl
	both.ExpiresAt = &deadline
	if _, err := e.scheduler(scheduler.Options{}).Admit(t.Context(), both); !errors.Is(err, db.ErrInvalidTTL) {
		t.Fatalf("both TTL sources: %v", err)
	}
	bad := schedAdmitRequest(ids.Sandbox(), own)
	zero := 0
	bad.TTLSeconds = &zero
	if _, err := e.scheduler(scheduler.Options{}).Admit(t.Context(), bad); !errors.Is(err, db.ErrInvalidTTL) {
		t.Fatalf("zero TTL: %v", err)
	}
}

func TestAdmitPlacesOnConnectedFreshNode(t *testing.T) {
	e := newSchedEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	e.startNode(t, "node-a")
	e.registerNode(t, "node-b", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	// node-b has inventory but no live link.
	own := e.owner(t)
	secret := "super-secret-env"
	req := schedAdmitRequest(ids.Sandbox(), own)
	req.Env = map[string]string{"TOKEN": secret}

	res, err := e.scheduler(scheduler.Options{}).Admit(t.Context(), req)
	if err != nil {
		t.Fatal("admit failed")
	}
	if strings.Contains(res.Job.Payload, secret) {
		t.Fatal("plaintext env leaked into the queued job")
	}
	if res.Sandbox.NodeID == nil || *res.Sandbox.NodeID != "node-a" {
		t.Fatal("placed on an unexpected node")
	}
	if res.Sandbox.State != db.StateProvisioning || res.Sandbox.Reserved != schedReserve() {
		t.Fatal("created sandbox row has unexpected state or reservation")
	}
	if res.Job.Kind != db.JobProvision || res.Job.State != db.JobQueued {
		t.Fatal("provision job has unexpected kind or state")
	}
	var p db.ProvisionPayload
	if err := json.Unmarshal([]byte(res.Job.Payload), &p); err != nil {
		t.Fatal("job payload did not decode")
	}
	if p.NodeID != "node-a" || p.Resources != schedReserve() {
		t.Fatal("job payload does not match placement")
	}
	n, err := e.store.Node(t.Context(), "node-a")
	if err != nil || n.Reserved != schedReserve() {
		t.Fatal("node reservation does not match the create")
	}
	events, err := e.store.SandboxEvents(t.Context(), own, res.Sandbox.ID, 10)
	if err != nil {
		t.Fatal("sandbox events read failed")
	}
	for _, ev := range events {
		if strings.Contains(ev.Data, secret) {
			t.Fatal("plaintext env leaked into an event")
		}
	}
	if len(events) != 1 || events[0].Type != "state.provisioning" {
		t.Fatal("sandbox events do not match the provisioning create")
	}
}

func TestAdmitReplaysBeforeCapacityAndConnectivity(t *testing.T) {
	e := newSchedEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	e.startNode(t, "node-a")
	own := e.owner(t)
	key := "idem-1"
	hash := sha256.Sum256([]byte(`{"type":"small"}`))
	s := e.scheduler(scheduler.Options{})

	first := schedAdmitRequest(ids.Sandbox(), own)
	first.IdempotencyKey = key
	first.RequestSHA256 = hash[:]
	res, err := s.Admit(t.Context(), first)
	if err != nil {
		t.Fatalf("first admit: %v", err)
	}

	// Remove every placement possibility. The replay must still answer.
	e.stopNode(t, "node-a")
	second := schedAdmitRequest(ids.Sandbox(), own)
	second.IdempotencyKey = key
	second.RequestSHA256 = hash[:]
	replayed, err := s.Admit(t.Context(), second)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replayed.Replayed || replayed.Sandbox.ID != res.Sandbox.ID {
		t.Fatalf("replay returned %+v, want %s", replayed.Sandbox, res.Sandbox.ID)
	}

	// A different request under the same key is rejected before placement.
	changed := second
	other := sha256.Sum256([]byte(`{"type":"large"}`))
	changed.RequestSHA256 = other[:]
	if _, err := s.Admit(t.Context(), changed); !errors.Is(err, db.ErrIdempotencyReused) {
		t.Fatalf("reused key: %v", err)
	}
	if n := schedCount(t, e.store, `SELECT count(*) FROM sandboxes`); n != 1 {
		t.Fatalf("sandboxes after replays: %d", n)
	}
}

func TestAdmitNoCapacity(t *testing.T) {
	e := newSchedEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 5})
	e.startNode(t, "node-a")
	if _, err := e.scheduler(scheduler.Options{}).Admit(t.Context(), schedAdmitRequest(ids.Sandbox(), e.owner(t))); !errors.Is(err, scheduler.ErrNoCapacity) {
		t.Fatalf("full node: %v", err)
	}
}

func TestAdmitNoConnectedNode(t *testing.T) {
	e := newSchedEnv(t)
	// Inventory exists but nothing is dialed into the hub.
	e.registerNode(t, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	if _, err := e.scheduler(scheduler.Options{}).Admit(t.Context(), schedAdmitRequest(ids.Sandbox(), e.owner(t))); !errors.Is(err, scheduler.ErrNoNode) {
		t.Fatalf("disconnected node: %v", err)
	}
}

func TestAdmitFailFastIsBounded(t *testing.T) {
	e := newSchedEnv(t)
	e.registerNode(t, "node-hung", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	startHungNode(t, e, "node-hung")
	req := schedAdmitRequest(ids.Sandbox(), e.owner(t))
	req.FailFast = true
	start := time.Now()
	_, err := e.scheduler(scheduler.Options{FailFastBudget: 200 * time.Millisecond}).Admit(t.Context(), req)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("hung node admitted")
	}
	if elapsed > time.Second {
		t.Fatalf("failFast took %s, want about 200ms", elapsed)
	}
	if !errors.Is(err, scheduler.ErrNoNode) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("failFast error: %v", err)
	}
}

func TestAdmitConcurrentSameKeyCreatesOneSandbox(t *testing.T) {
	e := newSchedEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 32, MemoryMiB: 64 << 10, DiskGiB: 500})
	e.startNode(t, "node-a")
	own := e.owner(t)
	key := "race-key"
	hash := sha256.Sum256([]byte("race"))
	s := e.scheduler(scheduler.Options{})

	const callers = 8
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		idsSeen = map[string]bool{}
	)
	for range callers {
		wg.Go(func() {
			req := schedAdmitRequest(ids.Sandbox(), own)
			req.IdempotencyKey = key
			req.RequestSHA256 = hash[:]
			res, err := s.Admit(t.Context(), req)
			if err != nil {
				t.Errorf("concurrent admit: %v", err)
				return
			}
			mu.Lock()
			idsSeen[res.Sandbox.ID] = true
			mu.Unlock()
		})
	}
	wg.Wait()
	if len(idsSeen) != 1 {
		t.Fatalf("distinct sandbox ids: %v", idsSeen)
	}
	if n := schedCount(t, e.store, `SELECT count(*) FROM sandboxes`); n != 1 {
		t.Fatalf("sandbox rows: %d", n)
	}
}

// startHungNode serves just enough Node to accept the link, never answering
// Info until the probe context expires.
func startHungNode(t *testing.T, e *schedEnv, id string) {
	t.Helper()
	srv := grpc.NewServer(nodelink.ServerOptions()...)
	nodev1.RegisterNodeServer(srv, &hungNode{})
	// Node link lifetime: started here and joined by cleanup.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = nodelink.Serve(ctx, nodelink.DialConfig{
			Addr: e.addr, NodeID: id, Token: "node-token", Log: schedQuiet(),
			MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		}, srv)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })
	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()
	if _, err := e.hub.Wait(waitCtx, id); err != nil {
		t.Fatalf("hung node did not connect: %v", err)
	}
}

type hungNode struct {
	nodev1.UnimplementedNodeServer
}

func (h *hungNode) Info(ctx context.Context, _ *nodev1.InfoRequest) (*nodev1.InfoResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func schedCount(t *testing.T, s *db.Store, query string) int64 {
	t.Helper()
	var n int64
	if err := s.Pool().QueryRow(t.Context(), query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// schedReadySource creates a ready sandbox on nodeID and finishes its
// provision job so it can be saved.
func schedReadySource(t *testing.T, s *db.Store, owner uuid.UUID, nodeID string) db.Sandbox {
	t.Helper()
	res, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: owner, MachineType: "default", Name: "source",
		NodeID: nodeID, Reserve: db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 51},
	})
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE sandboxes SET state = 'ready' WHERE id = $1`, res.Sandbox.ID); err != nil {
		t.Fatalf("mark source ready: %v", err)
	}
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE jobs SET state = 'done' WHERE sandbox_id = $1 AND kind = 'sandbox.provision'`,
		res.Sandbox.ID); err != nil {
		t.Fatalf("finish source job: %v", err)
	}
	sbx, err := s.Sandbox(t.Context(), owner, res.Sandbox.ID)
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	return sbx
}

// schedSavedSource inserts one ready generation recorded on nodeID.
func schedSavedSource(t *testing.T, s *db.Store, owner uuid.UUID, source db.Sandbox, nodeID string) db.SnapshotSource {
	t.Helper()
	rowID := uuid.NewV7()
	artifact := "snap-" + strings.Repeat("a", 32)
	if _, err := s.Pool().Exec(t.Context(), `
		INSERT INTO saved_snapshots
			(id, owner_id, name, sandbox_id, status, artifact_id, generation,
			 machine_type, disk_gib, node_id, is_current, created_at, updated_at, ready_at)
		VALUES ($1, $2, 'base', $3, 'ready', $4, 1, 'default', 51, $5, true, now(), now(), now())`,
		rowID, owner, source.ID, artifact, nodeID); err != nil {
		t.Fatalf("insert saved source: %v", err)
	}
	src, err := s.SnapshotRestoreSource(t.Context(), owner, "base")
	if err != nil {
		t.Fatalf("resolve saved source: %v", err)
	}
	return src
}

func TestAdmitRestoreUsesOnlyArtifactNode(t *testing.T) {
	env := newSchedEnv(t)
	env.startNode(t, "node-a")
	env.startNode(t, "node-b")
	env.registerNode(t, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 256 << 10, DiskGiB: 1000})
	env.registerNode(t, "node-b", db.Resources{VCPUs: 64, MemoryMiB: 256 << 10, DiskGiB: 1000})
	owner := env.owner(t)
	source := schedReadySource(t, env.store, owner, "node-b")
	src := schedSavedSource(t, env.store, owner, source, "node-b")

	req := schedAdmitRequest(ids.Sandbox(), owner)
	req.MachineType = "default"
	req.Reserve = db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 51}
	req.Restore = &src
	result, err := env.scheduler(scheduler.Options{}).Admit(t.Context(), req)
	if err != nil {
		t.Fatalf("restore admit: %v", err)
	}
	if result.Sandbox.NodeID == nil || *result.Sandbox.NodeID != "node-b" {
		t.Fatalf("restore node = %v, want node-b", result.Sandbox.NodeID)
	}
	if result.Sandbox.FromSnapshotID != src.ArtifactID || !result.Sandbox.SnapshotPin {
		t.Fatalf("restore row from_match=%v pin=%v", result.Sandbox.FromSnapshotID == src.ArtifactID, result.Sandbox.SnapshotPin)
	}
	// The artifact row now carries the restore pin.
	var pins int
	if err := env.store.Pool().QueryRow(t.Context(),
		`SELECT pins FROM saved_snapshots WHERE id = $1`, src.RowID).Scan(&pins); err != nil {
		t.Fatalf("read pins: %v", err)
	}
	if pins != 1 {
		t.Fatalf("pins = %d, want 1", pins)
	}
}

func TestAdmitRestoreSourceNodeOfflineDoesNotFallBack(t *testing.T) {
	env := newSchedEnv(t)
	env.startNode(t, "node-a")
	env.registerNode(t, "node-a", db.Resources{VCPUs: 64, MemoryMiB: 256 << 10, DiskGiB: 1000})
	// node-b has durable inventory but no nodelink connection: the artifact
	// row's FK needs the node row, and the probe must fail as offline.
	env.registerNode(t, "node-b", db.Resources{VCPUs: 64, MemoryMiB: 256 << 10, DiskGiB: 1000})
	owner := env.owner(t)
	// Source rows are durable even when their node is offline: insert the
	// sandbox/save directly instead of booting node-b.
	res, err := env.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: owner, MachineType: "default", Name: "source",
		NodeID: "node-a", Reserve: db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 51},
	})
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	if _, err := env.store.Pool().Exec(t.Context(), `
		INSERT INTO saved_snapshots
			(id, owner_id, name, sandbox_id, status, artifact_id, generation,
			 machine_type, disk_gib, node_id, is_current, created_at, updated_at, ready_at)
		VALUES ($1, $2, 'base', $3, 'ready', $4, 1, 'default', 51, 'node-b', true, now(), now(), now())`,
		uuid.NewV7(), owner, res.Sandbox.ID, "snap-"+strings.Repeat("b", 32)); err != nil {
		t.Fatalf("insert saved source: %v", err)
	}
	src, err := env.store.SnapshotRestoreSource(t.Context(), owner, "base")
	if err != nil {
		t.Fatalf("resolve saved source: %v", err)
	}
	req := schedAdmitRequest(ids.Sandbox(), owner)
	req.MachineType = "default"
	req.Reserve = db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 51}
	req.Restore = &src
	if _, err := env.scheduler(scheduler.Options{}).Admit(t.Context(), req); !errors.Is(err, scheduler.ErrNoNode) {
		t.Fatalf("offline source node error = %v, want ErrNoNode", err)
	}
	var count int
	if err := env.store.Pool().QueryRow(t.Context(),
		`SELECT count(*) FROM sandboxes WHERE id = $1`, req.ID).Scan(&count); err != nil {
		t.Fatalf("count created: %v", err)
	}
	if count != 0 {
		t.Fatalf("restore fell back to another node: %d rows", count)
	}
}
