package scheduler_test

// Multi-node placement tests: ranking, filters, the failFast budget
// with concurrent probes, creates from a generation and batch admission. They
// need PostgreSQL and skip unless BBRAFT_TEST_DATABASE_URL is set
// (dbtest.Open).

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// placeHangingNode is a fake whose Info blocks until the caller gives up.
type placeHangingNode struct {
	*nodefake.Node
}

func (n *placeHangingNode) Info(ctx context.Context, _ *nodev1.InfoRequest) (*nodev1.InfoResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// placeSlowNode is a fake whose Info answers after delay.
type placeSlowNode struct {
	*nodefake.Node
	delay time.Duration
}

func (n *placeSlowNode) Info(ctx context.Context, req *nodev1.InfoRequest) (*nodev1.InfoResponse, error) {
	select {
	case <-time.After(n.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return n.Node.Info(ctx, req)
}

// placeStartServer connects any node implementation through nodelink.
func placeStartServer(t *testing.T, e *schedEnv, id string, impl nodev1.NodeServer) {
	t.Helper()
	srv := grpc.NewServer(nodelink.ServerOptions()...)
	nodev1.RegisterNodeServer(srv, impl)
	// Node link lifetime: joined by t.Cleanup, so not the test context.
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
		t.Fatalf("node %s did not connect: %v", id, err)
	}
}

// placeNodes registers and connects fake nodes with the given inventory.
func placeNodes(t *testing.T, e *schedEnv, totals map[string]db.Resources) {
	t.Helper()
	for _, id := range slices.Sorted(maps.Keys(totals)) {
		e.registerNode(t, id, totals[id])
		e.startNode(t, id)
	}
}

func placeIDs(cands []scheduler.Candidate) []string {
	out := make([]string, len(cands))
	for i, c := range cands {
		out[i] = c.Node.ID
	}
	return out
}

// placeGeneration writes a completed generation of a sandbox row on nodeID
// (no VM needed: only the rows matter to admission).
func placeGeneration(t *testing.T, e *schedEnv, owner uuid.UUID, nodeID string, diskBytes uint64) db.Generation {
	t.Helper()
	src, err := e.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: owner, MachineType: "small", Name: "source", NodeID: nodeID,
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("create source sandbox: %v", err)
	}
	// Admission reads only the completed row; the source's own provision job
	// stands in as the attempt's owner.
	gen, err := e.store.BeginGeneration(t.Context(), db.BeginGenerationParams{
		Owner: owner, SandboxID: src.Sandbox.ID, Timeout: time.Minute,
		JobID: src.Job.ID, LeaseToken: uuid.NewV7(),
	})
	if err != nil {
		t.Fatalf("begin generation: %v", err)
	}
	done, err := e.store.CompleteGeneration(t.Context(), gen.ID, db.GenerationResult{
		Image: nodefake.Image, DiskBytes: diskBytes, Chunks: 1, ChunksUploaded: 1,
		BytesUploaded: 1, ContentBytes: 1, CutAt: time.Now(),
	}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("complete generation: %v", err)
	}
	return done
}

func placeSource(g db.Generation, prefer ...string) *scheduler.GenerationSource {
	return &scheduler.GenerationSource{
		GenerationID: g.ID, ManifestKey: g.ManifestKey, Image: g.Image,
		DiskBytes: g.DiskBytes, Prefer: prefer,
	}
}

// TestPlaceRanking ranks preferred nodes first in their order, then by free
// memory, then free vCPUs, then id.
func TestPlaceRanking(t *testing.T) {
	e := newSchedEnv(t)
	placeNodes(t, e, map[string]db.Resources{
		"node-a": {VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100},
		"node-b": {VCPUs: 8, MemoryMiB: 16384, DiskGiB: 100},
		"node-c": {VCPUs: 16, MemoryMiB: 16384, DiskGiB: 100},
		"node-d": {VCPUs: 8, MemoryMiB: 16384, DiskGiB: 100},
	})
	s := e.scheduler(scheduler.Options{})
	for _, tc := range []struct {
		name   string
		prefer []string
		want   []string
	}{
		{"free capacity", nil, []string{"node-c", "node-b", "node-d", "node-a"}},
		{"one preferred", []string{"node-a"}, []string{"node-a", "node-c", "node-b", "node-d"}},
		{"preference order", []string{"node-d", "node-a"}, []string{"node-d", "node-a", "node-c", "node-b"}},
		{"unknown preference", []string{"node-zz"}, []string{"node-c", "node-b", "node-d", "node-a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cands, err := s.Place(t.Context(), scheduler.PlaceRequest{Reserve: schedReserve(), Prefer: tc.prefer})
			if err != nil {
				t.Fatalf("place: %v", err)
			}
			if got := placeIDs(cands); !slices.Equal(got, tc.want) {
				t.Fatalf("ranking = %v, want %v", got, tc.want)
			}
		})
	}

	// Reservations count: filling node-c's memory drops it below the others.
	if err := e.store.ReserveCapacity(t.Context(), "node-c", db.Resources{MemoryMiB: 12288}); err != nil {
		t.Fatal(err)
	}
	cands, err := s.Place(t.Context(), scheduler.PlaceRequest{Reserve: schedReserve()})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := placeIDs(cands), []string{"node-b", "node-d", "node-a", "node-c"}; !slices.Equal(got, want) {
		t.Fatalf("ranking after reservation = %v, want %v", got, want)
	}
}

// TestPlaceFilters covers the image, exclude and object-store filters and
// skips stale and disconnected nodes.
func TestPlaceFilters(t *testing.T) {
	e := newSchedEnv(t)
	total := db.Resources{VCPUs: 8, MemoryMiB: 16384, DiskGiB: 100}
	placeNodes(t, e, map[string]db.Resources{"node-a": total, "node-b": total})
	// node-stale is connected but its inventory is old; node-gone is fresh
	// but not connected.
	if err := e.store.UpsertNodeInfo(t.Context(), db.NodeInfo{
		ID: "node-stale", DefaultImage: nodefake.Image,
		Capacity: db.Resources{VCPUs: 64, MemoryMiB: 1 << 20}, DiskFreeGiB: 1000,
		SeenAt: time.Now().Add(-time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	e.startNode(t, "node-stale")
	e.registerNode(t, "node-gone", db.Resources{VCPUs: 64, MemoryMiB: 1 << 20, DiskGiB: 1000})
	if err := e.store.MarkNodeObjectStore(t.Context(), "node-b", false); err != nil {
		t.Fatal(err)
	}
	s := e.scheduler(scheduler.Options{})

	for _, tc := range []struct {
		name    string
		req     scheduler.PlaceRequest
		want    []string
		wantErr error
	}{
		{"no filter", scheduler.PlaceRequest{}, []string{"node-a", "node-b"}, nil},
		{"image present", scheduler.PlaceRequest{Image: nodefake.Image}, []string{"node-a", "node-b"}, nil},
		{"image missing", scheduler.PlaceRequest{Image: "other-v9"}, nil, scheduler.ErrNoNode},
		{"exclude", scheduler.PlaceRequest{Exclude: []string{"node-a"}}, []string{"node-b"}, nil},
		{"object store", scheduler.PlaceRequest{NeedObjectStore: true}, []string{"node-a"}, nil},
		{"object store and exclude", scheduler.PlaceRequest{NeedObjectStore: true, Exclude: []string{"node-a"}}, nil, scheduler.ErrNoNode},
		{"too big", scheduler.PlaceRequest{Reserve: db.Resources{MemoryMiB: 1 << 30}}, nil, scheduler.ErrNoCapacity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := tc.req
			if req.Reserve == (db.Resources{}) {
				req.Reserve = schedReserve()
			}
			cands, err := s.Place(t.Context(), req)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("place error = %v, want %v", err, tc.wantErr)
			}
			if got := placeIDs(cands); !slices.Equal(got, tc.want) {
				t.Fatalf("candidates = %v, want %v", got, tc.want)
			}
		})
	}

	// The flag clears again and the node is a candidate once more.
	if err := e.store.MarkNodeObjectStore(t.Context(), "node-b", true); err != nil {
		t.Fatal(err)
	}
	n, err := e.store.Node(t.Context(), "node-b")
	if err != nil || !n.ObjectStoreOK || n.ObjectStoreCheckedAt == nil {
		t.Fatalf("node-b after mark ok = %+v, %v", n, err)
	}
	cands, err := s.Place(t.Context(), scheduler.PlaceRequest{Reserve: schedReserve(), NeedObjectStore: true})
	if err != nil || !slices.Equal(placeIDs(cands), []string{"node-a", "node-b"}) {
		t.Fatalf("object-store placement after mark ok = %v, %v", placeIDs(cands), err)
	}
	if err := e.store.MarkNodeObjectStore(t.Context(), "node-unknown", true); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("mark unknown node = %v, want ErrNotFound", err)
	}
}

// TestPlaceFailFastWithHangingNode proves probes run concurrently inside the
// failFast budget: a node whose Info never answers (and that the inventory
// lists first) neither spends the budget nor hides the healthy node.
func TestPlaceFailFastWithHangingNode(t *testing.T) {
	e := newSchedEnv(t)
	e.registerNode(t, "node-hang", db.Resources{VCPUs: 64, MemoryMiB: 1 << 20, DiskGiB: 1000})
	placeStartServer(t, e, "node-hang", &placeHangingNode{Node: nodefake.New("node-hang", t.TempDir())})
	placeNodes(t, e, map[string]db.Resources{"node-ok": {VCPUs: 8, MemoryMiB: 16384, DiskGiB: 100}})

	const budget = 400 * time.Millisecond
	s := e.scheduler(scheduler.Options{FailFastBudget: budget})
	start := time.Now()
	res, err := s.Admit(t.Context(), scheduler.AdmitRequest{
		ID: ids.Sandbox(), Owner: e.owner(t), MachineType: "small",
		Reserve: schedReserve(), MaxAttempts: 1, FailFast: true,
	})
	if err != nil {
		t.Fatalf("failFast admit with a hanging node: %v", err)
	}
	if elapsed := time.Since(start); elapsed > budget+time.Second {
		t.Fatalf("failFast admit took %v, budget %v", elapsed, budget)
	}
	if got := *res.Sandbox.NodeID; got != "node-ok" {
		t.Fatalf("placed on %s, want node-ok", got)
	}

	// Only the hanging node: the budget ends placement as no_ready_machine.
	start = time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	defer cancel()
	_, err = s.Place(ctx, scheduler.PlaceRequest{Reserve: schedReserve(), Exclude: []string{"node-ok"}})
	if !errors.Is(err, scheduler.ErrNoNode) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("place with only a hanging node = %v, want ErrNoNode joined with the deadline", err)
	}
	if elapsed := time.Since(start); elapsed > budget+time.Second {
		t.Fatalf("place took %v, budget %v", elapsed, budget)
	}
}

// TestPlaceWaitsForSlowPreferredNode: a preferred node that answers 300 ms
// after a non-preferred one still ranks first, with and without failFast
// (locality is never lost to the failFast cut-off).
func TestPlaceWaitsForSlowPreferredNode(t *testing.T) {
	e := newSchedEnv(t)
	total := db.Resources{VCPUs: 8, MemoryMiB: 16384, DiskGiB: 100}
	e.registerNode(t, "node-slow", total)
	slow := nodefake.New("node-slow", t.TempDir())
	slow.VCPUs, slow.MemoryMiB = 64, 256<<10
	placeStartServer(t, e, "node-slow", &placeSlowNode{Node: slow, delay: 300 * time.Millisecond})
	placeNodes(t, e, map[string]db.Resources{"node-fast": total})
	s := e.scheduler(scheduler.Options{})

	req := scheduler.PlaceRequest{Reserve: schedReserve(), Prefer: []string{"node-slow"}}
	cands, err := s.Place(t.Context(), req)
	if err != nil || !slices.Equal(placeIDs(cands), []string{"node-slow", "node-fast"}) {
		t.Fatalf("place without failFast = %v, %v; want node-slow first", placeIDs(cands), err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), scheduler.DefaultFailFastBudget)
	defer cancel()
	cands, err = s.Place(ctx, req)
	if err != nil || !slices.Equal(placeIDs(cands), []string{"node-slow", "node-fast"}) {
		t.Fatalf("place under the failFast budget = %v, %v; want node-slow first", placeIDs(cands), err)
	}

	// Without a preference, outside failFast the slow node is still waited
	// for and ranked by capacity (equal here: by id).
	cands, err = s.Place(t.Context(), scheduler.PlaceRequest{Reserve: schedReserve()})
	if err != nil || !slices.Equal(placeIDs(cands), []string{"node-fast", "node-slow"}) {
		t.Fatalf("place without preference = %v, %v; want both nodes", placeIDs(cands), err)
	}
}

// TestAdmitFromGeneration places a create from a generation on the
// preferred node with a grown disk, records the source generation on the
// provision job and starts a new chain.
func TestAdmitFromGeneration(t *testing.T) {
	e := newSchedEnv(t)
	total := db.Resources{VCPUs: 8, MemoryMiB: 16384, DiskGiB: 100}
	placeNodes(t, e, map[string]db.Resources{"node-a": total, "node-b": total})
	own := e.owner(t)
	gen := placeGeneration(t, e, own, "node-a", 15<<30+1) // just over 15 GiB
	s := e.scheduler(scheduler.Options{})

	req := schedAdmitRequest(ids.Sandbox(), own) // 10 GiB disk
	req.Source = placeSource(gen, "node-b")
	res, err := s.Admit(t.Context(), req)
	if err != nil {
		t.Fatalf("admit from generation: %v", err)
	}
	sbx := res.Sandbox
	if *sbx.NodeID != "node-b" || *sbx.DiskNodeID != "node-b" {
		t.Fatalf("placed on %v (disk %v), want the preferred node-b", *sbx.NodeID, *sbx.DiskNodeID)
	}
	if sbx.Reserved.DiskGiB != 16 || sbx.Desired.DiskGiB != 16 {
		t.Fatalf("disk = %d/%d GiB, want 16 (never smaller than the manifest)", sbx.Reserved.DiskGiB, sbx.Desired.DiskGiB)
	}
	if sbx.FromManifestKey != gen.ManifestKey || sbx.ChainID == gen.ChainID {
		t.Fatalf("sandbox manifest %q chain %s; want %q and a new chain", sbx.FromManifestKey, sbx.ChainID, gen.ManifestKey)
	}
	if res.Job.SourceGenerationID == nil || *res.Job.SourceGenerationID != gen.ID {
		t.Fatalf("job source generation = %v, want %s", res.Job.SourceGenerationID, gen.ID)
	}
	var payload db.ProvisionPayload
	if err := json.Unmarshal([]byte(res.Job.Payload), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.FromManifest != gen.ManifestKey || payload.Image != nodefake.Image || payload.NodeID != "node-b" {
		t.Fatalf("payload = %+v", payload)
	}

	// A larger requested disk is kept.
	big := schedAdmitRequest(ids.Sandbox(), own)
	big.Reserve.DiskGiB = 20
	big.Source = placeSource(gen)
	if res, err := s.Admit(t.Context(), big); err != nil || res.Sandbox.Reserved.DiskGiB != 20 {
		t.Fatalf("admit with a larger disk = %+v, %v", res.Sandbox.Reserved, err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*scheduler.GenerationSource)
		before func(t *testing.T)
		want   error
	}{
		{"wrong manifest key", func(s *scheduler.GenerationSource) { s.ManifestKey += "x" }, nil, db.ErrSnapshotNotFound},
		{"unknown generation", func(s *scheduler.GenerationSource) { s.GenerationID = uuid.NewV7() }, nil, db.ErrSnapshotNotFound},
		{"image on no node", func(s *scheduler.GenerationSource) { s.Image = "other-v9" }, nil, scheduler.ErrNoNode},
		{"purged generation", nil, func(t *testing.T) {
			if _, err := e.store.Pool().Exec(t.Context(), `UPDATE snapshots SET purged_at = now() WHERE id = $1`, gen.ID); err != nil {
				t.Fatal(err)
			}
		}, db.ErrSnapshotNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.before != nil {
				tc.before(t)
			}
			req := schedAdmitRequest(ids.Sandbox(), own)
			req.Source = placeSource(gen)
			if tc.mutate != nil {
				tc.mutate(req.Source)
			}
			if _, err := s.Admit(t.Context(), req); !errors.Is(err, tc.want) {
				t.Fatalf("admit = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestCreateFromManifestDiskTooSmall: the store refuses a reservation smaller
// than the generation's disk (the scheduler always grows it).
func TestCreateFromManifestDiskTooSmall(t *testing.T) {
	e := newSchedEnv(t)
	e.registerNode(t, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 16384, DiskGiB: 100})
	own := e.owner(t)
	gen := placeGeneration(t, e, own, "node-a", 3<<30)
	_, err := e.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: "node-a",
		Reserve:      db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 2},
		FromManifest: gen.ManifestKey, SourceGenerationID: new(gen.ID),
	})
	if !errors.Is(err, db.ErrSnapshotDiskTooSmall) {
		t.Fatalf("create with a 2 GiB disk from a 3 GiB manifest = %v, want ErrSnapshotDiskTooSmall", err)
	}
}

// placeBatch builds n batch requests from gen, the first carrying key.
func placeBatch(own uuid.UUID, gen db.Generation, n int, key string) []scheduler.AdmitRequest {
	reqs := make([]scheduler.AdmitRequest, n)
	hash := sha256.Sum256([]byte("batch body " + key))
	for i := range reqs {
		reqs[i] = schedAdmitRequest(ids.Sandbox(), own)
		reqs[i].Reserve.DiskGiB = 1
		reqs[i].Source = placeSource(gen)
	}
	if key != "" {
		reqs[0].IdempotencyKey = key
		reqs[0].RequestSHA256 = hash[:]
	}
	return reqs
}

func placeSandboxCount(t *testing.T, e *schedEnv, own uuid.UUID) int {
	t.Helper()
	var n int
	if err := e.store.Pool().QueryRow(t.Context(),
		`SELECT count(*) FROM sandboxes WHERE owner_id = $1`, own).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestAdmitBatchSpreadsAndReplays spreads 10 sandboxes 5/5 over two equal
// nodes and replays the whole batch in creation order.
func TestAdmitBatchSpreadsAndReplays(t *testing.T) {
	e := newSchedEnv(t)
	// Each node holds five 2 vCPU / 2 GiB sandboxes, the source row's
	// reservation (1 vCPU, 512 MiB) aside.
	total := db.Resources{VCPUs: 11, MemoryMiB: 5*2048 + 512, DiskGiB: 100}
	placeNodes(t, e, map[string]db.Resources{"node-a": total, "node-b": total})
	own := e.owner(t)
	// The generation belongs to the batch's own account: a cross-account
	// source is only legal through an active share code.
	gen := placeGeneration(t, e, own, "node-a", 1<<30)
	// The source sandbox now belongs to the batch owner; count only what the
	// batch itself adds.
	base := placeSandboxCount(t, e, own)
	if err := e.store.ReleaseCapacity(t.Context(), "node-a", db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}); err != nil {
		t.Fatal(err)
	}
	s := e.scheduler(scheduler.Options{})

	reqs := placeBatch(own, gen, 10, "batch-key")
	res, err := s.AdmitBatch(t.Context(), reqs)
	if err != nil {
		t.Fatalf("admit batch: %v", err)
	}
	if len(res) != 10 {
		t.Fatalf("batch created %d sandboxes, want 10", len(res))
	}
	perNode := map[string]int{}
	for i, r := range res {
		if r.Sandbox.ID != reqs[i].ID || r.Replayed {
			t.Fatalf("result %d = %s (replayed %v), want %s in request order", i, r.Sandbox.ID, r.Replayed, reqs[i].ID)
		}
		if r.Job.SourceGenerationID == nil || *r.Job.SourceGenerationID != gen.ID {
			t.Fatalf("result %d job source = %v", i, r.Job.SourceGenerationID)
		}
		perNode[*r.Sandbox.NodeID]++
	}
	if perNode["node-a"] != 5 || perNode["node-b"] != 5 {
		t.Fatalf("spread = %v, want 5/5", perNode)
	}

	// A replay with fresh ids returns the original batch in order.
	again := placeBatch(own, gen, 10, "batch-key")
	replayed, err := s.AdmitBatch(t.Context(), again)
	if err != nil {
		t.Fatalf("replay batch: %v", err)
	}
	if len(replayed) != 10 {
		t.Fatalf("replay returned %d sandboxes, want 10", len(replayed))
	}
	for i, r := range replayed {
		if r.Sandbox.ID != res[i].Sandbox.ID || !r.Replayed || r.Job.ID != res[i].Job.ID {
			t.Fatalf("replay %d = %s (replayed %v), want %s", i, r.Sandbox.ID, r.Replayed, res[i].Sandbox.ID)
		}
	}
	if n := placeSandboxCount(t, e, own); n != base+10 {
		t.Fatalf("owner has %d sandboxes after replay, want %d", n, base+10)
	}

	// The same key with another body is refused.
	other := placeBatch(own, gen, 2, "")
	other[0].IdempotencyKey = "batch-key"
	other[0].RequestSHA256 = []byte("different")
	if _, err := s.AdmitBatch(t.Context(), other); !errors.Is(err, db.ErrIdempotencyReused) {
		t.Fatalf("reused key = %v, want ErrIdempotencyReused", err)
	}
}

// TestAdmitBatchAllOrNothing creates nothing when the 10th sandbox does not
// fit, both when placement sees it and when the transaction does.
func TestAdmitBatchAllOrNothing(t *testing.T) {
	e := newSchedEnv(t)
	total := db.Resources{VCPUs: 64, MemoryMiB: 9 * 2048, DiskGiB: 100} // nine sandboxes
	placeNodes(t, e, map[string]db.Resources{"node-a": total})
	own := e.owner(t)
	gen := placeGeneration(t, e, own, "node-a", 1<<30)
	// The source sandbox belongs to the batch owner; a refused batch must add
	// nothing to the baseline.
	base := placeSandboxCount(t, e, own)
	if err := e.store.ReleaseCapacity(t.Context(), "node-a", db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}); err != nil {
		t.Fatal(err)
	}
	s := e.scheduler(scheduler.Options{})

	if _, err := s.AdmitBatch(t.Context(), placeBatch(own, gen, 10, "k1")); !errors.Is(err, scheduler.ErrNoCapacity) {
		t.Fatalf("batch of 10 on room for 9 = %v, want ErrNoCapacity", err)
	}
	if n := placeSandboxCount(t, e, own); n != base {
		t.Fatalf("owner has %d sandboxes after a refused batch, want %d", n, base)
	}

	// The transaction itself: ten rows whose tenth reservation fails roll
	// back the nine before it.
	params := make([]db.CreateSandboxParams, 10)
	for i := range params {
		params[i] = db.CreateSandboxParams{
			ID: ids.Sandbox(), Owner: own, NodeID: "node-a",
			Reserve:      db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 1},
			FromManifest: gen.ManifestKey, SourceGenerationID: new(gen.ID),
		}
	}
	if _, err := e.store.CreateSandboxes(t.Context(), params); !errors.Is(err, db.ErrNoCapacity) {
		t.Fatalf("create sandboxes = %v, want ErrNoCapacity", err)
	}
	if n := placeSandboxCount(t, e, own); n != base {
		t.Fatalf("owner has %d sandboxes after a rolled-back batch, want %d", n, base)
	}
	node, err := e.store.Node(t.Context(), "node-a")
	if err != nil || node.Reserved != (db.Resources{}) {
		t.Fatalf("node-a reservation after rollback = %+v, %v; want none", node.Reserved, err)
	}

	// Nine fit.
	res, err := s.AdmitBatch(t.Context(), placeBatch(own, gen, 9, ""))
	if err != nil || len(res) != 9 {
		t.Fatalf("batch of 9 = %d, %v", len(res), err)
	}

	fast := placeBatch(own, gen, 2, "")
	fast[1].FailFast = true
	if _, err := s.AdmitBatch(t.Context(), fast); err == nil {
		t.Fatal("batch with failFast accepted")
	}
}
