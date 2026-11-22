package server

// Fork admission adapter tests: the locality hint, the image filter and the
// failFast budget, over fakes (no database, no node).

import (
	"context"
	"errors"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// forkTestPlacer records the placement and returns scripted candidates.
type forkTestPlacer struct {
	req  scheduler.PlaceRequest
	got  bool
	res  []scheduler.Candidate
	err  error
	wait bool // block until the placement context ends
}

func (p *forkTestPlacer) Place(ctx context.Context, req scheduler.PlaceRequest) ([]scheduler.Candidate, error) {
	p.req, p.got = req, true
	if p.wait {
		<-ctx.Done()
		return nil, errors.Join(scheduler.ErrNoNode, ctx.Err())
	}
	return p.res, p.err
}

// forkTestStore records the admission and can refuse the first candidates.
type forkTestStore struct {
	nodes   map[string]db.Node
	admits  []db.ForkAdmitParams
	fullFor int // first n candidate nodes answer ErrNoCapacity
	// replayed is the fork a keyed replay returns; empty ID means not found.
	replayed db.Sandbox
	replays  int
}

func (s *forkTestStore) ReplayFork(context.Context, uuid.UUID, string, []byte) (db.CreateSandboxResult, bool, error) {
	s.replays++
	if s.replayed.ID == "" {
		return db.CreateSandboxResult{}, false, nil
	}
	return db.CreateSandboxResult{Sandbox: s.replayed, Replayed: true}, true, nil
}

func (s *forkTestStore) Node(_ context.Context, id string) (db.Node, error) {
	node, ok := s.nodes[id]
	if !ok {
		return db.Node{}, db.ErrNotFound
	}
	return node, nil
}

func (s *forkTestStore) AdmitFork(_ context.Context, p db.ForkAdmitParams) (db.CreateSandboxResult, error) {
	s.admits = append(s.admits, p)
	if len(s.admits) <= s.fullFor {
		return db.CreateSandboxResult{}, db.ErrNoCapacity
	}
	return db.CreateSandboxResult{Sandbox: db.Sandbox{
		ID: p.ID, OwnerID: p.Owner, State: db.StateProvisioning,
		MachineType: p.MachineType, NodeID: new(p.NodeID),
	}}, nil
}

// forkTestRequest is one ready fork request against a source on node-a.
func forkTestRequest(machine string) api.ForkRequest {
	node := "node-a"
	return api.ForkRequest{
		Owner:     uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-123456789abc"),
		SandboxID: "bx_3456789a",
		SourceID:  "bx_23456789",
		Source: db.Sandbox{
			ID: "bx_23456789", State: db.StateReady, MachineType: machine,
			NodeID: &node, SnapshotsEnabled: true,
			Reserved: db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51},
		},
		MachineType: machine,
		Resources:   db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51},
		TTLSeconds:  new(3600),
	}
}

// TestForkAdapterPlacementHints checks the image filter and the locality
// order for a ready source (its node's inventory image) and an archived one
// (its generation's image and node).
func TestForkAdapterPlacementHints(t *testing.T) {
	source := "node-a"
	generation := db.Generation{
		ID: uuid.NewV7(), SandboxID: "bx_23456789", NodeID: "node-b",
		Image: "img-gen", ManifestKey: "chains/c/gen/0.json", DiskBytes: 80 << 30,
	}
	archived := forkTestRequest("default")
	archived.Source.State = db.StateArchived
	archived.Latest = &generation

	ready := forkTestRequest("default")
	ready.Latest = &db.Generation{
		ID: uuid.NewV7(), NodeID: "node-a", Image: "img-old", DiskBytes: 80 << 30,
	}

	store := &forkTestStore{nodes: map[string]db.Node{
		source:   {ID: source, DefaultImage: "img-default"},
		"node-b": {ID: "node-b", DefaultImage: "img-b"},
	}}
	placer := &forkTestPlacer{res: []scheduler.Candidate{{Node: db.Node{ID: "node-b"}}}}
	adapter := apiForkAdmission{placer: placer, store: store}

	result, err := adapter.Fork(t.Context(), ready)
	if err != nil {
		t.Fatalf("ready fork: %v", err)
	}
	got := placer.req
	if got.Image != "img-old" || len(got.Prefer) != 1 || got.Prefer[0] != source {
		t.Fatalf("ready placement = image %q prefer %v, want img-old / [%s]", got.Image, got.Prefer, source)
	}
	if !got.NeedObjectStore || got.Reserve.DiskGiB != 80 {
		t.Fatalf("ready placement reserve = %+v objstore=%t, want the source disk grown to the generation",
			got.Reserve, got.NeedObjectStore)
	}
	if len(store.admits) != 1 || store.admits[0].SourceGenerationID != nil {
		t.Fatalf("ready-source admission = %+v, want no pinned generation", store.admits)
	}
	if result.Sandbox.ID != ready.SandboxID {
		t.Fatalf("result = %+v", result.Sandbox)
	}

	placer.res = []scheduler.Candidate{{Node: db.Node{ID: "node-b"}}}
	store.admits = nil
	if _, err := adapter.Fork(t.Context(), archived); err != nil {
		t.Fatalf("archived fork: %v", err)
	}
	if got := placer.req; got.Image != "img-gen" ||
		len(got.Prefer) != 2 || got.Prefer[0] != source || got.Prefer[1] != "node-b" {
		t.Fatalf("archived placement = image %q prefer %v, want img-gen / [node-a node-b]", got.Image, got.Prefer)
	}
	if len(store.admits) != 1 || store.admits[0].SourceGenerationID == nil ||
		*store.admits[0].SourceGenerationID != generation.ID {
		t.Fatalf("archived admission = %+v, want the pinned generation %s", store.admits, generation.ID)
	}
}

// TestForkAdapterFallbackAndRefusals checks the next-candidate fallback and
// the pinned sentinel mapping.
func TestForkAdapterFallbackAndRefusals(t *testing.T) {
	store := &forkTestStore{
		nodes:   map[string]db.Node{"node-a": {ID: "node-a", DefaultImage: "img"}},
		fullFor: 1,
	}
	placer := &forkTestPlacer{res: []scheduler.Candidate{
		{Node: db.Node{ID: "node-a"}}, {Node: db.Node{ID: "node-b"}},
	}}
	adapter := apiForkAdmission{placer: placer, store: store}
	result, err := adapter.Fork(t.Context(), forkTestRequest("default"))
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	if len(store.admits) != 2 || store.admits[1].NodeID != "node-b" {
		t.Fatalf("admissions = %+v, want a retry on node-b", store.admits)
	}
	if result.Sandbox.NodeID == nil || *result.Sandbox.NodeID != "node-b" {
		t.Fatalf("result node = %v, want node-b", result.Sandbox.NodeID)
	}

	if _, err := (apiForkAdmission{placer: &forkTestPlacer{err: scheduler.ErrNoNode}, store: store}).
		Fork(t.Context(), forkTestRequest("default")); !errors.Is(err, api.ErrNoReadyMachine) {
		t.Fatalf("no node = %v, want ErrNoReadyMachine", err)
	}
	if _, err := (apiForkAdmission{placer: &forkTestPlacer{err: scheduler.ErrNoCapacity}, store: store}).
		Fork(t.Context(), forkTestRequest("default")); !errors.Is(err, db.ErrNoCapacity) {
		t.Fatalf("no capacity = %v, want db.ErrNoCapacity", err)
	}
}

// TestForkAdapterReplayFirst checks a keyed duplicate is answered before
// placement (and again after a failed placement), so a retry never competes
// for capacity.
func TestForkAdapterReplayFirst(t *testing.T) {
	store := &forkTestStore{
		nodes:    map[string]db.Node{"node-a": {ID: "node-a"}},
		replayed: db.Sandbox{ID: "bx_9876543a", State: db.StateReady},
	}
	placer := &forkTestPlacer{err: scheduler.ErrNoCapacity}
	adapter := apiForkAdmission{placer: placer, store: store}
	request := forkTestRequest("default")
	request.IdempotencyKey = "retry-key"
	result, err := adapter.Fork(t.Context(), request)
	if err != nil {
		t.Fatalf("replayed fork: %v", err)
	}
	if result.Sandbox.ID != "bx_9876543a" || !result.Replayed {
		t.Fatalf("result = %+v, want the replayed fork", result)
	}
	if placer.got {
		t.Fatal("a keyed replay ran placement")
	}
	if len(store.admits) != 0 {
		t.Fatalf("a keyed replay admitted %+v", store.admits)
	}

	// Without a replay the placement runs; a duplicate that commits while it
	// runs is returned by the final recheck even when placement failed.
	store = &forkTestStore{nodes: map[string]db.Node{"node-a": {ID: "node-a"}}}
	adapter = apiForkAdmission{placer: &forkTestPlacer{err: scheduler.ErrNoCapacity}, store: store}
	if _, err := adapter.Fork(t.Context(), request); !errors.Is(err, db.ErrNoCapacity) {
		t.Fatalf("failed placement = %v, want db.ErrNoCapacity", err)
	}
	if store.replays != 2 {
		t.Fatalf("replays = %d, want the fast path and the final recheck", store.replays)
	}
}

// TestForkAdapterFailFastBudget checks failFast bounds placement inside the
// scheduler budget and still answers the no-ready-machine sentinel.
func TestForkAdapterFailFastBudget(t *testing.T) {
	store := &forkTestStore{nodes: map[string]db.Node{"node-a": {ID: "node-a"}}}
	placer := &forkTestPlacer{wait: true}
	adapter := apiForkAdmission{placer: placer, store: store}
	request := forkTestRequest("default")
	request.FailFast = true
	started := time.Now()
	_, err := adapter.Fork(t.Context(), request)
	if !errors.Is(err, api.ErrNoReadyMachine) {
		t.Fatalf("failFast placement = %v, want ErrNoReadyMachine", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("failFast fork took %s", elapsed)
	}
	if len(store.admits) != 0 {
		t.Fatalf("failFast fork admitted %+v after placement failed", store.admits)
	}
}
