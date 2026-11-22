package scheduler

// Focused unit tests of the batch-from admission used by a create with count.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

type batchFromStore struct {
	replay  []db.CreateSandboxResult
	hit     bool
	created []db.CreateSandboxParams
	err     error
}

func (s *batchFromStore) NodesWithCapacity(context.Context, db.Resources, time.Duration) ([]db.Node, error) {
	return nil, nil
}

func (s *batchFromStore) CreateSandbox(context.Context, db.CreateSandboxParams) (db.CreateSandboxResult, error) {
	return db.CreateSandboxResult{}, db.ErrNoCapacity
}

func (s *batchFromStore) ReplayCreateSandbox(context.Context, uuid.UUID, string, []byte) (db.CreateSandboxResult, bool, error) {
	return db.CreateSandboxResult{}, false, nil
}

func (s *batchFromStore) CreateSandboxes(_ context.Context, ps []db.CreateSandboxParams) ([]db.CreateSandboxResult, error) {
	s.created = ps
	if s.err != nil {
		return nil, s.err
	}
	out := make([]db.CreateSandboxResult, len(ps))
	for i, p := range ps {
		out[i] = db.CreateSandboxResult{
			Sandbox: db.Sandbox{ID: p.ID, OwnerID: p.Owner, NodeID: new(p.NodeID)},
		}
	}
	return out, nil
}

func (s *batchFromStore) ReplayCreateSandboxBatch(context.Context, uuid.UUID, string, []byte) ([]db.CreateSandboxResult, bool, error) {
	return s.replay, s.hit, nil
}

func (s *batchFromStore) Sandbox(context.Context, uuid.UUID, string) (db.Sandbox, error) {
	return db.Sandbox{}, db.ErrNotFound
}

func (s *batchFromStore) AcceptSandboxResume(context.Context, db.ResumeParams) (db.ResumeAcceptance, error) {
	return db.ResumeAcceptance{}, db.ErrInvalidState
}

type batchFromResolver struct {
	source  db.SnapshotSource
	machine string
	reserve db.Resources
	err     error
	calls   int
}

func (r *batchFromResolver) ResolveRestore(context.Context, uuid.UUID, string, bool, string) (db.SnapshotSource, string, db.Resources, error) {
	r.calls++
	return r.source, r.machine, r.reserve, r.err
}

func batchScheduler(t *testing.T, store Store, restore RestoreResolver) *Scheduler {
	t.Helper()
	s, err := New(Options{
		Store:   store,
		Dial:    func(string) (NodeClient, io.Closer, error) { return nil, nil, errors.New("offline") },
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Restore: restore,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func batchRequests(owner uuid.UUID, n int) []AdmitRequest {
	out := make([]AdmitRequest, n)
	for i := range out {
		out[i] = AdmitRequest{
			ID:          "bx_abcdefg" + string(rune('a'+i)),
			Owner:       owner,
			From:        "base",
			MachineType: "default",
		}
	}
	out[0].IdempotencyKey = "key"
	out[0].RequestSHA256 = []byte{1}
	return out
}

func TestAdmitBatchFromReplayBeforeResolution(t *testing.T) {
	owner := uuid.NewV7()
	want := db.CreateSandboxResult{Sandbox: db.Sandbox{ID: "bx_replayed", OwnerID: owner}, Replayed: true}
	store := &batchFromStore{replay: []db.CreateSandboxResult{want}, hit: true}
	resolver := &batchFromResolver{err: errors.New("must not resolve")}
	s := batchScheduler(t, store, resolver)

	res, err := s.AdmitBatchFrom(t.Context(), batchRequests(owner, 2))
	if err != nil {
		t.Fatalf("batch replay: %v", err)
	}
	if len(res) != 1 || res[0].Sandbox.ID != "bx_replayed" {
		t.Fatalf("results = %+v", res)
	}
	if resolver.calls != 0 {
		t.Fatalf("resolver ran %d times before replay", resolver.calls)
	}
}

func TestAdmitBatchFromValidation(t *testing.T) {
	owner := uuid.NewV7()
	t.Run("mixed names", func(t *testing.T) {
		reqs := batchRequests(owner, 2)
		reqs[1].From = "other"
		_, err := batchScheduler(t, &batchFromStore{}, &batchFromResolver{}).AdmitBatchFrom(t.Context(), reqs)
		if err == nil {
			t.Fatal("mixed names accepted")
		}
	})
	t.Run("caller source rejected", func(t *testing.T) {
		reqs := batchRequests(owner, 2)
		reqs[1].Source = &GenerationSource{GenerationID: uuid.NewV7()}
		_, err := batchScheduler(t, &batchFromStore{}, &batchFromResolver{}).AdmitBatchFrom(t.Context(), reqs)
		if err == nil {
			t.Fatal("caller-provided source accepted")
		}
	})
	t.Run("second idempotency key rejected", func(t *testing.T) {
		reqs := batchRequests(owner, 2)
		reqs[1].IdempotencyKey = "second"
		_, err := batchScheduler(t, &batchFromStore{}, &batchFromResolver{}).AdmitBatchFrom(t.Context(), reqs)
		if err == nil {
			t.Fatal("second key accepted")
		}
	})
}

func TestAdmitBatchFromLegacyPinnedNode(t *testing.T) {
	owner := uuid.NewV7()
	source := db.SnapshotSource{
		RowID: uuid.NewV7(), OwnerID: owner, Name: "base",
		SourceSandboxID: "bx_source",
		ArtifactID:      "snap-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		NodeID:          "node-a",
		MachineType:     "default",
		DiskGiB:         51,
		Generation:      1,
	}
	resolver := &batchFromResolver{source: source, machine: "default", reserve: db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 51}}
	store := &batchFromStore{}
	// The offline Dial makes the pinned-node probe fail: a legacy batch
	// never silently falls back to another node or a fresh disk.
	_, err := batchScheduler(t, store, resolver).AdmitBatchFrom(t.Context(), batchRequests(owner, 2))
	if !errors.Is(err, ErrNoNode) {
		t.Fatalf("legacy batch error = %v, want ErrNoNode", err)
	}
}

func TestGenerationSourceFromSnapshot(t *testing.T) {
	id := uuid.NewV7()
	source := db.SnapshotSource{
		GenerationID: &id, ManifestKey: "k", Image: "img", DiskBytes: 4096, NodeID: "node-a",
	}
	gen := generationSourceFromSnapshot(source)
	if gen == nil || gen.GenerationID != id || gen.ManifestKey != "k" || gen.Image != "img" ||
		gen.DiskBytes != 4096 || len(gen.Prefer) != 1 || gen.Prefer[0] != "node-a" {
		t.Fatalf("generation source = %+v", gen)
	}
	legacy := source
	legacy.GenerationID = nil
	if generationSourceFromSnapshot(legacy) != nil {
		t.Fatal("legacy source became a generation source")
	}
}

// Compile-time proof the test store is a scheduler Store.
var _ Store = (*batchFromStore)(nil)
