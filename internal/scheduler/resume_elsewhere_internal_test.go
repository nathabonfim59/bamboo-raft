package scheduler

// Resume on another node: the admission decisions of resumeArchived
// against deterministic fakes (no PostgreSQL): same node first, moving only
// on capacity or node availability, the placement it asks for, and the
// refusals without restorable data.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// rsmStore is a fixed sandbox, generation and node inventory with scripted
// admission answers. It records every admission call.
type rsmStore struct {
	*roomStore
	sbx       db.Sandbox
	gen       *db.Generation
	evictions []string
	sameErr   error
	elseErr   map[string]error // per target node; nil accepts

	sameCalls []db.ResumeParams
	elseCalls []db.ResumeElsewhereParams
}

func (s *rsmStore) Sandbox(context.Context, uuid.UUID, string) (db.Sandbox, error) { return s.sbx, nil }

func (s *rsmStore) AcceptSandboxResume(_ context.Context, p db.ResumeParams) (db.ResumeAcceptance, error) {
	s.sameCalls = append(s.sameCalls, p)
	if s.sameErr != nil {
		return db.ResumeAcceptance{}, s.sameErr
	}
	return db.ResumeAcceptance{Sandbox: s.sbx}, nil
}

func (s *rsmStore) LatestGeneration(context.Context, uuid.UUID, string) (db.Generation, bool, error) {
	if s.gen == nil {
		return db.Generation{}, false, nil
	}
	return *s.gen, true, nil
}

func (s *rsmStore) OpenEvictionNodes(context.Context, uuid.UUID, string) ([]string, error) {
	return slices.Clone(s.evictions), nil
}

func (s *rsmStore) AcceptSandboxResumeElsewhere(_ context.Context, p db.ResumeElsewhereParams) (db.ResumeAcceptance, error) {
	s.elseCalls = append(s.elseCalls, p)
	if err := s.elseErr[p.ToNode]; err != nil {
		return db.ResumeAcceptance{}, err
	}
	return db.ResumeAcceptance{Sandbox: db.Sandbox{ID: p.SandboxID, NodeID: new(p.ToNode)}}, nil
}

// rsmInfo is a live inventory with room and the listed images.
func rsmInfo(id string, images ...string) *nodev1.InfoResponse {
	info := &nodev1.InfoResponse{NodeId: id, Vcpus: 8, MemoryMib: 8192, DiskFreeMib: 100 << 10}
	for _, img := range images {
		info.Images = append(info.Images, &nodev1.Image{Id: img})
	}
	return info
}

func rsmNodeRow(id string, free uint64) db.Node {
	return db.Node{
		ID: id, ObjectStoreOK: true,
		Total:    db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100},
		Reserved: db.Resources{MemoryMiB: 8192 - free},
	}
}

// rsmScheduler serves infos by node id; a node missing from infos does not
// answer its probe.
func rsmScheduler(t *testing.T, store interface {
	Store
	ResumeElsewhereStore
}, infos map[string]*nodev1.InfoResponse, elsewhere bool) *Scheduler {
	t.Helper()
	opts := Options{
		Store: store,
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Dial: func(id string) (NodeClient, io.Closer, error) {
			if info, ok := infos[id]; ok {
				return roomNode{info: info}, io.NopCloser(nil), nil
			}
			return failingInfoClient{}, io.NopCloser(nil), nil
		},
		FreshWithin:  time.Minute,
		ProbeTimeout: time.Second,
	}
	if elsewhere {
		opts.ResumeElsewhere = store
	}
	s, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestResumeArchivedDecisions(t *testing.T) {
	archived := func() db.Sandbox {
		return db.Sandbox{
			ID: "bx_rsm", State: db.StateArchived, SnapshotsEnabled: true,
			NodeID: new("node-a"), DiskNodeID: new("node-a"),
			Desired:  db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1},
			Reserved: db.Resources{DiskGiB: 1},
		}
	}
	gen := &db.Generation{
		ID: uuid.NewV7(), ManifestKey: "manifests/c/3.json.zst", NodeID: "node-c",
		Image: "img-1", DiskBytes: 5<<30 + 1, // 6 GiB once rounded up
	}
	allUp := map[string]*nodev1.InfoResponse{
		"node-a": rsmInfo("node-a", "img-1"),
		"node-b": rsmInfo("node-b", "img-1"),
		"node-c": rsmInfo("node-c", "img-1"),
	}
	without := func(id string) map[string]*nodev1.InfoResponse {
		m := map[string]*nodev1.InfoResponse{}
		for k, v := range allUp {
			if k != id {
				m[k] = v
			}
		}
		return m
	}
	tests := []struct {
		name      string
		sbx       func(db.Sandbox) db.Sandbox
		gen       *db.Generation
		evictions []string
		sameErr   error
		elseErr   map[string]error
		infos     map[string]*nodev1.InfoResponse
		phase1    bool

		wantErr  error
		wantSame int
		wantElse []string // targets tried in order
	}{
		{name: "disk node fits", gen: gen, wantSame: 1},
		{name: "disk node full", gen: gen, sameErr: db.ErrNoCapacity, wantSame: 1, wantElse: []string{"node-c"}},
		{name: "disk node row gone", gen: gen, sameErr: db.ErrNodeUnavailable, wantSame: 1, wantElse: []string{"node-c"}},
		{name: "disk node away", gen: gen, infos: without("node-a"), wantSame: 0, wantElse: []string{"node-c"}},
		{name: "other refusal passes through", gen: gen, sameErr: db.ErrTypeTooSmall, wantErr: db.ErrTypeTooSmall, wantSame: 1},
		{name: "no generation, disk node full", sameErr: db.ErrNoCapacity, wantErr: db.ErrNoCapacity, wantSame: 1},
		{name: "no generation, disk node away", infos: without("node-a"), wantErr: ErrNoNode},
		{
			name: "no generation, no disk", sbx: func(s db.Sandbox) db.Sandbox { s.DiskNodeID = nil; return s },
			wantErr: db.ErrNoRestorableData,
		},
		{
			name: "data expired", gen: gen, sbx: func(s db.Sandbox) db.Sandbox { s.DataExpiredAt = new(time.Now()); return s },
			wantErr: db.ErrSnapshotDataExpired,
		},
		{
			name: "snapshots off, no disk", gen: gen,
			sbx:     func(s db.Sandbox) db.Sandbox { s.DiskNodeID, s.SnapshotsEnabled = nil, false; return s },
			wantErr: db.ErrSnapshotsDisabled,
		},
		{
			name: "no disk goes straight to the store", gen: gen,
			sbx:      func(s db.Sandbox) db.Sandbox { s.DiskNodeID = nil; return s },
			wantElse: []string{"node-c"},
		},
		{name: "open eviction excluded", gen: gen, sameErr: db.ErrNoCapacity, evictions: []string{"node-c"}, wantSame: 1, wantElse: []string{"node-b"}},
		{
			name: "lost race moves to the next candidate", gen: gen, sameErr: db.ErrNoCapacity,
			elseErr:  map[string]error{"node-c": db.ErrNoCapacity},
			wantSame: 1, wantElse: []string{"node-c", "node-b"},
		},
		{
			name: "every candidate lost", gen: gen, sameErr: db.ErrNoCapacity,
			elseErr:  map[string]error{"node-c": db.ErrNoCapacity, "node-b": db.ErrNodeUnavailable},
			wantErr:  ErrNoCapacity,
			wantSame: 1, wantElse: []string{"node-c", "node-b"},
		},
		{
			name: "store refusal stops the walk", gen: gen, sameErr: db.ErrNoCapacity,
			elseErr:  map[string]error{"node-c": db.ErrSnapshotNotFound},
			wantErr:  db.ErrSnapshotNotFound,
			wantSame: 1, wantElse: []string{"node-c"},
		},
		{
			name: "image filter", gen: gen, sameErr: db.ErrNoCapacity,
			infos: map[string]*nodev1.InfoResponse{
				"node-a": allUp["node-a"], "node-b": allUp["node-b"], "node-c": rsmInfo("node-c", "img-2"),
			},
			wantSame: 1, wantElse: []string{"node-b"},
		},
		{
			name: "archiving stays on the disk node", gen: gen, sameErr: db.ErrNoCapacity,
			sbx:     func(s db.Sandbox) db.Sandbox { s.State = db.StateArchiving; return s },
			wantErr: db.ErrNoCapacity, wantSame: 1,
		},
		{name: "phase 1 never moves", gen: gen, sameErr: db.ErrNoCapacity, phase1: true, wantErr: db.ErrNoCapacity, wantSame: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sbx := archived()
			if tc.sbx != nil {
				sbx = tc.sbx(sbx)
			}
			store := &rsmStore{
				roomStore: &roomStore{nodes: []db.Node{
					rsmNodeRow("node-a", 8192), rsmNodeRow("node-b", 4096), rsmNodeRow("node-c", 1024),
				}},
				sbx: sbx, gen: tc.gen, evictions: tc.evictions, sameErr: tc.sameErr, elseErr: tc.elseErr,
			}
			infos := tc.infos
			if infos == nil {
				infos = allUp
			}
			s := rsmScheduler(t, store, infos, !tc.phase1)
			_, err := s.Resume(t.Context(), ResumeRequest{Owner: uuid.NewV7(), SandboxID: sbx.ID})
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("resume = %v, want %v", err, tc.wantErr)
			}
			if len(store.sameCalls) != tc.wantSame {
				t.Fatalf("same-node admissions = %d, want %d", len(store.sameCalls), tc.wantSame)
			}
			var tried []string
			for _, c := range store.elseCalls {
				tried = append(tried, c.ToNode)
				if c.GenerationID != gen.ID || c.ManifestKey != gen.ManifestKey || c.SandboxID != sbx.ID {
					t.Fatalf("elsewhere params = %+v", c)
				}
			}
			if !slices.Equal(tried, tc.wantElse) {
				t.Fatalf("elsewhere targets = %v, want %v", tried, tc.wantElse)
			}
		})
	}
}

// TestResumeElsewhereGrowsTheDisk: placement asks for the generation's disk
// when it is larger than the machine's, and never less than the machine's.
func TestResumeElsewhereGrowsTheDisk(t *testing.T) {
	for _, tc := range []struct {
		name      string
		diskBytes uint64
		want      uint64
	}{
		{"generation larger", 5<<30 + 1, 6},
		{"generation smaller", 1 << 20, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var asked db.Resources
			store := &rsmStore{
				roomStore: &roomStore{nodes: []db.Node{rsmNodeRow("node-b", 8192)}},
				sbx: db.Sandbox{
					ID: "bx_rsm", State: db.StateArchived, SnapshotsEnabled: true, NodeID: new("node-a"),
					Desired: db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 3},
				},
				gen: &db.Generation{ID: uuid.NewV7(), ManifestKey: "m", Image: "img-1", DiskBytes: tc.diskBytes},
			}
			s := rsmScheduler(t, &rsmCapStore{rsmStore: store, asked: &asked},
				map[string]*nodev1.InfoResponse{"node-b": rsmInfo("node-b", "img-1")}, true)
			if _, err := s.Resume(t.Context(), ResumeRequest{Owner: uuid.NewV7(), SandboxID: "bx_rsm"}); err != nil {
				t.Fatal(err)
			}
			if asked.DiskGiB != tc.want || asked.VCPUs != 1 || asked.MemoryMiB != 512 {
				t.Fatalf("placement asked %+v, want disk %d", asked, tc.want)
			}
		})
	}
}

// rsmCapStore records the reservation placement asked capacity for.
type rsmCapStore struct {
	*rsmStore
	asked *db.Resources
}

func (s *rsmCapStore) NodesWithCapacity(ctx context.Context, need db.Resources, fresh time.Duration) ([]db.Node, error) {
	*s.asked = need
	return s.rsmStore.NodesWithCapacity(ctx, need, fresh)
}
