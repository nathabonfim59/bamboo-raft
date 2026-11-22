package scheduler

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func placeCand(id string, total, reserved db.Resources) Candidate {
	return Candidate{
		Node: db.Node{ID: id, Total: total, Reserved: reserved},
		Info: &nodev1.InfoResponse{NodeId: id, DiskFreeMib: 1 << 30},
	}
}

func placeReqs(n int, r db.Resources) []AdmitRequest {
	reqs := make([]AdmitRequest, n)
	for i := range reqs {
		reqs[i].Reserve = r
	}
	return reqs
}

// TestSpread covers the round-robin fan-out with per-node accounting.
func TestSpread(t *testing.T) {
	one := db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 10}
	five := db.Resources{VCPUs: 10, MemoryMiB: 5 * 2048, DiskGiB: 50}
	for _, tc := range []struct {
		name  string
		cands []Candidate
		n     int
		want  []string // nil: does not fit
	}{
		{"equal nodes alternate", []Candidate{placeCand("a", five, db.Resources{}), placeCand("b", five, db.Resources{})}, 4,
			[]string{"a", "b", "a", "b"}},
		{"ten over two fills 5/5", []Candidate{placeCand("a", five, db.Resources{}), placeCand("b", five, db.Resources{})}, 10,
			[]string{"a", "b", "a", "b", "a", "b", "a", "b", "a", "b"}},
		{"eleven do not fit", []Candidate{placeCand("a", five, db.Resources{}), placeCand("b", five, db.Resources{})}, 11, nil},
		{"full node is skipped", []Candidate{placeCand("a", five, db.Resources{VCPUs: 8}), placeCand("b", five, db.Resources{})}, 4,
			[]string{"a", "b", "b", "b"}},
		{"reserved disk counts", []Candidate{placeCand("a", five, db.Resources{DiskGiB: 45})}, 1, nil},
		{"no candidates", nil, 1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := spread(tc.cands, placeReqs(tc.n, one))
			if ok != (tc.want != nil) || !slices.Equal(got, tc.want) {
				t.Fatalf("spread = %v, %v; want %v", got, ok, tc.want)
			}
		})
	}

	// The live disk offer caps the inventory row.
	c := placeCand("a", five, db.Resources{})
	c.Info.DiskFreeMib = 15 * 1024
	if got, ok := spread([]Candidate{c}, placeReqs(2, one)); ok {
		t.Fatalf("spread over a 15 GiB live offer = %v, want no fit", got)
	}
}

// TestCompareCandidates ranks preference, free memory, free vCPUs, id.
func TestCompareCandidates(t *testing.T) {
	big := db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 10}
	cands := []Candidate{
		placeCand("d", big, db.Resources{MemoryMiB: 4096}),
		placeCand("c", big, db.Resources{VCPUs: 4}),
		placeCand("b", big, db.Resources{}),
		placeCand("a", big, db.Resources{}),
	}
	for _, tc := range []struct {
		prefer []string
		want   []string
	}{
		{nil, []string{"a", "b", "c", "d"}},
		{[]string{"d"}, []string{"d", "a", "b", "c"}},
		{[]string{"c", "d"}, []string{"c", "d", "a", "b"}},
	} {
		got := slices.Clone(cands)
		slices.SortFunc(got, func(x, y Candidate) int { return compareCandidates(tc.prefer, x, y) })
		ids := make([]string, len(got))
		for i, c := range got {
			ids[i] = c.Node.ID
		}
		if !slices.Equal(ids, tc.want) {
			t.Fatalf("prefer %v: order %v, want %v", tc.prefer, ids, tc.want)
		}
	}
}

// TestSourceReserveNeverShrinks grows the disk to the manifest's, rounding up.
func TestSourceReserveNeverShrinks(t *testing.T) {
	for _, tc := range []struct {
		disk, bytes, want uint64
	}{
		{10, 0, 10},
		{10, 10 << 30, 10},
		{10, 10<<30 + 1, 11},
		{20, 3 << 30, 20},
		{0, 1, 1},
	} {
		req := AdmitRequest{Reserve: db.Resources{DiskGiB: tc.disk}, Source: &GenerationSource{DiskBytes: tc.bytes}}
		if got := sourceReserve(req).DiskGiB; got != tc.want {
			t.Fatalf("disk %d GiB, manifest %d bytes: %d GiB, want %d", tc.disk, tc.bytes, got, tc.want)
		}
	}
}

// placeDelayNode answers Info after a per-node delay (negative: never).
type placeDelayNode struct {
	id    string
	delay time.Duration
}

func (n placeDelayNode) Info(ctx context.Context, _ *nodev1.InfoRequest, _ ...grpc.CallOption) (*nodev1.InfoResponse, error) {
	if n.delay < 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	select {
	case <-time.After(n.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &nodev1.InfoResponse{NodeId: n.id, Vcpus: 64, MemoryMib: 1 << 20, DiskFreeMib: 1 << 30}, nil
}

func placeDelayScheduler(t *testing.T, delays map[string]time.Duration) *Scheduler {
	t.Helper()
	total := db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100}
	var nodes []db.Node
	for _, id := range slices.Sorted(maps.Keys(delays)) {
		nodes = append(nodes, db.Node{ID: id, Total: total, ObjectStoreOK: true})
	}
	s, err := New(Options{
		Store: &roomStore{nodes: nodes},
		Dial: func(id string) (NodeClient, io.Closer, error) {
			return placeDelayNode{id: id, delay: delays[id]}, io.NopCloser(nil), nil
		},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestPlaceRankGrace: the cut-off of slow probes applies only under a
// deadline shorter than the probe timeout and never before every preferred
// node answered.
func TestPlaceRankGrace(t *testing.T) {
	need := db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1}
	const slow = 300 * time.Millisecond
	for _, tc := range []struct {
		name     string
		delays   map[string]time.Duration
		prefer   []string
		deadline time.Duration // 0: none
		want     []string
		maxTime  time.Duration
	}{
		{"no deadline waits for every node", map[string]time.Duration{"fast": 0, "slow": slow}, nil, 0,
			[]string{"fast", "slow"}, 2 * time.Second},
		{"no deadline keeps a slow preferred node", map[string]time.Duration{"fast": 0, "slow": slow}, []string{"slow"}, 0,
			[]string{"slow", "fast"}, 2 * time.Second},
		{"failFast keeps a slow preferred node", map[string]time.Duration{"fast": 0, "slow": slow}, []string{"slow"}, 1500 * time.Millisecond,
			[]string{"slow", "fast"}, 1500 * time.Millisecond},
		{"failFast cuts a slow non-preferred node", map[string]time.Duration{"fast": 0, "slow": slow}, nil, 1500 * time.Millisecond,
			[]string{"fast"}, slow},
		{"failFast cuts a hung node", map[string]time.Duration{"fast": 0, "hung": -1}, nil, 1500 * time.Millisecond,
			[]string{"fast"}, slow},
		{"a long deadline is not failFast", map[string]time.Duration{"fast": 0, "slow": slow}, nil, time.Minute,
			[]string{"fast", "slow"}, 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := placeDelayScheduler(t, tc.delays)
			ctx := t.Context()
			if tc.deadline > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.deadline)
				defer cancel()
			}
			start := time.Now()
			cands, err := s.Place(ctx, PlaceRequest{Reserve: need, Prefer: tc.prefer})
			elapsed := time.Since(start)
			ids := make([]string, len(cands))
			for i, c := range cands {
				ids[i] = c.Node.ID
			}
			if err != nil || !slices.Equal(ids, tc.want) {
				t.Fatalf("place = %v, %v; want %v", ids, err, tc.want)
			}
			if elapsed > tc.maxTime {
				t.Fatalf("place took %v, want at most %v", elapsed, tc.maxTime)
			}
		})
	}
}
