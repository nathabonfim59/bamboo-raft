package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// genUnitStore implements only the generation calls takeGeneration makes.
// The embedded nil Store makes any other call panic, failing the test.
type genUnitStore struct {
	Store

	mu        sync.Mutex
	chain     uuid.UUID
	node      string
	beginErrs []error // consumed per BeginGeneration call; nil entries succeed
	beginArgs []db.BeginGenerationParams
	next      uint32
	begun     []db.Generation
	completed []genUnitFinish
	failed    []genUnitFinish
	markedOK  []string // nodes MarkNodeObjectStore(ok=true) was called for
}

func (s *genUnitStore) MarkNodeObjectStore(_ context.Context, nodeID string, ok bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ok {
		s.markedOK = append(s.markedOK, nodeID)
	}
	return nil
}

type genUnitFinish struct {
	id     uuid.UUID
	reason string
	next   time.Time
}

func (s *genUnitStore) BeginGeneration(_ context.Context, p db.BeginGenerationParams) (db.Generation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beginArgs = append(s.beginArgs, p)
	if len(s.beginErrs) > 0 {
		err := s.beginErrs[0]
		s.beginErrs = s.beginErrs[1:]
		if err != nil {
			return db.Generation{}, err
		}
	}
	g := db.Generation{
		ID: uuid.NewV7(), SandboxID: p.SandboxID, ChainID: s.chain, Number: s.next,
		Status: db.GenerationPending, NodeID: s.node,
		ManifestKey: snapformat.ManifestKey(s.chain.String(), s.next),
		StartedAt:   time.Now(), DeadlineAt: time.Now().Add(p.Timeout),
		JobID: p.JobID, LeaseToken: p.LeaseToken,
	}
	s.next++
	s.begun = append(s.begun, g)
	return g, nil
}

func (s *genUnitStore) CompleteGeneration(_ context.Context, id uuid.UUID, r db.GenerationResult, next time.Time) (db.Generation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completed = append(s.completed, genUnitFinish{id: id, next: next})
	for _, g := range s.begun {
		if g.ID == id {
			g.Status = db.GenerationCompleted
			g.CutAt = new(r.CutAt)
			return g, nil
		}
	}
	return db.Generation{}, db.ErrNotFound
}

func (s *genUnitStore) FailGeneration(_ context.Context, id uuid.UUID, reason string, next time.Time) (db.Generation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed = append(s.failed, genUnitFinish{id: id, reason: reason, next: next})
	return db.Generation{ID: id, Status: db.GenerationFailed, Error: reason}, nil
}

// genUnitNode scripts SnapshotVM; every other node call panics.
type genUnitNode struct {
	nodev1.NodeClient

	mu      sync.Mutex
	replies []func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error)
	calls   []*nodev1.SnapshotVMRequest
}

func (n *genUnitNode) SnapshotVM(ctx context.Context, req *nodev1.SnapshotVMRequest, _ ...grpc.CallOption) (*nodev1.Generation, error) {
	n.mu.Lock()
	n.calls = append(n.calls, req)
	reply := genUnitOK
	if len(n.replies) > 0 {
		reply = n.replies[0]
		n.replies = n.replies[1:]
	}
	n.mu.Unlock()
	return reply(ctx, req)
}

// genUnitOK answers like a node that wrote the requested manifest.
func genUnitOK(_ context.Context, req *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
	return &nodev1.Generation{
		ManifestKey: snapformat.ManifestKey(req.GetChainId(), req.GetGeneration()),
		ChainId:     req.GetChainId(), Generation: req.GetGeneration(),
		Image: "img", DiskBytes: 1 << 30, Chunks: 3, ChunksUploaded: 2,
		BytesUploaded: 100, ContentBytes: 200, CreatedAtUnixMs: time.Now().UnixMilli(), Frozen: true,
	}, nil
}

// genUnitCode answers one gRPC status code.
func genUnitCode(code codes.Code) func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
	return func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
		return nil, status.Error(code, "scripted")
	}
}

// genLockedBuffer collects log output from concurrent writers.
type genLockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *genLockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *genLockedBuffer) count(s string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Count(b.buf.String(), s)
}

const genUnitInterval = time.Minute

// genUnitWorker builds a Worker over the fakes with jitter fixed at rnd.
func genUnitWorker(t *testing.T, store *genUnitStore, node *genUnitNode, rnd float64, logs io.Writer) *Worker {
	t.Helper()
	w, err := New(Options{
		Store: store,
		Dial: func(string) (NodeClient, io.Closer, error) {
			return node, io.NopCloser(nil), nil
		},
		Logger:           slog.New(slog.NewTextHandler(logs, nil)),
		ObjectStore:      memstore.New(),
		SnapshotInterval: genUnitInterval,
		SnapshotTimeout:  time.Second,
		SnapshotRand:     func() float64 { return rnd },
	})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// genUnitLease is a live claim whose loss the test triggers. Its job carries
// the id and lease token takeGeneration must pass to BeginGeneration.
func genUnitLease(t *testing.T) *Lease {
	t.Helper()
	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })
	return &Lease{
		job:    db.Job{ID: uuid.NewV7(), LeaseToken: uuid.NewV7()},
		ctx:    ctx,
		cancel: cancel,
	}
}

func genUnitSandbox(node string) db.Sandbox {
	return db.Sandbox{
		ID: "bx_23456789", OwnerID: uuid.NewV7(), State: db.StateReady,
		NodeID: new(node), SnapshotsEnabled: true,
	}
}

func TestNextSnapshotAtJitterBounds(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		rnd  float64
		want time.Duration
	}{
		{0, 54 * time.Second},             // interval - 10 %
		{0.5, 60 * time.Second},           // no jitter
		{0.75, 63 * time.Second},          // +5 %
		{0.999, 65988 * time.Millisecond}, // just under +10 %
	}
	for _, test := range tests {
		w := genUnitWorker(t, &genUnitStore{}, &genUnitNode{}, test.rnd, io.Discard)
		got := w.nextSnapshotAt(now).Sub(now)
		if diff := got - test.want; diff < -time.Millisecond || diff > time.Millisecond {
			t.Errorf("rand %v: next in %v, want %v", test.rnd, got, test.want)
		}
		if got < 54*time.Second || got > 66*time.Second {
			t.Errorf("rand %v: next in %v is outside interval ±10 %%", test.rnd, got)
		}
	}
}

func TestTakeGenerationOutcomes(t *testing.T) {
	const node = "node-a"
	sweep := []error{db.ErrGCSweeping}
	tests := []struct {
		name      string
		beginErrs []error
		replies   []func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error)
		outcome   generationOutcome
		calls     int      // SnapshotVM calls
		numbers   []uint32 // generation numbers sent to the node
		completed int
		failed    []string // fail reasons, in order
		// backoff is the expected distance of the LAST finish's next time
		// from now: zero skips the check.
		backoff time.Duration
	}{
		{name: "success", outcome: generationCompleted, calls: 1, numbers: []uint32{0}, completed: 1, backoff: genUnitInterval},
		{
			name:    "already exists bumps the number",
			replies: []func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error){genUnitCode(codes.AlreadyExists)},
			outcome: generationCompleted, calls: 2, numbers: []uint32{0, 1}, completed: 1, failed: []string{"exists"},
		},
		{
			name: "already exists is bounded",
			replies: []func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error){
				genUnitCode(codes.AlreadyExists), genUnitCode(codes.AlreadyExists), genUnitCode(codes.AlreadyExists),
			},
			outcome: generationFailed, calls: 3, numbers: []uint32{0, 1, 2}, failed: []string{"exists", "exists", "exists"},
		},
		{
			name:    "failed precondition backs off ten intervals",
			replies: []func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error){genUnitCode(codes.FailedPrecondition)},
			outcome: generationRefused, calls: 1, numbers: []uint32{0}, failed: []string{"precondition"},
			backoff: snapshotRefusedFactor * genUnitInterval,
		},
		{
			name:    "generic failure keeps the schedule",
			replies: []func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error){genUnitCode(codes.Unavailable)},
			outcome: generationFailed, calls: 1, numbers: []uint32{0}, failed: []string{"node_unavailable"},
			backoff: genUnitInterval,
		},
		{
			name:    "missing VM",
			replies: []func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error){genUnitCode(codes.NotFound)},
			outcome: generationFailed, calls: 1, numbers: []uint32{0}, failed: []string{"vm_missing"},
		},
		{
			name: "reply for another manifest is refused",
			replies: []func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error){
				func(ctx context.Context, req *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
					g, _ := genUnitOK(ctx, req)
					g.Generation++
					return g, nil
				},
			},
			outcome: generationFailed, calls: 1, numbers: []uint32{0}, failed: []string{"bad_reply"},
		},
		{name: "gc sweeping", beginErrs: sweep, outcome: generationSweeping},
		{name: "pending row exists", beginErrs: []error{db.ErrConflict}, outcome: generationInFlight},
		{name: "sandbox gone", beginErrs: []error{db.ErrNotFound}, outcome: generationSkipped},
		{name: "snapshots off", beginErrs: []error{db.ErrSnapshotsDisabled}, outcome: generationSkipped},
		{name: "no node", beginErrs: []error{db.ErrInvalidState}, outcome: generationSkipped},
		{name: "store failure", beginErrs: []error{errors.New("db down")}, outcome: generationStoreError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &genUnitStore{chain: uuid.NewV7(), node: node, beginErrs: test.beginErrs}
			nodeClient := &genUnitNode{replies: test.replies}
			w := genUnitWorker(t, store, nodeClient, 0.5, io.Discard)
			before := time.Now()
			att := w.takeGeneration(t.Context(), genUnitLease(t), &db.SandboxGuard{}, t.Context(), genUnitSandbox(node))
			if att.outcome != test.outcome {
				t.Fatalf("outcome = %d (%s), want %d", att.outcome, att.reason, test.outcome)
			}
			if len(nodeClient.calls) != test.calls {
				t.Fatalf("SnapshotVM calls = %d, want %d", len(nodeClient.calls), test.calls)
			}
			for i, call := range nodeClient.calls {
				if call.GetGeneration() != test.numbers[i] || call.GetChainId() != store.chain.String() ||
					call.GetSandboxId() != "bx_23456789" || !call.GetFreeze() {
					t.Errorf("call %d = %+v, want generation %d on chain %s with freeze", i, call, test.numbers[i], store.chain)
				}
			}
			if len(store.completed) != test.completed {
				t.Errorf("completed = %d, want %d", len(store.completed), test.completed)
			}
			// A completed generation proves the node's object store works.
			if want := min(test.completed, 1); len(store.markedOK) != want || (want == 1 && store.markedOK[0] != node) {
				t.Errorf("nodes marked usable = %v, want %d mark of %s", store.markedOK, want, node)
			}
			var reasons []string
			for _, f := range store.failed {
				reasons = append(reasons, f.reason)
			}
			if strings.Join(reasons, ",") != strings.Join(test.failed, ",") {
				t.Errorf("fail reasons = %v, want %v", reasons, test.failed)
			}
			if test.backoff > 0 {
				last := append(append([]genUnitFinish(nil), store.failed...), store.completed...)
				next := last[len(last)-1].next
				if d := next.Sub(before); d < test.backoff || d > test.backoff+time.Second {
					t.Errorf("next attempt in %v, want %v", d, test.backoff)
				}
			}
		})
	}
}

// TestTakeGenerationBeginsUnderItsClaim proves the row Begin creates is owned
// by the calling job and claim, never by an anonymous attempt.
func TestTakeGenerationBeginsUnderItsClaim(t *testing.T) {
	store := &genUnitStore{chain: uuid.NewV7(), node: "node-a"}
	w := genUnitWorker(t, store, &genUnitNode{}, 0.5, io.Discard)
	lease := genUnitLease(t)
	sbx := genUnitSandbox("node-a")
	if att := w.takeGeneration(t.Context(), lease, &db.SandboxGuard{}, t.Context(), sbx); att.outcome != generationCompleted {
		t.Fatalf("outcome = %d (%s), want completed", att.outcome, att.reason)
	}
	if len(store.beginArgs) != 1 {
		t.Fatalf("BeginGeneration calls = %d, want 1", len(store.beginArgs))
	}
	got := store.beginArgs[0]
	job := lease.Job()
	if got.Owner != sbx.OwnerID || got.SandboxID != sbx.ID || got.Timeout != time.Second ||
		got.JobID != job.ID || got.LeaseToken != job.LeaseToken {
		t.Fatalf("begin params = %+v, want owner %v, sandbox %s, timeout %v, job %v/%v",
			got, sbx.OwnerID, sbx.ID, time.Second, job.ID, job.LeaseToken)
	}
}

// TestTakeGenerationLeaseLostNeverCompletes loses the claim while SnapshotVM
// runs: the attempt must neither complete nor fail its row (the reaper owns
// it once the deadline passed).
func TestTakeGenerationLeaseLostNeverCompletes(t *testing.T) {
	store := &genUnitStore{chain: uuid.NewV7(), node: "node-a"}
	lease := genUnitLease(t)
	nodeClient := &genUnitNode{replies: []func(context.Context, *nodev1.SnapshotVMRequest) (*nodev1.Generation, error){
		func(ctx context.Context, req *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
			// The node finished, but the claim was lost meanwhile.
			lease.cancel(ErrLeaseLost)
			return genUnitOK(ctx, req)
		},
	}}
	w := genUnitWorker(t, store, nodeClient, 0.5, io.Discard)
	att := w.takeGeneration(t.Context(), lease, &db.SandboxGuard{}, lease.Context(), genUnitSandbox("node-a"))
	if att.outcome != generationAbandoned {
		t.Fatalf("outcome = %d, want abandoned", att.outcome)
	}
	if len(store.completed) != 0 || len(store.failed) != 0 {
		t.Fatalf("stale attempt finished its row: completed %v failed %v", store.completed, store.failed)
	}
}

// TestTakeGenerationMovedSandbox fails the row Begin placed on another node
// without calling SnapshotVM.
func TestTakeGenerationMovedSandbox(t *testing.T) {
	store := &genUnitStore{chain: uuid.NewV7(), node: "node-b"}
	nodeClient := &genUnitNode{}
	w := genUnitWorker(t, store, nodeClient, 0.5, io.Discard)
	att := w.takeGeneration(t.Context(), genUnitLease(t), &db.SandboxGuard{}, t.Context(), genUnitSandbox("node-a"))
	if att.outcome != generationSkipped || len(nodeClient.calls) != 0 {
		t.Fatalf("outcome = %d, calls = %d; want skipped without a node call", att.outcome, len(nodeClient.calls))
	}
	if len(store.failed) != 1 || store.failed[0].reason != "node_changed" {
		t.Fatalf("failed = %+v, want one node_changed", store.failed)
	}
}

func TestWarnSnapshotRefusedOncePerSandboxAndNode(t *testing.T) {
	logs := &genLockedBuffer{}
	w := genUnitWorker(t, &genUnitStore{}, &genUnitNode{}, 0.5, logs)
	a := genUnitSandbox("node-a")
	for range 3 {
		w.warnSnapshotRefused(a)
	}
	if n := logs.count("node cannot snapshot sandbox"); n != 1 {
		t.Fatalf("warnings = %d, want 1", n)
	}
	moved := a
	moved.NodeID = new("node-b")
	w.warnSnapshotRefused(moved) // another node: a new warning
	w.warnSnapshotRefused(moved)
	if n := logs.count("node cannot snapshot sandbox"); n != 2 {
		t.Fatalf("warnings after a move = %d, want 2", n)
	}
	w.precondWarned.forget(a.ID) // snapshotted again or left ready
	w.warnSnapshotRefused(moved)
	if n := logs.count("node cannot snapshot sandbox"); n != 3 {
		t.Fatalf("warnings after forget = %d, want 3", n)
	}
}

func TestWarnOnceIsBounded(t *testing.T) {
	var o warnOnce
	for i := range warnOnceCap + 10 {
		o.mark("bx_"+strconv.Itoa(i), "n")
	}
	if len(o.seen) > warnOnceCap {
		t.Fatalf("entries = %d, cap %d", len(o.seen), warnOnceCap)
	}
}

func TestGenerationFailureCodes(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{status.Error(codes.NotFound, "x"), "vm_missing"},
		{status.Error(codes.DeadlineExceeded, "x"), "timeout"},
		{context.DeadlineExceeded, "timeout"},
		{status.Error(codes.Unavailable, "secret node text"), "node_unavailable"},
		{status.Error(codes.ResourceExhausted, "x"), "disk_full"},
		{status.Error(codes.InvalidArgument, "x"), "rejected"},
		{status.Error(codes.Internal, "x"), "failed"},
		{errors.New("other"), "failed"},
	}
	for _, test := range tests {
		if got := generationFailure(test.err); got != test.want {
			t.Errorf("generationFailure(%v) = %q, want %q", test.err, got, test.want)
		}
	}
}

func TestGenerationResultChecksTheReply(t *testing.T) {
	chain := uuid.NewV7()
	gen := db.Generation{ChainID: chain, Number: 4, ManifestKey: snapformat.ManifestKey(chain.String(), 4)}
	good := func() *nodev1.Generation {
		g, _ := genUnitOK(context.Background(), &nodev1.SnapshotVMRequest{ChainId: chain.String(), Generation: 4})
		return g
	}
	tests := []struct {
		name   string
		mutate func(*nodev1.Generation)
		ok     bool
	}{
		{"matching", func(*nodev1.Generation) {}, true},
		{"other key", func(g *nodev1.Generation) { g.ManifestKey = "manifests/x/4.json.zst" }, false},
		{"other chain", func(g *nodev1.Generation) { g.ChainId = uuid.NewV7().String() }, false},
		{"other number", func(g *nodev1.Generation) { g.Generation = 5 }, false},
		{"no cut time", func(g *nodev1.Generation) { g.CreatedAtUnixMs = 0 }, false},
	}
	for _, test := range tests {
		reply := good()
		test.mutate(reply)
		res, ok := generationResult(reply, gen)
		if ok != test.ok {
			t.Errorf("%s: ok = %v, want %v", test.name, ok, test.ok)
		}
		if ok && (res.Chunks != 3 || res.BytesUploaded != 100 || res.ContentBytes != 200 || !res.Frozen || res.CutAt.IsZero()) {
			t.Errorf("%s: result = %+v", test.name, res)
		}
	}
}

func TestPhase1ModeSchedulesNothing(t *testing.T) {
	w, err := New(Options{
		Store:  &genUnitStore{}, // any store call would panic
		Dial:   func(string) (NodeClient, io.Closer, error) { return nil, nil, errors.New("no dial") },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := w.EnqueueDueSnapshots(t.Context()); n != 0 || err != nil {
		t.Fatalf("EnqueueDueSnapshots = %d, %v; want 0, nil", n, err)
	}
	job := db.Job{Kind: db.JobSnapshot, SandboxID: new("bx_23456789")}
	if err := w.snapshotSandbox(t.Context(), job, genUnitLease(t)); err != nil {
		t.Fatalf("stray snapshot job in Phase 1 mode = %v, want a no-op", err)
	}
}
