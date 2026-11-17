package snapshots

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// pluginNode starts one fake node behind a real in-process gRPC server and
// returns both the server-side fake (for seeding state) and the generated
// client the plugin consumes.
func pluginNode(t *testing.T, id string) (*nodefake.Node, nodev1.NodeClient) {
	t.Helper()
	fake := nodefake.New(id, t.TempDir())
	fake.VCPUs, fake.MemoryMiB = 8, 8192
	srv := grpc.NewServer()
	fake.Register(srv)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() { _ = srv.Serve(ln); close(done) }()
	t.Cleanup(func() { srv.Stop(); <-done })
	cc, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc client: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return fake, nodev1.NewNodeClient(cc)
}

func pluginQuiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func pluginArtifact(c byte) string { return "snap-" + strings.Repeat(string(c), 32) }

// pluginStore is a narrow Store fake: every hook is optional and a missing one
// returns the empty value, so a test states exactly the calls it expects.
type pluginStore struct {
	failedJobs    func(context.Context, *db.FailedJobCursor, int) ([]db.Job, *db.FailedJobCursor, error)
	saving        func(context.Context, *db.SnapshotCleanupCursor, int) ([]db.SavedSnapshot, *db.SnapshotCleanupCursor, error)
	reconcile     func(context.Context, db.ReconcileSnapshotSaveFailureParams) (db.SavedSnapshot, bool, error)
	releasePins   func(context.Context, *db.SnapshotPinCursor, int) (int, *db.SnapshotPinCursor, error)
	purgeable     func(context.Context, *db.SnapshotCleanupCursor, int) ([]db.SavedSnapshot, *db.SnapshotCleanupCursor, error)
	nodeIDs       func(context.Context) ([]string, error)
	nodeArtifacts func(context.Context, string, *db.SnapshotCleanupCursor, int) ([]db.SavedSnapshot, *db.SnapshotCleanupCursor, error)
	byArtifact    func(context.Context, string) (db.SavedSnapshot, bool, error)
	reopen        func(context.Context, uuid.UUID, uuid.UUID, string) (db.SavedSnapshot, bool, error)
	enqueueDelete func(context.Context, uuid.UUID, uuid.UUID) (bool, error)
	markedPurged  func(context.Context, uuid.UUID, uuid.UUID, string) (db.SavedSnapshot, error)
	snapshot      func(context.Context, uuid.UUID, uuid.UUID) (db.SavedSnapshot, error)
	sandboxAny    func(context.Context, uuid.UUID, string) (db.Sandbox, error)
	acquireGuard  func(context.Context, uuid.UUID, string) (*db.SandboxGuard, error)
	completeSave  func(context.Context, db.CompleteSnapshotSaveParams) (db.SavedSnapshot, bool, error)
	failSave      func(context.Context, db.FailSnapshotSaveParams) (db.SavedSnapshot, bool, error)
}

func (s *pluginStore) SavedSnapshot(ctx context.Context, owner uuid.UUID, id uuid.UUID) (db.SavedSnapshot, error) {
	if s.snapshot == nil {
		return db.SavedSnapshot{}, db.ErrNotFound
	}
	return s.snapshot(ctx, owner, id)
}

func (s *pluginStore) SandboxAny(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error) {
	if s.sandboxAny == nil {
		return db.Sandbox{}, db.ErrNotFound
	}
	return s.sandboxAny(ctx, owner, id)
}

func (s *pluginStore) AcquireSandboxGuard(ctx context.Context, owner uuid.UUID, id string) (*db.SandboxGuard, error) {
	if s.acquireGuard == nil {
		return nil, errors.New("fake: no guard")
	}
	return s.acquireGuard(ctx, owner, id)
}

func (s *pluginStore) CompleteSnapshotSave(ctx context.Context, p db.CompleteSnapshotSaveParams) (db.SavedSnapshot, bool, error) {
	if s.completeSave == nil {
		return db.SavedSnapshot{}, false, db.ErrJobNotLeased
	}
	return s.completeSave(ctx, p)
}

func (s *pluginStore) FailSnapshotSave(ctx context.Context, p db.FailSnapshotSaveParams) (db.SavedSnapshot, bool, error) {
	if s.failSave == nil {
		return db.SavedSnapshot{}, false, db.ErrJobNotLeased
	}
	return s.failSave(ctx, p)
}

func (s *pluginStore) ReconcileSnapshotSaveFailure(ctx context.Context, p db.ReconcileSnapshotSaveFailureParams) (db.SavedSnapshot, bool, error) {
	if s.reconcile == nil {
		return db.SavedSnapshot{}, false, nil
	}
	return s.reconcile(ctx, p)
}

func (s *pluginStore) FailedJobs(ctx context.Context, after *db.FailedJobCursor, limit int) ([]db.Job, *db.FailedJobCursor, error) {
	if s.failedJobs == nil {
		return nil, nil, nil
	}
	return s.failedJobs(ctx, after, limit)
}

func (s *pluginStore) SavingSnapshots(ctx context.Context, after *db.SnapshotCleanupCursor, limit int) ([]db.SavedSnapshot, *db.SnapshotCleanupCursor, error) {
	if s.saving == nil {
		return nil, nil, nil
	}
	return s.saving(ctx, after, limit)
}

func (s *pluginStore) PurgeableSnapshots(ctx context.Context, after *db.SnapshotCleanupCursor, limit int) ([]db.SavedSnapshot, *db.SnapshotCleanupCursor, error) {
	if s.purgeable == nil {
		return nil, nil, nil
	}
	return s.purgeable(ctx, after, limit)
}

func (s *pluginStore) CleanupArtifactsOnNode(ctx context.Context, nodeID string, after *db.SnapshotCleanupCursor, limit int) ([]db.SavedSnapshot, *db.SnapshotCleanupCursor, error) {
	if s.nodeArtifacts == nil {
		return nil, nil, nil
	}
	return s.nodeArtifacts(ctx, nodeID, after, limit)
}

func (s *pluginStore) SavedSnapshotByArtifact(ctx context.Context, artifactID string) (db.SavedSnapshot, bool, error) {
	if s.byArtifact == nil {
		return db.SavedSnapshot{}, false, nil
	}
	return s.byArtifact(ctx, artifactID)
}

func (s *pluginStore) ReopenSnapshotCleanup(ctx context.Context, owner uuid.UUID, id uuid.UUID, artifact string) (db.SavedSnapshot, bool, error) {
	if s.reopen == nil {
		return db.SavedSnapshot{}, false, db.ErrNotFound
	}
	return s.reopen(ctx, owner, id, artifact)
}

func (s *pluginStore) EnqueueSnapshotDelete(ctx context.Context, owner uuid.UUID, id uuid.UUID) (bool, error) {
	if s.enqueueDelete == nil {
		return false, nil
	}
	return s.enqueueDelete(ctx, owner, id)
}

func (s *pluginStore) MarkSnapshotPurged(ctx context.Context, owner uuid.UUID, id uuid.UUID, artifact string) (db.SavedSnapshot, error) {
	if s.markedPurged == nil {
		return db.SavedSnapshot{}, db.ErrNotFound
	}
	return s.markedPurged(ctx, owner, id, artifact)
}

func (s *pluginStore) ReleaseEligibleSnapshotPins(ctx context.Context, after *db.SnapshotPinCursor, limit int) (int, *db.SnapshotPinCursor, error) {
	if s.releasePins == nil {
		return 0, nil, nil
	}
	return s.releasePins(ctx, after, limit)
}

func (s *pluginStore) NodeIDs(ctx context.Context) ([]string, error) {
	if s.nodeIDs == nil {
		return nil, nil
	}
	return s.nodeIDs(ctx)
}

// dialOne returns a Dial that always hands back the same client and closer.
func dialOne(client lifecycle.NodeClient, closer io.Closer) lifecycle.Dial {
	return func(string) (lifecycle.NodeClient, io.Closer, error) { return client, closer, nil }
}

// wrongInfoClient overrides Info while delegating every other RPC.
type wrongInfoClient struct {
	nodev1.NodeClient
	info *nodev1.InfoResponse
}

func (n wrongInfoClient) Info(context.Context, *nodev1.InfoRequest, ...grpc.CallOption) (*nodev1.InfoResponse, error) {
	return n.info, nil
}

// countCloser records Close calls.
type countCloser struct{ closed int }

func (c *countCloser) Close() error { c.closed++; return nil }

func TestDialNodeNilAndCloserSafety(t *testing.T) {
	_, base := pluginNode(t, "node-a")
	nilClientCloser := &countCloser{}
	errorCloser := &countCloser{}
	tests := []struct {
		name      string
		dial      lifecycle.Dial
		wantErr   bool
		closer    *countCloser
		wantClose int
	}{
		{
			name: "nil client closes closer",
			dial: func(string) (lifecycle.NodeClient, io.Closer, error) {
				return nil, nilClientCloser, nil
			},
			wantErr: true, closer: nilClientCloser, wantClose: 1,
		},
		{
			name: "error closes nonnil closer",
			dial: func(string) (lifecycle.NodeClient, io.Closer, error) {
				return nil, errorCloser, errors.New("dial failed")
			},
			wantErr: true, closer: errorCloser, wantClose: 1,
		},
		{
			name: "nil closer is safe",
			dial: func(string) (lifecycle.NodeClient, io.Closer, error) { return base, nil, nil },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, closeFn, err := dialNode(tc.dial, "node-a")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			closeFn()
			if tc.closer != nil && tc.closer.closed != tc.wantClose {
				t.Fatalf("closes = %d, want %d", tc.closer.closed, tc.wantClose)
			}
		})
	}
}

func TestValidateSaveBindings(t *testing.T) {
	owner := uuid.NewV7()
	sandboxID := "bx_23456789"
	nodeID := "node-a"
	artifact := pluginArtifact('a')
	job := db.Job{
		ID: uuid.NewV7(), OwnerID: owner, SandboxID: &sandboxID,
		SnapshotID: new(uuid.NewV7()), LeasedBy: "worker", LeaseToken: uuid.NewV7(),
	}
	payload := db.SaveSnapshotPayload{
		SnapshotID: *job.SnapshotID, NodeID: nodeID, VMID: sandboxID,
		ArtifactID: artifact, Generation: 1,
	}
	snap := db.SavedSnapshot{
		ID: *job.SnapshotID, OwnerID: owner, SandboxID: sandboxID, ArtifactID: artifact,
		Generation: 1, NodeID: &nodeID,
	}
	sbx := db.Sandbox{ID: sandboxID, OwnerID: owner, NodeID: &nodeID}

	if err := validateSaveArtifactJob(job, payload, snap); err != nil {
		t.Fatalf("baseline artifact binding: %v", err)
	}
	if err := validateSaveSourceJob(job, payload, sbx); err != nil {
		t.Fatalf("baseline source binding: %v", err)
	}

	other := uuid.NewV7()
	otherText := "other"
	tamperedJob := []struct {
		name   string
		mutate func(*db.Job)
	}{
		{name: "job snapshot id", mutate: func(j *db.Job) { j.SnapshotID = &other }},
		{name: "job owner", mutate: func(j *db.Job) { j.OwnerID = other }},
		{name: "job sandbox", mutate: func(j *db.Job) { j.SandboxID = &otherText }},
	}
	for _, tc := range tamperedJob {
		t.Run(tc.name, func(t *testing.T) {
			bad := job
			tc.mutate(&bad)
			if err := validateSaveArtifactJob(bad, payload, snap); err == nil {
				t.Fatal("tampered job accepted")
			}
		})
	}

	tamperedPayload := []struct {
		name   string
		mutate func(*db.SaveSnapshotPayload)
	}{
		{name: "payload snapshot id", mutate: func(p *db.SaveSnapshotPayload) { p.SnapshotID = other }},
		{name: "payload vmid", mutate: func(p *db.SaveSnapshotPayload) { p.VMID = otherText }},
		{name: "payload node", mutate: func(p *db.SaveSnapshotPayload) { p.NodeID = otherText }},
		{name: "payload artifact", mutate: func(p *db.SaveSnapshotPayload) { p.ArtifactID = pluginArtifact('b') }},
		{name: "payload generation", mutate: func(p *db.SaveSnapshotPayload) { p.Generation++ }},
	}
	for _, tc := range tamperedPayload {
		t.Run(tc.name, func(t *testing.T) {
			bad := payload
			tc.mutate(&bad)
			if err := validateSaveArtifactJob(job, bad, snap); err == nil {
				t.Fatal("tampered payload accepted")
			}
		})
	}

	tamperedRow := []struct {
		name   string
		mutate func(*db.SavedSnapshot)
	}{
		{name: "row id", mutate: func(s *db.SavedSnapshot) { s.ID = other }},
		{name: "row owner", mutate: func(s *db.SavedSnapshot) { s.OwnerID = other }},
		{name: "row sandbox", mutate: func(s *db.SavedSnapshot) { s.SandboxID = otherText }},
		{name: "row artifact", mutate: func(s *db.SavedSnapshot) { s.ArtifactID = pluginArtifact('c') }},
		{name: "row generation", mutate: func(s *db.SavedSnapshot) { s.Generation = 2 }},
		{name: "row node", mutate: func(s *db.SavedSnapshot) { s.NodeID = &otherText }},
		{name: "row missing node", mutate: func(s *db.SavedSnapshot) { s.NodeID = nil }},
	}
	for _, tc := range tamperedRow {
		t.Run(tc.name, func(t *testing.T) {
			bad := snap
			tc.mutate(&bad)
			if err := validateSaveArtifactJob(job, payload, bad); err == nil {
				t.Fatal("tampered row accepted")
			}
		})
	}

	badSource := sbx
	badSource.OwnerID = other
	if err := validateSaveSourceJob(job, payload, badSource); err == nil {
		t.Fatal("foreign source accepted")
	}
	badSource = sbx
	badSource.NodeID = &otherText
	if err := validateSaveSourceJob(job, payload, badSource); err == nil {
		t.Fatal("source on another node accepted")
	}

	// The canonical artifact id must be valid even when the payload and row
	// agree: this is what stops a tampered pair before any node RPC.
	unsafePayload, unsafeRow := payload, snap
	unsafePayload.ArtifactID = "not-a-snapshot"
	unsafeRow.ArtifactID = unsafePayload.ArtifactID
	if err := validateSaveArtifactJob(job, unsafePayload, unsafeRow); err == nil {
		t.Fatal("paired unsafe artifact id accepted")
	}
}

func TestValidateDeleteBinding(t *testing.T) {
	owner := uuid.NewV7()
	sandboxID := "bx_23456789"
	nodeID := "node-a"
	artifact := pluginArtifact('a')
	rowID := uuid.NewV7()
	job := db.Job{ID: uuid.NewV7(), OwnerID: owner, SandboxID: &sandboxID, SnapshotID: &rowID}
	payload := db.DeleteSnapshotPayload{SnapshotID: rowID, NodeID: nodeID, ArtifactID: artifact}
	row := db.SavedSnapshot{ID: rowID, OwnerID: owner, SandboxID: sandboxID, ArtifactID: artifact, NodeID: &nodeID}
	if err := validateDeleteJob(job, payload, row); err != nil {
		t.Fatalf("baseline: %v", err)
	}
	other := uuid.NewV7()
	otherText := "other"
	mutations := []struct {
		name   string
		mutate func(*db.Job, *db.DeleteSnapshotPayload, *db.SavedSnapshot)
	}{
		{name: "job id", mutate: func(j *db.Job, _ *db.DeleteSnapshotPayload, _ *db.SavedSnapshot) { j.SnapshotID = &other }},
		{name: "job owner", mutate: func(j *db.Job, _ *db.DeleteSnapshotPayload, _ *db.SavedSnapshot) { j.OwnerID = other }},
		{name: "job sandbox", mutate: func(j *db.Job, _ *db.DeleteSnapshotPayload, _ *db.SavedSnapshot) { j.SandboxID = &otherText }},
		{name: "payload id", mutate: func(_ *db.Job, p *db.DeleteSnapshotPayload, _ *db.SavedSnapshot) { p.SnapshotID = other }},
		{name: "payload artifact", mutate: func(_ *db.Job, p *db.DeleteSnapshotPayload, _ *db.SavedSnapshot) { p.ArtifactID = pluginArtifact('b') }},
		{name: "payload node", mutate: func(_ *db.Job, p *db.DeleteSnapshotPayload, _ *db.SavedSnapshot) { p.NodeID = otherText }},
		{name: "row owner", mutate: func(_ *db.Job, _ *db.DeleteSnapshotPayload, r *db.SavedSnapshot) { r.OwnerID = other }},
		{name: "row artifact", mutate: func(_ *db.Job, _ *db.DeleteSnapshotPayload, r *db.SavedSnapshot) { r.ArtifactID = pluginArtifact('c') }},
		{name: "row node", mutate: func(_ *db.Job, _ *db.DeleteSnapshotPayload, r *db.SavedSnapshot) { r.NodeID = nil }},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			j, p, r := job, payload, row
			tc.mutate(&j, &p, &r)
			if err := validateDeleteJob(j, p, r); err == nil {
				t.Fatal("tampered delete binding accepted")
			}
		})
	}

	unsafePayload, unsafeRow := payload, row
	unsafePayload.ArtifactID = "not-a-snapshot"
	unsafeRow.ArtifactID = unsafePayload.ArtifactID
	if err := validateDeleteJob(job, unsafePayload, unsafeRow); err == nil {
		t.Fatal("paired unsafe delete artifact id accepted")
	}
}

func TestSaveOnNodeAlreadyExistsVerifiesInfo(t *testing.T) {
	ctx := t.Context()
	node, client := pluginNode(t, "node-a")
	if _, err := node.CreateVM(ctx, &nodev1.CreateVMRequest{
		SandboxId: "bx_23456789", Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10},
	}); err != nil {
		t.Fatalf("create vm: %v", err)
	}
	artifact := pluginArtifact('a')
	if _, err := node.SaveSnapshot(ctx, &nodev1.SaveSnapshotRequest{
		SandboxId: "bx_23456789", SnapshotId: artifact,
	}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}
	p := &Plugin{saveTimeout: time.Second, inventoryTimeout: time.Second}
	size, err := p.saveOnNode(ctx, client, "bx_23456789", artifact, "node-a")
	if err != nil || size != 0 {
		t.Fatalf("idempotent save = %d, %v", size, err)
	}

	// A known artifact whose source is another VM is an unexplained
	// collision, never success.
	if _, err := node.CreateVM(ctx, &nodev1.CreateVMRequest{
		SandboxId: "bx_other", Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 1024, DiskGib: 5},
	}); err != nil {
		t.Fatalf("create other vm: %v", err)
	}
	other := pluginArtifact('b')
	if _, err := node.SaveSnapshot(ctx, &nodev1.SaveSnapshotRequest{
		SandboxId: "bx_other", SnapshotId: other,
	}); err != nil {
		t.Fatalf("seed other snapshot: %v", err)
	}
	if _, err := p.saveOnNode(ctx, client, "bx_23456789", other, "node-a"); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("source mismatch = %v, want AlreadyExists", err)
	}

	// Negative node size is rejected.
	negative := wrongInfoClient{NodeClient: client, info: &nodev1.InfoResponse{
		NodeId: "node-a", Snapshots: []*nodev1.Snapshot{{
			SnapshotId: other, SourceSandboxId: "bx_23456789", SizeBytes: -1,
		}},
	}}
	if _, err := p.saveOnNode(ctx, negative, "bx_23456789", other, "node-a"); status.Code(err) != codes.Internal {
		t.Fatalf("negative size = %v, want Internal", err)
	}

	// Info from another node is not trusted.
	wrongNode := wrongInfoClient{NodeClient: client, info: &nodev1.InfoResponse{
		NodeId: "node-b", Snapshots: []*nodev1.Snapshot{{
			SnapshotId: other, SourceSandboxId: "bx_23456789",
		}},
	}}
	if _, err := p.saveOnNode(ctx, wrongNode, "bx_23456789", other, "node-a"); status.Code(err) != codes.Internal {
		t.Fatalf("wrong node info = %v, want Internal", err)
	}
}

func TestReconcileSavingUsesRequiredStoreScan(t *testing.T) {
	owner := uuid.NewV7()
	row := db.SavedSnapshot{
		ID: uuid.NewV7(), OwnerID: owner, Status: db.SnapshotSaving,
		ArtifactID: pluginArtifact('a'), Generation: 1,
	}
	var got db.ReconcileSnapshotSaveFailureParams
	store := &pluginStore{
		saving: func(context.Context, *db.SnapshotCleanupCursor, int) ([]db.SavedSnapshot, *db.SnapshotCleanupCursor, error) {
			return []db.SavedSnapshot{row}, nil, nil
		},
		reconcile: func(_ context.Context, p db.ReconcileSnapshotSaveFailureParams) (db.SavedSnapshot, bool, error) {
			got = p
			return row, true, nil
		},
	}
	reconciler, err := NewReconciler(ReconcilerOptions{Store: store, Dial: dialOne(nil, nil), Logger: pluginQuiet()})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	if err := reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if got.SnapshotID != row.ID || got.ArtifactID != row.ArtifactID || got.Reason != "save job missing" {
		t.Fatalf("missing-job repair snapshot=%s reason=%q", got.SnapshotID, got.Reason)
	}
}

func TestReconcileNodesClassifiesAndReports(t *testing.T) {
	newNode := func(id string) (*nodefake.Node, nodev1.NodeClient) {
		node, client := pluginNode(t, id)
		if _, err := node.CreateVM(t.Context(), &nodev1.CreateVMRequest{
			SandboxId: "bx_1", Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 1024, DiskGib: 5},
		}); err != nil {
			t.Fatalf("create vm: %v", err)
		}
		return node, client
	}

	t.Run("node id mismatch is incomplete", func(t *testing.T) {
		_, client := newNode("node-b")
		store := &pluginStore{nodeIDs: func(context.Context) ([]string, error) { return []string{"node-a"}, nil }}
		reconciler, err := NewReconciler(ReconcilerOptions{Store: store, Dial: dialOne(client, nil), Logger: pluginQuiet()})
		if err != nil {
			t.Fatal(err)
		}
		if err := reconciler.ReconcileOnce(t.Context()); !errors.Is(err, errSnapshotReconcile) {
			t.Fatalf("err = %v, want incomplete", err)
		}
	})

	t.Run("invalid artifact id is incomplete and never looked up", func(t *testing.T) {
		node, client := newNode("node-a")
		if _, err := node.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{
			SandboxId: "bx_1", SnapshotId: "not-a-snapshot",
		}); err != nil {
			t.Fatalf("seed invalid artifact: %v", err)
		}
		store := &pluginStore{
			nodeIDs: func(context.Context) ([]string, error) { return []string{"node-a"}, nil },
			byArtifact: func(context.Context, string) (db.SavedSnapshot, bool, error) {
				t.Fatal("invalid artifact id reached the store")
				return db.SavedSnapshot{}, false, nil
			},
		}
		reconciler, err := NewReconciler(ReconcilerOptions{Store: store, Dial: dialOne(client, nil), Logger: pluginQuiet()})
		if err != nil {
			t.Fatal(err)
		}
		if err := reconciler.ReconcileOnce(t.Context()); !errors.Is(err, errSnapshotReconcile) {
			t.Fatalf("err = %v, want incomplete", err)
		}
	})

	t.Run("unknown well formed artifact is preserved", func(t *testing.T) {
		node, client := newNode("node-a")
		artifact := pluginArtifact('a')
		if _, err := node.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{
			SandboxId: "bx_1", SnapshotId: artifact,
		}); err != nil {
			t.Fatalf("seed unknown artifact: %v", err)
		}
		enqueued := 0
		store := &pluginStore{
			nodeIDs:    func(context.Context) ([]string, error) { return []string{"node-a"}, nil },
			byArtifact: func(context.Context, string) (db.SavedSnapshot, bool, error) { return db.SavedSnapshot{}, false, nil },
			enqueueDelete: func(context.Context, uuid.UUID, uuid.UUID) (bool, error) {
				enqueued++
				return false, nil
			},
		}
		reconciler, err := NewReconciler(ReconcilerOptions{Store: store, Dial: dialOne(client, nil), Logger: pluginQuiet()})
		if err != nil {
			t.Fatal(err)
		}
		if err := reconciler.ReconcileOnce(t.Context()); err != nil {
			t.Fatalf("unknown artifact round = %v", err)
		}
		if enqueued != 0 {
			t.Fatalf("unknown artifact enqueued %d times", enqueued)
		}
	})
}

func TestReconcilerPageSize(t *testing.T) {
	dial := dialOne(nil, nil)
	for _, tc := range []struct {
		size    int
		wantErr bool
	}{
		{size: 0}, {size: 1}, {size: 500}, {size: 501, wantErr: true}, {size: -1, wantErr: true},
	} {
		_, err := NewReconciler(ReconcilerOptions{Store: &pluginStore{}, Dial: dial, Logger: pluginQuiet(), PageSize: tc.size})
		if (err != nil) != tc.wantErr {
			t.Fatalf("PageSize %d: err = %v, wantErr %v", tc.size, err, tc.wantErr)
		}
	}
}
