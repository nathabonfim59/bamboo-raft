package scheduler

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// replayStore lets the failFast final-replay test simulate a duplicate that
// commits while the admission budget expires.
type replayStore struct {
	replays int
	hit     db.CreateSandboxResult
}

func (s *replayStore) NodesWithCapacity(context.Context, db.Resources, time.Duration) ([]db.Node, error) {
	return []db.Node{{ID: "node-a"}}, nil
}

func (s *replayStore) CreateSandbox(context.Context, db.CreateSandboxParams) (db.CreateSandboxResult, error) {
	return db.CreateSandboxResult{}, db.ErrNoCapacity
}

func (s *replayStore) ReplayCreateSandbox(context.Context, uuid.UUID, string, []byte) (db.CreateSandboxResult, bool, error) {
	s.replays++
	if s.replays >= 2 {
		return s.hit, true, nil
	}
	return db.CreateSandboxResult{}, false, nil
}

// Sandbox and AcceptSandboxResume satisfy the resume-admission surface; the
// create-admission tests never resume.
func (s *replayStore) Sandbox(context.Context, uuid.UUID, string) (db.Sandbox, error) {
	return db.Sandbox{}, db.ErrNotFound
}

func (s *replayStore) AcceptSandboxResume(context.Context, db.ResumeParams) (db.ResumeAcceptance, error) {
	return db.ResumeAcceptance{}, db.ErrInvalidState
}

type blockingInfoClient struct{}

func (blockingInfoClient) Info(ctx context.Context, _ *nodev1.InfoRequest, _ ...grpc.CallOption) (*nodev1.InfoResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type cancelReplayStore struct {
	replays int
	hit     db.CreateSandboxResult
}

func (s *cancelReplayStore) NodesWithCapacity(context.Context, db.Resources, time.Duration) ([]db.Node, error) {
	return []db.Node{{ID: "node-a"}}, nil
}

func (s *cancelReplayStore) CreateSandbox(context.Context, db.CreateSandboxParams) (db.CreateSandboxResult, error) {
	return db.CreateSandboxResult{}, db.ErrNoCapacity
}

func (s *cancelReplayStore) ReplayCreateSandbox(ctx context.Context, _ uuid.UUID, _ string, _ []byte) (db.CreateSandboxResult, bool, error) {
	s.replays++
	if err := ctx.Err(); err != nil {
		return db.CreateSandboxResult{}, false, err
	}
	if s.replays >= 2 {
		return s.hit, true, nil
	}
	return db.CreateSandboxResult{}, false, nil
}

func (s *cancelReplayStore) Sandbox(context.Context, uuid.UUID, string) (db.Sandbox, error) {
	return db.Sandbox{}, db.ErrNotFound
}

func (s *cancelReplayStore) AcceptSandboxResume(context.Context, db.ResumeParams) (db.ResumeAcceptance, error) {
	return db.ResumeAcceptance{}, db.ErrInvalidState
}

type cancelingInfoClient struct{ cancel context.CancelFunc }

func (c cancelingInfoClient) Info(ctx context.Context, _ *nodev1.InfoRequest, _ ...grpc.CallOption) (*nodev1.InfoResponse, error) {
	c.cancel()
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestFinalReplayPreservesCallerCancellation(t *testing.T) {
	store := &cancelReplayStore{hit: db.CreateSandboxResult{
		Sandbox: db.Sandbox{ID: "bx_must_not_replay"}, Replayed: true,
	}}
	callerCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s, err := New(Options{
		Store:          store,
		Log:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		FailFastBudget: 5 * time.Second,
		Dial: func(string) (NodeClient, io.Closer, error) {
			return cancelingInfoClient{cancel: cancel}, io.NopCloser(nil), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Admit(callerCtx, AdmitRequest{
		ID: "bx_new", Owner: uuid.NewV7(), Reserve: db.Resources{VCPUs: 1},
		FailFast: true, IdempotencyKey: "key", RequestSHA256: []byte{1},
	})
	if res.Replayed {
		t.Fatal("replayed despite caller cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("admit error did not preserve caller cancellation: %v", err)
	}
}

func TestFailFastFinalKeyedReplayStillRuns(t *testing.T) {
	store := &replayStore{hit: db.CreateSandboxResult{
		Sandbox:  db.Sandbox{ID: "bx_replayed"},
		Replayed: true,
	}}
	s, err := New(Options{
		Store:          store,
		Log:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		FailFastBudget: 40 * time.Millisecond,
		Dial: func(string) (NodeClient, io.Closer, error) {
			return blockingInfoClient{}, io.NopCloser(nil), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	res, err := s.Admit(t.Context(), AdmitRequest{
		ID:             "bx_new",
		Owner:          uuid.NewV7(),
		Reserve:        db.Resources{VCPUs: 1},
		FailFast:       true,
		IdempotencyKey: "key",
		RequestSHA256:  []byte{1},
	})
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if !res.Replayed || res.Sandbox.ID != "bx_replayed" {
		t.Fatalf("final replay result: %+v", res)
	}
	if store.replays != 2 {
		t.Fatalf("replay calls: %d, want fast path + final recheck", store.replays)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("failFast final replay took %s", elapsed)
	}
}

type failingInfoClient struct{}

func (failingInfoClient) Info(context.Context, *nodev1.InfoRequest, ...grpc.CallOption) (*nodev1.InfoResponse, error) {
	return nil, status.Error(codes.Internal, "sentinel-dsn-password=do-not-log")
}

func TestProbeLogsFixedReasonNotNodeText(t *testing.T) {
	var buf bytes.Buffer
	store := &replayStore{}
	s, err := New(Options{
		Store: store,
		Log:   slog.New(slog.NewTextHandler(&buf, nil)),
		Dial: func(string) (NodeClient, io.Closer, error) {
			return failingInfoClient{}, io.NopCloser(nil), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Admit(t.Context(), AdmitRequest{
		ID:      "bx_new",
		Owner:   uuid.NewV7(),
		Reserve: db.Resources{VCPUs: 1},
	})
	if err == nil {
		t.Fatal("probe failure admitted")
	}
	logged := buf.String()
	if strings.Contains(logged, "sentinel-dsn-password") {
		t.Fatal("log echoed the raw node error sentinel")
	}
	if !strings.Contains(logged, "placement probe failed") || !strings.Contains(logged, "probe failed") {
		t.Fatal("log missing the fixed probe classification")
	}
}

// roomStore is a deterministic Store for the real Admit placement and restore
// branches. It never needs PostgreSQL.
type roomStore struct {
	nodes   []db.Node
	created []db.CreateSandboxParams
}

func (s *roomStore) NodesWithCapacity(context.Context, db.Resources, time.Duration) ([]db.Node, error) {
	return s.nodes, nil
}

func (s *roomStore) CreateSandbox(_ context.Context, p db.CreateSandboxParams) (db.CreateSandboxResult, error) {
	s.created = append(s.created, p)
	return db.CreateSandboxResult{Sandbox: db.Sandbox{ID: p.ID, OwnerID: p.Owner}}, nil
}

func (s *roomStore) ReplayCreateSandbox(context.Context, uuid.UUID, string, []byte) (db.CreateSandboxResult, bool, error) {
	return db.CreateSandboxResult{}, false, nil
}

func (s *roomStore) Sandbox(context.Context, uuid.UUID, string) (db.Sandbox, error) {
	return db.Sandbox{}, db.ErrNotFound
}

func (s *roomStore) AcceptSandboxResume(context.Context, db.ResumeParams) (db.ResumeAcceptance, error) {
	return db.ResumeAcceptance{}, db.ErrInvalidState
}

// roomNode serves one fixed live inventory through the scheduler's Info probe.
type roomNode struct{ info *nodev1.InfoResponse }

func (n roomNode) Info(context.Context, *nodev1.InfoRequest, ...grpc.CallOption) (*nodev1.InfoResponse, error) {
	return n.info, nil
}

// roomRestore resolves one saved snapshot onto one node.
type roomRestore struct {
	source  db.SnapshotSource
	machine string
	need    db.Resources
}

func (r roomRestore) ResolveRestore(context.Context, uuid.UUID, string, bool, string) (db.SnapshotSource, string, db.Resources, error) {
	return r.source, r.machine, r.need, nil
}

// roomScheduler wires a scheduler around a fixed live Info response and a
// buffer that captures its structured log.
func roomScheduler(t *testing.T, store Store, info *nodev1.InfoResponse, resolver RestoreResolver) (*Scheduler, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	s, err := New(Options{
		Store: store,
		Dial: func(string) (NodeClient, io.Closer, error) {
			return roomNode{info: info}, io.NopCloser(nil), nil
		},
		Log:     slog.New(slog.NewJSONHandler(&buf, nil)),
		Restore: resolver,
	})
	if err != nil {
		t.Fatalf("scheduler.New: %v", err)
	}
	return s, &buf
}

// roomLogRecord is the fixed structured scheduler log shape these tests read.
type roomLogRecord struct {
	Msg       string `json:"msg"`
	Node      string `json:"node"`
	Operation string `json:"operation"`
	Resource  string `json:"resource"`
	Need      uint64 `json:"need"`
	Offer     uint64 `json:"offer"`
	Unit      string `json:"unit"`
}

// roomLogRecords decodes every structured scheduler log line. Callers sweep
// secrets from the raw buffer before calling it.
func roomLogRecords(t *testing.T, buf *bytes.Buffer) []roomLogRecord {
	t.Helper()
	var records []roomLogRecord
	for line := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var record roomLogRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal("scheduler log line is not the expected JSON record")
		}
		records = append(records, record)
	}
	return records
}

// roomNoRoomRecords returns only the fixed no-room records.
func roomNoRoomRecords(t *testing.T, buf *bytes.Buffer) []roomLogRecord {
	t.Helper()
	var records []roomLogRecord
	for _, record := range roomLogRecords(t, buf) {
		if record.Msg == "node has no room" {
			records = append(records, record)
		}
	}
	return records
}

// roomInfo builds a live inventory. The CPU and QEMU fields carry secrets that
// must never reach the log.
func roomInfo(id string, vcpus uint32, memoryMib, diskFreeMib uint64) *nodev1.InfoResponse {
	return &nodev1.InfoResponse{
		NodeId:      id,
		Vcpus:       vcpus,
		MemoryMib:   memoryMib,
		DiskFreeMib: diskFreeMib,
		CpuModel:    "sentinel-info-secret",
		QemuVersion: "sentinel-qemu-secret",
	}
}

type roomShortfall struct {
	resource string
	need     uint64
	offer    uint64
	unit     string
}

// TestNoRoomLogNamesEveryShortResource exercises the real placement and restore
// Admit branches: each short dimension is logged once with operation, need,
// offer and a consistent unit; no create is attempted and secrets never leak.
func TestNoRoomLogNamesEveryShortResource(t *testing.T) {
	const nodeID = "node-1"
	need := db.Resources{VCPUs: 4, MemoryMiB: 1024, DiskGiB: 10}
	cases := []struct {
		name string
		info *nodev1.InfoResponse
		want []roomShortfall
	}{
		{
			name: "cpu",
			info: roomInfo(nodeID, 3, 1024, 10240),
			want: []roomShortfall{{resource: "vcpus", need: 4, offer: 3, unit: "vcpus"}},
		},
		{
			name: "memory",
			info: roomInfo(nodeID, 4, 1023, 10240),
			want: []roomShortfall{{resource: "memory", need: 1024, offer: 1023, unit: "MiB"}},
		},
		{
			name: "disk",
			info: roomInfo(nodeID, 4, 1024, 10239),
			want: []roomShortfall{{resource: "disk", need: 10240, offer: 10239, unit: "MiB"}},
		},
		{
			name: "combined",
			info: roomInfo(nodeID, 3, 1023, 10239),
			want: []roomShortfall{
				{resource: "vcpus", need: 4, offer: 3, unit: "vcpus"},
				{resource: "memory", need: 1024, offer: 1023, unit: "MiB"},
				{resource: "disk", need: 10240, offer: 10239, unit: "MiB"},
			},
		},
	}
	operations := []struct {
		name    string
		restore bool
	}{
		{name: "placement"},
		{name: "restore", restore: true},
	}
	for _, op := range operations {
		for _, tc := range cases {
			t.Run(op.name+"/"+tc.name, func(t *testing.T) {
				store := &roomStore{nodes: []db.Node{{ID: nodeID}}}
				var resolver RestoreResolver
				req := AdmitRequest{
					ID: "bx_new", Owner: uuid.NewV7(), MachineType: "default",
					Reserve: need, Env: map[string]string{"TOKEN": "sentinel-env-secret"},
				}
				if op.restore {
					store.nodes = nil
					resolver = roomRestore{
						source:  db.SnapshotSource{NodeID: nodeID},
						machine: "default",
						need:    need,
					}
					req.MachineType = ""
					req.From = "base"
				}
				s, buf := roomScheduler(t, store, tc.info, resolver)
				if _, err := s.Admit(t.Context(), req); !errors.Is(err, ErrNoCapacity) {
					t.Fatalf("admit error = %v, want ErrNoCapacity", err)
				}
				if len(store.created) != 0 {
					t.Fatalf("create calls = %d, want 0 without room", len(store.created))
				}
				logged := buf.String()
				for _, secret := range []string{"sentinel-env-secret", "sentinel-info-secret", "sentinel-qemu-secret"} {
					if strings.Contains(logged, secret) {
						t.Fatalf("scheduler log leaked a request/inventory sentinel")
					}
				}
				if strings.Contains(logged, "stale capacity") {
					t.Fatal("scheduler log kept the misleading stale-capacity claim")
				}
				records := roomNoRoomRecords(t, buf)
				if len(records) != len(tc.want) {
					t.Fatalf("no-room log records = %d, want %d", len(records), len(tc.want))
				}
				for i, want := range tc.want {
					wantRecord := roomLogRecord{
						Msg:       "node has no room",
						Node:      nodeID,
						Operation: op.name,
						Resource:  want.resource,
						Need:      want.need,
						Offer:     want.offer,
						Unit:      want.unit,
					}
					if records[i] != wantRecord {
						t.Fatalf("no-room record %d did not match the failed %s dimension", i, want.resource)
					}
				}
			})
		}
	}
}

// TestNoRoomLogAbsentWhenLiveInventoryFits proves equal-capacity admission still
// succeeds and writes no false no-room line in either branch.
func TestNoRoomLogAbsentWhenLiveInventoryFits(t *testing.T) {
	const nodeID = "node-1"
	need := db.Resources{VCPUs: 4, MemoryMiB: 1024, DiskGiB: 10}
	operations := []struct {
		name    string
		restore bool
	}{
		{name: "placement"},
		{name: "restore", restore: true},
	}
	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			store := &roomStore{nodes: []db.Node{{ID: nodeID}}}
			var resolver RestoreResolver
			req := AdmitRequest{
				ID: "bx_new", Owner: uuid.NewV7(), MachineType: "default", Reserve: need,
			}
			if op.restore {
				store.nodes = nil
				resolver = roomRestore{
					source:  db.SnapshotSource{NodeID: nodeID},
					machine: "default",
					need:    need,
				}
				req.MachineType = ""
				req.From = "base"
			}
			s, buf := roomScheduler(t, store, roomInfo(nodeID, 4, 1024, 10240), resolver)
			if _, err := s.Admit(t.Context(), req); err != nil {
				t.Fatalf("admit: %v", err)
			}
			if len(store.created) != 1 {
				t.Fatalf("create calls = %d, want 1", len(store.created))
			}
			logged := buf.String()
			for _, secret := range []string{"sentinel-info-secret", "sentinel-qemu-secret"} {
				if strings.Contains(logged, secret) {
					t.Fatalf("scheduler log leaked an inventory sentinel")
				}
			}
			if records := roomNoRoomRecords(t, buf); len(records) != 0 {
				t.Fatalf("false no-room log records = %d, want 0", len(records))
			}
		})
	}
}
