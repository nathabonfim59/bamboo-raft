package api_test

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
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
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

const (
	recoverTestOwner   = "018f4b1c-7e2d-7a3b-9c4d-123456789abc"
	recoverTestSandbox = "bx_23456789"
	recoverTestNodeID  = "node-recover-a"
)

type recoverTestStore struct {
	mu           sync.Mutex
	sandbox      func(context.Context, uuid.UUID, string) (db.Sandbox, error)
	accept       func(context.Context, uuid.UUID, string, time.Time) (db.Sandbox, error)
	acceptCalls  int
	lastAcceptAt time.Time
}

func (s *recoverTestStore) Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error) {
	s.mu.Lock()
	fn := s.sandbox
	s.mu.Unlock()
	if fn == nil {
		return db.Sandbox{}, db.ErrNotFound
	}
	return fn(ctx, owner, id)
}

func (s *recoverTestStore) AcceptRecover(ctx context.Context, owner uuid.UUID, sandboxID string, now time.Time) (db.Sandbox, error) {
	s.mu.Lock()
	s.acceptCalls++
	s.lastAcceptAt = now
	fn := s.accept
	s.mu.Unlock()
	if fn == nil {
		return db.Sandbox{}, db.ErrNotFound
	}
	return fn(ctx, owner, sandboxID, now)
}

var _ api.RecoverStore = (*recoverTestStore)(nil)

// recoverTestNodeWrapper wraps a nodev1.NodeClient to observe or block Exec
// and GetVM without modifying nodefake.
type recoverTestNodeWrapper struct {
	nodev1.NodeClient
	getVMHook func(context.Context, *nodev1.GetVMRequest, ...grpc.CallOption) (*nodev1.VM, error)
	execHook  func(context.Context, *nodev1.ExecRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ExecEvent], error)
}

func (w *recoverTestNodeWrapper) GetVM(ctx context.Context, req *nodev1.GetVMRequest, opts ...grpc.CallOption) (*nodev1.VM, error) {
	if w.getVMHook != nil {
		return w.getVMHook(ctx, req, opts...)
	}
	return w.NodeClient.GetVM(ctx, req, opts...)
}

func (w *recoverTestNodeWrapper) Exec(ctx context.Context, req *nodev1.ExecRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
	if w.execHook != nil {
		return w.execHook(ctx, req, opts...)
	}
	return w.NodeClient.Exec(ctx, req, opts...)
}

// recoverTestGateway wires a real nodelink.Hub + nodegateway.Gateway so tests
// exercise real connected and disconnected node links.
type recoverTestGateway struct {
	hub     *nodelink.Hub
	factory *nodegateway.ClientFactory
	hubAddr string
}

func newRecoverTestGateway(t *testing.T) *recoverTestGateway {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := slog.New(slog.DiscardHandler)
	hubLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hub := nodelink.NewHub(func(_ context.Context, h nodelink.Hello) error {
		want := sha256.Sum256([]byte("recover-node-token"))
		got := sha256.Sum256([]byte(h.Token))
		if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			return errors.New("bad node token")
		}
		return nil
	}, logger)
	hubDone := make(chan struct{})
	go func() { _ = hub.Serve(ctx, hubLn); close(hubDone) }()
	t.Cleanup(func() { cancel(); <-hubDone })

	gw, err := nodegateway.New(hub, "recover-gw-token")
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

	return &recoverTestGateway{
		hub:     hub,
		factory: nodegateway.NewClientFactory(gwLn.Addr().String(), "recover-gw-token", nil),
		hubAddr: hubLn.Addr().String(),
	}
}

func (g *recoverTestGateway) startFake(t *testing.T, nodeID string) *nodefake.Node {
	t.Helper()
	fake := nodefake.New(nodeID, t.TempDir())
	srv := grpc.NewServer(nodelink.ServerOptions()...)
	fake.Register(srv)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = nodelink.Serve(ctx, nodelink.DialConfig{
			Addr: g.hubAddr, NodeID: nodeID, Token: "recover-node-token",
			Log:        slog.New(slog.DiscardHandler),
			MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		}, srv)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })

	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()
	if _, err := g.hub.Wait(waitCtx, nodeID); err != nil {
		t.Fatalf("node %s did not connect: %v", nodeID, err)
	}
	return fake
}

func (g *recoverTestGateway) clientFactory(wrap func(nodev1.NodeClient) nodev1.NodeClient) api.NodeClientFactory {
	return func(nodeID string) (nodev1.NodeClient, func() error, error) {
		c, err := g.factory.Dial(nodeID)
		if err != nil {
			return nil, nil, err
		}
		var nc nodev1.NodeClient = c
		if wrap != nil {
			nc = wrap(c)
		}
		return nc, c.Close, nil
	}
}

func recoverReadyRow(lastRecoverAt *time.Time) db.Sandbox {
	nodeID := recoverTestNodeID
	return db.Sandbox{
		ID:            recoverTestSandbox,
		OwnerID:       uuid.MustParse(recoverTestOwner),
		State:         db.StateReady,
		NodeID:        &nodeID,
		LastRecoverAt: lastRecoverAt,
	}
}

func recoverRequest(owner, sandboxID string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/"+sandboxID+"/recover", nil)
	if sandboxID != "" {
		req.SetPathValue(api.ParamSandboxID, sandboxID)
	}
	if owner != "" {
		req = req.WithContext(auth.WithIdentity(req.Context(), auth.Identity{Owner: owner}))
	}
	return req
}

func decodeRecoverErrorCode(t *testing.T, body []byte) string {
	t.Helper()
	var env struct {
		OK   bool   `json:"ok"`
		Code string `json:"code"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode error envelope: %v (%s)", err, body)
	}
	if env.OK {
		t.Fatalf("expected error envelope, got ok=true: %s", body)
	}
	return env.Code
}

func TestNewRecoverHandlerValidation(t *testing.T) {
	store := &recoverTestStore{}
	clients := func(string) (nodev1.NodeClient, func() error, error) {
		return nil, nil, errors.New("unused")
	}
	if _, err := api.NewRecoverHandler(api.RecoverOptions{Clients: clients}); err == nil {
		t.Fatal("expected error for nil Store")
	}
	if _, err := api.NewRecoverHandler(api.RecoverOptions{Store: store}); err == nil {
		t.Fatal("expected error for nil Clients")
	}
}

func TestRegisterRecoverRoute(t *testing.T) {
	var gotID string
	mux := http.NewServeMux()
	api.Register(mux, api.Handlers{
		RecoverSandbox: func(w http.ResponseWriter, r *http.Request) {
			gotID = r.PathValue(api.ParamSandboxID)
			w.WriteHeader(http.StatusNoContent)
		},
	})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/bx_23456789/recover", nil))
	if rec.Code != http.StatusNoContent || gotID != "bx_23456789" {
		t.Fatalf("status=%d id=%q, want 204 bx_23456789", rec.Code, gotID)
	}
}

func TestRecoverRefusals(t *testing.T) {
	validator := compat.Load(t)
	gw := newRecoverTestGateway(t)
	fake := gw.startFake(t, recoverTestNodeID)
	if _, err := fake.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: recoverTestSandbox,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 10},
	}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	baseTime := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		owner      string
		sandboxID  string
		store      *recoverTestStore
		clients    api.NodeClientFactory
		now        time.Time
		wantStatus int
		wantCode   string
	}{
		{
			name:       "unauthorized",
			owner:      "",
			sandboxID:  recoverTestSandbox,
			store:      &recoverTestStore{},
			clients:    gw.clientFactory(nil),
			now:        baseTime,
			wantStatus: http.StatusUnauthorized,
			wantCode:   "unauthorized",
		},
		{
			name:       "not found",
			owner:      recoverTestOwner,
			sandboxID:  recoverTestSandbox,
			store:      &recoverTestStore{},
			clients:    gw.clientFactory(nil),
			now:        baseTime,
			wantStatus: http.StatusNotFound,
			wantCode:   "not_found",
		},
		{
			name:      "no_machine for archived",
			owner:     recoverTestOwner,
			sandboxID: recoverTestSandbox,
			store: &recoverTestStore{
				sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
					row := recoverReadyRow(nil)
					row.State = db.StateArchived
					return row, nil
				},
			},
			clients:    gw.clientFactory(nil),
			now:        baseTime,
			wantStatus: http.StatusConflict,
			wantCode:   "no_machine",
		},
		{
			name:      "no_machine for error",
			owner:     recoverTestOwner,
			sandboxID: recoverTestSandbox,
			store: &recoverTestStore{
				sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
					row := recoverReadyRow(nil)
					row.State = db.StateError
					return row, nil
				},
			},
			clients:    gw.clientFactory(nil),
			now:        baseTime,
			wantStatus: http.StatusConflict,
			wantCode:   "no_machine",
		},
		{
			name:      "no_machine when node missing",
			owner:     recoverTestOwner,
			sandboxID: recoverTestSandbox,
			store: &recoverTestStore{
				sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
					row := recoverReadyRow(nil)
					row.NodeID = nil
					return row, nil
				},
			},
			clients:    gw.clientFactory(nil),
			now:        baseTime,
			wantStatus: http.StatusConflict,
			wantCode:   "no_machine",
		},
		{
			name:      "too_soon at 9m59s",
			owner:     recoverTestOwner,
			sandboxID: recoverTestSandbox,
			store: &recoverTestStore{
				sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
					return recoverReadyRow(new(baseTime)), nil
				},
			},
			clients: func(string) (nodev1.NodeClient, func() error, error) {
				t.Error("node must not be dialed during cooldown")
				return nil, nil, errors.New("unexpected dial")
			},
			now:        baseTime.Add(9*time.Minute + 59*time.Second),
			wantStatus: http.StatusConflict,
			wantCode:   "too_soon",
		},
		{
			name:      "recover_unsupported with disconnected node",
			owner:     recoverTestOwner,
			sandboxID: recoverTestSandbox,
			store: &recoverTestStore{
				sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
					row := recoverReadyRow(nil)
					offline := "node-disconnected"
					row.NodeID = &offline
					return row, nil
				},
			},
			clients:    gw.clientFactory(nil),
			now:        baseTime,
			wantStatus: http.StatusConflict,
			wantCode:   "recover_unsupported",
		},
		{
			name:      "recover_unsupported on dial unavailable",
			owner:     recoverTestOwner,
			sandboxID: recoverTestSandbox,
			store: &recoverTestStore{
				sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
					return recoverReadyRow(nil), nil
				},
			},
			clients: func(string) (nodev1.NodeClient, func() error, error) {
				return nil, nil, status.Error(codes.Unavailable, "gateway down")
			},
			now:        baseTime,
			wantStatus: http.StatusConflict,
			wantCode:   "recover_unsupported",
		},
		{
			name:      "sandbox_is_reachable with healthy fake",
			owner:     recoverTestOwner,
			sandboxID: recoverTestSandbox,
			store: &recoverTestStore{
				sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
					return recoverReadyRow(nil), nil
				},
			},
			clients:    gw.clientFactory(nil),
			now:        baseTime,
			wantStatus: http.StatusConflict,
			wantCode:   "sandbox_is_reachable",
		},
		{
			name:      "no_machine when GetVM returns NOT_FOUND",
			owner:     recoverTestOwner,
			sandboxID: "bx_missing0",
			store: &recoverTestStore{
				sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
					row := recoverReadyRow(nil)
					row.ID = "bx_missing0"
					return row, nil
				},
			},
			clients:    gw.clientFactory(nil),
			now:        baseTime,
			wantStatus: http.StatusConflict,
			wantCode:   "no_machine",
		},
		{
			name:      "internal error hides store text",
			owner:     recoverTestOwner,
			sandboxID: recoverTestSandbox,
			store: &recoverTestStore{
				sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
					return db.Sandbox{}, errors.New("postgres://secret-dsn")
				},
			},
			clients:    gw.clientFactory(nil),
			now:        baseTime,
			wantStatus: http.StatusInternalServerError,
			wantCode:   "http_500",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, err := api.NewRecoverHandler(api.RecoverOptions{
				Store:   tc.store,
				Clients: tc.clients,
				Now:     func() time.Time { return tc.now },
			})
			if err != nil {
				t.Fatalf("NewRecoverHandler: %v", err)
			}
			rec := httptest.NewRecorder()
			h(rec, recoverRequest(tc.owner, tc.sandboxID))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if err := validator.Response("recover", rec.Code, rec.Header(), rec.Body.Bytes()); err != nil {
				t.Fatalf("compat validation failed: %v (body=%s)", err, rec.Body.String())
			}
			if got := decodeRecoverErrorCode(t, rec.Body.Bytes()); got != tc.wantCode {
				t.Fatalf("code = %q, want %q", got, tc.wantCode)
			}
			if strings.Contains(rec.Body.String(), "secret-dsn") {
				t.Fatal("error body leaked raw store error")
			}
			if tc.store.acceptCalls != 0 {
				t.Fatalf("AcceptRecover called %d times on refusal", tc.store.acceptCalls)
			}
		})
	}
}

func TestRecoverAcceptedAtExactCooldownAndHangingExec(t *testing.T) {
	validator := compat.Load(t)
	gw := newRecoverTestGateway(t)
	fake := gw.startFake(t, recoverTestNodeID)
	if _, err := fake.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: recoverTestSandbox,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 10},
	}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	baseTime := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	at10m := baseTime.Add(10 * time.Minute)
	var execObserved atomic.Pointer[nodev1.ExecRequest]

	store := &recoverTestStore{
		sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
			return recoverReadyRow(new(baseTime)), nil
		},
		accept: func(_ context.Context, owner uuid.UUID, sandboxID string, now time.Time) (db.Sandbox, error) {
			if owner != uuid.MustParse(recoverTestOwner) || sandboxID != recoverTestSandbox {
				return db.Sandbox{}, db.ErrNotFound
			}
			row := recoverReadyRow(&now)
			return row, nil
		},
	}

	hangingClients := gw.clientFactory(func(inner nodev1.NodeClient) nodev1.NodeClient {
		return &recoverTestNodeWrapper{
			NodeClient: inner,
			execHook: func(ctx context.Context, req *nodev1.ExecRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
				execObserved.Store(req)
				<-ctx.Done()
				return nil, status.FromContextError(ctx.Err()).Err()
			},
		}
	})

	h, err := api.NewRecoverHandler(api.RecoverOptions{
		Store:        store,
		Clients:      hangingClients,
		Now:          func() time.Time { return at10m },
		ProbeTimeout: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRecoverHandler: %v", err)
	}

	rec := httptest.NewRecorder()
	h(rec, recoverRequest(recoverTestOwner, recoverTestSandbox))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	if err := validator.Response("recover", rec.Code, rec.Header(), rec.Body.Bytes()); err != nil {
		t.Fatalf("compat validation failed: %v", err)
	}
	var resp api.RecoverResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode 200 response: %v", err)
	}
	if !resp.OK || resp.Type != api.TypeSandboxRecovering || resp.Status != "recovering" || resp.Message == "" {
		t.Fatalf("unexpected 200 payload: %+v", resp)
	}
	req := execObserved.Load()
	if req == nil || req.GetCommand() != "true" || req.GetSandboxId() != recoverTestSandbox {
		t.Fatalf("unexpected probe Exec request: %+v", req)
	}
	if store.acceptCalls != 1 || !store.lastAcceptAt.Equal(at10m) {
		t.Fatalf("AcceptRecover calls=%d at=%v, want 1 at %v", store.acceptCalls, store.lastAcceptAt, at10m)
	}
}

func TestRecoverDefaultProbeTimeoutIsThreeSeconds(t *testing.T) {
	gw := newRecoverTestGateway(t)
	fake := gw.startFake(t, recoverTestNodeID)
	if _, err := fake.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: recoverTestSandbox,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 10},
	}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	var gotTimeout uint32
	var gotDeadline time.Duration
	store := &recoverTestStore{
		sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
			return recoverReadyRow(nil), nil
		},
		accept: func(_ context.Context, _ uuid.UUID, _ string, now time.Time) (db.Sandbox, error) {
			return recoverReadyRow(&now), nil
		},
	}
	clients := gw.clientFactory(func(inner nodev1.NodeClient) nodev1.NodeClient {
		return &recoverTestNodeWrapper{
			NodeClient: inner,
			execHook: func(ctx context.Context, req *nodev1.ExecRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
				gotTimeout = req.GetTimeoutSeconds()
				if dl, ok := ctx.Deadline(); ok {
					gotDeadline = time.Until(dl)
				}
				return nil, status.Error(codes.DeadlineExceeded, "guest hung")
			},
		}
	})
	h, err := api.NewRecoverHandler(api.RecoverOptions{Store: store, Clients: clients})
	if err != nil {
		t.Fatalf("NewRecoverHandler: %v", err)
	}
	rec := httptest.NewRecorder()
	h(rec, recoverRequest(recoverTestOwner, recoverTestSandbox))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotTimeout != 3 {
		t.Fatalf("ExecRequest.TimeoutSeconds = %d, want 3", gotTimeout)
	}
	if gotDeadline <= 2*time.Second || gotDeadline > 3*time.Second+100*time.Millisecond {
		t.Fatalf("probe context deadline = %v, want ~3s", gotDeadline)
	}
}

func TestRecoverConcurrentAcceptTooSoonAndCanceledRequest(t *testing.T) {
	validator := compat.Load(t)
	gw := newRecoverTestGateway(t)
	fake := gw.startFake(t, recoverTestNodeID)
	if _, err := fake.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: recoverTestSandbox,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 10},
	}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	// AcceptRecover returns ErrTooSoon (e.g. a concurrent request won the row lock).
	t.Run("accept race returns 409 too_soon", func(t *testing.T) {
		store := &recoverTestStore{
			sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
				return recoverReadyRow(nil), nil
			},
			accept: func(context.Context, uuid.UUID, string, time.Time) (db.Sandbox, error) {
				return db.Sandbox{}, db.ErrTooSoon
			},
		}
		clients := gw.clientFactory(func(inner nodev1.NodeClient) nodev1.NodeClient {
			return &recoverTestNodeWrapper{
				NodeClient: inner,
				execHook: func(context.Context, *nodev1.ExecRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
					return nil, status.Error(codes.Internal, "vsock down")
				},
			}
		})
		h, err := api.NewRecoverHandler(api.RecoverOptions{Store: store, Clients: clients})
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		h(rec, recoverRequest(recoverTestOwner, recoverTestSandbox))
		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want 409", rec.Code)
		}
		if err := validator.Response("recover", rec.Code, rec.Header(), rec.Body.Bytes()); err != nil {
			t.Fatalf("compat: %v", err)
		}
		if got := decodeRecoverErrorCode(t, rec.Body.Bytes()); got != "too_soon" {
			t.Fatalf("code = %q, want too_soon", got)
		}
	})

	// Caller cancelling the HTTP request during the probe must not accept a recover.
	t.Run("cancelled request does not accept recover", func(t *testing.T) {
		store := &recoverTestStore{
			sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
				return recoverReadyRow(nil), nil
			},
			accept: func(context.Context, uuid.UUID, string, time.Time) (db.Sandbox, error) {
				return recoverReadyRow(nil), nil
			},
		}
		reqCtx, cancel := context.WithCancel(t.Context())
		clients := gw.clientFactory(func(inner nodev1.NodeClient) nodev1.NodeClient {
			return &recoverTestNodeWrapper{
				NodeClient: inner,
				execHook: func(ctx context.Context, _ *nodev1.ExecRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
					cancel()
					<-ctx.Done()
					return nil, io.EOF
				},
			}
		})
		h, err := api.NewRecoverHandler(api.RecoverOptions{
			Store:        store,
			Clients:      clients,
			ProbeTimeout: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		req := recoverRequest(recoverTestOwner, recoverTestSandbox).WithContext(
			auth.WithIdentity(reqCtx, auth.Identity{Owner: recoverTestOwner}),
		)
		rec := httptest.NewRecorder()
		h(rec, req)
		if store.acceptCalls != 0 {
			t.Fatalf("AcceptRecover called %d times after request cancel", store.acceptCalls)
		}
	})
}
