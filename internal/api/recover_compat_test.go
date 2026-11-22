package api_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func TestRecoverCompatMatrix(t *testing.T) {
	validator := compat.Load(t)
	if !validator.HasOperation(http.MethodPost, api.PathSandboxRecover) {
		t.Fatalf("api/bbraft-v1.yaml missing POST %s", api.PathSandboxRecover)
	}

	gw := newRecoverTestGateway(t)
	fake := gw.startFake(t, recoverTestNodeID)
	if _, err := fake.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: recoverTestSandbox,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 10},
	}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	unreachableClients := gw.clientFactory(func(inner nodev1.NodeClient) nodev1.NodeClient {
		return &recoverTestNodeWrapper{
			NodeClient: inner,
			execHook: func(context.Context, *nodev1.ExecRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
				return nil, status.Error(codes.DeadlineExceeded, "unreachable")
			},
		}
	})

	// 200 success envelope
	t.Run("200 sandbox.recovering", func(t *testing.T) {
		store := &recoverTestStore{
			sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
				return recoverReadyRow(nil), nil
			},
			accept: func(_ context.Context, _ uuid.UUID, _ string, at time.Time) (db.Sandbox, error) {
				return recoverReadyRow(&at), nil
			},
		}
		h, err := api.NewRecoverHandler(api.RecoverOptions{
			Store:   store,
			Clients: unreachableClients,
			Now:     func() time.Time { return now },
		})
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		h(rec, recoverRequest(recoverTestOwner, recoverTestSandbox))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if err := validator.Response("recover", rec.Code, rec.Header(), rec.Body.Bytes()); err != nil {
			t.Fatalf("compat 200 failed: %v", err)
		}
	})

	// All approved 401/404/409/500 error codes in the matrix.
	matrixErrors := []struct {
		name   string
		status int
		code   string
		wantOK bool
	}{
		{"401 unauthorized", http.StatusUnauthorized, apierr.CodeUnauthorized, true},
		{"404 not_found", http.StatusNotFound, apierr.CodeNotFound, true},
		{"409 no_machine", http.StatusConflict, "no_machine", true},
		{"409 too_soon", http.StatusConflict, "too_soon", true},
		{"409 recover_unsupported", http.StatusConflict, "recover_unsupported", true},
		{"409 sandbox_is_reachable", http.StatusConflict, "sandbox_is_reachable", true},
		{"409 reset_failed", http.StatusConflict, "reset_failed", true},
		{"500 http_500", http.StatusInternalServerError, apierr.CodeInternal, true},
		{"409 unapproved code rejected", http.StatusConflict, "bogus_recover_code", false},
	}
	for _, tc := range matrixErrors {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := recoverRequest(recoverTestOwner, recoverTestSandbox)
			apierr.WriteError(rec, req, apierr.New(tc.status, tc.code, "test error"))
			err := validator.Response("recover", rec.Code, rec.Header(), rec.Body.Bytes())
			if tc.wantOK && err != nil {
				t.Fatalf("expected compat pass, got: %v", err)
			}
			if !tc.wantOK && err == nil {
				t.Fatalf("expected compat rejection for %d %s", tc.status, tc.code)
			}
		})
	}

	// Real 500 path through handler.
	t.Run("handler 500", func(t *testing.T) {
		store := &recoverTestStore{
			sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
				return db.Sandbox{}, errors.New("boom")
			},
		}
		h, err := api.NewRecoverHandler(api.RecoverOptions{Store: store, Clients: unreachableClients})
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		h(rec, recoverRequest(recoverTestOwner, recoverTestSandbox))
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", rec.Code)
		}
		if err := validator.Response("recover", rec.Code, rec.Header(), rec.Body.Bytes()); err != nil {
			t.Fatalf("compat 500 failed: %v", err)
		}
		var env struct {
			Code string `json:"code"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatal(err)
		}
		if env.Code != apierr.CodeInternal {
			t.Fatalf("code = %q, want %q", env.Code, apierr.CodeInternal)
		}
	})
}
