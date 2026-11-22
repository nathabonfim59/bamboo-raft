package api_test

// Fork compat tests: every status and code the fork operation can answer
// validates against api/bbraft-v1.yaml through internal/compat, and the route
// is registered at the documented path.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

const (
	forkTestOwner   = "018f4b1c-7e2d-7a3b-9c4d-123456789abc"
	forkTestSandbox = "bx_23456789"
)

// forkCompatStore serves one running source with no generation.
type forkCompatStore struct{}

func (forkCompatStore) Sandbox(context.Context, uuid.UUID, string) (db.Sandbox, error) {
	return db.Sandbox{
		ID: forkTestSandbox, OwnerID: uuid.MustParse(forkTestOwner),
		State: db.StateReady, MachineType: "default", Name: "source",
		SnapshotsEnabled: true,
		Reserved:         db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51},
		Desired:          db.Resources{VCPUs: 4, MemoryMiB: 8192, DiskGiB: 51},
		NodeID:           new("node-a"),
	}, nil
}

func (forkCompatStore) LatestGeneration(context.Context, uuid.UUID, string) (db.Generation, bool, error) {
	return db.Generation{}, false, nil
}

// forkCompatAdmission accepts every fork with a provisioning sandbox.
type forkCompatAdmission struct{}

func (forkCompatAdmission) Fork(_ context.Context, request api.ForkRequest) (api.ForkResult, error) {
	return api.ForkResult{Sandbox: db.Sandbox{
		ID: request.SandboxID, OwnerID: request.Owner, State: db.StateProvisioning,
		MachineType: request.MachineType, TTLSeconds: request.TTLSeconds,
		SnapshotsEnabled: true,
	}}, nil
}

// forkCompatRequest builds one authenticated fork request.
func forkCompatRequest(owner string) *http.Request {
	request := httptest.NewRequest(http.MethodPost,
		"/api/v1/sandboxes/"+forkTestSandbox+"/fork", nil)
	request.SetPathValue(api.ParamSandboxID, forkTestSandbox)
	return request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner}))
}

// TestForkCompat checks the 202 envelope and the approved error matrix.
func TestForkCompat(t *testing.T) {
	validator := compat.Load(t)
	if !validator.HasOperation(http.MethodPost, api.PathSandboxFork) {
		t.Fatalf("api/bbraft-v1.yaml missing POST %s", api.PathSandboxFork)
	}
	handler, err := api.NewForkHandler(api.ForkOptions{
		Store:       forkCompatStore{},
		Admission:   forkCompatAdmission{},
		ObjectStore: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("202 sandbox.forking", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		handler(recorder, forkCompatRequest(forkTestOwner))
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 (%s)", recorder.Code, recorder.Body.String())
		}
		if err := validator.Response("fork", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
			t.Fatalf("compat 202 failed: %v", err)
		}
	})

	matrix := []struct {
		name   string
		status int
		code   string
		wantOK bool
	}{
		{"400 invalid_json", http.StatusBadRequest, apierr.CodeInvalidJSON, true},
		{"400 invalid_env", http.StatusBadRequest, apierr.CodeInvalidEnv, true},
		{"400 invalid_query", http.StatusBadRequest, apierr.CodeInvalidQuery, true},
		{"400 unknown_environment", http.StatusBadRequest, apierr.CodeUnknownEnvironment, true},
		{"401 unauthorized", http.StatusUnauthorized, apierr.CodeUnauthorized, true},
		{"404 not_found", http.StatusNotFound, apierr.CodeNotFound, true},
		{"404 snapshot_not_found", http.StatusNotFound, apierr.CodeSnapshotNotFound, true},
		{"409 conflict", http.StatusConflict, apierr.CodeConflict, true},
		{"409 fork_failed", http.StatusConflict, "fork_failed", true},
		{"409 snapshots_disabled", http.StatusConflict, apierr.CodeSnapshotsDisabled, true},
		{"409 snapshot_data_expired", http.StatusConflict, apierr.CodeSnapshotDataExpired, true},
		{"409 type_too_small", http.StatusConflict, "type_too_small", true},
		{"409 sandbox_stopping", http.StatusConflict, apierr.CodeSandboxStopping, true},
		{"409 sandbox_resuming", http.StatusConflict, apierr.CodeSandboxResuming, true},
		{"409 idempotency_in_progress", http.StatusConflict, apierr.CodeIdempotencyInProgress, true},
		{"409 idempotency_key_reused", http.StatusConflict, apierr.CodeIdempotencyKeyReused, true},
		{"500 http_500", http.StatusInternalServerError, apierr.CodeInternal, true},
		{"503 no_ready_machine", http.StatusServiceUnavailable, apierr.CodeNoReadyMachine, true},
		{"503 out_of_capacity", http.StatusServiceUnavailable, apierr.CodeOutOfCapacity, true},
		{"409 unapproved code rejected", http.StatusConflict, "bogus_fork_code", false},
		{"503 without a retry hint rejected", http.StatusServiceUnavailable, apierr.CodeNoReadyMachine, false},
	}
	for _, tc := range matrix {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := forkCompatRequest(forkTestOwner)
			apiErr := apierr.New(tc.status, tc.code, "test error")
			if tc.status == http.StatusServiceUnavailable && tc.wantOK {
				apiErr = apiErr.Retryable(true).RetryAfterSeconds(5)
			}
			apierr.WriteError(recorder, request, apiErr)
			err := validator.Response("fork", recorder.Code, recorder.Header(), recorder.Body.Bytes())
			if tc.wantOK && err != nil {
				t.Fatalf("expected compat pass, got: %v", err)
			}
			if !tc.wantOK && err == nil {
				t.Fatalf("expected compat rejection for %d %s", tc.status, tc.code)
			}
		})
	}
}

// TestForkRouteRegistered checks Register maps the documented path to the
// handler.
func TestForkRouteRegistered(t *testing.T) {
	handler, err := api.NewForkHandler(api.ForkOptions{
		Store: forkCompatStore{}, Admission: forkCompatAdmission{}, ObjectStore: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	api.Register(mux, api.Handlers{ForkSandbox: handler})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/"+forkTestSandbox+"/fork", nil)
	request = request.WithContext(auth.WithIdentity(request.Context(),
		auth.Identity{Owner: forkTestOwner}))
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", recorder.Code, recorder.Body.String())
	}
}
