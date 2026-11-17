package api_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

const coreCompatOwner = "018f4b1c-7e2d-7a3b-9c4d-123456789abc"

var coreCompatTime = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

// coreCompatStore is the external-package fake used only to record real
// handler bytes for the pinned and subset validators.
type coreCompatStore struct {
	mu        sync.Mutex
	sandbox   func(context.Context, uuid.UUID, string) (db.Sandbox, error)
	list      func(context.Context, db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error)
	del       func(context.Context, uuid.UUID, string, db.DeletionReason) (db.DeletionOperation, db.Sandbox, error)
	operation func(context.Context, uuid.UUID, string) (db.DeletionOperation, error)
}

func (s *coreCompatStore) Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sandbox == nil {
		return db.Sandbox{}, db.ErrNotFound
	}
	return s.sandbox(ctx, owner, id)
}

func (s *coreCompatStore) ListSandboxes(ctx context.Context, query db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.list == nil {
		return nil, nil, nil
	}
	return s.list(ctx, query)
}

func (s *coreCompatStore) AcceptSandboxDeletion(ctx context.Context, owner uuid.UUID, id string, reason db.DeletionReason) (db.DeletionOperation, db.Sandbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.del == nil {
		return db.DeletionOperation{}, db.Sandbox{}, db.ErrNotFound
	}
	return s.del(ctx, owner, id, reason)
}

func (s *coreCompatStore) DeletionOperation(ctx context.Context, owner uuid.UUID, id string) (db.DeletionOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.operation == nil {
		return db.DeletionOperation{}, db.ErrNotFound
	}
	return s.operation(ctx, owner, id)
}

// coreCompatAdmission returns one fixed outcome.
type coreCompatAdmission struct {
	result api.AdmissionResult
	err    error
}

func (a coreCompatAdmission) Admit(context.Context, api.AdmissionRequest) (api.AdmissionResult, error) {
	return a.result, a.err
}

func coreCompatSandbox() db.Sandbox {
	return db.Sandbox{
		ID: "bx_23456789", OwnerID: uuid.MustParse(coreCompatOwner), State: db.StateReady,
		MachineType: "default", Name: "job", Labels: map[string]string{"session": "abc"},
		CreatedAt: coreCompatTime, UpdatedAt: coreCompatTime,
		ExpiresAt: new(coreCompatTime.Add(time.Hour)),
	}
}

func coreCompatOperation() db.DeletionOperation {
	completed := coreCompatTime.Add(time.Minute)
	return db.DeletionOperation{
		ID: "bdop_0123456789abcdef0123456789abcdef", Kind: db.DeletionSandbox,
		TargetID: "bx_23456789", Reason: db.DeletionExplicit, Status: db.DeletionCompleted,
		Stage: db.StageCompleted, AttemptCount: 1, RequestedAt: coreCompatTime,
		CompletedAt: &completed,
	}
}

// coreCompatHandlers builds the six handlers over one store and admission.
func coreCompatHandlers(t *testing.T, store api.CoreStore, admission api.Admission) api.Handlers {
	t.Helper()
	handlers, err := api.NewCoreHandlers(api.CoreOptions{
		Store:     store,
		Admission: admission,
		Now:       func() time.Time { return coreCompatTime },
	})
	if err != nil {
		t.Fatalf("NewCoreHandlers: %v", err)
	}
	return handlers
}

// coreCompatRequest carries the authenticated identity unless owner is empty.
func coreCompatRequest(method, target, body, owner string) *http.Request {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	if owner != "" {
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner}))
	}
	return request
}

// coreCompatServe records one handler response.
func coreCompatServe(handler http.HandlerFunc, request *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	return recorder
}

// coreCompatValidators loads the v1 API document.
func coreCompatValidators(t *testing.T) []*compat.Validator {
	t.Helper()
	return []*compat.Validator{
		compat.Load(t),
	}
}

// coreCompatAssert validates the recorded bytes against the v1 API document.
func coreCompatAssert(t *testing.T, validators []*compat.Validator, operationID string, status int, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d", recorder.Code, status)
	}
	for _, validator := range validators {
		if err := validator.Response(operationID, status, recorder.Header(), recorder.Body.Bytes()); err != nil {
			t.Errorf("%s %d failed pinned/subset validation", operationID, status)
		}
	}
}

func TestCoreCompatSuccessAndErrors(t *testing.T) {
	validators := coreCompatValidators(t)
	sandbox := coreCompatSandbox()
	operation := coreCompatOperation()
	store := &coreCompatStore{
		sandbox: func(_ context.Context, owner uuid.UUID, id string) (db.Sandbox, error) {
			if owner.String() != coreCompatOwner || id != sandbox.ID {
				return db.Sandbox{}, db.ErrNotFound
			}
			return sandbox, nil
		},
		list: func(context.Context, db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error) {
			return []db.Sandbox{sandbox}, nil, nil
		},
		del: func(_ context.Context, owner uuid.UUID, id string, _ db.DeletionReason) (db.DeletionOperation, db.Sandbox, error) {
			if owner.String() != coreCompatOwner || id != sandbox.ID {
				return db.DeletionOperation{}, db.Sandbox{}, db.ErrNotFound
			}
			return operation, sandbox, nil
		},
		operation: func(_ context.Context, owner uuid.UUID, id string) (db.DeletionOperation, error) {
			if owner.String() != coreCompatOwner || id != operation.ID {
				return db.DeletionOperation{}, db.ErrNotFound
			}
			return operation, nil
		},
	}
	admission := coreCompatAdmission{result: api.AdmissionResult{Sandbox: sandbox}}
	handlers := coreCompatHandlers(t, store, admission)

	t.Run("me 200", func(t *testing.T) {
		recorder := coreCompatServe(handlers.Me, coreCompatRequest(http.MethodGet, api.PathMe, "", coreCompatOwner))
		coreCompatAssert(t, validators, "me", http.StatusOK, recorder)
	})
	t.Run("me 401", func(t *testing.T) {
		recorder := coreCompatServe(handlers.Me, coreCompatRequest(http.MethodGet, api.PathMe, "", ""))
		coreCompatAssert(t, validators, "me", http.StatusUnauthorized, recorder)
	})
	t.Run("me 500", func(t *testing.T) {
		// /me has no fallible dependency; record the real error writer used
		// by every handler so the approved exception is exercised.
		request := coreCompatRequest(http.MethodGet, api.PathMe, "", coreCompatOwner)
		recorder := httptest.NewRecorder()
		apierr.WriteError(recorder, request, errors.New("internal fault"))
		coreCompatAssert(t, validators, "me", http.StatusInternalServerError, recorder)
	})

	t.Run("list 200", func(t *testing.T) {
		recorder := coreCompatServe(handlers.ListSandboxes, coreCompatRequest(http.MethodGet, api.PathSandboxes, "", coreCompatOwner))
		coreCompatAssert(t, validators, "sandboxes", http.StatusOK, recorder)
	})
	t.Run("list 400", func(t *testing.T) {
		recorder := coreCompatServe(handlers.ListSandboxes, coreCompatRequest(http.MethodGet, api.PathSandboxes+"?bogus=1", "", coreCompatOwner))
		coreCompatAssert(t, validators, "sandboxes", http.StatusBadRequest, recorder)
	})
	t.Run("list 401", func(t *testing.T) {
		recorder := coreCompatServe(handlers.ListSandboxes, coreCompatRequest(http.MethodGet, api.PathSandboxes, "", ""))
		coreCompatAssert(t, validators, "sandboxes", http.StatusUnauthorized, recorder)
	})
	t.Run("list 500", func(t *testing.T) {
		failing := &coreCompatStore{list: func(context.Context, db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error) {
			return nil, nil, errors.New("storage fault")
		}}
		handlers := coreCompatHandlers(t, failing, admission)
		recorder := coreCompatServe(handlers.ListSandboxes, coreCompatRequest(http.MethodGet, api.PathSandboxes, "", coreCompatOwner))
		coreCompatAssert(t, validators, "sandboxes", http.StatusInternalServerError, recorder)
	})

	t.Run("create 202", func(t *testing.T) {
		recorder := coreCompatServe(handlers.CreateSandbox, coreCompatRequest(http.MethodPost, api.PathSandboxes, `{}`, coreCompatOwner))
		coreCompatAssert(t, validators, "create", http.StatusAccepted, recorder)
	})
	t.Run("create 400 codes", func(t *testing.T) {
		tests := []struct {
			name   string
			target string
			body   string
			code   string
		}{
			{"invalid_json", api.PathSandboxes, `{"type":`, apierr.CodeInvalidJSON},
			{"invalid_name", api.PathSandboxes, `{"name":"` + strings.Repeat("a", 121) + `"}`, apierr.CodeInvalidName},
			{"invalid_env", api.PathSandboxes, `{"env":{"AGENT_ID":"x"}}`, apierr.CodeInvalidEnv},
			{"unknown_environment", api.PathSandboxes, `{"environment":"custom"}`, apierr.CodeUnknownEnvironment},
			{"invalid_query", api.PathSandboxes + "?org=personal", `{}`, apierr.CodeInvalidQuery},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				recorder := coreCompatServe(handlers.CreateSandbox, coreCompatRequest(http.MethodPost, test.target, test.body, coreCompatOwner))
				coreCompatAssert(t, validators, "create", http.StatusBadRequest, recorder)
				if body := recorder.Body.String(); !strings.Contains(body, `"code":"`+test.code+`"`) {
					t.Errorf("error code mismatch")
				}
			})
		}
	})
	t.Run("create 401", func(t *testing.T) {
		recorder := coreCompatServe(handlers.CreateSandbox, coreCompatRequest(http.MethodPost, api.PathSandboxes, `{}`, ""))
		coreCompatAssert(t, validators, "create", http.StatusUnauthorized, recorder)
	})
	t.Run("create 409 idempotency", func(t *testing.T) {
		for _, test := range []struct {
			name string
			err  error
			code string
		}{
			{"reused", db.ErrIdempotencyReused, apierr.CodeIdempotencyKeyReused},
			{"in progress", db.ErrIdempotencyInProgress, apierr.CodeIdempotencyInProgress},
		} {
			t.Run(test.name, func(t *testing.T) {
				handlers := coreCompatHandlers(t, store, coreCompatAdmission{err: test.err})
				recorder := coreCompatServe(handlers.CreateSandbox, coreCompatRequest(http.MethodPost, api.PathSandboxes, `{}`, coreCompatOwner))
				coreCompatAssert(t, validators, "create", http.StatusConflict, recorder)
				if body := recorder.Body.String(); !strings.Contains(body, `"code":"`+test.code+`"`) {
					t.Errorf("error code mismatch")
				}
			})
		}
	})
	t.Run("create 503 capacity", func(t *testing.T) {
		for _, test := range []struct {
			name string
			body string
			code string
		}{
			{"fail fast", `{"failFast":true}`, apierr.CodeNoReadyMachine},
			{"queued", `{}`, apierr.CodeOutOfCapacity},
		} {
			t.Run(test.name, func(t *testing.T) {
				handlers := coreCompatHandlers(t, store, coreCompatAdmission{err: db.ErrNoCapacity})
				recorder := coreCompatServe(handlers.CreateSandbox, coreCompatRequest(http.MethodPost, api.PathSandboxes, test.body, coreCompatOwner))
				coreCompatAssert(t, validators, "create", http.StatusServiceUnavailable, recorder)
				if body := recorder.Body.String(); !strings.Contains(body, `"code":"`+test.code+`"`) {
					t.Errorf("error code mismatch")
				}
			})
		}
	})
	t.Run("create 500", func(t *testing.T) {
		handlers := coreCompatHandlers(t, store, coreCompatAdmission{err: errors.New("storage fault")})
		recorder := coreCompatServe(handlers.CreateSandbox, coreCompatRequest(http.MethodPost, api.PathSandboxes, `{}`, coreCompatOwner))
		coreCompatAssert(t, validators, "create", http.StatusInternalServerError, recorder)
	})

	t.Run("get 200", func(t *testing.T) {
		request := coreCompatRequest(http.MethodGet, api.PathSandbox, "", coreCompatOwner)
		request.SetPathValue(api.ParamSandboxID, sandbox.ID)
		recorder := coreCompatServe(handlers.GetSandbox, request)
		coreCompatAssert(t, validators, "get", http.StatusOK, recorder)
	})
	t.Run("get 404", func(t *testing.T) {
		request := coreCompatRequest(http.MethodGet, api.PathSandbox, "", coreCompatOwner)
		request.SetPathValue(api.ParamSandboxID, "bx_foreign0")
		recorder := coreCompatServe(handlers.GetSandbox, request)
		coreCompatAssert(t, validators, "get", http.StatusNotFound, recorder)
	})
	t.Run("get 401", func(t *testing.T) {
		request := coreCompatRequest(http.MethodGet, api.PathSandbox, "", "")
		request.SetPathValue(api.ParamSandboxID, sandbox.ID)
		recorder := coreCompatServe(handlers.GetSandbox, request)
		coreCompatAssert(t, validators, "get", http.StatusUnauthorized, recorder)
	})
	t.Run("get 500", func(t *testing.T) {
		failing := &coreCompatStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
			return db.Sandbox{}, errors.New("storage fault")
		}}
		handlers := coreCompatHandlers(t, failing, admission)
		request := coreCompatRequest(http.MethodGet, api.PathSandbox, "", coreCompatOwner)
		request.SetPathValue(api.ParamSandboxID, sandbox.ID)
		recorder := coreCompatServe(handlers.GetSandbox, request)
		coreCompatAssert(t, validators, "get", http.StatusInternalServerError, recorder)
	})

	t.Run("delete 202", func(t *testing.T) {
		request := coreCompatRequest(http.MethodDelete, api.PathSandbox, "", coreCompatOwner)
		request.SetPathValue(api.ParamSandboxID, sandbox.ID)
		request.Header.Set(api.ConfirmDeleteHeader, sandbox.ID)
		recorder := coreCompatServe(handlers.DeleteSandbox, request)
		coreCompatAssert(t, validators, "deleteSandbox", http.StatusAccepted, recorder)
	})
	t.Run("delete 404", func(t *testing.T) {
		request := coreCompatRequest(http.MethodDelete, api.PathSandbox, "", coreCompatOwner)
		request.SetPathValue(api.ParamSandboxID, "bx_foreign0")
		request.Header.Set(api.ConfirmDeleteHeader, "bx_foreign0")
		recorder := coreCompatServe(handlers.DeleteSandbox, request)
		coreCompatAssert(t, validators, "deleteSandbox", http.StatusNotFound, recorder)
	})
	t.Run("delete 409", func(t *testing.T) {
		request := coreCompatRequest(http.MethodDelete, api.PathSandbox, "", coreCompatOwner)
		request.SetPathValue(api.ParamSandboxID, sandbox.ID)
		recorder := coreCompatServe(handlers.DeleteSandbox, request)
		coreCompatAssert(t, validators, "deleteSandbox", http.StatusConflict, recorder)
	})
	t.Run("delete 401", func(t *testing.T) {
		request := coreCompatRequest(http.MethodDelete, api.PathSandbox, "", "")
		request.SetPathValue(api.ParamSandboxID, sandbox.ID)
		request.Header.Set(api.ConfirmDeleteHeader, sandbox.ID)
		recorder := coreCompatServe(handlers.DeleteSandbox, request)
		coreCompatAssert(t, validators, "deleteSandbox", http.StatusUnauthorized, recorder)
	})
	t.Run("delete 500", func(t *testing.T) {
		failing := &coreCompatStore{del: func(context.Context, uuid.UUID, string, db.DeletionReason) (db.DeletionOperation, db.Sandbox, error) {
			return db.DeletionOperation{}, db.Sandbox{}, errors.New("storage fault")
		}}
		handlers := coreCompatHandlers(t, failing, admission)
		request := coreCompatRequest(http.MethodDelete, api.PathSandbox, "", coreCompatOwner)
		request.SetPathValue(api.ParamSandboxID, sandbox.ID)
		request.Header.Set(api.ConfirmDeleteHeader, sandbox.ID)
		recorder := coreCompatServe(handlers.DeleteSandbox, request)
		coreCompatAssert(t, validators, "deleteSandbox", http.StatusInternalServerError, recorder)
	})

	t.Run("operation 200", func(t *testing.T) {
		request := coreCompatRequest(http.MethodGet, api.PathDeletionOperation, "", coreCompatOwner)
		request.SetPathValue(api.ParamOperationID, operation.ID)
		recorder := coreCompatServe(handlers.GetDeletionOperation, request)
		coreCompatAssert(t, validators, "getDeletionOperation", http.StatusOK, recorder)
	})
	t.Run("operation 404", func(t *testing.T) {
		request := coreCompatRequest(http.MethodGet, api.PathDeletionOperation, "", coreCompatOwner)
		request.SetPathValue(api.ParamOperationID, "bdop_ffffffffffffffffffffffffffffffff")
		recorder := coreCompatServe(handlers.GetDeletionOperation, request)
		coreCompatAssert(t, validators, "getDeletionOperation", http.StatusNotFound, recorder)
	})
	t.Run("operation 401", func(t *testing.T) {
		request := coreCompatRequest(http.MethodGet, api.PathDeletionOperation, "", "")
		request.SetPathValue(api.ParamOperationID, operation.ID)
		recorder := coreCompatServe(handlers.GetDeletionOperation, request)
		coreCompatAssert(t, validators, "getDeletionOperation", http.StatusUnauthorized, recorder)
	})
	t.Run("operation 500", func(t *testing.T) {
		failing := &coreCompatStore{operation: func(context.Context, uuid.UUID, string) (db.DeletionOperation, error) {
			return db.DeletionOperation{}, errors.New("storage fault")
		}}
		handlers := coreCompatHandlers(t, failing, admission)
		request := coreCompatRequest(http.MethodGet, api.PathDeletionOperation, "", coreCompatOwner)
		request.SetPathValue(api.ParamOperationID, operation.ID)
		recorder := coreCompatServe(handlers.GetDeletionOperation, request)
		coreCompatAssert(t, validators, "getDeletionOperation", http.StatusInternalServerError, recorder)
	})
}

func TestCoreInfoCompat(t *testing.T) {
	spec := compat.LoadFile(t, filepath.Join(compat.Root(t), "api", "bbraft-ext.yaml"))
	handler, err := api.NewInfoHandler(api.InfoOptions{Version: "v1.2.3", StreamMaxSeconds: 7200})
	if err != nil {
		t.Fatal(err)
	}
	recorder := coreCompatServe(handler, coreCompatRequest(http.MethodGet, api.PathInfo, "", coreCompatOwner))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if err := spec.Response("bbraftInfo", http.StatusOK, recorder.Header(), recorder.Body.Bytes()); err != nil {
		t.Errorf("info 200 failed spec validation")
	}
	recorder = coreCompatServe(handler, coreCompatRequest(http.MethodGet, api.PathInfo, "", ""))
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
	if err := spec.Response("bbraftInfo", http.StatusUnauthorized, recorder.Header(), recorder.Body.Bytes()); err != nil {
		t.Errorf("info 401 failed spec validation")
	}
}
