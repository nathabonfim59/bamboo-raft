package api_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// lifecycleCompatStore is the external fake the compatibility test drives
// with real handler bytes.
type lifecycleCompatStore struct {
	sandbox func(context.Context, uuid.UUID, string) (db.Sandbox, error)
	patch   func(context.Context, db.PatchSandboxParams) (db.Sandbox, error)
	stop    func(context.Context, uuid.UUID, string, bool) (db.StopOperation, db.Sandbox, db.Job, bool, error)
	stopOp  func(context.Context, uuid.UUID, string, string) (db.StopOperation, error)
}

func (s *lifecycleCompatStore) Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error) {
	if s.sandbox == nil {
		return db.Sandbox{}, db.ErrNotFound
	}
	return s.sandbox(ctx, owner, id)
}

func (s *lifecycleCompatStore) PatchSandbox(ctx context.Context, params db.PatchSandboxParams) (db.Sandbox, error) {
	if s.patch == nil {
		return db.Sandbox{}, db.ErrNotFound
	}
	return s.patch(ctx, params)
}

func (s *lifecycleCompatStore) AcceptSandboxStop(ctx context.Context, owner uuid.UUID, sandboxID string, force bool) (db.StopOperation, db.Sandbox, db.Job, bool, error) {
	if s.stop == nil {
		return db.StopOperation{}, db.Sandbox{}, db.Job{}, false, db.ErrNotFound
	}
	return s.stop(ctx, owner, sandboxID, force)
}

func (s *lifecycleCompatStore) StopOperation(ctx context.Context, owner uuid.UUID, sandboxID, stopID string) (db.StopOperation, error) {
	if s.stopOp == nil {
		return db.StopOperation{}, db.ErrNotFound
	}
	return s.stopOp(ctx, owner, sandboxID, stopID)
}

var _ api.LifecycleStore = (*lifecycleCompatStore)(nil)

// lifecycleCompatResume returns one fixed outcome.
type lifecycleCompatResume struct {
	result api.ResumeResult
	err    error
}

func (r lifecycleCompatResume) Resume(context.Context, api.ResumeRequest) (api.ResumeResult, error) {
	return r.result, r.err
}

// lifecycleCompatSandbox is one archived sandbox with stop metadata.
func lifecycleCompatSandbox() db.Sandbox {
	owner := uuid.MustParse(coreCompatOwner)
	stopped := coreCompatTime
	stopID := "stop_0123456789abcdef"
	return db.Sandbox{
		ID: "bx_23456789", OwnerID: owner, State: db.StateArchived,
		MachineType: "default", Name: "job", TTLSeconds: new(3600),
		ExpiresAt: new(coreCompatTime.Add(time.Hour)), StoppedAt: &stopped,
		StopOperationID: &stopID, CreatedAt: coreCompatTime, UpdatedAt: coreCompatTime,
	}
}

// lifecycleCompatStop is one completed stop.
func lifecycleCompatStop() db.StopOperation {
	stopped := coreCompatTime
	return db.StopOperation{
		ID: "stop_0123456789abcdef", OwnerID: uuid.MustParse(coreCompatOwner),
		SandboxID: "bx_23456789", Status: db.StopCompleted,
		RequestedAt: coreCompatTime, LastAttemptAt: &stopped, EndedAt: &stopped,
	}
}

// lifecycleCompatHandlers builds the four lifecycle handlers.
func lifecycleCompatHandlers(t *testing.T, store api.LifecycleStore, resume api.ResumeAdmission) api.Handlers {
	t.Helper()
	handlers, err := api.NewLifecycleHandlers(api.LifecycleOptions{Store: store, Resume: resume})
	if err != nil {
		t.Fatalf("NewLifecycleHandlers: %v", err)
	}
	return handlers
}

// lifecycleCompatPath sets the route parameters.
func lifecycleCompatPath(request *http.Request, sandboxID, stopID string) *http.Request {
	request.SetPathValue(api.ParamSandboxID, sandboxID)
	if stopID != "" {
		request.SetPathValue(api.ParamStopID, stopID)
	}
	return request
}

func TestLifecycleCompatSuccessAndErrors(t *testing.T) {
	validators := coreCompatValidators(t)
	archived := lifecycleCompatSandbox()
	completed := lifecycleCompatStop()
	pending := completed
	pending.Status = db.StopPending
	pending.LastAttemptAt = nil
	pending.EndedAt = nil

	store := &lifecycleCompatStore{
		patch: func(context.Context, db.PatchSandboxParams) (db.Sandbox, error) {
			return archived, nil
		},
		stopOp: func(context.Context, uuid.UUID, string, string) (db.StopOperation, error) {
			return completed, nil
		},
		stop: func(context.Context, uuid.UUID, string, bool) (db.StopOperation, db.Sandbox, db.Job, bool, error) {
			return completed, archived, db.Job{}, false, nil
		},
		sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
			return archived, nil
		},
	}
	resume := lifecycleCompatResume{result: api.ResumeResult{Sandbox: func() db.Sandbox {
		row := archived
		row.State = db.StateProvisioning
		return row
	}()}}
	handlers := lifecycleCompatHandlers(t, store, resume)

	tests := []struct {
		name        string
		handler     http.HandlerFunc
		method      string
		path        string
		body        string
		stopID      string
		operationID string
		status      int
	}{
		{"update", handlers.UpdateSandbox, http.MethodPatch, api.PathSandbox, `{"name":"x","ttlSeconds":7200,"subdomain":"acme-staging"}`, "", "update", http.StatusOK},
		{"stop archived", handlers.StopSandbox, http.MethodPost, api.PathSandboxStop, `{}`, "", "stop", http.StatusOK},
		{"stop status", handlers.GetSandboxStop, http.MethodGet, api.PathSandboxStops, "", completed.ID, "getStop", http.StatusOK},
		{"resume", handlers.ResumeSandbox, http.MethodPost, api.PathSandboxResume, `{"type":"large","ttlSeconds":7200}`, "", "resume", http.StatusAccepted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := lifecycleCompatPath(coreCompatRequest(test.method, test.path, test.body, coreCompatOwner), "bx_23456789", test.stopID)
			coreCompatAssert(t, validators, test.operationID, test.status, coreCompatServe(test.handler, request))
		})
	}

	// A pending stop is the pinned 202 shape with the status URL.
	pendingStore := &lifecycleCompatStore{stop: func(context.Context, uuid.UUID, string, bool) (db.StopOperation, db.Sandbox, db.Job, bool, error) {
		row := archived
		row.State = db.StateArchiving
		return pending, row, db.Job{}, true, nil
	}}
	pendingHandlers := lifecycleCompatHandlers(t, pendingStore, resume)
	request := lifecycleCompatPath(coreCompatRequest(http.MethodPost, api.PathSandboxStop, `{}`, coreCompatOwner), "bx_23456789", "")
	coreCompatAssert(t, validators, "stop", http.StatusAccepted, coreCompatServe(pendingHandlers.StopSandbox, request))

	// Every approved error status/code is validated too.
	errorTests := []struct {
		name        string
		method      string
		code        string
		operationID string
		status      int
		store       *lifecycleCompatStore
		resume      api.ResumeAdmission
		body        string
		stopID      string
	}{
		{
			name: "update invalid json", method: http.MethodPatch,
			code: apierr.CodeInvalidJSON, operationID: "update", status: http.StatusBadRequest, body: `{"subdomain":"ACME"}`,
		},
		{
			name: "update invalid name", method: http.MethodPatch,
			code: apierr.CodeInvalidName, operationID: "update", status: http.StatusBadRequest, body: `{"name":""}`,
		},
		{
			name: "update not found", method: http.MethodPatch,
			code: apierr.CodeNotFound, operationID: "update", status: http.StatusNotFound, body: `{"name":"x"}`,
			store: &lifecycleCompatStore{patch: func(context.Context, db.PatchSandboxParams) (db.Sandbox, error) {
				return db.Sandbox{}, db.ErrNotFound
			}},
		},
		{
			name: "update internal", method: http.MethodPatch,
			code: apierr.CodeInternal, operationID: "update", status: http.StatusInternalServerError, body: `{"name":"x"}`,
			store: &lifecycleCompatStore{patch: func(context.Context, db.PatchSandboxParams) (db.Sandbox, error) {
				return db.Sandbox{}, errors.New("db down")
			}},
		},
		{
			name: "stop invalid body", method: http.MethodPost,
			code: apierr.CodeInvalidJSON, operationID: "stop", status: http.StatusBadRequest, body: `{"memory":true}`,
		},
		{
			name: "stop invalid state", method: http.MethodPost,
			code: apierr.CodeInvalidJSON, operationID: "stop", status: http.StatusBadRequest, body: `{}`,
			store: &lifecycleCompatStore{stop: func(context.Context, uuid.UUID, string, bool) (db.StopOperation, db.Sandbox, db.Job, bool, error) {
				return db.StopOperation{}, db.Sandbox{}, db.Job{}, false, db.ErrInvalidState
			}},
		},
		{
			name: "stop not found", method: http.MethodPost,
			code: apierr.CodeNotFound, operationID: "stop", status: http.StatusNotFound, body: `{}`,
			store: &lifecycleCompatStore{stop: func(context.Context, uuid.UUID, string, bool) (db.StopOperation, db.Sandbox, db.Job, bool, error) {
				return db.StopOperation{}, db.Sandbox{}, db.Job{}, false, db.ErrNotFound
			}},
		},
		{
			name: "stop internal", method: http.MethodPost,
			code: apierr.CodeInternal, operationID: "stop", status: http.StatusInternalServerError, body: `{}`,
			store: &lifecycleCompatStore{stop: func(context.Context, uuid.UUID, string, bool) (db.StopOperation, db.Sandbox, db.Job, bool, error) {
				return db.StopOperation{}, db.Sandbox{}, db.Job{}, false, errors.New("node down")
			}},
		},
		{
			name: "getStop not found", method: http.MethodGet,
			code: apierr.CodeNotFound, operationID: "getStop", status: http.StatusNotFound, stopID: "stop_unknown",
			store: &lifecycleCompatStore{stopOp: func(context.Context, uuid.UUID, string, string) (db.StopOperation, error) {
				return db.StopOperation{}, db.ErrNotFound
			}},
		},
		{
			name: "getStop internal", method: http.MethodGet,
			code: apierr.CodeInternal, operationID: "getStop", status: http.StatusInternalServerError, stopID: completed.ID,
			store: &lifecycleCompatStore{stopOp: func(context.Context, uuid.UUID, string, string) (db.StopOperation, error) {
				return db.StopOperation{}, errors.New("db down")
			}},
		},
		{
			name: "resume invalid json", method: http.MethodPost,
			code: apierr.CodeInvalidJSON, operationID: "resume", status: http.StatusBadRequest, body: `{"noEnv":true}`,
		},
		{
			name: "resume invalid env", method: http.MethodPost,
			code: apierr.CodeInvalidEnv, operationID: "resume", status: http.StatusBadRequest, body: `{"env":{"FOO":1}}`,
		},
		{
			name: "resume unknown environment", method: http.MethodPost,
			code: apierr.CodeUnknownEnvironment, operationID: "resume", status: http.StatusBadRequest, body: `{"environment":"customer"}`,
		},
		{
			name: "resume not found", method: http.MethodPost,
			code: apierr.CodeNotFound, operationID: "resume", status: http.StatusNotFound, body: `{}`,
			store: &lifecycleCompatStore{},
		},
		{
			name: "resume stopping", method: http.MethodPost,
			code: apierr.CodeSandboxStopping, operationID: "resume", status: http.StatusConflict, body: `{}`,
			store: &lifecycleCompatStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
				row := archived
				row.State = db.StateArchiving
				return row, nil
			}},
			resume: lifecycleCompatResume{err: db.ErrConflict},
		},
		{
			name: "resume error sandbox", method: http.MethodPost,
			code: apierr.CodeSandboxNotReady, operationID: "resume", status: http.StatusConflict, body: `{}`,
			store: &lifecycleCompatStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
				row := archived
				row.State = db.StateError
				row.ErrorReason = "vm_missing"
				return row, nil
			}},
		},
		{
			name: "resume capacity", method: http.MethodPost,
			code: apierr.CodeNoReadyMachine, operationID: "resume", status: http.StatusServiceUnavailable, body: `{}`,
			resume: lifecycleCompatResume{err: db.ErrNoCapacity},
		},
		{
			name: "resume internal", method: http.MethodPost,
			code: apierr.CodeInternal, operationID: "resume", status: http.StatusInternalServerError, body: `{}`,
			resume: lifecycleCompatResume{err: errors.New("scheduler down")},
		},
	}
	for _, test := range errorTests {
		t.Run(test.name, func(t *testing.T) {
			activeStore := test.store
			if activeStore == nil {
				activeStore = store
			}
			activeResume := test.resume
			if activeResume == nil {
				activeResume = resume
			}
			active := lifecycleCompatHandlers(t, activeStore, activeResume)
			handler := lifecycleCompatHandler(active, test.operationID)
			request := lifecycleCompatPath(coreCompatRequest(test.method, api.PathSandbox, test.body, coreCompatOwner), "bx_23456789", test.stopID)
			recorder := coreCompatServe(handler, request)
			coreCompatAssert(t, validators, test.operationID, test.status, recorder)
			var envelope struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("decode error body: %v", err)
			}
			if envelope.Code != test.code {
				t.Errorf("code = %q, want %q", envelope.Code, test.code)
			}
		})
	}
}

// lifecycleCompatHandler selects the handler registered for one operation id.
func lifecycleCompatHandler(handlers api.Handlers, operationID string) http.HandlerFunc {
	switch operationID {
	case "update":
		return handlers.UpdateSandbox
	case "stop":
		return handlers.StopSandbox
	case "getStop":
		return handlers.GetSandboxStop
	default:
		return handlers.ResumeSandbox
	}
}
