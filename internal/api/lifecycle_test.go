package api

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// lifecycleFakeStore is an in-memory LifecycleStore with per-test hooks.
type lifecycleFakeStore struct {
	sandbox func(context.Context, uuid.UUID, string) (db.Sandbox, error)
	patch   func(context.Context, db.PatchSandboxParams) (db.Sandbox, error)
	stop    func(context.Context, uuid.UUID, string, bool) (db.StopOperation, db.Sandbox, db.Job, bool, error)
	stopOp  func(context.Context, uuid.UUID, string, string) (db.StopOperation, error)

	lastPatch db.PatchSandboxParams
	lastStop  bool
}

func (s *lifecycleFakeStore) Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error) {
	if s.sandbox == nil {
		return db.Sandbox{}, db.ErrNotFound
	}
	return s.sandbox(ctx, owner, id)
}

func (s *lifecycleFakeStore) PatchSandbox(ctx context.Context, params db.PatchSandboxParams) (db.Sandbox, error) {
	s.lastPatch = params
	if s.patch == nil {
		return db.Sandbox{}, db.ErrNotFound
	}
	return s.patch(ctx, params)
}

func (s *lifecycleFakeStore) AcceptSandboxStop(ctx context.Context, owner uuid.UUID, sandboxID string, force bool) (db.StopOperation, db.Sandbox, db.Job, bool, error) {
	s.lastStop = force
	if s.stop == nil {
		return db.StopOperation{}, db.Sandbox{}, db.Job{}, false, db.ErrNotFound
	}
	return s.stop(ctx, owner, sandboxID, force)
}

func (s *lifecycleFakeStore) StopOperation(ctx context.Context, owner uuid.UUID, sandboxID, stopID string) (db.StopOperation, error) {
	if s.stopOp == nil {
		return db.StopOperation{
			ID: stopID, OwnerID: owner, SandboxID: sandboxID,
			Status: db.StopCompleted, RequestedAt: coreFixedNow,
		}, nil
	}
	return s.stopOp(ctx, owner, sandboxID, stopID)
}

var _ LifecycleStore = (*lifecycleFakeStore)(nil)

// lifecycleFakeResume records resume requests and returns one outcome.
type lifecycleFakeResume struct {
	calls  []ResumeRequest
	result ResumeResult
	err    error
}

func (r *lifecycleFakeResume) Resume(_ context.Context, request ResumeRequest) (ResumeResult, error) {
	r.calls = append(r.calls, request)
	return r.result, r.err
}

var _ ResumeAdmission = (*lifecycleFakeResume)(nil)

// lifecycleTestHandlers builds the lifecycle handlers over the fakes.
func lifecycleTestHandlers(t *testing.T, store LifecycleStore, resume ResumeAdmission) Handlers {
	t.Helper()
	handlers, err := NewLifecycleHandlers(LifecycleOptions{Store: store, Resume: resume})
	if err != nil {
		t.Fatalf("NewLifecycleHandlers: %v", err)
	}
	return handlers
}

// lifecycleSandbox is one archived owner sandbox fixture.
func lifecycleSandbox(owner uuid.UUID) db.Sandbox {
	stopID := "stop_0123456789abcdef"
	stopped := coreFixedNow
	return db.Sandbox{
		ID: "bx_23456789", OwnerID: owner, State: db.StateArchived,
		MachineType: "default", Name: "job", TTLSeconds: new(3600),
		StopOperationID: &stopID, StoppedAt: &stopped,
		CreatedAt: coreFixedNow, UpdatedAt: coreFixedNow,
		SnapshotsEnabled: true, // the database default
	}
}

// lifecycleStop is one completed stop fixture.
func lifecycleStop() db.StopOperation {
	stopped := coreFixedNow
	return db.StopOperation{
		ID: "stop_0123456789abcdef", OwnerID: coreTestOwner, SandboxID: "bx_23456789",
		Status: db.StopCompleted, RequestedAt: coreFixedNow,
		LastAttemptAt: &stopped, EndedAt: &stopped,
	}
}

// lifecycleSetPath sets the route parameters a real mux would extract.
func lifecycleSetPath(request *http.Request, sandboxID, stopID string) *http.Request {
	request.SetPathValue(ParamSandboxID, sandboxID)
	if stopID != "" {
		request.SetPathValue(ParamStopID, stopID)
	}
	return request
}

// lifecycleDecode decodes one successful lifecycle body.
func lifecycleDecode[T any](t *testing.T, recorderBody []byte) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(recorderBody, &out); err != nil {
		t.Fatalf("decode lifecycle response failed (%d bytes)", len(recorderBody))
	}
	return out
}

func TestSandboxFromRowExportsPersistedLifecycleValues(t *testing.T) {
	stopped := coreFixedNow
	expires := coreFixedNow.Add(10 * time.Minute)
	row := db.Sandbox{
		ID: "bx_23456789", State: db.StateArchived, MachineType: "default",
		Name: "job", TTLSeconds: new(7200), ExpiresAt: &expires,
		Subdomain: new("acme-staging"), StoppedAt: &stopped,
		CreatedAt: coreFixedNow, UpdatedAt: coreFixedNow,
	}
	out := sandboxFromRow(row)
	if out.TTLSeconds == nil || *out.TTLSeconds != 7200 {
		t.Errorf("ttlSeconds = %v, want the configured 7200, not the remaining time", out.TTLSeconds)
	}
	if out.ArchiveAfter == nil || !out.ArchiveAfter.Equal(expires) {
		t.Errorf("archiveAfter = %v, want %v", out.ArchiveAfter, expires)
	}
	if out.Subdomain == nil || *out.Subdomain != "acme-staging" {
		t.Errorf("subdomain = %v", out.Subdomain)
	}
	if out.StoppedAt == nil || !out.StoppedAt.Equal(stopped) {
		t.Errorf("stoppedAt = %v, want %v", out.StoppedAt, stopped)
	}
}

func TestOperationFromRowTerminalAndFailing(t *testing.T) {
	requested := coreFixedNow
	failedAt := coreFixedNow.Add(time.Second)
	failing := db.StopOperation{
		ID: "stop_failing", Status: db.StopFailing, RequestedAt: requested,
		LastAttemptAt: &failedAt, Error: "disk busy",
	}
	out := operationFromRow(failing, false)
	if out.Error == nil || *out.Error != "disk busy" || out.RequestedAt == nil {
		t.Errorf("failing view = %+v", out)
	}
	completed := failing
	completed.Status = db.StopCompleted
	completed.EndedAt = &failedAt
	out = operationFromRow(completed, false)
	if out.RequestedAt != nil || out.Error != nil {
		t.Errorf("terminal sandbox view = %+v, want null requestedAt and error", out)
	}
	out = operationFromRow(completed, true)
	if out.RequestedAt == nil {
		t.Errorf("by-id view dropped requestedAt")
	}
}

func TestLifecycleHandlersRequireAuthentication(t *testing.T) {
	handlers := lifecycleTestHandlers(t, &lifecycleFakeStore{}, &lifecycleFakeResume{})
	tests := []struct {
		name    string
		handler func(Handlers) http.HandlerFunc
		method  string
		body    string
	}{
		{"update", func(h Handlers) http.HandlerFunc { return h.UpdateSandbox }, http.MethodPatch, `{}`},
		{"stop", func(h Handlers) http.HandlerFunc { return h.StopSandbox }, http.MethodPost, ``},
		{"getStop", func(h Handlers) http.HandlerFunc { return h.GetSandboxStop }, http.MethodGet, ``},
		{"resume", func(h Handlers) http.HandlerFunc { return h.ResumeSandbox }, http.MethodPost, ``},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := lifecycleSetPath(coreTestRequest(test.method, PathSandbox, strings.NewReader(test.body), uuid.Nil()), "bx_23456789", "stop_0123456789abcdef")
			recorder := coreServe(test.handler(handlers), request)
			coreAssertError(t, recorder, http.StatusUnauthorized, apierr.CodeUnauthorized)
		})
	}
}

func TestNewLifecycleHandlersRequiresDependencies(t *testing.T) {
	if _, err := NewLifecycleHandlers(LifecycleOptions{Resume: &lifecycleFakeResume{}}); err == nil {
		t.Error("NewLifecycleHandlers accepted a nil store")
	}
	if _, err := NewLifecycleHandlers(LifecycleOptions{Store: &lifecycleFakeStore{}}); err == nil {
		t.Error("NewLifecycleHandlers accepted a nil resume admission")
	}
}

func TestUpdateSandboxAppliesNameTTLAndSubdomain(t *testing.T) {
	store := &lifecycleFakeStore{}
	store.patch = func(_ context.Context, params db.PatchSandboxParams) (db.Sandbox, error) {
		row := lifecycleSandbox(coreTestOwner)
		row.State = db.StateArchived
		row.Name = *params.Name
		row.Subdomain = params.Subdomain
		if params.TTL.Set && params.TTL.Seconds == nil {
			row.TTLSeconds = nil
		} else if params.TTL.Seconds != nil {
			row.TTLSeconds = new(*params.TTL.Seconds)
		}
		return row, nil
	}
	handlers := lifecycleTestHandlers(t, store, &lifecycleFakeResume{})

	name := "Customer support run"
	body := `{"name":"` + name + `","ttlSeconds":7200,"subdomain":"acme-staging"}`
	request := lifecycleSetPath(coreTestRequest(http.MethodPatch, PathSandbox, strings.NewReader(body), coreTestOwner), "bx_23456789", "")
	request.Header.Set("Content-Type", "application/json")
	recorder := coreServe(handlers.UpdateSandbox, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	if store.lastPatch.Name == nil || *store.lastPatch.Name != name {
		t.Errorf("name param = %v", store.lastPatch.Name)
	}
	if !store.lastPatch.TTL.Set || store.lastPatch.TTL.Seconds == nil || *store.lastPatch.TTL.Seconds != 7200 {
		t.Errorf("ttl param = %+v", store.lastPatch.TTL)
	}
	if store.lastPatch.Subdomain == nil || *store.lastPatch.Subdomain != "acme-staging" {
		t.Errorf("subdomain param = %v", store.lastPatch.Subdomain)
	}
	response := lifecycleDecode[SandboxInfoResponse](t, recorder.Body.Bytes())
	if response.Type != TypeSandboxInfo || response.Sandbox.Name != name ||
		response.Sandbox.Subdomain == nil || *response.Sandbox.Subdomain != "acme-staging" {
		t.Errorf("response = %+v", response)
	}
	if response.Sandbox.TTLSeconds == nil || *response.Sandbox.TTLSeconds != 7200 {
		t.Errorf("response ttl = %v", response.Sandbox.TTLSeconds)
	}
	if response.Sandbox.Stop == nil || response.Sandbox.Stop.ID != "stop_0123456789abcdef" {
		t.Errorf("response stop = %+v", response.Sandbox.Stop)
	}
}

func TestUpdateSandboxTTLTriState(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		set   bool
		value *int
	}{
		{"omitted", `{"name":"x"}`, false, nil},
		{"null disables", `{"ttlSeconds":null}`, true, nil},
		{"value", `{"ttlSeconds":2592000}`, true, new(2592000)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &lifecycleFakeStore{}
			store.patch = func(_ context.Context, params db.PatchSandboxParams) (db.Sandbox, error) {
				if params.TTL.Set != test.set {
					t.Errorf("TTL.Set = %t, want %t", params.TTL.Set, test.set)
				}
				if test.value == nil {
					if params.TTL.Seconds != nil {
						t.Errorf("TTL.Seconds = %v, want nil", *params.TTL.Seconds)
					}
				} else if params.TTL.Seconds == nil || *params.TTL.Seconds != *test.value {
					t.Errorf("TTL.Seconds = %v, want %d", params.TTL.Seconds, *test.value)
				}
				return lifecycleSandbox(coreTestOwner), nil
			}
			handlers := lifecycleTestHandlers(t, store, &lifecycleFakeResume{})
			request := lifecycleSetPath(coreTestRequest(http.MethodPatch, PathSandbox, strings.NewReader(test.body), coreTestOwner), "bx_23456789", "")
			recorder := coreServe(handlers.UpdateSandbox, request)
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestUpdateSandboxValidation(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{"missing body", ``, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"null body", `null`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"array body", `[]`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"unknown member", `{"labels":{}}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"name null", `{"name":null}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"name empty", `{"name":""}`, http.StatusBadRequest, apierr.CodeInvalidName},
		{"name number", `{"name":7}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"ttl zero", `{"ttlSeconds":0}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"ttl too large", `{"ttlSeconds":2592001}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"ttl string", `{"ttlSeconds":"infinite"}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"ttl fraction", `{"ttlSeconds":1.5}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"subdomain null", `{"subdomain":null}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"subdomain uppercase", `{"subdomain":"Acme"}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"subdomain double hyphen", `{"subdomain":"acme--staging"}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"subdomain desktop suffix", `{"subdomain":"acme-desktop"}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"subdomain number suffix", `{"subdomain":"acme-12"}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"subdomain too short", `{"subdomain":"ab"}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"subdomain too long", `{"subdomain":"` + strings.Repeat("a", 41) + `"}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &lifecycleFakeStore{}
			handlers := lifecycleTestHandlers(t, store, &lifecycleFakeResume{})
			request := lifecycleSetPath(coreTestRequest(http.MethodPatch, PathSandbox, strings.NewReader(test.body), coreTestOwner), "bx_23456789", "")
			recorder := coreServe(handlers.UpdateSandbox, request)
			coreAssertError(t, recorder, test.status, test.code)
		})
	}
}

func TestUpdateSandboxTruncatesLongName(t *testing.T) {
	store := &lifecycleFakeStore{}
	store.patch = func(_ context.Context, params db.PatchSandboxParams) (db.Sandbox, error) {
		if params.Name == nil {
			t.Fatal("name param is nil")
		}
		if got := len([]rune(*params.Name)); got != db.MaxSandboxNameLength {
			t.Errorf("name runes = %d, want %d", got, db.MaxSandboxNameLength)
		}
		return lifecycleSandbox(coreTestOwner), nil
	}
	handlers := lifecycleTestHandlers(t, store, &lifecycleFakeResume{})
	long := strings.Repeat("é", db.MaxSandboxNameLength+10)
	body := `{"name":"` + long + `"}`
	request := lifecycleSetPath(coreTestRequest(http.MethodPatch, PathSandbox, strings.NewReader(body), coreTestOwner), "bx_23456789", "")
	recorder := coreServe(handlers.UpdateSandbox, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestUpdateSandboxFailures(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"missing", db.ErrNotFound, http.StatusNotFound, apierr.CodeNotFound},
		{"invalid name", db.ErrInvalidName, http.StatusBadRequest, apierr.CodeInvalidName},
		{"invalid subdomain", db.ErrInvalidSubdomain, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"taken subdomain", db.ErrSubdomainTaken, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"store fault", errors.New("connection reset"), http.StatusInternalServerError, apierr.CodeInternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &lifecycleFakeStore{patch: func(context.Context, db.PatchSandboxParams) (db.Sandbox, error) {
				return db.Sandbox{}, test.err
			}}
			handlers := lifecycleTestHandlers(t, store, &lifecycleFakeResume{})
			request := lifecycleSetPath(coreTestRequest(http.MethodPatch, PathSandbox, strings.NewReader(`{"name":"x"}`), coreTestOwner), "bx_23456789", "")
			recorder := coreServe(handlers.UpdateSandbox, request)
			coreAssertError(t, recorder, test.status, test.code)
		})
	}
}

func TestUpdateSandboxOwnerScope(t *testing.T) {
	store := &lifecycleFakeStore{patch: func(_ context.Context, params db.PatchSandboxParams) (db.Sandbox, error) {
		if params.Owner != coreTestOwner {
			t.Errorf("owner = %s", params.Owner)
		}
		if params.SandboxID != "bx_23456789" {
			t.Errorf("sandbox id = %q", params.SandboxID)
		}
		return db.Sandbox{}, db.ErrNotFound
	}}
	handlers := lifecycleTestHandlers(t, store, &lifecycleFakeResume{})
	request := lifecycleSetPath(coreTestRequest(http.MethodPatch, PathSandbox, strings.NewReader(`{"name":"x"}`), coreTestOwner), "bx_23456789", "")
	recorder := coreServe(handlers.UpdateSandbox, request)
	coreAssertError(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
}

func TestStopSandboxPendingAndArchived(t *testing.T) {
	pending := db.StopOperation{
		ID: "stop_0123456789abcdef", OwnerID: coreTestOwner, SandboxID: "bx_23456789",
		Status: db.StopPending, RequestedAt: coreFixedNow,
	}
	tests := []struct {
		name          string
		operation     db.StopOperation
		state         db.SandboxState
		status        int
		responseType  string
		statusValue   string
		requestedNull bool
		body          string
		forced        bool
	}{
		{"new stop", pending, db.StateArchiving, http.StatusAccepted, TypeSandboxStopping, "archiving", false, `{}`, false},
		{"force stop", pending, db.StateArchiving, http.StatusAccepted, TypeSandboxStopping, "archiving", false, `{"force":true}`, true},
		{"already archived", lifecycleStop(), db.StateArchived, http.StatusOK, TypeSandboxStopped, "archived", true, `{}`, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &lifecycleFakeStore{}
			store.stop = func(_ context.Context, owner uuid.UUID, sandboxID string, force bool) (db.StopOperation, db.Sandbox, db.Job, bool, error) {
				if owner != coreTestOwner || sandboxID != "bx_23456789" {
					t.Errorf("lookup = owner %s sandbox %q", owner, sandboxID)
				}
				row := lifecycleSandbox(coreTestOwner)
				row.State = test.state
				row.StopOperationID = new(test.operation.ID)
				return test.operation, row, db.Job{}, false, nil
			}
			handlers := lifecycleTestHandlers(t, store, &lifecycleFakeResume{})
			request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxStop, strings.NewReader(test.body), coreTestOwner), "bx_23456789", "")
			recorder := coreServe(handlers.StopSandbox, request)
			if recorder.Code != test.status {
				t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
			}
			if store.lastStop != test.forced {
				t.Errorf("force = %t, want %t", store.lastStop, test.forced)
			}
			response := lifecycleDecode[SandboxActionResponse](t, recorder.Body.Bytes())
			if response.Type != test.responseType || response.ID != "bx_23456789" || response.Status != test.statusValue {
				t.Errorf("response = %+v", response)
			}
			if response.Operation == nil || response.Operation.ID != test.operation.ID {
				t.Fatalf("operation = %+v", response.Operation)
			}
			if (response.Operation.RequestedAt == nil) != test.requestedNull {
				t.Errorf("requestedAt = %v, want null = %t", response.Operation.RequestedAt, test.requestedNull)
			}
			if response.StatusURL == nil || *response.StatusURL != "/api/v1/sandboxes/bx_23456789/stops/stop_0123456789abcdef" {
				t.Errorf("statusUrl = %v", response.StatusURL)
			}
			if response.Sandbox == nil || response.Sandbox.State != SandboxState(test.state) {
				t.Errorf("sandbox = %+v", response.Sandbox)
			}
		})
	}
}

func TestStopSandboxReplayReturnsSameOperation(t *testing.T) {
	pending := db.StopOperation{
		ID: "stop_0123456789abcdef", OwnerID: coreTestOwner, SandboxID: "bx_23456789",
		Status: db.StopFailing, RequestedAt: coreFixedNow, Error: "disk busy", ForceRequested: true,
	}
	store := &lifecycleFakeStore{}
	store.stop = func(context.Context, uuid.UUID, string, bool) (db.StopOperation, db.Sandbox, db.Job, bool, error) {
		row := lifecycleSandbox(coreTestOwner)
		row.State = db.StateArchiving
		return pending, row, db.Job{}, false, nil
	}
	handlers := lifecycleTestHandlers(t, store, &lifecycleFakeResume{})
	request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxStop, nil, coreTestOwner), "bx_23456789", "")
	recorder := coreServe(handlers.StopSandbox, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	response := lifecycleDecode[SandboxActionResponse](t, recorder.Body.Bytes())
	if response.Operation == nil || response.Operation.Status != StopFailing ||
		response.Operation.Error == nil || *response.Operation.Error != "disk busy" {
		t.Errorf("operation = %+v", response.Operation)
	}
	if response.Forced == nil || !*response.Forced {
		t.Errorf("forced = %v", response.Forced)
	}
}

func TestStopSandboxValidationAndFailures(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		err    error
		status int
		code   string
	}{
		{"null body", `null`, nil, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"unknown member", `{"memory":true}`, nil, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"force not bool", `{"force":"yes"}`, nil, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"missing", `{}`, db.ErrNotFound, http.StatusNotFound, apierr.CodeNotFound},
		{"invalid state", `{}`, db.ErrInvalidState, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"store fault", `{}`, errors.New("node down"), http.StatusInternalServerError, apierr.CodeInternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &lifecycleFakeStore{}
			if test.err != nil {
				store.stop = func(context.Context, uuid.UUID, string, bool) (db.StopOperation, db.Sandbox, db.Job, bool, error) {
					return db.StopOperation{}, db.Sandbox{}, db.Job{}, false, test.err
				}
			}
			handlers := lifecycleTestHandlers(t, store, &lifecycleFakeResume{})
			request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxStop, strings.NewReader(test.body), coreTestOwner), "bx_23456789", "")
			recorder := coreServe(handlers.StopSandbox, request)
			coreAssertError(t, recorder, test.status, test.code)
		})
	}
}

func TestGetSandboxStop(t *testing.T) {
	operation := lifecycleStop()
	store := &lifecycleFakeStore{stopOp: func(_ context.Context, owner uuid.UUID, sandboxID, stopID string) (db.StopOperation, error) {
		if owner != coreTestOwner || sandboxID != "bx_23456789" || stopID != operation.ID {
			t.Errorf("lookup = %s %s %s", owner, sandboxID, stopID)
		}
		return operation, nil
	}}
	handlers := lifecycleTestHandlers(t, store, &lifecycleFakeResume{})
	request := lifecycleSetPath(coreTestRequest(http.MethodGet, PathSandboxStops, nil, coreTestOwner), "bx_23456789", operation.ID)
	recorder := coreServe(handlers.GetSandboxStop, request)
	if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status = %d headers = %v body = %s", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	response := lifecycleDecode[StopOperationResponse](t, recorder.Body.Bytes())
	if response.Type != TypeStopStatus || response.ID != "bx_23456789" ||
		response.Stop.ID != operation.ID || response.Stop.RequestedAt == nil {
		t.Errorf("response = %+v", response)
	}
}

func TestGetSandboxStopFailures(t *testing.T) {
	tests := []struct {
		name   string
		stopID string
		err    error
		status int
		code   string
	}{
		{"missing stop", "stop_unknown", db.ErrNotFound, http.StatusNotFound, apierr.CodeNotFound},
		{"missing id", "", nil, http.StatusNotFound, apierr.CodeNotFound},
		{"store fault", "stop_0123456789abcdef", errors.New("connection reset"), http.StatusInternalServerError, apierr.CodeInternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &lifecycleFakeStore{stopOp: func(context.Context, uuid.UUID, string, string) (db.StopOperation, error) {
				return db.StopOperation{}, test.err
			}}
			handlers := lifecycleTestHandlers(t, store, &lifecycleFakeResume{})
			request := lifecycleSetPath(coreTestRequest(http.MethodGet, PathSandboxStops, nil, coreTestOwner), "bx_23456789", test.stopID)
			recorder := coreServe(handlers.GetSandboxStop, request)
			coreAssertError(t, recorder, test.status, test.code)
		})
	}
}

func TestResumeSandboxAppliesOptions(t *testing.T) {
	stopped := lifecycleSandbox(coreTestOwner)
	resume := &lifecycleFakeResume{}
	store := &lifecycleFakeStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
		return stopped, nil
	}}
	resume.result = ResumeResult{Sandbox: func() db.Sandbox {
		row := stopped
		row.State = db.StateProvisioning
		return row
	}()}
	handlers := lifecycleTestHandlers(t, store, resume)
	body := `{"type":"large","env":{"FOO":"bar"},"ttlSeconds":7200,"failFast":true}`
	request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, strings.NewReader(body), coreTestOwner), "bx_23456789", "")
	recorder := coreServe(handlers.ResumeSandbox, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	if len(resume.calls) != 1 {
		t.Fatalf("resume calls = %d", len(resume.calls))
	}
	call := resume.calls[0]
	if call.Owner != coreTestOwner || call.SandboxID != "bx_23456789" || !call.FailFast {
		t.Errorf("resume request = %+v", call)
	}
	if call.MachineType != "large" || call.Resources.VCPUs != 8 || call.Resources.DiskGiB != 111 {
		t.Errorf("shape = %s %+v", call.MachineType, call.Resources)
	}
	if !call.Env.Set || call.Env.Values["FOO"] != "bar" {
		t.Errorf("env = %+v", call.Env)
	}
	if !call.TTL.Set || call.TTL.Seconds == nil || *call.TTL.Seconds != 7200 {
		t.Errorf("ttl = %+v", call.TTL)
	}
	if call.SupersedeStopID != "" {
		t.Errorf("supersede = %q, want empty for archived", call.SupersedeStopID)
	}
	response := lifecycleDecode[SandboxActionResponse](t, recorder.Body.Bytes())
	if response.Type != TypeSandboxResuming || response.Status != "resuming" || response.ID != "bx_23456789" {
		t.Errorf("response = %+v", response)
	}
	if response.Sandbox == nil || response.Sandbox.State != StateProvisioning {
		t.Errorf("sandbox = %+v", response.Sandbox)
	}
}

func TestResumeSandboxDefaultBodyAndSupersede(t *testing.T) {
	archiving := lifecycleSandbox(coreTestOwner)
	archiving.State = db.StateArchiving
	archiving.StopOperationID = new("stop_live")
	resume := &lifecycleFakeResume{result: ResumeResult{Sandbox: archiving}}
	store := &lifecycleFakeStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
		return archiving, nil
	}}
	handlers := lifecycleTestHandlers(t, store, resume)
	request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, nil, coreTestOwner), "bx_23456789", "")
	recorder := coreServe(handlers.ResumeSandbox, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	call := resume.calls[0]
	if call.MachineType != "" || call.Resources != (db.Resources{}) {
		t.Errorf("default shape = %q %+v", call.MachineType, call.Resources)
	}
	if call.Env.Set || call.TTL.Set || call.FailFast {
		t.Errorf("default options = %+v", call)
	}
	if call.SupersedeStopID != "stop_live" {
		t.Errorf("supersede = %q, want stop_live", call.SupersedeStopID)
	}
}

func TestResumeSandboxValidation(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{"null body", `null`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"unknown member", `{"memory":true}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"bad type", `{"type":"huge"}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"type null", `{"type":null}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"env null", `{"env":null}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"env not string", `{"env":{"FOO":1}}`, http.StatusBadRequest, apierr.CodeInvalidEnv},
		{"env null value", `{"env":{"FOO":null}}`, http.StatusBadRequest, apierr.CodeInvalidEnv},
		{"env reserved", `{"env":{"ASCII_TOKEN":"x"}}`, http.StatusBadRequest, apierr.CodeInvalidEnv},
		{"environment unknown", `{"environment":"customer"}`, http.StatusBadRequest, apierr.CodeUnknownEnvironment},
		{"environment null", `{"environment":null}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"noEnv true", `{"noEnv":true}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"noEnv not bool", `{"noEnv":1}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"ttl zero", `{"ttlSeconds":0}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"ttl string", `{"ttlSeconds":"infinite"}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"failFast not bool", `{"failFast":1}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &lifecycleFakeStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
				return lifecycleSandbox(coreTestOwner), nil
			}}
			resume := &lifecycleFakeResume{}
			handlers := lifecycleTestHandlers(t, store, resume)
			request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, strings.NewReader(test.body), coreTestOwner), "bx_23456789", "")
			recorder := coreServe(handlers.ResumeSandbox, request)
			coreAssertError(t, recorder, test.status, test.code)
			if len(resume.calls) != 0 {
				t.Errorf("resume was called %d times for a rejected body", len(resume.calls))
			}
		})
	}
}

func TestResumeSandboxNullTTLDisablesAutoStop(t *testing.T) {
	row := lifecycleSandbox(coreTestOwner)
	row.TTLSeconds = nil
	resume := &lifecycleFakeResume{result: ResumeResult{Sandbox: row}}
	store := &lifecycleFakeStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
		return row, nil
	}}
	handlers := lifecycleTestHandlers(t, store, resume)
	request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, strings.NewReader(`{"ttlSeconds":null}`), coreTestOwner), "bx_23456789", "")
	recorder := coreServe(handlers.ResumeSandbox, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	call := resume.calls[0]
	if !call.TTL.Set || call.TTL.Seconds != nil {
		t.Errorf("ttl = %+v, want explicit null", call.TTL)
	}
	response := lifecycleDecode[SandboxActionResponse](t, recorder.Body.Bytes())
	if response.Sandbox == nil || response.Sandbox.TTLSeconds != nil || response.Sandbox.ArchiveAfter != nil {
		t.Errorf("response sandbox = %+v", response.Sandbox)
	}
}

func TestResumeSandboxNoEnvFalseAccepted(t *testing.T) {
	resume := &lifecycleFakeResume{result: ResumeResult{Sandbox: lifecycleSandbox(coreTestOwner)}}
	store := &lifecycleFakeStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
		return lifecycleSandbox(coreTestOwner), nil
	}}
	handlers := lifecycleTestHandlers(t, store, resume)
	request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, strings.NewReader(`{"noEnv":false,"environment":"base"}`), coreTestOwner), "bx_23456789", "")
	recorder := coreServe(handlers.ResumeSandbox, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	if resume.calls[0].Env.Set {
		t.Errorf("noEnv false changed env")
	}
}

func TestResumeSandboxFailures(t *testing.T) {
	tests := []struct {
		name   string
		state  db.SandboxState
		err    error
		body   string
		status int
		code   string
	}{
		{"missing sandbox", db.StateArchived, db.ErrNotFound, `{}`, http.StatusNotFound, apierr.CodeNotFound},
		{"capacity", db.StateArchived, db.ErrNoCapacity, `{}`, http.StatusServiceUnavailable, apierr.CodeNoReadyMachine},
		{"scheduler capacity", db.StateArchived, scheduler.ErrNoCapacity, `{}`, http.StatusServiceUnavailable, apierr.CodeNoReadyMachine},
		{"scheduler no node", db.StateArchived, scheduler.ErrNoNode, `{}`, http.StatusServiceUnavailable, apierr.CodeNoReadyMachine},
		{"node unavailable", db.StateArchived, db.ErrNodeUnavailable, `{}`, http.StatusServiceUnavailable, apierr.CodeNoReadyMachine},
		{"type too small", db.StateArchived, db.ErrTypeTooSmall, `{"type":"small"}`, http.StatusConflict, "type_too_small"},
		{"invalid machine", db.StateArchived, db.ErrInvalidMachine, `{}`, http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"provisioning", db.StateProvisioning, db.ErrInvalidState, `{}`, http.StatusConflict, apierr.CodeSandboxResuming},
		{"running with change", db.StateRunning, db.ErrInvalidState, `{"type":"large"}`, http.StatusConflict, apierr.CodeSandboxRunningStopFirst},
		{"running with env", db.StateRunning, db.ErrInvalidState, `{"env":{"FOO":"bar"}}`, http.StatusConflict, apierr.CodeSandboxRunningStopFirst},
		{"running with environment", db.StateRunning, db.ErrInvalidState, `{"environment":"base"}`, http.StatusConflict, apierr.CodeSandboxRunningStopFirst},
		{"running without change", db.StateRunning, db.ErrInvalidState, `{}`, http.StatusConflict, apierr.CodeConflict},
		{"conflict archived", db.StateArchived, db.ErrConflict, `{}`, http.StatusConflict, apierr.CodeConflict},
		{"superseded", db.StateArchived, db.ErrStopSuperseded, `{}`, http.StatusConflict, apierr.CodeConflict},
		{"store fault", db.StateArchived, errors.New("node down"), `{}`, http.StatusInternalServerError, apierr.CodeInternal},
		// resume on another node.
		{"data expired", db.StateArchived, db.ErrSnapshotDataExpired, `{}`, http.StatusConflict, apierr.CodeSnapshotDataExpired},
		{"nothing to restore", db.StateArchived, db.ErrNoRestorableData, `{}`, http.StatusConflict, apierr.CodeResumeFailed},
		{"snapshots disabled", db.StateArchived, db.ErrSnapshotsDisabled, `{}`, http.StatusConflict, apierr.CodeSnapshotsDisabled},
		{"generation purged meanwhile", db.StateArchived, db.ErrSnapshotNotFound, `{}`, http.StatusConflict, apierr.CodeConflict},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row := lifecycleSandbox(coreTestOwner)
			row.State = test.state
			if test.state == db.StateArchiving {
				row.StopOperationID = new("stop_live")
			}
			store := &lifecycleFakeStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
				return row, nil
			}}
			resume := &lifecycleFakeResume{err: test.err}
			handlers := lifecycleTestHandlers(t, store, resume)
			request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, strings.NewReader(test.body), coreTestOwner), "bx_23456789", "")
			recorder := coreServe(handlers.ResumeSandbox, request)
			coreAssertError(t, recorder, test.status, test.code)
		})
	}
}

func TestResumeSandboxInitialBootStopConflict(t *testing.T) {
	archiving := lifecycleSandbox(coreTestOwner)
	archiving.State = db.StateArchiving
	archiving.StopOperationID = new("stop_live")
	store := &lifecycleFakeStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
		return archiving, nil
	}}
	resume := &lifecycleFakeResume{err: db.ErrConflict}
	handlers := lifecycleTestHandlers(t, store, resume)
	request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, nil, coreTestOwner), "bx_23456789", "")
	recorder := coreServe(handlers.ResumeSandbox, request)
	body := coreAssertError(t, recorder, http.StatusConflict, apierr.CodeSandboxStopping)
	if body.Error.Details["statusUrl"] != "/api/v1/sandboxes/bx_23456789/stops/stop_live" {
		t.Errorf("details = %+v", body.Error.Details)
	}
}

func TestResumeSandboxErrorStateRefusedWithoutAdmission(t *testing.T) {
	errored := lifecycleSandbox(coreTestOwner)
	errored.State = db.StateError
	errored.ErrorReason = "vm_missing"
	store := &lifecycleFakeStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
		return errored, nil
	}}
	resume := &lifecycleFakeResume{}
	handlers := lifecycleTestHandlers(t, store, resume)
	request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, nil, coreTestOwner), "bx_23456789", "")
	recorder := coreServe(handlers.ResumeSandbox, request)
	body := coreAssertError(t, recorder, http.StatusConflict, apierr.CodeSandboxNotReady)
	if retryable, ok := body.Error.Details["retryable"].(bool); !ok || retryable {
		t.Errorf("details.retryable = %v, want false", body.Error.Details["retryable"])
	}
	if len(resume.calls) != 0 {
		t.Errorf("resume admission called %d times for an error sandbox", len(resume.calls))
	}
}

func TestResumeSandboxMalformedResult(t *testing.T) {
	tests := []struct {
		name   string
		result db.Sandbox
	}{
		{"empty id", db.Sandbox{OwnerID: coreTestOwner}},
		{"wrong id same owner", db.Sandbox{ID: "bx_abcdefgh", OwnerID: coreTestOwner}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &lifecycleFakeStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
				return lifecycleSandbox(coreTestOwner), nil
			}}
			resume := &lifecycleFakeResume{result: ResumeResult{Sandbox: test.result}}
			handlers := lifecycleTestHandlers(t, store, resume)
			request := lifecycleSetPath(coreTestRequest(http.MethodPost, PathSandboxResume, nil, coreTestOwner), "bx_23456789", "")
			recorder := coreServe(handlers.ResumeSandbox, request)
			coreAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
			if test.result.ID != "" && strings.Contains(recorder.Body.String(), test.result.ID) {
				t.Errorf("500 body leaked the mismatched sandbox id %q", test.result.ID)
			}
		})
	}
}
