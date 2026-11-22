package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
	"github.com/nathabonfim59/bamboo-raft/internal/version"
)

var coreTestOwner = uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-123456789abc")

var coreFixedNow = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

// coreFakeStore is an in-memory CoreStore with per-test hooks.
type coreFakeStore struct {
	sandbox    func(context.Context, uuid.UUID, string) (db.Sandbox, error)
	list       func(context.Context, db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error)
	del        func(context.Context, uuid.UUID, string, db.DeletionReason) (db.DeletionOperation, db.Sandbox, error)
	operation  func(context.Context, uuid.UUID, string) (db.DeletionOperation, error)
	lastList   db.SandboxListQuery
	listCalls  int
	sandboxHit int
}

func (s *coreFakeStore) Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error) {
	s.sandboxHit++
	if s.sandbox == nil {
		return db.Sandbox{}, db.ErrNotFound
	}
	return s.sandbox(ctx, owner, id)
}

func (s *coreFakeStore) ListSandboxes(ctx context.Context, query db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error) {
	s.listCalls++
	s.lastList = query
	if s.list == nil {
		return nil, nil, nil
	}
	return s.list(ctx, query)
}

func (s *coreFakeStore) AcceptSandboxDeletion(ctx context.Context, owner uuid.UUID, id string, reason db.DeletionReason) (db.DeletionOperation, db.Sandbox, error) {
	if s.del == nil {
		return db.DeletionOperation{}, db.Sandbox{}, db.ErrNotFound
	}
	return s.del(ctx, owner, id, reason)
}

func (s *coreFakeStore) DeletionOperation(ctx context.Context, owner uuid.UUID, id string) (db.DeletionOperation, error) {
	if s.operation == nil {
		return db.DeletionOperation{}, db.ErrNotFound
	}
	return s.operation(ctx, owner, id)
}

// coreFakeAdmission records requests and returns a generated sandbox.
type coreFakeAdmission struct {
	calls   []AdmissionRequest
	result  AdmissionResult
	err     error
	onAdmit func(AdmissionRequest)
}

func (a *coreFakeAdmission) Admit(_ context.Context, request AdmissionRequest) (AdmissionResult, error) {
	a.calls = append(a.calls, request)
	if a.onAdmit != nil {
		a.onAdmit(request)
	}
	return a.result, a.err
}

// coreSandboxFromRequest mirrors what a real admission adapter persists.
func coreSandboxFromRequest(request AdmissionRequest, state db.SandboxState) db.Sandbox {
	sandbox := db.Sandbox{
		ID:          request.SandboxID,
		OwnerID:     request.Owner,
		State:       state,
		MachineType: request.MachineType,
		Name:        request.Name,
		Labels:      request.Labels,
		CreatedAt:   coreFixedNow,
		UpdatedAt:   coreFixedNow,
		// nil means on, as CreateSandbox stores it.
		SnapshotsEnabled: request.SnapshotsEnabled == nil || *request.SnapshotsEnabled,
	}
	if request.TTLSeconds != nil {
		expiresAt := coreFixedNow.Add(time.Duration(*request.TTLSeconds) * time.Second)
		sandbox.ExpiresAt = &expiresAt
	}
	return sandbox
}

// coreTestHandlers builds handlers over the fakes with a frozen clock.
func coreTestHandlers(t *testing.T, store CoreStore, admission Admission) Handlers {
	t.Helper()
	handlers, err := NewCoreHandlers(CoreOptions{
		Store:     store,
		Admission: admission,
		Now:       func() time.Time { return coreFixedNow },
	})
	if err != nil {
		t.Fatalf("NewCoreHandlers: %v", err)
	}
	return handlers
}

// coreTestRequest builds a request carrying the authenticated identity.
func coreTestRequest(method, target string, body io.Reader, owner uuid.UUID) *http.Request {
	request := httptest.NewRequest(method, target, body)
	if owner != uuid.Nil() {
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner.String()}))
	}
	return request
}

// coreServe runs one handler and returns the recorded response.
func coreServe(handler http.HandlerFunc, request *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	return recorder
}

// coreErrorBody is the pinned error envelope as the tests read it.
type coreErrorBody struct {
	OK        bool   `json:"ok"`
	Type      string `json:"type"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"requestId"`
	Error     struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Status  int            `json:"status"`
		Details map[string]any `json:"details"`
	} `json:"error"`
}

func coreDecodeError(t *testing.T, recorder *httptest.ResponseRecorder) coreErrorBody {
	t.Helper()
	var body coreErrorBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error envelope failed (status %d, %d bytes)", recorder.Code, recorder.Body.Len())
	}
	return body
}

func coreAssertError(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) coreErrorBody {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d", recorder.Code, status)
	}
	body := coreDecodeError(t, recorder)
	if body.OK || body.Type != "sandbox.error" || body.Status != status || body.Code != code {
		t.Fatalf("error fields = ok:%t type:%q status:%d code:%q, want ok:false type:sandbox.error status:%d code:%q",
			body.OK, body.Type, body.Status, body.Code, status, code)
	}
	if body.Error.Status != status || body.Error.Code != code || body.RequestID == "" {
		t.Fatalf("nested error fields = code:%q status:%d requestId:%t",
			body.Error.Code, body.Error.Status, body.RequestID != "")
	}
	return body
}

func TestNewCoreHandlersRequiresDependencies(t *testing.T) {
	if _, err := NewCoreHandlers(CoreOptions{Admission: &coreFakeAdmission{}}); err == nil {
		t.Error("NewCoreHandlers accepted a nil store")
	}
	store := &coreFakeStore{}
	if _, err := NewCoreHandlers(CoreOptions{Store: store}); err == nil {
		t.Error("NewCoreHandlers accepted a nil admission")
	}
	handlers := coreTestHandlers(t, store, &coreFakeAdmission{})
	if handlers.Me == nil || handlers.ListSandboxes == nil || handlers.CreateSandbox == nil ||
		handlers.GetSandbox == nil || handlers.DeleteSandbox == nil || handlers.GetDeletionOperation == nil {
		t.Errorf("handlers = %+v", handlers)
	}
}

func TestCoreMe(t *testing.T) {
	handlers := coreTestHandlers(t, &coreFakeStore{}, &coreFakeAdmission{})

	t.Run("success", func(t *testing.T) {
		recorder := coreServe(handlers.Me, coreTestRequest(http.MethodGet, PathMe, nil, coreTestOwner))
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d", recorder.Code)
		}
		var response MeResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if !response.OK || response.Type != TypeUserInfo || response.User.ID != coreTestOwner.String() {
			t.Errorf("response = %+v", response)
		}
		if response.User.Login != "" || response.User.Email != nil || response.User.AccountPlan != "" {
			t.Errorf("me invented fields: %+v", response.User)
		}
	})

	t.Run("no identity", func(t *testing.T) {
		recorder := coreServe(handlers.Me, httptest.NewRequest(http.MethodGet, PathMe, nil))
		coreAssertError(t, recorder, http.StatusUnauthorized, apierr.CodeUnauthorized)
	})

	t.Run("bad owner", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, PathMe, nil)
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: "not-a-uuid"}))
		recorder := coreServe(handlers.Me, request)
		coreAssertError(t, recorder, http.StatusUnauthorized, apierr.CodeUnauthorized)
	})

	t.Run("undeclared query is ignored", func(t *testing.T) {
		recorder := coreServe(handlers.Me, coreTestRequest(http.MethodGet, PathMe+"?org=x", nil, coreTestOwner))
		if recorder.Code != http.StatusOK {
			t.Errorf("status = %d", recorder.Code)
		}
	})
}

func TestCoreCreateValidDefaults(t *testing.T) {
	store := &coreFakeStore{}
	admission := &coreFakeAdmission{}
	admission.onAdmit = func(request AdmissionRequest) {
		admission.result = AdmissionResult{Sandbox: coreSandboxFromRequest(request, db.StateProvisioning)}
	}
	handlers := coreTestHandlers(t, store, admission)

	recorder := coreServe(handlers.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, nil, coreTestOwner))
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d", recorder.Code)
	}
	if len(admission.calls) != 1 {
		t.Fatalf("admission calls = %d", len(admission.calls))
	}
	request := admission.calls[0]
	if request.Owner != coreTestOwner {
		t.Errorf("owner = %s", request.Owner)
	}
	if !ids.ValidSandbox(request.SandboxID) {
		t.Errorf("sandbox id = %q", request.SandboxID)
	}
	if request.MachineType != "default" || request.Resources.VCPUs != 4 ||
		request.Resources.MemoryMiB != 8<<10 || request.Resources.DiskGiB != 51 {
		t.Errorf("machine = %q %+v", request.MachineType, request.Resources)
	}
	if request.TTLSeconds == nil || *request.TTLSeconds != 3600 {
		t.Errorf("ttlSeconds = %v", request.TTLSeconds)
	}
	if request.RequestSHA256 != sha256.Sum256([]byte("{}")) {
		t.Errorf("hash = %x, want hash of {}", request.RequestSHA256)
	}
	if request.IdempotencyKey != "" || request.FailFast || request.Name != "Sandbox 2026-11-09 12:00" ||
		len(request.Labels) != 0 || len(request.Env) != 0 {
		t.Errorf("request fields = idempotencyKeySet:%t failFast:%t name:%q labels:%d env:%d",
			request.IdempotencyKey != "", request.FailFast, request.Name, len(request.Labels), len(request.Env))
	}

	var response CreateSandboxResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Status != CreateProvisioning || response.TTLSeconds == nil || *response.TTLSeconds != 3600 {
		t.Errorf("response = %+v", response)
	}
	if response.Sandbox.ID != request.SandboxID || response.Sandbox.State != StateProvisioning ||
		response.Sandbox.Type != "default" || response.Sandbox.VCPU != 4 ||
		response.Sandbox.MemoryGB != 8 || response.Sandbox.BillingMultiplier != 1 {
		t.Errorf("sandbox = %+v", response.Sandbox)
	}
	if response.Sandbox.URL != nil || response.Sandbox.IP != nil || response.Sandbox.DesktopURL != nil ||
		response.Sandbox.DesktopAvailable || response.Sandbox.SnapshotAvailable {
		t.Errorf("sandbox fabricated connection state: %+v", response.Sandbox)
	}
}

func TestCoreCreateNameDefaultAndExplicitEmpty(t *testing.T) {
	admission := &coreFakeAdmission{}
	admission.onAdmit = func(request AdmissionRequest) {
		admission.result = AdmissionResult{Sandbox: coreSandboxFromRequest(request, db.StateProvisioning)}
	}
	handlers := coreTestHandlers(t, &coreFakeStore{}, admission)

	recorder := coreServe(handlers.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, nil, coreTestOwner))
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("default status = %d", recorder.Code)
	}
	if got := admission.calls[0].Name; got != "Sandbox 2026-11-09 12:00" {
		t.Errorf("default name = %q", got)
	}

	recorder = coreServe(handlers.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(`{"name":""}`), coreTestOwner))
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("empty status = %d", recorder.Code)
	}
	if got := admission.calls[1].Name; got != "" {
		t.Errorf("explicit empty name = %q, want empty", got)
	}
}

func TestCoreCreateValidFullRequest(t *testing.T) {
	store := &coreFakeStore{}
	admission := &coreFakeAdmission{}
	admission.onAdmit = func(request AdmissionRequest) {
		admission.result = AdmissionResult{Sandbox: coreSandboxFromRequest(request, db.StateProvisioning)}
	}
	handlers := coreTestHandlers(t, store, admission)
	body := `{"type":"small","ttlSeconds":60,"labels":{"session":"abc"},"env":{"FOO":"bar"},"name":"job","failFast":true}`
	request := coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(body), coreTestOwner)
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	request.Header.Set(IdempotencyKeyHeader, "key-1")
	recorder := coreServe(handlers.CreateSandbox, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d", recorder.Code)
	}
	got := admission.calls[0]
	if got.MachineType != "small" || got.Resources.VCPUs != 2 || got.Resources.DiskGiB != 51 {
		t.Errorf("machine = %q %+v", got.MachineType, got.Resources)
	}
	if got.Name != "job" || got.Labels["session"] != "abc" || got.Env["FOO"] != "bar" || !got.FailFast {
		t.Errorf("request fields = name:%q labelCount:%d envCount:%d failFast:%t",
			got.Name, len(got.Labels), len(got.Env), got.FailFast)
	}
	if got.IdempotencyKey != "key-1" || got.RequestSHA256 != sha256.Sum256([]byte(body)) {
		t.Errorf("idempotency = %q %x", got.IdempotencyKey, got.RequestSHA256)
	}
	if got.TTLSeconds == nil || *got.TTLSeconds != 60 {
		t.Errorf("ttlSeconds = %v", got.TTLSeconds)
	}
	if strings.Contains(recorder.Body.String(), "bar") {
		t.Errorf("create response leaked env")
	}
	var response CreateSandboxResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.TTLSeconds == nil || *response.TTLSeconds != 60 {
		t.Errorf("ttlSeconds = %v", response.TTLSeconds)
	}
	if response.Sandbox.Labels["session"] != "abc" || response.Sandbox.Name != "job" {
		t.Errorf("sandbox = %+v", response.Sandbox)
	}
}

func TestCoreCreateNoExpiry(t *testing.T) {
	for _, body := range []string{`{"ttlSeconds":null}`, `{"ttlSeconds":"infinite"}`} {
		t.Run(body, func(t *testing.T) {
			admission := &coreFakeAdmission{}
			admission.onAdmit = func(request AdmissionRequest) {
				admission.result = AdmissionResult{Sandbox: coreSandboxFromRequest(request, db.StateProvisioning)}
			}
			handlers := coreTestHandlers(t, &coreFakeStore{}, admission)
			recorder := coreServe(handlers.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(body), coreTestOwner))
			if recorder.Code != http.StatusAccepted {
				t.Fatalf("status = %d", recorder.Code)
			}
			if admission.calls[0].TTLSeconds != nil {
				t.Errorf("ttlSeconds = %v, want nil", admission.calls[0].TTLSeconds)
			}
			var response CreateSandboxResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.TTLSeconds != nil {
				t.Errorf("ttlSeconds = %v, want null", *response.TTLSeconds)
			}
		})
	}
}

func TestCoreCreateAcceptedNoOps(t *testing.T) {
	body := `{"environment":"base","noEnv":true,"setupScript":"","hostedPorts":[],"files":[]}`
	admission := &coreFakeAdmission{}
	admission.onAdmit = func(request AdmissionRequest) {
		admission.result = AdmissionResult{Sandbox: coreSandboxFromRequest(request, db.StateProvisioning)}
	}
	handlers := coreTestHandlers(t, &coreFakeStore{}, admission)
	recorder := coreServe(handlers.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(body), coreTestOwner))
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestCoreCreateValidation(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		query   string
		headers map[string]string
		status  int
		code    string
	}{
		{name: "top-level null", body: `null`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "top-level array", body: `[]`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "malformed", body: `{"type":`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "trailing", body: `{} {}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "unknown member", body: `{"bogus":1}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "name null", body: `{"name":null}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "labels null", body: `{"labels":null}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "env null", body: `{"env":null}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "env null value", body: `{"env":{"A":null}}`, status: http.StatusBadRequest, code: apierr.CodeInvalidEnv},
		{name: "ttl zero", body: `{"ttlSeconds":0}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "ttl high", body: `{"ttlSeconds":2592001}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "ttl text", body: `{"ttlSeconds":"forever"}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "ttl fraction", body: `{"ttlSeconds":1.5}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "type empty", body: `{"type":""}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "type unknown", body: `{"type":"huge"}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "name long", body: `{"name":"` + strings.Repeat("é", 121) + `"}`, status: http.StatusBadRequest, code: apierr.CodeInvalidName},
		{name: "too many labels", body: coreBodyWithLabels(21), status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "bad label key", body: `{"labels":{"-bad":"x"}}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "long label value", body: `{"labels":{"k":"` + strings.Repeat("v", 257) + `"}}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "too many env", body: coreBodyWithEnvCount(101), status: http.StatusBadRequest, code: apierr.CodeInvalidEnv},
		{name: "bad env key", body: `{"env":{"1BAD":"x"}}`, status: http.StatusBadRequest, code: apierr.CodeInvalidEnv},
		{name: "reserved env", body: `{"env":{"AGENT_ID":"x"}}`, status: http.StatusBadRequest, code: apierr.CodeInvalidEnv},
		{name: "env NUL", body: `{"env":{"A":"x\u0000y"}}`, status: http.StatusBadRequest, code: apierr.CodeInvalidEnv},
		{name: "env aggregate", body: coreBodyWithEnvBytes(4, 20<<10), status: http.StatusBadRequest, code: apierr.CodeInvalidEnv},
		{name: "environment", body: `{"environment":"custom"}`, status: http.StatusBadRequest, code: apierr.CodeUnknownEnvironment},
		{name: "snapshots null", body: `{"snapshots":null}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "snapshots string", body: `{"snapshots":"false"}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "monitoring true", body: `{"monitoring":true}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "monitoring false", body: `{"monitoring":false}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "count one", body: `{"count":1}`, status: http.StatusBadRequest, code: apierr.CodeCountRequiresFrom},
		{name: "count zero", body: `{"count":0}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "count two", body: `{"count":2}`, status: http.StatusBadRequest, code: apierr.CodeCountRequiresFrom},
		{name: "count over maximum", body: `{"count":1001,"from":"base"}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "count with failFast", body: `{"count":2,"from":"base","failFast":true}`, status: http.StatusBadRequest, code: apierr.CodeCountWithFailFast},
		{name: "setup script", body: `{"setupScript":"true"}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "visibility", body: `{"visibility":"private"}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "hosted ports", body: `{"hostedPorts":[{"port":8080}]}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "files", body: `{"files":[{"path":"/tmp/a","content":"x"}]}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "org", body: `{"org":"personal"}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "teamId", body: `{"teamId":"team"}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "unknown query", body: `{}`, query: "?org=personal", status: http.StatusBadRequest, code: apierr.CodeInvalidQuery},
		{name: "malformed percent query", body: `{}`, query: "?a=%zz", status: http.StatusBadRequest, code: apierr.CodeInvalidQuery},
		{name: "malformed semicolon query", body: `{}`, query: "?a;b", status: http.StatusBadRequest, code: apierr.CodeInvalidQuery},
		{name: "bad content type", body: `{}`, headers: map[string]string{"Content-Type": "text/plain"}, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "long idempotency key", body: `{}`, headers: map[string]string{IdempotencyKeyHeader: strings.Repeat("k", 256)}, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "body too large", body: `{"name":"` + strings.Repeat("a", MaxJSONBodyBytes) + `"}`, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &coreFakeStore{}
			admission := &coreFakeAdmission{
				err: errors.New("admission must not be called for an invalid request"),
			}
			handlers := coreTestHandlers(t, store, admission)
			request := coreTestRequest(http.MethodPost, PathSandboxes+test.query, strings.NewReader(test.body), coreTestOwner)
			request.Header.Set("Content-Type", "application/json")
			for name, value := range test.headers {
				request.Header.Set(name, value)
			}
			recorder := coreServe(handlers.CreateSandbox, request)
			coreAssertError(t, recorder, test.status, test.code)
			if len(admission.calls) != 0 {
				t.Errorf("admission called for invalid request")
			}
		})
	}
}

// coreBodyWithLabels builds a body with count distinct labels.
func coreBodyWithLabels(count int) string {
	labels := make(map[string]string, count)
	for i := range count {
		labels["label"+strconv.Itoa(i)] = "v"
	}
	raw, _ := json.Marshal(map[string]any{"labels": labels})
	return string(raw)
}

// coreBodyWithEnvCount builds a body with count distinct env variables.
func coreBodyWithEnvCount(count int) string {
	env := make(map[string]string, count)
	for i := range count {
		env["VAR_"+strconv.Itoa(i)] = "v"
	}
	raw, _ := json.Marshal(map[string]any{"env": env})
	return string(raw)
}

// coreBodyWithEnvBytes builds count variables carrying valueBytes each.
func coreBodyWithEnvBytes(count, valueBytes int) string {
	env := make(map[string]string, count)
	for i := range count {
		env["VAR_"+strconv.Itoa(i)] = strings.Repeat("v", valueBytes)
	}
	raw, _ := json.Marshal(map[string]any{"env": env})
	return string(raw)
}

func TestCoreCreateReplayAndCapacity(t *testing.T) {
	t.Run("replay ready", func(t *testing.T) {
		admission := &coreFakeAdmission{}
		admission.onAdmit = func(request AdmissionRequest) {
			sandbox := coreSandboxFromRequest(request, db.StateReady)
			sandbox.ID = "bx_23456789"
			admission.result = AdmissionResult{Sandbox: sandbox, Replayed: true}
		}
		handlers := coreTestHandlers(t, &coreFakeStore{}, admission)
		request := coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(`{}`), coreTestOwner)
		request.Header.Set(IdempotencyKeyHeader, "replay")
		recorder := coreServe(handlers.CreateSandbox, request)
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("status = %d", recorder.Code)
		}
		var response CreateSandboxResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Status != CreateReady || response.Sandbox.ID != "bx_23456789" {
			t.Errorf("response = %+v", response)
		}
	})

	t.Run("fail fast capacity", func(t *testing.T) {
		admission := &coreFakeAdmission{err: db.ErrNoCapacity}
		handlers := coreTestHandlers(t, &coreFakeStore{}, admission)
		recorder := coreServe(handlers.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(`{"failFast":true}`), coreTestOwner))
		body := coreAssertError(t, recorder, http.StatusServiceUnavailable, apierr.CodeNoReadyMachine)
		if retry, ok := body.Error.Details["retryAfterSeconds"].(float64); !ok || retry <= 0 {
			t.Errorf("details = %+v", body.Error.Details)
		}
	})

	t.Run("capacity", func(t *testing.T) {
		admission := &coreFakeAdmission{err: db.ErrNoCapacity}
		handlers := coreTestHandlers(t, &coreFakeStore{}, admission)
		recorder := coreServe(handlers.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(`{}`), coreTestOwner))
		body := coreAssertError(t, recorder, http.StatusServiceUnavailable, apierr.CodeOutOfCapacity)
		if retry, ok := body.Error.Details["retryAfterSeconds"].(float64); !ok || retry <= 0 {
			t.Errorf("details = %+v", body.Error.Details)
		}
	})

	t.Run("no ready machine sentinel regardless of fail fast", func(t *testing.T) {
		admission := &coreFakeAdmission{err: ErrNoReadyMachine}
		handlers := coreTestHandlers(t, &coreFakeStore{}, admission)
		recorder := coreServe(handlers.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(`{}`), coreTestOwner))
		body := coreAssertError(t, recorder, http.StatusServiceUnavailable, apierr.CodeNoReadyMachine)
		if retry, ok := body.Error.Details["retryAfterSeconds"].(float64); !ok || retry <= 0 {
			t.Errorf("details = %+v", body.Error.Details)
		}
	})

	for _, test := range []struct {
		name string
		err  error
		code string
	}{
		{"idempotency reused", db.ErrIdempotencyReused, apierr.CodeIdempotencyKeyReused},
		{"idempotency in progress", db.ErrIdempotencyInProgress, apierr.CodeIdempotencyInProgress},
	} {
		t.Run(test.name, func(t *testing.T) {
			admission := &coreFakeAdmission{err: test.err}
			handlers := coreTestHandlers(t, &coreFakeStore{}, admission)
			recorder := coreServe(handlers.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(`{}`), coreTestOwner))
			coreAssertError(t, recorder, http.StatusConflict, test.code)
		})
	}

	t.Run("internal fault does not leak", func(t *testing.T) {
		admission := &coreFakeAdmission{err: errors.New("secret dsn postgres://user:pass@host/db")}
		handlers := coreTestHandlers(t, &coreFakeStore{}, admission)
		recorder := coreServe(handlers.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(`{}`), coreTestOwner))
		coreAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
		if strings.Contains(recorder.Body.String(), "pass") || strings.Contains(recorder.Body.String(), "postgres") {
			t.Errorf("admission fault leaked the cause")
		}
	})

	t.Run("empty admission result", func(t *testing.T) {
		admission := &coreFakeAdmission{}
		handlers := coreTestHandlers(t, &coreFakeStore{}, admission)
		recorder := coreServe(handlers.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(`{}`), coreTestOwner))
		coreAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
	})

	t.Run("foreign admission result", func(t *testing.T) {
		admission := &coreFakeAdmission{}
		admission.onAdmit = func(request AdmissionRequest) {
			sandbox := coreSandboxFromRequest(request, db.StateProvisioning)
			sandbox.OwnerID = uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-000000000000")
			admission.result = AdmissionResult{Sandbox: sandbox}
		}
		handlers := coreTestHandlers(t, &coreFakeStore{}, admission)
		recorder := coreServe(handlers.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(`{}`), coreTestOwner))
		body := coreAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
		if strings.Contains(body.Message, "018f4b1c") {
			t.Errorf("foreign owner leaked in the error")
		}
	})
}

func TestCoreFaultsDoNotLogCauses(t *testing.T) {
	const secret = "postgres://user:pass@host/db?token=SECRET"
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	handlers := func(store CoreStore, admission Admission) Handlers {
		t.Helper()
		logs.Reset()
		handlers, err := NewCoreHandlers(CoreOptions{
			Store:     store,
			Admission: admission,
			Logger:    logger,
			Now:       func() time.Time { return coreFixedNow },
		})
		if err != nil {
			t.Fatalf("NewCoreHandlers: %v", err)
		}
		return handlers
	}
	assertNoCause := func(t *testing.T) {
		t.Helper()
		for _, needle := range []string{"postgres", "pass", "SECRET", "token"} {
			if strings.Contains(logs.String(), needle) {
				t.Errorf("logs contain a raw cause fragment %q", needle)
			}
		}
	}

	t.Run("storage fault", func(t *testing.T) {
		store := &coreFakeStore{list: func(context.Context, db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error) {
			return nil, nil, errors.New(secret)
		}}
		h := handlers(store, &coreFakeAdmission{})
		recorder := coreServe(h.ListSandboxes, coreTestRequest(http.MethodGet, PathSandboxes, nil, coreTestOwner))
		coreAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
		assertNoCause(t)
	})

	t.Run("admission fault", func(t *testing.T) {
		h := handlers(&coreFakeStore{}, &coreFakeAdmission{err: errors.New(secret)})
		recorder := coreServe(h.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(`{}`), coreTestOwner))
		coreAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
		assertNoCause(t)
	})

	t.Run("capacity sentinels are not logged", func(t *testing.T) {
		for _, test := range []struct {
			name string
			err  error
			code string
		}{
			{"no ready machine", ErrNoReadyMachine, apierr.CodeNoReadyMachine},
			{"no capacity", db.ErrNoCapacity, apierr.CodeOutOfCapacity},
		} {
			t.Run(test.name, func(t *testing.T) {
				h := handlers(&coreFakeStore{}, &coreFakeAdmission{err: test.err})
				recorder := coreServe(h.CreateSandbox, coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(`{}`), coreTestOwner))
				body := coreAssertError(t, recorder, http.StatusServiceUnavailable, test.code)
				if logs.Len() != 0 {
					t.Errorf("sentinel failure was logged")
				}
				if strings.Contains(body.Message, "api: ") {
					t.Errorf("sentinel text leaked into the error")
				}
			})
		}
	})
}

func TestCoreListSandboxes(t *testing.T) {
	first := db.Sandbox{
		ID: "bx_23456789", State: db.StateReady, MachineType: "default",
		Name: "job", Labels: map[string]string{"session": "abc"},
		CreatedAt: coreFixedNow, UpdatedAt: coreFixedNow,
		ExpiresAt: new(coreFixedNow.Add(time.Hour)),
	}
	second := db.Sandbox{
		ID: "bx_3456789a", State: db.StateProvisioning, MachineType: "small",
		Name: "job-2", CreatedAt: coreFixedNow.Add(-time.Minute), UpdatedAt: coreFixedNow,
	}
	next := db.PageCursor{CreatedAt: second.CreatedAt.Truncate(time.Microsecond), ID: second.ID}
	store := &coreFakeStore{list: func(context.Context, db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error) {
		return []db.Sandbox{first, second}, &next, nil
	}}
	handlers := coreTestHandlers(t, store, &coreFakeAdmission{})
	target := PathSandboxes + "?limit=2&sort=asc&state=ready,idle&name=job&namePrefix=job-&label=session%3Dabc&label=env%3Dprod"
	request := coreTestRequest(http.MethodGet, target, nil, coreTestOwner)
	request.SetPathValue(ParamSandboxID, "")
	recorder := coreServe(handlers.ListSandboxes, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	query := store.lastList
	if query.Owner != coreTestOwner || query.Limit != 2 || !query.Ascending ||
		len(query.States) != 2 || query.States[0] != db.StateReady ||
		query.Name == nil || *query.Name != "job" || query.NamePrefix != "job-" ||
		query.Labels["session"] != "abc" || query.Labels["env"] != "prod" {
		t.Errorf("query = %+v", query)
	}
	var response SandboxListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Sandboxes) != 2 || response.PageInfo.Limit != 2 || !response.PageInfo.HasMore || response.PageInfo.NextCursor == nil {
		t.Fatalf("response = %+v", response)
	}
	cursor, err := DecodeCursor(CursorKindSandboxes, *response.PageInfo.NextCursor)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	if cursor.ID != second.ID || !cursor.CreatedAt.Equal(next.CreatedAt) || cursor.Sort != SortAscending {
		t.Errorf("cursor = %+v", cursor)
	}
	if response.Sandboxes[0].URL != nil || response.Sandboxes[0].IP != nil ||
		response.Sandboxes[0].TTLSeconds == nil || *response.Sandboxes[0].TTLSeconds != 3600 {
		t.Errorf("sandbox = %+v", response.Sandboxes[0])
	}
	if response.Sandboxes[1].TTLSeconds != nil || response.Sandboxes[1].ArchiveAfter != nil {
		t.Errorf("no-expiry sandbox = %+v", response.Sandboxes[1])
	}
}

func TestCoreListEmptyIsArray(t *testing.T) {
	store := &coreFakeStore{list: func(context.Context, db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error) {
		return nil, nil, nil
	}}
	handlers := coreTestHandlers(t, store, &coreFakeAdmission{})
	recorder := coreServe(handlers.ListSandboxes, coreTestRequest(http.MethodGet, PathSandboxes, nil, coreTestOwner))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	sandboxes, ok := body["sandboxes"].([]any)
	if !ok || len(sandboxes) != 0 {
		t.Errorf("sandboxes = %#v, want []", body["sandboxes"])
	}
}

func TestCoreListValidation(t *testing.T) {
	handlers := coreTestHandlers(t, &coreFakeStore{}, &coreFakeAdmission{})
	tests := []struct {
		name   string
		target string
	}{
		{"unknown parameter", PathSandboxes + "?bogus=1"},
		{"repeated scalar", PathSandboxes + "?name=a&name=b"},
		{"limit", PathSandboxes + "?limit=0"},
		{"state", PathSandboxes + "?state=bogus"},
		{"cursor", PathSandboxes + "?cursor=garbage"},
		{"malformed percent", PathSandboxes + "?label=%zz"},
		{"malformed semicolon", PathSandboxes + "?label=a;b"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := coreServe(handlers.ListSandboxes, coreTestRequest(http.MethodGet, test.target, nil, coreTestOwner))
			coreAssertError(t, recorder, http.StatusBadRequest, apierr.CodeInvalidQuery)
		})
	}

	t.Run("contradictory labels are an empty page", func(t *testing.T) {
		store := &coreFakeStore{}
		handlers := coreTestHandlers(t, store, &coreFakeAdmission{})
		recorder := coreServe(handlers.ListSandboxes, coreTestRequest(http.MethodGet, PathSandboxes+"?label=a%3D1&label=a%3D2", nil, coreTestOwner))
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d", recorder.Code)
		}
		var response SandboxListResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if len(response.Sandboxes) != 0 || response.PageInfo.HasMore || response.PageInfo.NextCursor != nil {
			t.Errorf("response = %+v", response)
		}
		if store.listCalls != 0 {
			t.Errorf("store list calls = %d, want 0", store.listCalls)
		}
	})

	t.Run("store fault", func(t *testing.T) {
		store := &coreFakeStore{list: func(context.Context, db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error) {
			return nil, nil, errors.New("password=secret")
		}}
		handlers := coreTestHandlers(t, store, &coreFakeAdmission{})
		recorder := coreServe(handlers.ListSandboxes, coreTestRequest(http.MethodGet, PathSandboxes, nil, coreTestOwner))
		coreAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
		if strings.Contains(recorder.Body.String(), "secret") {
			t.Errorf("list fault leaked the cause")
		}
	})

	t.Run("bad next cursor", func(t *testing.T) {
		store := &coreFakeStore{list: func(context.Context, db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error) {
			return nil, &db.PageCursor{}, nil
		}}
		handlers := coreTestHandlers(t, store, &coreFakeAdmission{})
		recorder := coreServe(handlers.ListSandboxes, coreTestRequest(http.MethodGet, PathSandboxes, nil, coreTestOwner))
		coreAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
	})
}

func TestCoreGetSandbox(t *testing.T) {
	row := db.Sandbox{
		ID: "bx_23456789", State: db.StateError, MachineType: "large", Name: "job",
		ErrorReason: "image boot failed", CreatedAt: coreFixedNow, UpdatedAt: coreFixedNow,
	}
	store := &coreFakeStore{sandbox: func(_ context.Context, owner uuid.UUID, id string) (db.Sandbox, error) {
		if owner != coreTestOwner || id != row.ID {
			return db.Sandbox{}, db.ErrNotFound
		}
		return row, nil
	}}
	handlers := coreTestHandlers(t, store, &coreFakeAdmission{})
	request := coreTestRequest(http.MethodGet, PathSandbox, nil, coreTestOwner)
	request.SetPathValue(ParamSandboxID, row.ID)
	recorder := coreServe(handlers.GetSandbox, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	var response SandboxInfoResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Sandbox.ID != row.ID || response.Sandbox.State != StateError ||
		response.Sandbox.Error == nil || *response.Sandbox.Error != row.ErrorReason ||
		response.Sandbox.Type != "large" || response.Sandbox.MemoryGB != 16 {
		t.Errorf("sandbox = %+v", response.Sandbox)
	}

	t.Run("foreign is not found", func(t *testing.T) {
		request := coreTestRequest(http.MethodGet, PathSandbox, nil, coreTestOwner)
		request.SetPathValue(ParamSandboxID, "bx_foreign0")
		recorder := coreServe(handlers.GetSandbox, request)
		coreAssertError(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	})

	t.Run("store fault", func(t *testing.T) {
		store := &coreFakeStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
			return db.Sandbox{}, errors.New("token=secret")
		}}
		handlers := coreTestHandlers(t, store, &coreFakeAdmission{})
		request := coreTestRequest(http.MethodGet, PathSandbox, nil, coreTestOwner)
		request.SetPathValue(ParamSandboxID, row.ID)
		recorder := coreServe(handlers.GetSandbox, request)
		coreAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
	})

	t.Run("undeclared query is ignored", func(t *testing.T) {
		request := coreTestRequest(http.MethodGet, PathSandbox+"?bogus=1", nil, coreTestOwner)
		request.SetPathValue(ParamSandboxID, row.ID)
		recorder := coreServe(handlers.GetSandbox, request)
		if recorder.Code != http.StatusOK {
			t.Errorf("status = %d", recorder.Code)
		}
	})
}

func coreTestOperation() db.DeletionOperation {
	completed := coreFixedNow.Add(2 * time.Minute)
	return db.DeletionOperation{
		ID: "bdop_0123456789abcdef0123456789abcdef", Kind: db.DeletionSandbox,
		TargetID: "bx_23456789", Reason: db.DeletionExplicit, Status: db.DeletionCompleted,
		Stage: db.StageCompleted, AttemptCount: 2, RequestedAt: coreFixedNow,
		CompletedAt: &completed,
	}
}

func TestCoreDeleteSandbox(t *testing.T) {
	operation := coreTestOperation()
	store := &coreFakeStore{del: func(_ context.Context, owner uuid.UUID, id string, reason db.DeletionReason) (db.DeletionOperation, db.Sandbox, error) {
		if owner != coreTestOwner || reason != db.DeletionExplicit {
			return db.DeletionOperation{}, db.Sandbox{}, db.ErrNotFound
		}
		if id != "bx_23456789" {
			return db.DeletionOperation{}, db.Sandbox{}, db.ErrNotFound
		}
		return operation, db.Sandbox{ID: id}, nil
	}}
	handlers := coreTestHandlers(t, store, &coreFakeAdmission{})

	t.Run("success", func(t *testing.T) {
		request := coreTestRequest(http.MethodDelete, PathSandbox, nil, coreTestOwner)
		request.SetPathValue(ParamSandboxID, "bx_23456789")
		request.Header.Set(ConfirmDeleteHeader, "bx_23456789")
		recorder := coreServe(handlers.DeleteSandbox, request)
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("status = %d", recorder.Code)
		}
		var response DeletionOperationResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Type != TypeSandboxDeleting || response.Operation.ID != operation.ID ||
			response.Operation.Status != DeletionCompleted || response.Operation.Stage == nil {
			t.Errorf("response = %+v", response)
		}
	})

	t.Run("missing confirmation", func(t *testing.T) {
		request := coreTestRequest(http.MethodDelete, PathSandbox, nil, coreTestOwner)
		request.SetPathValue(ParamSandboxID, "bx_23456789")
		recorder := coreServe(handlers.DeleteSandbox, request)
		coreAssertError(t, recorder, http.StatusConflict, apierr.CodeConflict)
	})

	t.Run("mismatched confirmation", func(t *testing.T) {
		request := coreTestRequest(http.MethodDelete, PathSandbox, nil, coreTestOwner)
		request.SetPathValue(ParamSandboxID, "bx_23456789")
		request.Header.Set(ConfirmDeleteHeader, "bx_other000")
		recorder := coreServe(handlers.DeleteSandbox, request)
		coreAssertError(t, recorder, http.StatusConflict, apierr.CodeConflict)
	})

	t.Run("foreign is not found", func(t *testing.T) {
		request := coreTestRequest(http.MethodDelete, PathSandbox, nil, coreTestOwner)
		request.SetPathValue(ParamSandboxID, "bx_foreign0")
		request.Header.Set(ConfirmDeleteHeader, "bx_foreign0")
		recorder := coreServe(handlers.DeleteSandbox, request)
		coreAssertError(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	})

	t.Run("store fault", func(t *testing.T) {
		store := &coreFakeStore{del: func(context.Context, uuid.UUID, string, db.DeletionReason) (db.DeletionOperation, db.Sandbox, error) {
			return db.DeletionOperation{}, db.Sandbox{}, errors.New("secret")
		}}
		handlers := coreTestHandlers(t, store, &coreFakeAdmission{})
		request := coreTestRequest(http.MethodDelete, PathSandbox, nil, coreTestOwner)
		request.SetPathValue(ParamSandboxID, "bx_23456789")
		request.Header.Set(ConfirmDeleteHeader, "bx_23456789")
		recorder := coreServe(handlers.DeleteSandbox, request)
		coreAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
	})
}

func TestCoreGetDeletionOperation(t *testing.T) {
	operation := coreTestOperation()
	store := &coreFakeStore{operation: func(_ context.Context, owner uuid.UUID, id string) (db.DeletionOperation, error) {
		if owner != coreTestOwner || id != operation.ID {
			return db.DeletionOperation{}, db.ErrNotFound
		}
		return operation, nil
	}}
	handlers := coreTestHandlers(t, store, &coreFakeAdmission{})

	t.Run("success", func(t *testing.T) {
		request := coreTestRequest(http.MethodGet, PathDeletionOperation, nil, coreTestOwner)
		request.SetPathValue(ParamOperationID, operation.ID)
		recorder := coreServe(handlers.GetDeletionOperation, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d", recorder.Code)
		}
		if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("Cache-Control = %q", got)
		}
		var response DeletionOperationResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Type != TypeDeletionOperation || response.Operation.ID != operation.ID ||
			response.Operation.Kind != DeletionKindSandbox || response.Operation.CompletedAt == nil {
			t.Errorf("response = %+v", response)
		}
	})

	t.Run("not found", func(t *testing.T) {
		request := coreTestRequest(http.MethodGet, PathDeletionOperation, nil, coreTestOwner)
		request.SetPathValue(ParamOperationID, "bdop_ffffffffffffffffffffffffffffffff")
		recorder := coreServe(handlers.GetDeletionOperation, request)
		coreAssertError(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
		if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("Cache-Control = %q", got)
		}
	})

	t.Run("store fault", func(t *testing.T) {
		store := &coreFakeStore{operation: func(context.Context, uuid.UUID, string) (db.DeletionOperation, error) {
			return db.DeletionOperation{}, errors.New("secret")
		}}
		handlers := coreTestHandlers(t, store, &coreFakeAdmission{})
		request := coreTestRequest(http.MethodGet, PathDeletionOperation, nil, coreTestOwner)
		request.SetPathValue(ParamOperationID, operation.ID)
		recorder := coreServe(handlers.GetDeletionOperation, request)
		coreAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
	})
}

func TestCoreInfoHandler(t *testing.T) {
	t.Run("construction rejects invalid limits", func(t *testing.T) {
		tests := []InfoOptions{
			{StreamMaxSeconds: 0},
			{StreamMaxSeconds: -1},
			{StreamMaxSeconds: int(math.MaxUint32) + 1},
			{StreamMaxSeconds: 7200, SyncMaxSeconds: -1},
			{StreamMaxSeconds: 7200, SyncMaxSeconds: 601},
		}
		for _, options := range tests {
			if _, err := NewInfoHandler(options); err == nil {
				t.Errorf("NewInfoHandler(%+v) accepted invalid limits", options)
			}
		}
	})

	t.Run("defaults", func(t *testing.T) {
		handler, err := NewInfoHandler(InfoOptions{StreamMaxSeconds: 7200})
		if err != nil {
			t.Fatal(err)
		}
		recorder := coreServe(handler, coreTestRequest(http.MethodGet, PathInfo, nil, coreTestOwner))
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d", recorder.Code)
		}
		var response InfoResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Type != TypeBBraftInfo || response.Version != version.Version ||
			response.Commands.StreamMaxSeconds != 7200 || response.Commands.SyncMaxSeconds != DefaultSyncMaxSeconds {
			t.Errorf("response = %+v", response)
		}
	})

	t.Run("actual response", func(t *testing.T) {
		handler, err := NewInfoHandler(InfoOptions{Version: "v1.2.3", StreamMaxSeconds: 9000, SyncMaxSeconds: 500})
		if err != nil {
			t.Fatal(err)
		}
		recorder := coreServe(handler, coreTestRequest(http.MethodGet, PathInfo, nil, coreTestOwner))
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d", recorder.Code)
		}
		var body map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if len(body) != 4 || body["ok"] != true || body["type"] != TypeBBraftInfo || body["version"] != "v1.2.3" {
			t.Errorf("body = %#v", body)
		}
		commands, ok := body["commands"].(map[string]any)
		if !ok || len(commands) != 2 || commands["streamMaxSeconds"] != float64(9000) || commands["syncMaxSeconds"] != float64(500) {
			t.Errorf("commands = %#v", body["commands"])
		}
		if strings.Contains(recorder.Body.String(), "token") || strings.Contains(recorder.Body.String(), "secret") {
			t.Errorf("info response leaked metadata")
		}
	})

	t.Run("detached capability", func(t *testing.T) {
		handler, err := NewInfoHandler(InfoOptions{Version: "v1.2.3", StreamMaxSeconds: 7200, Detached: true})
		if err != nil {
			t.Fatal(err)
		}
		recorder := coreServe(handler, coreTestRequest(http.MethodGet, PathInfo, nil, coreTestOwner))
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d", recorder.Code)
		}
		var body map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		commands, ok := body["commands"].(map[string]any)
		if !ok || len(commands) != 3 || commands["detached"] != true {
			t.Errorf("commands = %#v", body["commands"])
		}
	})

	t.Run("requires auth", func(t *testing.T) {
		handler, err := NewInfoHandler(InfoOptions{StreamMaxSeconds: 7200})
		if err != nil {
			t.Fatal(err)
		}
		recorder := coreServe(handler, httptest.NewRequest(http.MethodGet, PathInfo, nil))
		coreAssertError(t, recorder, http.StatusUnauthorized, apierr.CodeUnauthorized)
		recorder = coreServe(handler, coreTestRequest(http.MethodGet, PathInfo+"?bogus=1", nil, coreTestOwner))
		if recorder.Code != http.StatusOK {
			t.Errorf("status = %d", recorder.Code)
		}
	})

	t.Run("registers extension route", func(t *testing.T) {
		handler, err := NewInfoHandler(InfoOptions{StreamMaxSeconds: 7200})
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		RegisterExtensions(mux, ExtensionHandlers{Info: handler})
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, coreTestRequest(http.MethodGet, PathInfo, nil, coreTestOwner))
		if recorder.Code != http.StatusOK {
			t.Errorf("status = %d", recorder.Code)
		}
	})
}

func TestCreateSandboxRestoreRequest(t *testing.T) {
	large, ok := machinetype.Lookup("large")
	if !ok {
		t.Fatal("large machine type missing")
	}
	def, ok := machinetype.Lookup("default")
	if !ok {
		t.Fatal("default machine type missing")
	}
	tests := []struct {
		name          string
		body          string
		wantFrom      string
		wantExplicit  bool
		wantMachine   string
		wantResources db.Resources
	}{
		{
			name: "omitted type defers to snapshot", body: `{"from":"base"}`,
			wantFrom: "base", wantMachine: "", wantResources: db.Resources{},
		},
		{
			name: "explicit type", body: `{"from":"base","type":"large"}`,
			wantFrom: "base", wantExplicit: true, wantMachine: "large",
			wantResources: db.Resources{VCPUs: large.VCPUs, MemoryMiB: large.MemoryMiB, DiskGiB: large.DiskGiB},
		},
		{
			name: "empty from is a normal create", body: `{"from":""}`,
			wantMachine: "default",
			wantResources: db.Resources{
				VCPUs: def.VCPUs, MemoryMiB: def.MemoryMiB, DiskGiB: def.DiskGiB,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var admission *coreFakeAdmission
			admission = &coreFakeAdmission{onAdmit: func(request AdmissionRequest) {
				admission.result = AdmissionResult{Sandbox: coreSandboxFromRequest(request, db.StateProvisioning)}
			}}
			handlers := coreTestHandlers(t, &coreFakeStore{}, admission)
			request := coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(tc.body), coreTestOwner)
			request.Header.Set("Content-Type", "application/json")
			recorder := coreServe(handlers.CreateSandbox, request)
			if recorder.Code != http.StatusAccepted {
				t.Fatalf("status = %d (%s)", recorder.Code, recorder.Body.String())
			}
			if len(admission.calls) != 1 {
				t.Fatalf("admission calls = %d", len(admission.calls))
			}
			got := admission.calls[0]
			if got.From != tc.wantFrom || got.TypeExplicit != tc.wantExplicit ||
				got.MachineType != tc.wantMachine || got.Resources != tc.wantResources {
				t.Fatalf("admission from=%q explicit=%v machine=%q vcpus=%d memory=%d disk=%d",
					got.From, got.TypeExplicit, got.MachineType,
					got.Resources.VCPUs, got.Resources.MemoryMiB, got.Resources.DiskGiB)
			}
		})
	}
}

func TestCreateAdmissionSnapshotErrors(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{name: "missing source", err: db.ErrSnapshotNotFound, status: http.StatusNotFound, code: apierr.CodeSnapshotNotFound},
		{name: "unknown legacy disk", err: db.ErrSnapshotDiskUnknown, status: http.StatusNotFound, code: apierr.CodeSnapshotNotFound},
		{name: "shrink disk", err: db.ErrSnapshotDiskTooSmall, status: http.StatusBadRequest, code: apierr.CodeInvalidJSON},
		{name: "classified api error", err: snapshotNotFound(), status: http.StatusNotFound, code: apierr.CodeSnapshotNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handlers := coreTestHandlers(t, &coreFakeStore{}, &coreFakeAdmission{err: tc.err})
			request := coreTestRequest(http.MethodPost, PathSandboxes, strings.NewReader(`{"from":"base"}`), coreTestOwner)
			request.Header.Set("Content-Type", "application/json")
			recorder := coreServe(handlers.CreateSandbox, request)
			coreAssertError(t, recorder, tc.status, tc.code)
		})
	}
}
