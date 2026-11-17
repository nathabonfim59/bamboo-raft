package api_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const (
	processTestOwner     = "018f4b1c-7e2d-7a3b-9c4d-000000000001"
	processTestSandboxID = "bx_3456789a"
	processTestNodeID    = "node-process-test"
	processTestStartedAt = int64(1_700_000_000_000)
)

// processTestStore is an in-memory CommandStore. It mirrors the handler-visible
// contract closely enough for handler tests: ids never go backwards and byte
// counts only grow. Unlike the real store it applies the latest observation
// verbatim, so tests that need terminal stickiness live in the database suite.
type processTestStore struct {
	mu             sync.Mutex
	next           uint32
	records        map[uint32]db.CommandRecord
	storedPrefix   int
	allocateErr    error
	observeErr     error
	recordErr      error
	allocateMutate func(*db.CommandRecord)
	allocateHook   func()
	observeMutate  func(*db.CommandRecord)
	observeHook    func()
	recordMutate   func(*db.CommandRecord)
	recordHook     func()
	allocateCalls  int
	observeCalls   int
	recordCalls    int
	lastAllocate   db.AllocateCommandParams
	observations   []db.CommandObservation
}

func newProcessTestStore() *processTestStore {
	return &processTestStore{records: map[uint32]db.CommandRecord{}}
}

func (s *processTestStore) AllocateCommand(_ context.Context, owner uuid.UUID, p db.AllocateCommandParams) (db.CommandRecord, error) {
	s.mu.Lock()
	s.allocateCalls++
	s.lastAllocate = p
	if s.allocateErr != nil {
		err := s.allocateErr
		s.mu.Unlock()
		return db.CommandRecord{}, err
	}
	s.next++
	stored := p.Command
	if s.storedPrefix > 0 && len(stored) > s.storedPrefix {
		stored = stored[:s.storedPrefix]
	}
	now := time.Now().UTC()
	record := db.CommandRecord{
		CommandID: "cmd_processaaaaa",
		OwnerID:   owner,
		SandboxID: p.SandboxID,
		ProcessID: s.next,
		Mode:      db.CommandModeDetached,
		Command:   stored,
		Cwd:       p.Cwd,
		Status:    db.CommandStarting,
		StartedAt: now,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if s.allocateMutate != nil {
		s.allocateMutate(&record)
	}
	s.records[record.ProcessID] = record
	hook := s.allocateHook
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return record, nil
}

func (s *processTestStore) ObserveCommand(_ context.Context, _ uuid.UUID, _ string, processID uint32, obs db.CommandObservation) (db.CommandRecord, error) {
	s.mu.Lock()
	s.observeCalls++
	s.observations = append(s.observations, obs)
	if s.observeErr != nil {
		err := s.observeErr
		s.mu.Unlock()
		return db.CommandRecord{}, err
	}
	record, ok := s.records[processID]
	if !ok {
		s.mu.Unlock()
		return db.CommandRecord{}, db.ErrNotFound
	}
	record.Status = obs.Status
	if obs.PID > 0 {
		record.PID = obs.PID
	}
	record.ExitCode = obs.ExitCode
	record.Signal = obs.Signal
	if obs.StartedAt != nil {
		record.StartedAt = *obs.StartedAt
	}
	record.FinishedAt = obs.FinishedAt
	if obs.Cwd != nil {
		record.Cwd = obs.Cwd
	}
	if obs.LogPath != "" {
		record.LogPath = obs.LogPath
	}
	if obs.ErrLogPath != "" {
		record.ErrLogPath = obs.ErrLogPath
	}
	record.StdoutBytes = max(record.StdoutBytes, obs.StdoutBytes)
	record.StderrBytes = max(record.StderrBytes, obs.StderrBytes)
	record.OutputTruncated = record.OutputTruncated || obs.OutputTruncated
	record.UpdatedAt = time.Now().UTC()
	s.records[processID] = record
	hook := s.observeHook
	mutate := s.observeMutate
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	if mutate != nil {
		mutate(&record)
	}
	return record, nil
}

func (s *processTestStore) CommandRecord(_ context.Context, _ uuid.UUID, _ string, processID uint32) (db.CommandRecord, error) {
	s.mu.Lock()
	s.recordCalls++
	err := s.recordErr
	record, ok := s.records[processID]
	hook := s.recordHook
	mutate := s.recordMutate
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	if err != nil {
		return db.CommandRecord{}, err
	}
	if !ok {
		return db.CommandRecord{}, db.ErrNotFound
	}
	if mutate != nil {
		mutate(&record)
	}
	return record, nil
}

func (s *processTestStore) seed(record db.CommandRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[record.ProcessID] = record
	s.next = max(s.next, record.ProcessID)
}

func (s *processTestStore) record(processID uint32) (db.CommandRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[processID]
	return record, ok
}

func (s *processTestStore) callCounts() (allocate, observe, record int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allocateCalls, s.observeCalls, s.recordCalls
}

func (s *processTestStore) lastObservation() (db.CommandObservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.observations) == 0 {
		return db.CommandObservation{}, false
	}
	return s.observations[len(s.observations)-1], true
}

// processTestClient records StartProcess and GetProcess calls.
type processTestClient struct {
	nodev1.NodeClient
	mu         sync.Mutex
	start      func(context.Context, *nodev1.StartProcessRequest) (*nodev1.Process, error)
	get        func(context.Context, *nodev1.GetProcessRequest) (*nodev1.ProcessOutput, error)
	startCalls int
	getCalls   int
	startReqs  []*nodev1.StartProcessRequest
	getReqs    []*nodev1.GetProcessRequest
}

func (c *processTestClient) StartProcess(ctx context.Context, request *nodev1.StartProcessRequest, _ ...grpc.CallOption) (*nodev1.Process, error) {
	c.mu.Lock()
	c.startCalls++
	c.startReqs = append(c.startReqs, request)
	start := c.start
	c.mu.Unlock()
	if start == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return start(ctx, request)
}

func (c *processTestClient) GetProcess(ctx context.Context, request *nodev1.GetProcessRequest, _ ...grpc.CallOption) (*nodev1.ProcessOutput, error) {
	c.mu.Lock()
	c.getCalls++
	c.getReqs = append(c.getReqs, request)
	get := c.get
	c.mu.Unlock()
	if get == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return get(ctx, request)
}

func (c *processTestClient) counts() (start, get int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.startCalls, c.getCalls
}

func processTestFactory(client nodev1.NodeClient) api.NodeClientFactory {
	return func(string) (nodev1.NodeClient, func() error, error) {
		return client, func() error { return nil }, nil
	}
}

func processTestStartHandler(t *testing.T, store api.CommandStore, client nodev1.NodeClient, opts ...func(*api.CommandsOptions)) http.Handler {
	t.Helper()
	options := api.CommandsOptions{
		Lookup:  commandTestWithReadyLookup(processTestNodeID),
		Clients: processTestFactory(client),
		Store:   store,
		Logger:  commandTestLogger(),
	}
	for _, opt := range opts {
		opt(&options)
	}
	handler, err := api.NewCommandsHandler(options)
	if err != nil {
		t.Fatalf("NewCommandsHandler: %v", err)
	}
	return handler
}

func processTestStatusHandler(t *testing.T, store api.CommandStore, client nodev1.NodeClient, opts ...func(*api.CommandsOptions)) http.Handler {
	t.Helper()
	options := api.CommandsOptions{
		Lookup:  commandTestWithReadyLookup(processTestNodeID),
		Clients: processTestFactory(client),
		Store:   store,
		Logger:  commandTestLogger(),
	}
	for _, opt := range opts {
		opt(&options)
	}
	handler, err := api.NewCommandStatusHandler(options)
	if err != nil {
		t.Fatalf("NewCommandStatusHandler: %v", err)
	}
	return handler
}

func processTestServeStart(t *testing.T, handler http.Handler, owner, sandboxID, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("POST "+api.CommandsPath, handler)
	request := httptest.NewRequest(http.MethodPost,
		"/api/v1/sandboxes/"+sandboxID+"/commands", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if owner != "" {
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner, KeyID: "key_test"}))
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return recorder
}

func processTestServeStatus(t *testing.T, handler http.Handler, owner, sandboxID, processID, query string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET "+api.ProcessStatusPath, handler)
	target := "/api/v1/sandboxes/" + sandboxID + "/commands/" + processID
	if query != "" {
		target += "?" + query
	}
	request := httptest.NewRequest(http.MethodGet, target, nil)
	if owner != "" {
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner, KeyID: "key_test"}))
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return recorder
}

func processTestDecodeStarted(t *testing.T, recorder *httptest.ResponseRecorder) api.CommandStartedResponse {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", recorder.Code, recorder.Body.Bytes())
	}
	var response api.CommandStartedResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode command.started: %v\nbody: %s", err, recorder.Body.Bytes())
	}
	if !response.OK || response.Type != api.TypeCommandStarted || !response.Success {
		t.Fatalf("envelope = ok:%v type:%q success:%v", response.OK, response.Type, response.Success)
	}
	return response
}

func processTestDecodeStatus(t *testing.T, recorder *httptest.ResponseRecorder) api.CommandStatusResponse {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", recorder.Code, recorder.Body.Bytes())
	}
	var response api.CommandStatusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode command.status: %v\nbody: %s", err, recorder.Body.Bytes())
	}
	if !response.OK || response.Type != api.TypeCommandStatus {
		t.Fatalf("envelope = ok:%v type:%q", response.OK, response.Type)
	}
	return response
}

func processTestValidate(t *testing.T, operation string, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if err := compat.Load(t).Response(operation, recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
		t.Errorf("%s response does not validate against the pin: %v\nbody: %s", operation, err, recorder.Body.Bytes())
	}
}

func processTestRunningProcess(processID uint32, command string) *nodev1.Process {
	return &nodev1.Process{
		ProcessId:       processID,
		Pid:             4242,
		State:           nodev1.ProcessState_PROCESS_STATE_RUNNING,
		Command:         command,
		Cwd:             "/home/user",
		StartedAtUnixMs: processTestStartedAt,
		StdoutSize:      5,
		StderrSize:      3,
		LogPath:         "/home/user/.ascii/processes/1.log",
		ErrLogPath:      "/home/user/.ascii/processes/1.err.log",
	}
}

func processTestExitedProcess(processID uint32, command string, code *int32, signal string) *nodev1.Process {
	process := processTestRunningProcess(processID, command)
	process.State = nodev1.ProcessState_PROCESS_STATE_EXITED
	process.FinishedAtUnixMs = processTestStartedAt + 1000
	process.Exit = &nodev1.ExecEvent_Exit{ExitCode: code, Signal: signal, DurationMs: 1000}
	return process
}

func processTestBody(t *testing.T, fields map[string]any) string {
	t.Helper()
	body, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return string(body)
}

func TestDetachedStartSuccessPreservesFullCommand(t *testing.T) {
	store := newProcessTestStore()
	store.storedPrefix = 12
	longCommand := "printf " + strings.Repeat("a", 70*1024)
	client := &processTestClient{start: func(_ context.Context, request *nodev1.StartProcessRequest) (*nodev1.Process, error) {
		return processTestRunningProcess(request.ProcessId, request.Command), nil
	}}
	handler := processTestStartHandler(t, store, client)
	recorder := processTestServeStart(t, handler, processTestOwner, processTestSandboxID,
		processTestBody(t, map[string]any{"command": longCommand, "cwd": "/tmp", "detached": true}))

	response := processTestDecodeStarted(t, recorder)
	if response.Command != longCommand {
		t.Errorf("started command length = %d, want the full %d", len(response.Command), len(longCommand))
	}
	if response.CommandID == "" || response.ProcessID != 1 || response.PID != 4242 {
		t.Errorf("started identity = %+v", response)
	}
	if response.Cwd != "/home/user" || response.LogPath == "" || response.ErrLogPath == "" {
		t.Errorf("started node metadata = %+v", response)
	}
	if response.StartedAt.UnixMilli() != processTestStartedAt {
		t.Errorf("startedAt = %v, want %d", response.StartedAt, processTestStartedAt)
	}
	processTestValidate(t, "command", recorder)

	client.mu.Lock()
	startRequests := append([]*nodev1.StartProcessRequest(nil), client.startReqs...)
	client.mu.Unlock()
	if len(startRequests) != 1 {
		t.Fatalf("StartProcess calls = %d", len(startRequests))
	}
	if startRequests[0].Command != longCommand || startRequests[0].ProcessId != 1 || startRequests[0].Cwd != "/tmp" {
		t.Errorf("StartProcess request = %+v", startRequests[0])
	}
	record, ok := store.record(1)
	if !ok || record.Status != db.CommandRunning || record.Command == longCommand {
		t.Errorf("record = %+v, want running and a stored prefix", record)
	}
}

func TestDetachedStartIgnoresStreamAndValidatesTimeout(t *testing.T) {
	store := newProcessTestStore()
	client := &processTestClient{start: func(_ context.Context, request *nodev1.StartProcessRequest) (*nodev1.Process, error) {
		return processTestRunningProcess(request.ProcessId, request.Command), nil
	}}
	handler := processTestStartHandler(t, store, client)

	recorder := processTestServeStart(t, handler, processTestOwner, processTestSandboxID,
		`{"command":"true","detached":true,"stream":true,"timeoutSeconds":600}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("detached+stream status = %d\nbody: %s", recorder.Code, recorder.Body.Bytes())
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}
	processTestValidate(t, "command", recorder)

	for _, body := range []string{
		`{"command":"true","detached":true,"timeoutSeconds":0}`,
		`{"command":"true","detached":true,"timeoutSeconds":601}`,
	} {
		recorder := processTestServeStart(t, handler, processTestOwner, processTestSandboxID, body)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusBadRequest || envelope.Code != "invalid_timeout" {
			t.Errorf("timeout validation = %d %q", recorder.Code, envelope.Code)
		}
	}
	if start, _ := client.counts(); start != 1 {
		t.Errorf("invalid timeouts reached StartProcess: %d calls", start)
	}
	if allocate, _, _ := store.callCounts(); allocate != 1 {
		t.Errorf("invalid timeouts allocated ids: %d calls", allocate)
	}
}

func TestDetachedStartAllocationErrors(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"missing sandbox", db.ErrNotFound, http.StatusNotFound, "not_found"},
		{"state race", db.ErrInvalidState, http.StatusConflict, "sandbox_not_ready"},
		{"ids exhausted", db.ErrCommandIDExhausted, http.StatusConflict, "sandbox_not_ready"},
		{"invalid text", db.ErrInvalidCommand, http.StatusBadRequest, "invalid_json"},
		{"internal", errors.New("private database detail"), http.StatusInternalServerError, "http_500"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.allocateErr = test.err
			client := &processTestClient{}
			handler := processTestStartHandler(t, store, client)
			recorder := processTestServeStart(t, handler, processTestOwner, processTestSandboxID,
				`{"command":"true","detached":true}`)
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != test.status || envelope.Code != test.code {
				t.Fatalf("error = %d %q, want %d %q\nbody: %s", recorder.Code, envelope.Code, test.status, test.code, recorder.Body.Bytes())
			}
			processTestValidate(t, "command", recorder)
			if test.err == db.ErrCommandIDExhausted {
				if envelope.Message != "sandbox cannot start more commands; create a new sandbox" {
					t.Errorf("message = %q", envelope.Message)
				}
				if got, ok := envelope.Error.Details["retryable"].(bool); !ok || got {
					t.Errorf("retryable = %v, want false", envelope.Error.Details["retryable"])
				}
			}
			if start, _ := client.counts(); start != 0 {
				t.Errorf("allocation failure reached StartProcess: %d calls", start)
			}
		})
	}
}

func TestDetachedStartRejectsMalformedAllocation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*db.CommandRecord)
	}{
		{"foreign owner", func(r *db.CommandRecord) { r.OwnerID = uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-000000000002") }},
		{"foreign sandbox", func(r *db.CommandRecord) { r.SandboxID = "bx_other" }},
		{"zero process id", func(r *db.CommandRecord) { r.ProcessID = 0 }},
		{"empty command id", func(r *db.CommandRecord) { r.CommandID = "" }},
		{"wrong mode", func(r *db.CommandRecord) { r.Mode = "sync" }},
		{"not starting", func(r *db.CommandRecord) { r.Status = db.CommandRunning }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.allocateMutate = test.mutate
			client := &processTestClient{}
			handler := processTestStartHandler(t, store, client)
			recorder := processTestServeStart(t, handler, processTestOwner, processTestSandboxID,
				`{"command":"true","detached":true}`)
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != http.StatusInternalServerError || envelope.Code != "http_500" {
				t.Fatalf("error = %d %q, want 500 http_500", recorder.Code, envelope.Code)
			}
			if start, _ := client.counts(); start != 0 {
				t.Errorf("malformed allocation reached StartProcess: %d calls", start)
			}
			if strings.Contains(recorder.Body.String(), "cmd_") {
				t.Errorf("response leaked an allocation id: %s", recorder.Body.Bytes())
			}
		})
	}
}

func TestDetachedStartNodeErrors(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		status    int
		code      string
		calls     int
		failed    bool
		retryable *bool
	}{
		{"not found", status.Error(codes.NotFound, "secret node text"), http.StatusNotFound, "not_found", 1, true, nil},
		{"invalid argument", status.Error(codes.InvalidArgument, "bad cwd"), http.StatusBadRequest, "invalid_path", 1, true, nil},
		{"not ready", status.Error(codes.FailedPrecondition, "stopping"), http.StatusConflict, "sandbox_not_ready", 1, true, new(false)},
		{"resource exhausted is not capacity", status.Error(codes.ResourceExhausted, "size"), http.StatusBadGateway, "sandbox_direct_failed", 2, false, new(false)},
		{"unavailable", status.Error(codes.Unavailable, "link"), http.StatusBadGateway, "sandbox_direct_failed", 2, false, new(false)},
		{"internal", status.Error(codes.Internal, "node bug"), http.StatusBadGateway, "sandbox_direct_failed", 2, false, new(false)},
		{"already exists is not definite", status.Error(codes.AlreadyExists, "conflict"), http.StatusBadGateway, "sandbox_direct_failed", 1, false, new(false)},
		{"out of range is not definite", status.Error(codes.OutOfRange, "range"), http.StatusBadGateway, "sandbox_direct_failed", 1, false, new(false)},
		{"permission denied is not definite", status.Error(codes.PermissionDenied, "denied"), http.StatusBadGateway, "sandbox_direct_failed", 1, false, new(false)},
		{"unauthenticated is not definite", status.Error(codes.Unauthenticated, "auth"), http.StatusBadGateway, "sandbox_direct_failed", 1, false, new(false)},
		{"unimplemented is not definite", status.Error(codes.Unimplemented, "old node"), http.StatusBadGateway, "sandbox_direct_failed", 1, false, new(false)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			client := &processTestClient{start: func(_ context.Context, _ *nodev1.StartProcessRequest) (*nodev1.Process, error) {
				return nil, test.err
			}}
			handler := processTestStartHandler(t, store, client)
			recorder := processTestServeStart(t, handler, processTestOwner, processTestSandboxID,
				`{"command":"true","detached":true}`)
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != test.status || envelope.Code != test.code {
				t.Fatalf("error = %d %q, want %d %q\nbody: %s", recorder.Code, envelope.Code, test.status, test.code, recorder.Body.Bytes())
			}
			processTestValidate(t, "command", recorder)
			if _, ok := envelope.Error.Details["retryAfterSeconds"]; ok {
				t.Errorf("ResourceExhausted or another 502 carried a retry hint: %+v", envelope.Error.Details)
			}
			if test.retryable != nil {
				if got, ok := envelope.Error.Details["retryable"].(bool); !ok || got != *test.retryable {
					t.Errorf("retryable = %v, want %v", envelope.Error.Details["retryable"], *test.retryable)
				}
			}
			if start, _ := client.counts(); start != test.calls {
				t.Errorf("StartProcess calls = %d, want %d", start, test.calls)
			}
			client.mu.Lock()
			requests := append([]*nodev1.StartProcessRequest(nil), client.startReqs...)
			client.mu.Unlock()
			for _, request := range requests {
				if request.ProcessId != 1 || request.Command != "true" {
					t.Errorf("retry changed the request: %+v", request)
				}
			}
			record, ok := store.record(1)
			if !ok {
				t.Fatal("record missing")
			}
			wantStatus := db.CommandStarting
			if test.failed {
				wantStatus = db.CommandFailed
			}
			if record.Status != wantStatus {
				t.Errorf("record status = %q, want %q", record.Status, wantStatus)
			}
			if allocate, _, _ := store.callCounts(); allocate != 1 {
				t.Errorf("allocation count = %d, want 1", allocate)
			}
		})
	}
}

func TestDetachedStartSameIDRecovery(t *testing.T) {
	for _, first := range []error{
		status.Error(codes.ResourceExhausted, "size"),
		status.Error(codes.Unavailable, "link"),
	} {
		t.Run(status.Code(first).String(), func(t *testing.T) {
			store := newProcessTestStore()
			calls := 0
			client := &processTestClient{start: func(_ context.Context, request *nodev1.StartProcessRequest) (*nodev1.Process, error) {
				calls++
				if calls == 1 {
					return nil, first
				}
				return processTestRunningProcess(request.ProcessId, request.Command), nil
			}}
			handler := processTestStartHandler(t, store, client)
			recorder := processTestServeStart(t, handler, processTestOwner, processTestSandboxID,
				`{"command":"true","detached":true}`)
			response := processTestDecodeStarted(t, recorder)
			if response.ProcessID != 1 {
				t.Errorf("processId = %d, want the first allocation", response.ProcessID)
			}
			if start, _ := client.counts(); start != 2 {
				t.Errorf("StartProcess calls = %d, want 2", start)
			}
			client.mu.Lock()
			requests := append([]*nodev1.StartProcessRequest(nil), client.startReqs...)
			client.mu.Unlock()
			for _, request := range requests {
				if request.ProcessId != 1 || request.Command != "true" {
					t.Errorf("retry changed the request: %+v", request)
				}
			}
			record, _ := store.record(1)
			if record.Status != db.CommandRunning {
				t.Errorf("record status = %q, want running", record.Status)
			}
		})
	}
}

func TestDetachedStartLostProcessAccepted(t *testing.T) {
	store := newProcessTestStore()
	client := &processTestClient{start: func(_ context.Context, request *nodev1.StartProcessRequest) (*nodev1.Process, error) {
		process := processTestRunningProcess(request.ProcessId, request.Command)
		process.State = nodev1.ProcessState_PROCESS_STATE_LOST
		process.FinishedAtUnixMs = 0
		return process, nil
	}}
	handler := processTestStartHandler(t, store, client)
	recorder := processTestServeStart(t, handler, processTestOwner, processTestSandboxID,
		`{"command":"true","detached":true}`)
	response := processTestDecodeStarted(t, recorder)
	if response.ProcessID != 1 || response.PID != 4242 {
		t.Errorf("started response = %+v", response)
	}
	record, _ := store.record(1)
	if record.Status != db.CommandLost || record.FinishedAt != nil {
		t.Errorf("record = %+v, want lost with a nil finish", record)
	}
}

func TestDetachedStartMalformedProcess(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*nodev1.Process)
	}{
		{"nil process", nil},
		{"wrong id", func(p *nodev1.Process) { p.ProcessId = 99 }},
		{"different command", func(p *nodev1.Process) { p.Command = "other" }},
		{"zero pid", func(p *nodev1.Process) { p.Pid = 0 }},
		{"unknown state", func(p *nodev1.Process) { p.State = nodev1.ProcessState_PROCESS_STATE_UNSPECIFIED }},
		{"running with exit", func(p *nodev1.Process) { p.Exit = &nodev1.ExecEvent_Exit{ExitCode: new(int32(0))} }},
		{"exited without exit", func(p *nodev1.Process) {
			p.State = nodev1.ProcessState_PROCESS_STATE_EXITED
			p.FinishedAtUnixMs = processTestStartedAt + 1
		}},
		{"exited with code and signal", func(p *nodev1.Process) {
			p.State = nodev1.ProcessState_PROCESS_STATE_EXITED
			p.FinishedAtUnixMs = processTestStartedAt + 1
			p.Exit = &nodev1.ExecEvent_Exit{ExitCode: new(int32(1)), Signal: "SIGKILL"}
		}},
		{"exited timed out", func(p *nodev1.Process) {
			p.State = nodev1.ProcessState_PROCESS_STATE_EXITED
			p.FinishedAtUnixMs = processTestStartedAt + 1
			p.Exit = &nodev1.ExecEvent_Exit{ExitCode: new(int32(0)), TimedOut: true}
		}},
		{"relative cwd", func(p *nodev1.Process) { p.Cwd = "home/user" }},
		{"negative size", func(p *nodev1.Process) { p.StdoutSize = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			client := &processTestClient{start: func(_ context.Context, request *nodev1.StartProcessRequest) (*nodev1.Process, error) {
				if test.mutate == nil {
					return nil, nil
				}
				process := processTestRunningProcess(request.ProcessId, request.Command)
				test.mutate(process)
				return process, nil
			}}
			handler := processTestStartHandler(t, store, client)
			recorder := processTestServeStart(t, handler, processTestOwner, processTestSandboxID,
				`{"command":"true","detached":true}`)
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != http.StatusBadGateway || envelope.Code != "sandbox_direct_failed" {
				t.Fatalf("error = %d %q, want 502 sandbox_direct_failed", recorder.Code, envelope.Code)
			}
			if _, observe, _ := store.callCounts(); observe != 0 {
				t.Errorf("malformed process was observed %d times", observe)
			}
			record, _ := store.record(1)
			if record.Status != db.CommandStarting {
				t.Errorf("record status = %q, want starting", record.Status)
			}
		})
	}
}

func TestDetachedStartObservationFaultIsUnretryable500(t *testing.T) {
	store := newProcessTestStore()
	store.observeErr = errors.New("private database detail")
	client := &processTestClient{start: func(_ context.Context, request *nodev1.StartProcessRequest) (*nodev1.Process, error) {
		return processTestRunningProcess(request.ProcessId, request.Command), nil
	}}
	handler := processTestStartHandler(t, store, client)
	recorder := processTestServeStart(t, handler, processTestOwner, processTestSandboxID,
		`{"command":"true","detached":true}`)
	envelope := commandTestDecodeError(t, recorder)
	if recorder.Code != http.StatusInternalServerError || envelope.Code != "http_500" {
		t.Fatalf("error = %d %q, want 500 http_500", recorder.Code, envelope.Code)
	}
	if got, ok := envelope.Error.Details["retryable"].(bool); !ok || got {
		t.Errorf("retryable = %v, want false", envelope.Error.Details["retryable"])
	}
	processTestValidate(t, "command", recorder)
	if start, _ := client.counts(); start != 1 {
		t.Errorf("StartProcess calls = %d, want no retry", start)
	}
	if allocate, _, _ := store.callCounts(); allocate != 1 {
		t.Errorf("allocation count = %d, want no reallocation", allocate)
	}
}

func TestDetachedStartCancellationAfterAllocateAndObserve(t *testing.T) {
	t.Run("after allocate", func(t *testing.T) {
		store := newProcessTestStore()
		ctx, cancel := context.WithCancel(t.Context())
		store.allocateHook = cancel
		client := &processTestClient{}
		handler := processTestStartHandler(t, store, client)
		recorder := processTestServeStartWithContext(t, handler, ctx, processTestOwner, processTestSandboxID,
			`{"command":"true","detached":true}`)
		if start, _ := client.counts(); start != 0 {
			t.Errorf("StartProcess calls = %d after cancellation", start)
		}
		if recorder.Body.Len() != 0 {
			t.Errorf("cancelled start wrote a response: %s", recorder.Body.Bytes())
		}
	})
	t.Run("after observe", func(t *testing.T) {
		store := newProcessTestStore()
		ctx, cancel := context.WithCancel(t.Context())
		store.observeHook = cancel
		client := &processTestClient{start: func(_ context.Context, request *nodev1.StartProcessRequest) (*nodev1.Process, error) {
			return processTestRunningProcess(request.ProcessId, request.Command), nil
		}}
		handler := processTestStartHandler(t, store, client)
		recorder := processTestServeStartWithContext(t, handler, ctx, processTestOwner, processTestSandboxID,
			`{"command":"true","detached":true}`)
		if recorder.Body.Len() != 0 {
			t.Errorf("cancelled observation wrote a response: %s", recorder.Body.Bytes())
		}
	})
}

func processTestServeStartWithContext(t *testing.T, handler http.Handler, ctx context.Context, owner, sandboxID, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("POST "+api.CommandsPath, handler)
	request := httptest.NewRequest(http.MethodPost,
		"/api/v1/sandboxes/"+sandboxID+"/commands", strings.NewReader(body)).WithContext(ctx)
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner, KeyID: "key_test"}))
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return recorder
}

func TestDetachedStartAuthAndLookup(t *testing.T) {
	tests := []struct {
		name     string
		owner    string
		lookup   api.CommandTargetLookup
		status   int
		code     string
		allocate int
	}{
		{"unauthenticated", "", commandTestWithReadyLookup(processTestNodeID), http.StatusUnauthorized, "unauthorized", 0},
		{"foreign sandbox", processTestOwner, &commandTestLookup{err: api.ErrCommandTargetNotFound}, http.StatusNotFound, "not_found", 0},
		{"lookup fault", processTestOwner, &commandTestLookup{err: errors.New("private")}, http.StatusInternalServerError, "http_500", 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			handler := processTestStartHandler(t, store, &processTestClient{}, func(options *api.CommandsOptions) {
				options.Lookup = test.lookup
			})
			recorder := processTestServeStart(t, handler, test.owner, processTestSandboxID,
				`{"command":"true","detached":true}`)
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != test.status || envelope.Code != test.code {
				t.Fatalf("error = %d %q", recorder.Code, envelope.Code)
			}
			if allocate, _, _ := store.callCounts(); allocate != test.allocate {
				t.Errorf("allocate calls = %d, want %d", allocate, test.allocate)
			}
		})
	}
}

func TestDetachedStartMissingStore(t *testing.T) {
	handler := processTestStartHandler(t, nil, &processTestClient{})
	recorder := processTestServeStart(t, handler, processTestOwner, processTestSandboxID,
		`{"command":"true","detached":true}`)
	envelope := commandTestDecodeError(t, recorder)
	if recorder.Code != http.StatusInternalServerError || envelope.Code != "http_500" {
		t.Fatalf("error = %d %q, want 500 http_500", recorder.Code, envelope.Code)
	}
}

func TestCommandStatusHandlerRequiresStore(t *testing.T) {
	_, err := api.NewCommandStatusHandler(api.CommandsOptions{
		Lookup:  commandTestWithReadyLookup(processTestNodeID),
		Clients: processTestFactory(&processTestClient{}),
	})
	if err == nil {
		t.Fatal("status handler accepted a nil store")
	}
}
