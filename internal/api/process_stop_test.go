package api_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func processStopTestDecode(t *testing.T, recorder *httptest.ResponseRecorder) api.CommandStopResponse {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", recorder.Code, recorder.Body.Bytes())
	}
	var body api.CommandStopResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode command.stopped: %v\nbody: %s", err, recorder.Body.Bytes())
	}
	if !body.OK || body.Type != api.TypeCommandStopped || body.Running {
		t.Fatalf("envelope = ok:%v type:%q running:%v", body.OK, body.Type, body.Running)
	}
	if body.Command == nil {
		t.Fatal("command is null")
	}
	return body
}

func processStopTestServe(t *testing.T, handler http.Handler, owner, sandboxID, processID string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("DELETE "+api.ProcessStatusPath, handler)
	request := httptest.NewRequest(http.MethodDelete,
		"/api/v1/sandboxes/"+sandboxID+"/commands/"+processID, nil)
	if owner != "" {
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner, KeyID: "key_test"}))
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return recorder
}

// processStopTestBegin runs one DELETE in a goroutine with a caller-owned
// writer so cancellation tests can observe that no response was written.
func processStopTestBegin(t *testing.T, handler http.Handler, ctx context.Context, owner, sandboxID, processID string, writer http.ResponseWriter) <-chan struct{} {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("DELETE "+api.ProcessStatusPath, handler)
	request := httptest.NewRequest(http.MethodDelete,
		"/api/v1/sandboxes/"+sandboxID+"/commands/"+processID, nil)
	request = request.WithContext(ctx)
	if owner != "" {
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner, KeyID: "key_test"}))
	}
	done := make(chan struct{})
	go func() {
		mux.ServeHTTP(writer, request)
		close(done)
	}()
	return done
}

// processStopTestValidate checks one stop response against the v1 API document
// and the shipped subset.
func processStopTestValidate(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	processTestValidate(t, "stopCommand", recorder)
	subset := compat.Load(t)
	if err := subset.Response("stopCommand", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
		t.Errorf("stop response does not validate against the subset: %v\nbody: %s", err, recorder.Body.Bytes())
	}
}

func processStopTestKill(process *nodev1.Process, signaled bool) func(context.Context, *nodev1.KillProcessRequest) (*nodev1.KillProcessResponse, error) {
	return func(context.Context, *nodev1.KillProcessRequest) (*nodev1.KillProcessResponse, error) {
		return &nodev1.KillProcessResponse{Process: process, Signaled: signaled}, nil
	}
}

func TestStopRequestValidation(t *testing.T) {
	tests := []struct {
		name      string
		owner     string
		processID string
		seed      bool
		status    db.CommandStatus
		targets   []api.CommandTarget
		mutate    func(*db.CommandRecord)
		recordErr error
		want      int
		code      string
	}{
		{name: "unauthenticated", processID: "7", want: http.StatusUnauthorized, code: "unauthorized"},
		{name: "bad owner", owner: "not-a-uuid", processID: "7", want: http.StatusUnauthorized, code: "unauthorized"},
		{name: "zero process", owner: processTestOwner, processID: "0", want: http.StatusNotFound, code: "not_found"},
		{name: "text process", owner: processTestOwner, processID: "abc", want: http.StatusNotFound, code: "not_found"},
		{name: "missing record", owner: processTestOwner, processID: "7", want: http.StatusNotFound, code: "not_found"},
		{name: "failed start", owner: processTestOwner, processID: "7", seed: true, status: db.CommandFailed, want: http.StatusNotFound, code: "not_found"},
		{name: "record fault", owner: processTestOwner, processID: "7", seed: true, status: db.CommandRunning, recordErr: errors.New("private store detail"), want: http.StatusInternalServerError, code: "http_500"},
		{name: "foreign owner", owner: processTestOwner, processID: "7", seed: true, status: db.CommandRunning,
			mutate: func(r *db.CommandRecord) { r.OwnerID = processAttachTestUUID("018f4b1c-7e2d-7a3b-9c4d-000000000002") }, want: http.StatusInternalServerError, code: "http_500"},
		{name: "foreign sandbox", owner: processTestOwner, processID: "7", seed: true, status: db.CommandRunning,
			mutate: func(r *db.CommandRecord) { r.SandboxID = "bx_other" }, want: http.StatusInternalServerError, code: "http_500"},
		{name: "wrong process", owner: processTestOwner, processID: "7", seed: true, status: db.CommandRunning,
			mutate: func(r *db.CommandRecord) { r.ProcessID = 8 }, want: http.StatusInternalServerError, code: "http_500"},
		{name: "wrong mode", owner: processTestOwner, processID: "7", seed: true, status: db.CommandRunning,
			mutate: func(r *db.CommandRecord) { r.Mode = "sync" }, want: http.StatusInternalServerError, code: "http_500"},
		{name: "unknown status", owner: processTestOwner, processID: "7", seed: true, status: db.CommandStatus("future"), want: http.StatusInternalServerError, code: "http_500"},
		{name: "not ready sandbox", owner: processTestOwner, processID: "7", seed: true, status: db.CommandRunning,
			targets: []api.CommandTarget{{State: api.StateArchived}}, want: http.StatusConflict, code: "sandbox_not_ready"},
		{name: "starting sandbox", owner: processTestOwner, processID: "7", seed: true, status: db.CommandRunning,
			targets: []api.CommandTarget{{State: api.StateProvisioning}}, want: http.StatusConflict, code: "sandbox_starting"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			if test.seed {
				record := processTestRecord(processAttachTestProcessID, test.status)
				store.seed(record)
				store.recordMutate = test.mutate
			}
			store.recordErr = test.recordErr
			client := &processAttachTestClient{}
			lookup := processAttachTestLookup()
			if test.targets != nil {
				lookup.targets = test.targets
			}
			handler := processAttachTestStopHandler(t, store, client, func(opts *api.CommandsOptions) {
				opts.Lookup = lookup
				opts.ReadyDeadline = 20 * time.Millisecond
			})
			recorder := processTestServeStatus(t, handler, test.owner, processTestSandboxID, test.processID, "")
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != test.want || envelope.Code != test.code {
				t.Fatalf("error = %d %q, want %d %q\nbody: %s", recorder.Code, envelope.Code, test.want, test.code, recorder.Body.Bytes())
			}
			if _, kill := client.counts(); kill != 0 {
				t.Errorf("invalid request reached KillProcess: %d calls", kill)
			}
			processStopTestValidate(t, recorder)
		})
	}
}

func TestStopTerminalRecordShortcut(t *testing.T) {
	tests := []struct {
		name   string
		status db.CommandStatus
		code   *int32
		signal string
	}{
		{"exited", db.CommandExited, new(int32(0)), ""},
		{"killed", db.CommandKilled, nil, "SIGTERM"},
		{"lost", db.CommandLost, nil, ""},
		{"timed out", db.CommandTimedOut, nil, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			record := processTestRecord(processAttachTestProcessID, test.status)
			record.ExitCode = test.code
			if test.signal != "" {
				record.Signal = new(test.signal)
			}
			if test.status != db.CommandLost {
				finished := time.UnixMilli(processTestStartedAt + 1000).UTC()
				record.FinishedAt = &finished
			}
			store.seed(record)
			client := &processAttachTestClient{kill: func(context.Context, *nodev1.KillProcessRequest) (*nodev1.KillProcessResponse, error) {
				t.Error("terminal record reached KillProcess")
				return nil, status.Error(codes.Internal, "unexpected")
			}}
			handler := processAttachTestStopHandler(t, store, client)
			recorder := processStopTestServe(t, handler, processTestOwner, processTestSandboxID, "7")
			processStopTestValidate(t, recorder)
			response := processStopTestDecode(t, recorder)
			if response.Signaled || response.ProcessID != processAttachTestProcessID {
				t.Errorf("stop = %+v, want signaled false", response)
			}
			if response.Command.Status != string(test.status) {
				t.Errorf("record status = %q, want %q", response.Command.Status, test.status)
			}
			if _, kill := client.counts(); kill != 0 {
				t.Errorf("KillProcess calls = %d, want 0", kill)
			}
			if _, _, reads := store.callCounts(); reads != 1 {
				t.Errorf("CommandRecord calls = %d, want 1", reads)
			}
		})
	}
}

func TestStopKillVariants(t *testing.T) {
	tests := []struct {
		name         string
		process      *nodev1.Process
		signaled     bool
		wantStatus   db.CommandStatus
		wantCode     *int32
		wantSignal   string
		wantFinished bool
	}{
		{"graceful delete", processTestExitedProcess(processAttachTestProcessID, "cmd", new(int32(0)), ""), true,
			db.CommandKilled, new(int32(0)), "", true},
		{"already exited", processTestExitedProcess(processAttachTestProcessID, "cmd", new(int32(3)), ""), false,
			db.CommandExited, new(int32(3)), "", true},
		{"native signal", processTestExitedProcess(processAttachTestProcessID, "cmd", nil, "SIGTERM"), false,
			db.CommandKilled, nil, "SIGTERM", true},
		{"lost while signaled", processAttachTestLostProcess(processAttachTestProcessID), true,
			db.CommandLost, nil, "", false},
		{"lost already", processAttachTestLostProcess(processAttachTestProcessID), false,
			db.CommandLost, nil, "", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
			client := &processAttachTestClient{kill: processStopTestKill(test.process, test.signaled)}
			handler := processAttachTestStopHandler(t, store, client)
			recorder := processStopTestServe(t, handler, processTestOwner, processTestSandboxID, "7")
			processStopTestValidate(t, recorder)
			response := processStopTestDecode(t, recorder)
			if response.Signaled != test.signaled {
				t.Errorf("signaled = %v, want %v", response.Signaled, test.signaled)
			}
			if response.Command.Status != string(test.wantStatus) {
				t.Errorf("status = %q, want %q", response.Command.Status, test.wantStatus)
			}
			if !processStopTestSameInt32Pointer(response.Command.ExitCode, test.wantCode) {
				t.Errorf("exitCode = %v, want %v", response.Command.ExitCode, test.wantCode)
			}
			if test.wantSignal == "" {
				if response.Command.Signal != nil && *response.Command.Signal != "" {
					t.Errorf("signal = %q, want null", *response.Command.Signal)
				}
			} else if response.Command.Signal == nil || *response.Command.Signal != test.wantSignal {
				t.Errorf("signal = %v, want %q", response.Command.Signal, test.wantSignal)
			}
			if test.wantFinished != (response.Command.FinishedAt != nil) {
				t.Errorf("finishedAt = %v, want present=%v", response.Command.FinishedAt, test.wantFinished)
			}
			record, _ := store.record(processAttachTestProcessID)
			if record.Status != test.wantStatus {
				t.Errorf("stored status = %q, want %q", record.Status, test.wantStatus)
			}
			if test.wantStatus == db.CommandLost && record.FinishedAt != nil {
				t.Errorf("LOST stored finishedAt = %v, want nil", record.FinishedAt)
			}
			request, ok := client.killRequest()
			if !ok || request.GetSandboxId() != processTestSandboxID ||
				request.GetProcessId() != processAttachTestProcessID || request.GetGraceSeconds() != 0 {
				t.Errorf("KillProcess request = %+v", request)
			}
			if _, kill := client.counts(); kill != 1 {
				t.Errorf("KillProcess calls = %d, want 1", kill)
			}
			if _, observe, _ := store.callCounts(); observe != 1 {
				t.Errorf("ObserveCommand calls = %d, want 1", observe)
			}
		})
	}
}

func processAttachTestLostProcess(processID uint32) *nodev1.Process {
	process := processTestRunningProcess(processID, "cmd")
	process.State = nodev1.ProcessState_PROCESS_STATE_LOST
	process.FinishedAtUnixMs = 0
	return process
}

func processStopTestSameInt32Pointer(got *int, want *int32) bool {
	if got == nil || want == nil {
		return got == nil && want == nil
	}
	return *got == int(*want)
}

func TestStopMalformedKillReplyAndNodeErrors(t *testing.T) {
	tests := []struct {
		name      string
		response  *nodev1.KillProcessResponse
		err       error
		status    int
		code      string
		retryable *bool
	}{
		{"nil response", nil, nil, http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"nil process", &nodev1.KillProcessResponse{Signaled: true}, nil, http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"wrong process", &nodev1.KillProcessResponse{Process: processTestExitedProcess(8, "cmd", new(int32(0)), "")}, nil, http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"still running", &nodev1.KillProcessResponse{Process: processTestRunningProcess(processAttachTestProcessID, "cmd")}, nil, http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"unspecified", &nodev1.KillProcessResponse{Process: &nodev1.Process{ProcessId: processAttachTestProcessID}}, nil, http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"exited without exit", &nodev1.KillProcessResponse{Process: func() *nodev1.Process {
			p := processTestRunningProcess(processAttachTestProcessID, "cmd")
			p.State = nodev1.ProcessState_PROCESS_STATE_EXITED
			p.FinishedAtUnixMs = processTestStartedAt + 1
			return p
		}()}, nil, http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"exit code and signal", &nodev1.KillProcessResponse{Process: processTestExitedProcess(processAttachTestProcessID, "cmd", new(int32(0)), "SIGTERM")}, nil, http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"timed out exit", &nodev1.KillProcessResponse{Process: func() *nodev1.Process {
			p := processTestExitedProcess(processAttachTestProcessID, "cmd", new(int32(0)), "")
			p.Exit.TimedOut = true
			return p
		}()}, nil, http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"lost with exit", &nodev1.KillProcessResponse{Process: func() *nodev1.Process {
			p := processAttachTestLostProcess(processAttachTestProcessID)
			p.Exit = &nodev1.ExecEvent_Exit{ExitCode: new(int32(0))}
			return p
		}()}, nil, http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"not found", nil, status.Error(codes.NotFound, "gone"), http.StatusNotFound, "not_found", nil},
		{"invalid argument", nil, status.Error(codes.InvalidArgument, "bad"), http.StatusBadRequest, "invalid_path", nil},
		{"failed precondition", nil, status.Error(codes.FailedPrecondition, "not ready"), http.StatusConflict, "sandbox_not_ready", new(false)},
		{"resource exhausted is not capacity", nil, status.Error(codes.ResourceExhausted, "size"), http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"unavailable", nil, status.Error(codes.Unavailable, "lost"), http.StatusBadGateway, "sandbox_direct_failed", new(false)},
		{"eof", nil, io.EOF, http.StatusBadGateway, "sandbox_direct_failed", new(false)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
			client := &processAttachTestClient{kill: func(context.Context, *nodev1.KillProcessRequest) (*nodev1.KillProcessResponse, error) {
				if test.err != nil {
					return nil, test.err
				}
				return test.response, nil
			}}
			handler := processAttachTestStopHandler(t, store, client)
			recorder := processStopTestServe(t, handler, processTestOwner, processTestSandboxID, "7")
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != test.status || envelope.Code != test.code {
				t.Fatalf("error = %d %q, want %d %q\nbody: %s", recorder.Code, envelope.Code, test.status, test.code, recorder.Body.Bytes())
			}
			if test.retryable != nil {
				got, ok := envelope.Error.Details["retryable"].(bool)
				if !ok || got != *test.retryable {
					t.Errorf("retryable = %v, want %v", envelope.Error.Details["retryable"], *test.retryable)
				}
			}
			if _, kill := client.counts(); kill != 1 {
				t.Errorf("KillProcess calls = %d, want 1", kill)
			}
			if _, observe, _ := store.callCounts(); observe != 0 {
				t.Errorf("malformed kill was observed: %d calls", observe)
			}
			processStopTestValidate(t, recorder)
		})
	}
}

func TestStopPostKillStoreFaultIsNotRetried(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*processTestStore)
		status int
		code   string
	}{
		{"observe error", func(s *processTestStore) { s.observeErr = errors.New("private store detail") }, http.StatusInternalServerError, "http_500"},
		{"malformed observation", func(s *processTestStore) {
			s.observeMutate = func(record *db.CommandRecord) {
				record.OwnerID = processAttachTestUUID("018f4b1c-7e2d-7a3b-9c4d-000000000002")
			}
		}, http.StatusInternalServerError, "http_500"},
		{"internal status", func(s *processTestStore) {
			s.observeMutate = func(record *db.CommandRecord) { record.Status = db.CommandStarting }
		}, http.StatusInternalServerError, "http_500"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
			test.setup(store)
			client := &processAttachTestClient{kill: processStopTestKill(processTestExitedProcess(processAttachTestProcessID, "cmd", new(int32(0)), ""), true)}
			handler := processAttachTestStopHandler(t, store, client)
			recorder := processStopTestServe(t, handler, processTestOwner, processTestSandboxID, "7")
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != test.status || envelope.Code != test.code {
				t.Fatalf("error = %d %q, want %d %q", recorder.Code, envelope.Code, test.status, test.code)
			}
			if got, ok := envelope.Error.Details["retryable"].(bool); !ok || got {
				t.Errorf("retryable = %v, want explicit false", envelope.Error.Details["retryable"])
			}
			if _, kill := client.counts(); kill != 1 {
				t.Errorf("KillProcess calls = %d, want exactly 1", kill)
			}
			processStopTestValidate(t, recorder)
		})
	}
}

func TestStopSecondCallIsIdempotent(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(processAttachTestProcessID, db.CommandRunning))
	client := &processAttachTestClient{kill: processStopTestKill(processTestExitedProcess(processAttachTestProcessID, "cmd", new(int32(0)), ""), true)}
	handler := processAttachTestStopHandler(t, store, client)

	first := processStopTestServe(t, handler, processTestOwner, processTestSandboxID, "7")
	processStopTestValidate(t, first)
	firstResponse := processStopTestDecode(t, first)
	if !firstResponse.Signaled || firstResponse.Command.Status != "killed" {
		t.Fatalf("first stop = %+v", firstResponse)
	}

	second := processStopTestServe(t, handler, processTestOwner, processTestSandboxID, "7")
	processStopTestValidate(t, second)
	secondResponse := processStopTestDecode(t, second)
	if secondResponse.Signaled {
		t.Error("second stop signaled a process that had already ended")
	}
	if secondResponse.Command.Status != "killed" || secondResponse.Command.ExitCode == nil || *secondResponse.Command.ExitCode != 0 {
		t.Errorf("second stop record = %+v", secondResponse.Command)
	}
	if _, kill := client.counts(); kill != 1 {
		t.Errorf("KillProcess calls = %d, want exactly one", kill)
	}
	if _, observe, _ := store.callCounts(); observe != 1 {
		t.Errorf("ObserveCommand calls = %d, want exactly one", observe)
	}
}

func TestStopRealStackKillsProcessAndSecondStopDoesNotKill(t *testing.T) {
	env := commandTestStartEnv(t)
	store := newProcessTestStore()
	rawClient, err := env.serve.Dial(commandTestNodeID)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	t.Cleanup(func() { _ = rawClient.Close() })
	counting := &processAttachCountingClient{NodeClient: rawClient}
	factory := func(string) (nodev1.NodeClient, func() error, error) {
		return counting, func() error { return nil }, nil
	}
	handler := processAttachTestStopHandler(t, store, counting, func(opts *api.CommandsOptions) {
		opts.Lookup = commandTestWithReadyLookup(commandTestNodeID)
		opts.Clients = factory
	})

	process, err := env.node.StartProcess(t.Context(), &nodev1.StartProcessRequest{
		SandboxId: commandTestSandboxID,
		ProcessId: processAttachTestProcessID,
		Command:   "sleep 30",
	})
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	record := processAttachTestRecord(process, db.CommandRunning)
	record.SandboxID = commandTestSandboxID
	store.seed(record)

	recorder := processStopTestServe(t, handler, processTestOwner, commandTestSandboxID, "7")
	processStopTestValidate(t, recorder)
	response := processStopTestDecode(t, recorder)
	if !response.Signaled || response.Command.Status != "killed" {
		t.Errorf("stop response = %+v", response)
	}
	final, err := env.node.GetProcess(t.Context(), &nodev1.GetProcessRequest{
		SandboxId: commandTestSandboxID, ProcessId: processAttachTestProcessID,
	})
	if err != nil {
		t.Fatalf("GetProcess after stop: %v", err)
	}
	if final.Process.State == nodev1.ProcessState_PROCESS_STATE_RUNNING {
		t.Error("process still running after stop")
	}
	if got := counting.attachCount.Load() + counting.killCount.Load(); got != 1 {
		t.Errorf("node RPC calls = %d, want exactly one kill", got)
	}

	second := processStopTestServe(t, handler, processTestOwner, commandTestSandboxID, "7")
	processStopTestValidate(t, second)
	secondResponse := processStopTestDecode(t, second)
	if secondResponse.Signaled {
		t.Error("second stop signaled an ended process")
	}
	if got := counting.killCount.Load(); got != 1 {
		t.Errorf("KillProcess calls after second stop = %d, want 1", got)
	}
}

// processAttachCountingClient counts node RPCs while delegating to the real
// gateway client, so real-stack tests prove the handler's path, not a fake.
type processAttachCountingClient struct {
	nodev1.NodeClient
	attachCount atomic.Int32
	killCount   atomic.Int32
}

func (c *processAttachCountingClient) AttachProcess(ctx context.Context, request *nodev1.AttachProcessRequest, options ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
	c.attachCount.Add(1)
	return c.NodeClient.AttachProcess(ctx, request, options...)
}

func (c *processAttachCountingClient) KillProcess(ctx context.Context, request *nodev1.KillProcessRequest, options ...grpc.CallOption) (*nodev1.KillProcessResponse, error) {
	c.killCount.Add(1)
	return c.NodeClient.KillProcess(ctx, request, options...)
}

func processAttachTestRecord(process *nodev1.Process, status db.CommandStatus) db.CommandRecord {
	record := processTestRecord(process.GetProcessId(), status)
	record.PID = process.GetPid()
	record.StartedAt = time.UnixMilli(process.GetStartedAtUnixMs()).UTC()
	record.LogPath = process.GetLogPath()
	record.ErrLogPath = process.GetErrLogPath()
	return record
}
