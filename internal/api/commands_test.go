package api_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

const (
	commandTestOwner        = "acct_command_test"
	commandTestSandboxID    = "bx_23456789"
	commandTestNodeID       = "node-command-test"
	commandTestNodeToken    = "node-command-token"
	commandTestGatewayToken = "gateway-command-token"
)

func commandTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// commandTestLookup returns the targets in order, repeating the last one.
type commandTestLookup struct {
	mu        sync.Mutex
	calls     int
	owner     string
	sandboxID string
	targets   []api.CommandTarget
	err       error
}

func (l *commandTestLookup) TargetLookup(_ context.Context, owner, sandboxID string) (api.CommandTarget, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	l.owner = owner
	l.sandboxID = sandboxID
	if l.err != nil {
		return api.CommandTarget{}, l.err
	}
	if len(l.targets) == 0 {
		return api.CommandTarget{}, api.ErrCommandTargetNotFound
	}
	index := min(l.calls-1, len(l.targets)-1)
	return l.targets[index], nil
}

func (l *commandTestLookup) state() (calls int, owner, sandboxID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls, l.owner, l.sandboxID
}

// commandTestClient is a nodev1.NodeClient whose only useful method is Exec.
type commandTestClient struct {
	nodev1.NodeClient
	exec func(context.Context, *nodev1.ExecRequest) (grpc.ServerStreamingClient[nodev1.ExecEvent], error)
}

func (c *commandTestClient) Exec(ctx context.Context, req *nodev1.ExecRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
	return c.exec(ctx, req)
}

// commandTestStream delivers prepared events, then finalErr (or io.EOF).
type commandTestStream struct {
	grpc.ClientStream
	ctx        context.Context
	events     []*nodev1.ExecEvent
	finalErr   error
	blockAfter bool
	index      int
}

func (s *commandTestStream) Context() context.Context { return s.ctx }

func (s *commandTestStream) Recv() (*nodev1.ExecEvent, error) {
	if s.index < len(s.events) {
		event := s.events[s.index]
		s.index++
		return event, nil
	}
	if s.blockAfter {
		<-s.ctx.Done()
		return nil, s.ctx.Err()
	}
	if s.finalErr != nil {
		return nil, s.finalErr
	}
	return nil, io.EOF
}

func commandTestStarted() *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Started_{Started: &nodev1.ExecEvent_Started{Pid: 123}}}
}

func commandTestStdout(data string) *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Stdout{Stdout: []byte(data)}}
}

func commandTestStdoutBytes(data []byte) *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Stdout{Stdout: data}}
}

func commandTestStderr(data string) *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Stderr{Stderr: []byte(data)}}
}

func commandTestStderrBytes(data []byte) *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Stderr{Stderr: data}}
}

func commandTestExit(code *int32, signal string, timedOut, oomKilled bool) *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Exit_{Exit: &nodev1.ExecEvent_Exit{
		ExitCode:   code,
		Signal:     signal,
		TimedOut:   timedOut,
		OomKilled:  oomKilled,
		DurationMs: 5,
	}}}
}

// commandTestStaticClient returns a client serving events and finalErr and
// records the request and call count.
func commandTestStaticClient(events []*nodev1.ExecEvent, finalErr error) (*commandTestClient, **nodev1.ExecRequest, *int) {
	var request *nodev1.ExecRequest
	calls := 0
	client := &commandTestClient{exec: func(ctx context.Context, req *nodev1.ExecRequest) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
		calls++
		request = req
		return &commandTestStream{ctx: ctx, events: events, finalErr: finalErr}, nil
	}}
	return client, &request, &calls
}

func commandTestFactory(client nodev1.NodeClient) api.NodeClientFactory {
	return func(string) (nodev1.NodeClient, func() error, error) {
		return client, func() error { return nil }, nil
	}
}

func commandTestHandler(t *testing.T, lookup api.CommandTargetLookup, factory api.NodeClientFactory, opts ...func(*api.CommandsOptions)) http.Handler {
	t.Helper()
	options := api.CommandsOptions{
		Lookup:  lookup,
		Clients: factory,
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

func commandTestWithReadyLookup(nodeID string) *commandTestLookup {
	return &commandTestLookup{targets: []api.CommandTarget{{NodeID: nodeID, State: api.StateReady}}}
}

// commandTestServe sends body through a mux so PathValue is populated, with
// the authenticated owner in the context.
func commandTestServe(t *testing.T, handler http.Handler, owner, sandboxID, body string) *httptest.ResponseRecorder {
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

type commandTestErrorEnvelope struct {
	OK      bool   `json:"ok"`
	Type    string `json:"type"`
	Status  int    `json:"status"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Error   struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Status  int            `json:"status"`
		Details map[string]any `json:"details"`
	} `json:"error"`
	RequestID string `json:"requestId"`
}

func commandTestDecodeError(t *testing.T, recorder *httptest.ResponseRecorder) commandTestErrorEnvelope {
	t.Helper()
	var envelope commandTestErrorEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v\nbody: %s", err, recorder.Body.Bytes())
	}
	if envelope.OK {
		t.Fatalf("error envelope has ok=true: %s", recorder.Body.Bytes())
	}
	if envelope.Type != "sandbox.error" {
		t.Fatalf("type = %q, want sandbox.error", envelope.Type)
	}
	if envelope.Status != recorder.Code {
		t.Fatalf("top-level status = %d, HTTP %d", envelope.Status, recorder.Code)
	}
	if envelope.Error.Status != recorder.Code || envelope.Error.Code != envelope.Code {
		t.Fatalf("nested error = %+v, top-level code = %q status %d", envelope.Error, envelope.Code, envelope.Status)
	}
	if envelope.RequestID == "" {
		t.Fatalf("requestId is empty")
	}
	return envelope
}

func commandTestDecodeResponse(t *testing.T, recorder *httptest.ResponseRecorder) api.CommandResponse {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", recorder.Code, recorder.Body.Bytes())
	}
	var response api.CommandResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode command response: %v\nbody: %s", err, recorder.Body.Bytes())
	}
	if !response.OK || response.Type != api.TypeCommandFinished {
		t.Fatalf("envelope = ok:%v type:%q", response.OK, response.Type)
	}
	return response
}

func commandTestValidateJSON(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	validator := compat.Load(t)
	if err := validator.Response("command", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
		t.Errorf("response does not validate against the pin: %v\nbody: %s", err, recorder.Body.Bytes())
	}
}

func commandTestValidateNDJSON(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	validator := compat.Load(t)
	if err := validator.NDJSON("command", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
		t.Errorf("stream does not validate against the pin: %v\nbody: %s", err, recorder.Body.Bytes())
	}
}

func commandTestFrames(t *testing.T, recorder *httptest.ResponseRecorder) []api.CommandStreamFrame {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", recorder.Code, recorder.Body.Bytes())
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/x-ndjson" {
		t.Fatalf("Content-Type = %q, want application/x-ndjson", contentType)
	}
	var frames []api.CommandStreamFrame
	for _, line := range bytes.Split(recorder.Body.Bytes(), []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var frame api.CommandStreamFrame
		if err := json.Unmarshal(line, &frame); err != nil {
			t.Fatalf("decode frame %s: %v", line, err)
		}
		frames = append(frames, frame)
	}
	if len(frames) == 0 {
		t.Fatal("stream has no frames")
	}
	return frames
}

func TestNewCommandsHandlerDependencies(t *testing.T) {
	if _, err := api.NewCommandsHandler(api.CommandsOptions{Clients: commandTestFactory(&commandTestClient{})}); err == nil {
		t.Error("nil lookup accepted")
	}
	if _, err := api.NewCommandsHandler(api.CommandsOptions{Lookup: commandTestWithReadyLookup(commandTestNodeID)}); err == nil {
		t.Error("nil client factory accepted")
	}
	handler, err := api.NewCommandsHandler(api.CommandsOptions{
		Lookup:  commandTestWithReadyLookup(commandTestNodeID),
		Clients: commandTestFactory(&commandTestClient{}),
	})
	if err != nil || handler == nil {
		t.Fatalf("valid options: %v, %v", handler, err)
	}
	invalidStreamMax := []int{-1}
	if strconv.IntSize >= 64 {
		invalidStreamMax = append(invalidStreamMax, int(math.MaxUint32)+1)
	}
	for _, streamMax := range invalidStreamMax {
		if _, err := api.NewCommandsHandler(api.CommandsOptions{
			Lookup:           commandTestWithReadyLookup(commandTestNodeID),
			Clients:          commandTestFactory(&commandTestClient{}),
			StreamMaxSeconds: streamMax,
		}); err == nil {
			t.Errorf("StreamMaxSeconds %d accepted", streamMax)
		}
	}
}

func TestCommandRequestValidation(t *testing.T) {
	client, _, calls := commandTestStaticClient([]*nodev1.ExecEvent{commandTestStarted(), commandTestExit(new(int32(0)), "", false, false)}, nil)
	handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
	tests := []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{"empty", "", http.StatusBadRequest, "invalid_json"},
		{"syntax", "{", http.StatusBadRequest, "invalid_json"},
		{"unknown member", `{"command":"true","bogus":1}`, http.StatusBadRequest, "invalid_json"},
		{"missing command", `{}`, http.StatusBadRequest, "invalid_json"},
		{"null command", `{"command":null}`, http.StatusBadRequest, "invalid_json"},
		{"command type", `{"command":5}`, http.StatusBadRequest, "invalid_json"},
		{"cwd type", `{"command":"true","cwd":5}`, http.StatusBadRequest, "invalid_json"},
		{"null cwd", `{"command":"true","cwd":null}`, http.StatusBadRequest, "invalid_json"},
		{"stream type", `{"command":"true","stream":"yes"}`, http.StatusBadRequest, "invalid_json"},
		{"null stream", `{"command":"true","stream":null}`, http.StatusBadRequest, "invalid_json"},
		{"null detached", `{"command":"true","detached":null}`, http.StatusBadRequest, "invalid_json"},
		{"timeout string", `{"command":"true","timeoutSeconds":"30"}`, http.StatusBadRequest, "invalid_timeout"},
		{"timeout float", `{"command":"true","timeoutSeconds":1.5}`, http.StatusBadRequest, "invalid_timeout"},
		{"timeout bool", `{"command":"true","timeoutSeconds":true}`, http.StatusBadRequest, "invalid_timeout"},
		{"timeout null", `{"command":"true","timeoutSeconds":null}`, http.StatusBadRequest, "invalid_timeout"},
		{"timeout zero", `{"command":"true","timeoutSeconds":0}`, http.StatusBadRequest, "invalid_timeout"},
		{"timeout negative", `{"command":"true","timeoutSeconds":-1}`, http.StatusBadRequest, "invalid_timeout"},
		{"timeout over sync cap", `{"command":"true","timeoutSeconds":601}`, http.StatusBadRequest, "invalid_timeout"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, test.body)
			if recorder.Code != test.status {
				t.Fatalf("status = %d, want %d\nbody: %s", recorder.Code, test.status, recorder.Body.Bytes())
			}
			envelope := commandTestDecodeError(t, recorder)
			if envelope.Code != test.code {
				t.Errorf("code = %q, want %q", envelope.Code, test.code)
			}
			commandTestValidateJSON(t, recorder)
		})
	}
	if *calls != 0 {
		t.Errorf("invalid requests reached the node %d times", *calls)
	}

	t.Run("detached false accepted", func(t *testing.T) {
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true","detached":false}`)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200\nbody: %s", recorder.Code, recorder.Body.Bytes())
		}
	})
}

func TestCommandRequestMediaType(t *testing.T) {
	client, _, calls := commandTestStaticClient([]*nodev1.ExecEvent{commandTestStarted(), commandTestExit(new(int32(0)), "", false, false)}, nil)
	handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))

	serve := func(contentType string) *httptest.ResponseRecorder {
		mux := http.NewServeMux()
		mux.Handle("POST "+api.CommandsPath, handler)
		request := httptest.NewRequest(http.MethodPost,
			"/api/v1/sandboxes/"+commandTestSandboxID+"/commands",
			strings.NewReader(`{"command":"true"}`))
		if contentType != "" {
			request.Header.Set("Content-Type", contentType)
		}
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: commandTestOwner}))
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, request)
		return recorder
	}

	recorder := serve("text/plain")
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("text/plain status = %d\nbody: %s", recorder.Code, recorder.Body.Bytes())
	}
	if envelope := commandTestDecodeError(t, recorder); envelope.Code != "invalid_json" {
		t.Errorf("code = %q, want invalid_json", envelope.Code)
	}
	commandTestValidateJSON(t, recorder)
	if *calls != 0 {
		t.Errorf("wrong media type reached the node %d times", *calls)
	}

	recorder = serve("application/json; charset=utf-8")
	if recorder.Code != http.StatusOK {
		t.Fatalf("application/json status = %d\nbody: %s", recorder.Code, recorder.Body.Bytes())
	}

	recorder = serve("")
	if recorder.Code != http.StatusOK {
		t.Fatalf("absent Content-Type status = %d\nbody: %s", recorder.Code, recorder.Body.Bytes())
	}
}

func TestCommandTimeoutBounds(t *testing.T) {
	var durations []uint32
	client := &commandTestClient{exec: func(ctx context.Context, req *nodev1.ExecRequest) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
		durations = append(durations, req.GetTimeoutSeconds())
		return &commandTestStream{ctx: ctx, events: []*nodev1.ExecEvent{commandTestStarted(), commandTestExit(new(int32(0)), "", false, false)}}, nil
	}}
	handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client),
		func(options *api.CommandsOptions) { options.StreamMaxSeconds = 100 })

	tests := []struct {
		name   string
		body   string
		status int
		want   uint32
	}{
		{"sync default", `{"command":"true"}`, http.StatusOK, 30},
		{"sync explicit", `{"command":"true","timeoutSeconds":600}`, http.StatusOK, 600},
		{"stream default", `{"command":"true","stream":true}`, http.StatusOK, 30},
		{"stream configured cap", `{"command":"true","stream":true,"timeoutSeconds":100}`, http.StatusOK, 100},
		{"stream over configured cap", `{"command":"true","stream":true,"timeoutSeconds":101}`, http.StatusBadRequest, 0},
		{"stream above pinned cap", `{"command":"true","stream":true,"timeoutSeconds":7200}`, http.StatusBadRequest, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			durations = nil
			recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, test.body)
			if recorder.Code != test.status {
				t.Fatalf("status = %d, want %d\nbody: %s", recorder.Code, test.status, recorder.Body.Bytes())
			}
			if test.status != http.StatusOK {
				if envelope := commandTestDecodeError(t, recorder); envelope.Code != "invalid_timeout" {
					t.Errorf("code = %q, want invalid_timeout", envelope.Code)
				}
				commandTestValidateJSON(t, recorder)
				return
			}
			if len(durations) != 1 || durations[0] != test.want {
				t.Errorf("timeouts = %v, want [%d]", durations, test.want)
			}
		})
	}

	t.Run("stream default 7200", func(t *testing.T) {
		durations = nil
		defaultHandler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
		recorder := commandTestServe(t, defaultHandler, commandTestOwner, commandTestSandboxID,
			`{"command":"true","stream":true,"timeoutSeconds":7200}`)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d\nbody: %s", recorder.Code, recorder.Body.Bytes())
		}
		if len(durations) != 1 || durations[0] != 7200 {
			t.Errorf("timeouts = %v, want [7200]", durations)
		}
	})
}

func TestCommandSyncResult(t *testing.T) {
	zero := int32(0)
	three := int32(3)
	tests := []struct {
		name     string
		events   []*nodev1.ExecEvent
		success  bool
		exitCode *int
		signal   *string
		oom      bool
		timedOut bool
	}{
		{
			name:    "success",
			events:  []*nodev1.ExecEvent{commandTestStarted(), commandTestStdout("hello\n"), commandTestStderr("warn\n"), commandTestExit(&zero, "", false, false)},
			success: true, exitCode: new(0),
		},
		{
			name:    "nonzero",
			events:  []*nodev1.ExecEvent{commandTestStarted(), commandTestExit(&three, "", false, false)},
			success: false, exitCode: new(3),
		},
		{
			name:    "signal",
			events:  []*nodev1.ExecEvent{commandTestStarted(), commandTestExit(nil, "SIGKILL", false, false)},
			success: false, signal: new("SIGKILL"),
		},
		{
			name:     "timeout",
			events:   []*nodev1.ExecEvent{commandTestStarted(), commandTestStdout("before"), commandTestExit(nil, "", true, false)},
			success:  false,
			timedOut: true,
		},
		{
			name:    "oom",
			events:  []*nodev1.ExecEvent{commandTestStarted(), commandTestExit(nil, "", false, true)},
			success: false, oom: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, request, calls := commandTestStaticClient(test.events, nil)
			handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
			recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID,
				`{"command":"printf hello","cwd":"src","timeoutSeconds":45}`)
			response := commandTestDecodeResponse(t, recorder)

			if *calls != 1 {
				t.Fatalf("Exec calls = %d, want 1", *calls)
			}
			if got := (*request).GetSandboxId(); got != commandTestSandboxID {
				t.Errorf("sandbox_id = %q", got)
			}
			if got := (*request).GetCommand(); got != "printf hello" {
				t.Errorf("command = %q", got)
			}
			if got := (*request).GetCwd(); got != "src" {
				t.Errorf("cwd = %q", got)
			}
			if got := (*request).GetTimeoutSeconds(); got != 45 {
				t.Errorf("timeout = %d", got)
			}
			if response.Success != test.success {
				t.Errorf("success = %v, want %v", response.Success, test.success)
			}
			switch {
			case test.exitCode == nil && response.ExitCode != nil:
				t.Errorf("exitCode = %v, want null", *response.ExitCode)
			case test.exitCode != nil && (response.ExitCode == nil || *response.ExitCode != *test.exitCode):
				t.Errorf("exitCode = %v, want %d", response.ExitCode, *test.exitCode)
			}
			if test.signal == nil && response.Signal != nil {
				t.Errorf("signal = %q, want omitted", *response.Signal)
			}
			if test.signal != nil && (response.Signal == nil || *response.Signal != *test.signal) {
				t.Errorf("signal = %v, want %q", response.Signal, *test.signal)
			}
			if test.oom != (response.OOMKilled != nil && *response.OOMKilled) {
				t.Errorf("oomKilled = %v, want %v", response.OOMKilled, test.oom)
			}
			if response.TimedOut != test.timedOut {
				t.Errorf("timedOut = %v, want %v", response.TimedOut, test.timedOut)
			}
			if response.Cwd != "src" {
				t.Errorf("cwd = %q", response.Cwd)
			}
			if response.StartedAt == nil || response.FinishedAt == nil || response.FinishedAt.Before(*response.StartedAt) {
				t.Errorf("timestamps = %v, %v", response.StartedAt, response.FinishedAt)
			}
			if test.name == "success" {
				if response.Stdout != "hello\n" || response.Stderr != "warn\n" {
					t.Errorf("stdout/stderr = %q/%q", response.Stdout, response.Stderr)
				}
			}
			if test.name == "timeout" && response.Stdout != "before" {
				t.Errorf("stdout = %q", response.Stdout)
			}
			commandTestValidateJSON(t, recorder)
		})
	}
}

func TestCommandSyncNodeFailure(t *testing.T) {
	t.Run("exec call fails", func(t *testing.T) {
		client := &commandTestClient{exec: func(context.Context, *nodev1.ExecRequest) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
			return nil, status.Error(codes.NotFound, "gone")
		}}
		handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusNotFound || envelope.Code != "not_found" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		commandTestValidateJSON(t, recorder)
	})

	t.Run("stream fails after start", func(t *testing.T) {
		client, _, calls := commandTestStaticClient([]*nodev1.ExecEvent{commandTestStarted(), commandTestStdout("partial")}, status.Error(codes.Internal, "boom"))
		handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusBadGateway || envelope.Code != "sandbox_direct_failed" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		if retryable := envelope.Error.Details["retryable"]; retryable != false {
			t.Errorf("retryable = %v, want false", retryable)
		}
		if *calls != 1 {
			t.Errorf("Exec calls = %d, want 1 (no automatic retry)", *calls)
		}
		commandTestValidateJSON(t, recorder)
	})

	t.Run("early EOF", func(t *testing.T) {
		client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{commandTestStarted()}, nil)
		handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusBadGateway || envelope.Code != "sandbox_direct_failed" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		commandTestValidateJSON(t, recorder)
	})

	t.Run("malformed event", func(t *testing.T) {
		client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{commandTestStarted(), {}}, nil)
		handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusBadGateway || envelope.Code != "sandbox_direct_failed" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		commandTestValidateJSON(t, recorder)
	})
}

func TestCommandNodeErrorMapping(t *testing.T) {
	tests := []struct {
		name       string
		nodeCode   codes.Code
		status     int
		wireCode   string
		retryAfter float64
		retryable  *bool
	}{
		{"not found", codes.NotFound, http.StatusNotFound, "not_found", 0, nil},
		{"invalid argument", codes.InvalidArgument, http.StatusBadRequest, "invalid_path", 0, nil},
		{"failed precondition", codes.FailedPrecondition, http.StatusConflict, "sandbox_not_ready", 0, new(false)},
		{"resource exhausted", codes.ResourceExhausted, http.StatusServiceUnavailable, "out_of_capacity", 5, nil},
		{"already exists", codes.AlreadyExists, http.StatusConflict, "conflict", 0, nil},
		{"internal", codes.Internal, http.StatusBadGateway, "sandbox_direct_failed", 0, new(false)},
		{"unavailable", codes.Unavailable, http.StatusBadGateway, "sandbox_direct_failed", 0, new(false)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &commandTestClient{exec: func(context.Context, *nodev1.ExecRequest) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
				return nil, status.Error(test.nodeCode, "node message")
			}}
			handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
			recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
			if recorder.Code != test.status {
				t.Fatalf("status = %d, want %d\nbody: %s", recorder.Code, test.status, recorder.Body.Bytes())
			}
			envelope := commandTestDecodeError(t, recorder)
			if envelope.Code != test.wireCode {
				t.Errorf("code = %q, want %q", envelope.Code, test.wireCode)
			}
			if test.retryAfter > 0 {
				value, ok := envelope.Error.Details["retryAfterSeconds"].(float64)
				if !ok || value != test.retryAfter {
					t.Errorf("retryAfterSeconds = %v, want %v", envelope.Error.Details["retryAfterSeconds"], test.retryAfter)
				}
			}
			if test.retryable != nil {
				value, ok := envelope.Error.Details["retryable"].(bool)
				if !ok || value != *test.retryable {
					t.Errorf("retryable = %v, want %v", envelope.Error.Details["retryable"], *test.retryable)
				}
			}
			commandTestValidateJSON(t, recorder)
		})
	}
}

func TestCommandSyncProtocolState(t *testing.T) {
	tests := []struct {
		name    string
		events  []*nodev1.ExecEvent
		want    int
		wantErr bool
	}{
		{"output before started", []*nodev1.ExecEvent{commandTestStdout("x"), commandTestExit(new(int32(0)), "", false, false)}, http.StatusBadGateway, true},
		{"exit before started", []*nodev1.ExecEvent{commandTestExit(new(int32(0)), "", false, false)}, http.StatusBadGateway, true},
		{"empty before started", []*nodev1.ExecEvent{{}}, http.StatusBadGateway, true},
		{"duplicate started", []*nodev1.ExecEvent{commandTestStarted(), commandTestStarted(), commandTestExit(new(int32(0)), "", false, false)}, http.StatusBadGateway, true},
		{"empty after started", []*nodev1.ExecEvent{commandTestStarted(), {}}, http.StatusBadGateway, true},
		{"started then exit", []*nodev1.ExecEvent{commandTestStarted(), commandTestExit(new(int32(0)), "", false, false)}, http.StatusOK, false},
		{"started output exit", []*nodev1.ExecEvent{commandTestStarted(), commandTestStdout("x"), commandTestExit(new(int32(0)), "", false, false)}, http.StatusOK, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, _, _ := commandTestStaticClient(test.events, nil)
			handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
			recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
			if recorder.Code != test.want {
				t.Fatalf("status = %d, want %d\nbody: %s", recorder.Code, test.want, recorder.Body.Bytes())
			}
			if test.wantErr {
				if envelope := commandTestDecodeError(t, recorder); envelope.Code != "sandbox_direct_failed" {
					t.Errorf("code = %q", envelope.Code)
				}
			}
			commandTestValidateJSON(t, recorder)
		})
	}
}

func commandTestBlockingClient(events ...*nodev1.ExecEvent) *commandTestClient {
	return &commandTestClient{exec: func(ctx context.Context, _ *nodev1.ExecRequest) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
		return &commandTestStream{ctx: ctx, events: events, blockAfter: true}, nil
	}}
}

func TestCommandSyncWatchdog(t *testing.T) {
	t.Run("hung node times out without a 502", func(t *testing.T) {
		client := commandTestBlockingClient(commandTestStarted(), commandTestStdout("before"))
		handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client),
			func(options *api.CommandsOptions) {
				options.ExecutionBudget = func(time.Duration) time.Duration { return 20 * time.Millisecond }
			})
		start := time.Now()
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true","timeoutSeconds":1}`)
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("watchdog took %v", elapsed)
		}
		response := commandTestDecodeResponse(t, recorder)
		if !response.TimedOut || response.ExitCode != nil || response.Success {
			t.Errorf("timedOut/exitCode/success = %v/%v/%v", response.TimedOut, response.ExitCode, response.Success)
		}
		if response.Stdout != "before" {
			t.Errorf("stdout = %q", response.Stdout)
		}
		commandTestValidateJSON(t, recorder)
	})

	t.Run("normal node timeout wins", func(t *testing.T) {
		client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{
			commandTestStarted(), commandTestExit(nil, "", true, false),
		}, nil)
		handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client),
			func(options *api.CommandsOptions) {
				options.ExecutionBudget = func(time.Duration) time.Duration { return 5 * time.Second }
			})
		start := time.Now()
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true","timeoutSeconds":1}`)
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("normal timeout exit did not win: %v", elapsed)
		}
		response := commandTestDecodeResponse(t, recorder)
		if !response.TimedOut || response.ExitCode != nil {
			t.Errorf("timedOut/exitCode = %v/%v", response.TimedOut, response.ExitCode)
		}
	})

	t.Run("hung node before started is 502", func(t *testing.T) {
		client := commandTestBlockingClient()
		handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client),
			func(options *api.CommandsOptions) {
				options.ExecutionBudget = func(time.Duration) time.Duration { return 20 * time.Millisecond }
			})
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true","timeoutSeconds":1}`)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusBadGateway || envelope.Code != "sandbox_direct_failed" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		commandTestValidateJSON(t, recorder)
	})
}

func TestCommandSyncTruncation(t *testing.T) {
	t.Run("independent caps and drain", func(t *testing.T) {
		client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{
			commandTestStarted(),
			commandTestStdout("abcdefgh"),
			commandTestStdout("ijklmnop"),
			commandTestStderr("12345678"),
			commandTestStderr("9"),
			commandTestExit(new(int32(0)), "", false, false),
		}, nil)
		handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client),
			func(options *api.CommandsOptions) { options.OutputLimit = 8 })
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		response := commandTestDecodeResponse(t, recorder)
		if response.Stdout != "abcdefgh" || response.Stderr != "12345678" {
			t.Errorf("output = %q/%q", response.Stdout, response.Stderr)
		}
		if response.StdoutTruncated == nil || !*response.StdoutTruncated {
			t.Errorf("stdoutTruncated = %v", response.StdoutTruncated)
		}
		if response.StderrTruncated == nil || !*response.StderrTruncated {
			t.Errorf("stderrTruncated = %v", response.StderrTruncated)
		}
		commandTestValidateJSON(t, recorder)
	})

	t.Run("huge output is bounded and drained", func(t *testing.T) {
		chunk := strings.Repeat("x", 64<<10)
		events := []*nodev1.ExecEvent{commandTestStarted()}
		for range 20 {
			events = append(events, commandTestStdout(chunk))
		}
		events = append(events, commandTestExit(new(int32(0)), "", false, false))
		client, _, _ := commandTestStaticClient(events, nil)
		handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		response := commandTestDecodeResponse(t, recorder)
		if len(response.Stdout) != api.DefaultCommandOutputLimit {
			t.Errorf("stdout length = %d, want %d", len(response.Stdout), api.DefaultCommandOutputLimit)
		}
		if response.StdoutTruncated == nil || !*response.StdoutTruncated {
			t.Errorf("stdoutTruncated = %v", response.StdoutTruncated)
		}
		if response.ExitCode == nil || *response.ExitCode != 0 {
			t.Errorf("exitCode = %v, want the drained exit", response.ExitCode)
		}
		commandTestValidateJSON(t, recorder)
	})

	t.Run("cap does not split a rune", func(t *testing.T) {
		client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{
			commandTestStarted(),
			commandTestStdout("a€"),
			commandTestExit(new(int32(0)), "", false, false),
		}, nil)
		handler := commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client),
			func(options *api.CommandsOptions) { options.OutputLimit = 3 })
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		response := commandTestDecodeResponse(t, recorder)
		if response.Stdout != "a" {
			t.Errorf("stdout = %q, want %q", response.Stdout, "a")
		}
		if response.StdoutTruncated == nil || !*response.StdoutTruncated {
			t.Errorf("stdoutTruncated = %v", response.StdoutTruncated)
		}
		commandTestValidateJSON(t, recorder)
	})
}

func TestCommandReadiness(t *testing.T) {
	t.Run("starting then ready", func(t *testing.T) {
		lookup := &commandTestLookup{targets: []api.CommandTarget{
			{State: api.StateProvisioning},
			{State: api.StateCloning},
			{NodeID: commandTestNodeID, State: api.StateReady},
		}}
		client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{commandTestStarted(), commandTestExit(new(int32(0)), "", false, false)}, nil)
		var dialed string
		factory := func(nodeID string) (nodev1.NodeClient, func() error, error) {
			dialed = nodeID
			return client, func() error { return nil }, nil
		}
		handler := commandTestHandler(t, lookup, factory, func(options *api.CommandsOptions) {
			options.ReadyPoll = time.Millisecond
			options.ReadyDeadline = time.Second
		})
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d\nbody: %s", recorder.Code, recorder.Body.Bytes())
		}
		if calls, _, _ := lookup.state(); calls != 3 {
			t.Errorf("lookups = %d, want 3", calls)
		}
		if dialed != commandTestNodeID {
			t.Errorf("dialed = %q", dialed)
		}
	})

	t.Run("deadline", func(t *testing.T) {
		lookup := &commandTestLookup{targets: []api.CommandTarget{{State: api.StateProvisioning}}}
		client, _, calls := commandTestStaticClient(nil, nil)
		handler := commandTestHandler(t, lookup, commandTestFactory(client), func(options *api.CommandsOptions) {
			options.ReadyPoll = 5 * time.Millisecond
			options.ReadyDeadline = 20 * time.Millisecond
		})
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusConflict || envelope.Code != "sandbox_starting" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		if retryable := envelope.Error.Details["retryable"]; retryable != true {
			t.Errorf("retryable = %v, want true", retryable)
		}
		if state := envelope.Error.Details["state"]; state != string(api.StateProvisioning) {
			t.Errorf("state = %v", state)
		}
		if *calls != 0 {
			t.Errorf("node reached %d times", *calls)
		}
		commandTestValidateJSON(t, recorder)
	})

	for _, state := range []api.SandboxState{api.StateError, api.StateArchived, api.StateCancelled} {
		t.Run("terminal "+string(state), func(t *testing.T) {
			lookup := &commandTestLookup{targets: []api.CommandTarget{{NodeID: commandTestNodeID, State: state}}}
			client, _, calls := commandTestStaticClient(nil, nil)
			handler := commandTestHandler(t, lookup, commandTestFactory(client))
			recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != http.StatusConflict || envelope.Code != "sandbox_not_ready" {
				t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
			}
			if retryable := envelope.Error.Details["retryable"]; retryable != false {
				t.Errorf("retryable = %v, want false", retryable)
			}
			if got := envelope.Error.Details["state"]; got != string(state) {
				t.Errorf("state = %v, want %q", got, state)
			}
			if calls, _, _ := lookup.state(); calls != 1 {
				t.Errorf("lookups = %d, want 1", calls)
			}
			if *calls != 0 {
				t.Errorf("node reached %d times", *calls)
			}
			commandTestValidateJSON(t, recorder)
		})
	}

	t.Run("not found", func(t *testing.T) {
		lookup := &commandTestLookup{}
		handler := commandTestHandler(t, lookup, commandTestFactory(&commandTestClient{}))
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusNotFound || envelope.Code != "not_found" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		commandTestValidateJSON(t, recorder)
	})

	t.Run("lookup failure", func(t *testing.T) {
		lookup := &commandTestLookup{err: errors.New("database down")}
		handler := commandTestHandler(t, lookup, commandTestFactory(&commandTestClient{}))
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusInternalServerError || envelope.Code != "http_500" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		commandTestValidateJSON(t, recorder)
	})

	t.Run("ready without node", func(t *testing.T) {
		lookup := &commandTestLookup{targets: []api.CommandTarget{{State: api.StateReady}}}
		handler := commandTestHandler(t, lookup, commandTestFactory(&commandTestClient{}))
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusConflict || envelope.Code != "sandbox_not_ready" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		commandTestValidateJSON(t, recorder)
	})
}

// commandTestBlockingLookup blocks every lookup until the caller's context
// ends, proving the readiness deadline bounds a context-aware lookup.
type commandTestBlockingLookup struct {
	once    sync.Once
	entered chan struct{}
}

func (l *commandTestBlockingLookup) TargetLookup(ctx context.Context, _, _ string) (api.CommandTarget, error) {
	l.once.Do(func() { close(l.entered) })
	<-ctx.Done()
	return api.CommandTarget{}, ctx.Err()
}

func TestCommandReadinessDeadlineBoundsLookup(t *testing.T) {
	t.Run("blocked lookup", func(t *testing.T) {
		lookup := &commandTestBlockingLookup{entered: make(chan struct{})}
		handler := commandTestHandler(t, lookup, commandTestFactory(&commandTestClient{}), func(options *api.CommandsOptions) {
			options.ReadyDeadline = 20 * time.Millisecond
		})
		start := time.Now()
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("blocked lookup took %v", elapsed)
		}
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusConflict || envelope.Code != "sandbox_starting" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		if retryable := envelope.Error.Details["retryable"]; retryable != true {
			t.Errorf("retryable = %v, want true", retryable)
		}
	})

	t.Run("parent cancellation writes nothing", func(t *testing.T) {
		lookup := &commandTestBlockingLookup{entered: make(chan struct{})}
		handler := commandTestHandler(t, lookup, commandTestFactory(&commandTestClient{}), func(options *api.CommandsOptions) {
			options.ReadyDeadline = time.Minute
		})
		mux := http.NewServeMux()
		mux.Handle("POST "+api.CommandsPath, handler)
		ctx, cancel := context.WithCancel(t.Context())
		request := httptest.NewRequest(http.MethodPost,
			"/api/v1/sandboxes/"+commandTestSandboxID+"/commands", strings.NewReader(`{"command":"true"}`))
		request.Header.Set("Content-Type", "application/json")
		request = request.WithContext(auth.WithIdentity(ctx, auth.Identity{Owner: commandTestOwner}))
		recorder := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			mux.ServeHTTP(recorder, request)
			close(done)
		}()
		select {
		case <-lookup.entered:
		case <-time.After(time.Second):
			t.Fatal("lookup never started")
		}
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("handler did not return after cancellation")
		}
		if recorder.Body.Len() != 0 {
			t.Errorf("canceled request wrote a body: %s", recorder.Body.Bytes())
		}
	})

	t.Run("poll longer than deadline is clamped", func(t *testing.T) {
		lookup := &commandTestLookup{targets: []api.CommandTarget{{State: api.StateProvisioning}}}
		handler := commandTestHandler(t, lookup, commandTestFactory(&commandTestClient{}), func(options *api.CommandsOptions) {
			options.ReadyDeadline = 20 * time.Millisecond
			options.ReadyPoll = time.Hour
		})
		start := time.Now()
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("long poll overshot the deadline: %v", elapsed)
		}
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusConflict || envelope.Code != "sandbox_starting" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
	})
}

func TestCommandErrorsDoNotLeakNodeOrLookupText(t *testing.T) {
	const (
		lookupSecret = "LOOKUP_SECRET_TOKEN_ABC"
		nodeSecret   = "NODE_SECRET_ENV_VALUE"
		dialSecret   = "DIAL_SECRET_CREDENTIAL"
	)
	newHandler := func(lookup api.CommandTargetLookup, factory api.NodeClientFactory) (http.Handler, *bytes.Buffer) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		return commandTestHandler(t, lookup, factory, func(options *api.CommandsOptions) {
			options.Logger = logger
		}), &logs
	}

	t.Run("lookup", func(t *testing.T) {
		handler, logs := newHandler(&commandTestLookup{err: errors.New(lookupSecret)}, commandTestFactory(&commandTestClient{}))
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		commandTestRequireNoSecret(t, "lookup response", recorder.Body.Bytes(), lookupSecret)
		commandTestRequireNoSecret(t, "lookup logs", logs.Bytes(), lookupSecret)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusInternalServerError || envelope.Code != "http_500" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		commandTestValidateJSON(t, recorder)
	})

	t.Run("typed lookup", func(t *testing.T) {
		lookup := &commandTestLookup{err: apierr.New(http.StatusInternalServerError, apierr.CodeInternal, lookupSecret)}
		handler, logs := newHandler(lookup, commandTestFactory(&commandTestClient{}))
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		commandTestRequireNoSecret(t, "typed lookup response", recorder.Body.Bytes(), lookupSecret)
		commandTestRequireNoSecret(t, "typed lookup logs", logs.Bytes(), lookupSecret)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusInternalServerError || envelope.Code != "http_500" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		commandTestValidateJSON(t, recorder)
	})

	t.Run("node rpc", func(t *testing.T) {
		client := &commandTestClient{exec: func(context.Context, *nodev1.ExecRequest) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
			return nil, status.Error(codes.Internal, nodeSecret)
		}}
		handler, logs := newHandler(commandTestWithReadyLookup(commandTestNodeID), commandTestFactory(client))
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		commandTestRequireNoSecret(t, "node response", recorder.Body.Bytes(), nodeSecret)
		commandTestRequireNoSecret(t, "node logs", logs.Bytes(), nodeSecret)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusBadGateway || envelope.Code != "sandbox_direct_failed" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		commandTestValidateJSON(t, recorder)
	})

	t.Run("dial", func(t *testing.T) {
		factory := func(string) (nodev1.NodeClient, func() error, error) {
			return nil, nil, errors.New(dialSecret)
		}
		handler, logs := newHandler(commandTestWithReadyLookup(commandTestNodeID), factory)
		recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID, `{"command":"true"}`)
		commandTestRequireNoSecret(t, "dial response", recorder.Body.Bytes(), dialSecret)
		commandTestRequireNoSecret(t, "dial logs", logs.Bytes(), dialSecret)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusBadGateway || envelope.Code != "sandbox_direct_failed" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		commandTestValidateJSON(t, recorder)
	})
}

// commandTestRequireNoSecret fails with a fixed message and never echoes the
// captured data: a failing secrecy check must not print the secret itself.
func commandTestRequireNoSecret(t *testing.T, label string, data []byte, secret string) {
	t.Helper()
	if bytes.Contains(data, []byte(secret)) {
		t.Fatalf("%s leaked secret material", label)
	}
}

func TestCommandTenantScoping(t *testing.T) {
	t.Run("identity is required", func(t *testing.T) {
		lookup := commandTestWithReadyLookup(commandTestNodeID)
		handler := commandTestHandler(t, lookup, commandTestFactory(&commandTestClient{}))
		recorder := commandTestServe(t, handler, "", commandTestSandboxID, `{"command":"true"}`)
		envelope := commandTestDecodeError(t, recorder)
		if recorder.Code != http.StatusUnauthorized || envelope.Code != "unauthorized" {
			t.Fatalf("status/code = %d/%q", recorder.Code, envelope.Code)
		}
		if calls, _, _ := lookup.state(); calls != 0 {
			t.Errorf("lookup called %d times without an identity", calls)
		}
		commandTestValidateJSON(t, recorder)
	})

	t.Run("owner and id reach the lookup", func(t *testing.T) {
		lookup := commandTestWithReadyLookup(commandTestNodeID)
		client, _, _ := commandTestStaticClient([]*nodev1.ExecEvent{commandTestStarted(), commandTestExit(new(int32(0)), "", false, false)}, nil)
		handler := commandTestHandler(t, lookup, commandTestFactory(client))
		recorder := commandTestServe(t, handler, commandTestOwner, "bx_abcdefgh", `{"command":"true"}`)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d\nbody: %s", recorder.Code, recorder.Body.Bytes())
		}
		calls, owner, sandboxID := lookup.state()
		if calls != 1 || owner != commandTestOwner || sandboxID != "bx_abcdefgh" {
			t.Errorf("lookup = calls:%d owner:%q id:%q", calls, owner, sandboxID)
		}
	})
}

// commandTestEnv is a full command stack: nodefake behind nodelink behind the
// authenticated gateway, reached only through the handler's client factory.
type commandTestEnv struct {
	node  *nodefake.Node
	root  string
	hub   *nodelink.Hub
	addr  string
	link  *nodelink.Link
	serve *nodegateway.ClientFactory
}

func commandTestStartEnv(t *testing.T) *commandTestEnv {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	wantToken := sha256.Sum256([]byte(commandTestNodeToken))
	authn := func(_ context.Context, hello nodelink.Hello) error {
		gotToken := sha256.Sum256([]byte(hello.Token))
		if subtle.ConstantTimeCompare(gotToken[:], wantToken[:]) != 1 {
			return errors.New("bad node token")
		}
		return nil
	}
	hub := nodelink.NewHub(authn, commandTestLogger())
	hubCtx, hubCancel := context.WithCancel(context.Background())
	hubDone := make(chan struct{})
	go func() { _ = hub.Serve(hubCtx, ln); close(hubDone) }()
	t.Cleanup(func() { hubCancel(); <-hubDone })

	root := t.TempDir()
	node := nodefake.New(commandTestNodeID, root)
	server := grpc.NewServer(nodelink.ServerOptions()...)
	node.Register(server)
	nodeCtx, nodeCancel := context.WithCancel(context.Background())
	nodeDone := make(chan struct{})
	go func() {
		_ = nodelink.Serve(nodeCtx, nodelink.DialConfig{
			Addr: ln.Addr().String(), NodeID: commandTestNodeID, Token: commandTestNodeToken,
			Log: commandTestLogger(), MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		}, server)
		close(nodeDone)
	}()
	t.Cleanup(func() { nodeCancel(); <-nodeDone })

	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()
	link, err := hub.Wait(waitCtx, commandTestNodeID)
	if err != nil {
		t.Fatalf("node never connected: %v", err)
	}
	if _, err := link.Client.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: commandTestSandboxID}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	gateway, err := nodegateway.New(hub, commandTestGatewayToken)
	if err != nil {
		t.Fatal(err)
	}
	gatewayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gatewayCtx, gatewayCancel := context.WithCancel(context.Background())
	gatewayDone := make(chan error, 1)
	go func() { gatewayDone <- gateway.Serve(gatewayCtx, gatewayListener) }()
	t.Cleanup(func() {
		gatewayCancel()
		if err := <-gatewayDone; err != nil {
			t.Errorf("gateway Serve: %v", err)
		}
	})

	return &commandTestEnv{
		node:  node,
		root:  root,
		hub:   hub,
		addr:  gatewayListener.Addr().String(),
		link:  link,
		serve: nodegateway.NewClientFactory(gatewayListener.Addr().String(), commandTestGatewayToken, nil),
	}
}

func (e *commandTestEnv) factory() api.NodeClientFactory {
	return func(nodeID string) (nodev1.NodeClient, func() error, error) {
		client, err := e.serve.Dial(nodeID)
		if err != nil {
			return nil, nil, err
		}
		return client, client.Close, nil
	}
}

func (e *commandTestEnv) handler(t *testing.T, opts ...func(*api.CommandsOptions)) http.Handler {
	t.Helper()
	return commandTestHandler(t, commandTestWithReadyLookup(commandTestNodeID), e.factory(), opts...)
}

func TestCommandSyncRealStack(t *testing.T) {
	env := commandTestStartEnv(t)
	handler := env.handler(t)
	recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID,
		`{"command":"printf out; printf err >&2; exit 3"}`)
	response := commandTestDecodeResponse(t, recorder)
	if response.Success || response.ExitCode == nil || *response.ExitCode != 3 {
		t.Errorf("success/exitCode = %v/%v", response.Success, response.ExitCode)
	}
	if response.Stdout != "out" || response.Stderr != "err" {
		t.Errorf("stdout/stderr = %q/%q", response.Stdout, response.Stderr)
	}
	if response.TimedOut {
		t.Error("timedOut = true")
	}
	commandTestValidateJSON(t, recorder)
}

func TestCommandStreamRealStack(t *testing.T) {
	env := commandTestStartEnv(t)
	handler := env.handler(t)
	recorder := commandTestServe(t, handler, commandTestOwner, commandTestSandboxID,
		`{"command":"printf one; sleep 0.05; printf two; printf bad >&2; exit 7","stream":true}`)
	frames := commandTestFrames(t, recorder)
	if frames[0].Type != api.CommandFrameStarted {
		t.Fatalf("first frame = %q, want started", frames[0].Type)
	}
	var stdout, stderr strings.Builder
	terminals := 0
	for _, frame := range frames[1:] {
		switch frame.Type {
		case api.CommandFrameStdout:
			if !validUTF8(frame.Data) {
				t.Errorf("stdout frame is not UTF-8: %q", frame.Data)
			}
			stdout.WriteString(frame.Data)
		case api.CommandFrameStderr:
			if !validUTF8(frame.Data) {
				t.Errorf("stderr frame is not UTF-8: %q", frame.Data)
			}
			stderr.WriteString(frame.Data)
		case api.CommandFrameExit, api.CommandFrameError:
			terminals++
			if frame.Type != api.CommandFrameExit {
				t.Errorf("terminal frame = %q, want exit", frame.Type)
				continue
			}
			if !frame.ExitCode.Present || frame.ExitCode.Value == nil || *frame.ExitCode.Value != 7 {
				t.Errorf("exitCode = %+v, want 7", frame.ExitCode)
			}
			if !frame.Success.Present || frame.Success.Value == nil || *frame.Success.Value {
				t.Errorf("success = %+v, want false", frame.Success)
			}
		}
	}
	if stdout.String() != "onetwo" || stderr.String() != "bad" {
		t.Errorf("stdout/stderr = %q/%q", stdout.String(), stderr.String())
	}
	if terminals != 1 {
		t.Errorf("terminal frames = %d, want exactly 1", terminals)
	}
	commandTestValidateNDJSON(t, recorder)
}

func TestCommandStreamClientCancellation(t *testing.T) {
	env := commandTestStartEnv(t)
	handler := env.handler(t)
	mux := http.NewServeMux()
	mux.Handle("POST "+api.CommandsPath, handler)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(auth.WithIdentity(r.Context(), auth.Identity{Owner: commandTestOwner})))
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		server.URL+"/api/v1/sandboxes/"+commandTestSandboxID+"/commands",
		strings.NewReader(`{"command":"echo $$ > command.pid; sleep 30 & echo $! > child.pid; wait; touch command-done","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("start stream: %v", err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read started frame: %v", err)
	}
	var started api.CommandStreamFrame
	if err := json.Unmarshal([]byte(line), &started); err != nil || started.Type != api.CommandFrameStarted {
		t.Fatalf("first frame = %q, %v", line, err)
	}

	workDir := filepath.Join(env.root, "vms", commandTestSandboxID, "home", "user")
	shellPID := commandTestReadPID(t, filepath.Join(workDir, "command.pid"))
	childPID := commandTestReadPID(t, filepath.Join(workDir, "child.pid"))
	t.Cleanup(func() {
		// Bounded fallback so a broken cancellation cannot leak a sleep.
		_ = syscall.Kill(-shellPID, syscall.SIGKILL)
	})

	cancel()
	response.Body.Close()

	// The HTTP disconnect must cancel the node RPC, which kills the whole
	// process group: both the shell and its sleep child die, and the
	// post-sleep marker stays unwritten.
	donePath := filepath.Join(workDir, "command-done")
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, statErr := os.Stat(donePath)
		shellDead := commandTestProcessDead(shellPID)
		childDead := commandTestProcessDead(childPID)
		if errors.Is(statErr, os.ErrNotExist) && shellDead && childDead {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after cancel: shell dead=%v, child dead=%v, marker exists=%v", shellDead, childDead, statErr == nil)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// commandTestReadPID waits for a PID file and returns its value.
func commandTestReadPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			if pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data))); parseErr == nil && pid > 0 {
				return pid
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("command never wrote a usable %s", filepath.Base(path))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// commandTestProcessDead reports whether pid is gone or only a zombie. A
// killed process group can leave a zombie briefly until its parent is reaped.
func commandTestProcessDead(pid int) bool {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return errors.Is(err, os.ErrNotExist)
	}
	end := bytes.LastIndexByte(data, ')')
	if end < 0 || end+2 >= len(data) {
		return false
	}
	state := data[end+2]
	return state == 'Z' || state == 'X' || state == 'x'
}

func validUTF8(s string) bool {
	return utf8.ValidString(s)
}
