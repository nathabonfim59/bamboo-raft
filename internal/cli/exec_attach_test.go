package cli

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// execAttachTestFrame encodes one NDJSON frame.
func execAttachTestFrame(t *testing.T, members map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(members)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw) + "\n"
}

// execAttachTestScript is one scripted attach answer.
type execAttachTestScript struct {
	body        string
	status      int
	contentType string
	block       bool
}

// execAttachTestServer serves a sequence of attach scripts and records every
// request query. A missing script answers 404 not_found.
type execAttachTestServer struct {
	t       *testing.T
	mu      sync.Mutex
	scripts []execAttachTestScript
	queries []url.Values
	calls   int
}

// handler serves one attach request.
func (s *execAttachTestServer) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	query := r.URL.Query()
	s.queries = append(s.queries, query)
	s.calls++
	var script execAttachTestScript
	if len(s.scripts) > 0 {
		script = s.scripts[0]
		s.scripts = s.scripts[1:]
	} else {
		script = execAttachTestScript{status: http.StatusNotFound}
	}
	s.mu.Unlock()
	if script.status != 0 && script.status != http.StatusOK {
		if script.body == "" {
			cliTestWriteJSON(s.t, w, script.status,
				cliTestErrorEnvelope(script.status, "not_found", "missing"))
			return
		}
		w.WriteHeader(script.status)
		_, _ = io.WriteString(w, script.body)
		return
	}
	contentType := script.contentType
	if contentType == "" {
		contentType = "application/x-ndjson"
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, script.body)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	if script.block {
		<-r.Context().Done()
	}
}

// client builds the real API client for the scripted server.
func (s *execAttachTestServer) client(t *testing.T) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(s.handler))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// execAttachTestRunner returns a runner with fast deterministic retries.
func execAttachTestRunner(client execHTTPClient, out ExecutionOutput) *execAttachRunner {
	return &execAttachRunner{
		client:        client,
		out:           out,
		failureBudget: time.Minute,
		initialDelay:  time.Millisecond,
		maxDelay:      2 * time.Millisecond,
		now:           time.Now,
		sleep:         sleepCtx,
	}
}

// TestExecAttachExitStatuses covers the terminal exit variants.
func TestExecAttachExitStatuses(t *testing.T) {
	cases := []struct {
		name string
		exit map[string]any
		code int
	}{
		{
			name: "zero",
			exit: map[string]any{"type": "exit", "status": "exited", "exitCode": 0, "signal": nil, "stdoutOffset": 0, "stderrOffset": 0},
			code: 0,
		},
		{
			name: "seven",
			exit: map[string]any{"type": "exit", "status": "exited", "exitCode": 7, "signal": nil, "stdoutOffset": 0, "stderrOffset": 0},
			code: 7,
		},
		{
			name: "signal",
			exit: map[string]any{"type": "exit", "status": "exited", "exitCode": nil, "signal": "SIGTERM", "stdoutOffset": 0, "stderrOffset": 0},
			code: 1,
		},
		{
			name: "lost",
			exit: map[string]any{"type": "exit", "status": "lost", "exitCode": nil, "signal": nil, "stdoutOffset": 0, "stderrOffset": 0},
			code: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{{
				body: execAttachTestFrame(t, map[string]any{
					"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
				}) + execAttachTestFrame(t, tc.exit),
			}}}
			var stdout, stderr bytes.Buffer
			runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: &stdout, Stderr: &stderr})
			exit, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7})
			if err != nil {
				t.Fatal(err)
			}
			if got := execAttachStatusExitCode(exit); got != tc.code {
				t.Fatalf("exit code = %d, want %d", got, tc.code)
			}
		})
	}
}

// TestExecAttachRawDeltaDiffersFromDataLength proves the server-repaired UTF-8
// is trusted while the raw nextOffset stays authoritative.
func TestExecAttachRawDeltaDiffersFromDataLength(t *testing.T) {
	server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{{
		body: execAttachTestFrame(t, map[string]any{
			"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
		}) +
			execAttachTestFrame(t, map[string]any{
				"type": "stdout", "data": "a\uFFFDb", "nextOffset": 3,
			}) +
			execAttachTestFrame(t, map[string]any{
				"type": "stdout", "data": "c", "nextOffset": 4,
			}) +
			execAttachTestFrame(t, map[string]any{
				"type": "exit", "status": "exited", "exitCode": 0, "signal": nil,
				"stdoutOffset": 4, "stderrOffset": 0,
			}),
	}}}
	var stdout bytes.Buffer
	runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: &stdout, Stderr: io.Discard})
	if _, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7}); err != nil {
		t.Fatal(err)
	}
	if got := stdout.String(); got != "a\uFFFDb"+"c" {
		t.Fatalf("stdout = %q", got)
	}
}

// TestExecAttachClampedStart accepts a log that was replaced and restarts at 0.
func TestExecAttachClampedStart(t *testing.T) {
	var gotOffset string
	handler := func(w http.ResponseWriter, r *http.Request) {
		gotOffset = r.URL.Query().Get("stdoutOffset")
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, execAttachTestFrame(t, map[string]any{
			"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
		}))
		_, _ = io.WriteString(w, execAttachTestFrame(t, map[string]any{
			"type": "exit", "status": "exited", "exitCode": 0, "signal": nil,
			"stdoutOffset": 0, "stderrOffset": 0,
		}))
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	runner := execAttachTestRunner(client, ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
	if _, err := runner.run(t.Context(), execAttachOptions{
		SandboxID: cliTestSandboxID, ProcessID: 7, StdoutOffset: 5, StderrOffset: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if gotOffset != "5" {
		t.Fatalf("requested offset = %q, want 5", gotOffset)
	}
}

// TestExecAttachMalformedFrames rejects every protocol violation without a
// retry.
func TestExecAttachMalformedFrames(t *testing.T) {
	attached := map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0}
	attachedText := execAttachTestFrame(t, attached)
	chunk := attachedText + execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "abc", "nextOffset": 3})
	cases := map[string]string{
		"stdout first":            execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "x", "nextOffset": 1}),
		"heartbeat first":         execAttachTestFrame(t, map[string]any{"type": "heartbeat"}),
		"exit first":              execAttachTestFrame(t, map[string]any{"type": "exit", "status": "exited", "exitCode": 0, "signal": nil, "stdoutOffset": 0, "stderrOffset": 0}),
		"error first":             execAttachTestFrame(t, map[string]any{"type": "error", "error": "http_500", "message": "x", "stdoutOffset": 0, "stderrOffset": 0}),
		"duplicate attached":      attachedText + attachedText,
		"zero next offset":        attachedText + execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "x", "nextOffset": 0}),
		"backwards next offset":   attachedText + execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "x", "nextOffset": 2}) + execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "y", "nextOffset": 2}),
		"missing data":            attachedText + execAttachTestFrame(t, map[string]any{"type": "stdout", "nextOffset": 1}),
		"empty data":              attachedText + execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "", "nextOffset": 1}),
		"exit running":            attachedText + execAttachTestFrame(t, map[string]any{"type": "exit", "status": "running", "exitCode": 0, "signal": nil, "stdoutOffset": 0, "stderrOffset": 0}),
		"exit code out of range":  attachedText + execAttachTestFrame(t, map[string]any{"type": "exit", "status": "exited", "exitCode": 300, "signal": nil, "stdoutOffset": 0, "stderrOffset": 0}),
		"code and signal":         attachedText + execAttachTestFrame(t, map[string]any{"type": "exit", "status": "exited", "exitCode": 1, "signal": "SIGTERM", "stdoutOffset": 0, "stderrOffset": 0}),
		"lost with exit code":     attachedText + execAttachTestFrame(t, map[string]any{"type": "exit", "status": "lost", "exitCode": 0, "signal": nil, "stdoutOffset": 0, "stderrOffset": 0}),
		"lost with signal":        attachedText + execAttachTestFrame(t, map[string]any{"type": "exit", "status": "lost", "exitCode": nil, "signal": "SIGTERM", "stdoutOffset": 0, "stderrOffset": 0}),
		"unknown type":            attachedText + execAttachTestFrame(t, map[string]any{"type": "started"}),
		"unknown member":          attachedText + execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "x", "nextOffset": 1, "extra": true}),
		"null data":               attachedText + execAttachTestFrame(t, map[string]any{"type": "stdout", "data": nil, "nextOffset": 1}),
		"string next offset":      attachedText + execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "x", "nextOffset": "1"}),
		"attached past requested": execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 5, "stderrOffset": 0}),
		"other process":           execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 8, "stdoutOffset": 0, "stderrOffset": 0}),
		"reconnect ahead":         chunk + execAttachTestFrame(t, map[string]any{"type": "reconnect", "stdoutOffset": 4, "stderrOffset": 0}),
		"reconnect behind":        chunk + execAttachTestFrame(t, map[string]any{"type": "reconnect", "stdoutOffset": 2, "stderrOffset": 0}),
		"error ahead":             chunk + execAttachTestFrame(t, map[string]any{"type": "error", "error": "not_found", "message": "x", "stdoutOffset": 4, "stderrOffset": 0}),
		"error behind":            chunk + execAttachTestFrame(t, map[string]any{"type": "error", "error": "not_found", "message": "x", "stdoutOffset": 2, "stderrOffset": 0}),
		"exit ahead":              chunk + execAttachTestFrame(t, map[string]any{"type": "exit", "status": "exited", "exitCode": 0, "signal": nil, "stdoutOffset": 4, "stderrOffset": 0}),
		"exit behind":             chunk + execAttachTestFrame(t, map[string]any{"type": "exit", "status": "exited", "exitCode": 0, "signal": nil, "stdoutOffset": 2, "stderrOffset": 0}),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{{body: body}}}
			runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
			if _, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7}); err == nil {
				t.Fatal("malformed stream accepted")
			}
			if server.calls != 1 {
				t.Fatalf("attach calls = %d, want 1", server.calls)
			}
		})
	}
}

// TestExecAttachReconnectNoGap follows the pinned reconnect frame.
func TestExecAttachReconnectNoGap(t *testing.T) {
	server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{
		{body: execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0}) +
			execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "abc", "nextOffset": 3}) +
			execAttachTestFrame(t, map[string]any{"type": "reconnect", "stdoutOffset": 3, "stderrOffset": 0})},
		{body: execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 3, "stderrOffset": 0}) +
			execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "def", "nextOffset": 6}) +
			execAttachTestFrame(t, map[string]any{"type": "exit", "status": "exited", "exitCode": 0, "signal": nil, "stdoutOffset": 6, "stderrOffset": 0})},
	}}
	var stdout bytes.Buffer
	runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: &stdout, Stderr: io.Discard})
	if _, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7}); err != nil {
		t.Fatal(err)
	}
	if got := stdout.String(); got != "abcdef" {
		t.Fatalf("stdout = %q", got)
	}
	if server.calls != 2 || server.queries[1].Get("stdoutOffset") != "3" {
		t.Fatalf("calls = %d, second offset = %q", server.calls, server.queries[1].Get("stdoutOffset"))
	}
}

// TestExecAttachNetworkCutNoGap retries a cut stream from the last emitted
// frame and discards a partial JSON line.
func TestExecAttachNetworkCutNoGap(t *testing.T) {
	server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{
		{body: execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0}) +
			execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "abc", "nextOffset": 3}) +
			`{"type":"stdout","data":"x","nextOff`},
		{body: execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 3, "stderrOffset": 0}) +
			execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "def", "nextOffset": 6}) +
			execAttachTestFrame(t, map[string]any{"type": "exit", "status": "exited", "exitCode": 0, "signal": nil, "stdoutOffset": 6, "stderrOffset": 0})},
	}}
	var stdout bytes.Buffer
	runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: &stdout, Stderr: io.Discard})
	if _, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7}); err != nil {
		t.Fatal(err)
	}
	if got := stdout.String(); got != "abcdef" {
		t.Fatalf("stdout = %q", got)
	}
	if server.calls != 2 || server.queries[1].Get("stdoutOffset") != "3" {
		t.Fatalf("calls = %d, second offset = %q", server.calls, server.queries[1].Get("stdoutOffset"))
	}
}

// TestExecAttachNoProgressGuard aborts only repeated no-progress reconnects.
func TestExecAttachNoProgressGuard(t *testing.T) {
	t.Run("three silent reconnects", func(t *testing.T) {
		session := execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0}) +
			execAttachTestFrame(t, map[string]any{"type": "reconnect", "stdoutOffset": 0, "stderrOffset": 0})
		server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{{body: session}, {body: session}, {body: session}, {body: session}}}
		runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
		_, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7})
		cliErr, ok := errors.AsType[*CLIError](err)
		if !ok || cliErr.Code != "attach_failed" {
			t.Fatalf("err = %v", err)
		}
		if server.calls != execAttachNoProgress {
			t.Fatalf("attach calls = %d, want %d", server.calls, execAttachNoProgress)
		}
	})
	t.Run("heartbeats keep it healthy", func(t *testing.T) {
		reconnect := execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0}) +
			execAttachTestFrame(t, map[string]any{"type": "heartbeat"}) +
			execAttachTestFrame(t, map[string]any{"type": "reconnect", "stdoutOffset": 0, "stderrOffset": 0})
		exit := execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0}) +
			execAttachTestFrame(t, map[string]any{"type": "exit", "status": "exited", "exitCode": 0, "signal": nil, "stdoutOffset": 0, "stderrOffset": 0})
		server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{
			{body: reconnect}, {body: reconnect}, {body: reconnect}, {body: exit},
		}}
		runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
		if _, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7}); err != nil {
			t.Fatal(err)
		}
		if server.calls != 4 {
			t.Fatalf("attach calls = %d, want 4", server.calls)
		}
	})
}

// TestExecAttachErrorFrameClassification pins the post-header whitelist.
func TestExecAttachErrorFrameClassification(t *testing.T) {
	attached := execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0})
	exit := attached + execAttachTestFrame(t, map[string]any{"type": "exit", "status": "exited", "exitCode": 0, "signal": nil, "stdoutOffset": 0, "stderrOffset": 0})

	t.Run("sandbox_direct_failed retries", func(t *testing.T) {
		server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{
			{body: attached + execAttachTestFrame(t, map[string]any{"type": "error", "error": "sandbox_direct_failed", "message": "x", "stdoutOffset": 0, "stderrOffset": 0})},
			{body: exit},
		}}
		runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
		if _, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7}); err != nil {
			t.Fatal(err)
		}
		if server.calls != 2 {
			t.Fatalf("attach calls = %d, want 2", server.calls)
		}
	})
	t.Run("http_500 retries", func(t *testing.T) {
		server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{
			{body: attached + execAttachTestFrame(t, map[string]any{"type": "error", "error": "http_500", "message": "x", "stdoutOffset": 0, "stderrOffset": 0})},
			{body: exit},
		}}
		runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
		if _, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("other codes are permanent", func(t *testing.T) {
		server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{
			{body: attached + execAttachTestFrame(t, map[string]any{"type": "error", "error": "not_found", "message": "gone", "stdoutOffset": 0, "stderrOffset": 0})},
		}}
		runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
		_, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7})
		frameErr, ok := errors.AsType[*execAttachFrameError](err)
		if !ok || frameErr.Code != "not_found" {
			t.Fatalf("err = %v", err)
		}
		if server.calls != 1 {
			t.Fatalf("attach calls = %d, want 1", server.calls)
		}
	})
}

// TestExecAttachHTTPClassification pins the retryable envelope handling.
func TestExecAttachHTTPClassification(t *testing.T) {
	t.Run("retryable false is permanent", func(t *testing.T) {
		server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{
			{status: http.StatusBadGateway, body: execAttachTestEnvelope(t, http.StatusBadGateway, "sandbox_direct_failed", false), contentType: "application/json"},
		}}
		runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
		if _, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7}); err == nil {
			t.Fatal("permanent error retried")
		}
		if server.calls != 1 {
			t.Fatalf("attach calls = %d, want 1", server.calls)
		}
	})
	t.Run("retryable true retries", func(t *testing.T) {
		exit := execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0}) +
			execAttachTestFrame(t, map[string]any{"type": "exit", "status": "exited", "exitCode": 0, "signal": nil, "stdoutOffset": 0, "stderrOffset": 0})
		server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{
			{status: http.StatusInternalServerError, body: execAttachTestEnvelope(t, http.StatusInternalServerError, "http_500", true), contentType: "application/json"},
			{body: exit},
		}}
		runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
		if _, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7}); err != nil {
			t.Fatal(err)
		}
		if server.calls != 2 {
			t.Fatalf("attach calls = %d, want 2", server.calls)
		}
	})
	t.Run("plain 404 is permanent", func(t *testing.T) {
		server := &execAttachTestServer{t: t}
		runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
		if _, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7}); err == nil {
			t.Fatal("404 retried")
		}
		if server.calls != 1 {
			t.Fatalf("attach calls = %d, want 1", server.calls)
		}
	})
	t.Run("wrong media type is permanent", func(t *testing.T) {
		server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{
			{body: "{}", contentType: "application/json"},
		}}
		runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
		if _, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7}); err == nil {
			t.Fatal("wrong media type retried")
		}
		if server.calls != 1 {
			t.Fatalf("attach calls = %d, want 1", server.calls)
		}
	})
}

// execAttachTestEnvelope builds one pinned error envelope with a retryable detail.
func execAttachTestEnvelope(t *testing.T, status int, code string, retryable bool) string {
	t.Helper()
	raw, err := json.Marshal(cliTestErrorEnvelopeWithDetails(status, code, "failed", map[string]any{"retryable": retryable}))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestExecAttachBudgetIsNotResetByAttached proves a healthy attached frame
// alone does not renew the continuous-failure budget.
func TestExecAttachBudgetIsNotResetByAttached(t *testing.T) {
	attached := execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0})
	failure := attached + execAttachTestFrame(t, map[string]any{"type": "error", "error": "sandbox_direct_failed", "message": "x", "stdoutOffset": 0, "stderrOffset": 0})
	scripts := make([]execAttachTestScript, 10)
	for i := range scripts {
		scripts[i] = execAttachTestScript{body: failure}
	}
	server := &execAttachTestServer{t: t, scripts: scripts}
	clock := time.Unix(0, 0)
	runner := &execAttachRunner{
		client:        server.client(t),
		out:           ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard},
		failureBudget: 200 * time.Millisecond,
		initialDelay:  100 * time.Millisecond,
		maxDelay:      100 * time.Millisecond,
		now:           func() time.Time { return clock },
		sleep: func(_ context.Context, d time.Duration) error {
			clock = clock.Add(d)
			return nil
		},
	}
	_, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7})
	cliErr, ok := errors.AsType[*CLIError](err)
	if !ok || cliErr.Code != "attach_failed" {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(cliErr.Message, "--status 7") || !strings.Contains(cliErr.Message, "process 7") {
		t.Fatalf("missing reattach hint: %q", cliErr.Message)
	}
	if server.calls >= len(scripts) {
		t.Fatalf("attach calls = %d, budget never expired", server.calls)
	}
}

// execAttachHeaderBlockClient blocks in Do until the request context ends, so
// the caller can prove the idle watchdog covers waiting for HTTP headers.
type execAttachHeaderBlockClient struct {
	calls atomic.Int64
}

// NewRequest implements execHTTPClient.
func (c *execAttachHeaderBlockClient) NewRequest(ctx context.Context, method, path string, _ io.Reader) (*http.Request, error) {
	return http.NewRequestWithContext(ctx, method, "http://example.test"+path, nil)
}

// Do implements execHTTPClient.
func (c *execAttachHeaderBlockClient) Do(req *http.Request) (*http.Response, error) {
	c.calls.Add(1)
	<-req.Context().Done()
	return nil, req.Context().Err()
}

// TestExecAttachIdleWatchdogRetries proves a silent half-open body is retried
// from the committed cursors instead of blocking a timeout-0 command forever.
func TestExecAttachIdleWatchdogRetries(t *testing.T) {
	server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{
		{body: execAttachTestFrame(t, map[string]any{
			"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
		}), block: true},
		{body: execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0}) +
			execAttachTestFrame(t, map[string]any{"type": "exit", "status": "exited", "exitCode": 0, "signal": nil, "stdoutOffset": 0, "stderrOffset": 0})},
	}}
	runner := execAttachTestRunner(server.client(t), ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
	// The first session proves the 20 ms watchdog cancels a silent body.
	// Once it has fired and been joined, the retry raises the watchdog so a
	// loaded VM cannot time out the healthy second real HTTP session.
	runner.idleTimeout = 20 * time.Millisecond
	firstRetry := true
	runner.sleep = func(ctx context.Context, d time.Duration) error {
		if firstRetry {
			firstRetry = false
			runner.idleTimeout = 5 * time.Second
		}
		return sleepCtx(ctx, d)
	}
	if _, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7}); err != nil {
		t.Fatal(err)
	}
	if server.calls != 2 {
		t.Fatalf("attach calls = %d, want 2", server.calls)
	}
}

// TestExecAttachIdleCoversHeaders proves the watchdog also aborts a request
// that never returns HTTP headers.
func TestExecAttachIdleCoversHeaders(t *testing.T) {
	client := &execAttachHeaderBlockClient{}
	runner := execAttachTestRunner(client, ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
	runner.idleTimeout = 15 * time.Millisecond
	runner.failureBudget = 40 * time.Millisecond
	runner.initialDelay = 5 * time.Millisecond
	runner.maxDelay = 5 * time.Millisecond
	_, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7})
	cliErr, ok := errors.AsType[*CLIError](err)
	if !ok || cliErr.Code != "attach_failed" {
		t.Fatalf("err = %v", err)
	}
	if client.calls.Load() < 2 {
		t.Fatalf("Do calls = %d, want a retry", client.calls.Load())
	}
	if !strings.Contains(cliErr.Message, "--status 7") {
		t.Fatalf("missing reattach hint: %q", cliErr.Message)
	}
}

// TestExecAttachCancellation stops the stream at the caller's context.
func TestExecAttachCancellation(t *testing.T) {
	started := make(chan struct{})
	var startOnce sync.Once
	server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{{
		body: execAttachTestFrame(t, map[string]any{
			"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
		}),
		block: true,
	}}}
	handler := server.handler
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startOnce.Do(func() { close(started) })
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	client, err := NewClient(srv.URL, "test-key", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	var runErr error
	runDone := make(chan struct{})
	runner := execAttachTestRunner(client, ExecutionOutput{Stdout: io.Discard, Stderr: io.Discard})
	go func() {
		_, runErr = runner.run(ctx, execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7})
		close(runDone)
	}()
	// Unblock and join on every failure path before the server closes.
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(2 * time.Second):
			t.Error("attach did not stop during cleanup")
		}
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("attach stream never reached the blocking handler")
	}
	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("attach did not stop after cancellation")
	}
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("err = %v", runErr)
	}
}

// execAttachFailWriter always fails.
type execAttachFailWriter struct{}

// Write implements io.Writer.
func (execAttachFailWriter) Write([]byte) (int, error) { return 0, errors.New("boom") }

// TestExecAttachWriteFailure reports a safe error and stops reading.
func TestExecAttachWriteFailure(t *testing.T) {
	server := &execAttachTestServer{t: t, scripts: []execAttachTestScript{{
		body: execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0}) +
			execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "x", "nextOffset": 1}) +
			execAttachTestFrame(t, map[string]any{"type": "exit", "status": "exited", "exitCode": 0, "signal": nil, "stdoutOffset": 1, "stderrOffset": 0}),
		block: true,
	}}}
	runner := execAttachTestRunner(server.client(t), ExecutionOutput{JSON: execAttachFailWriter{}})
	_, err := runner.run(t.Context(), execAttachOptions{SandboxID: cliTestSandboxID, ProcessID: 7})
	if !execIsWriteFailure(err) {
		t.Fatalf("err = %v, want write_failed", err)
	}
}

// TestExecAttachDataFrameShape pins the legacy JSON frame conversion.
func TestExecAttachDataFrameShape(t *testing.T) {
	frame := api.CommandStreamFrame{Type: api.CommandFrameStdout, Data: "a\uFFFDb"}
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Type string `json:"type"`
		Data string `json:"data"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("frame is not valid JSON: %v", err)
	}
	if decoded.Type != "stdout" || decoded.Data != "a\uFFFDb" {
		t.Fatalf("decoded = %+v", decoded)
	}
}
