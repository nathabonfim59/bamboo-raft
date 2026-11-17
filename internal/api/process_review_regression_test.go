package api_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// TestDetachedStartAmbiguityAcrossRetriesKeepsStarting is the round-1 review
// regression: the first StartProcess may have created the process and then
// failed with Unavailable; the same-id retry can legitimately answer
// FailedPrecondition because the VM is stopping. That is still an ambiguous
// start, so the canonical record stays starting and is never marked failed.
func TestDetachedStartAmbiguityAcrossRetriesKeepsStarting(t *testing.T) {
	store := newProcessTestStore()
	calls := 0
	client := &processTestClient{start: func(_ context.Context, request *nodev1.StartProcessRequest) (*nodev1.Process, error) {
		calls++
		if calls == 1 {
			return nil, status.Error(codes.Unavailable, "response lost after start")
		}
		return nil, status.Error(codes.FailedPrecondition, "sandbox stopping")
	}}
	handler := processTestStartHandler(t, store, client)
	recorder := processTestServeStart(t, handler, processTestOwner, processTestSandboxID,
		`{"command":"true","detached":true}`)
	envelope := commandTestDecodeError(t, recorder)
	if recorder.Code != http.StatusBadGateway || envelope.Code != "sandbox_direct_failed" {
		t.Fatalf("error = %d %q, want 502 sandbox_direct_failed", recorder.Code, envelope.Code)
	}
	if got, ok := envelope.Error.Details["retryable"].(bool); !ok || got {
		t.Errorf("retryable = %v, want false", envelope.Error.Details["retryable"])
	}
	if calls != 2 {
		t.Fatalf("StartProcess calls = %d, want 2", calls)
	}
	client.mu.Lock()
	requests := append([]*nodev1.StartProcessRequest(nil), client.startReqs...)
	client.mu.Unlock()
	if requests[0].ProcessId != requests[1].ProcessId || requests[0].Command != requests[1].Command {
		t.Errorf("retry changed the request: %+v vs %+v", requests[0], requests[1])
	}
	if allocate, observe, _ := store.callCounts(); allocate != 1 || observe != 0 {
		t.Errorf("allocate/observe = %d/%d, want 1/0", allocate, observe)
	}
	record, _ := store.record(1)
	if record.Status != db.CommandStarting {
		t.Errorf("record status = %q, want starting", record.Status)
	}
}

// processTestWindowedClient behaves like nodefake's GetProcess: it serves the
// requested byte window from each fixed log and reports More from the per-stream
// sizes, independent of the process state.
func processTestWindowedClient(process *nodev1.Process, stdoutLog, stderrLog []byte) *processTestClient {
	return &processTestClient{get: func(_ context.Context, request *nodev1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
		limit := int64(request.GetLimitBytes())
		if limit == 0 {
			limit = 1 << 20
		}
		stdoutStart := min(request.GetStdoutOffset(), process.StdoutSize)
		stdoutEnd := min(stdoutStart+limit, process.StdoutSize)
		stderrStart := min(request.GetStderrOffset(), process.StderrSize)
		stderrEnd := min(stderrStart+limit, process.StderrSize)
		return &nodev1.ProcessOutput{
			Process:      process,
			Stdout:       stdoutLog[stdoutStart:stdoutEnd],
			Stderr:       stderrLog[stderrStart:stderrEnd],
			StdoutOffset: stdoutStart,
			StderrOffset: stderrStart,
			More:         stdoutEnd < process.StdoutSize || stderrEnd < process.StderrSize,
		}, nil
	}}
}

func TestCommandStatusExitedMultiWindowUTF8(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestExitedRecord(7, new(int32(0))))
	stdoutLog := append(bytes.Repeat([]byte("a"), int(api.CommandStatusOffsetBytes)-1), []byte("éz")...)
	stderrLog := []byte("done")
	process := processTestExitedProcess(7, "cmd", new(int32(0)), "")
	process.StdoutSize = int64(len(stdoutLog))
	process.StderrSize = int64(len(stderrLog))
	client := processTestWindowedClient(process, stdoutLog, stderrLog)
	handler := processTestStatusHandler(t, store, client)

	first := processTestDecodeStatus(t, processTestServeStatus(t, handler, processTestOwner,
		processTestSandboxID, "7", "stdoutOffset=0&stderrOffset=0"))
	if first.StdoutNextOffset == nil || *first.StdoutNextOffset != int64(api.CommandStatusOffsetBytes)-1 {
		t.Fatalf("first next offset = %v, want %d (rune boundary)",
			first.StdoutNextOffset, int64(api.CommandStatusOffsetBytes)-1)
	}
	if first.More == nil || !*first.More {
		t.Errorf("first more = %v, want true: stdout has another window", first.More)
	}
	if first.Stderr != "done" || first.StderrNextOffset == nil || *first.StderrNextOffset != 4 {
		t.Errorf("stderr first window = %q %v", first.Stderr, first.StderrNextOffset)
	}

	second := processTestDecodeStatus(t, processTestServeStatus(t, handler, processTestOwner,
		processTestSandboxID, "7", "stdoutOffset="+strconv.FormatInt(*first.StdoutNextOffset, 10)+"&stderrOffset=4"))
	if second.Stdout != "éz" {
		t.Errorf("second stdout = %q, want éz", second.Stdout)
	}
	if second.More == nil || *second.More {
		t.Errorf("second more = %v, want false", second.More)
	}
	if first.Stdout+second.Stdout != string(stdoutLog) {
		t.Errorf("decoded stdout does not reconstruct the log: %d + %d bytes", len(first.Stdout), len(second.Stdout))
	}
}

func TestCommandStatusLeadingContinuationAtRawZeroIsReplaced(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(7, db.CommandRunning))
	process := processTestRunningProcess(7, "cmd")
	process.StdoutSize = 2
	process.StderrSize = 0
	client := processTestOutputClient(process, processTestStatusOutput(process, []byte{0x80, 'a'}, nil, 0, 0))
	handler := processTestStatusHandler(t, store, client)
	recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", "stdoutOffset=0")
	response := processTestDecodeStatus(t, recorder)
	if response.Stdout != "\uFFFDa" {
		t.Errorf("stdout = %q, want the invalid leading byte replaced", response.Stdout)
	}
	if response.StdoutOffset == nil || *response.StdoutOffset != 0 ||
		response.StdoutNextOffset == nil || *response.StdoutNextOffset != 2 {
		t.Errorf("offsets = %v %v, want 0/2 (raw cursor advances)", response.StdoutOffset, response.StdoutNextOffset)
	}
}

func TestCommandStatusWindowSnapshots(t *testing.T) {
	process := func() *nodev1.Process {
		p := processTestRunningProcess(7, "cmd")
		p.StdoutSize = 10
		p.StderrSize = 0
		return p
	}
	tests := []struct {
		name   string
		query  string
		output *nodev1.ProcessOutput
		legal  bool
	}{
		{
			name:  "clamped offset replaying output is rejected",
			query: "stdoutOffset=10&stderrOffset=0",
			output: &nodev1.ProcessOutput{
				Process: process(), Stdout: []byte("x"), StdoutOffset: 0, StderrOffset: 0,
			},
		},
		{
			name:  "start past the requested offset is rejected",
			query: "stdoutOffset=0&stderrOffset=0",
			output: &nodev1.ProcessOutput{
				Process: process(), Stdout: []byte("x"), StdoutOffset: 1, StderrOffset: 0,
			},
		},
		{
			name:  "short tail with nonzero start is rejected",
			query: "tailBytes=4",
			output: &nodev1.ProcessOutput{
				Process: process(), Stdout: []byte("abc"), StdoutOffset: 2, StderrOffset: 0,
			},
		},
		{
			name:  "clamped empty offset at shrink is legal",
			query: "stdoutOffset=10&stderrOffset=0",
			output: &nodev1.ProcessOutput{
				Process: process(), StdoutOffset: 0, StderrOffset: 0,
			},
			legal: true,
		},
		{
			name:  "short tail at zero after shrink is legal",
			query: "tailBytes=4",
			output: &nodev1.ProcessOutput{
				Process: process(), Stdout: []byte("ab"), StdoutOffset: 0, StderrOffset: 0,
			},
			legal: true,
		},
		{
			name:  "full tail may start anywhere in a snapshot",
			query: "tailBytes=4",
			output: &nodev1.ProcessOutput{
				Process: process(), Stdout: []byte("abcd"), StdoutOffset: 6, StderrOffset: 0,
			},
			legal: true,
		},
		{
			name:  "offset window shorter than the stale size may be a shrink",
			query: "stdoutOffset=0&stderrOffset=0",
			output: &nodev1.ProcessOutput{
				Process: process(), Stdout: []byte("aaaaa"), StdoutOffset: 0, StderrOffset: 0,
			},
			legal: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(processTestRecord(7, db.CommandRunning))
			client := processTestOutputClient(test.output.Process, test.output)
			handler := processTestStatusHandler(t, store, client)
			recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", test.query)
			if test.legal {
				if recorder.Code != http.StatusOK {
					t.Fatalf("legal window = %d\nbody: %s", recorder.Code, recorder.Body.Bytes())
				}
				processTestValidate(t, "commandStatus", recorder)
				return
			}
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != http.StatusBadGateway || envelope.Code != "sandbox_direct_failed" {
				t.Fatalf("error = %d %q, want 502", recorder.Code, envelope.Code)
			}
			if _, observe, _ := store.callCounts(); observe != 0 {
				t.Errorf("bad window was observed %d times", observe)
			}
		})
	}
}

// TestCommandStatusPreservesNativeMoreWithStaleSize is the round-2 fail-before
// regression: the captured sizes can be older than the returned bytes, so the
// native More flag must stay true and the raw cursors must include the whole
// returned window even when both streams are stale.
func TestCommandStatusPreservesNativeMoreWithStaleSize(t *testing.T) {
	tests := []struct {
		name       string
		stdoutSize int64
		stderrSize int64
		stdout     []byte
		stderr     []byte
		nextOut    int64
		nextErr    int64
	}{
		{
			name:       "both streams stale",
			stdoutSize: 5, stderrSize: 3,
			stdout: []byte("0123456789"), stderr: []byte("xyz"),
			nextOut: 10, nextErr: 3,
		},
		{
			name:       "one stream stale while the other is at its snapshot EOF",
			stdoutSize: 5, stderrSize: 3,
			stdout: []byte("0123456789"), stderr: nil,
			nextOut: 10, nextErr: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(processTestRecord(7, db.CommandRunning))
			process := processTestRunningProcess(7, "cmd")
			process.StdoutSize = test.stdoutSize
			process.StderrSize = test.stderrSize
			output := processTestStatusOutput(process, test.stdout, test.stderr, 0, 0)
			output.More = true
			client := processTestOutputClient(process, output)
			handler := processTestStatusHandler(t, store, client)
			recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7",
				"stdoutOffset=0&stderrOffset=0")
			response := processTestDecodeStatus(t, recorder)
			processTestValidate(t, "commandStatus", recorder)
			if response.More == nil || !*response.More {
				t.Errorf("more = %v, want the native true flag", response.More)
			}
			if response.StdoutNextOffset == nil || *response.StdoutNextOffset != test.nextOut {
				t.Errorf("stdoutNextOffset = %v, want %d", response.StdoutNextOffset, test.nextOut)
			}
			if response.StderrNextOffset == nil || *response.StderrNextOffset != test.nextErr {
				t.Errorf("stderrNextOffset = %v, want %d", response.StderrNextOffset, test.nextErr)
			}
		})
	}
}

func TestCommandStatusObservationUsesWindowEnd(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(7, db.CommandRunning))
	process := processTestRunningProcess(7, "cmd")
	process.StdoutSize = 5
	process.StderrSize = 2
	output := processTestStatusOutput(process, []byte("0123456789"), []byte("xyz"), 0, 0)
	output.More = true
	client := processTestOutputClient(process, output)
	handler := processTestStatusHandler(t, store, client)
	processTestDecodeStatus(t, processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7",
		"stdoutOffset=0&stderrOffset=0"))
	record, _ := store.record(7)
	if record.StdoutBytes != 10 || record.StderrBytes != 3 {
		t.Errorf("stored bytes = %d/%d, want 10/3", record.StdoutBytes, record.StderrBytes)
	}
	observation, ok := store.lastObservation()
	if !ok || observation.StdoutBytes != 10 || observation.StderrBytes != 3 {
		t.Errorf("observation = %+v, want raw window ends", observation)
	}
}

// TestCommandStatusAcceptsConcurrentGrowthWindow documents the narrow
// exception to the size guard: the frozen reference stats the log before it
// reads it, so a writing guest can return a window larger than the captured
// size. The start and limit stay validated; the returned window is accepted
// and its raw end is recorded. Native More is the node's own continuation
// signal and is passed through unchanged.
func TestCommandStatusAcceptsConcurrentGrowthWindow(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"tail mode", ""},
		{"offset mode", "stdoutOffset=0&stderrOffset=0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(processTestRecord(7, db.CommandRunning))
			process := processTestRunningProcess(7, "cmd")
			process.StdoutSize = 5
			process.StderrSize = 0
			output := processTestStatusOutput(process, []byte("0123456789"), nil, 0, 0)
			client := processTestOutputClient(process, output)
			handler := processTestStatusHandler(t, store, client)
			recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", test.query)
			response := processTestDecodeStatus(t, recorder)
			if response.Stdout != "0123456789" {
				t.Errorf("stdout = %q, want the grown window", response.Stdout)
			}
		})
	}
}

func TestCommandStatusRejectsMalformedStoredRecord(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*db.CommandRecord)
	}{
		{"foreign owner", func(r *db.CommandRecord) { r.OwnerID = uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-000000000002") }},
		{"foreign sandbox", func(r *db.CommandRecord) { r.SandboxID = "bx_other" }},
		{"wrong process id", func(r *db.CommandRecord) { r.ProcessID = 99 }},
		{"wrong mode", func(r *db.CommandRecord) { r.Mode = "sync" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(processTestRecord(7, db.CommandRunning))
			store.recordMutate = test.mutate
			client := &processTestClient{}
			handler := processTestStatusHandler(t, store, client)
			recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", "")
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != http.StatusInternalServerError || envelope.Code != "http_500" {
				t.Fatalf("error = %d %q, want 500", recorder.Code, envelope.Code)
			}
			if _, get := client.counts(); get != 0 {
				t.Errorf("bad record reached GetProcess: %d calls", get)
			}
			if _, observe, _ := store.callCounts(); observe != 0 {
				t.Errorf("bad record was observed: %d calls", observe)
			}
		})
	}
}

func TestCommandStatusRejectsMalformedObserveResult(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(7, db.CommandRunning))
	store.observeMutate = func(record *db.CommandRecord) {
		record.OwnerID = uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-000000000002")
	}
	process := processTestRunningProcess(7, "cmd")
	process.StdoutSize = 0
	process.StderrSize = 0
	client := processTestOutputClient(process, processTestStatusOutput(process, nil, nil, 0, 0))
	handler := processTestStatusHandler(t, store, client)
	recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", "")
	envelope := commandTestDecodeError(t, recorder)
	if recorder.Code != http.StatusInternalServerError || envelope.Code != "http_500" {
		t.Fatalf("error = %d %q, want 500", recorder.Code, envelope.Code)
	}
}

func TestCommandStatusCancellationAfterRecordRead(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(7, db.CommandRunning))
	ctx, cancel := context.WithCancel(t.Context())
	store.recordHook = cancel
	client := &processTestClient{}
	handler := processTestStatusHandler(t, store, client)
	recorder := processTestServeStatusWithContext(t, handler, ctx, processTestOwner, processTestSandboxID, "7", "")
	if _, get := client.counts(); get != 0 {
		t.Errorf("cancelled status reached GetProcess: %d calls", get)
	}
	if recorder.Body.Len() != 0 {
		t.Errorf("cancelled status wrote a response: %s", recorder.Body.Bytes())
	}
}

func TestCommandStatusCancellationOnRecordError(t *testing.T) {
	store := newProcessTestStore()
	store.recordErr = db.ErrNotFound
	ctx, cancel := context.WithCancel(t.Context())
	store.recordHook = cancel
	handler := processTestStatusHandler(t, store, &processTestClient{})
	recorder := processTestServeStatusWithContext(t, handler, ctx, processTestOwner, processTestSandboxID, "7", "")
	if recorder.Body.Len() != 0 {
		t.Errorf("cancelled error path wrote a response: %s", recorder.Body.Bytes())
	}
}

func processTestServeStatusWithContext(t *testing.T, handler http.Handler, ctx context.Context, owner, sandboxID, processID, query string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET "+api.ProcessStatusPath, handler)
	target := "/api/v1/sandboxes/" + sandboxID + "/commands/" + processID
	if query != "" {
		target += "?" + query
	}
	request := httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
	request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner, KeyID: "key_test"}))
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return recorder
}
