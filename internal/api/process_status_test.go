package api_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func processTestRecord(processID uint32, commandStatus db.CommandStatus) db.CommandRecord {
	now := time.UnixMilli(processTestStartedAt).UTC()
	return db.CommandRecord{
		CommandID:   "cmd_processaaaaa",
		OwnerID:     uuid.MustParse(processTestOwner),
		SandboxID:   processTestSandboxID,
		ProcessID:   processID,
		Mode:        db.CommandModeDetached,
		Command:     "stored-command",
		Status:      commandStatus,
		PID:         4242,
		StartedAt:   now,
		StdoutBytes: 5,
		StderrBytes: 3,
		LogPath:     "/home/user/.ascii/processes/7.log",
		ErrLogPath:  "/home/user/.ascii/processes/7.err.log",
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func processTestExitedRecord(processID uint32, code *int32) db.CommandRecord {
	record := processTestRecord(processID, db.CommandExited)
	record.ExitCode = code
	finished := time.UnixMilli(processTestStartedAt + 1000).UTC()
	record.FinishedAt = &finished
	return record
}

func processTestStatusOutput(process *nodev1.Process, stdout, stderr []byte, stdoutOffset, stderrOffset int64) *nodev1.ProcessOutput {
	return &nodev1.ProcessOutput{
		Process:      process,
		Stdout:       stdout,
		Stderr:       stderr,
		StdoutOffset: stdoutOffset,
		StderrOffset: stderrOffset,
	}
}

func processTestOutputClient(process *nodev1.Process, output *nodev1.ProcessOutput) *processTestClient {
	return &processTestClient{get: func(_ context.Context, _ *nodev1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
		return output, nil
	}}
}

func TestCommandStatusTailDefaultRunning(t *testing.T) {
	store := newProcessTestStore()
	record := processTestRecord(7, db.CommandRunning)
	store.seed(record)
	process := processTestRunningProcess(7, "full-node-command")
	process.StdoutSize = 5
	process.StderrSize = 3
	client := processTestOutputClient(process, processTestStatusOutput(process, []byte("hello"), []byte("err"), 0, 0))
	handler := processTestStatusHandler(t, store, client)

	recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", "")
	response := processTestDecodeStatus(t, recorder)
	processTestValidate(t, "commandStatus", recorder)
	if response.Status != "running" || !response.Running || response.Success {
		t.Errorf("state = %+v", response)
	}
	if response.ExitCode != nil {
		t.Errorf("exitCode = %v, want null", *response.ExitCode)
	}
	if response.Command == nil || *response.Command != "full-node-command" {
		t.Errorf("command = %v", response.Command)
	}
	if response.Stdout != "hello" || response.Stderr != "err" || response.StdoutOffset != nil || response.More != nil {
		t.Errorf("window = %+v", response)
	}
	if response.Known == nil || !*response.Known || response.PID == nil || *response.PID != 4242 {
		t.Errorf("identity = %+v", response)
	}
	client.mu.Lock()
	requests := append([]*nodev1.GetProcessRequest(nil), client.getReqs...)
	client.mu.Unlock()
	if len(requests) != 1 || requests[0].LimitBytes != api.DefaultCommandStatusTailBytes ||
		requests[0].StdoutOffset != nil || requests[0].StderrOffset != nil {
		t.Errorf("GetProcess request = %+v", requests)
	}
	stored, _ := store.record(7)
	if stored.Status != db.CommandRunning || stored.StdoutBytes != 5 || stored.StderrBytes != 3 {
		t.Errorf("observation = %+v", stored)
	}
}

func TestCommandStatusOffsetMode(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(7, db.CommandRunning))
	process := processTestRunningProcess(7, "cmd")
	process.StdoutSize = 10
	process.StderrSize = 3
	output := processTestStatusOutput(process, []byte("23456"), []byte("rr"), 2, 1)
	output.More = true
	client := processTestOutputClient(process, output)
	handler := processTestStatusHandler(t, store, client)

	recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7",
		"stdoutOffset=2&stderrOffset=1&tailBytes=4")
	response := processTestDecodeStatus(t, recorder)
	processTestValidate(t, "commandStatus", recorder)
	if response.Stdout != "23456" || response.Stderr != "rr" {
		t.Errorf("output = %q %q", response.Stdout, response.Stderr)
	}
	if response.StdoutOffset == nil || *response.StdoutOffset != 2 ||
		response.StdoutNextOffset == nil || *response.StdoutNextOffset != 7 {
		t.Errorf("stdout offsets = %+v %+v", response.StdoutOffset, response.StdoutNextOffset)
	}
	if response.StderrOffset == nil || *response.StderrOffset != 1 ||
		response.StderrNextOffset == nil || *response.StderrNextOffset != 3 {
		t.Errorf("stderr offsets = %+v %+v", response.StderrOffset, response.StderrNextOffset)
	}
	if response.More == nil || !*response.More {
		t.Errorf("more = %v, want true", response.More)
	}
	if response.StdoutTruncated != nil || response.StderrTruncated != nil {
		t.Errorf("offset mode emitted tail truncation flags: %+v", response)
	}
	client.mu.Lock()
	request := client.getReqs[0]
	client.mu.Unlock()
	if request.LimitBytes != api.CommandStatusOffsetBytes ||
		request.StdoutOffset == nil || *request.StdoutOffset != 2 ||
		request.StderrOffset == nil || *request.StderrOffset != 1 {
		t.Errorf("GetProcess request = %+v; tailBytes must be ignored", request)
	}
}

func TestCommandStatusMixedWindowDefaultsMissingOffset(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(7, db.CommandRunning))
	process := processTestRunningProcess(7, "cmd")
	process.StdoutSize = 10
	process.StderrSize = 3
	output := processTestStatusOutput(process, []byte("23456"), []byte("err"), 2, 0)
	output.More = true
	client := processTestOutputClient(process, output)
	handler := processTestStatusHandler(t, store, client)

	recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7",
		"stdoutOffset=2&tailBytes=100")
	response := processTestDecodeStatus(t, recorder)
	if response.StderrOffset == nil || *response.StderrOffset != 0 ||
		response.StderrNextOffset == nil || *response.StderrNextOffset != 3 {
		t.Errorf("stderr offsets = %+v %+v", response.StderrOffset, response.StderrNextOffset)
	}
	client.mu.Lock()
	request := client.getReqs[0]
	client.mu.Unlock()
	if request.LimitBytes != api.CommandStatusOffsetBytes ||
		request.StderrOffset == nil || *request.StderrOffset != 0 {
		t.Errorf("GetProcess request = %+v; missing offset must default to 0", request)
	}
}

func TestCommandStatusQueryValidation(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"zero tail", "tailBytes=0"},
		{"negative tail", "tailBytes=-1"},
		{"tail above max", "tailBytes=8388609"},
		{"tail not integer", "tailBytes=x"},
		{"tail float", "tailBytes=1.5"},
		{"tail empty", "tailBytes="},
		{"unknown parameter", "bogus=1"},
		{"repeat tail", "tailBytes=4&tailBytes=5"},
		{"negative stdout offset", "stdoutOffset=-1"},
		{"negative stderr offset", "stderrOffset=-1"},
		{"bad stdout offset", "stdoutOffset=x"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(processTestRecord(7, db.CommandRunning))
			client := &processTestClient{}
			handler := processTestStatusHandler(t, store, client)
			recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", test.query)
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != http.StatusBadRequest || envelope.Code != "invalid_query" {
				t.Fatalf("error = %d %q, want 400 invalid_query\nbody: %s", recorder.Code, envelope.Code, recorder.Body.Bytes())
			}
			processTestValidate(t, "commandStatus", recorder)
			if _, get := client.counts(); get != 0 {
				t.Errorf("invalid query reached GetProcess: %d calls", get)
			}
		})
	}
}

func TestCommandStatusTailBytesLimit(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(7, db.CommandRunning))
	process := processTestRunningProcess(7, "cmd")
	process.StdoutSize = 10
	process.StderrSize = 0
	client := processTestOutputClient(process, processTestStatusOutput(process, []byte("6789"), nil, 6, 0))
	handler := processTestStatusHandler(t, store, client)

	recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", "tailBytes=4")
	response := processTestDecodeStatus(t, recorder)
	if response.Stdout != "6789" || response.StdoutTruncated == nil || !*response.StdoutTruncated {
		t.Errorf("tail response = %+v", response)
	}
	client.mu.Lock()
	request := client.getReqs[0]
	client.mu.Unlock()
	if request.LimitBytes != 4 {
		t.Errorf("LimitBytes = %d, want 4", request.LimitBytes)
	}
}

func TestCommandStatusWireStates(t *testing.T) {
	zero := int32(0)
	three := int32(3)
	tests := []struct {
		name        string
		record      db.CommandRecord
		process     *nodev1.Process
		status      string
		running     bool
		success     bool
		exitCode    *int
		signal      string
		recordState db.CommandStatus
		finished    bool
	}{
		{
			name:        "running",
			record:      processTestRecord(7, db.CommandRunning),
			process:     processTestRunningProcess(7, "cmd"),
			status:      "running",
			running:     true,
			recordState: db.CommandRunning,
		},
		{
			name:        "exited zero",
			record:      processTestRecord(7, db.CommandRunning),
			process:     processTestExitedProcess(7, "cmd", &zero, ""),
			status:      "exited",
			success:     true,
			exitCode:    new(0),
			recordState: db.CommandExited,
			finished:    true,
		},
		{
			name:        "exited nonzero",
			record:      processTestRecord(7, db.CommandRunning),
			process:     processTestExitedProcess(7, "cmd", &three, ""),
			status:      "exited",
			exitCode:    new(3),
			recordState: db.CommandExited,
			finished:    true,
		},
		{
			name:        "signal stays exited on the wire",
			record:      processTestRecord(7, db.CommandRunning),
			process:     processTestExitedProcess(7, "cmd", nil, "SIGKILL"),
			status:      "exited",
			signal:      "SIGKILL",
			recordState: db.CommandKilled,
			finished:    true,
		},
		{
			name:   "lost",
			record: processTestRecord(7, db.CommandRunning),
			process: func() *nodev1.Process {
				p := processTestRunningProcess(7, "cmd")
				p.State = nodev1.ProcessState_PROCESS_STATE_LOST
				return p
			}(),
			status:      "lost",
			recordState: db.CommandLost,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(test.record)
			process := test.process
			process.StdoutSize = 0
			process.StderrSize = 0
			client := processTestOutputClient(process, processTestStatusOutput(process, nil, nil, 0, 0))
			handler := processTestStatusHandler(t, store, client)
			recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", "")
			response := processTestDecodeStatus(t, recorder)
			processTestValidate(t, "commandStatus", recorder)
			if response.Status != test.status || response.Running != test.running || response.Success != test.success {
				t.Errorf("state = %+v", response)
			}
			if test.exitCode == nil {
				if response.ExitCode != nil {
					t.Errorf("exitCode = %v, want null", *response.ExitCode)
				}
			} else if response.ExitCode == nil || *response.ExitCode != *test.exitCode {
				t.Errorf("exitCode = %v, want %d", response.ExitCode, *test.exitCode)
			}
			if test.signal != "" {
				if response.Signal == nil || *response.Signal != test.signal {
					t.Errorf("signal = %v, want %q", response.Signal, test.signal)
				}
			}
			if test.finished && response.FinishedAt == nil {
				t.Error("finishedAt is missing")
			}
			if !test.finished && response.FinishedAt != nil {
				t.Errorf("finishedAt = %v, want null", response.FinishedAt)
			}
			stored, _ := store.record(7)
			if stored.Status != test.recordState {
				t.Errorf("record status = %q, want %q", stored.Status, test.recordState)
			}
		})
	}
}

func TestCommandStatusUTF8AndOffsets(t *testing.T) {
	exited := processTestExitedRecord(7, new(int32(0)))
	tests := []struct {
		name         string
		record       db.CommandRecord
		process      func() *nodev1.Process
		stdout       []byte
		size         int64
		offset       int64
		pastEnd      bool
		more         bool
		want         string
		wantOffset   int64
		wantNext     int64
		wantMore     bool
		wantObserve  int
		wantProtocol bool
	}{
		{
			name:        "offset past end",
			record:      processTestRecord(7, db.CommandRunning),
			process:     func() *nodev1.Process { return processTestRunningProcess(7, "cmd") },
			offset:      99,
			pastEnd:     true,
			size:        10,
			want:        "",
			wantOffset:  10,
			wantNext:    10,
			wantObserve: 1,
		},
		{
			name:        "running trailing partial rune",
			record:      processTestRecord(7, db.CommandRunning),
			process:     func() *nodev1.Process { return processTestRunningProcess(7, "cmd") },
			stdout:      []byte{'a', 0xC3},
			size:        2,
			want:        "a",
			wantOffset:  0,
			wantNext:    1,
			wantMore:    false,
			wantObserve: 1,
		},
		{
			name:        "ended trailing partial rune",
			record:      exited,
			process:     func() *nodev1.Process { return processTestExitedProcess(7, "cmd", new(int32(0)), "") },
			stdout:      []byte{'a', 0xC3},
			size:        2,
			want:        "a\uFFFD",
			wantOffset:  0,
			wantNext:    2,
			wantObserve: 1,
		},
		{
			name:        "leading continuation at raw zero is replaced",
			record:      processTestRecord(7, db.CommandRunning),
			process:     func() *nodev1.Process { return processTestRunningProcess(7, "cmd") },
			stdout:      []byte{0x80, 'a'},
			size:        2,
			want:        "\uFFFDa",
			wantOffset:  0,
			wantNext:    2,
			wantObserve: 1,
		},
		{
			name:        "interior invalid bytes replaced",
			record:      processTestRecord(7, db.CommandRunning),
			process:     func() *nodev1.Process { return processTestRunningProcess(7, "cmd") },
			stdout:      []byte{'a', 0xFF, 'b'},
			size:        3,
			want:        "a\uFFFDb",
			wantOffset:  0,
			wantNext:    3,
			wantObserve: 1,
		},
		{
			name:        "incomplete rune makes no progress while running",
			record:      processTestRecord(7, db.CommandRunning),
			process:     func() *nodev1.Process { return processTestRunningProcess(7, "cmd") },
			stdout:      []byte{0xC3},
			size:        1,
			want:        "",
			wantOffset:  0,
			wantNext:    0,
			wantMore:    false,
			wantObserve: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(test.record)
			process := test.process()
			process.StdoutSize = test.size
			process.StderrSize = 0
			queryOffset := test.offset
			outputOffset := test.offset
			if test.pastEnd {
				outputOffset = test.size
			}
			output := processTestStatusOutput(process, test.stdout, nil, outputOffset, 0)
			output.More = test.more
			client := processTestOutputClient(process, output)
			handler := processTestStatusHandler(t, store, client)
			recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7",
				"stdoutOffset="+strconv.FormatInt(queryOffset, 10))
			if test.wantProtocol {
				envelope := commandTestDecodeError(t, recorder)
				if recorder.Code != http.StatusBadGateway || envelope.Code != "sandbox_direct_failed" {
					t.Fatalf("error = %d %q, want 502", recorder.Code, envelope.Code)
				}
			} else {
				response := processTestDecodeStatus(t, recorder)
				processTestValidate(t, "commandStatus", recorder)
				if response.Stdout != test.want {
					t.Errorf("stdout = %q, want %q", response.Stdout, test.want)
				}
				if response.StdoutOffset == nil || *response.StdoutOffset != test.wantOffset {
					t.Errorf("stdoutOffset = %v, want %d", response.StdoutOffset, test.wantOffset)
				}
				if response.StdoutNextOffset == nil || *response.StdoutNextOffset != test.wantNext {
					t.Errorf("stdoutNextOffset = %v, want %d", response.StdoutNextOffset, test.wantNext)
				}
				if response.More == nil || *response.More != test.wantMore {
					t.Errorf("more = %v, want %v", response.More, test.wantMore)
				}
			}
			if _, observe, _ := store.callCounts(); observe != test.wantObserve {
				t.Errorf("observe calls = %d, want %d", observe, test.wantObserve)
			}
		})
	}
}

func TestCommandStatusUTF8SplitAcrossPolls(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(7, db.CommandRunning))
	calls := 0
	client := &processTestClient{get: func(_ context.Context, _ *nodev1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
		calls++
		process := processTestRunningProcess(7, "cmd")
		if calls == 1 {
			// The writer has emitted only the first byte of a two-byte
			// rune on each stream, so neither window can progress yet.
			process.StdoutSize = 1
			process.StderrSize = 1
			return processTestStatusOutput(process, []byte{0xC3}, []byte{0xC3}, 0, 0), nil
		}
		process.StdoutSize = 2
		process.StderrSize = 2
		// The continuation bytes arrived; both runes complete.
		return processTestStatusOutput(process, []byte{0xC3, 0xA9}, []byte{0xC3, 0xB1}, 0, 0), nil
	}}
	handler := processTestStatusHandler(t, store, client)

	first := processTestDecodeStatus(t, processTestServeStatus(t, handler, processTestOwner,
		processTestSandboxID, "7", "stdoutOffset=0&stderrOffset=0"))
	if first.Stdout != "" || first.Stderr != "" {
		t.Errorf("incomplete runes = %q %q, want empty", first.Stdout, first.Stderr)
	}
	if first.StdoutNextOffset == nil || *first.StdoutNextOffset != 0 ||
		first.StderrNextOffset == nil || *first.StderrNextOffset != 0 ||
		first.More == nil || *first.More {
		t.Errorf("first offsets = %+v %+v more %v", first.StdoutNextOffset, first.StderrNextOffset, first.More)
	}

	second := processTestDecodeStatus(t, processTestServeStatus(t, handler, processTestOwner,
		processTestSandboxID, "7", "stdoutOffset=0&stderrOffset=0"))
	if second.Stdout != "é" || second.Stderr != "ñ" {
		t.Errorf("completed output = %q %q, want é ñ", second.Stdout, second.Stderr)
	}
	if second.StdoutNextOffset == nil || *second.StdoutNextOffset != 2 ||
		second.StderrNextOffset == nil || *second.StderrNextOffset != 2 {
		t.Errorf("second offsets = %+v %+v", second.StdoutNextOffset, second.StderrNextOffset)
	}
}

func TestCommandStatusErrors(t *testing.T) {
	errorTests := []struct {
		name          string
		lookup        api.CommandTargetLookup
		seed          *db.CommandRecord
		missingRecord bool
		recordErr     error
		getErr        error
		status        int
		code          string
		retryable     *bool
		state         string
		getCalls      int
	}{
		{"target missing", &commandTestLookup{err: api.ErrCommandTargetNotFound}, nil, false, nil, nil, http.StatusNotFound, "not_found", nil, "", 0},
		{"lookup fault", &commandTestLookup{err: errors.New("private")}, nil, false, nil, nil, http.StatusInternalServerError, "http_500", nil, "", 0},
		{"record missing", nil, nil, true, nil, nil, http.StatusNotFound, "not_found", nil, "", 0},
		{"record fault", nil, nil, false, errors.New("private"), nil, http.StatusInternalServerError, "http_500", nil, "", 0},
		{"not ready", &commandTestLookup{targets: []api.CommandTarget{{NodeID: processTestNodeID, State: api.StateError}}}, nil, false, nil, nil, http.StatusConflict, "sandbox_not_ready", new(false), "error", 0},
		{"starting", &commandTestLookup{targets: []api.CommandTarget{{NodeID: processTestNodeID, State: api.StateProvisioning}}}, nil, false, nil, nil, http.StatusConflict, "sandbox_starting", new(true), "provisioning", 0},
		{"node not found", nil, nil, false, nil, status.Error(codes.NotFound, "gone"), http.StatusNotFound, "not_found", nil, "", 1},
		{"node invalid argument", nil, nil, false, nil, status.Error(codes.InvalidArgument, "bad"), http.StatusBadRequest, "invalid_path", nil, "", 1},
		{"node not ready", nil, nil, false, nil, status.Error(codes.FailedPrecondition, "stopped"), http.StatusConflict, "sandbox_not_ready", new(false), "ready", 1},
		{"resource exhausted is not capacity", nil, nil, false, nil, status.Error(codes.ResourceExhausted, "size"), http.StatusBadGateway, "sandbox_direct_failed", new(false), "", 1},
		{"unavailable", nil, nil, false, nil, status.Error(codes.Unavailable, "link"), http.StatusBadGateway, "sandbox_direct_failed", new(false), "", 1},
	}
	for _, test := range errorTests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			if test.seed != nil {
				store.seed(*test.seed)
			} else if !test.missingRecord {
				store.seed(processTestRecord(7, db.CommandRunning))
			}
			store.recordErr = test.recordErr
			process := processTestRunningProcess(7, "cmd")
			process.StdoutSize = 0
			process.StderrSize = 0
			client := &processTestClient{get: func(_ context.Context, _ *nodev1.GetProcessRequest) (*nodev1.ProcessOutput, error) {
				if test.getErr != nil {
					return nil, test.getErr
				}
				return processTestStatusOutput(process, nil, nil, 0, 0), nil
			}}
			options := []func(*api.CommandsOptions){}
			if test.lookup != nil {
				options = append(options, func(options *api.CommandsOptions) { options.Lookup = test.lookup })
			}
			handler := processTestStatusHandler(t, store, client, options...)
			recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", "")
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != test.status || envelope.Code != test.code {
				t.Fatalf("error = %d %q, want %d %q\nbody: %s", recorder.Code, envelope.Code, test.status, test.code, recorder.Body.Bytes())
			}
			processTestValidate(t, "commandStatus", recorder)
			if _, ok := envelope.Error.Details["retryAfterSeconds"]; ok {
				t.Errorf("status error carried a retry hint: %+v", envelope.Error.Details)
			}
			if test.retryable != nil {
				if got, ok := envelope.Error.Details["retryable"].(bool); !ok || got != *test.retryable {
					t.Errorf("retryable = %v, want %v", envelope.Error.Details["retryable"], *test.retryable)
				}
			}
			if test.state != "" {
				if got := envelope.Error.Details["state"]; got != test.state {
					t.Errorf("state = %v, want %q", got, test.state)
				}
			}
			if _, get := client.counts(); get != test.getCalls {
				t.Errorf("GetProcess calls = %d, want %d", get, test.getCalls)
			}
		})
	}
}

func TestCommandStatusFailedRecordIsNotFound(t *testing.T) {
	store := newProcessTestStore()
	failed := processTestRecord(7, db.CommandFailed)
	finished := time.Now().UTC()
	failed.FinishedAt = &finished
	store.seed(failed)
	client := &processTestClient{}
	handler := processTestStatusHandler(t, store, client)
	recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", "")
	envelope := commandTestDecodeError(t, recorder)
	if recorder.Code != http.StatusNotFound || envelope.Code != "not_found" {
		t.Fatalf("error = %d %q", recorder.Code, envelope.Code)
	}
	if _, get := client.counts(); get != 0 {
		t.Errorf("failed record reached GetProcess: %d calls", get)
	}
}

func TestCommandStatusMalformedOutput(t *testing.T) {
	tests := []struct {
		name  string
		query string
		build func() *nodev1.ProcessOutput
	}{
		{"nil output", "", func() *nodev1.ProcessOutput { return nil }},
		{"nil process", "", func() *nodev1.ProcessOutput { return &nodev1.ProcessOutput{} }},
		{"wrong id", "", func() *nodev1.ProcessOutput {
			process := processTestRunningProcess(7, "cmd")
			process.ProcessId = 99
			return &nodev1.ProcessOutput{Process: process}
		}},
		{"negative offset", "", func() *nodev1.ProcessOutput {
			return &nodev1.ProcessOutput{Process: processTestRunningProcess(7, "cmd"), StdoutOffset: -1}
		}},
		{"window over limit", "tailBytes=2", func() *nodev1.ProcessOutput {
			process := processTestRunningProcess(7, "cmd")
			process.StdoutSize = 10
			return &nodev1.ProcessOutput{Process: process, Stdout: []byte("abc"), StdoutOffset: 8}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newProcessTestStore()
			store.seed(processTestRecord(7, db.CommandRunning))
			client := processTestOutputClient(nil, test.build())
			handler := processTestStatusHandler(t, store, client)
			recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", test.query)
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != http.StatusBadGateway || envelope.Code != "sandbox_direct_failed" {
				t.Fatalf("error = %d %q, want 502", recorder.Code, envelope.Code)
			}
			if _, observe, _ := store.callCounts(); observe != 0 {
				t.Errorf("malformed output was observed %d times", observe)
			}
		})
	}
}

func TestCommandStatusObservationFault(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(7, db.CommandRunning))
	store.observeErr = errors.New("private")
	process := processTestRunningProcess(7, "cmd")
	process.StdoutSize = 0
	process.StderrSize = 0
	client := processTestOutputClient(process, processTestStatusOutput(process, nil, nil, 0, 0))
	handler := processTestStatusHandler(t, store, client)
	recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", "")
	envelope := commandTestDecodeError(t, recorder)
	if recorder.Code != http.StatusInternalServerError || envelope.Code != "http_500" {
		t.Fatalf("error = %d %q", recorder.Code, envelope.Code)
	}
	processTestValidate(t, "commandStatus", recorder)
	if _, get := client.counts(); get != 1 {
		t.Errorf("GetProcess calls = %d", get)
	}
}

func TestCommandStatusPathValidation(t *testing.T) {
	for _, processID := range []string{"0", "-1", "abc", "4294967296"} {
		t.Run(processID, func(t *testing.T) {
			store := newProcessTestStore()
			client := &processTestClient{}
			handler := processTestStatusHandler(t, store, client)
			recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, processID, "")
			envelope := commandTestDecodeError(t, recorder)
			if recorder.Code != http.StatusNotFound || envelope.Code != "not_found" {
				t.Fatalf("error = %d %q", recorder.Code, envelope.Code)
			}
			if _, get := client.counts(); get != 0 {
				t.Errorf("bad process id reached GetProcess: %d calls", get)
			}
		})
	}
}

func TestCommandStatusTenantIsolation(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(7, db.CommandRunning))
	client := &processTestClient{}
	handler := processTestStatusHandler(t, store, client, func(options *api.CommandsOptions) {
		options.Lookup = &commandTestLookup{err: api.ErrCommandTargetNotFound}
	})
	recorder := processTestServeStatus(t, handler, processTestOwner, processTestSandboxID, "7", "")
	envelope := commandTestDecodeError(t, recorder)
	if recorder.Code != http.StatusNotFound || envelope.Code != "not_found" {
		t.Fatalf("error = %d %q", recorder.Code, envelope.Code)
	}
	if _, _, record := store.callCounts(); record != 0 {
		t.Errorf("foreign lookup read the command store: %d calls", record)
	}
	if _, get := client.counts(); get != 0 {
		t.Errorf("foreign lookup reached GetProcess: %d calls", get)
	}
}

func TestCommandStatusUnauthenticated(t *testing.T) {
	store := newProcessTestStore()
	handler := processTestStatusHandler(t, store, &processTestClient{})
	recorder := processTestServeStatus(t, handler, "", processTestSandboxID, "7", "")
	envelope := commandTestDecodeError(t, recorder)
	if recorder.Code != http.StatusUnauthorized || envelope.Code != "unauthorized" {
		t.Fatalf("error = %d %q", recorder.Code, envelope.Code)
	}
}

func TestCommandStatusCancellationAfterObserve(t *testing.T) {
	store := newProcessTestStore()
	store.seed(processTestRecord(7, db.CommandRunning))
	ctx, cancel := context.WithCancel(t.Context())
	store.observeHook = cancel
	process := processTestRunningProcess(7, "cmd")
	process.StdoutSize = 0
	process.StderrSize = 0
	client := processTestOutputClient(process, processTestStatusOutput(process, nil, nil, 0, 0))
	handler := processTestStatusHandler(t, store, client)
	mux := http.NewServeMux()
	mux.Handle("GET "+api.ProcessStatusPath, handler)
	request := httptest.NewRequest(http.MethodGet,
		"/api/v1/sandboxes/"+processTestSandboxID+"/commands/7", nil).WithContext(ctx)
	request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: processTestOwner, KeyID: "key_test"}))
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Body.Len() != 0 {
		t.Errorf("cancelled status wrote a response: %s", recorder.Body.Bytes())
	}
}
