package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

// Fixture modes for the info endpoint.
const (
	execLongInfoLegacy    = "legacy"
	execLongInfoDetached  = "detached"
	execLongInfoFalse     = "false"
	execLongInfoNull      = "null"
	execLongInfoString    = "string"
	execLongInfoError     = "error"
	execLongInfoMalformed = "malformed"
	execLongInfoMissing   = "missing"
)

// execLongFixture is a scripted v1 API for long-exec, detach, status and kill.
type execLongFixture struct {
	t        *testing.T
	infoMode string
	exitCode int

	mu            sync.Mutex
	posts         []map[string]any
	attachQueries []execLongURLValues
	attachScripts []execAttachTestScript
	deletes       int
	infos         int
	started       chan struct{}
	startedOnce   sync.Once
	blockAttach   bool
	deleteStatus  int
	deleteBody    string
	legacyBody    string
	statusBody    map[string]any
}

// execLongURLValues is a copy of url.Values so the fixture stays import-light.
type execLongURLValues map[string][]string

// get returns the first value of name.
func (v execLongURLValues) get(name string) string {
	if values, ok := v[name]; ok && len(values) > 0 {
		return values[0]
	}
	return ""
}

// newExecLongFixture builds a fixture with defaults: exit 7, an info endpoint
// without a detached member.
func newExecLongFixture(t *testing.T) *execLongFixture {
	t.Helper()
	return &execLongFixture{
		t:        t,
		infoMode: execLongInfoLegacy,
		exitCode: 7,
		started:  make(chan struct{}),
	}
}

// serveHTTP serves the CLI's v1 API subset.
func (f *execLongFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.EscapedPath()
	switch {
	case path == execInfoPath:
		f.serveInfo(w)
	case r.Method == http.MethodPost && path == "/api/v1/sandboxes/bx_23456789/commands":
		f.serveCommand(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(path, "/stream"):
		f.serveAttach(w, r)
	case r.Method == http.MethodDelete && strings.Contains(path, "/commands/"):
		f.serveDelete(w)
	case r.Method == http.MethodGet && strings.Contains(path, "/commands/"):
		f.serveStatus(w, r)
	default:
		f.t.Errorf("unexpected request %s %s", r.Method, path)
		http.NotFound(w, r)
	}
}

// serveInfo writes the configured capability answer.
func (f *execLongFixture) serveInfo(w http.ResponseWriter) {
	f.mu.Lock()
	f.infos++
	f.mu.Unlock()
	switch f.infoMode {
	case execLongInfoMissing:
		cliTestWriteJSON(f.t, w, http.StatusNotFound,
			cliTestErrorEnvelope(http.StatusNotFound, "not_found", "missing"))
		return
	case execLongInfoError:
		cliTestWriteJSON(f.t, w, http.StatusInternalServerError,
			cliTestErrorEnvelope(http.StatusInternalServerError, "http_500", "failed"))
		return
	case execLongInfoMalformed:
		cliTestWriteJSON(f.t, w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	commands := map[string]any{"streamMaxSeconds": 7200, "syncMaxSeconds": 600}
	switch f.infoMode {
	case execLongInfoDetached:
		commands["detached"] = true
	case execLongInfoFalse:
		commands["detached"] = false
	case execLongInfoNull:
		commands["detached"] = nil
	case execLongInfoString:
		commands["detached"] = "yes"
	}
	cliTestWriteJSON(f.t, w, http.StatusOK, map[string]any{
		"ok": true, "type": execInfoType, "version": "test",
		"commands": commands,
	})
}

// serveCommand answers the command POST in either legacy stream or detached
// mode, decided by the request body exactly like the server.
func (f *execLongFixture) serveCommand(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		f.t.Errorf("read command body: %v", err)
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		f.t.Errorf("decode command body: %v", err)
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.posts = append(f.posts, body)
	f.mu.Unlock()
	if body["detached"] == true {
		command, _ := body["command"].(string)
		cliTestWriteJSON(f.t, w, http.StatusOK, map[string]any{
			"ok": true, "type": "command.started", "success": true,
			"commandId": "cmd_test", "processId": 7, "pid": 123,
			"command": command, "startedAt": "2026-01-02T03:04:05Z",
		})
		return
	}
	if body["stream"] != true {
		cliTestWriteJSON(f.t, w, http.StatusOK,
			api.NewCommandResponse(f.exitCode == 0, new(f.exitCode), "", "", false))
		return
	}
	if f.legacyBody != "" {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, f.legacyBody)
		return
	}
	frames := execAttachTestFrame(f.t, map[string]any{"type": "started", "commandId": "cmd_legacy"}) +
		execAttachTestFrame(f.t, map[string]any{"type": "stdout", "data": "out\n"}) +
		execAttachTestFrame(f.t, map[string]any{"type": "stderr", "data": "err\n"}) +
		execAttachTestFrame(f.t, map[string]any{
			"type": "exit", "exitCode": f.exitCode, "success": f.exitCode == 0, "timedOut": false,
		})
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, frames)
}

// serveAttach streams one scripted attach answer.
func (f *execLongFixture) serveAttach(w http.ResponseWriter, r *http.Request) {
	query := execLongURLValues{}
	for name, values := range r.URL.Query() {
		query[name] = values
	}
	f.mu.Lock()
	f.attachQueries = append(f.attachQueries, query)
	var script execAttachTestScript
	if len(f.attachScripts) > 0 {
		script = f.attachScripts[0]
		f.attachScripts = f.attachScripts[1:]
	} else {
		script = execAttachTestScript{}
	}
	block := f.blockAttach
	f.mu.Unlock()
	f.startedOnce.Do(func() { close(f.started) })
	if script.body == "" {
		script.body = f.defaultAttachBody()
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, script.body)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	if block {
		<-r.Context().Done()
	}
}

// defaultAttachBody is the canonical successful attach stream.
func (f *execLongFixture) defaultAttachBody() string {
	return execAttachTestFrame(f.t, map[string]any{
		"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
	}) +
		execAttachTestFrame(f.t, map[string]any{"type": "stdout", "data": "out\n", "nextOffset": 4}) +
		execAttachTestFrame(f.t, map[string]any{"type": "stderr", "data": "err\n", "nextOffset": 4}) +
		execAttachTestFrame(f.t, map[string]any{
			"type": "exit", "status": "exited", "exitCode": f.exitCode, "signal": nil,
			"stdoutOffset": 4, "stderrOffset": 4,
		})
}

// serveDelete answers the stop request.
func (f *execLongFixture) serveDelete(w http.ResponseWriter) {
	f.mu.Lock()
	f.deletes++
	status := f.deleteStatus
	body := f.deleteBody
	f.mu.Unlock()
	if status != 0 && status != http.StatusOK {
		cliTestWriteJSON(f.t, w, status, cliTestErrorEnvelope(status, "http_500", "failed"))
		return
	}
	if body != "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, body)
		return
	}
	now := "2026-01-02T03:04:06Z"
	cliTestWriteJSON(f.t, w, http.StatusOK, map[string]any{
		"ok": true, "type": "command.stopped", "processId": 7,
		"running": false, "signaled": true,
		"command": map[string]any{
			"commandId": "cmd_test", "sandboxId": "bx_23456789", "command": "true",
			"cwd": nil, "mode": "detached", "processId": 7, "status": "killed",
			"exitCode": nil, "signal": "SIGTERM", "stdoutBytes": 0, "stderrBytes": 0,
			"outputTruncated": false, "startedAt": "2026-01-02T03:04:05Z", "finishedAt": now,
		},
	})
}

// serveStatus answers a status request.
func (f *execLongFixture) serveStatus(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	query := execLongURLValues{}
	for name, values := range r.URL.Query() {
		query[name] = values
	}
	f.attachQueries = append(f.attachQueries, query)
	body := f.statusBody
	f.mu.Unlock()
	if body == nil {
		body = map[string]any{
			"ok": true, "type": "command.status", "success": f.exitCode == 0, "processId": 7,
			"status": "exited", "running": false, "exitCode": f.exitCode,
			"stdout": "out\n", "stderr": "err\n",
		}
	}
	cliTestWriteJSON(f.t, w, http.StatusOK, body)
}

// postCount returns the recorded command POSTs.
func (f *execLongFixture) postCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.posts)
}

// post returns one recorded POST body.
func (f *execLongFixture) post(t *testing.T, index int) map[string]any {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if index >= len(f.posts) {
		t.Fatalf("post %d missing; %d recorded", index, len(f.posts))
	}
	return f.posts[index]
}

// deleteCount returns the stop request count.
func (f *execLongFixture) deleteCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deletes
}

// query returns one recorded attach query.
func (f *execLongFixture) query(t *testing.T, index int) execLongURLValues {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if index >= len(f.attachQueries) {
		t.Fatalf("attach query %d missing; %d recorded", index, len(f.attachQueries))
	}
	return f.attachQueries[index]
}

// attachCount returns the number of attach or status requests.
func (f *execLongFixture) attachCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.attachQueries)
}

// infoCount returns the metadata request count.
func (f *execLongFixture) infoCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.infos
}

// execLongNewEnv builds the CLI environment for a fixture.
func execLongNewEnv(t *testing.T, fixture *execLongFixture, human bool) *cliTestEnv {
	t.Helper()
	return cliTestNew(t, fixture.serveHTTP, func(opts *Options) {
		if human {
			opts.IsTTY = func(io.Writer) bool { return true }
		}
	})
}

// execLongJSONLines decodes the JSON Lines written by the CLI.
func execLongJSONLines(t *testing.T, out *bytes.Buffer) []map[string]any {
	t.Helper()
	return cliTestJSONLines(t, out)
}

// execLongFindFrame returns the first JSON line of the given type.
func execLongFindFrame(t *testing.T, lines []map[string]any, frameType string) map[string]any {
	t.Helper()
	for _, line := range lines {
		if line["type"] == frameType {
			return line
		}
	}
	t.Fatalf("no %q frame in %v", frameType, lines)
	return nil
}

// TestExecLongDogfoodOldAndNew proves the exact dogfood invocation keeps its
// frames and exit code against both server generations.
func TestExecLongDogfoodOldAndNew(t *testing.T) {
	const script = "echo out; echo err >&2; exit 7"
	args := []string{"exec", cliTestSandboxID, "--timeout", "0", "--stream", "--json", "--", "bash", "-lc", script}

	t.Run("old server", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		env := execLongNewEnv(t, fixture, false)
		if got := env.run(t, args...); got != 7 {
			t.Fatalf("exit status = %d, want 7; stderr=%q", got, env.stderr.String())
		}
		if fixture.postCount() != 1 {
			t.Fatalf("POSTs = %d, want 1", fixture.postCount())
		}
		if fixture.infoCount() != 1 {
			t.Fatalf("metadata requests = %d, want 1", fixture.infoCount())
		}
		body := fixture.post(t, 0)
		if body["detached"] != nil {
			t.Error("legacy server received a detached request")
		}
		if body["stream"] != true || body["timeoutSeconds"] != float64(7200) {
			t.Errorf("legacy body = %#v", body)
		}
		if fixture.attachCount() != 0 {
			t.Errorf("legacy server received %d attach calls", fixture.attachCount())
		}
		execLongAssertDogfoodFrames(t, env.stdout, 7)
	})

	t.Run("new server", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.infoMode = execLongInfoDetached
		fixture.attachScripts = []execAttachTestScript{
			{body: execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0}) +
				execAttachTestFrame(t, map[string]any{"type": "stdout", "data": "out\n", "nextOffset": 4}) +
				execAttachTestFrame(t, map[string]any{"type": "reconnect", "stdoutOffset": 4, "stderrOffset": 0})},
			{body: execAttachTestFrame(t, map[string]any{"type": "attached", "processId": 7, "stdoutOffset": 4, "stderrOffset": 0}) +
				execAttachTestFrame(t, map[string]any{"type": "stderr", "data": "err\n", "nextOffset": 4}) +
				execAttachTestFrame(t, map[string]any{"type": "exit", "status": "exited", "exitCode": 7, "signal": nil, "stdoutOffset": 4, "stderrOffset": 4})},
		}
		env := execLongNewEnv(t, fixture, false)
		if got := env.run(t, args...); got != 7 {
			t.Fatalf("exit status = %d, want 7; stderr=%q", got, env.stderr.String())
		}
		if fixture.postCount() != 1 {
			t.Fatalf("POSTs = %d, want 1", fixture.postCount())
		}
		if fixture.infoCount() != 1 {
			t.Fatalf("metadata requests = %d, want 1", fixture.infoCount())
		}
		body := fixture.post(t, 0)
		if body["detached"] != true {
			t.Errorf("detached body = %#v", body)
		}
		for _, absent := range []string{"timeoutSeconds", "stream"} {
			if _, ok := body[absent]; ok {
				t.Errorf("detached start sent %q", absent)
			}
		}
		wantCommand := "'bash' '-lc' '" + script + "'"
		if body["command"] != wantCommand {
			t.Errorf("command = %q, want %q", body["command"], wantCommand)
		}
		if fixture.attachCount() != 2 {
			t.Fatalf("attach calls = %d, want 2", fixture.attachCount())
		}
		if got := fixture.query(t, 1).get("stdoutOffset"); got != "4" {
			t.Errorf("second stdoutOffset = %q", got)
		}
		execLongAssertDogfoodFrames(t, env.stdout, 7)
	})
}

// execLongAssertDogfoodFrames checks the stdout/stderr and terminal exit lines that the
// dogfood parser reads. A started line is allowed; attach-only frames are not.
// The whole stream must also validate against the pinned command operation.
func execLongAssertDogfoodFrames(t *testing.T, stdout *bytes.Buffer, code int) {
	t.Helper()
	validator := compat.Load(t)
	header := http.Header{"Content-Type": []string{compat.NDJSONMediaType}}
	if err := validator.NDJSON("command", http.StatusOK, header, stdout.Bytes()); err != nil {
		t.Errorf("emitted frames do not match the pin: %v", err)
	}
	lines := execLongJSONLines(t, stdout)
	stdoutFrame := execLongFindFrame(t, lines, "stdout")
	if stdoutFrame["data"] != "out\n" {
		t.Errorf("stdout frame = %#v", stdoutFrame)
	}
	stderrFrame := execLongFindFrame(t, lines, "stderr")
	if stderrFrame["data"] != "err\n" {
		t.Errorf("stderr frame = %#v", stderrFrame)
	}
	exitFrame := execLongFindFrame(t, lines, "exit")
	if exitFrame["exitCode"] != float64(code) || exitFrame["success"] != (code == 0) || exitFrame["timedOut"] != false {
		t.Errorf("exit frame = %#v", exitFrame)
	}
	for _, line := range lines {
		switch line["type"] {
		case "started", "stdout", "stderr", "exit":
		default:
			t.Errorf("legacy-incompatible frame %#v", line)
		}
	}
}

// TestExecLongPositiveTimeoutOldAndNew covers the above-600 migration.
func TestExecLongPositiveTimeoutOldAndNew(t *testing.T) {
	cases := []struct {
		name   string
		info   string
		exit   int
		want   map[string]any
		absent []string
	}{
		{
			name: "old server",
			info: execLongInfoLegacy, exit: 5,
			want:   map[string]any{"stream": true, "timeoutSeconds": float64(1800)},
			absent: []string{"detached"},
		},
		{
			name: "new server",
			info: execLongInfoDetached, exit: 5,
			want:   map[string]any{"detached": true},
			absent: []string{"stream", "timeoutSeconds"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newExecLongFixture(t)
			fixture.infoMode = tc.info
			fixture.exitCode = tc.exit
			env := execLongNewEnv(t, fixture, false)
			if got := env.run(t, "exec", cliTestSandboxID, "--timeout", "1800", "--json", "true"); got != tc.exit {
				t.Fatalf("exit status = %d, want %d; stderr=%q", got, tc.exit, env.stderr.String())
			}
			body := fixture.post(t, 0)
			for key, value := range tc.want {
				if body[key] != value {
					t.Errorf("%s = %#v, want %#v", key, body[key], value)
				}
			}
			for _, key := range tc.absent {
				if _, ok := body[key]; ok {
					t.Errorf("request sent %q", key)
				}
			}
		})
	}
}

// TestExecLongCapabilityStrict proves only a valid true selects detached and
// every other non-legacy answer stops before the command POST.
func TestExecLongCapabilityStrict(t *testing.T) {
	for _, timeout := range []string{"0", "1800"} {
		t.Run("timeout "+timeout, func(t *testing.T) {
			modes := []string{
				execLongInfoNull, execLongInfoString, execLongInfoError,
				execLongInfoMalformed,
			}
			for _, mode := range modes {
				t.Run(mode, func(t *testing.T) {
					fixture := newExecLongFixture(t)
					fixture.infoMode = mode
					env := execLongNewEnv(t, fixture, false)
					if got := env.run(t, "exec", cliTestSandboxID, "--timeout", timeout, "--json", "true"); got != 1 {
						t.Fatalf("exit status = %d, want 1; stderr=%q", got, env.stderr.String())
					}
					if fixture.postCount() != 0 {
						t.Fatalf("POSTs = %d, want 0", fixture.postCount())
					}
				})
			}
			t.Run("false is legacy", func(t *testing.T) {
				fixture := newExecLongFixture(t)
				fixture.infoMode = execLongInfoFalse
				env := execLongNewEnv(t, fixture, false)
				if got := env.run(t, "exec", cliTestSandboxID, "--timeout", timeout, "--json", "true"); got != 7 {
					t.Fatalf("exit status = %d, want 7; stderr=%q", got, env.stderr.String())
				}
				body := fixture.post(t, 0)
				if body["detached"] != nil {
					t.Errorf("non-true capability selected detached")
				}
				if timeout == "0" && body["timeoutSeconds"] != float64(7200) {
					t.Errorf("legacy zero-timeout cap = %#v, want 7200", body["timeoutSeconds"])
				}
			})
			t.Run("missing endpoint keeps the old path", func(t *testing.T) {
				fixture := newExecLongFixture(t)
				fixture.infoMode = execLongInfoMissing
				env := execLongNewEnv(t, fixture, false)
				if got := env.run(t, "exec", cliTestSandboxID, "--timeout", timeout, "--json", "true"); got != 7 {
					t.Fatalf("exit status = %d, want 7; stderr=%q", got, env.stderr.String())
				}
				body := fixture.post(t, 0)
				if timeout == "0" {
					if body["timeoutSeconds"] != float64(600) {
						t.Errorf("fallback timeout = %#v", body["timeoutSeconds"])
					}
					if !strings.Contains(env.stderr.String(), "600-second stream timeout") {
						t.Errorf("missing fallback note: %q", env.stderr.String())
					}
				} else if body["timeoutSeconds"] != float64(1800) {
					t.Errorf("timeout = %#v", body["timeoutSeconds"])
				}
			})
		})
	}
}

// TestExecLongTimeoutKillsOnce proves the local watchdog stops the known
// process exactly once and exits 124.
func TestExecLongTimeoutKillsOnce(t *testing.T) {
	fixture := newExecLongFixture(t)
	fixture.infoMode = execLongInfoDetached
	fixture.blockAttach = true
	fixture.attachScripts = []execAttachTestScript{{body: execAttachTestFrame(t, map[string]any{
		"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
	})}}
	server := newExecLongServer(t, fixture)
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	result, err := execRunDetachedLong(t.Context(), client, ExecutionRequest{
		SandboxID: cliTestSandboxID, Command: "sleep 100",
	}, ExecutionOutput{JSON: &stdout}, 20*time.Millisecond, ExecutionResult{})
	var exit *ExitError
	if !errors.As(err, &exit) || exit.Code != 124 {
		t.Fatalf("err = %v, want exit 124", err)
	}
	if !result.TimedOut || result.ExitCode != 124 {
		t.Fatalf("result = %+v", result)
	}
	if !result.Completed {
		t.Fatalf("confirmed stop did not mark the result completed: %+v", result)
	}
	if fixture.deleteCount() != 1 {
		t.Fatalf("DELETE count = %d, want 1", fixture.deleteCount())
	}
	lines := execLongJSONLines(t, &stdout)
	exitFrame := execLongFindFrame(t, lines, "exit")
	if exitFrame["timedOut"] != true || exitFrame["exitCode"] != nil {
		t.Fatalf("timeout frame = %#v", exitFrame)
	}
}

// TestExecLongTimeoutKillFailureStill124 proves a failed cleanup does not
// change the timeout status.
func TestExecLongTimeoutKillFailureStill124(t *testing.T) {
	fixture := newExecLongFixture(t)
	fixture.infoMode = execLongInfoDetached
	fixture.blockAttach = true
	fixture.deleteStatus = http.StatusInternalServerError
	fixture.attachScripts = []execAttachTestScript{{body: execAttachTestFrame(t, map[string]any{
		"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
	})}}
	server := newExecLongServer(t, fixture)
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	result, err := execRunDetachedLong(t.Context(), client, ExecutionRequest{
		SandboxID: cliTestSandboxID, Command: "sleep 100",
	}, ExecutionOutput{Stderr: &stderr}, 20*time.Millisecond, ExecutionResult{})
	var exit *ExitError
	if !errors.As(err, &exit) || exit.Code != 124 {
		t.Fatalf("err = %v, want exit 124", err)
	}
	if result.Completed {
		t.Fatalf("failed stop marked the result completed: %+v", result)
	}
	if !strings.Contains(stderr.String(), "stopping it failed") {
		t.Fatalf("missing cleanup warning: %q", stderr.String())
	}
}

// TestExecLongCancelKillsOnce proves Ctrl-C stops the known process with an
// independent context and exits 1.
func TestExecLongCancelKillsOnce(t *testing.T) {
	fixture := newExecLongFixture(t)
	fixture.infoMode = execLongInfoDetached
	fixture.blockAttach = true
	fixture.attachScripts = []execAttachTestScript{{body: execAttachTestFrame(t, map[string]any{
		"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
	})}}
	env := execLongNewEnv(t, fixture, false)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	var code int
	runDone := make(chan struct{})
	go func() {
		code = env.runContext(ctx, "exec", cliTestSandboxID, "--timeout", "0", "--stream", "--json", "true")
		close(runDone)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(2 * time.Second):
			t.Error("CLI did not stop during cleanup")
		}
	})
	select {
	case <-fixture.started:
	case <-time.After(5 * time.Second):
		t.Fatal("command never reached the blocking handler")
	}
	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("CLI did not stop after cancellation")
	}
	if code != 1 {
		t.Fatalf("exit status = %d, want 1; stderr=%q", code, env.stderr.String())
	}
	if fixture.deleteCount() != 1 {
		t.Fatalf("DELETE count = %d, want 1", fixture.deleteCount())
	}
}

// execLongCancelWriter cancels a context on its nth write and then succeeds.
type execLongCancelWriter struct {
	cancel func()
	at     int
	calls  int
	buf    bytes.Buffer
}

// Write implements io.Writer.
func (w *execLongCancelWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.at {
		w.cancel()
	}
	return w.buf.Write(p)
}

// TestExecLongCancelDuringTimeoutStop proves a foreground cancellation that
// arrives while the independent timeout stop runs wins over the timeout
// result: exactly one stop, no terminal frame, not completed and no 124.
func TestExecLongCancelDuringTimeoutStop(t *testing.T) {
	fixture := newExecLongFixture(t)
	fixture.blockAttach = true
	fixture.attachScripts = []execAttachTestScript{{body: execAttachTestFrame(t, map[string]any{
		"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
	})}}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			cancel()
		}
		fixture.serveHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	result, err := execRunDetachedLong(ctx, client, ExecutionRequest{
		SandboxID: cliTestSandboxID, Command: "true",
	}, ExecutionOutput{JSON: &out}, 20*time.Millisecond, ExecutionResult{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("parent cancellation lost to timeout cleanup: err=%v", err)
	}
	if result.Completed {
		t.Fatal("cancelled final cleanup marked completed")
	}
	if result.ExitCode == 124 || result.TimedOut {
		t.Fatalf("cancelled timeout reported as 124: %+v", result)
	}
	for _, frame := range cliTestJSONLines(t, &out) {
		if frame["type"] == "exit" {
			t.Fatal("terminal output emitted after cancellation")
		}
	}
	if fixture.deleteCount() != 1 {
		t.Fatalf("stop calls = %d, want 1", fixture.deleteCount())
	}
}

// TestExecLongCancelOnTimeoutFrameWrite proves a cancellation arriving while
// the timeout frame is rendered still wins: the result is not completed and no
// 124 is published, without a second stop.
func TestExecLongCancelOnTimeoutFrameWrite(t *testing.T) {
	fixture := newExecLongFixture(t)
	fixture.blockAttach = true
	fixture.attachScripts = []execAttachTestScript{{body: execAttachTestFrame(t, map[string]any{
		"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
	})}}
	server := newExecLongServer(t, fixture)
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	// Write 1 is the started frame; the timeout frame is write 2.
	writer := &execLongCancelWriter{cancel: cancel, at: 2}
	result, err := execRunDetachedLong(ctx, client, ExecutionRequest{
		SandboxID: cliTestSandboxID, Command: "true",
	}, ExecutionOutput{JSON: writer}, 20*time.Millisecond, ExecutionResult{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("write-time cancellation lost: err=%v", err)
	}
	if result.Completed {
		t.Fatal("write-time cancellation marked completed")
	}
	if result.ExitCode == 124 || result.TimedOut {
		t.Fatalf("write-time cancellation reported as 124: %+v", result)
	}
	if fixture.deleteCount() != 1 {
		t.Fatalf("stop calls = %d, want 1", fixture.deleteCount())
	}
}

// TestExecLongWriteFailureKillsOnce proves a broken local writer stops the
// known remote process and reports the safe write error.
func TestExecLongWriteFailureKillsOnce(t *testing.T) {
	fixture := newExecLongFixture(t)
	fixture.infoMode = execLongInfoDetached
	server := newExecLongServer(t, fixture)
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = execRunDetachedLong(t.Context(), client, ExecutionRequest{
		SandboxID: cliTestSandboxID, Command: "echo hi",
	}, ExecutionOutput{JSON: execAttachFailWriter{}}, 0, ExecutionResult{})
	if !execIsWriteFailure(err) {
		t.Fatalf("err = %v, want write_failed", err)
	}
	if fixture.deleteCount() != 1 {
		t.Fatalf("DELETE count = %d, want 1", fixture.deleteCount())
	}
}

// TestExecAttachFrameLimit refuses a frame above the bounded reader limit.
func TestExecAttachFrameLimit(t *testing.T) {
	reader := bufio.NewReaderSize(strings.NewReader(strings.Repeat("x", 64)+"\n"), 1<<10)
	if _, _, err := execReadCompleteNDJSONLine(reader, 32); !errors.Is(err, errExecFrameTooLarge) {
		t.Fatalf("err = %v, want frame-too-large", err)
	}
}

// TestExecAttachModeCancelLeavesProcess proves the explicit viewer never stops
// the remote command and exits 130.
func TestExecAttachModeCancelLeavesProcess(t *testing.T) {
	fixture := newExecLongFixture(t)
	fixture.infoMode = execLongInfoDetached
	fixture.blockAttach = true
	fixture.attachScripts = []execAttachTestScript{{body: execAttachTestFrame(t, map[string]any{
		"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
	})}}
	env := execLongNewEnv(t, fixture, false)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	var code int
	runDone := make(chan struct{})
	go func() {
		code = env.runContext(ctx, "exec", cliTestSandboxID, "--attach", "7")
		close(runDone)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(2 * time.Second):
			t.Error("attach viewer did not stop during cleanup")
		}
	})
	select {
	case <-fixture.started:
	case <-time.After(5 * time.Second):
		t.Fatal("attach never reached the blocking handler")
	}
	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("attach viewer did not stop after cancellation")
	}
	if code != 130 {
		t.Fatalf("exit status = %d, want 130; stderr=%q", code, env.stderr.String())
	}
	if fixture.deleteCount() != 0 {
		t.Fatalf("DELETE count = %d, want 0", fixture.deleteCount())
	}
}

// TestExecAttachModeExit covers explicit attach output and the remote status.
func TestExecAttachModeExit(t *testing.T) {
	fixture := newExecLongFixture(t)
	fixture.infoMode = execLongInfoDetached
	fixture.exitCode = 3
	env := execLongNewEnv(t, fixture, false)
	if got := env.run(t, "exec", cliTestSandboxID, "--attach", "7", "--json"); got != 3 {
		t.Fatalf("exit status = %d, want 3; stderr=%q", got, env.stderr.String())
	}
	lines := execLongJSONLines(t, env.stdout)
	for _, line := range lines {
		if line["type"] == "started" {
			t.Fatalf("explicit attach emitted a started frame")
		}
	}
	if execLongFindFrame(t, lines, "stdout")["data"] != "out\n" {
		t.Errorf("stdout frame missing")
	}
	if execLongFindFrame(t, lines, "exit")["exitCode"] != float64(3) {
		t.Errorf("exit frame = %#v", execLongFindFrame(t, lines, "exit"))
	}
}

// TestExecDetachMode covers the detached start output and request.
func TestExecDetachMode(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		env := execLongNewEnv(t, fixture, false)
		if got := env.run(t, "exec", cliTestSandboxID, "--detach", "echo hi"); got != 0 {
			t.Fatalf("exit status = %d; stderr=%q", got, env.stderr.String())
		}
		lines := execLongJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["type"] != "command.started" || lines[0]["processId"] != float64(7) {
			t.Fatalf("lines = %v", lines)
		}
	})
	t.Run("human", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		env := execLongNewEnv(t, fixture, true)
		if got := env.run(t, "exec", cliTestSandboxID, "--detach", "echo hi"); got != 0 {
			t.Fatalf("exit status = %d; stderr=%q", got, env.stderr.String())
		}
		if got := env.stdout.String(); got != "process 7 started\n" {
			t.Fatalf("stdout = %q", got)
		}
	})
	t.Run("detached wins over stream and timeout", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		env := execLongNewEnv(t, fixture, false)
		if got := env.run(t, "exec", cliTestSandboxID, "--detach", "--stream", "--timeout", "0", "--cwd", "src", "echo hi"); got != 0 {
			t.Fatalf("exit status = %d; stderr=%q", got, env.stderr.String())
		}
		body := fixture.post(t, 0)
		if body["detached"] != true {
			t.Errorf("body = %#v", body)
		}
		if body["cwd"] != "src" {
			t.Errorf("cwd = %#v, want src", body["cwd"])
		}
		for _, absent := range []string{"stream", "timeoutSeconds"} {
			if _, ok := body[absent]; ok {
				t.Errorf("detached start sent %q", absent)
			}
		}
	})
	t.Run("definite refusal is not ambiguous", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		var posts int
		handler := func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				posts++
				cliTestWriteJSON(t, w, http.StatusConflict,
					cliTestErrorEnvelope(http.StatusConflict, "sandbox_not_ready", "not ready"))
				return
			}
			fixture.serveHTTP(w, r)
		}
		env := cliTestNew(t, handler, nil)
		if got := env.run(t, "exec", cliTestSandboxID, "--detach", "true"); got != 1 {
			t.Fatalf("exit status = %d, want 1", got)
		}
		if posts != 1 {
			t.Fatalf("POST count = %d, want 1", posts)
		}
		if !strings.Contains(env.stdout.String(), "sandbox_not_ready") ||
			strings.Contains(env.stdout.String(), "command_ambiguous") {
			t.Fatalf("stdout = %q", env.stdout.String())
		}
	})
	t.Run("enveloped 502 is ambiguous", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		handler := func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				cliTestWriteJSON(t, w, http.StatusBadGateway,
					cliTestErrorEnvelope(http.StatusBadGateway, "sandbox_direct_failed", "failed"))
				return
			}
			fixture.serveHTTP(w, r)
		}
		env := cliTestNew(t, handler, nil)
		if got := env.run(t, "exec", cliTestSandboxID, "--detach", "true"); got != 1 {
			t.Fatalf("exit status = %d, want 1", got)
		}
		if !strings.Contains(env.stdout.String(), "command_ambiguous") {
			t.Fatalf("stdout = %q", env.stdout.String())
		}
	})
	t.Run("bare 502 is ambiguous", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		var posts int
		handler := func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				posts++
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			fixture.serveHTTP(w, r)
		}
		env := cliTestNew(t, handler, nil)
		if got := env.run(t, "exec", cliTestSandboxID, "--detach", "true"); got != 1 {
			t.Fatalf("exit status = %d, want 1; stderr=%q", got, env.stderr.String())
		}
		if posts != 1 {
			t.Fatalf("POST count = %d, want 1", posts)
		}
		if !strings.Contains(env.stdout.String(), "command_ambiguous") {
			t.Fatalf("stdout = %q", env.stdout.String())
		}
	})
}

// TestExecStatusMode covers both renderings and the tail query.
func TestExecStatusMode(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		env := execLongNewEnv(t, fixture, false)
		if got := env.run(t, "exec", cliTestSandboxID, "--status", "7"); got != 0 {
			t.Fatalf("exit status = %d; stderr=%q", got, env.stderr.String())
		}
		lines := execLongJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["type"] != "command.status" || lines[0]["status"] != "exited" {
			t.Fatalf("lines = %v", lines)
		}
		// A remote non-zero exit is data, not a CLI failure: the status
		// read succeeded even though the process reported success=false.
		if lines[0]["success"] != false || lines[0]["exitCode"] != float64(7) {
			t.Fatalf("status line = %v", lines[0])
		}
		if got := fixture.query(t, 0).get("tailBytes"); got != "" {
			t.Fatalf("JSON status sent tailBytes=%q", got)
		}
	})
	t.Run("human", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		env := execLongNewEnv(t, fixture, true)
		if got := env.run(t, "exec", cliTestSandboxID, "--status", "7"); got != 0 {
			t.Fatalf("exit status = %d; stderr=%q", got, env.stderr.String())
		}
		if got := env.stdout.String(); got != "process 7 exited 7\nout\n" {
			t.Fatalf("stdout = %q", got)
		}
		if got := env.stderr.String(); got != "err\n" {
			t.Fatalf("stderr = %q", got)
		}
		if got := fixture.query(t, 0).get("tailBytes"); got != "65536" {
			t.Fatalf("tailBytes = %q", got)
		}
	})
}

// TestExecKillMode covers both renderings and the idempotent answer.
func TestExecKillMode(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		env := execLongNewEnv(t, fixture, false)
		if got := env.run(t, "exec", cliTestSandboxID, "--kill", "7"); got != 0 {
			t.Fatalf("exit status = %d; stderr=%q", got, env.stderr.String())
		}
		lines := execLongJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["type"] != "command.stopped" || lines[0]["signaled"] != true {
			t.Fatalf("lines = %v", lines)
		}
	})
	t.Run("human already stopped", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.deleteBody = `{"ok":true,"type":"command.stopped","processId":7,"running":false,"signaled":false,"command":null}`
		env := execLongNewEnv(t, fixture, true)
		if got := env.run(t, "exec", cliTestSandboxID, "--kill", "7"); got != 0 {
			t.Fatalf("exit status = %d; stderr=%q", got, env.stderr.String())
		}
		if got := env.stdout.String(); got != "process 7 was not running\n" {
			t.Fatalf("stdout = %q", got)
		}
	})
}

// TestExecModeValidation rejects conflicted invocations before any request.
func TestExecModeValidation(t *testing.T) {
	cases := map[string][]string{
		"zero pid":                {"--status", "0"},
		"negative pid":            {"--status", "-1"},
		"fractional pid":          {"--status", "1.5"},
		"overflow pid":            {"--status", "4294967296"},
		"empty pid":               {"--status", ""},
		"status and kill":         {"--status", "1", "--kill", "2"},
		"status and detach":       {"--status", "1", "--detach", "true"},
		"status and detach false": {"--status", "1", "--detach=false"},
		"status with cwd":         {"--status", "1", "--cwd", "x"},
		"status with timeout":     {"--status", "1", "--timeout", "5"},
		"status with stream":      {"--status", "1", "--stream"},
		"status with command":     {"--status", "1", "true"},
		"kill with command":       {"--kill", "1", "true"},
		"attach with command":     {"--attach", "1", "true"},
		"detach without cmd":      {"--detach"},
	}
	for name, flags := range cases {
		t.Run(name, func(t *testing.T) {
			var calls int
			env := cliTestNew(t, func(http.ResponseWriter, *http.Request) {
				calls++
			}, nil)
			args := append([]string{"exec", cliTestSandboxID}, flags...)
			if got := env.run(t, args...); got != 2 {
				t.Fatalf("exit status = %d, want 2; stderr=%q", got, env.stderr.String())
			}
			if calls != 0 {
				t.Fatalf("server calls = %d, want 0", calls)
			}
		})
	}
}

// TestExecDetachFalseStaysForeground proves --detach=false is the plain
// foreground command: no capability discovery and no detached request.
func TestExecDetachFalseStaysForeground(t *testing.T) {
	fixture := newExecLongFixture(t)
	fixture.exitCode = 0
	env := execLongNewEnv(t, fixture, false)
	if got := env.run(t, "exec", cliTestSandboxID, "--detach=false", "--json", "true"); got != 0 {
		t.Fatalf("exit status = %d, want 0; stderr=%q", got, env.stderr.String())
	}
	if fixture.infoCount() != 0 {
		t.Fatalf("metadata requests = %d, want 0", fixture.infoCount())
	}
	body := fixture.post(t, 0)
	if _, ok := body["detached"]; ok {
		t.Errorf("foreground request sent detached: %#v", body)
	}
	if body["timeoutSeconds"] != float64(30) {
		t.Errorf("timeoutSeconds = %#v, want 30", body["timeoutSeconds"])
	}
	if body["stream"] != false {
		t.Errorf("stream = %#v, want false", body["stream"])
	}
}

// execCancelTransport cancels the caller while it returns a valid 200 answer,
// so the guard after successful I/O is deterministic.
type execCancelTransport struct {
	t            *testing.T
	cancel       context.CancelFunc
	cancelOnPost bool
	deletes      atomic.Int64
}

// RoundTrip implements http.RoundTripper.
func (tr *execCancelTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	path := req.URL.Path
	switch {
	case path == execInfoPath:
		return execCancelResponse(req, "application/json",
			`{"ok":true,"type":"bbraft.info","version":"test","commands":{"streamMaxSeconds":7200,"syncMaxSeconds":600,"detached":true}}`), nil
	case req.Method == http.MethodDelete:
		tr.deletes.Add(1)
		tr.cancel()
		return execCancelResponse(req, "application/json", execLongStoppedBody()), nil
	case strings.HasSuffix(path, "/stream"):
		tr.cancel()
		return execCancelResponse(req, "application/x-ndjson", execLongAttachBody()), nil
	case req.Method == http.MethodPost && strings.HasSuffix(path, "/commands"):
		if tr.cancelOnPost {
			tr.cancel()
		}
		return execCancelResponse(req, "application/json", execLongStartedBody()), nil
	case req.Method == http.MethodGet && strings.Contains(path, "/commands/"):
		tr.cancel()
		return execCancelResponse(req, "application/json", execLongStatusBody()), nil
	default:
		tr.t.Errorf("unexpected request %s %s", req.Method, path)
		return nil, errors.New("unexpected request")
	}
}

// execCancelResponse builds one synthesized 200 answer.
func execCancelResponse(req *http.Request, contentType, body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{contentType}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

// execLongStartedBody is a pinned started envelope.
func execLongStartedBody() string {
	return `{"ok":true,"type":"command.started","success":true,"commandId":"cmd_test","processId":7,"pid":123,"command":"true","startedAt":"2026-01-02T03:04:05Z"}`
}

// execLongStatusBody is a pinned status envelope.
func execLongStatusBody() string {
	return `{"ok":true,"type":"command.status","success":true,"processId":7,"status":"exited","running":false,"exitCode":0,"stdout":"","stderr":""}`
}

// execLongStoppedBody is a pinned stopped envelope.
func execLongStoppedBody() string {
	return `{"ok":true,"type":"command.stopped","processId":7,"running":false,"signaled":true,"command":null}`
}

// execLongAttachBody is a complete attach stream.
func execLongAttachBody() string {
	return `{"type":"attached","processId":7,"stdoutOffset":0,"stderrOffset":0}` + "\n" +
		`{"type":"exit","status":"exited","exitCode":0,"signal":null,"stdoutOffset":0,"stderrOffset":0}` + "\n"
}

// execCancelEnv builds a CLI whose transport cancels the parent context.
func execCancelEnv(t *testing.T, transport *execCancelTransport) (*cliTestEnv, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	transport.cancel = cancel
	t.Cleanup(cancel)
	env := cliTestNew(t, func(http.ResponseWriter, *http.Request) {}, func(opts *Options) {
		opts.HTTPClient = &http.Client{Transport: transport}
	})
	return env, ctx
}

// TestExecModeCancellationNoOutput proves cancellation after a successful
// answer never writes output and keeps the approved cleanup semantics.
func TestExecModeCancellationNoOutput(t *testing.T) {
	t.Run("status", func(t *testing.T) {
		transport := &execCancelTransport{t: t}
		env, ctx := execCancelEnv(t, transport)
		if got := env.runContext(ctx, "exec", cliTestSandboxID, "--status", "7"); got != 1 {
			t.Fatalf("exit status = %d, want 1", got)
		}
		for _, line := range cliTestJSONLines(t, env.stdout) {
			if line["type"] == "command.status" {
				t.Fatalf("status envelope written after cancellation: %#v", line)
			}
		}
	})
	t.Run("kill", func(t *testing.T) {
		transport := &execCancelTransport{t: t}
		env, ctx := execCancelEnv(t, transport)
		if got := env.runContext(ctx, "exec", cliTestSandboxID, "--kill", "7"); got != 1 {
			t.Fatalf("exit status = %d, want 1", got)
		}
		for _, line := range cliTestJSONLines(t, env.stdout) {
			if line["type"] == "command.stopped" {
				t.Fatalf("stop envelope written after cancellation: %#v", line)
			}
		}
		if transport.deletes.Load() != 1 {
			t.Fatalf("DELETE count = %d, want 1", transport.deletes.Load())
		}
	})
	t.Run("detach stops the known process", func(t *testing.T) {
		transport := &execCancelTransport{t: t, cancelOnPost: true}
		env, ctx := execCancelEnv(t, transport)
		if got := env.runContext(ctx, "exec", cliTestSandboxID, "--detach", "true"); got != 1 {
			t.Fatalf("exit status = %d, want 1", got)
		}
		for _, line := range cliTestJSONLines(t, env.stdout) {
			if line["type"] == "command.started" {
				t.Fatalf("started envelope written after cancellation: %#v", line)
			}
		}
		if transport.deletes.Load() != 1 {
			t.Fatalf("DELETE count = %d, want 1", transport.deletes.Load())
		}
	})
	t.Run("attach leaves the process", func(t *testing.T) {
		transport := &execCancelTransport{t: t}
		env, ctx := execCancelEnv(t, transport)
		if got := env.runContext(ctx, "exec", cliTestSandboxID, "--attach", "7"); got != 130 {
			t.Fatalf("exit status = %d, want 130", got)
		}
		if env.stdout.Len() != 0 {
			t.Fatalf("stdout after cancellation = %q", env.stdout.String())
		}
		if transport.deletes.Load() != 0 {
			t.Fatalf("DELETE count = %d, want 0", transport.deletes.Load())
		}
	})
	t.Run("transparent exit never reports 0 after cancel", func(t *testing.T) {
		transport := &execCancelTransport{t: t}
		env, ctx := execCancelEnv(t, transport)
		if got := env.runContext(ctx, "exec", cliTestSandboxID, "--timeout", "0", "--stream", "--json", "true"); got != 1 {
			t.Fatalf("exit status = %d, want 1", got)
		}
		if transport.deletes.Load() != 1 {
			t.Fatalf("DELETE count = %d, want 1", transport.deletes.Load())
		}
		for _, line := range cliTestJSONLines(t, env.stdout) {
			if line["type"] == "exit" {
				t.Fatalf("terminal exit frame written after cancellation: %#v", line)
			}
		}
	})
}

// TestExecLongPreconfirmedZeroUnlimited proves the run consumer's seam: an
// already-confirmed zero timeout goes straight to detached start with no
// metadata request and no local timeout.
func TestExecLongPreconfirmedZeroUnlimited(t *testing.T) {
	fixture := newExecLongFixture(t)
	fixture.infoMode = execLongInfoDetached
	fixture.exitCode = 0
	server := newExecLongServer(t, fixture)
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	result, err := ExecuteCommand(t.Context(), client, ExecutionRequest{
		SandboxID:       cliTestSandboxID,
		Command:         "true",
		TimeoutSeconds:  0,
		DetachedAllowed: true,
	}, ExecutionOutput{JSON: &stdout})
	if err != nil {
		t.Fatal(err)
	}
	if fixture.infoCount() != 0 {
		t.Fatalf("metadata requests = %d, want 0", fixture.infoCount())
	}
	if fixture.postCount() != 1 {
		t.Fatalf("POSTs = %d, want 1", fixture.postCount())
	}
	body := fixture.post(t, 0)
	if body["detached"] != true {
		t.Fatalf("body = %#v, want detached", body)
	}
	if !result.Detached || !result.Streamed || !result.Completed || result.TimeoutSeconds != 0 || result.TimedOut {
		t.Fatalf("result = %+v, want unlimited completed detached", result)
	}
	if fixture.deleteCount() != 0 {
		t.Fatalf("DELETE count = %d, want 0", fixture.deleteCount())
	}
}

// TestExecLongPreconfirmedShortStaysSync proves a positive timeout at or below
// the pinned cap stays on the legacy synchronous wire even with the confirmed
// capability, which is the resume readiness probe.
func TestExecLongPreconfirmedShortStaysSync(t *testing.T) {
	fixture := newExecLongFixture(t)
	fixture.infoMode = execLongInfoDetached
	fixture.exitCode = 0
	server := newExecLongServer(t, fixture)
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	result, err := ExecuteCommand(t.Context(), client, ExecutionRequest{
		SandboxID:       cliTestSandboxID,
		Command:         "true",
		TimeoutSeconds:  30,
		DetachedAllowed: true,
	}, ExecutionOutput{JSON: &stdout})
	if err != nil {
		t.Fatal(err)
	}
	if fixture.infoCount() != 0 {
		t.Fatalf("metadata requests = %d, want 0", fixture.infoCount())
	}
	body := fixture.post(t, 0)
	if _, ok := body["detached"]; ok {
		t.Fatalf("sync probe sent detached: %#v", body)
	}
	if body["timeoutSeconds"] != float64(30) || body["stream"] != false {
		t.Fatalf("body = %#v", body)
	}
	if result.Streamed || result.Detached {
		t.Fatalf("result = %+v, want synchronous", result)
	}
}

// execLongNthFailWriter fails on the nth write (1-based).
type execLongNthFailWriter struct {
	failAt int
	calls  int
}

// Write implements io.Writer.
func (w *execLongNthFailWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		return 0, errors.New("boom")
	}
	return len(p), nil
}

// execLongErrorBody serves data and then a non-EOF read error.
type execLongErrorBody struct {
	data []byte
	err  error
}

// Read implements io.Reader.
func (b *execLongErrorBody) Read(p []byte) (int, error) {
	if len(b.data) == 0 {
		return 0, b.err
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

// Close implements io.Closer.
func (b *execLongErrorBody) Close() error { return nil }

// execLongErrorTransport returns a 200 NDJSON stream that fails mid-body.
type execLongErrorTransport struct {
	body *execLongErrorBody
}

// RoundTrip implements http.RoundTripper.
func (t *execLongErrorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{"application/x-ndjson"}},
		Body:       t.body,
		Request:    req,
	}, nil
}

// TestExecResultCompleted pins the additive consumer contract: Completed is
// true only after a validated terminal result, and false after an ambiguous or
// failed stop.
func TestExecResultCompleted(t *testing.T) {
	newClient := func(t *testing.T, fixture *execLongFixture) *Client {
		t.Helper()
		server := newExecLongServer(t, fixture)
		client, err := NewClient(server.URL, "test-key", server.Client())
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	run := func(t *testing.T, fixture *execLongFixture, request ExecutionRequest) (ExecutionResult, error) {
		t.Helper()
		return ExecuteCommand(t.Context(), newClient(t, fixture), request, ExecutionOutput{JSON: io.Discard})
	}

	t.Run("sync normal", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.exitCode = 0
		result, err := run(t, fixture, ExecutionRequest{
			SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: 30,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !result.Completed || result.Detached || result.Streamed {
			t.Fatalf("result = %+v", result)
		}
	})
	t.Run("sync nonzero still completed", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.exitCode = 7
		result, err := run(t, fixture, ExecutionRequest{
			SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: 30,
		})
		var exit *ExitError
		if !errors.As(err, &exit) || exit.Code != 7 {
			t.Fatalf("err = %v", err)
		}
		if !result.Completed {
			t.Fatalf("result = %+v", result)
		}
	})
	t.Run("legacy stream terminal exit", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.exitCode = 7
		result, err := run(t, fixture, ExecutionRequest{
			SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: 30, Stream: true,
		})
		var exit *ExitError
		if !errors.As(err, &exit) || exit.Code != 7 {
			t.Fatalf("err = %v", err)
		}
		if !result.Completed {
			t.Fatalf("result = %+v", result)
		}
	})
	t.Run("legacy stream error frame is not completed", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.legacyBody = `{"type":"started"}` + "\n" + `{"type":"error","error":"sandbox_direct_failed","message":"x"}` + "\n"
		result, err := run(t, fixture, ExecutionRequest{
			SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: 30, Stream: true,
		})
		var exit *ExitError
		if !errors.As(err, &exit) || exit.Code != 1 {
			t.Fatalf("err = %v", err)
		}
		if result.Completed {
			t.Fatalf("error frame marked completed: %+v", result)
		}
	})
	t.Run("detached exit is completed", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.infoMode = execLongInfoDetached
		fixture.exitCode = 7
		result, err := run(t, fixture, ExecutionRequest{
			SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: 0, DetachedAllowed: true,
		})
		var exit *ExitError
		if !errors.As(err, &exit) || exit.Code != 7 {
			t.Fatalf("err = %v", err)
		}
		if !result.Completed || !result.Detached {
			t.Fatalf("result = %+v", result)
		}
	})
	t.Run("detached lost is completed with unknown result", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.infoMode = execLongInfoDetached
		fixture.attachScripts = []execAttachTestScript{{body: execAttachTestFrame(t, map[string]any{
			"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
		}) + execAttachTestFrame(t, map[string]any{
			"type": "exit", "status": "lost", "exitCode": nil, "signal": nil,
			"stdoutOffset": 0, "stderrOffset": 0,
		})}}
		result, err := run(t, fixture, ExecutionRequest{
			SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: 0, DetachedAllowed: true,
		})
		var exit *ExitError
		if !errors.As(err, &exit) || exit.Code != 1 {
			t.Fatalf("err = %v", err)
		}
		if !result.Completed || result.ExitCode != 1 {
			t.Fatalf("result = %+v", result)
		}
	})
	t.Run("detached post-header error is not completed", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.infoMode = execLongInfoDetached
		fixture.attachScripts = []execAttachTestScript{{body: execAttachTestFrame(t, map[string]any{
			"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
		}) + execAttachTestFrame(t, map[string]any{
			"type": "error", "error": "not_found", "message": "gone",
			"stdoutOffset": 0, "stderrOffset": 0,
		})}}
		result, err := run(t, fixture, ExecutionRequest{
			SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: 0, DetachedAllowed: true,
		})
		var exit *ExitError
		if !errors.As(err, &exit) || exit.Code != 1 {
			t.Fatalf("err = %v", err)
		}
		if result.Completed {
			t.Fatalf("attach error marked completed: %+v", result)
		}
	})
	t.Run("legacy exit then extra frame is not completed", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.legacyBody = `{"type":"started"}` + "\n" +
			`{"type":"exit","exitCode":0,"success":true,"timedOut":false}` + "\n" +
			`{"type":"stdout","data":"late"}` + "\n"
		result, err := run(t, fixture, ExecutionRequest{
			SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: 30, Stream: true,
		})
		if err == nil {
			t.Fatal("trailing frame accepted")
		}
		if result.Completed {
			t.Fatalf("trailing frame marked completed: %+v", result)
		}
	})
	t.Run("legacy exit then transport error is not completed", func(t *testing.T) {
		body := &execLongErrorBody{
			data: []byte(`{"type":"started"}` + "\n" +
				`{"type":"exit","exitCode":0,"success":true,"timedOut":false}` + "\n"),
			err: errors.New("cut"),
		}
		client, err := NewClient("http://example.test", "test-key", &http.Client{
			Transport: &execLongErrorTransport{body: body},
		})
		if err != nil {
			t.Fatal(err)
		}
		result, err := ExecuteCommand(t.Context(), client, ExecutionRequest{
			SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: 30, Stream: true,
		}, ExecutionOutput{JSON: io.Discard})
		if err == nil {
			t.Fatal("transport error after exit accepted")
		}
		if result.Completed {
			t.Fatalf("transport error marked completed: %+v", result)
		}
	})
	t.Run("sync final writer failure is not completed", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.exitCode = 0
		writer := &execLongNthFailWriter{failAt: 1}
		result, err := ExecuteCommand(t.Context(), newClient(t, fixture), ExecutionRequest{
			SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: 30,
		}, ExecutionOutput{JSON: writer})
		if !execIsWriteFailure(err) {
			t.Fatalf("err = %v", err)
		}
		if result.Completed {
			t.Fatalf("sync writer failure marked completed: %+v", result)
		}
	})
	t.Run("legacy final writer failure is not completed", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.exitCode = 0
		writer := &execLongNthFailWriter{failAt: 4}
		result, err := ExecuteCommand(t.Context(), newClient(t, fixture), ExecutionRequest{
			SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: 30, Stream: true,
		}, ExecutionOutput{JSON: writer})
		if !execIsWriteFailure(err) {
			t.Fatalf("err = %v", err)
		}
		if result.Completed {
			t.Fatalf("legacy exit writer failure marked completed: %+v", result)
		}
	})
	t.Run("detached final writer failure is not completed", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.infoMode = execLongInfoDetached
		fixture.exitCode = 0
		writer := &execLongNthFailWriter{failAt: 4}
		result, err := ExecuteCommand(t.Context(), newClient(t, fixture), ExecutionRequest{
			SandboxID: cliTestSandboxID, Command: "true", TimeoutSeconds: 0, DetachedAllowed: true,
		}, ExecutionOutput{JSON: writer})
		if !execIsWriteFailure(err) {
			t.Fatalf("err = %v", err)
		}
		if result.Completed {
			t.Fatalf("detached exit writer failure marked completed: %+v", result)
		}
	})
	t.Run("timeout frame writer failure is not completed", func(t *testing.T) {
		fixture := newExecLongFixture(t)
		fixture.infoMode = execLongInfoDetached
		fixture.blockAttach = true
		fixture.attachScripts = []execAttachTestScript{{body: execAttachTestFrame(t, map[string]any{
			"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
		})}}
		// Write 1 is the started frame; the failed write is the terminal
		// timeout frame after the validated stop.
		writer := &execLongNthFailWriter{failAt: 2}
		result, err := execRunDetachedLong(t.Context(), newClient(t, fixture), ExecutionRequest{
			SandboxID: cliTestSandboxID, Command: "sleep 100",
		}, ExecutionOutput{JSON: writer}, 20*time.Millisecond, ExecutionResult{})
		if !execIsWriteFailure(err) {
			t.Fatalf("err = %v, want write_failed", err)
		}
		if result.Completed {
			t.Fatalf("timeout writer failure marked completed: %+v", result)
		}
	})
}

// execLongSyncBuffer is a goroutine-safe output buffer for the gated test.
type execLongSyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write implements io.Writer.
func (b *execLongSyncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// Snapshot returns the output written so far.
func (b *execLongSyncBuffer) Snapshot() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// execLongGate is a gated long-command fixture. It holds the command stream
// open until release, so the test can model a 30-minute command and assert the
// CLI keeps waiting without any real long wait.
type execLongGate struct {
	t        *testing.T
	detached bool

	reached     chan struct{}
	reachOnce   sync.Once
	gate        chan struct{}
	releaseOnce sync.Once

	mu            sync.Mutex
	posts         []map[string]any
	attachOffsets []string

	postDeadlineSet    bool
	postDeadlineHas    bool
	postDeadlineLeft   time.Duration
	attachDeadlineSet  bool
	attachDeadlineHas  bool
	attachDeadlineLeft time.Duration
}

// newExecLongGate builds a gate for the old (streamed) or new (detached) wire.
func newExecLongGate(t *testing.T, detached bool) *execLongGate {
	return &execLongGate{
		t:        t,
		detached: detached,
		reached:  make(chan struct{}),
		gate:     make(chan struct{}),
	}
}

// release closes the gate once and is safe from cleanup.
func (g *execLongGate) release() {
	g.releaseOnce.Do(func() { close(g.gate) })
}

// serveHTTP serves the gated command.
func (g *execLongGate) serveHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.EscapedPath()
	switch {
	case path == execInfoPath:
		commands := map[string]any{"streamMaxSeconds": 7200, "syncMaxSeconds": 600}
		if g.detached {
			commands["detached"] = true
		}
		cliTestWriteJSON(g.t, w, http.StatusOK, map[string]any{
			"ok": true, "type": execInfoType, "version": "test", "commands": commands,
		})
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/commands"):
		g.serveCommand(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(path, "/stream"):
		g.serveAttach(w, r)
	default:
		g.t.Errorf("unexpected request %s %s", r.Method, path)
		http.NotFound(w, r)
	}
}

// serveCommand starts the command; in legacy mode it holds the stream open.
func (g *execLongGate) serveCommand(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		g.t.Errorf("read command body: %v", err)
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		g.t.Errorf("decode command body: %v", err)
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	g.mu.Lock()
	g.posts = append(g.posts, body)
	g.mu.Unlock()
	if g.detached {
		command, _ := body["command"].(string)
		cliTestWriteJSON(g.t, w, http.StatusOK, map[string]any{
			"ok": true, "type": "command.started", "success": true,
			"commandId": "cmd_test", "processId": 7, "pid": 123,
			"command": command, "startedAt": "2026-01-02T03:04:05Z",
		})
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, execAttachTestFrame(g.t, map[string]any{"type": "started", "commandId": "cmd_test"}))
	_, _ = io.WriteString(w, execAttachTestFrame(g.t, map[string]any{"type": "stdout", "data": "before\n"}))
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	g.reach()
	select {
	case <-g.gate:
	case <-r.Context().Done():
		return
	}
	_, _ = io.WriteString(w, execAttachTestFrame(g.t, map[string]any{"type": "stdout", "data": "after\n"}))
	_, _ = io.WriteString(w, execAttachTestFrame(g.t, map[string]any{"type": "stderr", "data": "err\n"}))
	_, _ = io.WriteString(w, execAttachTestFrame(g.t, map[string]any{
		"type": "exit", "exitCode": 7, "success": false, "timedOut": false,
	}))
}

// serveAttach serves the detached attach stream. The first session ends with a
// heartbeat and the pinned reconnect boundary; the second waits at the gate.
func (g *execLongGate) serveAttach(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	g.attachOffsets = append(g.attachOffsets, r.URL.Query().Get("stdoutOffset"))
	attempt := len(g.attachOffsets)
	g.mu.Unlock()

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	if attempt == 1 {
		_, _ = io.WriteString(w, execAttachTestFrame(g.t, map[string]any{
			"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
		}))
		_, _ = io.WriteString(w, execAttachTestFrame(g.t, map[string]any{
			"type": "stdout", "data": "before\n", "nextOffset": 7,
		}))
		_, _ = io.WriteString(w, execAttachTestFrame(g.t, map[string]any{"type": "heartbeat"}))
		_, _ = io.WriteString(w, execAttachTestFrame(g.t, map[string]any{
			"type": "reconnect", "stdoutOffset": 7, "stderrOffset": 0,
		}))
		return
	}
	_, _ = io.WriteString(w, execAttachTestFrame(g.t, map[string]any{
		"type": "attached", "processId": 7, "stdoutOffset": 7, "stderrOffset": 0,
	}))
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	g.reach()
	select {
	case <-g.gate:
	case <-r.Context().Done():
		return
	}
	_, _ = io.WriteString(w, execAttachTestFrame(g.t, map[string]any{
		"type": "stdout", "data": "after\n", "nextOffset": 13,
	}))
	_, _ = io.WriteString(w, execAttachTestFrame(g.t, map[string]any{
		"type": "stderr", "data": "err\n", "nextOffset": 4,
	}))
	_, _ = io.WriteString(w, execAttachTestFrame(g.t, map[string]any{
		"type": "exit", "status": "exited", "exitCode": 7, "signal": nil,
		"stdoutOffset": 13, "stderrOffset": 4,
	}))
}

// reach signals the gate point once.
func (g *execLongGate) reach() {
	g.reachOnce.Do(func() { close(g.reached) })
}

// recordClientDeadline records the client-side request deadline for the
// command POST and the first attach request. The server cannot see the
// client's context deadline, so the recording happens in the transport.
func (g *execLongGate) recordClientDeadline(req *http.Request) {
	deadline, ok := req.Context().Deadline()
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/commands"):
		if g.postDeadlineSet {
			return
		}
		g.postDeadlineSet = true
		g.postDeadlineHas = ok
		if ok {
			g.postDeadlineLeft = time.Until(deadline)
		}
	case strings.HasSuffix(req.URL.Path, "/stream"):
		if g.attachDeadlineSet {
			return
		}
		g.attachDeadlineSet = true
		g.attachDeadlineHas = ok
		if ok {
			g.attachDeadlineLeft = time.Until(deadline)
		}
	}
}

// postDeadline returns the recorded command POST deadline evidence.
func (g *execLongGate) postDeadline() (bool, time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.postDeadlineHas, g.postDeadlineLeft
}

// attachDeadline returns the recorded first-attach deadline evidence.
func (g *execLongGate) attachDeadline() (bool, time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.attachDeadlineHas, g.attachDeadlineLeft
}

// execLongDeadlineTransport records the client request deadlines and then
// delegates to the real test-server transport.
type execLongDeadlineTransport struct {
	base http.RoundTripper
	gate *execLongGate
}

// RoundTrip implements http.RoundTripper.
func (t *execLongDeadlineTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.gate.recordClientDeadline(req)
	return t.base.RoundTrip(req)
}

// postCount returns the recorded command POSTs.
func (g *execLongGate) postCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.posts)
}

// post returns one recorded command body.
func (g *execLongGate) post(t *testing.T, index int) map[string]any {
	t.Helper()
	g.mu.Lock()
	defer g.mu.Unlock()
	if index >= len(g.posts) {
		t.Fatalf("post %d missing", index)
	}
	return g.posts[index]
}

// offsets returns the recorded attach stdout offsets.
func (g *execLongGate) offsets() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.attachOffsets...)
}

// execLongGatedApp builds an App over a goroutine-safe writer pair and a
// transport that records client-side request deadlines.
func execLongGatedApp(t *testing.T, server *httptest.Server, gate *execLongGate, stdout, stderr io.Writer) *App {
	t.Helper()
	return NewApp(Options{
		Stdin:        strings.NewReader(""),
		Stdout:       stdout,
		Stderr:       stderr,
		HTTPClient:   &http.Client{Transport: &execLongDeadlineTransport{base: server.Client().Transport, gate: gate}},
		ConfigDir:    t.TempDir(),
		IsTTY:        func(io.Writer) bool { return false },
		PollInterval: time.Millisecond,
		WaitTimeout:  5 * time.Second,
		Getenv: func(key string) string {
			switch key {
			case "BBRAFT_API_URL":
				return server.URL
			case "BBRAFT_API_KEY":
				return "test-key"
			default:
				return ""
			}
		},
	})
}

// TestExecLongGatedDogfoodOldAndNew models a 30-minute dogfood script with a
// gated stream. The gate and client-deadline assertions prove neither wire
// imposes a short overall timeout; the test does not claim that more than 600
// seconds of real time elapsed. It releases the exact dogfood argv and checks
// one POST, no gap and the remote exit.
func TestExecLongGatedDogfoodOldAndNew(t *testing.T) {
	const script = "echo before; sleep 1800; echo after; echo err >&2; exit 7"
	const commandLine = "'bash' '-lc' '" + script + "'"
	for _, tc := range []struct {
		name     string
		detached bool
	}{
		{name: "old streamed wire", detached: false},
		{name: "new detached wire", detached: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := newExecLongGate(t, tc.detached)
			server := httptest.NewServer(http.HandlerFunc(gate.serveHTTP))
			t.Cleanup(server.Close)
			var stdout, stderr execLongSyncBuffer
			app := execLongGatedApp(t, server, gate, &stdout, &stderr)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			var code int
			runDone := make(chan struct{})
			go func() {
				app.SetArgs([]string{"exec", cliTestSandboxID, "--timeout", "0", "--stream", "--json", "--", "bash", "-lc", script})
				code = app.Run(ctx)
				close(runDone)
			}()
			// Release, cancel and join on every failure path.
			t.Cleanup(func() {
				gate.release()
				cancel()
				select {
				case <-runDone:
				case <-time.After(2 * time.Second):
					t.Error("CLI did not stop during cleanup")
				}
			})

			select {
			case <-gate.reached:
			case <-time.After(5 * time.Second):
				t.Fatal("gated command never reached the hold point")
			}
			// A logical 30-minute command must still be running: no return
			// and no terminal frame while the server holds the gate.
			select {
			case <-runDone:
				t.Fatalf("CLI returned %d before the command was released", code)
			case <-time.After(100 * time.Millisecond):
			}
			if strings.Contains(stdout.Snapshot(), `"type":"exit"`) {
				t.Fatalf("terminal frame emitted before the gate: %q", stdout.Snapshot())
			}

			hasDeadline, left := gate.attachDeadline()
			if tc.detached {
				if hasDeadline {
					t.Fatalf("detached zero attach has an overall deadline (%s left)", left)
				}
			} else {
				hasDeadline, left = gate.postDeadline()
				if !hasDeadline || left <= 30*time.Minute {
					t.Fatalf("legacy command deadline = has:%v left:%s, want more than 30m", hasDeadline, left)
				}
			}

			gate.release()
			select {
			case <-runDone:
			case <-time.After(5 * time.Second):
				t.Fatal("CLI did not return after release")
			}
			if code != 7 {
				t.Fatalf("exit status = %d, want 7; stderr=%q", code, stderr.Snapshot())
			}
			if gate.postCount() != 1 {
				t.Fatalf("POSTs = %d, want 1", gate.postCount())
			}
			body := gate.post(t, 0)
			if body["command"] != commandLine {
				t.Fatalf("command = %#v, want %q", body["command"], commandLine)
			}
			if tc.detached {
				if body["detached"] != true {
					t.Fatalf("detached body = %#v", body)
				}
				for _, absent := range []string{"timeoutSeconds", "stream"} {
					if _, ok := body[absent]; ok {
						t.Fatalf("detached start sent %q", absent)
					}
				}
				if got := gate.offsets(); len(got) != 2 || got[0] != "0" || got[1] != "7" {
					t.Fatalf("attach offsets = %v, want [0 7]", got)
				}
			} else {
				if body["stream"] != true || body["timeoutSeconds"] != float64(7200) {
					t.Fatalf("legacy body = %#v", body)
				}
				if _, ok := body["detached"]; ok {
					t.Fatalf("legacy body sent detached")
				}
				if got := gate.offsets(); len(got) != 0 {
					t.Fatalf("legacy wire made attach calls: %v", got)
				}
			}
			execLongAssertGatedDogfoodOutput(t, &stdout, &stderr)
		})
	}
}

// execLongAssertGatedDogfoodOutput checks the released stdout/stderr and terminal exit.
func execLongAssertGatedDogfoodOutput(t *testing.T, stdout, stderr *execLongSyncBuffer) {
	t.Helper()
	var out, errOut strings.Builder
	var exitCode any
	for _, line := range strings.Split(strings.TrimSpace(stdout.Snapshot()), "\n") {
		if line == "" {
			continue
		}
		var frame map[string]any
		if err := json.Unmarshal([]byte(line), &frame); err != nil {
			t.Fatalf("decode JSON line %q: %v", line, err)
		}
		switch frame["type"] {
		case "stdout":
			out.WriteString(frame["data"].(string))
		case "stderr":
			errOut.WriteString(frame["data"].(string))
		case "exit":
			exitCode = frame["exitCode"]
		}
	}
	if out.String() != "before\nafter\n" {
		t.Fatalf("stdout = %q, want before/after with no gap", out.String())
	}
	if errOut.String() != "err\n" {
		t.Fatalf("stderr = %q", errOut.String())
	}
	if exitCode != float64(7) {
		t.Fatalf("exit frame code = %#v, want 7", exitCode)
	}
	if stderr.Snapshot() != "" {
		t.Fatalf("stderr notes = %q", stderr.Snapshot())
	}
}

// newExecLongServer starts an httptest server for a fixture.
func newExecLongServer(t *testing.T, fixture *execLongFixture) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
	t.Cleanup(server.Close)
	return server
}
