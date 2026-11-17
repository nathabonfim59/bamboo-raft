package cli

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

// runTestAPI is a fake v1 API shaped for the run command tests. It records
// every request so a test can assert ordering, credentials and body fields.
type runTestAPI struct {
	t *testing.T

	mu           sync.Mutex
	creates      []api.CreateSandboxRequest
	commandReqs  []api.CommandRequest
	patchReqs    []api.UpdateSandboxRequest
	deleteIDs    []string
	confirmHeads []string
	operationIDs []string
	requestLog   []string
	infoCalls    int
	uploadPaths  []string
	killCalls    int

	sandboxType    string
	sandboxError   *string
	states         []api.SandboxState
	stateIndex     int
	deleteOpStatus api.DeletionStatus
	opStatuses     []api.DeletionStatus
	opIndex        int
	opTarget       string
	uploadBase     string
	patchTTL       *int
	patchArchive   *time.Time
	attachBody     string

	createFunc    func(http.ResponseWriter, *http.Request, api.CreateSandboxRequest) bool
	getFunc       func(http.ResponseWriter, *http.Request, string) bool
	commandFunc   func(http.ResponseWriter, *http.Request, api.CommandRequest) bool
	patchFunc     func(http.ResponseWriter, *http.Request, string, api.UpdateSandboxRequest) bool
	deleteFunc    func(http.ResponseWriter, *http.Request, string) bool
	operationFunc func(http.ResponseWriter, *http.Request, string) bool
	uploadURLFunc func(http.ResponseWriter, *http.Request) bool
	infoFunc      func(http.ResponseWriter, *http.Request)
	attachFunc    func(http.ResponseWriter, *http.Request) bool
}

// runTestNewAPI builds a fake with the common defaults: a ready sandbox, a
// pending delete operation and a completed poll.
func runTestNewAPI(t *testing.T) *runTestAPI {
	t.Helper()
	return &runTestAPI{
		t:              t,
		sandboxType:    "default",
		states:         []api.SandboxState{api.StateReady},
		deleteOpStatus: api.DeletionPending,
		opStatuses:     []api.DeletionStatus{api.DeletionCompleted},
	}
}

// serveHTTP serves the run command's v1 API subset.
func (f *runTestAPI) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if !cliTestSecretEqual(r.Header.Get("Authorization"), "Bearer test-key") {
		f.t.Errorf("%s %s does not carry the fixture bearer key", r.Method, r.URL.Path)
	}
	path := r.URL.Path
	switch {
	case r.Method == http.MethodPost && path == api.PathSandboxes:
		f.serveCreate(w, r)
	case r.Method == http.MethodPatch && strings.HasPrefix(path, api.PathSandboxes+"/"):
		f.servePatch(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/commands"):
		f.serveCommand(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/files/upload-url"):
		f.serveUploadURL(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(path, "/stream"):
		f.serveAttach(w, r)
	case r.Method == http.MethodGet && path == execInfoPath:
		f.serveInfo(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(path, api.PathSandboxes+"/"):
		f.serveGet(w, r)
	case r.Method == http.MethodDelete && strings.Contains(path, "/commands/"):
		f.serveKill(w, r)
	case r.Method == http.MethodDelete && strings.HasPrefix(path, api.PathSandboxes+"/"):
		f.serveDelete(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/api/v1/deletion-operations/"):
		f.serveOperation(w, r)
	default:
		f.t.Errorf("unexpected request %s %s", r.Method, path)
		http.NotFound(w, r)
	}
}

// serveCreate answers POST /api/v1/sandboxes.
func (f *runTestAPI) serveCreate(w http.ResponseWriter, r *http.Request) {
	var request api.CreateSandboxRequest
	if err := json.UnmarshalRead(r.Body, &request); err != nil {
		f.t.Errorf("decode create body failed")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.creates = append(f.creates, request)
	f.requestLog = append(f.requestLog, "create")
	handler := f.createFunc
	f.mu.Unlock()
	if handler != nil && handler(w, r, request) {
		return
	}
	ttl := api.DefaultTTLSeconds
	cliTestWriteJSON(f.t, w, http.StatusAccepted, api.NewCreateSandboxResponse(
		api.CreateProvisioning, &ttl, f.sandbox(api.StateProvisioning),
	))
}

// serveGet answers GET /api/v1/sandboxes/{id}.
func (f *runTestAPI) serveGet(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, api.PathSandboxes+"/")
	f.mu.Lock()
	f.requestLog = append(f.requestLog, "get")
	handler := f.getFunc
	f.mu.Unlock()
	if handler != nil && handler(w, r, id) {
		return
	}
	state := f.nextState()
	sandbox := f.sandbox(state)
	if state == api.StateError && f.sandboxError != nil {
		sandbox.Error = f.sandboxError
	}
	cliTestWriteJSON(f.t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
}

// serveCommand answers POST /api/v1/sandboxes/{id}/commands.
func (f *runTestAPI) serveCommand(w http.ResponseWriter, r *http.Request) {
	var request api.CommandRequest
	if err := json.UnmarshalRead(r.Body, &request); err != nil {
		f.t.Errorf("decode command body failed")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.commandReqs = append(f.commandReqs, request)
	f.requestLog = append(f.requestLog, "command")
	handler := f.commandFunc
	f.mu.Unlock()
	if handler != nil && handler(w, r, request) {
		return
	}
	if request.Detached != nil && *request.Detached {
		cliTestWriteJSON(f.t, w, http.StatusOK, api.CommandStartedResponse{
			SuccessBase: api.SuccessBase{OK: true, Type: api.TypeCommandStarted},
			Success:     true,
			CommandID:   "cmd_test",
			ProcessID:   7,
			PID:         123,
			Command:     request.Command,
			StartedAt:   time.Now(),
		})
		return
	}
	if request.Stream != nil && *request.Stream {
		if got := r.Header.Get("Accept"); got != "application/x-ndjson" {
			f.t.Errorf("streamed command Accept = %q", got)
		}
		runTestWriteStream(f.t, w, runTestFrames("", "", new(0), false))
		return
	}
	code := 0
	cliTestWriteJSON(f.t, w, http.StatusOK, api.NewCommandResponse(true, &code, "", "", false))
}

// serveAttach answers a detached attach stream. The default is a clean exit 0
// after one stdout chunk; a test can replace it with attachBody.
func (f *runTestAPI) serveAttach(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requestLog = append(f.requestLog, "attach")
	body := f.attachBody
	handler := f.attachFunc
	f.mu.Unlock()
	if handler != nil && handler(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	if body == "" {
		body = execAttachTestFrame(f.t, map[string]any{
			"type": "attached", "processId": 7, "stdoutOffset": 0, "stderrOffset": 0,
		}) + execAttachTestFrame(f.t, map[string]any{
			"type": "stdout", "data": "detached\n", "nextOffset": 9,
		}) + execAttachTestFrame(f.t, map[string]any{
			"type": "exit", "status": "exited", "exitCode": 0, "signal": nil,
			"stdoutOffset": 9, "stderrOffset": 0,
		})
	}
	if _, err := io.WriteString(w, body); err != nil {
		f.t.Errorf("write attach body: %v", err)
	}
}

// serveKill answers DELETE /api/v1/sandboxes/{id}/commands/{pid}.
func (f *runTestAPI) serveKill(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.killCalls++
	f.requestLog = append(f.requestLog, "kill")
	f.mu.Unlock()
	cliTestWriteJSON(f.t, w, http.StatusOK, map[string]any{
		"ok": true, "type": api.TypeCommandStopped,
		"processId": 7, "running": false, "signaled": true,
		"command": map[string]any{
			"commandId":   "cmd_test",
			"command":     "true",
			"sandboxId":   cliTestSandboxID,
			"mode":        "detached",
			"status":      "killed",
			"processId":   7,
			"stdoutBytes": 0, "stderrBytes": 0, "outputTruncated": false,
			"startedAt":  "2026-01-02T03:04:05Z",
			"finishedAt": "2026-01-02T03:04:06Z",
		},
	})
}

// serveUploadURL answers the signed upload URL request.
func (f *runTestAPI) serveUploadURL(w http.ResponseWriter, r *http.Request) {
	var request api.FileURLRequest
	if err := json.UnmarshalRead(r.Body, &request); err != nil {
		f.t.Errorf("decode upload-url body failed")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.uploadPaths = append(f.uploadPaths, request.Path)
	f.requestLog = append(f.requestLog, "upload-url")
	handler := f.uploadURLFunc
	base := f.uploadBase
	f.mu.Unlock()
	if handler != nil && handler(w, r) {
		return
	}
	if base == "" {
		cliTestWriteJSON(f.t, w, http.StatusNotFound,
			cliTestErrorEnvelope(http.StatusNotFound, "not_found", "uploading is disabled"))
		return
	}
	cliTestWriteJSON(f.t, w, http.StatusOK, api.NewFileUploadURLResponse(
		base+"/upload?token=run-test-token", request.Path, time.Now().Add(time.Hour),
	))
}

// servePatch answers PATCH /api/v1/sandboxes/{id} for a rolling TTL renewal.
func (f *runTestAPI) servePatch(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, api.PathSandboxes+"/")
	var request api.UpdateSandboxRequest
	if err := json.UnmarshalRead(r.Body, &request); err != nil {
		f.t.Errorf("decode patch body failed")
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.patchReqs = append(f.patchReqs, request)
	f.requestLog = append(f.requestLog, "patch")
	handler := f.patchFunc
	ttl := int(runRollingTTLSeconds)
	if f.patchTTL != nil {
		ttl = *f.patchTTL
	}
	archive := time.Now().Add(time.Duration(ttl) * time.Second)
	if f.patchArchive != nil {
		archive = *f.patchArchive
	}
	f.mu.Unlock()
	if handler != nil && handler(w, r, id, request) {
		return
	}
	sandbox := f.sandbox(api.StateReady)
	sandbox.TTLSeconds = new(ttl)
	sandbox.ArchiveAfter = new(archive)
	cliTestWriteJSON(f.t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
}

// serveDelete answers DELETE /api/v1/sandboxes/{id}.
func (f *runTestAPI) serveDelete(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, api.PathSandboxes+"/")
	f.mu.Lock()
	f.deleteIDs = append(f.deleteIDs, id)
	f.confirmHeads = append(f.confirmHeads, r.Header.Get("X-Ascii-Confirm-Delete"))
	f.requestLog = append(f.requestLog, "delete")
	handler := f.deleteFunc
	status := f.deleteOpStatus
	f.mu.Unlock()
	if handler != nil && handler(w, r, id) {
		return
	}
	cliTestWriteJSON(f.t, w, http.StatusAccepted, api.NewSandboxDeletingResponse(
		cliTestOperation(cliTestOperationID, id, status),
	))
}

// serveOperation answers GET /api/v1/deletion-operations/{id}.
func (f *runTestAPI) serveOperation(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/deletion-operations/")
	f.mu.Lock()
	f.operationIDs = append(f.operationIDs, id)
	f.requestLog = append(f.requestLog, "operation")
	handler := f.operationFunc
	status := f.nextOperationStatusLocked()
	target := f.opTarget
	f.mu.Unlock()
	if handler != nil && handler(w, r, id) {
		return
	}
	if target == "" {
		target = cliTestSandboxID
	}
	cliTestWriteJSON(f.t, w, http.StatusOK, api.NewDeletionOperationResponse(
		cliTestOperation(id, target, status),
	))
}

// serveInfo answers the metadata extension unless a test overrides it.
func (f *runTestAPI) serveInfo(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.infoCalls++
	f.requestLog = append(f.requestLog, "info")
	handler := f.infoFunc
	f.mu.Unlock()
	if handler != nil {
		handler(w, r)
		return
	}
	cliTestWriteJSON(f.t, w, http.StatusNotFound,
		cliTestErrorEnvelope(http.StatusNotFound, "not_found", "no metadata"))
}

// sandbox builds one pinned-shaped sandbox with the fake's type.
func (f *runTestAPI) sandbox(state api.SandboxState) api.Sandbox {
	f.mu.Lock()
	sandboxType := f.sandboxType
	f.mu.Unlock()
	sandbox := cliTestSandbox(cliTestSandboxID, state)
	sandbox.Type = sandboxType
	return sandbox
}

// nextState returns the next configured poll state, repeating the last one.
func (f *runTestAPI) nextState() api.SandboxState {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.states) == 0 {
		return api.StateReady
	}
	index := min(f.stateIndex, len(f.states)-1)
	f.stateIndex++
	return f.states[index]
}

// nextOperationStatusLocked returns the next configured operation status. The
// caller must hold the mutex.
func (f *runTestAPI) nextOperationStatusLocked() api.DeletionStatus {
	if len(f.opStatuses) == 0 {
		return api.DeletionCompleted
	}
	index := min(f.opIndex, len(f.opStatuses)-1)
	f.opIndex++
	return f.opStatuses[index]
}

// createRequests returns a copy of the recorded create bodies.
func (f *runTestAPI) createRequests() []api.CreateSandboxRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]api.CreateSandboxRequest(nil), f.creates...)
}

// commandRequests returns a copy of the recorded command bodies.
func (f *runTestAPI) commandRequests() []api.CommandRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]api.CommandRequest(nil), f.commandReqs...)
}

// patchRequests returns a copy of the recorded PATCH bodies.
func (f *runTestAPI) patchRequests() []api.UpdateSandboxRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]api.UpdateSandboxRequest(nil), f.patchReqs...)
}

// patchRequestCount returns the recorded PATCH count.
func (f *runTestAPI) patchRequestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.patchReqs)
}

// killCount returns the recorded detached stop count.
func (f *runTestAPI) killCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.killCalls
}

// deleteRequests returns the recorded sandbox delete ids.
func (f *runTestAPI) deleteRequests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleteIDs...)
}

// confirmations returns the recorded X-Ascii-Confirm-Delete headers.
func (f *runTestAPI) confirmations() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.confirmHeads...)
}

// sequence returns the recorded request order.
func (f *runTestAPI) sequence() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requestLog...)
}

// infoRequestCount returns the metadata request count.
func (f *runTestAPI) infoRequestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.infoCalls
}

// uploadPathRequests returns the recorded temporary archive paths.
func (f *runTestAPI) uploadPathRequests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.uploadPaths...)
}

// operationRequestCount returns the deletion operation poll count.
func (f *runTestAPI) operationRequestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.operationIDs)
}

// runTestNewEnv wires a fake API into a CLI environment.
func runTestNewEnv(t *testing.T, fake *runTestAPI, mutate func(*Options)) *cliTestEnv {
	t.Helper()
	return cliTestNew(t, fake.serveHTTP, mutate)
}

// runTestWriteStream writes one NDJSON command stream.
func runTestWriteStream(t *testing.T, w http.ResponseWriter, body string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, body); err != nil {
		t.Errorf("write command stream: %v", err)
	}
}

// runTestFrames builds a valid command stream with optional output. A nil
// exitCode is a signal exit; timedOut uses an explicit null exit code.
func runTestFrames(stdout, stderr string, exitCode *int, timedOut bool) string {
	var body strings.Builder
	body.WriteString("{\"type\":\"started\"}\n")
	writeFrame := func(typ, data string) {
		raw, _ := json.Marshal(api.CommandStreamFrame{Type: api.CommandFrameType(typ), Data: data})
		body.Write(raw)
		body.WriteByte('\n')
	}
	if stdout != "" {
		writeFrame("stdout", stdout)
	}
	if stderr != "" {
		writeFrame("stderr", stderr)
	}
	code := "null"
	success := "false"
	if exitCode != nil {
		code = strconv.Itoa(*exitCode)
		success = strconv.FormatBool(*exitCode == 0)
	}
	fmt.Fprintf(&body, "{\"type\":\"exit\",\"exitCode\":%s,\"success\":%s,\"timedOut\":%t}\n", code, success, timedOut)
	return body.String()
}

// runTestSummaryLines returns the human summary lines on stderr.
func runTestSummaryLines(stderr string) []string {
	var lines []string
	for line := range strings.SplitSeq(strings.TrimSpace(stderr), "\n") {
		if strings.HasPrefix(line, "exit ") {
			lines = append(lines, line)
		}
	}
	return lines
}

// runTestAssertBefore proves first appears before second without printing the
// surrounding diagnostics.
func runTestAssertBefore(t *testing.T, output, first, second string) {
	t.Helper()
	firstIndex := strings.Index(output, first)
	secondIndex := strings.Index(output, second)
	if firstIndex < 0 {
		t.Fatalf("output does not contain the %q diagnostic", first)
	}
	if secondIndex < 0 {
		t.Fatalf("output does not contain the %q terminal line", second)
	}
	if firstIndex >= secondIndex {
		t.Fatalf("%q must appear before %q", first, second)
	}
}

// runTestFailWriter fails every write without echoing its input.
type runTestFailWriter struct {
	calls atomic.Int64
}

func (w *runTestFailWriter) Write([]byte) (int, error) {
	w.calls.Add(1)
	return 0, errors.New("sentinel writer failure")
}

// runTestFailingTransport fails selected requests with a sentinel that must
// never reach command output.
type runTestFailingTransport struct {
	base http.RoundTripper
	fail func(*http.Request) bool
}

func (t runTestFailingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if t.fail(request) {
		return nil, errors.New("sentinel transport failure")
	}
	return t.base.RoundTrip(request)
}

// TestRunWireFixturesMatchPin validates the typed responses and command frames
// the run fake uses against the v1 API document.
func TestRunWireFixturesMatchPin(t *testing.T) {
	validator := compat.Load(t)
	ttl := api.DefaultTTLSeconds
	sandbox := cliTestSandbox(cliTestSandboxID, api.StateProvisioning)
	sandbox.Type = "small"
	cases := []struct {
		name   string
		status int
		value  any
	}{
		{name: "create", status: http.StatusAccepted, value: api.NewCreateSandboxResponse(api.CreateProvisioning, &ttl, sandbox)},
		{name: "get", status: http.StatusOK, value: api.NewSandboxInfoResponse(sandbox)},
		{name: "delete", status: http.StatusAccepted, value: api.NewSandboxDeletingResponse(
			cliTestOperation(cliTestOperationID, cliTestSandboxID, api.DeletionPending),
		)},
		{name: "operation", status: http.StatusOK, value: api.NewDeletionOperationResponse(
			cliTestOperation(cliTestOperationID, cliTestSandboxID, api.DeletionCompleted),
		)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cliTestValidatePinned(t, validator, tc.status, tc.value)
		})
	}
	t.Run("command frames", func(t *testing.T) {
		header := http.Header{"Content-Type": []string{compat.NDJSONMediaType}}
		frames := []byte(runTestFrames("out", "err", new(0), false))
		if err := validator.NDJSON("command", http.StatusOK, header, frames); err != nil {
			t.Fatalf("command frames do not match the pin: %v", err)
		}
	})
}

// TestRunLocalValidationNoRequests proves every local failure exits 2 before
// the first HTTP request, including unsupported reference flags.
func TestRunLocalValidationNoRequests(t *testing.T) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	cases := []struct {
		name string
		args []string
	}{
		{name: "no command", args: []string{"run"}},
		{name: "whitespace command", args: []string{"run", "--", "   "}},
		{name: "nul command", args: []string{"run", "--", "echo\x00x"}},
		{name: "unknown type", args: []string{"run", "--type", "huge", "--", "true"}},
		{name: "env without equals", args: []string{"run", "--env", "SUPER_SECRET_VALUE", "--", "true"}},
		{name: "env nul", args: []string{"run", "--env", "A=b\x00c", "--", "true"}},
		{name: "cwd nul", args: []string{"run", "--cwd", "a\x00b", "--", "true"}},
		{name: "negative timeout", args: []string{"run", "--timeout", "-1", "--", "true"}},
		{name: "timeout above uint32", args: []string{"run", "--timeout", "4294967296", "--", "true"}},
		{name: "ttl above maximum", args: []string{"run", "--timeout", "2588281", "--", "true"}},
		{name: "personal", args: []string{"run", "--personal", "--", "true"}},
		{name: "environment", args: []string{"run", "--environment", "prod", "--", "true"}},
		{name: "no-env", args: []string{"run", "--no-env", "--", "true"}},
		{name: "unknown flag", args: []string{"run", "--bogus", "--", "true"}},
		{name: "push missing", args: []string{"run", "--push", filepath.Join(t.TempDir(), "missing"), "--", "true"}},
		{name: "push file", args: []string{"run", "--push", file, "--", "true"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int64
			env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, nil)
			status := env.run(t, tc.args...)
			if strings.Contains(env.stderr.String(), "SUPER_SECRET_VALUE") {
				t.Fatal("stderr contains the env value")
			}
			if status != 2 {
				t.Fatalf("exit status = %d, want 2; stderr=%q", status, env.stderr.String())
			}
			if hits.Load() != 0 {
				t.Fatalf("server received %d requests, want 0", hits.Load())
			}
		})
	}
}

// TestRunCreatesReadyRunsAndDeletes covers the full success path: silent ready
// polling, owner credentials, request order, raw output and one summary.
func TestRunCreatesReadyRunsAndDeletes(t *testing.T) {
	fake := runTestNewAPI(t)
	fake.sandboxType = "small"
	fake.states = []api.SandboxState{api.StateProvisioning, api.StateReady}
	fake.deleteOpStatus = api.DeletionPending
	fake.opStatuses = []api.DeletionStatus{api.DeletionProcessing, api.DeletionCompleted}
	fake.commandFunc = func(w http.ResponseWriter, r *http.Request, request api.CommandRequest) bool {
		if request.Stream == nil || !*request.Stream {
			t.Error("run command was not streamed")
			return true
		}
		if got := r.Header.Get("Accept"); got != "application/x-ndjson" {
			t.Errorf("Accept = %q, want NDJSON", got)
		}
		runTestWriteStream(t, w, runTestFrames("héllo\n", "warn\n", new(0), false))
		return true
	}
	env := runTestNewEnv(t, fake, nil)

	status := env.run(t, "run", "--type", "small", "--from", cliTestSnapshotName, "-e", "A=1", "--timeout", "30", "--", "echo", "hi")
	if status != 0 {
		t.Fatalf("exit status = %d, want 0; stderr=%q", status, env.stderr.String())
	}
	if got := env.stdout.String(); got != "héllo\n" {
		t.Fatalf("stdout = %q, want raw command output", got)
	}
	summary := fmt.Sprintf("exit 0 · 0m 0s · small · $0 · %s deleted", cliTestSandboxID)
	if !strings.HasSuffix(env.stderr.String(), summary+"\n") {
		t.Fatalf("stderr = %q, want summary %q last", env.stderr.String(), summary)
	}
	if strings.Count(env.stderr.String(), "exit 0 ·") != 1 {
		t.Fatalf("stderr = %q, want exactly one summary", env.stderr.String())
	}
	if strings.Contains(env.stdout.String(), "\"event\"") {
		t.Fatal("raw stdout contains a JSON event")
	}
	for _, event := range []string{"created", "state", "ready"} {
		if strings.Contains(env.stdout.String(), event) {
			t.Fatalf("raw stdout contains the %q setup event", event)
		}
	}

	creates := fake.createRequests()
	if len(creates) != 1 {
		t.Fatalf("create requests = %d, want 1", len(creates))
	}
	create := creates[0]
	if create.Type == nil || *create.Type != "small" {
		t.Error("create type is not small")
	}
	if create.From == nil || *create.From != cliTestSnapshotName {
		t.Error("create from is not the requested snapshot")
	}
	if create.Env["A"] != "1" {
		t.Error("create env is missing A=1")
	}
	if ttl, ok := create.TTLSeconds.Seconds(); !ok || ttl != 3750 {
		t.Errorf("create ttl = %d set=%v, want 3750", ttl, ok)
	}

	commands := fake.commandRequests()
	if len(commands) != 1 {
		t.Fatalf("command requests = %d, want 1", len(commands))
	}
	command := commands[0]
	if command.Command != "'echo' 'hi'" {
		t.Errorf("command = %q, want quoted argv", command.Command)
	}
	if command.Cwd == nil || *command.Cwd != runDefaultCwd {
		t.Errorf("cwd = %v, want %s", command.Cwd, runDefaultCwd)
	}
	if command.TimeoutSeconds == nil || *command.TimeoutSeconds != 30 {
		t.Errorf("timeout = %v, want 30", command.TimeoutSeconds)
	}

	deletes := fake.deleteRequests()
	if len(deletes) != 1 || deletes[0] != cliTestSandboxID {
		t.Fatalf("deletes = %v, want the created id", deletes)
	}
	confirms := fake.confirmations()
	if len(confirms) != 1 || confirms[0] != cliTestSandboxID {
		t.Fatalf("confirmation headers = %v, want the created id", confirms)
	}
	if fake.operationRequestCount() != 2 {
		t.Fatalf("operation polls = %d, want pending -> processing -> completed", fake.operationRequestCount())
	}
	wantSequence := []string{"info", "create", "get", "get", "command", "delete", "operation", "operation"}
	if got := fake.sequence(); !slices.Equal(got, wantSequence) {
		t.Fatalf("request sequence = %v, want %v", got, wantSequence)
	}
}

// TestRunInheritedSnapshotKeepsNilType proves omitted --type stays absent in
// the create body and the summary uses the actual returned type.
func TestRunInheritedSnapshotKeepsNilType(t *testing.T) {
	fake := runTestNewAPI(t)
	fake.sandboxType = "xlarge"
	fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
		runTestWriteStream(t, w, runTestFrames("", "", new(0), false))
		return true
	}
	env := runTestNewEnv(t, fake, nil)
	if status := env.run(t, "run", "--from", cliTestSnapshotName, "--timeout", "30", "--", "true"); status != 0 {
		t.Fatalf("exit status = %d, stderr=%q", status, env.stderr.String())
	}
	creates := fake.createRequests()
	if len(creates) != 1 {
		t.Fatalf("create requests = %d", len(creates))
	}
	if creates[0].Type != nil {
		t.Fatal("create type was not omitted for snapshot inheritance")
	}
	if !strings.Contains(env.stderr.String(), "· xlarge ·") {
		t.Fatalf("summary does not use the actual inherited type: %q", env.stderr.String())
	}
}

// TestRunRequiresActualSandboxType proves a create response without a type is
// not papered over with a guessed default; cleanup still runs.
func TestRunRequiresActualSandboxType(t *testing.T) {
	fake := runTestNewAPI(t)
	fake.sandboxType = ""
	fake.commandFunc = func(http.ResponseWriter, *http.Request, api.CommandRequest) bool {
		t.Error("command ran without a sandbox type")
		return true
	}
	env := runTestNewEnv(t, fake, nil)
	if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if len(fake.deleteRequests()) != 1 {
		t.Fatal("sandbox was not cleaned up after the missing type")
	}
	if strings.Contains(env.stderr.String(), "· default ·") || strings.Contains(env.stderr.String(), "· small ·") {
		t.Fatalf("summary invented a sandbox type: %q", env.stderr.String())
	}
	if !strings.Contains(env.stderr.String(), "no sandbox type") {
		t.Fatalf("stderr = %q, want the invalid-response diagnostic", env.stderr.String())
	}
}

// TestRunRejectsUnsupportedSandboxType proves a create response's type must be
// one of the known machine sizes.
func TestRunRejectsUnsupportedSandboxType(t *testing.T) {
	fake := runTestNewAPI(t)
	fake.sandboxType = "huge"
	fake.commandFunc = func(http.ResponseWriter, *http.Request, api.CommandRequest) bool {
		t.Error("command ran with an unsupported sandbox type")
		return true
	}
	env := runTestNewEnv(t, fake, nil)
	if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if len(fake.deleteRequests()) != 1 {
		t.Fatal("sandbox was not cleaned up")
	}
	if !strings.Contains(env.stderr.String(), "unsupported type") {
		t.Fatalf("stderr = %q, want the unsupported-type diagnostic", env.stderr.String())
	}
	if strings.Contains(env.stderr.String(), "· default ·") || strings.Contains(env.stderr.String(), "· huge ·") {
		t.Fatalf("summary invented a supported type: %q", env.stderr.String())
	}
}

// TestRunValidatesEveryPollType proves every ready poll carries the same,
// supported type as the create response.
func TestRunValidatesEveryPollType(t *testing.T) {
	cases := []struct {
		name     string
		pollType string
		want     string
	}{
		{name: "empty", pollType: "", want: "no sandbox type"},
		{name: "unsupported", pollType: "huge", want: "unsupported type"},
		{name: "changed", pollType: "xlarge", want: "type changed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := runTestNewAPI(t)
			fake.sandboxType = "default"
			fake.getFunc = func(w http.ResponseWriter, _ *http.Request, _ string) bool {
				sandbox := cliTestSandbox(cliTestSandboxID, api.StateReady)
				sandbox.Type = tc.pollType
				cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
				return true
			}
			env := runTestNewEnv(t, fake, nil)
			if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
				t.Fatalf("exit status = %d, want 1", status)
			}
			if len(fake.commandRequests()) != 0 {
				t.Fatal("command ran after an invalid poll type")
			}
			if len(fake.deleteRequests()) != 1 {
				t.Fatal("sandbox was not cleaned up")
			}
			if !strings.Contains(env.stderr.String(), tc.want) {
				t.Fatalf("stderr = %q, want %q", env.stderr.String(), tc.want)
			}
		})
	}
}

// TestRunDerivesTTLAndCap covers every timeout/TTL combination, including the
// timeout-0 metadata discovery and the 404 fallback note.
func TestRunDerivesTTLAndCap(t *testing.T) {
	cases := []struct {
		name        string
		timeout     string
		infoStatus  int
		streamMax   any
		wantInfo    int
		wantNote    bool
		wantTTL     int
		wantTimeout int
	}{
		{name: "short command", timeout: "1", wantInfo: 1, wantTTL: 3721, wantTimeout: 1},
		{name: "long command", timeout: "1800", wantInfo: 1, wantTTL: 5520, wantTimeout: 1800},
		{name: "max ttl", timeout: "2588280", wantInfo: 1, wantTTL: 2592000, wantTimeout: 2588280},
		{
			name: "metadata cap", timeout: "0", infoStatus: http.StatusOK, streamMax: 7200,
			wantInfo: 1, wantTTL: 10920, wantTimeout: 7200,
		},
		{
			name: "metadata fallback", timeout: "0", infoStatus: http.StatusNotFound,
			wantInfo: 1, wantNote: true, wantTTL: 4320, wantTimeout: 600,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := runTestNewAPI(t)
			fake.infoFunc = func(w http.ResponseWriter, _ *http.Request) {
				if tc.infoStatus == http.StatusNotFound {
					cliTestWriteJSON(t, w, http.StatusNotFound,
						cliTestErrorEnvelope(http.StatusNotFound, "not_found", "no metadata"))
					return
				}
				streamMax := any(tc.streamMax)
				if tc.streamMax == nil {
					streamMax = 7200
				}
				execTestWriteInfo(t, w, streamMax, 600)
			}
			fake.commandFunc = func(w http.ResponseWriter, r *http.Request, request api.CommandRequest) bool {
				if request.TimeoutSeconds == nil || *request.TimeoutSeconds != tc.wantTimeout {
					t.Errorf("command timeout = %v, want %d", request.TimeoutSeconds, tc.wantTimeout)
				}
				if request.Stream == nil || !*request.Stream {
					t.Error("run command was not streamed")
				}
				runTestWriteStream(t, w, runTestFrames("", "", new(0), false))
				return true
			}
			env := runTestNewEnv(t, fake, nil)
			if status := env.run(t, "run", "--timeout", tc.timeout, "--", "true"); status != 0 {
				t.Fatalf("exit status = %d, stderr=%q", status, env.stderr.String())
			}
			creates := fake.createRequests()
			if len(creates) != 1 {
				t.Fatalf("create requests = %d", len(creates))
			}
			ttl, ok := creates[0].TTLSeconds.Seconds()
			if !ok || ttl != tc.wantTTL {
				t.Fatalf("ttl = %d set=%v, want %d", ttl, ok, tc.wantTTL)
			}
			if fake.infoRequestCount() != tc.wantInfo {
				t.Fatalf("metadata requests = %d, want %d", fake.infoRequestCount(), tc.wantInfo)
			}
			if tc.wantInfo > 0 {
				sequence := fake.sequence()
				if len(sequence) < 2 || sequence[0] != "info" || sequence[1] != "create" {
					t.Fatalf("request sequence = %v, want metadata before create", sequence)
				}
			}
			hasNote := strings.Contains(env.stderr.String(), "600-second stream timeout")
			if hasNote != tc.wantNote {
				t.Fatalf("fallback note = %v, want %v; stderr=%q", hasNote, tc.wantNote, env.stderr.String())
			}
		})
	}
}

// TestRunRejectsTTLOverflowBeforeCreate proves the 30-day guard runs before
// create, including after a timeout-0 metadata discovery.
func TestRunRejectsTTLOverflowBeforeCreate(t *testing.T) {
	t.Run("explicit timeout", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.infoFunc = func(http.ResponseWriter, *http.Request) {
			t.Error("metadata fetched for an explicit timeout")
		}
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "4294967295", "--", "true"); status != 2 {
			t.Fatalf("exit status = %d, want 2", status)
		}
		if len(fake.createRequests()) != 0 {
			t.Fatal("sandbox created despite the TTL guard")
		}
	})
	t.Run("discovered timeout", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.infoFunc = func(w http.ResponseWriter, _ *http.Request) {
			execTestWriteInfo(t, w, uint64(execMaxTimeoutSeconds), 600)
		}
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "0", "--", "true"); status != 2 {
			t.Fatalf("exit status = %d, want 2", status)
		}
		if fake.infoRequestCount() != 1 {
			t.Fatalf("metadata requests = %d, want 1", fake.infoRequestCount())
		}
		if len(fake.createRequests()) != 0 {
			t.Fatal("sandbox created despite the TTL guard")
		}
	})
}

// TestRunCommandLine proves one argument is verbatim and argv is quoted.
func TestRunCommandLine(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "single line", args: []string{"echo \"a b\""}, want: "echo \"a b\""},
		{name: "argv", args: []string{"echo", "a b", "it's"}, want: `'echo' 'a b' 'it'\''s'`},
		{name: "leading dash", args: []string{"-x"}, want: "-x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := runTestNewAPI(t)
			fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, request api.CommandRequest) bool {
				if request.Command != tc.want {
					t.Error("command line does not match the expected shell encoding")
				}
				runTestWriteStream(t, w, runTestFrames("", "", new(0), false))
				return true
			}
			env := runTestNewEnv(t, fake, nil)
			args := append([]string{"run", "--timeout", "30", "--"}, tc.args...)
			if status := env.run(t, args...); status != 0 {
				t.Fatalf("exit status = %d, stderr=%q", status, env.stderr.String())
			}
		})
	}
}

// TestRunRawOutputWhenPiped proves non-TTY stdout stays raw without --json.
func TestRunRawOutputWhenPiped(t *testing.T) {
	fake := runTestNewAPI(t)
	fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
		runTestWriteStream(t, w, runTestFrames("hello\n", "warn\n", new(0), false))
		return true
	}
	env := runTestNewEnv(t, fake, nil)
	if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 0 {
		t.Fatalf("exit status = %d", status)
	}
	if got := env.stdout.String(); got != "hello\n" {
		t.Fatalf("stdout = %q, want raw output", got)
	}
	if !strings.HasPrefix(env.stderr.String(), "warn\n") {
		t.Fatalf("stderr = %q, want raw command stderr first", env.stderr.String())
	}
	if len(runTestSummaryLines(env.stderr.String())) != 1 {
		t.Fatalf("stderr = %q, want one summary", env.stderr.String())
	}
}

// TestRunExplicitJSON proves JSON is opt-in and ends in exactly one summary.
func TestRunExplicitJSON(t *testing.T) {
	fake := runTestNewAPI(t)
	fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
		runTestWriteStream(t, w, runTestFrames("out", "err", new(7), false))
		return true
	}
	env := runTestNewEnv(t, fake, nil)
	if status := env.run(t, "run", "--json", "--timeout", "30", "--", "true"); status != 7 {
		t.Fatalf("exit status = %d, want 7; stderr=%q", status, env.stderr.String())
	}
	if env.stderr.String() != "" {
		t.Fatalf("stderr = %q, want no human summary in JSON mode", env.stderr.String())
	}
	lines := cliTestJSONLines(t, env.stdout)
	if len(lines) != 5 {
		t.Fatalf("JSON lines = %d, want started/stdout/stderr/exit/summary: %v", len(lines), lines)
	}
	summary := lines[len(lines)-1]
	if summary["event"] != "summary" {
		t.Fatalf("last event = %v, want summary", summary["event"])
	}
	if len(summary) != 6 {
		t.Fatalf("summary fields = %v, want exactly the reference fields", summary)
	}
	if summary["exitCode"] != float64(7) || summary["dollars"] != float64(0) ||
		summary["sandboxType"] != "default" || summary["deleted"] != true {
		t.Fatalf("summary = %v", summary)
	}
	if _, ok := summary["elapsedSeconds"].(float64); !ok {
		t.Fatalf("elapsedSeconds = %v, want a number", summary["elapsedSeconds"])
	}
	count := 0
	for _, line := range lines {
		if line["event"] == "summary" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("summary count = %d, want 1", count)
	}
}

// TestRunRemoteExitCodes covers success, a remote failure, a signal and a
// timeout with the matching process status and summary.
func TestRunRemoteExitCodes(t *testing.T) {
	cases := []struct {
		name     string
		frames   string
		wantCode int
	}{
		{name: "zero", frames: runTestFrames("", "", new(0), false), wantCode: 0},
		{name: "seven", frames: runTestFrames("", "", new(7), false), wantCode: 7},
		{name: "signal", frames: runTestFrames("", "", nil, false), wantCode: 1},
		{name: "timeout", frames: runTestFrames("", "", nil, true), wantCode: 124},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := runTestNewAPI(t)
			fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
				runTestWriteStream(t, w, tc.frames)
				return true
			}
			env := runTestNewEnv(t, fake, nil)
			if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != tc.wantCode {
				t.Fatalf("exit status = %d, want %d; stderr=%q", status, tc.wantCode, env.stderr.String())
			}
			if len(fake.deleteRequests()) != 1 {
				t.Fatal("sandbox was not deleted")
			}
			want := fmt.Sprintf("exit %d ·", tc.wantCode)
			lines := runTestSummaryLines(env.stderr.String())
			if len(lines) != 1 || !strings.HasPrefix(lines[0], want) {
				t.Fatalf("summary = %v, want one line starting %q", lines, want)
			}
		})
	}
}

// TestRunFailuresStillCleanup covers every post-create failure path.
func TestRunFailuresStillCleanup(t *testing.T) {
	t.Run("ready error", func(t *testing.T) {
		fake := runTestNewAPI(t)
		reason := "boot failed"
		fake.sandboxError = &reason
		fake.states = []api.SandboxState{api.StateError}
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if len(fake.commandRequests()) != 0 {
			t.Fatal("command ran after a ready failure")
		}
		if len(fake.deleteRequests()) != 1 {
			t.Fatal("sandbox was not cleaned up")
		}
		if !strings.Contains(env.stderr.String(), reason) {
			t.Fatalf("stderr = %q, want the sandbox error", env.stderr.String())
		}
	})

	t.Run("push failure", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.uploadBase = ""
		source := pushTestSource(t, map[string]string{"file.txt": "x"})
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--push", source, "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if len(fake.commandRequests()) != 0 {
			t.Fatal("command ran after a push failure")
		}
		if len(fake.deleteRequests()) != 1 {
			t.Fatal("sandbox was not cleaned up")
		}
	})

	t.Run("exec failure", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
			cliTestWriteJSON(t, w, http.StatusInternalServerError,
				cliTestErrorEnvelope(http.StatusInternalServerError, "http_500", "boom"))
			return true
		}
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if len(fake.deleteRequests()) != 1 {
			t.Fatal("sandbox was not cleaned up")
		}
	})

	t.Run("output write failure", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
			runTestWriteStream(t, w, runTestFrames("lost\n", "", new(0), false))
			return true
		}
		writer := new(runTestFailWriter)
		env := runTestNewEnv(t, fake, func(opts *Options) { opts.Stdout = writer })
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if writer.calls.Load() == 0 {
			t.Fatal("the injected writer never saw command output")
		}
		if len(fake.deleteRequests()) != 1 {
			t.Fatal("sandbox was not cleaned up after the write failure")
		}
		if len(runTestSummaryLines(env.stderr.String())) != 1 {
			t.Fatalf("stderr = %q, want exactly one summary", env.stderr.String())
		}
	})
}

// TestRunPushThenRun proves the pushed directory becomes the default cwd and
// an explicit --cwd overrides it. Extraction stays synchronous.
func TestRunPushThenRun(t *testing.T) {
	source := pushTestSource(t, map[string]string{"file.txt": "hello\n"})
	base := filepath.Base(source)

	cases := []struct {
		name string
		cwd  string
		want string
	}{
		{name: "pushed directory", want: "/home/user/" + base},
		{name: "explicit cwd", cwd: "work", want: "work"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture := new(pushTestCapture)
			upload := httptest.NewServer(pushTestTarHandler(t, capture, http.StatusNoContent))
			t.Cleanup(upload.Close)
			fake := runTestNewAPI(t)
			fake.uploadBase = upload.URL
			var mu sync.Mutex
			var commands []api.CommandRequest
			fake.commandFunc = func(w http.ResponseWriter, r *http.Request, request api.CommandRequest) bool {
				mu.Lock()
				commands = append(commands, request)
				mu.Unlock()
				if request.Stream != nil && *request.Stream {
					if request.Cwd == nil || *request.Cwd != tc.want {
						t.Error("user command cwd does not match the expected directory")
					}
					runTestWriteStream(t, w, runTestFrames("", "", new(0), false))
					return true
				}
				if request.TimeoutSeconds == nil || *request.TimeoutSeconds != pushExtractTimeoutSeconds {
					t.Errorf("extraction timeout = %v", request.TimeoutSeconds)
				}
				code := 0
				cliTestWriteJSON(t, w, http.StatusOK, api.NewCommandResponse(true, &code, "", "", false))
				return true
			}
			env := runTestNewEnv(t, fake, nil)
			args := []string{"run", "--push", source, "--timeout", "30"}
			if tc.cwd != "" {
				args = append(args, "--cwd", tc.cwd)
			}
			args = append(args, "--", "true")
			if status := env.run(t, args...); status != 0 {
				t.Fatalf("exit status = %d, stderr=%q", status, env.stderr.String())
			}
			if capture.pushTestUploads() != 1 {
				t.Fatalf("uploads = %d, want 1", capture.pushTestUploads())
			}
			if len(fake.uploadPathRequests()) != 1 {
				t.Fatalf("upload URL requests = %d, want 1", len(fake.uploadPathRequests()))
			}
			if fake.infoRequestCount() != 1 {
				t.Fatal("push extraction metadata probe count is not exactly one")
			}
			mu.Lock()
			defer mu.Unlock()
			if len(commands) != 2 {
				t.Fatalf("commands = %d, want extraction + user command", len(commands))
			}
			if strings.Contains(env.stdout.String(), "pushed") || strings.Contains(env.stderr.String(), "pushed") {
				t.Fatal("push progress leaked into run output")
			}
		})
	}
}

// TestRunCleanupFailures covers blocked, wrong-target and unconfirmed deletion
// operations, including preserving an earlier remote exit status.
func TestRunCleanupFailures(t *testing.T) {
	t.Run("blocked reports not deleted", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.deleteOpStatus = api.DeletionBlocked
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := runTestSummaryLines(env.stderr.String())
		if len(lines) != 1 || !strings.HasPrefix(lines[0], "exit 1 ·") || !strings.Contains(lines[0], "not deleted") {
			t.Fatalf("summary = %v, want one exit 1 not-deleted line", lines)
		}
		if !strings.HasSuffix(strings.TrimSpace(env.stderr.String()), lines[0]) {
			t.Fatal("summary is not the last stderr line")
		}
		if !strings.Contains(env.stderr.String(), "blocked") {
			t.Fatalf("stderr = %q, want the blocked diagnostic", env.stderr.String())
		}
		runTestAssertBefore(t, env.stderr.String(), "blocked", "exit 1 ·")
	})

	t.Run("wrong target not deleted", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.opTarget = cliTestSandboxID2
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := runTestSummaryLines(env.stderr.String())
		if len(lines) != 1 || !strings.HasPrefix(lines[0], "exit 1 ·") || !strings.Contains(lines[0], "not deleted") {
			t.Fatalf("summary = %v, want one exit 1 not-deleted line", lines)
		}
	})

	t.Run("remote code preserved", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.deleteOpStatus = api.DeletionBlocked
		fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
			runTestWriteStream(t, w, runTestFrames("", "", new(7), false))
			return true
		}
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 7 {
			t.Fatalf("exit status = %d, want 7", status)
		}
		lines := runTestSummaryLines(env.stderr.String())
		if len(lines) != 1 || !strings.HasPrefix(lines[0], "exit 7 ·") || !strings.Contains(lines[0], "not deleted") {
			t.Fatalf("summary = %v, want exit 7 and not deleted", lines)
		}
	})

	t.Run("json cleanup failure", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.deleteOpStatus = api.DeletionBlocked
		env := runTestNewEnv(t, fake, nil)
		status := env.run(t, "run", "--json", "--timeout", "30", "--", "true")
		if status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) == 0 || lines[len(lines)-1]["event"] != "summary" {
			t.Fatalf("last event = %v, want the terminal summary", lines)
		}
		summaries := 0
		errorsSeen := 0
		for _, line := range lines {
			switch line["event"] {
			case "summary":
				summaries++
				if line["exitCode"] != float64(status) {
					t.Fatalf("summary exitCode = %v, want process status %d", line["exitCode"], status)
				}
				if line["deleted"] != false {
					t.Fatalf("summary claims deletion: %v", line)
				}
			case "error":
				errorsSeen++
				if line["code"] != "deletion_blocked" {
					t.Fatalf("error code = %v, want deletion_blocked", line["code"])
				}
			}
		}
		if summaries != 1 || errorsSeen != 1 {
			t.Fatalf("events = %v, want one error and one summary", lines)
		}
	})

	t.Run("target changes between polls", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.deleteOpStatus = api.DeletionPending
		var polls atomic.Int64
		fake.operationFunc = func(w http.ResponseWriter, _ *http.Request, id string) bool {
			target := cliTestSandboxID
			status := api.DeletionProcessing
			if polls.Add(1) >= 2 {
				target = cliTestSandboxID2
				status = api.DeletionCompleted
			}
			cliTestWriteJSON(t, w, http.StatusOK, api.NewDeletionOperationResponse(
				cliTestOperation(id, target, status),
			))
			return true
		}
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := runTestSummaryLines(env.stderr.String())
		if len(lines) != 1 || !strings.HasPrefix(lines[0], "exit 1 ·") || !strings.Contains(lines[0], "not deleted") {
			t.Fatalf("summary = %v, want one exit 1 not-deleted line", lines)
		}
		if polls.Load() < 2 {
			t.Fatalf("operation polls = %d, want the changed target to be observed", polls.Load())
		}
	})

	t.Run("completed delete needs no poll", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.deleteOpStatus = api.DeletionCompleted
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 0 {
			t.Fatalf("exit status = %d, want 0; stderr=%q", status, env.stderr.String())
		}
		if fake.operationRequestCount() != 0 {
			t.Fatalf("operation polls = %d, want 0", fake.operationRequestCount())
		}
	})
}

// TestRunCleanupPollTimeout proves a pending operation never claims deletion.
func TestRunCleanupPollTimeout(t *testing.T) {
	fake := runTestNewAPI(t)
	fake.deleteOpStatus = api.DeletionPending
	fake.opStatuses = []api.DeletionStatus{api.DeletionPending}
	env := runTestNewEnv(t, fake, nil)
	client, err := NewClient(env.server.URL, "test-key", env.server.Client())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	deleted, err := env.app.runDeleteSandbox(ctx, client, cliTestSandboxID)
	if deleted {
		t.Fatal("pending cleanup claimed deletion")
	}
	cliErr, ok := errors.AsType[*CLIError](err)
	if !ok || cliErr.Code != "timeout" {
		t.Fatalf("cleanup error = %v, want a timeout CLIError", err)
	}
	if len(fake.deleteRequests()) != 1 {
		t.Fatalf("delete requests = %d, want 1", len(fake.deleteRequests()))
	}
}

// TestRunAmbiguousCreate covers an unenveloped 5xx, a transport failure and a
// definite 4xx. A validated 5xx error envelope is covered separately as a
// definite refusal.
func TestRunAmbiguousCreate(t *testing.T) {
	t.Run("unenveloped server error is ambiguous", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.createFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CreateSandboxRequest) bool {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, "<html>sentinel-body</html>")
			return true
		}
		env := runTestNewEnv(t, fake, nil)
		status := env.run(t, "run", "--timeout", "30", "--", "true")
		if strings.Contains(env.stderr.String(), "sentinel-body") {
			t.Fatal("server body leaked into the diagnostic")
		}
		if status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if !strings.Contains(env.stderr.String(), "could not be confirmed") {
			t.Fatalf("stderr = %q, want the ambiguity explanation", env.stderr.String())
		}
		if strings.Contains(env.stderr.String(), "bx_") {
			t.Fatal("ambiguous create invented a sandbox id")
		}
		if len(fake.deleteRequests()) != 0 {
			t.Fatal("delete sent without a sandbox id")
		}
		if len(fake.creates) != 1 {
			t.Fatalf("create requests = %d, want exactly 1", len(fake.creates))
		}
	})

	t.Run("transport failure is ambiguous", func(t *testing.T) {
		fake := runTestNewAPI(t)
		env := runTestNewEnv(t, fake, func(opts *Options) {
			base := opts.HTTPClient.Transport
			opts.HTTPClient = &http.Client{Transport: runTestFailingTransport{
				base: base,
				fail: func(r *http.Request) bool {
					return r.Method == http.MethodPost && r.URL.Path == api.PathSandboxes
				},
			}}
		})
		status := env.run(t, "run", "--timeout", "30", "--", "true")
		if strings.Contains(env.stderr.String(), "sentinel transport failure") {
			t.Fatal("transport sentinel leaked into the diagnostic")
		}
		if status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if !strings.Contains(env.stderr.String(), "could not be confirmed") {
			t.Fatalf("stderr = %q, want the ambiguity explanation", env.stderr.String())
		}
		if len(fake.deleteRequests()) != 0 {
			t.Fatal("delete sent without a sandbox id")
		}
	})

	t.Run("4xx is definite", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.createFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CreateSandboxRequest) bool {
			cliTestWriteJSON(t, w, http.StatusBadRequest,
				cliTestErrorEnvelope(http.StatusBadRequest, "invalid_json", "bad create"))
			return true
		}
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if strings.Contains(env.stderr.String(), "could not be confirmed") {
			t.Fatalf("stderr = %q, want a definite rejection", env.stderr.String())
		}
		if !strings.Contains(env.stderr.String(), "bad create") {
			t.Fatalf("stderr = %q, want the API message", env.stderr.String())
		}
	})
}

// TestRunAmbiguousUnenvelopedFailures proves only a validated API error
// envelope is definite. Arbitrary, malformed or inconsistent 5xx bodies, and a
// missing or wrong ok, remain ambiguous without leaking the body.
func TestRunAmbiguousUnenvelopedFailures(t *testing.T) {
	valid := func(status int) string {
		return `{"ok":false,"type":"sandbox.error","status":` + strconv.Itoa(status) + `,"code":"out_of_capacity","message":"no capacity is available right now","error":{"status":` + strconv.Itoa(status) + `,"code":"out_of_capacity","message":"no capacity is available right now"},"requestId":"req_test"}`
	}
	cases := []struct {
		name string
		body string
	}{
		{name: "plain html", body: "<html>sentinel-body</html>"},
		{name: "arbitrary json", body: `{"error":"sentinel-body","message":"sentinel-body"}`},
		{name: "malformed json", body: `{"ok":false,"type":"sandbox.error"`},
		{name: "missing nested envelope", body: `{"ok":false,"type":"sandbox.error","status":503,"code":"out_of_capacity","message":"sentinel-body"}`},
		{name: "missing request id", body: strings.Replace(valid(http.StatusServiceUnavailable), `,"requestId":"req_test"`, "", 1)},
		{name: "null request id", body: strings.Replace(valid(http.StatusServiceUnavailable), `"req_test"`, "null", 1)},
		{name: "missing ok", body: strings.Replace(valid(http.StatusServiceUnavailable), `"ok":false,`, "", 1)},
		{name: "null ok", body: strings.Replace(valid(http.StatusServiceUnavailable), `"ok":false`, `"ok":null`, 1)},
		{name: "true ok", body: strings.Replace(valid(http.StatusServiceUnavailable), `"ok":false`, `"ok":true`, 1)},
		{name: "top status mismatch", body: strings.Replace(valid(http.StatusServiceUnavailable), `"status":503`, `"status":502`, 1)},
		{name: "nested status mismatch", body: strings.Replace(valid(http.StatusServiceUnavailable), `"error":{"status":503`, `"error":{"status":502`, 1)},
		{name: "nested code mismatch", body: strings.Replace(valid(http.StatusServiceUnavailable), `"error":{"status":503,"code":"out_of_capacity"`, `"error":{"status":503,"code":"no_ready_machine"`, 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := runTestNewAPI(t)
			fake.createFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CreateSandboxRequest) bool {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, tc.body)
				return true
			}
			env := runTestNewEnv(t, fake, nil)
			if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
				t.Fatalf("exit status = %d, want 1", status)
			}
			if strings.Contains(env.stderr.String(), "sentinel-body") {
				t.Fatal("response body leaked into the diagnostic")
			}
			if !strings.Contains(env.stderr.String(), "could not be confirmed") {
				t.Fatal("stderr lacks the ambiguity explanation")
			}
			if len(fake.creates) != 1 {
				t.Fatalf("create requests = %d, want exactly 1", len(fake.creates))
			}
			if len(fake.deleteRequests()) != 0 {
				t.Fatal("delete sent without a sandbox id")
			}
		})
	}
}

// TestRunDefiniteCreateRefusal proves a validated API error envelope is a
// definite refusal even for a 5xx, printing the code, safe message and only a
// whitelisted retry hint; invalid hints are omitted without changing that.
func TestRunDefiniteCreateRefusal(t *testing.T) {
	const message = "no capacity is available right now"
	writeEnvelope := func(fake *runTestAPI, status int, code, bodyMessage string, details map[string]any) {
		fake.createFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CreateSandboxRequest) bool {
			cliTestWriteJSON(t, w, status, cliTestErrorEnvelopeWithDetails(status, code, bodyMessage, details))
			return true
		}
	}

	t.Run("human out of capacity with retry hint", func(t *testing.T) {
		fake := runTestNewAPI(t)
		writeEnvelope(fake, http.StatusServiceUnavailable, "out_of_capacity", message, map[string]any{"retryAfterSeconds": 5})
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		stderr := env.stderr.String()
		for _, want := range []string{"out_of_capacity", message, "retry after 5 seconds"} {
			if !strings.Contains(stderr, want) {
				t.Fatalf("human diagnostic missing %q", want)
			}
		}
		if strings.Contains(stderr, "could not be confirmed") {
			t.Fatal("human diagnostic claimed an ambiguous create")
		}
		if strings.Contains(stderr, "bx_") {
			t.Fatal("definite refusal invented a sandbox id")
		}
		if len(fake.creates) != 1 {
			t.Fatalf("create requests = %d, want exactly 1", len(fake.creates))
		}
		if len(fake.deleteRequests()) != 0 {
			t.Fatal("delete sent without a sandbox id")
		}
		if lines := runTestSummaryLines(stderr); len(lines) != 0 {
			t.Fatalf("summary lines = %d, want none before a sandbox exists", len(lines))
		}
	})

	t.Run("json no ready machine with retry hint", func(t *testing.T) {
		fake := runTestNewAPI(t)
		writeEnvelope(fake, http.StatusServiceUnavailable, "no_ready_machine", "no machine of this type is ready", map[string]any{"retryAfterSeconds": 7})
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "--json", "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 {
			t.Fatalf("json records = %d, want one error and no summary", len(lines))
		}
		event := lines[0]
		if event["event"] != "error" {
			t.Fatal("terminal json record is not an error event")
		}
		if event["code"] != "no_ready_machine" {
			t.Fatal("error event lost the API code")
		}
		if event["status"] != float64(http.StatusServiceUnavailable) {
			t.Fatal("error event lost the HTTP status")
		}
		if event["error"] != "no machine of this type is ready" {
			t.Fatal("error event lost the safe API message")
		}
		if event["retryAfterSeconds"] != float64(7) {
			t.Fatal("error event lost the retry hint")
		}
	})

	t.Run("valid 500 envelope is definite", func(t *testing.T) {
		fake := runTestNewAPI(t)
		writeEnvelope(fake, http.StatusInternalServerError, "http_500", "internal server error", nil)
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "--json", "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["code"] != "http_500" {
			t.Fatalf("json records = %d, want one definite http_500 refusal", len(lines))
		}
		if strings.Contains(env.stderr.String(), "could not be confirmed") {
			t.Fatal("valid 500 envelope was reported as ambiguous")
		}
	})

	t.Run("envelope without hint stays definite", func(t *testing.T) {
		fake := runTestNewAPI(t)
		writeEnvelope(fake, http.StatusServiceUnavailable, "out_of_capacity", message, nil)
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "--json", "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["code"] != "out_of_capacity" {
			t.Fatalf("json records = %d, want one definite refusal", len(lines))
		}
		if _, ok := lines[0]["retryAfterSeconds"]; ok {
			t.Fatal("error event invented a retry hint")
		}
	})

	hints := []struct {
		name  string
		value any
	}{
		{name: "string", value: "sentinel-hint"},
		{name: "zero", value: 0},
		{name: "negative", value: -1},
		{name: "fractional", value: 1.5},
		{name: "overflow", value: jsontext.Value("99999999999999999999")},
	}
	for _, tc := range hints {
		t.Run("invalid hint "+tc.name, func(t *testing.T) {
			fake := runTestNewAPI(t)
			writeEnvelope(fake, http.StatusServiceUnavailable, "out_of_capacity", message, map[string]any{"retryAfterSeconds": tc.value})
			env := runTestNewEnv(t, fake, nil)
			if status := env.run(t, "--json", "run", "--timeout", "30", "--", "true"); status != 1 {
				t.Fatalf("exit status = %d, want 1", status)
			}
			if strings.Contains(env.stdout.String(), "sentinel-hint") {
				t.Fatal("invalid hint leaked into the event")
			}
			lines := cliTestJSONLines(t, env.stdout)
			if len(lines) != 1 || lines[0]["code"] != "out_of_capacity" {
				t.Fatalf("json records = %d, want one definite refusal", len(lines))
			}
			if _, ok := lines[0]["retryAfterSeconds"]; ok {
				t.Fatal("error event kept an invalid retry hint")
			}
		})
	}
}

// TestRunMalformedResponses labels the negative fixtures explicitly: a lost
// create body stays ambiguous, a malformed poll still cleans up.
func TestRunMalformedResponses(t *testing.T) {
	t.Run("malformed create response", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.createFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CreateSandboxRequest) bool {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, "{")
			return true
		}
		env := runTestNewEnv(t, fake, nil)
		status := env.run(t, "run", "--timeout", "30", "--", "true")
		if status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if !strings.Contains(env.stderr.String(), "could not be confirmed") {
			t.Fatalf("stderr = %q, want the ambiguity explanation", env.stderr.String())
		}
		if len(fake.deleteRequests()) != 0 {
			t.Fatal("delete sent without a sandbox id")
		}
	})

	t.Run("malformed ready response", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.getFunc = func(w http.ResponseWriter, _ *http.Request, _ string) bool {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "{")
			return true
		}
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if len(fake.deleteRequests()) != 1 {
			t.Fatal("sandbox was not cleaned up")
		}
		if lines := runTestSummaryLines(env.stderr.String()); len(lines) != 1 || !strings.HasPrefix(lines[0], "exit 1 ·") {
			t.Fatalf("summary = %v, want one exit 1 line", lines)
		}
	})

	t.Run("empty create id", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.createFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CreateSandboxRequest) bool {
			ttl := api.DefaultTTLSeconds
			sandbox := cliTestSandbox("", api.StateProvisioning)
			sandbox.Type = "default"
			cliTestWriteJSON(t, w, http.StatusAccepted,
				api.NewCreateSandboxResponse(api.CreateProvisioning, &ttl, sandbox))
			return true
		}
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if !strings.Contains(env.stderr.String(), "could not be confirmed") {
			t.Fatalf("stderr = %q, want the ambiguity explanation", env.stderr.String())
		}
		if len(fake.deleteRequests()) != 0 {
			t.Fatal("delete sent without a sandbox id")
		}
	})
}

// TestRunCancellation covers Ctrl-C during create, ready, push and execute.
func TestRunCancellation(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		fake := runTestNewAPI(t)
		started := make(chan struct{})
		fake.createFunc = func(_ http.ResponseWriter, r *http.Request, _ api.CreateSandboxRequest) bool {
			close(started)
			<-r.Context().Done()
			return true
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		env := runTestNewEnv(t, fake, nil)
		done, finished := runTestStartCommand(t, env, ctx, "run", "--timeout", "30", "--", "true")
		waitRunTestSignal(t, started, "create")
		cancel()
		status := waitRunTestStatus(t, done)
		<-finished
		if strings.Contains(env.stderr.String(), "bx_") {
			t.Fatal("canceled create invented a sandbox id")
		}
		if status != 130 {
			t.Fatalf("exit status = %d, want 130", status)
		}
		if len(fake.deleteRequests()) != 0 {
			t.Fatal("delete sent without a known id")
		}
		if !strings.Contains(env.stderr.String(), "could not be confirmed") {
			t.Fatalf("stderr = %q, want the safe ambiguity explanation after cancellation", env.stderr.String())
		}
	})

	t.Run("ready", func(t *testing.T) {
		fake := runTestNewAPI(t)
		started := make(chan struct{})
		fake.getFunc = func(_ http.ResponseWriter, r *http.Request, _ string) bool {
			close(started)
			<-r.Context().Done()
			return true
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		env := runTestNewEnv(t, fake, nil)
		done, finished := runTestStartCommand(t, env, ctx, "run", "--timeout", "30", "--", "true")
		waitRunTestSignal(t, started, "ready")
		cancel()
		if status := waitRunTestStatus(t, done); status != 130 {
			t.Fatalf("exit status = %d, want 130", status)
		}
		<-finished
		if len(fake.deleteRequests()) != 1 {
			t.Fatal("sandbox was not cleaned up after cancellation")
		}
		if lines := runTestSummaryLines(env.stderr.String()); len(lines) != 1 || !strings.HasPrefix(lines[0], "exit 130 ·") {
			t.Fatalf("summary = %v, want one exit 130 line", lines)
		}
	})

	t.Run("push", func(t *testing.T) {
		fake := runTestNewAPI(t)
		started := make(chan struct{})
		fake.uploadURLFunc = func(_ http.ResponseWriter, r *http.Request) bool {
			close(started)
			<-r.Context().Done()
			return true
		}
		source := pushTestSource(t, map[string]string{"file.txt": "x"})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		env := runTestNewEnv(t, fake, nil)
		done, finished := runTestStartCommand(t, env, ctx, "run", "--push", source, "--timeout", "30", "--", "true")
		waitRunTestSignal(t, started, "push")
		cancel()
		if status := waitRunTestStatus(t, done); status != 130 {
			t.Fatalf("exit status = %d, want 130", status)
		}
		<-finished
		if len(fake.deleteRequests()) != 1 {
			t.Fatal("sandbox was not cleaned up after push cancellation")
		}
	})

	t.Run("execute", func(t *testing.T) {
		fake := runTestNewAPI(t)
		started := make(chan struct{})
		fake.commandFunc = func(w http.ResponseWriter, r *http.Request, _ api.CommandRequest) bool {
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "{\"type\":\"started\"}\n")
			w.(http.Flusher).Flush()
			close(started)
			<-r.Context().Done()
			return true
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		env := runTestNewEnv(t, fake, nil)
		done, finished := runTestStartCommand(t, env, ctx, "run", "--timeout", "30", "--", "true")
		waitRunTestSignal(t, started, "execute")
		cancel()
		if status := waitRunTestStatus(t, done); status != 130 {
			t.Fatalf("exit status = %d, want 130", status)
		}
		<-finished
		if len(fake.deleteRequests()) != 1 {
			t.Fatal("sandbox was not cleaned up after execution cancellation")
		}
	})
}

// TestRunCancelDuringCleanup proves Ctrl-C after a successful command still
// finishes deletion, then exits 130; an earlier remote nonzero or 124 is
// preserved.
func TestRunCancelDuringCleanup(t *testing.T) {
	cases := []struct {
		name     string
		frames   string
		wantCode int
	}{
		{name: "success", frames: runTestFrames("", "", new(0), false), wantCode: 130},
		{name: "remote seven", frames: runTestFrames("", "", new(7), false), wantCode: 7},
		{name: "remote timeout", frames: runTestFrames("", "", nil, true), wantCode: 124},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := runTestNewAPI(t)
			deleteStarted := make(chan struct{})
			releaseDelete := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseDelete) }) }
			fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
				runTestWriteStream(t, w, tc.frames)
				return true
			}
			fake.deleteFunc = func(w http.ResponseWriter, _ *http.Request, id string) bool {
				close(deleteStarted)
				<-releaseDelete
				cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSandboxDeletingResponse(
					cliTestOperation(cliTestOperationID, id, api.DeletionPending),
				))
				return true
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			env := runTestNewEnv(t, fake, nil)
			done, finished := runTestStartCommand(t, env, ctx, "run", "--timeout", "30", "--", "true")
			// Registered after runTestStartCommand so this runs before its join
			// cleanup and before httptest's server.Close. Cleanup uses
			// WithoutCancel, so the barrier must always be released.
			t.Cleanup(func() {
				release()
				cancel()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Errorf("cleanup command goroutine did not join")
				}
			})
			waitRunTestSignal(t, deleteStarted, "cleanup")
			cancel()
			release()
			if status := waitRunTestStatus(t, done); status != tc.wantCode {
				t.Fatalf("exit status = %d, want %d", status, tc.wantCode)
			}
			<-finished
			if len(fake.deleteRequests()) != 1 {
				t.Fatal("cleanup did not finish after cancellation")
			}
			lines := runTestSummaryLines(env.stderr.String())
			want := fmt.Sprintf("exit %d ·", tc.wantCode)
			if len(lines) != 1 || !strings.HasPrefix(lines[0], want) || !strings.Contains(lines[0], "deleted") {
				t.Fatalf("summary = %v, want one %q with confirmed deletion", lines, want)
			}
		})
	}
}

// runTestStartCommand runs one command in a goroutine and gives the test a
// completion channel for immediate cancel-and-join on every fatal path.
func runTestStartCommand(t *testing.T, env *cliTestEnv, ctx context.Context, args ...string) (<-chan int, <-chan struct{}) {
	t.Helper()
	done := make(chan int, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		done <- env.runContext(ctx, args...)
	}()
	t.Cleanup(func() {
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Errorf("run command goroutine did not join")
		}
	})
	return done, finished
}

// waitRunTestSignal waits for a handler to announce it is blocked.
func waitRunTestSignal(t *testing.T, signal <-chan struct{}, phase string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s phase did not start", phase)
	}
}

// waitRunTestStatus waits for a command goroutine to return.
func waitRunTestStatus(t *testing.T, done <-chan int) int {
	t.Helper()
	select {
	case status := <-done:
		return status
	case <-time.After(5 * time.Second):
		t.Fatal("run command did not return")
		return 0
	}
}

// TestRunNoDuplicateSummaryOnFailures proves output and cleanup failures never
// append a second terminal summary or claim deletion.
func TestRunNoDuplicateSummaryOnFailures(t *testing.T) {
	t.Run("output failure", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
			runTestWriteStream(t, w, runTestFrames("lost\n", "", new(0), false))
			return true
		}
		env := runTestNewEnv(t, fake, func(opts *Options) { opts.Stdout = new(runTestFailWriter) })
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := runTestSummaryLines(env.stderr.String())
		if len(lines) != 1 || !strings.HasPrefix(lines[0], "exit 1 ·") {
			t.Fatalf("summary = %v, want one exit 1 line matching the process status", lines)
		}
		if count := strings.Count(env.stderr.String(), " · $0 · "); count != 1 {
			t.Fatalf("summary count = %d, want 1; stderr=%q", count, env.stderr.String())
		}
		if !strings.Contains(env.stderr.String(), "deleted") || strings.Contains(env.stderr.String(), "not deleted") {
			t.Fatalf("stderr = %q, want confirmed deletion", env.stderr.String())
		}
	})

	t.Run("cleanup failure", func(t *testing.T) {
		fake := runTestNewAPI(t)
		fake.deleteOpStatus = api.DeletionBlocked
		fake.commandFunc = func(w http.ResponseWriter, _ *http.Request, _ api.CommandRequest) bool {
			runTestWriteStream(t, w, runTestFrames("", "", new(7), false))
			return true
		}
		env := runTestNewEnv(t, fake, nil)
		if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 7 {
			t.Fatalf("exit status = %d, want the remote 7", status)
		}
		if count := strings.Count(env.stderr.String(), " · $0 · "); count != 1 {
			t.Fatalf("summary count = %d, want 1; stderr=%q", count, env.stderr.String())
		}
		if !strings.Contains(env.stderr.String(), "blocked") {
			t.Fatalf("stderr = %q, want the cleanup diagnostic", env.stderr.String())
		}
		lines := runTestSummaryLines(env.stderr.String())
		if len(lines) != 1 || !strings.HasPrefix(lines[0], "exit 7 ·") {
			t.Fatalf("summary = %v, want the remote exit 7", lines)
		}
		if !strings.HasSuffix(strings.TrimSpace(env.stderr.String()), lines[0]) {
			t.Fatal("summary is not the last stderr line")
		}
		runTestAssertBefore(t, env.stderr.String(), "blocked", "exit 7 ·")
	})
}

// TestRunReadyTypeMismatch proves a changed type is rejected instead of being
// reported in the summary.
func TestRunReadyTypeMismatch(t *testing.T) {
	fake := runTestNewAPI(t)
	fake.sandboxType = "default"
	fake.getFunc = func(w http.ResponseWriter, _ *http.Request, _ string) bool {
		sandbox := cliTestSandbox(cliTestSandboxID, api.StateReady)
		sandbox.Type = "xlarge"
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
		return true
	}
	env := runTestNewEnv(t, fake, nil)
	if status := env.run(t, "run", "--timeout", "30", "--", "true"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if !strings.Contains(env.stderr.String(), "type changed") {
		t.Fatalf("stderr = %q, want the type diagnostic", env.stderr.String())
	}
	if len(fake.deleteRequests()) != 1 {
		t.Fatal("sandbox was not cleaned up")
	}
}

// TestRunWriteSummaryFormatting calls the production writer and pins the
// reference wording, including the not-deleted alternative and JSON shape.
func TestRunWriteSummaryFormatting(t *testing.T) {
	cases := []struct {
		name    string
		code    int
		elapsed time.Duration
		disp    runDisposition
		want    string
	}{
		{
			name: "minutes", code: 0, elapsed: 125 * time.Second, disp: runDeleted,
			want: "exit 0 · 2m 5s · small · $0 · " + cliTestSandboxID + " deleted\n",
		},
		{
			name: "subsecond", code: 0, elapsed: 500 * time.Millisecond, disp: runDeleted,
			want: "exit 0 · 0m 0s · small · $0 · " + cliTestSandboxID + " deleted\n",
		},
		{
			name: "kept", code: 0, elapsed: 61 * time.Second, disp: runKept,
			want: "exit 0 · 1m 1s · small · $0 · " + cliTestSandboxID + " kept\n",
		},
		{
			name: "not deleted", code: 1, elapsed: 61 * time.Second, disp: runNotDeleted,
			want: "exit 1 · 1m 1s · small · $0 · " + cliTestSandboxID + " not deleted\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			app := &App{
				opts: Options{Stdout: &stdout, Stderr: &stderr},
				out:  &Output{Stdout: &stdout, Stderr: &stderr},
			}
			if app.runWriteSummary(tc.code, tc.elapsed, "small", cliTestSandboxID, tc.disp) {
				t.Fatal("summary write failed")
			}
			if got := stderr.String(); got != tc.want {
				t.Fatalf("summary = %q, want %q", got, tc.want)
			}
			if stdout.Len() != 0 {
				t.Fatalf("summary wrote to stdout: %q", stdout.String())
			}
		})
	}

	t.Run("json", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		app := &App{
			opts: Options{Stdout: &stdout, Stderr: &stderr},
			out:  &Output{Stdout: &stdout, Stderr: &stderr, ExplicitJSON: true},
		}
		if app.runWriteSummary(7, 90*time.Second, "xlarge", cliTestSandboxID, runNotDeleted) {
			t.Fatal("summary write failed")
		}
		lines := cliTestJSONLines(t, &stdout)
		if len(lines) != 1 {
			t.Fatalf("summary lines = %d, want 1", len(lines))
		}
		summary := lines[0]
		if summary["event"] != "summary" || summary["exitCode"] != float64(7) ||
			summary["elapsedSeconds"] != float64(90) || summary["dollars"] != float64(0) ||
			summary["sandboxType"] != "xlarge" || summary["deleted"] != false {
			t.Fatalf("summary = %v", summary)
		}
		if stderr.Len() != 0 {
			t.Fatalf("JSON summary wrote to stderr: %q", stderr.String())
		}
	})
}
