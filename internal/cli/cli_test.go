package cli

import (
	"bytes"
	"context"
	"crypto/subtle"
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

// Pinned fixture identifiers. Sandbox ids follow ^bx_[23456789abcdefghjkmnpqrstuvwxyz]{8}$,
// deletion operations ^bdop_[a-f0-9]{32}$, and saved snapshot names
// ^[a-z0-9][a-z0-9-]{0,62}$.
const (
	cliTestSandboxID    = "bx_23456789"
	cliTestSandboxID2   = "bx_87654322"
	cliTestOperationID  = "bdop_0123456789abcdef0123456789abcdef"
	cliTestOperationID2 = "bdop_ffffffffffffffffffffffffffffffff"
	cliTestSnapshotID   = "snap-0123456789abcdef0123456789abcdef"
	cliTestSnapshotID2  = "snap-ffffffffffffffffffffffffffffffff"
	cliTestSnapshotName = "web-stack"
)

// cliTestTime is a fixed, valid date-time used in pinned fixtures.
var cliTestTime = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

// cliTestSecretEqual compares key material without branching on its content.
func cliTestSecretEqual(got, want string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// cliTestBarrierReader signals started on the first Read, so tests can cancel
// only after the reader is actually blocked instead of sleeping.
type cliTestBarrierReader struct {
	reader  io.Reader
	started chan struct{}
	once    sync.Once
}

func cliTestNewBarrierReader(r io.Reader) *cliTestBarrierReader {
	return &cliTestBarrierReader{reader: r, started: make(chan struct{})}
}

func (b *cliTestBarrierReader) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	return b.reader.Read(p)
}

func (b *cliTestBarrierReader) Close() error {
	if closer, ok := b.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// cliTestNonComparableReader has a slice field, so its dynamic value is not
// comparable. Comparing two interfaces holding it panics; readWithContext must
// track replacement with a boolean instead.
type cliTestNonComparableReader struct {
	data      []byte
	offset    *int
	started   chan struct{}
	release   chan struct{}
	startOnce *sync.Once
	closeOnce *sync.Once
}

func (r cliTestNonComparableReader) Read(p []byte) (int, error) {
	if r.startOnce != nil {
		r.startOnce.Do(func() { close(r.started) })
	}
	if r.release != nil {
		<-r.release
	}
	if r.offset == nil || *r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[*r.offset:])
	*r.offset += n
	return n, nil
}

func (r cliTestNonComparableReader) Close() error {
	if r.closeOnce != nil {
		r.closeOnce.Do(func() { close(r.release) })
	}
	return nil
}

// cliTestSandbox builds a pinned-shaped sandbox. Required members are always
// set; tests add the optional ones they exercise.
func cliTestSandbox(id string, state api.SandboxState) api.Sandbox {
	return api.Sandbox{
		ID:                id,
		Name:              "test sandbox",
		State:             state,
		DesktopAvailable:  false,
		SnapshotAvailable: false,
	}
}

// cliTestOperation builds a pinned-shaped deletion operation.
func cliTestOperation(id, target string, status api.DeletionStatus) api.DeletionOperation {
	return api.DeletionOperation{
		ID:           id,
		Kind:         api.DeletionKindSandbox,
		TargetID:     target,
		Reason:       api.DeletionReasonExplicit,
		Status:       status,
		AttemptCount: 0,
		RequestedAt:  cliTestTime,
		CompletedAt:  nil,
	}
}

// cliTestSnapshot builds a pinned-shaped saved snapshot.
func cliTestSnapshot(name string, status api.SavedSnapshotStatus) api.SavedSnapshot {
	return api.SavedSnapshot{
		Name:            name,
		Status:          status,
		SourceSandboxID: cliTestSandboxID,
		CreatedAt:       cliTestTime,
	}
}

// cliTestOperationFor maps a typed success DTO to its pinned operation id.
func cliTestOperationFor(value any) (string, bool) {
	switch v := value.(type) {
	case api.MeResponse:
		return "me", true
	case api.CreateSandboxResponse:
		return "create", true
	case api.SandboxInfoResponse:
		return "get", true
	case api.SandboxListResponse:
		return "sandboxes", true
	case api.DeletionOperationResponse:
		if v.Type == api.TypeSandboxDeleting {
			return "deleteSandbox", true
		}
		if v.Type == api.TypeDeletionOperation {
			return "getDeletionOperation", true
		}
		return "", false
	case api.SavedSnapshotSavingResponse:
		return "saveSavedSnapshot", true
	case api.SavedSnapshotInfoResponse:
		return "getSavedSnapshot", true
	case api.SavedSnapshotDeletedResponse:
		return "deleteSavedSnapshot", true
	case api.CommandStartedResponse:
		return "command", true
	case api.CommandStatusResponse:
		return "commandStatus", true
	case api.CommandStopResponse:
		return "stopCommand", true
	default:
		return "", false
	}
}

// cliTestOperationForType maps an emitted envelope type to its pinned
// operation id.
func cliTestOperationForType(typ string) (string, bool) {
	switch typ {
	case api.TypeUserInfo:
		return "me", true
	case api.TypeSandboxCreated:
		return "create", true
	case api.TypeSandboxInfo:
		return "get", true
	case api.TypeSandboxList:
		return "sandboxes", true
	case api.TypeSandboxDeleting:
		return "deleteSandbox", true
	case api.TypeDeletionOperation:
		return "getDeletionOperation", true
	case api.TypeSavedSnapshotSaving:
		return "saveSavedSnapshot", true
	case api.TypeSavedSnapshotInfo:
		return "getSavedSnapshot", true
	case api.TypeSavedSnapshotDeleted:
		return "deleteSavedSnapshot", true
	case api.TypeCommandStarted:
		return "command", true
	case api.TypeCommandStatus:
		return "commandStatus", true
	case api.TypeCommandStopped:
		return "stopCommand", true
	default:
		return "", false
	}
}

// cliTestValidateEmitted validates the actual JSON bytes a command wrote.
func cliTestValidateEmitted(t *testing.T, validator *compat.Validator, status int, raw []byte) {
	t.Helper()
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		t.Fatalf("decode emitted envelope: %v", err)
	}
	operation, ok := cliTestOperationForType(head.Type)
	if !ok {
		t.Fatalf("no pinned operation for emitted type %q", head.Type)
	}
	header := http.Header{"Content-Type": []string{"application/json"}}
	if err := validator.Response(operation, status, header, raw); err != nil {
		t.Errorf("emitted %s response does not match the pin: %v", operation, err)
	}
}

// cliTestValidatePinned validates one typed success fixture against the pinned
// document. Malformed negative fixtures stay outside this helper.
func cliTestValidatePinned(t *testing.T, validator *compat.Validator, status int, value any) {
	t.Helper()
	operation, ok := cliTestOperationFor(value)
	if !ok {
		t.Fatalf("no pinned operation for %T", value)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %T: %v", value, err)
	}
	header := http.Header{"Content-Type": []string{"application/json"}}
	if err := validator.Response(operation, status, header, raw); err != nil {
		t.Errorf("%s response does not match the pin: %v", operation, err)
	}
}

// cliTestEnv is one CLI invocation wired to an httptest server.
type cliTestEnv struct {
	app    *App
	stdout *bytes.Buffer
	stderr *bytes.Buffer
	dir    string
	server *httptest.Server
}

// cliTestNew builds an App whose environment points at handler. mutate may
// adjust the options before the app is built.
func cliTestNew(t *testing.T, handler http.HandlerFunc, mutate func(*Options)) *cliTestEnv {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	dir := t.TempDir()
	opts := Options{
		Stdin:        strings.NewReader(""),
		Stdout:       stdout,
		Stderr:       stderr,
		HTTPClient:   server.Client(),
		ConfigDir:    dir,
		IsTTY:        func(io.Writer) bool { return false },
		PollInterval: time.Millisecond,
		WaitTimeout:  2 * time.Second,
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
	}
	if mutate != nil {
		mutate(&opts)
	}
	return &cliTestEnv{app: NewApp(opts), stdout: stdout, stderr: stderr, dir: dir, server: server}
}

// cliTestRun executes args and returns the process status.
func (e *cliTestEnv) run(t *testing.T, args ...string) int {
	t.Helper()
	e.app.SetArgs(args)
	return e.app.Run(t.Context())
}

// cliTestRunContext executes args with an explicit context.
func (e *cliTestEnv) runContext(ctx context.Context, args ...string) int {
	e.app.SetArgs(args)
	return e.app.Run(ctx)
}

// cliTestJSONLines decodes every JSON Lines object written to stdout.
func cliTestJSONLines(t *testing.T, out *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if line == "" {
			continue
		}
		var object map[string]any
		if err := json.Unmarshal([]byte(line), &object); err != nil {
			t.Fatalf("decode JSON line %q: %v", line, err)
		}
		lines = append(lines, object)
	}
	return lines
}

// cliTestWriteJSON writes a pinned-shaped response. It reports a failure with
// Errorf rather than Fatalf because HTTP handlers run outside the test
// goroutine.
func cliTestWriteJSON(t *testing.T, w http.ResponseWriter, status int, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Errorf("marshal response: %v", err)
		http.Error(w, "test encode failure", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(raw); err != nil {
		t.Errorf("write response: %v", err)
	}
}

// cliTestErrorEnvelope builds the pinned error body.
func cliTestErrorEnvelope(status int, code, message string) map[string]any {
	return map[string]any{
		"ok":        false,
		"type":      "sandbox.error",
		"status":    status,
		"code":      code,
		"message":   message,
		"error":     map[string]any{"status": status, "code": code, "message": message},
		"requestId": "req_test",
	}
}

// cliTestErrorEnvelopeWithDetails builds the pinned error body with an
// explicit error.details object.
func cliTestErrorEnvelopeWithDetails(status int, code, message string, details map[string]any) map[string]any {
	envelope := cliTestErrorEnvelope(status, code, message)
	envelope["error"].(map[string]any)["details"] = details
	return envelope
}

// TestPinnedSuccessFixtures validates every typed success shape the CLI emits
// or accepts against the v1 API document. Negative malformed responses are
// covered separately and deliberately skip this validator.
func TestPinnedSuccessFixtures(t *testing.T) {
	validator := compat.Load(t)
	ttl := 3600
	sandbox := cliTestSandbox(cliTestSandboxID, api.StateProvisioning)
	ready := cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotReady)
	ready.SnapshotID = cliTestSnapshotID
	ready.Type = "small"
	cases := []struct {
		name   string
		status int
		value  any
	}{
		{name: "me", status: http.StatusOK, value: api.NewMeResponse(api.MeUser{ID: "acct_1", Login: "alice"})},
		{name: "create", status: http.StatusAccepted, value: api.NewCreateSandboxResponse(api.CreateProvisioning, &ttl, sandbox)},
		{name: "get", status: http.StatusOK, value: api.NewSandboxInfoResponse(sandbox)},
		{name: "list", status: http.StatusOK, value: api.NewSandboxListResponse([]api.Sandbox{sandbox}, api.NewPageInfo(nil, 100))},
		{name: "delete sandbox", status: http.StatusAccepted, value: api.NewSandboxDeletingResponse(cliTestOperation(cliTestOperationID, cliTestSandboxID, api.DeletionPending))},
		{name: "deletion operation", status: http.StatusOK, value: api.NewDeletionOperationResponse(cliTestOperation(cliTestOperationID, cliTestSandboxID, api.DeletionCompleted))},
		{name: "save snapshot", status: http.StatusAccepted, value: api.NewSavedSnapshotSavingResponse(cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotSaving))},
		{name: "saved snapshot", status: http.StatusOK, value: api.NewSavedSnapshotInfoResponse(ready)},
		{name: "deleted snapshot", status: http.StatusOK, value: api.NewSavedSnapshotDeletedResponse(cliTestSnapshotName)},
		{
			name:   "command started",
			status: http.StatusOK,
			value: api.CommandStartedResponse{
				SuccessBase: api.SuccessBase{OK: true, Type: api.TypeCommandStarted},
				Success:     true,
				CommandID:   "cmd_fixture",
				ProcessID:   7,
				PID:         123,
				Command:     "true",
				StartedAt:   cliTestTime,
			},
		},
		{
			name:   "command status",
			status: http.StatusOK,
			value: api.CommandStatusResponse{
				SuccessBase: api.SuccessBase{OK: true, Type: api.TypeCommandStatus},
				Success:     true,
				ProcessID:   7,
				Status:      "exited",
				Running:     false,
				ExitCode:    new(0),
			},
		},
		{
			name:   "command stopped",
			status: http.StatusOK,
			value: api.CommandStopResponse{
				SuccessBase: api.SuccessBase{OK: true, Type: api.TypeCommandStopped},
				ProcessID:   7,
				Command: &api.CommandRecord{
					CommandID: "cmd_fixture", SandboxID: cliTestSandboxID, Command: "true",
					Mode: "detached", Status: "killed", StartedAt: cliTestTime,
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cliTestValidatePinned(t, validator, tc.status, tc.value)
		})
	}
}

// TestEmittedEnvelopesMatchPin validates the actual bytes commands write, not
// only the representative DTO fixtures. It covers an empty list, a merged
// multi-page list, info and both delete envelopes.
func TestEmittedEnvelopesMatchPin(t *testing.T) {
	validator := compat.Load(t)

	t.Run("empty list", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse(nil, api.NewPageInfo(nil, 100)))
		}, nil)
		if status := env.run(t, "list"); status != 0 {
			t.Fatalf("exit status = %d, want 0", status)
		}
		line := strings.TrimSpace(env.stdout.String())
		lines := cliTestJSONLines(t, env.stdout)
		sandboxes, ok := lines[0]["sandboxes"].([]any)
		if !ok || len(sandboxes) != 0 {
			t.Fatalf("sandboxes = %v, want an empty array", lines[0]["sandboxes"])
		}
		cliTestValidateEmitted(t, validator, http.StatusOK, []byte(line))
	})

	t.Run("multi-page list", func(t *testing.T) {
		var pages atomic.Int64
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			switch pages.Add(1) {
			case 1:
				next := "cursor-2"
				cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse([]api.Sandbox{
					cliTestSandbox(cliTestSandboxID, api.StateReady),
				}, api.NewPageInfo(&next, 1)))
			case 2:
				cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse([]api.Sandbox{
					cliTestSandbox(cliTestSandboxID2, api.StateRunning),
				}, api.NewPageInfo(nil, 1)))
			default:
				t.Error("unexpected extra page")
			}
		}, nil)
		if status := env.run(t, "list", "--limit", "1"); status != 0 {
			t.Fatalf("exit status = %d, want 0", status)
		}
		line := strings.TrimSpace(env.stdout.String())
		lines := cliTestJSONLines(t, env.stdout)
		sandboxes := lines[0]["sandboxes"].([]any)
		if len(sandboxes) != 2 {
			t.Fatalf("sandboxes = %d, want 2", len(sandboxes))
		}
		cliTestValidateEmitted(t, validator, http.StatusOK, []byte(line))
	})

	t.Run("info", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(cliTestSandbox(cliTestSandboxID, api.StateIdle)))
		}, nil)
		if status := env.run(t, "info", cliTestSandboxID); status != 0 {
			t.Fatalf("exit status = %d, want 0", status)
		}
		cliTestValidateEmitted(t, validator, http.StatusOK, []byte(strings.TrimSpace(env.stdout.String())))
	})

	t.Run("delete accepted", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSandboxDeletingResponse(
				cliTestOperation(cliTestOperationID, cliTestSandboxID, api.DeletionPending),
			))
		}, nil)
		if status := env.run(t, "delete", cliTestSandboxID, "--yes"); status != 0 {
			t.Fatalf("exit status = %d, want 0", status)
		}
		cliTestValidateEmitted(t, validator, http.StatusAccepted, []byte(strings.TrimSpace(env.stdout.String())))
	})

	t.Run("delete completed", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodDelete {
				cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSandboxDeletingResponse(
					cliTestOperation(cliTestOperationID, cliTestSandboxID, api.DeletionPending),
				))
				return
			}
			cliTestWriteJSON(t, w, http.StatusOK, api.NewDeletionOperationResponse(
				cliTestOperation(cliTestOperationID, cliTestSandboxID, api.DeletionCompleted),
			))
		}, nil)
		if status := env.run(t, "delete", cliTestSandboxID, "--yes", "--wait"); status != 0 {
			t.Fatalf("exit status = %d, want 0", status)
		}
		cliTestValidateEmitted(t, validator, http.StatusOK, []byte(strings.TrimSpace(env.stdout.String())))
	})
}

// TestReadWithContextNonComparableReader proves interface replacement uses a
// boolean, so a reader whose dynamic type is not comparable neither panics on
// completion nor on cancellation.
func TestReadWithContextNonComparableReader(t *testing.T) {
	t.Run("completion", func(t *testing.T) {
		offset := 0
		reader := cliTestNonComparableReader{
			data: []byte("hello"), offset: &offset,
			started: make(chan struct{}), startOnce: new(sync.Once),
		}
		var got []byte
		err := readWithContext(t.Context(), reader, func(r io.Reader) error {
			var readErr error
			got, readErr = io.ReadAll(r)
			return readErr
		})
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(got) != "hello" {
			t.Fatalf("got %q, want hello", got)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		release := make(chan struct{})
		reader := cliTestNonComparableReader{
			started: make(chan struct{}), release: release,
			startOnce: new(sync.Once), closeOnce: new(sync.Once),
		}
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- readWithContext(ctx, reader, func(r io.Reader) error {
				var one [1]byte
				_, err := r.Read(one[:])
				return err
			})
		}()
		select {
		case <-reader.started:
		case <-time.After(time.Second):
			t.Fatal("read did not start")
		}
		cancel()
		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("read did not cancel")
		}
	})
}
