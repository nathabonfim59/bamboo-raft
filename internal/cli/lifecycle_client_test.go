package cli

import (
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

// lifecycleTestStopID is a pinned-shaped stop id. The pin only requires a
// string; this keeps fixtures readable.
const lifecycleTestStopID = "stop_3f9a1c2e7b4d5a60"

// lifecycleTestStop builds one pinned StopOperation. completed and superseded
// carry an end time; pending and failing do not.
func lifecycleTestStop(status api.StopStatus) api.StopOperation {
	requested := cliTestTime
	last := cliTestTime.Add(30 * time.Second)
	stop := api.StopOperation{
		ID:            lifecycleTestStopID,
		Status:        status,
		RequestedAt:   &requested,
		LastAttemptAt: &last,
	}
	switch status {
	case api.StopCompleted, api.StopSuperseded:
		ended := last.Add(time.Second)
		stop.EndedAt = &ended
	case api.StopFailing:
		message := "disk save failed"
		stop.Error = &message
	}
	return stop
}

// lifecycleTestSandbox builds a sandbox with the lifecycle fields set.
func lifecycleTestSandbox(id string, state api.SandboxState) api.Sandbox {
	sandbox := cliTestSandbox(id, state)
	ttl := 3600
	archive := cliTestTime.Add(time.Hour)
	sandbox.TTLSeconds = &ttl
	sandbox.ArchiveAfter = &archive
	return sandbox
}

// lifecycleTestStopAction builds the pinned stop action envelope for a sandbox.
func lifecycleTestStopAction(typ, status string, state api.SandboxState, stop api.StopOperation, forced bool) api.SandboxActionResponse {
	sandbox := lifecycleTestSandbox(cliTestSandboxID, state)
	sandbox.Stop = &stop
	statusURL := "/api/v1/sandboxes/" + cliTestSandboxID + "/stops/" + stop.ID
	return api.NewStopActionResponse(typ, cliTestSandboxID, status, sandbox, stop, statusURL, forced)
}

// lifecycleTestDecodeBody decodes one JSON request body as a generic object.
// HTTP handlers run outside the test goroutine, so failures use Errorf.
func lifecycleTestDecodeBody(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Errorf("read request body: %v", err)
		return nil
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Errorf("decode request body: %v", err)
		return nil
	}
	return body
}

// lifecycleTestValidate checks one emitted API envelope against the pin.
func lifecycleTestValidate(t *testing.T, validator *compat.Validator, operation string, status int, raw []byte) {
	t.Helper()
	header := http.Header{"Content-Type": []string{"application/json"}}
	if err := validator.Response(operation, status, header, raw); err != nil {
		t.Errorf("emitted %s response does not match the pin: %v", operation, err)
	}
}

// TestLifecycleCommandsRegistered proves the four lifecycle commands are part
// of the core tree, not only reachable with an injected factory.
func TestLifecycleCommandsRegistered(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}, nil)
	for _, name := range []string{"stop", "resume", "extend", "rename"} {
		cmd, _, err := env.app.root.Find([]string{name})
		if err != nil {
			t.Fatalf("command %q is not registered: %v", name, err)
		}
		if cmd.Name() != name {
			t.Fatalf("registered command = %q, want %q", cmd.Name(), name)
		}
	}
}

// TestLifecycleHelpClaims pins the user-visible lifecycle vocabulary: a forced
// stop is a power-off that keeps the local disk, and a TTL is an auto-stop, not
// an expiry that deletes data.
func TestLifecycleHelpClaims(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}, nil)

	stop, _, err := env.app.root.Find([]string{"stop"})
	if err != nil {
		t.Fatalf("find stop: %v", err)
	}
	force := stop.Flags().Lookup("force")
	if force == nil {
		t.Fatal("stop has no --force flag")
	}
	forceUsage := strings.ToLower(force.Usage)
	for _, want := range []string{"power", "disk"} {
		if !strings.Contains(forceUsage, want) {
			t.Errorf("--force help = %q, want %q", force.Usage, want)
		}
	}
	for _, forbidden := range []string{"lost", "snapshot"} {
		if strings.Contains(forceUsage, forbidden) {
			t.Errorf("--force help = %q, must not claim %q", force.Usage, forbidden)
		}
	}

	newCmd, _, err := env.app.root.Find([]string{"new"})
	if err != nil {
		t.Fatalf("find new: %v", err)
	}
	ttl := newCmd.Flags().Lookup("ttl")
	if ttl == nil {
		t.Fatal("new has no --ttl flag")
	}
	ttlUsage := strings.ToLower(ttl.Usage)
	if !strings.Contains(ttlUsage, "auto-stop") {
		t.Errorf("--ttl help = %q, want auto-stop wording", ttl.Usage)
	}
	for _, forbidden := range []string{"expire", "delet"} {
		if strings.Contains(ttlUsage, forbidden) {
			t.Errorf("--ttl help = %q, must not claim %q", ttl.Usage, forbidden)
		}
	}
}

// TestLifecycleFieldsThroughExistingCommands proves the existing info and list
// commands already expose the stop metadata and archived state without
// production changes.
func TestLifecycleFieldsThroughExistingCommands(t *testing.T) {
	t.Run("info", func(t *testing.T) {
		validator := compat.Load(t)
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != "/api/v1/sandboxes/"+cliTestSandboxID {
				t.Errorf("info request = %s %s", r.Method, r.URL.Path)
			}
			sandbox := lifecycleTestSandbox(cliTestSandboxID, api.StateArchived)
			stop := lifecycleTestStop(api.StopCompleted)
			sandbox.Stop = &stop
			stopped := cliTestTime
			sandbox.StoppedAt = &stopped
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
		}, nil)
		if status := env.run(t, "info", cliTestSandboxID); status != 0 {
			t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
		}
		line := strings.TrimSpace(env.stdout.String())
		lifecycleTestValidate(t, validator, "get", http.StatusOK, []byte(line))
		lines := cliTestJSONLines(t, env.stdout)
		sandbox := lines[0]["sandbox"].(map[string]any)
		if sandbox["state"] != string(api.StateArchived) || sandbox["stoppedAt"] == nil {
			t.Fatalf("sandbox = %v, want archived with stoppedAt", sandbox)
		}
		if stop, ok := sandbox["stop"].(map[string]any); !ok || stop["status"] != string(api.StopCompleted) {
			t.Fatalf("stop = %v, want completed", sandbox["stop"])
		}
	})

	t.Run("list stopped filter", func(t *testing.T) {
		validator := compat.Load(t)
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("state") != string(api.StateArchived) {
				t.Errorf("state query = %q, want archived", r.URL.Query().Get("state"))
			}
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse(
				[]api.Sandbox{lifecycleTestSandbox(cliTestSandboxID, api.StateArchived)},
				api.NewPageInfo(nil, 100),
			))
		}, nil)
		if status := env.run(t, "list", "--filter", "s"); status != 0 {
			t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
		}
		lifecycleTestValidate(t, validator, "sandboxes", http.StatusOK,
			[]byte(strings.TrimSpace(env.stdout.String())))
	})
}

// TestLifecycleClientAcceptsOnlyPinnedTypes proves the variadic success-type
// check still rejects a foreign envelope and does not weaken single-type
// operations.
func TestLifecycleClientAcceptsOnlyPinnedTypes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			cliTestWriteJSON(t, w, http.StatusAccepted,
				api.NewSandboxInfoResponse(lifecycleTestSandbox(cliTestSandboxID, api.StateArchiving)))
		default:
			cliTestWriteJSON(t, w, http.StatusOK,
				lifecycleTestStopAction(api.TypeSandboxStopping, "archiving", api.StateArchiving,
					lifecycleTestStop(api.StopPending), false))
		}
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	t.Run("stop rejects sandbox.info", func(t *testing.T) {
		if _, err := client.StopSandbox(t.Context(), cliTestSandboxID, false); err == nil {
			t.Fatal("StopSandbox accepted sandbox.info, want an invalid_response error")
		} else if _, ok := lifecycleTestErrorsAsCLI(err); !ok {
			t.Fatalf("error = %v, want *CLIError", err)
		}
	})

	t.Run("get keeps a single accepted type", func(t *testing.T) {
		if _, err := client.GetSandbox(t.Context(), cliTestSandboxID); err == nil {
			t.Fatal("GetSandbox accepted sandbox.stopping, want an invalid_response error")
		}
	})
}

// TestAPIErrorRetryableDetail pins that only a JSON boolean
// error.details.retryable is retained; absent, null, string and numeric values
// stay nil.
func TestAPIErrorRetryableDetail(t *testing.T) {
	yes, no := true, false
	cases := []struct {
		name    string
		details map[string]any
		want    *bool
	}{
		{name: "absent", want: nil},
		{name: "true", details: map[string]any{"retryable": true}, want: &yes},
		{name: "false", details: map[string]any{"retryable": false}, want: &no},
		{name: "null", details: map[string]any{"retryable": nil}, want: nil},
		{name: "string", details: map[string]any{"retryable": "true"}, want: nil},
		{name: "number", details: map[string]any{"retryable": 1}, want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				envelope := cliTestErrorEnvelope(http.StatusConflict, codeSandboxStarting, "sandbox is starting")
				if tc.details != nil {
					envelope = cliTestErrorEnvelopeWithDetails(
						http.StatusConflict, codeSandboxStarting, "sandbox is starting", tc.details)
				}
				cliTestWriteJSON(t, w, http.StatusConflict, envelope)
			}))
			t.Cleanup(server.Close)
			client, err := NewClient(server.URL, "test-key", server.Client())
			if err != nil {
				t.Fatalf("new client: %v", err)
			}
			_, err = client.StopSandbox(t.Context(), cliTestSandboxID, false)
			apiErr, ok := errors.AsType[*APIError](err)
			if !ok {
				t.Fatalf("error = %v, want *APIError", err)
			}
			switch {
			case tc.want == nil && apiErr.Retryable != nil:
				t.Fatalf("Retryable = %v, want nil", *apiErr.Retryable)
			case tc.want != nil && apiErr.Retryable == nil:
				t.Fatalf("Retryable = nil, want %v", *tc.want)
			case tc.want != nil && *apiErr.Retryable != *tc.want:
				t.Fatalf("Retryable = %v, want %v", *apiErr.Retryable, *tc.want)
			}
		})
	}
}

// lifecycleTestErrorsAsCLI reports whether err is the invalid-response CLI
// error the envelope check produces.
func lifecycleTestErrorsAsCLI(err error) (*CLIError, bool) {
	cliErr, ok := errors.AsType[*CLIError](err)
	if !ok || cliErr.Code != "invalid_response" {
		return cliErr, false
	}
	return cliErr, true
}
