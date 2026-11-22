package cli

import (
	"context"
	"encoding/json/v2"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

func TestNewWaitsForReadyAndEmitsJSONLines(t *testing.T) {
	var created atomic.Bool
	var polls atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == api.PathSandboxes:
			created.Store(true)
			if got := r.Header.Get("Authorization"); !cliTestSecretEqual(got, "Bearer test-key") {
				t.Error("Authorization header does not carry the expected key")
			}
			var body api.CreateSandboxRequest
			if err := json.UnmarshalRead(r.Body, &body); err != nil {
				t.Errorf("decode create body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if body.Type == nil || *body.Type != "small" {
				t.Errorf("type = %v, want small", body.Type)
			}
			if body.Name == nil || *body.Name != "api worker" {
				t.Errorf("name = %v", body.Name)
			}
			if ttl, ok := body.TTLSeconds.Seconds(); !ok || ttl != 600 {
				t.Errorf("ttl = %d set=%v, want 600", ttl, ok)
			}
			if body.From == nil || *body.From != cliTestSnapshotName {
				t.Errorf("from = %v", body.From)
			}
			if body.Env["A"] != "1" || body.Env["B"] != "two" {
				t.Errorf("env = %v", body.Env)
			}
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewCreateSandboxResponse(
				api.CreateProvisioning, new(3600),
				cliTestSandbox(cliTestSandboxID, api.StateProvisioning),
			))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/sandboxes/"+cliTestSandboxID:
			state := api.StateProvisioning
			if polls.Add(1) >= 2 {
				state = api.StateReady
			}
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(cliTestSandbox(cliTestSandboxID, state)))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}, nil)
	status := env.run(t, "new", "--type", "small", "--name", "api worker", "--ttl", "600", "--from", cliTestSnapshotName, "--env", "A=1", "--env", "B=two")
	if status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	if !created.Load() {
		t.Fatal("create request was not sent")
	}
	lines := cliTestJSONLines(t, env.stdout)
	var ready int
	for _, line := range lines {
		if line["event"] == "ready" {
			ready++
			if line["id"] != cliTestSandboxID {
				t.Errorf("ready id = %v, want the created id", line["id"])
			}
		}
	}
	if ready != 1 {
		t.Fatalf("ready events = %d, want exactly 1: %v", ready, lines)
	}
	if len(lines) != 3 || lines[0]["event"] != "created" || lines[1]["event"] != "state" {
		t.Fatalf("lines = %v, want created/state/ready", lines)
	}
}

func TestNewAlreadyReadyEmitsOneReady(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		cliTestWriteJSON(t, w, http.StatusAccepted, api.NewCreateSandboxResponse(
			api.CreateReady, nil, cliTestSandbox(cliTestSandboxID, api.StateReady),
		))
	}, nil)
	if status := env.run(t, "new"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if len(lines) != 3 || lines[0]["event"] != "created" || lines[1]["event"] != "state" || lines[2]["event"] != "ready" {
		t.Fatalf("lines = %v, want created/state/ready", lines)
	}
}

func TestNewRejectsInvalidFlagsBeforeRequest(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "type", args: []string{"new", "--type", "huge"}},
		{name: "ttl low", args: []string{"new", "--ttl", "0"}},
		{name: "ttl high", args: []string{"new", "--ttl", "2592001"}},
		{name: "env no equals", args: []string{"new", "--env", "SUPER_SECRET_VALUE"}},
		{name: "env empty key", args: []string{"new", "--env", "=value"}},
		{name: "empty from", args: []string{"new", "--from", ""}},
		{name: "count without from", args: []string{"new", "--count", "2"}},
		{name: "count one without from", args: []string{"new", "--count", "1"}},
		{name: "count with fail-fast", args: []string{"new", "--from", cliTestSnapshotName, "--count", "2", "--fail-fast"}},
		{name: "count one with fail-fast", args: []string{"new", "--from", cliTestSnapshotName, "--count", "1", "--fail-fast"}},
		{name: "count low", args: []string{"new", "--from", cliTestSnapshotName, "--count", "0"}},
		{name: "count negative", args: []string{"new", "--from", cliTestSnapshotName, "--count", "-1"}},
		{name: "count high", args: []string{"new", "--from", cliTestSnapshotName, "--count", "1001"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int64
			env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, nil)
			if status := env.run(t, tc.args...); status != 2 {
				t.Fatalf("exit status = %d, want 2", status)
			}
			if hits.Load() != 0 {
				t.Fatalf("server received %d requests, want 0", hits.Load())
			}
			if strings.Contains(env.stderr.String(), "SUPER_SECRET_VALUE") {
				t.Fatal("stderr contains the env value")
			}
		})
	}
}

func TestNewNameLimitIsCharacters(t *testing.T) {
	name := strings.Repeat("é", maxNameChars)
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		cliTestWriteJSON(t, w, http.StatusAccepted, api.NewCreateSandboxResponse(
			api.CreateReady, nil, cliTestSandbox(cliTestSandboxID, api.StateReady),
		))
	}, nil)
	if status := env.run(t, "new", "--name", name); status != 0 {
		t.Fatalf("exit status = %d, want 0 for %d characters (%d bytes): %s", status, maxNameChars, len(name), env.stderr)
	}

	env = cliTestNew(t, func(http.ResponseWriter, *http.Request) {
		t.Error("server must not be called")
	}, nil)
	if status := env.run(t, "new", "--name", name+"é"); status != 2 {
		t.Fatalf("exit status = %d, want 2 for %d characters", status, maxNameChars+1)
	}
}

func TestNewTimeoutAndCancellation(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				cliTestWriteJSON(t, w, http.StatusAccepted, api.NewCreateSandboxResponse(
					api.CreateProvisioning, nil, cliTestSandbox(cliTestSandboxID, api.StateProvisioning),
				))
				return
			}
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(cliTestSandbox(cliTestSandboxID, api.StateProvisioning)))
		}, func(opts *Options) {
			opts.WaitTimeout = 20 * time.Millisecond
			opts.PollInterval = 2 * time.Millisecond
		})
		if status := env.run(t, "new"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		last := lines[len(lines)-1]
		if last["event"] != "error" || last["code"] != "timeout" {
			t.Fatalf("last event = %v, want timeout", last)
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				cliTestWriteJSON(t, w, http.StatusAccepted, api.NewCreateSandboxResponse(
					api.CreateProvisioning, nil, cliTestSandbox(cliTestSandboxID, api.StateProvisioning),
				))
				return
			}
			cancel()
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(cliTestSandbox(cliTestSandboxID, api.StateProvisioning)))
		}, nil)
		if status := env.runContext(ctx, "new"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		last := lines[len(lines)-1]
		if last["event"] != "error" || last["code"] != "cancelled" {
			t.Fatalf("last event = %v, want cancelled", last)
		}
	})
}

func TestNewReportsTerminalErrorState(t *testing.T) {
	reason := "boot failed"
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewCreateSandboxResponse(
				api.CreateProvisioning, nil, cliTestSandbox(cliTestSandboxID, api.StateProvisioning),
			))
			return
		}
		failed := cliTestSandbox(cliTestSandboxID, api.StateError)
		failed.Error = &reason
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(failed))
	}, nil)
	if status := env.run(t, "new"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "sandbox_error" {
		t.Fatalf("last event = %v, want sandbox_error", last)
	}
	if !strings.Contains(last["error"].(string), reason) {
		t.Fatalf("error = %v, want the sandbox reason", last["error"])
	}
}

func TestNewRejectsWrongSandboxIdentity(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewCreateSandboxResponse(
				api.CreateProvisioning, nil, cliTestSandbox(cliTestSandboxID, api.StateProvisioning),
			))
			return
		}
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(cliTestSandbox(cliTestSandboxID2, api.StateReady)))
	}, nil)
	if status := env.run(t, "new"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "invalid_response" {
		t.Fatalf("last event = %v, want invalid_response", last)
	}
}

func TestNewHelpTTLWording(t *testing.T) {
	env := cliTestNew(t, func(http.ResponseWriter, *http.Request) {
		t.Error("help must not call the API")
	}, nil)
	if status := env.run(t, "new", "--help"); status != 0 {
		t.Fatalf("exit status = %d, want 0", status)
	}
	help := strings.ToLower(env.stdout.String())
	if !strings.Contains(help, "auto-stops") {
		t.Fatalf("help does not describe the TTL as an auto-stop: %q", env.stdout)
	}
	for _, forbidden := range []string{"expire", "delet"} {
		if strings.Contains(help, forbidden) {
			t.Fatalf("help must not claim TTL %q expiry: %q", forbidden, env.stdout)
		}
	}
}

// newTestCreateFailure answers every request with the given status and body,
// counting create attempts.
func newTestCreateFailure(t *testing.T, status int, body func(http.ResponseWriter)) (http.HandlerFunc, *atomic.Int64) {
	t.Helper()
	var creates atomic.Int64
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != api.PathSandboxes {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		creates.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		body(w)
	}
	return handler, &creates
}

// TestNewDefiniteCreateRefusal proves a validated 5xx envelope is a definite
// refusal for new too: code, safe message and only a whitelisted hint are
// printed, with no invented id or ambiguity claim.
func TestNewDefiniteCreateRefusal(t *testing.T) {
	const message = "no capacity is available right now"
	writeEnvelope := func(details map[string]any) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			raw, err := json.Marshal(cliTestErrorEnvelopeWithDetails(
				http.StatusServiceUnavailable, "out_of_capacity", message, details,
			))
			if err != nil {
				t.Errorf("marshal envelope: %v", err)
				return
			}
			_, _ = w.Write(raw)
		}
	}

	t.Run("json with retry hint", func(t *testing.T) {
		handler, creates := newTestCreateFailure(t, http.StatusServiceUnavailable, writeEnvelope(map[string]any{"retryAfterSeconds": 5}))
		env := cliTestNew(t, handler, nil)
		if status := env.run(t, "new"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 {
			t.Fatalf("json records = %d, want one error", len(lines))
		}
		event := lines[0]
		if event["event"] != "error" {
			t.Fatal("terminal json record is not an error event")
		}
		if event["code"] != "out_of_capacity" {
			t.Fatal("error event lost the API code")
		}
		if event["status"] != float64(http.StatusServiceUnavailable) {
			t.Fatal("error event lost the HTTP status")
		}
		if event["error"] != message {
			t.Fatal("error event lost the safe API message")
		}
		if event["retryAfterSeconds"] != float64(5) {
			t.Fatal("error event lost the retry hint")
		}
		if creates.Load() != 1 {
			t.Fatalf("create requests = %d, want exactly 1", creates.Load())
		}
	})

	t.Run("human with retry hint", func(t *testing.T) {
		handler, _ := newTestCreateFailure(t, http.StatusServiceUnavailable, writeEnvelope(map[string]any{"retryAfterSeconds": 5}))
		env := cliTestNew(t, handler, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "new"); status != 1 {
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
		if env.stdout.Len() != 0 {
			t.Fatal("human mode wrote success events")
		}
	})

	t.Run("no or invalid hint stays definite", func(t *testing.T) {
		cases := []struct {
			name    string
			details map[string]any
		}{
			{name: "none"},
			{name: "zero", details: map[string]any{"retryAfterSeconds": 0}},
			{name: "negative", details: map[string]any{"retryAfterSeconds": -1}},
			{name: "fractional", details: map[string]any{"retryAfterSeconds": 1.5}},
			{name: "string", details: map[string]any{"retryAfterSeconds": "sentinel-hint"}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				handler, _ := newTestCreateFailure(t, http.StatusServiceUnavailable, writeEnvelope(tc.details))
				env := cliTestNew(t, handler, nil)
				if status := env.run(t, "new"); status != 1 {
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
	})

	t.Run("4xx remains definite", func(t *testing.T) {
		handler, _ := newTestCreateFailure(t, http.StatusBadRequest, func(w http.ResponseWriter) {
			raw, err := json.Marshal(cliTestErrorEnvelope(http.StatusBadRequest, "invalid_name", "invalid name"))
			if err != nil {
				t.Errorf("marshal envelope: %v", err)
				return
			}
			_, _ = w.Write(raw)
		})
		env := cliTestNew(t, handler, nil)
		if status := env.run(t, "new"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["code"] != "invalid_name" || lines[0]["error"] != "invalid name" {
			t.Fatalf("json records = %d, want the API refusal", len(lines))
		}
	})
}

// TestNewAmbiguousCreate proves unenveloped, malformed and lost responses stay
// ambiguous, invent no id, and only claim an expiry when --ttl asked for one.
func TestNewAmbiguousCreate(t *testing.T) {
	unconfirmed := func(env *cliTestEnv, t *testing.T, forbidden ...string) string {
		t.Helper()
		if status := env.run(t, "new"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		for _, secret := range forbidden {
			if strings.Contains(env.stdout.String(), secret) {
				t.Fatalf("create failure leaked a response/transport sentinel")
			}
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 {
			t.Fatalf("json records = %d, want one error", len(lines))
		}
		event := lines[0]
		if event["event"] != "error" {
			t.Fatal("terminal json record is not an error event")
		}
		if event["code"] != "create_ambiguous" {
			t.Fatal("error event is not create_ambiguous")
		}
		message, ok := event["error"].(string)
		if !ok || !strings.Contains(message, "could not be confirmed") {
			t.Fatal("error event lacks the ambiguity explanation")
		}
		return message
	}

	t.Run("unenveloped server error without ttl", func(t *testing.T) {
		handler, creates := newTestCreateFailure(t, http.StatusServiceUnavailable, func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(w, "<html>sentinel-body</html>")
		})
		env := cliTestNew(t, handler, nil)
		message := unconfirmed(env, t, "sentinel-body")
		if strings.Contains(message, "expires") || strings.Contains(message, "automatically") {
			t.Fatal("ambiguity message claimed an unrequested expiry")
		}
		if creates.Load() != 1 {
			t.Fatalf("create requests = %d, want exactly 1", creates.Load())
		}
	})

	t.Run("unenveloped server error with ttl states the supplied value", func(t *testing.T) {
		handler, _ := newTestCreateFailure(t, http.StatusServiceUnavailable, func(w http.ResponseWriter) {
			_, _ = io.WriteString(w, "not an envelope")
		})
		env := cliTestNew(t, handler, nil)
		env.app.SetArgs([]string{"new", "--ttl", "600"})
		if status := env.app.Run(t.Context()); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["code"] != "create_ambiguous" {
			t.Fatalf("json records = %d, want one create_ambiguous error", len(lines))
		}
		if message, _ := lines[0]["error"].(string); !strings.Contains(message, "600 seconds") {
			t.Fatal("ambiguity message omitted the supplied ttl")
		}
	})

	t.Run("malformed success body", func(t *testing.T) {
		handler, _ := newTestCreateFailure(t, http.StatusAccepted, func(w http.ResponseWriter) {
			_, _ = io.WriteString(w, "{")
		})
		env := cliTestNew(t, handler, nil)
		unconfirmed(env, t)
	})

	t.Run("empty created id", func(t *testing.T) {
		handler, _ := newTestCreateFailure(t, http.StatusAccepted, func(w http.ResponseWriter) {
			ttl := api.DefaultTTLSeconds
			sandbox := cliTestSandbox("", api.StateProvisioning)
			sandbox.Type = "default"
			raw, err := json.Marshal(api.NewCreateSandboxResponse(api.CreateProvisioning, &ttl, sandbox))
			if err != nil {
				t.Errorf("marshal create response: %v", err)
				return
			}
			_, _ = w.Write(raw)
		})
		env := cliTestNew(t, handler, nil)
		unconfirmed(env, t)
	})

	t.Run("transport failure", func(t *testing.T) {
		handler, _ := newTestCreateFailure(t, http.StatusServiceUnavailable, func(http.ResponseWriter) {})
		env := cliTestNew(t, handler, func(opts *Options) {
			opts.HTTPClient = &http.Client{Transport: runTestFailingTransport{
				base: opts.HTTPClient.Transport,
				fail: func(r *http.Request) bool {
					return r.Method == http.MethodPost && r.URL.Path == api.PathSandboxes
				},
			}}
		})
		message := unconfirmed(env, t, "sentinel transport failure")
		if strings.Contains(message, "sentinel transport failure") {
			t.Fatal("transport sentinel leaked into the event")
		}
	})
}

func TestNewFromShareCodeAndFailFast(t *testing.T) {
	shareCode := "bsh_" + strings.Repeat("c", 52)
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != api.PathSandboxes {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var body api.CreateSandboxRequest
		if err := json.UnmarshalRead(r.Body, &body); err != nil {
			t.Errorf("decode body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if body.From == nil || *body.From != shareCode {
			t.Errorf("from = %v, want %q", body.From, shareCode)
		}
		if body.FailFast == nil || !*body.FailFast {
			t.Errorf("failFast = %v, want true", body.FailFast)
		}
		if body.Count != nil {
			t.Errorf("count = %v, want nil", body.Count)
		}
		cliTestWriteJSON(t, w, http.StatusAccepted, api.NewCreateSandboxResponse(
			api.CreateReady, new(3600), cliTestSandbox(cliTestSandboxID, api.StateReady),
		))
	}, nil)
	if status := env.run(t, "new", "--from", shareCode, "--fail-fast"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if len(lines) != 3 || lines[2]["event"] != "ready" {
		t.Fatalf("lines = %v, want created/state/ready", lines)
	}
}

func TestNewCountBatchHumanAndJSON(t *testing.T) {
	const cliTestSandboxID3 = "bx_34567892"
	batch := []api.Sandbox{
		cliTestSandbox(cliTestSandboxID, api.StateProvisioning),
		cliTestSandbox(cliTestSandboxID2, api.StateProvisioning),
		cliTestSandbox(cliTestSandboxID3, api.StateProvisioning),
	}

	t.Run("human prints one id per line without polling", func(t *testing.T) {
		shareCode := "bsh_" + strings.Repeat("d", 52)
		var requests atomic.Int64
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			if r.Method != http.MethodPost || r.URL.Path != api.PathSandboxes {
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			var body api.CreateSandboxRequest
			if err := json.UnmarshalRead(r.Body, &body); err != nil {
				t.Errorf("decode body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if body.From == nil || *body.From != shareCode {
				t.Errorf("from = %v, want %q", body.From, shareCode)
			}
			if body.Count == nil || *body.Count != 3 {
				t.Errorf("count = %v, want 3", body.Count)
			}
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewCreateSandboxBatchResponse(
				api.CreateProvisioning, new(3600), batch,
			))
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "new", "--from", shareCode, "--count", "3"); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		if requests.Load() != 1 {
			t.Fatalf("requests = %d, want 1 (no polling)", requests.Load())
		}
		want := strings.Join([]string{cliTestSandboxID, cliTestSandboxID2, cliTestSandboxID3}, "\n") + "\n"
		if env.stdout.String() != want {
			t.Fatalf("stdout = %q, want %q", env.stdout.String(), want)
		}
	})

	t.Run("json prints full batch response matching v1 API schema", func(t *testing.T) {
		validator := compat.Load(t)
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			}
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewCreateSandboxBatchResponse(
				api.CreateProvisioning, new(3600), batch,
			))
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "new", "--from", cliTestSnapshotName, "--count", "3", "--json"); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		raw := strings.TrimSpace(env.stdout.String())
		lifecycleTestValidate(t, validator, "create", http.StatusAccepted, []byte(raw))
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["type"] != api.TypeSandboxCreated {
			t.Fatalf("lines = %v", lines)
		}
		sandboxes, ok := lines[0]["sandboxes"].([]any)
		if !ok || len(sandboxes) != 3 {
			t.Fatalf("sandboxes = %v, want 3 entries", lines[0]["sandboxes"])
		}
	})

	t.Run("count one waits like single create", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			var body api.CreateSandboxRequest
			if err := json.UnmarshalRead(r.Body, &body); err != nil {
				t.Errorf("decode body: %v", err)
			}
			if body.Count == nil || *body.Count != 1 {
				t.Errorf("count = %v, want 1", body.Count)
			}
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewCreateSandboxResponse(
				api.CreateReady, new(3600), cliTestSandbox(cliTestSandboxID, api.StateReady),
			))
		}, nil)
		if status := env.run(t, "new", "--from", cliTestSnapshotName, "--count", "1"); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 3 || lines[2]["event"] != "ready" {
			t.Fatalf("lines = %v, want created/state/ready", lines)
		}
	})

	t.Run("incomplete batch response is create_ambiguous", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewCreateSandboxBatchResponse(
				api.CreateProvisioning, new(3600), batch[:2],
			))
		}, nil)
		if status := env.run(t, "new", "--from", cliTestSnapshotName, "--count", "3"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["code"] != "create_ambiguous" {
			t.Fatalf("lines = %v, want create_ambiguous", lines)
		}
	})
}
