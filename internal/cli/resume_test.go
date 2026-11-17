package cli

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

// resumeTestAccepted builds the pinned resume acceptance envelope.
func resumeTestAccepted(state api.SandboxState) api.SandboxActionResponse {
	return api.NewResumeActionResponse(cliTestSandboxID, lifecycleTestSandbox(cliTestSandboxID, state))
}

// resumeTestCommand builds one successful pinned command envelope.
func resumeTestCommand() api.CommandResponse {
	return api.NewCommandResponse(true, new(0), "", "", false)
}

// resumeTestWriteEnvelope writes an envelope and validates it as an API
// response fixture for operation and status.
func resumeTestWriteEnvelope(t *testing.T, validator *compat.Validator, w http.ResponseWriter, operation string, status int, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Errorf("marshal %s fixture: %v", operation, err)
		http.Error(w, "test encode failure", http.StatusInternalServerError)
		return
	}
	lifecycleTestValidate(t, validator, operation, status, raw)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(raw); err != nil {
		t.Errorf("write %s fixture: %v", operation, err)
	}
}

// resumeTestDeadlineRecorder records the outgoing request deadline per path.
type resumeTestDeadlineRecorder struct {
	base      http.RoundTripper
	mu        sync.Mutex
	deadlines map[string]time.Time
}

func (r *resumeTestDeadlineRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	if deadline, ok := req.Context().Deadline(); ok {
		r.mu.Lock()
		if r.deadlines == nil {
			r.deadlines = make(map[string]time.Time)
		}
		r.deadlines[req.URL.Path] = deadline
		r.mu.Unlock()
	}
	return r.base.RoundTrip(req)
}

func (r *resumeTestDeadlineRecorder) deadline(path string) (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	deadline, ok := r.deadlines[path]
	return deadline, ok
}

// resumeTestCancelOnEOF cancels a context when the matching response body
// reaches EOF, reproducing a cancel racing a successful final read.
type resumeTestCancelOnEOF struct {
	base   http.RoundTripper
	match  func(*http.Request) bool
	cancel func()
	once   *sync.Once
}

func (t *resumeTestCancelOnEOF) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if !t.match(req) {
		return resp, nil
	}
	resp.Body = &resumeTestCancelBody{
		ReadCloser: resp.Body,
		cancel:     func() { t.once.Do(t.cancel) },
	}
	return resp, nil
}

type resumeTestCancelBody struct {
	io.ReadCloser
	cancel func()
}

func (b *resumeTestCancelBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if errors.Is(err, io.EOF) {
		b.cancel()
	}
	return n, err
}

// TestResumeNoWaitSendsEmptyBody proves --no-wait sends one POST with omitted
// fields, returns at 202 and never polls or probes.
func TestResumeNoWaitSendsEmptyBody(t *testing.T) {
	validator := compat.Load(t)
	var calls atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/sandboxes/"+cliTestSandboxID+"/resume" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		body := lifecycleTestDecodeBody(t, r)
		if len(body) != 0 {
			t.Errorf("body = %v, want {}", body)
		}
		resumeTestWriteEnvelope(t, validator, w, "resume", http.StatusAccepted, resumeTestAccepted(api.StateProvisioning))
	}, nil)
	if status := env.run(t, "resume", cliTestSandboxID, "--no-wait"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP calls = %d, want 1", calls.Load())
	}
	line := strings.TrimSpace(env.stdout.String())
	lifecycleTestValidate(t, validator, "resume", http.StatusAccepted, []byte(line))
	lines := cliTestJSONLines(t, env.stdout)
	if lines[0]["type"] != api.TypeSandboxResuming || lines[0]["status"] != "resuming" {
		t.Fatalf("response = %v", lines[0])
	}
}

// TestResumeRequestBodies pins omitted versus null versus value TTL, the env
// replacement map and every forwarded flag.
func TestResumeRequestBodies(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want map[string]any
	}{
		{name: "omitted", want: map[string]any{}},
		{name: "type", args: []string{"--type", "small"}, want: map[string]any{"type": "small"}},
		{name: "fail fast", args: []string{"--fail-fast"}, want: map[string]any{"failFast": true}},
		{
			name: "env replaces",
			args: []string{"-e", "A=B", "--env", "C=D"},
			want: map[string]any{"env": map[string]any{"A": "B", "C": "D"}},
		},
		{name: "environment", args: []string{"--environment", "base"}, want: map[string]any{"environment": "base"}},
		{name: "no env forwarded", args: []string{"--no-env"}, want: map[string]any{"noEnv": true}},
		{name: "ttl value", args: []string{"--ttl", "7200"}, want: map[string]any{"ttlSeconds": float64(7200)}},
		{name: "ttl null", args: []string{"--no-auto-stop"}, want: map[string]any{"ttlSeconds": nil}},
		{
			name: "combined",
			args: []string{"--type", "large", "-e", "A=B", "--ttl", "60", "--fail-fast"},
			want: map[string]any{
				"type": "large", "env": map[string]any{"A": "B"},
				"ttlSeconds": float64(60), "failFast": true,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
				body := lifecycleTestDecodeBody(t, r)
				if !reflect.DeepEqual(body, tc.want) {
					t.Errorf("body = %v, want %v", body, tc.want)
				}
				cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateProvisioning))
			}, nil)
			args := append([]string{"resume", cliTestSandboxID, "--no-wait"}, tc.args...)
			if status := env.run(t, args...); status != 0 {
				t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
			}
		})
	}
}

// TestResumeFlagErrors proves invalid combinations and values fail before any
// request.
func TestResumeFlagErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "ttl and no-auto-stop", args: []string{"--ttl", "60", "--no-auto-stop"}},
		{name: "ttl zero", args: []string{"--ttl", "0"}},
		{name: "ttl too large", args: []string{"--ttl", "2592001"}},
		{name: "unknown type", args: []string{"--type", "huge"}},
		{name: "malformed env", args: []string{"-e", "NOVALUE"}},
		{name: "empty env key", args: []string{"-e", "=value"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
				t.Error("unexpected HTTP request")
			}, nil)
			args := append([]string{"resume", cliTestSandboxID, "--no-wait"}, tc.args...)
			if status := env.run(t, args...); status != 2 {
				t.Fatalf("exit status = %d, want 2", status)
			}
		})
	}
}

// TestResumeWaitsPollsAndProbes proves a ready state alone is not enough: the
// no-op command must exit zero before the final ready event.
func TestResumeWaitsPollsAndProbes(t *testing.T) {
	var gets, probes atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/resume"):
			cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateProvisioning))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/sandboxes/"+cliTestSandboxID:
			gets.Add(1)
			cliTestWriteJSON(t, w, http.StatusOK,
				api.NewSandboxInfoResponse(lifecycleTestSandbox(cliTestSandboxID, api.StateReady)))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/commands"):
			probes.Add(1)
			body := lifecycleTestDecodeBody(t, r)
			if body["command"] != "true" || body["timeoutSeconds"] != float64(resumeProbeTimeoutSeconds) || body["stream"] != false {
				t.Errorf("probe body = %v", body)
			}
			cliTestWriteJSON(t, w, http.StatusOK, resumeTestCommand())
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}, nil)
	if status := env.run(t, "resume", cliTestSandboxID); status != 0 {
		t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
	}
	if gets.Load() != 1 || probes.Load() != 1 {
		t.Fatalf("gets = %d, probes = %d, want 1 and 1", gets.Load(), probes.Load())
	}
	lines := cliTestJSONLines(t, env.stdout)
	wantEvents := []string{"state", "state", "ready"}
	if len(lines) != len(wantEvents) {
		t.Fatalf("lines = %v, want %v", lines, wantEvents)
	}
	for i, want := range wantEvents {
		if lines[i]["event"] != want {
			t.Fatalf("line %d = %v, want event %q", i, lines[i], want)
		}
	}
	if lines[0]["state"] != string(api.StateProvisioning) || lines[1]["state"] != string(api.StateReady) {
		t.Fatalf("state events = %v", lines[:2])
	}
}

// TestResumeReadyRowWithoutProbeSuccess proves a ready row does not emit the
// final ready event when the probe command fails.
func TestResumeReadyRowWithoutProbeSuccess(t *testing.T) {
	var probes atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/resume"):
			cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateProvisioning))
		case r.Method == http.MethodGet:
			cliTestWriteJSON(t, w, http.StatusOK,
				api.NewSandboxInfoResponse(lifecycleTestSandbox(cliTestSandboxID, api.StateReady)))
		case strings.HasSuffix(r.URL.Path, "/commands"):
			probes.Add(1)
			cliTestWriteJSON(t, w, http.StatusOK, api.NewCommandResponse(false, new(3), "", "", false))
		}
	}, nil)
	if status := env.run(t, "resume", cliTestSandboxID); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if probes.Load() != 1 {
		t.Fatalf("probes = %d, want 1", probes.Load())
	}
	lines := cliTestJSONLines(t, env.stdout)
	for _, line := range lines {
		if line["event"] == "ready" {
			t.Fatalf("emitted a ready event without a successful probe: %v", lines)
		}
	}
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "sandbox_probe_failed" {
		t.Fatalf("last event = %v", last)
	}
}

// TestResumeProbeRetriesTransient proves only a documented transient refusal
// is retried, then ready is emitted after the successful retry.
func TestResumeProbeRetriesTransient(t *testing.T) {
	var probes atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/resume"):
			cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateReady))
		case strings.HasSuffix(r.URL.Path, "/commands"):
			if probes.Add(1) == 1 {
				cliTestWriteJSON(t, w, http.StatusConflict, cliTestErrorEnvelopeWithDetails(
					http.StatusConflict, codeSandboxStarting, "sandbox is starting",
					map[string]any{"retryable": true},
				))
				return
			}
			cliTestWriteJSON(t, w, http.StatusOK, resumeTestCommand())
		case r.Method == http.MethodGet:
			t.Error("a ready acceptance must not be polled again")
		}
	}, nil)
	if status := env.run(t, "resume", cliTestSandboxID); status != 0 {
		t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
	}
	if probes.Load() != 2 {
		t.Fatalf("probes = %d, want 2", probes.Load())
	}
	lines := cliTestJSONLines(t, env.stdout)
	if lines[len(lines)-1]["event"] != "ready" {
		t.Fatalf("lines = %v, want a final ready event", lines)
	}
}

// TestResumeProbeTerminalError proves an auth failure ends the resume at once
// without a ready event or another probe.
func TestResumeProbeTerminalError(t *testing.T) {
	var probes atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/resume") {
			cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateReady))
			return
		}
		probes.Add(1)
		cliTestWriteJSON(t, w, http.StatusUnauthorized, cliTestErrorEnvelope(401, "unauthorized", "bad key"))
	}, nil)
	if status := env.run(t, "resume", cliTestSandboxID); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if probes.Load() != 1 {
		t.Fatalf("probes = %d, want 1", probes.Load())
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "unauthorized" {
		t.Fatalf("last event = %v", last)
	}
	for _, line := range lines {
		if line["event"] == "ready" {
			t.Fatalf("emitted ready after a terminal probe error: %v", lines)
		}
	}
}

// TestResumeProbeCancellation proves cancellation stops the probe without a
// retry, another poll or a ready event.
func TestResumeProbeCancellation(t *testing.T) {
	var resumes, gets, probes atomic.Int64
	var releaseOnce sync.Once
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/resume"):
			resumes.Add(1)
			cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateReady))
		case strings.HasSuffix(r.URL.Path, "/commands"):
			probes.Add(1)
			cancel()
			<-release
		default:
			gets.Add(1)
			cliTestWriteJSON(t, w, http.StatusOK,
				api.NewSandboxInfoResponse(lifecycleTestSandbox(cliTestSandboxID, api.StateReady)))
		}
	}, nil)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	if status := env.runContext(ctx, "resume", cliTestSandboxID); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if resumes.Load() != 1 || gets.Load() != 0 || probes.Load() != 1 {
		t.Fatalf("resumes=%d gets=%d probes=%d, want 1/0/1", resumes.Load(), gets.Load(), probes.Load())
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "cancelled" {
		t.Fatalf("last event = %v", last)
	}
	for _, line := range lines {
		if line["event"] == "ready" {
			t.Fatalf("emitted ready after cancellation: %v", lines)
		}
	}
}

// TestResumeWaitTimeoutCoversPollingAndProbe proves the same bounded wait
// covers state polling and the readiness probe.
func TestResumeWaitTimeoutCoversPollingAndProbe(t *testing.T) {
	t.Run("polling", func(t *testing.T) {
		var probes atomic.Int64
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/resume"):
				cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateProvisioning))
			case strings.HasSuffix(r.URL.Path, "/commands"):
				probes.Add(1)
				cliTestWriteJSON(t, w, http.StatusOK, resumeTestCommand())
			default:
				cliTestWriteJSON(t, w, http.StatusOK,
					api.NewSandboxInfoResponse(lifecycleTestSandbox(cliTestSandboxID, api.StateProvisioning)))
			}
		}, func(opts *Options) {
			opts.WaitTimeout = 100 * time.Millisecond
			opts.PollInterval = 5 * time.Millisecond
		})
		if status := env.run(t, "resume", cliTestSandboxID); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if probes.Load() != 0 {
			t.Fatalf("probes = %d, want 0 before ready", probes.Load())
		}
		lines := cliTestJSONLines(t, env.stdout)
		last := lines[len(lines)-1]
		if last["event"] != "error" || last["code"] != "timeout" {
			t.Fatalf("last event = %v", last)
		}
	})

	t.Run("probe", func(t *testing.T) {
		var probes atomic.Int64
		var releaseOnce sync.Once
		release := make(chan struct{})
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/resume"):
				cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateReady))
			case strings.HasSuffix(r.URL.Path, "/commands"):
				probes.Add(1)
				<-release
			}
		}, func(opts *Options) {
			opts.WaitTimeout = 100 * time.Millisecond
			opts.PollInterval = 5 * time.Millisecond
		})
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
		if status := env.run(t, "resume", cliTestSandboxID); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if probes.Load() != 1 {
			t.Fatalf("probes = %d, want 1", probes.Load())
		}
		lines := cliTestJSONLines(t, env.stdout)
		last := lines[len(lines)-1]
		if last["event"] != "error" || last["code"] != "timeout" {
			t.Fatalf("last event = %v", last)
		}
	})
}

// TestResumeRemoteError proves a stop conflict exits with the pinned error and
// never polls or probes.
func TestResumeRemoteError(t *testing.T) {
	var calls atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/resume") {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		cliTestWriteJSON(t, w, http.StatusConflict,
			cliTestErrorEnvelope(409, "sandbox_stopping", "sandbox is stopping"))
	}, nil)
	if status := env.run(t, "resume", cliTestSandboxID); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "sandbox_stopping" || last["status"] != float64(409) {
		t.Fatalf("last event = %v", last)
	}
}

// TestResumeRejectsMismatchedResponse proves a resume for another sandbox is
// never reported as this sandbox's resume.
func TestResumeRejectsMismatchedResponse(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		response := api.NewResumeActionResponse(cliTestSandboxID2,
			lifecycleTestSandbox(cliTestSandboxID2, api.StateProvisioning))
		cliTestWriteJSON(t, w, http.StatusAccepted, response)
	}, nil)
	if status := env.run(t, "resume", cliTestSandboxID); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "invalid_response" {
		t.Fatalf("last event = %v", last)
	}
}

// TestResumeUnsupportedFlagsForwarded proves --environment and --no-env are
// sent to the accepted API, whose deliberate refusals are surfaced instead of
// the flags being silently dropped.
func TestResumeUnsupportedFlagsForwarded(t *testing.T) {
	t.Run("unknown environment refusal", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			body := lifecycleTestDecodeBody(t, r)
			if body["environment"] != "staging" {
				t.Errorf("body = %v, want environment=staging", body)
			}
			cliTestWriteJSON(t, w, http.StatusBadRequest,
				cliTestErrorEnvelope(http.StatusBadRequest, "unknown_environment", "unknown environment"))
		}, nil)
		if status := env.run(t, "resume", cliTestSandboxID, "--no-wait", "--environment", "staging"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		last := lines[len(lines)-1]
		if last["event"] != "error" || last["code"] != "unknown_environment" {
			t.Fatalf("last event = %v", last)
		}
	})

	t.Run("no env refusal", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			body := lifecycleTestDecodeBody(t, r)
			if body["noEnv"] != true {
				t.Errorf("body = %v, want noEnv=true", body)
			}
			cliTestWriteJSON(t, w, http.StatusBadRequest,
				cliTestErrorEnvelope(http.StatusBadRequest, "invalid_json", "noEnv is not supported in Phase 1"))
		}, nil)
		if status := env.run(t, "resume", cliTestSandboxID, "--no-wait", "--no-env"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		last := lines[len(lines)-1]
		if last["event"] != "error" || last["code"] != "invalid_json" || last["status"] != float64(400) {
			t.Fatalf("last event = %v", last)
		}
	})
}

// TestResumeHumanOutput proves the human state and ready lines, and the
// no-wait acceptance line.
func TestResumeHumanOutput(t *testing.T) {
	t.Run("wait", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/resume"):
				cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateProvisioning))
			case strings.HasSuffix(r.URL.Path, "/commands"):
				cliTestWriteJSON(t, w, http.StatusOK, resumeTestCommand())
			default:
				cliTestWriteJSON(t, w, http.StatusOK,
					api.NewSandboxInfoResponse(lifecycleTestSandbox(cliTestSandboxID, api.StateReady)))
			}
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "resume", cliTestSandboxID); status != 0 {
			t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
		}
		out := env.stdout.String()
		for _, want := range []string{"state provisioning", "state ready", "ready " + cliTestSandboxID} {
			if !strings.Contains(out, want) {
				t.Fatalf("stdout = %q, want %q", out, want)
			}
		}
	})

	t.Run("no wait", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateProvisioning))
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "resume", cliTestSandboxID, "--no-wait"); status != 0 {
			t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
		}
		if got, want := env.stdout.String(), "resuming "+cliTestSandboxID+"\n"; got != want {
			t.Fatalf("stdout = %q, want %q", got, want)
		}
	})
}

// TestResumeArchivedAfterAcceptanceFailsFast proves an accepted resume that
// falls back to archived ends without a probe or a full timeout.
func TestResumeArchivedAfterAcceptanceFailsFast(t *testing.T) {
	var probes atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/resume"):
			cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateProvisioning))
		case strings.HasSuffix(r.URL.Path, "/commands"):
			probes.Add(1)
			cliTestWriteJSON(t, w, http.StatusOK, resumeTestCommand())
		default:
			cliTestWriteJSON(t, w, http.StatusOK,
				api.NewSandboxInfoResponse(lifecycleTestSandbox(cliTestSandboxID, api.StateArchived)))
		}
	}, nil)
	if status := env.run(t, "resume", cliTestSandboxID); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if probes.Load() != 0 {
		t.Fatalf("probes = %d, want 0", probes.Load())
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != codeSandboxNotReady {
		t.Fatalf("last event = %v", last)
	}
}

// TestResumeSharedWaitDeadlineAcrossPollAndProbe proves the same overall
// deadline is attached to the successful poll and the following blocked
// probe. Comparing the recorded context deadlines avoids an elapsed-time race.
func TestResumeSharedWaitDeadlineAcrossPollAndProbe(t *testing.T) {
	var releaseOnce sync.Once
	release := make(chan struct{})

	var polls, probes atomic.Int64
	recorder := &resumeTestDeadlineRecorder{}
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/resume"):
			cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateProvisioning))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/sandboxes/"+cliTestSandboxID:
			polls.Add(1)
			cliTestWriteJSON(t, w, http.StatusOK,
				api.NewSandboxInfoResponse(lifecycleTestSandbox(cliTestSandboxID, api.StateReady)))
		case strings.HasSuffix(r.URL.Path, "/commands"):
			probes.Add(1)
			<-release
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}, func(opts *Options) {
		opts.WaitTimeout = 250 * time.Millisecond
		opts.PollInterval = time.Millisecond
		recorder.base = opts.HTTPClient.Transport
		clone := *opts.HTTPClient
		clone.Transport = recorder
		opts.HTTPClient = &clone
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	if status := env.run(t, "resume", cliTestSandboxID); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if polls.Load() != 1 || probes.Load() != 1 {
		t.Fatalf("polls = %d, probes = %d, want 1 and 1", polls.Load(), probes.Load())
	}
	pollDeadline, ok := recorder.deadline("/api/v1/sandboxes/" + cliTestSandboxID)
	if !ok {
		t.Fatal("poll request carried no deadline")
	}
	probeDeadline, ok := recorder.deadline("/api/v1/sandboxes/" + cliTestSandboxID + "/commands")
	if !ok {
		t.Fatal("probe request carried no deadline")
	}
	if !pollDeadline.Equal(probeDeadline) {
		t.Fatalf("poll deadline %v and probe deadline %v differ; the wait budget was reset", pollDeadline, probeDeadline)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "timeout" {
		t.Fatalf("last event = %v", last)
	}
}

// TestResumeCancellationAfterSuccessfulRead proves a cancel that races a
// successful response read cannot emit a state ready or the final ready event,
// and starts no further RPC.
func TestResumeCancellationAfterSuccessfulRead(t *testing.T) {
	t.Run("poll", func(t *testing.T) {
		var resumes, gets, probes atomic.Int64
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var once sync.Once
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/resume"):
				resumes.Add(1)
				cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateProvisioning))
			case r.Method == http.MethodGet:
				gets.Add(1)
				cliTestWriteJSON(t, w, http.StatusOK,
					api.NewSandboxInfoResponse(lifecycleTestSandbox(cliTestSandboxID, api.StateReady)))
			case strings.HasSuffix(r.URL.Path, "/commands"):
				probes.Add(1)
				cliTestWriteJSON(t, w, http.StatusOK, resumeTestCommand())
			}
		}, func(opts *Options) {
			base := opts.HTTPClient.Transport
			clone := *opts.HTTPClient
			clone.Transport = &resumeTestCancelOnEOF{
				base: base,
				match: func(r *http.Request) bool {
					return r.Method == http.MethodGet && r.URL.Path == "/api/v1/sandboxes/"+cliTestSandboxID
				},
				cancel: cancel,
				once:   &once,
			}
			opts.HTTPClient = &clone
		})
		if status := env.runContext(ctx, "resume", cliTestSandboxID); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if resumes.Load() != 1 || gets.Load() != 1 || probes.Load() != 0 {
			t.Fatalf("resumes=%d gets=%d probes=%d, want 1/1/0", resumes.Load(), gets.Load(), probes.Load())
		}
		lines := cliTestJSONLines(t, env.stdout)
		for _, line := range lines {
			if line["event"] == "ready" || (line["event"] == "state" && line["state"] == string(api.StateReady)) {
				t.Fatalf("emitted ready output after cancellation: %v", lines)
			}
		}
		last := lines[len(lines)-1]
		if last["event"] != "error" || last["code"] != "cancelled" {
			t.Fatalf("last event = %v", last)
		}
	})

	t.Run("probe", func(t *testing.T) {
		var resumes, probes atomic.Int64
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var once sync.Once
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/resume"):
				resumes.Add(1)
				cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateReady))
			case strings.HasSuffix(r.URL.Path, "/commands"):
				probes.Add(1)
				cliTestWriteJSON(t, w, http.StatusOK, resumeTestCommand())
			default:
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			}
		}, func(opts *Options) {
			base := opts.HTTPClient.Transport
			clone := *opts.HTTPClient
			clone.Transport = &resumeTestCancelOnEOF{
				base: base,
				match: func(r *http.Request) bool {
					return r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/commands")
				},
				cancel: cancel,
				once:   &once,
			}
			opts.HTTPClient = &clone
		})
		if status := env.runContext(ctx, "resume", cliTestSandboxID); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if resumes.Load() != 1 || probes.Load() != 1 {
			t.Fatalf("resumes=%d probes=%d, want 1/1", resumes.Load(), probes.Load())
		}
		lines := cliTestJSONLines(t, env.stdout)
		for _, line := range lines {
			if line["event"] == "ready" {
				t.Fatalf("emitted ready after cancellation racing the probe read: %v", lines)
			}
		}
		last := lines[len(lines)-1]
		if last["event"] != "error" || last["code"] != "cancelled" {
			t.Fatalf("last event = %v", last)
		}
	})
}

// TestResumeProbeRetryableClassification pins the exact transient set and the
// explicit retryable gate.
func TestResumeProbeRetryableClassification(t *testing.T) {
	yes, no := new(true), new(false)
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "starting true", err: &APIError{Status: http.StatusConflict, Code: codeSandboxStarting, Retryable: yes}, want: true},
		{name: "starting false", err: &APIError{Status: http.StatusConflict, Code: codeSandboxStarting, Retryable: no}, want: false},
		{name: "starting missing", err: &APIError{Status: http.StatusConflict, Code: codeSandboxStarting}, want: false},
		{name: "not ready true", err: &APIError{Status: http.StatusConflict, Code: codeSandboxNotReady, Retryable: yes}, want: true},
		{name: "not ready false", err: &APIError{Status: http.StatusConflict, Code: codeSandboxNotReady, Retryable: no}, want: false},
		{name: "direct failed true", err: &APIError{Status: http.StatusBadGateway, Code: codeSandboxDirectFailed, Retryable: yes}, want: true},
		{name: "direct failed false", err: &APIError{Status: http.StatusBadGateway, Code: codeSandboxDirectFailed, Retryable: no}, want: false},
		{name: "capacity true", err: &APIError{Status: http.StatusServiceUnavailable, Code: "out_of_capacity", Retryable: yes}, want: false},
		{name: "unauthorized true", err: &APIError{Status: http.StatusUnauthorized, Code: "unauthorized", Retryable: yes}, want: false},
		{name: "conflict other true", err: &APIError{Status: http.StatusConflict, Code: "conflict", Retryable: yes}, want: false},
		{name: "bad gateway other true", err: &APIError{Status: http.StatusBadGateway, Code: "gateway_error", Retryable: yes}, want: false},
		{name: "cli", err: &CLIError{Code: "invalid_response", Message: "bad envelope"}, want: false},
		{name: "exit", err: &ExitError{Code: 1}, want: false},
		{name: "transport", err: errors.New("connection reset"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resumeProbeRetryable(tc.err); got != tc.want {
				t.Fatalf("resumeProbeRetryable(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

// TestResumeProbeNonRetryableDetail proves details.retryable=false ends the
// resume after exactly one probe, with no ready event and no repeated resume.
func TestResumeProbeNonRetryableDetail(t *testing.T) {
	var resumes, probes atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/resume"):
			resumes.Add(1)
			cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateReady))
		case strings.HasSuffix(r.URL.Path, "/commands"):
			probes.Add(1)
			cliTestWriteJSON(t, w, http.StatusConflict, cliTestErrorEnvelopeWithDetails(
				http.StatusConflict, codeSandboxStarting, "sandbox is not ready",
				map[string]any{"retryable": false, "state": "error"},
			))
		}
	}, nil)
	if status := env.run(t, "resume", cliTestSandboxID); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if resumes.Load() != 1 || probes.Load() != 1 {
		t.Fatalf("resumes=%d probes=%d, want 1/1", resumes.Load(), probes.Load())
	}
	lines := cliTestJSONLines(t, env.stdout)
	for _, line := range lines {
		if line["event"] == "ready" {
			t.Fatalf("emitted ready after a non-retryable probe error: %v", lines)
		}
	}
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != codeSandboxStarting {
		t.Fatalf("last event = %v", last)
	}
}

// TestResumeProbeMissingOrMalformedRetryable proves an absent or non-boolean
// hint is terminal rather than an implicit retry.
func TestResumeProbeMissingOrMalformedRetryable(t *testing.T) {
	cases := []struct {
		name    string
		details map[string]any
	}{
		{name: "missing"},
		{name: "null", details: map[string]any{"retryable": nil}},
		{name: "string", details: map[string]any{"retryable": "true"}},
		{name: "number", details: map[string]any{"retryable": 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var probes atomic.Int64
			env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/resume"):
					cliTestWriteJSON(t, w, http.StatusAccepted, resumeTestAccepted(api.StateReady))
				case strings.HasSuffix(r.URL.Path, "/commands"):
					probes.Add(1)
					if tc.details == nil {
						cliTestWriteJSON(t, w, http.StatusConflict,
							cliTestErrorEnvelope(http.StatusConflict, codeSandboxStarting, "sandbox is starting"))
						return
					}
					cliTestWriteJSON(t, w, http.StatusConflict, cliTestErrorEnvelopeWithDetails(
						http.StatusConflict, codeSandboxStarting, "sandbox is starting", tc.details))
				}
			}, nil)
			if status := env.run(t, "resume", cliTestSandboxID); status != 1 {
				t.Fatalf("exit status = %d, want 1", status)
			}
			if probes.Load() != 1 {
				t.Fatalf("probes = %d, want 1", probes.Load())
			}
		})
	}
}
