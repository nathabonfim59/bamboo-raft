package cli

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

// TestStopAsyncAccepted proves the default stop sends one POST with no body,
// returns at 202, follows no URL and emits the pinned action envelope.
func TestStopAsyncAccepted(t *testing.T) {
	validator := compat.Load(t)
	var calls atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/sandboxes/"+cliTestSandboxID+"/stop" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.URL.RawQuery != "" {
			t.Errorf("query = %q, want none", r.URL.RawQuery)
		}
		raw := make([]byte, 1)
		if n, _ := r.Body.Read(raw); n != 0 || r.ContentLength > 0 {
			t.Errorf("stop body = %d bytes, want no body", r.ContentLength)
		}
		stop := lifecycleTestStop(api.StopPending)
		response := lifecycleTestStopAction(api.TypeSandboxStopping, "archiving", api.StateArchiving, stop, false)
		response.StatusURL = new("http://203.0.113.9/never-followed")
		cliTestWriteJSON(t, w, http.StatusAccepted, response)
	}, nil)
	if status := env.run(t, "stop", cliTestSandboxID); status != 0 {
		t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP calls = %d, want 1", calls.Load())
	}
	line := strings.TrimSpace(env.stdout.String())
	lifecycleTestValidate(t, validator, "stop", http.StatusAccepted, []byte(line))
	lines := cliTestJSONLines(t, env.stdout)
	if lines[0]["id"] != cliTestSandboxID || lines[0]["status"] != "archiving" {
		t.Fatalf("response = %v", lines[0])
	}
	if operation := lines[0]["operation"].(map[string]any); operation["id"] != lifecycleTestStopID {
		t.Fatalf("operation = %v", operation)
	}
}

// TestStopForceSendsForce proves --force sends the optional exact body.
func TestStopForceSendsForce(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		body := lifecycleTestDecodeBody(t, r)
		if len(body) != 1 || body["force"] != true {
			t.Errorf("body = %v, want only force=true", body)
		}
		stop := lifecycleTestStop(api.StopPending)
		cliTestWriteJSON(t, w, http.StatusAccepted,
			lifecycleTestStopAction(api.TypeSandboxStopping, "archiving", api.StateArchiving, stop, true))
	}, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
	})
	if status := env.run(t, "stop", cliTestSandboxID, "--force"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
	}
	if !strings.Contains(env.stdout.String(), "stopping "+cliTestSandboxID) {
		t.Fatalf("stdout = %q, want a stopping line", env.stdout)
	}
	if strings.Contains(strings.ToLower(env.stdout.String()), "snapshot") {
		t.Fatalf("stdout = %q, must not claim a snapshot", env.stdout)
	}
}

// TestStopAlreadyStopped proves the pinned 200 answer is accepted and shown as
// stopped.
func TestStopAlreadyStopped(t *testing.T) {
	validator := compat.Load(t)
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		stop := lifecycleTestStop(api.StopCompleted)
		cliTestWriteJSON(t, w, http.StatusOK,
			lifecycleTestStopAction(api.TypeSandboxStopped, "archived", api.StateArchived, stop, false))
	}, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
	})
	if status := env.run(t, "stop", cliTestSandboxID); status != 0 {
		t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
	}
	if !strings.Contains(env.stdout.String(), "stopped "+cliTestSandboxID) {
		t.Fatalf("stdout = %q, want a stopped line", env.stdout)
	}

	jsonEnv := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		stop := lifecycleTestStop(api.StopCompleted)
		cliTestWriteJSON(t, w, http.StatusOK,
			lifecycleTestStopAction(api.TypeSandboxStopped, "archived", api.StateArchived, stop, false))
	}, nil)
	if status := jsonEnv.run(t, "stop", cliTestSandboxID); status != 0 {
		t.Fatalf("json exit status = %d, stderr = %q", status, jsonEnv.stderr)
	}
	lifecycleTestValidate(t, validator, "stop", http.StatusOK,
		[]byte(strings.TrimSpace(jsonEnv.stdout.String())))
}

// TestStopStatusReadsOwnerScopedPath proves --status reads the fixed
// owner+sandbox path and emits the pinned stop envelope.
func TestStopStatusReadsOwnerScopedPath(t *testing.T) {
	validator := compat.Load(t)
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		want := "/api/v1/sandboxes/" + cliTestSandboxID + "/stops/" + lifecycleTestStopID
		if r.Method != http.MethodGet || r.URL.Path != want {
			t.Errorf("request = %s %s, want GET %s", r.Method, r.URL.Path, want)
		}
		cliTestWriteJSON(t, w, http.StatusOK,
			api.NewStopOperationResponse(cliTestSandboxID, lifecycleTestStop(api.StopCompleted)))
	}, nil)
	if status := env.run(t, "stop", cliTestSandboxID, "--status", lifecycleTestStopID, "--yes"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
	}
	lifecycleTestValidate(t, validator, "getStop", http.StatusOK,
		[]byte(strings.TrimSpace(env.stdout.String())))
	lines := cliTestJSONLines(t, env.stdout)
	stop := lines[0]["stop"].(map[string]any)
	if lines[0]["type"] != api.TypeStopStatus || stop["status"] != string(api.StopCompleted) {
		t.Fatalf("response = %v", lines[0])
	}
}

// TestStopStatusHumanOutput covers the human block, including a failing stop.
func TestStopStatusHumanOutput(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		cliTestWriteJSON(t, w, http.StatusOK,
			api.NewStopOperationResponse(cliTestSandboxID, lifecycleTestStop(api.StopFailing)))
	}, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
	})
	if status := env.run(t, "stop", cliTestSandboxID, "--status", lifecycleTestStopID); status != 0 {
		t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
	}
	out := env.stdout.String()
	for _, want := range []string{"stop:", "status:         failing", "error:          disk save failed"} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout = %q, want %q", out, want)
		}
	}
}

// TestStopRemoteStatusFailure proves an unknown stop exits non-zero with the
// pinned error identity.
func TestStopRemoteStatusFailure(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		cliTestWriteJSON(t, w, http.StatusNotFound, cliTestErrorEnvelope(404, "not_found", "stop not found"))
	}, nil)
	if status := env.run(t, "stop", cliTestSandboxID, "--status", lifecycleTestStopID); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "not_found" || last["status"] != float64(404) {
		t.Fatalf("last event = %v", last)
	}
}

// TestStopExplicitEmptyStatusRejected proves an explicitly empty --status is a
// usage error, not an absent status that starts a new stop.
func TestStopExplicitEmptyStatusRejected(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "empty status", args: []string{"--status", ""}},
		{name: "empty status with force", args: []string{"--status", "", "--force"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				t.Error("unexpected HTTP request")
			}, nil)
			args := append([]string{"stop", cliTestSandboxID}, tc.args...)
			if status := env.run(t, args...); status != 2 {
				t.Fatalf("exit status = %d, want 2", status)
			}
			if calls.Load() != 0 {
				t.Fatalf("HTTP calls = %d, want 0", calls.Load())
			}
		})
	}
}

// TestStopRejectsWrongEmbeddedSandboxID proves a present embedded sandbox row
// for another id is rejected before any success output, without printing that
// foreign id.
func TestStopRejectsWrongEmbeddedSandboxID(t *testing.T) {
	response := func() api.SandboxActionResponse {
		stop := lifecycleTestStop(api.StopPending)
		sandbox := lifecycleTestSandbox(cliTestSandboxID2, api.StateArchiving)
		return api.NewStopActionResponse(api.TypeSandboxStopping, cliTestSandboxID, "archiving",
			sandbox, stop, "/api/v1/sandboxes/"+cliTestSandboxID+"/stops/"+stop.ID, false)
	}

	t.Run("human", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusAccepted, response())
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "stop", cliTestSandboxID); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		if env.stdout.Len() != 0 {
			t.Fatalf("stdout = %q, want no success output", env.stdout)
		}
		combined := env.stdout.String() + env.stderr.String()
		if strings.Contains(combined, cliTestSandboxID2) {
			t.Fatalf("output = %q, must not print the foreign id", combined)
		}
	})

	t.Run("json", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusAccepted, response())
		}, nil)
		if status := env.run(t, "stop", cliTestSandboxID); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		last := lines[len(lines)-1]
		if last["event"] != "error" || last["code"] != "invalid_response" {
			t.Fatalf("last event = %v", last)
		}
		if strings.Contains(env.stdout.String(), cliTestSandboxID2) {
			t.Fatalf("stdout = %q, must not print the foreign id", env.stdout)
		}
	})
}

// TestStopStatusWithForceRejected proves the two modes cannot be combined and
// no request is made.
func TestStopStatusWithForceRejected(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("unexpected HTTP request")
	}, nil)
	if status := env.run(t, "stop", cliTestSandboxID, "--status", lifecycleTestStopID, "--force"); status != 2 {
		t.Fatalf("exit status = %d, want 2", status)
	}
	if env.stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want nothing", env.stdout)
	}
}

// TestStopRejectsMismatchedResponse proves a stop for another sandbox is never
// reported as this sandbox's stop.
func TestStopRejectsMismatchedResponse(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		stop := lifecycleTestStop(api.StopPending)
		sandbox := lifecycleTestSandbox(cliTestSandboxID2, api.StateArchiving)
		response := api.NewStopActionResponse(api.TypeSandboxStopping, cliTestSandboxID2, "archiving",
			sandbox, stop, "/api/v1/sandboxes/"+cliTestSandboxID2+"/stops/"+stop.ID, false)
		cliTestWriteJSON(t, w, http.StatusAccepted, response)
	}, nil)
	if status := env.run(t, "stop", cliTestSandboxID); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "invalid_response" {
		t.Fatalf("last event = %v", last)
	}
}
