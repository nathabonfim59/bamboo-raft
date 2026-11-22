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

func TestForkFlagsToRequestBody(t *testing.T) {
	cases := []struct {
		name  string
		args  []string
		check func(t *testing.T, body map[string]any)
	}{
		{
			name: "default empty body",
			args: []string{"fork", cliTestSandboxID},
			check: func(t *testing.T, body map[string]any) {
				if len(body) != 0 {
					t.Errorf("body = %v, want empty object", body)
				}
			},
		},
		{
			name: "all flags with ttl",
			args: []string{
				"fork", cliTestSandboxID,
				"--type", "large",
				"-e", "A=1",
				"--env", "B=two=2",
				"--ttl", "900",
				"--fail-fast",
				"--label", "env=staging",
				"--label", "team=core",
			},
			check: func(t *testing.T, body map[string]any) {
				if body["type"] != "large" {
					t.Errorf("type = %v, want large", body["type"])
				}
				if body["ttlSeconds"] != float64(900) {
					t.Errorf("ttlSeconds = %v, want 900", body["ttlSeconds"])
				}
				if body["failFast"] != true {
					t.Errorf("failFast = %v, want true", body["failFast"])
				}
				env, ok := body["env"].(map[string]any)
				if !ok || env["A"] != "1" || env["B"] != "two=2" {
					t.Errorf("env = %v", body["env"])
				}
				labels, ok := body["labels"].(map[string]any)
				if !ok || labels["env"] != "staging" || labels["team"] != "core" {
					t.Errorf("labels = %v", body["labels"])
				}
			},
		},
		{
			name: "no auto stop sends ttlSeconds null",
			args: []string{"fork", cliTestSandboxID, "--no-auto-stop"},
			check: func(t *testing.T, body map[string]any) {
				val, present := body["ttlSeconds"]
				if !present || val != nil {
					t.Errorf("ttlSeconds = %v (present=%v), want explicit null", val, present)
				}
				if len(body) != 1 {
					t.Errorf("body = %v, want only ttlSeconds", body)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotBody map[string]any
			env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/api/v1/sandboxes/"+cliTestSandboxID+"/fork" {
					t.Errorf("request = %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
					return
				}
				gotBody = lifecycleTestDecodeBody(t, r)
				cliTestWriteJSON(t, w, http.StatusAccepted, api.NewForkActionResponse(
					cliTestSandboxID2, cliTestSandbox(cliTestSandboxID2, api.StateProvisioning),
				))
			}, nil)
			if status := env.run(t, tc.args...); status != 0 {
				t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
			}
			tc.check(t, gotBody)
		})
	}
}

func TestForkOutputTableAndJSON(t *testing.T) {
	t.Run("human prints new sandbox id", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewForkActionResponse(
				cliTestSandboxID2, cliTestSandbox(cliTestSandboxID2, api.StateProvisioning),
			))
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "fork", cliTestSandboxID); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		if got := strings.TrimSpace(env.stdout.String()); got != cliTestSandboxID2 {
			t.Fatalf("stdout = %q, want %q", got, cliTestSandboxID2)
		}
	})

	t.Run("json matches v1 API schema", func(t *testing.T) {
		validator := compat.Load(t)
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewForkActionResponse(
				cliTestSandboxID2, cliTestSandbox(cliTestSandboxID2, api.StateProvisioning),
			))
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "fork", cliTestSandboxID, "--json"); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		raw := strings.TrimSpace(env.stdout.String())
		lifecycleTestValidate(t, validator, "fork", http.StatusAccepted, []byte(raw))
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["type"] != api.TypeSandboxForking || lines[0]["id"] != cliTestSandboxID2 {
			t.Fatalf("lines = %v", lines)
		}
	})
}

func TestForkWaitPollsUntilReady(t *testing.T) {
	t.Run("polls provisioning to ready in json mode", func(t *testing.T) {
		var polls atomic.Int64
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sandboxes/"+cliTestSandboxID+"/fork":
				cliTestWriteJSON(t, w, http.StatusAccepted, api.NewForkActionResponse(
					cliTestSandboxID2, cliTestSandbox(cliTestSandboxID2, api.StateProvisioning),
				))
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/sandboxes/"+cliTestSandboxID2:
				state := api.StateProvisioning
				if polls.Add(1) >= 2 {
					state = api.StateReady
				}
				cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(cliTestSandbox(cliTestSandboxID2, state)))
			default:
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
			}
		}, nil)
		if status := env.run(t, "fork", cliTestSandboxID, "--wait"); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 3 || lines[0]["event"] != "created" || lines[1]["event"] != "state" || lines[2]["event"] != "ready" {
			t.Fatalf("lines = %v, want created/state/ready", lines)
		}
		if lines[0]["id"] != cliTestSandboxID2 || lines[2]["id"] != cliTestSandboxID2 {
			t.Fatalf("ids = %v/%v, want %s", lines[0]["id"], lines[2]["id"], cliTestSandboxID2)
		}
	})

	t.Run("polls provisioning to ready in human mode", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				cliTestWriteJSON(t, w, http.StatusAccepted, api.NewForkActionResponse(
					cliTestSandboxID2, cliTestSandbox(cliTestSandboxID2, api.StateProvisioning),
				))
				return
			}
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(cliTestSandbox(cliTestSandboxID2, api.StateReady)))
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "fork", cliTestSandboxID, "--wait"); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		out := env.stdout.String()
		for _, want := range []string{"created " + cliTestSandboxID2, "state ready", "ready " + cliTestSandboxID2} {
			if !strings.Contains(out, want) {
				t.Errorf("human wait output missing %q:\n%s", want, out)
			}
		}
	})

	t.Run("already ready emits created state ready", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Errorf("unexpected poll %s %s", r.Method, r.URL.Path)
			}
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewForkActionResponse(
				cliTestSandboxID2, cliTestSandbox(cliTestSandboxID2, api.StateReady),
			))
		}, nil)
		if status := env.run(t, "fork", cliTestSandboxID, "--wait"); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 3 || lines[0]["event"] != "created" || lines[1]["event"] != "state" || lines[2]["event"] != "ready" {
			t.Fatalf("lines = %v, want created/state/ready", lines)
		}
	})

	t.Run("terminal error state exits 1", func(t *testing.T) {
		reason := "restore failed"
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				cliTestWriteJSON(t, w, http.StatusAccepted, api.NewForkActionResponse(
					cliTestSandboxID2, cliTestSandbox(cliTestSandboxID2, api.StateProvisioning),
				))
				return
			}
			failed := cliTestSandbox(cliTestSandboxID2, api.StateError)
			failed.Error = &reason
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(failed))
		}, nil)
		if status := env.run(t, "fork", cliTestSandboxID, "--wait"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		last := lines[len(lines)-1]
		if last["event"] != "error" || last["code"] != "sandbox_error" {
			t.Fatalf("last = %v, want sandbox_error", last)
		}
	})

	t.Run("timeout and cancellation", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				cliTestWriteJSON(t, w, http.StatusAccepted, api.NewForkActionResponse(
					cliTestSandboxID2, cliTestSandbox(cliTestSandboxID2, api.StateProvisioning),
				))
				return
			}
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(cliTestSandbox(cliTestSandboxID2, api.StateProvisioning)))
		}, func(opts *Options) {
			opts.WaitTimeout = 20 * time.Millisecond
			opts.PollInterval = 2 * time.Millisecond
		})
		if status := env.run(t, "fork", cliTestSandboxID, "--wait"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if last := lines[len(lines)-1]; last["code"] != "timeout" {
			t.Fatalf("last = %v, want timeout", last)
		}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		env = cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				cliTestWriteJSON(t, w, http.StatusAccepted, api.NewForkActionResponse(
					cliTestSandboxID2, cliTestSandbox(cliTestSandboxID2, api.StateProvisioning),
				))
				return
			}
			cancel()
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(cliTestSandbox(cliTestSandboxID2, api.StateProvisioning)))
		}, nil)
		if status := env.runContext(ctx, "fork", cliTestSandboxID, "--wait"); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines = cliTestJSONLines(t, env.stdout)
		if last := lines[len(lines)-1]; last["code"] != "cancelled" {
			t.Fatalf("last = %v, want cancelled", last)
		}
	})
}

func TestForkRejectsUsageErrorsBeforeRequest(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		secret string
	}{
		{name: "missing id", args: []string{"fork"}},
		{name: "extra arg", args: []string{"fork", cliTestSandboxID, "extra"}},
		{name: "unknown type", args: []string{"fork", cliTestSandboxID, "--type", "giant"}},
		{name: "empty type", args: []string{"fork", cliTestSandboxID, "--type", ""}},
		{name: "ttl low", args: []string{"fork", cliTestSandboxID, "--ttl", "0"}},
		{name: "ttl high", args: []string{"fork", cliTestSandboxID, "--ttl", "2592001"}},
		{name: "ttl and no-auto-stop", args: []string{"fork", cliTestSandboxID, "--ttl", "60", "--no-auto-stop"}},
		{name: "env missing equals", args: []string{"fork", cliTestSandboxID, "-e", "SECRET_ENV_VALUE"}, secret: "SECRET_ENV_VALUE"},
		{name: "env empty key", args: []string{"fork", cliTestSandboxID, "-e", "=SECRET_ENV_VALUE"}, secret: "SECRET_ENV_VALUE"},
		{name: "env nul byte", args: []string{"fork", cliTestSandboxID, "-e", "A=val\x00ue"}},
		{name: "label missing equals", args: []string{"fork", cliTestSandboxID, "--label", "BAD_LABEL"}},
		{name: "label empty key", args: []string{"fork", cliTestSandboxID, "--label", "=val"}},
		{name: "label nul byte", args: []string{"fork", cliTestSandboxID, "--label", "k=v\x00"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int64
			env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, nil)
			if status := env.run(t, tc.args...); status != 2 {
				t.Fatalf("exit status = %d, want 2 (stderr=%s)", status, env.stderr)
			}
			if hits.Load() != 0 {
				t.Fatalf("server received %d requests, want 0", hits.Load())
			}
			if tc.secret != "" && strings.Contains(env.stderr.String(), tc.secret) {
				t.Fatal("stderr leaked secret value")
			}
		})
	}
}

func TestForkAPIErrorsAndMalformedResponses(t *testing.T) {
	errorCases := []struct {
		status int
		code   string
	}{
		{http.StatusNotFound, "not_found"},
		{http.StatusConflict, "snapshots_disabled"},
		{http.StatusConflict, "snapshot_data_expired"},
		{http.StatusConflict, "type_too_small"},
		{http.StatusConflict, "fork_failed"},
		{http.StatusServiceUnavailable, "no_ready_machine"},
	}
	for _, tc := range errorCases {
		t.Run(tc.code, func(t *testing.T) {
			env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
				cliTestWriteJSON(t, w, tc.status, cliTestErrorEnvelope(tc.status, tc.code, "fork refused"))
			}, nil)
			if status := env.run(t, "fork", cliTestSandboxID); status != 1 {
				t.Fatalf("exit status = %d, want 1", status)
			}
			lines := cliTestJSONLines(t, env.stdout)
			if len(lines) != 1 || lines[0]["code"] != tc.code || lines[0]["status"] != float64(tc.status) {
				t.Fatalf("lines = %v", lines)
			}
		})
	}

	malformed := []struct {
		name string
		resp any
	}{
		{
			name: "empty fork id",
			resp: api.NewForkActionResponse("", cliTestSandbox("", api.StateProvisioning)),
		},
		{
			name: "fork id equals source id",
			resp: api.NewForkActionResponse(cliTestSandboxID, cliTestSandbox(cliTestSandboxID, api.StateProvisioning)),
		},
		{
			name: "mismatched sandbox id",
			resp: api.NewForkActionResponse(cliTestSandboxID2, cliTestSandbox(cliTestSandboxID, api.StateProvisioning)),
		},
		{
			name: "wrong envelope type",
			resp: api.NewSandboxInfoResponse(cliTestSandbox(cliTestSandboxID2, api.StateProvisioning)),
		},
	}
	for _, tc := range malformed {
		t.Run(tc.name, func(t *testing.T) {
			env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
				raw, err := json.Marshal(tc.resp)
				if err != nil {
					t.Errorf("marshal: %v", err)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write(raw)
			}, nil)
			if status := env.run(t, "fork", cliTestSandboxID); status != 1 {
				t.Fatalf("exit status = %d, want 1", status)
			}
			lines := cliTestJSONLines(t, env.stdout)
			if len(lines) != 1 || lines[0]["code"] != "invalid_response" {
				t.Fatalf("lines = %v, want invalid_response", lines)
			}
		})
	}
}
