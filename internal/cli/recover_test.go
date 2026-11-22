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

func TestRecoverSuccessHumanAndJSON(t *testing.T) {
	const recoverMsg = "Recovery started. The virtual machine is being power-cycled with its disk intact."
	resp := api.RecoverResponse{
		SuccessBase: api.SuccessBase{OK: true, Type: api.TypeSandboxRecovering},
		Status:      "recovering",
		Message:     recoverMsg,
	}

	t.Run("human prints message", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/api/v1/sandboxes/"+cliTestSandboxID+"/recover" {
				t.Errorf("request = %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			cliTestWriteJSON(t, w, http.StatusOK, resp)
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "recover", cliTestSandboxID); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		if got := strings.TrimSpace(env.stdout.String()); got != recoverMsg {
			t.Fatalf("stdout = %q, want %q", got, recoverMsg)
		}
	})

	t.Run("json matches v1 API schema", func(t *testing.T) {
		validator := compat.Load(t)
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, resp)
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "recover", cliTestSandboxID, "--json"); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		raw := strings.TrimSpace(env.stdout.String())
		lifecycleTestValidate(t, validator, "recover", http.StatusOK, []byte(raw))
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["type"] != api.TypeSandboxRecovering || lines[0]["message"] != recoverMsg {
			t.Fatalf("lines = %v", lines)
		}
	})
}

func TestRecoverConflictCodes(t *testing.T) {
	cases := []struct {
		code    string
		message string
		wantSub string
	}{
		{
			code:    "no_machine",
			message: "sandbox has no running machine; resume it instead",
			wantSub: "no running machine",
		},
		{
			code:    "too_soon",
			message: "a recovery was already started within the last 10 minutes",
			wantSub: "10 minutes",
		},
		{
			code:    "recover_unsupported",
			message: "the sandbox machine cannot be power-cycled right now",
			wantSub: "power-cycled",
		},
		{
			code:    "sandbox_is_reachable",
			message: "sandbox is reachable and responding normally",
			wantSub: "reachable",
		},
	}

	for _, tc := range cases {
		t.Run(tc.code+" human", func(t *testing.T) {
			env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
				cliTestWriteJSON(t, w, http.StatusConflict,
					cliTestErrorEnvelope(http.StatusConflict, tc.code, tc.message))
			}, func(opts *Options) {
				opts.IsTTY = func(io.Writer) bool { return true }
			})
			if status := env.run(t, "recover", cliTestSandboxID); status != 1 {
				t.Fatalf("exit status = %d, want 1", status)
			}
			if env.stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", env.stdout)
			}
			stderr := strings.TrimRight(env.stderr.String(), "\n")
			if strings.Contains(stderr, "\n") {
				t.Fatalf("stderr is multi-line: %q", stderr)
			}
			if !strings.Contains(stderr, tc.code) || !strings.Contains(stderr, tc.wantSub) {
				t.Fatalf("stderr = %q, want code %q and %q", stderr, tc.code, tc.wantSub)
			}
		})

		t.Run(tc.code+" json", func(t *testing.T) {
			env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
				cliTestWriteJSON(t, w, http.StatusConflict,
					cliTestErrorEnvelope(http.StatusConflict, tc.code, tc.message))
			}, nil)
			if status := env.run(t, "recover", cliTestSandboxID); status != 1 {
				t.Fatalf("exit status = %d, want 1", status)
			}
			lines := cliTestJSONLines(t, env.stdout)
			if len(lines) != 1 || lines[0]["event"] != "error" ||
				lines[0]["code"] != tc.code || lines[0]["status"] != float64(http.StatusConflict) {
				t.Fatalf("lines = %v", lines)
			}
		})
	}

	t.Run("multiline conflict message collapses to one line", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusConflict,
				cliTestErrorEnvelope(http.StatusConflict, "no_machine", "line one\nline two"))
		}, func(opts *Options) {
			opts.IsTTY = func(io.Writer) bool { return true }
		})
		if status := env.run(t, "recover", cliTestSandboxID); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		stderr := strings.TrimRight(env.stderr.String(), "\n")
		if strings.Contains(stderr, "\n") {
			t.Fatalf("stderr is multi-line: %q", stderr)
		}
		if !strings.Contains(stderr, "line one line two") {
			t.Fatalf("stderr = %q", stderr)
		}
	})
}

func TestRecoverUsageErrorsMakeNoRequest(t *testing.T) {
	for _, args := range [][]string{
		{"recover"},
		{"recover", cliTestSandboxID, "extra"},
		{"recover", cliTestSandboxID, "--unknown"},
	} {
		var hits atomic.Int64
		env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, nil)
		if status := env.run(t, args...); status != 2 {
			t.Fatalf("args %v: exit status = %d, want 2", args, status)
		}
		if hits.Load() != 0 {
			t.Fatalf("args %v: server received %d requests, want 0", args, hits.Load())
		}
	}
}

func TestRecoverRejectsWrongEnvelope(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(cliTestSandbox(cliTestSandboxID, api.StateReady)))
	}, nil)
	if status := env.run(t, "recover", cliTestSandboxID); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if len(lines) != 1 || lines[0]["code"] != "invalid_response" {
		t.Fatalf("lines = %v, want invalid_response", lines)
	}
}
