package cli

import (
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

// TestExtendSetsRemainingTime proves --hours and --ttl PATCH the exact
// ttlSeconds value and emit the pinned updated sandbox envelope.
func TestExtendSetsRemainingTime(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		wantTTL any
	}{
		{name: "hours", args: []string{"--hours", "12"}, wantTTL: float64(43200)},
		{name: "ttl", args: []string{"--ttl", "600"}, wantTTL: float64(600)},
		{name: "minimum", args: []string{"--ttl", "1"}, wantTTL: float64(1)},
		{name: "maximum", args: []string{"--hours", "720"}, wantTTL: float64(2592000)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			validator := compat.Load(t)
			env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPatch || r.URL.Path != "/api/v1/sandboxes/"+cliTestSandboxID {
					t.Errorf("request = %s %s", r.Method, r.URL.Path)
				}
				body := lifecycleTestDecodeBody(t, r)
				want := map[string]any{"ttlSeconds": tc.wantTTL}
				if !reflect.DeepEqual(body, want) {
					t.Errorf("body = %v, want %v", body, want)
				}
				cliTestWriteJSON(t, w, http.StatusOK,
					api.NewSandboxInfoResponse(lifecycleTestSandbox(cliTestSandboxID, api.StateIdle)))
			}, nil)
			args := append([]string{"extend", cliTestSandboxID}, tc.args...)
			if status := env.run(t, args...); status != 0 {
				t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
			}
			line := strings.TrimSpace(env.stdout.String())
			lifecycleTestValidate(t, validator, "update", http.StatusOK, []byte(line))
		})
	}
}

// TestExtendNoAutoStop proves the explicit null PATCH and the human wording.
func TestExtendNoAutoStop(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		body := lifecycleTestDecodeBody(t, r)
		want := map[string]any{"ttlSeconds": nil}
		if !reflect.DeepEqual(body, want) {
			t.Errorf("body = %v, want %v", body, want)
		}
		sandbox := cliTestSandbox(cliTestSandboxID, api.StateIdle)
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
	}, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
	})
	if status := env.run(t, "extend", cliTestSandboxID, "--no-auto-stop"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
	}
	if !strings.Contains(env.stdout.String(), "auto-stop disabled") {
		t.Fatalf("stdout = %q, want auto-stop disabled", env.stdout)
	}
}

// TestExtendHumanReportsDeadline proves the human line uses the returned
// archiveAfter and ttlSeconds, not the requested value or an invented local
// deadline.
func TestExtendHumanReportsDeadline(t *testing.T) {
	returnedTTL := 1800
	archive := cliTestTime.Add(time.Duration(returnedTTL) * time.Second)
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		sandbox := lifecycleTestSandbox(cliTestSandboxID, api.StateIdle)
		sandbox.TTLSeconds = &returnedTTL
		sandbox.ArchiveAfter = &archive
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
	}, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
	})
	if status := env.run(t, "extend", cliTestSandboxID, "--hours", "1"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
	}
	out := env.stdout.String()
	if !strings.Contains(out, "auto-stop at "+formatTime(archive)) {
		t.Fatalf("stdout = %q, want the server archiveAfter", out)
	}
	if !strings.Contains(out, "(1800 seconds)") {
		t.Fatalf("stdout = %q, want the returned ttl", out)
	}
	if strings.Contains(out, "3600 seconds") {
		t.Fatalf("stdout = %q, must not echo the requested ttl", out)
	}
}

// TestExtendHumanConfiguredTTLWithoutDeadline proves a non-nil TTL with no
// active archiveAfter is reported as a configured timer, never as disabled;
// only a null TTL means auto-stop is off.
func TestExtendHumanConfiguredTTLWithoutDeadline(t *testing.T) {
	ttl := 7200
	sandbox := lifecycleTestSandbox(cliTestSandboxID, api.StateIdle)
	sandbox.TTLSeconds = &ttl
	sandbox.ArchiveAfter = nil
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
	}, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
	})
	if status := env.run(t, "extend", cliTestSandboxID, "--ttl", "7200"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
	}
	out := env.stdout.String()
	if !strings.Contains(out, "7200 seconds") {
		t.Fatalf("stdout = %q, want the configured ttl", out)
	}
	if strings.Contains(out, "disabled") {
		t.Fatalf("stdout = %q, a non-nil TTL is not disabled", out)
	}
}

// TestExtendSelectorRules proves exactly one selector is required and
// --no-auto-stop=false never counts as one (and never disables the timer).
func TestExtendSelectorRules(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			t.Error("unexpected HTTP request")
		}, nil)
		if status := env.run(t, "extend", cliTestSandboxID); status != 2 {
			t.Fatalf("exit status = %d, want 2", status)
		}
	})
	t.Run("two", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			t.Error("unexpected HTTP request")
		}, nil)
		if status := env.run(t, "extend", cliTestSandboxID, "--hours", "1", "--ttl", "60"); status != 2 {
			t.Fatalf("exit status = %d, want 2", status)
		}
	})
	t.Run("false alone", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			t.Error("unexpected HTTP request")
		}, nil)
		if status := env.run(t, "extend", cliTestSandboxID, "--no-auto-stop=false"); status != 2 {
			t.Fatalf("exit status = %d, want 2", status)
		}
	})
	t.Run("false with hours keeps timer", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			body := lifecycleTestDecodeBody(t, r)
			want := map[string]any{"ttlSeconds": float64(3600)}
			if !reflect.DeepEqual(body, want) {
				t.Errorf("body = %v, want %v", body, want)
			}
			cliTestWriteJSON(t, w, http.StatusOK,
				api.NewSandboxInfoResponse(lifecycleTestSandbox(cliTestSandboxID, api.StateIdle)))
		}, nil)
		if status := env.run(t, "extend", cliTestSandboxID, "--hours", "1", "--no-auto-stop=false"); status != 0 {
			t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
		}
	})
}

// TestExtendRangeErrors proves out-of-range and fractional hours fail before
// any request.
func TestExtendRangeErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "hours zero", args: []string{"--hours", "0"}},
		{name: "hours too large", args: []string{"--hours", "721"}},
		{name: "hours fractional", args: []string{"--hours", "1.5"}},
		{name: "ttl zero", args: []string{"--ttl", "0"}},
		{name: "ttl too large", args: []string{"--ttl", "2592001"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
				t.Error("unexpected HTTP request")
			}, nil)
			args := append([]string{"extend", cliTestSandboxID}, tc.args...)
			if status := env.run(t, args...); status != 2 {
				t.Fatalf("exit status = %d, want 2", status)
			}
		})
	}
}

// TestExtendRemoteError proves a PATCH refusal exits non-zero.
func TestExtendRemoteError(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		cliTestWriteJSON(t, w, http.StatusNotFound, cliTestErrorEnvelope(404, "not_found", "sandbox not found"))
	}, nil)
	if status := env.run(t, "extend", cliTestSandboxID, "--ttl", "60"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "not_found" {
		t.Fatalf("last event = %v", last)
	}
}
