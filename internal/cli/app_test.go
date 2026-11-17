package cli

import (
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spf13/cobra"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// TestCLIUnknownFlagHasNoSideEffects proves Cobra parse failures exit 2 before
// any config write or API request.
func TestCLIUnknownFlagHasNoSideEffects(t *testing.T) {
	var hits atomic.Int64
	env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, nil)
	status := env.run(t, "new", "--not-a-flag")
	if status != 2 {
		t.Fatalf("exit status = %d, want 2", status)
	}
	if hits.Load() != 0 {
		t.Fatalf("server received %d requests, want 0", hits.Load())
	}
	entries, err := os.ReadDir(env.dir)
	if err != nil {
		t.Fatalf("read config dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("config dir contains %v, want empty", entries)
	}
	if !strings.Contains(env.stderr.String(), "not-a-flag") {
		t.Fatalf("stderr = %q, want the flag name", env.stderr.String())
	}
	if env.stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", env.stdout.String())
	}
}

// TestCLIJSONAutoOnNonTTY proves a pipe gets JSON without --json.
func TestCLIJSONAutoOnNonTTY(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != api.PathSandboxes {
			t.Errorf("path = %q, want %q", r.URL.Path, api.PathSandboxes)
		}
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse(nil, api.NewPageInfo(nil, 100)))
	}, nil)
	if status := env.run(t, "list"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if len(lines) != 1 {
		t.Fatalf("lines = %d, want 1: %q", len(lines), env.stdout)
	}
	if lines[0]["type"] != api.TypeSandboxList {
		t.Fatalf("type = %v, want %s", lines[0]["type"], api.TypeSandboxList)
	}
}

// TestCLIHumanOutputOnTTY proves a terminal gets the table, not JSON.
func TestCLIHumanOutputOnTTY(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse(nil, api.NewPageInfo(nil, 100)))
	}, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
	})
	if status := env.run(t, "list"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	if strings.TrimSpace(env.stdout.String()) != "no sandboxes" {
		t.Fatalf("stdout = %q, want the human line", env.stdout.String())
	}
}

// TestCLIJSONErrorEvent proves a local runtime failure ends with the pinned
// JSON error event and a non-zero status.
func TestCLIJSONErrorEvent(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		cliTestWriteJSON(t, w, http.StatusNotFound, cliTestErrorEnvelope(http.StatusNotFound, "not_found", "sandbox not found"))
	}, nil)
	if status := env.run(t, "info", "bx_missing"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if len(lines) != 1 {
		t.Fatalf("lines = %d, want 1: %q", len(lines), env.stdout)
	}
	event := lines[0]
	if event["event"] != "error" || event["code"] != "not_found" || event["status"] != float64(404) {
		t.Fatalf("event = %v, want error/not_found/404", event)
	}
}

// TestCLILateFlagHasNoSideEffects proves an unknown flag after valid flags is
// still a parse failure before any request or config write.
func TestCLILateFlagHasNoSideEffects(t *testing.T) {
	var hits atomic.Int64
	env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, nil)
	if status := env.run(t, "new", "--type", "small", "--not-a-flag"); status != 2 {
		t.Fatalf("exit status = %d, want 2", status)
	}
	if hits.Load() != 0 {
		t.Fatalf("server received %d requests, want 0", hits.Load())
	}
	entries, err := os.ReadDir(env.dir)
	if err != nil {
		t.Fatalf("read config dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("config dir contains %v, want empty", entries)
	}
}

// TestCLIUnknownCommandHasNoSideEffects proves Cobra's unknown-command error is
// a usage error, not an API event.
func TestCLIUnknownCommandHasNoSideEffects(t *testing.T) {
	var hits atomic.Int64
	env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, nil)
	if status := env.run(t, "not-a-command"); status != 2 {
		t.Fatalf("exit status = %d, want 2", status)
	}
	if hits.Load() != 0 {
		t.Fatalf("server received %d requests, want 0", hits.Load())
	}
	if env.stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", env.stdout.String())
	}
}

// TestCLIExplicitJSONOnTTY proves --json wins over the TTY decision.
func TestCLIExplicitJSONOnTTY(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse(nil, api.NewPageInfo(nil, 100)))
	}, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
	})
	if status := env.run(t, "--json", "list"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if len(lines) != 1 || lines[0]["type"] != api.TypeSandboxList {
		t.Fatalf("stdout = %q, want one JSON list", env.stdout)
	}
	if !env.app.out.ExplicitJSON {
		t.Fatal("ExplicitJSON = false, want true")
	}
}

// TestCLIJSONErrorUsesActualHTTPStatus proves the event status is the HTTP
// status, not a disagreeing envelope field.
func TestCLIJSONErrorUsesActualHTTPStatus(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		cliTestWriteJSON(t, w, http.StatusNotFound, cliTestErrorEnvelope(http.StatusInternalServerError, "http_500", "server says 500"))
	}, nil)
	if status := env.run(t, "info", cliTestSandboxID); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if lines[0]["status"] != float64(http.StatusNotFound) {
		t.Fatalf("status = %v, want the actual HTTP 404", lines[0]["status"])
	}
}

// TestCLIExitErrorNilDoesNotPrintAgain proves a remote command can report its
// own status without a duplicate error event.
func TestCLIExitErrorNilDoesNotPrintAgain(t *testing.T) {
	env := cliTestNew(t, func(http.ResponseWriter, *http.Request) {
		t.Error("server must not be called")
	}, func(opts *Options) {
		opts.Commands = []CommandFactory{func(*App) *cobra.Command {
			return &cobra.Command{
				Use: "remote",
				RunE: func(*cobra.Command, []string) error {
					return &ExitError{Code: 7}
				},
			}
		}}
	})
	if status := env.run(t, "remote"); status != 7 {
		t.Fatalf("exit status = %d, want 7", status)
	}
	if env.stdout.Len() != 0 || env.stderr.Len() != 0 {
		t.Fatalf("stdout = %q stderr = %q, want both empty", env.stdout, env.stderr)
	}
}

// cliTestFailWriter always fails, standing in for a closed pipe.
type cliTestFailWriter struct{}

func (cliTestFailWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

// TestCLICommandFailsOnBrokenOutput proves a command does not report success
// after its human output writer failed.
func TestCLICommandFailsOnBrokenOutput(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse(nil, api.NewPageInfo(nil, 100)))
	}, func(opts *Options) {
		opts.Stdout = cliTestFailWriter{}
		opts.IsTTY = func(io.Writer) bool { return true }
	})
	if status := env.run(t, "list"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
}

// TestCLITerminalDetectionRejectsDevNull proves a character device that is not
// a terminal does not enable interactive prompts.
func TestCLITerminalDetectionRejectsDevNull(t *testing.T) {
	file, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open %s: %v", os.DevNull, err)
	}
	defer file.Close()
	if isTerminal(file) {
		t.Fatalf("%s detected as a terminal", os.DevNull)
	}
}

// TestAPIURLFlagOverridesMalformedFallback proves a valid --api-url wins even
// when the environment carries a malformed URL, because validation runs after
// the whole precedence chain.
func TestAPIURLFlagOverridesMalformedFallback(t *testing.T) {
	var hits atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse(nil, api.NewPageInfo(nil, 100)))
	}, nil)
	env.app.opts.Getenv = func(key string) string {
		switch key {
		case "BBRAFT_API_URL":
			return "://not-a-url"
		case "BBRAFT_API_KEY":
			return "test-key"
		default:
			return ""
		}
	}
	if status := env.run(t, "--api-url", env.server.URL, "list"); status != 0 {
		t.Fatalf("exit status = %d, want 0", status)
	}
	if hits.Load() != 1 {
		t.Fatalf("server received %d requests, want 1", hits.Load())
	}
}
