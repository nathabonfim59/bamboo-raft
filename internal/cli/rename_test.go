package cli

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
)

// TestRenamePatchesName proves rename sends only the name and emits the pinned
// updated sandbox envelope.
func TestRenamePatchesName(t *testing.T) {
	validator := compat.Load(t)
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/api/v1/sandboxes/"+cliTestSandboxID {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		body := lifecycleTestDecodeBody(t, r)
		if len(body) != 1 || body["name"] != "api worker" {
			t.Errorf("body = %v, want only name", body)
		}
		sandbox := lifecycleTestSandbox(cliTestSandboxID, api.StateIdle)
		sandbox.Name = "api worker"
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
	}, nil)
	if status := env.run(t, "rename", cliTestSandboxID, "api worker"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
	}
	line := strings.TrimSpace(env.stdout.String())
	lifecycleTestValidate(t, validator, "update", http.StatusOK, []byte(line))
}

// TestRenameHumanOutput covers the human line.
func TestRenameHumanOutput(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		sandbox := lifecycleTestSandbox(cliTestSandboxID, api.StateIdle)
		sandbox.Name = "api worker"
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
	}, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
	})
	if status := env.run(t, "rename", cliTestSandboxID, "api worker"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
	}
	if got, want := env.stdout.String(), "renamed "+cliTestSandboxID+" to \"api worker\"\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
}

// TestRenameNameBounds proves the CLI applies the same 1..120 Unicode limit as
// new, without truncating or sending an invalid request.
func TestRenameNameBounds(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			t.Error("unexpected HTTP request")
		}, nil)
		if status := env.run(t, "rename", cliTestSandboxID, ""); status != 2 {
			t.Fatalf("exit status = %d, want 2", status)
		}
	})
	t.Run("too long", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			t.Error("unexpected HTTP request")
		}, nil)
		if status := env.run(t, "rename", cliTestSandboxID, strings.Repeat("é", maxNameChars+1)); status != 2 {
			t.Fatalf("exit status = %d, want 2", status)
		}
	})
	t.Run("exactly max unicode", func(t *testing.T) {
		name := strings.Repeat("é", maxNameChars)
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			body := lifecycleTestDecodeBody(t, r)
			if body["name"] != name {
				t.Errorf("name = %v, want %d runes", body["name"], maxNameChars)
			}
			sandbox := lifecycleTestSandbox(cliTestSandboxID, api.StateIdle)
			sandbox.Name = name
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
		}, nil)
		if status := env.run(t, "rename", cliTestSandboxID, name); status != 0 {
			t.Fatalf("exit status = %d, stderr = %q", status, env.stderr)
		}
	})
}

// TestRenameRemoteError proves a PATCH refusal exits non-zero without a
// success line.
func TestRenameRemoteError(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
		cliTestWriteJSON(t, w, http.StatusNotFound, cliTestErrorEnvelope(404, "not_found", "sandbox not found"))
	}, nil)
	if status := env.run(t, "rename", cliTestSandboxID, "new name"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "not_found" || last["status"] != float64(404) {
		t.Fatalf("last event = %v", last)
	}
}
