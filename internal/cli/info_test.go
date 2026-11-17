package cli

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

func TestInfoDecodesPinnedDTO(t *testing.T) {
	ip := "203.0.113.10"
	ttl := 3600
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/sandboxes/"+cliTestSandboxID {
			t.Errorf("path = %q", r.URL.Path)
		}
		sandbox := cliTestSandbox(cliTestSandboxID, api.StateIdle)
		sandbox.Name = "one"
		sandbox.Type = "small"
		sandbox.IP = &ip
		sandbox.TTLSeconds = &ttl
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
	}, nil)
	if status := env.run(t, "info", cliTestSandboxID); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if lines[0]["type"] != api.TypeSandboxInfo {
		t.Fatalf("type = %v, want %s", lines[0]["type"], api.TypeSandboxInfo)
	}
	sandbox := lines[0]["sandbox"].(map[string]any)
	if sandbox["id"] != cliTestSandboxID || sandbox["ip"] != ip || sandbox["ttlSeconds"] != float64(ttl) {
		t.Fatalf("sandbox = %v", sandbox)
	}
}

func TestInfoHumanOutput(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		sandbox := cliTestSandbox(cliTestSandboxID, api.StateIdle)
		sandbox.Name = "one"
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxInfoResponse(sandbox))
	}, func(opts *Options) {
		opts.IsTTY = func(io.Writer) bool { return true }
	})
	if status := env.run(t, "info", cliTestSandboxID); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	if !strings.Contains(env.stdout.String(), "state:        idle") {
		t.Fatalf("stdout = %q, want the human state line", env.stdout)
	}
}
