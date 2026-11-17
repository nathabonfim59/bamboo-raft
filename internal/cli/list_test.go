package cli

import (
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

func TestListSendsStateFilterAndDecodesDTO(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != api.PathSandboxes {
			t.Errorf("path = %q", r.URL.Path)
		}
		if got, want := r.URL.Query().Get("state"), "ready,cloning,idle,running"; got != want {
			t.Errorf("state = %q, want %q", got, want)
		}
		if got := r.URL.Query().Get("limit"); got != "100" {
			t.Errorf("limit = %q, want 100", got)
		}
		sandbox := cliTestSandbox(cliTestSandboxID, api.StateReady)
		sandbox.Name = "one"
		sandbox.Type = "small"
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse([]api.Sandbox{
			sandbox,
		}, api.NewPageInfo(nil, 100)))
	}, nil)
	if status := env.run(t, "list"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if len(lines) != 1 {
		t.Fatalf("lines = %d, want 1", len(lines))
	}
	sandboxes, ok := lines[0]["sandboxes"].([]any)
	if !ok || len(sandboxes) != 1 {
		t.Fatalf("sandboxes = %v", lines[0]["sandboxes"])
	}
	first := sandboxes[0].(map[string]any)
	if first["id"] != cliTestSandboxID || first["state"] != "ready" {
		t.Fatalf("sandbox = %v", first)
	}
}

func TestListAllFollowsCursor(t *testing.T) {
	var pages atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("state"); got != "" {
			t.Errorf("state = %q, want none with --all", got)
		}
		switch pages.Add(1) {
		case 1:
			if r.URL.Query().Get("cursor") != "" {
				t.Errorf("first page cursor = %q, want empty", r.URL.Query().Get("cursor"))
			}
			next := "cursor-2"
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse([]api.Sandbox{
				cliTestSandbox(cliTestSandboxID, api.StateArchived),
			}, api.NewPageInfo(&next, 1)))
		case 2:
			if got := r.URL.Query().Get("cursor"); got != "cursor-2" {
				t.Errorf("second page cursor = %q, want cursor-2", got)
			}
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse([]api.Sandbox{
				cliTestSandbox(cliTestSandboxID2, api.StateArchived),
			}, api.NewPageInfo(nil, 1)))
		default:
			t.Error("unexpected extra page")
		}
	}, nil)
	if status := env.run(t, "list", "--all", "--limit", "1"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	lines := cliTestJSONLines(t, env.stdout)
	sandboxes := lines[0]["sandboxes"].([]any)
	if len(sandboxes) != 2 {
		t.Fatalf("sandboxes = %d, want 2", len(sandboxes))
	}
	page := lines[0]["pageInfo"].(map[string]any)
	if page["hasMore"] != false || page["nextCursor"] != nil {
		t.Fatalf("pageInfo = %v, want a final page", page)
	}
}

func TestListRejectsBadFilterBeforeRequest(t *testing.T) {
	tests := [][]string{
		{"list", "--filter", "z"},
		{"list", "--filter", ""},
		{"list", "--limit", "0"},
		{"list", "--limit", "201"},
	}
	for _, args := range tests {
		var hits atomic.Int64
		env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, nil)
		if status := env.run(t, args...); status != 2 {
			t.Fatalf("%v: exit status = %d, want 2", args, status)
		}
		if hits.Load() != 0 {
			t.Fatalf("%v: server received %d requests, want 0", args, hits.Load())
		}
	}
}

func TestListAPIURLFlagOverridesEnvironment(t *testing.T) {
	var first, second atomic.Int64
	firstEnv := cliTestNew(t, func(http.ResponseWriter, *http.Request) { first.Add(1) }, nil)
	secondServer := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		second.Add(1)
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse(nil, api.NewPageInfo(nil, 100)))
	}, nil)
	if status := firstEnv.run(t, "--api-url", secondServer.server.URL, "list"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, firstEnv.stderr)
	}
	if first.Load() != 0 {
		t.Fatalf("environment server received %d requests, want 0", first.Load())
	}
	if second.Load() != 1 {
		t.Fatalf("flag server received %d requests, want 1", second.Load())
	}
}

// TestListFilteredFollowsCursor proves a filtered list keeps its state query
// on every page and merges the final metadata.
func TestListFilteredFollowsCursor(t *testing.T) {
	var pages atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Query().Get("state"), "ready,cloning,idle,running"; got != want {
			t.Errorf("state = %q, want %q", got, want)
		}
		switch pages.Add(1) {
		case 1:
			if r.URL.Query().Get("cursor") != "" {
				t.Errorf("first page cursor = %q, want empty", r.URL.Query().Get("cursor"))
			}
			next := "cursor-2"
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse([]api.Sandbox{
				cliTestSandbox(cliTestSandboxID, api.StateReady),
			}, api.NewPageInfo(&next, 1)))
		case 2:
			if got := r.URL.Query().Get("cursor"); got != "cursor-2" {
				t.Errorf("second page cursor = %q, want cursor-2", got)
			}
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSandboxListResponse([]api.Sandbox{
				cliTestSandbox(cliTestSandboxID2, api.StateRunning),
			}, api.NewPageInfo(nil, 1)))
		default:
			t.Error("unexpected extra page")
		}
	}, nil)
	if status := env.run(t, "list", "--limit", "1"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	if pages.Load() != 2 {
		t.Fatalf("pages = %d, want 2", pages.Load())
	}
	lines := cliTestJSONLines(t, env.stdout)
	sandboxes := lines[0]["sandboxes"].([]any)
	if len(sandboxes) != 2 {
		t.Fatalf("sandboxes = %d, want 2", len(sandboxes))
	}
	page := lines[0]["pageInfo"].(map[string]any)
	if page["hasMore"] != false || page["nextCursor"] != nil || page["limit"] != float64(1) {
		t.Fatalf("pageInfo = %v, want final page with limit 1", page)
	}
}

// TestListRejectsHasMoreWithoutCursor proves a truncated pagination answer is
// an error, not a silent partial success.
func TestListRejectsHasMoreWithoutCursor(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		cliTestWriteJSON(t, w, http.StatusOK, api.SandboxListResponse{
			SuccessBase: api.SuccessBase{OK: true, Type: api.TypeSandboxList},
			Sandboxes:   []api.Sandbox{cliTestSandbox(cliTestSandboxID, api.StateReady)},
			PageInfo:    api.PageInfo{HasMore: true, NextCursor: nil, Limit: 100},
		})
	}, nil)
	if status := env.run(t, "list"); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "invalid_response" {
		t.Fatalf("last event = %v, want invalid_response", last)
	}
}
