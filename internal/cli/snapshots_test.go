package cli

import (
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

// snapshotsTestTime is the fixed cut time of the CLI snapshot fixtures.
var snapshotsTestTime = cliTestTime.Add(2 * time.Hour)

// snapshotsTestSummary builds one pinned-shaped snapshot summary.
func snapshotsTestSummary(id, sandboxID string, generation int64, kind api.SnapshotKind) api.SnapshotSummary {
	completed := snapshotsTestTime.Add(30 * time.Second)
	size := int64(18 << 20)
	content := int64(3 << 20)
	chain := "018f4b1c-7e2d-7a3b-9c4d-000000000456"
	return api.SnapshotSummary{
		ID:               id,
		SandboxID:        sandboxID,
		Status:           api.SnapshotCompleted,
		Kind:             &kind,
		Generation:       &generation,
		ChainID:          &chain,
		CreatedAt:        snapshotsTestTime,
		CompletedAt:      &completed,
		SizeBytes:        &size,
		ContentSizeBytes: &content,
		Origin:           api.SnapshotCaptured,
	}
}

// snapshotsCompatValidator loads the v1 API document.
func snapshotsCompatValidator(t *testing.T) *compat.Validator {
	t.Helper()
	return compat.Load(t)
}

// snapshotsAssertPinned validates the JSON envelopes a command wrote against
// the pinned document.
func snapshotsAssertPinned(t *testing.T, raw string, status int, operationID string) {
	t.Helper()
	if strings.TrimSpace(raw) == "" {
		return
	}
	validator := snapshotsCompatValidator(t)
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		header := http.Header{"Content-Type": []string{"application/json"}}
		if err := validator.Response(operationID, status, header, []byte(line)); err != nil {
			t.Errorf("emitted %s does not match the v1 API document: %v", operationID, err)
		}
	}
}

func TestSnapshotsTableOutput(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != api.PathSnapshots {
			t.Errorf("path = %q, want %q", r.URL.Path, api.PathSnapshots)
		}
		if got := r.URL.Query().Get("limit"); got != "100" {
			t.Errorf("limit = %q, want 100", got)
		}
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSnapshotListResponse([]api.SnapshotSummary{
			snapshotsTestSummary("018f4b1c-7e2d-7a3b-9c4d-000000000123", cliTestSandboxID, 0, api.SnapshotBase),
			snapshotsTestSummary("018f4b1c-7e2d-7a3b-9c4d-000000000124", cliTestSandboxID2, 3, api.SnapshotIncremental),
		}, api.NewPageInfo(nil, 100)))
	}, func(opts *Options) { opts.IsTTY = func(io.Writer) bool { return true } })
	if status := env.run(t, "snapshots"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	out := env.stdout.String()
	for _, want := range []string{"ID", "SANDBOX", "GEN", "KIND", "SIZE", "CREATED",
		"018f4b1c-7e2d-7a3b-9c4d-000000000123", cliTestSandboxID, "base", "18874368"} {
		if !strings.Contains(out, want) {
			t.Errorf("table output is missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, `"ok"`) {
		t.Errorf("table output looks like JSON:\n%s", out)
	}
}

func TestSnapshotsJSONOutputMatchesPinnedDocument(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/sandboxes/"+cliTestSandboxID+"/snapshots" {
			t.Errorf("path = %q", r.URL.Path)
		}
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSnapshotListResponse([]api.SnapshotSummary{
			snapshotsTestSummary("018f4b1c-7e2d-7a3b-9c4d-000000000123", cliTestSandboxID, 1, api.SnapshotIncremental),
		}, api.NewPageInfo(nil, 100)))
	}, nil)
	if status := env.run(t, "snapshots", cliTestSandboxID); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	snapshotsAssertPinned(t, env.stdout.String(), http.StatusOK, "listSandboxSnapshots")
	lines := cliTestJSONLines(t, env.stdout)
	if len(lines) != 1 {
		t.Fatalf("lines = %d, want 1", len(lines))
	}
	snapshots := lines[0]["snapshots"].([]any)
	if len(snapshots) != 1 {
		t.Fatalf("snapshots = %v", snapshots)
	}
	first := snapshots[0].(map[string]any)
	if first["fileCount"] != nil || first["contentFileCount"] != nil {
		t.Fatalf("file counts = %v/%v, want null", first["fileCount"], first["contentFileCount"])
	}
	if first["origin"] != "captured" || first["kind"] != "incremental" {
		t.Fatalf("snapshot = %v", first)
	}
}

func TestSnapshotsAllFollowsCursor(t *testing.T) {
	var pages atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		switch pages.Add(1) {
		case 1:
			if got := r.URL.Query().Get("cursor"); got != "" {
				t.Errorf("first page cursor = %q, want empty", got)
			}
			next := "cursor-2"
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSnapshotListResponse([]api.SnapshotSummary{
				snapshotsTestSummary("018f4b1c-7e2d-7a3b-9c4d-000000000123", cliTestSandboxID, 0, api.SnapshotBase),
			}, api.NewPageInfo(&next, 1)))
		case 2:
			if got := r.URL.Query().Get("cursor"); got != "cursor-2" {
				t.Errorf("second page cursor = %q, want cursor-2", got)
			}
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSnapshotListResponse([]api.SnapshotSummary{
				snapshotsTestSummary("018f4b1c-7e2d-7a3b-9c4d-000000000124", cliTestSandboxID, 1, api.SnapshotIncremental),
			}, api.NewPageInfo(nil, 1)))
		default:
			t.Error("unexpected extra page")
		}
	}, nil)
	if status := env.run(t, "snapshots", "--all", "--limit", "1"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	lines := cliTestJSONLines(t, env.stdout)
	snapshots := lines[0]["snapshots"].([]any)
	if len(snapshots) != 2 {
		t.Fatalf("snapshots = %d, want 2", len(snapshots))
	}
	page := lines[0]["pageInfo"].(map[string]any)
	if page["hasMore"] != false || page["nextCursor"] != nil {
		t.Fatalf("pageInfo = %v, want a final page", page)
	}
}

func TestSnapshotsWithoutAllReadsOnePage(t *testing.T) {
	var hits atomic.Int64
	next := "cursor-2"
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSnapshotListResponse([]api.SnapshotSummary{
			snapshotsTestSummary("018f4b1c-7e2d-7a3b-9c4d-000000000123", cliTestSandboxID, 0, api.SnapshotBase),
		}, api.NewPageInfo(&next, 1)))
	}, nil)
	if status := env.run(t, "snapshots"); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	if hits.Load() != 1 {
		t.Fatalf("requests = %d, want 1", hits.Load())
	}
	page := cliTestJSONLines(t, env.stdout)[0]["pageInfo"].(map[string]any)
	if page["hasMore"] != true || page["nextCursor"] != next {
		t.Fatalf("pageInfo = %v, want the page cursor", page)
	}
}

func TestSnapshotsRejectsBadLimitBeforeRequest(t *testing.T) {
	for _, limit := range []string{"0", "201", "ten"} {
		var hits atomic.Int64
		env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, nil)
		if status := env.run(t, "snapshots", "--limit", limit); status != 2 {
			t.Fatalf("--limit %s: exit status = %d, want 2", limit, status)
		}
		if hits.Load() != 0 {
			t.Fatalf("--limit %s: server received %d requests, want 0", limit, hits.Load())
		}
	}
}

func TestSnapshotLatestTableAndJSON(t *testing.T) {
	t.Run("table", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/v1/sandboxes/"+cliTestSandboxID+"/snapshots/latest" {
				t.Errorf("path = %q", r.URL.Path)
			}
			snapshot := snapshotsTestSummary("018f4b1c-7e2d-7a3b-9c4d-000000000123", cliTestSandboxID, 2, api.SnapshotIncremental)
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSnapshotLatestResponse(&snapshot))
		}, func(opts *Options) { opts.IsTTY = func(io.Writer) bool { return true } })
		if status := env.run(t, "snapshot", "latest", cliTestSandboxID); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		if !strings.Contains(env.stdout.String(), "018f4b1c-7e2d-7a3b-9c4d-000000000123") {
			t.Fatalf("output = %q", env.stdout.String())
		}
	})

	t.Run("json null", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSnapshotLatestResponse(nil))
		}, nil)
		if status := env.run(t, "snapshot", "latest", cliTestSandboxID); status != 0 {
			t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
		}
		snapshotsAssertPinned(t, env.stdout.String(), http.StatusOK, "getLatestSandboxSnapshot")
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["type"] != api.TypeSnapshotLatest {
			t.Fatalf("lines = %v", lines)
		}
		if value, present := lines[0]["snapshot"]; !present || value != nil {
			t.Fatalf("snapshot = %v (present=%v), want an explicit null", value, present)
		}
	})

	t.Run("404 exits 1", func(t *testing.T) {
		env := cliTestNew(t, func(w http.ResponseWriter, _ *http.Request) {
			cliTestWriteJSON(t, w, http.StatusNotFound, cliTestErrorEnvelope(http.StatusNotFound, "not_found", "not found"))
		}, nil)
		if status := env.run(t, "snapshot", "latest", cliTestSandboxID); status != 1 {
			t.Fatalf("exit status = %d, want 1", status)
		}
		lines := cliTestJSONLines(t, env.stdout)
		if len(lines) != 1 || lines[0]["code"] != "not_found" || lines[0]["status"] != float64(http.StatusNotFound) {
			t.Fatalf("error event = %v", lines)
		}
	})

	t.Run("bare latest is a usage error", func(t *testing.T) {
		var hits atomic.Int64
		env := cliTestNew(t, func(http.ResponseWriter, *http.Request) { hits.Add(1) }, nil)
		if status := env.run(t, "snapshot", "latest"); status != 2 {
			t.Fatalf("exit status = %d, want 2", status)
		}
		if hits.Load() != 0 {
			t.Fatalf("server received %d requests, want 0", hits.Load())
		}
	})
}

// TestSnapshotSaveStillDispatches proves the new `latest` subcommand did not
// steal the `snapshot ID NAME` form.
func TestSnapshotSaveStillDispatches(t *testing.T) {
	var saves atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == api.PathSavedSnapshots {
			saves.Add(1)
			var request api.SavedSnapshotSaveRequest
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read save request: %v", err)
			}
			if err := json.Unmarshal(raw, &request); err != nil {
				t.Errorf("decode save request: %v", err)
			}
			if request.SandboxID != cliTestSandboxID || request.Name != cliTestSnapshotName {
				t.Errorf("save request = %+v", request)
			}
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSavedSnapshotSavingResponse(cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotSaving)))
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == api.PathSavedSnapshots+"/"+cliTestSnapshotName {
			ready := cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotReady)
			ready.SnapshotID = cliTestSnapshotID
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSavedSnapshotInfoResponse(ready))
			return
		}
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
	}, nil)
	if status := env.run(t, "snapshot", cliTestSandboxID, cliTestSnapshotName); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	if saves.Load() != 1 {
		t.Fatalf("saves = %d, want 1", saves.Load())
	}
}
