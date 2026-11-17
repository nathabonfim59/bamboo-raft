package cli

import (
	"encoding/json/v2"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

func TestSnapshotSavePollsToReady(t *testing.T) {
	var polls atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/saved-snapshots":
			var body api.SavedSnapshotSaveRequest
			if err := json.UnmarshalRead(r.Body, &body); err != nil {
				t.Errorf("decode save body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if body.SandboxID != cliTestSandboxID || body.Name != cliTestSnapshotName {
				t.Errorf("body = %+v", body)
			}
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSavedSnapshotSavingResponse(cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotSaving)))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/saved-snapshots/"+cliTestSnapshotName:
			status := api.SavedSnapshotSaving
			if polls.Add(1) >= 2 {
				status = api.SavedSnapshotReady
			}
			snapshot := cliTestSnapshot(cliTestSnapshotName, status)
			snapshot.SnapshotID = cliTestSnapshotID
			cliTestWriteJSON(t, w, http.StatusOK, api.NewSavedSnapshotInfoResponse(snapshot))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}, nil)
	if status := env.run(t, "snapshot", cliTestSandboxID, cliTestSnapshotName); status != 0 {
		t.Fatalf("exit status = %d, stderr = %s", status, env.stderr)
	}
	lines := cliTestJSONLines(t, env.stdout)
	if len(lines) != 2 || lines[0]["event"] != "saving" || lines[1]["event"] != "ready" {
		t.Fatalf("lines = %v, want saving/ready", lines)
	}
	if lines[1]["snapshotId"] != cliTestSnapshotID {
		t.Fatalf("snapshotId = %v", lines[1]["snapshotId"])
	}
}

func TestSnapshotSaveFailedIsError(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSavedSnapshotSavingResponse(cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotSaving)))
			return
		}
		failed := cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotFailed)
		failed.Error = "disk full"
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSavedSnapshotInfoResponse(failed))
	}, nil)
	if status := env.run(t, "snapshot", cliTestSandboxID, cliTestSnapshotName); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "snapshot_failed" {
		t.Fatalf("last event = %v, want snapshot_failed", last)
	}
}

func TestSnapshotSaveRejectsWrongName(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSavedSnapshotSavingResponse(cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotSaving)))
			return
		}
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSavedSnapshotInfoResponse(cliTestSnapshot("other", api.SavedSnapshotReady)))
	}, nil)
	if status := env.run(t, "snapshot", cliTestSandboxID, cliTestSnapshotName); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "invalid_response" {
		t.Fatalf("last event = %v, want invalid_response", last)
	}
}

func TestSnapshotSaveTimeout(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSavedSnapshotSavingResponse(cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotSaving)))
			return
		}
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSavedSnapshotInfoResponse(cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotSaving)))
	}, func(opts *Options) {
		opts.WaitTimeout = 20 * time.Millisecond
		opts.PollInterval = 2 * time.Millisecond
	})
	if status := env.run(t, "snapshot", cliTestSandboxID, cliTestSnapshotName); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "timeout" {
		t.Fatalf("last event = %v, want timeout", last)
	}
}

func TestSnapshotRemoveEscapesName(t *testing.T) {
	const name = "my snap/é"
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/api/v1/saved-snapshots/"+name {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		// An unsafe name is a negative fixture: the server refuses it with
		// 400, so it is deliberately outside the success-pin validator.
		cliTestWriteJSON(t, w, http.StatusBadRequest, cliTestErrorEnvelope(http.StatusBadRequest, "invalid_name", "invalid snapshot name"))
	}, nil)
	if status := env.run(t, "snapshot", "rm", name); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "invalid_name" {
		t.Fatalf("last event = %v, want invalid_name", last)
	}
}

// TestSnapshotSaveRejectsReplacementGeneration proves a later save under the
// same name is not reported as the save this command started.
func TestSnapshotSaveRejectsReplacementGeneration(t *testing.T) {
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			accepted := cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotSaving)
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSavedSnapshotSavingResponse(accepted))
			return
		}
		replacement := cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotReady)
		replacement.CreatedAt = cliTestTime.Add(time.Minute)
		replacement.SnapshotID = cliTestSnapshotID2
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSavedSnapshotInfoResponse(replacement))
	}, nil)
	if status := env.run(t, "snapshot", cliTestSandboxID, cliTestSnapshotName); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "invalid_response" {
		t.Fatalf("last event = %v, want invalid_response", last)
	}
	for _, line := range lines {
		if line["event"] == "ready" {
			t.Fatal("reported a replacement save as ready")
		}
	}
}

func TestSnapshotSaveRejectsReplacementArtifact(t *testing.T) {
	accepted := cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotSaving)
	accepted.SnapshotID = cliTestSnapshotID
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSavedSnapshotSavingResponse(accepted))
			return
		}
		replacement := cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotReady)
		replacement.SnapshotID = cliTestSnapshotID2
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSavedSnapshotInfoResponse(replacement))
	}, nil)
	if status := env.run(t, "snapshot", cliTestSandboxID, cliTestSnapshotName); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "invalid_response" {
		t.Fatalf("last event = %v, want invalid_response", last)
	}
}

func TestSnapshotSaveRejectsMissingAcceptedGeneration(t *testing.T) {
	var polls atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			accepted := cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotSaving)
			accepted.CreatedAt = time.Time{}
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSavedSnapshotSavingResponse(accepted))
			return
		}
		polls.Add(1)
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSavedSnapshotInfoResponse(cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotReady)))
	}, nil)
	if status := env.run(t, "snapshot", cliTestSandboxID, cliTestSnapshotName); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if polls.Load() != 0 {
		t.Fatalf("polls = %d, want 0 after an unusable accepted generation", polls.Load())
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "invalid_response" {
		t.Fatalf("last event = %v, want invalid_response", last)
	}
}

func TestSnapshotSaveRejectsWrongAcceptedSource(t *testing.T) {
	var polls atomic.Int64
	env := cliTestNew(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			accepted := cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotSaving)
			accepted.SourceSandboxID = cliTestSandboxID2
			cliTestWriteJSON(t, w, http.StatusAccepted, api.NewSavedSnapshotSavingResponse(accepted))
			return
		}
		polls.Add(1)
		cliTestWriteJSON(t, w, http.StatusOK, api.NewSavedSnapshotInfoResponse(cliTestSnapshot(cliTestSnapshotName, api.SavedSnapshotReady)))
	}, nil)
	if status := env.run(t, "snapshot", cliTestSandboxID, cliTestSnapshotName); status != 1 {
		t.Fatalf("exit status = %d, want 1", status)
	}
	if polls.Load() != 0 {
		t.Fatalf("polls = %d, want 0 for a foreign accepted source", polls.Load())
	}
	lines := cliTestJSONLines(t, env.stdout)
	last := lines[len(lines)-1]
	if last["event"] != "error" || last["code"] != "invalid_response" {
		t.Fatalf("last event = %v, want invalid_response", last)
	}
}
