package api_test

// Expired stored data on the wire: the sandbox body reports dataExpiredAt, and
// saving a snapshot from an expired sandbox answers 409 snapshot_data_expired.
// Resume and fork have their own handler and compat tests; this file records
// the two surfaces the retention path adds, validated against
// api/bbraft-v1.yaml.

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// TestSandboxBodyReportsDataExpiredAt checks that both the get and list
// sandbox objects carry the expiry timestamp the durable row holds, and that
// the body still validates against the pinned document.
func TestSandboxBodyReportsDataExpiredAt(t *testing.T) {
	validators := coreCompatValidators(t)
	expiredAt := coreCompatTime.Add(-72 * time.Hour)
	stoppedAt := coreCompatTime.Add(-96 * time.Hour)
	sandbox := coreCompatSandbox()
	sandbox.State = db.StateArchived
	sandbox.ExpiresAt = nil
	sandbox.StoppedAt = &stoppedAt
	sandbox.DataExpiredAt = &expiredAt
	store := &coreCompatStore{
		sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
			return sandbox, nil
		},
		list: func(context.Context, db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error) {
			return []db.Sandbox{sandbox}, nil, nil
		},
	}
	handlers := coreCompatHandlers(t, store, coreCompatAdmission{result: api.AdmissionResult{Sandbox: sandbox}})

	request := coreCompatRequest(http.MethodGet, api.PathSandbox, "", coreCompatOwner)
	request.SetPathValue(api.ParamSandboxID, sandbox.ID)
	recorder := coreCompatServe(handlers.GetSandbox, request)
	coreCompatAssert(t, validators, "get", http.StatusOK, recorder)
	want := `"dataExpiredAt":"` + expiredAt.Format(time.RFC3339) + `"`
	if body := recorder.Body.String(); !strings.Contains(body, want) {
		t.Fatalf("get body does not report %s: %s", want, body)
	}

	recorder = coreCompatServe(handlers.ListSandboxes, coreCompatRequest(http.MethodGet, api.PathSandboxes, "", coreCompatOwner))
	coreCompatAssert(t, validators, "sandboxes", http.StatusOK, recorder)
	if body := recorder.Body.String(); !strings.Contains(body, want) {
		t.Fatalf("list body does not report %s: %s", want, body)
	}

	// A sandbox whose data has not expired omits the field entirely.
	sandbox.DataExpiredAt = nil
	request = coreCompatRequest(http.MethodGet, api.PathSandbox, "", coreCompatOwner)
	request.SetPathValue(api.ParamSandboxID, sandbox.ID)
	recorder = coreCompatServe(handlers.GetSandbox, request)
	coreCompatAssert(t, validators, "get", http.StatusOK, recorder)
	if body := recorder.Body.String(); strings.Contains(body, "dataExpiredAt") {
		t.Fatalf("live sandbox body reports dataExpiredAt: %s", body)
	}
}

// TestResumeAndForkExpiredSourceCompat answers 409 snapshot_data_expired
// through the real resume and fork handlers for a source whose stored data
// expired, validated against the pinned document.
func TestResumeAndForkExpiredSourceCompat(t *testing.T) {
	validators := coreCompatValidators(t)
	expiredAt := coreCompatTime
	archived := lifecycleCompatSandbox()
	archived.DataExpiredAt = &expiredAt

	t.Run("resume", func(t *testing.T) {
		store := &lifecycleCompatStore{sandbox: func(context.Context, uuid.UUID, string) (db.Sandbox, error) {
			return archived, nil
		}}
		handlers := lifecycleCompatHandlers(t, store, lifecycleCompatResume{err: db.ErrSnapshotDataExpired})
		request := lifecycleCompatPath(coreCompatRequest(http.MethodPost, api.PathSandboxResume, `{}`, coreCompatOwner),
			archived.ID, "")
		recorder := coreCompatServe(handlers.ResumeSandbox, request)
		coreCompatAssert(t, validators, "resume", http.StatusConflict, recorder)
		if body := recorder.Body.String(); !strings.Contains(body, `"code":"`+apierr.CodeSnapshotDataExpired+`"`) {
			t.Fatalf("resume of an expired sandbox answered the wrong code: %s", body)
		}
	})

	t.Run("fork", func(t *testing.T) {
		handler, err := api.NewForkHandler(api.ForkOptions{
			Store:       expiredForkStore{source: archived},
			Admission:   unexpectedForkAdmission{t: t},
			ObjectStore: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		request := coreCompatRequest(http.MethodPost, api.PathSandboxFork, "", coreCompatOwner)
		request.SetPathValue(api.ParamSandboxID, archived.ID)
		recorder := coreCompatServe(handler, request)
		coreCompatAssert(t, validators, "fork", http.StatusConflict, recorder)
		if body := recorder.Body.String(); !strings.Contains(body, `"code":"`+apierr.CodeSnapshotDataExpired+`"`) {
			t.Fatalf("fork of an expired sandbox answered the wrong code: %s", body)
		}
	})
}

// expiredForkStore serves one expired source and no generation.
type expiredForkStore struct {
	source db.Sandbox
}

func (s expiredForkStore) Sandbox(context.Context, uuid.UUID, string) (db.Sandbox, error) {
	return s.source, nil
}

func (expiredForkStore) LatestGeneration(context.Context, uuid.UUID, string) (db.Generation, bool, error) {
	return db.Generation{}, false, nil
}

// unexpectedForkAdmission fails the test if an expired fork ever reaches
// admission.
type unexpectedForkAdmission struct {
	t *testing.T
}

func (a unexpectedForkAdmission) Fork(context.Context, api.ForkRequest) (api.ForkResult, error) {
	a.t.Error("fork admission reached for an expired source")
	return api.ForkResult{}, errors.New("unexpected fork admission")
}

// TestSaveSnapshotExpiredSourceCompat checks that a save refused because the
// source's stored data expired answers the pinned 409 snapshot_data_expired.
func TestSaveSnapshotExpiredSourceCompat(t *testing.T) {
	validators := coreCompatValidators(t)
	mux := snapshotCompatMux(t, &snapshotCompatStore{accept: func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
		return db.SavedSnapshot{}, db.ErrSnapshotDataExpired
	}})
	request := coreCompatRequest(http.MethodPost, api.PathSavedSnapshots,
		`{"sandboxId":"bx_23456789","name":"web"}`, coreCompatOwner)
	request.Header.Set("Content-Type", "application/json")
	recorder := snapshotCompatServe(mux, request)
	coreCompatAssert(t, validators, "saveSavedSnapshot", http.StatusConflict, recorder)
	if body := recorder.Body.String(); !strings.Contains(body, `"code":"`+apierr.CodeSnapshotDataExpired+`"`) {
		t.Fatalf("save from an expired source answered the wrong code: %s", body)
	}
}
