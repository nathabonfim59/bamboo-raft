package api_test

// Actual-byte validation of the saved-snapshot handlers and the create-from
// answer against the v1 API document. Every
// emitted status/code pair the handlers can produce is recorded here.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

type snapshotCompatStore struct {
	accept func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error)
	byName func(context.Context, uuid.UUID, string) (db.SavedSnapshot, error)
	all    func(context.Context, db.SnapshotListQuery) ([]db.SavedSnapshot, error)
	remove func(context.Context, db.AcceptSnapshotDeleteParams) (db.SavedSnapshot, error)
}

func (s *snapshotCompatStore) AcceptSnapshotSave(ctx context.Context, p db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
	if s.accept == nil {
		return db.SavedSnapshot{}, errors.New("fake: unexpected save")
	}
	return s.accept(ctx, p)
}

func (s *snapshotCompatStore) SavedSnapshotByName(ctx context.Context, owner uuid.UUID, name string) (db.SavedSnapshot, error) {
	if s.byName == nil {
		return db.SavedSnapshot{}, db.ErrNotFound
	}
	return s.byName(ctx, owner, name)
}

func (s *snapshotCompatStore) AllSavedSnapshots(ctx context.Context, q db.SnapshotListQuery) ([]db.SavedSnapshot, error) {
	if s.all == nil {
		return nil, nil
	}
	return s.all(ctx, q)
}

func (s *snapshotCompatStore) AcceptSnapshotDelete(ctx context.Context, p db.AcceptSnapshotDeleteParams) (db.SavedSnapshot, error) {
	if s.remove == nil {
		return db.SavedSnapshot{}, db.ErrNotFound
	}
	return s.remove(ctx, p)
}

func snapshotCompatRow(owner uuid.UUID, status db.SnapshotStatus) db.SavedSnapshot {
	return db.SavedSnapshot{
		OwnerID: owner, Name: "web", SandboxID: "bx_23456789", Status: status,
		ArtifactID: "snap-" + strings.Repeat("a", 32), MachineType: "default",
		CreatedAt: coreCompatTime,
	}
}

func snapshotCompatMux(t *testing.T, store api.SavedSnapshotStore) *http.ServeMux {
	t.Helper()
	handlers, err := api.NewSavedSnapshotHandlers(api.SavedSnapshotOptions{Store: store})
	if err != nil {
		t.Fatalf("NewSavedSnapshotHandlers: %v", err)
	}
	mux := http.NewServeMux()
	api.RegisterSavedSnapshots(mux, handlers)
	return mux
}

func snapshotCompatServe(mux *http.ServeMux, request *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return recorder
}

func TestSavedSnapshotCompatBytes(t *testing.T) {
	validators := coreCompatValidators(t)
	owner := uuid.MustParse(coreCompatOwner)
	ready := snapshotCompatRow(owner, db.SnapshotReady)
	failed := ready
	failed.Status = db.SnapshotFailed
	failed.ErrorReason = "save interrupted"

	saveCases := []struct {
		name   string
		store  *snapshotCompatStore
		body   string
		status int
	}{
		{
			name: "202", store: &snapshotCompatStore{accept: func(_ context.Context, p db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
				row := snapshotCompatRow(owner, db.SnapshotSaving)
				row.Name = p.Name
				row.SandboxID = p.SandboxID
				return row, nil
			}},
			body: `{"sandboxId":"bx_23456789","name":"web"}`, status: http.StatusAccepted,
		},
		{name: "400 invalid json", store: &snapshotCompatStore{}, body: `{"name":"web"}`, status: http.StatusBadRequest},
		{name: "400 invalid name", store: &snapshotCompatStore{}, body: `{"sandboxId":"bx_23456789","name":"Bad_Name"}`, status: http.StatusBadRequest},
		{
			name: "404", store: &snapshotCompatStore{accept: func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
				return db.SavedSnapshot{}, db.ErrNotFound
			}},
			body: `{"sandboxId":"bx_23456789","name":"web"}`, status: http.StatusNotFound,
		},
		{
			name: "409", store: &snapshotCompatStore{accept: func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
				return db.SavedSnapshot{}, db.ErrSnapshotSaveInProgress
			}},
			body: `{"sandboxId":"bx_23456789","name":"web"}`, status: http.StatusConflict,
		},
		{
			name: "409 source not ready", store: &snapshotCompatStore{accept: func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
				return db.SavedSnapshot{}, db.ErrSnapshotSourceNotReady
			}},
			body: `{"sandboxId":"bx_23456789","name":"web"}`, status: http.StatusConflict,
		},
		{
			name: "409 snapshots disabled", store: &snapshotCompatStore{accept: func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
				return db.SavedSnapshot{}, db.ErrSnapshotsDisabled
			}},
			body: `{"sandboxId":"bx_23456789","name":"web"}`, status: http.StatusConflict,
		},
		{
			name: "500", store: &snapshotCompatStore{accept: func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
				return db.SavedSnapshot{}, errors.New("driver")
			}},
			body: `{"sandboxId":"bx_23456789","name":"web"}`, status: http.StatusInternalServerError,
		},
	}
	for _, tc := range saveCases {
		t.Run("save "+tc.name, func(t *testing.T) {
			mux := snapshotCompatMux(t, tc.store)
			request := coreCompatRequest(http.MethodPost, api.PathSavedSnapshots, tc.body, coreCompatOwner)
			request.Header.Set("Content-Type", "application/json")
			coreCompatAssert(t, validators, "saveSavedSnapshot", tc.status, snapshotCompatServe(mux, request))
		})
	}

	getCases := []struct {
		name   string
		store  *snapshotCompatStore
		status int
	}{
		{name: "200 ready", store: &snapshotCompatStore{byName: func(context.Context, uuid.UUID, string) (db.SavedSnapshot, error) {
			return ready, nil
		}}, status: http.StatusOK},
		{name: "200 failed", store: &snapshotCompatStore{byName: func(context.Context, uuid.UUID, string) (db.SavedSnapshot, error) {
			return failed, nil
		}}, status: http.StatusOK},
		{name: "404", store: &snapshotCompatStore{}, status: http.StatusNotFound},
		{name: "500", store: &snapshotCompatStore{byName: func(context.Context, uuid.UUID, string) (db.SavedSnapshot, error) {
			return db.SavedSnapshot{}, errors.New("driver")
		}}, status: http.StatusInternalServerError},
	}
	for _, tc := range getCases {
		t.Run("get "+tc.name, func(t *testing.T) {
			mux := snapshotCompatMux(t, tc.store)
			request := coreCompatRequest(http.MethodGet, api.PathSavedSnapshots+"/web", "", coreCompatOwner)
			coreCompatAssert(t, validators, "getSavedSnapshot", tc.status, snapshotCompatServe(mux, request))
		})
	}

	listCases := []struct {
		name   string
		store  *snapshotCompatStore
		target string
		status int
	}{
		{name: "200 empty", store: &snapshotCompatStore{}, target: api.PathSavedSnapshots, status: http.StatusOK},
		{name: "200 rows", store: &snapshotCompatStore{all: func(context.Context, db.SnapshotListQuery) ([]db.SavedSnapshot, error) {
			return []db.SavedSnapshot{ready, failed}, nil
		}}, target: api.PathSavedSnapshots + "?namePrefix=w", status: http.StatusOK},
		{name: "400 query", store: &snapshotCompatStore{}, target: api.PathSavedSnapshots + "?limit=1", status: http.StatusBadRequest},
		{name: "500", store: &snapshotCompatStore{all: func(context.Context, db.SnapshotListQuery) ([]db.SavedSnapshot, error) {
			return nil, errors.New("driver")
		}}, target: api.PathSavedSnapshots, status: http.StatusInternalServerError},
	}
	for _, tc := range listCases {
		t.Run("list "+tc.name, func(t *testing.T) {
			mux := snapshotCompatMux(t, tc.store)
			request := coreCompatRequest(http.MethodGet, tc.target, "", coreCompatOwner)
			coreCompatAssert(t, validators, "listSavedSnapshots", tc.status, snapshotCompatServe(mux, request))
		})
	}

	deleteCases := []struct {
		name   string
		store  *snapshotCompatStore
		status int
	}{
		{name: "200", store: &snapshotCompatStore{remove: func(context.Context, db.AcceptSnapshotDeleteParams) (db.SavedSnapshot, error) {
			return ready, nil
		}}, status: http.StatusOK},
		{name: "404", store: &snapshotCompatStore{}, status: http.StatusNotFound},
		{name: "409", store: &snapshotCompatStore{remove: func(context.Context, db.AcceptSnapshotDeleteParams) (db.SavedSnapshot, error) {
			return db.SavedSnapshot{}, db.ErrSnapshotSaveInProgress
		}}, status: http.StatusConflict},
		{name: "500", store: &snapshotCompatStore{remove: func(context.Context, db.AcceptSnapshotDeleteParams) (db.SavedSnapshot, error) {
			return db.SavedSnapshot{}, errors.New("driver")
		}}, status: http.StatusInternalServerError},
	}
	for _, tc := range deleteCases {
		t.Run("delete "+tc.name, func(t *testing.T) {
			mux := snapshotCompatMux(t, tc.store)
			request := coreCompatRequest(http.MethodDelete, api.PathSavedSnapshots+"/web", "", coreCompatOwner)
			coreCompatAssert(t, validators, "deleteSavedSnapshot", tc.status, snapshotCompatServe(mux, request))
		})
	}

	// All four operations require an authenticated account; the actual
	// bytes must validate against the v1 API document without an identity.
	unauthorized := []struct {
		operation, method, target, body string
	}{
		{"listSavedSnapshots", http.MethodGet, api.PathSavedSnapshots, ""},
		{"saveSavedSnapshot", http.MethodPost, api.PathSavedSnapshots, `{"sandboxId":"bx_23456789","name":"web"}`},
		{"getSavedSnapshot", http.MethodGet, api.PathSavedSnapshots + "/web", ""},
		{"deleteSavedSnapshot", http.MethodDelete, api.PathSavedSnapshots + "/web", ""},
	}
	for _, tc := range unauthorized {
		t.Run(tc.operation+" 401", func(t *testing.T) {
			mux := snapshotCompatMux(t, &snapshotCompatStore{})
			request := coreCompatRequest(tc.method, tc.target, tc.body, "")
			request.Header.Set("Content-Type", "application/json")
			coreCompatAssert(t, validators, tc.operation, http.StatusUnauthorized, snapshotCompatServe(mux, request))
		})
	}
}

func TestCreateFromCompatBytes(t *testing.T) {
	validators := coreCompatValidators(t)

	t.Run("202", func(t *testing.T) {
		sandbox := coreCompatSandbox()
		handlers := coreCompatHandlers(t, &coreCompatStore{},
			coreCompatAdmission{result: api.AdmissionResult{Sandbox: sandbox}})
		request := coreCompatRequest(http.MethodPost, api.PathSandboxes, `{"from":"base"}`, coreCompatOwner)
		request.Header.Set("Content-Type", "application/json")
		coreCompatAssert(t, validators, "create", http.StatusAccepted, coreCompatServe(handlers.CreateSandbox, request))
	})

	t.Run("404 snapshot_not_found", func(t *testing.T) {
		handlers := coreCompatHandlers(t, &coreCompatStore{}, coreCompatAdmission{
			err: apierr.New(http.StatusNotFound, apierr.CodeSnapshotNotFound, "saved snapshot not found"),
		})
		request := coreCompatRequest(http.MethodPost, api.PathSandboxes, `{"from":"gone"}`, coreCompatOwner)
		request.Header.Set("Content-Type", "application/json")
		coreCompatAssert(t, validators, "create", http.StatusNotFound, coreCompatServe(handlers.CreateSandbox, request))
	})
}
