package api

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

type savedSnapshotFakeStore struct {
	accept func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error)
	byName func(context.Context, uuid.UUID, string) (db.SavedSnapshot, error)
	all    func(context.Context, db.SnapshotListQuery) ([]db.SavedSnapshot, error)
	remove func(context.Context, db.AcceptSnapshotDeleteParams) (db.SavedSnapshot, error)
}

func (f *savedSnapshotFakeStore) AcceptSnapshotSave(ctx context.Context, p db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
	if p.Owner == uuid.Nil() || p.SandboxID == "" || !db.ValidSnapshotName(p.Name) {
		return db.SavedSnapshot{}, errors.New("fake: invalid save params")
	}
	if f.accept == nil {
		return db.SavedSnapshot{}, nil
	}
	return f.accept(ctx, p)
}

func (f *savedSnapshotFakeStore) SavedSnapshotByName(ctx context.Context, owner uuid.UUID, name string) (db.SavedSnapshot, error) {
	if owner == uuid.Nil() || name == "" {
		return db.SavedSnapshot{}, errors.New("fake: invalid name lookup")
	}
	if f.byName == nil {
		return db.SavedSnapshot{}, db.ErrNotFound
	}
	return f.byName(ctx, owner, name)
}

func (f *savedSnapshotFakeStore) AllSavedSnapshots(ctx context.Context, q db.SnapshotListQuery) ([]db.SavedSnapshot, error) {
	if q.Owner == uuid.Nil() {
		return nil, errors.New("fake: invalid list query")
	}
	if f.all == nil {
		return nil, nil
	}
	return f.all(ctx, q)
}

func (f *savedSnapshotFakeStore) AcceptSnapshotDelete(ctx context.Context, p db.AcceptSnapshotDeleteParams) (db.SavedSnapshot, error) {
	if p.Owner == uuid.Nil() || p.Name == "" {
		return db.SavedSnapshot{}, errors.New("fake: invalid delete params")
	}
	if f.remove == nil {
		return db.SavedSnapshot{}, nil
	}
	return f.remove(ctx, p)
}

func savedSnapshotMux(t *testing.T, store SavedSnapshotStore) *http.ServeMux {
	t.Helper()
	handlers, err := NewSavedSnapshotHandlers(SavedSnapshotOptions{Store: store})
	if err != nil {
		t.Fatalf("NewSavedSnapshotHandlers: %v", err)
	}
	mux := http.NewServeMux()
	RegisterSavedSnapshots(mux, handlers)
	return mux
}

func savedSnapshotRequest(owner uuid.UUID, method, target, body string) *http.Request {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	r := httptest.NewRequest(method, target, reader)
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	return r.WithContext(auth.WithIdentity(r.Context(), auth.Identity{Owner: owner.String()}))
}

func snapshotBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body is not a JSON object (status %d, %d bytes)", rec.Code, rec.Body.Len())
	}
	return body
}

func TestSavedSnapshotSaveHandlerMatrix(t *testing.T) {
	owner := uuid.NewV7()
	source := "bx_23456789"
	created := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		body       string
		accept     func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error)
		wantStatus int
		wantCode   string
	}{
		{
			name: "accepted",
			body: `{"sandboxId":"bx_23456789","name":"web-stack"}`,
			accept: func(_ context.Context, p db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
				if p.Owner != owner || p.SandboxID != source || p.Name != "web-stack" {
					return db.SavedSnapshot{}, errors.New("wrong params")
				}
				return db.SavedSnapshot{
					OwnerID: owner, Name: p.Name, SandboxID: p.SandboxID,
					Status: db.SnapshotSaving, ArtifactID: "snap-" + strings.Repeat("a", 32),
					MachineType: "default", CreatedAt: created,
				}, nil
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "invalid name",
			body:       `{"sandboxId":"bx_23456789","name":"Bad_Name"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   apierr.CodeInvalidName,
		},
		{
			name:       "reserved name",
			body:       `{"sandboxId":"bx_23456789","name":"latest"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   apierr.CodeInvalidName,
		},
		{
			name:       "unknown member",
			body:       `{"sandboxId":"bx_23456789","name":"ok","extra":true}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   apierr.CodeInvalidJSON,
		},
		{
			name:       "missing sandbox id",
			body:       `{"name":"ok"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   apierr.CodeInvalidJSON,
		},
		{
			name: "source missing",
			body: `{"sandboxId":"bx_23456789","name":"ok"}`,
			accept: func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
				return db.SavedSnapshot{}, db.ErrNotFound
			},
			wantStatus: http.StatusNotFound,
			wantCode:   apierr.CodeNotFound,
		},
		{
			name: "source not ready",
			body: `{"sandboxId":"bx_23456789","name":"ok"}`,
			accept: func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
				return db.SavedSnapshot{}, db.ErrSnapshotSourceNotReady
			},
			wantStatus: http.StatusConflict,
			wantCode:   apierr.CodeSandboxNotReady,
		},
		{
			name: "invalid artifact",
			body: `{"sandboxId":"bx_23456789","name":"ok"}`,
			accept: func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
				return db.SavedSnapshot{}, db.ErrInvalidSnapshotArtifact
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   apierr.CodeInvalidName,
		},
		{
			name: "save in progress",
			body: `{"sandboxId":"bx_23456789","name":"ok"}`,
			accept: func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
				return db.SavedSnapshot{}, db.ErrSnapshotSaveInProgress
			},
			wantStatus: http.StatusConflict,
			wantCode:   apierr.CodeSaveInProgress,
		},
		{
			name: "internal",
			body: `{"sandboxId":"bx_23456789","name":"ok"}`,
			accept: func(context.Context, db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
				return db.SavedSnapshot{}, errors.New("token=secret")
			},
			wantStatus: http.StatusInternalServerError,
			wantCode:   apierr.CodeInternal,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &savedSnapshotFakeStore{accept: tc.accept}
			rec := httptest.NewRecorder()
			savedSnapshotMux(t, store).ServeHTTP(rec,
				savedSnapshotRequest(owner, http.MethodPost, PathSavedSnapshots, tc.body))
			// The internal cause must never reach the wire; sweep before
			// looking at any body field or printing diagnostics.
			if strings.Contains(rec.Body.String(), "secret") {
				t.Fatal("response leaked an internal cause")
			}
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			body := snapshotBody(t, rec)
			if tc.wantCode != "" && body["code"] != tc.wantCode {
				t.Fatalf("code = %v, want %s", body["code"], tc.wantCode)
			}
			if tc.wantStatus != http.StatusAccepted {
				if body["ok"] != false || body["type"] != "sandbox.error" {
					t.Fatalf("error envelope ok=%v type=%v", body["ok"], body["type"])
				}
				return
			}
			if body["ok"] != true || body["type"] != TypeSavedSnapshotSaving || body["status"] != "saving" {
				t.Fatalf("saving envelope ok=%v type=%v status=%v", body["ok"], body["type"], body["status"])
			}
			snapshot, ok := body["snapshot"].(map[string]any)
			if !ok {
				t.Fatal("saving response has no snapshot object")
			}
			if snapshot["name"] != "web-stack" || snapshot["status"] != "saving" {
				t.Fatalf("snapshot name=%v status=%v", snapshot["name"], snapshot["status"])
			}
		})
	}
}

func TestSavedSnapshotQueryParity(t *testing.T) {
	owner := uuid.NewV7()
	created := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	ready := db.SavedSnapshot{
		OwnerID: owner, Name: "web", SandboxID: "bx_23456789", Status: db.SnapshotReady,
		ArtifactID: "snap-" + strings.Repeat("d", 32), MachineType: "default",
		CreatedAt: created,
	}
	// POST, GET and DELETE ignore query parameters; only the list has a
	// documented finite query and refuses others with invalid_query.
	store := &savedSnapshotFakeStore{
		accept: func(_ context.Context, p db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error) {
			return db.SavedSnapshot{
				OwnerID: owner, Name: p.Name, SandboxID: p.SandboxID, Status: db.SnapshotSaving,
				ArtifactID: "snap-" + strings.Repeat("e", 32), MachineType: "default",
				CreatedAt: created,
			}, nil
		},
		byName: func(context.Context, uuid.UUID, string) (db.SavedSnapshot, error) {
			return ready, nil
		},
		remove: func(_ context.Context, p db.AcceptSnapshotDeleteParams) (db.SavedSnapshot, error) {
			return ready, nil
		},
	}
	mux := savedSnapshotMux(t, store)
	tests := []struct {
		method, target, body string
		status               int
	}{
		{http.MethodPost, PathSavedSnapshots + "?limit=1", `{"sandboxId":"bx_23456789","name":"ok"}`, http.StatusAccepted},
		{http.MethodGet, PathSavedSnapshots + "/web?limit=1", "", http.StatusOK},
		{http.MethodDelete, PathSavedSnapshots + "/web?limit=1", "", http.StatusOK},
	}
	for _, tc := range tests {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, savedSnapshotRequest(owner, tc.method, tc.target, tc.body))
		if rec.Code != tc.status {
			t.Fatalf("%s %s status = %d", tc.method, tc.target, rec.Code)
		}
	}
}

func TestSavedSnapshotDTORejectsNonCanonicalRows(t *testing.T) {
	owner := uuid.NewV7()
	created := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	valid := db.SavedSnapshot{
		OwnerID: owner, Name: "web", SandboxID: "bx_23456789", Status: db.SnapshotReady,
		ArtifactID: "snap-" + strings.Repeat("f", 32), MachineType: "default",
		CreatedAt: created,
	}
	tests := []struct {
		name string
		row  db.SavedSnapshot
	}{
		{name: "zero created at", row: func() db.SavedSnapshot { r := valid; r.CreatedAt = time.Time{}; return r }()},
		{name: "foreign owner", row: func() db.SavedSnapshot { r := valid; r.OwnerID = uuid.NewV7(); return r }()},
		{name: "bad name", row: func() db.SavedSnapshot { r := valid; r.Name = "Bad_Name"; return r }()},
		{name: "bad status", row: func() db.SavedSnapshot { r := valid; r.Status = "weird"; return r }()},
		{name: "bad artifact", row: func() db.SavedSnapshot { r := valid; r.ArtifactID = "not-an-artifact"; return r }()},
		{name: "unknown type", row: func() db.SavedSnapshot { r := valid; r.MachineType = "retired"; return r }()},
		{name: "negative size", row: func() db.SavedSnapshot { r := valid; r.SizeBytes = new(int64(-1)); return r }()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &savedSnapshotFakeStore{byName: func(context.Context, uuid.UUID, string) (db.SavedSnapshot, error) {
				return tc.row, nil
			}}
			rec := httptest.NewRecorder()
			savedSnapshotMux(t, store).ServeHTTP(rec,
				savedSnapshotRequest(owner, http.MethodGet, PathSavedSnapshots+"/web", ""))
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d", rec.Code)
			}
			if code := snapshotBody(t, rec)["code"]; code != apierr.CodeInternal {
				t.Fatalf("code = %v", code)
			}
		})
	}
}

func TestSavedSnapshotGetHandler(t *testing.T) {
	owner := uuid.NewV7()
	created := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	ready := db.SavedSnapshot{
		OwnerID: owner, Name: "web", SandboxID: "bx_23456789", Status: db.SnapshotReady,
		ArtifactID: "snap-" + strings.Repeat("b", 32), MachineType: "default",
		SizeBytes: new(int64(2048)), CreatedAt: created,
	}
	failed := ready
	failed.Status = db.SnapshotFailed
	failed.ErrorReason = "save interrupted"

	tests := []struct {
		name       string
		row        db.SavedSnapshot
		err        error
		wantStatus int
		check      func(*testing.T, map[string]any)
	}{
		{
			name: "ready", row: ready, wantStatus: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				if body["type"] != TypeSavedSnapshotInfo {
					t.Fatalf("type: %v", body["type"])
				}
				snap, _ := body["snapshot"].(map[string]any)
				if snap["snapshotId"] != ready.ArtifactID || snap["sizeBytes"] != float64(2048) || snap["error"] != nil {
					t.Fatalf("ready snapshot: %v", snap)
				}
			},
		},
		{
			name: "failed", row: failed, wantStatus: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				snap, _ := body["snapshot"].(map[string]any)
				if snap["status"] != "failed" || snap["error"] != "save interrupted" || snap["snapshotId"] != nil {
					t.Fatalf("failed snapshot: %v", snap)
				}
			},
		},
		{
			name: "missing", err: db.ErrNotFound, wantStatus: http.StatusNotFound,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				if body["code"] != apierr.CodeNotFound {
					t.Fatalf("code: %v", body["code"])
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &savedSnapshotFakeStore{byName: func(context.Context, uuid.UUID, string) (db.SavedSnapshot, error) {
				return tc.row, tc.err
			}}
			rec := httptest.NewRecorder()
			savedSnapshotMux(t, store).ServeHTTP(rec,
				savedSnapshotRequest(owner, http.MethodGet, PathSavedSnapshots+"/web", ""))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			tc.check(t, snapshotBody(t, rec))
		})
	}
}

func TestSavedSnapshotListHandler(t *testing.T) {
	owner := uuid.NewV7()
	var got db.SnapshotListQuery
	store := &savedSnapshotFakeStore{all: func(_ context.Context, q db.SnapshotListQuery) ([]db.SavedSnapshot, error) {
		got = q
		return []db.SavedSnapshot{
			{OwnerID: owner, Name: "alpha", SandboxID: "bx_1", Status: db.SnapshotReady,
				ArtifactID: "snap-" + strings.Repeat("c", 32), CreatedAt: time.Now()},
			{OwnerID: owner, Name: "beta", SandboxID: "bx_2", Status: db.SnapshotSaving,
				ArtifactID: "snap-" + strings.Repeat("f", 32), CreatedAt: time.Now()},
		}, nil
	}}
	rec := httptest.NewRecorder()
	savedSnapshotMux(t, store).ServeHTTP(rec,
		savedSnapshotRequest(owner, http.MethodGet, PathSavedSnapshots+"?namePrefix=al", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if got.Owner != owner || got.NamePrefix != "al" || got.Name != "" {
		t.Fatalf("query owner_match=%v name=%q prefix=%q", got.Owner == owner, got.Name, got.NamePrefix)
	}
	body := snapshotBody(t, rec)
	if body["ok"] != true || body["type"] != TypeSavedSnapshotList {
		t.Fatalf("envelope ok=%v type=%v", body["ok"], body["type"])
	}
	if _, ok := body["pageInfo"]; ok {
		t.Fatal("saved-snapshot list must not carry pageInfo")
	}
	snapshots, _ := body["snapshots"].([]any)
	if len(snapshots) != 2 {
		t.Fatalf("snapshots count = %d, want 2", len(snapshots))
	}

	// An exact empty name is an empty result, not "no filter".
	store.all = func(context.Context, db.SnapshotListQuery) ([]db.SavedSnapshot, error) {
		t.Fatal("exact empty name reached the store")
		return nil, nil
	}
	rec = httptest.NewRecorder()
	savedSnapshotMux(t, store).ServeHTTP(rec,
		savedSnapshotRequest(owner, http.MethodGet, PathSavedSnapshots+"?name=", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("empty name status: %d", rec.Code)
	}
	if snapshots, _ := snapshotBody(t, rec)["snapshots"].([]any); len(snapshots) != 0 {
		t.Fatalf("empty name snapshots count = %d, want 0", len(snapshots))
	}

	// Unknown and repeated scalar parameters are invalid_query.
	for _, raw := range []string{"?limit=1", "?name=a&name=b"} {
		rec = httptest.NewRecorder()
		savedSnapshotMux(t, store).ServeHTTP(rec,
			savedSnapshotRequest(owner, http.MethodGet, PathSavedSnapshots+raw, ""))
		if rec.Code != http.StatusBadRequest || snapshotBody(t, rec)["code"] != apierr.CodeInvalidQuery {
			t.Fatalf("query %q status = %d", raw, rec.Code)
		}
	}
}

func TestSavedSnapshotDeleteHandler(t *testing.T) {
	owner := uuid.NewV7()
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{name: "deleted", wantStatus: http.StatusOK},
		{name: "missing", err: db.ErrNotFound, wantStatus: http.StatusNotFound, wantCode: apierr.CodeNotFound},
		{name: "saving", err: db.ErrSnapshotSaveInProgress, wantStatus: http.StatusConflict, wantCode: apierr.CodeSaveInProgress},
		{name: "internal", err: errors.New("driver"), wantStatus: http.StatusInternalServerError, wantCode: apierr.CodeInternal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &savedSnapshotFakeStore{remove: func(context.Context, db.AcceptSnapshotDeleteParams) (db.SavedSnapshot, error) {
				return db.SavedSnapshot{}, tc.err
			}}
			rec := httptest.NewRecorder()
			savedSnapshotMux(t, store).ServeHTTP(rec,
				savedSnapshotRequest(owner, http.MethodDelete, PathSavedSnapshots+"/web", ""))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			body := snapshotBody(t, rec)
			if tc.wantCode != "" {
				if body["code"] != tc.wantCode {
					t.Fatalf("code = %v, want %s", body["code"], tc.wantCode)
				}
				return
			}
			if body["type"] != TypeSavedSnapshotDeleted || body["name"] != "web" || body["status"] != "deleted" {
				t.Fatalf("deleted envelope type=%v name=%v status=%v", body["type"], body["name"], body["status"])
			}
		})
	}
}

func TestNewSavedSnapshotHandlersRequiresStore(t *testing.T) {
	if _, err := NewSavedSnapshotHandlers(SavedSnapshotOptions{}); err == nil {
		t.Fatal("accepted a nil store")
	}
}

func TestSavedSnapshotHandlersRegisterOnlyOwnRoutes(t *testing.T) {
	owner := uuid.NewV7()
	store := &savedSnapshotFakeStore{
		byName: func(context.Context, uuid.UUID, string) (db.SavedSnapshot, error) {
			return db.SavedSnapshot{OwnerID: owner, Name: "x", SandboxID: "bx_1", Status: db.SnapshotSaving, CreatedAt: time.Now()}, nil
		},
	}
	mux := savedSnapshotMux(t, store)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, savedSnapshotRequest(owner, http.MethodGet, "/api/v1/sandboxes", ""))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("core route unexpectedly registered: %d", rec.Code)
	}
	for _, tc := range []struct {
		method, path, body string
	}{
		{http.MethodGet, PathSavedSnapshots, ""},
		{http.MethodPost, PathSavedSnapshots, `{}`},
		{http.MethodGet, PathSavedSnapshots + "/x", ""},
		{http.MethodDelete, PathSavedSnapshots + "/x", ""},
	} {
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, savedSnapshotRequest(owner, tc.method, tc.path, tc.body))
		if rec.Code == http.StatusMethodNotAllowed || rec.Code == http.StatusNotFound {
			t.Fatalf("%s %s not registered: %d", tc.method, tc.path, rec.Code)
		}
	}
}
