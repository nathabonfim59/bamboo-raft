package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

type snapshotShareFakeStore struct {
	create func(context.Context, uuid.UUID, string) (db.SnapshotShare, string, error)
	list   func(context.Context, uuid.UUID, string) ([]db.SnapshotShare, error)
	revoke func(context.Context, uuid.UUID, string, string) (db.SnapshotShare, error)
}

func (f *snapshotShareFakeStore) CreateSnapshotShare(ctx context.Context, owner uuid.UUID, name string) (db.SnapshotShare, string, error) {
	if f.create == nil {
		return db.SnapshotShare{}, "", db.ErrNotFound
	}
	return f.create(ctx, owner, name)
}

func (f *snapshotShareFakeStore) ListSnapshotShares(ctx context.Context, owner uuid.UUID, name string) ([]db.SnapshotShare, error) {
	if f.list == nil {
		return nil, db.ErrNotFound
	}
	return f.list(ctx, owner, name)
}

func (f *snapshotShareFakeStore) RevokeSnapshotShare(ctx context.Context, owner uuid.UUID, name, id string) (db.SnapshotShare, error) {
	if f.revoke == nil {
		return db.SnapshotShare{}, db.ErrNotFound
	}
	return f.revoke(ctx, owner, name, id)
}

func shareFakeMux(t *testing.T, store SnapshotShareStore) *http.ServeMux {
	t.Helper()
	handlers, err := NewSnapshotShareHandlers(SnapshotShareOptions{Store: store})
	if err != nil {
		t.Fatalf("NewSnapshotShareHandlers: %v", err)
	}
	mux := http.NewServeMux()
	RegisterSnapshotShares(mux, handlers)
	return mux
}

func shareFakeRow(owner uuid.UUID, status db.SnapshotShareStatus) db.SnapshotShare {
	row := db.SnapshotShare{
		ID:         "shr_" + strings.Repeat("a", 32),
		OwnerID:    owner,
		Name:       "web",
		CodePrefix: "bsh_abcd1234",
		Status:     status,
		UseCount:   2,
		CreatedAt:  time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC),
	}
	if status == db.SnapshotShareRevoked {
		row.RevokedAt = new(row.CreatedAt.Add(time.Hour))
	}
	return row
}

func shareFakeRequest(owner uuid.UUID, method, target string) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	if owner != uuid.Nil() {
		r = r.WithContext(auth.WithIdentity(r.Context(), auth.Identity{Owner: owner.String()}))
	}
	return r
}

func TestSnapshotShareCreate(t *testing.T) {
	owner := uuid.NewV7()
	row := shareFakeRow(owner, db.SnapshotShareActive)
	code := "bsh_" + strings.Repeat("a", 52)
	store := &snapshotShareFakeStore{
		create: func(_ context.Context, got uuid.UUID, name string) (db.SnapshotShare, string, error) {
			if got != owner || name != "web" {
				t.Fatalf("create params owner %s name %q", got, name)
			}
			return row, code, nil
		},
	}
	mux := shareFakeMux(t, store)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, shareFakeRequest(owner, http.MethodPost, PathSavedSnapshots+"/web/shares"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d body %s", rec.Code, rec.Body)
	}
	body := snapshotBody(t, rec)
	if body["type"] != TypeSnapshotShareCreated || body["shareCode"] != code || body["name"] != "web" {
		t.Fatalf("body = %v", body)
	}
	share, ok := body["share"].(map[string]any)
	if !ok {
		t.Fatalf("share = %T", body["share"])
	}
	if share["codePrefix"] != "bsh_abcd1234" || share["status"] != "active" {
		t.Fatalf("share = %v", share)
	}
	if _, leaked := share["code"]; leaked {
		t.Fatal("share object leaked the full code")
	}
}

func TestSnapshotShareMatrix(t *testing.T) {
	owner := uuid.NewV7()
	active := shareFakeRow(owner, db.SnapshotShareActive)
	revoked := shareFakeRow(owner, db.SnapshotShareRevoked)
	revoked.ID = "shr_" + strings.Repeat("b", 32)

	tests := []struct {
		name   string
		store  *snapshotShareFakeStore
		method string
		target string
		owner  uuid.UUID
		status int
	}{
		{
			name: "create missing", store: &snapshotShareFakeStore{},
			method: http.MethodPost, target: PathSavedSnapshots + "/web/shares",
			owner: owner, status: http.StatusNotFound,
		},
		{
			name: "create malformed code", store: &snapshotShareFakeStore{
				create: func(context.Context, uuid.UUID, string) (db.SnapshotShare, string, error) {
					return active, "not-a-code", nil
				},
			},
			method: http.MethodPost, target: PathSavedSnapshots + "/web/shares",
			owner: owner, status: http.StatusInternalServerError,
		},
		{
			name: "create driver", store: &snapshotShareFakeStore{
				create: func(context.Context, uuid.UUID, string) (db.SnapshotShare, string, error) {
					return db.SnapshotShare{}, "", errors.New("driver")
				},
			},
			method: http.MethodPost, target: PathSavedSnapshots + "/web/shares",
			owner: owner, status: http.StatusInternalServerError,
		},
		{
			name: "list", store: &snapshotShareFakeStore{
				list: func(context.Context, uuid.UUID, string) ([]db.SnapshotShare, error) {
					return []db.SnapshotShare{active, revoked}, nil
				},
			},
			method: http.MethodGet, target: PathSavedSnapshots + "/web/shares",
			owner: owner, status: http.StatusOK,
		},
		{
			name: "list missing", store: &snapshotShareFakeStore{},
			method: http.MethodGet, target: PathSavedSnapshots + "/web/shares",
			owner: owner, status: http.StatusNotFound,
		},
		{
			name: "list driver", store: &snapshotShareFakeStore{
				list: func(context.Context, uuid.UUID, string) ([]db.SnapshotShare, error) {
					return nil, errors.New("driver")
				},
			},
			method: http.MethodGet, target: PathSavedSnapshots + "/web/shares",
			owner: owner, status: http.StatusInternalServerError,
		},
		{
			name: "revoke", store: &snapshotShareFakeStore{
				revoke: func(_ context.Context, got uuid.UUID, name, id string) (db.SnapshotShare, error) {
					if got != owner || name != "web" || id != active.ID {
						t.Fatalf("revoke params %s %q %q", got, name, id)
					}
					return revoked, nil
				},
			},
			method: http.MethodDelete, target: PathSavedSnapshots + "/web/shares/" + active.ID,
			owner: owner, status: http.StatusOK,
		},
		{
			name: "revoke invalid id", store: &snapshotShareFakeStore{},
			method: http.MethodDelete, target: PathSavedSnapshots + "/web/shares/nope",
			owner: owner, status: http.StatusNotFound,
		},
		{
			name: "revoke missing", store: &snapshotShareFakeStore{},
			method: http.MethodDelete, target: PathSavedSnapshots + "/web/shares/" + active.ID,
			owner: owner, status: http.StatusNotFound,
		},
		{
			name: "unauthorized", store: &snapshotShareFakeStore{},
			method: http.MethodGet, target: PathSavedSnapshots + "/web/shares",
			owner: uuid.Nil(), status: http.StatusUnauthorized,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mux := shareFakeMux(t, tc.store)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, shareFakeRequest(tc.owner, tc.method, tc.target))
			if rec.Code != tc.status {
				t.Fatalf("status = %d body %s, want %d", rec.Code, rec.Body, tc.status)
			}
			if tc.status == http.StatusOK && tc.method == http.MethodGet {
				body := snapshotBody(t, rec)
				shares, ok := body["shares"].([]any)
				if !ok || len(shares) != 2 {
					t.Fatalf("shares = %v", body["shares"])
				}
			}
		})
	}
}
