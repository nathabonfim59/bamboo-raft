package api_test

// Actual-byte validation of the saved-snapshot share handlers against the v1
// API document.

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

type shareCompatStore struct {
	create func(context.Context, uuid.UUID, string) (db.SnapshotShare, string, error)
	list   func(context.Context, uuid.UUID, string) ([]db.SnapshotShare, error)
	revoke func(context.Context, uuid.UUID, string, string) (db.SnapshotShare, error)
}

func (s *shareCompatStore) CreateSnapshotShare(ctx context.Context, owner uuid.UUID, name string) (db.SnapshotShare, string, error) {
	if s.create == nil {
		return db.SnapshotShare{}, "", db.ErrNotFound
	}
	return s.create(ctx, owner, name)
}

func (s *shareCompatStore) ListSnapshotShares(ctx context.Context, owner uuid.UUID, name string) ([]db.SnapshotShare, error) {
	if s.list == nil {
		return nil, db.ErrNotFound
	}
	return s.list(ctx, owner, name)
}

func (s *shareCompatStore) RevokeSnapshotShare(ctx context.Context, owner uuid.UUID, name, id string) (db.SnapshotShare, error) {
	if s.revoke == nil {
		return db.SnapshotShare{}, db.ErrNotFound
	}
	return s.revoke(ctx, owner, name, id)
}

func shareCompatRow(owner uuid.UUID, status db.SnapshotShareStatus) db.SnapshotShare {
	row := db.SnapshotShare{
		ID: "shr_" + strings.Repeat("a", 32), OwnerID: owner, Name: "web",
		CodePrefix: "bsh_abcd1234", Status: status, UseCount: 1,
		CreatedAt: coreCompatTime,
	}
	if status == db.SnapshotShareRevoked {
		row.RevokedAt = new(coreCompatTime.Add(time.Minute))
	}
	return row
}

func shareCompatMux(t *testing.T, store api.SnapshotShareStore) *http.ServeMux {
	t.Helper()
	handlers, err := api.NewSnapshotShareHandlers(api.SnapshotShareOptions{Store: store})
	if err != nil {
		t.Fatalf("NewSnapshotShareHandlers: %v", err)
	}
	mux := http.NewServeMux()
	api.RegisterSnapshotShares(mux, handlers)
	return mux
}

func TestSnapshotSharesCompatBytes(t *testing.T) {
	validators := coreCompatValidators(t)
	owner := uuid.MustParse(coreCompatOwner)
	active := shareCompatRow(owner, db.SnapshotShareActive)
	revoked := shareCompatRow(owner, db.SnapshotShareRevoked)
	sharePath := api.PathSavedSnapshots + "/web/shares"

	createCases := []struct {
		name   string
		store  *shareCompatStore
		status int
	}{
		{name: "201", store: &shareCompatStore{create: func(context.Context, uuid.UUID, string) (db.SnapshotShare, string, error) {
			return active, "bsh_" + strings.Repeat("a", 52), nil
		}}, status: http.StatusCreated},
		{name: "404", store: &shareCompatStore{}, status: http.StatusNotFound},
		{name: "500", store: &shareCompatStore{create: func(context.Context, uuid.UUID, string) (db.SnapshotShare, string, error) {
			return db.SnapshotShare{}, "", errors.New("driver")
		}}, status: http.StatusInternalServerError},
	}
	for _, tc := range createCases {
		t.Run("create "+tc.name, func(t *testing.T) {
			mux := shareCompatMux(t, tc.store)
			request := coreCompatRequest(http.MethodPost, sharePath, "", coreCompatOwner)
			coreCompatAssert(t, validators, "createSnapshotShare", tc.status, snapshotCompatServe(mux, request))
		})
	}

	listCases := []struct {
		name   string
		store  *shareCompatStore
		status int
	}{
		{name: "200", store: &shareCompatStore{list: func(context.Context, uuid.UUID, string) ([]db.SnapshotShare, error) {
			return []db.SnapshotShare{active, revoked}, nil
		}}, status: http.StatusOK},
		{name: "200 empty", store: &shareCompatStore{list: func(context.Context, uuid.UUID, string) ([]db.SnapshotShare, error) {
			return nil, nil
		}}, status: http.StatusOK},
		{name: "404", store: &shareCompatStore{}, status: http.StatusNotFound},
		{name: "500", store: &shareCompatStore{list: func(context.Context, uuid.UUID, string) ([]db.SnapshotShare, error) {
			return nil, errors.New("driver")
		}}, status: http.StatusInternalServerError},
	}
	for _, tc := range listCases {
		t.Run("list "+tc.name, func(t *testing.T) {
			mux := shareCompatMux(t, tc.store)
			request := coreCompatRequest(http.MethodGet, sharePath, "", coreCompatOwner)
			coreCompatAssert(t, validators, "listSnapshotShares", tc.status, snapshotCompatServe(mux, request))
		})
	}

	revokeCases := []struct {
		name   string
		store  *shareCompatStore
		target string
		status int
	}{
		{name: "200", store: &shareCompatStore{revoke: func(context.Context, uuid.UUID, string, string) (db.SnapshotShare, error) {
			return revoked, nil
		}}, target: sharePath + "/" + active.ID, status: http.StatusOK},
		{name: "404 invalid id", store: &shareCompatStore{}, target: sharePath + "/nope", status: http.StatusNotFound},
		{name: "404", store: &shareCompatStore{}, target: sharePath + "/" + active.ID, status: http.StatusNotFound},
		{name: "500", store: &shareCompatStore{revoke: func(context.Context, uuid.UUID, string, string) (db.SnapshotShare, error) {
			return db.SnapshotShare{}, errors.New("driver")
		}}, target: sharePath + "/" + active.ID, status: http.StatusInternalServerError},
	}
	for _, tc := range revokeCases {
		t.Run("revoke "+tc.name, func(t *testing.T) {
			mux := shareCompatMux(t, tc.store)
			request := coreCompatRequest(http.MethodDelete, tc.target, "", coreCompatOwner)
			coreCompatAssert(t, validators, "revokeSnapshotShare", tc.status, snapshotCompatServe(mux, request))
		})
	}

	unauthorized := []struct {
		operation, method, target string
	}{
		{"createSnapshotShare", http.MethodPost, sharePath},
		{"listSnapshotShares", http.MethodGet, sharePath},
		{"revokeSnapshotShare", http.MethodDelete, sharePath + "/" + active.ID},
	}
	for _, tc := range unauthorized {
		t.Run(tc.operation+" 401", func(t *testing.T) {
			mux := shareCompatMux(t, &shareCompatStore{})
			request := coreCompatRequest(tc.method, tc.target, "", "")
			coreCompatAssert(t, validators, tc.operation, http.StatusUnauthorized, snapshotCompatServe(mux, request))
		})
	}
}
