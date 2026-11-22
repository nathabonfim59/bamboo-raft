package api_test

// Actual-byte validation of the snapshot listing handlers against the v1 API
// document. Every status the handlers can emit is recorded here, including the
// documented 400 invalid_query, 401 and 404 answers and the approved 500.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// generationCompatStore is the external-package fake that records handler
// bytes. Its zero value answers an empty page and no latest generation.
type generationCompatStore struct {
	sandboxErr error
	listErr    error
	latestErr  error
	sandboxes  map[string]bool
	rows       []db.Generation
	latest     *db.Generation
}

func (s *generationCompatStore) Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error) {
	if err := ctx.Err(); err != nil {
		return db.Sandbox{}, err
	}
	if s.sandboxErr != nil {
		return db.Sandbox{}, s.sandboxErr
	}
	if owner == uuid.Nil() || !s.sandboxes[id] {
		return db.Sandbox{}, db.ErrNotFound
	}
	return db.Sandbox{ID: id, OwnerID: owner}, nil
}

func (s *generationCompatStore) ListGenerations(ctx context.Context, _ db.GenerationListQuery) ([]db.Generation, *db.GenerationCursor, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if s.listErr != nil {
		return nil, nil, s.listErr
	}
	return s.rows, nil, nil
}

func (s *generationCompatStore) LatestGeneration(ctx context.Context, _ uuid.UUID, _ string) (db.Generation, bool, error) {
	if err := ctx.Err(); err != nil {
		return db.Generation{}, false, err
	}
	if s.latestErr != nil {
		return db.Generation{}, false, s.latestErr
	}
	if s.latest == nil {
		return db.Generation{}, false, nil
	}
	return *s.latest, true, nil
}

// generationCompatRow builds one canonical completed generation.
func generationCompatRow(owner uuid.UUID) db.Generation {
	cut := coreCompatTime
	completed := cut.Add(45 * time.Second)
	return db.Generation{
		ID:             uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-000000000123"),
		OwnerID:        owner,
		SandboxID:      "bx_23456789",
		ChainID:        uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-000000000456"),
		Number:         2,
		Status:         db.GenerationCompleted,
		ManifestKey:    "chains/018f4b1c/manifest.json",
		NodeID:         "node-a",
		Image:          "guest",
		DiskBytes:      10 << 30,
		Chunks:         900,
		ChunksUploaded: 12,
		BytesUploaded:  18 << 20,
		ContentBytes:   3 << 20,
		Frozen:         true,
		CutAt:          &cut,
		StartedAt:      cut,
		CompletedAt:    &completed,
		DeadlineAt:     cut.Add(10 * time.Minute),
	}
}

// generationCompatMux builds a mux with only the snapshot listing routes.
func generationCompatMux(t *testing.T, store api.GenerationStore) *http.ServeMux {
	t.Helper()
	handlers, err := api.NewGenerationHandlers(api.GenerationOptions{Store: store})
	if err != nil {
		t.Fatalf("NewGenerationHandlers: %v", err)
	}
	mux := http.NewServeMux()
	api.Register(mux, handlers)
	return mux
}

// generationCompatAssert serves the request and validates the recorded bytes.
func generationCompatAssert(t *testing.T, validators []*compat.Validator, operationID string, status int, mux *http.ServeMux, request *http.Request) {
	t.Helper()
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != status {
		t.Fatalf("%s status = %d, want %d; body = %s", operationID, recorder.Code, status, recorder.Body.String())
	}
	for _, validator := range validators {
		if err := validator.Response(operationID, status, recorder.Header(), recorder.Body.Bytes()); err != nil {
			t.Errorf("%s %d failed API validation: %v", operationID, status, err)
		}
	}
}

func TestSnapshotListingsCompatBytes(t *testing.T) {
	validators := coreCompatValidators(t)
	owner := uuid.MustParse(coreCompatOwner)
	row := generationCompatRow(owner)
	const (
		sandboxList = "/api/v1/sandboxes/bx_23456789/snapshots"
		latest      = "/api/v1/sandboxes/bx_23456789/snapshots/latest"
	)

	listCases := []struct {
		name   string
		store  *generationCompatStore
		target string
		status int
	}{
		{name: "200 rows", store: &generationCompatStore{rows: []db.Generation{row}}, target: api.PathSnapshots, status: http.StatusOK},
		{name: "200 empty", store: &generationCompatStore{}, target: api.PathSnapshots, status: http.StatusOK},
		{name: "400 unknown parameter", store: &generationCompatStore{}, target: api.PathSnapshots + "?since=1", status: http.StatusBadRequest},
		{name: "400 bad cursor", store: &generationCompatStore{}, target: api.PathSnapshots + "?cursor=nope", status: http.StatusBadRequest},
		{name: "500", store: &generationCompatStore{listErr: errors.New("driver")}, target: api.PathSnapshots, status: http.StatusInternalServerError},
	}
	for _, tc := range listCases {
		t.Run("list "+tc.name, func(t *testing.T) {
			mux := generationCompatMux(t, tc.store)
			request := coreCompatRequest(http.MethodGet, tc.target, "", coreCompatOwner)
			generationCompatAssert(t, validators, "listSnapshots", tc.status, mux, request)
		})
	}

	sandboxCases := []struct {
		name   string
		store  *generationCompatStore
		target string
		status int
	}{
		{name: "200 rows", store: &generationCompatStore{sandboxes: map[string]bool{"bx_23456789": true}, rows: []db.Generation{row}}, target: sandboxList, status: http.StatusOK},
		{name: "200 empty", store: &generationCompatStore{sandboxes: map[string]bool{"bx_23456789": true}}, target: sandboxList, status: http.StatusOK},
		{name: "400 unknown parameter", store: &generationCompatStore{sandboxes: map[string]bool{"bx_23456789": true}}, target: sandboxList + "?sort=up", status: http.StatusBadRequest},
		{name: "404", store: &generationCompatStore{}, target: sandboxList, status: http.StatusNotFound},
		{name: "500 lookup", store: &generationCompatStore{sandboxErr: errors.New("driver")}, target: sandboxList, status: http.StatusInternalServerError},
		{name: "500 list", store: &generationCompatStore{sandboxes: map[string]bool{"bx_23456789": true}, listErr: errors.New("driver")}, target: sandboxList, status: http.StatusInternalServerError},
	}
	for _, tc := range sandboxCases {
		t.Run("sandbox list "+tc.name, func(t *testing.T) {
			mux := generationCompatMux(t, tc.store)
			request := coreCompatRequest(http.MethodGet, tc.target, "", coreCompatOwner)
			generationCompatAssert(t, validators, "listSandboxSnapshots", tc.status, mux, request)
		})
	}

	latestCases := []struct {
		name  string
		store *generationCompatStore
		code  int
	}{
		{name: "200 some", store: &generationCompatStore{sandboxes: map[string]bool{"bx_23456789": true}, latest: &row}, code: http.StatusOK},
		{name: "200 null", store: &generationCompatStore{sandboxes: map[string]bool{"bx_23456789": true}}, code: http.StatusOK},
		{name: "404", store: &generationCompatStore{}, code: http.StatusNotFound},
		{name: "500", store: &generationCompatStore{sandboxes: map[string]bool{"bx_23456789": true}, latestErr: errors.New("driver")}, code: http.StatusInternalServerError},
	}
	for _, tc := range latestCases {
		t.Run("latest "+tc.name, func(t *testing.T) {
			mux := generationCompatMux(t, tc.store)
			request := coreCompatRequest(http.MethodGet, latest, "", coreCompatOwner)
			generationCompatAssert(t, validators, "getLatestSandboxSnapshot", tc.code, mux, request)
		})
	}

	// Every operation requires an authenticated account; the actual bytes of
	// the 401 must validate against the document.
	unauthorized := []struct {
		operation, target string
	}{
		{"listSnapshots", api.PathSnapshots},
		{"listSandboxSnapshots", sandboxList},
		{"getLatestSandboxSnapshot", latest},
	}
	for _, tc := range unauthorized {
		t.Run(tc.operation+" 401", func(t *testing.T) {
			mux := generationCompatMux(t, &generationCompatStore{})
			request := coreCompatRequest(http.MethodGet, tc.target, "", "")
			generationCompatAssert(t, validators, tc.operation, http.StatusUnauthorized, mux, request)
		})
	}
}
