package api

import (
	"context"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// generationTestOwner is a fixed account id every test request authenticates
// as.
var generationTestOwner = uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-123456789abc")

// generationTestTime is the cut time of the fixtures.
var generationTestTime = time.Date(2026, 11, 20, 9, 30, 0, 0, time.UTC)

// generationFakeStore is an in-memory GenerationStore. It applies the same
// owner, status, sort and keyset rules the SQL store documents so the handler
// tests exercise their real pagination contract.
type generationFakeStore struct {
	mu         sync.Mutex
	sandboxes  map[string]bool
	rows       []db.Generation
	listErr    error
	latestErr  error
	sandboxErr error
	calls      []db.GenerationListQuery
}

func (s *generationFakeStore) Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error) {
	if err := ctx.Err(); err != nil {
		return db.Sandbox{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sandboxErr != nil {
		return db.Sandbox{}, s.sandboxErr
	}
	if owner != generationTestOwner || !s.sandboxes[id] {
		return db.Sandbox{}, db.ErrNotFound
	}
	return db.Sandbox{ID: id, OwnerID: owner}, nil
}

func (s *generationFakeStore) ListGenerations(ctx context.Context, q db.GenerationListQuery) ([]db.Generation, *db.GenerationCursor, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, q)
	if s.listErr != nil {
		return nil, nil, s.listErr
	}
	var rows []db.Generation
	for _, row := range s.rows {
		if row.OwnerID != q.Owner || row.Status != db.GenerationCompleted || row.PurgedAt != nil {
			continue
		}
		if q.SandboxID != "" && row.SandboxID != q.SandboxID {
			continue
		}
		if q.After != nil {
			if q.Ascending && !generationAfter(row, *q.After) {
				continue
			}
			if !q.Ascending && !generationBefore(row, *q.After) {
				continue
			}
		}
		rows = append(rows, row)
	}
	slices.SortFunc(rows, func(a, b db.Generation) int {
		order := generationOrder(a, b)
		if q.Ascending {
			return order
		}
		return -order
	})
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	if len(rows) > limit {
		last := rows[limit-1]
		return rows[:limit], &db.GenerationCursor{At: *last.CutAt, ID: last.ID}, nil
	}
	return rows, nil, nil
}

func (s *generationFakeStore) LatestGeneration(ctx context.Context, owner uuid.UUID, sandboxID string) (db.Generation, bool, error) {
	if err := ctx.Err(); err != nil {
		return db.Generation{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latestErr != nil {
		return db.Generation{}, false, s.latestErr
	}
	var found bool
	var newest db.Generation
	for _, row := range s.rows {
		if row.OwnerID != owner || row.SandboxID != sandboxID ||
			row.Status != db.GenerationCompleted || row.PurgedAt != nil {
			continue
		}
		if !found || generationOrder(row, newest) > 0 {
			newest, found = row, true
		}
	}
	return newest, found, nil
}

// generationOrder compares two rows by the (cut time, id) pair the listing
// orders by.
func generationOrder(a, b db.Generation) int {
	if order := a.CutAt.Compare(*b.CutAt); order != 0 {
		return order
	}
	return a.ID.Compare(b.ID)
}

// generationAfter reports whether row is strictly after cursor in the listing
// order.
func generationAfter(row db.Generation, cursor db.GenerationCursor) bool {
	if order := row.CutAt.Compare(cursor.At); order != 0 {
		return order > 0
	}
	return row.ID.Compare(cursor.ID) > 0
}

// generationBefore reports whether row is strictly before cursor in the
// listing order.
func generationBefore(row db.Generation, cursor db.GenerationCursor) bool {
	if order := row.CutAt.Compare(cursor.At); order != 0 {
		return order < 0
	}
	return row.ID.Compare(cursor.ID) < 0
}

// generationTestRow builds one completed generation fixture.
func generationTestRow(owner uuid.UUID, sandboxID string, number uint32, cut time.Time) db.Generation {
	completed := cut.Add(40 * time.Second)
	return db.Generation{
		ID:             uuid.NewV7(),
		OwnerID:        owner,
		SandboxID:      sandboxID,
		ChainID:        uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-000000000001"),
		Number:         number,
		Status:         db.GenerationCompleted,
		ManifestKey:    "chains/x/" + sandboxID,
		NodeID:         "node-a",
		Image:          "guest",
		DiskBytes:      10 << 30,
		Chunks:         100,
		ChunksUploaded: 7,
		BytesUploaded:  18 << 20,
		ContentBytes:   3 << 20,
		Frozen:         true,
		CutAt:          &cut,
		StartedAt:      cut,
		CompletedAt:    &completed,
		DeadlineAt:     cut.Add(10 * time.Minute),
	}
}

// generationTestMux builds a mux with only the snapshot listing routes.
func generationTestMux(t *testing.T, store GenerationStore) *http.ServeMux {
	t.Helper()
	handlers, err := NewGenerationHandlers(GenerationOptions{Store: store})
	if err != nil {
		t.Fatalf("NewGenerationHandlers: %v", err)
	}
	mux := http.NewServeMux()
	Register(mux, handlers)
	return mux
}

// generationTestRequest builds an authenticated request, or an anonymous one
// when owner is empty.
func generationTestRequest(method, target, owner string) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	if owner != "" {
		r = r.WithContext(auth.WithIdentity(r.Context(), auth.Identity{Owner: owner}))
	}
	return r
}

// generationTestServe records one handler response.
func generationTestServe(t *testing.T, handler http.Handler, request *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// generationTestList decodes a 200 list envelope.
func generationTestList(t *testing.T, recorder *httptest.ResponseRecorder) SnapshotListResponse {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", recorder.Code, recorder.Body.String())
	}
	var got SnapshotListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if !got.OK || got.Type != TypeSnapshotList {
		t.Fatalf("envelope = %+v", got.SuccessBase)
	}
	return got
}

// generationTestError asserts one error envelope names code and, when
// non-empty, mentions param.
func generationTestError(t *testing.T, recorder *httptest.ResponseRecorder, status int, code, param string) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, status, recorder.Body.String())
	}
	var envelope struct {
		OK      bool   `json:"ok"`
		Type    string `json:"type"`
		Status  int    `json:"status"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if envelope.OK || envelope.Type != "sandbox.error" || envelope.Status != status || envelope.Code != code {
		t.Fatalf("error envelope = %+v", envelope)
	}
	if param != "" && !strings.Contains(envelope.Message, param) {
		t.Fatalf("message %q does not name %q", envelope.Message, param)
	}
}

func TestListSnapshotsMapsGenerations(t *testing.T) {
	base := generationTestRow(generationTestOwner, "bx_23456789", 0, generationTestTime)
	incremental := generationTestRow(generationTestOwner, "bx_23456789", 3, generationTestTime.Add(time.Minute))
	otherOwner := generationTestRow(uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-999999999999"), "bx_23456789", 1, generationTestTime)
	pending := generationTestRow(generationTestOwner, "bx_23456789", 4, generationTestTime.Add(2*time.Minute))
	pending.Status = db.GenerationPending
	purged := generationTestRow(generationTestOwner, "bx_23456789", 5, generationTestTime.Add(3*time.Minute))
	purged.PurgedAt = new(generationTestTime.Add(4 * time.Minute))

	store := &generationFakeStore{
		sandboxes: map[string]bool{"bx_23456789": true},
		rows:      []db.Generation{base, incremental, otherOwner, pending, purged},
	}
	recorder := generationTestServe(t, generationTestMux(t, store),
		generationTestRequest(http.MethodGet, PathSnapshots, generationTestOwner.String()))
	got := generationTestList(t, recorder)

	if len(got.Snapshots) != 2 {
		t.Fatalf("snapshots = %d, want 2 (completed, owned, unpurged): %+v", len(got.Snapshots), got.Snapshots)
	}
	newest := got.Snapshots[0]
	if newest.ID != incremental.ID.String() || newest.SandboxID != "bx_23456789" ||
		newest.Status != SnapshotCompleted || newest.Origin != SnapshotCaptured {
		t.Fatalf("newest = %+v", newest)
	}
	if newest.Kind == nil || *newest.Kind != SnapshotIncremental {
		t.Fatalf("kind = %v, want incremental", newest.Kind)
	}
	if newest.Generation == nil || *newest.Generation != 3 {
		t.Fatalf("generation = %v, want 3", newest.Generation)
	}
	if newest.ChainID == nil || *newest.ChainID != incremental.ChainID.String() {
		t.Fatalf("chainId = %v", newest.ChainID)
	}
	if newest.SizeBytes == nil || *newest.SizeBytes != int64(incremental.BytesUploaded) {
		t.Fatalf("sizeBytes = %v, want the uploaded bytes", newest.SizeBytes)
	}
	if newest.ContentSizeBytes == nil || *newest.ContentSizeBytes != int64(incremental.ContentBytes) {
		t.Fatalf("contentSizeBytes = %v, want the content bytes", newest.ContentSizeBytes)
	}
	if newest.FileCount != nil || newest.ContentFileCount != nil {
		t.Fatalf("fileCount/contentFileCount = %v/%v, want null", newest.FileCount, newest.ContentFileCount)
	}
	if !newest.CreatedAt.Equal(*incremental.CutAt) || newest.CompletedAt == nil ||
		!newest.CompletedAt.Equal(*incremental.CompletedAt) {
		t.Fatalf("times = %v/%v", newest.CreatedAt, newest.CompletedAt)
	}
	if got.PageInfo.Limit != 100 || got.PageInfo.HasMore || got.PageInfo.NextCursor != nil {
		t.Fatalf("pageInfo = %+v", got.PageInfo)
	}
}

func TestListSnapshotsQueryParsing(t *testing.T) {
	store := &generationFakeStore{
		sandboxes: map[string]bool{"bx_23456789": true},
		rows:      []db.Generation{generationTestRow(generationTestOwner, "bx_23456789", 0, generationTestTime)},
	}
	mux := generationTestMux(t, store)
	owner := generationTestOwner.String()

	bad := []struct {
		name   string
		target string
		param  string
	}{
		{"unknown parameter", PathSnapshots + "?sandboxId=bx_23456789", "sandboxId"},
		{"unknown parameter with a valid one", PathSnapshots + "?limit=2&bogus=1", "bogus"},
		{"limit zero", PathSnapshots + "?limit=0", "limit"},
		{"limit too large", PathSnapshots + "?limit=201", "limit"},
		{"limit not a number", PathSnapshots + "?limit=ten", "limit"},
		{"repeated limit", PathSnapshots + "?limit=2&limit=3", "limit"},
		{"bad sort", PathSnapshots + "?sort=sideways", "sort"},
		{"bad cursor", PathSnapshots + "?cursor=not-a-cursor", "cursor"},
		{"foreign cursor", PathSnapshots + "?cursor=" + Cursor{
			Kind: CursorKindSandboxes, Sort: SortDescending, CreatedAt: generationTestTime, ID: "bx_23456789",
		}.Encode(), "cursor"},
		{"cursor without an id", PathSnapshots + "?cursor=" + Cursor{
			Kind: CursorKindSnapshots, Sort: SortDescending, CreatedAt: generationTestTime, ID: "not-a-uuid",
		}.Encode(), "cursor"},
		{"cursor sort mismatch", PathSnapshots + "?sort=asc&cursor=" + Cursor{
			Kind: CursorKindSnapshots, Sort: SortDescending, CreatedAt: generationTestTime, ID: uuid.NewV7().String(),
		}.Encode(), "cursor"},
		{"malformed query", PathSnapshots + "?limit=%zz", "query"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			recorder := generationTestServe(t, mux, generationTestRequest(http.MethodGet, tc.target, owner))
			generationTestError(t, recorder, http.StatusBadRequest, "invalid_query", tc.param)
		})
	}

	t.Run("limit is forwarded", func(t *testing.T) {
		for _, limit := range []int{1, 7, 200} {
			store.calls = nil
			recorder := generationTestServe(t, mux, generationTestRequest(
				http.MethodGet, PathSnapshots+"?limit="+strconv.Itoa(limit), owner))
			got := generationTestList(t, recorder)
			if got.PageInfo.Limit != limit {
				t.Fatalf("pageInfo.limit = %d, want %d", got.PageInfo.Limit, limit)
			}
			if len(store.calls) != 1 || store.calls[0].Limit != limit || store.calls[0].Ascending {
				t.Fatalf("store query = %+v", store.calls)
			}
			if store.calls[0].SandboxID != "" {
				t.Fatalf("sandbox filter = %q, want none", store.calls[0].SandboxID)
			}
		}
	})

	t.Run("sort asc is forwarded", func(t *testing.T) {
		store.calls = nil
		generationTestServe(t, mux, generationTestRequest(http.MethodGet, PathSnapshots+"?sort=asc", owner))
		if len(store.calls) != 1 || !store.calls[0].Ascending {
			t.Fatalf("store query = %+v", store.calls)
		}
	})
}

func TestListSnapshotsCursorWalk(t *testing.T) {
	// Equal cut times force the id tie-break, the case the keyset must keep
	// stable across pages.
	store := &generationFakeStore{sandboxes: map[string]bool{"bx_23456789": true}}
	for range 5 {
		store.rows = append(store.rows,
			generationTestRow(generationTestOwner, "bx_23456789", uint32(len(store.rows)), generationTestTime))
	}
	mux := generationTestMux(t, store)
	owner := generationTestOwner.String()

	for _, sort := range []SortOrder{SortAscending, SortDescending} {
		t.Run("sort "+string(sort), func(t *testing.T) {
			var walked []string
			target := PathSnapshots + "?limit=2&sort=" + string(sort)
			for pages := 0; ; pages++ {
				if pages > len(store.rows) {
					t.Fatal("cursor walk did not terminate")
				}
				recorder := generationTestServe(t, mux, generationTestRequest(http.MethodGet, target, owner))
				page := generationTestList(t, recorder)
				for _, snapshot := range page.Snapshots {
					walked = append(walked, snapshot.ID)
				}
				if page.PageInfo.HasMore != (page.PageInfo.NextCursor != nil) {
					t.Fatalf("pageInfo = %+v", page.PageInfo)
				}
				if page.PageInfo.NextCursor == nil {
					break
				}
				target = PathSnapshots + "?limit=2&sort=" + string(sort) + "&cursor=" + *page.PageInfo.NextCursor
			}
			if len(walked) != len(store.rows) {
				t.Fatalf("walked %d rows, want %d: %v", len(walked), len(store.rows), walked)
			}
			seen := map[string]bool{}
			for _, id := range walked {
				if seen[id] {
					t.Fatalf("row %s appeared twice: %v", id, walked)
				}
				seen[id] = true
			}
		})
	}
}

func TestListSandboxSnapshots(t *testing.T) {
	store := &generationFakeStore{
		sandboxes: map[string]bool{"bx_23456789": true},
		rows: []db.Generation{
			generationTestRow(generationTestOwner, "bx_23456789", 0, generationTestTime),
			generationTestRow(generationTestOwner, "bx_87654322", 1, generationTestTime.Add(time.Minute)),
		},
	}
	mux := generationTestMux(t, store)
	owner := generationTestOwner.String()

	t.Run("200 lists only the sandbox", func(t *testing.T) {
		recorder := generationTestServe(t, mux,
			generationTestRequest(http.MethodGet, "/api/v1/sandboxes/bx_23456789/snapshots", owner))
		got := generationTestList(t, recorder)
		if len(got.Snapshots) != 1 || got.Snapshots[0].SandboxID != "bx_23456789" {
			t.Fatalf("snapshots = %+v", got.Snapshots)
		}
		if len(store.calls) == 0 || store.calls[len(store.calls)-1].SandboxID != "bx_23456789" {
			t.Fatalf("store query = %+v", store.calls)
		}
	})

	t.Run("404 missing sandbox", func(t *testing.T) {
		recorder := generationTestServe(t, mux,
			generationTestRequest(http.MethodGet, "/api/v1/sandboxes/bx_87654322/snapshots", owner))
		generationTestError(t, recorder, http.StatusNotFound, "not_found", "")
	})

	t.Run("404 foreign sandbox", func(t *testing.T) {
		other := "018f4b1c-7e2d-7a3b-9c4d-999999999999"
		recorder := generationTestServe(t, mux,
			generationTestRequest(http.MethodGet, "/api/v1/sandboxes/bx_23456789/snapshots", other))
		generationTestError(t, recorder, http.StatusNotFound, "not_found", "")
	})

	t.Run("400 unknown parameter", func(t *testing.T) {
		recorder := generationTestServe(t, mux,
			generationTestRequest(http.MethodGet, "/api/v1/sandboxes/bx_23456789/snapshots?since=1", owner))
		generationTestError(t, recorder, http.StatusBadRequest, "invalid_query", "since")
	})

	t.Run("500 store failure", func(t *testing.T) {
		failing := &generationFakeStore{
			sandboxes: map[string]bool{"bx_23456789": true},
			listErr:   errors.New("driver"),
		}
		recorder := generationTestServe(t, generationTestMux(t, failing),
			generationTestRequest(http.MethodGet, "/api/v1/sandboxes/bx_23456789/snapshots", owner))
		generationTestError(t, recorder, http.StatusInternalServerError, "http_500", "")
	})
}

func TestGetLatestSandboxSnapshot(t *testing.T) {
	older := generationTestRow(generationTestOwner, "bx_23456789", 0, generationTestTime)
	newer := generationTestRow(generationTestOwner, "bx_23456789", 1, generationTestTime.Add(time.Minute))
	store := &generationFakeStore{
		sandboxes: map[string]bool{"bx_23456789": true, "bx_87654322": true},
		rows:      []db.Generation{older, newer},
	}
	mux := generationTestMux(t, store)
	owner := generationTestOwner.String()

	decodeLatest := func(t *testing.T, recorder *httptest.ResponseRecorder) SnapshotLatestResponse {
		t.Helper()
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body = %s", recorder.Code, recorder.Body.String())
		}
		var got SnapshotLatestResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode latest response: %v", err)
		}
		if !got.OK || got.Type != TypeSnapshotLatest {
			t.Fatalf("envelope = %+v", got.SuccessBase)
		}
		return got
	}

	t.Run("200 newest", func(t *testing.T) {
		got := decodeLatest(t, generationTestServe(t, mux,
			generationTestRequest(http.MethodGet, "/api/v1/sandboxes/bx_23456789/snapshots/latest", owner)))
		if got.Snapshot == nil || got.Snapshot.ID != newer.ID.String() {
			t.Fatalf("snapshot = %+v, want %s", got.Snapshot, newer.ID)
		}
	})

	t.Run("200 null when none", func(t *testing.T) {
		got := decodeLatest(t, generationTestServe(t, mux,
			generationTestRequest(http.MethodGet, "/api/v1/sandboxes/bx_87654322/snapshots/latest", owner)))
		if got.Snapshot != nil {
			t.Fatalf("snapshot = %+v, want null", got.Snapshot)
		}
	})

	t.Run("query string is not a 400", func(t *testing.T) {
		recorder := generationTestServe(t, mux,
			generationTestRequest(http.MethodGet, "/api/v1/sandboxes/bx_23456789/snapshots/latest?foo=bar", owner))
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", recorder.Code)
		}
	})

	t.Run("404", func(t *testing.T) {
		recorder := generationTestServe(t, mux,
			generationTestRequest(http.MethodGet, "/api/v1/sandboxes/bx_00000000/snapshots/latest", owner))
		generationTestError(t, recorder, http.StatusNotFound, "not_found", "")
	})

	t.Run("500", func(t *testing.T) {
		failing := &generationFakeStore{
			sandboxes: map[string]bool{"bx_23456789": true},
			latestErr: errors.New("driver"),
		}
		recorder := generationTestServe(t, generationTestMux(t, failing),
			generationTestRequest(http.MethodGet, "/api/v1/sandboxes/bx_23456789/snapshots/latest", owner))
		generationTestError(t, recorder, http.StatusInternalServerError, "http_500", "")
	})

	t.Run("500 when the sandbox lookup faults", func(t *testing.T) {
		failing := &generationFakeStore{sandboxErr: errors.New("driver")}
		recorder := generationTestServe(t, generationTestMux(t, failing),
			generationTestRequest(http.MethodGet, "/api/v1/sandboxes/bx_23456789/snapshots/latest", owner))
		generationTestError(t, recorder, http.StatusInternalServerError, "http_500", "")
	})
}

func TestSnapshotEndpointsRequireIdentity(t *testing.T) {
	store := &generationFakeStore{sandboxes: map[string]bool{"bx_23456789": true}}
	mux := generationTestMux(t, store)
	for _, target := range []string{
		PathSnapshots,
		"/api/v1/sandboxes/bx_23456789/snapshots",
		"/api/v1/sandboxes/bx_23456789/snapshots/latest",
	} {
		t.Run(target, func(t *testing.T) {
			recorder := generationTestServe(t, mux, generationTestRequest(http.MethodGet, target, ""))
			generationTestError(t, recorder, http.StatusUnauthorized, "unauthorized", "")
		})
	}
}

func TestNewGenerationHandlersNeedsStore(t *testing.T) {
	if _, err := NewGenerationHandlers(GenerationOptions{}); err == nil {
		t.Fatal("expected an error without a store")
	}
}

func TestSnapshotSummaryRejectsNonCanonicalRows(t *testing.T) {
	owner := generationTestOwner
	valid := generationTestRow(owner, "bx_23456789", 1, generationTestTime)
	h := &generationHandlers{store: &generationFakeStore{}, logger: slog.New(slog.DiscardHandler)}
	tests := []struct {
		name string
		row  db.Generation
	}{
		{"foreign owner", func() db.Generation { r := valid; r.OwnerID = uuid.NewV7(); return r }()},
		{"not completed", func() db.Generation { r := valid; r.Status = db.GenerationFailed; return r }()},
		{"no cut time", func() db.Generation { r := valid; r.CutAt = nil; return r }()},
		{"no id", func() db.Generation { r := valid; r.ID = uuid.Nil(); return r }()},
		{"no sandbox", func() db.Generation { r := valid; r.SandboxID = ""; return r }()},
		{"size overflow", func() db.Generation { r := valid; r.BytesUploaded = ^uint64(0); return r }()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, apiErr := h.summary(t.Context(), owner, tc.row); apiErr == nil ||
				apiErr.Status != http.StatusInternalServerError {
				t.Fatalf("summary = %v, want a 500", apiErr)
			}
		})
	}
}
