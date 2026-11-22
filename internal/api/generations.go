package api

// Snapshot listing HTTP surface (/api/v1/snapshots and the per-sandbox
// snapshot routes). It owns the strict query parsing, the opaque cursor and
// the DTO mapping; the owner-scoped reads live in internal/db. Only completed,
// unpurged generations ever reach the API, and a foreign or missing sandbox is
// indistinguishable from a missing one.

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// CursorKindSnapshots is the cursor collection space of snapshot listings. A
// sandbox-list cursor never applies to a snapshot page.
const CursorKindSnapshots CursorKind = "snapshots"

// Snapshot envelope discriminators. They live with the listing handlers
// because no other feature emits them.
const (
	TypeSnapshotList   = "snapshot.list"
	TypeSnapshotLatest = "snapshot.latest"
)

// SnapshotStatus is the state of a listed snapshot. The list endpoints expose
// completed generations only.
type SnapshotStatus string

// SnapshotCompleted is the only listed status.
const SnapshotCompleted SnapshotStatus = "completed"

// SnapshotKind names the position of a generation in its chain.
type SnapshotKind string

// Snapshot kinds.
const (
	SnapshotBase        SnapshotKind = "base"
	SnapshotIncremental SnapshotKind = "incremental"
)

// SnapshotOrigin says where a listed generation came from. This server always
// captures its own generations.
type SnapshotOrigin string

// SnapshotCaptured marks a generation this sandbox captured itself.
const SnapshotCaptured SnapshotOrigin = "captured"

// SnapshotSummary is the public snapshot object of the list and latest
// endpoints. Fields the schema makes nullable are pointers so an unknown value
// serializes as an explicit null instead of being invented.
type SnapshotSummary struct {
	ID        string         `json:"id"`
	SandboxID string         `json:"sandboxId"`
	Status    SnapshotStatus `json:"status"`
	// Kind is "base" for generation 0 and "incremental" afterwards.
	Kind *SnapshotKind `json:"kind"`
	// Generation is the chain position; 0 is the base generation.
	Generation  *int64     `json:"generation"`
	ChainID     *string    `json:"chainId"`
	CreatedAt   time.Time  `json:"createdAt"`
	CompletedAt *time.Time `json:"completedAt"`
	// SizeBytes is what this generation added, not the full restored size.
	SizeBytes *int64 `json:"sizeBytes"`
	// FileCount is null: block-level generations carry no file inventory.
	FileCount *int64 `json:"fileCount"`
	// ContentSizeBytes is the bytes of the sandbox's own data this
	// generation restores, excluding the base image.
	ContentSizeBytes *int64 `json:"contentSizeBytes"`
	// ContentFileCount is null for the same reason as FileCount.
	ContentFileCount *int64         `json:"contentFileCount"`
	Origin           SnapshotOrigin `json:"origin"`
}

// SnapshotListResponse is the snapshot list envelope shared by the account and
// per-sandbox listings.
type SnapshotListResponse struct {
	SuccessBase
	Snapshots []SnapshotSummary `json:"snapshots"`
	PageInfo  PageInfo          `json:"pageInfo"`
}

// NewSnapshotListResponse builds a snapshot list envelope. A nil slice becomes
// an empty array because the schema requires one.
func NewSnapshotListResponse(snapshots []SnapshotSummary, page PageInfo) SnapshotListResponse {
	if snapshots == nil {
		snapshots = []SnapshotSummary{}
	}
	return SnapshotListResponse{
		SuccessBase: success(TypeSnapshotList),
		Snapshots:   snapshots,
		PageInfo:    page,
	}
}

// SnapshotLatestResponse is the GET /snapshots/latest envelope. A nil snapshot
// is the explicit null the schema allows when the sandbox has no generation.
type SnapshotLatestResponse struct {
	SuccessBase
	Snapshot *SnapshotSummary `json:"snapshot"`
}

// NewSnapshotLatestResponse builds a latest-snapshot envelope.
func NewSnapshotLatestResponse(snapshot *SnapshotSummary) SnapshotLatestResponse {
	return SnapshotLatestResponse{SuccessBase: success(TypeSnapshotLatest), Snapshot: snapshot}
}

// GenerationStore is the owner-scoped persistence the snapshot list handlers
// consume. The accepted [*db.Store] implements it directly. A sandbox read is
// separate from the listing because a missing or deleted sandbox must answer
// 404 instead of an empty page.
type GenerationStore interface {
	// Sandbox returns one live sandbox owned by owner.
	Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error)
	// ListGenerations returns one owner-scoped page of completed, unpurged
	// generations and its next cursor.
	ListGenerations(ctx context.Context, query db.GenerationListQuery) ([]db.Generation, *db.GenerationCursor, error)
	// LatestGeneration returns the sandbox's newest completed, unpurged
	// generation; ok is false when there is none.
	LatestGeneration(ctx context.Context, owner uuid.UUID, sandboxID string) (db.Generation, bool, error)
}

// GenerationStoreContract keeps the database facade inside the accepted
// contract.
var _ GenerationStore = (*db.Store)(nil)

// GenerationOptions configures the snapshot list handlers. Store is required;
// a nil Logger discards operational failures.
type GenerationOptions struct {
	Store  GenerationStore
	Logger *slog.Logger
}

// NewGenerationHandlers builds the three snapshot listing handlers without
// registering them. The returned [Handlers] has only those fields set, so it
// can be passed to [Register] beside the core route set.
func NewGenerationHandlers(options GenerationOptions) (Handlers, error) {
	if options.Store == nil {
		return Handlers{}, errors.New("api: snapshot list handlers need a store")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	h := &generationHandlers{store: options.Store, logger: logger}
	return Handlers{
		ListSnapshots:            h.list,
		ListSandboxSnapshots:     h.listSandbox,
		GetLatestSandboxSnapshot: h.latest,
	}, nil
}

type generationHandlers struct {
	store  GenerationStore
	logger *slog.Logger
}

// list serves GET /api/v1/snapshots: every completed generation of the
// owner's live sandboxes, newest first unless sort=asc.
func (h *generationHandlers) list(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	values, apiErr := coreParseQuery(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	query, err := ParseListSnapshotsQuery(values)
	if err != nil {
		apierr.WriteError(w, r, queryFailure(err))
		return
	}
	h.writePage(w, r, owner, query, "")
}

// listSandbox serves GET /api/v1/sandboxes/{sandboxId}/snapshots. A foreign or
// missing sandbox is the same 404, never an empty page.
func (h *generationHandlers) listSandbox(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	sandboxID := r.PathValue(ParamSandboxID)
	if sandboxID == "" {
		apierr.WriteError(w, r, coreNotFound())
		return
	}
	values, apiErr := coreParseQuery(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	query, err := ParseListSnapshotsQuery(values)
	if err != nil {
		apierr.WriteError(w, r, queryFailure(err))
		return
	}
	if apiErr := h.requireSandbox(r.Context(), owner, sandboxID); apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	h.writePage(w, r, owner, query, sandboxID)
}

// latest serves GET /api/v1/sandboxes/{sandboxId}/snapshots/latest: the newest
// completed generation, or an explicit null when the sandbox has none. The
// pinned operation declares no 400, so the query string is not parsed here.
func (h *generationHandlers) latest(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	sandboxID := r.PathValue(ParamSandboxID)
	if sandboxID == "" {
		apierr.WriteError(w, r, coreNotFound())
		return
	}
	if apiErr := h.requireSandbox(r.Context(), owner, sandboxID); apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	row, ok, err := h.store.LatestGeneration(r.Context(), owner, sandboxID)
	if err != nil {
		if apiErr := h.storeFailure(r.Context(), "get latest snapshot", err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	if !ok {
		apierr.Write(w, http.StatusOK, TypeSnapshotLatest, NewSnapshotLatestResponse(nil))
		return
	}
	snapshot, apiErr := h.summary(r.Context(), owner, row)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	apierr.Write(w, http.StatusOK, TypeSnapshotLatest, NewSnapshotLatestResponse(&snapshot))
}

// writePage runs one listing and writes it. sandboxID empty lists the whole
// account.
func (h *generationHandlers) writePage(w http.ResponseWriter, r *http.Request, owner uuid.UUID, query SnapshotListQuery, sandboxID string) {
	dbQuery := db.GenerationListQuery{
		Owner:     owner,
		SandboxID: sandboxID,
		Limit:     query.Limit,
		Ascending: query.Sort == SortAscending,
	}
	if query.Cursor != nil {
		id, err := uuid.Parse(query.Cursor.ID)
		if err != nil || id == uuid.Nil() {
			apierr.WriteError(w, r, coreInvalidQuery("cursor", "cursor id is not valid"))
			return
		}
		dbQuery.After = &db.GenerationCursor{At: query.Cursor.CreatedAt, ID: id}
	}
	rows, next, err := h.store.ListGenerations(r.Context(), dbQuery)
	if err != nil {
		if apiErr := h.storeFailure(r.Context(), "list snapshots", err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	snapshots := make([]SnapshotSummary, len(rows))
	for i, row := range rows {
		snapshot, apiErr := h.summary(r.Context(), owner, row)
		if apiErr != nil {
			apierr.WriteError(w, r, apiErr)
			return
		}
		snapshots[i] = snapshot
	}
	var nextCursor *string
	if next != nil {
		if next.ID == uuid.Nil() || next.At.IsZero() {
			if apiErr := h.storeFailure(r.Context(), "list snapshots cursor",
				errors.New("store returned an empty next cursor")); apiErr != nil {
				apierr.WriteError(w, r, apiErr)
			}
			return
		}
		cursor := Cursor{
			Kind:      CursorKindSnapshots,
			Sort:      query.Sort,
			CreatedAt: next.At,
			ID:        next.ID.String(),
		}.Encode()
		nextCursor = &cursor
	}
	page := NewPageInfo(nextCursor, query.Limit)
	apierr.Write(w, http.StatusOK, TypeSnapshotList, NewSnapshotListResponse(snapshots, page))
}

// requireSandbox proves the sandbox exists, is the owner's and is not deleted,
// so the per-sandbox listings can answer 404.
func (h *generationHandlers) requireSandbox(ctx context.Context, owner uuid.UUID, sandboxID string) *apierr.Error {
	if _, err := h.store.Sandbox(ctx, owner, sandboxID); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, db.ErrNotFound) {
			return coreNotFound()
		}
		h.logger.ErrorContext(ctx, "api: snapshot sandbox lookup failed", "error", coreErrorName(err))
		return coreInternal()
	}
	return nil
}

// summary maps one durable generation to the public object. A row that is not
// the owner's canonical completed generation is an internal fault, never a
// repaired success.
func (h *generationHandlers) summary(ctx context.Context, owner uuid.UUID, row db.Generation) (SnapshotSummary, *apierr.Error) {
	if owner == uuid.Nil() || row.OwnerID != owner || row.ID == uuid.Nil() || row.SandboxID == "" ||
		row.Status != db.GenerationCompleted || row.CutAt == nil || row.CutAt.IsZero() {
		h.logger.ErrorContext(ctx, "api: snapshot row is not canonical")
		return SnapshotSummary{}, coreInternal()
	}
	uploaded, ok := snapshotSize(row.BytesUploaded)
	if !ok {
		return SnapshotSummary{}, coreInternal()
	}
	content, ok := snapshotSize(row.ContentBytes)
	if !ok {
		return SnapshotSummary{}, coreInternal()
	}
	kind := SnapshotKind(row.Kind())
	switch kind {
	case SnapshotBase, SnapshotIncremental:
	default:
		h.logger.ErrorContext(ctx, "api: snapshot row has an unknown kind")
		return SnapshotSummary{}, coreInternal()
	}
	generation := int64(row.Number)
	var chainID *string
	if row.ChainID != uuid.Nil() {
		chainID = new(row.ChainID.String())
	}
	out := SnapshotSummary{
		ID:               row.ID.String(),
		SandboxID:        row.SandboxID,
		Status:           SnapshotCompleted,
		Kind:             &kind,
		Generation:       &generation,
		ChainID:          chainID,
		CreatedAt:        row.CutAt.UTC(),
		SizeBytes:        &uploaded,
		ContentSizeBytes: &content,
		Origin:           SnapshotCaptured,
	}
	if row.CompletedAt != nil && !row.CompletedAt.IsZero() {
		out.CompletedAt = new(row.CompletedAt.UTC())
	}
	return out, nil
}

// storeFailure maps owner-scoped read faults. Missing rows are the 404; every
// other fault is the safe 500.
func (h *generationHandlers) storeFailure(ctx context.Context, operation string, err error) *apierr.Error {
	if ctx.Err() != nil {
		return nil
	}
	h.logger.ErrorContext(ctx, "api: "+operation+" failed", "error", coreErrorName(err))
	if errors.Is(err, db.ErrNotFound) {
		return coreNotFound()
	}
	return coreInternal()
}

// snapshotSize converts a stored uint64 size to the nullable integer the
// schema carries. A value above MaxInt64 has no wire representation.
func snapshotSize(value uint64) (int64, bool) {
	if value > math.MaxInt64 {
		return 0, false
	}
	return int64(value), true
}

// SnapshotListQuery is the strict parse of both snapshot list endpoints. The
// pinned operations support limit, cursor and sort and nothing else.
type SnapshotListQuery struct {
	Limit  int
	Cursor *Cursor
	Sort   SortOrder
}

// ParseListSnapshotsQuery parses and validates a snapshot list query. Unknown
// parameters, repeated scalar parameters, out-of-range limits and cursors from
// another collection or sort are QueryErrors naming the parameter.
func ParseListSnapshotsQuery(values url.Values) (SnapshotListQuery, error) {
	for name := range values {
		switch name {
		case "limit", "cursor", "sort":
		default:
			return SnapshotListQuery{}, &QueryError{Param: name, Reason: "unsupported query parameter"}
		}
	}

	q := SnapshotListQuery{Limit: DefaultListLimit, Sort: SortDescending}

	if raw, ok, err := singleValue(values, "limit"); err != nil {
		return SnapshotListQuery{}, err
	} else if ok {
		limit, convErr := strconv.Atoi(raw)
		if convErr != nil || limit < MinListLimit || limit > MaxListLimit {
			return SnapshotListQuery{}, &QueryError{
				Param:  "limit",
				Reason: "must be an integer from 1 to 200",
			}
		}
		q.Limit = limit
	}

	if raw, ok, err := singleValue(values, "sort"); err != nil {
		return SnapshotListQuery{}, err
	} else if ok {
		switch SortOrder(raw) {
		case SortAscending, SortDescending:
			q.Sort = SortOrder(raw)
		default:
			return SnapshotListQuery{}, &QueryError{Param: "sort", Reason: "must be asc or desc"}
		}
	}

	if raw, ok, err := singleValue(values, "cursor"); err != nil {
		return SnapshotListQuery{}, err
	} else if ok {
		cursor, cursorErr := DecodeCursor(CursorKindSnapshots, raw)
		if cursorErr != nil {
			return SnapshotListQuery{}, &QueryError{Param: "cursor", Reason: cursorErr.Error()}
		}
		if cursor.Sort != q.Sort {
			return SnapshotListQuery{}, &QueryError{Param: "cursor", Reason: "does not match the requested sort"}
		}
		q.Cursor = &cursor
	}

	return q, nil
}
