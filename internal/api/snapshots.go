package api

// Saved-snapshot HTTP surface (/api/v1/saved-snapshots). It owns only the
// strict request parsing, finite error matrix and DTO mapping; all durable
// generation, pinning and cleanup live in internal/db. The server registers
// these routes and the create-from adapter together.

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
)

// Saved-snapshot route patterns and path parameter names.
const (
	PathSavedSnapshots = "/api/v1/saved-snapshots"
	PathSavedSnapshot  = "/api/v1/saved-snapshots/{name}"
	ParamSnapshotName  = "name"
)

// ReservedSnapshotNames are the pin's reserved handles; a save must answer
// 400 invalid_name instead of creating them.
func ReservedSnapshotNames() []string {
	return []string{"latest", "tree", "pull", "rm", "save", "current", "self", "new"}
}

// SavedSnapshotStore is the owner-scoped persistence the handlers consume.
// *db.Store implements it directly.
type SavedSnapshotStore interface {
	AcceptSnapshotSave(ctx context.Context, params db.AcceptSnapshotSaveParams) (db.SavedSnapshot, error)
	SavedSnapshotByName(ctx context.Context, owner uuid.UUID, name string) (db.SavedSnapshot, error)
	AllSavedSnapshots(ctx context.Context, query db.SnapshotListQuery) ([]db.SavedSnapshot, error)
	AcceptSnapshotDelete(ctx context.Context, params db.AcceptSnapshotDeleteParams) (db.SavedSnapshot, error)
}

var _ SavedSnapshotStore = (*db.Store)(nil)

// SavedSnapshotOptions configures the saved-snapshot handlers. Store is
// required; a nil Logger discards operational failures.
type SavedSnapshotOptions struct {
	Store  SavedSnapshotStore
	Logger *slog.Logger
}

// SavedSnapshotHandlers is the saved-snapshot route set. A nil handler leaves
// its route unregistered.
type SavedSnapshotHandlers struct {
	Save   http.HandlerFunc
	Get    http.HandlerFunc
	List   http.HandlerFunc
	Delete http.HandlerFunc
}

// NewSavedSnapshotHandlers builds the four handlers without registering them.
func NewSavedSnapshotHandlers(options SavedSnapshotOptions) (SavedSnapshotHandlers, error) {
	if options.Store == nil {
		return SavedSnapshotHandlers{}, errors.New("api: saved snapshot handlers need a store")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	h := &savedSnapshotHandlers{store: options.Store, logger: logger}
	return SavedSnapshotHandlers{
		Save:   h.save,
		Get:    h.get,
		List:   h.list,
		Delete: h.delete,
	}, nil
}

// RegisterSavedSnapshots maps the API saved-snapshot routes on mux.
func RegisterSavedSnapshots(mux *http.ServeMux, handlers SavedSnapshotHandlers) {
	handle := func(pattern string, handler http.HandlerFunc) {
		if handler != nil {
			mux.HandleFunc(pattern, handler)
		}
	}
	handle("GET "+PathSavedSnapshots, handlers.List)
	handle("POST "+PathSavedSnapshots, handlers.Save)
	handle("GET "+PathSavedSnapshot, handlers.Get)
	handle("DELETE "+PathSavedSnapshot, handlers.Delete)
}

type savedSnapshotHandlers struct {
	store  SavedSnapshotStore
	logger *slog.Logger
}

// save serves POST /api/v1/saved-snapshots. The body is strict
// {sandboxId,name}; the name and source checks produce the finite matrix:
// invalid_json, invalid_name, unauthorized, not_found, sandbox_not_ready,
// snapshot_data_expired, save_in_progress and http_500.
func (h *savedSnapshotHandlers) save(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	if apiErr := coreRequireJSONMedia(r); apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	var request SavedSnapshotSaveRequest
	if err := DecodeJSON(r, &request); err != nil {
		apierr.WriteError(w, r, coreInvalidJSON("request body is not valid JSON"))
		return
	}
	if request.SandboxID == "" {
		apierr.WriteError(w, r, coreInvalidJSON("sandboxId is required"))
		return
	}
	if apiErr := savedSnapshotNameError(request.Name); apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	row, err := h.store.AcceptSnapshotSave(r.Context(), db.AcceptSnapshotSaveParams{
		Owner: owner, SandboxID: request.SandboxID, Name: request.Name,
	})
	if err != nil {
		if apiErr := h.saveFailure(r.Context(), err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	snapshot, apiErr := h.snapshotDTO(r.Context(), owner, row)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	apierr.Write(w, http.StatusAccepted, TypeSavedSnapshotSaving,
		NewSavedSnapshotSavingResponse(snapshot))
}

// get serves GET /api/v1/saved-snapshots/{name}. There is no 400 here: a
// malformed or reserved name is simply not found.
func (h *savedSnapshotHandlers) get(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	name := r.PathValue(ParamSnapshotName)
	if name == "" {
		apierr.WriteError(w, r, coreNotFound())
		return
	}
	row, err := h.store.SavedSnapshotByName(r.Context(), owner, name)
	if err != nil {
		if apiErr := h.storageFailure(r.Context(), "get saved snapshot", err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	snapshot, apiErr := h.snapshotDTO(r.Context(), owner, row)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	apierr.Write(w, http.StatusOK, TypeSavedSnapshotInfo,
		NewSavedSnapshotInfoResponse(snapshot))
}

// list serves GET /api/v1/saved-snapshots. The pin has no pagination: filters
// are only exact name and literal namePrefix, and the store walks every
// internal DB page.
func (h *savedSnapshotHandlers) list(w http.ResponseWriter, r *http.Request) {
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
	query, err := ParseListSavedSnapshotsQuery(values)
	if err != nil {
		apierr.WriteError(w, r, queryFailure(err))
		return
	}
	// An exact empty name can never be a saved snapshot; answer empty
	// without translating it into "no filter".
	if query.Name != nil && *query.Name == "" {
		apierr.Write(w, http.StatusOK, TypeSavedSnapshotList,
			NewSavedSnapshotListResponse([]SavedSnapshot{}))
		return
	}
	dbQuery := db.SnapshotListQuery{Owner: owner}
	if query.Name != nil {
		dbQuery.Name = *query.Name
	}
	if query.NamePrefix != nil {
		dbQuery.NamePrefix = *query.NamePrefix
	}
	rows, err := h.store.AllSavedSnapshots(r.Context(), dbQuery)
	if err != nil {
		if apiErr := h.storageFailure(r.Context(), "list saved snapshots", err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	snapshots := make([]SavedSnapshot, len(rows))
	for i, row := range rows {
		snapshot, apiErr := h.snapshotDTO(r.Context(), owner, row)
		if apiErr != nil {
			apierr.WriteError(w, r, apiErr)
			return
		}
		snapshots[i] = snapshot
	}
	apierr.Write(w, http.StatusOK, TypeSavedSnapshotList,
		NewSavedSnapshotListResponse(snapshots))
}

// delete serves DELETE /api/v1/saved-snapshots/{name}. A save in progress
// answers 409 save_in_progress; missing and foreign names are 404.
func (h *savedSnapshotHandlers) delete(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	name := r.PathValue(ParamSnapshotName)
	if name == "" {
		apierr.WriteError(w, r, coreNotFound())
		return
	}
	if _, err := h.store.AcceptSnapshotDelete(r.Context(), db.AcceptSnapshotDeleteParams{
		Owner: owner, Name: name,
	}); err != nil {
		if apiErr := h.deleteFailure(r.Context(), err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	apierr.Write(w, http.StatusOK, TypeSavedSnapshotDeleted,
		NewSavedSnapshotDeletedResponse(name))
}

// saveFailure maps accepted durable save errors to the finite wire matrix.
func (h *savedSnapshotHandlers) saveFailure(ctx context.Context, err error) *apierr.Error {
	if ctx.Err() != nil {
		return nil
	}
	switch {
	case errors.Is(err, db.ErrInvalidSnapshotName), errors.Is(err, db.ErrInvalidSnapshotArtifact):
		return coreInvalidName("name must be 1-63 lowercase letters, digits or dashes and start with a letter or digit")
	case errors.Is(err, db.ErrNotFound):
		return coreNotFound()
	case errors.Is(err, db.ErrSnapshotSaveInProgress):
		return apierr.New(http.StatusConflict, apierr.CodeSaveInProgress,
			"a save under this name is already running")
	case errors.Is(err, db.ErrSnapshotSourceNotReady):
		return apierr.New(http.StatusConflict, apierr.CodeSandboxNotReady,
			"the sandbox is not ready to be saved")
	case errors.Is(err, db.ErrSnapshotsDisabled):
		return apierr.New(http.StatusConflict, apierr.CodeSnapshotsDisabled,
			"snapshots are disabled for this sandbox")
	case errors.Is(err, db.ErrSnapshotDataExpired):
		return apierr.New(http.StatusConflict, apierr.CodeSnapshotDataExpired,
			"the sandbox's stored data expired; it cannot be saved").Retryable(false)
	default:
		h.logger.ErrorContext(ctx, "api: saved snapshot save failed", "error", coreErrorName(err))
		return coreInternal()
	}
}

// deleteFailure maps accepted durable deletion errors.
func (h *savedSnapshotHandlers) deleteFailure(ctx context.Context, err error) *apierr.Error {
	if ctx.Err() != nil {
		return nil
	}
	switch {
	case errors.Is(err, db.ErrNotFound):
		return coreNotFound()
	case errors.Is(err, db.ErrSnapshotSaveInProgress):
		return apierr.New(http.StatusConflict, apierr.CodeSaveInProgress,
			"a save under this name is still running")
	default:
		h.logger.ErrorContext(ctx, "api: saved snapshot delete failed", "error", coreErrorName(err))
		return coreInternal()
	}
}

// storageFailure maps owner-scoped read failures. Missing and foreign rows are
// indistinguishable 404s; every other fault is the safe 500.
func (h *savedSnapshotHandlers) storageFailure(ctx context.Context, operation string, err error) *apierr.Error {
	if ctx.Err() != nil {
		return nil
	}
	h.logger.ErrorContext(ctx, "api: "+operation+" failed", "error", coreErrorName(err))
	if errors.Is(err, db.ErrNotFound) {
		return coreNotFound()
	}
	return coreInternal()
}

// savedSnapshotNameError validates the POST-only name rules. GET and DELETE
// deliberately do not call it, because the pin gives them no 400.
func savedSnapshotNameError(name string) *apierr.Error {
	if !db.ValidSnapshotName(name) {
		return coreInvalidName("name must be 1-63 lowercase letters, digits or dashes and start with a letter or digit")
	}
	if slices.Contains(ReservedSnapshotNames(), name) {
		return coreInvalidName("name is reserved")
	}
	return nil
}

// snapshotDTO validates one durable generation before mapping it to the pinned
// object. A non-canonical row (foreign owner, bad name/status/artifact/time)
// is an internal fault, never a repaired success.
func (h *savedSnapshotHandlers) snapshotDTO(ctx context.Context, owner uuid.UUID, row db.SavedSnapshot) (SavedSnapshot, *apierr.Error) {
	if apiErr := validateSavedSnapshotRow(owner, row); apiErr != nil {
		h.logger.ErrorContext(ctx, "api: saved snapshot row is not canonical")
		return SavedSnapshot{}, apiErr
	}
	out := SavedSnapshot{
		Name:            row.Name,
		Status:          SavedSnapshotStatus(row.Status),
		SourceSandboxID: row.SandboxID,
		Type:            row.MachineType,
		SizeBytes:       row.SizeBytes,
		CreatedAt:       row.CreatedAt.UTC(),
	}
	if row.Status == db.SnapshotFailed && row.ErrorReason != "" {
		out.Error = row.ErrorReason
	}
	if row.Status == db.SnapshotReady {
		out.SnapshotID = row.ArtifactID
	}
	return out, nil
}

// validateSavedSnapshotRow proves the row is the owner's canonical generation.
// It never substitutes a value the database did not store.
func validateSavedSnapshotRow(owner uuid.UUID, row db.SavedSnapshot) *apierr.Error {
	if owner == uuid.Nil() || row.OwnerID != owner || !db.ValidSnapshotName(row.Name) ||
		row.SandboxID == "" || row.CreatedAt.IsZero() {
		return coreInternal()
	}
	switch row.Status {
	case db.SnapshotSaving, db.SnapshotReady, db.SnapshotFailed:
	default:
		return coreInternal()
	}
	if !db.ValidSnapshotArtifactID(row.ArtifactID) {
		return coreInternal()
	}
	if row.MachineType != "" {
		if _, ok := machinetype.Lookup(row.MachineType); !ok {
			return coreInternal()
		}
	}
	if row.SizeBytes != nil && *row.SizeBytes < 0 {
		return coreInternal()
	}
	return nil
}
