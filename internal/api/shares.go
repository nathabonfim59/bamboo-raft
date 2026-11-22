package api

// Saved-snapshot share-code HTTP surface
// (/api/v1/saved-snapshots/{name}/shares). The code is shown once in the
// create response; every other response carries only the stored prefix.

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// Share response types.
const (
	TypeSnapshotShareCreated = "snapshot.share.created"
	TypeSnapshotShareList    = "snapshot.share.list"
	TypeSnapshotShareRevoked = "snapshot.share.revoked"
)

// ParamShareID is the share route's path parameter name.
const ParamShareID = "shareId"

// SnapshotShareStore is the owner-scoped persistence the share handlers
// consume. *db.Store implements it.
type SnapshotShareStore interface {
	CreateSnapshotShare(ctx context.Context, owner uuid.UUID, name string) (db.SnapshotShare, string, error)
	ListSnapshotShares(ctx context.Context, owner uuid.UUID, name string) ([]db.SnapshotShare, error)
	RevokeSnapshotShare(ctx context.Context, owner uuid.UUID, name, shareID string) (db.SnapshotShare, error)
}

var _ SnapshotShareStore = (*db.Store)(nil)

// SnapshotShareOptions configures the share handlers.
type SnapshotShareOptions struct {
	Store  SnapshotShareStore
	Logger *slog.Logger
}

// SnapshotShareHandlers is the share route set.
type SnapshotShareHandlers struct {
	Create http.HandlerFunc
	List   http.HandlerFunc
	Revoke http.HandlerFunc
}

// NewSnapshotShareHandlers builds the handlers without registering them.
func NewSnapshotShareHandlers(options SnapshotShareOptions) (SnapshotShareHandlers, error) {
	if options.Store == nil {
		return SnapshotShareHandlers{}, errors.New("api: snapshot share handlers need a store")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	h := &snapshotShareHandlers{store: options.Store, logger: logger}
	return SnapshotShareHandlers{Create: h.create, List: h.list, Revoke: h.revoke}, nil
}

// RegisterSnapshotShares maps the share routes on mux.
func RegisterSnapshotShares(mux *http.ServeMux, handlers SnapshotShareHandlers) {
	handle := func(pattern string, handler http.HandlerFunc) {
		if handler != nil {
			mux.HandleFunc(pattern, handler)
		}
	}
	handle("GET "+PathSavedSnapshotShares, handlers.List)
	handle("POST "+PathSavedSnapshotShares, handlers.Create)
	handle("DELETE "+PathSavedSnapshotShare, handlers.Revoke)
}

type snapshotShareHandlers struct {
	store  SnapshotShareStore
	logger *slog.Logger
}

// create serves POST /api/v1/saved-snapshots/{name}/shares. The plaintext
// code appears exactly once, in this 201 response.
func (h *snapshotShareHandlers) create(w http.ResponseWriter, r *http.Request) {
	owner, name, apiErr := snapshotShareTarget(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	share, code, err := h.store.CreateSnapshotShare(r.Context(), owner, name)
	if err != nil {
		if apiErr := h.shareFailure(r.Context(), "create snapshot share", err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	if code == "" || !db.ValidSnapshotShareCode(code) {
		if apiErr := h.shareFailure(r.Context(), "create snapshot share", errors.New("store returned a malformed code")); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	view, apiErr := shareView(share)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	apierr.Write(w, http.StatusCreated, TypeSnapshotShareCreated,
		SnapshotShareCreatedResponse{
			SuccessBase: success(TypeSnapshotShareCreated),
			Name:        name,
			ShareCode:   code,
			Share:       view,
		})
}

// list serves GET /api/v1/saved-snapshots/{name}/shares.
func (h *snapshotShareHandlers) list(w http.ResponseWriter, r *http.Request) {
	owner, name, apiErr := snapshotShareTarget(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	rows, err := h.store.ListSnapshotShares(r.Context(), owner, name)
	if err != nil {
		if apiErr := h.shareFailure(r.Context(), "list snapshot shares", err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	shares := make([]SnapshotShare, len(rows))
	for i, row := range rows {
		view, apiErr := shareView(row)
		if apiErr != nil {
			apierr.WriteError(w, r, apiErr)
			return
		}
		shares[i] = view
	}
	apierr.Write(w, http.StatusOK, TypeSnapshotShareList,
		SnapshotShareListResponse{
			SuccessBase: success(TypeSnapshotShareList),
			Name:        name,
			Shares:      shares,
		})
}

// revoke serves DELETE /api/v1/saved-snapshots/{name}/shares/{shareId}.
func (h *snapshotShareHandlers) revoke(w http.ResponseWriter, r *http.Request) {
	owner, name, apiErr := snapshotShareTarget(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	shareID := r.PathValue(ParamShareID)
	if !db.ValidSnapshotShareID(shareID) {
		apierr.WriteError(w, r, coreNotFound())
		return
	}
	share, err := h.store.RevokeSnapshotShare(r.Context(), owner, name, shareID)
	if err != nil {
		if apiErr := h.shareFailure(r.Context(), "revoke snapshot share", err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	view, apiErr := shareView(share)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	apierr.Write(w, http.StatusOK, TypeSnapshotShareRevoked,
		SnapshotShareRevokedResponse{
			SuccessBase: success(TypeSnapshotShareRevoked),
			Name:        name,
			Share:       view,
		})
}

// snapshotShareTarget reads the authenticated owner and the path name.
func snapshotShareTarget(r *http.Request) (uuid.UUID, string, *apierr.Error) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		return uuid.Nil(), "", apiErr
	}
	name := r.PathValue(ParamSnapshotName)
	if name == "" {
		return uuid.Nil(), "", coreNotFound()
	}
	return owner, name, nil
}

// shareFailure maps durable share errors. Missing, foreign and mismatched
// names or ids are all the same 404.
func (h *snapshotShareHandlers) shareFailure(ctx context.Context, operation string, err error) *apierr.Error {
	if ctx.Err() != nil {
		return nil
	}
	switch {
	case errors.Is(err, db.ErrNotFound):
		return coreNotFound()
	case errors.Is(err, db.ErrInvalidShareCode):
		return coreNotFound()
	default:
		h.logger.ErrorContext(ctx, "api: "+operation+" failed", "error", coreErrorName(err))
		return coreInternal()
	}
}

// shareView validates one durable share row before mapping it. The digest is
// never copied to the wire.
func shareView(row db.SnapshotShare) (SnapshotShare, *apierr.Error) {
	if !db.ValidSnapshotShareID(row.ID) || row.OwnerID == uuid.Nil() ||
		!db.ValidSnapshotName(row.Name) || row.CodePrefix == "" || row.CreatedAt.IsZero() ||
		row.UseCount < 0 {
		return SnapshotShare{}, coreInternal()
	}
	switch row.Status {
	case db.SnapshotShareActive, db.SnapshotShareRevoked:
	default:
		return SnapshotShare{}, coreInternal()
	}
	view := SnapshotShare{
		ID:         row.ID,
		CodePrefix: row.CodePrefix,
		Status:     SnapshotShareStatus(row.Status),
		UseCount:   row.UseCount,
		CreatedAt:  row.CreatedAt.UTC(),
	}
	if row.LastUsedAt != nil {
		view.LastUsedAt = new(row.LastUsedAt.UTC())
	}
	if row.RevokedAt != nil {
		view.RevokedAt = new(row.RevokedAt.UTC())
	}
	return view, nil
}

// SnapshotShareStatus is the active/revoked marker.
type SnapshotShareStatus string

// Share statuses.
const (
	SnapshotShareActive  SnapshotShareStatus = "active"
	SnapshotShareRevoked SnapshotShareStatus = "revoked"
)

// SnapshotShare is the public share-code object. The code itself is never a
// member: only CodePrefix is stored and returned.
type SnapshotShare struct {
	ID         string              `json:"id"`
	CodePrefix string              `json:"codePrefix"`
	Status     SnapshotShareStatus `json:"status"`
	UseCount   int64               `json:"useCount"`
	LastUsedAt *time.Time          `json:"lastUsedAt"`
	RevokedAt  *time.Time          `json:"revokedAt"`
	CreatedAt  time.Time           `json:"createdAt"`
}

// SnapshotShareCreatedResponse is the 201 response of POST shares.
type SnapshotShareCreatedResponse struct {
	SuccessBase
	Name      string        `json:"name"`
	ShareCode string        `json:"shareCode"`
	Share     SnapshotShare `json:"share"`
}

// SnapshotShareListResponse is the GET shares response.
type SnapshotShareListResponse struct {
	SuccessBase
	Name   string          `json:"name"`
	Shares []SnapshotShare `json:"shares"`
}

// SnapshotShareRevokedResponse is the DELETE share response.
type SnapshotShareRevokedResponse struct {
	SuccessBase
	Name  string        `json:"name"`
	Share SnapshotShare `json:"share"`
}
