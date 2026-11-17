package api

import "time"

// SavedSnapshotStatus is the state of a saved snapshot.
type SavedSnapshotStatus string

// Saved snapshot statuses.
const (
	SavedSnapshotSaving SavedSnapshotStatus = "saving"
	SavedSnapshotReady  SavedSnapshotStatus = "ready"
	SavedSnapshotFailed SavedSnapshotStatus = "failed"
)

// SavedSnapshot is the public saved-snapshot object. SnapshotID is the
// server-chosen immutable artifact behind the name; it is separate from the
// user-facing name.
type SavedSnapshot struct {
	Name            string              `json:"name"`
	Status          SavedSnapshotStatus `json:"status"`
	Error           string              `json:"error,omitzero"`
	SourceSandboxID string              `json:"sourceSandboxId"`
	SnapshotID      string              `json:"snapshotId,omitzero"`
	Type            string              `json:"type,omitzero"`
	SizeBytes       *int64              `json:"sizeBytes,omitzero"`
	CreatedAt       time.Time           `json:"createdAt"`
}

// SavedSnapshotListResponse is the GET /api/v1/saved-snapshots envelope.
type SavedSnapshotListResponse struct {
	SuccessBase
	Snapshots []SavedSnapshot         `json:"snapshots"`
	Allowance *SavedSnapshotAllowance `json:"allowance,omitzero"`
}

// NewSavedSnapshotListResponse builds the saved-snapshot list envelope. A nil
// slice becomes an empty array because the pinned schema requires an array.
func NewSavedSnapshotListResponse(snapshots []SavedSnapshot) SavedSnapshotListResponse {
	if snapshots == nil {
		snapshots = []SavedSnapshot{}
	}
	return SavedSnapshotListResponse{SuccessBase: success(TypeSavedSnapshotList), Snapshots: snapshots}
}

// SavedSnapshotAllowance describes the snapshot allowance of the request's
// wallet. Billing is outside the Phase 0.5 subset; the pinned list shape still
// carries it.
type SavedSnapshotAllowance struct {
	Org                         *string  `json:"org,omitzero"`
	OrgName                     *string  `json:"orgName,omitzero"`
	Included                    *int     `json:"included,omitzero"`
	Free                        *int     `json:"free,omitzero"`
	Used                        *int     `json:"used,omitzero"`
	Extra                       *int     `json:"extra,omitzero"`
	ExtraMonthlyDollars         *float64 `json:"extraMonthlyDollars,omitzero"`
	ExtraDailyDollars           *float64 `json:"extraDailyDollars,omitzero"`
	PricePerExtraMonthlyDollars *float64 `json:"pricePerExtraMonthlyDollars,omitzero"`
	PricePerExtraDailyDollars   *float64 `json:"pricePerExtraDailyDollars,omitzero"`
}

// SavedSnapshotInfoResponse is the GET /api/v1/saved-snapshots/{name}
// envelope.
type SavedSnapshotInfoResponse struct {
	SuccessBase
	Snapshot SavedSnapshot `json:"snapshot"`
}

// NewSavedSnapshotInfoResponse builds a saved-snapshot info envelope.
func NewSavedSnapshotInfoResponse(s SavedSnapshot) SavedSnapshotInfoResponse {
	return SavedSnapshotInfoResponse{SuccessBase: success(TypeSavedSnapshotInfo), Snapshot: s}
}

// SavedSnapshotSavingResponse is the POST /api/v1/saved-snapshots envelope.
type SavedSnapshotSavingResponse struct {
	SuccessBase
	Status   SavedSnapshotStatus `json:"status"`
	Snapshot SavedSnapshot       `json:"snapshot"`
}

// NewSavedSnapshotSavingResponse builds a save-accepted envelope. The status is
// always saving; a ready snapshot is read through the info endpoint.
func NewSavedSnapshotSavingResponse(s SavedSnapshot) SavedSnapshotSavingResponse {
	return SavedSnapshotSavingResponse{
		SuccessBase: success(TypeSavedSnapshotSaving),
		Status:      SavedSnapshotSaving,
		Snapshot:    s,
	}
}

// SavedSnapshotDeletionStatus is the deleted marker of a removal response.
type SavedSnapshotDeletionStatus string

// SavedSnapshotDeleted is the only deletion status.
const SavedSnapshotDeleted SavedSnapshotDeletionStatus = "deleted"

// SavedSnapshotDeletedResponse is the DELETE /api/v1/saved-snapshots/{name}
// envelope.
type SavedSnapshotDeletedResponse struct {
	SuccessBase
	Name   string                      `json:"name"`
	Status SavedSnapshotDeletionStatus `json:"status"`
}

// NewSavedSnapshotDeletedResponse builds a removal envelope.
func NewSavedSnapshotDeletedResponse(name string) SavedSnapshotDeletedResponse {
	return SavedSnapshotDeletedResponse{
		SuccessBase: success(TypeSavedSnapshotDeleted),
		Name:        name,
		Status:      SavedSnapshotDeleted,
	}
}

// FileMethod is the HTTP method a file URL expects.
type FileMethod string

// File URL methods.
const (
	FileMethodGet FileMethod = "GET"
	FileMethodPut FileMethod = "PUT"
)

// FileURLResponse is the POST /api/v1/sandboxes/{sandboxId}/files/upload-url
// envelope. The URL is secret-bearing until it expires.
type FileURLResponse struct {
	SuccessBase
	URL       string     `json:"url"`
	Method    FileMethod `json:"method"`
	Path      string     `json:"path"`
	ExpiresAt time.Time  `json:"expiresAt"`
}

// NewFileUploadURLResponse builds an upload-URL envelope.
func NewFileUploadURLResponse(rawURL, path string, expiresAt time.Time) FileURLResponse {
	return FileURLResponse{
		SuccessBase: success(TypeFileUploadURL),
		URL:         rawURL,
		Method:      FileMethodPut,
		Path:        path,
		ExpiresAt:   expiresAt,
	}
}
