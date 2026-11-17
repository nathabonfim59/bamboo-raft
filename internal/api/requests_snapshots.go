package api

import "net/url"

// ListSavedSnapshotsQuery is the strict parse of GET /api/v1/saved-snapshots.
// The pinned endpoint supports exact name and literal namePrefix filters and
// nothing else.
type ListSavedSnapshotsQuery struct {
	Name       *string
	NamePrefix *string
}

// ParseListSavedSnapshotsQuery parses and validates the saved-snapshot list
// query. Unknown parameters and repeated scalar parameters are QueryErrors.
func ParseListSavedSnapshotsQuery(values url.Values) (ListSavedSnapshotsQuery, error) {
	for name := range values {
		switch name {
		case "name", "namePrefix":
		default:
			return ListSavedSnapshotsQuery{}, &QueryError{Param: name, Reason: "unsupported query parameter"}
		}
	}
	var q ListSavedSnapshotsQuery
	if raw, ok, err := singleValue(values, "name"); err != nil {
		return ListSavedSnapshotsQuery{}, err
	} else if ok {
		q.Name = &raw
	}
	if raw, ok, err := singleValue(values, "namePrefix"); err != nil {
		return ListSavedSnapshotsQuery{}, err
	} else if ok {
		q.NamePrefix = &raw
	}
	return q, nil
}

// FileURLRequest is the request body of POST
// /api/v1/sandboxes/{sandboxId}/files/upload-url.
type FileURLRequest struct {
	Path       string `json:"path"`
	TTLSeconds *int   `json:"ttlSeconds,omitzero"`
}

// SavedSnapshotSaveRequest is the request body of POST
// /api/v1/saved-snapshots. Names are the user-facing handles; the artifact id
// is chosen by the server.
type SavedSnapshotSaveRequest struct {
	SandboxID string `json:"sandboxId"`
	Name      string `json:"name"`
}
