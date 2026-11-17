package api

import (
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// MaxJSONBodyBytes caps a parsed JSON request body. It applies only to
// DecodeJSON callers; the raw upload endpoint never goes through this limit.
const MaxJSONBodyBytes = 1 << 20

// Sentinel request-body failures. They let handlers map a decode failure to a
// API error code without parsing the message.
var (
	ErrEmptyBody    = errors.New("request body is empty")
	ErrBodyTooLarge = errors.New("request body is too large")
	ErrInvalidJSON  = errors.New("request body is not valid JSON")
	ErrTrailingJSON = errors.New("request body must contain exactly one JSON value")
)

// RequestError reports a request body that cannot be accepted. Handlers map it
// to the API error code for the operation (usually invalid_json).
type RequestError struct {
	Field  string
	Reason string
	Err    error
}

// Error implements error.
func (e *RequestError) Error() string {
	if e.Field == "" {
		return e.Reason
	}
	return e.Field + ": " + e.Reason
}

// Unwrap exposes the sentinel failure for errors.Is.
func (e *RequestError) Unwrap() error { return e.Err }

// QueryError reports an invalid, unknown or unsupported query parameter.
// Handlers map it to 400 invalid_query and name Param.
type QueryError struct {
	Param  string
	Reason string
}

// Error implements error.
func (e *QueryError) Error() string {
	return "query parameter " + strconv.Quote(e.Param) + ": " + e.Reason
}

// DecodeJSON decodes exactly one JSON value from r.Body into dst. It rejects
// unknown object members, trailing values, empty bodies and bodies over
// MaxJSONBodyBytes. It does not enforce Content-Type; handlers decide that per
// operation.
func DecodeJSON(r *http.Request, dst any) error {
	if r.Body == nil {
		return &RequestError{Reason: ErrEmptyBody.Error(), Err: ErrEmptyBody}
	}
	body := http.MaxBytesReader(nil, r.Body, MaxJSONBodyBytes)
	dec := jsontext.NewDecoder(body)
	if err := json.UnmarshalDecode(dec, dst, json.RejectUnknownMembers(true)); err != nil {
		if errors.Is(err, io.EOF) {
			return &RequestError{Reason: ErrEmptyBody.Error(), Err: ErrEmptyBody}
		}
		if maxErr, ok := errors.AsType[*http.MaxBytesError](err); ok {
			return &RequestError{
				Reason: fmt.Sprintf("request body exceeds %d bytes", maxErr.Limit),
				Err:    ErrBodyTooLarge,
			}
		}
		return invalidJSON(err)
	}
	var extra any
	switch err := json.UnmarshalDecode(dec, &extra); {
	case err == nil:
		return &RequestError{Reason: ErrTrailingJSON.Error(), Err: ErrTrailingJSON}
	case errors.Is(err, io.EOF):
		return nil
	default:
		return &RequestError{Reason: ErrTrailingJSON.Error(), Err: ErrTrailingJSON}
	}
}

// invalidJSON returns a generic, safe error. Decoder text can echo request
// values (env vars, tokens), so only the byte offset is exposed.
func invalidJSON(err error) *RequestError {
	reason := ErrInvalidJSON.Error()
	if syntaxErr, ok := errors.AsType[*jsontext.SyntacticError](err); ok {
		reason = fmt.Sprintf("request body is not valid JSON at byte %d", syntaxErr.ByteOffset)
	} else if semanticErr, ok := errors.AsType[*json.SemanticError](err); ok {
		reason = fmt.Sprintf("request body is not valid JSON at byte %d", semanticErr.ByteOffset)
	}
	return &RequestError{Reason: reason, Err: ErrInvalidJSON}
}

// IdempotencyKeyHeader is the API header that makes POST /sandboxes safe to
// retry.
const IdempotencyKeyHeader = "Idempotency-Key"

// ReadIdempotencyKey returns the create idempotency key, or "" when absent.
// The v1 API document limits it to 255 characters.
func ReadIdempotencyKey(r *http.Request) (string, error) {
	key := r.Header.Get(IdempotencyKeyHeader)
	if key == "" {
		return "", nil
	}
	if len([]rune(key)) > 255 {
		return "", &RequestError{Field: IdempotencyKeyHeader, Reason: "must be at most 255 characters"}
	}
	return key, nil
}

// SortOrder is the cursor sort direction. The default is descending, matching
// the pinned Limit/Sort parameters.
type SortOrder string

// Sort directions.
const (
	SortAscending  SortOrder = "asc"
	SortDescending SortOrder = "desc"
)

// CursorKind names the collection space of a cursor. A cursor from one space
// never applies to another.
type CursorKind string

// Cursor spaces.
const (
	CursorKindSandboxes CursorKind = "sandboxes"
)

// Cursor is the decoded form of an opaque page cursor. Lists order by the
// (created_at, id) pair: ascending for SortAscending and descending for
// SortDescending. The pair is exclusive, so the next page starts strictly
// after it in that direction. created_at is stored at PostgreSQL microsecond
// precision; id is the stable tie-break. Owner scoping is a WHERE clause on
// the query and is never carried inside the cursor.
type Cursor struct {
	Kind      CursorKind
	Sort      SortOrder
	CreatedAt time.Time
	ID        string
}

type cursorPayload struct {
	Version   int        `json:"v"`
	Kind      CursorKind `json:"k"`
	Sort      SortOrder  `json:"s"`
	CreatedAt string     `json:"t"`
	ID        string     `json:"i"`
}

// Encode returns the opaque, canonical string form of c. It panics on an
// invalid cursor because callers build cursors from database rows, never from
// user input.
func (c Cursor) Encode() string {
	if c.ID == "" || c.CreatedAt.IsZero() || (c.Sort != SortAscending && c.Sort != SortDescending) || c.Kind == "" {
		panic("api: invalid cursor")
	}
	payload := cursorPayload{
		Version:   1,
		Kind:      c.Kind,
		Sort:      c.Sort,
		CreatedAt: c.CreatedAt.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano),
		ID:        c.ID,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		panic("api: marshal cursor: " + err.Error()) // only strings and ints
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// DecodeCursor parses the opaque cursor raw for the kind collection. It
// rejects cursors that were tampered with, use another collection or are not
// canonically encoded.
func DecodeCursor(kind CursorKind, raw string) (Cursor, error) {
	if raw == "" {
		return Cursor{}, errors.New("cursor is empty")
	}
	if len(raw) > 1024 {
		return Cursor{}, errors.New("cursor is too long")
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return Cursor{}, errors.New("cursor is not valid base64url")
	}
	var payload cursorPayload
	if err := json.Unmarshal(data, &payload, json.RejectUnknownMembers(true)); err != nil {
		return Cursor{}, errors.New("cursor is not valid")
	}
	if payload.Version != 1 {
		return Cursor{}, errors.New("cursor version is not supported")
	}
	if payload.Kind != kind {
		return Cursor{}, errors.New("cursor belongs to another collection")
	}
	if payload.Sort != SortAscending && payload.Sort != SortDescending {
		return Cursor{}, errors.New("cursor sort is not valid")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, payload.CreatedAt)
	if err != nil || createdAt.IsZero() {
		return Cursor{}, errors.New("cursor timestamp is not valid")
	}
	if payload.ID == "" || len(payload.ID) > 128 {
		return Cursor{}, errors.New("cursor id is not valid")
	}
	cursor := Cursor{Kind: payload.Kind, Sort: payload.Sort, CreatedAt: createdAt, ID: payload.ID}
	if cursor.Encode() != raw {
		return Cursor{}, errors.New("cursor is not canonical")
	}
	return cursor, nil
}

// Label is one key=value label filter.
type Label struct {
	Key   string
	Value string
}

// List query defaults and bounds from the pinned Limit parameter.
const (
	DefaultListLimit = 100
	MinListLimit     = 1
	MaxListLimit     = 200
)

var labelKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,62}$`)

// ListSandboxesQuery is the strict parse of GET /api/v1/sandboxes. The Phase
// 0.5 subset supports pagination plus the state, name, namePrefix and label
// filters; every other pinned parameter is refused as unsupported instead of
// being silently ignored.
type ListSandboxesQuery struct {
	Limit      int
	Cursor     *Cursor
	Sort       SortOrder
	States     []SandboxState
	Name       *string
	NamePrefix *string
	Labels     []Label
}

// ParseListSandboxesQuery parses and validates the list query. Unknown or
// unsupported parameters, repeated scalar parameters and invalid values are
// QueryErrors naming the parameter.
func ParseListSandboxesQuery(values url.Values) (ListSandboxesQuery, error) {
	for name := range values {
		switch name {
		case "limit", "cursor", "sort", "state", "name", "namePrefix", "label":
		default:
			return ListSandboxesQuery{}, &QueryError{Param: name, Reason: "unsupported query parameter"}
		}
	}

	q := ListSandboxesQuery{Limit: DefaultListLimit, Sort: SortDescending}

	if raw, ok, err := singleValue(values, "limit"); err != nil {
		return ListSandboxesQuery{}, err
	} else if ok {
		limit, convErr := strconv.Atoi(raw)
		if convErr != nil || limit < MinListLimit || limit > MaxListLimit {
			return ListSandboxesQuery{}, &QueryError{
				Param:  "limit",
				Reason: fmt.Sprintf("must be an integer from %d to %d", MinListLimit, MaxListLimit),
			}
		}
		q.Limit = limit
	}

	if raw, ok, err := singleValue(values, "sort"); err != nil {
		return ListSandboxesQuery{}, err
	} else if ok {
		switch SortOrder(raw) {
		case SortAscending, SortDescending:
			q.Sort = SortOrder(raw)
		default:
			return ListSandboxesQuery{}, &QueryError{Param: "sort", Reason: "must be asc or desc"}
		}
	}

	if raw, ok, err := singleValue(values, "state"); err != nil {
		return ListSandboxesQuery{}, err
	} else if ok {
		for state := range strings.SplitSeq(raw, ",") {
			if state == "" {
				return ListSandboxesQuery{}, &QueryError{Param: "state", Reason: "must be a comma-separated list of states"}
			}
			q.States = append(q.States, SandboxState(state))
		}
	}

	if raw, ok, err := singleValue(values, "name"); err != nil {
		return ListSandboxesQuery{}, err
	} else if ok {
		q.Name = &raw
	}

	if raw, ok, err := singleValue(values, "namePrefix"); err != nil {
		return ListSandboxesQuery{}, err
	} else if ok {
		q.NamePrefix = &raw
	}

	for _, label := range values["label"] {
		key, value, found := strings.Cut(label, "=")
		if !found {
			return ListSandboxesQuery{}, &QueryError{Param: "label", Reason: "must be key=value"}
		}
		if !labelKeyPattern.MatchString(key) {
			return ListSandboxesQuery{}, &QueryError{Param: "label", Reason: "has an invalid key"}
		}
		if len(value) > 256 {
			return ListSandboxesQuery{}, &QueryError{Param: "label", Reason: "value must be at most 256 bytes"}
		}
		q.Labels = append(q.Labels, Label{Key: key, Value: value})
	}

	if raw, ok, err := singleValue(values, "cursor"); err != nil {
		return ListSandboxesQuery{}, err
	} else if ok {
		cursor, cursorErr := DecodeCursor(CursorKindSandboxes, raw)
		if cursorErr != nil {
			return ListSandboxesQuery{}, &QueryError{Param: "cursor", Reason: cursorErr.Error()}
		}
		if cursor.Sort != q.Sort {
			return ListSandboxesQuery{}, &QueryError{Param: "cursor", Reason: "does not match the requested sort"}
		}
		q.Cursor = &cursor
	}

	return q, nil
}

// singleValue returns the one value of a scalar query parameter. Repeated
// parameters are refused rather than silently taking the first.
func singleValue(values url.Values, name string) (string, bool, error) {
	entries, ok := values[name]
	if !ok {
		return "", false, nil
	}
	if len(entries) != 1 {
		return "", false, &QueryError{Param: name, Reason: "must be given at most once"}
	}
	return entries[0], true, nil
}
