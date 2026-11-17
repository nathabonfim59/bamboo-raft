// Package apierr holds the v1 response envelopes, the error
// code table and the request id middleware.
//
// Every /api/v1 answer is one JSON envelope. Success is
// {"ok":true,"type":<discriminator>,...}. An error is
//
//	{"ok":false,"type":"sandbox.error","status":<n>,"code":<code>,
//	 "message":<text>,"error":{"code","message","status","details"?},
//	 "requestId":"req_..."}
//
// exactly as the pinned spec's ErrorEnvelope requires.
package apierr

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
)

// The API error codes. Codes are
// strings on the wire; the constants keep the table in one place.
const (
	CodeInvalidJSON             = "invalid_json"
	CodeInvalidTimeout          = "invalid_timeout"
	CodeInvalidPath             = "invalid_path"
	CodeInvalidEnv              = "invalid_env"
	CodeInvalidQuery            = "invalid_query"
	CodeInvalidName             = "invalid_name"
	CodeUnknownEnvironment      = "unknown_environment"
	CodeMachineNotRunning       = "machine_not_running"
	CodeUnauthorized            = "unauthorized"
	CodeForbidden               = "forbidden"
	CodeOrgSuspended            = "org_suspended"
	CodeNotFound                = "not_found"
	CodeSnapshotNotFound        = "snapshot_not_found"
	CodeSandboxStarting         = "sandbox_starting"
	CodeSandboxNotReady         = "sandbox_not_ready"
	CodeSandboxResuming         = "sandbox_resuming"
	CodeSandboxStopping         = "sandbox_stopping"
	CodeSandboxRunningStopFirst = "sandbox_running_stop_first"
	CodeSnapshotsDisabled       = "snapshots_disabled"
	CodeSnapshotDataExpired     = "snapshot_data_expired"
	CodeSnapshotCompacted       = "snapshot_compacted"
	CodeIdempotencyInProgress   = "idempotency_in_progress"
	CodeIdempotencyKeyReused    = "idempotency_key_reused"
	CodeFileTooLarge            = "file_too_large"
	CodeRateLimited             = "rate_limited"
	CodeLimitReached            = "limit_reached"
	CodeIPRateLimitExceeded     = "IP_RATE_LIMIT_EXCEEDED"
	CodeSandboxDirectFailed     = "sandbox_direct_failed"
	CodeNoReadyMachine          = "no_ready_machine"
	CodeOutOfCapacity           = "out_of_capacity"
	CodeSSHHostKeyUnavailable   = "ssh_host_key_unavailable"
	CodeInternal                = "http_500"
	// CodeConflict is the generic 409 fallback for ALREADY_EXISTS and
	// defensive conflicts.
	CodeConflict = "conflict"
	// CodeSaveInProgress reports a concurrent save of the same snapshot
	// name; the snapshot worker uses it.
	CodeSaveInProgress = "save_in_progress"
)

// Error is a v1 API error. The wire form repeats Code, Message and Status
// inside the nested error object; Details lands only in error.details.
type Error struct {
	Status  int
	Code    string
	Message string
	Details map[string]any
}

// New returns an Error without details.
func New(status int, code, message string) *Error {
	return &Error{Status: status, Code: code, Message: message}
}

// Error returns "status code: message"; it never carries secret material.
func (e *Error) Error() string {
	return fmt.Sprintf("%d %s: %s", e.Status, e.Code, e.Message)
}

// WithDetail sets one error.details entry and returns e for chaining.
func (e *Error) WithDetail(key string, v any) *Error {
	if e.Details == nil {
		e.Details = map[string]any{}
	}
	e.Details[key] = v
	return e
}

// Retryable sets details.retryable, the flag clients read on 409/502
// machine errors (docs error model).
func (e *Error) Retryable(ok bool) *Error { return e.WithDetail("retryable", ok) }

// RetryAfterSeconds sets details.retryAfterSeconds, the hint the API sends on
// some 503 answers such as ssh_host_key_unavailable.
func (e *Error) RetryAfterSeconds(n int) *Error {
	return e.WithDetail("retryAfterSeconds", n)
}

// errorEnvelope mirrors the pinned spec's ErrorEnvelope field order.
type errorEnvelope struct {
	OK        bool      `json:"ok"`
	Type      string    `json:"type"`
	Status    int       `json:"status"`
	Code      string    `json:"code"`
	Message   string    `json:"message"`
	Error     errorBody `json:"error"`
	RequestID string    `json:"requestId"`
}

// errorBody is the nested error object; details is optional per the spec.
type errorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Status  int            `json:"status"`
	Details map[string]any `json:"details,omitempty"`
}

// Write sends a success envelope: {"ok":true,"type":typ} merged with the
// JSON object v marshals to. Members keep their raw encoding, so integers
// past 2^53 stay exact. v may be nil. A marshal failure is a programmer
// error; it answers 500 http_500 instead, keeping the request id the
// RequestID middleware already sent in the X-Request-Id header.
func Write(w http.ResponseWriter, status int, typ string, v any) {
	reqID := w.Header().Get("X-Request-Id")
	body := map[string]jsontext.Value{}
	if v != nil {
		raw, err := json.Marshal(v)
		if err != nil {
			writeError(w, reqID, fmt.Errorf("apierr: marshal %s response: %w", typ, err))
			return
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			writeError(w, reqID, fmt.Errorf("apierr: %s response is not a JSON object", typ))
			return
		}
		if body == nil { // a null payload
			body = map[string]jsontext.Value{}
		}
	}
	// The envelope wins over any payload member named ok or type.
	body["ok"], _ = json.Marshal(true)
	body["type"], _ = json.Marshal(typ)
	raw, err := json.Marshal(body)
	if err != nil {
		writeFallback(w, reqID)
		return
	}
	writeRaw(w, status, raw)
}

// WriteError sends the error envelope for err. An *Error keeps its status,
// code, message and details; any other error becomes 500 http_500 with a
// safe message — the original error is the caller's to log, it never
// reaches the wire. The request id comes from r's context (RequestID
// middleware), or from the X-Request-Id header the middleware already set;
// a fresh one is generated when neither exists.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	var reqID string
	if r != nil {
		reqID = RequestIDFrom(r.Context())
	}
	if reqID == "" {
		reqID = w.Header().Get("X-Request-Id")
	}
	writeError(w, reqID, err)
}

// writeError sends the envelope for err under request id reqID, generating
// one when empty and mirroring it to the X-Request-Id header.
func writeError(w http.ResponseWriter, reqID string, err error) {
	e, ok := errors.AsType[*Error](err)
	if !ok {
		e = New(http.StatusInternalServerError, CodeInternal, "internal server error")
	}
	if reqID == "" {
		reqID = NewRequestID()
	}
	w.Header().Set("X-Request-Id", reqID)
	env := errorEnvelope{
		OK:      false,
		Type:    "sandbox.error",
		Status:  e.Status,
		Code:    e.Code,
		Message: e.Message,
		Error: errorBody{
			Code:    e.Code,
			Message: e.Message,
			Status:  e.Status,
			Details: e.Details,
		},
		RequestID: reqID,
	}
	raw, err := json.Marshal(env)
	if err != nil {
		writeFallback(w, reqID)
		return
	}
	writeRaw(w, e.Status, raw)
}

// writeFallback is the last-resort answer when even the error envelope
// cannot marshal (for example unmarshalable Details). It still produces
// the full required shape with a request id.
func writeFallback(w http.ResponseWriter, reqID string) {
	if reqID == "" {
		reqID = NewRequestID()
	}
	writeRaw(w, http.StatusInternalServerError,
		fmt.Appendf(nil, `{"ok":false,"type":"sandbox.error","status":500,"code":"http_500","message":"internal server error","error":{"code":"http_500","message":"internal server error","status":500},"requestId":%q}`, reqID))
}

func writeRaw(w http.ResponseWriter, status int, raw []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(raw)
}
