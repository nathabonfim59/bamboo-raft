package cli

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/version"
)

// maxResponseBytes bounds a JSON response. Core commands never need more;
// streaming responses (exec, uploads) use Do and read their own body.
const maxResponseBytes = 4 << 20

// defaultRequestTimeout keeps a hung server from blocking a command forever.
const defaultRequestTimeout = 60 * time.Second

// maxRedirects matches net/http's default.
const maxRedirects = 10

// APIClient is the typed v1 API surface used by the CLI commands. Do and
// NewRequest are the extension point for command packages that need raw
// request or streaming control (exec, run, push): they still get the base
// URL, credentials and redirect policy from this client.
type APIClient interface {
	Me(ctx context.Context) (api.MeResponse, error)
	CreateSandbox(ctx context.Context, req api.CreateSandboxRequest) (api.CreateSandboxResponse, error)
	GetSandbox(ctx context.Context, id string) (api.SandboxInfoResponse, error)
	ListSandboxes(ctx context.Context, query url.Values) (api.SandboxListResponse, error)
	DeleteSandbox(ctx context.Context, id string) (api.DeletionOperationResponse, error)
	GetDeletionOperation(ctx context.Context, id string) (api.DeletionOperationResponse, error)
	SaveSnapshot(ctx context.Context, req api.SavedSnapshotSaveRequest) (api.SavedSnapshotSavingResponse, error)
	GetSavedSnapshot(ctx context.Context, name string) (api.SavedSnapshotInfoResponse, error)
	DeleteSavedSnapshot(ctx context.Context, name string) (api.SavedSnapshotDeletedResponse, error)
	ListSnapshots(ctx context.Context, sandboxID string, query url.Values) (api.SnapshotListResponse, error)
	GetLatestSandboxSnapshot(ctx context.Context, sandboxID string) (api.SnapshotLatestResponse, error)

	StopSandbox(ctx context.Context, id string, force bool) (api.SandboxActionResponse, error)
	GetSandboxStop(ctx context.Context, sandboxID, stopID string) (api.StopOperationResponse, error)
	ResumeSandbox(ctx context.Context, id string, req api.ResumeSandboxRequest) (api.SandboxActionResponse, error)
	ForkSandbox(ctx context.Context, id string, req api.ForkSandboxRequest) (api.SandboxActionResponse, error)
	RecoverSandbox(ctx context.Context, id string) (api.RecoverResponse, error)
	UpdateSandbox(ctx context.Context, id string, req api.UpdateSandboxRequest) (api.SandboxInfoResponse, error)
	CreateSnapshotShare(ctx context.Context, name string) (api.SnapshotShareCreatedResponse, error)
	ListSnapshotShares(ctx context.Context, name string) (api.SnapshotShareListResponse, error)
	RevokeSnapshotShare(ctx context.Context, name, shareID string) (api.SnapshotShareRevokedResponse, error)

	// NewRequest builds an authenticated request against path (which must
	// start with "/") with the client's base URL. The caller owns the body.
	NewRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error)
	// Do sends a request built by NewRequest. The caller closes the response
	// body and is responsible for bounding it.
	Do(req *http.Request) (*http.Response, error)
}

// Client is the HTTP implementation of APIClient.
type Client struct {
	base   *url.URL
	apiKey string
	http   *http.Client
	// jsonTimeout bounds JSON requests when the injected client has no
	// timeout. Raw Do never applies it, so streaming commands can live for the
	// configured 7200-second stream limit.
	jsonTimeout time.Duration
}

// APIError is an API error envelope. Message is the server's safe message;
// the raw response body is never retained.
type APIError struct {
	Status    int
	Code      string
	Message   string
	RequestID string
	// Envelope reports that the response carried a validated API error
	// envelope, not just an HTTP status. A create refusal with a valid
	// envelope is definite even for a 5xx status.
	Envelope bool
	// RetryAfterSeconds is the positive integer error.details hint, zero
	// when absent or invalid. It and Retryable are the only details retained.
	RetryAfterSeconds int
	// Retryable is the boolean error.details.retryable hint, nil when the
	// member is absent or not a JSON boolean. Commands must treat nil and
	// false as terminal.
	Retryable *bool
}

// Error implements error.
func (e *APIError) Error() string {
	if e.RetryAfterSeconds > 0 {
		return fmt.Sprintf("%d %s: %s (retry after %d seconds)", e.Status, e.Code, e.Message, e.RetryAfterSeconds)
	}
	return fmt.Sprintf("%d %s: %s", e.Status, e.Code, e.Message)
}

// NewClient builds a client for baseURL. The HTTP client is cloned so the
// caller's transport and timeout are kept; redirects may only stay on the
// configured origin, so credentials never follow a cross-origin redirect.
// JSON requests get a 60-second bound through their context when the injected
// client has no timeout; raw Do is unbounded so streaming commands can run for
// their own configured lifetime.
func NewClient(baseURL, apiKey string, httpClient *http.Client) (*Client, error) {
	if err := ValidateAPIURL(baseURL); err != nil {
		return nil, err
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, errors.New("invalid api url")
	}
	base.Path = strings.TrimRight(base.Path, "/")
	base.RawPath = ""
	jsonTimeout := defaultRequestTimeout
	if httpClient == nil {
		httpClient = &http.Client{}
	} else {
		clone := *httpClient
		httpClient = &clone
		if httpClient.Timeout != 0 {
			jsonTimeout = 0
		}
	}
	previous := httpClient.CheckRedirect
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxRedirects {
			return errors.New("stopped after too many redirects")
		}
		if req.URL.Scheme != base.Scheme || req.URL.Host != base.Host {
			// Never echo the target: a redirect can carry an upload token in
			// its query.
			return errors.New("refusing redirect to another origin")
		}
		if previous != nil {
			return previous(req, via)
		}
		return nil
	}
	return &Client{base: base, apiKey: apiKey, http: httpClient, jsonTimeout: jsonTimeout}, nil
}

// requestError is a transport failure with a safe message. It keeps the
// cancellation classification but never the URL or query, which can carry an
// upload token.
type requestError struct {
	method string
	kind   error
}

// errRequestFailed is the generic transport failure cause.
var errRequestFailed = errors.New("request failed")

// Error implements error.
func (e *requestError) Error() string {
	switch {
	case errors.Is(e.kind, context.Canceled):
		return e.method + ": request cancelled"
	case errors.Is(e.kind, context.DeadlineExceeded):
		return e.method + ": request timed out"
	default:
		return e.method + ": request failed"
	}
}

// Unwrap preserves errors.Is(err, context.Canceled) and DeadlineExceeded.
func (e *requestError) Unwrap() error { return e.kind }

// NewRequest implements APIClient.
func (c *Client) NewRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	if !strings.HasPrefix(path, "/") {
		return nil, errors.New("api path must start with /")
	}
	raw := strings.TrimRight(c.base.String(), "/") + path
	target, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("build api url failed")
	}
	req, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		// The error can quote the full URL, including an upload token.
		return nil, errors.New("build api request failed")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "bbraft/"+version.Version)
	return req, nil
}

// Do implements APIClient. Transport errors are reduced to a safe message that
// keeps context cancellation and timeout classification; the URL and query are
// never included because an upload URL carries a token.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		kind := errRequestFailed
		if errors.Is(err, context.Canceled) {
			kind = context.Canceled
		} else if errors.Is(err, context.DeadlineExceeded) {
			kind = context.DeadlineExceeded
		}
		return nil, &requestError{method: req.Method, kind: kind}
	}
	return resp, nil
}

// do performs one JSON request. body may be nil. out must be non-nil with
// wantTypes set to the pinned success discriminators the operation may return;
// a 2xx answer whose ok or type is outside that exact set is rejected so an
// empty or foreign object cannot be mistaken for success.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, header http.Header, body, out any, wantTypes ...string) error {
	if c.jsonTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.jsonTimeout)
		defer cancel()
	}
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("%s %s: encode request", method, path)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := c.NewRequest(ctx, method, path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if query != nil {
		req.URL.RawQuery = query.Encode()
	}
	for key, values := range header {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	resp, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: request failed: %w", method, path, err)
	}
	defer resp.Body.Close()

	raw, err := readBounded(resp.Body)
	if err != nil {
		return fmt.Errorf("%s %s: read response: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return decodeAPIError(resp.StatusCode, raw)
	}
	if out == nil {
		return nil
	}
	var head struct {
		OK   bool   `json:"ok"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return fmt.Errorf("%s %s: invalid response", method, path)
	}
	if !head.OK || !slices.Contains(wantTypes, head.Type) {
		return &CLIError{
			Code:    "invalid_response",
			Message: fmt.Sprintf("%s %s: unexpected response envelope", method, path),
		}
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("%s %s: invalid response", method, path)
	}
	return nil
}

// readBounded reads at most maxResponseBytes and refuses a larger body.
func readBounded(r io.Reader) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(r, maxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxResponseBytes {
		return nil, fmt.Errorf("response exceeds %d bytes", maxResponseBytes)
	}
	return raw, nil
}

// apiErrorEnvelope mirrors the pinned error envelope without keeping the raw
// body or its details. RequestID is a pointer so a missing or null member is
// distinguishable from the empty string the pin allows.
type apiErrorEnvelope struct {
	OK      *bool  `json:"ok"`
	Type    string `json:"type"`
	Status  int    `json:"status"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Error   struct {
		Status  int                       `json:"status"`
		Code    string                    `json:"code"`
		Message string                    `json:"message"`
		Details map[string]jsontext.Value `json:"details"`
	} `json:"error"`
	RequestID *string `json:"requestId"`
}

// decodeAPIError turns a non-2xx body into an APIError. A body that is not the
// pinned envelope becomes a generic status error; its content is discarded.
// The status is always the actual HTTP status, even when the body disagrees.
//
// A valid envelope is definite evidence of a refusal: ok is false, type is
// sandbox.error, the top-level and nested statuses match the HTTP status, the
// top-level and nested codes agree, both messages are present and requestId is
// a string. Only a positive integer retryAfterSeconds and a boolean retryable
// detail are retained; all other details and the raw body are dropped.
func decodeAPIError(status int, raw []byte) error {
	if apiErr, ok := decodeErrorEnvelope(status, raw); ok {
		return apiErr
	}
	return &APIError{Status: status, Code: fmt.Sprintf("http_%d", status), Message: http.StatusText(status)}
}

// decodeErrorEnvelope validates one API error envelope. It reports false for
// any body that does not carry the complete pinned identity, including a
// mismatched status or code.
func decodeErrorEnvelope(status int, raw []byte) (*APIError, bool) {
	var env apiErrorEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, false
	}
	if env.OK == nil || *env.OK || env.Type != "sandbox.error" || env.RequestID == nil ||
		env.Status != status || env.Error.Status != status ||
		env.Code == "" || env.Message == "" || env.Error.Message == "" ||
		env.Error.Code != env.Code {
		return nil, false
	}
	return &APIError{
		Status:            status,
		Code:              env.Code,
		Message:           env.Message,
		RequestID:         *env.RequestID,
		Envelope:          true,
		RetryAfterSeconds: retryAfterSeconds(env.Error.Details),
		Retryable:         retryableDetail(env.Error.Details),
	}, true
}

// retryAfterSeconds returns the positive integer hint from details, or zero
// when it is absent, a non-integer, non-positive, fractional or overflowing.
func retryAfterSeconds(details map[string]jsontext.Value) int {
	raw, ok := details["retryAfterSeconds"]
	if !ok {
		return 0
	}
	seconds, err := strconv.Atoi(string(bytes.TrimSpace(raw)))
	if err != nil || seconds <= 0 {
		return 0
	}
	return seconds
}

// retryableDetail returns the boolean error.details.retryable hint. An absent,
// null, string, number or other non-boolean value yields nil, which callers
// treat as not retryable.
func retryableDetail(details map[string]jsontext.Value) *bool {
	raw, ok := details["retryable"]
	if !ok {
		return nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil
	}
	retryable, ok := value.(bool)
	if !ok {
		return nil
	}
	return new(retryable)
}

// Me implements APIClient.
func (c *Client) Me(ctx context.Context) (api.MeResponse, error) {
	var out api.MeResponse
	err := c.do(ctx, http.MethodGet, api.PathMe, nil, nil, nil, &out, api.TypeUserInfo)
	return out, err
}

// CreateSandbox implements APIClient.
func (c *Client) CreateSandbox(ctx context.Context, in api.CreateSandboxRequest) (api.CreateSandboxResponse, error) {
	var out api.CreateSandboxResponse
	err := c.do(ctx, http.MethodPost, api.PathSandboxes, nil, nil, in, &out, api.TypeSandboxCreated)
	return out, err
}

// GetSandbox implements APIClient.
func (c *Client) GetSandbox(ctx context.Context, id string) (api.SandboxInfoResponse, error) {
	var out api.SandboxInfoResponse
	path := "/api/v1/sandboxes/" + url.PathEscape(id)
	err := c.do(ctx, http.MethodGet, path, nil, nil, nil, &out, api.TypeSandboxInfo)
	return out, err
}

// ListSandboxes implements APIClient.
func (c *Client) ListSandboxes(ctx context.Context, query url.Values) (api.SandboxListResponse, error) {
	var out api.SandboxListResponse
	err := c.do(ctx, http.MethodGet, api.PathSandboxes, query, nil, nil, &out, api.TypeSandboxList)
	return out, err
}

// DeleteSandbox implements APIClient. The confirmation header is mandatory in
// the pinned API and always carries the exact sandbox id.
func (c *Client) DeleteSandbox(ctx context.Context, id string) (api.DeletionOperationResponse, error) {
	var out api.DeletionOperationResponse
	header := http.Header{}
	header.Set("X-Ascii-Confirm-Delete", id)
	path := "/api/v1/sandboxes/" + url.PathEscape(id)
	err := c.do(ctx, http.MethodDelete, path, nil, header, nil, &out, api.TypeSandboxDeleting)
	return out, err
}

// GetDeletionOperation implements APIClient.
func (c *Client) GetDeletionOperation(ctx context.Context, id string) (api.DeletionOperationResponse, error) {
	var out api.DeletionOperationResponse
	path := "/api/v1/deletion-operations/" + url.PathEscape(id)
	err := c.do(ctx, http.MethodGet, path, nil, nil, nil, &out, api.TypeDeletionOperation)
	return out, err
}

// SaveSnapshot implements APIClient.
func (c *Client) SaveSnapshot(ctx context.Context, in api.SavedSnapshotSaveRequest) (api.SavedSnapshotSavingResponse, error) {
	var out api.SavedSnapshotSavingResponse
	err := c.do(ctx, http.MethodPost, "/api/v1/saved-snapshots", nil, nil, in, &out, api.TypeSavedSnapshotSaving)
	return out, err
}

// GetSavedSnapshot implements APIClient.
func (c *Client) GetSavedSnapshot(ctx context.Context, name string) (api.SavedSnapshotInfoResponse, error) {
	var out api.SavedSnapshotInfoResponse
	path := "/api/v1/saved-snapshots/" + url.PathEscape(name)
	err := c.do(ctx, http.MethodGet, path, nil, nil, nil, &out, api.TypeSavedSnapshotInfo)
	return out, err
}

// DeleteSavedSnapshot implements APIClient.
func (c *Client) DeleteSavedSnapshot(ctx context.Context, name string) (api.SavedSnapshotDeletedResponse, error) {
	var out api.SavedSnapshotDeletedResponse
	path := "/api/v1/saved-snapshots/" + url.PathEscape(name)
	err := c.do(ctx, http.MethodDelete, path, nil, nil, nil, &out, api.TypeSavedSnapshotDeleted)
	return out, err
}

// ListSnapshots implements APIClient. An empty sandboxID lists the whole
// account.
func (c *Client) ListSnapshots(ctx context.Context, sandboxID string, query url.Values) (api.SnapshotListResponse, error) {
	var out api.SnapshotListResponse
	path := api.PathSnapshots
	if sandboxID != "" {
		path = "/api/v1/sandboxes/" + url.PathEscape(sandboxID) + "/snapshots"
	}
	err := c.do(ctx, http.MethodGet, path, query, nil, nil, &out, api.TypeSnapshotList)
	return out, err
}

// GetLatestSandboxSnapshot implements APIClient.
func (c *Client) GetLatestSandboxSnapshot(ctx context.Context, sandboxID string) (api.SnapshotLatestResponse, error) {
	var out api.SnapshotLatestResponse
	path := "/api/v1/sandboxes/" + url.PathEscape(sandboxID) + "/snapshots/latest"
	err := c.do(ctx, http.MethodGet, path, nil, nil, nil, &out, api.TypeSnapshotLatest)
	return out, err
}

// ForkSandbox implements APIClient.
func (c *Client) ForkSandbox(ctx context.Context, id string, req api.ForkSandboxRequest) (api.SandboxActionResponse, error) {
	var out api.SandboxActionResponse
	path := "/api/v1/sandboxes/" + url.PathEscape(id) + "/fork"
	err := c.do(ctx, http.MethodPost, path, nil, nil, req, &out, api.TypeSandboxForking)
	return out, err
}

// RecoverSandbox implements APIClient.
func (c *Client) RecoverSandbox(ctx context.Context, id string) (api.RecoverResponse, error) {
	var out api.RecoverResponse
	path := "/api/v1/sandboxes/" + url.PathEscape(id) + "/recover"
	err := c.do(ctx, http.MethodPost, path, nil, nil, nil, &out, api.TypeSandboxRecovering)
	return out, err
}

// CreateSnapshotShare implements APIClient.
func (c *Client) CreateSnapshotShare(ctx context.Context, name string) (api.SnapshotShareCreatedResponse, error) {
	var out api.SnapshotShareCreatedResponse
	path := "/api/v1/saved-snapshots/" + url.PathEscape(name) + "/shares"
	err := c.do(ctx, http.MethodPost, path, nil, nil, nil, &out, api.TypeSnapshotShareCreated)
	return out, err
}

// ListSnapshotShares implements APIClient.
func (c *Client) ListSnapshotShares(ctx context.Context, name string) (api.SnapshotShareListResponse, error) {
	var out api.SnapshotShareListResponse
	path := "/api/v1/saved-snapshots/" + url.PathEscape(name) + "/shares"
	err := c.do(ctx, http.MethodGet, path, nil, nil, nil, &out, api.TypeSnapshotShareList)
	return out, err
}

// RevokeSnapshotShare implements APIClient.
func (c *Client) RevokeSnapshotShare(ctx context.Context, name, shareID string) (api.SnapshotShareRevokedResponse, error) {
	var out api.SnapshotShareRevokedResponse
	path := "/api/v1/saved-snapshots/" + url.PathEscape(name) + "/shares/" + url.PathEscape(shareID)
	err := c.do(ctx, http.MethodDelete, path, nil, nil, nil, &out, api.TypeSnapshotShareRevoked)
	return out, err
}
