package cli

import (
	"context"
	"encoding/json/jsontext"
	"net/http"
	"net/url"
	"strconv"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// lifecycleSandboxPath returns the owner-scoped sandbox path used by the
// lifecycle commands. Every segment is escaped, so a caller can never inject a
// path or query.
func lifecycleSandboxPath(id string) string {
	return "/api/v1/sandboxes/" + url.PathEscape(id)
}

// lifecycleTTLJSON encodes a positive ttlSeconds member for PATCH or resume.
func lifecycleTTLJSON(seconds int) jsontext.Value {
	return jsontext.Value(strconv.Itoa(seconds))
}

// lifecycleTTLDisabled encodes the explicit null that disables auto-stop. It
// is distinct from an omitted member, which keeps the stored timer.
func lifecycleTTLDisabled() jsontext.Value {
	return jsontext.Value("null")
}

// StopSandbox implements APIClient. A force request sends the optional body;
// every other stop omits it, which the pin treats as force false. Both pinned
// stop success types are accepted, and nothing else is.
func (c *Client) StopSandbox(ctx context.Context, id string, force bool) (api.SandboxActionResponse, error) {
	var out api.SandboxActionResponse
	var body any
	if force {
		body = api.StopSandboxRequest{Force: new(true)}
	}
	err := c.do(ctx, http.MethodPost, lifecycleSandboxPath(id)+"/stop", nil, nil, body, &out,
		api.TypeSandboxStopping, api.TypeSandboxStopped)
	return out, err
}

// GetSandboxStop implements APIClient. The stop id is bound by the fixed
// owner-scoped sandbox path; a server-supplied status URL is never followed.
func (c *Client) GetSandboxStop(ctx context.Context, sandboxID, stopID string) (api.StopOperationResponse, error) {
	var out api.StopOperationResponse
	path := lifecycleSandboxPath(sandboxID) + "/stops/" + url.PathEscape(stopID)
	err := c.do(ctx, http.MethodGet, path, nil, nil, nil, &out, api.TypeStopStatus)
	return out, err
}

// ResumeSandbox implements APIClient. An empty request marshals to {}, which
// keeps every member omitted rather than sending a zero value.
func (c *Client) ResumeSandbox(ctx context.Context, id string, req api.ResumeSandboxRequest) (api.SandboxActionResponse, error) {
	var out api.SandboxActionResponse
	err := c.do(ctx, http.MethodPost, lifecycleSandboxPath(id)+"/resume", nil, nil, req, &out, api.TypeSandboxResuming)
	return out, err
}

// UpdateSandbox implements APIClient. The pinned PATCH answer is the updated
// sandbox info envelope.
func (c *Client) UpdateSandbox(ctx context.Context, id string, req api.UpdateSandboxRequest) (api.SandboxInfoResponse, error) {
	var out api.SandboxInfoResponse
	err := c.do(ctx, http.MethodPatch, lifecycleSandboxPath(id), nil, nil, req, &out, api.TypeSandboxInfo)
	return out, err
}
