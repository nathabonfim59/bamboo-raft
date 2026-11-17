package api

// Command timeouts from the pinned CommandRequest. The stream mode difference
// approved for Phase 0.5 raises the maximum to the configured
// commands.stream-max-seconds, default DefaultStreamMaxSeconds, so a test job
// can run longer than 600 seconds.
const (
	DefaultCommandTimeoutSeconds = 30
	MaxCommandTimeoutSeconds     = 600
	DefaultStreamMaxSeconds      = 7200
)

// CommandRequest is the request body of POST
// /api/v1/sandboxes/{sandboxId}/commands. Detached commands are outside the
// Phase 0.5 subset; handlers reject Detached instead of ignoring it.
type CommandRequest struct {
	Command        string  `json:"command"`
	Cwd            *string `json:"cwd,omitzero"`
	TimeoutSeconds *int    `json:"timeoutSeconds,omitzero"`
	Detached       *bool   `json:"detached,omitzero"`
	Stream         *bool   `json:"stream,omitzero"`
}
