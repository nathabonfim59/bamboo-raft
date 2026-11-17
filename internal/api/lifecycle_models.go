package api

import (
	"encoding/json/jsontext"
	"time"
)

// Success discriminators emitted by the Phase 1 lifecycle operations.
// TypeStopStatus is the pin's const for GET /sandboxes/{sandboxId}/stops/{stopId}.
// The stop and resume action types are our selected values for the pin's
// unconstrained SandboxActionResponse.type: the pin only gives
// sandbox.stopping and sandbox.resuming as examples.
const (
	TypeSandboxStopping = "sandbox.stopping"
	TypeSandboxStopped  = "sandbox.stopped"
	TypeSandboxResuming = "sandbox.resuming"
	TypeStopStatus      = "stop.status"
)

// StopStatus is the pinned StopOperation lifecycle.
type StopStatus string

// The pinned StopOperation statuses.
const (
	StopPending    StopStatus = "pending"
	StopFailing    StopStatus = "failing"
	StopCompleted  StopStatus = "completed"
	StopSuperseded StopStatus = "superseded"
)

// StopOperation is the pinned stop object. Every nullable member is always
// serialized because the pinned schema requires all six core members on the
// wire, even when their value is null.
type StopOperation struct {
	ID            string     `json:"id"`
	Status        StopStatus `json:"status"`
	RequestedAt   *time.Time `json:"requestedAt"`
	LastAttemptAt *time.Time `json:"lastAttemptAt"`
	Error         *string    `json:"error"`
	EndedAt       *time.Time `json:"endedAt"`
	Forced        bool       `json:"forced,omitzero"`
}

// StopOperationResponse is the GET /sandboxes/{sandboxId}/stops/{stopId}
// envelope. The pinned type constant is stop.status and id is the sandbox id.
type StopOperationResponse struct {
	SuccessBase
	ID   string        `json:"id"`
	Stop StopOperation `json:"stop"`
}

// NewStopOperationResponse builds the stop-status envelope.
func NewStopOperationResponse(sandboxID string, stop StopOperation) StopOperationResponse {
	return StopOperationResponse{
		OK:   true,
		Type: TypeStopStatus,
		ID:   sandboxID,
		Stop: stop,
	}
}

// SandboxActionResponse is the shared stop/resume/fork action envelope. Only
// the members that operation defines are set; every other member is omitted.
type SandboxActionResponse struct {
	SuccessBase
	ID         string         `json:"id"`
	Status     string         `json:"status"`
	Sandbox    *Sandbox       `json:"sandbox,omitzero"`
	Forced     *bool          `json:"forced,omitzero"`
	Operation  *StopOperation `json:"operation,omitzero"`
	StatusURL  *string        `json:"statusUrl,omitzero"`
	GiftNotice string         `json:"giftLimitNotice,omitempty"`
}

// NewStopActionResponse builds a stop response with its operation and status
// URL.
func NewStopActionResponse(typ, sandboxID, status string, sandbox Sandbox, operation StopOperation, statusURL string, forced bool) SandboxActionResponse {
	return SandboxActionResponse{
		OK:        true,
		Type:      typ,
		ID:        sandboxID,
		Status:    status,
		Sandbox:   &sandbox,
		Forced:    &forced,
		Operation: &operation,
		StatusURL: &statusURL,
	}
}

// NewResumeActionResponse builds the pinned resume acceptance envelope.
func NewResumeActionResponse(sandboxID string, sandbox Sandbox) SandboxActionResponse {
	return SandboxActionResponse{
		OK:      true,
		Type:    TypeSandboxResuming,
		ID:      sandboxID,
		Status:  "resuming",
		Sandbox: &sandbox,
	}
}

// StopSandboxRequest is the optional POST /sandboxes/{sandboxId}/stop body.
type StopSandboxRequest struct {
	Force *bool `json:"force,omitzero"`
}

// UpdateSandboxRequest is the PATCH /sandboxes/{sandboxId} body. The raw TTL
// member keeps an omitted field distinguishable from an explicit null.
type UpdateSandboxRequest struct {
	Name       *string        `json:"name,omitzero"`
	TTLSeconds jsontext.Value `json:"ttlSeconds,omitzero"`
	Subdomain  *string        `json:"subdomain,omitzero"`
}

// ResumeSandboxRequest is the optional POST /sandboxes/{sandboxId}/resume
// body. The raw TTL member keeps omitted, null and value distinguishable.
type ResumeSandboxRequest struct {
	FailFast    *bool             `json:"failFast,omitzero"`
	Type        *string           `json:"type,omitzero"`
	Env         map[string]string `json:"env,omitzero"`
	Environment *string           `json:"environment,omitzero"`
	NoEnv       *bool             `json:"noEnv,omitzero"`
	TTLSeconds  jsontext.Value    `json:"ttlSeconds,omitzero"`
}
