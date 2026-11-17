package api

import "time"

// DeletionKind is the resource kind of a deletion operation.
type DeletionKind string

// Deletion operation kinds.
const (
	DeletionKindSandbox  DeletionKind = "sandbox"
	DeletionKindSnapshot DeletionKind = "snapshot"
)

// DeletionReason is why a deletion operation exists.
type DeletionReason string

// Deletion reasons.
const (
	DeletionReasonExplicit DeletionReason = "explicit"
	DeletionReasonZDR      DeletionReason = "zdr"
	DeletionReasonAccount  DeletionReason = "account"
)

// DeletionStatus is the durable state of a deletion operation. These are the
// only states in the v1 API document.
type DeletionStatus string

// Deletion statuses.
const (
	DeletionPending    DeletionStatus = "pending"
	DeletionProcessing DeletionStatus = "processing"
	DeletionBlocked    DeletionStatus = "blocked"
	DeletionCompleted  DeletionStatus = "completed"
)

// DeletionStage is the background purge step.
type DeletionStage string

// Deletion stages.
const (
	DeletionStageRemoving          DeletionStage = "removing"
	DeletionStageWaitingForUploads DeletionStage = "waiting_for_uploads"
	DeletionStageRetrying          DeletionStage = "retrying"
	DeletionStageCompleted         DeletionStage = "completed"
)

// DeletionOperation is the durable record returned by DELETE /api/v1/sandboxes
// and polled through GET /api/v1/deletion-operations/{operationId}. The
// database and lifecycle packages own the id and the state transitions; this
// type only carries them.
type DeletionOperation struct {
	ID           string         `json:"id"`
	Kind         DeletionKind   `json:"kind"`
	TargetID     string         `json:"targetId"`
	Reason       DeletionReason `json:"reason"`
	Status       DeletionStatus `json:"status"`
	Stage        *DeletionStage `json:"stage,omitzero"`
	ExpectedBy   *time.Time     `json:"expectedBy,omitzero"`
	AttemptCount int            `json:"attemptCount"`
	RequestedAt  time.Time      `json:"requestedAt"`
	CompletedAt  *time.Time     `json:"completedAt"`
}

// DeletionOperationResponse is the GET /api/v1/deletion-operations/{id}
// envelope (type deletion.operation).
type DeletionOperationResponse struct {
	SuccessBase
	Operation DeletionOperation `json:"operation"`
}

// NewDeletionOperationResponse builds a poll response. It never invents an
// operation id or status; the database row supplies them.
func NewDeletionOperationResponse(op DeletionOperation) DeletionOperationResponse {
	return DeletionOperationResponse{SuccessBase: success(TypeDeletionOperation), Operation: op}
}

// NewSandboxDeletingResponse builds the 202 envelope of DELETE
// /api/v1/sandboxes/{sandboxId} (type sandbox.deleting).
func NewSandboxDeletingResponse(op DeletionOperation) DeletionOperationResponse {
	return DeletionOperationResponse{SuccessBase: success(TypeSandboxDeleting), Operation: op}
}
