package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// errResumeResult reports a resume admission result that is not an
// owner-matching sandbox row. It is a control-plane fault, never a client
// error.
var errResumeResult = errors.New("api: resume admission returned a malformed or foreign sandbox")

// operationFromRow converts one durable stop operation to the pinned object.
// keepRequested keeps requestedAt for the by-id read, where the pin keeps it
// after the stop ended; the sandbox view nulls it once the stop is terminal.
func operationFromRow(op db.StopOperation, keepRequested bool) StopOperation {
	out := StopOperation{
		ID:     op.ID,
		Status: StopStatus(op.Status),
		Forced: op.Forced,
	}
	terminal := op.Status == db.StopCompleted || op.Status == db.StopSuperseded
	if keepRequested || !terminal {
		out.RequestedAt = new(op.RequestedAt.UTC())
	}
	if op.LastAttemptAt != nil {
		out.LastAttemptAt = new(op.LastAttemptAt.UTC())
	}
	if op.Status == db.StopFailing && op.Error != "" {
		out.Error = new(op.Error)
	}
	if op.EndedAt != nil {
		out.EndedAt = new(op.EndedAt.UTC())
	}
	return out
}

// sandboxView converts a row and, when the sandbox has been stopped, attaches
// its latest stop operation. The stop lookup binds owner, sandbox and stop id.
func sandboxView(ctx context.Context, stops StopReader, owner uuid.UUID, row db.Sandbox) (Sandbox, error) {
	out := sandboxFromRow(row)
	if stops == nil || row.StopOperationID == nil || *row.StopOperationID == "" {
		return out, nil
	}
	op, err := stops.StopOperation(ctx, owner, row.ID, *row.StopOperationID)
	if err != nil {
		return Sandbox{}, err
	}
	view := operationFromRow(op, false)
	out.Stop = &view
	return out, nil
}

// coreSandboxView is the core get/list conversion with its safe error
// mapping. A cancellation writes nothing.
func (h *coreHandlers) coreSandboxView(ctx context.Context, owner uuid.UUID, row db.Sandbox) (Sandbox, *apierr.Error) {
	out, err := sandboxView(ctx, h.stops, owner, row)
	if err == nil {
		return out, nil
	}
	if ctx.Err() != nil {
		return Sandbox{}, nil
	}
	h.logger.ErrorContext(ctx, "api: core stop metadata failed", "error", lifecycleErrorName(err))
	return Sandbox{}, coreInternal()
}

// updateFailure maps a PATCH persistence failure to the pinned safe error. A
// taken or malformed subdomain is a documented BadRequest because the pinned
// update response table has no 409; the message says which rule failed.
func updateFailure(ctx context.Context, logger *slog.Logger, err error) *apierr.Error {
	if ctx.Err() != nil {
		return nil
	}
	switch {
	case errors.Is(err, db.ErrNotFound):
		return coreNotFound()
	case errors.Is(err, db.ErrInvalidName):
		return coreInvalidName("name must be between 1 and 120 characters")
	case errors.Is(err, db.ErrInvalidSubdomain):
		return coreInvalidJSON("subdomain must be 3 to 40 lowercase letters, digits or single hyphens, with no reserved suffix")
	case errors.Is(err, db.ErrSubdomainTaken):
		return coreInvalidJSON("subdomain is already in use")
	}
	return lifecycleInternal(ctx, logger, "update sandbox", err)
}

// stopFailure maps a stop acceptance failure. The pinned stop table has only
// 400/401/404/500 for local faults; an unsupported state is a documented
// BadRequest rather than an undocumented 409.
func stopFailure(ctx context.Context, logger *slog.Logger, err error) *apierr.Error {
	if ctx.Err() != nil {
		return nil
	}
	switch {
	case errors.Is(err, db.ErrNotFound):
		return coreNotFound()
	case errors.Is(err, db.ErrInvalidState):
		return coreInvalidJSON("sandbox cannot be stopped in its current state")
	}
	return lifecycleInternal(ctx, logger, "stop sandbox", err)
}

// stopReadFailure maps a sandbox or stop lookup failure. Foreign and missing
// rows are the same 404.
func stopReadFailure(ctx context.Context, logger *slog.Logger, err error) *apierr.Error {
	if ctx.Err() != nil {
		return nil
	}
	if errors.Is(err, db.ErrNotFound) {
		return coreNotFound()
	}
	return lifecycleInternal(ctx, logger, "read sandbox lifecycle", err)
}

// resumeChanges records what a resume asked to change, so a running sandbox
// refusal can use the pinned sandbox_running_stop_first code.
type resumeChanges struct {
	shape       bool
	env         bool
	environment bool
	noEnv       bool
}

// any reports whether the request asked for a change that only a stopped
// sandbox can apply.
func (c resumeChanges) any() bool {
	return c.shape || c.env || c.environment || c.noEnv
}

// resumeFailure maps a resume admission failure to the pinned answer.
func resumeFailure(ctx context.Context, logger *slog.Logger, capacityRetry int, row db.Sandbox, changes resumeChanges, err error) *apierr.Error {
	if ctx.Err() != nil {
		return nil
	}
	if apiErr, ok := errors.AsType[*apierr.Error](err); ok {
		return apiErr
	}
	switch {
	case errors.Is(err, db.ErrNotFound):
		return coreNotFound()
	case errors.Is(err, db.ErrNoCapacity), errors.Is(err, ErrNoReadyMachine),
		errors.Is(err, scheduler.ErrNoCapacity), errors.Is(err, scheduler.ErrNoNode):
		return coreNoReadyMachine(lifecycleRetrySeconds(capacityRetry))
	case errors.Is(err, db.ErrNodeUnavailable):
		return coreNoReadyMachine(lifecycleRetrySeconds(capacityRetry))
	case errors.Is(err, db.ErrTypeTooSmall):
		return apierr.New(http.StatusConflict, "type_too_small",
			"the retained disk is larger than the requested machine type")
	case errors.Is(err, db.ErrInvalidMachine):
		return coreInvalidJSON("type must be a supported machine type")
	case errors.Is(err, db.ErrInvalidState):
		return resumeStateFailure(row, changes)
	case errors.Is(err, db.ErrStopSuperseded):
		return coreConflict("the stop this resume took over already ended")
	case errors.Is(err, db.ErrConflict):
		if row.State == db.StateArchiving {
			return sandboxStopping(row)
		}
		return coreConflict("sandbox changed while the resume was accepted")
	}
	return lifecycleInternal(ctx, logger, "resume sandbox", err)
}

// resumeStateFailure chooses the pinned 409 code for a state that cannot be
// resumed. A provisioning sandbox is already resuming; a running sandbox that
// was asked to change shape, environment or noEnv must be stopped first; every
// other state is a plain conflict.
func resumeStateFailure(row db.Sandbox, changes resumeChanges) *apierr.Error {
	switch row.State {
	case db.StateError:
		return sandboxNotReady()
	case db.StateProvisioning:
		return apierr.New(http.StatusConflict, apierr.CodeSandboxResuming,
			"the sandbox is already resuming").Retryable(true)
	case db.StateReady, db.StateIdle, db.StateRunning:
		if changes.any() {
			return apierr.New(http.StatusConflict, apierr.CodeSandboxRunningStopFirst,
				"stop the sandbox before resuming with a new type, environment or noEnv")
		}
		return coreConflict("the sandbox is already running")
	default:
		return coreConflict("sandbox state does not allow resume")
	}
}

// sandboxNotReady is the pinned 409 for a sandbox that stopped with an error
// and has no resumable disk. It is retryable=false: only deletion removes it.
func sandboxNotReady() *apierr.Error {
	return apierr.New(http.StatusConflict, apierr.CodeSandboxNotReady,
		"the sandbox stopped with an error and cannot be resumed; delete it instead").Retryable(false)
}

// sandboxStopping builds the pinned sandbox_stopping conflict for a resume
// that cannot take over an in-flight stop. The retry hint is positive, and
// the status URL lets a client poll the stop it must wait for.
func sandboxStopping(row db.Sandbox) *apierr.Error {
	apiErr := apierr.New(http.StatusConflict, apierr.CodeSandboxStopping,
		"sandbox is stopping; retry after the stop finishes").Retryable(true)
	if row.StopOperationID != nil && *row.StopOperationID != "" {
		apiErr.WithDetail("statusUrl", stopStatusPath(row.ID, *row.StopOperationID))
	}
	return apiErr
}
