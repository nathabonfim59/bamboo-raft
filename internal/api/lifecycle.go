package api

import (
	"context"
	"errors"
	"log/slog"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// StopReader loads durable stop metadata for an owner-scoped sandbox. Every
// lookup binds the sandbox id as well as the stop id, so a stop that belongs
// to another sandbox in the same account is never returned. *db.Store
// satisfies it.
type StopReader interface {
	StopOperation(ctx context.Context, owner uuid.UUID, sandboxID, stopID string) (db.StopOperation, error)
}

// LifecycleStore is the owner-scoped persistence the Phase 1 lifecycle
// handlers need. Create, get and list still come from CoreStore; the server
// passes the same *db.Store to both constructors.
type LifecycleStore interface {
	// Sandbox returns one live sandbox owned by owner.
	Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error)
	// PatchSandbox applies an owner-scoped PATCH of name, TTL and
	// subdomain.
	PatchSandbox(ctx context.Context, params db.PatchSandboxParams) (db.Sandbox, error)
	// AcceptSandboxStop durably accepts a stop and returns the canonical
	// operation, the updated sandbox and whether a new operation was
	// written. A repeat while pending returns the same operation.
	AcceptSandboxStop(ctx context.Context, owner uuid.UUID, sandboxID string, force bool) (db.StopOperation, db.Sandbox, db.Job, bool, error)
	// StopOperation returns one stop by owner, sandbox and stop id.
	StopOperation(ctx context.Context, owner uuid.UUID, sandboxID, stopID string) (db.StopOperation, error)
}

// LifecycleStoreContract keeps the accepted database facade inside the
// contract without exposing scheduler, gateway or lifecycle worker types to
// the API.
var _ LifecycleStore = (*db.Store)(nil)

// ResumeRequest is the validated, API-owned resume admission contract. It
// carries no node, hub or gateway handle: the admission implementation owns
// capacity, placement and the durable accept.
type ResumeRequest struct {
	Owner       uuid.UUID
	SandboxID   string
	MachineType string
	// Resources is the complete requested shape, or zero to keep the
	// stored desired shape. MachineType is empty exactly when Resources is
	// zero.
	Resources db.Resources
	// Env is the three-state resume env: omitted keeps the sealed env,
	// present replaces it (an empty map replaces it with none).
	Env db.EnvChange
	// TTL is the three-state resume TTL: omitted keeps the configured
	// duration; null disables auto-stop.
	TTL db.TTLOption
	// SupersedeStopID names the active stop when the sandbox is still
	// archiving. It is empty for an archived sandbox.
	SupersedeStopID string
	// FailFast asks admission to refuse immediately when no machine is
	// ready instead of queueing the start.
	FailFast bool
}

// ResumeResult is one accepted resume. Superseded is set when the resume
// ended an in-flight stop.
type ResumeResult struct {
	Sandbox    db.Sandbox
	Superseded *db.StopOperation
}

// ResumeAdmission durably accepts one resume. The lifecycle scheduler owns
// the implementation; the HTTP handler never touches a node or the job
// tables itself.
type ResumeAdmission interface {
	Resume(ctx context.Context, request ResumeRequest) (ResumeResult, error)
}

// ResumeAdmissionFunc adapts a plain function to [ResumeAdmission].
type ResumeAdmissionFunc func(ctx context.Context, request ResumeRequest) (ResumeResult, error)

// Resume implements [ResumeAdmission].
func (f ResumeAdmissionFunc) Resume(ctx context.Context, request ResumeRequest) (ResumeResult, error) {
	return f(ctx, request)
}

// LifecycleOptions are the dependencies of the Phase 1 lifecycle handlers.
type LifecycleOptions struct {
	// Store is the owner-scoped lifecycle persistence. Required.
	Store LifecycleStore
	// Resume accepts resumes. Required.
	Resume ResumeAdmission
	// Logger receives operational failures. Nil discards them.
	Logger *slog.Logger
	// CapacityRetrySeconds is the positive retry hint on a capacity
	// refusal. Zero or negative uses DefaultCapacityRetrySeconds.
	CapacityRetrySeconds int
}

// lifecycleHandlers serves PATCH /sandboxes/{sandboxId}, stop, stop status and
// resume. It deliberately holds only the store, the resume seam, a logger and
// a retry hint.
type lifecycleHandlers struct {
	store         LifecycleStore
	resume        ResumeAdmission
	logger        *slog.Logger
	capacityRetry int
}

// NewLifecycleHandlers returns the Phase 1 lifecycle routes. The caller
// registers them with [Register]; the server owns authentication, request ids
// and the shared error rendering.
func NewLifecycleHandlers(options LifecycleOptions) (Handlers, error) {
	if options.Store == nil {
		return Handlers{}, errors.New("api: lifecycle handlers need a store")
	}
	if options.Resume == nil {
		return Handlers{}, errors.New("api: lifecycle handlers need a resume admission implementation")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	capacityRetry := options.CapacityRetrySeconds
	if capacityRetry <= 0 {
		capacityRetry = DefaultCapacityRetrySeconds
	}
	handler := &lifecycleHandlers{
		store:         options.Store,
		resume:        options.Resume,
		logger:        logger,
		capacityRetry: capacityRetry,
	}
	return Handlers{
		UpdateSandbox:  handler.updateSandbox,
		StopSandbox:    handler.stopSandbox,
		GetSandboxStop: handler.getSandboxStop,
		ResumeSandbox:  handler.resumeSandbox,
	}, nil
}

// lifecycleErrorName classifies a lifecycle failure for logs without echoing
// store or node text.
func lifecycleErrorName(err error) string {
	switch {
	case errors.Is(err, db.ErrNotFound):
		return "not_found"
	case errors.Is(err, db.ErrConflict):
		return "conflict"
	case errors.Is(err, db.ErrInvalidState):
		return "invalid_state"
	case errors.Is(err, db.ErrInvalidName):
		return "invalid_name"
	case errors.Is(err, db.ErrInvalidSubdomain):
		return "invalid_subdomain"
	case errors.Is(err, db.ErrSubdomainTaken):
		return "subdomain_taken"
	case errors.Is(err, db.ErrNoCapacity):
		return "no_capacity"
	case errors.Is(err, db.ErrNodeUnavailable):
		return "node_unavailable"
	case errors.Is(err, db.ErrTypeTooSmall):
		return "type_too_small"
	case errors.Is(err, db.ErrInvalidMachine):
		return "invalid_machine"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	}
	return "internal"
}

// lifecycleInternal logs one control-plane failure and returns the approved
// safe 500. A canceled request writes nothing.
func lifecycleInternal(ctx context.Context, logger *slog.Logger, operation string, err error) *apierr.Error {
	if ctx.Err() != nil {
		return nil
	}
	logger.ErrorContext(ctx, "api: lifecycle "+operation+" failed", "error", lifecycleErrorName(err))
	return coreInternal()
}

// lifecycleRetrySeconds returns the positive capacity retry hint.
func lifecycleRetrySeconds(hint int) int {
	if hint <= 0 {
		return DefaultCapacityRetrySeconds
	}
	return hint
}

// stopStatusPath is the pinned path that reads one stop of one sandbox.
func stopStatusPath(sandboxID, stopID string) string {
	return "/api/v1/sandboxes/" + sandboxID + "/stops/" + stopID
}
