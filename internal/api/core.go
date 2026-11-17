package api

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// CoreStore is the owner-scoped persistence the core handlers read and mutate.
// Every method takes the caller's account UUID explicitly; implementations
// must treat a row owned by another account exactly like a missing one. The
// accepted [*db.Store] satisfies it directly.
type CoreStore interface {
	// Sandbox returns one live sandbox owned by owner.
	Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error)
	// ListSandboxes returns one owner-scoped page and its next cursor.
	ListSandboxes(ctx context.Context, query db.SandboxListQuery) ([]db.Sandbox, *db.PageCursor, error)
	// AcceptSandboxDeletion atomically accepts an irreversible deletion.
	AcceptSandboxDeletion(ctx context.Context, owner uuid.UUID, sandboxID string, reason db.DeletionReason) (db.DeletionOperation, db.Sandbox, error)
	// DeletionOperation returns one operation owned by owner.
	DeletionOperation(ctx context.Context, owner uuid.UUID, operationID string) (db.DeletionOperation, error)
}

// CoreStoreContract keeps the database facade inside the accepted contract.
var _ CoreStore = (*db.Store)(nil)

// AdmissionRequest is the API-owned create admission contract. It carries
// everything placement needs and nothing about a specific node or image:
// admission chooses the node and the base image itself. MachineType and
// Resources are the canonical machinetype lookup result, not a second guess
// from the raw request.
//
// Implementations must check the owner idempotency key before any placement
// work, so the same key and RequestSHA256 replay the original sandbox id even
// after that sandbox was soft-deleted. On a replay they return the stored
// sandbox with Replayed set.
type AdmissionRequest struct {
	Owner       uuid.UUID
	SandboxID   string
	MachineType string
	Resources   db.Resources
	Name        string
	Labels      map[string]string
	Env         map[string]string
	// TTLSeconds is the requested auto-stop duration. nil disables expiry
	// (the request sent null or "infinite"), matching the pinned create
	// field. Admission computes the durable deadline at placement time so
	// the stored expires_at-created_at pair stays exactly this duration
	// even when placement is delayed or the clocks differ.
	TTLSeconds     *int
	FailFast       bool
	IdempotencyKey string
	RequestSHA256  [sha256.Size]byte
	// From is the saved-snapshot name the create asked to restore, empty
	// for a normal create. The admission implementation resolves it after
	// checking the idempotency replay; this request never carries a source
	// row or node.
	From string
	// TypeExplicit reports whether the raw create carried a type member.
	// An omitted type on a restore inherits the snapshot's canonical type;
	// every other create already carries the default in MachineType.
	TypeExplicit bool
}

// AdmissionResult is the durable create outcome returned by admission.
type AdmissionResult struct {
	Sandbox  db.Sandbox
	Replayed bool
}

// Admission places and durably records one create. Implementations own the
// scheduler and database transaction; the HTTP handler never touches a node,
// the node hub or any node credential.
type Admission interface {
	Admit(ctx context.Context, request AdmissionRequest) (AdmissionResult, error)
}

// AdmissionFunc adapts a plain function to [Admission].
type AdmissionFunc func(ctx context.Context, request AdmissionRequest) (AdmissionResult, error)

// Admit implements [Admission].
func (f AdmissionFunc) Admit(ctx context.Context, request AdmissionRequest) (AdmissionResult, error) {
	return f(ctx, request)
}

// ErrNoReadyMachine is the admission sentinel for a fail-fast placement that
// found no ready machine. A server adapter translates its scheduler's
// no-ready-node error into this value; the handler maps it to the pinned 503
// no_ready_machine regardless of whether failFast was requested.
var ErrNoReadyMachine = errors.New("api: no ready machine")

// DefaultCapacityRetrySeconds is the positive retry hint on a create capacity
// refusal. Clients back off for this many seconds, matching the command path.
const DefaultCapacityRetrySeconds = 5

// CoreOptions are the dependencies of the Phase 0.5 core handlers.
type CoreOptions struct {
	// Store is the owner-scoped persistence. Required.
	Store CoreStore
	// Admission places creates. Required.
	Admission Admission
	// Logger receives operational failures. Nil discards them.
	Logger *slog.Logger
	// Now supplies the generated display name when a create omits one. Nil
	// uses time.Now.
	Now func() time.Time
	// CapacityRetrySeconds is the positive retry hint on a capacity
	// refusal. Zero or negative uses DefaultCapacityRetrySeconds.
	CapacityRetrySeconds int
	// Stops loads stop metadata for get/list. Nil falls back to the store
	// when it implements [StopReader]; a store without it omits the stop
	// member.
	Stops StopReader
}

// coreHandlers serves the core v1 routes. It deliberately keeps
// only a store, an admission seam, a logger and a clock: no node hub, node
// credentials or scheduler instance.
type coreHandlers struct {
	store         CoreStore
	admission     Admission
	logger        *slog.Logger
	now           func() time.Time
	capacityRetry int
	stops         StopReader
}

// NewCoreHandlers returns the handlers of GET /me, the sandbox CRUD subset and
// GET /deletion-operations/{operationId}. The caller registers them with
// [Register]; the server owns authentication and request ids.
func NewCoreHandlers(options CoreOptions) (Handlers, error) {
	if options.Store == nil {
		return Handlers{}, errors.New("api: core handlers need a store")
	}
	if options.Admission == nil {
		return Handlers{}, errors.New("api: core handlers need an admission implementation")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	capacityRetry := options.CapacityRetrySeconds
	if capacityRetry <= 0 {
		capacityRetry = DefaultCapacityRetrySeconds
	}
	stops := options.Stops
	if stops == nil {
		stops, _ = options.Store.(StopReader)
	}
	handler := &coreHandlers{
		store:         options.Store,
		admission:     options.Admission,
		logger:        logger,
		now:           now,
		capacityRetry: capacityRetry,
		stops:         stops,
	}
	return Handlers{
		Me:                   handler.me,
		ListSandboxes:        handler.listSandboxes,
		CreateSandbox:        handler.createSandbox,
		GetSandbox:           handler.getSandbox,
		DeleteSandbox:        handler.deleteSandbox,
		GetDeletionOperation: handler.getDeletionOperation,
	}, nil
}

// coreOwner reads the account UUID the server authenticated into the request
// context. Missing, malformed or nil identities are the same 401: the handler
// never guesses a tenant.
func coreOwner(r *http.Request) (uuid.UUID, *apierr.Error) {
	identity, ok := auth.IdentityFrom(r.Context())
	if !ok || identity.Owner == "" {
		return uuid.Nil(), coreUnauthorized()
	}
	owner, err := uuid.Parse(identity.Owner)
	if err != nil || owner == uuid.Nil() {
		return uuid.Nil(), coreUnauthorized()
	}
	return owner, nil
}

// coreParseQuery parses the raw query without url.Values' silent fallback.
// A malformed percent escape or a bare semicolon is invalid_query, never a
// quietly dropped filter.
func coreParseQuery(r *http.Request) (url.Values, *apierr.Error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, coreInvalidQuery("query", "malformed query string")
	}
	return values, nil
}

// coreRejectUnknownQuery refuses every query parameter on create, which
// supports none. Repeating an unsupported parameter is still unsupported; the
// first name in sorted order is reported so the answer is deterministic.
func coreRejectUnknownQuery(values url.Values) *apierr.Error {
	if len(values) == 0 {
		return nil
	}
	names := slices.Sorted(maps.Keys(values))
	return coreInvalidQuery(names[0], "unsupported query parameter")
}

// coreErrorName classifies an error for logs without echoing its text. Store
// errors can embed SQL or connection details; request bodies can embed env
// values; scheduler refusal wrappers can carry either. Only fixed sentinel
// labels or the Go type are safe.
func coreErrorName(err error) string {
	switch {
	case errors.Is(err, db.ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrNoReadyMachine), errors.Is(err, scheduler.ErrNoNode):
		return "no_ready_machine"
	case errors.Is(err, db.ErrNoCapacity), errors.Is(err, scheduler.ErrNoCapacity):
		return "no_capacity"
	case errors.Is(err, db.ErrIdempotencyReused):
		return "idempotency_reused"
	case errors.Is(err, db.ErrIdempotencyInProgress):
		return "idempotency_in_progress"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	}
	return fmt.Sprintf("%T", err)
}

// storeFailure logs one storage failure and maps it to a safe API error. A
// canceled request writes nothing. Foreign and missing rows are the same 404;
// every other fault is the approved internal 500.
func (h *coreHandlers) storeFailure(ctx context.Context, operation string, err error) *apierr.Error {
	if ctx.Err() != nil {
		return nil
	}
	h.logger.ErrorContext(ctx, "api: core "+operation+" failed", "error", coreErrorName(err))
	if errors.Is(err, db.ErrNotFound) {
		return coreNotFound()
	}
	return coreInternal()
}

// sandboxFromRow converts a durable row into the pinned sandbox object. It
// never fabricates a URL, IP or desktop address, never exposes env and never
// invents a deleting/deleted state.
func sandboxFromRow(row db.Sandbox) Sandbox {
	out := Sandbox{
		ID:                  row.ID,
		Name:                row.Name,
		State:               SandboxState(row.State),
		URL:                 nil,
		IP:                  nil,
		DesktopAvailable:    false,
		DesktopURL:          nil,
		SnapshotAvailable:   false,
		SnapshotCompletedAt: nil,
	}
	// The SSH endpoint is connection metadata for a running machine only.
	// The database clears the pair on every stop, resume and terminal
	// transition; gate the export on the state as well, so a malformed row
	// that still carries an old pair never advertises a dead boot.
	switch row.State {
	case db.StateReady, db.StateIdle, db.StateRunning:
		if row.SSH != nil {
			out.SSHEndpoint = new(row.SSH.Endpoint)
		}
	}
	if row.ErrorReason != "" {
		out.Error = new(row.ErrorReason)
	}
	if len(row.Labels) > 0 {
		out.Labels = maps.Clone(row.Labels)
	}
	if machine, ok := machinetype.Lookup(row.MachineType); ok && row.MachineType != "" {
		out.Type = machine.Name
		out.VCPU = int(machine.VCPUs)
		out.MemoryGB = int(machine.MemoryMiB / 1024)
		out.BillingMultiplier = machine.BillingMultiplier
	}
	if !row.CreatedAt.IsZero() {
		out.CreatedAt = new(row.CreatedAt.UTC())
	}
	if !row.UpdatedAt.IsZero() {
		out.UpdatedAt = new(row.UpdatedAt.UTC())
	}
	if row.ExpiresAt != nil && !row.ExpiresAt.IsZero() {
		out.ArchiveAfter = new(row.ExpiresAt.UTC())
	}
	// The configured duration is the durable value, not the time left. A
	// PATCH or resume stores it explicitly; older rows fall back to the
	// creation/expiry pair.
	if ttl := configuredTTL(row); ttl != nil {
		out.TTLSeconds = ttl
	}
	if row.Subdomain != nil {
		out.Subdomain = new(*row.Subdomain)
	}
	if row.StoppedAt != nil && !row.StoppedAt.IsZero() {
		out.StoppedAt = new(row.StoppedAt.UTC())
	}
	return out
}

// configuredTTL returns the auto-stop duration the client configured. The
// durable ttl_seconds value wins; older rows without it fall back to the
// creation/expiry pair. This is the configured TTL, not the time left: a list
// row one hour into a two-hour TTL still reports 7200.
func configuredTTL(row db.Sandbox) *int {
	if row.TTLSeconds != nil {
		return new(*row.TTLSeconds)
	}
	if row.ExpiresAt == nil || row.CreatedAt.IsZero() {
		return nil
	}
	seconds := int(row.ExpiresAt.Sub(row.CreatedAt).Round(time.Second).Seconds())
	if seconds < 1 {
		seconds = 1
	}
	return &seconds
}

// deletionOperationFromRow converts a durable operation row. It never invents
// an id, status or stage.
func deletionOperationFromRow(row db.DeletionOperation) DeletionOperation {
	out := DeletionOperation{
		ID:           row.ID,
		Kind:         DeletionKind(row.Kind),
		TargetID:     row.TargetID,
		Reason:       DeletionReason(row.Reason),
		Status:       DeletionStatus(row.Status),
		AttemptCount: row.AttemptCount,
		RequestedAt:  row.RequestedAt.UTC(),
	}
	if row.Stage != "" {
		out.Stage = new(DeletionStage(row.Stage))
	}
	if row.ExpectedBy != nil {
		out.ExpectedBy = new(row.ExpectedBy.UTC())
	}
	if row.CompletedAt != nil {
		out.CompletedAt = new(row.CompletedAt.UTC())
	}
	return out
}

// createStatus is the pinned 202 status. Only an idempotent replay that finds
// a usable sandbox reports ready; a fresh create is always provisioning.
func createStatus(result AdmissionResult) CreateStatus {
	if !result.Replayed {
		return CreateProvisioning
	}
	switch result.Sandbox.State {
	case db.StateReady, db.StateIdle, db.StateRunning:
		return CreateReady
	default:
		return CreateProvisioning
	}
}

// admissionFailure maps a create admission failure to the pinned safe error.
// Capacity is retryable and always carries a positive retry hint. Lookup and
// SQL text are logged by type only, never forwarded.
func (h *coreHandlers) admissionFailure(ctx context.Context, failFast bool, err error) *apierr.Error {
	if ctx.Err() != nil {
		return nil
	}
	// The restore adapter resolves the owner-scoped source and returns the
	// already-classified API error. It carries no raw store or node text.
	if apiErr, ok := errors.AsType[*apierr.Error](err); ok {
		return apiErr
	}
	switch {
	case errors.Is(err, ErrNoReadyMachine):
		return coreNoReadyMachine(h.capacityRetry)
	case errors.Is(err, db.ErrNoCapacity):
		if failFast {
			return coreNoReadyMachine(h.capacityRetry)
		}
		return coreOutOfCapacity(h.capacityRetry)
	case errors.Is(err, db.ErrSnapshotNotFound), errors.Is(err, db.ErrSnapshotDiskUnknown):
		return snapshotNotFound()
	case errors.Is(err, db.ErrSnapshotDiskTooSmall):
		return coreInvalidJSON("type disk is smaller than the snapshot's disk")
	case errors.Is(err, db.ErrIdempotencyReused):
		return apierr.New(http.StatusConflict, apierr.CodeIdempotencyKeyReused,
			"idempotency key was reused with a different request")
	case errors.Is(err, db.ErrIdempotencyInProgress):
		return apierr.New(http.StatusConflict, apierr.CodeIdempotencyInProgress,
			"a create with this idempotency key is still in progress").Retryable(true)
	}
	h.logger.ErrorContext(ctx, "api: core create admission failed", "error", coreErrorName(err))
	return coreInternal()
}

// coreUnauthorized is the safe missing-identity answer.
func coreUnauthorized() *apierr.Error {
	return apierr.New(http.StatusUnauthorized, apierr.CodeUnauthorized, "unauthorized")
}

// coreNotFound is the owner-scoped missing-resource answer. Foreign and missing
// ids are deliberately indistinguishable.
func coreNotFound() *apierr.Error {
	return apierr.New(http.StatusNotFound, apierr.CodeNotFound, "not found")
}

// coreInternal is the safe 500 for control-plane faults.
func coreInternal() *apierr.Error {
	return apierr.New(http.StatusInternalServerError, apierr.CodeInternal, "internal server error")
}

// coreInvalidJSON is a safe 400 for a malformed or unsupported body.
func coreInvalidJSON(message string) *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON, message)
}

// coreInvalidEnv is the pinned bad-environment-variable 400.
func coreInvalidEnv(message string) *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidEnv, message)
}

// coreInvalidName is the pinned bad-name 400.
func coreInvalidName(message string) *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidName, message)
}

// coreUnknownEnvironment is the pinned unsupported-environment 400.
func coreUnknownEnvironment() *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeUnknownEnvironment,
		"environment is not supported in Phase 0.5")
}

// coreInvalidQuery is the pinned bad-query 400; it names the parameter, never
// its value.
func coreInvalidQuery(param, reason string) *apierr.Error {
	return &apierr.Error{
		Status:  http.StatusBadRequest,
		Code:    apierr.CodeInvalidQuery,
		Message: "query parameter " + param + ": " + reason,
	}
}

// coreNoReadyMachine is the failFast capacity refusal. The pin calls the
// status 503 and the code no_ready_machine; the retry hint stays positive.
func coreNoReadyMachine(retrySeconds int) *apierr.Error {
	return apierr.New(http.StatusServiceUnavailable, apierr.CodeNoReadyMachine,
		"no machine of this type is ready").Retryable(true).RetryAfterSeconds(retrySeconds)
}

// coreOutOfCapacity is the queued create's capacity refusal.
func coreOutOfCapacity(retrySeconds int) *apierr.Error {
	return apierr.New(http.StatusServiceUnavailable, apierr.CodeOutOfCapacity,
		"no capacity is available right now").Retryable(true).RetryAfterSeconds(retrySeconds)
}

// coreConflict is a safe 409 without details.
func coreConflict(message string) *apierr.Error {
	return apierr.New(http.StatusConflict, apierr.CodeConflict, message)
}
