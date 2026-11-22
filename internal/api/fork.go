package api

// Forking: POST /api/v1/sandboxes/{sandboxId}/fork creates a new sandbox from
// one generation of an existing sandbox. The API role resolves the source and
// authorizes the request; it never cuts a generation (that needs the source's
// per-sandbox guard and a node call, which belong to the lifecycle role), so
// the accepted fork is a durable job.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"net/http"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
)

// TypeSandboxForking is the success discriminator of a forked sandbox.
const TypeSandboxForking = "sandbox.forking"

// codeForkFailed is the pinned 409 for a source whose stored data cannot start
// a fork; sandbox_stopping and sandbox_resuming are the pinned state conflicts
// a resume uses.
const codeForkFailed = "fork_failed"

// ForkStore is the owner-scoped persistence the fork handler reads. [*db.Store]
// satisfies it directly.
type ForkStore interface {
	// Sandbox returns one live sandbox owned by owner.
	Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error)
	// LatestGeneration returns the sandbox's newest completed, unpurged
	// generation; ok is false when there is none.
	LatestGeneration(ctx context.Context, owner uuid.UUID, sandboxID string) (db.Generation, bool, error)
}

var _ ForkStore = (*db.Store)(nil)

// ForkRequest is the API-owned fork admission contract. It carries the
// owner-scoped source row the handler read, the newest generation (nil when the
// source has none) and the validated request; placement and the durable accept
// belong to the admission implementation, exactly like a create.
type ForkRequest struct {
	Owner     uuid.UUID
	SandboxID string
	// SourceID names the sandbox being forked.
	SourceID string
	Source   db.Sandbox
	// Latest is the source's newest completed, unpurged generation, if any.
	// A ready source is cut fresh by the fork job; a stopped source restores
	// this one, so the job's generation is pinned at admission.
	Latest *db.Generation
	// MachineType and Resources are the fork's shape; Resources.DiskGiB is
	// already grown to hold the source's disk (a fork never shrinks it).
	MachineType string
	Resources   db.Resources
	Labels      map[string]string
	// Env is the replacement env; InheritEnv copies the source's sealed env
	// inside the admission transaction instead.
	Env        map[string]string
	InheritEnv bool
	// TTLSeconds is the validated auto-stop length; nil disables it. The
	// fork never inherits the source's TTL.
	TTLSeconds     *int
	FailFast       bool
	IdempotencyKey string
	RequestSHA256  [sha256.Size]byte
}

// ForkResult is one accepted fork.
type ForkResult struct {
	Sandbox  db.Sandbox
	Replayed bool
}

// ForkAdmission durably accepts one fork: it places the fork's node and writes
// the sandbox, its reservation and its job. The HTTP handler never touches a
// node, the node hub or any node credential.
type ForkAdmission interface {
	Fork(ctx context.Context, request ForkRequest) (ForkResult, error)
}

// ForkAdmissionFunc adapts a plain function to [ForkAdmission].
type ForkAdmissionFunc func(ctx context.Context, request ForkRequest) (ForkResult, error)

// Fork implements [ForkAdmission].
func (f ForkAdmissionFunc) Fork(ctx context.Context, request ForkRequest) (ForkResult, error) {
	return f(ctx, request)
}

// ForkOptions are the dependencies of the fork handler.
type ForkOptions struct {
	// Store is the owner-scoped persistence. Required.
	Store ForkStore
	// Admission places and durably accepts forks. Required.
	Admission ForkAdmission
	// ObjectStore reports whether the installation has an object store. A
	// fork without one answers 409 snapshots_disabled, naming it (no fork
	// can be restored with nowhere to read the generation from).
	ObjectStore bool
	// Logger receives operational failures. Nil discards them.
	Logger *slog.Logger
	// CapacityRetrySeconds is the positive retry hint on a capacity
	// refusal. Zero or negative uses DefaultCapacityRetrySeconds.
	CapacityRetrySeconds int
}

// forkHandler serves POST /api/v1/sandboxes/{sandboxId}/fork.
type forkHandler struct {
	store         ForkStore
	admission     ForkAdmission
	objectStore   bool
	logger        *slog.Logger
	capacityRetry int
}

// NewForkHandler returns the handler for POST
// /api/v1/sandboxes/{sandboxId}/fork. The caller registers it with [Register]
// on [Handlers.ForkSandbox].
func NewForkHandler(options ForkOptions) (http.HandlerFunc, error) {
	if options.Store == nil {
		return nil, errors.New("api: fork handler needs a store")
	}
	if options.Admission == nil {
		return nil, errors.New("api: fork handler needs an admission implementation")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	capacityRetry := options.CapacityRetrySeconds
	if capacityRetry <= 0 {
		capacityRetry = DefaultCapacityRetrySeconds
	}
	h := &forkHandler{
		store:         options.Store,
		admission:     options.Admission,
		objectStore:   options.ObjectStore,
		logger:        logger,
		capacityRetry: capacityRetry,
	}
	return h.serveHTTP, nil
}

func (h *forkHandler) serveHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if ctx.Err() != nil {
		return
	}
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	sourceID := r.PathValue(ParamSandboxID)
	if sourceID == "" {
		apierr.WriteError(w, r, coreNotFound())
		return
	}
	values, apiErr := coreParseQuery(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	if apiErr := coreRejectUnknownQuery(values); apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	input, apiErr := parseForkSandbox(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	if !h.objectStore {
		// No generation could be read back; answer before any source work.
		apierr.WriteError(w, r, forkObjectStoreMissing())
		return
	}

	source, err := h.store.Sandbox(ctx, owner, sourceID)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, db.ErrNotFound) {
			apierr.WriteError(w, r, coreNotFound())
			return
		}
		h.logger.ErrorContext(ctx, "api: fork source lookup failed", "error", coreErrorName(err))
		apierr.WriteError(w, r, coreInternal())
		return
	}
	if source.ID != sourceID || source.OwnerID != owner || source.DeletedAt != nil {
		// The store contract makes this unreachable; fail closed rather than
		// fork another account's data.
		h.logger.ErrorContext(ctx, "api: fork source identity mismatch", "sandbox_id", sourceID)
		apierr.WriteError(w, r, coreInternal())
		return
	}
	if !source.SnapshotsEnabled {
		apierr.WriteError(w, r, forkSnapshotsDisabled())
		return
	}
	if source.DataExpiredAt != nil {
		apierr.WriteError(w, r, forkDataExpired())
		return
	}
	latest, hasLatest, err := h.store.LatestGeneration(ctx, owner, sourceID)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		h.logger.ErrorContext(ctx, "api: fork source generation lookup failed", "error", coreErrorName(err))
		apierr.WriteError(w, r, coreInternal())
		return
	}
	var latestPtr *db.Generation
	if hasLatest {
		latestPtr = &latest
	}
	if apiErr := forkSourceState(source); apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	if source.State == db.StateArchived && !hasLatest {
		apierr.WriteError(w, r, forkNoGeneration())
		return
	}
	machine, reserve, apiErr := forkShape(source, input.machine, latestPtr)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}

	result, err := h.admission.Fork(ctx, ForkRequest{
		Owner:          owner,
		SandboxID:      ids.Sandbox(),
		SourceID:       sourceID,
		Source:         source,
		Latest:         latestPtr,
		MachineType:    machine.Name,
		Resources:      reserve,
		Labels:         input.labels,
		Env:            input.env,
		InheritEnv:     input.inheritEnv,
		TTLSeconds:     input.ttl,
		FailFast:       input.failFast,
		IdempotencyKey: input.idempotencyKey,
		RequestSHA256:  input.requestSHA256,
	})
	if err != nil {
		if apiErr := h.admissionFailure(ctx, input.failFast, err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	if result.Sandbox.ID == "" || result.Sandbox.OwnerID != owner || result.Sandbox.ID == sourceID {
		h.logger.ErrorContext(ctx, "api: fork admission returned a malformed or foreign sandbox")
		apierr.WriteError(w, r, coreInternal())
		return
	}
	fork := sandboxFromRow(result.Sandbox)
	apierr.Write(w, http.StatusAccepted, TypeSandboxForking,
		NewForkActionResponse(result.Sandbox.ID, fork))
}

// forkInput is the validated optional fork body.
type forkInput struct {
	machine        machinetype.Type
	labels         map[string]string
	env            map[string]string
	inheritEnv     bool
	ttl            *int
	failFast       bool
	idempotencyKey string
	requestSHA256  [sha256.Size]byte
}

// ForkSandboxRequest is the pinned fork request body.
type ForkSandboxRequest struct {
	Type        *string           `json:"type,omitzero"`
	TTLSeconds  TTLSeconds        `json:"ttlSeconds,omitzero"`
	Labels      map[string]string `json:"labels,omitzero"`
	Env         map[string]string `json:"env,omitzero"`
	Environment *string           `json:"environment,omitzero"`
	NoEnv       *bool             `json:"noEnv,omitzero"`
	Monitoring  *bool             `json:"monitoring,omitzero"`
	FailFast    *bool             `json:"failFast,omitzero"`
}

// parseForkSandbox reads an optional fork body. An omitted body is the pinned
// default (no members): the fork inherits the source's type and env, gets no
// labels, does not inherit the source's TTL and auto-stops after
// [DefaultTTLSeconds]. The idempotency hash covers the exact request bytes.
func parseForkSandbox(r *http.Request) (forkInput, *apierr.Error) {
	raw, present, readErr := coreReadCreateBody(r)
	if readErr != nil {
		return forkInput{}, coreInvalidJSON("request body is not valid JSON")
	}
	var request ForkSandboxRequest
	hash := sha256.Sum256([]byte("{}"))
	if present {
		if apiErr := coreRequireJSONMedia(r); apiErr != nil {
			return forkInput{}, apiErr
		}
		if apiErr := forkValidateSyntax(raw); apiErr != nil {
			return forkInput{}, apiErr
		}
		if err := json.Unmarshal(raw, &request, json.RejectUnknownMembers(true)); err != nil {
			return forkInput{}, coreInvalidJSON("request body is not valid JSON")
		}
		hash = sha256.Sum256(raw)
	}

	if request.Environment != nil && *request.Environment != "base" {
		return forkInput{}, coreUnknownEnvironment()
	}
	if request.Monitoring != nil {
		// Same refusal create gives: monitoring is a durable per-sandbox
		// policy this phase does not store.
		return forkInput{}, coreInvalidJSON("monitoring is not supported in Phase 0.5")
	}
	var machine machinetype.Type
	if request.Type != nil {
		looked, ok := machinetype.Lookup(*request.Type)
		if !ok || *request.Type == "" {
			return forkInput{}, coreInvalidJSON("type must be a supported machine type")
		}
		machine = looked
	}
	ttl := request.TTLSeconds
	var ttlValue *int
	if !ttl.Present {
		value := DefaultTTLSeconds
		ttlValue = &value
	} else {
		ttlValue = ttl.Value
	}
	if ttlValue != nil && (*ttlValue < 1 || *ttlValue > coreMaxTTLSeconds) {
		return forkInput{}, coreInvalidJSON("ttlSeconds must be an integer from 1 to 2592000, null or infinite")
	}
	if apiErr := coreValidateLabels(request.Labels); apiErr != nil {
		return forkInput{}, apiErr
	}
	if apiErr := coreValidateEnv(request.Env); apiErr != nil {
		return forkInput{}, apiErr
	}
	key, keyErr := ReadIdempotencyKey(r)
	if keyErr != nil {
		return forkInput{}, coreInvalidJSON("Idempotency-Key must be at most 255 characters")
	}
	return forkInput{
		machine:        machine,
		labels:         request.Labels,
		env:            request.Env,
		inheritEnv:     !forkEnvPresent(raw, present),
		ttl:            ttlValue,
		failFast:       request.FailFast != nil && *request.FailFast,
		idempotencyKey: key,
		requestSHA256:  hash,
	}, nil
}

// forkEnvPresent reports whether the body carried an env member (including an
// empty object). An omitted env inherits the source's env; a present one
// replaces it, so an empty object means the fork has no env.
func forkEnvPresent(raw []byte, present bool) bool {
	if !present {
		return false
	}
	var syntax struct {
		Env jsontext.Value `json:"env,omitzero"`
	}
	if err := json.Unmarshal(raw, &syntax); err != nil {
		return false
	}
	return !coreJSONAbsent(syntax.Env)
}

// forkValidateSyntax enforces the pinned member kinds before the typed decode.
// No fork member is nullable, so an explicit null is invalid_json.
func forkValidateSyntax(raw []byte) *apierr.Error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return coreInvalidJSON("request body must be a JSON object")
	}
	var syntax struct {
		Type        jsontext.Value `json:"type,omitzero"`
		TTLSeconds  jsontext.Value `json:"ttlSeconds,omitzero"`
		Labels      jsontext.Value `json:"labels,omitzero"`
		Env         jsontext.Value `json:"env,omitzero"`
		Environment jsontext.Value `json:"environment,omitzero"`
		NoEnv       jsontext.Value `json:"noEnv,omitzero"`
		Monitoring  jsontext.Value `json:"monitoring,omitzero"`
		FailFast    jsontext.Value `json:"failFast,omitzero"`
	}
	if err := json.Unmarshal(raw, &syntax, json.RejectUnknownMembers(true)); err != nil {
		return coreInvalidJSON("request body is not valid JSON")
	}
	checks := []struct {
		field    string
		value    jsontext.Value
		ok       func(jsontext.Value) bool
		expected string
	}{
		{"type", syntax.Type, coreJSONString, "a string"},
		{"environment", syntax.Environment, coreJSONString, "a string"},
		{"noEnv", syntax.NoEnv, coreJSONBool, "a boolean"},
		{"monitoring", syntax.Monitoring, coreJSONBool, "a boolean"},
		{"failFast", syntax.FailFast, coreJSONBool, "a boolean"},
	}
	for _, check := range checks {
		if coreJSONAbsent(check.value) {
			continue
		}
		if !check.ok(check.value) {
			return coreInvalidJSON(check.field + " must be " + check.expected)
		}
	}
	if !coreJSONAbsent(syntax.TTLSeconds) {
		value := coreJSONTrim(syntax.TTLSeconds)
		if !bytes.Equal(value, []byte("null")) && !coreJSONString(value) && !coreJSONInteger(value) {
			return coreInvalidJSON(`ttlSeconds must be an integer, null or "infinite"`)
		}
	}
	if !coreJSONAbsent(syntax.Labels) {
		if !coreJSONObject(syntax.Labels) || !coreStringMapShape(syntax.Labels) {
			return coreInvalidJSON("labels must be an object of string values")
		}
	}
	if !coreJSONAbsent(syntax.Env) {
		if !coreJSONObject(syntax.Env) {
			return coreInvalidJSON("env must be an object of string values")
		}
		if !coreStringMapShape(syntax.Env) {
			return coreInvalidEnv("env values must be strings")
		}
	}
	return nil
}

// forkSourceState maps the source's state to the pinned 409 (or nil when the
// source can be forked). A ready-ish source is cut fresh; an archived one
// restores its latest generation. A starting sandbox is still resuming, a
// stopping one still has its stop in flight, and an errored one has no
// data a fork may claim.
func forkSourceState(source db.Sandbox) *apierr.Error {
	switch source.State {
	case db.StateReady, db.StateIdle, db.StateRunning, db.StateArchived:
		return nil
	case db.StateArchiving:
		return sandboxStopping(source)
	case db.StateProvisioning, db.StateProvisioned, db.StateCloning:
		return apierr.New(http.StatusConflict, apierr.CodeSandboxResuming,
			"the sandbox is still starting; fork it once it is ready").Retryable(true)
	default:
		return forkFailed("the sandbox stopped with an error and cannot be forked")
	}
}

// forkShape resolves the fork's machine type and reservation. An omitted type
// inherits the source's type; a type whose disk is smaller than the source's
// data is 409 type_too_small. The reservation never shrinks below the source's
// disk or the generation's disk (A2).
func forkShape(source db.Sandbox, requested machinetype.Type, latest *db.Generation) (machinetype.Type, db.Resources, *apierr.Error) {
	machine := requested
	if machine.Name == "" {
		inherited, ok := machinetype.Lookup(source.MachineType)
		if !ok {
			return machinetype.Type{}, db.Resources{}, forkFailed("the sandbox machine type is unknown")
		}
		machine = inherited
	}
	sourceDisk := max(source.Reserved.DiskGiB, source.Desired.DiskGiB)
	needDisk := sourceDisk
	if latest != nil {
		needDisk = max(needDisk, db.DiskGiBFor(latest.DiskBytes))
	}
	if machine.DiskGiB < needDisk {
		return machinetype.Type{}, db.Resources{}, apierr.New(http.StatusConflict, "type_too_small",
			"the sandbox data is larger than the requested machine type's disk")
	}
	// A2: the reservation disk is the larger of the type's and the source's,
	// so a restore never shrinks the disk under the data.
	return machine, db.Resources{
		VCPUs:     machine.VCPUs,
		MemoryMiB: machine.MemoryMiB,
		DiskGiB:   max(machine.DiskGiB, needDisk),
	}, nil
}

// admissionFailure maps a fork admission failure to the pinned safe error.
func (h *forkHandler) admissionFailure(ctx context.Context, failFast bool, err error) *apierr.Error {
	if ctx.Err() != nil {
		return nil
	}
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
	case errors.Is(err, db.ErrNotFound):
		return coreNotFound()
	case errors.Is(err, db.ErrSnapshotsDisabled):
		return forkSnapshotsDisabled()
	case errors.Is(err, db.ErrSnapshotDataExpired):
		return forkDataExpired()
	case errors.Is(err, db.ErrSnapshotNotFound):
		return apierr.New(http.StatusNotFound, apierr.CodeSnapshotNotFound,
			"the source snapshot is no longer available")
	case errors.Is(err, db.ErrSnapshotDiskTooSmall), errors.Is(err, db.ErrTypeTooSmall):
		return apierr.New(http.StatusConflict, "type_too_small",
			"the sandbox data is larger than the requested machine type's disk")
	case errors.Is(err, db.ErrIdempotencyReused):
		return apierr.New(http.StatusConflict, apierr.CodeIdempotencyKeyReused,
			"idempotency key was reused with a different request")
	case errors.Is(err, db.ErrIdempotencyInProgress):
		return apierr.New(http.StatusConflict, apierr.CodeIdempotencyInProgress,
			"a fork with this idempotency key is still in progress").Retryable(true)
	}
	h.logger.ErrorContext(ctx, "api: fork admission failed", "error", coreErrorName(err))
	return coreInternal()
}

// forkObjectStoreMissing is the D6 refusal when the installation has no object
// store: the generation could not be read back.
func forkObjectStoreMissing() *apierr.Error {
	return apierr.New(http.StatusConflict, apierr.CodeSnapshotsDisabled,
		"fork needs the object store, and this installation has none configured").Retryable(false)
}

// forkSnapshotsDisabled is the refusal for a source created with snapshots off.
func forkSnapshotsDisabled() *apierr.Error {
	return apierr.New(http.StatusConflict, apierr.CodeSnapshotsDisabled,
		"the sandbox was created with snapshots off, so it has no snapshot to fork").Retryable(false)
}

// forkDataExpired is the refusal for a stopped sandbox whose stored data was
// deleted after its retention window.
func forkDataExpired() *apierr.Error {
	return apierr.New(http.StatusConflict, apierr.CodeSnapshotDataExpired,
		"the sandbox data expired and can no longer be forked").Retryable(false)
}

// forkNoGeneration is the refusal for a stopped sandbox without a completed
// generation, so there is no disk to restore.
func forkNoGeneration() *apierr.Error {
	return forkFailed("the sandbox has no completed snapshot to fork").Retryable(false)
}

// forkFailed is the generic 409 for a source whose data cannot start a fork.
func forkFailed(message string) *apierr.Error {
	return apierr.New(http.StatusConflict, codeForkFailed, message)
}

// NewForkActionResponse builds the pinned 202 fork acceptance envelope. The
// response id is the fork's sandbox id, never the source's.
func NewForkActionResponse(forkID string, sandbox Sandbox) SandboxActionResponse {
	return SandboxActionResponse{
		SuccessBase: success(TypeSandboxForking),
		ID:          forkID,
		Status:      "forking",
		Sandbox:     &sandbox,
	}
}
