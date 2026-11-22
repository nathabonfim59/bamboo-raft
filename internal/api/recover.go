package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// TypeSandboxRecovering is the success discriminator for POST
// /api/v1/sandboxes/{sandboxId}/recover.
const TypeSandboxRecovering = "sandbox.recovering"

// DefaultRecoverProbeTimeout bounds the synchronous reachability probe before
// accepting a recovery.
const DefaultRecoverProbeTimeout = 3 * time.Second

// recoverGetVMTimeout bounds the synchronous node liveness check.
const recoverGetVMTimeout = 5 * time.Second

// Recover 409 conflict codes defined by the v1 API.
const (
	codeRecoverNoMachine          = "no_machine"
	codeRecoverTooSoon            = "too_soon"
	codeRecoverUnsupported        = "recover_unsupported"
	codeRecoverSandboxIsReachable = "sandbox_is_reachable"
)

// RecoverResponse is the 200 envelope of POST
// /api/v1/sandboxes/{sandboxId}/recover.
type RecoverResponse struct {
	SuccessBase
	Status  string `json:"status"`
	Message string `json:"message"`
}

// RecoverStore is the owner-scoped persistence the recover handler needs.
// [*db.Store] satisfies it directly.
type RecoverStore interface {
	// Sandbox returns one live sandbox owned by owner.
	Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error)
	// AcceptRecover atomically validates cooldown/state, sets last_recover_at,
	// writes sandbox.recovering and enqueues the recovery job.
	AcceptRecover(ctx context.Context, owner uuid.UUID, sandboxID string, now time.Time) (db.Sandbox, error)
}

var _ RecoverStore = (*db.Store)(nil)

// RecoverOptions are the dependencies of the sandbox recovery handler.
type RecoverOptions struct {
	// Store is the owner-scoped persistence. Required.
	Store RecoverStore
	// Clients returns a node client bound to a node id. Required.
	Clients NodeClientFactory
	// Logger receives operational failures. Nil discards them.
	Logger *slog.Logger
	// Now returns the current time for cooldown checks. Nil uses time.Now.
	Now func() time.Time
	// ProbeTimeout bounds the reachability Exec probe. Non-positive uses
	// [DefaultRecoverProbeTimeout] (3 seconds).
	ProbeTimeout time.Duration
}

type recoverHandler struct {
	store        RecoverStore
	clients      NodeClientFactory
	logger       *slog.Logger
	now          func() time.Time
	probeTimeout time.Duration
}

// NewRecoverHandler returns the handler for POST
// /api/v1/sandboxes/{sandboxId}/recover. The caller registers it with
// [Register] on [Handlers.RecoverSandbox].
func NewRecoverHandler(options RecoverOptions) (http.HandlerFunc, error) {
	if options.Store == nil {
		return nil, errors.New("api: recover handler needs a store")
	}
	if options.Clients == nil {
		return nil, errors.New("api: recover handler needs a node client factory")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	probeTimeout := options.ProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = DefaultRecoverProbeTimeout
	}
	h := &recoverHandler{
		store:        options.Store,
		clients:      options.Clients,
		logger:       logger,
		now:          now,
		probeTimeout: probeTimeout,
	}
	return h.serveHTTP, nil
}

func (h *recoverHandler) serveHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if ctx.Err() != nil {
		return
	}
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	sandboxID := r.PathValue(ParamSandboxID)
	if sandboxID == "" {
		apierr.WriteError(w, r, coreNotFound())
		return
	}

	row, err := h.store.Sandbox(ctx, owner, sandboxID)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, db.ErrNotFound) {
			apierr.WriteError(w, r, coreNotFound())
			return
		}
		h.logger.ErrorContext(ctx, "api: recover sandbox lookup failed",
			"sandbox_id", sandboxID, "error", recoverErrorName(err))
		apierr.WriteError(w, r, coreInternal())
		return
	}
	if row.ID != sandboxID || row.OwnerID != owner || row.DeletedAt != nil {
		h.logger.ErrorContext(ctx, "api: recover sandbox lookup identity mismatch", "sandbox_id", sandboxID)
		apierr.WriteError(w, r, coreInternal())
		return
	}

	if !recoverReadyState(row.State) || row.NodeID == nil || *row.NodeID == "" {
		apierr.WriteError(w, r, recoverNoMachine())
		return
	}

	now := h.now().UTC()
	if row.LastRecoverAt != nil && now.Sub(*row.LastRecoverAt) < db.RecoverCooldown {
		apierr.WriteError(w, r, recoverTooSoon())
		return
	}

	nodeID := *row.NodeID
	client, closeClient, apiErr := h.dial(ctx, nodeID)
	if apiErr != nil {
		if ctx.Err() == nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	if ctx.Err() != nil {
		h.closeClient(closeClient)
		return
	}
	defer h.closeClient(closeClient)

	if apiErr := h.checkNodeAndVM(ctx, client, sandboxID); apiErr != nil {
		if ctx.Err() == nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	if ctx.Err() != nil {
		return
	}

	reachable, ok := h.probeReachable(ctx, client, sandboxID)
	if !ok {
		return // request cancelled
	}
	if reachable {
		apierr.WriteError(w, r, recoverSandboxIsReachable())
		return
	}

	if _, err := h.store.AcceptRecover(ctx, owner, sandboxID, h.now().UTC()); err != nil {
		if ctx.Err() != nil {
			return
		}
		switch {
		case errors.Is(err, db.ErrNotFound):
			apierr.WriteError(w, r, coreNotFound())
		case errors.Is(err, db.ErrInvalidState):
			apierr.WriteError(w, r, recoverNoMachine())
		case errors.Is(err, db.ErrTooSoon):
			apierr.WriteError(w, r, recoverTooSoon())
		default:
			h.logger.ErrorContext(ctx, "api: accept recover failed",
				"sandbox_id", sandboxID, "error", recoverErrorName(err))
			apierr.WriteError(w, r, coreInternal())
		}
		return
	}

	apierr.Write(w, http.StatusOK, TypeSandboxRecovering, RecoverResponse{
		SuccessBase: success(TypeSandboxRecovering),
		Status:      "recovering",
		Message:     "Recovery started. The virtual machine is being power-cycled with its disk intact.",
	})
}

func (h *recoverHandler) dial(ctx context.Context, nodeID string) (nodev1.NodeClient, func() error, *apierr.Error) {
	client, closeClient, err := h.clients(nodeID)
	if closeClient == nil {
		closeClient = func() error { return nil }
	}
	if err != nil {
		h.closeClient(closeClient)
		if ctx.Err() != nil {
			return nil, nil, nil
		}
		h.logger.WarnContext(ctx, "api: recover node client dial failed",
			"node_id", nodeID, "error", recoverErrorName(err))
		return nil, nil, recoverUnsupported()
	}
	if client == nil {
		h.closeClient(closeClient)
		if ctx.Err() != nil {
			return nil, nil, nil
		}
		h.logger.ErrorContext(ctx, "api: recover node client is nil", "node_id", nodeID)
		return nil, nil, coreInternal()
	}
	return client, closeClient, nil
}

func (h *recoverHandler) closeClient(closeClient func() error) {
	if closeClient == nil {
		return
	}
	if err := closeClient(); err != nil {
		h.logger.Debug("api: close recover node client", "error", recoverErrorName(err))
	}
}

// checkNodeAndVM proves the node link is up and the VM exists in a non-stopped
// state before probing the guest.
func (h *recoverHandler) checkNodeAndVM(ctx context.Context, client nodev1.NodeClient, sandboxID string) *apierr.Error {
	getCtx, cancel := context.WithTimeout(ctx, recoverGetVMTimeout)
	defer cancel()
	vm, err := client.GetVM(getCtx, &nodev1.GetVMRequest{SandboxId: sandboxID})
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if status.Code(err) == codes.NotFound {
			return recoverNoMachine()
		}
		if status.Code(err) == codes.Unavailable || getCtx.Err() != nil {
			return recoverUnsupported()
		}
		h.logger.WarnContext(ctx, "api: recover GetVM failed",
			"sandbox_id", sandboxID, "error", recoverErrorName(err))
		return coreInternal()
	}
	if vm.GetState() == nodev1.VMState_VM_STATE_STOPPED {
		return recoverNoMachine()
	}
	return nil
}

// probeReachable runs `true` inside the sandbox with a bounded deadline. It
// returns reachable=true only when the command exits 0; any timeout, non-zero
// exit or RPC error means the guest is unreachable. ok is false only when the
// caller's HTTP request context was cancelled.
func (h *recoverHandler) probeReachable(ctx context.Context, client nodev1.NodeClient, sandboxID string) (reachable, ok bool) {
	probeCtx, cancel := context.WithTimeout(ctx, h.probeTimeout)
	defer cancel()

	timeoutSecs := uint32(max(1, int(math.Ceil(h.probeTimeout.Seconds()))))
	stream, err := client.Exec(probeCtx, &nodev1.ExecRequest{
		SandboxId:      sandboxID,
		Command:        "true",
		TimeoutSeconds: timeoutSecs,
	})
	if err != nil {
		if ctx.Err() != nil {
			return false, false
		}
		return false, true
	}

	for {
		event, recvErr := stream.Recv()
		if recvErr != nil {
			if ctx.Err() != nil {
				return false, false
			}
			if errors.Is(recvErr, io.EOF) {
				return false, true
			}
			return false, true
		}
		if exit := event.GetExit(); exit != nil {
			exitedZero := !exit.GetTimedOut() &&
				exit.GetSignal() == "" &&
				!exit.GetOomKilled() &&
				exit.ExitCode != nil &&
				*exit.ExitCode == 0
			return exitedZero, true
		}
	}
}

func recoverReadyState(state db.SandboxState) bool {
	switch state {
	case db.StateReady, db.StateIdle, db.StateRunning:
		return true
	default:
		return false
	}
}

func recoverNoMachine() *apierr.Error {
	return apierr.New(http.StatusConflict, codeRecoverNoMachine,
		"sandbox has no running machine; resume it instead")
}

func recoverTooSoon() *apierr.Error {
	return apierr.New(http.StatusConflict, codeRecoverTooSoon,
		"a recovery was already started within the last 10 minutes")
}

func recoverUnsupported() *apierr.Error {
	return apierr.New(http.StatusConflict, codeRecoverUnsupported,
		"the sandbox machine cannot be power-cycled right now")
}

func recoverSandboxIsReachable() *apierr.Error {
	return apierr.New(http.StatusConflict, codeRecoverSandboxIsReachable,
		"sandbox is reachable and responding normally")
}

// recoverErrorName classifies a failure for logs without echoing node or store text.
func recoverErrorName(err error) string {
	switch {
	case errors.Is(err, db.ErrNotFound):
		return "not_found"
	case errors.Is(err, db.ErrInvalidState):
		return "invalid_state"
	case errors.Is(err, db.ErrTooSoon):
		return "too_soon"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	}
	if st, ok := status.FromError(err); ok {
		return "grpc_" + st.Code().String()
	}
	return fmt.Sprintf("%T", err)
}
