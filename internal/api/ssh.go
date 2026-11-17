package api

// SSH key endpoint (B4.1). POST /api/v1/sandboxes/{sandboxId}/sshkey installs
// one OpenSSH public key through the node's AddSSHKey and persists the
// node-confirmed endpoint and host key for that exact boot. The node owns the
// key installation and the connection metadata; this handler owns request
// validation, the owner-scoped lookup, the state gate and the fenced metadata
// write.
//
// The metadata write happens only after AddSSHKey succeeded and the response
// passed validation. It carries the node and lifecycle generation read from
// the owner-scoped row before the RPC, never the row version, so an unrelated
// PATCH during the call is harmless while a stop or a resume makes the write
// stale. The database clears the pair in every state change that ends a boot,
// so a late response can never resurrect old endpoint or host-key metadata.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// PathSandboxSSHKey is the pinned path of the SSH key endpoint. The server must
// register it with the POST method; the handler does not re-check the method.
const PathSandboxSSHKey = "/api/v1/sandboxes/{sandboxId}/sshkey"

// TypeSSHKeyConfigured is the pinned success discriminator of the SSH key
// response.
const TypeSSHKeyConfigured = "ssh_key.configured"

// sshHostKeyRetrySeconds is the retry hint on the pinned 503 answer. The
// pinned error model sends details.retryAfterSeconds there; clients retry
// after a few seconds once the guest has generated its host key.
const sshHostKeyRetrySeconds = 5

// SSHKeyRequest is the pinned request body of POST .../sshkey.
type SSHKeyRequest struct {
	// Key is one OpenSSH public key line. Private keys and options are not
	// accepted.
	Key string `json:"key"`
}

// SSHKeyResponse is the pinned success envelope of POST .../sshkey.
// MachineIP is the native unbracketed host and SSHEndpoint is the rendered
// host:port pair a client dials.
type SSHKeyResponse struct {
	SuccessBase
	Success     bool   `json:"success"`
	MachineIP   string `json:"machineIp"`
	SSHUser     string `json:"sshUser"`
	SSHEndpoint string `json:"sshEndpoint"`
	HostKey     string `json:"hostKey"`
}

// SSHKeyStore is the owner-scoped persistence the SSH key handler needs.
// Every method takes the caller's account UUID explicitly; implementations
// must treat a row owned by another account exactly like a missing one. The
// accepted [*db.Store] satisfies it directly.
type SSHKeyStore interface {
	// Sandbox returns one live sandbox owned by owner.
	Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error)
	// SetSandboxSSHAccess persists the endpoint and host key of one exact
	// boot, fenced by node and lifecycle generation.
	SetSandboxSSHAccess(ctx context.Context, params db.SetSSHAccessParams) (db.Sandbox, error)
}

// The accepted database facade satisfies the interface directly.
var _ SSHKeyStore = (*db.Store)(nil)

// SSHKeyOptions are the dependencies of the SSH key handler.
type SSHKeyOptions struct {
	// Store is the owner-scoped persistence. Required.
	Store SSHKeyStore
	// Clients returns a client for the sandbox's node. Required. The
	// server passes its gateway adapter, the same seam commands use.
	Clients NodeClientFactory
	// Logger receives operational failures. Nil discards them.
	Logger *slog.Logger
}

// sshKeyHandler serves POST /api/v1/sandboxes/{sandboxId}/sshkey. It holds
// only the store, the node-client seam and a logger: no hub, node credential
// or scheduler instance.
type sshKeyHandler struct {
	store   SSHKeyStore
	clients NodeClientFactory
	logger  *slog.Logger
}

// NewSSHKeyHandler returns the SSH key handler. The caller registers it for
// "POST "+[PathSandboxSSHKey]. The handler authenticates the tenant, validates
// the public key, runs AddSSHKey on the sandbox's node and persists only a
// validated success for the same boot.
func NewSSHKeyHandler(options SSHKeyOptions) (http.Handler, error) {
	if options.Store == nil {
		return nil, errors.New("api: ssh key handler needs a store")
	}
	if options.Clients == nil {
		return nil, errors.New("api: ssh key handler needs a node client factory")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &sshKeyHandler{store: options.Store, clients: options.Clients, logger: logger}, nil
}

// ServeHTTP runs one SSH key request.
func (h *sshKeyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
		apierr.WriteError(w, r, sshKeyNotFound())
		return
	}
	request, apiErr := decodeSSHKeyRequest(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}

	row, apiErr := h.lookup(ctx, owner, sandboxID)
	if apiErr != nil {
		if ctx.Err() == nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	if ctx.Err() != nil {
		return
	}
	if apiErr := sshKeyReady(row, owner, sandboxID); apiErr != nil {
		if ctx.Err() == nil {
			apierr.WriteError(w, r, apiErr)
		}
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

	access, apiErr := h.addKey(ctx, client, sandboxID, request.Key)
	if apiErr != nil {
		if ctx.Err() == nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	if ctx.Err() != nil {
		return
	}

	stored, apiErr := h.persist(ctx, owner, row, access)
	if apiErr != nil {
		if ctx.Err() == nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	if ctx.Err() != nil {
		return
	}
	if !sshKeyStoredRowMatches(stored, owner, row, access) {
		h.logger.ErrorContext(ctx, "api: ssh key store returned a different boot",
			"sandbox_id", sandboxID, "node_id", nodeID)
		apierr.WriteError(w, r, sshKeyInternal())
		return
	}

	apierr.Write(w, http.StatusOK, TypeSSHKeyConfigured, SSHKeyResponse{
		OK:          true,
		Type:        TypeSSHKeyConfigured,
		Success:     true,
		MachineIP:   access.MachineIP,
		SSHUser:     access.User,
		SSHEndpoint: access.Endpoint,
		HostKey:     access.HostKey,
	})
}

// decodeSSHKeyRequest parses and validates the pinned body. The key is
// canonicalized to one space-separated OpenSSH line before any lookup or RPC.
func decodeSSHKeyRequest(r *http.Request) (SSHKeyRequest, *apierr.Error) {
	var request SSHKeyRequest
	if err := DecodeJSON(r, &request); err != nil {
		return request, sshKeyInvalidJSON()
	}
	key, ok := canonicalSSHPublicKey(request.Key)
	if !ok {
		if strings.TrimSpace(request.Key) == "" {
			return request, apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON, "key is required")
		}
		return request, apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON,
			"key must be one OpenSSH public key line")
	}
	request.Key = key
	return request, nil
}

// lookup resolves one owner-scoped sandbox. Foreign and missing rows are the
// same 404; a request canceled while the store call runs writes nothing.
func (h *sshKeyHandler) lookup(ctx context.Context, owner uuid.UUID, sandboxID string) (db.Sandbox, *apierr.Error) {
	row, err := h.store.Sandbox(ctx, owner, sandboxID)
	if err != nil {
		if ctx.Err() != nil {
			return db.Sandbox{}, nil
		}
		if errors.Is(err, db.ErrNotFound) {
			return db.Sandbox{}, sshKeyNotFound()
		}
		h.logger.ErrorContext(ctx, "api: ssh key sandbox lookup failed",
			"sandbox_id", sandboxID, "error", sshKeyErrorName(err))
		return db.Sandbox{}, sshKeyInternal()
	}
	return row, nil
}

// sshKeyReady gates the node call on the durable row identity and state. A
// store that returns another owner's row, another sandbox id or a deleted row
// is a contract violation, not a client error. SSH works only while a machine
// is running, so every other state is the pinned 400 machine_not_running
// rather than the command/file 409.
func sshKeyReady(row db.Sandbox, owner uuid.UUID, sandboxID string) *apierr.Error {
	if row.ID != sandboxID || row.OwnerID != owner || row.DeletedAt != nil {
		return sshKeyInternal()
	}
	switch row.State {
	case db.StateReady, db.StateIdle, db.StateRunning:
	default:
		return sshKeyMachineNotRunning()
	}
	if row.NodeID == nil || *row.NodeID == "" {
		return sshKeyMachineNotRunning()
	}
	return nil
}

// dial returns the node client of nodeID and releases it when done. Any
// cleanup the factory supplies is called even on failure; a factory that
// cancels the request writes no error response. A nil client is a wiring
// error, not a node failure.
func (h *sshKeyHandler) dial(ctx context.Context, nodeID string) (nodev1.NodeClient, func() error, *apierr.Error) {
	client, closeClient, err := h.clients(nodeID)
	if closeClient == nil {
		closeClient = func() error { return nil }
	}
	if err != nil {
		h.closeClient(closeClient)
		if ctx.Err() != nil {
			return nil, nil, nil
		}
		h.logger.Error("api: ssh key node client", "node_id", nodeID, "error", sshKeyErrorName(err))
		return nil, nil, sshKeyDialError(err)
	}
	if client == nil {
		h.closeClient(closeClient)
		if ctx.Err() != nil {
			return nil, nil, nil
		}
		h.logger.Error("api: ssh key node client is nil", "node_id", nodeID)
		return nil, nil, sshKeyInternal()
	}
	return client, closeClient, nil
}

// closeClient releases a node client, logging a failure without failing the
// already-finished response.
func (h *sshKeyHandler) closeClient(closeClient func() error) {
	if closeClient == nil {
		return
	}
	if err := closeClient(); err != nil {
		h.logger.Debug("api: close ssh key node client", "error", sshKeyErrorName(err))
	}
}

// addKey installs the public key on the node and validates the returned
// connection metadata. The node's error text and the key never reach a log.
func (h *sshKeyHandler) addKey(ctx context.Context, client nodev1.NodeClient, sandboxID, key string) (sshKeyAccess, *apierr.Error) {
	response, err := client.AddSSHKey(ctx, &nodev1.AddSSHKeyRequest{
		SandboxId: sandboxID,
		PublicKey: key,
	})
	if err != nil {
		if ctx.Err() != nil {
			return sshKeyAccess{}, nil
		}
		h.logger.WarnContext(ctx, "api: ssh key node call failed",
			"sandbox_id", sandboxID, "error", sshKeyErrorName(err))
		return sshKeyAccess{}, sshKeyNodeError(err)
	}
	return sshKeyAccessFromNode(response)
}

// persist writes the node-confirmed metadata for the boot captured by row.
func (h *sshKeyHandler) persist(ctx context.Context, owner uuid.UUID, row db.Sandbox, access sshKeyAccess) (db.Sandbox, *apierr.Error) {
	stored, err := h.store.SetSandboxSSHAccess(ctx, db.SetSSHAccessParams{
		Owner:      owner,
		SandboxID:  row.ID,
		NodeID:     *row.NodeID,
		Generation: row.LifecycleGeneration,
		Endpoint:   access.Endpoint,
		HostKey:    access.HostKey,
	})
	if err != nil {
		if ctx.Err() != nil {
			return db.Sandbox{}, nil
		}
		switch {
		case errors.Is(err, db.ErrNotFound):
			return db.Sandbox{}, sshKeyNotFound()
		case errors.Is(err, db.ErrSSHAccessStale):
			return db.Sandbox{}, sshKeyMachineNotRunning()
		}
		h.logger.ErrorContext(ctx, "api: ssh key metadata write failed",
			"sandbox_id", row.ID, "error", sshKeyErrorName(err))
		return db.Sandbox{}, sshKeyInternal()
	}
	return stored, nil
}

// sshKeyStoredRowMatches proves the write landed on the exact live sandbox
// boot the RPC ran against. The handler never answers 200 for another owner,
// row, deleted row, non-running state, node, generation or pair.
func sshKeyStoredRowMatches(stored db.Sandbox, owner uuid.UUID, row db.Sandbox, access sshKeyAccess) bool {
	if stored.ID != row.ID || stored.OwnerID != owner || stored.DeletedAt != nil {
		return false
	}
	switch stored.State {
	case db.StateReady, db.StateIdle, db.StateRunning:
	default:
		return false
	}
	if stored.NodeID == nil || row.NodeID == nil || *stored.NodeID != *row.NodeID {
		return false
	}
	if stored.LifecycleGeneration != row.LifecycleGeneration {
		return false
	}
	return stored.SSH != nil &&
		stored.SSH.Endpoint == access.Endpoint &&
		stored.SSH.HostKey == access.HostKey
}

// sshKeyDialError maps a node-client factory failure. RESOURCE_EXHAUSTED is
// the approved 502 retryable=false exception regardless of origin; every other
// dial failure stays the ambiguous retryable 502 because AddSSHKey is
// idempotent. A factory UNAVAILABLE is not the node's host-key 503.
func sshKeyDialError(err error) *apierr.Error {
	if st, ok := status.FromError(err); ok && st.Code() == codes.ResourceExhausted {
		return sshKeyDirectFailed(false)
	}
	return sshKeyDirectFailed(true)
}

// sshKeyNodeError maps an AddSSHKey failure to the approved sshKey table. The
// node's own text is never forwarded.
func sshKeyNodeError(err error) *apierr.Error {
	st, ok := status.FromError(err)
	if !ok {
		return sshKeyDirectFailed(true)
	}
	switch st.Code() {
	case codes.InvalidArgument:
		return apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON,
			"key must be one OpenSSH public key line")
	case codes.NotFound:
		return sshKeyNotFound()
	case codes.FailedPrecondition:
		return sshKeyMachineNotRunning()
	case codes.Unavailable:
		return sshKeyHostKeyUnavailable()
	case codes.ResourceExhausted:
		// A size or transport limit cannot be fixed by retrying.
		return sshKeyDirectFailed(false)
	default:
		// AddSSHKey is idempotent: retrying the same key is safe.
		return sshKeyDirectFailed(true)
	}
}

// sshKeyErrorName classifies a failure for logs without echoing node, store or
// key text.
func sshKeyErrorName(err error) string {
	switch {
	case errors.Is(err, db.ErrNotFound):
		return "not_found"
	case errors.Is(err, db.ErrSSHAccessStale):
		return "ssh_access_stale"
	case errors.Is(err, db.ErrInvalidSSHAccess):
		return "invalid_ssh_access"
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

// sshKeyNotFound is the owner-scoped missing-sandbox answer.
func sshKeyNotFound() *apierr.Error {
	return apierr.New(http.StatusNotFound, apierr.CodeNotFound, "not found")
}

// sshKeyMachineNotRunning is the pinned SSH state and stale-fence answer. The
// cached API table puts SSH key in the 400 machine_not_running group, not in
// the command/file 409 group.
func sshKeyMachineNotRunning() *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeMachineNotRunning, "sandbox has no running machine")
}

// sshKeyHostKeyUnavailable is the retryable 503 while the guest host key is
// not generated yet.
func sshKeyHostKeyUnavailable() *apierr.Error {
	return apierr.New(http.StatusServiceUnavailable, apierr.CodeSSHHostKeyUnavailable,
		"ssh host key is not available yet").RetryAfterSeconds(sshHostKeyRetrySeconds)
}

// sshKeyDirectFailed is the ambiguous node failure. Retryable is true for
// every cause where repeating the idempotent AddSSHKey is safe.
func sshKeyDirectFailed(retryable bool) *apierr.Error {
	return apierr.New(http.StatusBadGateway, apierr.CodeSandboxDirectFailed,
		"the sandbox machine could not complete the call").Retryable(retryable)
}

// sshKeyInternal is the safe 500 for failures that are not the node's answer.
func sshKeyInternal() *apierr.Error {
	return apierr.New(http.StatusInternalServerError, apierr.CodeInternal, "internal server error")
}

// sshKeyInvalidJSON is the pinned bad-body answer.
func sshKeyInvalidJSON() *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON, "request body is not valid JSON")
}
