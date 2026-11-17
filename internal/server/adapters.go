// Package server wires bbraft-server's roles into one runnable daemon. Each
// role opens its own PostgreSQL pool, listener and clients; roles share only
// the configured network and database boundaries.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

// DefaultAPIKeyTouchTimeout bounds the last-used write made before a request
// reaches a handler. A slow database fails the request safely instead of
// hanging it or silently answering 401.
const DefaultAPIKeyTouchTimeout = 2 * time.Second

// APIAuthStore is the startup/credential persistence the api role needs.
// *db.Store implements it.
type APIAuthStore interface {
	EnsureAccount(ctx context.Context, name string) (db.Account, error)
	EnsureAPIKey(ctx context.Context, accountID uuid.UUID, name, prefix string, keySHA256 [32]byte) (db.APIKey, error)
	TouchAPIKeyUsed(ctx context.Context, id uuid.UUID) error
}

// authMapping is the durable identity mapping bootstrapped from the
// configured keys. Every configured owner name and key id maps to the
// account and api_keys row created (or found) at startup; request middleware
// never reparses credentials or keeps raw key material.
type authMapping struct {
	store        APIAuthStore
	log          *slog.Logger
	accounts     map[string]uuid.UUID
	keys         map[string]uuid.UUID
	touchTimeout time.Duration
}

// bootstrapAuth persists the configured key records with their owner
// accounts and key rows, then returns the mapping middleware. The raw key
// never reaches this function: auth.Keys hashed it at load time.
func bootstrapAuth(ctx context.Context, store APIAuthStore, keys *auth.Keys, prefix string, log *slog.Logger) (*authMapping, error) {
	if store == nil {
		return nil, errors.New("server: auth bootstrap needs a store")
	}
	if keys == nil {
		return nil, errors.New("server: auth bootstrap needs loaded keys")
	}
	m := &authMapping{
		store:        store,
		log:          log,
		accounts:     make(map[string]uuid.UUID),
		keys:         make(map[string]uuid.UUID),
		touchTimeout: DefaultAPIKeyTouchTimeout,
	}
	for _, record := range keys.Records() {
		account, err := store.EnsureAccount(ctx, record.Owner)
		if err != nil {
			return nil, fmt.Errorf("server: ensure account: %w", err)
		}
		key, err := store.EnsureAPIKey(ctx, account.ID, record.ID, prefix, record.Digest)
		if err != nil {
			return nil, fmt.Errorf("server: ensure api key %q: %w", record.ID, err)
		}
		m.accounts[record.Owner] = account.ID
		m.keys[record.ID] = key.ID
	}
	return m, nil
}

// Middleware maps auth.Keys' configured identity to the durable account and
// key rows before any handler runs. It records last use under a bounded
// request context; a database failure is a safe 500 before effects, never a
// silent 401.
func (m *authMapping) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		configured, ok := auth.IdentityFrom(r.Context())
		if !ok || configured.Owner == "" || configured.KeyID == "" {
			apierr.WriteError(w, r, apierr.New(http.StatusUnauthorized, apierr.CodeUnauthorized, "unauthorized"))
			return
		}
		account, ok := m.accounts[configured.Owner]
		if !ok {
			m.failure(w, r, "account mapping missing")
			return
		}
		key, ok := m.keys[configured.KeyID]
		if !ok {
			m.failure(w, r, "key mapping missing")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), m.touchTimeout)
		err := m.store.TouchAPIKeyUsed(ctx, key)
		cancel()
		if err != nil {
			if r.Context().Err() != nil {
				return // client gone; no response bytes
			}
			m.log.ErrorContext(r.Context(), "api: api key last-used update failed", "error", safeErrorName(err))
			m.failure(w, r, "key last-used update failed")
			return
		}
		identity := auth.Identity{Owner: account.String(), KeyID: key.String()}
		next.ServeHTTP(w, r.WithContext(auth.WithIdentity(r.Context(), identity)))
	})
}

// failure writes the safe internal error after logging a non-secret reason.
func (m *authMapping) failure(w http.ResponseWriter, r *http.Request, reason string) {
	m.log.ErrorContext(r.Context(), "api: authentication bootstrap failed", "reason", reason)
	apierr.WriteError(w, r, apierr.New(http.StatusInternalServerError, apierr.CodeInternal, "internal server error"))
}

// admissionPlacer is scheduler's admission surface.
type admissionPlacer interface {
	Admit(ctx context.Context, req scheduler.AdmitRequest) (db.CreateSandboxResult, error)
}

// snapshotRestoreResolver adapts the API restore policy to the scheduler's
// budget-owning resolver interface. The API package owns the wire errors; the
// scheduler only sees the resolved immutable source and machine shape.
type snapshotRestoreResolver struct {
	store api.SnapshotRestoreResolver
}

// ResolveRestore implements scheduler.RestoreResolver.
func (r snapshotRestoreResolver) ResolveRestore(ctx context.Context, owner uuid.UUID,
	name string, typeExplicit bool, requestedType string,
) (db.SnapshotSource, string, db.Resources, error) {
	target, apiErr := api.ResolveSnapshotRestore(ctx, r.store, owner, name, typeExplicit, requestedType)
	// A lookup that ended because the admission budget or the caller ended
	// must keep its context classification; the wire policy turns an
	// ordinary lookup fault into the safe 500, not a deadline.
	if err := ctx.Err(); err != nil {
		return db.SnapshotSource{}, "", db.Resources{}, err
	}
	if apiErr != nil {
		return db.SnapshotSource{}, "", db.Resources{}, apiErr
	}
	return target.Source, target.MachineType, target.Resources, nil
}

// apiAdmission adapts the accepted scheduler to the api role's admission
// contract. It is the only bridge between the two, so the API never sees a
// node, hub or gateway.
type apiAdmission struct {
	placer admissionPlacer
}

// Admit maps an API create to a scheduler admit and back. Replay, create-from
// resolution and placement all run inside the scheduler's single failFast
// budget; this adapter never reads a snapshot itself.
func (a apiAdmission) Admit(ctx context.Context, request api.AdmissionRequest) (api.AdmissionResult, error) {
	result, err := a.placer.Admit(ctx, scheduler.AdmitRequest{
		ID:             request.SandboxID,
		Owner:          request.Owner,
		MachineType:    request.MachineType,
		Name:           request.Name,
		Labels:         request.Labels,
		Env:            request.Env,
		TTLSeconds:     request.TTLSeconds,
		Reserve:        request.Resources,
		FailFast:       request.FailFast,
		IdempotencyKey: request.IdempotencyKey,
		RequestSHA256:  request.RequestSHA256[:],
		From:           request.From,
		TypeExplicit:   request.TypeExplicit,
	})
	return admissionResult(result, err)
}

// admissionResult maps the scheduler's capacity and connectivity refusals
// to the API's pinned sentinels and preserves the original classification
// alongside them, so wrapped and joined forms keep errors.Is working for
// both the scheduler cause and any context cancellation. Every other error
// is untouched.
func admissionResult(result db.CreateSandboxResult, err error) (api.AdmissionResult, error) {
	switch {
	case err == nil:
		return api.AdmissionResult{Sandbox: result.Sandbox, Replayed: result.Replayed}, nil
	case errors.Is(err, scheduler.ErrNoCapacity):
		// The API's failFast policy already splits db.ErrNoCapacity into
		// out_of_capacity and no_ready_machine; without this normalization
		// a scheduler capacity refusal is an unknown fault and becomes 500.
		return api.AdmissionResult{}, errors.Join(db.ErrNoCapacity, err)
	case errors.Is(err, scheduler.ErrNoNode):
		return api.AdmissionResult{}, errors.Join(api.ErrNoReadyMachine, err)
	default:
		return api.AdmissionResult{}, err
	}
}

// commandTargetStore is the owner-scoped read the command handler needs.
type commandTargetStore interface {
	Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error)
}

// commandTargets adapts the store to api.CommandTargetLookup.
type commandTargets struct {
	store commandTargetStore
}

// TargetLookup resolves one owner-scoped sandbox. Only db.ErrNotFound becomes
// the handler's missing-target sentinel; other store errors stay internal.
func (l commandTargets) TargetLookup(ctx context.Context, owner, sandboxID string) (api.CommandTarget, error) {
	ownerID, err := uuid.Parse(owner)
	if err != nil || ownerID == uuid.Nil() {
		return api.CommandTarget{}, errors.New("api: command owner is not an account id")
	}
	row, err := l.store.Sandbox(ctx, ownerID, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return api.CommandTarget{}, api.ErrCommandTargetNotFound
	}
	if err != nil {
		return api.CommandTarget{}, err
	}
	return api.CommandTarget{NodeID: derefString(row.NodeID), State: api.SandboxState(row.State)}, nil
}

// nodeInventoryStore is the inventory persistence the nodehub adapter uses.
type nodeInventoryStore interface {
	UpsertNodeInfo(ctx context.Context, info db.NodeInfo) error
	TouchNodeSeen(ctx context.Context, id string, at time.Time) error
}

// inventoryWriter adapts nodegateway.NodeState to the accepted inventory
// methods. The epoch ordering guarantee belongs to nodegateway's poller, so
// this adapter writes what it is given.
type inventoryWriter struct {
	store nodeInventoryStore
	now   func() time.Time
}

// WriteNode stores one observation. A live node with fresh info refreshes its
// vCPU and memory totals and its free disk; a disconnected or info-failed
// observation marks the node stale immediately so placement cannot use old
// inventory. Reservations are never touched here.
//
// Errors returned here are what the accepted inventory poller logs verbatim:
// they carry a fixed operation and classification only, never the store cause
// or a node-supplied identity.
func (w inventoryWriter) WriteNode(ctx context.Context, state nodegateway.NodeState) error {
	if state.Connected && state.Info != nil {
		if state.Info.GetNodeId() != state.NodeID {
			return errors.New("server: node info identifies a different node")
		}
		if err := w.store.UpsertNodeInfo(ctx, db.NodeInfo{
			ID:           state.NodeID,
			CPUModel:     state.Info.GetCpuModel(),
			QEMUVersion:  state.Info.GetQemuVersion(),
			DefaultImage: state.Info.GetDefaultImage(),
			Capacity: db.Resources{
				VCPUs:     state.Info.GetVcpus(),
				MemoryMiB: state.Info.GetMemoryMib(),
			},
			// Free disk for new sandboxes, already net of the node's own
			// reservations: the store turns it into a total plus the
			// reservations it holds, so they are not subtracted twice.
			DiskFreeGiB: state.Info.GetDiskFreeMib() / 1024,
			SeenAt:      w.now().UTC(),
		}); err != nil {
			return inventoryWriteError("update", err)
		}
		return nil
	}
	err := w.store.TouchNodeSeen(ctx, state.NodeID, time.Unix(0, 0).UTC())
	if errors.Is(err, db.ErrNotFound) {
		return nil // never registered: nothing to mark stale
	}
	if err != nil {
		return inventoryWriteError("touch", err)
	}
	return nil
}

// inventoryWriteError classifies a store failure without wrapping it: the
// poller logs this text, so the driver cause (which may carry a DSN) never
// reaches a log line.
func inventoryWriteError(operation string, err error) error {
	return fmt.Errorf("server: inventory %s failed: %s", operation, safeErrorName(err))
}

// gatewayAPICommands adapts the internal gateway factory to the API command
// handler's node-client seam.
func gatewayAPICommands(factory *nodegateway.ClientFactory) api.NodeClientFactory {
	return func(nodeID string) (nodev1.NodeClient, func() error, error) {
		client, err := factory.Dial(nodeID)
		if err != nil {
			return nil, nil, err
		}
		return client, client.Close, nil
	}
}

// gatewayLifecycleDial adapts the factory to lifecycle's gateway dialer.
func gatewayLifecycleDial(factory *nodegateway.ClientFactory) lifecycle.Dial {
	return func(nodeID string) (lifecycle.NodeClient, io.Closer, error) {
		client, err := factory.Dial(nodeID)
		if err != nil {
			return nil, nil, err
		}
		return client, client, nil
	}
}

// gatewaySchedulerDial adapts the factory to scheduler's placement probe.
func gatewaySchedulerDial(factory *nodegateway.ClientFactory) scheduler.Dial {
	return func(nodeID string) (scheduler.NodeClient, io.Closer, error) {
		client, err := factory.Dial(nodeID)
		if err != nil {
			return nil, nil, err
		}
		return client, client, nil
	}
}

// derefString returns the pointed-to string, or "" for nil.
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// safeErrorName classifies an error for logs without echoing driver text,
// which can carry SQL, a connection string or request material.
func safeErrorName(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, db.ErrNotFound):
		return "not_found"
	case errors.Is(err, db.ErrKeyOwned):
		return "key_owned"
	case errors.Is(err, db.ErrKeyRevoked):
		return "key_revoked"
	case errors.Is(err, db.ErrMasterKeyMismatch):
		return "master_key_mismatch"
	default:
		return fmt.Sprintf("%T", err)
	}
}
