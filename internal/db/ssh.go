package db

// SSH access metadata (B4.1). One sandbox boot can have at most one
// endpoint+host-key pair, confirmed by the node through AddSSHKey. The pair
// belongs to the boot identified by owner, sandbox, node and lifecycle
// generation: a late response from a stopped or resumed boot is rejected, and
// every state change that ends the boot clears the pair in the same fenced
// update. Host keys are public; private keys, user public keys, environment
// values and command output never reach this table.

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// SSHAccess is the node-confirmed connection metadata of one sandbox boot.
// Endpoint is the rendered host:port a client dials; HostKey is the public
// host-key line clients pin in known_hosts. A nil [Sandbox].SSH means the pair
// is absent; the schema stores both values together or neither.
type SSHAccess struct {
	Endpoint string
	HostKey  string
}

// SetSSHAccessParams is one owner-scoped SSH metadata write. Owner, SandboxID,
// NodeID and Generation identify the exact live boot the metadata belongs to;
// Generation is compared exactly, with zero being a real generation, never a
// wildcard. Endpoint and HostKey are both required: endpoint-only or
// host-key-only metadata is not representable.
type SetSSHAccessParams struct {
	Owner      uuid.UUID
	SandboxID  string
	NodeID     string
	Generation int64
	Endpoint   string
	HostKey    string
}

// ErrSSHAccessStale and ErrInvalidSSHAccess are returned unwrapped so callers
// compare them with errors.Is. The method can also return other database
// sentinels ([ErrNoOwner], [ErrNotFound]) and wrapped operational failures.
var (
	// ErrSSHAccessStale means the live row exists and belongs to Owner but no
	// longer identifies the boot that produced the metadata: a stop, resume,
	// delete intent, terminal error, node change or newer lifecycle
	// generation won. Nothing is written; the API maps it to its pinned
	// not-ready response and never resurrects the old endpoint or host key.
	ErrSSHAccessStale = errors.New("db: sandbox boot changed; ssh access metadata is stale")

	// ErrInvalidSSHAccess rejects metadata that cannot be a canonical
	// endpoint+host-key pair: an empty endpoint or host key, control
	// characters, or an endpoint that is not host:port with a numeric port in
	// 1..65535 and a non-empty host. The API still validates the OpenSSH key
	// syntax and canonicalizes legal whitespace before calling.
	ErrInvalidSSHAccess = errors.New("db: invalid ssh access metadata")
)

// SetSandboxSSHAccess persists the SSH endpoint and host key a successful
// AddSSHKey produced for one live sandbox boot. The caller must have read the
// owner-scoped sandbox before the RPC and pass that row's node and lifecycle
// generation; the row version is deliberately not an input, so an unrelated
// PATCH during the RPC does not invalidate the same boot.
//
// The write runs in one transaction. It locks the owner-scoped live row, then
// requires the same node, the exact lifecycle generation and a ready-ish state
// (ready, idle or running). An unknown, foreign or deleted sandbox answers
// [ErrNotFound]; any other mismatch answers [ErrSSHAccessStale]. Repeating the
// same pair is a no-op: updated_at, version and lifecycle_generation stay
// unchanged. A changed pair updates both values and updated_at only; it never
// bumps version or lifecycle_generation and writes no event.
func (s *Store) SetSandboxSSHAccess(ctx context.Context, p SetSSHAccessParams) (Sandbox, error) {
	if err := validateSSHAccess(p); err != nil {
		return Sandbox{}, err
	}
	var out Sandbox
	err := s.runTx(ctx, "set sandbox ssh access", func(ctx context.Context, tx pgx.Tx) error {
		sbx, err := sandboxRowForUpdateTx(ctx, tx, p.Owner, p.SandboxID)
		if err != nil {
			return err
		}
		if sbx.NodeID == nil || *sbx.NodeID != p.NodeID || sbx.LifecycleGeneration != p.Generation {
			return ErrSSHAccessStale
		}
		switch sbx.State {
		case StateReady, StateIdle, StateRunning:
		default:
			return ErrSSHAccessStale
		}
		if sbx.SSH != nil && sbx.SSH.Endpoint == p.Endpoint && sbx.SSH.HostKey == p.HostKey {
			out = sbx
			return nil
		}
		row := tx.QueryRow(ctx, `
			UPDATE sandboxes SET
				ssh_endpoint = $3, ssh_host_key = $4, updated_at = now()
			WHERE id = $1 AND owner_id = $2
			  AND deleted_at IS NULL
			  AND node_id = $5 AND lifecycle_generation = $6
			  AND state IN ('ready', 'idle', 'running')
			RETURNING `+sandboxCols,
			p.SandboxID, p.Owner, p.Endpoint, p.HostKey, p.NodeID, p.Generation)
		out, err = scanSandbox(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSSHAccessStale
		}
		if err != nil {
			return fmtError("set sandbox ssh access", err)
		}
		return nil
	})
	if err != nil {
		return Sandbox{}, err
	}
	return out, nil
}

// validateSSHAccess rejects inputs before any row lock. The endpoint must be a
// plain host:port with no whitespace or control characters, a non-empty host
// and a numeric port in 1..65535; IPv6 hosts are bracketed as
// net.SplitHostPort requires. The host key must be one non-blank line with no
// control characters. OpenSSH syntax and whitespace canonicalization remain
// the API's responsibility.
func validateSSHAccess(p SetSSHAccessParams) error {
	if p.Owner == uuid.Nil() {
		return ErrNoOwner
	}
	if p.Endpoint == "" || p.HostKey == "" {
		return ErrInvalidSSHAccess
	}
	if strings.ContainsFunc(p.Endpoint, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) {
		return ErrInvalidSSHAccess
	}
	host, port, err := net.SplitHostPort(p.Endpoint)
	if err != nil || host == "" || !isDigits(port) {
		return ErrInvalidSSHAccess
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return ErrInvalidSSHAccess
	}
	if strings.TrimSpace(p.HostKey) == "" ||
		strings.ContainsFunc(p.HostKey, unicode.IsControl) {
		return ErrInvalidSSHAccess
	}
	return nil
}
