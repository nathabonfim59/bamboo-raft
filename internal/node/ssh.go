package node

import (
	"context"
	"net/netip"
	"strings"

	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const (
	// sshAccessUser is the only account the frozen AddSSHKey contract exposes.
	// The guest reports it; the node refuses to publish anything else.
	sshAccessUser = "user"
	// maxSSHHostKeyBytes bounds the untrusted host-key text a guest may send
	// back before it is parsed. A real ed25519 line is under 100 bytes; a
	// payload near this bound is not a key and must not reach the parser.
	maxSSHHostKeyBytes = 64 << 10
)

// AddSSHKey proxies one key install to the guest and returns the externally
// reachable endpoint of the sandbox's current network allocation.
//
// Sandbox state is resolved before the guest sees the key and before any
// payload validation: an unknown or deleting sandbox is NOT_FOUND, one that
// cannot serve commands is FAILED_PRECONDITION, and a node without SSH ingress
// or without an allocation for this sandbox is UNAVAILABLE (never a fabricated
// host). The guest owns key-line validation and the idempotent, user-owned
// install, so its status codes and fixed messages pass through unchanged.
//
// The guest reply is untrusted input even though the guest produced it: the
// user must be exactly [sshAccessUser] and the host key exactly one bare
// ed25519 public key line. Only then is the endpoint read again and the reply
// bound to the boot generation it was produced for, so a Stop/Start or Delete
// that races the guest call can never pair an old host key with a newer
// endpoint. No node lock is held across the guest RPC or the endpoint read.
func (m *Manager) AddSSHKey(ctx context.Context, req *nodev1.AddSSHKeyRequest) (*nodev1.SSHAccess, error) {
	id := req.GetSandboxId()
	client, gen, v, err := m.sshKeyTarget(id)
	if err != nil {
		return nil, err
	}
	network := m.cfg.Network
	if network == nil {
		return nil, status.Error(codes.Unavailable, "node SSH ingress is not configured")
	}
	if _, ok := network.SSHEndpoint(id); !ok {
		return nil, status.Error(codes.Unavailable, "sandbox SSH endpoint is not allocated")
	}
	resp, err := client.AddSSHKey(ctx, &guestv1.AddSSHKeyRequest{PublicKey: req.GetPublicKey()})
	if err != nil {
		return nil, err
	}
	hostKey, err := sshKeyHostKey(resp)
	if err != nil {
		return nil, err
	}
	endpoint, ok := network.SSHEndpoint(id)
	if !ok {
		return nil, status.Error(codes.Unavailable, "sandbox SSH endpoint is not allocated")
	}
	// The endpoint read and the reply now belong to the same snapshot, or the
	// whole call fails: a guest host key from one boot must never be reported
	// with another boot's address and port.
	if !m.sshKeyStillCurrent(id, v, gen, client) {
		return nil, status.Error(codes.Unavailable, "sandbox changed while installing the SSH key")
	}
	if !validSSHEndpoint(endpoint) {
		return nil, status.Error(codes.Internal, "node network returned an invalid SSH endpoint")
	}
	return &nodev1.SSHAccess{
		User:    sshAccessUser,
		Host:    endpoint.Addr().String(),
		Port:    uint32(endpoint.Port()),
		HostKey: hostKey,
	}, nil
}

// sshKeyTarget snapshots the guest client and boot generation of a READY VM.
// lookup takes and releases the manager lock before this function takes the VM
// lock alone, so the two are never held in the reverse order; both are released
// before the caller runs any RPC. A deleting sandbox is NOT_FOUND, as lookup
// and the other guest proxies report it.
func (m *Manager) sshKeyTarget(id string) (guestv1.GuestClient, uint64, *vm, error) {
	v, err := m.lookup(id)
	if err != nil {
		return nil, 0, nil, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.deleting {
		return nil, 0, nil, status.Errorf(codes.NotFound, "sandbox %s not found", id)
	}
	if v.state != nodev1.VMState_VM_STATE_READY {
		return nil, 0, nil, status.Errorf(codes.FailedPrecondition, "sandbox %s is %s", id, vmStateName(v.state))
	}
	if v.client == nil {
		return nil, 0, nil, status.Errorf(codes.FailedPrecondition, "sandbox %s has no guest connection", id)
	}
	return v.client, v.bootGen, v, nil
}

// sshKeyStillCurrent reports whether the captured VM record, boot generation
// and guest client are still the live READY boot for id. It takes the manager
// lock before the VM lock, never the reverse, and holds neither across I/O.
func (m *Manager) sshKeyStillCurrent(id string, v *vm, gen uint64, client guestv1.GuestClient) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.vms[id] != v {
		return false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return !v.deleting && v.state == nodev1.VMState_VM_STATE_READY &&
		v.client == client && v.bootGen == gen
}

// sshKeyHostKey validates the guest's half of [nodev1.SSHAccess] and returns
// the canonical bare public key line. Raw CR, LF and NUL are rejected before
// parsing rather than trimmed, so a second line or an option sentence cannot
// slip through, and the result is rebuilt from the parsed key so no guest
// comment or spacing is echoed to the control plane. Errors name the protocol,
// never any byte of the untrusted input.
func sshKeyHostKey(resp *guestv1.AddSSHKeyResponse) (string, error) {
	if resp == nil || resp.GetUser() != sshAccessUser {
		return "", status.Error(codes.Internal, "guest protocol: invalid SSH access user")
	}
	raw := resp.GetHostKey()
	if len(raw) > maxSSHHostKeyBytes {
		return "", status.Error(codes.Internal, "guest protocol: SSH host key is too large")
	}
	if strings.ContainsAny(raw, "\r\n\x00") {
		return "", status.Error(codes.Internal, "guest protocol: SSH host key is not one line")
	}
	key, _, options, rest, err := ssh.ParseAuthorizedKey([]byte(raw))
	if err != nil || key == nil || key.Type() != "ssh-ed25519" ||
		len(options) != 0 || strings.TrimSpace(string(rest)) != "" {
		return "", status.Error(codes.Internal, "guest protocol: SSH host key is not a bare ssh-ed25519 public key")
	}
	return strings.TrimSuffix(string(ssh.MarshalAuthorizedKey(key)), "\n"), nil
}

// validSSHEndpoint reports whether the network produced a dialable endpoint.
// The zero AddrPort, an unspecified address and port 0 are rejected: they are
// configuration or allocation failures, not a host and port a client can use.
func validSSHEndpoint(endpoint netip.AddrPort) bool {
	return endpoint.IsValid() && endpoint.Port() != 0 && !endpoint.Addr().IsUnspecified()
}
