package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strings"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	nodenet "github.com/nathabonfim59/bamboo-raft/internal/node/net"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// VMNetwork is the node's consumer interface for per-VM networking: TAP
// allocation on the node bridge, the nftables isolation and NAT rules, and
// SSH ingress. It is implemented by *nodenet.Manager; tests inject a fake.
//
// The manager borrows the network: it never closes it and never releases
// live networking on Shutdown or a failed [New]. The command that constructs
// it owns its lifetime and closes it after the manager's Shutdown returns.
//
// Transitional real-TAP testing boundary (until A10): privileged network
// tests may run only inside gated throwaway VMs under an unprivileged test
// user with ambient CAP_NET_ADMIN. No role template or permanent binary
// capability grant may carry that; production grants are A10's.
type VMNetwork interface {
	// Setup allocates or adopts the sandbox's network attachment and returns
	// the launch lease: the durable allocation, the pre-opened TAP and
	// vhost-net descriptors QEMU inherits (fd 3/4), and the guest's static
	// identity. The caller owns the descriptors and closes them once QEMU has
	// its own references; releasing the sandbox is Release, never descriptor
	// closure.
	Setup(ctx context.Context, sandboxID string) (*nodenet.Lease, error)
	// Release removes the sandbox's rules, owned TAP and durable allocation.
	// It is idempotent and retryable. The caller must have proven the
	// sandbox's QEMU absent first: it must never run while a process may
	// still hold the TAP.
	Release(ctx context.Context, sandboxID string) error
	// SSHEndpoint reports the externally reachable SSH address:port of the
	// sandbox's durable allocation, or ok=false when SSH ingress is not
	// configured or the sandbox has no allocation.
	SSHEndpoint(sandboxID string) (netip.AddrPort, bool)
	// Allocations lists every durable allocation ordered by guest address.
	// It is the restart and reconciliation inspection path and never mutates
	// state.
	Allocations() []nodenet.Allocation
	// Reconcile releases every allocation that is not in live, sweeps owned
	// TAPs left orphaned by a crash before their state commit, and re-renders
	// the owned rules for the remaining set.
	Reconcile(ctx context.Context, live []string) (nodenet.ReconcileReport, error)
}

// The production network manager satisfies the consumer interface. The
// assertion keeps the contract and the implementation from drifting apart;
// the command constructs the manager and owns Close.
var _ VMNetwork = (*nodenet.Manager)(nil)

// networkLaunchLease sets up the sandbox's network inside the boot worker and
// renders the QEMU NIC description from the lease. In the dev/test
// user-networking mode (Config.Network nil) both results are nil and QEMU
// keeps its user-mode NIC. The lease's descriptors stay owned by the caller:
// QEMU inherits its own references, and the caller closes these once Start
// has returned.
func (m *Manager) networkLaunchLease(ctx context.Context, sandboxID string) (*qemu.Network, *nodenet.Lease, error) {
	if m.net == nil {
		return nil, nil, nil
	}
	lease, err := m.net.Setup(ctx, sandboxID)
	if err != nil {
		// A failed Setup may still have retained a durable allocation
		// (nodenet's uncertain commit); the boot's failure settlement releases
		// it once the absence of any QEMU is proven.
		return nil, nil, fmt.Errorf("network setup: %w", err)
	}
	nic := &qemu.Network{
		GuestIP:   lease.GuestIP,
		PrefixLen: lease.Prefix.Bits(),
		Gateway:   lease.Gateway,
		MAC:       lease.MAC,
		Resolvers: lease.Resolvers,
		Tap:       lease.Tap,
		Vhost:     lease.Vhost,
	}
	if err := nic.Validate(); err != nil {
		if cerr := lease.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
		return nil, nil, fmt.Errorf("network lease: %w", err)
	}
	return nic, lease, nil
}

// releaseNetwork removes the sandbox's network attachment. It runs only after
// the sandbox's QEMU absence has been proven, with no manager locks held, and
// on a detached context: nodenet bounds the cleanup itself, so a canceled
// boot or request cannot strand the release.
func (m *Manager) releaseNetwork(sandboxID string) error {
	if m.net == nil {
		return nil
	}
	if err := m.net.Release(context.Background(), sandboxID); err != nil {
		return fmt.Errorf("release network: %w", err)
	}
	return nil
}

// networkAllocation returns the sandbox's durable network allocation. It is
// the conservative identity lookup restart recovery uses before adopting a
// live QEMU under production networking.
func (m *Manager) networkAllocation(sandboxID string) (nodenet.Allocation, bool) {
	if m.net == nil {
		return nodenet.Allocation{}, false
	}
	for _, a := range m.net.Allocations() {
		if a.SandboxID == sandboxID {
			return a, true
		}
	}
	return nodenet.Allocation{}, false
}

// tapNetdevArg is the exact -netdev argument value qemu.Args renders for a
// TAP-mode launch, built from the exported fixed descriptor numbers so it
// cannot drift from the launcher's rendering. Restart recovery compares the
// pinned process's own argv against it before adopting a live QEMU under
// production networking: an allocation whose machine does not run the TAP NIC
// (a legacy user-mode launch under a stale allocation) is never adopted.
func tapNetdevArg() string {
	return fmt.Sprintf("tap,id=net0,fd=%d,vhost=on,vhostfd=%d", qemu.TapFD, qemu.VhostFD)
}

// tapArgvIdentity proves from the pinned process's own argv that the machine
// runs exactly the network the durable allocation describes: exactly one
// -netdev, and it is the TAP backend on the fixed fd numbers; exactly one
// virtio-net-pci device, and it carries the allocation's MAC; and none of
// the legacy -nic/-net shortcuts (an extra backend or shortcut could bypass
// the isolation the allocation stands for). Every other -device entry is
// untouched by this check. The comparison is fail-closed: anything else is
// an identity mismatch.
func tapArgvIdentity(argv []string, alloc nodenet.Allocation) error {
	if slices.Contains(argv, "-nic") || slices.Contains(argv, "-net") {
		return errors.New("a legacy -nic/-net network argument is present")
	}
	netdevs := allArgsAfter(argv, "-netdev")
	if len(netdevs) != 1 {
		return fmt.Errorf("want exactly one -netdev, found %d", len(netdevs))
	}
	if netdevs[0] != tapNetdevArg() {
		return fmt.Errorf("netdev %q is not the TAP backend %q", netdevs[0], tapNetdevArg())
	}
	wantDevice := "virtio-net-pci,netdev=net0,mac=" + alloc.MAC
	var nics []string
	for _, d := range allArgsAfter(argv, "-device") {
		// The bare device name (no options) is a NIC too: counting only the
		// comma form would let an expected NIC plus a bare extra one evade
		// the exactly-one rule.
		if d == "virtio-net-pci" || strings.HasPrefix(d, "virtio-net-pci,") {
			nics = append(nics, d)
		}
	}
	if len(nics) != 1 {
		return fmt.Errorf("want exactly one virtio-net-pci device, found %d", len(nics))
	}
	if nics[0] != wantDevice && nics[0] != wantDevice+",romfile=" {
		return fmt.Errorf("NIC device %q does not carry the allocation MAC %s", nics[0], alloc.MAC)
	}
	return nil
}

// allArgsAfter returns the value following every occurrence of flag in argv,
// not only the first: an argument list with two -netdev entries must be
// distinguishable from one.
func allArgsAfter(argv []string, flag string) []string {
	var out []string
	for i := 0; i+1 < len(argv); i++ {
		if argv[i] == flag {
			out = append(out, argv[i+1])
		}
	}
	return out
}

// restoreNetwork is the conservative first step of a production-networking
// restart, run before any VM is exposed or adopted: the owned rules are
// re-rendered for every durable allocation plus every owned VM directory, so
// a live VM's isolation exists before it can serve, while nothing is released
// and only no-allocation orphan TAPs are swept.
func (m *Manager) restoreNetwork(ctx context.Context) error {
	keep := make(map[string]struct{})
	for _, a := range m.net.Allocations() {
		keep[a.SandboxID] = struct{}{}
	}
	entries, err := m.store.ListVMDirs()
	if err != nil {
		return fmt.Errorf("node: restore network rules: list vm dirs: %w", err)
	}
	for _, e := range entries {
		if e.Kind == storage.VMKindOwned {
			keep[e.Name] = struct{}{}
		}
	}
	live := slices.Sorted(maps.Keys(keep))
	if _, err := m.net.Reconcile(ctx, live); err != nil {
		return fmt.Errorf("node: restore network rules: %w", err)
	}
	return nil
}

// pruneNetwork is the second step of a production-networking restart, run
// after VM recovery decided every record's fate: exactly the allocations of
// proven-dead and stopped records are released. Live records keep theirs, and
// so do unresolved ones (ownershipUnresolved, with or without a pinned
// handle: an ambiguous process scan is possible liveness), records that still
// hold a process handle (an adopted VM whose stop could not be proven), and
// quarantined entries — quarantine proves no live process uses the owned
// paths before isolating a corrupt manifest, but a corrupt manifest makes the
// identity unknowable, so its allocation stays for explicit operator cleanup
// and is never released automatically. The quarantined set is durable: this
// boot's quarantine map covers the window before the directory move, and
// every id with a quarantined directory under orphans/ (inspected read-only,
// by name only) keeps its allocation across any number of restarts.
func (m *Manager) pruneNetwork(ctx context.Context) error {
	orphans, err := m.store.ListQuarantinedIDs()
	if err != nil {
		return fmt.Errorf("node: prune network allocations: %w", err)
	}
	m.mu.Lock()
	keep := make(map[string]struct{}, len(m.vms)+len(m.quarantined)+len(orphans))
	for id := range m.quarantined {
		keep[id] = struct{}{}
	}
	for _, id := range orphans {
		keep[id] = struct{}{}
	}
	for id, v := range m.vms {
		v.mu.Lock()
		live := v.process != nil || v.ownershipUnresolved || v.jail != (jailer.Ref{}) ||
			v.state == nodev1.VMState_VM_STATE_READY ||
			v.state == nodev1.VMState_VM_STATE_BOOTING ||
			v.state == nodev1.VMState_VM_STATE_STOPPING
		v.mu.Unlock()
		if live {
			keep[id] = struct{}{}
		}
	}
	m.mu.Unlock()
	live := slices.Sorted(maps.Keys(keep))
	if _, err := m.net.Reconcile(ctx, live); err != nil {
		return fmt.Errorf("node: prune network allocations: %w", err)
	}
	return nil
}
