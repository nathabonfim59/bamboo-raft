package nodenet

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net"
	"net/netip"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// ErrStateMismatch reports a persisted allocation that does not describe the
// configured network. It is fatal instead of silently re-addressing a guest:
// the operator changed the configuration while sandboxes existed, and the
// right recovery is an explicit release.
var ErrStateMismatch = errors.New("nodenet: persisted allocation does not match the configuration")

// stateSuffix is appended to a sandbox id to name its state file.
const stateSuffix = ".json"

// tmpPrefix marks a state file being written. Leftovers from a crash are
// removed on load; the committed name is only ever the result of a rename.
const tmpPrefix = ".tmp-"

// Allocation is the durable identity of one sandbox's network resources. It is
// what a restart, a reconciliation and a rollback reason about, and it is
// stored as JSON in Config.StateDir.
type Allocation struct {
	// SandboxID is the nodev1 sandbox id this allocation belongs to.
	SandboxID string `json:"sandboxId"`
	// GuestIP is the guest's IPv4 address inside Prefix.
	GuestIP netip.Addr `json:"guestIp"`
	// Prefix, Gateway and Bridge are the network identity at allocation time.
	// A configuration change while the allocation exists is ErrStateMismatch.
	Prefix  netip.Prefix `json:"prefix"`
	Gateway netip.Addr   `json:"gateway"`
	Bridge  string       `json:"bridge"`
	// MAC is the guest's Ethernet address, derived from GuestIP. It is what QEMU
	// puts on virtio-net and what the bridge source filter accepts; the host TAP
	// end carries hostMACForAddr instead.
	MAC string `json:"mac"`
	// TAPName is the host TAP interface, derived from SandboxID.
	TAPName string `json:"tap"`
	// IngressAddr is the address the SSH port was published on, or the zero
	// address when SSH ingress is not configured. It is part of the identity:
	// a configuration change means a different externally reachable endpoint.
	IngressAddr netip.Addr `json:"ingressAddr,omitzero"`
	// IngressIface is the host interface SSH ingress was bound to, or empty for
	// the explicitly unbound library behaviour. It is part of the identity: a
	// changed binding would expose the same guest port on another interface.
	IngressIface string `json:"ingressIface,omitzero"`
	// SSHPort is the DNAT port on IngressAddr, or zero when SSH ingress is not
	// configured.
	SSHPort uint16 `json:"sshPort,omitzero"`
	// CreatedAt is diagnostic only and never used for policy.
	CreatedAt time.Time `json:"createdAt"`
}

// Validate checks that a persisted allocation is self-consistent. It is the
// first line of defence against a tampered or truncated state file.
func (a Allocation) Validate() error {
	if !ids.ValidSandbox(a.SandboxID) {
		return fmt.Errorf("nodenet: state: invalid sandbox id %q", a.SandboxID)
	}
	if !a.Prefix.IsValid() || !a.Prefix.Addr().Is4() || a.Prefix.Addr().Is4In6() {
		return fmt.Errorf("nodenet: state %s: prefix %q is not IPv4", a.SandboxID, a.Prefix)
	}
	if err := validAddr("state guest address", a.GuestIP); err != nil {
		return fmt.Errorf("nodenet: state %s: %w", a.SandboxID, err)
	}
	if !a.Prefix.Contains(a.GuestIP) {
		return fmt.Errorf("nodenet: state %s: guest address %s is outside %s", a.SandboxID, a.GuestIP, a.Prefix)
	}
	if a.GuestIP == a.Prefix.Masked().Addr() || a.GuestIP == lastAddr(a.Prefix.Masked()) {
		return fmt.Errorf("nodenet: state %s: guest address %s is a network or broadcast address", a.SandboxID, a.GuestIP)
	}
	if err := validAddr("state gateway", a.Gateway); err != nil {
		return fmt.Errorf("nodenet: state %s: %w", a.SandboxID, err)
	}
	if a.Gateway == a.GuestIP {
		return fmt.Errorf("nodenet: state %s: guest address equals the gateway", a.SandboxID)
	}
	if err := validIfaceName(a.Bridge); err != nil {
		return fmt.Errorf("nodenet: state %s: bridge: %w", a.SandboxID, err)
	}
	hw, err := net.ParseMAC(a.MAC)
	if err != nil || len(hw) != 6 {
		return fmt.Errorf("nodenet: state %s: invalid MAC %q", a.SandboxID, a.MAC)
	}
	if hw[0]&1 != 0 {
		return fmt.Errorf("nodenet: state %s: MAC %s is multicast", a.SandboxID, a.MAC)
	}
	if want := macForAddr(a.GuestIP); a.MAC != want {
		return fmt.Errorf("nodenet: state %s: MAC %s is not the address derived from %s (%s)", a.SandboxID, a.MAC, a.GuestIP, want)
	}
	if err := validIfaceName(a.TAPName); err != nil {
		return fmt.Errorf("nodenet: state %s: tap: %w", a.SandboxID, err)
	}
	if want := tapFor(a.SandboxID); a.TAPName != want {
		return fmt.Errorf("nodenet: state %s: tap %s is not the name derived from the sandbox id (%s)", a.SandboxID, a.TAPName, want)
	}
	if a.SSHPort != 0 && a.SSHPort < minPort {
		return fmt.Errorf("nodenet: state %s: SSH port %d is below %d", a.SandboxID, a.SSHPort, minPort)
	}
	if a.IngressAddr.IsValid() != (a.SSHPort != 0) {
		return fmt.Errorf("nodenet: state %s: ingress address and SSH port must be set together", a.SandboxID)
	}
	if a.IngressAddr.IsValid() {
		if err := validAddr("state ingress address", a.IngressAddr); err != nil {
			return fmt.Errorf("nodenet: state %s: %w", a.SandboxID, err)
		}
	}
	if a.IngressIface != "" {
		if err := validIfaceName(a.IngressIface); err != nil {
			return fmt.Errorf("nodenet: state %s: ingress interface: %w", a.SandboxID, err)
		}
		if a.IngressIface == a.Bridge {
			return fmt.Errorf("nodenet: state %s: ingress interface %q is the bridge", a.SandboxID, a.IngressIface)
		}
		if !a.IngressAddr.IsValid() {
			return fmt.Errorf("nodenet: state %s: ingress interface is set without an ingress address", a.SandboxID)
		}
	}
	return nil
}

// matchesConfig reports whether the persisted identity still describes cfg.
func (a Allocation) matchesConfig(cfg Config) error {
	if a.Bridge != cfg.Bridge {
		return fmt.Errorf("%w: sandbox %s uses bridge %q, configured %q", ErrStateMismatch, a.SandboxID, a.Bridge, cfg.Bridge)
	}
	if a.Prefix != cfg.Prefix.Masked() {
		return fmt.Errorf("%w: sandbox %s uses prefix %s, configured %s", ErrStateMismatch, a.SandboxID, a.Prefix, cfg.Prefix.Masked())
	}
	if a.Gateway != cfg.Gateway {
		return fmt.Errorf("%w: sandbox %s uses gateway %s, configured %s", ErrStateMismatch, a.SandboxID, a.Gateway, cfg.Gateway)
	}
	if a.IngressAddr != cfg.IngressAddr {
		return fmt.Errorf("%w: sandbox %s uses ingress address %s, configured %s", ErrStateMismatch, a.SandboxID, a.IngressAddr, cfg.IngressAddr)
	}
	if a.IngressIface != cfg.IngressIface {
		return fmt.Errorf("%w: sandbox %s uses ingress interface %q, configured %q", ErrStateMismatch, a.SandboxID, a.IngressIface, cfg.IngressIface)
	}
	if a.IngressAddr.IsValid() {
		if a.SSHPort == 0 || !cfg.SSHPorts.Contains(a.SSHPort) {
			return fmt.Errorf("%w: sandbox %s SSH port %d is outside the configured range %d-%d",
				ErrStateMismatch, a.SandboxID, a.SSHPort, cfg.SSHPorts.Min, cfg.SSHPorts.Max)
		}
	}
	return nil
}

func stateName(sandboxID string) string { return sandboxID + stateSuffix }

// stateStore is the durable allocation store. It is not part of Deps; tests
// replace Manager.state to inject rename, directory-sync and removal faults at
// the exact commit boundary.
type stateStore interface {
	Write(root *os.Root, a Allocation) error
	Remove(root *os.Root, sandboxID string) error
}

// fileStates is the production stateStore.
type fileStates struct{}

func (fileStates) Write(root *os.Root, a Allocation) error { return writeAllocation(root, a) }
func (fileStates) Remove(root *os.Root, sandboxID string) error {
	return removeAllocation(root, sandboxID)
}

// loadAllocations reads every committed allocation below root and removes
// temporary files left by an interrupted write. Files that fail validation are
// errors: guessing would mean adopting resources nobody can describe.
func loadAllocations(root *os.Root, cfg Config) (map[string]Allocation, error) {
	dir, err := root.Open(".")
	if err != nil {
		return nil, fmt.Errorf("nodenet: open state directory: %w", err)
	}
	entries, readErr := dir.ReadDir(-1)
	closeErr := dir.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, fmt.Errorf("nodenet: list state directory: %w", err)
	}

	allocs := make(map[string]Allocation, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, tmpPrefix) {
			if err := root.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, fmt.Errorf("nodenet: remove stale state file %s: %w", name, err)
			}
			continue
		}
		if !strings.HasSuffix(name, stateSuffix) {
			continue
		}
		stored, err := readAllocation(root, name)
		if err != nil {
			return nil, err
		}
		if stateName(stored.SandboxID) != name {
			return nil, fmt.Errorf("nodenet: state file %s holds sandbox %s", name, stored.SandboxID)
		}
		if err := stored.matchesConfig(cfg); err != nil {
			return nil, err
		}
		allocs[stored.SandboxID] = stored
	}
	if err := checkUnique(allocs); err != nil {
		return nil, err
	}
	return allocs, nil
}

// checkUnique refuses a state directory where two sandboxes claim the same
// address, port, TAP or MAC. That can only come from a corrupted or hand-edited
// state file, and reconciling it would give two guests the same identity.
func checkUnique(allocs map[string]Allocation) error {
	seenIP := make(map[netip.Addr]string, len(allocs))
	seenPort := make(map[uint16]string, len(allocs))
	seenTAP := make(map[string]string, len(allocs))
	seenMAC := make(map[string]string, len(allocs))
	ids := slices.Sorted(maps.Keys(allocs))
	for _, id := range ids {
		a := allocs[id]
		if other, dup := seenIP[a.GuestIP]; dup {
			return fmt.Errorf("nodenet: state: sandboxes %s and %s share address %s", id, other, a.GuestIP)
		}
		seenIP[a.GuestIP] = id
		if a.SSHPort != 0 {
			if other, dup := seenPort[a.SSHPort]; dup {
				return fmt.Errorf("nodenet: state: sandboxes %s and %s share SSH port %d", id, other, a.SSHPort)
			}
			seenPort[a.SSHPort] = id
		}
		if other, dup := seenTAP[a.TAPName]; dup {
			return fmt.Errorf("nodenet: state: sandboxes %s and %s share tap %s", id, other, a.TAPName)
		}
		seenTAP[a.TAPName] = id
		if other, dup := seenMAC[a.MAC]; dup {
			return fmt.Errorf("nodenet: state: sandboxes %s and %s share MAC %s", id, other, a.MAC)
		}
		seenMAC[a.MAC] = id
	}
	return nil
}

func readAllocation(root *os.Root, name string) (Allocation, error) {
	data, err := root.ReadFile(name)
	if err != nil {
		return Allocation{}, fmt.Errorf("nodenet: read state file %s: %w", name, err)
	}
	var a Allocation
	if err := json.Unmarshal(data, &a); err != nil {
		return Allocation{}, fmt.Errorf("nodenet: decode state file %s: %w", name, err)
	}
	if err := a.Validate(); err != nil {
		return Allocation{}, err
	}
	return a, nil
}

// writeAllocation commits an allocation: a synced temporary file, an atomic
// rename, then a synced directory. The rename is the commit point; the final
// directory sync only makes the rename durable across a power loss. A failure
// after the rename is therefore an uncertain commit, which the caller resolves
// by removing the file durably or keeping the reservation.
func writeAllocation(root *os.Root, a Allocation) error {
	if err := writeAllocationFile(root, a); err != nil {
		return err
	}
	return syncDir(root)
}

// writeAllocationFile writes, fsyncs and renames the state file without syncing
// the directory. It is the part whose success (rename) is the commit.
func writeAllocationFile(root *os.Root, a Allocation) error {
	data, err := json.Marshal(a)
	if err != nil {
		return fmt.Errorf("nodenet: encode state for %s: %w", a.SandboxID, err)
	}
	name := stateName(a.SandboxID)
	tmp := tmpPrefix + name
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("nodenet: create state file %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		root.Remove(tmp)
		return fmt.Errorf("nodenet: write state file %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		root.Remove(tmp)
		return fmt.Errorf("nodenet: sync state file %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		root.Remove(tmp)
		return fmt.Errorf("nodenet: close state file %s: %w", tmp, err)
	}
	if err := root.Rename(tmp, name); err != nil {
		root.Remove(tmp)
		return fmt.Errorf("nodenet: commit state file %s: %w", name, err)
	}
	return nil
}

// removeAllocation deletes a committed allocation and syncs the directory. A
// missing file is success, which is what makes release idempotent.
func removeAllocation(root *os.Root, sandboxID string) error {
	if err := root.Remove(stateName(sandboxID)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("nodenet: remove state for %s: %w", sandboxID, err)
	}
	return syncDir(root)
}

func syncDir(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("nodenet: open state directory for sync: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("nodenet: sync state directory: %w", err)
	}
	return nil
}

// sortedAllocations returns the map's values ordered by guest address, which
// makes the rendered ruleset deterministic.
func sortedAllocations(allocs map[string]Allocation) []Allocation {
	out := make([]Allocation, 0, len(allocs))
	for _, a := range allocs {
		out = append(out, a)
	}
	slices.SortFunc(out, func(a, b Allocation) int {
		if c := a.GuestIP.Compare(b.GuestIP); c != 0 {
			return c
		}
		return strings.Compare(a.SandboxID, b.SandboxID)
	})
	return out
}
