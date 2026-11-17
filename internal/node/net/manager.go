package nodenet

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

var (
	// ErrPoolExhausted reports that Config.Prefix has no free guest address.
	ErrPoolExhausted = errors.New("nodenet: guest address pool exhausted")
	// ErrPortExhausted reports that Config.SSHPorts has no free port.
	ErrPortExhausted = errors.New("nodenet: SSH ingress port pool exhausted")
	// ErrClosed reports use of a Manager after Close.
	ErrClosed = errors.New("nodenet: manager is closed")
)

// cleanupTimeout bounds rollback and release commands. They run with a context
// detached from the caller's cancellation, so a cancelled boot still frees
// what it created.
const cleanupTimeout = 5 * time.Second

// Lease is one sandbox's live network attachment: the durable [Allocation]
// plus the descriptors and guest parameters needed to launch QEMU.
//
// The caller owns Tap and Vhost and must Close the lease once QEMU has the
// descriptors (QEMU duplicates them into the child). Releasing the sandbox is
// Release, not Close.
type Lease struct {
	Allocation
	// Resolvers are the guest's DNS servers; they are configuration, not part
	// of the durable identity.
	Resolvers []netip.Addr
	// SSHEndpoint is the externally reachable SSH address:port, or the zero
	// value when SSH ingress is not configured.
	SSHEndpoint netip.AddrPort
	// Tap is the pre-opened TAP descriptor for "-netdev tap,fd=N".
	Tap *os.File
	// Vhost is the pre-opened /dev/vhost-net descriptor, or nil.
	Vhost *os.File
}

// Close closes the lease's descriptors. It is idempotent and never touches the
// host network state.
func (l *Lease) Close() error {
	if l == nil {
		return nil
	}
	var errs []error
	if l.Tap != nil {
		errs = append(errs, l.Tap.Close())
		l.Tap = nil
	}
	if l.Vhost != nil {
		errs = append(errs, l.Vhost.Close())
		l.Vhost = nil
	}
	return errors.Join(errs...)
}

// Manager owns a node's VM network. All methods are safe for concurrent use;
// mutations are serialized because every one of them rewrites the nftables
// tables from the current allocation set.
//
// One Manager owns one state directory. Node roles share nothing in-process,
// so a second manager for the same directory is an operator error, not a
// supported mode.
type Manager struct {
	cfg  Config
	deps Deps
	log  *slog.Logger
	root *os.Root

	// readSysctl and now are seams for tests; production reads /proc/sys in the
	// manager's own namespace (the node's) and uses the host clock.
	readSysctl func(context.Context, string) ([]byte, error)
	now        func() time.Time
	// state is the durable allocation store; tests replace it to inject commit
	// faults.
	state stateStore

	mu     sync.Mutex
	allocs map[string]Allocation
}

// New validates cfg, opens the state directory and loads the persisted
// allocations. A state file that disagrees with cfg is ErrStateMismatch: the
// node refuses to start with guests it cannot describe rather than
// re-addressing them.
func New(cfg Config, deps Deps) (*Manager, error) {
	cfg = cfg.normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return nil, fmt.Errorf("nodenet: create state directory %s: %w", cfg.StateDir, err)
	}
	root, err := os.OpenRoot(cfg.StateDir)
	if err != nil {
		return nil, fmt.Errorf("nodenet: open state directory %s: %w", cfg.StateDir, err)
	}
	allocs, err := loadAllocations(root, cfg)
	if err != nil {
		root.Close()
		return nil, err
	}
	m := &Manager{
		cfg:        cfg,
		deps:       deps,
		log:        deps.logger(),
		root:       root,
		readSysctl: readSysctl,
		now:        time.Now,
		state:      fileStates{},
		allocs:     allocs,
	}
	m.log.Debug("nodenet: manager ready", "bridge", cfg.Bridge, "prefix", cfg.Prefix, "allocations", len(allocs))
	return m, nil
}

// Close releases the state directory handle. The manager must not be used
// afterwards; host resources are unaffected (Release owns those).
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return nil
	}
	err := m.root.Close()
	m.root = nil
	return err
}

// Setup allocates and applies the sandbox's network, or adopts the existing
// allocation when one is persisted for it. Called again after the first lease
// was closed (a retry or a node restart), it returns the same address, MAC, TAP
// and SSH port and re-applies the rules.
//
// Setup returns before QEMU starts. The descriptors in the lease are already
// usable; the TAP is brought up only after the rules that constrain it exist,
// so no frame can escape unvalidated. The caller owns the descriptors and must
// Close the lease once QEMU has them: a second TUNSETIFF on a TAP that another
// descriptor still holds fails with EBUSY by design.
func (m *Manager) Setup(ctx context.Context, sandboxID string) (*Lease, error) {
	if !ids.ValidSandbox(sandboxID) {
		return nil, fmt.Errorf("nodenet: invalid sandbox id %q", sandboxID)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if existing, ok := m.allocs[sandboxID]; ok {
		lease, err := m.activate(ctx, existing, true)
		if err != nil {
			return nil, fmt.Errorf("nodenet: adopt %s: %w", sandboxID, err)
		}
		return lease, nil
	}
	alloc, err := m.allocate(sandboxID)
	if err != nil {
		return nil, err
	}
	lease, err := m.activate(ctx, alloc, false)
	if err != nil {
		return nil, fmt.Errorf("nodenet: setup %s: %w", sandboxID, err)
	}
	m.log.Info("nodenet: allocated",
		"sandbox", sandboxID, "ip", alloc.GuestIP, "tap", alloc.TAPName, "sshPort", alloc.SSHPort)
	return lease, nil
}

// activate performs the bridge/TAP/rules/state transaction. existed selects
// whether the allocation came from the state directory: an adopted allocation
// is never deleted by a failed retry, and its durable record is never removed.
func (m *Manager) activate(ctx context.Context, a Allocation, existed bool) (*Lease, error) {
	if err := m.ensureBridge(ctx); err != nil {
		return nil, err
	}
	// Refuse to take over any existing same-name link that does not carry this
	// sandbox's ownership alias. TAPs are only made persistent after the alias
	// exists, so an unmarked same-name link is never ours: it is reported and
	// left untouched.
	if err := m.checkTapAdoptable(ctx, a); err != nil {
		return nil, err
	}
	tap, err := m.deps.taps().OpenTap(ctx, a.TAPName)
	if err != nil {
		return nil, err
	}
	// abandon releases what this call created, or restores an adopted
	// allocation, always closes the descriptor, and joins every error.
	abandon := func(cause error, adopted bool) (*Lease, error) {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		var errs []error
		if adopted {
			// Never delete or leave down a resource that existed before this
			// call. Restore the link so an adopt retry (or the running QEMU)
			// finds it usable.
			if err := m.setLinkUp(cleanupCtx, a.TAPName); err != nil {
				errs = append(errs, fmt.Errorf("restore tap: %w", err))
			}
		} else {
			ruleErr := m.removeFromRules(cleanupCtx, a.SandboxID)
			linkErr := m.deleteOwnedTap(cleanupCtx, a.SandboxID)
			if ruleErr != nil {
				errs = append(errs, ruleErr)
			}
			if linkErr != nil {
				errs = append(errs, linkErr)
			}
			if ruleErr != nil || linkErr != nil {
				// Rollback could not finish. Hold the reservation so the
				// address and port cannot be handed to another sandbox while
				// rules or an owned TAP may still exist; Release or Reconcile
				// finishes the cleanup and is retryable.
				if _, ok := m.allocs[a.SandboxID]; !ok {
					m.allocs[a.SandboxID] = a
				}
			}
		}
		if err := tap.Close(); err != nil {
			errs = append(errs, err)
		}
		if len(errs) == 0 {
			return nil, cause
		}
		return nil, errors.Join(cause, fmt.Errorf("nodenet: rollback: %w", errors.Join(errs...)))
	}

	if err := m.configureTap(ctx, a); err != nil {
		return abandon(err, existed)
	}
	if !existed {
		m.allocs[a.SandboxID] = a
	}
	if err := m.applyRules(ctx); err != nil {
		return abandon(err, existed)
	}
	if err := m.upTap(ctx, a.TAPName); err != nil {
		return abandon(err, existed)
	}
	vhost, err := m.openVhost(ctx)
	if err != nil {
		return abandon(err, existed)
	}
	// Persist only now: the TAP is marked with its ownership alias and its rules
	// exist, so a crash from here on leaves a link Reconcile can sweep. Before
	// this point the device dies with the descriptor, which is what removes the
	// "unmarked same-name link" ambiguity.
	if err := m.deps.taps().PersistTap(ctx, tap); err != nil {
		vhost.Close()
		return abandon(err, existed)
	}
	if err := m.state.Write(m.root, a); err != nil {
		if existed {
			// The durable record predates this call and still describes the
			// same allocation: keep the link and rules, only drop the new
			// descriptors.
			vhost.Close()
			tap.Close()
			return nil, fmt.Errorf("nodenet: refresh state for %s: %w", a.SandboxID, err)
		}
		// The rename may already have committed. Resolve the uncertainty
		// durably: remove the record, and only then roll back. If removal also
		// fails, keep the reservation so a retry adopts it instead of handing
		// the address and port to another sandbox.
		if rmErr := m.state.Remove(m.root, a.SandboxID); rmErr != nil {
			vhostErr := vhost.Close()
			tapErr := tap.Close()
			uncertain := fmt.Errorf("nodenet: state commit for %s is uncertain; the reservation was kept for an adopt retry: %w", a.SandboxID, rmErr)
			return nil, errors.Join(err, uncertain, vhostErr, tapErr)
		}
		vhost.Close()
		return abandon(err, false)
	}
	return m.lease(a, tap, vhost), nil
}

// removeFromRules drops an allocation from the in-memory set and re-renders.
// A failed render restores the entry so a committed rule set and the
// reservation never disagree.
func (m *Manager) removeFromRules(ctx context.Context, sandboxID string) error {
	a, ok := m.allocs[sandboxID]
	delete(m.allocs, sandboxID)
	if err := m.applyRules(ctx); err != nil {
		if ok {
			m.allocs[sandboxID] = a
		}
		return err
	}
	return nil
}

// checkTapAdoptable refuses to touch an existing link unless its ownership
// alias is exactly this sandbox's. A missing link is fine (it will be created
// non-persistently, marked with the alias, and only then persisted, so a crash
// can never leave an unmarked TAP behind and an unmarked same-name link is
// always someone else's).
func (m *Manager) checkTapAdoptable(ctx context.Context, a Allocation) error {
	state, err := m.linkInfo(ctx, a.TAPName)
	if err != nil {
		return err
	}
	switch {
	case state.Kind == "":
		return nil
	case state.Kind == "bridge":
		return fmt.Errorf("nodenet: link %q is a bridge, refusing to use it as a tap", a.TAPName)
	case state.Alias != aliasFor(a.SandboxID):
		return fmt.Errorf("nodenet: link %q exists without this sandbox's ownership alias (found %q); refusing to take it over",
			a.TAPName, state.Alias)
	default:
		return nil
	}
}

// openVhost opens the /dev/vhost-net descriptor every production TAP uses
// (virtio-net with vhost-net). There is no production opt-out:
// a host without the device fails CheckHost and Setup, because falling back to
// userspace virtio networking would be a silent performance and isolation
// regression. Tests substitute Deps.Taps instead of weakening this.
func (m *Manager) openVhost(ctx context.Context) (*os.File, error) {
	f, err := m.deps.taps().OpenVhostNet(ctx)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, errors.New("nodenet: the TAP implementation returned no /dev/vhost-net descriptor")
	}
	return f, nil
}

func (m *Manager) lease(a Allocation, tap, vhost *os.File) *Lease {
	lease := &Lease{
		Allocation: a,
		Resolvers:  slices.Clone(m.cfg.Resolvers),
		Tap:        tap,
		Vhost:      vhost,
	}
	if a.SSHPort != 0 && a.IngressAddr.IsValid() {
		lease.SSHEndpoint = netip.AddrPortFrom(a.IngressAddr, a.SSHPort)
	}
	return lease
}

// Release removes the sandbox's rules, TAP, state and port, in that order:
// the TAP is quiesced while the original rules still exist, then deleted, then
// removed from the rules, and the durable record goes last. Every step is
// idempotent, so a failed release can be retried. The address, port and state
// stay reserved until the link is gone and the record is durably removed, which
// is what stops a crash from handing a live stale TAP's identity to a new
// sandbox.
//
// Releasing an unknown sandbox succeeds when nothing is left; a same-name link
// that does not carry this package's ownership alias is reported and never
// touched.
func (m *Manager) Release(ctx context.Context, sandboxID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return ErrClosed
	}
	if !ids.ValidSandbox(sandboxID) {
		return fmt.Errorf("nodenet: invalid sandbox id %q", sandboxID)
	}
	return m.release(ctx, sandboxID)
}

func (m *Manager) release(ctx context.Context, sandboxID string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()

	a, ok := m.allocs[sandboxID]
	if !ok {
		// No reservation in memory: delete only a link that proves ownership
		// (the alias) and then any durable record.
		var errs []error
		if err := m.deleteOwnedTap(ctx, sandboxID); err != nil {
			errs = append(errs, err)
		}
		if err := m.state.Remove(m.root, sandboxID); err != nil {
			errs = append(errs, err)
		}
		if err := errors.Join(errs...); err != nil {
			return fmt.Errorf("nodenet: release %s: %w", sandboxID, err)
		}
		return nil
	}

	// 1. Prove ownership before touching anything: a foreign same-name link must
	// not be quiesced, even though a down/up would not delete it.
	owned, err := m.ownedTap(ctx, sandboxID)
	if err != nil {
		return fmt.Errorf("nodenet: release %s: %w", sandboxID, err)
	}
	// 2. Quiesce the TAP while the rules that constrain it are still installed.
	if owned.Kind != "" {
		if err := m.setLinkDown(ctx, a.TAPName); err != nil {
			return fmt.Errorf("nodenet: release %s: quiesce tap: %w", sandboxID, err)
		}
	}
	// 3. Delete the owned link. The reservation, the rules and the durable
	// record all survive a failure here.
	if err := m.deleteOwnedTap(ctx, sandboxID); err != nil {
		if upErr := m.setLinkUp(ctx, a.TAPName); upErr != nil {
			err = errors.Join(err, fmt.Errorf("restore tap: %w", upErr))
		}
		return fmt.Errorf("nodenet: release %s: delete tap: %w", sandboxID, err)
	}
	// 4. Remove it from the rules.
	delete(m.allocs, sandboxID)
	if err := m.applyRules(ctx); err != nil {
		m.allocs[sandboxID] = a
		return fmt.Errorf("nodenet: release %s: remove rules: %w", sandboxID, err)
	}
	// 5. Remove the durable record last; until it is gone the address and port
	// remain reserved for this sandbox.
	if err := m.state.Remove(m.root, sandboxID); err != nil {
		m.allocs[sandboxID] = a
		if applyErr := m.applyRules(ctx); applyErr != nil {
			err = errors.Join(err, fmt.Errorf("restore rules: %w", applyErr))
		}
		return fmt.Errorf("nodenet: release %s: remove state: %w", sandboxID, err)
	}
	m.log.Info("nodenet: released", "sandbox", sandboxID, "tap", a.TAPName)
	return nil
}

// ReconcileReport lists what Reconcile released or swept.
type ReconcileReport struct {
	// Released holds sandbox ids with a durable allocation that were released,
	// in address order.
	Released []string
	// SweptOrphans holds sandbox ids whose TAP carried our ownership alias but
	// had no durable allocation (a crash before the state commit).
	SweptOrphans []string
}

// Reconcile releases every allocation that is not in live, deletes owned TAPs
// left by a crash before their state commit, and re-renders the rules for the
// rest. It is what a node restart calls before accepting work: with an empty
// live list it releases everything, and it always leaves the owned tables
// installed, even when nothing else changed.
func (m *Manager) Reconcile(ctx context.Context, live []string) (ReconcileReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return ReconcileReport{}, ErrClosed
	}
	keep := make(map[string]struct{}, len(live))
	for _, id := range live {
		keep[id] = struct{}{}
	}
	var report ReconcileReport
	var errs []error
	for _, a := range sortedAllocations(m.allocs) {
		if _, ok := keep[a.SandboxID]; ok {
			continue
		}
		if err := m.release(ctx, a.SandboxID); err != nil {
			errs = append(errs, err)
			continue
		}
		report.Released = append(report.Released, a.SandboxID)
	}
	swept, sweepErr := m.sweepOwnedOrphans(ctx, keep)
	if sweepErr != nil {
		errs = append(errs, sweepErr)
	}
	report.SweptOrphans = swept
	// Always replace the owned tables once the allocation set is final. A
	// release above renders too, but a sweep never does, and a restart may find
	// the tables absent or stale; re-rendering unconditionally is the idempotent
	// way to leave one invariant for every path, including an orphan-only sweep.
	if err := m.applyRules(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return report, fmt.Errorf("nodenet: reconcile: %w", err)
	}
	return report, nil
}

// Allocation returns a copy of the sandbox's durable allocation and whether it
// exists.
func (m *Manager) Allocation(sandboxID string) (Allocation, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.allocs[sandboxID]
	return a, ok
}

// Allocations returns every allocation ordered by guest address. It is the
// inspection path for restart and reconciliation, and it never mutates state.
func (m *Manager) Allocations() []Allocation {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sortedAllocations(m.allocs)
}

// SSHEndpoint returns the externally reachable SSH address:port for the
// sandbox, or ok=false when SSH ingress is not configured or the sandbox has
// no allocation. The address comes from the persisted allocation, so a
// configuration change cannot silently re-point a running sandbox.
func (m *Manager) SSHEndpoint(sandboxID string) (netip.AddrPort, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.allocs[sandboxID]
	if !ok || a.SSHPort == 0 || !a.IngressAddr.IsValid() {
		return netip.AddrPort{}, false
	}
	return netip.AddrPortFrom(a.IngressAddr, a.SSHPort), true
}

// allocate picks the lowest free address and port for a new sandbox and
// derives its MAC and TAP name from the id, so the identity is reproducible
// after a restart.
func (m *Manager) allocate(sandboxID string) (Allocation, error) {
	used := make(map[netip.Addr]struct{}, len(m.allocs))
	usedPorts := make(map[uint16]struct{}, len(m.allocs))
	usedTaps := make(map[string]struct{}, len(m.allocs))
	usedMACs := make(map[string]struct{}, len(m.allocs))
	for _, a := range m.allocs {
		used[a.GuestIP] = struct{}{}
		usedTaps[a.TAPName] = struct{}{}
		usedMACs[a.MAC] = struct{}{}
		if a.SSHPort != 0 {
			usedPorts[a.SSHPort] = struct{}{}
		}
	}
	ip, ok := freeAddr(m.cfg.Prefix, m.cfg.Gateway, used)
	if !ok {
		return Allocation{}, fmt.Errorf("%w: %s", ErrPoolExhausted, m.cfg.Prefix)
	}
	// Every identity component is checked against the existing allocations
	// before the caller touches the host, so a clash can never create a state
	// file that the next start refuses to load.
	tap := tapFor(sandboxID)
	if _, clash := usedTaps[tap]; clash {
		return Allocation{}, fmt.Errorf("nodenet: tap name %q is derived from another sandbox", tap)
	}
	mac := macForAddr(ip)
	if _, clash := usedMACs[mac]; clash {
		return Allocation{}, fmt.Errorf("nodenet: MAC %s derived for %s is already in use", mac, sandboxID)
	}
	var port uint16
	var ingress netip.Addr
	var ingressIface string
	if m.cfg.IngressAddr.IsValid() {
		port, ok = freePort(m.cfg.SSHPorts, usedPorts)
		if !ok {
			return Allocation{}, fmt.Errorf("%w: %d-%d", ErrPortExhausted, m.cfg.SSHPorts.Min, m.cfg.SSHPorts.Max)
		}
		ingress = m.cfg.IngressAddr
		ingressIface = m.cfg.IngressIface
	}
	a := Allocation{
		SandboxID:    sandboxID,
		GuestIP:      ip,
		Prefix:       m.cfg.Prefix,
		Gateway:      m.cfg.Gateway,
		Bridge:       m.cfg.Bridge,
		MAC:          mac,
		TAPName:      tap,
		IngressAddr:  ingress,
		IngressIface: ingressIface,
		SSHPort:      port,
		CreatedAt:    m.now().UTC(),
	}
	if err := a.Validate(); err != nil {
		return Allocation{}, err
	}
	return a, nil
}

// freeAddr returns the lowest free guest address: the first host address after
// the network address up to (excluding) the broadcast address, skipping the
// gateway.
func freeAddr(prefix netip.Prefix, gateway netip.Addr, used map[netip.Addr]struct{}) (netip.Addr, bool) {
	prefix = prefix.Masked()
	last := lastAddr(prefix)
	for addr := prefix.Addr().Next(); addr.IsValid() && addr.Compare(last) < 0; addr = addr.Next() {
		if addr == gateway {
			continue
		}
		if _, taken := used[addr]; taken {
			continue
		}
		return addr, true
	}
	return netip.Addr{}, false
}

func freePort(ports PortRange, used map[uint16]struct{}) (uint16, bool) {
	if ports.Min > ports.Max {
		return 0, false
	}
	for p := ports.Min; ; p++ {
		if _, taken := used[p]; !taken {
			return p, true
		}
		if p == ports.Max {
			return 0, false
		}
	}
}

// hostMACForAddr derives the host-side TAP MAC from the guest address, in a
// separate locally administered prefix (02:54:00) from the guest's 52:54:00, so
// the host end of a TAP can never share the guest's address. It is distinct per
// guest (addresses are unique) and deterministic across a restart.
func hostMACForAddr(ip netip.Addr) string {
	if !ip.Is4() || ip.Is4In6() {
		return ""
	}
	v4 := ip.As4()
	hw := net.HardwareAddr{0x02, 0x54, 0x00, v4[1], v4[2], v4[3]}
	return hw.String()
}

// macForAddr derives the guest MAC from its address: QEMU's 52:54:00 OUI plus
// the low three octets of the IPv4 address. Two guests on one bridged prefix
// can never share a MAC because their addresses are unique, the result is
// stable across a restart because the address is persisted, and the first octet
// has the locally-administered bit set and the multicast bit clear. The previous
// id hash had only 24 bits and could collide; deriving from the address removes
// that possibility instead of rejecting a boot later.
func macForAddr(ip netip.Addr) string {
	if !ip.Is4() || ip.Is4In6() {
		return ""
	}
	v4 := ip.As4()
	hw := net.HardwareAddr{0x52, 0x54, 0x00, v4[1], v4[2], v4[3]}
	return hw.String()
}

// tapFor derives the TAP name from the sandbox id: "bt" plus 8 hex digits,
// inside the 15-byte IFNAMSIZ limit and stable across restarts.
func tapFor(sandboxID string) string {
	sum := sha256.Sum256([]byte("tap:" + sandboxID))
	return fmt.Sprintf("bt%x", sum[:4])
}
