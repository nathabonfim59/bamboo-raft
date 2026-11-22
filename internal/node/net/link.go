package nodenet

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// ipLink is the subset of `ip -j -d link show` this package reads.
type ipLink struct {
	IfName   string `json:"ifname"`
	IfAlias  string `json:"ifalias,omitzero"`
	LinkInfo *struct {
		InfoKind string `json:"info_kind"`
	} `json:"linkinfo,omitzero"`
}

// linkState is what the manager needs to know about a link.
type linkState struct {
	// Kind is the netlink link kind, "unknown" for a link without linkinfo, and
	// empty when the link does not exist.
	Kind string
	// Alias is the interface alias, which carries the ownership marker.
	Alias string
}

// aliasPrefix marks every link this package creates. It is the ownership proof
// used before releasing or sweeping a link: a same-name link without our alias
// belongs to someone else and is never touched.
const aliasPrefix = "nodenet:"

func aliasFor(sandboxID string) string { return aliasPrefix + sandboxID }

// ipAddr is the subset of `ip -j -4 addr show` this package reads.
type ipAddr struct {
	IfName   string `json:"ifname"`
	AddrInfo []struct {
		Local string `json:"local"`
		// PrefixLen is absent in hand-made fixtures; an address without one is a
		// host address (/32), never the whole address space.
		PrefixLen *int `json:"prefixlen,omitzero"`
	} `json:"addr_info,omitzero"`
}

// hostAddr is one IPv4 address of a host interface other than the node's own
// bridge, with the network it is connected to.
type hostAddr struct {
	// Iface is the interface name.
	Iface string
	// Addr is the address as configured, host bits included (10.42.0.2/16).
	Addr netip.Prefix
}

// hostAddrs lists the IPv4 addresses the host holds on every interface except
// the node's own bridge, which legitimately carries the gateway. Loopback and
// unparsable entries are skipped: neither can overlap a private guest prefix.
func (m *Manager) hostAddrs(ctx context.Context) ([]hostAddr, error) {
	out, err := m.output(ctx, "ip", "-j", "-4", "addr", "show")
	if err != nil {
		return nil, fmt.Errorf("nodenet: list host addresses: %w", err)
	}
	var links []ipAddr
	if err := json.Unmarshal(out, &links); err != nil {
		return nil, fmt.Errorf("nodenet: decode host addresses: %w", err)
	}
	var addrs []hostAddr
	for _, link := range links {
		if link.IfName == m.cfg.Bridge {
			continue
		}
		for _, info := range link.AddrInfo {
			local, err := netip.ParseAddr(info.Local)
			if err != nil || !local.Is4() || local.IsLoopback() {
				continue
			}
			bits := 32
			if info.PrefixLen != nil && *info.PrefixLen >= 0 && *info.PrefixLen <= 32 {
				bits = *info.PrefixLen
			}
			addrs = append(addrs, hostAddr{Iface: link.IfName, Addr: netip.PrefixFrom(local, bits)})
		}
	}
	return addrs, nil
}

// guestPrefixConflict returns an error naming the first host address that is
// inside the guest prefix, or whose connected network overlaps it. A guest in
// such a prefix could be handed an address the host already owns, and an SSH
// ingress DNAT to it would reach a host service.
func guestPrefixConflict(guest netip.Prefix, addrs []hostAddr) error {
	for _, a := range addrs {
		if guest.Contains(a.Addr.Addr()) || guest.Overlaps(a.Addr) {
			return fmt.Errorf("nodenet: host interface %s holds %s, which overlaps the guest prefix %s: "+
				"guests could be given host addresses; choose a different -node.network-prefix "+
				"(and -node.network-gateway) that no host interface uses",
				a.Iface, a.Addr, guest)
		}
	}
	return nil
}

// run executes a host command, preferring the context's error over a start
// failure so cancellation is recognizable with errors.Is.
func (m *Manager) run(ctx context.Context, name string, args ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return m.deps.runner().Run(ctx, name, args, nil)
}

func (m *Manager) output(ctx context.Context, name string, args ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return m.deps.runner().Output(ctx, name, args)
}

// linkInfo returns the link's kind and alias; Kind is empty when the link does
// not exist. Errors that are not "does not exist" are returned: a host that
// cannot be inspected must not be treated as empty.
func (m *Manager) linkInfo(ctx context.Context, name string) (linkState, error) {
	out, err := m.output(ctx, "ip", "-j", "-d", "link", "show", "dev", name)
	if err != nil {
		// iproute2 exits 1 with "Device ... does not exist" for an absent link.
		if exitCode(err) == 1 && strings.Contains(stderrOf(err), "does not exist") {
			return linkState{}, nil
		}
		return linkState{}, fmt.Errorf("nodenet: inspect link %s: %w", name, err)
	}
	var links []ipLink
	if err := json.Unmarshal(out, &links); err != nil {
		return linkState{}, fmt.Errorf("nodenet: decode link %s: %w", name, err)
	}
	if len(links) == 0 {
		return linkState{}, nil
	}
	state := linkState{Alias: links[0].IfAlias}
	switch {
	case links[0].LinkInfo == nil || links[0].LinkInfo.InfoKind == "":
		state.Kind = "unknown"
	default:
		state.Kind = links[0].LinkInfo.InfoKind
	}
	return state, nil
}

// linkKind returns the link's kind, or "" when the link does not exist.
func (m *Manager) linkKind(ctx context.Context, name string) (string, error) {
	state, err := m.linkInfo(ctx, name)
	return state.Kind, err
}

// ensureBridge creates the bridge when absent, refuses to reuse a link of
// another kind, and (re)applies the gateway address. It is idempotent and never
// deletes a link.
func (m *Manager) ensureBridge(ctx context.Context) error {
	cfg := m.cfg
	kind, err := m.linkKind(ctx, cfg.Bridge)
	if err != nil {
		return err
	}
	switch kind {
	case "":
		m.log.Info("nodenet: creating bridge", "bridge", cfg.Bridge)
		if err := m.run(ctx, "ip", "link", "add", cfg.Bridge, "type", "bridge"); err != nil {
			return fmt.Errorf("nodenet: create bridge %s: %w", cfg.Bridge, err)
		}
	case "bridge":
		// An existing bridge is reused; the operator may own it.
	default:
		return fmt.Errorf("nodenet: link %q exists as %s, refusing to use it as the VM bridge", cfg.Bridge, kind)
	}
	addr := cfg.Gateway.String() + "/" + strconv.Itoa(cfg.Prefix.Masked().Bits())
	if err := m.run(ctx, "ip", "addr", "replace", addr, "dev", cfg.Bridge); err != nil {
		return fmt.Errorf("nodenet: address %s on %s: %w", addr, cfg.Bridge, err)
	}
	if err := m.run(ctx, "ip", "link", "set", "dev", cfg.Bridge, "up"); err != nil {
		return fmt.Errorf("nodenet: bring up bridge %s: %w", cfg.Bridge, err)
	}
	return nil
}

// configureTap enslaves the TAP, isolates it at L2, gives it its host identity
// and marks it as owned. The link is brought up separately, after the rules
// exist.
//
// The TAP device itself gets hostMACForAddr: a host end that never shares the
// guest's address, which keeps the L2 domain unambiguous. The guest MAC
// (Allocation.MAC) is the one QEMU puts on virtio-net and the one the bridge
// source filter accepts; the two are deliberately different.
func (m *Manager) configureTap(ctx context.Context, a Allocation) error {
	steps := []struct {
		what string
		args []string
	}{
		// The alias is the durable ownership proof for release and sweep; it is
		// set first so the window where an adopted link is unmarked is tiny.
		{"alias", []string{"ip", "link", "set", "dev", a.TAPName, "alias", aliasFor(a.SandboxID)}},
		{"down", []string{"ip", "link", "set", "dev", a.TAPName, "down"}},
		{"master", []string{"ip", "link", "set", "dev", a.TAPName, "master", m.cfg.Bridge}},
		// Isolation is what stops two TAPs talking at L2 without the router.
		{"isolate", []string{"ip", "link", "set", "dev", a.TAPName, "type", "bridge_slave", "isolated", "on"}},
		{"address", []string{"ip", "link", "set", "dev", a.TAPName, "address", hostMACForAddr(a.GuestIP), "mtu", strconv.Itoa(tapMTU)}},
	}
	for _, step := range steps {
		if err := m.run(ctx, step.args[0], step.args[1:]...); err != nil {
			return fmt.Errorf("nodenet: tap %s %s: %w", a.TAPName, step.what, err)
		}
	}
	return nil
}

func (m *Manager) upTap(ctx context.Context, name string) error {
	if err := m.run(ctx, "ip", "link", "set", "dev", name, "up"); err != nil {
		return fmt.Errorf("nodenet: tap %s up: %w", name, err)
	}
	return nil
}

// setLinkDown quiesces a TAP before its rules or link are removed. A missing
// link is success, which makes a retried release idempotent.
func (m *Manager) setLinkDown(ctx context.Context, name string) error {
	kind, err := m.linkKind(ctx, name)
	if err != nil {
		return err
	}
	if kind == "" {
		return nil
	}
	if err := m.run(ctx, "ip", "link", "set", "dev", name, "down"); err != nil {
		return fmt.Errorf("nodenet: tap %s down: %w", name, err)
	}
	return nil
}

// setLinkUp restores a link after a failed release step. A missing link is
// success.
func (m *Manager) setLinkUp(ctx context.Context, name string) error {
	kind, err := m.linkKind(ctx, name)
	if err != nil {
		return err
	}
	if kind == "" {
		return nil
	}
	return m.upTap(ctx, name)
}

// ownedTap inspects the TAP for sandboxID and proves ownership from the alias.
// A missing link returns a zero linkState and no error. An existing link with a
// different alias, or a bridge, is a hard error: touching someone else's link is
// never allowed, including quiescing it.
func (m *Manager) ownedTap(ctx context.Context, sandboxID string) (linkState, error) {
	name := tapFor(sandboxID)
	state, err := m.linkInfo(ctx, name)
	if err != nil {
		return linkState{}, err
	}
	switch {
	case state.Kind == "":
		return state, nil
	case state.Kind == "bridge":
		return state, fmt.Errorf("nodenet: link %q is a %s; refusing to touch it", name, state.Kind)
	case state.Alias != aliasFor(sandboxID):
		return state, fmt.Errorf("nodenet: link %q carries alias %q and is not the allocation for %s; refusing to touch it",
			name, state.Alias, sandboxID)
	default:
		return state, nil
	}
}

// deleteOwnedTap deletes the TAP for sandboxID only after proving ownership.
// A missing link is success.
func (m *Manager) deleteOwnedTap(ctx context.Context, sandboxID string) error {
	state, err := m.ownedTap(ctx, sandboxID)
	if err != nil {
		return err
	}
	if state.Kind == "" {
		return nil
	}
	name := tapFor(sandboxID)
	if err := m.run(ctx, "ip", "link", "del", "dev", name); err != nil {
		return fmt.Errorf("nodenet: delete link %s: %w", name, err)
	}
	return nil
}

// sweepOwnedOrphans removes links that carry this package's alias but have no
// durable allocation and are not in live. It is the crash recovery for a TAP
// created before the state commit, and it never considers a link without our
// alias.
func (m *Manager) sweepOwnedOrphans(ctx context.Context, live map[string]struct{}) ([]string, error) {
	out, err := m.output(ctx, "ip", "-j", "-d", "link", "show")
	if err != nil {
		return nil, fmt.Errorf("nodenet: list links: %w", err)
	}
	var links []ipLink
	if err := json.Unmarshal(out, &links); err != nil {
		return nil, fmt.Errorf("nodenet: decode links: %w", err)
	}
	var removed []string
	var errs []error
	for _, link := range links {
		id, ok := strings.CutPrefix(link.IfAlias, aliasPrefix)
		if !ok || !ids.ValidSandbox(id) || link.IfName != tapFor(id) {
			continue
		}
		if _, reserved := m.allocs[id]; reserved {
			continue
		}
		if _, isLive := live[id]; isLive {
			continue
		}
		if err := m.deleteOwnedTap(ctx, id); err != nil {
			errs = append(errs, err)
			continue
		}
		removed = append(removed, id)
	}
	return removed, errors.Join(errs...)
}

// applyRules replaces the owned nftables tables in one atomic transaction.
func (m *Manager) applyRules(ctx context.Context) error {
	batch, err := Ruleset(m.cfg, sortedAllocations(m.allocs))
	if err != nil {
		return err
	}
	if err := m.deps.runner().Run(ctx, "nft", []string{"-f", "-"}, []byte(batch)); err != nil {
		return fmt.Errorf("nodenet: apply nftables rules: %w", err)
	}
	return nil
}

// CheckHost verifies the facilities Setup depends on, without changing
// anything: the ip and nft binaries (probed with ip -Version and nft --version,
// the spellings those tools actually support), IPv4 forwarding, the ingress
// address being local, no other host interface holding an address inside (or a
// connected network overlapping) the guest prefix, the bridge name not being taken by another kind of link,
// and the TAP and vhost-net devices. Production always uses vhost-net, so a
// missing /dev/vhost-net is an error, not a silent userspace fallback. Node
// startup calls it and fails closed.
func (m *Manager) CheckHost(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return ErrClosed
	}
	for _, probe := range []struct {
		binary string
		args   []string
	}{
		// iproute2 has no --version: Ubuntu's ip exits 255 with 'Option
		// -version is unknown'. Its supported spelling is -Version (or -V). nft
		// does accept --version.
		{"ip", []string{"-Version"}},
		{"nft", []string{"--version"}},
	} {
		if _, err := m.output(ctx, probe.binary, probe.args...); err != nil {
			return fmt.Errorf("nodenet: %s is required (probed with %s %s): %w",
				probe.binary, probe.binary, strings.Join(probe.args, " "), err)
		}
	}
	// The host's own addresses must not overlap the guest prefix. Checked before
	// anything else about the network so a clash is reported as such.
	addrs, err := m.hostAddrs(ctx)
	if err != nil {
		return err
	}
	if err := guestPrefixConflict(m.cfg.Prefix, addrs); err != nil {
		return err
	}
	forward, err := m.readSysctl(ctx, "net/ipv4/ip_forward")
	if err != nil {
		return fmt.Errorf("nodenet: read net.ipv4.ip_forward: %w", err)
	}
	if strings.TrimSpace(string(forward)) != "1" {
		return errors.New("nodenet: net.ipv4.ip_forward must be 1: the node routes its guests")
	}
	if err := checkLocalAddr(m.cfg.IngressAddr, addrs); err != nil {
		return err
	}
	kind, err := m.linkKind(ctx, m.cfg.Bridge)
	if err != nil {
		return err
	}
	if kind != "" && kind != "bridge" {
		return fmt.Errorf("nodenet: link %q exists as %s, refusing to use it as the VM bridge", m.cfg.Bridge, kind)
	}
	for _, device := range m.devices() {
		f, err := os.OpenFile(device, os.O_RDWR, 0)
		if err != nil {
			return fmt.Errorf("nodenet: open %s: %w", device, err)
		}
		f.Close()
	}
	m.log.Debug("nodenet: host ready", "bridge", m.cfg.Bridge, "prefix", m.cfg.Prefix)
	return nil
}

func (m *Manager) devices() []string { return []string{tunDevice, vhostNetDevice} }

// checkLocalAddr verifies that the configured ingress address belongs to one of
// the host's interfaces. DNAT to an address the node does not own would accept
// nothing, which is a configuration error rather than a silent no-op.
func checkLocalAddr(addr netip.Addr, addrs []hostAddr) error {
	if !addr.IsValid() {
		return nil
	}
	for _, a := range addrs {
		if a.Addr.Addr() == addr {
			return nil
		}
	}
	return fmt.Errorf("nodenet: ingress address %s is not assigned to a host interface", addr)
}

// readSysctl reads a /proc/sys value in the process's own network namespace.
// It is a seam so tests and the namespaced integration test can read the value
// where the manager actually runs, never the outer VM's namespace by accident.
func readSysctl(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.ReadFile(filepath.Join("/proc/sys", filepath.FromSlash(name)))
}
