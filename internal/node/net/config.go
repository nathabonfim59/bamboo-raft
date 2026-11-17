package nodenet

import (
	"errors"
	"fmt"
	"net/netip"
	"path/filepath"
	"slices"
	"strings"
)

const (
	// maxResolvers bounds Config.Resolvers. A guest needs a handful at most,
	// and every resolver becomes a rule-set element.
	maxResolvers = 8

	// tapMTU is the MTU of every TAP. The uplink MTU is not probed: guests
	// speak Ethernet to the bridge and the node's uplink MTU is the operator's
	// concern (jumbo frames are not part of Phase 1).
	tapMTU = 1500

	// minPort is the lowest SSH ingress port this package will DNAT. Privileged
	// host ports are never taken from another service by accident.
	minPort = 1024
)

// PortRange is an inclusive TCP port range for SSH ingress DNAT.
type PortRange struct {
	// Min and Max bound the range; both are inclusive.
	Min uint16
	Max uint16
}

// Contains reports whether p is inside the range.
func (r PortRange) Contains(p uint16) bool { return p >= r.Min && p <= r.Max }

// Len is the number of ports in the range; it is zero when Min > Max.
func (r PortRange) Len() int {
	if r.Min > r.Max {
		return 0
	}
	return int(r.Max) - int(r.Min) + 1
}

// Config is one node's VM networking configuration. It contains no guest
// data and no credentials.
type Config struct {
	// Bridge is the per-node bridge that carries every VM TAP, for example
	// "bbraft0". It is created when absent.
	Bridge string
	// Prefix is the private IPv4 range guests are allocated from, for example
	// 10.42.0.0/16. It must be a /8 to /30; host bits are ignored.
	Prefix netip.Prefix
	// Gateway is the node's address on the bridge and the guests' default
	// gateway. It must be inside Prefix and is never allocated to a guest.
	Gateway netip.Addr
	// Resolvers are the guests' DNS servers. They must be public unicast IPv4
	// addresses: resolvers inside Prefix or inside a blocked range make no
	// sense and are rejected (fail closed).
	Resolvers []netip.Addr
	// Management lists the networks a guest must never reach, typically the
	// node's management/tailnet network. Prefix overlaps are rejected.
	Management []netip.Prefix
	// IngressAddr is the host address on which SSH ingress ports are published
	// (the node's reachable address). Empty disables SSH ingress, in which case
	// SSHPorts must be zero. A private address is allowed: the first deploy is
	// tailnet-only, and the operator chooses the exposure explicitly.
	IngressAddr netip.Addr
	// IngressIface, when non-empty, names the host interface SSH ingress is
	// bound to, for example "tailscale0". It must not be Bridge. Binding keeps
	// a frame that arrives on another interface from being DNATed to a guest,
	// even when its destination address is IngressAddr. Empty preserves the
	// explicitly unbound library behaviour: ingress is accepted on every
	// interface except the guest bridge.
	IngressIface string
	// SSHPorts is the bounded pool of published ports. It is required when
	// IngressAddr is set.
	SSHPorts PortRange
	// StateDir holds one JSON allocation file per sandbox and is the restart
	// identity. It must be an absolute, clean path.
	StateDir string
}

// Validate checks the configuration without touching the host. It is called by
// New, Ruleset and every entry point that trusts the configuration.
func (c Config) Validate() error {
	if err := validIfaceName(c.Bridge); err != nil {
		return fmt.Errorf("nodenet: bridge: %w", err)
	}
	if !c.Prefix.IsValid() || !c.Prefix.Addr().Is4() || c.Prefix.Addr().Is4In6() {
		return fmt.Errorf("nodenet: prefix %q is not an IPv4 prefix", c.Prefix)
	}
	if bits := c.Prefix.Bits(); bits < 8 || bits > 30 {
		return fmt.Errorf("nodenet: prefix %s must be between /8 and /30", c.Prefix)
	}
	if !isPrivatePrefix(c.Prefix) {
		return fmt.Errorf("nodenet: guest prefix %s is not a private range (10.0.0.0/8, 172.16.0.0/12 or 192.168.0.0/16)", c.Prefix)
	}
	if err := validAddr("gateway", c.Gateway); err != nil {
		return err
	}
	if !c.Prefix.Contains(c.Gateway) {
		return fmt.Errorf("nodenet: gateway %s is outside %s", c.Gateway, c.Prefix)
	}
	if c.Gateway == c.Prefix.Masked().Addr() {
		return fmt.Errorf("nodenet: gateway %s is the network address of %s", c.Gateway, c.Prefix)
	}
	if c.Gateway == lastAddr(c.Prefix.Masked()) {
		return fmt.Errorf("nodenet: gateway %s is the broadcast address of %s", c.Gateway, c.Prefix)
	}
	if len(c.Resolvers) == 0 {
		return errors.New("nodenet: at least one resolver is required")
	}
	if len(c.Resolvers) > maxResolvers {
		return fmt.Errorf("nodenet: %d resolvers exceed the maximum of %d", len(c.Resolvers), maxResolvers)
	}
	for _, r := range c.Resolvers {
		if err := validAddr("resolver", r); err != nil {
			return err
		}
		if !isPublicIPv4(r) {
			return fmt.Errorf("nodenet: resolver %s is not a public unicast address", r)
		}
		if c.Prefix.Contains(r) {
			return fmt.Errorf("nodenet: resolver %s is inside the guest prefix %s", r, c.Prefix)
		}
		for _, p := range c.Management {
			if p.Contains(r) {
				return fmt.Errorf("nodenet: resolver %s is inside management network %s", r, p)
			}
		}
	}
	for _, p := range c.Management {
		if !p.IsValid() || !p.Addr().Is4() || p.Addr().Is4In6() {
			return fmt.Errorf("nodenet: management %q is not an IPv4 prefix", p)
		}
		if p.Overlaps(c.Prefix) {
			return fmt.Errorf("nodenet: management %s overlaps the guest prefix %s", p, c.Prefix)
		}
	}
	if err := c.validateIngress(); err != nil {
		return err
	}
	if len(c.StateDir) == 0 {
		return errors.New("nodenet: state directory is empty")
	}
	if !isCleanAbsPath(c.StateDir) {
		return fmt.Errorf("nodenet: state directory %q is not an absolute clean path", c.StateDir)
	}
	return nil
}

// validateIngress checks the SSH ingress address, port pool and optional
// interface binding as one unit. The interface uses the same strict,
// nft-safe naming as every other interface this package names and must never
// be the guest bridge: binding ingress to the bridge would admit guest traffic.
func (c Config) validateIngress() error {
	if c.IngressIface != "" {
		if err := validIfaceName(c.IngressIface); err != nil {
			return fmt.Errorf("nodenet: ingress interface: %w", err)
		}
		if c.IngressIface == c.Bridge {
			return fmt.Errorf("nodenet: ingress interface %q is the VM bridge", c.IngressIface)
		}
	}
	if c.IngressAddr.IsValid() {
		if err := validAddr("ingress address", c.IngressAddr); err != nil {
			return err
		}
		if !c.IngressAddr.IsGlobalUnicast() {
			return fmt.Errorf("nodenet: ingress address %s is not a unicast address", c.IngressAddr)
		}
		// The ingress address may sit inside a management network: a tailnet
		// address is the normal case for the first deploy. The DNAT path is
		// deliberate and separate from guest egress, which still cannot reach
		// that network (see Ruleset).
		if c.Prefix.Contains(c.IngressAddr) {
			return fmt.Errorf("nodenet: ingress address %s is inside the guest prefix %s", c.IngressAddr, c.Prefix)
		}
		if c.SSHPorts == (PortRange{}) || c.SSHPorts.Len() == 0 {
			return errors.New("nodenet: SSH ingress requires a non-empty port range")
		}
		if c.SSHPorts.Min < minPort {
			return fmt.Errorf("nodenet: SSH ingress ports must start at or above %d", minPort)
		}
		return nil
	}
	if c.SSHPorts != (PortRange{}) {
		return errors.New("nodenet: SSH ingress ports are set without an ingress address")
	}
	if c.IngressIface != "" {
		return errors.New("nodenet: SSH ingress interface is set without an ingress address")
	}
	return nil
}

// normalize returns a copy with masked, de-duplicated prefixes and cloned
// slices, so a caller cannot mutate the manager's configuration through a
// shared backing array.
func (c Config) normalize() Config {
	c.Prefix = c.Prefix.Masked()
	c.Resolvers = slices.Clone(c.Resolvers)
	if len(c.Management) > 0 {
		mgmt := make([]netip.Prefix, 0, len(c.Management))
		for _, p := range c.Management {
			mgmt = append(mgmt, p.Masked())
		}
		slices.SortFunc(mgmt, comparePrefix)
		c.Management = slices.CompactFunc(mgmt, func(a, b netip.Prefix) bool { return a == b })
	}
	return c
}

// validAddr rejects empty, IPv6-mapped and zoned addresses.
func validAddr(what string, a netip.Addr) error {
	if !a.IsValid() || !a.Is4() || a.Is4In6() {
		return fmt.Errorf("nodenet: %s %q is not an IPv4 address", what, a)
	}
	if a.Zone() != "" {
		return fmt.Errorf("nodenet: %s %s has a zone", what, a)
	}
	return nil
}

func isCleanAbsPath(p string) bool { return filepath.IsAbs(p) && filepath.Clean(p) == p }

// validIfaceName enforces the kernel's IFNAMSIZ limit and the characters the
// host accepts, and rejects names that would be ambiguous as an ip argument.
func validIfaceName(name string) error {
	if name == "" {
		return errors.New("name is empty")
	}
	// IFNAMSIZ includes the terminating NUL, so 15 bytes is the maximum.
	if len(name) >= 16 {
		return fmt.Errorf("%q is longer than %d bytes", name, 15)
	}
	if name == "." || name == ".." {
		return fmt.Errorf("%q is not an interface name", name)
	}
	if strings.HasPrefix(name, "-") {
		return fmt.Errorf("%q starts with a dash", name)
	}
	// Quotes, backslashes and control characters are rejected even though the
	// name reaches nft inside a quoted string: an interface name is interpolated
	// into the ruleset, and escaping must never be the only defence.
	for i := range len(name) {
		c := name[i]
		if c <= 0x1f || c == 0x7f {
			return fmt.Errorf("%q contains a control character", name)
		}
	}
	if strings.ContainsAny(name, "/:\\\"'` $|&;()<>{}[]*?\x00\n\r\t ") {
		return fmt.Errorf("%q contains a forbidden character", name)
	}
	if name == "lo" {
		return errors.New(`"lo" is reserved`)
	}
	return nil
}

// privateGuestRanges are the only ranges a guest prefix may come from: a guest
// must never be handed an address that also exists on the public internet.
var privateGuestRanges = []netip.Prefix{
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.168.0.0/16"),
}

// isPrivatePrefix reports whether p is wholly inside one private range.
func isPrivatePrefix(p netip.Prefix) bool {
	p = p.Masked()
	for _, r := range privateGuestRanges {
		if p.Bits() >= r.Bits() && r.Contains(p.Addr()) {
			return true
		}
	}
	return false
}

// builtinBlocked are destinations no guest may reach, independent of
// configuration: this-network, loopback, RFC 1918, CGNAT (also the tailnet),
// link-local, multicast and reserved space.
var builtinBlocked = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
}

// blockedDestinations is the configured set rendered into the forward chain:
// the built-in ranges, the operator's management networks and the guest
// prefix itself (unused addresses and the gateway included).
func blockedDestinations(cfg Config) []netip.Prefix {
	out := make([]netip.Prefix, 0, len(builtinBlocked)+len(cfg.Management)+1)
	out = append(out, builtinBlocked...)
	out = append(out, cfg.Management...)
	out = append(out, cfg.Prefix.Masked())
	slices.SortFunc(out, comparePrefix)
	return slices.CompactFunc(out, func(a, b netip.Prefix) bool { return a == b })
}

func comparePrefix(a, b netip.Prefix) int {
	if c := a.Addr().Compare(b.Addr()); c != 0 {
		return c
	}
	return a.Bits() - b.Bits()
}

// isPublicIPv4 reports whether a is globally routable enough to be a DNS
// resolver: unicast and outside every built-in blocked range.
func isPublicIPv4(a netip.Addr) bool {
	if !a.Is4() || a.Is4In6() || !a.IsGlobalUnicast() {
		return false
	}
	for _, p := range builtinBlocked {
		if p.Contains(a) {
			return false
		}
	}
	return true
}

// lastAddr returns the broadcast (all host bits set) address of a masked
// prefix.
func lastAddr(p netip.Prefix) netip.Addr {
	a := p.Addr().As4()
	bits := p.Bits()
	for i := range 4 {
		switch keep := bits - 8*i; {
		case keep >= 8:
			// This octet is fully part of the network prefix.
		case keep <= 0:
			a[i] = 0xff
		default:
			a[i] |= byte(0xff) >> keep
		}
	}
	return netip.AddrFrom4(a)
}
