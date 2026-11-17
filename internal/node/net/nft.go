package nodenet

import (
	"fmt"
	"net/netip"
	"slices"
	"strings"
)

// The two nftables tables this package owns. They are re-created in full on
// every change: no other table is named, flushed or deleted, so hardening's
// inet bbraft_filter keeps working untouched.
//
// Two families are required, not one:
//
//   - bridge bbraft_bridge sees every frame at the bridge ports, with iifname
//     still being the enslaved TAP. That is the only place where a guest's
//     source MAC and source IPv4 can be checked before the frame is bridged or
//     routed, and the only place ARP can be inspected (the arp family has no
//     forward hook, and the inet forward hook only sees the bridge device).
//   - inet bbraft_net sees routed IPv4: VM-to-host input delivery, guest
//     egress, NAT and the return path.
const (
	inetTableName   = "bbraft_net"
	bridgeTableName = "bbraft_bridge"
)

// Ruleset renders the complete nftables batch for the given allocations. The
// output is deterministic for equal input: addresses, TAPs and ports are
// sorted, so a golden test detects any rule change.
//
// The batch first declares and deletes each owned table (the idempotent way to
// replace a table without `flush ruleset`), then defines it again with the
// current allocations. Applying it is one atomic nft transaction.
func Ruleset(cfg Config, allocs []Allocation) (string, error) {
	cfg = cfg.normalize()
	if err := cfg.Validate(); err != nil {
		return "", err
	}
	sorted := slices.Clone(allocs)
	slices.SortFunc(sorted, func(a, b Allocation) int {
		if c := a.GuestIP.Compare(b.GuestIP); c != 0 {
			return c
		}
		return strings.Compare(a.SandboxID, b.SandboxID)
	})
	for _, a := range sorted {
		if err := a.Validate(); err != nil {
			return "", err
		}
		if err := a.matchesConfig(cfg); err != nil {
			return "", err
		}
	}

	var b strings.Builder
	b.WriteString("# Managed by bbraft-node; replaced atomically. Do not edit.\n")
	fmt.Fprintf(&b, "# Isolation for VM TAPs on %s. Own tables only: hardening's inet\n", cfg.Bridge)
	b.WriteString("# bbraft_filter is never named, flushed or edited by this ruleset.\n\n")

	// Declare-then-delete is the idempotent replacement idiom. `flush ruleset`
	// would destroy tailscaled's tables and hardening's filter.
	fmt.Fprintf(&b, "table bridge %s\ndelete table bridge %s\n", bridgeTableName, bridgeTableName)
	fmt.Fprintf(&b, "table inet %s\ndelete table inet %s\n\n", inetTableName, inetTableName)

	writeBridgeTable(&b, cfg, sorted)
	b.WriteString("\n")
	writeInetTable(&b, cfg, sorted)
	return b.String(), nil
}

// writeBridgeTable emits the L2 table. Source checks live in a regular chain
// jumped from both the input and forward hooks so a frame is validated whether
// the host or another port is its destination.
func writeBridgeTable(b *strings.Builder, cfg Config, allocs []Allocation) {
	fmt.Fprintf(b, "table bridge %s {\n", bridgeTableName)
	writeSet(b, "vm_ports", "ifname", tapStrings(allocs))
	b.WriteString("\n\t# Source validation for every frame a guest TAP emits. The arp\n")
	b.WriteString("\t# family has no forward hook, so ARP is inspected here.\n")
	b.WriteString("\tchain vm_source {\n")
	for _, a := range allocs {
		fmt.Fprintf(b, "\t\tiifname \"%s\" ether saddr != %s drop\n", a.TAPName, a.MAC)
		fmt.Fprintf(b, "\t\tiifname \"%s\" ether type != { ip, arp } drop\n", a.TAPName)
		fmt.Fprintf(b, "\t\tiifname \"%s\" ether type ip ip saddr != %s drop\n", a.TAPName, a.GuestIP)
		fmt.Fprintf(b, "\t\tiifname \"%s\" ether type arp arp saddr ip != %s drop\n", a.TAPName, a.GuestIP)
		fmt.Fprintf(b, "\t\tiifname \"%s\" ether type arp arp saddr ether != %s drop\n", a.TAPName, a.MAC)
	}
	if len(allocs) == 0 {
		b.WriteString("\t\t# No guests allocated.\n")
	}
	b.WriteString("\t}\n\n")

	b.WriteString("\tchain input {\n")
	b.WriteString("\t\ttype filter hook input priority filter; policy accept;\n")
	b.WriteString("\t\tjump vm_source\n")
	b.WriteString("\t}\n\n")

	b.WriteString("\tchain forward {\n")
	b.WriteString("\t\ttype filter hook forward priority filter; policy accept;\n")
	b.WriteString("\t\tjump vm_source\n")
	b.WriteString("\t\t# No frame may pass between two guest TAPs, even with forged L2.\n")
	b.WriteString("\t\t# The bridge slave isolation flag is the first line; this rule also\n")
	b.WriteString("\t\t# covers broadcast and hairpin copies.\n")
	fmt.Fprintf(b, "\t\tiifname @vm_ports oifname @vm_ports drop\n")
	b.WriteString("\t\t# ARP never crosses the bridge: guests only resolve the gateway.\n")
	b.WriteString("\t\tether type arp drop\n")
	b.WriteString("\t}\n")
	b.WriteString("}\n")
}

func writeInetTable(b *strings.Builder, cfg Config, allocs []Allocation) {
	fmt.Fprintf(b, "table inet %s {\n", inetTableName)

	writeSet(b, "vm_addrs", "ipv4_addr", addrStrings(allocs))
	writeSet(b, "dns_addrs", "ipv4_addr", resolverStrings(cfg.Resolvers))

	b.WriteString("\n\tchain input {\n")
	b.WriteString("\t\ttype filter hook input priority filter; policy accept;\n")
	b.WriteString("\t\t# Nothing a guest sends may reach a service on the node itself.\n")
	fmt.Fprintf(b, "\t\tiifname \"%s\" drop\n", cfg.Bridge)
	b.WriteString("\t}\n\n")

	b.WriteString("\tchain forward {\n")
	b.WriteString("\t\ttype filter hook forward priority filter; policy accept;\n")
	b.WriteString("\t\t# Nothing may traverse the node between two guests, whatever\n")
	b.WriteString("\t\t# conntrack says. This also blocks a hairpin through DNAT.\n")
	fmt.Fprintf(b, "\t\tiifname \"%s\" oifname \"%s\" drop\n", cfg.Bridge, cfg.Bridge)
	b.WriteString("\t\t# Inbound is admitted before any egress destination drop, and never\n")
	b.WriteString("\t\t# from a guest: replies to guest flows, and the exact SSH ingress\n")
	b.WriteString("\t\t# tuples this node published. The generic return excludes DNAT\n")
	b.WriteString("\t\t# connections; otherwise an established SSH flow could enter through\n")
	b.WriteString("\t\t# it and bypass the ingress interface binding below.\n")
	fmt.Fprintf(b, "\t\toifname \"%s\" iifname != \"%s\" ct state established,related ct status and dnat != dnat accept\n", cfg.Bridge, cfg.Bridge)
	for _, a := range allocs {
		if a.SSHPort == 0 {
			continue
		}
		fmt.Fprintf(b, "\t\toifname \"%s\" %s ip daddr %s tcp dport 22 ct status dnat "+
			"ct original ip daddr %s ct original proto-dst %d accept\n",
			cfg.Bridge, ingressMatch(cfg), a.GuestIP, cfg.IngressAddr, a.SSHPort)
	}
	fmt.Fprintf(b, "\t\toifname \"%s\" drop\n", cfg.Bridge)

	b.WriteString("\t\t# Egress: routed IPv4 from a guest, sanitized by the bridge table.\n")
	b.WriteString("\t\t# Routed IPv6 per VM is not part of Phase 1.\n")
	fmt.Fprintf(b, "\t\tiifname \"%s\" meta nfproto != ipv4 drop\n", cfg.Bridge)
	b.WriteString("\t\t# Invalid conntrack state is dropped instead of being NATed.\n")
	fmt.Fprintf(b, "\t\tiifname \"%s\" ct state invalid drop\n", cfg.Bridge)
	b.WriteString("\t\t# Replies of our own SSH ingress, for clients (a tailnet address,\n")
	b.WriteString("\t\t# for example) that the egress policy blocks. This is the only\n")
	b.WriteString("\t\t# established exception, and it is bound to the owned DNAT tuple and\n")
	b.WriteString("\t\t# (when configured) leaves through the same external interface the\n")
	b.WriteString("\t\t# connection arrived on.\n")
	replyMatch := fmt.Sprintf("iifname %q", cfg.Bridge)
	if cfg.IngressIface != "" {
		replyMatch += fmt.Sprintf(" oifname %q", cfg.IngressIface)
	}
	for _, a := range allocs {
		if a.SSHPort == 0 {
			continue
		}
		fmt.Fprintf(b, "\t\t%s ip saddr %s tcp sport 22 ct state established ct status dnat "+
			"ct original ip daddr %s ct original proto-dst %d accept\n",
			replyMatch, a.GuestIP, cfg.IngressAddr, a.SSHPort)
	}
	b.WriteString("\t\t# No guest-to-guest path, whether spoofed or routed.\n")
	fmt.Fprintf(b, "\t\tiifname \"%s\" ip daddr @vm_addrs drop\n", cfg.Bridge)
	b.WriteString("\t\t# No route into the node's private, link-local, management or\n")
	b.WriteString("\t\t# loopback networks; the guest prefix itself is included.\n")
	fmt.Fprintf(b, "\t\tiifname \"%s\" ip daddr { %s } drop\n", cfg.Bridge, prefixStrings(blockedDestinations(cfg)))
	b.WriteString("\t\t# DNS answers come from the configured resolvers only.\n")
	fmt.Fprintf(b, "\t\tiifname \"%s\" udp dport 53 ip daddr != @dns_addrs drop\n", cfg.Bridge)
	fmt.Fprintf(b, "\t\tiifname \"%s\" tcp dport 53 ip daddr != @dns_addrs drop\n", cfg.Bridge)
	b.WriteString("\t\t# Everything left is IPv4 egress, subject to NAT below.\n")
	fmt.Fprintf(b, "\t\tiifname \"%s\" accept\n", cfg.Bridge)
	b.WriteString("\t}\n")

	if cfg.IngressAddr.IsValid() {
		b.WriteString("\n\tchain prerouting {\n")
		b.WriteString("\t\ttype nat hook prerouting priority dstnat; policy accept;\n")
		b.WriteString("\t\t# Bounded SSH ingress: one DNAT rule per allocated guest, never\n")
		b.WriteString("\t\t# from the guest bridge (that would let a guest hairpin to a peer).\n")
		b.WriteString("\t\t# When an ingress interface is configured, only frames arriving on\n")
		b.WriteString("\t\t# it are DNATed, even if their destination address matches.\n")
		for _, a := range allocs {
			if a.SSHPort == 0 {
				continue
			}
			fmt.Fprintf(b, "\t\t%s ip daddr %s tcp dport %d dnat ip to %s:22\n",
				ingressMatch(cfg), cfg.IngressAddr, a.SSHPort, a.GuestIP)
		}
		b.WriteString("\t}\n")
	}

	b.WriteString("\n\tchain postrouting {\n")
	b.WriteString("\t\ttype nat hook postrouting priority srcnat; policy accept;\n")
	fmt.Fprintf(b, "\t\tiifname \"%s\" ip saddr @vm_addrs oifname != \"%s\" masquerade\n", cfg.Bridge, cfg.Bridge)
	b.WriteString("\t}\n")
	b.WriteString("}\n")
}

// ingressMatch is the source-interface selector for SSH ingress DNAT and its
// exact forward accept. A configured IngressIface restricts ingress to that
// interface; empty keeps the explicitly unbound library behaviour, where any
// interface except the guest bridge is admitted.
func ingressMatch(cfg Config) string {
	if cfg.IngressIface != "" {
		return fmt.Sprintf("iifname %q", cfg.IngressIface)
	}
	return fmt.Sprintf("iifname != %q", cfg.Bridge)
}

func writeSet(b *strings.Builder, name, typ string, elems []string) {
	fmt.Fprintf(b, "\tset %s {\n\t\ttype %s\n", name, typ)
	if len(elems) > 0 {
		fmt.Fprintf(b, "\t\telements = { %s }\n", strings.Join(elems, ", "))
	}
	b.WriteString("\t}\n")
}

func addrStrings(allocs []Allocation) []string {
	out := make([]string, len(allocs))
	for i, a := range allocs {
		out[i] = a.GuestIP.String()
	}
	return out
}

func tapStrings(allocs []Allocation) []string {
	out := make([]string, len(allocs))
	for i, a := range allocs {
		out[i] = `"` + a.TAPName + `"`
	}
	return out
}

func resolverStrings(addrs []netip.Addr) []string {
	out := make([]string, len(addrs))
	for i, a := range addrs {
		out[i] = a.String()
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func prefixStrings(prefixes []netip.Prefix) string {
	out := make([]string, len(prefixes))
	for i, p := range prefixes {
		out[i] = p.String()
	}
	return strings.Join(out, ", ")
}
