package nodenet

import (
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// updateGolden rewrites testdata/ when NODENET_UPDATE_GOLDEN is set. The
// goldens are the reviewable contract of every rule this package installs.
const updateGolden = "NODENET_UPDATE_GOLDEN"

func netTestGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if os.Getenv(updateGolden) != "" {
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatalf("write golden: %v", err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	if string(want) == got {
		return
	}
	t.Fatalf("%s does not match the golden file (run with %s=1 to update)\n%s",
		name, updateGolden, netTestDiff(string(want), got))
}

// netTestDiff reports the first differing line with a little context.
func netTestDiff(want, got string) string {
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")
	for i := range max(len(wantLines), len(gotLines)) {
		var w, g string
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if w != g {
			var b strings.Builder
			for j := max(0, i-2); j < i; j++ {
				b.WriteString("  " + wantLines[j] + "\n")
			}
			b.WriteString("- " + w + "\n")
			b.WriteString("+ " + g + "\n")
			return b.String()
		}
	}
	return "golden and rendered output differ only in trailing content"
}

func TestRulesetGolden(t *testing.T) {
	tests := []struct {
		name   string
		config func(*Config)
		allocs func(Config) []Allocation
	}{
		{
			name: "ingress",
			allocs: func(cfg Config) []Allocation {
				return []Allocation{
					netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000),
					netTestAllocation(cfg, "bx_98765432", netip.MustParseAddr("10.42.0.3"), 30001),
				}
			},
		},
		{
			name: "no-ingress",
			config: func(cfg *Config) {
				cfg.IngressAddr = netip.Addr{}
				cfg.SSHPorts = PortRange{}
			},
			allocs: func(cfg Config) []Allocation {
				return []Allocation{
					netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 0),
				}
			},
		},
		{
			name: "ingress-interface",
			config: func(cfg *Config) {
				cfg.IngressIface = "up0"
			},
			allocs: func(cfg Config) []Allocation {
				return []Allocation{
					netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000),
					netTestAllocation(cfg, "bx_98765432", netip.MustParseAddr("10.42.0.3"), 30001),
				}
			},
		},
		{
			name: "empty",
			allocs: func(Config) []Allocation {
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := netTestConfig(t)
			if tt.config != nil {
				tt.config(&cfg)
			}
			got, err := Ruleset(cfg, tt.allocs(cfg))
			if err != nil {
				t.Fatalf("Ruleset: %v", err)
			}
			netTestGolden(t, "ruleset-"+tt.name+".nft", got)
		})
	}
}

// TestRulesetDeterministic proves the batch does not depend on map or caller
// ordering.
func TestRulesetDeterministic(t *testing.T) {
	cfg := netTestConfig(t)
	allocs := []Allocation{
		netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000),
		netTestAllocation(cfg, "bx_98765432", netip.MustParseAddr("10.42.0.3"), 30001),
	}
	first, err := Ruleset(cfg, allocs)
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	slices.Reverse(allocs)
	second, err := Ruleset(cfg, allocs)
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	if first != second {
		t.Fatal("Ruleset depends on allocation order")
	}
	// An allocation's own fields are not order-dependent either.
	allocs[0], allocs[1] = allocs[1], allocs[0]
	third, err := Ruleset(cfg, allocs)
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	if first != third {
		t.Fatal("Ruleset depends on allocation order")
	}
}

// TestRulesetNeverTouchesForeignState is the ownership guarantee: the batch
// can only create, delete and define this package's two tables.
func TestRulesetNeverTouchesForeignState(t *testing.T) {
	cfg := netTestConfig(t)
	allocs := []Allocation{
		netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000),
	}
	batch, err := Ruleset(cfg, allocs)
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	if strings.Contains(batch, "flush ruleset") {
		t.Fatal("ruleset must never flush the whole ruleset")
	}
	for _, line := range nonCommentLines(batch) {
		statement := strings.TrimPrefix(line, "delete ")
		if strings.HasPrefix(statement, "table ") {
			fields := strings.Fields(statement)
			if len(fields) < 3 {
				t.Fatalf("malformed table statement %q", line)
			}
			family, name := fields[1], fields[2]
			owned := family == "inet" && name == inetTableName || family == "bridge" && name == bridgeTableName
			if !owned {
				t.Fatalf("ruleset touches a foreign table: %q", line)
			}
		}
		if strings.Contains(line, "bbraft_filter") {
			t.Fatalf("ruleset references the hardening table: %q", line)
		}
		if strings.Contains(line, "flush") {
			t.Fatalf("ruleset flushes something: %q", line)
		}
	}
	if !strings.Contains(batch, "delete table bridge "+bridgeTableName) ||
		!strings.Contains(batch, "delete table inet "+inetTableName) {
		t.Fatal("ruleset must replace both owned tables idempotently")
	}
}

// TestRulesetDeniesByDefault asserts the security-critical rules exist in the
// right chain, independent of the golden files.
func TestRulesetDeniesByDefault(t *testing.T) {
	cfg := netTestConfig(t)
	alloc := netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
	batch, err := Ruleset(cfg, []Allocation{alloc})
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	required := []string{
		// L2 source validation and isolation live in the bridge family.
		`iifname "` + alloc.TAPName + `" ether saddr != ` + alloc.MAC + ` drop`,
		`iifname "` + alloc.TAPName + `" ether type != { ip, arp } drop`,
		`iifname "` + alloc.TAPName + `" ether type ip ip saddr != ` + alloc.GuestIP.String() + ` drop`,
		`iifname "` + alloc.TAPName + `" ether type arp arp saddr ip != ` + alloc.GuestIP.String() + ` drop`,
		`iifname "` + alloc.TAPName + `" ether type arp arp saddr ether != ` + alloc.MAC + ` drop`,
		`iifname @vm_ports oifname @vm_ports drop`,
		`ether type arp drop`,
		// No hairpin between guests, before any established or DNAT accept.
		`iifname "bbraft0" oifname "bbraft0" drop`,
		// Routed isolation.
		`iifname "bbraft0" drop`,
		`iifname "bbraft0" meta nfproto != ipv4 drop`,
		`iifname "bbraft0" ct state invalid drop`,
		`iifname "bbraft0" ip daddr @vm_addrs drop`,
		`iifname "bbraft0" ip daddr { 0.0.0.0/8, 10.0.0.0/8, 10.42.0.0/16, 100.64.0.0/10, 127.0.0.0/8, 169.254.0.0/16, 172.16.0.0/12, 192.0.2.0/24, 192.168.0.0/16, 224.0.0.0/4, 240.0.0.0/4 } drop`,
		`iifname "bbraft0" udp dport 53 ip daddr != @dns_addrs drop`,
		`iifname "bbraft0" tcp dport 53 ip daddr != @dns_addrs drop`,
		// Inbound is only return traffic and the owned SSH tuple, never from a
		// guest. The generic return excludes DNAT flows so an established
		// ingress connection cannot bypass the exact tuple's scope.
		`oifname "bbraft0" iifname != "bbraft0" ct state established,related ct status and dnat != dnat accept`,
		`oifname "bbraft0" iifname != "bbraft0" ip daddr 10.42.0.2 tcp dport 22 ct status dnat ct original ip daddr 203.0.113.10 ct original proto-dst 30000 accept`,
		`oifname "bbraft0" drop`,
		// The SSH reply path for a client the egress policy blocks.
		`iifname "bbraft0" ip saddr 10.42.0.2 tcp sport 22 ct state established ct status dnat ct original ip daddr 203.0.113.10 ct original proto-dst 30000 accept`,
		// NAT in both directions, never applied to guest-originated ingress.
		`iifname != "bbraft0" ip daddr 203.0.113.10 tcp dport 30000 dnat ip to 10.42.0.2:22`,
		`iifname "bbraft0" ip saddr @vm_addrs oifname != "bbraft0" masquerade`,
	}
	for _, want := range required {
		if !strings.Contains(batch, want) {
			t.Errorf("ruleset is missing %q", want)
		}
	}
}

// TestRulesetInboundAcceptPrecedesEgressDrops proves the inbound patterns are
// evaluated before the private-destination drops, so a tailnet SSH peer is
// admitted while a guest-originated tailnet connection is not, and that the
// SSH reply for such a peer is admitted only by the scoped rule.
func TestRulesetInboundAcceptPrecedesEgressDrops(t *testing.T) {
	cfg := netTestConfig(t)
	alloc := netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
	batch, err := Ruleset(cfg, []Allocation{alloc})
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	hairpin := strings.Index(batch, `iifname "bbraft0" oifname "bbraft0" drop`)
	returnAccept := strings.Index(batch, `oifname "bbraft0" iifname != "bbraft0" ct state established,related ct status and dnat != dnat accept`)
	tupleAccept := strings.Index(batch, `oifname "bbraft0" iifname != "bbraft0" ip daddr 10.42.0.2`)
	invalidDrop := strings.Index(batch, `iifname "bbraft0" ct state invalid drop`)
	replyAccept := strings.Index(batch, `iifname "bbraft0" ip saddr 10.42.0.2 tcp sport 22 ct state established`)
	blockedDrop := strings.Index(batch, `ip daddr { 0.0.0.0/8`)
	tailnetDrop := strings.Index(batch, "100.64.0.0/10")
	egressAccept := strings.Index(batch, `iifname "bbraft0" accept`)
	for name, index := range map[string]int{
		"hairpin": hairpin, "return": returnAccept, "tuple": tupleAccept,
		"invalid": invalidDrop, "reply": replyAccept, "blocked": blockedDrop,
		"tailnet": tailnetDrop, "egress accept": egressAccept,
	} {
		if index < 0 {
			t.Fatalf("%s rule is missing", name)
		}
	}
	if !(hairpin < returnAccept && returnAccept < tupleAccept) {
		t.Fatalf("inbound rule order is wrong: hairpin=%d return=%d tuple=%d", hairpin, returnAccept, tupleAccept)
	}
	// The SSH reply must be admitted before the destination policy drops it.
	if !(invalidDrop < replyAccept && replyAccept < blockedDrop && blockedDrop < tailnetDrop) {
		t.Fatalf("ssh reply rule order is wrong: invalid=%d reply=%d blocked=%d tailnet=%d",
			invalidDrop, replyAccept, blockedDrop, tailnetDrop)
	}
	if egressAccept < tailnetDrop {
		t.Fatal("the egress accept must come after every destination drop")
	}
	// Inbound admission must be bound to the owned original tuple, not to any
	// DNAT to guest 22.
	if strings.Contains(batch, "ct status dnat accept") {
		t.Fatal("inbound SSH acceptance must be bound to the owned original tuple")
	}
	for _, line := range nonCommentLines(batch) {
		if !strings.Contains(line, "dnat ip to") {
			continue
		}
		if !strings.HasPrefix(line, `iifname != "bbraft0"`) {
			t.Fatalf("SSH DNAT must not match packets arriving from the guest bridge: %q", line)
		}
	}
}

// TestRulesetHasNoUnscopedEgressEstablishedRule guards the only established
// exception on the egress path: the reply of our own SSH ingress.
func TestRulesetHasNoUnscopedEgressEstablishedRule(t *testing.T) {
	cfg := netTestConfig(t)
	allocs := []Allocation{
		netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000),
		netTestAllocation(cfg, "bx_98765432", netip.MustParseAddr("10.42.0.3"), 30001),
	}
	batch, err := Ruleset(cfg, allocs)
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	found := 0
	for _, line := range nonCommentLines(batch) {
		if !strings.Contains(line, "ct state established") || !strings.Contains(line, `iifname "bbraft0"`) {
			continue
		}
		found++
		for _, want := range []string{"tcp sport 22", "ct status dnat", "ct original ip daddr", "ct original proto-dst", "accept"} {
			if !strings.Contains(line, want) {
				t.Fatalf("egress established rule is not scoped to the owned SSH tuple (missing %q): %q", want, line)
			}
		}
	}
	if want := len(allocs); found != want {
		t.Errorf("scoped SSH reply rules = %d, want %d", found, want)
	}
}

// TestRulesetIngressInterfaceScope pins the interface restriction for SSH
// ingress: when Config.IngressIface is set, DNAT and the exact inbound accept
// name it, the generic established return must not admit a DNAT flow, and the
// guest-side reply leaves through the same external interface while keeping its
// guest-side match.
func TestRulesetIngressInterfaceScope(t *testing.T) {
	cfg := netTestConfig(t)
	cfg.IngressIface = "up0"
	alloc := netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
	batch, err := Ruleset(cfg, []Allocation{alloc})
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}

	// Every DNAT is scoped to the configured interface; neither the bridge nor
	// the unbound wildcard may appear in the match.
	dnat := 0
	for _, line := range nonCommentLines(batch) {
		if !strings.Contains(line, "dnat ip to") {
			continue
		}
		dnat++
		if !strings.HasPrefix(line, `iifname "up0" `) {
			t.Errorf("bound SSH DNAT does not start with the configured interface: %q", line)
		}
		if strings.Contains(line, `iifname != "bbraft0"`) {
			t.Errorf("bound SSH DNAT still matches every non-bridge interface: %q", line)
		}
	}
	if dnat != 1 {
		t.Errorf("SSH DNAT rules = %d, want 1", dnat)
	}

	// The generic return accept must exclude DNAT connections, or an already
	// established SSH flow would bypass the interface restriction through it.
	generic := `oifname "bbraft0" iifname != "bbraft0" ct state established,related ct status and dnat != dnat accept`
	if !strings.Contains(batch, generic) {
		t.Errorf("ruleset is missing the non-DNAT generic return accept %q", generic)
	}

	// The exact owned tuple is the only inbound accept for a DNAT flow, names
	// the configured interface, and has no state qualifier: every packet of the
	// owned connection is accepted there. No other bridge-output established
	// accept may admit a DNAT flow.
	for _, line := range nonCommentLines(batch) {
		if !strings.Contains(line, `oifname "bbraft0"`) ||
			!strings.Contains(line, "ct state established") ||
			strings.Contains(line, "ct status dnat") {
			continue
		}
		if !strings.Contains(line, "ct status and dnat != dnat") {
			t.Errorf("an established bridge-output accept may admit a DNAT flow: %q", line)
		}
	}
	wantTuple := `oifname "bbraft0" iifname "up0" ip daddr 10.42.0.2 tcp dport 22 ct status dnat ct original ip daddr 203.0.113.10 ct original proto-dst 30000 accept`
	if !strings.Contains(batch, wantTuple) {
		t.Errorf("ruleset is missing the interface-bound exact SSH accept %q", wantTuple)
	}

	// The reply keeps the guest-side match and leaves only through the bound
	// external interface.
	wantReply := `iifname "bbraft0" oifname "up0" ip saddr 10.42.0.2 tcp sport 22 ct state established ct status dnat ct original ip daddr 203.0.113.10 ct original proto-dst 30000 accept`
	if !strings.Contains(batch, wantReply) {
		t.Errorf("ruleset is missing the interface-bound SSH reply accept %q", wantReply)
	}

	// Ordering: hairpin drop, generic return, exact tuple, bridge output drop.
	hairpin := strings.Index(batch, `iifname "bbraft0" oifname "bbraft0" drop`)
	genericIdx := strings.Index(batch, generic)
	tupleIdx := strings.Index(batch, wantTuple)
	dropIdx := strings.Index(batch, "\n\t\toifname \"bbraft0\" drop\n")
	for name, index := range map[string]int{
		"hairpin": hairpin, "generic return": genericIdx, "exact tuple": tupleIdx, "bridge drop": dropIdx,
	} {
		if index < 0 {
			t.Fatalf("%s rule is missing", name)
		}
	}
	if !(hairpin < genericIdx && genericIdx < tupleIdx && tupleIdx < dropIdx) {
		t.Fatalf("inbound rule order is wrong: hairpin=%d generic=%d tuple=%d drop=%d",
			hairpin, genericIdx, tupleIdx, dropIdx)
	}
}

// TestRulesetUnboundIngressStaysExplicitlyUnbound pins the compatibility
// guarantee of an empty IngressIface: DNAT and its exact accept admit every
// non-bridge interface and the reply has no external-interface guard.
func TestRulesetUnboundIngressStaysExplicitlyUnbound(t *testing.T) {
	cfg := netTestConfig(t)
	alloc := netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
	batch, err := Ruleset(cfg, []Allocation{alloc})
	if err != nil {
		t.Fatalf("Ruleset: %v", err)
	}
	dnat := 0
	for _, line := range nonCommentLines(batch) {
		if !strings.Contains(line, "dnat ip to") {
			continue
		}
		dnat++
		if !strings.HasPrefix(line, `iifname != "bbraft0" `) {
			t.Errorf("unbound SSH DNAT is no longer keyed to the non-bridge wildcard: %q", line)
		}
	}
	if dnat != 1 {
		t.Errorf("SSH DNAT rules = %d, want 1", dnat)
	}
	if !strings.Contains(batch, `oifname "bbraft0" iifname != "bbraft0" ip daddr 10.42.0.2 tcp dport 22 ct status dnat`) {
		t.Error("unbound exact SSH accept is missing")
	}
	reply := `iifname "bbraft0" ip saddr 10.42.0.2 tcp sport 22 ct state established ct status dnat`
	if !strings.Contains(batch, reply) {
		t.Errorf("unbound SSH reply is missing its guest-side match %q", reply)
	}
	for _, line := range nonCommentLines(batch) {
		if strings.Contains(line, reply) && strings.Contains(line, "oifname ") {
			t.Errorf("unbound SSH reply gained an external interface guard: %q", line)
		}
	}
}

func TestRulesetRejectsForeignAllocations(t *testing.T) {
	cfg := netTestConfig(t)
	foreign := netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
	foreign.Prefix = netip.MustParsePrefix("10.43.0.0/16")
	if _, err := Ruleset(cfg, []Allocation{foreign}); err == nil {
		t.Fatal("Ruleset must reject an allocation from another prefix")
	}
	bad := netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
	bad.MAC = "not-a-mac"
	if _, err := Ruleset(cfg, []Allocation{bad}); err == nil {
		t.Fatal("Ruleset must reject an invalid allocation")
	}
	// The ingress interface is part of the identity too: a ruleset must not
	// silently render an allocation bound to a different interface.
	bound := cfg
	bound.IngressIface = "up0"
	wrongIface := netTestAllocation(bound, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
	wrongIface.IngressIface = "eth0"
	if _, err := Ruleset(bound, []Allocation{wrongIface}); err == nil {
		t.Fatal("Ruleset must reject an allocation bound to another ingress interface")
	}
}
