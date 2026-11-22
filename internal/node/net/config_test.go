package nodenet

import (
	"net"
	"net/netip"
	"strings"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		change  func(*Config)
		wantErr string
	}{
		{name: "reference configuration"},
		{name: "no ssh ingress", change: func(c *Config) {
			c.IngressAddr = netip.Addr{}
			c.SSHPorts = PortRange{}
		}},
		{name: "private ingress allowed for tailnet deployments", change: func(c *Config) {
			c.IngressAddr = netip.MustParseAddr("100.100.1.2")
		}},
		{name: "resolver list of one", change: func(c *Config) {
			c.Resolvers = c.Resolvers[:1]
		}},
		{name: "empty bridge", change: func(c *Config) { c.Bridge = "" }, wantErr: "bridge"},
		{name: "bridge too long", change: func(c *Config) {
			c.Bridge = "bbraft0123456789"
		}, wantErr: "longer than 15"},
		{name: "bridge with slash", change: func(c *Config) { c.Bridge = "br/0" }, wantErr: "forbidden character"},
		{name: "bridge with a quote", change: func(c *Config) { c.Bridge = `br"0` }, wantErr: "forbidden character"},
		{name: "bridge with a backslash", change: func(c *Config) { c.Bridge = `br\0` }, wantErr: "forbidden character"},
		{name: "bridge with a control character", change: func(c *Config) { c.Bridge = "br\x010" }, wantErr: "control character"},
		{name: "bridge with a newline", change: func(c *Config) { c.Bridge = "br\n0" }, wantErr: "control character"},
		{name: "bridge starting with dash", change: func(c *Config) { c.Bridge = "-br0" }, wantErr: "dash"},
		{name: "loopback bridge", change: func(c *Config) { c.Bridge = "lo" }, wantErr: "reserved"},
		{name: "no prefix", change: func(c *Config) { c.Prefix = netip.Prefix{} }, wantErr: "IPv4 prefix"},
		{name: "ipv6 prefix", change: func(c *Config) {
			c.Prefix = netip.MustParsePrefix("fd00::/64")
		}, wantErr: "IPv4 prefix"},
		{name: "prefix too small", change: func(c *Config) {
			c.Prefix = netip.MustParsePrefix("10.42.0.0/31")
		}, wantErr: "/8 and /30"},
		{name: "prefix too large", change: func(c *Config) {
			c.Prefix = netip.MustParsePrefix("10.0.0.0/4")
		}, wantErr: "/8 and /30"},
		{name: "public guest prefix", change: func(c *Config) {
			c.Prefix = netip.MustParsePrefix("203.0.113.0/24")
			c.Gateway = netip.MustParseAddr("203.0.113.1")
		}, wantErr: "not a private range"},
		{name: "cg nat guest prefix", change: func(c *Config) {
			c.Prefix = netip.MustParsePrefix("100.64.0.0/16")
			c.Gateway = netip.MustParseAddr("100.64.0.1")
		}, wantErr: "not a private range"},
		{name: "gateway outside prefix", change: func(c *Config) {
			c.Gateway = netip.MustParseAddr("10.43.0.1")
		}, wantErr: "outside"},
		{name: "gateway is network address", change: func(c *Config) {
			c.Gateway = netip.MustParseAddr("10.42.0.0")
		}, wantErr: "network address"},
		{name: "gateway is broadcast address", change: func(c *Config) {
			c.Gateway = netip.MustParseAddr("10.42.255.255")
		}, wantErr: "broadcast address"},
		{name: "gateway is ipv6", change: func(c *Config) {
			c.Gateway = netip.MustParseAddr("fd00::1")
		}, wantErr: "not an IPv4 address"},
		{name: "no resolvers", change: func(c *Config) { c.Resolvers = nil }, wantErr: "at least one resolver"},
		{name: "private resolver", change: func(c *Config) {
			c.Resolvers = []netip.Addr{netip.MustParseAddr("10.0.0.2")}
		}, wantErr: "not a public"},
		{name: "loopback resolver", change: func(c *Config) {
			c.Resolvers = []netip.Addr{netip.MustParseAddr("127.0.0.1")}
		}, wantErr: "not a public"},
		{name: "cg nat resolver", change: func(c *Config) {
			c.Resolvers = []netip.Addr{netip.MustParseAddr("100.64.0.53")}
		}, wantErr: "not a public"},
		{name: "multicast resolver", change: func(c *Config) {
			c.Resolvers = []netip.Addr{netip.MustParseAddr("224.0.0.1")}
		}, wantErr: "not a public"},
		{name: "too many resolvers", change: func(c *Config) {
			c.Resolvers = make([]netip.Addr, maxResolvers+1)
			for i := range c.Resolvers {
				c.Resolvers[i] = netip.AddrFrom4([4]byte{9, 9, 9, byte(i)})
			}
		}, wantErr: "exceed"},
		{name: "management overlaps prefix", change: func(c *Config) {
			c.Management = []netip.Prefix{netip.MustParsePrefix("10.42.1.0/24")}
		}, wantErr: "overlaps"},
		{name: "resolver inside management", change: func(c *Config) {
			c.Management = []netip.Prefix{netip.MustParsePrefix("1.1.1.0/24")}
		}, wantErr: "resolver 1.1.1.1 is inside management"},
		{name: "management is ipv6", change: func(c *Config) {
			c.Management = []netip.Prefix{netip.MustParsePrefix("fd00::/8")}
		}, wantErr: "not an IPv4 prefix"},
		{name: "ingress inside guest prefix", change: func(c *Config) {
			c.IngressAddr = netip.MustParseAddr("10.42.0.9")
		}, wantErr: "inside the guest prefix"},
		{name: "ingress is the first guest address (the host-sshd e2e case)", change: func(c *Config) {
			c.IngressAddr = netip.MustParseAddr("10.42.0.2")
		}, wantErr: "ingress address 10.42.0.2 is inside the guest prefix"},
		{name: "ingress is the gateway", change: func(c *Config) {
			c.IngressAddr = c.Gateway
		}, wantErr: "inside the guest prefix"},
		{name: "ingress inside management is the tailnet case", change: func(c *Config) {
			c.IngressAddr = netip.MustParseAddr("192.0.2.9")
		}},
		{name: "bound ingress interface", change: func(c *Config) {
			c.IngressIface = "tailscale0"
		}},
		{name: "ingress interface is the bridge", change: func(c *Config) {
			c.IngressIface = c.Bridge
		}, wantErr: "is the VM bridge"},
		{name: "ingress interface with a slash", change: func(c *Config) {
			c.IngressIface = "tail/scale"
		}, wantErr: "forbidden character"},
		{name: "ingress interface with a quote", change: func(c *Config) {
			c.IngressIface = `tail"scale`
		}, wantErr: "forbidden character"},
		{name: "ingress interface with a control character", change: func(c *Config) {
			c.IngressIface = "tail\x01scale"
		}, wantErr: "control character"},
		{name: "loopback ingress interface", change: func(c *Config) {
			c.IngressIface = "lo"
		}, wantErr: "reserved"},
		{name: "ingress interface without an address", change: func(c *Config) {
			c.IngressAddr = netip.Addr{}
			c.SSHPorts = PortRange{}
			c.IngressIface = "tailscale0"
		}, wantErr: "without an ingress address"},
		{name: "ingress is loopback", change: func(c *Config) {
			c.IngressAddr = netip.MustParseAddr("127.0.0.1")
		}, wantErr: "not a unicast"},
		{name: "ingress is unspecified", change: func(c *Config) {
			c.IngressAddr = netip.MustParseAddr("0.0.0.0")
		}, wantErr: "not a unicast"},
		{name: "ingress without ports", change: func(c *Config) {
			c.SSHPorts = PortRange{}
		}, wantErr: "non-empty port range"},
		{name: "ports without ingress", change: func(c *Config) {
			c.IngressAddr = netip.Addr{}
		}, wantErr: "without an ingress address"},
		{name: "privileged port", change: func(c *Config) {
			c.SSHPorts = PortRange{Min: 22, Max: 99}
		}, wantErr: "at or above"},
		{name: "empty port range", change: func(c *Config) {
			c.SSHPorts = PortRange{Min: 30001, Max: 30000}
		}, wantErr: "non-empty port range"},
		{name: "relative state dir", change: func(c *Config) {
			c.StateDir = "state"
		}, wantErr: "absolute"},
		{name: "unclean state dir", change: func(c *Config) {
			c.StateDir = c.StateDir + "/"
		}, wantErr: "absolute"},
		{name: "empty state dir", change: func(c *Config) {
			c.StateDir = ""
		}, wantErr: "empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := netTestConfig(t)
			if tt.change != nil {
				tt.change(&cfg)
			}
			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate: unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate: expected an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestConfigNormalize(t *testing.T) {
	cfg := netTestConfig(t)
	cfg.Prefix = netip.MustParsePrefix("10.42.7.9/16")
	cfg.Management = []netip.Prefix{
		netip.MustParsePrefix("192.0.2.128/24"),
		netip.MustParsePrefix("172.31.0.0/16"),
		netip.MustParsePrefix("192.0.2.0/24"),
	}
	cfg.Resolvers = []netip.Addr{netip.MustParseAddr("8.8.8.8")}

	got := cfg.normalize()
	if want := netip.MustParsePrefix("10.42.0.0/16"); got.Prefix != want {
		t.Errorf("prefix = %s, want %s", got.Prefix, want)
	}
	wantMgmt := []netip.Prefix{
		netip.MustParsePrefix("172.31.0.0/16"),
		netip.MustParsePrefix("192.0.2.0/24"),
	}
	if len(got.Management) != len(wantMgmt) {
		t.Fatalf("management = %v, want %v", got.Management, wantMgmt)
	}
	for i, want := range wantMgmt {
		if got.Management[i] != want {
			t.Errorf("management[%d] = %s, want %s", i, got.Management[i], want)
		}
	}

	// The normalized copy must not alias the caller's slices.
	cfg.Resolvers[0] = netip.MustParseAddr("9.9.9.9")
	if got.Resolvers[0] != netip.MustParseAddr("8.8.8.8") {
		t.Error("normalize aliased the caller's resolver slice")
	}
}

func TestLastAddr(t *testing.T) {
	tests := []struct {
		prefix string
		want   string
	}{
		{"10.42.0.0/16", "10.42.255.255"},
		{"192.0.2.0/24", "192.0.2.255"},
		{"10.0.0.0/8", "10.255.255.255"},
		{"203.0.113.10/30", "203.0.113.11"},
		{"0.0.0.0/0", "255.255.255.255"},
		{"198.51.100.128/25", "198.51.100.255"},
	}
	for _, tt := range tests {
		t.Run(tt.prefix, func(t *testing.T) {
			got := lastAddr(netip.MustParsePrefix(tt.prefix))
			if want := netip.MustParseAddr(tt.want); got != want {
				t.Fatalf("lastAddr(%s) = %s, want %s", tt.prefix, got, want)
			}
		})
	}
}

func TestPortRangeLen(t *testing.T) {
	tests := []struct {
		r    PortRange
		want int
	}{
		{PortRange{}, 1},
		{PortRange{Min: 30000, Max: 30000}, 1},
		{PortRange{Min: 30000, Max: 30002}, 3},
		{PortRange{Min: 30001, Max: 30000}, 0},
		{PortRange{Min: 1, Max: 65535}, 65535},
	}
	for _, tt := range tests {
		if got := tt.r.Len(); got != tt.want {
			t.Errorf("%+v.Len() = %d, want %d", tt.r, got, tt.want)
		}
	}
}

func TestFreeAddr(t *testing.T) {
	prefix := netip.MustParsePrefix("10.42.0.0/30")
	gateway := netip.MustParseAddr("10.42.0.1")
	used := map[netip.Addr]struct{}{}
	got, ok := freeAddr(prefix, gateway, used)
	if !ok || got != netip.MustParseAddr("10.42.0.2") {
		t.Fatalf("first allocation = %s (%v), want 10.42.0.2", got, ok)
	}
	used[got] = struct{}{}
	if _, ok := freeAddr(prefix, gateway, used); ok {
		t.Fatal("a /30 with only 10.42.0.2 usable must be exhausted")
	}

	// The lowest free address is reused after a release, so a restart gets the
	// same identity back.
	clear(used)
	if got, _ := freeAddr(prefix, gateway, used); got != netip.MustParseAddr("10.42.0.2") {
		t.Fatalf("released address = %s, want 10.42.0.2", got)
	}

	// Sparse usage: skip used addresses but keep the lowest free.
	used = map[netip.Addr]struct{}{
		netip.MustParseAddr("10.42.0.2"): {},
		netip.MustParseAddr("10.42.0.4"): {},
	}
	got, ok = freeAddr(netip.MustParsePrefix("10.42.0.0/29"), gateway, used)
	if !ok || got != netip.MustParseAddr("10.42.0.3") {
		t.Fatalf("sparse allocation = %s (%v), want 10.42.0.3", got, ok)
	}
}

func TestFreePort(t *testing.T) {
	ports := PortRange{Min: 30000, Max: 30002}
	used := map[uint16]struct{}{}
	for _, want := range []uint16{30000, 30001, 30002} {
		got, ok := freePort(ports, used)
		if !ok || got != want {
			t.Fatalf("freePort = %d (%v), want %d", got, ok, want)
		}
		used[got] = struct{}{}
	}
	if _, ok := freePort(ports, used); ok {
		t.Fatal("port pool must be exhausted")
	}
	delete(used, 30001)
	if got, ok := freePort(ports, used); !ok || got != 30001 {
		t.Fatalf("released port = %d (%v), want 30001", got, ok)
	}
}

func TestDerivedIdentity(t *testing.T) {
	cfg := netTestConfig(t)
	seenMAC := make(map[string]string)
	seenTAP := make(map[string]string)
	addresses := []string{"10.42.0.2", "10.42.0.3", "10.42.0.4"}
	for i, id := range []string{"bx_abcdefgh", "bx_98765432", "bx_22222222"} {
		ip := netip.MustParseAddr(addresses[i])
		mac := macForAddr(ip)
		tap := tapFor(id)
		if err := validIfaceName(tap); err != nil {
			t.Fatalf("tapFor(%s) = %q is not a valid interface name: %v", id, tap, err)
		}
		if len(tap) > 15 {
			t.Fatalf("tapFor(%s) = %q is longer than IFNAMSIZ", id, tap)
		}
		if hw, err := net.ParseMAC(mac); err != nil || hw[0]&1 != 0 {
			t.Fatalf("macForAddr(%s) = %q is not a unicast MAC", ip, mac)
		}
		if hostMACForAddr(ip) == mac {
			t.Fatalf("host and guest MAC for %s are the same (%s)", ip, mac)
		}
		a := netTestAllocation(cfg, id, ip, 30000)
		if a.MAC != mac || a.TAPName != tap {
			t.Fatalf("allocation identity differs from the derived values")
		}
		if err := a.Validate(); err != nil {
			t.Fatalf("allocation for %s: %v", id, err)
		}
		if other, dup := seenMAC[mac]; dup {
			t.Fatalf("MAC collision between %s and %s", id, other)
		}
		if other, dup := seenTAP[tap]; dup {
			t.Fatalf("TAP collision between %s and %s", id, other)
		}
		seenMAC[mac] = id
		seenTAP[tap] = id
		if mac != macForAddr(ip) || tap != tapFor(id) {
			t.Fatalf("identity for %s is not deterministic", id)
		}
	}
	// The MAC is a function of the address, so two guests can never share one:
	// a /8 pool still differs in the three low octets.
	if macForAddr(netip.MustParseAddr("10.1.2.3")) == macForAddr(netip.MustParseAddr("10.2.3.4")) {
		t.Fatal("distinct addresses produced the same MAC")
	}
}

func TestBuiltinBlockedCoversManagementRanges(t *testing.T) {
	for _, want := range []string{
		"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10",
		"169.254.0.0/16", "127.0.0.0/8", "224.0.0.0/4",
	} {
		prefix := netip.MustParsePrefix(want)
		if !slicesContainsPrefix(builtinBlocked, prefix) {
			t.Errorf("builtinBlocked does not contain %s", want)
		}
	}
}

func slicesContainsPrefix(prefixes []netip.Prefix, want netip.Prefix) bool {
	for _, p := range prefixes {
		if p == want {
			return true
		}
	}
	return false
}
