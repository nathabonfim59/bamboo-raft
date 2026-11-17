package nodenet

import (
	"encoding/json/v2"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAllocationRequiresDerivedIdentity(t *testing.T) {
	cfg := netTestConfig(t)
	base := netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
	if err := base.Validate(); err != nil {
		t.Fatalf("reference allocation: %v", err)
	}
	tests := []struct {
		name    string
		change  func(*Allocation)
		wantErr string
	}{
		{name: "mac from another address", change: func(a *Allocation) {
			a.MAC = macForAddr(netip.MustParseAddr("10.42.0.9"))
		}, wantErr: "derived"},
		{name: "random mac", change: func(a *Allocation) {
			a.MAC = "52:54:00:01:02:03"
		}, wantErr: "derived"},
		{name: "tap from another sandbox", change: func(a *Allocation) {
			a.TAPName = tapFor("bx_98765432")
		}, wantErr: "derived"},
		{name: "tap with a quote", change: func(a *Allocation) {
			a.TAPName = `bt"abc`
		}, wantErr: "forbidden"},
		{name: "ingress without a port", change: func(a *Allocation) {
			a.SSHPort = 0
		}, wantErr: "set together"},
		{name: "port without ingress", change: func(a *Allocation) {
			a.IngressAddr = netip.Addr{}
		}, wantErr: "set together"},
		{name: "privileged port", change: func(a *Allocation) {
			a.SSHPort = 22
		}, wantErr: "below"},
		{name: "ingress interface without ingress", change: func(a *Allocation) {
			a.IngressAddr = netip.Addr{}
			a.SSHPort = 0
			a.IngressIface = "tailscale0"
		}, wantErr: "without an ingress address"},
		{name: "ingress interface is the bridge", change: func(a *Allocation) {
			a.IngressIface = a.Bridge
		}, wantErr: "is the bridge"},
		{name: "ingress interface with a quote", change: func(a *Allocation) {
			a.IngressIface = `tail"scale`
		}, wantErr: "forbidden"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := base
			tt.change(&a)
			err := a.Validate()
			if err == nil {
				t.Fatalf("Validate accepted an invalid allocation")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate error %q does not contain %q", err, tt.wantErr)
			}
		})
	}

	bound := base
	bound.IngressIface = "tailscale0"
	if err := bound.Validate(); err != nil {
		t.Fatalf("allocation with a bound ingress interface: %v", err)
	}
}

// writeStateFile writes a raw allocation file, bypassing the manager, so the
// loader can be tested against hand-edited state.
func writeStateFile(t *testing.T, dir string, a Allocation) {
	t.Helper()
	data, err := json.Marshal(a)
	if err != nil {
		t.Fatalf("encode state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, stateName(a.SandboxID)), data, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}
}

func TestLoadRejectsDuplicateIdentity(t *testing.T) {
	tests := []struct {
		name    string
		change  func(*Allocation)
		wantErr string
	}{
		{name: "address", change: func(a *Allocation) {
			a.GuestIP = netip.MustParseAddr("10.42.0.2")
			a.MAC = macForAddr(a.GuestIP)
		}, wantErr: "share address"},
		{name: "ssh port", change: func(a *Allocation) { a.SSHPort = 30000 }, wantErr: "share SSH port"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := netTestConfig(t)
			first := netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
			second := netTestAllocation(cfg, "bx_98765432", netip.MustParseAddr("10.42.0.3"), 30001)
			tt.change(&second)
			// The duplicate is only representable when the identity stays
			// derivable, so duplicate address/port with a distinct sandbox id.
			writeStateFile(t, cfg.StateDir, first)
			writeStateFile(t, cfg.StateDir, second)
			runner := netTestNewRunner(t)
			m, err := New(cfg, Deps{Runner: runner, Taps: netTestNewTapper(t, runner)})
			if err == nil {
				m.Close()
				t.Fatal("New accepted two allocations with the same identity")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("New error %q does not contain %q", err, tt.wantErr)
			}
		})
	}

	t.Run("mac and tap cannot collide while derived", func(t *testing.T) {
		cfg := netTestConfig(t)
		a := netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
		b := netTestAllocation(cfg, "bx_98765432", netip.MustParseAddr("10.42.0.3"), 30001)
		if err := checkUnique(map[string]Allocation{a.SandboxID: a, b.SandboxID: b}); err != nil {
			t.Fatalf("distinct identities rejected: %v", err)
		}
		b.MAC = a.MAC
		if err := checkUnique(map[string]Allocation{a.SandboxID: a, b.SandboxID: b}); err == nil {
			t.Fatal("checkUnique accepted a duplicate MAC")
		}
	})
}

func TestLoadEnforcesSSHPortRange(t *testing.T) {
	cfg := netTestConfig(t)
	alloc := netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
	writeStateFile(t, cfg.StateDir, alloc)

	// A shrunken pool must fail before the manager touches the host.
	shrunk := cfg
	shrunk.SSHPorts = PortRange{Min: 30001, Max: 30002}
	runner := netTestNewRunner(t)
	if m, err := New(shrunk, Deps{Runner: runner, Taps: netTestNewTapper(t, runner)}); !errors.Is(err, ErrStateMismatch) {
		if m != nil {
			m.Close()
		}
		t.Fatalf("New with a shrunken port pool = %v, want ErrStateMismatch", err)
	}
	if got := runner.commands(); len(got) != 0 {
		t.Errorf("host was touched before the drift was detected: %v", got)
	}
}

func TestLoadEnforcesIngressIdentity(t *testing.T) {
	cfg := netTestConfig(t)
	alloc := netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
	writeStateFile(t, cfg.StateDir, alloc)

	changed := cfg
	changed.IngressAddr = netip.MustParseAddr("203.0.113.11")
	runner := netTestNewRunner(t)
	m, err := New(changed, Deps{Runner: runner, Taps: netTestNewTapper(t, runner)})
	if !errors.Is(err, ErrStateMismatch) {
		if m != nil {
			m.Close()
		}
		t.Fatalf("New with a changed ingress address = %v, want ErrStateMismatch", err)
	}
	if got := runner.commands(); len(got) != 0 {
		t.Errorf("host was touched before the drift was detected: %v", got)
	}
}

func TestLoadEnforcesIngressInterface(t *testing.T) {
	base := netTestConfig(t)
	base.IngressIface = "tailscale0"
	alloc := netTestAllocation(base, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
	writeStateFile(t, base.StateDir, alloc)

	tests := []struct {
		name   string
		change func(*Config)
	}{
		{name: "different interface", change: func(c *Config) { c.IngressIface = "eth0" }},
		{name: "unbound after binding", change: func(c *Config) { c.IngressIface = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.change(&cfg)
			runner := netTestNewRunner(t)
			m, err := New(cfg, Deps{Runner: runner, Taps: netTestNewTapper(t, runner)})
			if !errors.Is(err, ErrStateMismatch) {
				if m != nil {
					m.Close()
				}
				t.Fatalf("New with %s = %v, want ErrStateMismatch", tt.name, err)
			}
			if got := runner.commands(); len(got) != 0 {
				t.Errorf("host was touched before the drift was detected: %v", got)
			}
		})
	}

	// The exact binding survives the JSON round trip and is inspectable after a
	// restart.
	runner := netTestNewRunner(t)
	m, err := New(base, Deps{Runner: runner, Taps: netTestNewTapper(t, runner)})
	if err != nil {
		t.Fatalf("New with the persisted binding: %v", err)
	}
	defer m.Close()
	loaded, ok := m.Allocation("bx_abcdefgh")
	if !ok || loaded.IngressIface != "tailscale0" {
		t.Fatalf("loaded allocation = %+v (ok %v), want ingress interface tailscale0", loaded, ok)
	}
}

func TestLoadRejectsNonDerivedIdentity(t *testing.T) {
	cfg := netTestConfig(t)
	alloc := netTestAllocation(cfg, "bx_abcdefgh", netip.MustParseAddr("10.42.0.2"), 30000)
	alloc.MAC = "52:54:00:01:02:03"
	writeStateFile(t, cfg.StateDir, alloc)
	runner := netTestNewRunner(t)
	if m, err := New(cfg, Deps{Runner: runner, Taps: netTestNewTapper(t, runner)}); err == nil {
		m.Close()
		t.Fatal("New accepted a hand-edited MAC")
	}
}
