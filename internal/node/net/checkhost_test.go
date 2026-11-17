package nodenet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// netTestHostPoints the device paths at files the test can create, and restores
// them afterwards.
func netTestHostPoints(t *testing.T) (tun, vhost string) {
	t.Helper()
	dir := t.TempDir()
	tun = filepath.Join(dir, "tun")
	vhost = filepath.Join(dir, "vhost-net")
	for _, path := range []string{tun, vhost} {
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("create device fixture: %v", err)
		}
	}
	oldTun, oldVhost := tunDevice, vhostNetDevice
	tunDevice, vhostNetDevice = tun, vhost
	t.Cleanup(func() {
		tunDevice, vhostNetDevice = oldTun, oldVhost
	})
	return tun, vhost
}

func TestCheckHost(t *testing.T) {
	netTestHostPoints(t)
	parentTun, parentVhost := tunDevice, vhostNetDevice
	addrJSON := fmt.Appendf(nil,
		`[{"ifname":"eth0","addr_info":[{"local":"203.0.113.10"}]},{"ifname":"bbraft0","addr_info":[{"local":"10.42.0.1"}]}]`)

	tests := []struct {
		name    string
		prepare func(*netTestRunner, *Manager, Config)
		change  func(*Config)
		wantErr string
	}{
		{name: "ready"},
		{
			name: "forwarding disabled",
			prepare: func(_ *netTestRunner, m *Manager, _ Config) {
				m.readSysctl = func(context.Context, string) ([]byte, error) { return []byte("0\n"), nil }
			},
			wantErr: "ip_forward",
		},
		{
			name: "sysctl unreadable",
			prepare: func(_ *netTestRunner, m *Manager, _ Config) {
				m.readSysctl = func(context.Context, string) ([]byte, error) { return nil, os.ErrPermission }
			},
			wantErr: "ip_forward",
		},
		{
			name: "ingress not local",
			prepare: func(runner *netTestRunner, _ *Manager, _ Config) {
				runner.out["ip -j -4 addr show"] = netTestOutput{data: fmt.Appendf(nil,
					`[{"ifname":"eth0","addr_info":[{"local":"203.0.113.11"}]}]`)}
			},
			wantErr: "not assigned to a host interface",
		},
		{
			name: "ingress only on the bridge is not accepted",
			prepare: func(runner *netTestRunner, _ *Manager, _ Config) {
				runner.out["ip -j -4 addr show"] = netTestOutput{data: fmt.Appendf(nil,
					`[{"ifname":"bbraft0","addr_info":[{"local":"203.0.113.10"}]}]`)}
			},
			wantErr: "not assigned to a host interface",
		},
		{
			name: "bridge name taken by another kind",
			prepare: func(runner *netTestRunner, _ *Manager, _ Config) {
				runner.setLink("bbraft0", "veth")
			},
			wantErr: "refusing to use it as the VM bridge",
		},
		{
			name: "nft missing",
			prepare: func(runner *netTestRunner, _ *Manager, _ Config) {
				runner.out["nft --version"] = netTestOutput{err: &CommandError{
					Name: "nft", Args: []string{"--version"}, Stderr: "not found", ExitCode: 127,
					Err: os.ErrNotExist,
				}}
			},
			wantErr: "nft is required",
		},
		{
			name: "ip missing",
			prepare: func(runner *netTestRunner, _ *Manager, _ Config) {
				runner.out["ip -Version"] = netTestOutput{err: os.ErrNotExist}
			},
			wantErr: "ip is required",
		},
		{
			name: "tap device missing",
			prepare: func(_ *netTestRunner, _ *Manager, _ Config) {
				tunDevice = filepath.Join(t.TempDir(), "absent")
			},
			wantErr: "open",
		},
		{
			name: "vhost device missing",
			prepare: func(_ *netTestRunner, _ *Manager, _ Config) {
				vhostNetDevice = filepath.Join(t.TempDir(), "absent")
			},
			wantErr: "absent",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tunDevice, vhostNetDevice = parentTun, parentVhost
			t.Cleanup(func() { tunDevice, vhostNetDevice = parentTun, parentVhost })
			cfg := netTestConfig(t)
			if tt.change != nil {
				tt.change(&cfg)
			}
			runner := netTestNewRunner(t)
			runner.out["nft --version"] = netTestOutput{data: []byte("nftables v1.1.7\n")}
			runner.out["ip -Version"] = netTestOutput{data: []byte("ip utility, iproute2-6.12.0\n")}
			runner.out["ip -j -4 addr show"] = netTestOutput{data: addrJSON}
			m, err := New(cfg, Deps{Runner: runner, Taps: netTestNewTapper(t, runner)})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer m.Close()
			m.readSysctl = func(context.Context, string) ([]byte, error) { return []byte("1\n"), nil }
			if tt.prepare != nil {
				tt.prepare(runner, m, cfg)
			}

			err = m.CheckHost(t.Context())
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("CheckHost: %v", err)
				}
				if got := runner.commands(); len(got) != 0 {
					t.Errorf("CheckHost changed host state: %v", got)
				}
				return
			}
			if err == nil {
				t.Fatalf("CheckHost: expected an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("CheckHost error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}

// TestDevicesAreAlwaysTunAndVhost proves production always requires both
// devices: vhost-net has no opt-out, only a Deps.Taps test seam.
func TestDevicesAreAlwaysTunAndVhost(t *testing.T) {
	cfg := netTestConfig(t)
	runner := netTestNewRunner(t)
	m, err := New(cfg, Deps{Runner: runner, Taps: netTestNewTapper(t, runner)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer m.Close()
	if got := m.devices(); !slices.Equal(got, []string{tunDevice, vhostNetDevice}) {
		t.Errorf("devices = %v, want tun and vhost-net", got)
	}
}
