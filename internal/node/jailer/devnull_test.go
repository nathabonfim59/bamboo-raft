package jailer

import (
	"encoding/json/v2"
	"os"
	"os/exec"
	"slices"
	"testing"
)

// Root-gate round: QEMU probes OFD locks on /dev/null, so the jail gets a
// private, package-fixed /dev/null node; and the virtio-net option ROM is
// disabled with romfile= instead of staging distribution firmware.

// TestHelperConfigCarriesNullDevice: the helper is told the host /dev/null
// rdev so it can mknod the private node next to kvm and urandom.
func TestHelperConfigCarriesNullDevice(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	spec := jailerSpec(t, jailerLaunchID)
	var cfg map[string]any
	inner := m.hooks.command
	m.hooks.command = func(cmd *exec.Cmd) {
		inner(cmd)
		raw, err := os.ReadFile(fdPath(int(cmd.ExtraFiles[len(spec.Files)].Fd())))
		if err == nil {
			err = json.Unmarshal(raw, &cfg)
		}
		if err != nil {
			t.Error(err)
		}
	}
	child, owner, err := jailerStart(t, m, t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	want, err := fixedDevice("/dev/null")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := cfg["nullRdev"].(float64); !ok || uint64(got) != want {
		t.Fatalf("helper config nullRdev = %v, want %d", cfg["nullRdev"], want)
	}
	_ = child.Kill()
	owner.waitKilled(t)
	if err := child.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// TestNetDeviceDisablesOptionROM pins the direct-kernel-boot argv form: the
// virtio-net device carries romfile= so QEMU loads no option ROM, and the
// allowlist accepts it.
func TestNetDeviceDisablesOptionROM(t *testing.T) {
	spec := jailerSpec(t, jailerArgvID)
	const want = "virtio-net-pci,netdev=net0,mac=02:00:00:00:00:01,romfile="
	i := slices.Index(spec.Args, want)
	if i < 1 || spec.Args[i-1] != "-device" {
		t.Fatalf("argv lacks -device %s", want)
	}
	qcow, err := validateDiskChain(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateArgs(spec, qcow); err != nil {
		t.Fatalf("romfile= form rejected: %v", err)
	}
}

// TestPrivateDeviceAllowlist: exactly kvm, urandom and null may be device
// nodes in a jail, each tied to its host node's rdev.
func TestPrivateDeviceAllowlist(t *testing.T) {
	for _, tt := range []struct {
		path, host string
	}{
		{"root/dev/kvm", "/dev/kvm"},
		{"root/dev/urandom", "/dev/urandom"},
		{"root/dev/null", "/dev/null"},
		{"root/dev/zero", ""},
		{"root/dev/mem", ""},
		{"root/null", ""},
		{"dev/null", ""},
	} {
		host, ok := privateDeviceHost(tt.path)
		if host != tt.host || ok != (tt.host != "") || isPrivateDevice(tt.path) != ok {
			t.Errorf("privateDeviceHost(%q) = %q, %v; want %q", tt.path, host, ok, tt.host)
		}
	}
}
