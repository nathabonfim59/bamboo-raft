package node

import (
	"maps"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
)

// jailKVMDirectRendered is a complete rendered ExecStart flag set, as
// kvm-harness-render.yml writes it.
func jailKVMDirectRendered() map[string]string {
	return map[string]string{
		"-server.addr": "127.0.0.1:7701", "-node.id": "kt-a1", "-node.token-file": "/etc/bbraft-kt-a1/node.token",
		"-node.data-dir": "/var/lib/bbraft-kt-a1", "-node.image-dir": "/var/lib/bbraft-kt-a1/images", "-node.default-image": "v1",
		"-node.cpu-model": "Haswell-noTSX", "-node.cpu-nested": "auto", "-node.boot-timeout": "2m",
		"-node.capacity-vcpus": "0", "-node.capacity-memory-mib": "0", "-node.capacity-disk-gib": "0",
		"-node.cpu-overcommit": "1.0", "-node.memory-overcommit": "1.0", "-node.disk-overcommit": "1.0",
		"-node.reserve-vcpus": "1", "-node.reserve-memory-mib": "1024", "-node.reserve-disk-gib": "1",
		"-node.network-mode": "tap", "-node.network-bridge": "bbraft0", "-node.network-prefix": "10.42.0.0/16",
		"-node.network-gateway": "10.42.0.1", "-node.network-resolvers": "1.1.1.1,9.9.9.9",
		"-node.ssh-ingress-addr": "203.0.113.1", "-node.ssh-ingress-iface": "up0", "-node.ssh-port-range": "30000-30010",
		"-node.jail-uid-range": "900000-900999", "-node.jail-cgroup": "/sys/fs/cgroup/system.slice/bbraft-kt-a1.service/vms",
		"-node.jail-qemu-binary": "/usr/bin/qemu-system-x86_64", "-node.jail-tool-binary": "/usr/bin/qemu-img",
		"-node.jail-firmware":     "/usr/share/seabios/bios-256k.bin,/usr/share/qemu/linuxboot_dma.bin",
		"-node.jail-io-write-bps": "104857600", "-node.jail-io-device": "8:0",
	}
}

func TestJailKVMDirectPlan(t *testing.T) {
	facts := jailKVMHostFacts{VCPUs: 8, MemoryMiB: 16384, DiskGiB: 100}
	cfg := jailKVMDirectConfig{Flags: jailKVMDirectRendered(), Socket: "/var/lib/bbraft-kt-a1/direct.sock"}
	got, err := jailKVMDirectPlan(cfg, facts)
	if err != nil {
		t.Fatalf("rendered flags refused: %v", err)
	}
	switch {
	case got.NodeID != "kt-a1" || got.DataDir != "/var/lib/bbraft-kt-a1" || got.ImageDir != "/var/lib/bbraft-kt-a1/images" || got.DefaultImage != "v1":
		t.Fatalf("identity %+v", got)
	case got.CPUModel != "Haswell-noTSX" || got.Nested != qemu.NestedAuto || got.BootTimeout != 2*time.Minute:
		t.Fatalf("cpu %+v", got)
	case got.Capacity.GetVcpus() != 7 || got.Capacity.GetMemoryMib() != 15360 || got.Capacity.GetDiskGib() != 99 || got.HostVCPUs != 8:
		t.Fatalf("capacity %+v (host vcpus %d), want host facts minus reserves", got.Capacity, got.HostVCPUs)
	case got.Socket != "/var/lib/bbraft-kt-a1/direct.sock":
		t.Fatalf("socket %q", got.Socket)
	}
	n := got.Network
	if n.Bridge != "bbraft0" || n.Prefix != netip.MustParsePrefix("10.42.0.0/16") || n.Gateway != netip.MustParseAddr("10.42.0.1") ||
		!slices.Equal(n.Resolvers, []netip.Addr{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("9.9.9.9")}) ||
		n.IngressAddr != netip.MustParseAddr("203.0.113.1") || n.IngressIface != "up0" || n.SSHPorts.Min != 30000 || n.SSHPorts.Max != 30010 ||
		n.StateDir != "/var/lib/bbraft-kt-a1/net" || len(n.Management) != 0 {
		t.Fatalf("network %+v", n)
	}
	if got.UIDs != (jailer.UIDRange{Min: 900000, Max: 900999}) || got.Cgroup != "/sys/fs/cgroup/system.slice/bbraft-kt-a1.service/vms" ||
		got.QEMUBinary != "/usr/bin/qemu-system-x86_64" || got.ToolBinary != "/usr/bin/qemu-img" ||
		!slices.Equal(got.Firmware, []string{"/usr/share/seabios/bios-256k.bin", "/usr/share/qemu/linuxboot_dma.bin"}) || len(got.LibraryDirs) != 0 ||
		got.IO != (jailer.IOLimits{Device: jailer.BlockDevice{Major: 8, Minor: 0}, WriteBPS: 104857600}) {
		t.Fatalf("jail %+v", got)
	}
	with := func(k, v string) jailKVMDirectConfig {
		f := maps.Clone(cfg.Flags)
		if v == "\x00drop" {
			delete(f, k)
		} else {
			f[k] = v
		}
		return jailKVMDirectConfig{Flags: f, Socket: cfg.Socket}
	}
	for name, c := range map[string]jailKVMDirectConfig{
		"unknown flag":     with("-node.cpu-allow-host", ""),
		"unmapped flag":    with("-node.something-new", "1"),
		"no node id":       with("-node.id", "\x00drop"),
		"no default image": with("-node.default-image", "\x00drop"),
		// Without the required check a missing reserve would read as 0 and
		// offer the whole host.
		"no vcpu reserve":         with("-node.reserve-vcpus", "\x00drop"),
		"explicit capacity":       with("-node.capacity-vcpus", "4"),
		"other ratio":             with("-node.memory-overcommit", "1.5"),
		"reserve eats the host":   with("-node.reserve-vcpus", "8"),
		"user networking":         with("-node.network-mode", "user"),
		"bad prefix":              with("-node.network-prefix", "10.42.0.0"),
		"gateway outside":         with("-node.network-gateway", "10.43.0.1"),
		"bad port range":          with("-node.ssh-port-range", "30010-30000"),
		"bad uid range":           with("-node.jail-uid-range", "0-10"),
		"cgroup not vms":          with("-node.jail-cgroup", "/sys/fs/cgroup/system.slice/bbraft-kt-a1.service"),
		"relative qemu":           with("-node.jail-qemu-binary", "qemu-system-x86_64"),
		"relative firmware":       with("-node.jail-firmware", "bios.bin"),
		"empty firmware entry":    with("-node.jail-firmware", "/usr/share/a.bin,"),
		"no io device":            with("-node.jail-io-device", "\x00drop"),
		"no io rate":              with("-node.jail-io-write-bps", "\x00drop"),
		"bad io rate":             with("-node.jail-io-write-bps", "fast"),
		"bad boot timeout":        with("-node.boot-timeout", "soon"),
		"bad nested":              with("-node.cpu-nested", "sometimes"),
		"socket outside data dir": {Flags: cfg.Flags, Socket: "/tmp/direct.sock"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := jailKVMDirectPlan(c, facts); err == nil {
				t.Fatal("accepted")
			}
		})
	}
	if _, err := jailKVMDirectPlan(cfg, jailKVMHostFacts{VCPUs: 8, MemoryMiB: 1024, DiskGiB: 100}); err == nil {
		t.Error("a memory reserve that leaves nothing was accepted")
	}
}

func TestJailKVMReadDirectConfig(t *testing.T) {
	uid := uint32(os.Geteuid())
	good := `{"flags":{"-node.id":"kt-a1"},"socket":"/var/lib/bbraft-kt-a1/direct.sock"}`
	write := func(t *testing.T, body string, mode os.FileMode) string {
		p := filepath.Join(t.TempDir(), "direct.json")
		if err := os.WriteFile(p, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
		return p
	}
	got, err := jailKVMReadDirectConfig(write(t, good, 0o600), uid)
	if err != nil || got.Flags["-node.id"] != "kt-a1" || got.Socket != "/var/lib/bbraft-kt-a1/direct.sock" {
		t.Fatalf("read = %+v, %v", got, err)
	}
	for name, path := range map[string]func(t *testing.T) string{
		"group readable": func(t *testing.T) string { return write(t, good, 0o640) },
		"symlink": func(t *testing.T) string {
			p := filepath.Join(t.TempDir(), "link.json")
			if err := os.Symlink(write(t, good, 0o600), p); err != nil {
				t.Fatal(err)
			}
			return p
		},
		"fifo": func(t *testing.T) string {
			p := filepath.Join(t.TempDir(), "fifo.json")
			if err := syscall.Mkfifo(p, 0o600); err != nil {
				t.Fatal(err)
			}
			return p
		},
		"hard linked": func(t *testing.T) string {
			p := write(t, good, 0o600)
			if err := os.Link(p, p+".2"); err != nil {
				t.Fatal(err)
			}
			return p
		},
		"unknown member": func(t *testing.T) string { return write(t, `{"flags":{},"socket":"/x","extra":1}`, 0o600) },
		"trailing data":  func(t *testing.T) string { return write(t, good+`{}`, 0o600) },
		"oversized": func(t *testing.T) string {
			return write(t, `{"flags":{"a":"`+strings.Repeat("x", 70<<10)+`"},"socket":"/x"}`, 0o600)
		},
		"missing": func(t *testing.T) string { return filepath.Join(t.TempDir(), "absent.json") },
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := jailKVMReadDirectConfig(path(t), uid); err == nil {
				t.Fatal("accepted")
			}
		})
	}
	if _, err := jailKVMReadDirectConfig(write(t, good, 0o600), uid+1); err == nil {
		t.Error("a config owned by another uid was accepted")
	}
}
