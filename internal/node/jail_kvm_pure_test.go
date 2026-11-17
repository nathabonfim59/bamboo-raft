package node

// Pure, unprivileged tests of the A10 final jailed harness helpers
// (jail_kvm_helpers_test.go). They run in every local `go test`; the gated
// acceptance flows in jail_kvm_test.go run only in the lead's throwaway VM.

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/iotest"
	"time"
)

// jailKVMSampleUnit is a complete unit rendered by the role's own
// kvm-harness-render.yml (roles/bbraft_node templates/bbraft-node.service.j2)
// with the attempt id replaced by "a1"; TestJailKVMRenderedUnits checks live
// render output against the same helpers.
const jailKVMSampleUnit = `# Managed by bamboo-raft infra/ansible (role bbraft_node).
#
# ExecStart contains only the exact typed flags in cmd/bbraft-node/config.go.
# The network mode is the literal quoted "tap": the role never renders the
# node's dev/test user-mode fallback, and validate.yml refuses any override.
# Every other argv element is quoted by bbraft_node_systemd_exec_quote, which
# escapes spaces, %, $, " and \ and rejects line breaks. ReadWritePaths uses
# bbraft_node_systemd_unit_quote, which keeps a literal $ and escapes %.
# The token is a file path here; no secret value appears in this unit.
#
# bbraft-node is the root jail supervisor (A10): it runs as root with only the
# capabilities below, and every QEMU and qemu-img attempt it starts runs jailed
# on its own uid from -node.jail-uid-range with zero groups and capabilities,
# NoNewPrivileges, namespaces, a chroot and cgroup limits under the delegated
# <service cgroup>/vms. The node process itself lives in <service>/supervisor.
[Unit]
Description=bbraft node agent
After=network-online.target
Wants=network-online.target
RequiresMountsFor="/var/lib/bbraft-kt-a1" "/var/lib/bbraft-kt-a1/images"

[Service]
Type=simple
UMask=0077
ExecStart="/var/lib/bbraft-kt-stage-a1/bin/bbraft-node" \
    -server.addr "127.0.0.1:7701" \
    -node.id "kt-a1" \
    -node.token-file "/etc/bbraft-kt-a1/node.token" \
    -node.data-dir "/var/lib/bbraft-kt-a1" \
    -node.image-dir "/var/lib/bbraft-kt-a1/images" \
    -node.default-image "image-fixture" \
    -node.cpu-model "Haswell-noTSX" \
    -node.cpu-nested "auto" \
    -node.boot-timeout "2m" \
    -node.capacity-vcpus "16" \
    -node.capacity-memory-mib "32768" \
    -node.capacity-disk-gib "200" \
    -node.cpu-overcommit "1.0" \
    -node.memory-overcommit "1.0" \
    -node.disk-overcommit "1.0" \
    -node.reserve-vcpus "1" \
    -node.reserve-memory-mib "1024" \
    -node.reserve-disk-gib "1" \
    -node.network-mode "tap" \
    -node.network-bridge "bbraft0" \
    -node.network-prefix "10.42.0.0/16" \
    -node.network-gateway "10.42.0.1" \
    -node.network-resolvers "1.1.1.1,9.9.9.9" \
    -node.ssh-ingress-addr "203.0.113.1" \
    -node.ssh-ingress-iface "up0" \
    -node.ssh-port-range "30000-30010" \
    -node.jail-uid-range "900000-900999" \
    -node.jail-cgroup "/sys/fs/cgroup/system.slice/bbraft-kt-a1.service/vms" \
    -node.jail-qemu-binary "/usr/bin/qemu-system-x86_64" \
    -node.jail-tool-binary "/usr/bin/qemu-img" \
    -node.jail-firmware "/usr/share/seabios/bios-256k.bin,/usr/share/qemu/linuxboot_dma.bin,/usr/share/qemu/kvmvapic.bin" \
    -node.jail-io-write-bps "104857600" \
    -node.jail-io-device "8:0"

Restart=on-failure
RestartSec=5
# QEMU VMs are children of this process. KillMode=process signals only the
# node, whose own bounded shutdown (20 s) stops workers and releases the lock;
# the QEMU children survive and the next node process adopts them.
KillMode=process
TimeoutStopSec=30
LimitNOFILE=65536
TasksMax=infinity
LimitCORE=0

# Each capability is tied to jail code (docs/node.md): SYS_ADMIN clone3 and
# namespaces and detached mounts; SYS_CHROOT the jail chroot; MKNOD the
# private /dev nodes; SETUID/SETGID/SETPCAP the per-attempt credential and
# bounding-set drop; CHOWN/FOWNER/DAC_OVERRIDE per-attempt ownership of
# disks, the control dir and the QMP socket; KILL and SYS_PTRACE signalling
# and proving a different-uid QEMU; NET_ADMIN bridge, TAP and nftables.
# QEMU inherits none of them (the jail drops all five sets).
CapabilityBoundingSet=CAP_SYS_ADMIN CAP_SYS_CHROOT CAP_MKNOD CAP_SETUID CAP_SETGID CAP_SETPCAP CAP_CHOWN CAP_FOWNER CAP_DAC_OVERRIDE CAP_KILL CAP_SYS_PTRACE CAP_NET_ADMIN
AmbientCapabilities=
NoNewPrivileges=true
SystemCallArchitectures=native
# The node proves a systemd-delegated cgroup at startup: it lives in
# <service>/supervisor and owns <service>/vms (systemd >= 254).
Delegate=yes
DelegateSubgroup=supervisor
# The device policy sits on the service cgroup, so it also bounds every jail.
DevicePolicy=closed
DeviceAllow=/dev/kvm rwm
DeviceAllow=/dev/vhost-vsock rw
DeviceAllow=/dev/vhost-net rw
DeviceAllow=/dev/net/tun rw
DeviceAllow=/dev/null rwm
DeviceAllow=/dev/urandom rwm
# The supervisor reads the host's local vsock CID once at startup
# (VM_SOCKETS_GET_LOCAL_CID on a read-only fd) to keep it out of guest CIDs.
DeviceAllow=/dev/vsock r
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
# /dev/kvm and /dev/vhost-vsock must stay visible: do not use PrivateDevices.
PrivateDevices=false
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectKernelLogs=true
# The delegated service subtree is the only writable cgroup path.
ProtectControlGroups=false
ReadOnlyPaths=/sys/fs/cgroup
ProtectClock=true
RestrictSUIDSGID=true
LockPersonality=true
# QEMU needs vsock (guest agent), AF_UNIX (QMP), and store-and-forward
# user-mode networking. No request is served over host TCP.
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK AF_VSOCK
# Deliberately unset: systemd's namespace restriction (its seccomp filter
# answers clone3 with ENOSYS, which breaks CLONE_INTO_CGROUP, so startup
# admission would fail) and a system-call filter (QEMU would inherit it on
# top of its own -sandbox).
ReadWritePaths="/var/lib/bbraft-kt-a1" /sys/fs/cgroup/system.slice/bbraft-kt-a1.service

[Install]
WantedBy=multi-user.target
`

// jailKVMSampleWant is the sample attempt's expected paths.
var jailKVMSampleWant = jailKVMBoundaryWant{NetnsPath: "/run/netns/bbj1n", DataDir: "/var/lib/bbraft-kt-a1", Service: "bbraft-kt-a1"}

// jailKVMSetFlag and jailKVMDropFlag edit one flag of an ExecStart argv.
func jailKVMSetFlag(argv []string, name, value string) []string {
	out := slices.Clone(argv)
	i := slices.Index(out, name)
	out[i+1] = value
	return out
}

func jailKVMDropFlag(argv []string, name string) []string {
	i := slices.Index(argv, name)
	return slices.Delete(slices.Clone(argv), i, i+2)
}

func TestJailKVMParseUnit(t *testing.T) {
	u, err := jailKVMParseUnit(jailKVMSampleUnit)
	if err != nil {
		t.Fatal(err)
	}
	if u.ExecStart[0] != "/var/lib/bbraft-kt-stage-a1/bin/bbraft-node" || len(u.ExecStart)%2 != 1 {
		t.Fatalf("ExecStart = %q", u.ExecStart)
	}
	for _, pair := range [][2]string{{"-node.data-dir", "/var/lib/bbraft-kt-a1"}, {"-node.network-mode", "tap"},
		{"-node.jail-cgroup", "/sys/fs/cgroup/system.slice/bbraft-kt-a1.service/vms"}, {"-node.jail-io-device", "8:0"}} {
		if i := slices.Index(u.ExecStart, pair[0]); i < 0 || u.ExecStart[i+1] != pair[1] {
			t.Fatalf("ExecStart %s = %q, want %q", pair[0], u.ExecStart, pair[1])
		}
	}
	if got := u.Service["DeviceAllow"]; len(got) != 7 || got[0] != "/dev/kvm rwm" || got[5] != "/dev/urandom rwm" || got[6] != "/dev/vsock r" {
		t.Fatalf("DeviceAllow = %q", got)
	}
	if got := u.Service["AmbientCapabilities"]; !slices.Equal(got, []string{""}) {
		t.Fatalf("AmbientCapabilities = %q, want one empty value", got)
	}
	if _, ok := u.Service["WantedBy"]; ok {
		t.Fatal("an [Install] directive leaked into [Service]")
	}
	for name, text := range map[string]string{
		"no service section":    "[Unit]\nDescription=x\n",
		"no ExecStart":          "[Service]\nType=simple\n",
		"dangling continuation": "[Service]\nExecStart=/bin/x \\\n",
		"duplicate scalar":      "[Service]\nExecStart=/bin/x\nKillMode=process\nKillMode=mixed\n",
		"two ExecStart":         "[Service]\nExecStart=/bin/x\nExecStart=/bin/y\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := jailKVMParseUnit(text); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestJailKVMCheckExec(t *testing.T) {
	u, err := jailKVMParseUnit(jailKVMSampleUnit)
	if err != nil {
		t.Fatal(err)
	}
	want := jailKVMExpect{Binary: "/var/lib/bbraft-kt-stage-a1/bin/bbraft-node", Flags: map[string]string{
		"-server.addr": "127.0.0.1:7701", "-node.data-dir": "/var/lib/bbraft-kt-a1",
		"-node.jail-cgroup": "/sys/fs/cgroup/system.slice/bbraft-kt-a1.service/vms",
	}}
	if err := jailKVMCheckExec(u.ExecStart, want); err != nil {
		t.Fatalf("matching ExecStart refused: %v", err)
	}
	for name, argv := range map[string][]string{
		"other binary":   append([]string{"/usr/local/bin/bbraft-node"}, u.ExecStart[1:]...),
		"other data dir": jailKVMSetFlag(u.ExecStart, "-node.data-dir", "/var/lib/bbraft-node-v1"),
		"user mode":      jailKVMSetFlag(u.ExecStart, "-node.network-mode", "user"),
		"no tap flag":    jailKVMDropFlag(u.ExecStart, "-node.network-mode"),
		"allow host":     append(slices.Clone(u.ExecStart), "-node.cpu-allow-host"),
		"missing flag":   jailKVMDropFlag(u.ExecStart, "-node.jail-cgroup"),
		"flag twice":     append(slices.Clone(u.ExecStart), "-node.data-dir", "/var/lib/bbraft-kt-a1"),
	} {
		t.Run(name, func(t *testing.T) {
			if err := jailKVMCheckExec(argv, want); err == nil {
				t.Fatalf("accepted %q", argv)
			}
		})
	}
}

func TestJailKVMDropIn(t *testing.T) {
	got, err := jailKVMDropIn("/run/netns/bbj1n", nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := "[Service]\nNetworkNamespacePath=/run/netns/bbj1n\n"; got != want {
		t.Fatalf("link drop-in = %q, want %q", got, want)
	}
	got, err = jailKVMDropIn("/run/netns/bbj1n", &jailKVMDirect{
		Exec: []string{"/var/lib/bbraft-kt-stage-a1/bin/node.test", "-test.run", "^TestKVMJailDirectHelper$"},
		Env:  map[string]string{"BBRAFT_TEST_KVM_JAIL_HELPER": `{"socket":"/var/lib/bbraft-kt-a1/direct.sock"}`},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"NetworkNamespacePath=/run/netns/bbj1n\n",
		"ExecStart=\n",
		`ExecStart="/var/lib/bbraft-kt-stage-a1/bin/node.test" "-test.run" "^TestKVMJailDirectHelper$$"` + "\n",
		`Environment="BBRAFT_TEST_KVM_JAIL_HELPER={\"socket\":\"/var/lib/bbraft-kt-a1/direct.sock\"}"` + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("direct drop-in %q lacks %q", got, want)
		}
	}
	if strings.Index(got, "ExecStart=\n") > strings.Index(got, `ExecStart="`) {
		t.Fatal("the ExecStart reset must precede the helper ExecStart")
	}
	for name, tc := range map[string]struct {
		ns     string
		direct *jailKVMDirect
	}{
		"relative netns":    {"run/netns/x", nil},
		"netns newline":     {"/run/netns/x\nUser=nobody", nil},
		"relative exec":     {"/run/netns/x", &jailKVMDirect{Exec: []string{"node.test"}}},
		"exec newline":      {"/run/netns/x", &jailKVMDirect{Exec: []string{"/bin/x", "a\nUser=nobody"}}},
		"env key with =":    {"/run/netns/x", &jailKVMDirect{Exec: []string{"/bin/x"}, Env: map[string]string{"A=B": "c"}}},
		"env value newline": {"/run/netns/x", &jailKVMDirect{Exec: []string{"/bin/x"}, Env: map[string]string{"A": "b\nc"}}},
		"empty direct exec": {"/run/netns/x", &jailKVMDirect{}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := jailKVMDropIn(tc.ns, tc.direct); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestJailKVMImmutableAndShow(t *testing.T) {
	u, err := jailKVMParseUnit(jailKVMSampleUnit)
	if err != nil {
		t.Fatal(err)
	}
	want, err := jailKVMImmutable(u, jailKVMSampleWant)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{
		"CapabilityBoundingSet":   "cap_chown cap_dac_override cap_fowner cap_kill cap_mknod cap_net_admin cap_setgid cap_setpcap cap_setuid cap_sys_admin cap_sys_chroot cap_sys_ptrace",
		"AmbientCapabilities":     "",
		"NoNewPrivileges":         "yes",
		"Delegate":                "yes",
		"DelegateSubgroup":        "supervisor",
		"DevicePolicy":            "closed",
		"KillMode":                "process",
		"ProtectSystem":           "strict",
		"ProtectHome":             "yes",
		"PrivateTmp":              "yes",
		"ProtectControlGroups":    "no",
		"User":                    "",
		"RestrictNamespaces":      "no",
		"SystemCallFilter":        "",
		"NetworkNamespacePath":    "/run/netns/bbj1n",
		"ProtectKernelTunables":   "yes",
		"ProtectKernelModules":    "yes",
		"ProtectKernelLogs":       "yes",
		"ProtectClock":            "yes",
		"RestrictSUIDSGID":        "yes",
		"LockPersonality":         "yes",
		"RestrictAddressFamilies": "AF_INET AF_INET6 AF_NETLINK AF_UNIX AF_VSOCK",
		"ReadOnlyPaths":           "/sys/fs/cgroup",
		"ReadWritePaths":          "/var/lib/bbraft-kt-a1 /sys/fs/cgroup/system.slice/bbraft-kt-a1.service",
		"UMask":                   "0077",
		"LimitCORE":               "0",
		"LimitNOFILE":             "65536",
		"TasksMax":                "infinity",
		"TimeoutStopUSec":         "30s",
	} {
		if got, ok := want[key]; !ok || got != value {
			t.Errorf("immutable %s = %q (present %v), want %q", key, got, ok, value)
		}
	}
	show := func(edit map[string]string) string {
		var b strings.Builder
		for _, k := range slices.Sorted(maps.Keys(want)) {
			v := want[k]
			if e, ok := edit[k]; ok {
				v = e
			}
			if v == "\x00drop" {
				continue
			}
			// A list property is printed as one KEY=VALUE line per entry
			// (to be confirmed against the VM's systemd in the gate).
			for line := range strings.SplitSeq(v, "\n") {
				b.WriteString(k + "=" + line + "\n")
			}
		}
		return b.String()
	}
	if err := jailKVMCheckShow(want, show(nil)); err != nil {
		t.Fatalf("exact show refused: %v", err)
	}
	// DeviceAllow is a set: systemd's presentation order is not promised.
	// The live set is the seven explicit lines plus the read-only char-rtc
	// entry ProtectClock=yes implies (systemd.exec(5)), and nothing else.
	live := jailKVMBoundaryLiveDevices()
	if !slices.Contains(live, "char-rtc r") || !slices.Contains(live, "/dev/vsock r") || len(live) != 8 || len(jailKVMBoundaryDevices) != 7 {
		t.Fatalf("live DeviceAllow %q: want the explicit seven (with /dev/vsock r) plus char-rtc r", live)
	}
	reversed := slices.Clone(live)
	slices.Reverse(reversed)
	if err := jailKVMCheckShow(want, show(map[string]string{"DeviceAllow": strings.Join(reversed, "\n"),
		"ReadWritePaths": "/sys/fs/cgroup/system.slice/bbraft-kt-a1.service /var/lib/bbraft-kt-a1"})); err != nil {
		t.Fatalf("reordered set properties refused: %v", err)
	}
	for name, edit := range map[string]map[string]string{
		"extra capability":        {"CapabilityBoundingSet": want["CapabilityBoundingSet"] + " cap_sys_module"},
		"ambient set":             {"AmbientCapabilities": "cap_net_admin"},
		"no delegation":           {"Delegate": "no"},
		"no subgroup":             {"DelegateSubgroup": ""},
		"open devices":            {"DevicePolicy": "auto"},
		"other netns":             {"NetworkNamespacePath": "/run/netns/other"},
		"user set":                {"User": "bbraft-node"},
		"namespaces locked":       {"RestrictNamespaces": "yes"},
		"missing key":             {"KillMode": "\x00drop"},
		"device missing":          {"DeviceAllow": strings.Join(slices.DeleteFunc(slices.Clone(live), func(d string) bool { return d == "/dev/urandom rwm" }), "\n")},
		"device extra":            {"DeviceAllow": strings.Join(append(slices.Clone(live), "/dev/sda rw"), "\n")},
		"device duplicate":        {"DeviceAllow": strings.Join(append(slices.Clone(live), "/dev/kvm rwm"), "\n")},
		"rtc implication missing": {"DeviceAllow": strings.Join(jailKVMBoundaryDevices, "\n")},
		"rtc writable":            {"DeviceAllow": strings.Replace(want["DeviceAllow"], "char-rtc r", "char-rtc rw", 1)},
		"rtc mknod":               {"DeviceAllow": strings.Replace(want["DeviceAllow"], "char-rtc r", "char-rtc rwm", 1)},
		"rtc duplicate":           {"DeviceAllow": strings.Join(append(slices.Clone(live), "char-rtc r"), "\n")},
		"rtc as block":            {"DeviceAllow": strings.Replace(want["DeviceAllow"], "char-rtc r", "block-rtc r", 1)},
		"rtc node path":           {"DeviceAllow": strings.Replace(want["DeviceAllow"], "char-rtc r", "/dev/rtc0 r", 1)},
		"no local vsock":          {"DeviceAllow": strings.Replace(want["DeviceAllow"], "/dev/vsock r\n", "", 1)},
		"writable local vsock":    {"DeviceAllow": strings.Replace(want["DeviceAllow"], "/dev/vsock r", "/dev/vsock rw", 1)},
		"device wider mode":       {"DeviceAllow": strings.Replace(want["DeviceAllow"], "/dev/vhost-net rw", "/dev/vhost-net rwm", 1)},
		"extra family":            {"RestrictAddressFamilies": want["RestrictAddressFamilies"] + " AF_PACKET"},
		"extra rw path":           {"ReadWritePaths": want["ReadWritePaths"] + " /"},
		"core dumps":              {"LimitCORE": "infinity"},
		"other umask":             {"UMask": "0022"},
		"short stop":              {"TimeoutStopUSec": "5s"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := jailKVMCheckShow(want, show(edit)); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// TestJailKVMImmutableRefusesWeakenedBoundary: a supplied unit is
// validated against the fixed supervisor boundary, never mirrored: a weaker
// or wider unit is refused before anything is installed.
func TestJailKVMImmutableRefusesWeakenedBoundary(t *testing.T) {
	const caps = "CapabilityBoundingSet=CAP_SYS_ADMIN CAP_SYS_CHROOT CAP_MKNOD CAP_SETUID CAP_SETGID CAP_SETPCAP CAP_CHOWN CAP_FOWNER CAP_DAC_OVERRIDE CAP_KILL CAP_SYS_PTRACE CAP_NET_ADMIN"
	// edit replaces text that starts a line of the sample (comments may
	// mention directive names too).
	edit := func(old, new string) string {
		if strings.Count(jailKVMSampleUnit, "\n"+old) != 1 {
			t.Fatalf("sample unit lacks a line starting %q", old)
		}
		return strings.Replace(jailKVMSampleUnit, "\n"+old, "\n"+new, 1)
	}
	for name, text := range map[string]string{
		"extra capability":       edit(caps, caps+" CAP_SYS_MODULE"),
		"missing capability":     edit(caps, strings.Replace(caps, " CAP_SYS_PTRACE", "", 1)),
		"all capabilities":       edit(caps, "CapabilityBoundingSet=~"),
		"no bounding set":        edit(caps+"\n", ""),
		"ambient capability":     edit("AmbientCapabilities=\n", "AmbientCapabilities=CAP_NET_ADMIN\n"),
		"no ambient line":        edit("AmbientCapabilities=\n", ""),
		"NoNewPrivileges off":    edit("NoNewPrivileges=true", "NoNewPrivileges=false"),
		"no NoNewPrivileges":     edit("NoNewPrivileges=true\n", ""),
		"KillMode mixed":         edit("KillMode=process", "KillMode=mixed"),
		"KillMode control-group": edit("KillMode=process", "KillMode=control-group"),
		"no delegation":          edit("Delegate=yes\n", ""),
		"other subgroup":         edit("DelegateSubgroup=supervisor", "DelegateSubgroup=node"),
		"device policy auto":     edit("DevicePolicy=closed", "DevicePolicy=auto"),
		"extra device":           edit("DeviceAllow=/dev/urandom rwm\n", "DeviceAllow=/dev/urandom rwm\nDeviceAllow=/dev/sda rw\n"),
		"wider device mode":      edit("DeviceAllow=/dev/vhost-net rw", "DeviceAllow=/dev/vhost-net rwm"),
		"no local vsock":         edit("DeviceAllow=/dev/vsock r\n", ""),
		"writable local vsock":   edit("DeviceAllow=/dev/vsock r", "DeviceAllow=/dev/vsock rw"),
		"local vsock mknod":      edit("DeviceAllow=/dev/vsock r", "DeviceAllow=/dev/vsock rwm"),
		// char-rtc r is systemd's ProtectClock implication, never a line the
		// unit states itself.
		"explicit rtc":         edit("DeviceAllow=/dev/urandom rwm\n", "DeviceAllow=/dev/urandom rwm\nDeviceAllow=char-rtc r\n"),
		"protect system full":  edit("ProtectSystem=strict", "ProtectSystem=full"),
		"no ProtectHome":       edit("ProtectHome=true", "ProtectHome=false"),
		"no PrivateTmp":        edit("PrivateTmp=true", "PrivateTmp=false"),
		"service user":         edit("UMask=0077", "UMask=0077\nUser=bbraft-node"),
		"namespace filter":     edit("UMask=0077", "UMask=0077\nRestrictNamespaces=yes"),
		"syscall filter":       edit("UMask=0077", "UMask=0077\nSystemCallFilter=@system-service"),
		"own netns path":       edit("UMask=0077", "UMask=0077\nNetworkNamespacePath=/run/netns/other"),
		"private devices":      edit("PrivateDevices=false", "PrivateDevices=true"),
		"other umask":          edit("UMask=0077", "UMask=0022"),
		"fewer files":          edit("LimitNOFILE=65536", "LimitNOFILE=1024"),
		"core dumps":           edit("LimitCORE=0", "LimitCORE=infinity"),
		"bounded tasks":        edit("TasksMax=infinity", "TasksMax=4096"),
		"short stop timeout":   edit("TimeoutStopSec=30", "TimeoutStopSec=5"),
		"kernel tunables":      edit("ProtectKernelTunables=true", "ProtectKernelTunables=false"),
		"kernel modules":       edit("ProtectKernelModules=true", "ProtectKernelModules=false"),
		"kernel logs":          edit("ProtectKernelLogs=true", "ProtectKernelLogs=false"),
		"clock":                edit("ProtectClock=true", "ProtectClock=false"),
		"suid":                 edit("RestrictSUIDSGID=true", "RestrictSUIDSGID=false"),
		"personality":          edit("LockPersonality=true", "LockPersonality=false"),
		"packet family":        edit("RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK AF_VSOCK", "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK AF_VSOCK AF_PACKET"),
		"no read-only cgroup":  edit("ReadOnlyPaths=/sys/fs/cgroup\n", ""),
		"wider read-write":     edit(`ReadWritePaths="/var/lib/bbraft-kt-a1"`, `ReadWritePaths=/ "/var/lib/bbraft-kt-a1"`),
		"other data dir":       edit(`ReadWritePaths="/var/lib/bbraft-kt-a1"`, `ReadWritePaths="/var/lib/other"`),
		"other service cgroup": edit(`ReadWritePaths="/var/lib/bbraft-kt-a1" /sys/fs/cgroup/system.slice/bbraft-kt-a1.service`, `ReadWritePaths="/var/lib/bbraft-kt-a1" /sys/fs/cgroup/system.slice/other.service`),
		"duplicate device":     edit("DeviceAllow=/dev/urandom rwm\n", "DeviceAllow=/dev/urandom rwm\nDeviceAllow=/dev/kvm rwm\n"),
		"bind paths":           edit("UMask=0077", "UMask=0077\nBindPaths=/srv"),
	} {
		t.Run(name, func(t *testing.T) {
			u, err := jailKVMParseUnit(text)
			if err != nil {
				return // a malformed unit is refused too
			}
			if got, err := jailKVMImmutable(u, jailKVMSampleWant); err == nil {
				t.Fatalf("a weakened boundary was mirrored as expected: %v", got)
			}
		})
	}
}

func TestJailKVMCheckStartupLog(t *testing.T) {
	ok := []string{
		`time=T level=INFO msg="jail admitted" uid_min=900000 uid_max=900999 cgroup=/sys/fs/cgroup/system.slice/bbraft-kt-a1.service/vms`,
		`time=T level=INFO msg="cpu realized" cpu_option=Haswell-noTSX,enforce status=ok qemu=10.0.13 pid=42 jailed=true`,
		`time=T level=INFO msg="vm networking ready" network_mode=tap bridge=bbraft0 prefix=10.42.0.0/16`,
		`time=T level=INFO msg="node: qemu-img self-check passed" image=v1 steps="[recover create-overlay]" recovered=0 elapsed=1s`,
		`time=T level=INFO msg="node: manager ready" vms=0`,
		`time=T level=INFO msg="bbraft-node ready" version=dev node_id=n1 server_addr=127.0.0.1:7701 vcpus=8`,
	}
	if err := jailKVMCheckStartupLog(ok); err != nil {
		t.Fatalf("production order refused: %v", err)
	}
	drop := func(i int) []string { return slices.Delete(slices.Clone(ok), i, i+1) }
	swap := func(i, j int) []string { l := slices.Clone(ok); l[i], l[j] = l[j], l[i]; return l }
	for name, lines := range map[string][]string{
		"no admission":        drop(0),
		"no cpu":              drop(1),
		"no network":          drop(2),
		"no self-check":       drop(3),
		"no ready":            drop(5),
		"self-check first":    swap(2, 3),
		"cpu before admitted": swap(0, 1),
		"unjailed cpu":        append(slices.Clone(ok[:1]), append([]string{strings.Replace(ok[1], " jailed=true", "", 1)}, ok[2:]...)...),
		"cpu twice":           append(slices.Clone(ok[:2]), ok[1:]...),
		"user networking":     append(slices.Clone(ok), `time=T level=INFO msg="vm networking: QEMU user-mode dev/test fallback" network_mode=user dev_test=true`),
	} {
		t.Run(name, func(t *testing.T) {
			if err := jailKVMCheckStartupLog(lines); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// TestJailKVMCheckStartupCID: the real main's startup log must carry the
// local-CID exclusion exactly when the CID the harness probed itself is in
// the node's default allocation range (3..MaxUint32-1), with that CID,
// once and before "bbraft-node ready"; otherwise no exclusion at all.
func TestJailKVMCheckStartupCID(t *testing.T) {
	const excl = `time=T level=INFO msg="node: excluding local vsock cid from allocation" cid=`
	ready := `time=T level=INFO msg="bbraft-node ready" version=dev node_id=n1`
	admitted := `time=T level=INFO msg="jail admitted" uid_min=900000`
	for name, tc := range map[string]struct {
		lines []string
		local uint32
		ok    bool
	}{
		"nested guest cid excluded":       {[]string{excl + "5", admitted, ready}, 5, true},
		"host cid 2 not excluded":         {[]string{admitted, ready}, 2, true},
		"no device, nothing excluded":     {[]string{admitted, ready}, 0, true},
		"lowest in range":                 {[]string{excl + "3", ready}, 3, true},
		"highest in range":                {[]string{excl + "4294967294", ready}, math.MaxUint32 - 1, true},
		"VMADDR_CID_ANY is out of range":  {[]string{ready}, math.MaxUint32, true},
		"in range but not logged":         {[]string{admitted, ready}, 5, false},
		"other cid logged":                {[]string{excl + "6", ready}, 5, false},
		"logged twice":                    {[]string{excl + "5", excl + "5", ready}, 5, false},
		"logged after ready":              {[]string{ready, excl + "5"}, 5, false},
		"out of range but logged":         {[]string{excl + "2", ready}, 2, false},
		"exclusion logged with no device": {[]string{excl + "5", ready}, 0, false},
		"cid not a number":                {[]string{excl + "five", ready}, 5, false},
		"no ready line":                   {[]string{excl + "5"}, 5, false},
		"cid attribute missing":           {[]string{`time=T level=INFO msg="node: excluding local vsock cid from allocation"`, ready}, 5, false},
		"cid attribute missing, host cid": {[]string{`time=T level=INFO msg="node: excluding local vsock cid from allocation"`, ready}, 2, false},
	} {
		t.Run(name, func(t *testing.T) {
			if err := jailKVMCheckStartupCID(tc.lines, tc.local); (err == nil) != tc.ok {
				t.Fatalf("err %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestJailKVMCleanupPlan(t *testing.T) {
	owned := []jailKVMOwned{
		{Kind: "dir", Path: "/var/lib/bbraft-kt-a1", ID: jailKVMIdentity{1, 10}},
		{Kind: "file", Path: "/run/systemd/system/bbraft-kt-a1.service", ID: jailKVMIdentity{2, 20}},
		{Kind: "file", Path: "/run/systemd/system/bbraft-kt-a1.service.d/10-kvmtest.conf", ID: jailKVMIdentity{2, 21}},
		{Kind: "netns", Path: "/run/netns/bbj1n", ID: jailKVMIdentity{3, 30}},
	}
	match := func() []jailKVMObserved {
		out := make([]jailKVMObserved, len(owned))
		for i, o := range owned {
			out[i] = jailKVMObserved{Exists: true, ID: o.ID}
		}
		return out
	}
	t.Run("proven: remove all in reverse creation order", func(t *testing.T) {
		p := jailKVMCleanupPlan(jailKVMTeardown{UnitStopped: true, Owned: owned, Observed: match()})
		if len(p.Preserve) != 0 || len(p.Remove) != 4 || p.Remove[0] != owned[3] || p.Remove[3] != owned[0] {
			t.Fatalf("plan %+v", p)
		}
	})
	t.Run("already absent object is skipped, not uncertainty", func(t *testing.T) {
		obs := match()
		obs[2] = jailKVMObserved{Exists: false}
		p := jailKVMCleanupPlan(jailKVMTeardown{UnitStopped: true, Owned: owned, Observed: obs})
		if len(p.Preserve) != 0 || len(p.Remove) != 3 || slices.Contains(p.Remove, owned[2]) {
			t.Fatalf("plan %+v", p)
		}
	})
	for name, td := range map[string]jailKVMTeardown{
		"unit not proven stopped":  {UnitStopped: false, Owned: owned, Observed: match()},
		"populated subtree":        {UnitStopped: true, SubtreeEmpty: errors.New("populated 1"), Owned: owned, Observed: match()},
		"identity swapped":         {UnitStopped: true, Owned: owned, Observed: func() []jailKVMObserved { o := match(); o[0].ID.Ino = 99; return o }()},
		"unreadable object":        {UnitStopped: true, Owned: owned, Observed: func() []jailKVMObserved { o := match(); o[3].Err = errors.New("EIO"); return o }()},
		"listener worker unjoined": {UnitStopped: true, Owned: owned, Observed: match(), WorkersDone: errors.New("netns listener worker did not finish within 10s")},
		"observation missing":      {UnitStopped: true, Owned: owned, Observed: match()[:3]},
	} {
		t.Run(name, func(t *testing.T) {
			p := jailKVMCleanupPlan(td)
			if len(p.Remove) != 0 || len(p.Preserve) == 0 {
				t.Fatalf("uncertainty must preserve everything: %+v", p)
			}
		})
	}
}

// jailKVMFakeCgroup writes one fake cgroup directory.
func jailKVMFakeCgroup(t *testing.T, dir string, populated bool, procs string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	pop := "0"
	if populated {
		pop = "1"
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup.events"), []byte("populated "+pop+"\nfrozen 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte(procs), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestJailKVMSubtreeEmpty(t *testing.T) {
	build := func(t *testing.T, edit func(root string)) string {
		root := filepath.Join(t.TempDir(), "unit.service")
		jailKVMFakeCgroup(t, root, false, "")
		jailKVMFakeCgroup(t, filepath.Join(root, "supervisor"), false, "")
		jailKVMFakeCgroup(t, filepath.Join(root, "vms"), false, "")
		jailKVMFakeCgroup(t, filepath.Join(root, "vms", "qemu-bx_1"), false, "")
		if edit != nil {
			edit(root)
		}
		return root
	}
	if err := jailKVMSubtreeEmpty(t.Context(), build(t, nil), 16); err != nil {
		t.Fatalf("empty subtree refused: %v", err)
	}
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := jailKVMSubtreeEmpty(ctx, build(t, nil), 16); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled walk = %v, want context.Canceled", err)
		}
	})
	t.Run("symlinked root", func(t *testing.T) {
		root := build(t, nil)
		link := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink(root, link); err != nil {
			t.Fatal(err)
		}
		if err := jailKVMSubtreeEmpty(t.Context(), link, 16); err == nil {
			t.Fatal("a symlinked root was followed")
		}
	})
	for _, name := range []string{"cgroup.events", "cgroup.procs"} {
		t.Run("no-follow read of "+name, func(t *testing.T) {
			root := build(t, nil)
			real := filepath.Join(t.TempDir(), name)
			body := map[string]string{"cgroup.events": "populated 0\n", "cgroup.procs": ""}[name]
			if err := os.WriteFile(real, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			p := filepath.Join(root, "vms", name)
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(real, p); err != nil {
				t.Fatal(err)
			}
			err := jailKVMSubtreeEmpty(t.Context(), root, 16)
			if err == nil || !strings.Contains(err.Error(), "open "+name) || !errors.Is(err, syscall.ELOOP) {
				t.Fatalf("symlinked %s = %v, want the no-follow read itself to refuse (ELOOP)", name, err)
			}
		})
	}
	for name, edit := range map[string]func(string){
		"root populated":       func(r string) { jailKVMFakeCgroup(t, r, true, "") },
		"leaf populated":       func(r string) { jailKVMFakeCgroup(t, filepath.Join(r, "vms", "qemu-bx_1"), true, "") },
		"leaf procs only":      func(r string) { jailKVMFakeCgroup(t, filepath.Join(r, "vms", "qemu-bx_1"), false, "1234\n") },
		"deep leaf populated":  func(r string) { jailKVMFakeCgroup(t, filepath.Join(r, "vms", "qemu-bx_1", "inner"), true, "") },
		"events missing":       func(r string) { _ = os.Remove(filepath.Join(r, "vms", "cgroup.events")) },
		"symlinked descendant": func(r string) { _ = os.Symlink(t.TempDir(), filepath.Join(r, "vms", "link")) },
		"symlinked cgroup.events": func(r string) {
			real := filepath.Join(t.TempDir(), "events")
			if err := os.WriteFile(real, []byte("populated 0\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			_ = os.Remove(filepath.Join(r, "vms", "cgroup.events"))
			if err := os.Symlink(real, filepath.Join(r, "vms", "cgroup.events")); err != nil {
				t.Fatal(err)
			}
		},
		"symlinked cgroup.procs": func(r string) {
			real := filepath.Join(t.TempDir(), "procs")
			if err := os.WriteFile(real, nil, 0o644); err != nil {
				t.Fatal(err)
			}
			_ = os.Remove(filepath.Join(r, "vms", "cgroup.procs"))
			if err := os.Symlink(real, filepath.Join(r, "vms", "cgroup.procs")); err != nil {
				t.Fatal(err)
			}
		},
		"duplicate populated": func(r string) {
			_ = os.WriteFile(filepath.Join(r, "vms", "cgroup.events"), []byte("populated 0\npopulated 1\n"), 0o644)
		},
		"duplicate populated zero": func(r string) {
			_ = os.WriteFile(filepath.Join(r, "vms", "cgroup.events"), []byte("populated 0\npopulated 0\n"), 0o644)
		},
		"malformed populated": func(r string) {
			_ = os.WriteFile(filepath.Join(r, "vms", "cgroup.events"), []byte("populated 0 extra\n"), 0o644)
		},
		"no populated line": func(r string) {
			_ = os.WriteFile(filepath.Join(r, "vms", "cgroup.events"), []byte("frozen 0\n"), 0o644)
		},
		// Whitespace only: without the bound it would read as empty.
		"oversized procs": func(r string) {
			_ = os.WriteFile(filepath.Join(r, "vms", "cgroup.procs"), bytes.Repeat([]byte(" "), jailKVMEvidenceLimit+1), 0o644)
		},
		"oversized events": func(r string) {
			_ = os.WriteFile(filepath.Join(r, "vms", "cgroup.events"), append([]byte("populated 0\n"), bytes.Repeat([]byte(" "), jailKVMEvidenceLimit)...), 0o644)
		},
		"procs not a pid":    func(r string) { jailKVMFakeCgroup(t, filepath.Join(r, "vms", "qemu-bx_1"), true, "not-a-pid\n") },
		"procs mixed":        func(r string) { jailKVMFakeCgroup(t, filepath.Join(r, "vms", "qemu-bx_1"), true, "1234\nabc\n") },
		"procs zero":         func(r string) { jailKVMFakeCgroup(t, filepath.Join(r, "vms", "qemu-bx_1"), true, "0\n") },
		"procs negative":     func(r string) { jailKVMFakeCgroup(t, filepath.Join(r, "vms", "qemu-bx_1"), true, "-5\n") },
		"procs duplicate":    func(r string) { jailKVMFakeCgroup(t, filepath.Join(r, "vms", "qemu-bx_1"), true, "5\n5\n") },
		"procs leading zero": func(r string) { jailKVMFakeCgroup(t, filepath.Join(r, "vms", "qemu-bx_1"), true, "05\n") },
		// A populated parent never hides malformed or unreadable evidence
		// further down: only a fully valid subtree may be "populated".
		"malformed child under a populated parent": func(r string) {
			jailKVMFakeCgroup(t, filepath.Join(r, "vms"), true, "")
			_ = os.WriteFile(filepath.Join(r, "vms", "qemu-bx_1", "cgroup.events"), []byte("populated 0 extra\n"), 0o644)
		},
		"unreadable child under a populated parent": func(r string) {
			jailKVMFakeCgroup(t, filepath.Join(r, "vms"), true, "")
			_ = os.Remove(filepath.Join(r, "vms", "qemu-bx_1", "cgroup.procs"))
		},
		"too many cgroups": func(r string) {
			for i := range 20 {
				jailKVMFakeCgroup(t, filepath.Join(r, "vms", "x"+string(rune('a'+i))), false, "")
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := jailKVMSubtreeEmpty(t.Context(), build(t, edit), 16)
			if err == nil {
				t.Fatal("accepted")
			}
			// Only a well-formed subtree that holds processes is
			// "populated" (the teardown's one recovery); anything
			// malformed, unreadable or unbounded is not.
			populated := map[string]bool{"root populated": true, "leaf populated": true, "leaf procs only": true, "deep leaf populated": true}[name]
			if errors.Is(err, errJailKVMPopulated) != populated {
				t.Fatalf("err %v: populated classification %v, want %v", err, errors.Is(err, errJailKVMPopulated), populated)
			}
		})
	}
}

// jailKVMFakeNetns records the namespace operations of one listener call.
type jailKVMFakeNetns struct {
	mu          sync.Mutex
	calls       []string
	restoreErr  error
	targetErr   error
	listenBlock chan struct{}
	// listening, when set, is closed once Listen has started.
	listening chan struct{}
	lis       *jailKVMFakeListener
}

type jailKVMFakeListener struct {
	net.Listener
	f      *jailKVMFakeNetns
	closed bool
}

func (l *jailKVMFakeListener) Close() error {
	l.f.mu.Lock()
	defer l.f.mu.Unlock()
	l.closed = true
	return nil
}

func (f *jailKVMFakeNetns) add(c string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
}

func (f *jailKVMFakeNetns) ops() jailKVMNetnsOps {
	return jailKVMNetnsOps{
		OpenCurrent: func() (int, error) { f.add("open-current"); return 100, nil },
		OpenTarget:  func(string) (int, error) { f.add("open-target"); return 101, nil },
		Setns: func(fd int) error {
			f.add("setns-" + map[int]string{100: "restore", 101: "target"}[fd])
			if fd == 101 {
				return f.targetErr
			}
			return f.restoreErr
		},
		Close: func(fd int) error { f.add("close-fd"); return nil },
		Listen: func(string, string) (net.Listener, error) {
			f.add("listen")
			if f.listening != nil {
				close(f.listening)
			}
			if f.listenBlock != nil {
				<-f.listenBlock
			}
			l := &jailKVMFakeListener{f: f}
			f.mu.Lock()
			f.lis = l
			f.mu.Unlock()
			return l, nil
		},
		Unlock: func() { f.add("unlock") },
	}
}

// jailKVMWorkerDone waits (bounded) for a listener worker; a test fake that
// blocks registers its release with t.Cleanup before any Fatal path.
func jailKVMWorkerDone(t *testing.T, w *jailKVMListenWorker) {
	t.Helper()
	if w == nil {
		t.Fatal("no worker owner returned")
	}
	if err := w.Wait(5 * time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestJailKVMListenInNetns(t *testing.T) {
	t.Run("restore then unlock", func(t *testing.T) {
		f := &jailKVMFakeNetns{}
		lis, w, err := jailKVMListenInNetns(t.Context(), "/run/netns/x", "127.0.0.1:7701", time.Second, f.ops())
		if err != nil || lis == nil {
			t.Fatalf("listen: %v", err)
		}
		jailKVMWorkerDone(t, w)
		f.mu.Lock()
		defer f.mu.Unlock()
		if i, j := slices.Index(f.calls, "setns-restore"), slices.Index(f.calls, "unlock"); i < 0 || j < i || slices.Index(f.calls, "listen") > i {
			t.Fatalf("calls %q: want listen, then restore, then unlock", f.calls)
		}
		if n := strings.Count(strings.Join(f.calls, " "), "close-fd"); n != 2 {
			t.Fatalf("calls %q: both namespace descriptors must be closed before Done", f.calls)
		}
	})
	t.Run("failed restore never unlocks and closes the listener", func(t *testing.T) {
		f := &jailKVMFakeNetns{restoreErr: syscall.EPERM}
		lis, w, err := jailKVMListenInNetns(t.Context(), "/run/netns/x", "127.0.0.1:7701", time.Second, f.ops())
		if err == nil || lis != nil {
			t.Fatalf("listen = %v, %v; want an error and no listener", lis, err)
		}
		jailKVMWorkerDone(t, w)
		f.mu.Lock()
		defer f.mu.Unlock()
		if slices.Contains(f.calls, "unlock") {
			t.Fatalf("calls %q: a contaminated thread was unlocked", f.calls)
		}
		if f.lis == nil || !f.lis.closed {
			t.Fatal("the listener created in the target namespace was not closed")
		}
	})
	t.Run("failed target setns restores and unlocks without listening", func(t *testing.T) {
		f := &jailKVMFakeNetns{targetErr: syscall.EINVAL}
		_, w, err := jailKVMListenInNetns(t.Context(), "/run/netns/x", "127.0.0.1:7701", time.Second, f.ops())
		if err == nil {
			t.Fatal("accepted")
		}
		jailKVMWorkerDone(t, w)
		f.mu.Lock()
		defer f.mu.Unlock()
		if slices.Contains(f.calls, "listen") || !slices.Contains(f.calls, "setns-restore") || !slices.Contains(f.calls, "unlock") {
			t.Fatalf("calls %q", f.calls)
		}
	})
	t.Run("pre-canceled starts nothing", func(t *testing.T) {
		f := &jailKVMFakeNetns{}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		lis, w, err := jailKVMListenInNetns(ctx, "/run/netns/x", "127.0.0.1:7701", time.Second, f.ops())
		if lis != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("listen = %v, %v; want context.Canceled", lis, err)
		}
		jailKVMWorkerDone(t, w)
		f.mu.Lock()
		defer f.mu.Unlock()
		if len(f.calls) != 0 {
			t.Fatalf("a pre-canceled call did namespace work: %q", f.calls)
		}
	})
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		cancel  bool
		want    error
	}{
		{"timeout keeps DeadlineExceeded and the owner closes the late listener", 20 * time.Millisecond, false, context.DeadlineExceeded},
		{"cancellation keeps Canceled and the owner closes the late listener", time.Minute, true, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &jailKVMFakeNetns{listenBlock: make(chan struct{}), listening: make(chan struct{})}
			release := sync.OnceFunc(func() { close(f.listenBlock) })
			t.Cleanup(release)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			type ret struct {
				w   *jailKVMListenWorker
				err error
			}
			out := make(chan ret, 1)
			go func() {
				lis, w, err := jailKVMListenInNetns(ctx, "/run/netns/x", "127.0.0.1:7701", tc.timeout, f.ops())
				if lis != nil {
					_ = lis.Close()
				}
				out <- ret{w, err}
			}()
			<-f.listening
			if tc.cancel {
				cancel()
			}
			var r ret
			select {
			case r = <-out:
			case <-time.After(5 * time.Second):
				t.Fatal("the bounded call did not return")
			}
			if !errors.Is(r.err, tc.want) {
				t.Fatalf("listen error %v, want %v", r.err, tc.want)
			}
			select {
			case <-r.w.Done():
				t.Fatal("the worker reported done while its listen was still blocked")
			default:
			}
			release()
			jailKVMWorkerDone(t, r.w)
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.lis == nil || !f.lis.closed {
				t.Fatal("the worker owner did not close the late listener before Done")
			}
			if n := strings.Count(strings.Join(f.calls, " "), "close-fd"); n != 2 {
				t.Fatalf("calls %q: both namespace descriptors must be closed before Done", f.calls)
			}
		})
	}
	t.Run("cancellation just before success closes the listener", func(t *testing.T) {
		// The worker's result and the ended ctx race in the caller's select;
		// repeat so either branch is exercised.
		for range 200 {
			jailKVMCancelBeforeSuccess(t, true)
			jailKVMCancelBeforeSuccess(t, false)
		}
	})
}

// jailKVMCancelBeforeSuccess cancels inside Listen: the call must close the
// listener and report Canceled on either select branch.
func jailKVMCancelBeforeSuccess(t *testing.T, inListen bool) {
	t.Helper()
	f := &jailKVMFakeNetns{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ops := f.ops()
	if inListen {
		listen := ops.Listen
		ops.Listen = func(n, a string) (net.Listener, error) {
			l, err := listen(n, a)
			cancel()
			return l, err
		}
	} else {
		// The worker finishes with a listener; only then does the ctx end, so
		// the caller's select sees both ready and may take either branch.
		ops.BeforeSelect = func(workerDone <-chan struct{}) {
			<-workerDone
			cancel()
		}
	}
	lis, w, err := jailKVMListenInNetns(ctx, "/run/netns/x", "127.0.0.1:7701", time.Minute, ops)
	if lis != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("listen = %v, %v; want context.Canceled and no listener", lis, err)
	}
	jailKVMWorkerDone(t, w)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lis == nil || !f.lis.closed {
		t.Fatal("a listener produced under a canceled ctx was returned or left open")
	}
}

func TestJailKVMCheckProbeStatus(t *testing.T) {
	ok := "Name:\tnode.test\nUid:\t900010\t900010\t900010\t900010\nGid:\t900010\t900010\t900010\t900010\nGroups:\t\n" +
		"NoNewPrivs:\t1\nCapInh:\t0000000000000000\nCapPrm:\t0000000000000000\nCapEff:\t0000000000000000\nCapBnd:\t0000000000000000\nCapAmb:\t0000000000000000\n"
	if err := jailKVMCheckProbeStatus(ok, 900010); err != nil {
		t.Fatalf("dropped probe refused: %v", err)
	}
	for name, status := range map[string]string{
		"bounding set kept": strings.Replace(ok, "CapBnd:\t0000000000000000", "CapBnd:\t000001ffffffffff", 1),
		"effective cap":     strings.Replace(ok, "CapEff:\t0000000000000000", "CapEff:\t0000000000001000", 1),
		"ambient cap":       strings.Replace(ok, "CapAmb:\t0000000000000000", "CapAmb:\t0000000000001000", 1),
		"saved uid root":    strings.Replace(ok, "Uid:\t900010\t900010\t900010\t900010", "Uid:\t900010\t900010\t0\t900010", 1),
		"other gid":         strings.Replace(ok, "Gid:\t900010\t900010\t900010\t900010", "Gid:\t900010\t900011\t900010\t900010", 1),
		"supplementary":     strings.Replace(ok, "Groups:\t\n", "Groups:\t27 \n", 1),
		"no NNP":            strings.Replace(ok, "NoNewPrivs:\t1", "NoNewPrivs:\t0", 1),
		"missing CapBnd":    strings.Replace(ok, "CapBnd:\t0000000000000000\n", "", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if err := jailKVMCheckProbeStatus(status, 900010); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestJailKVMCrossUIDCases(t *testing.T) {
	a := jailKVMQEMU{Sandbox: "bx_aaaaaaaa", UID: 900010, Overlay: "/d/vms/bx_aaaaaaaa/disk.qcow2", ControlDir: "/d/vms/bx_aaaaaaaa/control"}
	b := jailKVMQEMU{Sandbox: "bx_bbbbbbbb", UID: 900011, Overlay: "/d/vms/bx_bbbbbbbb/disk.qcow2", ControlDir: "/d/vms/bx_bbbbbbbb/control"}
	got := jailKVMCrossUIDCases(a, b)
	// Every op reopens a root-pinned O_PATH leaf through /proc/self/fd/N:
	// the 0700 data/vm ancestors are never chmod'ed to make A's positive
	// controls pass, and no privileged open file description is reused.
	want := []jailKVMProbeCase{
		{As: a.UID, Op: "reopen-rw", Path: a.Overlay, Want: "ok"},
		{As: a.UID, Op: "reopen-dir", Path: a.ControlDir, Want: "ok"},
		{As: b.UID, Op: "reopen-rw", Path: a.Overlay, Want: "EACCES"},
		{As: b.UID, Op: "reopen-r", Path: a.Overlay, Want: "EACCES"},
		{As: b.UID, Op: "reopen-dir", Path: a.ControlDir, Want: "EACCES"},
		{As: b.UID, Op: "reopen-rw", Path: b.Overlay, Want: "ok"},
		{As: b.UID, Op: "reopen-dir", Path: b.ControlDir, Want: "ok"},
		{As: a.UID, Op: "reopen-rw", Path: b.Overlay, Want: "EACCES"},
		{As: a.UID, Op: "reopen-r", Path: b.Overlay, Want: "EACCES"},
		{As: a.UID, Op: "reopen-dir", Path: b.ControlDir, Want: "EACCES"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("cases\n got %+v\nwant %+v", got, want)
	}
}

func TestJailKVMCheckAdoptedAndNewLink(t *testing.T) {
	before := jailKVMQEMU{Sandbox: "bx_aaaaaaaa", UID: 900010, PID: 4242, StartTicks: 777, Cgroup: "/sys/fs/cgroup/system.slice/u.service/vms/qemu-bx_aaaaaaaa"}
	if err := jailKVMCheckAdopted(before, before); err != nil {
		t.Fatalf("same QEMU refused: %v", err)
	}
	for name, edit := range map[string]func(*jailKVMQEMU){
		"pid":    func(q *jailKVMQEMU) { q.PID++ },
		"ticks":  func(q *jailKVMQEMU) { q.StartTicks++ },
		"uid":    func(q *jailKVMQEMU) { q.UID++ },
		"cgroup": func(q *jailKVMQEMU) { q.Cgroup += "x" },
	} {
		t.Run(name, func(t *testing.T) {
			after := before
			edit(&after)
			if err := jailKVMCheckAdopted(before, after); err == nil {
				t.Fatal("accepted")
			}
		})
	}
	closed := make(chan struct{})
	close(closed)
	if err := jailKVMCheckNewLink(closed, "inv-1", "inv-2"); err != nil {
		t.Fatalf("rebound link refused: %v", err)
	}
	open := make(chan struct{})
	for name, tc := range map[string]struct {
		done     <-chan struct{}
		old, new string
	}{
		"old link still open": {open, "inv-1", "inv-2"},
		"same invocation":     {closed, "inv-1", "inv-1"},
		"empty invocation":    {closed, "", "inv-2"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := jailKVMCheckNewLink(tc.done, tc.old, tc.new); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// TestJailKVMRenderedUnits checks the role's harness render output
// (kvm-harness-render.yml) against the harness contract: every attempt unit
// passes the fixed boundary, and its ExecStart names the attempt's staged
// binary, data dir, node id and the cgroup of its own service name. It runs
// only when BBRAFT_TEST_KVM_JAIL_UNITS points at a render output.
func TestJailKVMRenderedUnits(t *testing.T) {
	dir := os.Getenv(jailKVMUnitsEnv)
	if dir == "" {
		t.Skipf("set %s to a kvm-harness-render.yml output directory", jailKVMUnitsEnv)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m jailKVMManifest
	if err := json.Unmarshal(raw, &m, json.RejectUnknownMembers(true)); err != nil {
		t.Fatal(err)
	}
	if got := slices.Sorted(maps.Keys(m.Attempts)); !slices.Equal(got, []string{"direct", "link", "restart", "snapshot", "startup"}) {
		t.Fatalf("attempts %q", got)
	}
	seen := map[string]bool{}
	for name, r := range m.Attempts {
		t.Run(name, func(t *testing.T) {
			if len(r.BinarySHA256) != 64 {
				t.Fatalf("binarySha256 %q", r.BinarySHA256)
			}
			for _, v := range []string{r.Attempt, r.Service, r.DataDir, r.ConfigDir, r.StageDir} {
				if seen[v] {
					t.Fatalf("%q is shared between attempts", v)
				}
				seen[v] = true
			}
			text, err := os.ReadFile(r.Unit)
			if err != nil {
				t.Fatal(err)
			}
			u, err := jailKVMParseUnit(string(text))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := jailKVMImmutable(u, jailKVMBoundaryWant{NetnsPath: "/run/netns/x", DataDir: r.DataDir, Service: r.Service}); err != nil {
				t.Fatalf("rendered unit boundary: %v", err)
			}
			if filepath.Base(r.UnitPath) != r.Service+".service" || !strings.HasSuffix(r.Service, r.Attempt) {
				t.Fatalf("unit path %q / service %q / attempt %q disagree", r.UnitPath, r.Service, r.Attempt)
			}
			if err := jailKVMCheckExec(u.ExecStart, jailKVMExpect{Binary: r.BinaryPath, Flags: map[string]string{
				"-node.id": r.NodeID, "-node.data-dir": r.DataDir, "-node.image-dir": r.ImageDir,
				"-node.token-file": r.ConfigDir + "/node.token", "-server.addr": "127.0.0.1:7701",
				"-node.ssh-ingress-iface": "up0", "-node.ssh-ingress-addr": "203.0.113.1",
				"-node.jail-cgroup": "/sys/fs/cgroup/system.slice/" + r.Service + ".service/vms",
				// Auto capacity at ratio 1.0: the harness never offers more
				// than the throwaway VM has.
				"-node.capacity-vcpus": "0", "-node.capacity-memory-mib": "0", "-node.capacity-disk-gib": "0",
				"-node.cpu-overcommit": "1.0", "-node.memory-overcommit": "1.0", "-node.disk-overcommit": "1.0",
			}}); err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(u.Service["ReadWritePaths"], `"`+r.DataDir+`" /sys/fs/cgroup/system.slice/`+r.Service+`.service`) {
				t.Fatalf("ReadWritePaths %q", u.Service["ReadWritePaths"])
			}
		})
	}
}

// TestJailKVMReadBounded: an evidence read either returns every byte within
// the bound or fails; a non-EOF read error is never a short success.
func TestJailKVMReadBounded(t *testing.T) {
	if got, err := jailKVMReadBounded(strings.NewReader("populated 0\n"), 64); err != nil || got != "populated 0\n" {
		t.Fatalf("read = %q, %v", got, err)
	}
	if _, err := jailKVMReadBounded(strings.NewReader(strings.Repeat("x", 65)), 64); err == nil {
		t.Fatal("an oversized read was accepted")
	}
	boom := errors.New("injected EIO")
	if _, err := jailKVMReadBounded(io.MultiReader(strings.NewReader("populated 0\n"), iotest.ErrReader(boom)), 64); !errors.Is(err, boom) {
		t.Fatalf("read error = %v, want the injected error", err)
	}
}

// TestJailKVMSubtreeListingBound: a cgroup directory listing larger than the
// bound is refused, never accepted as a partial page.
func TestJailKVMSubtreeListingBound(t *testing.T) {
	root := filepath.Join(t.TempDir(), "unit.service")
	jailKVMFakeCgroup(t, root, false, "")
	for i := range jailKVMListingSlack + 17 {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("f%04d", i)), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := jailKVMSubtreeEmpty(t.Context(), root, 16); err == nil || !strings.Contains(err.Error(), "entries") {
		t.Fatalf("an over-bound listing = %v, want a refusal", err)
	}
}

// TestJailKVMShowSystemCallFilter: systemd 255 prints an unset
// SystemCallFilter as "~" (an empty denylist); only exactly that means no
// filter, and any other deny or allow list is refused.
func TestJailKVMShowSystemCallFilter(t *testing.T) {
	want := map[string]string{"SystemCallFilter": ""}
	for _, ok := range []string{"SystemCallFilter=~\n", "SystemCallFilter=\n"} {
		if err := jailKVMCheckShow(want, ok); err != nil {
			t.Fatalf("%q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"SystemCallFilter=~@mount\n", "SystemCallFilter=@system-service\n", "SystemCallFilter=~ \n", "SystemCallFilter=~~\n"} {
		if err := jailKVMCheckShow(want, bad); err == nil {
			t.Fatalf("%q accepted as no filter", bad)
		}
	}
}
