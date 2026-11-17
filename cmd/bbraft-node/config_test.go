package main

import (
	"errors"
	"io"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	nodenet "github.com/nathabonfim59/bamboo-raft/internal/node/net"
	"github.com/nathabonfim59/bamboo-raft/internal/overcommit"
)

func testEnv(kv map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := kv[k]
		return v, ok
	}
}

func okHostname() (string, error) { return "test-host", nil }

func TestEnvName(t *testing.T) {
	for flagName, want := range map[string]string{
		"server.addr":              "BBRAFT_SERVER_ADDR",
		"node.id":                  "BBRAFT_NODE_ID",
		"node.token-file":          "BBRAFT_NODE_TOKEN_FILE",
		"node.data-dir":            "BBRAFT_NODE_DATA_DIR",
		"node.image-dir":           "BBRAFT_NODE_IMAGE_DIR",
		"node.default-image":       "BBRAFT_NODE_DEFAULT_IMAGE",
		"node.cpu-model":           "BBRAFT_NODE_CPU_MODEL",
		"node.cpu-nested":          "BBRAFT_NODE_CPU_NESTED",
		"node.cpu-allow-host":      "BBRAFT_NODE_CPU_ALLOW_HOST",
		"node.boot-timeout":        "BBRAFT_NODE_BOOT_TIMEOUT",
		"node.capacity-vcpus":      "BBRAFT_NODE_CAPACITY_VCPUS",
		"node.capacity-memory-mib": "BBRAFT_NODE_CAPACITY_MEMORY_MIB",
		"node.capacity-disk-gib":   "BBRAFT_NODE_CAPACITY_DISK_GIB",
		"node.reserve-vcpus":       "BBRAFT_NODE_RESERVE_VCPUS",
		"node.reserve-memory-mib":  "BBRAFT_NODE_RESERVE_MEMORY_MIB",
		"node.reserve-disk-gib":    "BBRAFT_NODE_RESERVE_DISK_GIB",
		"node.network-mode":        "BBRAFT_NODE_NETWORK_MODE",
		"node.network-bridge":      "BBRAFT_NODE_NETWORK_BRIDGE",
		"node.network-prefix":      "BBRAFT_NODE_NETWORK_PREFIX",
		"node.network-gateway":     "BBRAFT_NODE_NETWORK_GATEWAY",
		"node.network-resolvers":   "BBRAFT_NODE_NETWORK_RESOLVERS",
		"node.network-management":  "BBRAFT_NODE_NETWORK_MANAGEMENT",
		"node.ssh-ingress-addr":    "BBRAFT_NODE_SSH_INGRESS_ADDR",
		"node.ssh-ingress-iface":   "BBRAFT_NODE_SSH_INGRESS_IFACE",
		"node.ssh-port-range":      "BBRAFT_NODE_SSH_PORT_RANGE",
	} {
		if got := envName(flagName); got != want {
			t.Errorf("envName(%q) = %q, want %q", flagName, got, want)
		}
	}
}

func TestParseFlagsDefaults(t *testing.T) {
	o, err := parseFlags(nil, io.Discard, testEnv(nil), okHostname)
	if err != nil {
		t.Fatal(err)
	}
	if o.serverAddr != "127.0.0.1:7701" {
		t.Errorf("serverAddr %q, want 127.0.0.1:7701", o.serverAddr)
	}
	if o.nodeID != "test-host" {
		t.Errorf("nodeID %q, want the hostname test-host", o.nodeID)
	}
	if o.tokenFile != "" || o.defaultImage != "" {
		t.Errorf("tokenFile %q defaultImage %q, want empty", o.tokenFile, o.defaultImage)
	}
	if o.dataDir != "/var/lib/bbraft" || o.imageDir != "/var/lib/bbraft/images" {
		t.Errorf("dirs %q %q", o.dataDir, o.imageDir)
	}
	if o.cpuModel != "Haswell-noTSX" || o.cpuNested != "auto" || o.bootTimeout != 2*time.Minute {
		t.Errorf("cpuModel %q cpuNested %q bootTimeout %v", o.cpuModel, o.cpuNested, o.bootTimeout)
	}
	if o.cpuAllowHost {
		t.Error("cpuAllowHost must default to false")
	}
	if o.capacityVCPUs != 0 || o.capacityMemoryMiB != 0 || o.capacityDiskGiB != 0 {
		t.Errorf("capacity %+v, want zeros (autodetect)", o)
	}
	if o.reserveVCPUs != 1 || o.reserveMemoryMiB != 1024 || o.reserveDiskGiB != 1 {
		t.Errorf("reserves %d %d %d, want 1/1024/1", o.reserveVCPUs, o.reserveMemoryMiB, o.reserveDiskGiB)
	}
	// Production TAP networking is the default; only the harmless bridge and
	// resolver defaults are built in, so parseFlags accepts the default set
	// and networking() is the fail-closed completeness check.
	if o.networkMode != networkModeTap {
		t.Errorf("networkMode %q, want %q", o.networkMode, networkModeTap)
	}
	if o.networkBridge != defaultNetworkBridge || o.networkResolvers != "1.1.1.1,9.9.9.9" {
		t.Errorf("network defaults bridge %q resolvers %q", o.networkBridge, o.networkResolvers)
	}
	if o.networkPrefix != "" || o.networkGateway != "" || o.networkManagement != "" ||
		o.sshIngressAddr != "" || o.sshIngressIface != "" || o.sshPortRange != "" {
		t.Errorf("operator network inputs must have no defaults: %+v", o)
	}
}

func TestParseFlagsEnvFallback(t *testing.T) {
	env := testEnv(map[string]string{
		"BBRAFT_SERVER_ADDR":              "10.0.0.7:9000",
		"BBRAFT_NODE_ID":                  "env-node",
		"BBRAFT_NODE_TOKEN_FILE":          "/run/secrets/node-token",
		"BBRAFT_NODE_DATA_DIR":            "/data/bbraft",
		"BBRAFT_NODE_IMAGE_DIR":           "/data/images",
		"BBRAFT_NODE_DEFAULT_IMAGE":       "ubuntu-24.04-v0",
		"BBRAFT_NODE_CPU_MODEL":           "x86-64-v4",
		"BBRAFT_NODE_CPU_NESTED":          "off",
		"BBRAFT_NODE_BOOT_TIMEOUT":        "90s",
		"BBRAFT_NODE_CAPACITY_VCPUS":      "12",
		"BBRAFT_NODE_CAPACITY_MEMORY_MIB": "24576",
		"BBRAFT_NODE_CAPACITY_DISK_GIB":   "300",
		"BBRAFT_NODE_RESERVE_VCPUS":       "2",
		"BBRAFT_NODE_RESERVE_MEMORY_MIB":  "2048",
		"BBRAFT_NODE_RESERVE_DISK_GIB":    "5",
		"BBRAFT_NODE_NETWORK_MODE":        "user",
		"BBRAFT_NODE_NETWORK_BRIDGE":      "example0",
		"BBRAFT_NODE_NETWORK_RESOLVERS":   "9.9.9.9",
		"BBRAFT_NODE_SSH_INGRESS_IFACE":   "example0",
	})
	o, err := parseFlags(nil, io.Discard, env, okHostname)
	if err != nil {
		t.Fatal(err)
	}
	if o.serverAddr != "10.0.0.7:9000" || o.nodeID != "env-node" || o.tokenFile != "/run/secrets/node-token" {
		t.Fatalf("env fallback %+v", o)
	}
	if o.dataDir != "/data/bbraft" || o.imageDir != "/data/images" || o.defaultImage != "ubuntu-24.04-v0" {
		t.Fatalf("env fallback %+v", o)
	}
	if o.cpuModel != "x86-64-v4" || o.cpuNested != "off" || o.bootTimeout != 90*time.Second {
		t.Fatalf("env fallback %+v", o)
	}
	if o.capacityVCPUs != 12 || o.capacityMemoryMiB != 24576 || o.capacityDiskGiB != 300 {
		t.Fatalf("env fallback %+v", o)
	}
	if o.reserveVCPUs != 2 || o.reserveMemoryMiB != 2048 || o.reserveDiskGiB != 5 {
		t.Fatalf("env fallback %+v", o)
	}
	if o.networkMode != networkModeUser || o.networkBridge != "example0" ||
		o.networkResolvers != "9.9.9.9" || o.sshIngressIface != "example0" {
		t.Fatalf("network env fallback %+v", o)
	}
}

func TestParseFlagsFlagBeatsEnv(t *testing.T) {
	env := testEnv(map[string]string{
		"BBRAFT_NODE_CAPACITY_VCPUS": "8",
		"BBRAFT_NODE_BOOT_TIMEOUT":   "90s",
		"BBRAFT_NODE_ID":             "env-node",
	})
	args := []string{"-node.capacity-vcpus=4", "-node.boot-timeout=30s", "-node.id=flag-node"}
	o, err := parseFlags(args, io.Discard, env, okHostname)
	if err != nil {
		t.Fatal(err)
	}
	if o.capacityVCPUs != 4 || o.bootTimeout != 30*time.Second || o.nodeID != "flag-node" {
		t.Fatalf("flags %+v", o)
	}
}

// A malformed env value is a config error only when no explicit flag
// replaces it; the override path never parses the bad env value. Errors
// name the variable and a safe reason, never the offending value.
func TestParseFlagsMalformedEnv(t *testing.T) {
	for name, val := range map[string]string{
		"BBRAFT_NODE_CAPACITY_VCPUS":      "abc",
		"BBRAFT_NODE_CAPACITY_MEMORY_MIB": "-5",
		"BBRAFT_NODE_CAPACITY_DISK_GIB":   "1.5",
		"BBRAFT_NODE_RESERVE_VCPUS":       "many",
		"BBRAFT_NODE_RESERVE_MEMORY_MIB":  "ten",
		"BBRAFT_NODE_RESERVE_DISK_GIB":    "18446744073709551616",
		"BBRAFT_NODE_BOOT_TIMEOUT":        "soon",
		"BBRAFT_NODE_CPU_ALLOW_HOST":      "maybe",
	} {
		env := testEnv(map[string]string{name: val})
		if _, err := parseFlags(nil, io.Discard, env, okHostname); err == nil {
			t.Errorf("env %s=%q: want error", name, val)
		} else if !strings.Contains(err.Error(), name) {
			t.Errorf("env %s=%q: error %v does not name the variable", name, val, err)
		}
	}
	// Sentinel non-disclosure: a distinctive bad value must appear in
	// neither the error nor anything written to stderr.
	var stderr strings.Builder
	for _, name := range []string{"BBRAFT_NODE_CAPACITY_VCPUS", "BBRAFT_NODE_BOOT_TIMEOUT", "BBRAFT_NODE_CPU_ALLOW_HOST"} {
		env := testEnv(map[string]string{name: "SENTINEL-bad-9f3e"})
		_, err := parseFlags(nil, &stderr, env, okHostname)
		if err == nil {
			t.Fatalf("env %s: want error", name)
		}
		if strings.Contains(err.Error(), "SENTINEL-bad-9f3e") {
			t.Errorf("env %s: error %v contains the bad value", name, err)
		}
	}
	if strings.Contains(stderr.String(), "SENTINEL-bad-9f3e") {
		t.Error("stderr contains the bad env value")
	}

	// Explicit flags mean the env value is never parsed at all.
	env := testEnv(map[string]string{
		"BBRAFT_NODE_CAPACITY_VCPUS": "abc",
		"BBRAFT_NODE_BOOT_TIMEOUT":   "soon",
	})
	o, err := parseFlags([]string{"-node.capacity-vcpus=4", "-node.boot-timeout=30s"}, io.Discard, env, okHostname)
	if err != nil {
		t.Fatalf("explicit flags must override malformed env: %v", err)
	}
	if o.capacityVCPUs != 4 || o.bootTimeout != 30*time.Second {
		t.Fatalf("flags %+v", o)
	}
}

func TestParseFlagsNodeID(t *testing.T) {
	o, err := parseFlags([]string{"-node.id=  "}, io.Discard, testEnv(nil), okHostname)
	if err != nil {
		t.Fatal(err)
	}
	if o.nodeID != "test-host" {
		t.Fatalf("nodeID %q, want hostname fallback", o.nodeID)
	}
	if _, err := parseFlags(nil, io.Discard, testEnv(nil), func() (string, error) {
		return "", io.ErrUnexpectedEOF
	}); err == nil {
		t.Fatal("want error when the hostname is unavailable and node.id is unset")
	}
}

// The opt-in is a plain boolean: default false, env fallback parses like
// any flag, and an explicit flag always wins — including =false.
func TestParseFlagsCPUAllowHost(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want bool
	}{
		{"default", nil, nil, false},
		{"env true", nil, map[string]string{"BBRAFT_NODE_CPU_ALLOW_HOST": "true"}, true},
		{"env 1", nil, map[string]string{"BBRAFT_NODE_CPU_ALLOW_HOST": "1"}, true},
		{"env false", nil, map[string]string{"BBRAFT_NODE_CPU_ALLOW_HOST": "false"}, false},
		{"bare flag", []string{"-node.cpu-allow-host"}, nil, true},
		{"flag true", []string{"-node.cpu-allow-host=true"}, nil, true},
		{"flag true beats env false", []string{"-node.cpu-allow-host"}, map[string]string{"BBRAFT_NODE_CPU_ALLOW_HOST": "false"}, true},
		{"explicit flag false beats env true", []string{"-node.cpu-allow-host=false"}, map[string]string{"BBRAFT_NODE_CPU_ALLOW_HOST": "true"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, err := parseFlags(tc.args, io.Discard, testEnv(tc.env), okHostname)
			if err != nil {
				t.Fatal(err)
			}
			if o.cpuAllowHost != tc.want {
				t.Fatalf("cpuAllowHost %v, want %v", o.cpuAllowHost, tc.want)
			}
		})
	}
}

// host and max track the machine, so they are refused by default. The
// dev/test opt-in lifts the refusal for exactly "host" — max, other
// casings and every other host-dependent spelling stay refused.
func TestParseFlagsCPUModelHost(t *testing.T) {
	for _, tc := range []struct {
		name      string
		args      []string
		env       map[string]string
		wantErr   bool
		wantModel string
	}{
		{"host refused by default", []string{"-node.cpu-model=host"}, nil, true, ""},
		{"host refused with env opt-in false", []string{"-node.cpu-model=host"}, map[string]string{"BBRAFT_NODE_CPU_ALLOW_HOST": "false"}, true, ""},
		{"host refused when flag false overrides env true", []string{"-node.cpu-allow-host=false", "-node.cpu-model=host"}, map[string]string{"BBRAFT_NODE_CPU_ALLOW_HOST": "true"}, true, ""},
		{"host allowed with flag opt-in", []string{"-node.cpu-allow-host", "-node.cpu-model=host"}, nil, false, "host"},
		{"host allowed with env opt-in", []string{"-node.cpu-model=host"}, map[string]string{"BBRAFT_NODE_CPU_ALLOW_HOST": "true"}, false, "host"},
		{"Host casing refused with opt-in", []string{"-node.cpu-allow-host", "-node.cpu-model=Host"}, nil, true, ""},
		{"HOST casing refused with opt-in", []string{"-node.cpu-allow-host", "-node.cpu-model=HOST"}, nil, true, ""},
		{"max refused with opt-in", []string{"-node.cpu-allow-host", "-node.cpu-model=max"}, nil, true, ""},
		{"Max refused with opt-in", []string{"-node.cpu-allow-host", "-node.cpu-model=Max"}, nil, true, ""},
		{"named model allowed with opt-in", []string{"-node.cpu-allow-host", "-node.cpu-model=Skylake-Server"}, nil, false, "Skylake-Server"},
		{"opt-in alone keeps the default model", []string{"-node.cpu-allow-host"}, nil, false, "Haswell-noTSX"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, err := parseFlags(tc.args, io.Discard, testEnv(tc.env), okHostname)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseFlags(%v) accepted a host-dependent model", tc.args)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if o.cpuModel != tc.wantModel {
				t.Fatalf("cpuModel %q, want %q", o.cpuModel, tc.wantModel)
			}
		})
	}
}

func TestParseFlagsRejectsBadValues(t *testing.T) {
	for _, args := range [][]string{
		{"-node.boot-timeout=0s"},
		{"-node.boot-timeout=-1s"},
		{"-node.data-dir="},
		{"-node.image-dir="},
		{"-node.cpu-model=host"},
		{"-node.cpu-model=Max"},
		{"-node.cpu-model=x86-64-v3,+vmx"},
		{"-node.cpu-nested=maybe"},
		{"-node.cpu-nested=ON"},
		{"-node.capacity-vcpus=4294967296"},
		{"-node.reserve-vcpus=4294967296"},
		{"-server.addr=8.8.8.8:7701"},
		{"extra-arg"},
	} {
		if _, err := parseFlags(args, io.Discard, testEnv(nil), okHostname); err == nil {
			t.Errorf("parseFlags(%v) accepted bad values", args)
		}
	}
}

func TestCheckServerAddr(t *testing.T) {
	for _, addr := range []string{
		"127.0.0.1:7701",
		"127.0.0.2:1",
		"localhost:7701",
		"LOCALHOST:7701",
		"[::1]:7701",
		"10.0.0.5:7701",
		"172.16.3.4:7701",
		"192.168.1.9:7701",
		"[fd00::1]:7701",
		"[::ffff:10.0.0.1]:7701",
		"[::ffff:192.168.0.1]:7701",
	} {
		if err := checkServerAddr(addr); err != nil {
			t.Errorf("checkServerAddr(%q) = %v, want nil", addr, err)
		}
	}
	for _, addr := range []string{
		"0.0.0.0:7701",          // wildcard, not a server address
		"[::]:7701",             // wildcard v6
		"8.8.8.8:7701",          // public
		"[::ffff:8.8.8.8]:7701", // mapped public v6
		"[fe80::1%eth0]:7701",   // link-local is neither loopback nor private
		"example.com:7701",      // ambiguous hostname
		"node1:7701",            // ambiguous hostname
		"myhost.local:7701",     // ambiguous hostname
		"127.0.0.1",             // missing port
		"127.0.0.1:",            // empty port
		"127.0.0.1:0",           // zero port
		"127.0.0.1:65536",       // out of range
		"127.0.0.1:abc",         // not a number
		":7701",                 // no host
		"",                      // empty
		"localhost",             // missing port
		"169.254.1.1:7701",      // link-local v4
	} {
		if err := checkServerAddr(addr); err == nil {
			t.Errorf("checkServerAddr(%q) accepted a non-private address", addr)
		}
	}
}

func TestResolveCapacity(t *testing.T) {
	facts := hostFacts{vcpus: 8, memoryMiB: 16384, diskGiB: 200}

	// Autodetect deducts each resource's reserve.
	o := &options{reserveVCPUs: 1, reserveMemoryMiB: 1024, reserveDiskGiB: 1}
	r, err := resolveCapacity(o, facts)
	if err != nil {
		t.Fatal(err)
	}
	if r.GetVcpus() != 7 || r.GetMemoryMib() != 15360 || r.GetDiskGib() != 199 {
		t.Fatalf("detected offer %v", r)
	}

	// Explicit values are authoritative: no reserve is deducted.
	o = &options{
		capacityVCPUs: 4, capacityMemoryMiB: 4096, capacityDiskGiB: 100,
		reserveVCPUs: 1, reserveMemoryMiB: 1024, reserveDiskGiB: 1,
	}
	r, err = resolveCapacity(o, facts)
	if err != nil {
		t.Fatal(err)
	}
	if r.GetVcpus() != 4 || r.GetMemoryMib() != 4096 || r.GetDiskGib() != 100 {
		t.Fatalf("explicit offer %v", r)
	}

	// Mixed: only the detected fields pay the reserve.
	o = &options{capacityVCPUs: 4, reserveVCPUs: 1, reserveMemoryMiB: 1024, reserveDiskGiB: 1}
	r, err = resolveCapacity(o, facts)
	if err != nil {
		t.Fatal(err)
	}
	if r.GetVcpus() != 4 || r.GetMemoryMib() != 15360 || r.GetDiskGib() != 199 {
		t.Fatalf("mixed offer %v", r)
	}
}

func TestResolveCapacityReserveExhausts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		o     options
		facts hostFacts
	}{
		{"reserve equals vcpus", options{reserveVCPUs: 4}, hostFacts{vcpus: 4, memoryMiB: 8192, diskGiB: 50}},
		{"reserve exceeds vcpus", options{reserveVCPUs: 9}, hostFacts{vcpus: 8, memoryMiB: 8192, diskGiB: 50}},
		{"reserve equals memory", options{reserveMemoryMiB: 8192}, hostFacts{vcpus: 8, memoryMiB: 8192, diskGiB: 50}},
		{"reserve exceeds disk", options{reserveDiskGiB: 60}, hostFacts{vcpus: 8, memoryMiB: 8192, diskGiB: 50}},
		{"undetectable cpu", options{}, hostFacts{vcpus: 0, memoryMiB: 8192, diskGiB: 50}},
		{"undetectable memory", options{}, hostFacts{vcpus: 8, memoryMiB: 0, diskGiB: 50}},
		{"undetectable disk", options{}, hostFacts{vcpus: 8, memoryMiB: 8192, diskGiB: 0}},
		{"zero reserve still needs a host", options{reserveVCPUs: 0}, hostFacts{vcpus: 0, memoryMiB: 8192, diskGiB: 50}},
		// Explicit values must fit the detected host: overcommit is a
		// config error, and an unverifiable host cannot take even a
		// plausible explicit offer.
		{"explicit vcpus overcommit", options{capacityVCPUs: 9}, hostFacts{vcpus: 8, memoryMiB: 8192, diskGiB: 50}},
		{"explicit memory overcommit", options{capacityMemoryMiB: 8193}, hostFacts{vcpus: 8, memoryMiB: 8192, diskGiB: 50}},
		{"explicit disk overcommit", options{capacityDiskGiB: 51}, hostFacts{vcpus: 8, memoryMiB: 8192, diskGiB: 50}},
		{"explicit on unverifiable host", options{capacityVCPUs: 2}, hostFacts{vcpus: 0, memoryMiB: 8192, diskGiB: 50}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := tc.o
			if _, err := resolveCapacity(&o, tc.facts); err == nil {
				t.Fatal("want error; capacity must never be fabricated")
			}
		})
	}
}

func TestResolveCapacityOvercommit(t *testing.T) {
	facts := hostFacts{vcpus: 8, memoryMiB: 16384, diskGiB: 200}
	o := options{
		reserveVCPUs: 1, reserveMemoryMiB: 1024, reserveDiskGiB: 1,
		cpuOvercommit: 2, memoryOvercommit: 1.5, diskOvercommit: 1.15,
	}
	r, err := resolveCapacity(&o, facts)
	if err != nil {
		t.Fatal(err)
	}
	if r.GetVcpus() != 15 || r.GetMemoryMib() != 23552 || r.GetDiskGib() != 229 {
		t.Fatalf("overcommitted offer %v, want 15/23552/229", r)
	}
	// Explicit capacity may exceed the detected host but never the scaled
	// promise; no reserve is deducted from explicit values.
	o = options{
		capacityVCPUs: 16, capacityMemoryMiB: 24576, capacityDiskGiB: 230,
		cpuOvercommit: 2, memoryOvercommit: 1.5, diskOvercommit: 1.15,
	}
	if r, err = resolveCapacity(&o, facts); err != nil {
		t.Fatal(err)
	}
	if r.GetVcpus() != 16 || r.GetMemoryMib() != 24576 || r.GetDiskGib() != 230 {
		t.Fatalf("explicit scaled offer %v", r)
	}
	for _, tc := range []struct {
		name string
		o    options
	}{
		{"vcpus above scaled", options{capacityVCPUs: 17, cpuOvercommit: 2}},
		{"memory above scaled", options{capacityMemoryMiB: 24577, memoryOvercommit: 1.5}},
		{"disk above scaled", options{capacityDiskGiB: 231, diskOvercommit: 1.15}},
		{"cpu scales to zero", options{cpuOvercommit: 0.01}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := tc.o
			if _, err := resolveCapacity(&o, facts); err == nil {
				t.Fatal("want overcommit bound error")
			}
		})
	}
}

func TestParseFlagsOvercommitValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
	}{
		{"cpu explicit zero", []string{"-node.cpu-overcommit=0"}, nil},
		{"cpu negative", []string{"-node.cpu-overcommit=-1"}, nil},
		{"memory NaN", []string{"-node.memory-overcommit=NaN"}, nil},
		{"disk infinite", []string{"-node.disk-overcommit=Inf"}, nil},
		{"env disk zero", nil, map[string]string{"BBRAFT_NODE_DISK_OVERCOMMIT": "0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseFlags(tc.args, io.Discard, testEnv(tc.env), okHostname); err == nil {
				t.Fatal("invalid overcommit ratio accepted")
			}
		})
	}
	o, err := parseFlags([]string{"-node.cpu-overcommit=1.25", "-node.memory-overcommit=2", "-node.disk-overcommit=1.15"},
		io.Discard, testEnv(nil), okHostname)
	if err != nil {
		t.Fatal(err)
	}
	if o.cpuOvercommit != 1.25 || o.memoryOvercommit != 2 || o.diskOvercommit != 1.15 {
		t.Fatalf("parsed ratios = %v/%v/%v", o.cpuOvercommit, o.memoryOvercommit, o.diskOvercommit)
	}
	def, err := parseFlags(nil, io.Discard, testEnv(nil), okHostname)
	if err != nil {
		t.Fatal(err)
	}
	if def.cpuOvercommit != 1 || def.memoryOvercommit != 1 || def.diskOvercommit != 1 {
		t.Fatalf("default ratios = %v/%v/%v", def.cpuOvercommit, def.memoryOvercommit, def.diskOvercommit)
	}

	fromEnv, err := parseFlags(nil, io.Discard, testEnv(map[string]string{
		"BBRAFT_NODE_CPU_OVERCOMMIT":    "1.5",
		"BBRAFT_NODE_MEMORY_OVERCOMMIT": "2",
		"BBRAFT_NODE_DISK_OVERCOMMIT":   "1.25",
	}), okHostname)
	if err != nil {
		t.Fatal(err)
	}
	if fromEnv.cpuOvercommit != 1.5 || fromEnv.memoryOvercommit != 2 || fromEnv.diskOvercommit != 1.25 {
		t.Fatalf("env ratios = %v/%v/%v", fromEnv.cpuOvercommit, fromEnv.memoryOvercommit, fromEnv.diskOvercommit)
	}
	// Explicit flags win over malformed or zero environment values.
	precedence, err := parseFlags(
		[]string{"-node.cpu-overcommit=3", "-node.memory-overcommit=2.5", "-node.disk-overcommit=1.1"},
		io.Discard,
		testEnv(map[string]string{
			"BBRAFT_NODE_CPU_OVERCOMMIT":    "not-a-number",
			"BBRAFT_NODE_MEMORY_OVERCOMMIT": "Inf",
			"BBRAFT_NODE_DISK_OVERCOMMIT":   "0",
		}),
		okHostname,
	)
	if err != nil {
		t.Fatal(err)
	}
	if precedence.cpuOvercommit != 3 || precedence.memoryOvercommit != 2.5 || precedence.diskOvercommit != 1.1 {
		t.Fatalf("flag precedence = %v/%v/%v", precedence.cpuOvercommit, precedence.memoryOvercommit, precedence.diskOvercommit)
	}
}

func TestResolveCapacityBounds(t *testing.T) {
	huge := hostFacts{vcpus: math.MaxUint64, memoryMiB: math.MaxUint64, diskGiB: math.MaxUint64}
	for _, tc := range []struct {
		name     string
		o        options
		facts    hostFacts
		wantErr  error
		wantFlag string
	}{
		{"vcpus above uint32", options{capacityVCPUs: math.MaxUint32 + 1}, huge, nil, "node.capacity-vcpus"},
		{"memory above byte bound", options{
			capacityVCPUs: 1, capacityMemoryMiB: math.MaxUint64>>20 + 1, capacityDiskGiB: 1,
		}, huge, nil, "node.capacity-memory-mib"},
		{"disk above byte bound", options{
			capacityVCPUs: 1, capacityMemoryMiB: 1, capacityDiskGiB: math.MaxUint64>>30 + 1,
		}, huge, nil, "node.capacity-disk-gib"},
		// Each scaled case uses a huge value only in its own resource so the
		// intended overflow block, not an earlier field, is executed.
		{"scaled vcpus overflow", options{cpuOvercommit: 2},
			hostFacts{vcpus: math.MaxUint64, memoryMiB: 64, diskGiB: 1}, overcommit.ErrOverflow, "node.cpu-overcommit"},
		{"scaled memory overflow", options{memoryOvercommit: 2},
			hostFacts{vcpus: 1, memoryMiB: math.MaxUint64, diskGiB: 1}, overcommit.ErrOverflow, "node.memory-overcommit"},
		{"scaled disk overflow", options{diskOvercommit: 2},
			hostFacts{vcpus: 1, memoryMiB: 64, diskGiB: math.MaxUint64}, overcommit.ErrOverflow, "node.disk-overcommit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := tc.o
			_, err := resolveCapacity(&o, tc.facts)
			if err == nil {
				t.Fatal("want overflow/bound error")
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantFlag) {
				t.Fatalf("error %q does not name %s", err, tc.wantFlag)
			}
		})
	}
}

func TestStatfsAvailGiB(t *testing.T) {
	gib, err := statfsAvailGiB(&syscall.Statfs_t{Bsize: 4096, Bavail: 1 << 18}, "x")
	if err != nil {
		t.Fatal(err)
	}
	if gib != 1 {
		t.Fatalf("got %d GiB, want 1", gib)
	}
	if _, err := statfsAvailGiB(&syscall.Statfs_t{Bsize: 4096, Bavail: math.MaxUint64}, "x"); err == nil {
		t.Fatal("want overflow error")
	}
	if _, err := statfsAvailGiB(&syscall.Statfs_t{Bsize: 0, Bavail: 1}, "x"); err == nil {
		t.Fatal("want error for non-positive block size")
	}
}

func TestMemTotalMiB(t *testing.T) {
	fixture := filepath.Join(t.TempDir(), "meminfo")
	if err := os.WriteFile(fixture, []byte("MemTotal:       16384000 kB\nMemFree:         8000000 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mi, err := memTotalMiB(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if mi != 16000 {
		t.Fatalf("memTotalMiB = %d, want 16000", mi)
	}

	if err := os.WriteFile(fixture, []byte("MemFree: 1 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := memTotalMiB(fixture); err == nil {
		t.Fatal("want error when MemTotal is missing")
	}
	if _, err := memTotalMiB(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("want error for unreadable meminfo")
	}
}

func TestDiskFreeGiB(t *testing.T) {
	if _, err := diskFreeGiB(t.TempDir()); err != nil {
		t.Fatalf("statfs on a temp dir: %v", err)
	}
	// A missing data dir falls back to its deepest existing ancestor.
	missing := filepath.Join(t.TempDir(), "a", "b", "c")
	if _, err := diskFreeGiB(missing); err != nil {
		t.Fatalf("statfs ancestor walk: %v", err)
	}
}

func writeToken(t *testing.T, dir, name, content string, perm os.FileMode) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), perm); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestReadTokenFile(t *testing.T) {
	dir := t.TempDir()
	p := writeToken(t, dir, "tok", "  tok-abc\n", 0o600)
	tok, err := readTokenFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "tok-abc" {
		t.Fatalf("token %q, want trimmed tok-abc", tok)
	}

	// Stricter than 0600 is fine.
	p = writeToken(t, dir, "ro", "tok-ro", 0o400)
	if _, err := readTokenFile(p); err != nil {
		t.Fatalf("mode 0400 must be accepted: %v", err)
	}
}

func TestReadTokenFileRejects(t *testing.T) {
	dir := t.TempDir()

	for _, perm := range []os.FileMode{0o644, 0o640, 0o604, 0o777} {
		p := writeToken(t, dir, "leak"+perm.String(), "tok", perm)
		if _, err := readTokenFile(p); err == nil {
			t.Errorf("mode %04o must be refused", perm)
		}
	}

	if _, err := readTokenFile(writeToken(t, dir, "empty", "", 0o600)); err == nil {
		t.Error("empty file must be refused")
	}
	if _, err := readTokenFile(writeToken(t, dir, "ws", "  \n\t ", 0o600)); err == nil {
		t.Error("whitespace-only file must be refused")
	}
	big := writeToken(t, dir, "big", strings.Repeat("x", maxTokenBytes+1), 0o600)
	if _, err := readTokenFile(big); err == nil {
		t.Error("oversized file must be refused")
	}
	if _, err := readTokenFile(dir); err == nil {
		t.Error("a directory is not a regular file")
	}
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readTokenFile(fifo); err == nil {
		t.Error("a fifo must be refused without blocking")
	}
	bait := writeToken(t, dir, "bait", "bait", 0o600)
	link := filepath.Join(dir, "link")
	if err := os.Symlink(bait, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readTokenFile(link); err == nil {
		t.Error("a symlink must be refused")
	}
	if _, err := readTokenFile(filepath.Join(dir, "missing")); err == nil {
		t.Error("missing file must fail")
	}
}

// A swap between stat and open must never smuggle the bait token through a
// symlink: every call either fails or reads the real file. The writer performs
// one real swap before a one-time started barrier, then keeps alternating
// continuously while the reader runs, preserving actual concurrency. The
// reader proves bait rejection and at least one successful regular-file read
// inside a bounded attempt loop; stop + join observe every failure path.
func TestReadTokenFileSwapRace(t *testing.T) {
	dir := t.TempDir()
	path := writeToken(t, dir, "tok", "real-token", 0o600)
	bait := writeToken(t, dir, "bait", "bait-token", 0o600)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	defer func() {
		close(stop)
		wg.Wait()
	}()
	started := make(chan struct{})
	var swaps, reads atomic.Int64
	wg.Go(func() {
		link := filepath.Join(dir, "swap")
		// Causally prove the writer ran: perform the first real swap before
		// releasing the reader.
		_ = os.Symlink(bait, link)
		if err := os.Rename(link, path); err == nil {
			swaps.Add(1)
		}
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = os.Symlink(bait, link)
			if err := os.Rename(link, path); err == nil {
				swaps.Add(1)
			}
			runtime.Gosched()
			select {
			case <-stop:
				return
			default:
			}
			_ = os.Remove(path)
			_ = os.WriteFile(path, []byte("real-token"), 0o600)
			runtime.Gosched()
		}
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("writer never performed its first swap")
	}
	deadline := time.Now().Add(10 * time.Second)
	attempts := 0
	for attempts < 300 || swaps.Load() == 0 || reads.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("race not exercised in time: attempts=%d swaps=%d reads=%d", attempts, swaps.Load(), reads.Load())
		}
		tok, err := readTokenFile(path)
		attempts++
		if err == nil {
			if tok != "real-token" {
				t.Fatalf("readTokenFile returned %q during a swap", tok)
			}
			reads.Add(1)
		}
		if attempts%64 == 0 {
			runtime.Gosched()
		}
	}
	if attempts < 300 || swaps.Load() == 0 || reads.Load() == 0 {
		t.Fatalf("race not exercised: attempts=%d swaps=%d reads=%d", attempts, swaps.Load(), reads.Load())
	}
}

func TestLoadToken(t *testing.T) {
	dir := t.TempDir()
	p := writeToken(t, dir, "tok", "file-token\n", 0o600)

	// A configured token file wins over the environment.
	tok, err := loadToken(testEnv(map[string]string{"BBRAFT_NODE_TOKEN": "env-token"}), p)
	if err != nil || tok != "file-token" {
		t.Fatalf("file precedence: tok %q err %v", tok, err)
	}
	tok, err = loadToken(testEnv(map[string]string{"BBRAFT_NODE_TOKEN": " env-token\n"}), "")
	if err != nil || tok != "env-token" {
		t.Fatalf("env fallback: tok %q err %v", tok, err)
	}
	if _, err := loadToken(testEnv(map[string]string{"BBRAFT_NODE_TOKEN": "  "}), ""); err == nil {
		t.Fatal("empty BBRAFT_NODE_TOKEN must fail")
	}
	if _, err := loadToken(testEnv(map[string]string{"BBRAFT_NODE_TOKEN": strings.Repeat("x", maxTokenBytes+1)}), ""); err == nil {
		t.Fatal("oversized BBRAFT_NODE_TOKEN must fail")
	}
	err = func() error { _, e := loadToken(testEnv(nil), ""); return e }()
	if err == nil {
		t.Fatal("no token source must fail")
	}
	for _, name := range []string{"node.token-file", "BBRAFT_NODE_TOKEN_FILE", "BBRAFT_NODE_TOKEN"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %s", err, name)
		}
	}
	// An explicitly configured file fails on its own: no silent env
	// fallback hides the mistake.
	if _, err := loadToken(testEnv(map[string]string{"BBRAFT_NODE_TOKEN": "env-token"}), filepath.Join(dir, "missing")); err == nil {
		t.Fatal("a configured but missing token file must fail, not fall back")
	}
}

// tapArgs is a complete production TAP configuration: a private guest
// prefix, its gateway, a documentation-only ingress address and a bounded
// port pool. The bridge and resolvers come from their built-in defaults.
func tapArgs() []string {
	return []string{
		"-node.network-prefix=10.42.0.0/16",
		"-node.network-gateway=10.42.0.1",
		"-node.ssh-ingress-addr=203.0.113.10",
		"-node.ssh-port-range=22000-22999",
	}
}

// parse tap: flag and env precedence for network flags, including malformed
// env values that an explicit flag replaces.
func TestParseFlagsNetworkPrecedence(t *testing.T) {
	env := testEnv(map[string]string{
		"BBRAFT_NODE_NETWORK_MODE":      "user",
		"BBRAFT_NODE_NETWORK_PREFIX":    "not-a-prefix",
		"BBRAFT_NODE_NETWORK_GATEWAY":   "not-an-address",
		"BBRAFT_NODE_NETWORK_RESOLVERS": "not-a-resolver",
		"BBRAFT_NODE_SSH_PORT_RANGE":    "not-a-range",
		"BBRAFT_NODE_SSH_INGRESS_IFACE": "env0",
	})
	args := append(tapArgs(),
		"-node.network-mode=tap",
		"-node.network-resolvers=1.0.0.1,9.9.9.9",
		"-node.ssh-ingress-iface=flag0",
	)
	o, err := parseFlags(args, io.Discard, env, okHostname)
	if err != nil {
		t.Fatalf("explicit flags must override malformed network env: %v", err)
	}
	if o.networkMode != networkModeTap || o.sshIngressIface != "flag0" {
		t.Fatalf("network flags %+v", o)
	}
	if !o.supplied["node.network-mode"] || !o.supplied["node.ssh-ingress-iface"] {
		t.Fatalf("supplied provenance %v", o.supplied)
	}
	// The env values not replaced by a flag stay malformed and fail.
	bad := append(tapArgs(), "-node.network-mode=tap")
	if _, err := parseFlags(bad, io.Discard, env, okHostname); err == nil {
		t.Fatal("a malformed network env value not overridden by a flag must fail")
	} else if !strings.Contains(err.Error(), "-node.network-resolvers") {
		t.Fatalf("error %q does not name the malformed setting", err)
	}
}

// Network flags are syntax-checked at parse time: a malformed value fails
// before networking(), a merely incomplete tap set does not.
func TestParseFlagsNetworkSyntax(t *testing.T) {
	for _, tc := range []struct {
		name string
		arg  string
		want string
	}{
		{"unknown mode", "-node.network-mode=maybe", "want \"tap\" or \"user\""},
		{"prefix without length", "-node.network-prefix=10.42.0.0", "-node.network-prefix"},
		{"IPv6 prefix", "-node.network-prefix=fd00::/64", "-node.network-prefix"},
		{"invalid prefix length", "-node.network-prefix=10.42.0.0/33", "-node.network-prefix"},
		{"gateway not an address", "-node.network-gateway=10.42.0", "-node.network-gateway"},
		{"IPv6 gateway", "-node.network-gateway=fd00::1", "-node.network-gateway"},
		{"empty resolver entry", "-node.network-resolvers=1.1.1.1,,9.9.9.9", "-node.network-resolvers"},
		{"padded resolver", "-node.network-resolvers=1.1.1.1, 9.9.9.9", "-node.network-resolvers"},
		{"hostname resolver", "-node.network-resolvers=dns.example", "-node.network-resolvers"},
		{"three resolvers", "-node.network-resolvers=1.1.1.1,9.9.9.9,8.8.8.8", "at most 2"},
		{"empty management entry", "-node.network-management=100.64.0.0/10,", "-node.network-management"},
		{"management not a prefix", "-node.network-management=tailscale0", "-node.network-management"},
		{"ingress hostname", "-node.ssh-ingress-addr=example.com", "-node.ssh-ingress-addr"},
		{"ingress IPv6", "-node.ssh-ingress-addr=fd00::1", "-node.ssh-ingress-addr"},
		{"range without maximum", "-node.ssh-port-range=22000", "is not min-max"},
		{"range with empty maximum", "-node.ssh-port-range=22000-", "is not min-max"},
		{"range with empty minimum", "-node.ssh-port-range=-22999", "is not min-max"},
		{"zero minimum", "-node.ssh-port-range=0-22999", "not a port in 1..65535"},
		{"maximum above 65535", "-node.ssh-port-range=22000-65536", "not a port in 1..65535"},
		{"descending range", "-node.ssh-port-range=22999-22000", "minimum 22999 is above maximum 22000"},
		{"padded range", "-node.ssh-port-range= 22000-22999", "not a port in 1..65535"},
		{"extra separator", "-node.ssh-port-range=22000-22999-23000", "not a port in 1..65535"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseFlags([]string{tc.arg}, io.Discard, testEnv(nil), okHostname)
			if err == nil {
				t.Fatalf("parseFlags(%q) accepted a malformed network value", tc.arg)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
	// Syntax-only completeness deferral: the defaults parse, even though
	// networking() below refuses them as an incomplete tap configuration.
	if _, err := parseFlags(nil, io.Discard, testEnv(nil), okHostname); err != nil {
		t.Fatalf("the default network syntax must parse: %v", err)
	}
}

// The zero value and the default flags are not a runnable production
// configuration: networking() fails closed and names every missing input.
func TestNetworkingFailsClosedWithoutConfig(t *testing.T) {
	o, err := parseFlags(nil, io.Discard, testEnv(nil), okHostname)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := o.networking()
	if err == nil {
		t.Fatalf("default tap configuration accepted: %+v", plan)
	}
	for _, name := range []string{
		"-node.network-prefix",
		"-node.network-gateway",
		"-node.ssh-ingress-addr",
		"-node.ssh-port-range",
	} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %s", err, name)
		}
	}
	if plan.Tap != nil || plan.Mode != "" {
		t.Errorf("a failed check returned a plan: %+v", plan)
	}

	var zero options
	if _, err := zero.networking(); err == nil {
		t.Fatal("zero options accepted")
	}
	// A tap-mode options value built by hand still requires the complete set.
	if _, err := (&options{networkMode: networkModeTap}).networking(); err == nil {
		t.Fatal("empty tap options accepted")
	}
}

func TestNetworkingCompleteTap(t *testing.T) {
	env := testEnv(map[string]string{
		"BBRAFT_NODE_SSH_INGRESS_IFACE": "example0",
	})
	args := append(tapArgs(),
		"-node.network-management=100.64.0.0/10",
		"-node.data-dir=/var/lib/example",
	)
	o, err := parseFlags(args, io.Discard, env, okHostname)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := o.networking()
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != networkModeTap {
		t.Fatalf("mode %q, want %q", plan.Mode, networkModeTap)
	}
	if plan.Tap == nil {
		t.Fatal("tap mode returned no configuration")
	}
	want := nodenet.Config{
		Bridge:  defaultNetworkBridge,
		Prefix:  netip.MustParsePrefix("10.42.0.0/16"),
		Gateway: netip.MustParseAddr("10.42.0.1"),
		Resolvers: []netip.Addr{
			netip.MustParseAddr("1.1.1.1"),
			netip.MustParseAddr("9.9.9.9"),
		},
		Management:   []netip.Prefix{netip.MustParsePrefix("100.64.0.0/10")},
		IngressAddr:  netip.MustParseAddr("203.0.113.10"),
		IngressIface: "example0",
		SSHPorts:     nodenet.PortRange{Min: 22000, Max: 22999},
		StateDir:     "/var/lib/example/net",
	}
	if !reflect.DeepEqual(*plan.Tap, want) {
		t.Fatalf("tap config\n got %+v\nwant %+v", *plan.Tap, want)
	}
	if err := plan.Tap.Validate(); err != nil {
		t.Fatalf("returned configuration does not validate: %v", err)
	}

	// The state directory always lives below the node data dir, including a
	// data dir with a trailing slash.
	o, err = parseFlags(append(tapArgs(), "-node.data-dir=/srv/bbraft/"), io.Discard, testEnv(nil), okHostname)
	if err != nil {
		t.Fatal(err)
	}
	plan, err = o.networking()
	if err != nil {
		t.Fatal(err)
	}
	if plan.Tap.StateDir != "/srv/bbraft/net" {
		t.Fatalf("StateDir %q, want /srv/bbraft/net", plan.Tap.StateDir)
	}
}

// The dev/test user mode is explicit and returns no TAP configuration.
func TestNetworkingUserMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
	}{
		{"flag", []string{"-node.network-mode=user"}, nil},
		{"env", nil, map[string]string{"BBRAFT_NODE_NETWORK_MODE": "user"}},
		{"flag beats env tap", []string{"-node.network-mode=user"}, map[string]string{"BBRAFT_NODE_NETWORK_MODE": "tap"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, err := parseFlags(tc.args, io.Discard, testEnv(tc.env), okHostname)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := o.networking()
			if err != nil {
				t.Fatal(err)
			}
			if plan.Mode != networkModeUser {
				t.Fatalf("mode %q, want %q", plan.Mode, networkModeUser)
			}
			if plan.Tap != nil {
				t.Fatalf("user mode returned a TAP configuration: %+v", plan.Tap)
			}
		})
	}

	// Built-in bridge and resolver defaults do not count as operator input.
	o, err := parseFlags(nil, io.Discard, testEnv(map[string]string{"BBRAFT_NODE_NETWORK_MODE": "user"}), okHostname)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.networking(); err != nil {
		t.Fatalf("user mode with only built-in defaults must work: %v", err)
	}
}

// User mode refuses every operator-supplied TAP or SSH setting, whether it
// came from a flag or a BBRAFT_* variable, even when the value equals the
// built-in default.
func TestNetworkingUserModeRejectsTapSettings(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"bridge flag at the default value", []string{"-node.network-mode=user", "-node.network-bridge=" + defaultNetworkBridge}, nil, "-node.network-bridge"},
		{"prefix flag", []string{"-node.network-mode=user", "-node.network-prefix=10.42.0.0/16"}, nil, "-node.network-prefix"},
		{"gateway flag", []string{"-node.network-mode=user", "-node.network-gateway=10.42.0.1"}, nil, "-node.network-gateway"},
		{"resolvers env", nil, map[string]string{
			"BBRAFT_NODE_NETWORK_MODE":      "user",
			"BBRAFT_NODE_NETWORK_RESOLVERS": defaultNetworkResolvers,
		}, "-node.network-resolvers"},
		{"management flag", []string{"-node.network-mode=user", "-node.network-management=100.64.0.0/10"}, nil, "-node.network-management"},
		{"ingress address flag", []string{"-node.network-mode=user", "-node.ssh-ingress-addr=203.0.113.10"}, nil, "-node.ssh-ingress-addr"},
		{"ingress interface env", nil, map[string]string{
			"BBRAFT_NODE_NETWORK_MODE":      "user",
			"BBRAFT_NODE_SSH_INGRESS_IFACE": "example0",
		}, "-node.ssh-ingress-iface"},
		{"port range flag", []string{"-node.network-mode=user", "-node.ssh-port-range=22000-22999"}, nil, "-node.ssh-port-range"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, err := parseFlags(tc.args, io.Discard, testEnv(tc.env), okHostname)
			if err != nil {
				t.Fatalf("parseFlags: %v", err)
			}
			plan, err := o.networking()
			if err == nil {
				t.Fatalf("user mode accepted an operator TAP setting: %+v", plan)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name %s", err, tc.want)
			}
		})
	}
}

// A complete tap set still fails closed on unsafe, narrow, overlapping or
// unrepresentable values; the errors come from the shared nodenet checks.
func TestNetworkingTapRejectsUnsafeConfig(t *testing.T) {
	for _, tc := range []struct {
		name     string
		override map[string]string
		want     string
	}{
		{"public prefix", map[string]string{"node.network-prefix": "8.8.8.0/24"}, "not a private range"},
		{"prefix too narrow", map[string]string{"node.network-prefix": "10.42.0.0/31"}, "between /8 and /30"},
		{"gateway outside the prefix", map[string]string{"node.network-gateway": "10.43.0.1"}, "is outside"},
		{"gateway is the network address", map[string]string{"node.network-gateway": "10.42.0.0"}, "is the network address of"},
		{"gateway is the broadcast address", map[string]string{"node.network-gateway": "10.42.255.255"}, "is the broadcast address of"},
		{"no resolvers", map[string]string{"node.network-resolvers": ""}, "at least one resolver"},
		{"private resolver", map[string]string{"node.network-resolvers": "1.1.1.1,10.42.0.53"}, "not a public unicast address"},
		{"management overlaps the guest prefix", map[string]string{"node.network-management": "10.42.0.0/24"}, "overlaps the guest prefix"},
		{"ingress inside the guest prefix", map[string]string{"node.ssh-ingress-addr": "10.42.0.9"}, "is inside the guest prefix"},
		{"ingress port below 1024", map[string]string{"node.ssh-port-range": "22-22999"}, "at or above 1024"},
		{"ingress interface is the bridge", map[string]string{"node.ssh-ingress-iface": defaultNetworkBridge}, "is the VM bridge"},
		{"invalid ingress interface", map[string]string{"node.ssh-ingress-iface": "bad/iface"}, "forbidden character"},
		{"reserved bridge", map[string]string{"node.network-bridge": "lo"}, "reserved"},
		{"overlong bridge", map[string]string{"node.network-bridge": "bbraft-bridge-0123"}, "longer than 15 bytes"},
		{"relative data dir", map[string]string{"node.data-dir": "var/lib/bbraft"}, "not an absolute clean path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{}, tapArgs()...)
			for name, value := range tc.override {
				args = append(args, "-"+name+"="+value)
			}
			o, err := parseFlags(args, io.Discard, testEnv(nil), okHostname)
			if err != nil {
				// Some overrides are rejected at the syntax boundary, which
				// is also a valid fail-closed result.
				if strings.Contains(err.Error(), tc.want) {
					return
				}
				t.Fatalf("parseFlags error %q does not contain %q", err, tc.want)
			}
			plan, err := o.networking()
			if err == nil {
				t.Fatalf("unsafe configuration accepted: %+v", plan)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}
