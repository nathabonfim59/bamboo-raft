// Configuration for bbraft-node: flags with BBRAFT_* environment fallback,
// token loading, the phase-0.5 plaintext address rule and host capacity
// detection.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	nodenet "github.com/nathabonfim59/bamboo-raft/internal/node/net"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/overcommit"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const (
	defaultServerAddr = "127.0.0.1:7701"
	defaultDataDir    = "/var/lib/bbraft"
	defaultImageDir   = "/var/lib/bbraft/images"

	defaultBootTimeout      = 2 * time.Minute
	defaultReserveVCPUs     = 1
	defaultReserveMemoryMiB = 1024
	defaultReserveDiskGiB   = 1
	defaultOvercommit       = 1.0

	// Production VM networking is always TAP on a per-node bridge; the
	// user-mode network is the explicit dev/test fallback.
	networkModeTap  = "tap"
	networkModeUser = "user"

	// defaultNetworkBridge is the per-node bridge the manager creates when
	// absent. It is a harmless built-in default: the dev/test user mode does
	// not treat it as operator-supplied configuration.
	defaultNetworkBridge = "bbraft0"

	// defaultNetworkResolvers are public unicast resolvers handed to guests;
	// the operator overrides them when the host must use different ones.
	defaultNetworkResolvers = "1.1.1.1,9.9.9.9"

	// maxGuestResolvers is QEMU's limit: the direct-boot ip= parameter takes
	// at most two IPv4 nameservers, so more are refused at the CLI boundary
	// even though nodenet.Config could carry more.
	maxGuestResolvers = 2

	// networkStateDirName is the nodenet state directory below the node data
	// dir. It holds one allocation record per sandbox and must stay on the
	// node's own filesystem.
	networkStateDirName = "net"

	// maxTokenBytes bounds a token from file or environment; real tokens
	// are short, so anything larger is a configuration mistake, not a
	// secret.
	maxTokenBytes = 8192

	// meminfoPath is the procfs file host memory is read from.
	meminfoPath = "/proc/meminfo"

	// defaultJailQEMUBinary is the Debian QEMU the jailer stages.
	defaultJailQEMUBinary = "/usr/bin/qemu-system-x86_64"
	// defaultJailToolBinary is the trusted qemu-img the jailer stages for
	// image tools (node.DefaultJailToolBinary).
	defaultJailToolBinary = "/usr/bin/qemu-img"
)

// options is the parsed command line. It never holds the token: secrets
// stay out of configuration structs so no dump can print them.
type options struct {
	serverAddr   string
	nodeID       string
	tokenFile    string
	dataDir      string
	imageDir     string
	defaultImage string
	cpuModel     string
	cpuNested    string
	cpuFeatures  []string
	cpuAllowHost bool
	bootTimeout  time.Duration

	capacityVCPUs     uint64
	capacityMemoryMiB uint64
	capacityDiskGiB   uint64
	reserveVCPUs      uint64
	reserveMemoryMiB  uint64
	reserveDiskGiB    uint64
	cpuOvercommit     float64
	memoryOvercommit  float64
	diskOvercommit    float64

	// VM networking. The mode is networkModeTap (production) or
	// networkModeUser (the explicit dev/test fallback); networking() turns
	// the rest into a validated nodenet.Config.
	networkMode       string
	networkBridge     string
	networkPrefix     string
	networkGateway    string
	networkResolvers  string
	networkManagement string
	sshIngressAddr    string
	sshIngressIface   string
	sshPortRange      string

	// Jailed QEMU (tap mode). jail() turns these into a validated plan.
	jailUIDRange    string
	jailCgroup      string
	jailQEMUBinary  string
	jailToolBinary  string
	jailFirmware    string
	jailLibraryDirs string
	jailIODevice    string
	jailIOReadBPS   uint64
	jailIOWriteBPS  uint64
	jailIOReadIOPS  uint64
	jailIOWriteIOPS uint64

	// supplied records flags set explicitly or through their BBRAFT_*
	// variable. networking() uses it to refuse operator-supplied TAP/SSH
	// settings in the dev/test user mode while letting built-in defaults
	// through.
	supplied map[string]bool
}

// envName maps a flag name to its BBRAFT_* variable: dots and dashes become
// underscores, upper-cased (parkana style).
func envName(flagName string) string {
	rep := strings.NewReplacer(".", "_", "-", "_")
	return "BBRAFT_" + strings.ToUpper(rep.Replace(flagName))
}

// parseFlags fills options from flags and environment. Flags always win: an
// env value is parsed only for a flag the command line did not set, so a
// malformed env var cannot break an explicit override.
//
// Network flags are only syntax-checked here (mode name, address and port
// range forms). Completeness — a full tap configuration or a clean dev/test
// user mode — is networking()'s job, because that is the boundary the startup
// wiring calls before the manager exists.
func parseFlags(args []string, out io.Writer, lookupEnv func(string) (string, bool), hostname func() (string, error)) (*options, error) {
	fs := flag.NewFlagSet("bbraft-node", flag.ContinueOnError)
	fs.SetOutput(out)

	var o options
	fs.StringVar(&o.serverAddr, "server.addr", defaultServerAddr,
		"control-plane node port as host:port; plaintext, so only localhost, a loopback or a private IP (env BBRAFT_SERVER_ADDR)")
	fs.StringVar(&o.nodeID, "node.id", "",
		"node id reported to the server; empty uses the hostname (env BBRAFT_NODE_ID)")
	fs.StringVar(&o.tokenFile, "node.token-file", "",
		"file holding the raw node token: a regular file, mode 0600 or stricter (env BBRAFT_NODE_TOKEN_FILE; BBRAFT_NODE_TOKEN is the fallback)")
	fs.StringVar(&o.dataDir, "node.data-dir", defaultDataDir,
		"writable node state root: lock, VM disks and control sockets (env BBRAFT_NODE_DATA_DIR)")
	fs.StringVar(&o.imageDir, "node.image-dir", defaultImageDir,
		"read-only root of verified image versions (env BBRAFT_NODE_IMAGE_DIR)")
	fs.StringVar(&o.defaultImage, "node.default-image", "",
		"image id for requests that name none; empty uses the first verified image, sorted by id (env BBRAFT_NODE_DEFAULT_IMAGE)")
	fs.StringVar(&o.cpuModel, "node.cpu-model", qemu.DefaultCPUModel,
		"fixed named QEMU CPU model; max is refused, host requires -node.cpu-allow-host (env BBRAFT_NODE_CPU_MODEL)")
	fs.StringVar(&o.cpuNested, "node.cpu-nested", "auto",
		"nested virtualization passed to guests: auto (vmx on GenuineIntel hosts, none otherwise), on (require it) or off (env BBRAFT_NODE_CPU_NESTED)")
	var cpuFeatures string
	fs.StringVar(&cpuFeatures, "node.cpu-features", "",
		"extra QEMU CPU feature toggles, comma-separated +name/-name (e.g. -vmx-apicv-vid); vmx/svm belong to -node.cpu-nested; enforce still applies (env BBRAFT_NODE_CPU_FEATURES)")
	fs.BoolVar(&o.cpuAllowHost, "node.cpu-allow-host", false,
		"dev/test opt-in: permits -node.cpu-model=host exactly (max stays refused); never enable in production (env BBRAFT_NODE_CPU_ALLOW_HOST)")
	fs.DurationVar(&o.bootTimeout, "node.boot-timeout", defaultBootTimeout,
		"bound on one VM boot, from create until the guest answers Ping (env BBRAFT_NODE_BOOT_TIMEOUT)")
	fs.Uint64Var(&o.capacityVCPUs, "node.capacity-vcpus", 0,
		"vCPUs offered to sandboxes; 0 detects host CPUs and deducts the reserve (env BBRAFT_NODE_CAPACITY_VCPUS)")
	fs.Uint64Var(&o.capacityMemoryMiB, "node.capacity-memory-mib", 0,
		"MiB of memory offered to sandboxes; 0 detects MemTotal and deducts the reserve (env BBRAFT_NODE_CAPACITY_MEMORY_MIB)")
	fs.Uint64Var(&o.capacityDiskGiB, "node.capacity-disk-gib", 0,
		"GiB of disk offered to sandboxes; 0 measures free space under the data dir and deducts the reserve (env BBRAFT_NODE_CAPACITY_DISK_GIB)")
	fs.Uint64Var(&o.reserveVCPUs, "node.reserve-vcpus", defaultReserveVCPUs,
		"vCPUs kept for the host; deducted only from detected capacity (env BBRAFT_NODE_RESERVE_VCPUS)")
	fs.Uint64Var(&o.reserveMemoryMiB, "node.reserve-memory-mib", defaultReserveMemoryMiB,
		"MiB of memory kept for the host; deducted only from detected capacity (env BBRAFT_NODE_RESERVE_MEMORY_MIB)")
	fs.Uint64Var(&o.reserveDiskGiB, "node.reserve-disk-gib", defaultReserveDiskGiB,
		"GiB of disk kept for the host; deducted only from detected capacity (env BBRAFT_NODE_RESERVE_DISK_GIB)")
	fs.Float64Var(&o.cpuOvercommit, "node.cpu-overcommit", defaultOvercommit,
		"multiplier for detected host vCPUs before the reserve; finite and >0 (env BBRAFT_NODE_CPU_OVERCOMMIT)")
	fs.Float64Var(&o.memoryOvercommit, "node.memory-overcommit", defaultOvercommit,
		"multiplier for detected host memory before the reserve; finite and >0 (env BBRAFT_NODE_MEMORY_OVERCOMMIT)")
	fs.Float64Var(&o.diskOvercommit, "node.disk-overcommit", defaultOvercommit,
		"multiplier for detected free disk (and live disk promise) before the reserve; finite and >0 (env BBRAFT_NODE_DISK_OVERCOMMIT)")
	fs.StringVar(&o.networkMode, "node.network-mode", networkModeTap,
		"VM networking: tap (production, requires a complete network configuration) or user (dev/test fallback that refuses TAP settings) (env BBRAFT_NODE_NETWORK_MODE)")
	fs.StringVar(&o.networkBridge, "node.network-bridge", defaultNetworkBridge,
		"per-node bridge carrying every VM TAP; created when absent (env BBRAFT_NODE_NETWORK_BRIDGE)")
	fs.StringVar(&o.networkPrefix, "node.network-prefix", "",
		"private IPv4 range guests are allocated from, /8 to /30; required in tap mode (env BBRAFT_NODE_NETWORK_PREFIX)")
	fs.StringVar(&o.networkGateway, "node.network-gateway", "",
		"node address inside node.network-prefix and the guests' default gateway; required in tap mode (env BBRAFT_NODE_NETWORK_GATEWAY)")
	fs.StringVar(&o.networkResolvers, "node.network-resolvers", defaultNetworkResolvers,
		"public IPv4 DNS resolvers for guests, comma-separated, at most two (QEMU ip= limit) (env BBRAFT_NODE_NETWORK_RESOLVERS)")
	fs.StringVar(&o.networkManagement, "node.network-management", "",
		"networks guests must never reach, comma-separated IPv4 prefixes (for example a tailnet) (env BBRAFT_NODE_NETWORK_MANAGEMENT)")
	fs.StringVar(&o.sshIngressAddr, "node.ssh-ingress-addr", "",
		"node address SSH ingress ports are published on; required in tap mode (env BBRAFT_NODE_SSH_INGRESS_ADDR)")
	fs.StringVar(&o.sshIngressIface, "node.ssh-ingress-iface", "",
		"host interface SSH ingress is bound to; optional here, the production role requires it (env BBRAFT_NODE_SSH_INGRESS_IFACE)")
	fs.StringVar(&o.sshPortRange, "node.ssh-port-range", "",
		"inclusive SSH ingress port pool as min-max, for example 22000-22999; required in tap mode (env BBRAFT_NODE_SSH_PORT_RANGE)")

	fs.StringVar(&o.jailUIDRange, "node.jail-uid-range", "",
		"inclusive host uid range min-max, the jailer's pool: one uid per jailed VM, probe or tool attempt; required in tap mode (env BBRAFT_NODE_JAIL_UID_RANGE)")
	fs.StringVar(&o.jailCgroup, "node.jail-cgroup", "",
		"delegated cgroup v2 directory for jailed processes: <service cgroup>/vms; required in tap mode (env BBRAFT_NODE_JAIL_CGROUP)")
	fs.StringVar(&o.jailQEMUBinary, "node.jail-qemu-binary", defaultJailQEMUBinary,
		"trusted root-owned QEMU executable staged into every jail (env BBRAFT_NODE_JAIL_QEMU_BINARY)")
	fs.StringVar(&o.jailToolBinary, "node.jail-tool-binary", defaultJailToolBinary,
		"trusted root-owned qemu-img staged into every image-tool jail (env BBRAFT_NODE_JAIL_TOOL_BINARY)")
	fs.StringVar(&o.jailFirmware, "node.jail-firmware", "",
		"comma-separated trusted firmware files staged read-only into every jail (env BBRAFT_NODE_JAIL_FIRMWARE)")
	fs.StringVar(&o.jailLibraryDirs, "node.jail-library-dirs", "",
		"comma-separated trusted directories searched for QEMU's ELF libraries; empty uses the Debian multiarch defaults (env BBRAFT_NODE_JAIL_LIBRARY_DIRS)")
	fs.StringVar(&o.jailIODevice, "node.jail-io-device", "",
		"MAJ:MIN of the whole disk holding the data and image dirs, bounded per VM by io.max; required in tap mode (env BBRAFT_NODE_JAIL_IO_DEVICE)")
	fs.Uint64Var(&o.jailIOReadBPS, "node.jail-io-read-bps", 0, "per-VM read bytes/s limit; 0 omits it (env BBRAFT_NODE_JAIL_IO_READ_BPS)")
	fs.Uint64Var(&o.jailIOWriteBPS, "node.jail-io-write-bps", 0, "per-VM write bytes/s limit; 0 omits it (env BBRAFT_NODE_JAIL_IO_WRITE_BPS)")
	fs.Uint64Var(&o.jailIOReadIOPS, "node.jail-io-read-iops", 0, "per-VM read IOPS limit; 0 omits it (env BBRAFT_NODE_JAIL_IO_READ_IOPS)")
	fs.Uint64Var(&o.jailIOWriteIOPS, "node.jail-io-write-iops", 0, "per-VM write IOPS limit; 0 omits it (env BBRAFT_NODE_JAIL_IO_WRITE_IOPS)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	supplied := make(map[string]bool, fs.NFlag())
	fs.Visit(func(f *flag.Flag) { supplied[f.Name] = true })
	var envErrs []error
	fs.VisitAll(func(f *flag.Flag) {
		if supplied[f.Name] {
			return
		}
		v, ok := lookupEnv(envName(f.Name))
		if !ok {
			return
		}
		supplied[f.Name] = true
		if err := fs.Set(f.Name, v); err != nil {
			envErrs = append(envErrs, fmt.Errorf("%s: %w", envName(f.Name), err))
		}
	})
	if err := errors.Join(envErrs...); err != nil {
		return nil, err
	}
	o.supplied = supplied

	if strings.TrimSpace(o.nodeID) == "" {
		h, err := hostname()
		if err != nil || strings.TrimSpace(h) == "" {
			return nil, errors.New("node.id is empty and the hostname is unavailable; pass -node.id or set BBRAFT_NODE_ID")
		}
		o.nodeID = h
	}
	if err := checkServerAddr(o.serverAddr); err != nil {
		return nil, err
	}
	if o.dataDir == "" {
		return nil, errors.New("node.data-dir must not be empty")
	}
	if o.imageDir == "" {
		return nil, errors.New("node.image-dir must not be empty")
	}
	if o.bootTimeout <= 0 {
		return nil, errors.New("node.boot-timeout must be positive")
	}
	if err := checkCPUModel(o.cpuModel, o.cpuAllowHost); err != nil {
		return nil, err
	}
	if _, err := qemu.ParseNestedMode(o.cpuNested); err != nil {
		return nil, err
	}
	features, err := qemu.ParseCPUFeatures(cpuFeatures)
	if err != nil {
		return nil, err
	}
	o.cpuFeatures = features
	if o.capacityVCPUs > math.MaxUint32 {
		return nil, fmt.Errorf("node.capacity-vcpus %d exceeds %d", o.capacityVCPUs, uint64(math.MaxUint32))
	}
	if o.reserveVCPUs > math.MaxUint32 {
		return nil, fmt.Errorf("node.reserve-vcpus %d exceeds %d", o.reserveVCPUs, uint64(math.MaxUint32))
	}
	for _, ratio := range []struct {
		name  string
		value float64
	}{
		{"node.cpu-overcommit", o.cpuOvercommit},
		{"node.memory-overcommit", o.memoryOvercommit},
		{"node.disk-overcommit", o.diskOvercommit},
	} {
		if err := overcommit.Validate(ratio.value); err != nil {
			return nil, fmt.Errorf("-%s: %w", ratio.name, err)
		}
	}
	if o.networkMode != networkModeTap && o.networkMode != networkModeUser {
		return nil, fmt.Errorf("node.network-mode %q: want %q or %q", o.networkMode, networkModeTap, networkModeUser)
	}
	if _, err := parseNetworkValues(&o); err != nil {
		return nil, err
	}
	return &o, nil
}

// networkValues is the parsed form of the network flags. A zero field means
// the operator did not supply a usable value; networking() decides whether
// that is complete enough for the selected mode.
type networkValues struct {
	prefix       netip.Prefix
	gateway      netip.Addr
	resolvers    []netip.Addr
	management   []netip.Prefix
	ingressAddr  netip.Addr
	ingressIface string
	sshPorts     nodenet.PortRange
}

// parseNetworkValues syntax-checks every network flag without deciding
// whether the set is complete. It is called by parseFlags (so a malformed
// value fails at startup) and by networking() (which owns completeness and
// reuses the result).
func parseNetworkValues(o *options) (networkValues, error) {
	var v networkValues
	if s := o.networkPrefix; s != "" {
		p, err := netip.ParsePrefix(s)
		if err != nil || !p.Addr().Is4() || p.Addr().Is4In6() {
			return networkValues{}, fmt.Errorf("-node.network-prefix %q is not an IPv4 prefix such as 10.42.0.0/16", s)
		}
		v.prefix = p
	}
	if s := o.networkGateway; s != "" {
		a, err := netip.ParseAddr(s)
		if err != nil || !a.Is4() || a.Is4In6() {
			return networkValues{}, fmt.Errorf("-node.network-gateway %q is not an IPv4 address", s)
		}
		v.gateway = a
	}
	resolvers, err := parseAddrList("node.network-resolvers", o.networkResolvers, maxGuestResolvers)
	if err != nil {
		return networkValues{}, err
	}
	v.resolvers = resolvers
	management, err := parsePrefixList("node.network-management", o.networkManagement)
	if err != nil {
		return networkValues{}, err
	}
	v.management = management
	if s := o.sshIngressAddr; s != "" {
		a, err := netip.ParseAddr(s)
		if err != nil || !a.Is4() || a.Is4In6() {
			return networkValues{}, fmt.Errorf("-node.ssh-ingress-addr %q is not an IPv4 address", s)
		}
		v.ingressAddr = a
	}
	v.ingressIface = o.sshIngressIface
	if s := o.sshPortRange; s != "" {
		ports, err := parsePortRange(s)
		if err != nil {
			return networkValues{}, err
		}
		v.sshPorts = ports
	}
	return v, nil
}

// parseAddrList splits a strict comma-separated IPv4 list. An empty value is
// an empty list; an empty entry, surrounding space or a non-IPv4 entry is a
// configuration error. maxLen bounds the list (QEMU's ip= parameter for
// resolvers); zero means no bound.
func parseAddrList(flagName, value string, maxLen int) ([]netip.Addr, error) {
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	if maxLen > 0 && len(parts) > maxLen {
		return nil, fmt.Errorf("-%s has %d entries; at most %d are supported", flagName, len(parts), maxLen)
	}
	out := make([]netip.Addr, 0, len(parts))
	for _, p := range parts {
		if p == "" || p != strings.TrimSpace(p) {
			return nil, fmt.Errorf("-%s %q: entries must be non-empty IPv4 addresses separated by commas without spaces", flagName, value)
		}
		a, err := netip.ParseAddr(p)
		if err != nil || !a.Is4() || a.Is4In6() {
			return nil, fmt.Errorf("-%s %q: entry %q is not an IPv4 address", flagName, value, p)
		}
		out = append(out, a)
	}
	return out, nil
}

// parsePrefixList splits a strict comma-separated IPv4 prefix list.
func parsePrefixList(flagName, value string) ([]netip.Prefix, error) {
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	out := make([]netip.Prefix, 0, len(parts))
	for _, p := range parts {
		if p == "" || p != strings.TrimSpace(p) {
			return nil, fmt.Errorf("-%s %q: entries must be non-empty IPv4 prefixes separated by commas without spaces", flagName, value)
		}
		prefix, err := netip.ParsePrefix(p)
		if err != nil || !prefix.Addr().Is4() || prefix.Addr().Is4In6() {
			return nil, fmt.Errorf("-%s %q: entry %q is not an IPv4 prefix", flagName, value, p)
		}
		out = append(out, prefix)
	}
	return out, nil
}

// parsePortRange parses an inclusive "min-max" TCP port range. Only decimal
// ports 1..65535 with min <= max are accepted; nodenet additionally refuses
// published ingress ports below 1024.
func parsePortRange(value string) (nodenet.PortRange, error) {
	lo, hi, ok := strings.Cut(value, "-")
	if !ok || lo == "" || hi == "" {
		return nodenet.PortRange{}, fmt.Errorf("-node.ssh-port-range %q is not min-max, for example 22000-22999", value)
	}
	minPort, err := strconv.ParseUint(lo, 10, 16)
	if err != nil || minPort == 0 {
		return nodenet.PortRange{}, fmt.Errorf("-node.ssh-port-range %q: minimum %q is not a port in 1..65535", value, lo)
	}
	maxPort, err := strconv.ParseUint(hi, 10, 16)
	if err != nil || maxPort == 0 {
		return nodenet.PortRange{}, fmt.Errorf("-node.ssh-port-range %q: maximum %q is not a port in 1..65535", value, hi)
	}
	if minPort > maxPort {
		return nodenet.PortRange{}, fmt.Errorf("-node.ssh-port-range %q: minimum %d is above maximum %d", value, minPort, maxPort)
	}
	return nodenet.PortRange{Min: uint16(minPort), Max: uint16(maxPort)}, nil
}

// networkPlan is the resolved VM networking for one node process: either a
// complete production TAP plan or the explicit dev/test user-mode fallback.
type networkPlan struct {
	// Mode is networkModeTap or networkModeUser.
	Mode string
	// Tap is the validated production configuration; nil in user mode.
	Tap *nodenet.Config
}

// networking is the single authoritative, fail-closed check of this node's VM
// networking configuration. The startup wiring must call it before the
// manager exists.
//
// In tap mode it requires the operator's guest prefix, gateway, SSH ingress
// address and SSH port range, then validates the whole nodenet.Config,
// including the state directory below the node data dir. The ingress
// interface stays optional here; the production role requires it. In user
// mode it returns no TAP plan and refuses any operator-supplied TAP or SSH
// setting, so the dev/test fallback is never reached with production
// configuration silently ignored.
func (o *options) networking() (networkPlan, error) {
	switch o.networkMode {
	case networkModeUser:
		// A local fixed array keeps package state immutable.
		for _, name := range [...]string{
			"node.network-bridge",
			"node.network-prefix",
			"node.network-gateway",
			"node.network-resolvers",
			"node.network-management",
			"node.ssh-ingress-addr",
			"node.ssh-ingress-iface",
			"node.ssh-port-range",
		} {
			if o.supplied[name] {
				return networkPlan{}, fmt.Errorf("-%s cannot be combined with -node.network-mode=%s: the dev/test user fallback never uses TAP or SSH ingress settings", name, networkModeUser)
			}
		}
		return networkPlan{Mode: networkModeUser}, nil
	case networkModeTap:
	default:
		return networkPlan{}, fmt.Errorf("node.network-mode %q: want %q or %q", o.networkMode, networkModeTap, networkModeUser)
	}

	var missing []string
	if o.networkPrefix == "" {
		missing = append(missing, "-node.network-prefix")
	}
	if o.networkGateway == "" {
		missing = append(missing, "-node.network-gateway")
	}
	if o.sshIngressAddr == "" {
		missing = append(missing, "-node.ssh-ingress-addr")
	}
	if o.sshPortRange == "" {
		missing = append(missing, "-node.ssh-port-range")
	}
	if len(missing) > 0 {
		return networkPlan{}, fmt.Errorf("node.network-mode=tap requires %s; pass -node.network-mode=user only for unprivileged dev/test runs", strings.Join(missing, ", "))
	}
	values, err := parseNetworkValues(o)
	if err != nil {
		return networkPlan{}, err
	}
	cfg := nodenet.Config{
		Bridge:       o.networkBridge,
		Prefix:       values.prefix,
		Gateway:      values.gateway,
		Resolvers:    values.resolvers,
		Management:   values.management,
		IngressAddr:  values.ingressAddr,
		IngressIface: values.ingressIface,
		SSHPorts:     values.sshPorts,
		StateDir:     filepath.Join(o.dataDir, networkStateDirName),
	}
	if err := cfg.Validate(); err != nil {
		return networkPlan{}, fmt.Errorf("node networking: %w", err)
	}
	return networkPlan{Mode: networkModeTap, Tap: &cfg}, nil
}

// checkServerAddr enforces the phase-0.5 transport rule: the link is plain
// TCP, so it must stay on loopback or a private network.
func checkServerAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("server.addr %q: %w", addr, err)
	}
	if host == "" {
		return fmt.Errorf("server.addr %q: a host is required; the wildcard address is not a usable server address", addr)
	}
	p, err := strconv.ParseUint(port, 10, 16)
	if err != nil || p == 0 {
		return fmt.Errorf("server.addr %q: port %q is not a number in 1..65535", addr, port)
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("server.addr %q: host %q is not a literal IP; plaintext links only allow localhost, a loopback or a private IP", addr, host)
	}
	ip = ip.Unmap() // ::ffff:8.8.8.8 must not smuggle a public address past the check
	if ip.IsUnspecified() {
		return fmt.Errorf("server.addr %q: the wildcard address is not a usable server address", addr)
	}
	if !ip.IsLoopback() && !ip.IsPrivate() {
		return fmt.Errorf("server.addr %q: host %s is not loopback or private; plaintext links must stay on loopback or a private network", addr, ip)
	}
	return nil
}

// checkCPUModel accepts only fixed named models: "host" and "max" track the
// machine and would break memory snapshots and cluster moves.
// allowHost is the dev/test opt-in: it lifts the refusal for exactly "host"
// (the shared qemu.ResolveTestCPU policy); "max" stays refused either way.
func checkCPUModel(s string, allowHost bool) error {
	if s == "" {
		return errors.New("node.cpu-model must not be empty")
	}
	if !(allowHost && s == "host") && (strings.EqualFold(s, "host") || strings.EqualFold(s, "max")) {
		return fmt.Errorf("node.cpu-model %q is host-dependent; use a fixed named model", s)
	}
	if len(s) > 64 {
		return fmt.Errorf("node.cpu-model %q is too long", s)
	}
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '+', c == '-':
		default:
			return fmt.Errorf("node.cpu-model %q contains invalid character %q", s, c)
		}
	}
	return nil
}

// hostFacts is what the host can offer before the reserve.
type hostFacts struct {
	vcpus     uint64
	memoryMiB uint64
	diskGiB   uint64
}

// probeHostFacts reads CPU count, total memory and free space under the
// data dir.
func probeHostFacts(dataDir string) (hostFacts, error) {
	mem, err := memTotalMiB(meminfoPath)
	if err != nil {
		return hostFacts{}, err
	}
	disk, err := diskFreeGiB(dataDir)
	if err != nil {
		return hostFacts{}, err
	}
	return hostFacts{
		vcpus:     uint64(max(runtime.NumCPU(), 0)),
		memoryMiB: mem,
		diskGiB:   disk,
	}, nil
}

// memTotalMiB parses MemTotal from a procfs meminfo file.
func memTotalMiB(path string) (uint64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", path, err)
	}
	for line := range strings.Lines(string(b)) {
		v, ok := strings.CutPrefix(line, "MemTotal:")
		if !ok {
			continue
		}
		fields := strings.Fields(v)
		if len(fields) != 2 || fields[1] != "kB" {
			return 0, fmt.Errorf("%s: malformed MemTotal line %q", path, line)
		}
		kb, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%s: MemTotal: %w", path, err)
		}
		return kb / 1024, nil
	}
	return 0, fmt.Errorf("%s: no MemTotal line", path)
}

// diskFreeGiB measures available space on the filesystem holding dir. If
// dir does not exist yet, its deepest existing ancestor stands in for it.
func diskFreeGiB(dir string) (uint64, error) {
	p := filepath.Clean(dir)
	for {
		var st syscall.Statfs_t
		err := syscall.Statfs(p, &st)
		if err == nil {
			return statfsAvailGiB(&st, p)
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return 0, fmt.Errorf("statfs %s: %w", p, err)
		}
		parent := filepath.Dir(p)
		if parent == p {
			return 0, fmt.Errorf("statfs %s: no existing ancestor", dir)
		}
		p = parent
	}
}

// statfsAvailGiB converts Bavail, the blocks usable by unprivileged
// processes, into GiB with checked arithmetic.
func statfsAvailGiB(st *syscall.Statfs_t, p string) (uint64, error) {
	if st.Bsize <= 0 {
		return 0, fmt.Errorf("statfs %s: invalid block size %d", p, st.Bsize)
	}
	bsize := uint64(st.Bsize)
	if st.Bavail > math.MaxUint64/bsize {
		return 0, fmt.Errorf("statfs %s: free space overflows uint64", p)
	}
	return st.Bavail * bsize / (1 << 30), nil
}

// resolveCapacity computes the offer advertised in Info: explicit capacity
// flags are checked against the detected host (no reserve is deducted from
// them); each zero field detects the host and deducts only that resource's
// reserve.
func resolveCapacity(o *options, f hostFacts) (*nodev1.Resources, error) {
	vcpus, err := offerResource(o.capacityVCPUs, o.reserveVCPUs, f.vcpus, effectiveRatio(o.cpuOvercommit),
		"vcpus", "node.capacity-vcpus", "node.reserve-vcpus", "node.cpu-overcommit")
	if err != nil {
		return nil, err
	}
	if vcpus > math.MaxUint32 {
		return nil, fmt.Errorf("node.capacity-vcpus %d exceeds %d", vcpus, uint64(math.MaxUint32))
	}
	memory, err := offerResource(o.capacityMemoryMiB, o.reserveMemoryMiB, f.memoryMiB, effectiveRatio(o.memoryOvercommit),
		"MiB of memory", "node.capacity-memory-mib", "node.reserve-memory-mib", "node.memory-overcommit")
	if err != nil {
		return nil, err
	}
	if memory > math.MaxUint64>>20 {
		return nil, fmt.Errorf("node.capacity-memory-mib %d overflows byte conversion", memory)
	}
	disk, err := offerResource(o.capacityDiskGiB, o.reserveDiskGiB, f.diskGiB, effectiveRatio(o.diskOvercommit),
		"GiB of disk", "node.capacity-disk-gib", "node.reserve-disk-gib", "node.disk-overcommit")
	if err != nil {
		return nil, err
	}
	if disk > math.MaxUint64>>30 {
		return nil, fmt.Errorf("node.capacity-disk-gib %d overflows byte conversion", disk)
	}
	return &nodev1.Resources{Vcpus: uint32(vcpus), MemoryMib: memory, DiskGib: disk}, nil
}

// effectiveRatio preserves resolveCapacity's zero-value behavior for callers
// that construct options directly; parsed command lines always carry a
// validated ratio (explicit zero is rejected before this point).
func effectiveRatio(ratio float64) float64 {
	if ratio == 0 {
		return 1
	}
	return ratio
}

// offerResource picks one resource. Detected capacity is scaled by the exact
// shared ratio first; an explicit flag is the operator's offer and must fit
// inside that scaled promise (no reserve is deducted from explicit values). A
// zero detects the host, scales it and deducts the reserve, failing cleanly
// rather than fabricating capacity.
func offerResource(explicit, reserve, detected uint64, ratio float64, unit, capFlag, resFlag, ratioFlag string) (uint64, error) {
	if detected == 0 {
		return 0, fmt.Errorf("cannot verify host %s; cannot offer or detect -%s", unit, capFlag)
	}
	scaled, err := overcommit.Scale(detected, ratio)
	if err != nil {
		return 0, fmt.Errorf("-%s for host %s: %w", ratioFlag, unit, err)
	}
	if scaled == 0 {
		return 0, fmt.Errorf("-%s=%g scales host %d %s to zero; increase the ratio", ratioFlag, ratio, detected, unit)
	}
	if explicit != 0 {
		if explicit > scaled {
			return 0, fmt.Errorf("-%s=%d exceeds the scaled host promise of %d %s (-%s=%g of detected %d)",
				capFlag, explicit, scaled, unit, ratioFlag, ratio, detected)
		}
		return explicit, nil
	}
	if reserve >= scaled {
		return 0, fmt.Errorf("host has %d %s scaled to %d by -%s=%g but -%s=%d leaves nothing to offer; lower the reserve or set -%s",
			detected, unit, scaled, ratioFlag, ratio, resFlag, reserve, capFlag)
	}
	return scaled - reserve, nil
}

// loadToken resolves the node credential. A configured token file wins;
// BBRAFT_NODE_TOKEN is the fallback. Errors name files and variables, never
// token contents.
func loadToken(lookupEnv func(string) (string, bool), tokenFile string) (string, error) {
	if tokenFile != "" {
		return readTokenFile(tokenFile)
	}
	if v, ok := lookupEnv("BBRAFT_NODE_TOKEN"); ok {
		if len(v) > maxTokenBytes {
			return "", fmt.Errorf("BBRAFT_NODE_TOKEN exceeds %d bytes", maxTokenBytes)
		}
		if tok := strings.TrimSpace(v); tok != "" {
			return tok, nil
		}
		return "", errors.New("BBRAFT_NODE_TOKEN is set but empty; provide a token with -node.token-file")
	}
	return "", errors.New("no node token: pass -node.token-file or set BBRAFT_NODE_TOKEN_FILE / BBRAFT_NODE_TOKEN")
}

// readTokenFile reads a token from a regular file with private permissions.
// The descriptor itself is validated, not the path: O_NOFOLLOW refuses a
// symlink swapped in after any lstat, O_NONBLOCK keeps a FIFO or device
// from blocking the open, and the read is bounded.
func readTokenFile(path string) (string, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return "", fmt.Errorf("token file %s is a symlink; refusing", path)
		}
		return "", fmt.Errorf("token file %s: %w", path, err)
	}
	f := os.NewFile(uintptr(fd), path)
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("token file %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("token file %s is not a regular file", path)
	}
	if perm := info.Mode().Perm(); perm&^0o600 != 0 {
		return "", fmt.Errorf("token file %s has mode %04o; must be 0600 or stricter", path, perm)
	}
	if info.Size() > maxTokenBytes {
		return "", fmt.Errorf("token file %s is %d bytes; at most %d", path, info.Size(), maxTokenBytes)
	}
	b, err := io.ReadAll(io.LimitReader(f, maxTokenBytes+1))
	if err != nil {
		return "", fmt.Errorf("token file %s: %w", path, err)
	}
	if len(b) > maxTokenBytes {
		return "", fmt.Errorf("token file %s exceeds %d bytes", path, maxTokenBytes)
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("token file %s is empty", path)
	}
	return tok, nil
}
