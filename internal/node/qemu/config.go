package qemu

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

const (
	// DefaultBinary is the QEMU system emulator for x86-64.
	DefaultBinary = "qemu-system-x86_64"

	// TapFD and VhostFD are the descriptor numbers QEMU sees for Config.Network's
	// pre-opened TAP and /dev/vhost-net descriptors. They follow the process's
	// inherited descriptors (0, 1 and 2 are stdio), so the launcher must pass
	// Network.Tap and Network.Vhost as the first two entries of
	// exec.Cmd.ExtraFiles, in that order. The table is append-only: the jailed
	// launcher (A9) may add further resources after these two, but must never
	// renumber them, because the -netdev argument below and the QEMU side of
	// the contract are fixed by these constants.
	TapFD   = 3
	VhostFD = 4

	// DefaultCPUModel is the fixed named CPU model. It is
	// never "-cpu host", so a VM can move between cluster nodes and memory
	// snapshots have a stable feature set. Haswell-noTSX is the closest QEMU
	// named model to the x86-64-v3 psABI level (AVX2, BMI1/2, F16C, FMA,
	// MOVBE, LZCNT) with TSX disabled. Launch always appends ",enforce" so a
	// host that lacks a feature fails the boot instead of silently filtering.
	DefaultCPUModel = "Haswell-noTSX"

	// DefaultQMPTimeout bounds the wait for a fresh QEMU's QMP greeting.
	DefaultQMPTimeout = 30 * time.Second

	// DefaultStopGrace is how long Stop waits after QMP quit before killing.
	DefaultStopGrace = 10 * time.Second

	// QMPFilename is the QMP socket inside Config.WorkDir.
	QMPFilename = "qmp.sock"

	// SerialFilename is the serial console log inside Config.WorkDir.
	SerialFilename = "serial.log"

	// KVMDevice and VsockDevice are the host devices a node must be able to
	// open. CheckHost verifies access without launching QEMU.
	KVMDevice   = "/dev/kvm"
	VsockDevice = "/dev/vhost-vsock"

	// KernelArgs is the fixed direct-boot command line. Root is the single
	// virtio-blk disk. panic=-1 plus -no-reboot makes a guest panic exit
	// QEMU instead of looping, so the node sees a dead VM.
	KernelArgs = "root=/dev/vda rw console=ttyS0,115200n8 panic=-1"

	maxVCPUs       = 1024
	minMemoryMiB   = 64
	maxMemoryMiB   = 1 << 20 // 1 TiB, expressed in MiB
	cleanupTimeout = 5 * time.Second
)

// Network is one VM's TAP-mode NIC: the two host descriptors QEMU inherits and
// the static guest identity that replaces QEMU's user-mode DHCP network. A nil
// Config.Network keeps the user-mode network, which exists only as the explicit
// dev/test fallback (node.Config.AllowUserNetworking); production launches
// always set it.
//
// Ownership: the caller keeps sole ownership of Tap and Vhost. Start hands them
// to the child through exec.Cmd.ExtraFiles and never closes them, so the caller
// must Close both once Start has returned (the child owns its own references)
// and on every Start failure. The same descriptor must never be handed to two
// QEMU processes: a second TUNSETIFF on a live TAP fails with EBUSY by design.
// The NIC is single-queue (no multiqueue), matching nodenet's one TAP per VM.
type Network struct {
	// GuestIP, PrefixLen and Gateway are the guest's static IPv4 configuration.
	// They become the direct-boot ip= kernel parameter; the guest has no DHCP.
	GuestIP   netip.Addr
	PrefixLen int
	Gateway   netip.Addr
	// MAC is the guest virtio-net address, in colon-separated form. It must be
	// a unicast, non-zero EUI-48; nodenet derives it from the guest address so
	// the bridge source filter can recognize the VM.
	MAC string
	// Resolvers are the guest's DNS servers. The kernel ip= parameter carries
	// at most two, so more than two is rejected rather than silently truncated.
	// Each must be a usable unicast IPv4 address outside the guest subnet; the
	// node's network layer additionally requires public resolvers.
	Resolvers []netip.Addr
	// Tap is the pre-opened TAP descriptor ("-netdev tap,fd=3").
	Tap *os.File
	// Vhost is the pre-opened /dev/vhost-net descriptor ("vhostfd=4").
	Vhost *os.File
}

// Validate checks the NIC description without touching the files' contents. It
// requires both descriptors to be open and distinct, an IPv4 guest address and
// gateway inside a /8..30 prefix, a unicast non-zero MAC and at most two IPv4
// DNS resolvers.
func (n *Network) Validate() error {
	if n == nil {
		return nil
	}
	if n.Tap == nil || n.Vhost == nil {
		return errors.New("qemu: network requires pre-opened tap and vhost descriptors")
	}
	if n.Tap == n.Vhost {
		return errors.New("qemu: network tap and vhost descriptors are the same file")
	}
	for _, d := range []struct {
		name string
		file *os.File
	}{{"tap", n.Tap}, {"vhost", n.Vhost}} {
		if _, err := d.file.Stat(); err != nil {
			return fmt.Errorf("qemu: network %s descriptor: %w", d.name, err)
		}
	}
	// Two distinct os.File values can wrap the same descriptor; that would make
	// QEMU's fd table alias one device onto both -netdev slots.
	if n.Tap.Fd() == n.Vhost.Fd() {
		return errors.New("qemu: network tap and vhost descriptors are the same descriptor")
	}
	if !validIPv4(n.GuestIP) {
		return fmt.Errorf("qemu: network guest address %q is not an IPv4 address", n.GuestIP)
	}
	if !usableHostAddr(n.GuestIP) {
		return fmt.Errorf("qemu: network guest address %s is not a unicast host address", n.GuestIP)
	}
	if n.PrefixLen < 8 || n.PrefixLen > 30 {
		return fmt.Errorf("qemu: network prefix length %d is outside 8..30", n.PrefixLen)
	}
	if !validIPv4(n.Gateway) {
		return fmt.Errorf("qemu: network gateway %q is not an IPv4 address", n.Gateway)
	}
	if !usableHostAddr(n.Gateway) {
		return fmt.Errorf("qemu: network gateway %s is not a unicast host address", n.Gateway)
	}
	prefix := netip.PrefixFrom(n.GuestIP, n.PrefixLen).Masked()
	if !prefix.Contains(n.Gateway) {
		return fmt.Errorf("qemu: network gateway %s is outside %s", n.Gateway, prefix)
	}
	first, last := prefix.Addr(), broadcast(prefix)
	if n.GuestIP == first || n.GuestIP == last {
		return fmt.Errorf("qemu: network guest address %s is the network or broadcast address of %s", n.GuestIP, prefix)
	}
	if n.Gateway == first || n.Gateway == last {
		return fmt.Errorf("qemu: network gateway %s is the network or broadcast address of %s", n.Gateway, prefix)
	}
	if n.Gateway == n.GuestIP {
		return fmt.Errorf("qemu: network gateway %s equals the guest address", n.Gateway)
	}
	hw, err := net.ParseMAC(n.MAC)
	if err != nil {
		return fmt.Errorf("qemu: network MAC %q: %w", n.MAC, err)
	}
	if len(hw) != 6 || hw[0]&1 != 0 || slices.Equal(hw, make(net.HardwareAddr, 6)) {
		return fmt.Errorf("qemu: network MAC %q is not a unicast non-zero EUI-48", n.MAC)
	}
	if len(n.Resolvers) > 2 {
		return fmt.Errorf("qemu: network has %d resolvers; the kernel ip= parameter carries at most 2", len(n.Resolvers))
	}
	for _, r := range n.Resolvers {
		if !usableHostAddr(r) {
			return fmt.Errorf("qemu: network resolver %q is not a usable unicast IPv4 address", r)
		}
		if prefix.Contains(r) {
			return fmt.Errorf("qemu: network resolver %s is inside the guest subnet %s", r, prefix)
		}
	}
	return nil
}

// validIPv4 rejects empty, IPv6, IPv4-mapped and zoned addresses.
func validIPv4(a netip.Addr) bool {
	return a.IsValid() && a.Is4() && !a.Is4In6() && a.Zone() == ""
}

// unusableHostRanges are the IPv4 classes that can never name a guest,
// gateway or resolver: this-network/unspecified, loopback, multicast and
// reserved space (which includes the limited broadcast address). The guest
// image's static-network generator rejects the same first-octet classes, so
// the node and guest agree on what a usable unicast address is.
var unusableHostRanges = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
}

// usableHostAddr reports whether a may name a guest, its gateway or a
// resolver: a unicast IPv4 address outside the this-network, loopback,
// multicast and reserved classes.
func usableHostAddr(a netip.Addr) bool {
	if !validIPv4(a) {
		return false
	}
	for _, p := range unusableHostRanges {
		if p.Contains(a) {
			return false
		}
	}
	return true
}

// broadcast returns the all-host-bits-set address of a masked IPv4 prefix.
func broadcast(p netip.Prefix) netip.Addr {
	b := p.Addr().As4()
	for i := range 4 {
		keep := p.Bits() - 8*i
		switch {
		case keep >= 8:
		case keep <= 0:
			b[i] = 0xff
		default:
			b[i] |= byte(0xff) >> keep
		}
	}
	return netip.AddrFrom4(b)
}

// netmask renders PrefixLen as a dotted-quad netmask for the kernel ip=
// parameter, which does not accept a prefix length.
func (n *Network) netmask() string {
	return net.IP(net.CIDRMask(n.PrefixLen, 32)).String()
}

// ipArg renders the direct-boot ip= value:
//
//	ip=<guest>::<gateway>:<netmask>:::<autoconf>[:<dns0>:<dns1>]
//
// The server, hostname and device fields are deliberately empty: there is no
// boot server, systemd sets the hostname separately, and the image's
// predictable interface name is not known at launch, so the kernel configures
// the first usable device.
func (n *Network) ipArg() string {
	fields := []string{n.GuestIP.String(), "", n.Gateway.String(), n.netmask(), "", "", "off"}
	for _, r := range n.Resolvers {
		fields = append(fields, r.String())
	}
	return "ip=" + strings.Join(fields, ":")
}

// Config describes one VM's launch parameters. It contains host paths and
// sizing only: no guest environment, no credentials and no guest command.
//
// Config.Hostname is the only guest-visible value here. It becomes the
// systemd.hostname= kernel parameter; everything else is set in the guest after
// boot. QEMU only learns the sandbox id via -name.
type Config struct {
	// Binary is the QEMU executable; empty means DefaultBinary.
	Binary string
	// SandboxID is the nodev1 sandbox id, e.g. bx_f7k2q9hd.
	SandboxID string
	// WorkDir holds this VM's qmp.sock and serial.log. It must be absolute,
	// clean, exist and contain no pre-existing QMP socket.
	WorkDir string
	// ImageDir holds the verified kernel and initrd (fixed filenames).
	ImageDir string
	// DiskPath is the writable qcow2 overlay opened as the root disk.
	DiskPath string
	// VCPUs and MemoryMiB size the VM.
	VCPUs     uint32
	MemoryMiB uint64
	// VsockCID is the unique guest CID; the caller allocates it per VM.
	VsockCID uint32
	// CPUModel is the named model; empty means DefaultCPUModel.
	CPUModel string
	// Nested is the -node.cpu-nested policy; empty means NestedAuto.
	Nested NestedMode
	// CPUFeatures are validated "+name"/"-name" toggles (ParseCPUFeatures)
	// appended to the resolved model: the operator's -node.cpu-features plus
	// any nested VMX features the startup probe found missing on the host.
	CPUFeatures []string
	// HostCPU carries the host facts used by the automatic policy. Start
	// fills it with DetectHostCPU when it is nil and nested is not off.
	// Tests and other consumers may inject it; Args stays pure.
	HostCPU *HostCPU
	// AllowHostCPU is a test-only opt-in: when true and CPUModel is exactly
	// "host", Args may emit "-cpu host,enforce" for the KVM test harness or
	// image probe. Production configs (node CLI/role) never set it. max stays
	// rejected and ResolveCPU stays strict.
	AllowHostCPU bool
	// Hostname is the optional guest hostname. It is a kernel parameter
	// (systemd.hostname=...), not a QEMU device property and not a restore
	// hook; empty leaves the image's default.
	Hostname string
	// Network selects TAP-mode networking with pre-opened descriptors. nil uses
	// QEMU's user-mode network and is only valid for the explicit dev/test
	// fallback (node.Config.AllowUserNetworking).
	Network *Network
}

// Validate checks Config syntax and, when Network is set, that both inherited
// descriptors are open and distinct (one Stat on each; no other filesystem
// access). It never inspects the image, disk or work directory and never opens
// host devices: use Start for the per-VM files and socket, and CheckHost for
// the host devices and privileges.
func (c Config) Validate() error {
	if !ids.ValidSandbox(c.SandboxID) {
		return fmt.Errorf("qemu: invalid sandbox id %q", c.SandboxID)
	}
	for _, p := range []struct {
		name string
		path string
	}{
		{"work dir", c.WorkDir},
		{"image dir", c.ImageDir},
		{"disk", c.DiskPath},
	} {
		if err := checkOptionPath(p.name, p.path); err != nil {
			return err
		}
	}
	if c.VCPUs == 0 || c.VCPUs > maxVCPUs {
		return fmt.Errorf("qemu: vcpus %d out of range 1..%d", c.VCPUs, maxVCPUs)
	}
	if c.MemoryMiB < minMemoryMiB || c.MemoryMiB > maxMemoryMiB {
		return fmt.Errorf("qemu: memory %d MiB out of range %d..%d", c.MemoryMiB, minMemoryMiB, maxMemoryMiB)
	}
	if c.VsockCID < 3 || c.VsockCID >= 1<<32-1 {
		return fmt.Errorf("qemu: vsock cid %d out of range 3..%d", c.VsockCID, uint32(1<<32-2))
	}
	if c.CPUModel != "" && !validCPUModel(c.CPUModel) {
		return fmt.Errorf("qemu: invalid CPU model %q", c.CPUModel)
	}
	if c.CPUModel == "host" && c.AllowHostCPU {
		// Explicit test-only host passthrough; see ResolveTestCPU.
	} else if isHostDependentCPU(c.CPUModel) {
		return fmt.Errorf("qemu: CPU model %q is host-dependent; use a fixed named model", c.CPUModel)
	}
	if _, err := ParseNestedMode(string(c.Nested)); err != nil {
		return err
	}
	if _, err := ParseCPUFeatures(strings.Join(c.CPUFeatures, ",")); err != nil {
		return err
	}
	if !validHostname(c.Hostname) {
		return fmt.Errorf("qemu: invalid hostname %q", c.Hostname)
	}
	if err := c.Network.Validate(); err != nil {
		return err
	}
	if c.Binary != "" {
		if strings.ContainsAny(c.Binary, ",\x00\n\r\t") {
			return fmt.Errorf("qemu: invalid QEMU binary path %q", c.Binary)
		}
		if strings.Contains(c.Binary, "/") && !filepath.IsAbs(c.Binary) {
			return fmt.Errorf("qemu: QEMU binary %q is not absolute", c.Binary)
		}
	}
	return nil
}

// checkOptionPath rejects values that cannot be a safe single QEMU option
// value. QEMU's command line uses commas as option separators, so a comma in a
// path can change the meaning of an option; there is no shell involved.
func checkOptionPath(name, p string) error {
	if p == "" {
		return fmt.Errorf("qemu: %s is empty", name)
	}
	if !filepath.IsAbs(p) {
		return fmt.Errorf("qemu: %s %q is not absolute", name, p)
	}
	if filepath.Clean(p) != p {
		return fmt.Errorf("qemu: %s %q is not clean", name, p)
	}
	if strings.ContainsAny(p, ",\x00\n\r\t") {
		return fmt.Errorf("qemu: %s %q contains a character QEMU option syntax forbids", name, p)
	}
	return nil
}

func validCPUModel(s string) bool {
	for i := range len(s) {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			continue
		}
		switch c {
		case '.', '_', '+', '-':
			continue
		default:
			return false
		}
	}
	return true
}

// isHostDependentCPU rejects the QEMU models that expose the host CPU. The
// fixed named model contract keeps memory snapshots portable and
// prevents a node with a different host from silently changing the guest ABI.
func isHostDependentCPU(s string) bool {
	return strings.EqualFold(s, "host") || strings.EqualFold(s, "max")
}

// validHostname accepts an empty hostname or an RFC 1123-style name up to the
// 64-byte HOST_NAME_MAX. It is a kernel command-line value, so injection of
// spaces, commas or additional kernel parameters must be impossible.
func validHostname(s string) bool {
	if s == "" {
		return true
	}
	if len(s) > 64 || s[0] == '.' || s[0] == '-' || s[len(s)-1] == '.' || s[len(s)-1] == '-' {
		return false
	}
	for label := range strings.SplitSeq(s, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := range len(label) {
			c := label[i]
			if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' {
				continue
			}
			return false
		}
	}
	return true
}

// QMPPath returns the QMP socket path inside WorkDir.
func (c Config) QMPPath() string { return filepath.Join(c.WorkDir, QMPFilename) }

// SerialPath returns the serial console log path inside WorkDir.
func (c Config) SerialPath() string { return filepath.Join(c.WorkDir, SerialFilename) }

// CPU resolves the fixed model and nested policy against HostCPU. Args uses
// it, and nodecli/image must use it (or DetectHostCPU+ResolveCPU) rather than
// inventing their own policy.
//
// NestedOff needs no host facts. NestedAuto and NestedOn require HostCPU to be
// injected: omitting it is a caller error, not a silent "no nesting" decision.
// A non-nil HostCPU with unknown or missing facts is valid input and resolves
// conservatively.
func (c Config) CPU() (CPUOption, error) {
	mode, err := ParseNestedMode(string(c.Nested))
	if err != nil {
		return CPUOption{}, err
	}
	if mode != NestedOff && c.HostCPU == nil {
		return CPUOption{}, errors.New("qemu: host CPU facts are not injected; call DetectHostCPU and set Config.HostCPU")
	}
	var host HostCPU
	if c.HostCPU != nil {
		host = *c.HostCPU
	}
	resolve := ResolveCPU
	if c.AllowHostCPU && c.CPUModel == "host" {
		resolve = ResolveTestCPU
	}
	option, err := resolve(c.CPUModel, mode, host)
	if err != nil {
		return CPUOption{}, err
	}
	option.Features = slices.Clone(c.CPUFeatures)
	return option, nil
}

// nestedOrDefault returns the validated mode, treating empty as auto. Validate
// must have run first.
func (c Config) nestedOrDefault() NestedMode {
	mode, _ := ParseNestedMode(string(c.Nested))
	return mode
}

// checkFiles validates this VM's files and owns no pre-existing QMP socket.
// The socket check uses os.Root so the names stay inside WorkDir even if a
// component is a symlink.
func (c Config) checkFiles() error {
	info, err := os.Stat(c.WorkDir)
	if err != nil {
		return fmt.Errorf("qemu: work dir: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("qemu: work dir %q is not a directory", c.WorkDir)
	}
	for _, p := range []string{
		filepath.Join(c.ImageDir, "kernel"),
		filepath.Join(c.ImageDir, "initrd"),
		c.DiskPath,
	} {
		li, err := os.Lstat(p)
		if err != nil {
			return fmt.Errorf("qemu: %w", err)
		}
		if li.Mode()&fs.ModeSymlink != 0 {
			return fmt.Errorf("qemu: %q is a symlink, refusing an unverified file", p)
		}
		if !li.Mode().IsRegular() {
			return fmt.Errorf("qemu: %q is not a regular file", p)
		}
	}

	root, err := os.OpenRoot(c.WorkDir)
	if err != nil {
		return fmt.Errorf("qemu: open work dir: %w", err)
	}
	defer root.Close()
	// Linux sun_path is 108 bytes including the terminating NUL.
	if len(c.QMPPath()) > 107 {
		return fmt.Errorf("qemu: QMP socket path %q is too long", c.QMPPath())
	}
	if _, err := root.Lstat(QMPFilename); err == nil {
		return fmt.Errorf("qemu: %s already exists; refusing to reuse another process's socket", c.QMPPath())
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("qemu: inspect %s: %w", c.QMPPath(), err)
	}
	return nil
}

// CheckHost verifies the facilities a node needs: a non-root effective uid,
// the QEMU binary, and read-write access to the KVM and vhost-vsock devices.
// It never launches QEMU. It is meant for node startup, before any VM.
func CheckHost(ctx context.Context, log *slog.Logger) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if err := checkUnprivileged(os.Geteuid()); err != nil {
		return err
	}
	binary, err := exec.LookPath(DefaultBinary)
	if err != nil {
		return fmt.Errorf("qemu: find %s: %w", DefaultBinary, err)
	}
	if err := checkDevices(KVMDevice, VsockDevice); err != nil {
		return err
	}
	log.Debug("qemu: host ready", "binary", binary, "euid", os.Geteuid())
	return nil
}

func checkUnprivileged(uid int) error {
	if uid == 0 {
		return errors.New("qemu: refusing to run as root: QEMU must run as an unprivileged service account")
	}
	return nil
}

func checkDevices(paths ...string) error {
	for _, p := range paths {
		f, err := os.OpenFile(p, os.O_RDWR, 0)
		if err != nil {
			return fmt.Errorf("qemu: open host device %s: %w", p, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("qemu: close host device %s: %w", p, err)
		}
	}
	return nil
}

// parseCID is useful to keep the range check and the argv conversion in one
// place; it also documents that the CID never becomes a path.
func parseCID(cid uint32) string {
	return strconv.FormatUint(uint64(cid), 10)
}
