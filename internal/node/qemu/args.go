package qemu

import (
	"cmp"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
)

// Args returns the complete QEMU argv, including the program at index 0. It
// performs no I/O: Config.Validate rejects unsafe option values, and Start
// checks the files and the host.
//
// The CPU comes from Config.CPU (ResolveCPU, or ResolveTestCPU when the
// test-only AllowHostCPU opt-in is set): the model, an optional "+vmx" chosen
// by the nested policy and host facts, and always ",enforce". QEMU's "check"
// is on by default but only warns and filters unsupported features; "enforce"
// makes it abort with "Host doesn't support requested features" instead
// (QEMU 10.0.0 target/i386/cpu.c, x86_cpu_filter_features and
// DEFINE_PROP_BOOL("enforce")). With a single accel=kvm entry there is no TCG
// fallback: QEMU uses the next accelerator only when the list has more than
// one (qemu-options.hx, -machine accel). A host without the fixed model's
// features therefore fails the boot instead of running a VM whose CPU differs
// from Info.cpu_model.
//
// The disk is opened with the three writeback settings made explicit:
// cache.direct=off, cache.no-flush=off (flush requests are honoured) and
// lazy-refcounts=off on the qcow2 node. The virtio-blk device reports a write
// cache, so the guest sees writeback semantics. See the package documentation
// for the snapshot contract this supports.
func (c Config) Args() ([]string, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	binary := cmp.Or(c.Binary, DefaultBinary)
	kernel := filepath.Join(c.ImageDir, "kernel")
	initrd := filepath.Join(c.ImageDir, "initrd")
	cpu, err := c.CPU()
	if err != nil {
		return nil, err
	}
	netdevArgs, netDeviceArgs, err := c.networkArgs()
	if err != nil {
		return nil, err
	}

	blockdev := "driver=qcow2,node-name=" + rootDiskNode + ",lazy-refcounts=off," +
		"file.driver=file,file.node-name=rootfile,file.filename=" + c.DiskPath +
		",file.cache.direct=off,file.cache.no-flush=off"

	return []string{
		binary,
		"-name", "guest=" + c.SandboxID + ",process=" + c.SandboxID,
		"-machine", "q35,accel=kvm",
		"-cpu", cpu.String(),
		"-nodefaults",
		"-no-user-config",
		"-no-reboot",
		"-m", strconv.FormatUint(c.MemoryMiB, 10) + "M",
		"-smp", strconv.FormatUint(uint64(c.VCPUs), 10),
		"-kernel", kernel,
		"-initrd", initrd,
		"-append", c.kernelArgs(),
		"-blockdev", blockdev,
		"-device", "virtio-blk-pci,drive=" + rootDiskNode + ",write-cache=on,werror=stop,rerror=stop",
		"-netdev", netdevArgs,
		"-device", netDeviceArgs,
		"-device", "vhost-vsock-pci,guest-cid=" + parseCID(c.VsockCID),
		"-object", "rng-random,filename=/dev/urandom,id=rng0",
		"-device", "virtio-rng-pci,rng=rng0",
		"-device", "virtio-balloon-pci,free-page-reporting=on",
		"-device", "vmgenid",
		"-chardev", "file,id=serial0,path=" + c.SerialPath() + ",append=on",
		"-serial", "chardev:serial0",
		"-qmp", "unix:" + c.QMPPath() + ",server=on,wait=off",
		"-display", "none",
		"-sandbox", sandboxPolicy,
	}, nil
}

// networkArgs renders the -netdev and virtio-net arguments. TAP-mode networking
// uses the two pre-opened descriptors at the fixed numbers documented on
// Network; the user-mode fallback has no descriptors at all and is what the
// dev/test mode keeps working.
func (c Config) networkArgs() (netdev, device string, err error) {
	if c.Network == nil {
		return "user,id=net0", "virtio-net-pci,netdev=net0", nil
	}
	hw, err := net.ParseMAC(c.Network.MAC)
	if err != nil {
		return "", "", fmt.Errorf("qemu: network MAC: %w", err)
	}
	return fmt.Sprintf("tap,id=net0,fd=%d,vhost=on,vhostfd=%d", TapFD, VhostFD),
		"virtio-net-pci,netdev=net0,mac=" + hw.String(), nil
}

// sandboxPolicy enables QEMU's seccomp mode 2 filter and denies obsolete
// syscalls, privilege elevation, process spawning and scheduler changes.
const sandboxPolicy = "on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny"

// kernelArgs returns the fixed direct-boot command line plus the optional
// guest hostname and, in TAP mode, the static ip= configuration. systemd reads
// systemd.hostname= from the kernel command line before any guest unit runs, so
// no restore hook is involved.
func (c Config) kernelArgs() string {
	args := KernelArgs
	if c.Network != nil {
		args += " " + c.Network.ipArg()
	}
	if c.Hostname != "" {
		args += " systemd.hostname=" + c.Hostname
	}
	return args
}

// rootDiskNode is the QMP node name of the root qcow2 device. Pause uses it to
// find the disk's BlockInfo in query-block.
const rootDiskNode = "rootdisk"

// String renders a Config for diagnostics without any guest data. It is not a
// command line and must not be logged as one; only the sandbox id and sizes
// are useful in logs.
func (c Config) String() string {
	return fmt.Sprintf("%s %dx%dMiB cid=%d", c.SandboxID, c.VCPUs, c.MemoryMiB, c.VsockCID)
}
