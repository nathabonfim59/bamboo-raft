package qemu

import (
	"errors"
	"net"
	"strconv"
)

// Jailed descriptor map. It extends the A8 table (TapFD, VhostFD) append-only:
// the jailed launcher inherits exactly these descriptors, in this order, as
// exec.Cmd.ExtraFiles (fd 3 onward), and the jailer appends its own helper
// descriptors after them, all closed before QEMU starts.
const (
	// KernelFD and InitrdFD are the verified image kernel and initrd, opened
	// read-only.
	KernelFD = 5
	InitrdFD = 6
	// VsockFD is a pre-opened /dev/vhost-vsock (vhostfd of vhost-vsock-pci).
	VsockFD = 7
	// BaseDiskFD is the verified standalone image root.qcow2, read-only.
	BaseDiskFD = 8
	// OverlayFD is the VM's writable overlay, read-write.
	OverlayFD = 9
)

// Jailed control paths. ControlDirName is the per-VM directory under
// Config.WorkDir that holds only QMP's socket; it is attached at /control in
// the chroot. The serial log stays Config.SerialPath() on the host and is
// attached at JailSerialPath.
const (
	ControlDirName = "control"
	JailQMPPath    = "/control/qmp.sock"
	JailSerialPath = "/serial.log"
)

// ControlQMPPath returns the host path of a jailed VM's QMP socket.
func (c Config) ControlQMPPath() string {
	return c.WorkDir + "/" + ControlDirName + "/" + QMPFilename
}

func fdArg(fd int) string { return "/proc/self/fd/" + strconv.Itoa(fd) }

// JailedArgs returns the production jailed QEMU argv without argv0: the form
// the jailer's argument validator accepts. Every host resource is one of the
// fixed inherited descriptors, the disk graph is explicit (the verified base
// as a read-only node, the overlay naming it as backing, so QEMU never opens
// a stored backing filename), the control paths are the chroot ones and the
// virtio-net option ROM is disabled: direct-kernel boot needs none and no
// distribution firmware has to be staged for it. TAP networking is required.
// Like Args, it starts QEMU paused (-S).
func (c Config) JailedArgs() ([]string, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c.Network == nil {
		return nil, errors.New("qemu: jailed launches require TAP networking")
	}
	cpu, err := c.CPU()
	if err != nil {
		return nil, err
	}
	hw, err := net.ParseMAC(c.Network.MAC)
	if err != nil {
		return nil, err
	}
	base := "driver=qcow2,node-name=base,read-only=on,file.driver=file,file.node-name=basefile," +
		"file.filename=" + fdArg(BaseDiskFD) + ",file.cache.direct=off,file.cache.no-flush=off"
	overlay := "driver=qcow2,node-name=" + rootDiskNode + ",lazy-refcounts=off,backing=base,file.driver=file," +
		"file.node-name=rootfile,file.filename=" + fdArg(OverlayFD) + ",file.cache.direct=off,file.cache.no-flush=off"
	return []string{
		"-name", "guest=" + c.SandboxID + ",process=" + c.SandboxID,
		"-machine", "q35,accel=kvm",
		"-cpu", cpu.String(),
		"-nodefaults",
		"-no-user-config",
		"-no-reboot",
		// Paused launch (D2): the node prepares the dirty bitmap over QMP
		// and only then sends cont, so no guest write predates it.
		"-S",
		"-m", strconv.FormatUint(c.MemoryMiB, 10) + "M",
		"-smp", strconv.FormatUint(uint64(c.VCPUs), 10),
		"-kernel", fdArg(KernelFD),
		"-initrd", fdArg(InitrdFD),
		"-append", c.kernelArgs(),
		"-blockdev", base,
		"-blockdev", overlay,
		"-device", "virtio-blk-pci,drive=" + rootDiskNode + ",write-cache=on,werror=stop,rerror=stop",
		"-netdev", "tap,id=net0,fd=" + strconv.Itoa(TapFD) + ",vhost=on,vhostfd=" + strconv.Itoa(VhostFD),
		"-device", "virtio-net-pci,netdev=net0,mac=" + hw.String() + ",romfile=",
		"-device", "vhost-vsock-pci,guest-cid=" + parseCID(c.VsockCID) + ",vhostfd=" + strconv.Itoa(VsockFD),
		"-object", "rng-random,filename=/dev/urandom,id=rng0",
		"-device", "virtio-rng-pci,rng=rng0",
		"-device", "virtio-balloon-pci,free-page-reporting=on",
		"-device", "vmgenid",
		"-chardev", "file,id=serial0,path=" + JailSerialPath + ",append=on",
		"-serial", "chardev:serial0",
		"-qmp", "unix:" + JailQMPPath + ",server=on,wait=off",
		"-display", "none",
		"-sandbox", sandboxPolicy,
	}, nil
}
