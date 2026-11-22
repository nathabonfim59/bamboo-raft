package qemu

import (
	"cmp"
	"path/filepath"
	"strconv"
)

// Maintenance descriptor map. A maintenance launch inherits no TAP,
// vhost-net or vhost-vsock, so its fixed map is its own: the jailer's
// maintenance descriptor list is kernel, initrd, base, overlay at fd 3 on.
const (
	MaintenanceKernelFD   = 3
	MaintenanceInitrdFD   = 4
	MaintenanceBaseDiskFD = 5
	MaintenanceOverlayFD  = 6
)

// MaxMaintenanceMemoryMiB bounds a maintenance launch's -m. QEMU never runs
// the guest, so the guest RAM is never touched; the bound only caps what a
// fault could commit.
const MaxMaintenanceMemoryMiB = 256

// MaintenanceMemoryMiB is the -m of a maintenance launch of a VM with
// vmMemoryMiB: min(MaxMaintenanceMemoryMiB, the VM's memory), so a session
// never asks for more than the VM itself was admitted with.
func MaintenanceMemoryMiB(vmMemoryMiB uint64) uint64 {
	return min(MaxMaintenanceMemoryMiB, vmMemoryMiB)
}

// MaintenanceName is the -name value of a maintenance launch: the VM's own
// guest name with the process name marked "-maint". A restarted node tells a
// maintenance QEMU from the VM's normal QEMU (both start with -S) by this
// marker only; the jailer's ValidateMaintenanceArgv requires it.
func MaintenanceName(sandboxID string) string {
	return "guest=" + sandboxID + ",process=" + sandboxID + "-maint"
}

// MaintenanceArgs returns the jailed maintenance argv without argv0: the VM's
// disk graph (verified base read-only, overlay naming it as backing) at the
// fixed maintenance descriptors, QEMU paused from the start (-S, never
// continued), and no device at all: no virtio-blk (a writable NBD export of
// rootdisk conflicts with it), network, vsock, rng, balloon or vmgenid. The
// caller sizes MemoryMiB with MaintenanceMemoryMiB. It is the form the
// jailer accepts for LaunchSpec.Maintenance.
func (c Config) MaintenanceArgs() ([]string, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	cpu, err := c.CPU()
	if err != nil {
		return nil, err
	}
	base := "driver=qcow2,node-name=base,read-only=on,file.driver=file,file.node-name=basefile," +
		"file.filename=" + fdArg(MaintenanceBaseDiskFD) + ",file.cache.direct=off,file.cache.no-flush=off"
	overlay := "driver=qcow2,node-name=" + rootDiskNode + ",lazy-refcounts=off,backing=base,file.driver=file," +
		"file.node-name=rootfile,file.filename=" + fdArg(MaintenanceOverlayFD) + ",file.cache.direct=off,file.cache.no-flush=off"
	return []string{
		"-name", MaintenanceName(c.SandboxID),
		"-machine", "q35,accel=kvm",
		"-cpu", cpu.String(),
		"-nodefaults",
		"-no-user-config",
		"-no-reboot",
		"-S",
		"-m", strconv.FormatUint(c.MemoryMiB, 10) + "M",
		"-smp", strconv.FormatUint(uint64(c.VCPUs), 10),
		"-kernel", fdArg(MaintenanceKernelFD),
		"-initrd", fdArg(MaintenanceInitrdFD),
		"-blockdev", base,
		"-blockdev", overlay,
		"-chardev", "file,id=serial0,path=" + JailSerialPath + ",append=on",
		"-serial", "chardev:serial0",
		"-qmp", "unix:" + JailQMPPath + ",server=on,wait=off",
		"-display", "none",
		"-sandbox", sandboxPolicy,
	}, nil
}

// PlainMaintenanceArgs is MaintenanceArgs for the non-jailed launcher: argv0
// included, host paths as Args renders them (the overlay opens its stored
// backing like a non-jailed VM does) and no inherited descriptor. Start uses
// it for StartOptions.Maintenance.
func (c Config) PlainMaintenanceArgs() ([]string, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	cpu, err := c.CPU()
	if err != nil {
		return nil, err
	}
	blockdev := "driver=qcow2,node-name=" + rootDiskNode + ",lazy-refcounts=off," +
		"file.driver=file,file.node-name=rootfile,file.filename=" + c.DiskPath +
		",file.cache.direct=off,file.cache.no-flush=off"
	return []string{
		cmp.Or(c.Binary, DefaultBinary),
		"-name", MaintenanceName(c.SandboxID),
		"-machine", "q35,accel=kvm",
		"-cpu", cpu.String(),
		"-nodefaults",
		"-no-user-config",
		"-no-reboot",
		"-S",
		"-m", strconv.FormatUint(c.MemoryMiB, 10) + "M",
		"-smp", strconv.FormatUint(uint64(c.VCPUs), 10),
		"-kernel", filepath.Join(c.ImageDir, "kernel"),
		"-initrd", filepath.Join(c.ImageDir, "initrd"),
		"-blockdev", blockdev,
		"-chardev", "file,id=serial0,path=" + c.SerialPath() + ",append=on",
		"-serial", "chardev:serial0",
		"-qmp", "unix:" + c.QMPPath() + ",server=on,wait=off",
		"-display", "none",
		"-sandbox", sandboxPolicy,
	}, nil
}
