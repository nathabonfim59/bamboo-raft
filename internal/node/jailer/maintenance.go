package jailer

import (
	"errors"
	"fmt"
	"os"
)

// The maintenance form is a second, explicit launch form of a VM's own QEMU
// (LaunchSpec.Maintenance). It works on a disk no guest uses: QEMU starts
// paused (-S) and is never continued, has no network, vsock or other guest
// device, and serves only the explicit disk graph over QMP and its NBD
// server at /control/nbd.sock. The VM rules are not loosened for it: a VM
// spec still needs every device, a maintenance spec may hold none.
//
//	-name guest=<id>,process=<id>-maint   MaintenanceName, the fixed marker
//	-machine q35,accel=kvm
//	-cpu <model>[,<feature>...]
//	-nodefaults -no-user-config -no-reboot
//	-S                                    required
//	-m <n>M   -smp <n>
//	-kernel /proc/self/fd/<kernel>        -initrd /proc/self/fd/<initrd>
//	-append <guest kernel command line>   optional (never booted)
//	-blockdev (one per Disks entry, exactly as for a VM)
//	-chardev file,id=serial0,path=/serial.log,append=on
//	-serial chardev:serial0
//	-qmp unix:/control/qmp.sock,server=on,wait=off
//	-display none
//	-sandbox SandboxPolicy
//
// -netdev, -device and -object are refused, so virtio-blk (which would
// conflict with a writable export of the overlay), virtio-net,
// vhost-vsock, rng, balloon and vmgenid cannot appear.

// MaintenanceName returns the fixed -name value of a maintenance launch of
// sandboxID. The process= suffix is the explicit marker a restarted node
// uses to tell a maintenance QEMU from the VM's normal one (both carry -S),
// so it is never booted or adopted as the VM.
func MaintenanceName(sandboxID string) string {
	return "guest=" + sandboxID + ",process=" + sandboxID + "-maint"
}

// maintenanceRequiredOptions are the options a maintenance argv must carry
// exactly once (-blockdev at least once).
func maintenanceRequiredOptions() []string {
	return []string{
		"-name", "-machine", "-cpu", "-nodefaults", "-no-user-config", "-no-reboot", "-S",
		"-m", "-smp", "-kernel", "-initrd", "-blockdev",
		"-chardev", "-serial", "-qmp", "-display", "-sandbox",
	}
}

// checkMaintenanceOptions refuses every option the maintenance form forbids.
// counts are the occurrences validateArgs saw.
func checkMaintenanceOptions(counts map[string]int) error {
	for _, option := range []string{"-netdev", "-device", "-object"} {
		if counts[option] != 0 {
			return fmt.Errorf("jailer: QEMU option %s is not allowed in a maintenance launch", option)
		}
	}
	return nil
}

// validateMaintenanceFiles checks a maintenance spec's descriptors: kernel
// and initrd at fds 3 and 4, then the Disks entries, and nothing else. No
// TAP, vhost-net or vhost-vsock descriptor may be inherited.
func (s LaunchSpec) validateMaintenanceFiles() error {
	if len(s.Files) < 3 {
		return errors.New("jailer: kernel, initrd and disk descriptors are required")
	}
	if len(s.Files) > maxDescriptors {
		return fmt.Errorf("jailer: %d descriptors exceed %d", len(s.Files), maxDescriptors)
	}
	if s.Files[0].Name != KernelName || s.Files[1].Name != InitrdName {
		return fmt.Errorf("jailer: first maintenance descriptors must be %s and %s at fds 3 and 4", KernelName, InitrdName)
	}
	for _, name := range []string{TAPName, VhostNetName, VhostVsockName} {
		if s.descriptor(name) != nil || s.fdOf(name) >= 0 {
			return fmt.Errorf("jailer: a maintenance launch never inherits %s", name)
		}
	}
	return s.checkDescriptors(map[string]descriptorKind{
		KernelName: {readOnly: true},
		InitrdName: {readOnly: true},
	})
}

// ValidateMaintenanceArgv checks a maintenance argv (without argv0) against
// exactly the maintenance form Start accepts for the fixed descriptor order
// kernel, initrd, then the qcow2 disks named by disks (oldest first). A
// restarted node uses it to prove a recovered maintenance QEMU before it
// quits it. It never accepts a VM argv, and ValidateArgv never accepts a
// maintenance argv.
func ValidateMaintenanceArgv(sandboxID string, args []string, disks []string) error {
	if len(disks) == 0 {
		return errors.New("jailer: disk chain is empty")
	}
	files := []Descriptor{{Name: KernelName}, {Name: InitrdName}}
	for _, name := range disks {
		files = append(files, Descriptor{Name: name})
	}
	spec := LaunchSpec{SandboxID: sandboxID, Args: args, Files: files, Disks: disks, Maintenance: true}
	qcow := make([]bool, len(disks))
	for i := range qcow {
		qcow[i] = true
	}
	return validateArgs(spec, qcow)
}

// IsMaintenanceArgv reports whether argv (without argv0) names sandboxID's
// maintenance marker (MaintenanceName as the -name value). It is only the
// classification a restarted node needs before choosing a recovery path; the
// full proof is ValidateMaintenanceArgv.
func IsMaintenanceArgv(sandboxID string, args []string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "-name" {
			return args[i+1] == MaintenanceName(sandboxID)
		}
	}
	return false
}

// checkMaintenanceDevices is checkDeviceRoles for a maintenance spec: it
// holds no device descriptor, which validateMaintenanceFiles already proved,
// so there is nothing to compare against the host devices.
func (m *Manager) checkMaintenanceDevices(spec LaunchSpec) error {
	for _, d := range spec.Files {
		info, err := d.File.Stat()
		if err != nil {
			return fmt.Errorf("jailer: descriptor %q: %w", d.Name, err)
		}
		if info.Mode()&os.ModeDevice != 0 {
			return fmt.Errorf("jailer: maintenance descriptor %q is a device", d.Name)
		}
	}
	return nil
}
