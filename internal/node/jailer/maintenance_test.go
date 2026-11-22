package jailer

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// jailerMaintArgs renders the supported maintenance argv for spec: the VM
// form of jailerArgs without any -device, -netdev or -object, with -S and the
// maintenance -name marker.
func jailerMaintArgs(spec LaunchSpec, qcow []bool) []string {
	vm := jailerArgs(spec, qcow)
	out := []string{"-S"}
	for i := 0; i < len(vm); i++ {
		option := vm[i]
		if rule, _ := supportedOption(option); rule.value {
			value := vm[i+1]
			i++
			switch option {
			case "-device", "-netdev", "-object":
				continue
			case "-name":
				value = MaintenanceName(spec.SandboxID)
			}
			out = append(out, option, value)
			continue
		}
		out = append(out, option)
	}
	return out
}

// jailerMaintSpec turns jailerSpec into a valid maintenance spec: kernel and
// initrd at fds 3 and 4, then the disks; no TAP, vhost-net or vhost-vsock.
func jailerMaintSpec(t *testing.T, id string, disks ...[]byte) LaunchSpec {
	t.Helper()
	spec := jailerSpec(t, id, disks...)
	spec.Files = slices.DeleteFunc(slices.Clone(spec.Files), func(d Descriptor) bool {
		return d.Name == TAPName || d.Name == VhostNetName || d.Name == VhostVsockName
	})
	spec.Maintenance = true
	qcow, err := validateDiskChain(spec)
	if err != nil {
		t.Fatal(err)
	}
	spec.Args = jailerMaintArgs(spec, qcow)
	return spec
}

// jailerMaintFDOnly are descriptor-free specs naming only the disk chain, as
// ValidateArgv and ValidateMaintenanceArgv build them.
func jailerMaintFDOnly(maintenance bool, disks ...string) LaunchSpec {
	var files []Descriptor
	if !maintenance {
		files = []Descriptor{{Name: TAPName}, {Name: VhostNetName}}
	}
	files = append(files, Descriptor{Name: KernelName}, Descriptor{Name: InitrdName})
	if !maintenance {
		files = append(files, Descriptor{Name: VhostVsockName})
	}
	for _, d := range disks {
		files = append(files, Descriptor{Name: d})
	}
	return LaunchSpec{SandboxID: jailerArgvID, Files: files, Disks: disks, Maintenance: maintenance}
}

func TestValidateMaintenanceArgs(t *testing.T) {
	spec := jailerMaintSpec(t, jailerArgvID)
	chain := jailerMaintSpec(t, jailerArgvID, jailerQCOW("", 0), jailerQCOW("stored/base", 0))
	base := spec.Args
	vm := jailerSpec(t, jailerArgvID)
	tests := []struct {
		name   string
		spec   *LaunchSpec
		args   []string
		reason string
	}{
		{name: "supported raw form", args: base},
		{name: "supported qcow2 chain", spec: &chain, args: chain.Args},
		{name: "without append", args: jailerArgvRemove(base, "-append")},

		{name: "missing -S", args: jailerArgvRemove(base, "-S"), reason: "-S is missing"},
		{name: "-S twice", args: append(slices.Clone(base), "-S"), reason: "more than once"},
		{name: "netdev", args: append(slices.Clone(base), "-netdev", argNetdev), reason: "-netdev is not allowed"},
		{name: "virtio-blk", args: append(slices.Clone(base), "-device", "virtio-blk-pci,drive=disk0"), reason: "-device is not allowed"},
		{name: "virtio-net", args: append(slices.Clone(base), "-device", "virtio-net-pci,netdev=net0,romfile="), reason: "-device is not allowed"},
		{name: "vhost-vsock", args: append(slices.Clone(base), "-device", "vhost-vsock-pci,guest-cid=3,vhostfd=7"), reason: "-device is not allowed"},
		{name: "rng", args: append(slices.Clone(base), "-object", argObject, "-device", "virtio-rng-pci,rng=rng0"), reason: "-device is not allowed"},
		{name: "rng object alone", args: append(slices.Clone(base), "-object", argObject), reason: "-object is not allowed"},
		{name: "balloon", args: append(slices.Clone(base), "-device", "virtio-balloon-pci"), reason: "-device is not allowed"},
		{name: "vmgenid", args: append(slices.Clone(base), "-device", "vmgenid"), reason: "-device is not allowed"},
		{name: "vm name", args: jailerArgvReplace(base, "-name", 0, "guest="+jailerArgvID+",process="+jailerArgvID), reason: "unsupported -name value"},
		{name: "other sandbox marker", args: jailerArgvReplace(base, "-name", 0, MaintenanceName("bx_eeeeeeee")), reason: "unsupported -name value"},
		{name: "missing qmp", args: jailerArgvRemove(base, "-qmp"), reason: "-qmp is missing"},
		{name: "qmp elsewhere", args: jailerArgvReplace(base, "-qmp", 0, "unix:/control/nbd.sock,server=on,wait=off"), reason: "unsupported -qmp value"},
		{name: "missing sandbox", args: jailerArgvRemove(base, "-sandbox"), reason: "-sandbox is missing"},
		{name: "weak sandbox", args: jailerArgvReplace(base, "-sandbox", 0, "on"), reason: "unsupported -sandbox value"},
		{name: "missing kernel", args: jailerArgvRemove(base, "-kernel"), reason: "-kernel is missing"},
		{name: "missing initrd", args: jailerArgvRemove(base, "-initrd"), reason: "-initrd is missing"},
		{name: "missing memory", args: jailerArgvRemove(base, "-m"), reason: "-m is missing"},
		{name: "missing smp", args: jailerArgvRemove(base, "-smp"), reason: "-smp is missing"},
		{name: "missing blockdev", args: jailerArgvRemove(base, "-blockdev"), reason: "-blockdev is missing"},
		{name: "kernel at the VM fd", args: jailerArgvReplace(base, "-kernel", 0, "/proc/self/fd/5"), reason: "unsupported -kernel value"},
		{name: "blockdev host filename", args: jailerArgvReplace(base, "-blockdev", 0,
			"driver=raw,node-name=disk0,file.driver=file,file.node-name=file0,file.filename=/etc/shadow"), reason: "file.filename"},
		{name: "chain stored backing name", spec: &chain, args: jailerArgvReplace(chain.Args, "-blockdev", 1,
			"driver=qcow2,node-name=disk1,file.driver=file,file.node-name=file1,file.filename=/proc/self/fd/6,backing=stored/base"), reason: "backing"},
		{name: "unsupported option", args: append(slices.Clone(base), "-monitor", "stdio"), reason: "unsupported QEMU argument"},
		{name: "full VM argv", args: vm.Args, reason: "unsupported -name value"},
		{name: "full VM argv with the marker and -S", args: append(jailerArgvReplace(jailerArgs(spec, []bool{false}), "-name", 0, MaintenanceName(jailerArgvID)), "-S"), reason: "is not allowed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := spec
			if tt.spec != nil {
				s = *tt.spec
			}
			s.Args = tt.args
			qcow, err := validateDiskChain(s)
			if err != nil {
				t.Fatal(err)
			}
			err = validateArgs(s, qcow)
			if tt.reason == "" {
				if err != nil {
					t.Fatalf("supported maintenance form rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("validateArgs = %v, want %q", err, tt.reason)
			}
		})
	}
}

// TestVMSpecRefusesMaintenanceForms proves the maintenance mode does not
// loosen VM specs: a VM spec with a maintenance argv, or without a netdev,
// is still refused.
func TestVMSpecRefusesMaintenanceForms(t *testing.T) {
	vm := jailerSpec(t, jailerArgvID)
	maint := jailerMaintArgs(vm, []bool{false})
	maintVMName := jailerArgvReplace(maint, "-name", 0, "guest="+jailerArgvID+",process="+jailerArgvID)
	tests := []struct {
		name   string
		args   []string
		reason string
	}{
		{name: "VM form with -S", args: append(slices.Clone(vm.Args), "-S")},
		{name: "maintenance argv", args: maint, reason: "unsupported -name value"},
		{name: "maintenance argv with VM name", args: maintVMName, reason: "-device is missing"},
		{name: "no netdev", args: jailerArgvRemove(vm.Args, "-netdev"), reason: "-netdev is missing"},
		{name: "no virtio-blk", args: jailerArgvDrop(vm.Args, "-device", "virtio-blk-pci,drive=disk0,write-cache=on,werror=stop,rerror=stop"), reason: "virtio-blk-pci is missing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := vm
			s.Args = tt.args
			_, err := s.validate()
			if tt.reason == "" {
				if err != nil {
					t.Fatalf("VM form rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("validate = %v, want %q", err, tt.reason)
			}
		})
	}
}

func TestValidateMaintenanceFiles(t *testing.T) {
	vm := jailerSpec(t, jailerArgvID)
	tests := []struct {
		name   string
		edit   func(*LaunchSpec)
		reason string
	}{
		{name: "supported", edit: func(*LaunchSpec) {}},
		{name: "kernel not first", edit: func(s *LaunchSpec) { s.Files[0], s.Files[1] = s.Files[1], s.Files[0] }, reason: "fds 3 and 4"},
		{name: "tap first", edit: func(s *LaunchSpec) {
			s.Files = append([]Descriptor{vm.Files[0]}, s.Files...)
		}, reason: "fds 3 and 4"},
		{name: "tap appended", edit: func(s *LaunchSpec) { s.Files = append(s.Files, vm.Files[0]) }, reason: "never inherits tap"},
		{name: "vhost-net appended", edit: func(s *LaunchSpec) { s.Files = append(s.Files, vm.Files[1]) }, reason: "never inherits vhost-net"},
		{name: "vhost-vsock appended", edit: func(s *LaunchSpec) { s.Files = append(s.Files, vm.Files[4]) }, reason: "never inherits vhost-vsock"},
		{name: "undeclared descriptor", edit: func(s *LaunchSpec) {
			s.Files = append(s.Files, Descriptor{Name: "extra", File: s.Files[2].File})
		}, reason: "neither a fixed resource nor a declared disk"},
		{name: "disk is a device", edit: func(s *LaunchSpec) { s.Files[2].File = vm.Files[0].File }, reason: "not a regular file"},
		{name: "kernel writable", edit: func(s *LaunchSpec) { s.Files[0].File = s.Files[2].File }, reason: "read-only"},
		{name: "no disk descriptor", edit: func(s *LaunchSpec) { s.Files = s.Files[:2] }, reason: "descriptors are required"},
		{name: "missing disk", edit: func(s *LaunchSpec) { s.Files[2].Name = "other" }, reason: "neither a fixed resource"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := jailerMaintSpec(t, jailerArgvID)
			s.Files = slices.Clone(s.Files)
			tt.edit(&s)
			err := s.validateFiles()
			if tt.reason == "" {
				if err != nil {
					t.Fatalf("supported maintenance descriptors rejected: %v", err)
				}
				if _, err := s.validate(); err != nil {
					t.Fatalf("supported maintenance spec rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("validateFiles = %v, want %q", err, tt.reason)
			}
		})
	}
}

// TestMaintenanceArgvClassification proves the two adoption grammars are
// disjoint: a VM argv carrying -S is never classified or validated as
// maintenance, and a maintenance argv never passes the VM ValidateArgv.
func TestMaintenanceArgvClassification(t *testing.T) {
	disks := []string{"base", "overlay"}
	qcow := []bool{true, true}
	vmArgs := append(jailerArgs(jailerMaintFDOnly(false, disks...), qcow), "-S")
	maintArgs := jailerMaintArgs(jailerMaintFDOnly(true, disks...), qcow)

	if err := ValidateArgv(jailerArgvID, vmArgs, disks); err != nil {
		t.Fatalf("ValidateArgv(VM argv with -S) = %v", err)
	}
	if IsMaintenanceArgv(jailerArgvID, vmArgs) {
		t.Fatal("a VM argv with -S is classified as maintenance")
	}
	if err := ValidateMaintenanceArgv(jailerArgvID, vmArgs, disks); err == nil {
		t.Fatal("ValidateMaintenanceArgv accepted a VM argv")
	}

	if err := ValidateMaintenanceArgv(jailerArgvID, maintArgs, disks); err != nil {
		t.Fatalf("ValidateMaintenanceArgv(maintenance argv) = %v", err)
	}
	if !IsMaintenanceArgv(jailerArgvID, maintArgs) {
		t.Fatal("a maintenance argv is not classified as maintenance")
	}
	if err := ValidateArgv(jailerArgvID, maintArgs, disks); err == nil {
		t.Fatal("ValidateArgv accepted a maintenance argv")
	}
	// Even with the VM's own -name, the maintenance argv is no VM argv.
	renamed := jailerArgvReplace(maintArgs, "-name", 0, "guest="+jailerArgvID+",process="+jailerArgvID)
	if err := ValidateArgv(jailerArgvID, renamed, disks); err == nil {
		t.Fatal("ValidateArgv accepted a maintenance argv with the VM name")
	}
	if IsMaintenanceArgv("bx_eeeeeeee", maintArgs) {
		t.Fatal("another sandbox's id matches this maintenance marker")
	}
	if err := ValidateMaintenanceArgv(jailerArgvID, maintArgs, nil); err == nil {
		t.Fatal("ValidateMaintenanceArgv accepted an empty disk chain")
	}
}

// TestStartMaintenanceSkipsDeviceRoles proves Start's device check for a
// maintenance spec needs no TAP/vhost device and refuses a device descriptor.
func TestStartMaintenanceSkipsDeviceRoles(t *testing.T) {
	m := &Manager{}
	spec := jailerMaintSpec(t, jailerArgvID)
	if err := m.checkMaintenanceDevices(spec); err != nil {
		t.Fatalf("checkMaintenanceDevices(valid) = %v", err)
	}
	dev, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()
	spec.Files = slices.Clone(spec.Files)
	spec.Files[0].File = dev
	if err := m.checkMaintenanceDevices(spec); err == nil {
		t.Fatal("checkMaintenanceDevices accepted a device descriptor")
	}
}
