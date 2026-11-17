package jailer

import (
	"os"
	"slices"
	"strings"
	"testing"
)

const jailerArgvID = "bx_dddddddd"

// jailerArgvReplace returns args with the value following option (its
// index-th occurrence) replaced.
func jailerArgvReplace(args []string, option string, index int, value string) []string {
	out := slices.Clone(args)
	seen := 0
	for i := 0; i+1 < len(out); i++ {
		if out[i] == option {
			if seen == index {
				out[i+1] = value
				return out
			}
			seen++
		}
	}
	panic("option not found: " + option)
}

// jailerArgvDrop removes the option whose value is exactly value.
func jailerArgvDrop(args []string, option, value string) []string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == option && args[i+1] == value {
			return slices.Delete(slices.Clone(args), i, i+2)
		}
	}
	panic("option value not found: " + option + " " + value)
}

func jailerArgvRemove(args []string, option string) []string {
	i := slices.Index(args, option)
	out := slices.Clone(args)
	if r, _ := supportedOption(option); r.value {
		return slices.Delete(out, i, i+2)
	}
	return slices.Delete(out, i, i+1)
}

func TestValidateArgs(t *testing.T) {
	spec := jailerSpec(t, jailerArgvID)
	chain := jailerSpec(t, jailerArgvID, jailerQCOW("", 0), jailerQCOW("stored/base", 0))
	base := spec.Args
	tests := []struct {
		name   string
		spec   *LaunchSpec
		args   []string
		reason string
	}{
		{name: "supported raw form", args: base},
		{name: "supported qcow2 chain", spec: &chain, args: chain.Args},
		{name: "minimal optional set", args: jailerArgvDrop(jailerArgvDrop(jailerArgvDrop(jailerArgvRemove(jailerArgvRemove(
			base, "-append"), "-object"),
			"-device", "virtio-rng-pci,rng=rng0"),
			"-device", "virtio-balloon-pci,free-page-reporting=on"),
			"-device", "vmgenid")},

		{name: "positional argument", args: append(slices.Clone(base), "/etc/passwd"), reason: "unsupported QEMU argument"},
		{name: "double-dash spelling", args: append(slices.Clone(base), "--qmp", "unix:/tmp/x"), reason: "unsupported QEMU argument"},
		{name: "equals spelling", args: append(slices.Clone(base), "-qmp=unix:/tmp/x"), reason: "unsupported QEMU argument"},
		{name: "firmware path option", args: append(slices.Clone(base), "-L", "/"), reason: "unsupported QEMU argument"},
		{name: "bios option", args: append(slices.Clone(base), "-bios", "/etc/shadow"), reason: "unsupported QEMU argument"},
		{name: "readconfig", args: append(slices.Clone(base), "-readconfig", "/etc/qemu.conf"), reason: "unsupported QEMU argument"},
		{name: "monitor", args: append(slices.Clone(base), "-monitor", "stdio"), reason: "unsupported QEMU argument"},
		{name: "second qmp overrides", args: append(slices.Clone(base), "-qmp", "unix:/control/qmp.sock,server=on,wait=off"), reason: "more than once"},
		{name: "second sandbox overrides", args: append(slices.Clone(base), "-sandbox", "off"), reason: "more than once"},
		{name: "missing sandbox", args: jailerArgvRemove(base, "-sandbox"), reason: "-sandbox is missing"},
		{name: "missing no-user-config", args: jailerArgvRemove(base, "-no-user-config"), reason: "-no-user-config is missing"},
		{name: "dangling option", args: base[:len(base)-1], reason: "-sandbox has no value"},
		{name: "weak sandbox", args: jailerArgvReplace(base, "-sandbox", 0, "on"), reason: "unsupported -sandbox value"},
		{name: "qmp elsewhere", args: jailerArgvReplace(base, "-qmp", 0, "unix:/tmp/qmp.sock,server=on,wait=off"), reason: "unsupported -qmp value"},
		{name: "qmp tcp", args: jailerArgvReplace(base, "-qmp", 0, "tcp:0.0.0.0:4444,server=on,wait=off"), reason: "unsupported -qmp value"},
		{name: "serial log elsewhere", args: jailerArgvReplace(base, "-chardev", 0, "file,id=serial0,path=/control/x,append=on"), reason: "unsupported -chardev value"},
		{name: "name mismatch", args: jailerArgvReplace(base, "-name", 0, "guest=bx_other,process=bx_other"), reason: "unsupported -name value"},
		{name: "kernel path", args: jailerArgvReplace(base, "-kernel", 0, "/boot/vmlinuz"), reason: "unsupported -kernel value"},
		{name: "kernel wrong fd", args: jailerArgvReplace(base, "-kernel", 0, "/proc/self/fd/8"), reason: "unsupported -kernel value"},
		{name: "netdev script", args: jailerArgvReplace(base, "-netdev", 0, "tap,id=net0,script=/tmp/x"), reason: "unsupported -netdev value"},
		{name: "object filename", args: jailerArgvReplace(base, "-object", 0, "rng-random,filename=/dev/sda,id=rng0"), reason: "unsupported -object value"},
		{name: "cpu help", args: jailerArgvReplace(base, "-cpu", 0, "help"), reason: "unsupported -cpu value"},
		{name: "cpu path", args: jailerArgvReplace(base, "-cpu", 0, "qemu64,x=/etc"), reason: "unsupported -cpu value"},
		{name: "memory unit", args: jailerArgvReplace(base, "-m", 0, "1G"), reason: "unsupported -m value"},
		{name: "comma escape", args: jailerArgvReplace(base, "-append", 0, "a,,b"), reason: "escaped or JSON"},
		{name: "json device", args: jailerArgvReplace(base, "-device", 0, `{"driver":"virtio-blk-pci"}`), reason: "escaped or JSON"},

		{name: "blockdev host filename", args: jailerArgvReplace(base, "-blockdev", 0,
			"driver=raw,node-name=disk0,file.driver=file,file.node-name=file0,file.filename=/etc/shadow"), reason: "file.filename"},
		{name: "blockdev wrong fd", args: jailerArgvReplace(base, "-blockdev", 0,
			"driver=raw,node-name=disk0,file.driver=file,file.node-name=file0,file.filename=/proc/self/fd/5"), reason: "file.filename"},
		{name: "blockdev host driver", args: jailerArgvReplace(base, "-blockdev", 0,
			"driver=raw,node-name=disk0,file.driver=host_device,file.node-name=file0,file.filename=/proc/self/fd/8"), reason: "file.driver"},
		{name: "blockdev format mismatch", args: jailerArgvReplace(base, "-blockdev", 0,
			"driver=qcow2,node-name=disk0,file.driver=file,file.node-name=file0,file.filename=/proc/self/fd/8"), reason: "driver"},
		{name: "blockdev duplicate key", args: jailerArgvReplace(base, "-blockdev", 0,
			"driver=raw,node-name=disk0,file.driver=file,file.node-name=file0,file.filename=/proc/self/fd/8,file.filename=/etc/shadow"), reason: "repeats key"},
		{name: "blockdev unknown key", args: jailerArgvReplace(base, "-blockdev", 0,
			"driver=raw,node-name=disk0,file.driver=file,file.node-name=file0,file.filename=/proc/self/fd/8,file.x-check-cache-dropped=on"), reason: "not supported"},
		{name: "blockdev missing file node", args: jailerArgvReplace(base, "-blockdev", 0,
			"driver=raw,node-name=disk0,file.driver=file,file.filename=/proc/self/fd/8"), reason: "requires file.node-name"},
		{name: "extra blockdev", args: append(slices.Clone(base), "-blockdev",
			"driver=raw,node-name=extra,file.driver=file,file.node-name=extraf,file.filename=/proc/self/fd/8"), reason: "-blockdev options for 1 disks"},
		{name: "chain without backing", spec: &chain, args: jailerArgvReplace(chain.Args, "-blockdev", 1,
			"driver=qcow2,node-name=disk1,file.driver=file,file.node-name=file1,file.filename=/proc/self/fd/9"), reason: "requires backing"},
		{name: "chain stored backing name", spec: &chain, args: jailerArgvReplace(chain.Args, "-blockdev", 1,
			"driver=qcow2,node-name=disk1,file.driver=file,file.node-name=file1,file.filename=/proc/self/fd/9,backing=stored/base"), reason: "backing"},
		{name: "chain writable base", spec: &chain, args: jailerArgvReplace(chain.Args, "-blockdev", 0,
			"driver=qcow2,node-name=disk0,file.driver=file,file.node-name=file0,file.filename=/proc/self/fd/8"), reason: "requires read-only"},
		{name: "chain base backing", spec: &chain, args: jailerArgvReplace(chain.Args, "-blockdev", 0,
			"driver=qcow2,node-name=disk0,file.driver=file,file.node-name=file0,file.filename=/proc/self/fd/8,read-only=on,backing=disk1"), reason: "not supported"},
		{name: "duplicate node name", spec: &chain, args: jailerArgvReplace(chain.Args, "-blockdev", 1,
			"driver=qcow2,node-name=disk0,file.driver=file,file.node-name=file1,file.filename=/proc/self/fd/9,backing=disk0"), reason: "used twice"},

		{name: "device drive is base", spec: &chain, args: jailerArgvReplace(chain.Args, "-device", 0, "virtio-blk-pci,drive=disk0"), reason: "drive"},
		{name: "device romfile path", args: jailerArgvReplace(base, "-device", 1, "virtio-net-pci,netdev=net0,romfile=/etc/shadow"), reason: "romfile"},
		{name: "device unknown driver", args: jailerArgvReplace(base, "-device", 4, "vfio-pci,host=00:01.0"), reason: "unsupported -device driver"},
		{name: "device duplicate", args: append(slices.Clone(base), "-device", "vmgenid"), reason: "occurs more than once"},
		{name: "vsock wrong fd", args: jailerArgvReplace(base, "-device", 2, "vhost-vsock-pci,guest-cid=3,vhostfd=5"), reason: "vhostfd"},
		{name: "vsock reserved cid", args: jailerArgvReplace(base, "-device", 2, "vhost-vsock-pci,guest-cid=2,vhostfd=7"), reason: "guest cid"},
		{name: "rng without object", args: jailerArgvRemove(base, "-object"), reason: "used together"},
		{name: "missing vsock", args: jailerArgvDrop(base, "-device", "vhost-vsock-pci,guest-cid=3,vhostfd=7"), reason: "vhost-vsock-pci is missing"},
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
					t.Fatalf("supported form rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("validateArgs = %v, want %q", err, tt.reason)
			}
		})
	}
}

func TestValidateFiles(t *testing.T) {
	tests := []struct {
		name   string
		edit   func(*LaunchSpec)
		reason string
	}{
		{name: "tap not first", edit: func(s *LaunchSpec) { s.Files[0], s.Files[1] = s.Files[1], s.Files[0] }, reason: "fds 3 and 4"},
		{name: "undeclared descriptor", edit: func(s *LaunchSpec) {
			s.Files = append(s.Files, Descriptor{Name: "extra", File: s.Files[5].File})
		}, reason: "neither a fixed resource nor a declared disk"},
		{name: "tap is a regular file", edit: func(s *LaunchSpec) { s.Files[0].File = s.Files[2].File }, reason: "not a character device"},
		{name: "kernel is a device", edit: func(s *LaunchSpec) { s.Files[2].File = s.Files[0].File }, reason: "not a regular file"},
		{name: "kernel writable", edit: func(s *LaunchSpec) { s.Files[2].File = s.Files[5].File }, reason: "read-only"},
		{name: "disk reuses fixed name", edit: func(s *LaunchSpec) { s.Disks = []string{KernelName} }, reason: "reuses a fixed descriptor"},
		{name: "duplicate disk", edit: func(s *LaunchSpec) { s.Disks = []string{"disk0", "disk0"} }, reason: "occurs twice"},
		{name: "too few descriptors", edit: func(s *LaunchSpec) { s.Files = slices.Delete(s.Files, 4, 5) }, reason: "descriptors are required"},
		{name: "missing vsock", edit: func(s *LaunchSpec) {
			s.Files[4] = Descriptor{Name: "disk1", File: s.Files[5].File}
			s.Disks = []string{"disk0"}
		}, reason: "neither a fixed resource"},
		{name: "unsafe name", edit: func(s *LaunchSpec) { s.Files[5].Name = "a,b"; s.Disks = []string{"a,b"} }, reason: "invalid descriptor name"},
		{name: "binary under jail mount", edit: func(s *LaunchSpec) { s.Binary = "/proc/self/exe" }, reason: "collides"},
		{name: "relative binary", edit: func(s *LaunchSpec) { s.Binary = "qemu" }, reason: "absolute clean path"},
		{name: "pre-existing qmp socket", edit: func(s *LaunchSpec) {}, reason: "pre-existing QMP socket"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := jailerSpec(t, jailerArgvID)
			s.Files = slices.Clone(s.Files)
			tt.edit(&s)
			if tt.name == "pre-existing qmp socket" {
				if err := os.WriteFile(s.ControlDir.Name()+"/qmp.sock", nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			_, err := s.validate()
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("validate = %v, want %q", err, tt.reason)
			}
		})
	}
}
