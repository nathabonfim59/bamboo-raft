package qemu

import (
	"slices"
	"strings"
	"testing"
)

// TestJailedArgs pins the production jailed argv: every host resource is a
// fixed inherited descriptor, the disk graph is explicit from the verified
// base to the overlay, the control paths are the chroot ones and the NIC
// option ROM is disabled for direct-kernel boot.
func TestJailedArgs(t *testing.T) {
	cfg := testConfig(t)
	cfg.Network = testNetwork(t)
	cfg.Network.MAC = "52:54:00:AA:BB:CC"
	argv, err := cfg.JailedArgs()
	if err != nil {
		t.Fatalf("JailedArgs: %v", err)
	}
	want := []string{
		"-name", "guest=" + cfg.SandboxID + ",process=" + cfg.SandboxID,
		"-machine", "q35,accel=kvm",
		"-cpu", "Haswell-noTSX,+vmx,enforce",
		"-nodefaults",
		"-no-user-config",
		"-no-reboot",
		"-S",
		"-m", "8192M",
		"-smp", "4",
		"-kernel", "/proc/self/fd/5",
		"-initrd", "/proc/self/fd/6",
		"-append", cfg.kernelArgs(),
		"-blockdev", "driver=qcow2,node-name=base,read-only=on,file.driver=file,file.node-name=basefile," +
			"file.filename=/proc/self/fd/8,file.cache.direct=off,file.cache.no-flush=off",
		"-blockdev", "driver=qcow2,node-name=rootdisk,lazy-refcounts=off,backing=base,file.driver=file," +
			"file.node-name=rootfile,file.filename=/proc/self/fd/9,file.cache.direct=off,file.cache.no-flush=off",
		"-device", "virtio-blk-pci,drive=rootdisk,write-cache=on,werror=stop,rerror=stop",
		"-netdev", "tap,id=net0,fd=3,vhost=on,vhostfd=4",
		"-device", "virtio-net-pci,netdev=net0,mac=52:54:00:aa:bb:cc,romfile=",
		"-device", "vhost-vsock-pci,guest-cid=42,vhostfd=7",
		"-object", "rng-random,filename=/dev/urandom,id=rng0",
		"-device", "virtio-rng-pci,rng=rng0",
		"-device", "virtio-balloon-pci,free-page-reporting=on",
		"-device", "vmgenid",
		"-chardev", "file,id=serial0,path=/serial.log,append=on",
		"-serial", "chardev:serial0",
		"-qmp", "unix:/control/qmp.sock,server=on,wait=off",
		"-display", "none",
		"-sandbox", "on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny",
	}
	if !slices.Equal(argv, want) {
		t.Fatalf("JailedArgs mismatch\n got: %q\nwant: %q", argv, want)
	}
	for _, arg := range argv {
		for _, host := range []string{cfg.WorkDir, cfg.ImageDir, cfg.DiskPath} {
			if strings.Contains(arg, host) {
				t.Fatalf("jailed argv names host path %s in %q", host, arg)
			}
		}
	}
}

func TestJailedArgsRequiresTapNetwork(t *testing.T) {
	cfg := testConfig(t)
	if _, err := cfg.JailedArgs(); err == nil || !strings.Contains(err.Error(), "TAP") {
		t.Fatalf("JailedArgs without a TAP network = %v", err)
	}
}

// TestJailedDescriptorMapIsAppendOnly: the jailed table extends A8's fixed
// TAP/vhost numbers without renumbering them.
func TestJailedDescriptorMapIsAppendOnly(t *testing.T) {
	got := []int{TapFD, VhostFD, KernelFD, InitrdFD, VsockFD, BaseDiskFD, OverlayFD}
	if want := []int{3, 4, 5, 6, 7, 8, 9}; !slices.Equal(got, want) {
		t.Fatalf("descriptor map = %v, want %v", got, want)
	}
	if ControlDirName != "control" || JailQMPPath != "/control/qmp.sock" || JailSerialPath != "/serial.log" {
		t.Fatal("jailed control paths drifted")
	}
}
