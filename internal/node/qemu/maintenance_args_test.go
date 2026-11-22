package qemu

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestMaintenanceArgs pins the jailed maintenance argv: the VM's explicit
// disk graph at the fixed maintenance descriptors, paused (-S), the "-maint"
// process marker and no device, netdev or object at all.
func TestMaintenanceArgs(t *testing.T) {
	cfg := testConfig(t)
	cfg.MemoryMiB = MaintenanceMemoryMiB(cfg.MemoryMiB)
	cfg.VCPUs = 1
	argv, err := cfg.MaintenanceArgs()
	if err != nil {
		t.Fatalf("MaintenanceArgs: %v", err)
	}
	want := []string{
		"-name", "guest=" + cfg.SandboxID + ",process=" + cfg.SandboxID + "-maint",
		"-machine", "q35,accel=kvm",
		"-cpu", "Haswell-noTSX,+vmx,enforce",
		"-nodefaults",
		"-no-user-config",
		"-no-reboot",
		"-S",
		"-m", "256M",
		"-smp", "1",
		"-kernel", "/proc/self/fd/3",
		"-initrd", "/proc/self/fd/4",
		"-blockdev", "driver=qcow2,node-name=base,read-only=on,file.driver=file,file.node-name=basefile," +
			"file.filename=/proc/self/fd/5,file.cache.direct=off,file.cache.no-flush=off",
		"-blockdev", "driver=qcow2,node-name=rootdisk,lazy-refcounts=off,backing=base,file.driver=file," +
			"file.node-name=rootfile,file.filename=/proc/self/fd/6,file.cache.direct=off,file.cache.no-flush=off",
		"-chardev", "file,id=serial0,path=/serial.log,append=on",
		"-serial", "chardev:serial0",
		"-qmp", "unix:/control/qmp.sock,server=on,wait=off",
		"-display", "none",
		"-sandbox", "on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny",
	}
	if !slices.Equal(argv, want) {
		t.Fatalf("MaintenanceArgs mismatch\n got: %q\nwant: %q", argv, want)
	}
	for _, arg := range argv {
		for _, host := range []string{cfg.WorkDir, cfg.ImageDir, cfg.DiskPath} {
			if strings.Contains(arg, host) {
				t.Fatalf("maintenance argv names host path %s in %q", host, arg)
			}
		}
	}
}

// TestMaintenanceArgsIgnoreNetwork proves a Config carrying the VM's network
// still renders no NIC: the maintenance form never has one.
func TestMaintenanceArgsIgnoreNetwork(t *testing.T) {
	cfg := testConfig(t)
	cfg.Network = testNetwork(t)
	for name, render := range map[string]func() ([]string, error){
		"jailed": cfg.MaintenanceArgs,
		"plain":  cfg.PlainMaintenanceArgs,
	} {
		argv, err := render()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, forbidden := range []string{"-device", "-netdev", "-object", "-append"} {
			if slices.Contains(argv, forbidden) {
				t.Fatalf("%s maintenance argv has %s: %q", name, forbidden, argv)
			}
		}
		if !slices.Contains(argv, "-S") {
			t.Fatalf("%s maintenance argv is not paused: %q", name, argv)
		}
	}
}

// TestPlainMaintenanceArgs pins the non-jailed form: argv0, host paths and
// the overlay as one node, as Args renders a non-jailed VM's disk.
func TestPlainMaintenanceArgs(t *testing.T) {
	cfg := testConfig(t)
	cfg.MemoryMiB = 128
	cfg.VCPUs = 1
	argv, err := cfg.PlainMaintenanceArgs()
	if err != nil {
		t.Fatalf("PlainMaintenanceArgs: %v", err)
	}
	want := []string{
		cfg.Binary,
		"-name", "guest=" + cfg.SandboxID + ",process=" + cfg.SandboxID + "-maint",
		"-machine", "q35,accel=kvm",
		"-cpu", "Haswell-noTSX,+vmx,enforce",
		"-nodefaults",
		"-no-user-config",
		"-no-reboot",
		"-S",
		"-m", "128M",
		"-smp", "1",
		"-kernel", filepath.Join(cfg.ImageDir, "kernel"),
		"-initrd", filepath.Join(cfg.ImageDir, "initrd"),
		"-blockdev", "driver=qcow2,node-name=rootdisk,lazy-refcounts=off,file.driver=file,file.node-name=rootfile," +
			"file.filename=" + cfg.DiskPath + ",file.cache.direct=off,file.cache.no-flush=off",
		"-chardev", "file,id=serial0,path=" + cfg.SerialPath() + ",append=on",
		"-serial", "chardev:serial0",
		"-qmp", "unix:" + cfg.QMPPath() + ",server=on,wait=off",
		"-display", "none",
		"-sandbox", "on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny",
	}
	if !slices.Equal(argv, want) {
		t.Fatalf("PlainMaintenanceArgs mismatch\n got: %q\nwant: %q", argv, want)
	}
}

func TestMaintenanceMemoryMiB(t *testing.T) {
	for _, tt := range []struct{ vm, want uint64 }{
		{64, 64}, {255, 255}, {256, 256}, {257, 256}, {8192, 256},
	} {
		if got := MaintenanceMemoryMiB(tt.vm); got != tt.want {
			t.Errorf("MaintenanceMemoryMiB(%d) = %d, want %d", tt.vm, got, tt.want)
		}
	}
}

func TestMaintenanceArgsValidate(t *testing.T) {
	cfg := testConfig(t)
	cfg.SandboxID = "not-a-sandbox"
	if _, err := cfg.MaintenanceArgs(); err == nil {
		t.Fatal("MaintenanceArgs accepted an invalid config")
	}
	if _, err := cfg.PlainMaintenanceArgs(); err == nil {
		t.Fatal("PlainMaintenanceArgs accepted an invalid config")
	}
}
