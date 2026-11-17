package qemu

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestArgs(t *testing.T) {
	cfg := testConfig(t)
	argv, err := cfg.Args()
	if err != nil {
		t.Fatalf("Args: %v", err)
	}

	disk := "driver=qcow2,node-name=rootdisk,lazy-refcounts=off," +
		"file.driver=file,file.node-name=rootfile,file.filename=" + cfg.DiskPath +
		",file.cache.direct=off,file.cache.no-flush=off"
	want := []string{
		cfg.Binary,
		"-name", "guest=" + cfg.SandboxID + ",process=" + cfg.SandboxID,
		"-machine", "q35,accel=kvm",
		"-cpu", "Haswell-noTSX,+vmx,enforce",
		"-nodefaults",
		"-no-user-config",
		"-no-reboot",
		"-m", "8192M",
		"-smp", "4",
		"-kernel", filepath.Join(cfg.ImageDir, "kernel"),
		"-initrd", filepath.Join(cfg.ImageDir, "initrd"),
		"-append", KernelArgs,
		"-blockdev", disk,
		"-device", "virtio-blk-pci,drive=rootdisk,write-cache=on,werror=stop,rerror=stop",
		"-netdev", "user,id=net0",
		"-device", "virtio-net-pci,netdev=net0",
		"-device", "vhost-vsock-pci,guest-cid=42",
		"-object", "rng-random,filename=/dev/urandom,id=rng0",
		"-device", "virtio-rng-pci,rng=rng0",
		"-device", "virtio-balloon-pci,free-page-reporting=on",
		"-device", "vmgenid",
		"-chardev", "file,id=serial0,path=" + cfg.SerialPath() + ",append=on",
		"-serial", "chardev:serial0",
		"-qmp", "unix:" + cfg.QMPPath() + ",server=on,wait=off",
		"-display", "none",
		"-sandbox", "on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny",
	}
	if !slices.Equal(argv, want) {
		t.Fatalf("Args mismatch\n got: %q\nwant: %q", argv, want)
	}
}

func TestArgsTapNetwork(t *testing.T) {
	cfg := testConfig(t)
	cfg.Network = testNetwork(t)
	cfg.Network.MAC = "52:54:00:AA:BB:CC"
	argv, err := cfg.Args()
	if err != nil {
		t.Fatalf("Args: %v", err)
	}
	var netdevs, nics []string
	for i := range len(argv) - 1 {
		switch argv[i] {
		case "-netdev":
			netdevs = append(netdevs, argv[i+1])
		case "-device":
			if strings.HasPrefix(argv[i+1], "virtio-net-pci") {
				nics = append(nics, argv[i+1])
			}
		}
	}
	if len(netdevs) != 1 {
		t.Fatalf("argv has %d -netdev values, want exactly one: %q", len(netdevs), argv)
	}
	if want := "tap,id=net0,fd=3,vhost=on,vhostfd=4"; netdevs[0] != want {
		t.Fatalf("-netdev = %q, want %q", netdevs[0], want)
	}
	if len(nics) != 1 {
		t.Fatalf("argv has %d virtio-net devices, want exactly one: %q", len(nics), argv)
	}
	if want := "virtio-net-pci,netdev=net0,mac=52:54:00:aa:bb:cc"; nics[0] != want {
		t.Fatalf("virtio-net device = %q, want %q", nics[0], want)
	}
	if slices.Contains(argv, "user,id=net0") {
		t.Fatalf("TAP mode kept the user-mode netdev: %q", argv)
	}
}

func TestArgsTapNetworkWithHostname(t *testing.T) {
	cfg := testConfig(t)
	cfg.Network = testNetwork(t)
	cfg.Hostname = "sandbox-1.example"
	argv, err := cfg.Args()
	if err != nil {
		t.Fatalf("Args: %v", err)
	}
	i := slices.Index(argv, "-append")
	if i < 0 || i+1 >= len(argv) {
		t.Fatalf("argv has no -append: %q", argv)
	}
	want := KernelArgs +
		" ip=10.42.0.2::10.42.0.1:255.255.0.0:::off:1.1.1.1:9.9.9.9" +
		" systemd.hostname=sandbox-1.example"
	if argv[i+1] != want {
		t.Fatalf("append = %q, want %q", argv[i+1], want)
	}
}

func TestArgsDefaults(t *testing.T) {
	cfg := testConfig(t)
	cfg.Binary = ""
	cfg.CPUModel = ""
	argv, err := cfg.Args()
	if err != nil {
		t.Fatalf("Args: %v", err)
	}
	if argv[0] != DefaultBinary {
		t.Errorf("binary = %q, want %q", argv[0], DefaultBinary)
	}
	if !slices.Contains(argv, "Haswell-noTSX,+vmx,enforce") {
		t.Errorf("argv has no default CPU model: %q", argv)
	}
}

func TestArgsIsPure(t *testing.T) {
	// Args must not need the files to exist; it only validates syntax.
	cfg := testConfig(t)
	cfg.WorkDir = "/nonexistent/work"
	cfg.ImageDir = "/nonexistent/image"
	cfg.DiskPath = "/nonexistent/disk.qcow2"
	if _, err := cfg.Args(); err != nil {
		t.Fatalf("Args with nonexistent files: %v", err)
	}

	// Repeated calls are identical and never append.
	cfg = testConfig(t)
	first, err := cfg.Args()
	if err != nil {
		t.Fatalf("Args: %v", err)
	}
	second, err := cfg.Args()
	if err != nil {
		t.Fatalf("Args: %v", err)
	}
	if !slices.Equal(first, second) {
		t.Fatalf("Args is not deterministic")
	}
}

func TestArgsHostname(t *testing.T) {
	cfg := testConfig(t)
	cfg.Hostname = "sandbox-1.example"
	argv, err := cfg.Args()
	if err != nil {
		t.Fatalf("Args: %v", err)
	}
	i := slices.Index(argv, "-append")
	if i < 0 || i+1 >= len(argv) {
		t.Fatalf("argv has no -append: %q", argv)
	}
	want := KernelArgs + " systemd.hostname=sandbox-1.example"
	if argv[i+1] != want {
		t.Fatalf("append = %q, want %q", argv[i+1], want)
	}
}

func TestArgsRejectsUnsafeInput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{name: "comma in disk", mutate: func(c *Config) { c.DiskPath = "/var/lib/x,a.qcow2" }, want: "character"},
		{name: "newline in workdir", mutate: func(c *Config) { c.WorkDir = "/var/lib/x\ny" }, want: "character"},
		{name: "relative image dir", mutate: func(c *Config) { c.ImageDir = "images" }, want: "absolute"},
		{name: "bad id", mutate: func(c *Config) { c.SandboxID = "guest,process=x" }, want: "sandbox id"},
		{name: "host cpu", mutate: func(c *Config) { c.CPUModel = "host" }, want: "host-dependent"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t)
			tt.mutate(&cfg)
			argv, err := cfg.Args()
			if err == nil {
				t.Fatalf("Args succeeded: %q", argv)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Args error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestArgsCPUStrict(t *testing.T) {
	cfg := testConfig(t)
	argv, err := cfg.Args()
	if err != nil {
		t.Fatalf("Args: %v", err)
	}
	cpu := ""
	for i := 0; i+1 < len(argv); i++ {
		if argv[i] == "-cpu" {
			if cpu != "" {
				t.Fatalf("argv has more than one -cpu: %q", argv)
			}
			cpu = argv[i+1]
		}
	}
	if cpu != "Haswell-noTSX,+vmx,enforce" {
		t.Fatalf("cpu = %q, want fixed Haswell-noTSX with vmx and enforce", cpu)
	}
}

func TestArgsIsolationFlags(t *testing.T) {
	cfg := testConfig(t)
	argv, err := cfg.Args()
	if err != nil {
		t.Fatalf("Args: %v", err)
	}
	joined := strings.Join(argv, " ")
	for _, want := range []string{
		"-machine q35,accel=kvm",
		"-cpu Haswell-noTSX,+vmx,enforce",
		"-nodefaults",
		"-no-user-config",
		"-no-reboot",
		"root=/dev/vda rw",
		"-sandbox on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny",
		"-display none",
		"virtio-blk-pci,drive=rootdisk,write-cache=on,werror=stop,rerror=stop",
		"vhost-vsock-pci,guest-cid=42",
		"file.cache.direct=off",
		"file.cache.no-flush=off",
		"lazy-refcounts=off",
		"-netdev user,id=net0",
		"virtio-rng-pci",
		"virtio-balloon-pci,free-page-reporting=on",
		"vmgenid",
		"file,id=serial0",
		"unix:" + cfg.QMPPath() + ",server=on,wait=off",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("argv is missing %q\n%s", want, joined)
		}
	}
	// The command must not use a shell: no element may be a shell command.
	for _, a := range argv {
		if a == "-c" || a == "sh" || a == "bash" {
			t.Errorf("shell-like argv element %q", a)
		}
	}
}
