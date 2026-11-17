package qemu

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	dir := t.TempDir()
	work := filepath.Join(dir, "work")
	image := filepath.Join(dir, "images", "ubuntu-24.04-v0")
	if err := os.MkdirAll(work, 0o755); err != nil {
		t.Fatalf("mkdir work: %v", err)
	}
	if err := os.MkdirAll(image, 0o755); err != nil {
		t.Fatalf("mkdir image: %v", err)
	}
	for _, name := range []string{"kernel", "initrd"} {
		if err := os.WriteFile(filepath.Join(image, name), []byte("test"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	disk := filepath.Join(work, "root.qcow2")
	if err := os.WriteFile(disk, []byte("test"), 0o600); err != nil {
		t.Fatalf("write disk: %v", err)
	}
	return Config{
		Binary:    os.Args[0],
		SandboxID: "bx_f7k2q9hd",
		WorkDir:   work,
		ImageDir:  image,
		DiskPath:  disk,
		VCPUs:     4,
		MemoryMiB: 8 << 10,
		VsockCID:  42,
		Nested:    NestedAuto,
		HostCPU:   &HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 8},
	}
}

// testNetwork returns a valid TAP-mode NIC with two fresh open descriptors. The
// caller (or the test's cleanup) owns them; Start never closes them.
func testNetwork(t *testing.T) *Network {
	t.Helper()
	dir := t.TempDir()
	tap, err := os.CreateTemp(dir, "tap")
	if err != nil {
		t.Fatalf("create tap fixture: %v", err)
	}
	vhost, err := os.CreateTemp(dir, "vhost")
	if err != nil {
		tap.Close()
		t.Fatalf("create vhost fixture: %v", err)
	}
	t.Cleanup(func() {
		_ = tap.Close()
		_ = vhost.Close()
	})
	return &Network{
		GuestIP:   netip.MustParseAddr("10.42.0.2"),
		PrefixLen: 16,
		Gateway:   netip.MustParseAddr("10.42.0.1"),
		MAC:       "52:54:00:00:00:02",
		Resolvers: []netip.Addr{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("9.9.9.9")},
		Tap:       tap,
		Vhost:     vhost,
	}
}

func TestNetworkValidate(t *testing.T) {
	closedFile := func(t *testing.T) *os.File {
		t.Helper()
		f, err := os.CreateTemp(t.TempDir(), "closed")
		if err != nil {
			t.Fatalf("create fixture: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close fixture: %v", err)
		}
		return f
	}
	tests := []struct {
		name      string
		noNetwork bool
		mutate    func(t *testing.T, n *Network)
		want      string
	}{
		{name: "valid", mutate: func(*testing.T, *Network) {}},
		{name: "nil", noNetwork: true},
		{
			name:   "missing tap",
			mutate: func(_ *testing.T, n *Network) { n.Tap = nil },
			want:   "tap and vhost",
		},
		{
			name:   "missing vhost",
			mutate: func(_ *testing.T, n *Network) { n.Vhost = nil },
			want:   "tap and vhost",
		},
		{
			name:   "same descriptor",
			mutate: func(_ *testing.T, n *Network) { n.Vhost = n.Tap },
			want:   "same file",
		},
		{
			name: "closed tap",
			mutate: func(t *testing.T, n *Network) {
				t.Helper()
				n.Tap = closedFile(t)
			},
			want: "closed",
		},
		{
			name:   "guest loopback",
			mutate: func(_ *testing.T, n *Network) { n.GuestIP = netip.MustParseAddr("127.0.0.1") },
			want:   "unicast host address",
		},
		{
			name:   "guest unspecified",
			mutate: func(_ *testing.T, n *Network) { n.GuestIP = netip.MustParseAddr("0.0.0.0") },
			want:   "unicast host address",
		},
		{
			name:   "guest multicast",
			mutate: func(_ *testing.T, n *Network) { n.GuestIP = netip.MustParseAddr("224.0.0.1") },
			want:   "unicast host address",
		},
		{
			name:   "guest limited broadcast",
			mutate: func(_ *testing.T, n *Network) { n.GuestIP = netip.MustParseAddr("255.255.255.255") },
			want:   "unicast host address",
		},
		{
			name:   "gateway loopback",
			mutate: func(_ *testing.T, n *Network) { n.Gateway = netip.MustParseAddr("127.0.0.1") },
			want:   "unicast host address",
		},
		{
			name:   "gateway unspecified",
			mutate: func(_ *testing.T, n *Network) { n.Gateway = netip.MustParseAddr("0.0.0.0") },
			want:   "unicast host address",
		},
		{
			name:   "gateway multicast",
			mutate: func(_ *testing.T, n *Network) { n.Gateway = netip.MustParseAddr("239.1.1.1") },
			want:   "unicast host address",
		},
		{
			name: "guest this-network 0/8",
			mutate: func(_ *testing.T, n *Network) {
				n.GuestIP = netip.MustParseAddr("0.1.2.3")
				n.Gateway = netip.MustParseAddr("0.1.0.1")
				n.PrefixLen = 16
			},
			want: "guest address 0.1.2.3 is not a unicast host address",
		},
		{
			name: "gateway this-network 0/8",
			mutate: func(_ *testing.T, n *Network) {
				// A valid private guest: the error must name the gateway and
				// the unicast class, not the outside-prefix check.
				n.GuestIP = netip.MustParseAddr("10.42.0.2")
				n.Gateway = netip.MustParseAddr("0.1.0.1")
				n.PrefixLen = 16
			},
			want: "gateway 0.1.0.1 is not a unicast host address",
		},
		{
			name: "guest reserved 240/4",
			mutate: func(_ *testing.T, n *Network) {
				n.GuestIP = netip.MustParseAddr("240.0.0.2")
				n.Gateway = netip.MustParseAddr("240.0.0.1")
				n.PrefixLen = 16
			},
			want: "guest address 240.0.0.2 is not a unicast host address",
		},
		{
			name: "gateway reserved 240/4",
			mutate: func(_ *testing.T, n *Network) {
				n.GuestIP = netip.MustParseAddr("10.42.0.2")
				n.Gateway = netip.MustParseAddr("240.0.0.1")
				n.PrefixLen = 16
			},
			want: "gateway 240.0.0.1 is not a unicast host address",
		},
		{
			name:   "guest ipv6",
			mutate: func(_ *testing.T, n *Network) { n.GuestIP = netip.MustParseAddr("fd00::2") },
			want:   "guest address",
		},
		{
			name:   "guest ipv4-mapped",
			mutate: func(_ *testing.T, n *Network) { n.GuestIP = netip.MustParseAddr("::ffff:10.42.0.2") },
			want:   "guest address",
		},
		{
			name:   "prefix too small",
			mutate: func(_ *testing.T, n *Network) { n.PrefixLen = 7 },
			want:   "prefix length",
		},
		{
			name:   "prefix too large",
			mutate: func(_ *testing.T, n *Network) { n.PrefixLen = 31 },
			want:   "prefix length",
		},
		{
			name:   "gateway outside",
			mutate: func(_ *testing.T, n *Network) { n.Gateway = netip.MustParseAddr("10.43.0.1") },
			want:   "outside",
		},
		{
			name:   "guest is network address",
			mutate: func(_ *testing.T, n *Network) { n.GuestIP = netip.MustParseAddr("10.42.0.0") },
			want:   "network or broadcast",
		},
		{
			name:   "guest is broadcast address",
			mutate: func(_ *testing.T, n *Network) { n.GuestIP = netip.MustParseAddr("10.42.255.255") },
			want:   "network or broadcast",
		},
		{
			name:   "gateway is network address",
			mutate: func(_ *testing.T, n *Network) { n.Gateway = netip.MustParseAddr("10.42.0.0") },
			want:   "network or broadcast",
		},
		{
			name:   "gateway equals guest",
			mutate: func(_ *testing.T, n *Network) { n.Gateway = n.GuestIP },
			want:   "equals the guest",
		},
		{
			name:   "missing mac",
			mutate: func(_ *testing.T, n *Network) { n.MAC = "" },
			want:   "MAC",
		},
		{
			name:   "multicast mac",
			mutate: func(_ *testing.T, n *Network) { n.MAC = "01:00:5e:00:00:01" },
			want:   "unicast",
		},
		{
			name:   "zero mac",
			mutate: func(_ *testing.T, n *Network) { n.MAC = "00:00:00:00:00:00" },
			want:   "unicast",
		},
		{
			name:   "eight-byte mac",
			mutate: func(_ *testing.T, n *Network) { n.MAC = "52:54:00:00:00:00:00:02" },
			want:   "MAC",
		},
		{
			name: "three resolvers",
			mutate: func(_ *testing.T, n *Network) {
				n.Resolvers = append(n.Resolvers, netip.MustParseAddr("8.8.8.8"))
			},
			want: "at most 2",
		},
		{
			name: "ipv6 resolver",
			mutate: func(_ *testing.T, n *Network) {
				n.Resolvers = []netip.Addr{netip.MustParseAddr("2606:4700:4700::1111")}
			},
			want: "resolver",
		},
		{
			name:   "empty resolver",
			mutate: func(_ *testing.T, n *Network) { n.Resolvers = []netip.Addr{{}} },
			want:   "resolver",
		},
		{
			name:   "resolver this-network 0/8",
			mutate: func(_ *testing.T, n *Network) { n.Resolvers = []netip.Addr{netip.MustParseAddr("0.1.2.3")} },
			want:   "resolver",
		},
		{
			name:   "resolver reserved 240/4",
			mutate: func(_ *testing.T, n *Network) { n.Resolvers = []netip.Addr{netip.MustParseAddr("240.0.0.2")} },
			want:   "resolver",
		},
		{
			name:   "resolver inside guest subnet",
			mutate: func(_ *testing.T, n *Network) { n.Resolvers = []netip.Addr{netip.MustParseAddr("10.42.0.53")} },
			want:   "resolver",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Network = testNetwork(t)
			if tt.noNetwork {
				cfg.Network = nil
			} else {
				tt.mutate(t, cfg.Network)
			}
			err := cfg.Validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestUsableHostAddr(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want bool
	}{
		{name: "private guest", addr: "10.42.0.2", want: true},
		{name: "private gateway", addr: "192.168.0.1", want: true},
		{name: "this-network 0/8", addr: "0.1.2.3", want: false},
		{name: "unspecified", addr: "0.0.0.0", want: false},
		{name: "loopback", addr: "127.0.0.1", want: false},
		{name: "multicast", addr: "224.0.0.1", want: false},
		{name: "reserved 240/4", addr: "240.0.0.2", want: false},
		{name: "limited broadcast", addr: "255.255.255.255", want: false},
		{name: "link-local unicast stays a unicast class", addr: "169.254.1.1", want: true},
		{name: "ipv6", addr: "fd00::2", want: false},
		{name: "ipv4-mapped", addr: "::ffff:10.42.0.2", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := usableHostAddr(netip.MustParseAddr(tt.addr)); got != tt.want {
				t.Fatalf("usableHostAddr(%s) = %t, want %t", tt.addr, got, tt.want)
			}
		})
	}
}

// TestNetworkRejectsSameUnderlyingDescriptor proves distinct os.File values
// wrapping one descriptor are refused. The alias is a shallow copy of the
// outer os.File value: it shares the original's managed *file state (Fd,
// close guard and finalizer) without adopting the numeric descriptor a second
// time, so the original's Close, registered immediately, is the only cleanup
// and the alias needs no close of its own.
func TestNetworkRejectsSameUnderlyingDescriptor(t *testing.T) {
	original, err := os.CreateTemp(t.TempDir(), "shared-fd")
	if err != nil {
		t.Fatalf("create fixture: %v", err)
	}
	t.Cleanup(func() { _ = original.Close() })
	// Copying the outer value copies only the *file pointer and the name;
	// os.File contains no lock by value, so vet is satisfied.
	alias := *original

	n := &Network{
		GuestIP:   netip.MustParseAddr("10.42.0.2"),
		PrefixLen: 16,
		Gateway:   netip.MustParseAddr("10.42.0.1"),
		MAC:       "52:54:00:00:00:02",
		Resolvers: []netip.Addr{netip.MustParseAddr("1.1.1.1")},
		Tap:       original,
		Vhost:     &alias,
	}
	err = n.Validate()
	if err == nil || !strings.Contains(err.Error(), "same descriptor") {
		t.Fatalf("Validate = %v, want same-descriptor error", err)
	}
}

func TestNetworkIPArg(t *testing.T) {
	for _, tc := range []struct {
		name      string
		resolvers []netip.Addr
		want      string
	}{
		{
			name: "two resolvers",
			resolvers: []netip.Addr{
				netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("9.9.9.9"),
			},
			want: "ip=10.42.0.2::10.42.0.1:255.255.0.0:::off:1.1.1.1:9.9.9.9",
		},
		{
			name:      "one resolver",
			resolvers: []netip.Addr{netip.MustParseAddr("1.1.1.1")},
			want:      "ip=10.42.0.2::10.42.0.1:255.255.0.0:::off:1.1.1.1",
		},
		{name: "no resolvers", want: "ip=10.42.0.2::10.42.0.1:255.255.0.0:::off"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Network = testNetwork(t)
			cfg.Network.Resolvers = tc.resolvers
			argv, err := cfg.Args()
			if err != nil {
				t.Fatalf("Args: %v", err)
			}
			appendIdx := -1
			for i := range len(argv) - 1 {
				if argv[i] == "-append" {
					appendIdx = i + 1
					break
				}
			}
			if appendIdx < 0 {
				t.Fatalf("argv has no -append: %q", argv)
			}
			if !strings.Contains(argv[appendIdx], tc.want) {
				t.Fatalf("-append = %q, want it to contain %q", argv[appendIdx], tc.want)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	valid := testConfig(t)
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{name: "valid", mutate: func(*Config) {}},
		{name: "bad sandbox id", mutate: func(c *Config) { c.SandboxID = "../escape" }, want: "sandbox id"},
		{name: "empty sandbox id", mutate: func(c *Config) { c.SandboxID = "" }, want: "sandbox id"},
		{name: "relative work dir", mutate: func(c *Config) { c.WorkDir = "work" }, want: "not absolute"},
		{name: "unclean work dir", mutate: func(c *Config) { c.WorkDir += "/" }, want: "not clean"},
		{name: "comma in work dir", mutate: func(c *Config) { c.WorkDir += "/a,b" }, want: "character"},
		{name: "newline in disk", mutate: func(c *Config) { c.DiskPath += "\n" }, want: "character"},
		{name: "NUL in image dir", mutate: func(c *Config) { c.ImageDir += "\x00" }, want: "character"},
		{name: "zero vcpus", mutate: func(c *Config) { c.VCPUs = 0 }, want: "vcpus"},
		{name: "too many vcpus", mutate: func(c *Config) { c.VCPUs = maxVCPUs + 1 }, want: "vcpus"},
		{name: "too little memory", mutate: func(c *Config) { c.MemoryMiB = minMemoryMiB - 1 }, want: "memory"},
		{name: "memory overflow", mutate: func(c *Config) { c.MemoryMiB = maxMemoryMiB + 1 }, want: "memory"},
		{name: "cid zero", mutate: func(c *Config) { c.VsockCID = 0 }, want: "vsock cid"},
		{name: "cid hypervisor", mutate: func(c *Config) { c.VsockCID = 1 }, want: "vsock cid"},
		{name: "cid host", mutate: func(c *Config) { c.VsockCID = 2 }, want: "vsock cid"},
		{name: "cid any", mutate: func(c *Config) { c.VsockCID = 1<<32 - 1 }, want: "vsock cid"},
		{name: "comma in cpu model", mutate: func(c *Config) { c.CPUModel = "x86-64-v3,+vmx" }, want: "CPU model"},
		{name: "space in cpu model", mutate: func(c *Config) { c.CPUModel = "x86 64" }, want: "CPU model"},
		{name: "host cpu model", mutate: func(c *Config) { c.CPUModel = "host" }, want: "host-dependent"},
		{name: "host cpu model uppercase", mutate: func(c *Config) { c.CPUModel = "Host" }, want: "host-dependent"},
		{name: "max cpu model", mutate: func(c *Config) { c.CPUModel = "max" }, want: "host-dependent"},
		{name: "host allowed by test flag", mutate: func(c *Config) { c.CPUModel = "host"; c.AllowHostCPU = true }},
		{name: "max rejected even with test flag", mutate: func(c *Config) { c.CPUModel = "max"; c.AllowHostCPU = true }, want: "host-dependent"},
		{name: "host casing rejected with test flag", mutate: func(c *Config) { c.CPUModel = "Host"; c.AllowHostCPU = true }, want: "host-dependent"},
		{name: "newline in binary", mutate: func(c *Config) { c.Binary = "/bin/qemu\n" }, want: "binary"},
		{name: "hostname injection", mutate: func(c *Config) { c.Hostname = "x systemd.unit=evil" }, want: "hostname"},
		{name: "hostname comma", mutate: func(c *Config) { c.Hostname = "a,b" }, want: "hostname"},
		{name: "hostname empty label", mutate: func(c *Config) { c.Hostname = "a..b" }, want: "hostname"},
		{name: "hostname leading dash", mutate: func(c *Config) { c.Hostname = "-a" }, want: "hostname"},
		{name: "hostname trailing dot", mutate: func(c *Config) { c.Hostname = "a." }, want: "hostname"},
		{name: "hostname underscore", mutate: func(c *Config) { c.Hostname = "a_b" }, want: "hostname"},
		{name: "hostname too long", mutate: func(c *Config) { c.Hostname = strings.Repeat("a", 65) }, want: "hostname"},
		{name: "bad nested mode", mutate: func(c *Config) { c.Nested = "sometimes" }, want: "cpu-nested"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestConfigBoundsAccepted(t *testing.T) {
	cfg := testConfig(t)
	cfg.VCPUs = maxVCPUs
	cfg.MemoryMiB = maxMemoryMiB
	cfg.VsockCID = 1<<32 - 2
	cfg.Hostname = "sandbox-1.example"
	cfg.CPUModel = "Haswell-noTSX"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate at bounds: %v", err)
	}
}

func TestCheckFiles(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, c *Config)
		want   string
	}{
		{name: "valid", mutate: func(*testing.T, *Config) {}},
		{
			name: "missing work dir",
			mutate: func(t *testing.T, c *Config) {
				t.Helper()
				if err := os.RemoveAll(c.WorkDir); err != nil {
					t.Fatal(err)
				}
			},
			want: "work dir",
		},
		{
			name: "work dir is a file",
			mutate: func(t *testing.T, c *Config) {
				t.Helper()
				if err := os.RemoveAll(c.WorkDir); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(c.WorkDir, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "not a directory",
		},
		{
			name: "missing kernel",
			mutate: func(t *testing.T, c *Config) {
				t.Helper()
				if err := os.Remove(filepath.Join(c.ImageDir, "kernel")); err != nil {
					t.Fatal(err)
				}
			},
			want: "kernel",
		},
		{
			name: "symlink kernel",
			mutate: func(t *testing.T, c *Config) {
				t.Helper()
				kernel := filepath.Join(c.ImageDir, "kernel")
				if err := os.Remove(kernel); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("/bin/true", kernel); err != nil {
					t.Fatal(err)
				}
			},
			want: "symlink",
		},
		{
			name: "missing disk",
			mutate: func(t *testing.T, c *Config) {
				t.Helper()
				if err := os.Remove(c.DiskPath); err != nil {
					t.Fatal(err)
				}
			},
			want: "no such file",
		},
		{
			name: "existing qmp socket",
			mutate: func(t *testing.T, c *Config) {
				t.Helper()
				if err := os.WriteFile(c.QMPPath(), []byte("stale"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "already exists",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t)
			tt.mutate(t, &cfg)
			err := cfg.checkFiles()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("checkFiles: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("checkFiles = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestCheckFilesSocketPathTooLong(t *testing.T) {
	cfg := testConfig(t)
	long := filepath.Join(cfg.WorkDir, strings.Repeat("x", 120))
	if err := os.MkdirAll(long, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	cfg.WorkDir = long
	cfg.DiskPath = filepath.Join(long, "root.qcow2")
	if err := os.WriteFile(cfg.DiskPath, []byte("test"), 0o600); err != nil {
		t.Fatalf("write disk: %v", err)
	}
	err := cfg.checkFiles()
	if err == nil || !strings.Contains(err.Error(), "too long") {
		t.Fatalf("checkFiles = %v, want socket path length error", err)
	}
}

func TestCheckUnprivileged(t *testing.T) {
	if err := checkUnprivileged(0); err == nil {
		t.Fatal("root accepted")
	}
	if err := checkUnprivileged(1000); err != nil {
		t.Fatalf("uid 1000: %v", err)
	}
}

func TestCheckDevices(t *testing.T) {
	dir := t.TempDir()
	kvm := filepath.Join(dir, "kvm")
	vsock := filepath.Join(dir, "vsock")
	for _, p := range []string{kvm, vsock} {
		if err := os.WriteFile(p, nil, 0o600); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
	if err := checkDevices(kvm, vsock); err != nil {
		t.Fatalf("checkDevices: %v", err)
	}
	if err := checkDevices(kvm, filepath.Join(dir, "missing")); err == nil {
		t.Fatal("missing device accepted")
	}
}

func TestCheckHostObeysContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := CheckHost(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("CheckHost = %v, want canceled", err)
	}
}
