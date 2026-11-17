package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/node"
	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
)

// A10 slice4: tap-mode startup constructs and admits the jail before any
// network or manager work, in a fixed order, and hands the probe's admitted
// CPU evidence to every launch.

// jailArgs are the complete jail flags of a tap-mode fixture.
func jailArgs(dataDir, imageDir string) []string {
	return append([]string{"-node.data-dir=" + dataDir, "-node.image-dir=" + imageDir}, jailFlags()...)
}

// jailFlags are the jail flags of a tap-mode fixture with default dirs.
func jailFlags() []string {
	return []string{
		"-node.jail-uid-range=900000-900099",
		"-node.jail-cgroup=/sys/fs/cgroup/system.slice/bbraft-node.service/vms",
		"-node.jail-firmware=/usr/share/seabios/bios-256k.bin,/usr/share/qemu/linuxboot_dma.bin",
		"-node.jail-io-device=253:0",
		"-node.jail-io-read-bps=524288000",
		"-node.jail-io-write-iops=4000",
	}
}

// jailFixture wires fakeDeps for a tap-mode jailed startup. The jailer is a
// real Manager on temporary directories (New needs no privilege); the
// reclaimer is the zero value, which owns nothing; host proofs, cgroup
// mutation, admission and the probe are recording fakes.
type jailFixture struct {
	d       deps
	s       *spy
	args    []string
	jm      *jailer.Manager
	jailCfg jailer.Config
	admit   jailer.AdmitSpec
	probes  []node.JailProbeOptions
	// probeErrs is consumed per probe call.
	probeErrs []error
}

// jailEnv is the environment form of jailFlags.
func jailEnv() map[string]string {
	return map[string]string{
		"BBRAFT_NODE_JAIL_UID_RANGE":     "900000-900099",
		"BBRAFT_NODE_JAIL_CGROUP":        "/sys/fs/cgroup/system.slice/bbraft-node.service/vms",
		"BBRAFT_NODE_JAIL_FIRMWARE":      "/usr/share/seabios/bios-256k.bin,/usr/share/qemu/linuxboot_dma.bin",
		"BBRAFT_NODE_JAIL_IO_DEVICE":     "253:0",
		"BBRAFT_NODE_JAIL_IO_READ_BPS":   "524288000",
		"BBRAFT_NODE_JAIL_IO_WRITE_IOPS": "4000",
	}
}

// jailStartEvents and jailCloseEvents are the jailed startup and teardown
// steps of a successful tap-mode fixture, in order.
var (
	jailStartEvents = []string{"jailHost", "jailCgroup", "newReclaimer", "newJailer", "admit", "probeJailed"}
	jailCloseEvents = []string{"closeJailer", "closeReclaimer"}
)

func newJailFixture(t *testing.T) *jailFixture {
	t.Helper()
	d, s := fakeDeps(map[string]string{"BBRAFT_NODE_TOKEN": "tok"})
	d.newNetwork = s.networkFactory
	serveReturns(&d, s, nil)
	dataDir, imageDir := t.TempDir(), t.TempDir()
	f := withJailFakes(t, d, s, dataDir, imageDir)
	f.args = append(tapNetworkArgs(), jailArgs(dataDir, imageDir)...)
	return f
}

// withJailFakes installs the recording jail fakes on d for a node whose
// data and image dirs are dataDir and imageDir.
func withJailFakes(t *testing.T, d deps, s *spy, dataDir, imageDir string) *jailFixture {
	t.Helper()
	f := &jailFixture{d: d, s: s}
	cgroup, state := t.TempDir(), t.TempDir()
	f.d.jailHost = func(_ context.Context, plan *jailPlan, gotData, gotImage string) (string, error) {
		s.add("jailHost")
		if plan == nil || gotData != dataDir || gotImage != imageDir {
			t.Errorf("jailHost(%+v, %q, %q)", plan, gotData, gotImage)
		}
		return "/sys/fs/cgroup/system.slice/bbraft-node.service", nil
	}
	f.d.jailCgroup = func(_ context.Context, service, jail string) error {
		s.add("jailCgroup")
		if service != "/sys/fs/cgroup/system.slice/bbraft-node.service" || jail != service+"/vms" {
			t.Errorf("jailCgroup(%q, %q)", service, jail)
		}
		return nil
	}
	f.d.newReclaimer = func(_ context.Context, gotData, gotImage string) (*node.JailReclaimer, error) {
		s.add("newReclaimer")
		if gotData != dataDir || gotImage != imageDir {
			t.Errorf("reclaimer roots %q, %q; want %q, %q", gotData, gotImage, dataDir, imageDir)
		}
		return &node.JailReclaimer{}, nil
	}
	f.d.newJailer = func(cfg jailer.Config) (*jailer.Manager, error) {
		s.add("newJailer")
		f.jailCfg = cfg
		// The real Manager on test directories: the jail cgroup and the
		// state dir under the node data dir only exist on the host.
		cfg.CgroupParent = cgroup
		cfg.StateDir = filepath.Join(state, "jailer")
		m, err := jailer.New(cfg)
		f.jm = m
		return m, err
	}
	f.d.admit = func(_ context.Context, jm *jailer.Manager, spec jailer.AdmitSpec) error {
		s.add("admit")
		f.admit = spec
		if jm != f.jm {
			t.Error("admission ran on another jailer")
		}
		return nil
	}
	f.d.probeJailed = func(_ context.Context, jm *jailer.Manager, _ *node.JailReclaimer, opts node.JailProbeOptions) (node.CPUEvidence, error) {
		s.add("probeJailed")
		f.probes = append(f.probes, opts)
		if len(f.probeErrs) > 0 {
			err := f.probeErrs[0]
			f.probeErrs = f.probeErrs[1:]
			if err != nil {
				return node.CPUEvidence{}, err
			}
		}
		return node.CPUEvidence{QEMUVersion: "10.0.0", Option: opts.Option, Host: opts.Host, Status: "prelaunch", PID: 4242}, nil
	}
	f.d.closeJailer = func(jm *jailer.Manager) error {
		s.add("closeJailer")
		return jm.Close()
	}
	f.d.closeReclaimer = func(*node.JailReclaimer) error {
		s.add("closeReclaimer")
		return nil
	}
	// Tap mode must never run the unprivileged host check or unjailed probe.
	f.d.checkHost = func(context.Context, *slog.Logger) error {
		s.add("checkHost")
		return errors.New("tap mode ran the unjailed host check")
	}
	f.d.probeCPU = func(context.Context, qemu.CPUOption, qemu.CPUProbeOptions) (qemu.CPUProbeEvidence, error) {
		s.add("probeCPU")
		return qemu.CPUProbeEvidence{}, errors.New("tap mode ran the unjailed probe")
	}
	return f
}

func (f *jailFixture) run(t *testing.T) error {
	t.Helper()
	return runWithDeps(t.Context(), f.args, io.Discard, f.d)
}

func TestRunJailedStartupOrderAndWiring(t *testing.T) {
	f := newJailFixture(t)
	if err := f.run(t); err != nil {
		t.Fatal(err)
	}
	want := []string{"hostCPU", "resolveCPU", "jailHost", "jailCgroup", "newReclaimer", "newJailer", "admit", "probeJailed",
		"newNetwork", "checkNetwork", "newManager", "serve", "shutdown", "closeNetwork", "closeJailer", "closeReclaimer"}
	if got := f.s.snapshot(); !equalStrings(got, want) {
		t.Fatalf("startup order %v, want %v", got, want)
	}
	cfg := f.jailCfg
	if cfg.StateDir == "" || filepath.Base(cfg.StateDir) != "jailer" || cfg.Reclaim == nil ||
		cfg.UIDs != (jailer.UIDRange{Min: 900000, Max: 900099}) ||
		!slices.Equal(cfg.FirmwareFiles, []string{"/usr/share/seabios/bios-256k.bin", "/usr/share/qemu/linuxboot_dma.bin"}) {
		t.Fatalf("jailer config %+v", cfg)
	}
	if !validAttemptText(f.admit.Attempt) || f.admit.Limits.IO.Device != (jailer.BlockDevice{Major: 253}) {
		t.Fatalf("admission spec %+v", f.admit)
	}
	nc := f.s.nodeCfg
	ev := f.probes[len(f.probes)-1]
	if nc.Jailer != f.jm || nc.QemuVersion != "10.0.0" || nc.HostCPU == nil || *nc.HostCPU != ev.Host ||
		!slices.Equal(nc.CPUFeatures, ev.Option.Features) || nc.JailBinary != defaultJailQEMUBinary ||
		nc.JailIO != (jailer.IOLimits{Device: jailer.BlockDevice{Major: 253}, ReadBPS: 524288000, WriteIOPS: 4000}) {
		t.Fatalf("manager config does not carry the admitted jail: %+v", nc)
	}
	if nc.Launcher != nil || nc.AllowUserNetworking {
		t.Fatal("tap mode configured an unjailed launcher")
	}
}

func validAttemptText(s string) bool { return len(s) == 36 && strings.Count(s, "-") == 4 }

func TestRunJailedFailuresStopBeforeLaterSteps(t *testing.T) {
	injected := errors.New("injected failure")
	cases := map[string]struct {
		setup func(f *jailFixture)
		want  []string
	}{
		"host proof": {func(f *jailFixture) {
			f.d.jailHost = func(context.Context, *jailPlan, string, string) (string, error) {
				f.s.add("jailHost")
				return "", injected
			}
		}, []string{"hostCPU", "resolveCPU", "jailHost"}},
		"cgroup preparation": {func(f *jailFixture) {
			f.d.jailCgroup = func(context.Context, string, string) error {
				f.s.add("jailCgroup")
				return injected
			}
		}, []string{"hostCPU", "resolveCPU", "jailHost", "jailCgroup"}},
		"admission": {func(f *jailFixture) {
			f.d.admit = func(context.Context, *jailer.Manager, jailer.AdmitSpec) error {
				f.s.add("admit")
				return injected
			}
		}, []string{"hostCPU", "resolveCPU", "jailHost", "jailCgroup", "newReclaimer", "newJailer", "admit", "closeJailer", "closeReclaimer"}},
		"jailed probe": {func(f *jailFixture) { f.probeErrs = []error{injected} },
			[]string{"hostCPU", "resolveCPU", "jailHost", "jailCgroup", "newReclaimer", "newJailer", "admit", "probeJailed", "closeJailer", "closeReclaimer"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newJailFixture(t)
			tc.setup(f)
			if err := f.run(t); !errors.Is(err, injected) {
				t.Fatalf("run = %v, want the injected failure", err)
			}
			if got := f.s.snapshot(); !equalStrings(got, tc.want) {
				t.Fatalf("events %v, want %v", got, tc.want)
			}
		})
	}
}

// TestRunJailedNestedFallbackCarriesTheExactCPU: after the optional-feature
// fallback the manager's launches render exactly the probed -cpu value.
func TestRunJailedNestedFallbackCarriesTheExactCPU(t *testing.T) {
	f := newJailFixture(t)
	f.probeErrs = []error{errors.New("qemu: cpu probe: qemu exited during startup: exit status 1: " +
		"qemu-system-x86_64: warning: host doesn't support requested feature: MSR(48BH).vmx-apicv-register [bit 8]\n" +
		"qemu-system-x86_64: warning: host doesn't support requested feature: MSR(48DH).vmx-posted-intr [bit 7]")}
	if err := f.run(t); err != nil {
		t.Fatal(err)
	}
	if len(f.probes) != 2 {
		t.Fatalf("%d probes, want the refused one and the fallback", len(f.probes))
	}
	const want = "Haswell-noTSX,+vmx,-vmx-apicv-register,-vmx-posted-intr,enforce"
	if got := f.probes[1].Option.String(); got != want {
		t.Fatalf("fallback probe -cpu %q, want %q", got, want)
	}
	nc := f.s.nodeCfg
	cpu, err := qemu.Config{CPUModel: nc.CPUModel, Nested: nc.Nested, CPUFeatures: nc.CPUFeatures, HostCPU: nc.HostCPU}.CPU()
	if err != nil {
		t.Fatal(err)
	}
	if cpu.String() != want {
		t.Fatalf("launch -cpu %q, want the admitted %q", cpu.String(), want)
	}
}

func TestJailPlanFlags(t *testing.T) {
	base := append(tapArgs(), jailArgs("/var/lib/bbraft-node", "/var/lib/bbraft-node/images")...)
	o, err := parseFlags(base, io.Discard, testEnv(nil), func() (string, error) { return "h", nil })
	if err != nil {
		t.Fatal(err)
	}
	plan, err := o.jail()
	if err != nil {
		t.Fatal(err)
	}
	if plan.UIDs != (jailer.UIDRange{Min: 900000, Max: 900099}) || plan.Cgroup != "/sys/fs/cgroup/system.slice/bbraft-node.service/vms" ||
		plan.Binary != defaultJailQEMUBinary || plan.IO != (jailer.IOLimits{Device: jailer.BlockDevice{Major: 253}, ReadBPS: 524288000, WriteIOPS: 4000}) {
		t.Fatalf("plan %+v", plan)
	}
	env := testEnv(map[string]string{
		"BBRAFT_NODE_JAIL_UID_RANGE": "900000-900099", "BBRAFT_NODE_JAIL_CGROUP": "/sys/fs/cgroup/s.service/vms",
		"BBRAFT_NODE_JAIL_IO_DEVICE": "8:0", "BBRAFT_NODE_JAIL_IO_WRITE_BPS": "1",
	})
	o, err = parseFlags(tapArgs(), io.Discard, env, func() (string, error) { return "h", nil })
	if err != nil {
		t.Fatal(err)
	}
	if plan, err = o.jail(); err != nil || plan.Cgroup != "/sys/fs/cgroup/s.service/vms" || plan.IO.WriteBPS != 1 {
		t.Fatalf("env plan %+v, %v", plan, err)
	}
	replace := func(flag, value string) []string {
		args := slices.Clone(base)
		for i, a := range args {
			if strings.HasPrefix(a, flag+"=") {
				args[i] = flag + "=" + value
			}
		}
		return args
	}
	drop := func(flag string) []string {
		return slices.DeleteFunc(slices.Clone(base), func(a string) bool { return strings.HasPrefix(a, flag+"=") })
	}
	for name, args := range map[string][]string{
		"missing uid range": drop("-node.jail-uid-range"),
		"missing cgroup":    drop("-node.jail-cgroup"),
		"missing io device": drop("-node.jail-io-device"),
		"no io rate":        drop("-node.jail-io-write-iops"),
		"root uid range":    replace("-node.jail-uid-range", "0-99"),
		"inverted range":    replace("-node.jail-uid-range", "900099-900000"),
		"single uid":        replace("-node.jail-uid-range", "900000-900000"),
		"relative cgroup":   replace("-node.jail-cgroup", "vms"),
		"partition syntax":  replace("-node.jail-io-device", "253"),
		"relative binary":   append(slices.Clone(base), "-node.jail-qemu-binary=qemu"),
		"relative firmware": replace("-node.jail-firmware", "bios-256k.bin"),
	} {
		t.Run(name, func(t *testing.T) {
			if name == "no io rate" {
				args = slices.DeleteFunc(slices.Clone(args), func(a string) bool { return strings.HasPrefix(a, "-node.jail-io-read-bps=") })
			}
			o, err := parseFlags(args, io.Discard, testEnv(nil), func() (string, error) { return "h", nil })
			if err == nil {
				_, err = o.jail()
			}
			if err == nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
	t.Run("user mode refuses jail flags", func(t *testing.T) {
		o, err := parseFlags([]string{"-node.network-mode=user", "-node.jail-uid-range=900000-900099"}, io.Discard, testEnv(nil), func() (string, error) { return "h", nil })
		if err == nil {
			var plan *jailPlan
			plan, err = o.jail()
			if err == nil && plan != nil {
				t.Fatal("user mode returned a jail plan")
			}
		}
		if err == nil {
			t.Fatal("user mode accepted a jail flag")
		}
	})
	t.Run("user mode without jail flags", func(t *testing.T) {
		o, err := parseFlags([]string{"-node.network-mode=user"}, io.Discard, testEnv(nil), func() (string, error) { return "h", nil })
		if err != nil {
			t.Fatal(err)
		}
		if plan, err := o.jail(); err != nil || plan != nil {
			t.Fatalf("user mode plan %+v, %v", plan, err)
		}
	})
}

func TestCheckJailAccounts(t *testing.T) {
	passwd := "root:x:0:0::/root:/bin/sh\nalice:x:1000:1000::/home/alice:/bin/sh\n# comment\n\n"
	group := "root:x:0:\nkvm:x:992:\n"
	uids := jailer.UIDRange{Min: 900000, Max: 900099}
	if err := checkJailAccounts(strings.NewReader(passwd), strings.NewReader(group), uids); err != nil {
		t.Fatalf("disjoint range refused: %v", err)
	}
	for name, tc := range map[string][2]string{
		"uid inside":      {passwd + "svc:x:900050:100::/:/bin/false\n", group},
		"gid inside":      {passwd + "svc:x:1001:900000::/:/bin/false\n", group},
		"group inside":    {passwd, group + "vmgroup:x:900099:\n"},
		"malformed entry": {passwd + "broken-line\n", group},
	} {
		t.Run(name, func(t *testing.T) {
			if err := checkJailAccounts(strings.NewReader(tc[0]), strings.NewReader(tc[1]), uids); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// jailCgroupTree lays out a fake delegated service cgroup.
func jailCgroupTree(t *testing.T) (root, service string) {
	t.Helper()
	root = t.TempDir()
	service = filepath.Join(root, "system.slice", "bbraft-node.service")
	for _, dir := range []string{service, filepath.Join(service, "supervisor")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for path, content := range map[string]string{
		filepath.Join(root, "cgroup.controllers"):        "cpu memory io pids",
		filepath.Join(service, "cgroup.controllers"):     "cpu memory io pids",
		filepath.Join(service, "cgroup.subtree_control"): "",
		filepath.Join(service, "cgroup.procs"):           "",
	} {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root, service
}

func TestCheckDelegation(t *testing.T) {
	const cgroup2Magic = 0x63677270
	good := func(root, service string) delegationEnv {
		return delegationEnv{
			CgroupRoot: root, FSType: cgroup2Magic,
			SelfCgroup: []byte("0::/system.slice/bbraft-node.service/supervisor\n"),
			SelfNS:     4026531835, InitNS: 4026531835,
			Xattr: func(path, name string) (string, error) {
				if path == service && name == "trusted.delegate" {
					return "1", nil
				}
				return "", errors.New("no xattr")
			},
			JailCgroup: service + "/vms",
			Owner:      uint32(os.Getuid()),
		}
	}
	root, service := jailCgroupTree(t)
	got, err := checkDelegation(good(root, service))
	if err != nil || got != service {
		t.Fatalf("checkDelegation = %q, %v; want %q", got, err, service)
	}
	for name, edit := range map[string]func(e *delegationEnv, service string){
		"not cgroup2":      func(e *delegationEnv, _ string) { e.FSType = 0x27e0eb },
		"cgroup namespace": func(e *delegationEnv, _ string) { e.SelfNS++ },
		"hybrid lines": func(e *delegationEnv, _ string) {
			e.SelfCgroup = []byte("1:name=systemd:/x\n0::/system.slice/bbraft-node.service/supervisor\n")
		},
		"not the supervisor leaf": func(e *delegationEnv, _ string) { e.SelfCgroup = []byte("0::/system.slice/bbraft-node.service\n") },
		"not delegated": func(e *delegationEnv, _ string) {
			e.Xattr = func(string, string) (string, error) { return "", errors.New("ENODATA") }
		},
		"jail outside the service": func(e *delegationEnv, _ string) {
			e.JailCgroup = e.CgroupRoot + "/system.slice/other.service/vms"
		},
		"jail not named vms": func(e *delegationEnv, service string) { e.JailCgroup = service + "/jail" },
		"jail path unclean":  func(e *delegationEnv, service string) { e.JailCgroup = service + "/supervisor/../vms" },
		"service has members": func(e *delegationEnv, service string) {
			if err := os.WriteFile(filepath.Join(service, "cgroup.procs"), []byte("4242\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"existing vms is a file": func(e *delegationEnv, service string) {
			if err := os.WriteFile(filepath.Join(service, "vms"), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"existing vms is a symlink": func(e *delegationEnv, service string) {
			if err := os.Symlink(t.TempDir(), filepath.Join(service, "vms")); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			root, service := jailCgroupTree(t)
			env := good(root, service)
			edit(&env, service)
			if _, err := checkDelegation(env); err == nil {
				t.Fatalf("checkDelegation accepted %s", name)
			}
		})
	}
}

func TestCheckJailIODevice(t *testing.T) {
	disk := jailer.BlockDevice{Major: 253}
	same := func(string) (jailer.BlockDevice, error) { return disk, nil }
	if err := checkJailIODevice(disk, []string{"/data", "/images"}, same); err != nil {
		t.Fatal(err)
	}
	other := func(dir string) (jailer.BlockDevice, error) {
		if dir == "/images" {
			return jailer.BlockDevice{Major: 8}, nil
		}
		return disk, nil
	}
	if err := checkJailIODevice(disk, []string{"/data", "/images"}, other); err == nil {
		t.Fatal("an image dir on another disk was accepted")
	}
	tmpfs := func(string) (jailer.BlockDevice, error) { return jailer.BlockDevice{}, errors.New("no block device") }
	if err := checkJailIODevice(disk, []string{"/data"}, tmpfs); err == nil {
		t.Fatal("a dir without a block device was accepted")
	}
}

func TestPrepareJailCgroup(t *testing.T) {
	t.Run("enables controllers and creates vms", func(t *testing.T) {
		_, service := jailCgroupTree(t)
		if err := prepareJailCgroupAs(t.Context(), service, service+"/vms", uint32(os.Getuid())); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(service, "cgroup.subtree_control"))
		if err != nil || strings.TrimSpace(string(raw)) != "+cpu +memory +io +pids" {
			t.Fatalf("subtree_control %q, %v", raw, err)
		}
		if info, err := os.Lstat(filepath.Join(service, "vms")); err != nil || !info.IsDir() {
			t.Fatalf("vms: %v %v", info, err)
		}
	})
	t.Run("existing vms directory is kept", func(t *testing.T) {
		_, service := jailCgroupTree(t)
		if err := os.Mkdir(filepath.Join(service, "vms"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := prepareJailCgroupAs(t.Context(), service, service+"/vms", uint32(os.Getuid())); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("canceled changes nothing", func(t *testing.T) {
		_, service := jailCgroupTree(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := prepareJailCgroupAs(ctx, service, service+"/vms", uint32(os.Getuid())); !errors.Is(err, context.Canceled) {
			t.Fatalf("prepareJailCgroup = %v, want Canceled", err)
		}
		if _, err := os.Lstat(filepath.Join(service, "vms")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("a canceled preparation created vms")
		}
	})
}

// mainEnv makes the re-executed test binary run the real main.
const mainEnv = "BBRAFT_NODE_TEST_RUN_MAIN"

func TestMain(m *testing.M) {
	if os.Getenv(mainEnv) == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// TestMainDispatchesHelperFirst: the real main runs the jailer helper before
// it parses flags, environment or credentials, and exits the reserved code.
func TestMainDispatchesHelperFirst(t *testing.T) {
	token := filepath.Join(t.TempDir(), "token-never-read")
	cmd := exec.Command(os.Args[0], jailer.HelperArg, "x", "y", "z")
	cmd.Env = []string{mainEnv + "=1", "BBRAFT_NODE_TOKEN_FILE=" + token, "BBRAFT_NODE_NETWORK_MODE=bogus"}
	out, err := cmd.CombinedOutput()
	exitErr, ok := errors.AsType[*exec.ExitError](err)
	if !ok || exitErr.ExitCode() != jailer.HelperExitCode {
		t.Fatalf("main with the helper selector = %v (%s), want exit %d", err, out, jailer.HelperExitCode)
	}
	if strings.Contains(string(out), "flag") || strings.Contains(string(out), "network-mode") || strings.Contains(string(out), "token") {
		t.Fatalf("main parsed flags, environment or credentials before the helper: %s", out)
	}
}
