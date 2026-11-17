// Command bbraft-node is the agent on one KVM host. It owns QEMU VMs under
// node.data-dir, dials bbraft-server (nodes dial out; the server never dials
// back) and serves nodev1.Node over that link, reconnecting
// after drops without touching running VMs.
//
// Phase 0.5 links are plaintext TCP, so server.addr is restricted to
// localhost, loopback and private IPs (see docs/node.md). TLS is a later
// phase.
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
	"uuid"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/node"
	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	nodenet "github.com/nathabonfim59/bamboo-raft/internal/node/net"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/version"
)

// shutdownTimeout bounds manager cleanup after the link loop ends. It is a
// fresh context: the signal context that stopped Serve is already done.
const shutdownTimeout = 20 * time.Second

func main() {
	// The jailer's re-exec helpers run before flags, environment or
	// credentials are read; a selected helper never returns.
	jailer.RunHelperIfSelected()
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "bbraft-node:", err)
		os.Exit(1)
	}
}

// vmManager is the part of *node.Manager the command drives; tests inject a
// fake.
type vmManager interface {
	Register(*grpc.Server)
	Shutdown(context.Context) error
}

// nodeNetwork is the command's view of per-VM networking: the published
// [node.VMNetwork] consumer interface, the startup host check and the handle
// the command owns. The command constructs it in TAP mode and closes it after
// the manager's Shutdown; the manager only borrows it.
type nodeNetwork interface {
	node.VMNetwork
	CheckHost(context.Context) error
	Close() error
}

// The production manager satisfies the command's view; the assertion keeps
// the wiring from drifting away from the published interface.
var _ nodeNetwork = (*nodenet.Manager)(nil)

// deps carries the replaceable seams: environment, host facts and the
// manager/link implementations.
type deps struct {
	lookupEnv  func(string) (string, bool)
	hostname   func() (string, error)
	facts      func(dataDir string) (hostFacts, error)
	hostCPU    func() (qemu.HostCPU, error)
	resolveCPU func(model string, mode qemu.NestedMode, host qemu.HostCPU) (qemu.CPUOption, error)
	// resolveTestCPU is the dev/test-only resolver used when the
	// cpu-allow-host opt-in and model "host" are both set.
	resolveTestCPU func(model string, mode qemu.NestedMode, host qemu.HostCPU) (qemu.CPUOption, error)
	checkHost      func(ctx context.Context, log *slog.Logger) error
	// probeCPU proves the resolved enforced CPU can actually be realized
	// before any manager state, listener or advertisement exists.
	probeCPU   func(ctx context.Context, option qemu.CPUOption, opts qemu.CPUProbeOptions) (qemu.CPUProbeEvidence, error)
	newManager func(ctx context.Context, cfg node.Config) (vmManager, error)
	// newNetwork constructs the TAP network manager from the validated
	// plan's exact [nodenet.Config]. The returned handle is owned by the
	// command, which closes it after the node manager shuts down.
	newNetwork func(cfg nodenet.Config, log *slog.Logger) (nodeNetwork, error)
	serve      func(ctx context.Context, cfg nodelink.DialConfig, srv *grpc.Server) error

	// Jailed production (tap) startup, in this order: read-only host
	// proofs, the cgroup mutation, the reclaimer, the jailer (with the
	// reclaimer as its mandatory policy), functional admission and the
	// jailed CPU probe. closeJailer and closeReclaimer run after the
	// manager's Shutdown and the network's Close, in that order.
	jailHost       func(ctx context.Context, plan *jailPlan, dataDir, imageDir string) (serviceDir string, err error)
	jailCgroup     func(ctx context.Context, serviceDir, jailCgroup string) error
	newReclaimer   func(ctx context.Context, dataDir, imageDir string) (*node.JailReclaimer, error)
	newJailer      func(cfg jailer.Config) (*jailer.Manager, error)
	admit          func(ctx context.Context, jm *jailer.Manager, spec jailer.AdmitSpec) error
	probeJailed    func(ctx context.Context, jm *jailer.Manager, rc *node.JailReclaimer, opts node.JailProbeOptions) (node.CPUEvidence, error)
	closeJailer    func(*jailer.Manager) error
	closeReclaimer func(*node.JailReclaimer) error
}

func realDeps() deps {
	return deps{
		lookupEnv:      os.LookupEnv,
		hostname:       os.Hostname,
		facts:          probeHostFacts,
		hostCPU:        func() (qemu.HostCPU, error) { return qemu.DetectHostCPU("") },
		resolveCPU:     qemu.ResolveCPU,
		resolveTestCPU: qemu.ResolveTestCPU,
		checkHost:      qemu.CheckHost,
		probeCPU:       qemu.ProbeCPU,
		newManager:     func(ctx context.Context, cfg node.Config) (vmManager, error) { return node.New(ctx, cfg) },
		newNetwork: func(cfg nodenet.Config, log *slog.Logger) (nodeNetwork, error) {
			return nodenet.New(cfg, nodenet.Deps{Logger: log})
		},
		serve: nodelink.Serve,

		jailHost:     realJailHost,
		jailCgroup:   prepareJailCgroup,
		newReclaimer: node.NewJailReclaimer,
		newJailer:    jailer.New,
		admit: func(ctx context.Context, jm *jailer.Manager, spec jailer.AdmitSpec) error {
			_, err := jm.Admit(ctx, spec)
			return err
		},
		probeJailed:    node.ProbeJailedCPU,
		closeJailer:    (*jailer.Manager).Close,
		closeReclaimer: (*node.JailReclaimer).Close,
	}
}

func run(args []string, stderr io.Writer) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runWithDeps(ctx, args, stderr, realDeps())
}

// runWithDeps resolves configuration, builds the manager and keeps the link
// to the server until ctx ends. The manager's lifetime is the process, not
// one link: a dropped connection or reconnect never stops a VM.
func runWithDeps(ctx context.Context, args []string, stderr io.Writer, d deps) (retErr error) {
	o, err := parseFlags(args, stderr, d.lookupEnv, d.hostname)
	if err != nil {
		return err
	}
	// The authoritative fail-closed networking check runs before host facts,
	// credentials, CPU probes and every manager side effect.
	plan, err := o.networking()
	if err != nil {
		return err
	}
	// Tap mode is production: every QEMU is jailed. The plan is validated
	// before host facts and credentials; user mode refuses jail flags.
	jail, err := o.jail()
	if err != nil {
		return err
	}
	if plan.Tap != nil && jail == nil {
		return errors.New("tap networking requires the jail configuration")
	}
	facts, err := d.facts(o.dataDir)
	if err != nil {
		return err
	}
	capacity, err := resolveCapacity(o, facts)
	if err != nil {
		return err
	}
	if facts.vcpus > math.MaxUint32 {
		return fmt.Errorf("host CPU count %d exceeds %d", facts.vcpus, uint64(math.MaxUint32))
	}
	token, err := loadToken(d.lookupEnv, o.tokenFile)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(stderr, nil))
	if plan.Mode == networkModeUser {
		log.Info("vm networking: QEMU user-mode dev/test fallback", "network_mode", plan.Mode, "dev_test", true)
	}
	log.Info("capacity resolved",
		"vcpus", capacity.GetVcpus(),
		"memory_mib", capacity.GetMemoryMib(),
		"disk_gib", capacity.GetDiskGib(),
		"host_vcpus", facts.vcpus,
		"host_memory_mib", facts.memoryMiB,
		"host_disk_gib", facts.diskGiB,
		"cpu_overcommit", o.cpuOvercommit,
		"memory_overcommit", o.memoryOvercommit,
		"disk_overcommit", o.diskOvercommit,
	)
	if o.cpuAllowHost {
		log.Warn("-node.cpu-allow-host is a dev/test opt-in: -cpu host is permitted; never enable it in production")
	}

	mode, err := qemu.ParseNestedMode(o.cpuNested)
	if err != nil {
		return err
	}
	// off needs no host facts, matching the shared helper's contract.
	var host qemu.HostCPU
	if mode != qemu.NestedOff {
		host, err = d.hostCPU()
		if err != nil {
			// Detection failure is not a boot blocker in auto: resolve
			// with an empty host, which conservatively exposes nothing.
			// on requires it.
			if mode == qemu.NestedOn {
				return fmt.Errorf("cannot detect host CPU for -node.cpu-nested=on: %w", err)
			}
			log.Warn("cannot detect host CPU; nested virtualization stays off", "err", err)
			host = qemu.HostCPU{}
		}
	}
	// The opt-in permits exactly "host" and routes it through the shared
	// test policy (same enforce and nested rules); every other model keeps
	// the production resolver.
	testHost := o.cpuAllowHost && o.cpuModel == "host"
	resolve := d.resolveCPU
	if testHost {
		resolve = d.resolveTestCPU
	}
	cpu, err := resolve(o.cpuModel, mode, host)
	if err != nil {
		return err
	}
	cpu.Features = slices.Clone(o.cpuFeatures)
	log.Info("cpu resolved",
		"cpu_option", cpu.String(),
		"model", cpu.Model,
		"nested", cmp.Or(cpu.Nested, "none"),
		"mode", mode.String(),
		"vendor", cmp.Or(host.Vendor, "unknown"),
		"cpus", host.CPUs,
	)
	if mode == qemu.NestedAuto && cpu.Nested == "" {
		reason := "not every CPU advertises vmx or the vendor is unknown"
		if host.Vendor == "AuthenticAMD" {
			reason = "AuthenticAMD host: the fixed Intel guest vendor makes kvm_amd reject svm"
			if testHost {
				reason = "AuthenticAMD host: the nested policy appends no +svm; guest CPUID vendor and features follow the host"
			}
		}
		log.Info("cpu-nested=auto resolved without nested virtualization", "reason", reason)
	}

	var jm *jailer.Manager
	var qemuVersion string
	if jail != nil {
		// Production: read-only host proofs, then the one cgroup mutation,
		// then the reclaimer, the jailer with that reclaimer as its policy,
		// functional admission and the jailed CPU probe. Root never runs
		// QEMU, so the unjailed host check and probe are not used. Each
		// close is registered at construction, so LIFO order closes the
		// jailer after the manager and network, and the reclaimer last.
		ev, closers, err := startJail(ctx, d, o, jail, &cpu, host, log)
		defer func() { retErr = errors.Join(retErr, closers()) }()
		if err != nil {
			return err
		}
		jm, qemuVersion = ev.jailer, ev.evidence.QEMUVersion
		host = ev.evidence.Host
	} else {
		if err := d.checkHost(ctx, log); err != nil {
			return err
		}
		evidence, err := d.probeCPU(ctx, cpu, qemu.CPUProbeOptions{})
		if toggles, dropped, ok := qemu.NestedFallback(cpu, err); ok {
			// Nested VMX stays on with the optional capabilities this host
			// lacks (APICv, posted interrupts on desktop CPUs) turned off;
			// any other missing feature still fails below.
			log.Warn("host lacks some nested VMX features; guests keep nested virtualization without them",
				"dropped", strings.Join(dropped, ","),
				"cpu_option_refused", cpu.String(),
			)
			cpu.Features = append(cpu.Features, toggles...)
			evidence, err = d.probeCPU(ctx, cpu, qemu.CPUProbeOptions{})
		}
		if err != nil {
			return fmt.Errorf("enforced CPU %s cannot be realized: %w", cpu.String(), err)
		}
		log.Info("cpu realized",
			"cpu_option", cpu.String(),
			"status", evidence.Status,
			"cpus", evidence.CPUs,
			"qemu", evidence.QEMUVersion,
			"pid", evidence.PID,
		)
	}
	// Production TAP networking is constructed from the exact validated plan
	// only after the CPU probe succeeded. The command owns the handle: the
	// manager borrows it. This defer is registered before the manager's
	// Shutdown defer, so LIFO order shuts the manager down first.
	var network nodeNetwork
	if plan.Tap != nil {
		network, err = d.newNetwork(*plan.Tap, log)
		if err != nil {
			return fmt.Errorf("node networking: %w", err)
		}
		defer func() {
			if cerr := network.Close(); cerr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("node networking: close: %w", cerr))
			}
		}()
		if err := network.CheckHost(ctx); err != nil {
			return fmt.Errorf("node networking: check host: %w", err)
		}
		log.Info("vm networking ready",
			"network_mode", plan.Mode,
			"bridge", plan.Tap.Bridge,
			"prefix", plan.Tap.Prefix,
		)
	}
	cfg := node.Config{
		NodeID:         o.nodeID,
		DataDir:        o.dataDir,
		ImageDir:       o.imageDir,
		DefaultImage:   o.defaultImage,
		CPUModel:       o.cpuModel,
		AllowHostCPU:   o.cpuAllowHost,
		Capacity:       capacity,
		HostVCPUs:      uint32(facts.vcpus),
		DiskOvercommit: o.diskOvercommit,
		BootTimeout:    o.bootTimeout,
		Log:            log,
		Nested:         mode,
		CPUFeatures:    cpu.Features,
	}
	if jm != nil {
		admitted := host
		cfg.Jailer = jm
		cfg.QemuVersion = qemuVersion
		cfg.HostCPU = &admitted
		cfg.JailBinary = jail.Binary
		cfg.JailToolBinary = jail.ToolBinary
		cfg.JailIO = jail.IO
	}
	if network != nil {
		cfg.Network = network
	} else {
		// The dev/test fallback is never selected by a configuration or host
		// error: only an explicit -node.network-mode=user reaches this branch.
		cfg.AllowUserNetworking = true
	}
	mgr, err := d.newManager(ctx, cfg)
	if err != nil {
		return err
	}
	// Cleanup is bounded by its own context: ctx is already canceled here,
	// and Shutdown stays callable so a retry can still report the outcome.
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := mgr.Shutdown(sctx); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("node: shutdown: %w", err))
		}
	}()

	srv := grpc.NewServer(nodelink.ServerOptions()...)
	mgr.Register(srv)
	log.Info("bbraft-node ready",
		"version", version.Version,
		"node_id", o.nodeID,
		"server_addr", o.serverAddr,
		"vcpus", capacity.GetVcpus(),
		"memory_mib", capacity.GetMemoryMib(),
		"disk_gib", capacity.GetDiskGib(),
	)
	return d.serve(ctx, nodelink.DialConfig{
		Addr:    o.serverAddr,
		NodeID:  o.nodeID,
		Token:   token,
		Version: version.Version,
		Log:     log,
	}, srv)
}

// jailStart is what a successful jailed startup admitted.
type jailStart struct {
	jailer   *jailer.Manager
	evidence node.CPUEvidence
}

// startJail runs the jailed startup sequence and returns the closers of
// everything it constructed (jailer first, reclaimer last), which the caller
// runs on every path. On a nested-VMX optional-feature refusal it retries once
// with those features off (cpu is updated), and the admitted evidence must
// carry exactly the probed -cpu and host facts.
func startJail(ctx context.Context, d deps, o *options, plan *jailPlan, cpu *qemu.CPUOption, host qemu.HostCPU, log *slog.Logger) (jailStart, func() error, error) {
	var closers []func() error
	closeAll := func() error {
		var errs []error
		for _, c := range slices.Backward(closers) {
			errs = append(errs, c())
		}
		return errors.Join(errs...)
	}
	service, err := d.jailHost(ctx, plan, o.dataDir, o.imageDir)
	if err != nil {
		return jailStart{}, closeAll, fmt.Errorf("jail host: %w", err)
	}
	if err := d.jailCgroup(ctx, service, plan.Cgroup); err != nil {
		return jailStart{}, closeAll, fmt.Errorf("jail cgroup: %w", err)
	}
	rc, err := d.newReclaimer(ctx, o.dataDir, o.imageDir)
	if err != nil {
		return jailStart{}, closeAll, fmt.Errorf("jail reclaimer: %w", err)
	}
	closers = append(closers, func() error {
		if err := d.closeReclaimer(rc); err != nil {
			return fmt.Errorf("jail reclaimer: close: %w", err)
		}
		return nil
	})
	jm, err := d.newJailer(jailer.Config{
		StateDir:      filepath.Join(o.dataDir, "jailer"),
		CgroupParent:  plan.Cgroup,
		UIDs:          plan.UIDs,
		LibraryDirs:   plan.LibraryDirs,
		FirmwareFiles: plan.Firmware,
		Reclaim:       rc.Reclaim,
		Log:           log,
	})
	if err != nil {
		return jailStart{}, closeAll, fmt.Errorf("jailer: %w", err)
	}
	closers = append(closers, func() error {
		if err := d.closeJailer(jm); err != nil {
			return fmt.Errorf("jailer: close: %w", err)
		}
		return nil
	})
	if err := d.admit(ctx, jm, jailer.AdmitSpec{Attempt: uuid.NewV7().String(), Limits: jailer.Limits{
		CPUQuotaPercent: 100, MemoryBytes: 256 << 20, Pids: 64, IO: plan.IO,
	}}); err != nil {
		return jailStart{}, closeAll, fmt.Errorf("jail admission: %w", err)
	}
	log.Info("jail admitted", "uid_min", plan.UIDs.Min, "uid_max", plan.UIDs.Max, "cgroup", plan.Cgroup)
	probe := func() (node.CPUEvidence, error) {
		return d.probeJailed(ctx, jm, rc, node.JailProbeOptions{
			Binary: plan.Binary, Option: *cpu, Host: host,
			Limits: jailer.Limits{CPUQuotaPercent: 100, MemoryBytes: 512 << 20, Pids: 64, IO: plan.IO},
		})
	}
	ev, err := probe()
	if toggles, dropped, ok := qemu.NestedFallback(*cpu, err); ok {
		log.Warn("host lacks some nested VMX features; guests keep nested virtualization without them",
			"dropped", strings.Join(dropped, ","),
			"cpu_option_refused", cpu.String(),
		)
		cpu.Features = append(slices.Clip(cpu.Features), toggles...)
		ev, err = probe()
	}
	if err != nil {
		return jailStart{}, closeAll, fmt.Errorf("enforced CPU %s cannot be realized: %w", cpu.String(), err)
	}
	if ev.Option.String() != cpu.String() || ev.Host != host {
		return jailStart{}, closeAll, fmt.Errorf("jailed CPU probe evidence %s does not match the requested %s", ev.Option.String(), cpu.String())
	}
	log.Info("cpu realized",
		"cpu_option", cpu.String(),
		"status", ev.Status,
		"qemu", ev.QEMUVersion,
		"pid", ev.PID,
		"jailed", true,
	)
	return jailStart{jailer: jm, evidence: ev}, closeAll, nil
}
