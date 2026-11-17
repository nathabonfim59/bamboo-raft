// CPU policy resolution and the CPU-only QEMU probe.
//
// The policy itself is shared with the node: internal/node/qemu owns
// ResolveCPU/DetectHostCPU/DefaultCPUModel and the bounded ProbeCPU process
// discipline, and internal/node/qmp owns the QMP client. This file only wires
// flags to those helpers and prints the probe's PASS evidence.
package main

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
)

// resolveCPUShared is the single resolution path for both smoke modes. All CPU
// decisions come from the shared qemu helpers; nothing here inspects the host
// on its own. The reason for a disabled nested feature is returned separately
// so callers can keep machine-parseable output on stdout.
//
// Detection mirrors qemu.Start: off needs no host facts at all; auto with a
// detection failure clears the partial facts, records why, and resolves with
// empty facts (no nested feature); on fails clearly when it cannot detect.
//
// allowTestHost selects qemu.ResolveTestCPU instead of qemu.ResolveCPU. It is
// the smoke/probe opt-in for an explicit -cpu-model host; the production
// resolver stays strict, and ResolveTestCPU still rejects max.
func resolveCPUShared(model, nestedMode, cpuinfoPath string, allowTestHost bool) (qemu.CPUOption, qemu.HostCPU, qemu.NestedMode, string, error) {
	resolve := qemu.ResolveCPU
	if allowTestHost {
		resolve = qemu.ResolveTestCPU
	}
	mode, err := qemu.ParseNestedMode(nestedMode)
	if err != nil {
		return qemu.CPUOption{}, qemu.HostCPU{}, "", "", err
	}
	if mode == qemu.NestedOff {
		option, err := resolve(model, mode, qemu.HostCPU{})
		if err != nil {
			return option, qemu.HostCPU{}, mode, "", err
		}
		return option, qemu.HostCPU{}, mode, nestedReason(mode, qemu.HostCPU{}, option.Nested, model), nil
	}
	host, err := qemu.DetectHostCPU(cpuinfoPath)
	if err != nil {
		if mode == qemu.NestedOn {
			return qemu.CPUOption{}, qemu.HostCPU{}, mode, "", fmt.Errorf("cpu-nested=on: detect host CPU: %w", err)
		}
		reason := "host CPU detection failed; nested auto resolved with no host facts: " + err.Error()
		option, resolveErr := resolve(model, mode, qemu.HostCPU{})
		if resolveErr != nil {
			return option, qemu.HostCPU{}, mode, "", resolveErr
		}
		return option, qemu.HostCPU{}, mode, reason, nil
	}
	option, err := resolve(model, mode, host)
	if err != nil {
		return option, host, mode, "", err
	}
	return option, host, mode, nestedReason(mode, host, option.Nested, model), nil
}

// nestedReason explains a resolved outcome in one human-readable sentence. It
// reports the shared policy's outcome; it never makes a policy decision. For
// an explicit test -cpu model host it must not claim the fixed Intel CPUID
// vendor, because the guest then sees the host vendor.
func nestedReason(mode qemu.NestedMode, host qemu.HostCPU, nested, model string) string {
	switch {
	case mode == qemu.NestedOff:
		return "nested virtualization disabled by cpu-nested=off"
	case nested == "vmx":
		return "GenuineIntel host with vmx on every CPU"
	case model == "host" && host.Vendor == "AuthenticAMD":
		return "AuthenticAMD host with explicit test -cpu host: the policy appends no +svm in phase 0.5 (nested append policy unchanged); the guest CPUID vendor and features follow the host"
	case host.Vendor == "AuthenticAMD":
		return "AuthenticAMD host: no +svm is exposed in phase 0.5 because the fixed model keeps an Intel CPUID vendor and in-guest kvm_amd would refuse nested virtualization"
	case host.Vendor == "GenuineIntel":
		return "GenuineIntel host without vmx on every CPU"
	default:
		return "host CPU vendor is unknown or mixed"
	}
}

// noteTestHostOverride prints the one-line test-only test override notice. It
// states the production default and the truthful guest CPUID vendor.
func noteTestHostOverride(allowTestHost bool) {
	if !allowTestHost {
		return
	}
	fmt.Fprintf(os.Stderr, "smoke: TEST OVERRIDE -cpu-model=host (smoke/probe only; production default is %s,enforce; the guest sees the host CPUID vendor, not the fixed Intel model)\n", qemu.DefaultCPUModel)
}

// resolveCPUMode implements -resolve-cpu: print the resolved policy and exit
// without touching QEMU or vsock. stdout stays key=value without spaces so
// shell callers can parse it; the reason goes to stderr.
func resolveCPUMode(model, nestedMode, cpuinfoPath string, allowTestHost bool) error {
	noteTestHostOverride(allowTestHost)
	option, host, mode, reason, err := resolveCPUShared(model, nestedMode, cpuinfoPath, allowTestHost)
	if err != nil {
		return err
	}
	fmt.Printf("PASS resolve-cpu cpu=%s model=%s nested=%s mode=%s vendor=%s vmx=%t svm=%t cpus=%d\n",
		option.String(), option.Model, cmp.Or(option.Nested, "none"), mode,
		cmp.Or(host.Vendor, "unknown"), host.Vmx, host.Svm, host.CPUs)
	fmt.Fprintf(os.Stderr, "smoke: resolve-cpu reason: %s\n", reason)
	return nil
}

// probeConfig carries the -probe-cpu knobs. Zero-value fields use the flag
// defaults the caller parsed.
type probeConfig struct {
	binary           string
	cpuinfo          string
	cpuModel         string
	cpuNested        string
	cpuModelTestHost bool
	kvmDevice        string
	dir              string
	timeout          time.Duration
	grace            time.Duration
}

// probeCPUMode implements -probe-cpu: resolve the shared CPU policy, then use
// the shared qemu.ProbeCPU core. Signals are registered before the probe so
// cancellation still runs the core's independent cleanup and can never print
// PASS.
func probeCPUMode(cfg probeConfig) error {
	noteTestHostOverride(cfg.cpuModelTestHost)
	option, host, mode, reason, err := resolveCPUShared(cfg.cpuModel, cfg.cpuNested, cfg.cpuinfo, cfg.cpuModelTestHost)
	if err != nil {
		return fmt.Errorf("probe: resolve CPU: %w", err)
	}
	fmt.Fprintf(os.Stderr, "smoke: probe-cpu cpu=%s nested=%s mode=%s vendor=%s vmx=%t svm=%t cpus=%d\n",
		option.String(), cmp.Or(option.Nested, "none"), mode, cmp.Or(host.Vendor, "unknown"), host.Vmx, host.Svm, host.CPUs)
	fmt.Fprintf(os.Stderr, "smoke: probe-cpu reason: %s\n", reason)

	rootCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	evidence, err := qemu.ProbeCPU(rootCtx, option, qemu.CPUProbeOptions{
		Binary:    cfg.binary,
		KVMDevice: cfg.kvmDevice,
		WorkDir:   cfg.dir,
		Timeout:   cfg.timeout,
		Grace:     cfg.grace,
	})
	if err != nil {
		return fmt.Errorf("probe: %w", err)
	}
	// An interrupt after cleanup still must not report PASS.
	if rootErr := rootCtx.Err(); rootErr != nil {
		return fmt.Errorf("probe: interrupted before completion: %w", context.Cause(rootCtx))
	}
	fmt.Printf("PASS probe-cpu cpu=%s status=%s cpus=%d qemu=%s pid=%d\n",
		option.String(), evidence.Status, evidence.CPUs, evidence.QEMUVersion, evidence.PID)
	return nil
}
