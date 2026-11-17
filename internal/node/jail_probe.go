package node

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"time"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
)

// probeRunner is the jailer view the jailed CPU probe needs.
type probeRunner interface {
	StartProbe(ctx context.Context, spec jailer.ProbeSpec) (jailRun, error)
	Record(id string) (jailer.Record, bool)
	ReleaseAttempt(ctx context.Context, ref jailer.Ref) error
}

// JailProbeOptions configures one jailed CPU admission probe.
type JailProbeOptions struct {
	// Binary is the trusted QEMU the jailer stages.
	Binary string
	// Option is the resolved CPU option to realize (model, nested choice
	// and feature toggles); Host the host facts it was resolved from.
	Option qemu.CPUOption
	Host   qemu.HostCPU
	// Limits bounds the probe's cgroup.
	Limits jailer.Limits
	// Timeout bounds QMP readiness and the queries (default 20s); Grace
	// bounds each quit, kill and reap step (default 5s).
	Timeout time.Duration
	Grace   time.Duration
	// dial replaces the QMP dial (test seam).
	dial jailQMPDial
}

// CPUEvidence is what a successful jailed CPU probe proved through its
// verified QMP peer. Every jailed launch carries Option and Host unchanged
// and reports QEMUVersion.
type CPUEvidence struct {
	QEMUVersion string
	Option      qemu.CPUOption
	Host        qemu.HostCPU
	Status      string
	PID         int
}

// Jailed CPU probe defaults.
const (
	jailProbeTimeout = 20 * time.Second
	jailProbeGrace   = 5 * time.Second
)

// ProbeJailedCPU realizes opts.Option in a paused CPU-only QEMU launched
// through the jailer (own uid, cgroup, namespaces, chroot, no capabilities)
// and proves it through QMP, so root never runs QEMU. A retained probe
// record from an earlier start is settled first, only by the jailer's proof
// of death and an empty group; a live one fails startup and is never killed.
// Every child the jailer returns is reaped by this function's sole Wait;
// failure keeps every handle and resource until settlement is proven.
func ProbeJailedCPU(ctx context.Context, jm *jailer.Manager, rc *JailReclaimer, opts JailProbeOptions) (CPUEvidence, error) {
	return probeJailedCPU(ctx, jailerProbeRunner{jm}, rc, opts)
}

func probeJailedCPU(ctx context.Context, runner probeRunner, rc *JailReclaimer, opts JailProbeOptions) (CPUEvidence, error) {
	if err := ctx.Err(); err != nil {
		return CPUEvidence{}, fmt.Errorf("node: cpu probe: %w", context.Cause(ctx))
	}
	if runner == nil || rc == nil || opts.Binary == "" {
		return CPUEvidence{}, errors.New("node: cpu probe: jailer, reclaimer and QEMU binary are required")
	}
	timeout := cmp.Or(opts.Timeout, jailProbeTimeout)
	grace := cmp.Or(opts.Grace, jailProbeGrace)
	if err := settleRetainedProbe(ctx, runner); err != nil {
		return CPUEvidence{}, err
	}
	attempt := uuid.NewV7().String()
	control, serial, err := rc.ProbeScratch(ctx, attempt)
	if err != nil {
		return CPUEvidence{}, err
	}
	defer control.Close()
	defer serial.Close()
	cpu := opts.Option.String()
	stderr := &jailTail{}
	run, err := runner.StartProbe(ctx, jailer.ProbeSpec{
		Attempt: attempt, Binary: opts.Binary, CPU: cpu,
		ControlDir: control, SerialLog: serial, Limits: opts.Limits,
		Stdout: io.Discard, Stderr: stderr,
	})
	if run == nil {
		if err == nil {
			err = errors.New("no child and no error")
		}
		err = fmt.Errorf("node: cpu probe %s: %w", cpu, err)
		if rec, ok := runner.Record(jailer.ProbeID); ok && rec.Attempt == attempt {
			// The jailer kept this attempt's reservation: its scratch stays
			// with it until a later start settles the record.
			return CPUEvidence{}, fmt.Errorf("%w; its reservation and scratch are kept", err)
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), grace)
		defer cancel()
		return CPUEvidence{}, errors.Join(err, rc.RemoveProbeScratch(cleanup, attempt, control, serial))
	}
	ref := jailer.Ref{SandboxID: jailer.ProbeID, Attempt: attempt}
	proc := newJailedProcess(jailer.ProbeID, ref, run, slog.New(slog.DiscardHandler), stderr)
	proc.stopGrace = grace
	if err != nil {
		return CPUEvidence{}, failJailedProbe(ctx, proc, grace, fmt.Errorf("node: cpu probe %s: %w", cpu, err))
	}
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dial := opts.dial
	if dial == nil {
		dial = dialJailQMP
	}
	client, err := dial(qctx, filepath.Join(control.Name(), qemu.QMPFilename), run.PID(), proc.done)
	if err != nil {
		return CPUEvidence{}, failJailedProbe(ctx, proc, grace, fmt.Errorf("node: cpu probe %s: %w%s", cpu, err, stderr.suffix()))
	}
	proc.setClient(client)
	status, err := qemu.CheckProbeMachine(qctx, client)
	if err != nil {
		return CPUEvidence{}, failJailedProbe(ctx, proc, grace, fmt.Errorf("node: cpu probe %s: %w", cpu, err))
	}
	ev := CPUEvidence{
		QEMUVersion: client.Greeting().Version.String(),
		Option:      opts.Option, Host: opts.Host, Status: status, PID: run.PID(),
	}
	if err := ctx.Err(); err != nil {
		return CPUEvidence{}, failJailedProbe(ctx, proc, grace, fmt.Errorf("node: cpu probe: %w", context.Cause(ctx)))
	}
	// The evidence counts only once the probe is quit, reaped and released
	// (its reclaim removes the scratch).
	stop, stopCancel := context.WithTimeout(context.WithoutCancel(ctx), grace+jailCleanupWait)
	defer stopCancel()
	if err := proc.Stop(stop); err != nil || !proc.exited() {
		// The reservation, scratch and the reaper (the sole Wait) stay; the
		// private handle has no other owner once this error returns, so it
		// is detached here, exactly once.
		return CPUEvidence{}, errors.Join(fmt.Errorf("node: cpu probe %d did not exit; its reservation is kept", ev.PID), err, proc.Release())
	}
	if err := proc.releaseJail(stop); err != nil {
		return CPUEvidence{}, fmt.Errorf("node: cpu probe: release: %w", err)
	}
	// The probe is quit, reaped and released; a context that ended meanwhile
	// still fails the probe honestly.
	if err := ctx.Err(); err != nil {
		return CPUEvidence{}, fmt.Errorf("node: cpu probe: canceled after the probe was settled: %w", context.Cause(ctx))
	}
	return ev, nil
}

// settleRetainedProbe releases a probe reservation an earlier start left
// behind, only through the jailer's proofs; anything else under ProbeID, or
// an unprovable probe, fails startup untouched.
func settleRetainedProbe(ctx context.Context, runner probeRunner) error {
	rec, ok := runner.Record(jailer.ProbeID)
	if !ok {
		return nil
	}
	if rec.Kind != jailer.KindProbe || rec.SandboxID != jailer.ProbeID {
		return fmt.Errorf("node: cpu probe: retained %q record of kind %q is not a probe; it is kept", rec.SandboxID, rec.Kind)
	}
	if err := runner.ReleaseAttempt(ctx, rec.Ref()); err != nil {
		return fmt.Errorf("node: cpu probe: retained probe attempt %s is kept: %w", rec.Attempt, err)
	}
	if again, ok := runner.Record(jailer.ProbeID); ok {
		return fmt.Errorf("node: cpu probe: probe attempt %s is retained", again.Attempt)
	}
	return nil
}

// failJailedProbe kills a post-spawn failure and settles it when it can: the
// reservation is released only after the reap is observed; otherwise the
// reservation, scratch and the reaper (the sole Wait) are kept and the
// private handle is detached exactly once.
func failJailedProbe(ctx context.Context, proc *jailedProcess, grace time.Duration, cause error) error {
	killErr := proc.kill()
	wait := time.NewTimer(grace)
	defer wait.Stop()
	select {
	case <-proc.done:
	case <-wait.C:
		// The reservation, scratch and the reaper stay; the private handle
		// is detached here, exactly once, since the caller gets no process.
		return errors.Join(cause, killErr, fmt.Errorf("node: cpu probe %d was not proven dead; its reservation is kept", proc.PID()), proc.Release())
	}
	release, cancel := context.WithTimeout(context.WithoutCancel(ctx), jailSettleTimeout)
	defer cancel()
	if err := proc.releaseJail(release); err != nil {
		return errors.Join(cause, fmt.Errorf("node: cpu probe: release: %w", err))
	}
	return cause
}

// jailerProbeRunner adapts *jailer.Manager to probeRunner.
type jailerProbeRunner struct{ m *jailer.Manager }

func (r jailerProbeRunner) StartProbe(ctx context.Context, spec jailer.ProbeSpec) (jailRun, error) {
	child, err := r.m.StartProbe(ctx, spec)
	if child == nil {
		return nil, err
	}
	return childRun{child}, err
}

func (r jailerProbeRunner) Record(id string) (jailer.Record, bool) { return r.m.Record(id) }

func (r jailerProbeRunner) ReleaseAttempt(ctx context.Context, ref jailer.Ref) error {
	return r.m.ReleaseAttempt(ctx, ref)
}
