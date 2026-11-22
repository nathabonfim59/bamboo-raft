package qemu

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
)

// CPUProbeOptions configures the CPU-only QEMU realization probe. Zero-value
// fields use the documented probe defaults: DefaultBinary, KVMDevice, a 20s
// timeout and a 5s per-step grace. WorkDir uses a fresh temporary directory.
type CPUProbeOptions struct {
	// Binary is the QEMU executable; empty means DefaultBinary.
	Binary string
	// KVMDevice is the KVM device that must be writable; empty means KVMDevice.
	KVMDevice string
	// WorkDir holds the probe's QMP socket. Empty creates a temporary dir.
	WorkDir string
	// Timeout bounds startup, QMP handshake and queries. Zero means 20s.
	Timeout time.Duration
	// Grace bounds each quit/TERM/KILL/group-empty step. Zero means 5s.
	Grace time.Duration
}

func (o CPUProbeOptions) withDefaults() CPUProbeOptions {
	o.Binary = cmp.Or(o.Binary, DefaultBinary)
	o.KVMDevice = cmp.Or(o.KVMDevice, KVMDevice)
	if o.Timeout <= 0 {
		o.Timeout = 20 * time.Second
	}
	if o.Grace <= 0 {
		o.Grace = 5 * time.Second
	}
	return o
}

// CPUProbeEvidence is what a successful CPU-only probe proved through a
// verified QMP peer: an idle prelaunch/paused machine with one realized CPU.
type CPUProbeEvidence struct {
	Status      string
	CPUs        int
	QEMUVersion string
	PID         int
	// Phase2 is the probe QEMU's Phase 2 schema check (CheckPhase2Schema),
	// nil when the schema could not be read: the node then checks on the
	// first VM's QMP connection instead.
	Phase2 *Phase2Support
}

// probeStatus is query-status's StatusInfo (QEMU qapi/run-state.json). Pointer
// fields distinguish a missing reply field from a zero value.
type probeStatus struct {
	Running *bool  `json:"running"`
	Status  string `json:"status"`
}

// probeCPUFast is one entry of query-cpus-fast's return. Every field must be
// present before the probe accepts a realized CPU.
type probeCPUFast struct {
	CPUIndex *int    `json:"cpu-index"`
	ThreadID *int64  `json:"thread-id"`
	QOMPath  *string `json:"qom-path"`
}

// probeCPUsRealized validates exactly one fully realized CPU, matching -smp 1.
func probeCPUsRealized(cpus []probeCPUFast) error {
	if len(cpus) != 1 {
		return fmt.Errorf("probe: query-cpus-fast returned %d CPUs, want exactly 1 (the probe launches -smp 1)", len(cpus))
	}
	cpu := cpus[0]
	switch {
	case cpu.CPUIndex == nil || *cpu.CPUIndex < 0:
		return errors.New("probe: query-cpus-fast entry has no cpu-index")
	case cpu.ThreadID == nil || *cpu.ThreadID <= 0:
		return errors.New("probe: query-cpus-fast entry has no host thread-id")
	case cpu.QOMPath == nil || *cpu.QOMPath == "":
		return errors.New("probe: query-cpus-fast entry has no qom-path")
	}
	return nil
}

// probeAcceptsStatus implements the pinned QEMU 8.2.2 run-state contract for a
// -S CPU-only probe: prelaunch or paused, both with running=false.
func probeAcceptsStatus(status string, running *bool) bool {
	return running != nil && !*running && (status == "prelaunch" || status == "paused")
}

const probeAcceptedStates = `prelaunch or paused with running=false (qemu 8.2.2 qapi/run-state.json: "prelaunch: QEMU was started with -S and guest has not started", "paused: guest has been paused via the 'stop' command")`

// probeArgv builds the CPU-only QEMU command line: the node's machine, CPU and
// isolation flags without kernel, initrd, disk or network, plus -S so nothing
// boots.
func probeArgv(binary string, option CPUOption, socket string) []string {
	return []string{
		binary,
		"-name", "guest=bbraft-cpu-probe,process=bbraft-cpu-probe",
		"-machine", "q35,accel=kvm",
		"-cpu", option.String(),
		"-S",
		"-nodefaults",
		"-no-user-config",
		"-no-reboot",
		"-m", "128M",
		"-smp", "1",
		"-qmp", "unix:" + socket + ",server=on,wait=off",
		"-display", "none",
		"-monitor", "none",
		"-serial", "none",
		"-sandbox", "on",
	}
}

// ProbeCPU launches one unprivileged CPU-only paused QEMU, proves through QMP
// that the child owns the socket, the machine is idle with one realized CPU,
// then stops it with bounded independent cleanup. It never boots a guest and
// never uses a kernel, initrd, disk or network. Parent cancellation is honored
// after cleanup and never reports success.
func ProbeCPU(ctx context.Context, option CPUOption, opts CPUProbeOptions) (CPUProbeEvidence, error) {
	opts = opts.withDefaults()
	if option.Model == "" {
		return CPUProbeEvidence{}, errors.New("qemu: cpu probe: resolved CPU option is empty")
	}
	if err := checkUnprivileged(os.Geteuid()); err != nil {
		return CPUProbeEvidence{}, err
	}
	if err := checkDevices(opts.KVMDevice); err != nil {
		return CPUProbeEvidence{}, err
	}
	if err := ctx.Err(); err != nil {
		return CPUProbeEvidence{}, context.Cause(ctx)
	}

	dir := opts.WorkDir
	cleanupDir := func() {}
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "bbraft-cpu-probe-")
		if err != nil {
			return CPUProbeEvidence{}, fmt.Errorf("qemu: cpu probe: create work dir: %w", err)
		}
		cleanupDir = func() { _ = os.RemoveAll(dir) }
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return CPUProbeEvidence{}, fmt.Errorf("qemu: cpu probe: create work dir %s: %w", dir, err)
	}
	defer cleanupDir()
	socket := filepath.Join(dir, "qmp.sock")
	if _, err := os.Lstat(socket); err == nil {
		return CPUProbeEvidence{}, fmt.Errorf("qemu: cpu probe: %s already exists; refusing to reuse another process's socket", socket)
	} else if !errors.Is(err, os.ErrNotExist) {
		return CPUProbeEvidence{}, fmt.Errorf("qemu: cpu probe: inspect %s: %w", socket, err)
	}

	probeCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	argv := probeArgv(opts.Binary, option, socket)
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdout = io.Discard
	stderr := &tailBuffer{max: 16 << 10}
	cmd.Stderr = stderr
	// Own process group, like the node's QEMU children.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 2 * time.Second
	if err := cmd.Start(); err != nil {
		return CPUProbeEvidence{}, fmt.Errorf("qemu: cpu probe: start %s: %w", opts.Binary, err)
	}

	done := make(chan struct{})
	var exitErr error
	go func() {
		exitErr = cmd.Wait()
		close(done)
	}()

	client, err := probeDial(probeCtx, socket, done, &exitErr, stderr)
	if err != nil {
		return CPUProbeEvidence{}, errors.Join(err, probeStop(cmd, done, nil, opts.Grace))
	}
	peerVerified := false
	peer, err := client.PeerPID()
	if err != nil {
		err = fmt.Errorf("qemu: cpu probe: verify QMP peer: %w", err)
	} else if peer != cmd.Process.Pid {
		err = fmt.Errorf("qemu: cpu probe: QMP socket is owned by pid %d, not child pid %d", peer, cmd.Process.Pid)
	} else {
		peerVerified = true
	}
	var status probeStatus
	if err == nil {
		if qerr := client.Execute(probeCtx, "query-status", nil, &status); qerr != nil {
			err = fmt.Errorf("qemu: cpu probe: query-status: %w", qerr)
		} else if !probeAcceptsStatus(status.Status, status.Running) {
			running := "<missing>"
			if status.Running != nil {
				running = fmt.Sprintf("%t", *status.Running)
			}
			err = fmt.Errorf("qemu: cpu probe: query-status reported status=%q running=%s; want %s", status.Status, running, probeAcceptedStates)
		}
	}
	if err == nil {
		var cpus []probeCPUFast
		if qerr := client.Execute(probeCtx, "query-cpus-fast", nil, &cpus); qerr != nil {
			err = fmt.Errorf("qemu: cpu probe: query-cpus-fast: %w", qerr)
		} else if cerr := probeCPUsRealized(cpus); cerr != nil {
			err = cerr
		}
	}

	// The Phase 2 capability gate reads the same binary's schema here, so
	// no extra QEMU is launched for it. A failed read is not a probe
	// failure: it only defers the check.
	var phase2 *Phase2Support
	if err == nil {
		if s, serr := CheckPhase2Schema(probeCtx, client); serr == nil {
			phase2 = &s
		}
	}

	// Only a verified peer receives quit.
	var stopClient *qmp.Client
	if peerVerified {
		stopClient = client
	}
	stopErr := probeStop(cmd, done, stopClient, opts.Grace)
	client.Close()
	if stopErr != nil {
		return CPUProbeEvidence{}, errors.Join(err, stopErr)
	}
	if err != nil {
		return CPUProbeEvidence{}, err
	}
	if rootErr := ctx.Err(); rootErr != nil {
		return CPUProbeEvidence{}, fmt.Errorf("qemu: cpu probe: interrupted before completion: %w", context.Cause(ctx))
	}
	return CPUProbeEvidence{
		Status:      status.Status,
		CPUs:        1,
		QEMUVersion: client.Greeting().Version.String(),
		PID:         cmd.Process.Pid,
		Phase2:      phase2,
	}, nil
}

// CheckProbeMachine proves through a verified QMP connection that a CPU-only
// probe machine is idle (prelaunch or paused, not running) with exactly one
// realized CPU. The jailed probe uses it; ProbeCPU applies the same rules.
func CheckProbeMachine(ctx context.Context, client *qmp.Client) (string, error) {
	var status probeStatus
	if err := client.Execute(ctx, "query-status", nil, &status); err != nil {
		return "", fmt.Errorf("qemu: cpu probe: query-status: %w", err)
	}
	if !probeAcceptsStatus(status.Status, status.Running) {
		return "", fmt.Errorf("qemu: cpu probe: query-status reported status=%q; want %s", status.Status, probeAcceptedStates)
	}
	var cpus []probeCPUFast
	if err := client.Execute(ctx, "query-cpus-fast", nil, &cpus); err != nil {
		return "", fmt.Errorf("qemu: cpu probe: query-cpus-fast: %w", err)
	}
	if err := probeCPUsRealized(cpus); err != nil {
		return "", err
	}
	return status.Status, nil
}

// probeDial waits for the QMP socket and performs the shared client's
// handshake, bounded by ctx. A child that exits first fails immediately with
// its stderr tail, preserving the exact missing-feature diagnostic.
func probeDial(ctx context.Context, socket string, done <-chan struct{}, exitErr *error, stderr *tailBuffer) (*qmp.Client, error) {
	var lastErr error
	for {
		select {
		case <-done:
			return nil, fmt.Errorf("qemu: cpu probe: qemu exited during startup: %w%s", *exitErr, probeStderrSuffix(stderr))
		default:
		}
		if _, err := os.Lstat(socket); err == nil {
			client, err := qmp.Dial(ctx, socket, slog.New(slog.DiscardHandler))
			if err == nil {
				return client, nil
			}
			lastErr = err
		} else if !errors.Is(err, os.ErrNotExist) {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return nil, fmt.Errorf("qemu: cpu probe: QMP not ready: %w (last: %w)%s", context.Cause(ctx), lastErr, probeStderrSuffix(stderr))
			}
			return nil, fmt.Errorf("qemu: cpu probe: QMP not ready: %w%s", context.Cause(ctx), probeStderrSuffix(stderr))
		case <-done:
			return nil, fmt.Errorf("qemu: cpu probe: qemu exited during startup: %w%s", *exitErr, probeStderrSuffix(stderr))
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// probeStop terminates the probe child and proves its process group is empty.
// Cleanup is independent and bounded: quit only through a verified peer, then
// graceful wait, SIGTERM, SIGKILL, final reap and group-empty polling.
func probeStop(cmd *exec.Cmd, done <-chan struct{}, client *qmp.Client, grace time.Duration) error {
	if client != nil {
		quitCtx, cancel := context.WithTimeout(context.Background(), min(grace, 3*time.Second))
		_ = client.Execute(quitCtx, "quit", nil, nil)
		cancel()
	}
	pgid := -cmd.Process.Pid
	select {
	case <-done:
	case <-time.After(grace):
		if err := signalGroup(pgid, syscall.SIGTERM); err != nil {
			return err
		}
	}
	select {
	case <-done:
	case <-time.After(grace):
		if err := signalGroup(pgid, syscall.SIGKILL); err != nil {
			return err
		}
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("qemu: cpu probe: child pid %d did not exit after SIGTERM and SIGKILL", cmd.Process.Pid)
	}
	return waitGroupEmpty(pgid, grace)
}

// signalGroup sends sig to the child's process group. A fully exited group is
// not an error.
func signalGroup(pgid int, sig syscall.Signal) error {
	if err := syscall.Kill(pgid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("qemu: cpu probe: %s process group %d: %w", sig, -pgid, err)
	}
	return nil
}

// waitGroupEmpty proves no process remains in the owned group.
func waitGroupEmpty(pgid int, grace time.Duration) error {
	var lastErr error
	rounds := []struct {
		sig    syscall.Signal
		sendIt bool
	}{
		{0, false},
		{syscall.SIGTERM, true},
		{syscall.SIGKILL, true},
		{syscall.SIGKILL, true},
	}
	for _, round := range rounds {
		if round.sendIt {
			if err := signalGroup(pgid, round.sig); err != nil {
				return err
			}
		}
		roundDeadline := time.Now().Add(grace)
		for {
			err := syscall.Kill(pgid, 0)
			if errors.Is(err, syscall.ESRCH) {
				return nil
			}
			if err != nil {
				lastErr = err
			}
			if time.Now().After(roundDeadline) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if lastErr != nil {
		return fmt.Errorf("qemu: cpu probe: process group %d still has live members after SIGTERM and SIGKILL: %w", -pgid, lastErr)
	}
	return fmt.Errorf("qemu: cpu probe: process group %d still has live members after SIGTERM and SIGKILL", -pgid)
}

func probeStderrSuffix(b *tailBuffer) string {
	tail := strings.TrimSpace(b.String())
	if tail == "" {
		return ""
	}
	return ": " + tail
}
