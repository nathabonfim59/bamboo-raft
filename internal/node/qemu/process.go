package qemu

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
)

// StartOptions tunes process startup and shutdown. Zero values use the
// package defaults.
type StartOptions struct {
	// Logger receives lifecycle messages. Guest data and QEMU argv are never
	// logged.
	Logger *slog.Logger
	// QMPTimeout bounds the wait for QEMU's QMP greeting.
	QMPTimeout time.Duration
	// StopGrace is how long Stop waits after QMP quit before SIGKILL.
	StopGrace time.Duration
}

func (o StartOptions) withDefaults() StartOptions {
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.QMPTimeout <= 0 {
		o.QMPTimeout = DefaultQMPTimeout
	}
	if o.StopGrace <= 0 {
		o.StopGrace = DefaultStopGrace
	}
	return o
}

// Process owns one running QEMU child. Its lifetime is independent of the
// context passed to Start: that context only bounds startup. A background
// goroutine reaps the child exactly once; Done closes and Err reports the exit
// status.
//
// Stop and Kill are bounded and never leave an unreaped child: the background
// reaper owns Wait, and both methods signal the child as a fallback even when
// their own context has ended.
type Process struct {
	cfg       Config
	log       *slog.Logger
	cmd       *exec.Cmd
	stderr    *tailBuffer
	stopGrace time.Duration

	mu      sync.Mutex
	client  *qmp.Client
	exitErr error

	done chan struct{}
}

// Start validates cfg, resolves the CPU policy, launches QEMU as the current
// (unprivileged) user, waits for QMP, and proves the socket belongs to the
// child via SO_PEERCRED.
//
// Start is the only place automatic host CPU detection happens: when Config
// has no HostCPU and nested is not off, it calls DetectHostCPU once, injects
// the facts, and logs the choice. An injected HostCPU is honoured, which keeps
// tests deterministic. On an auto detection failure it logs and continues with
// empty facts (nested off); nested=on fails clearly.
//
// Start refuses a pre-existing QMP socket instead of connecting to or
// deleting another process's socket.
//
// Failure contract: a failure before the child is spawned returns a nil
// Process (nothing exists). A failure after the spawn returns either a nil
// Process — the child was killed and reaped, its absence proven — or the
// live *Process together with the error, when startupFailure could not prove
// the death within its bounded cleanup. A caller holding a returned process
// must finish it (Stop) before treating the machine as absent: only that
// handle observes the reap that turns the kill into proof.
func Start(ctx context.Context, cfg Config, opts StartOptions) (*Process, error) {
	opts = opts.withDefaults()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := checkUnprivileged(os.Geteuid()); err != nil {
		return nil, err
	}
	if err := cfg.checkFiles(); err != nil {
		return nil, err
	}

	// CPU policy: this is the only place automatic host detection happens.
	// An injected HostCPU (tests, or a caller that already probed) is honoured.
	mode := cfg.nestedOrDefault()
	if mode != NestedOff && cfg.HostCPU == nil {
		host, err := DetectHostCPU("")
		if err != nil {
			if mode == NestedOn {
				return nil, fmt.Errorf("qemu: %s: cpu-nested=on: detect host CPU: %w", cfg.SandboxID, err)
			}
			opts.Logger.Warn("qemu: host CPU detection failed; nested auto disabled",
				"sandbox", cfg.SandboxID, "error", err)
			host = HostCPU{}
		}
		cfg.HostCPU = &host
	}
	option, err := cfg.CPU()
	if err != nil {
		return nil, fmt.Errorf("qemu: %s: %w", cfg.SandboxID, err)
	}
	var host HostCPU
	if cfg.HostCPU != nil {
		host = *cfg.HostCPU
	}
	opts.Logger.Debug("qemu: cpu resolved",
		"sandbox", cfg.SandboxID, "model", option.Model, "nested", option.Nested,
		"vendor", host.Vendor, "vmx", host.Vmx, "svm", host.Svm, "cpus", host.CPUs)
	testHost := cfg.AllowHostCPU && cfg.CPUModel == "host"
	if testHost {
		opts.Logger.Info("qemu: test CPU model override",
			"sandbox", cfg.SandboxID, "model", option.Model, "nested", option.Nested)
	}
	if mode == NestedAuto && option.Nested == "" && !(testHost && host.Vendor == "AuthenticAMD") {
		switch host.Vendor {
		case "AuthenticAMD":
			opts.Logger.Info("qemu: nested virtualization disabled: AuthenticAMD host with the fixed Intel CPU model; in-guest kvm_amd requires an AMD CPUID vendor and +svm is not exposed in phase 0.5",
				"sandbox", cfg.SandboxID, "model", option.Model)
		case "GenuineIntel":
			opts.Logger.Info("qemu: nested virtualization disabled: not every CPU advertises vmx on this GenuineIntel host",
				"sandbox", cfg.SandboxID, "model", option.Model)
		default:
			opts.Logger.Info("qemu: nested virtualization disabled: host CPU vendor is unknown or mixed",
				"sandbox", cfg.SandboxID, "vendor", host.Vendor)
		}
	}

	argv, err := cfg.Args()
	if err != nil {
		return nil, err
	}
	if _, err := exec.LookPath(argv[0]); err != nil {
		return nil, fmt.Errorf("qemu: find %s: %w", argv[0], err)
	}

	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdout = io.Discard
	stderr := &tailBuffer{max: 16 << 10}
	cmd.Stderr = stderr
	// Inherited descriptors are exactly the pre-opened network pair, in the
	// order the fixed TapFD/VhostFD numbers require. The caller owns these
	// files and keeps them open until Start returns; QEMU holds its own
	// references afterwards.
	cmd.ExtraFiles = cfg.extraFiles()
	// Own process group: terminal signals aimed at the node's group do not
	// reach QEMU, and QEMU starts no descendants (sandbox spawn=deny).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("qemu: start %s: %w", cfg.SandboxID, err)
	}

	p := &Process{
		cfg:       cfg,
		log:       opts.Logger,
		cmd:       cmd,
		stderr:    stderr,
		stopGrace: opts.StopGrace,
		done:      make(chan struct{}),
	}
	go p.reap()

	client, err := p.connect(ctx, opts.QMPTimeout)
	if err != nil {
		return p.startupFailure(err, cleanupTimeout)
	}
	peer, err := client.PeerPID()
	if err != nil {
		client.Close()
		return p.startupFailure(fmt.Errorf("qemu: %s: verify QMP peer: %w", cfg.SandboxID, err), cleanupTimeout)
	}
	if peer != cmd.Process.Pid {
		client.Close()
		return p.startupFailure(fmt.Errorf("qemu: %s: QMP socket is owned by pid %d, not child pid %d", cfg.SandboxID, peer, cmd.Process.Pid), cleanupTimeout)
	}

	p.mu.Lock()
	p.client = client
	p.mu.Unlock()
	select {
	case <-p.done:
		client.Close()
		return nil, fmt.Errorf("qemu: %s exited during startup: %w%s", cfg.SandboxID, p.Err(), p.stderrSuffix())
	default:
	}
	p.log.Debug("qemu: started",
		"sandbox", cfg.SandboxID,
		"pid", cmd.Process.Pid,
		"qemu", client.Greeting().Version.String(),
	)
	return p, nil
}

// extraFiles returns the descriptors the child inherits, in the fixed order
// TapFD, VhostFD. A nil Config.Network inherits nothing (the dev/test
// user-mode fallback). The jailed launcher (A9) appends its resources after
// these two and must not renumber them.
func (c Config) extraFiles() []*os.File {
	if c.Network == nil {
		return nil
	}
	return []*os.File{c.Network.Tap, c.Network.Vhost}
}

func (p *Process) connect(ctx context.Context, timeout time.Duration) (*qmp.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastErr error
	for {
		select {
		case <-p.done:
			return nil, fmt.Errorf("qemu: %s exited during startup: %w%s", p.cfg.SandboxID, p.Err(), p.stderrSuffix())
		default:
		}

		if _, err := os.Lstat(p.cfg.QMPPath()); err == nil {
			client, err := qmp.Dial(ctx, p.cfg.QMPPath(), p.log)
			if err == nil {
				return client, nil
			}
			lastErr = err
		} else if !errors.Is(err, fs.ErrNotExist) {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return nil, fmt.Errorf("qemu: %s: QMP not ready: %w (last: %w)%s", p.cfg.SandboxID, context.Cause(ctx), lastErr, p.stderrSuffix())
			}
			return nil, fmt.Errorf("qemu: %s: QMP not ready: %w%s", p.cfg.SandboxID, context.Cause(ctx), p.stderrSuffix())
		case <-p.done:
			return nil, fmt.Errorf("qemu: %s exited during startup: %w%s", p.cfg.SandboxID, p.Err(), p.stderrSuffix())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (p *Process) reap() {
	err := p.cmd.Wait()
	var wrapped error
	if err != nil {
		wrapped = fmt.Errorf("qemu: %s: %w", p.cfg.SandboxID, err)
	}

	p.mu.Lock()
	p.exitErr = wrapped
	client := p.client
	p.mu.Unlock()
	if client != nil {
		client.Close()
	}
	close(p.done)

	if wrapped != nil {
		p.log.Warn("qemu: exited", "sandbox", p.cfg.SandboxID, "error", wrapped, "stderr", p.stderrSuffix())
	} else {
		p.log.Debug("qemu: exited", "sandbox", p.cfg.SandboxID)
	}
}

func (p *Process) stderrSuffix() string {
	tail := strings.TrimSpace(p.stderr.String())
	if tail == "" {
		return ""
	}
	return ": " + tail
}

// startupFailure settles a failure after the child was spawned. It kills the
// child and waits for the exit and the reap to complete within wait. When
// they do, the child's absence is proven and the original cause is returned
// alone: the caller needs no handle. When they do not, the Process itself is
// returned with the cause joined to a cleanup error: the child may still be
// running, and only this handle can finish and prove its death (its Stop
// re-kills and waits; Done closes only after the reap). Start is the only
// production caller; tests stage the unproven outcome deterministically by
// holding the reaper back so Done never closes within the wait — a short
// wait alone is not proof, because the real reaper could win the race.
func (p *Process) startupFailure(cause error, wait time.Duration) (*Process, error) {
	p.signalKill()
	if err := p.waitExit(context.Background(), wait); err != nil {
		return p, errors.Join(
			fmt.Errorf("qemu: %s: cleanup after failed startup did not prove the child dead within %s", p.cfg.SandboxID, wait),
			cause, err)
	}
	return nil, cause
}

// PID returns the QEMU process id.
func (p *Process) PID() int { return p.cmd.Process.Pid }

// QMP returns the QMP client. It remains non-nil after the process exits but
// its Execute calls then fail with qmp.ErrClosed.
func (p *Process) QMP() *qmp.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client
}

// QMPPath returns this VM's QMP socket path.
func (p *Process) QMPPath() string { return p.cfg.QMPPath() }

// SerialPath returns this VM's serial log path.
func (p *Process) SerialPath() string { return p.cfg.SerialPath() }

// Done is closed when QEMU has exited and been reaped.
func (p *Process) Done() <-chan struct{} { return p.done }

// Err reports the exit status after Done is closed, and nil while running.
func (p *Process) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exitErr
}

// Wait blocks until QEMU has exited and been reaped.
func (p *Process) Wait() error {
	<-p.done
	return p.Err()
}

func (p *Process) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// Stop asks QEMU to quit through QMP, waits up to StopGrace, then SIGKILLs the
// child. It always reaps. The returned error reports why QEMU had to be
// killed; a QEMU that exited normally (any status) is not an error.
func (p *Process) Stop(ctx context.Context) error {
	if p.exited() {
		return nil
	}
	if client := p.QMP(); client != nil {
		quitCtx, cancel := context.WithTimeout(ctx, min(p.stopGrace, 3*time.Second))
		err := client.Execute(quitCtx, "quit", nil, nil)
		cancel()
		if err != nil && ctx.Err() == nil {
			// QEMU documents that a premature EOF after quit is expected.
			p.log.Debug("qemu: quit did not answer", "sandbox", p.cfg.SandboxID, "error", err)
		}
	}

	err := p.waitExit(ctx, p.stopGrace)
	if err == nil {
		return nil
	}

	p.log.Warn("qemu: forcing kill", "sandbox", p.cfg.SandboxID, "error", err)
	// The caller's context may already be gone; cleanup must still finish.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	p.signalKill()
	killErr := p.waitExit(cleanupCtx, cleanupTimeout)
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if killErr == nil {
		// The fallback is part of Stop's contract: the VM is gone.
		return nil
	}
	return errors.Join(err, killErr)
}

// Kill sends SIGKILL and waits, bounded by ctx. The signal is sent even when
// ctx is already canceled.
func (p *Process) Kill(ctx context.Context) error {
	p.signalKill()
	return p.waitExit(ctx, cleanupTimeout)
}

// Close releases the QMP connection. It does not stop QEMU: call Stop or Kill
// first. It is safe to call more than once.
func (p *Process) Close() error {
	if client := p.QMP(); client != nil {
		return client.Close()
	}
	return nil
}

func (p *Process) signalKill() {
	if p.exited() {
		return
	}
	// os.Process uses a pidfd on Linux where available, so a reaped child's
	// reused pid cannot be signalled. QEMU runs with -sandbox spawn=deny and
	// starts no descendants; the process group (Setpgid) still isolates it
	// from the node's group.
	if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		p.log.Warn("qemu: kill", "sandbox", p.cfg.SandboxID, "error", err)
	}
}

func (p *Process) waitExit(ctx context.Context, grace time.Duration) error {
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-p.done:
		return nil
	case <-timer.C:
		return fmt.Errorf("qemu: %s did not exit within %s", p.cfg.SandboxID, grace)
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Pause delegates to the shared strict QMP validator so owned, adopted and
// rescue controllers all enforce the same stop/status/io-status contract.
func (p *Process) Pause(ctx context.Context) error {
	return Pause(ctx, p.QMP(), p.cfg.SandboxID)
}

// Resume delegates to the shared strict QMP validator.
func (p *Process) Resume(ctx context.Context) error {
	return Resume(ctx, p.QMP(), p.cfg.SandboxID)
}

type queryStatus struct {
	Running bool   `json:"running"`
	Status  string `json:"status"`
}

type queryBlockEntry struct {
	Device   string `json:"device"`
	IOStatus string `json:"io-status"`
	Inserted *struct {
		NodeName string `json:"node-name"`
	} `json:"inserted"`
}

// tailBuffer keeps the last max bytes written to QEMU's stderr.
type tailBuffer struct {
	mu   sync.Mutex
	data []byte
	max  int
}

// Write implements io.Writer; it never fails.
func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)
	if len(b.data) > b.max {
		b.data = append([]byte(nil), b.data[len(b.data)-b.max:]...)
	}
	return len(p), nil
}

// String returns the retained stderr tail.
func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.data)
}
