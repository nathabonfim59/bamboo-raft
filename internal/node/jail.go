package node

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

// jailRunner is the manager's view of the jailer: production wraps
// *jailer.Manager (jailerRunner); tests substitute a fake backed by real
// child processes so the sole-Wait and kill semantics stay genuine.
type jailRunner interface {
	// Start launches spec. A nil jailRun proves no process was spawned; the
	// attempt's durable reservation may still be retained.
	Start(ctx context.Context, spec jailer.LaunchSpec) (jailRun, error)
	// Record returns the id's current reservation, of any attempt.
	Record(id string) (jailer.Record, bool)
	// ReleaseAttempt releases exactly ref by proof (jailer.ReleaseAttempt).
	ReleaseAttempt(ctx context.Context, ref jailer.Ref) error
	// VerifyProcess proves a recovered attempt's live process.
	VerifyProcess(ctx context.Context, expected jailer.Record) error
	// Records lists every retained reservation, of every kind.
	Records() []jailer.Record
	// SettleTools settles earlier processes' tool records by proof; New
	// runs it after the store lock and before any storage cleanup.
	SettleTools(ctx context.Context) (jailer.ToolReport, error)
	// TargetHolder returns the attempt holding an inode (tool or VM).
	TargetHolder(dev, ino uint64) (jailer.Ref, bool)
}

// jailRun is one launched jailer child. Wait is the sole consuming wait:
// only jailedProcess's reaper calls it, exactly once.
type jailRun interface {
	PID() int
	Wait() error
	Identity() jailer.Record
	Kill() error
	// Release proves death and an empty owned cgroup, runs the reclaim
	// policy and releases the reservation, then closes the handle.
	Release(ctx context.Context) error
	// Detach closes only the handle; the reservation stays durable.
	Detach() error
}

// jailerRunner adapts *jailer.Manager to jailRunner.
type jailerRunner struct{ m *jailer.Manager }

func (r jailerRunner) Start(ctx context.Context, spec jailer.LaunchSpec) (jailRun, error) {
	child, err := r.m.Start(ctx, spec)
	if child == nil {
		// Never a typed nil: an absent child is a nil interface.
		return nil, err
	}
	return childRun{child}, err
}

func (r jailerRunner) Record(id string) (jailer.Record, bool) { return r.m.Record(id) }

func (r jailerRunner) Records() []jailer.Record { return r.m.Records() }

func (r jailerRunner) ReleaseAttempt(ctx context.Context, ref jailer.Ref) error {
	return r.m.ReleaseAttempt(ctx, ref)
}

func (r jailerRunner) VerifyProcess(ctx context.Context, expected jailer.Record) error {
	return r.m.VerifyProcess(ctx, expected)
}

func (r jailerRunner) SettleTools(ctx context.Context) (jailer.ToolReport, error) {
	return r.m.SettleTools(ctx)
}

func (r jailerRunner) TargetHolder(dev, ino uint64) (jailer.Ref, bool) {
	return r.m.TargetHolder(dev, ino)
}

type childRun struct{ c *jailer.Child }

func (c childRun) PID() int                          { return c.c.Cmd.Process.Pid }
func (c childRun) Wait() error                       { return c.c.Cmd.Wait() }
func (c childRun) Identity() jailer.Record           { return c.c.Identity() }
func (c childRun) Kill() error                       { return c.c.Kill() }
func (c childRun) Release(ctx context.Context) error { return c.c.Release(ctx) }
func (c childRun) Detach() error                     { return c.c.Detach() }

// jailQMPDial connects to a jailed VM's QMP socket and proves its peer is
// pid (SO_PEERCRED): QEMU is the jailer's direct same-host-PID child. done is
// the process's reaper channel; a QEMU that exits first ends the dial.
type jailQMPDial func(ctx context.Context, path string, pid int, done <-chan struct{}) (*qmp.Client, error)

// errJailConflict reports a retained jail record this VM's current attempt
// does not own: it is never settled or overwritten automatically.
var errJailConflict = errors.New("node: a retained jail reservation of another attempt exists")

// errJailHandleClosed reports a jailedProcess whose run handle was already
// released or detached; settlement then proceeds by attempt identity.
var errJailHandleClosed = errors.New("node: jailed process handle is closed")

// jailCleanupWait bounds the wait for a killed child's reap after a failed
// jailed startup, when the caller set none.
const jailCleanupWait = 5 * time.Second

// jailLimits derives one VM's cgroup bounds with checked arithmetic: one
// CPU per vCPU, the guest memory plus max(256 MiB, guest/8) of QEMU
// overhead, and 256 + 2×vCPUs tasks.
func jailLimits(res storage.Resources, io jailer.IOLimits) (jailer.Limits, error) {
	if res.VCPUs == 0 || res.VCPUs > 10_000 {
		return jailer.Limits{}, fmt.Errorf("node: %d vCPUs cannot be expressed as a CPU quota", res.VCPUs)
	}
	if res.MemoryMiB == 0 || res.MemoryMiB > math.MaxUint64>>21 {
		return jailer.Limits{}, fmt.Errorf("node: %d MiB of memory overflows the memory limit", res.MemoryMiB)
	}
	guest := res.MemoryMiB << 20
	overhead := max(uint64(256)<<20, guest/8)
	limits := jailer.Limits{
		CPUQuotaPercent: res.VCPUs * 100,
		MemoryBytes:     guest + overhead,
		Pids:            256 + 2*res.VCPUs,
		IO:              io,
	}
	if err := limits.Validate(); err != nil {
		return jailer.Limits{}, err
	}
	return limits, nil
}

// argvDigest is the sha256 of argv exactly as /proc/pid/cmdline holds it:
// every element followed by a NUL.
func argvDigest(argv []string) string {
	h := sha256.New()
	for _, arg := range argv {
		h.Write([]byte(arg))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// buildJailSpec assembles the jailer launch for the fixed descriptor map
// (qemu.TapFD..qemu.OverlayFD): tap, vhost-net, kernel, initrd,
// vhost-vsock, base, overlay, with the explicit two-node disk chain.
func buildJailSpec(id, attempt, binary string, args []string, nic *qemu.Network, pins *storage.LaunchPins, vsock *os.File, limits jailer.Limits, stderr io.Writer) jailer.LaunchSpec {
	return jailer.LaunchSpec{
		SandboxID: id,
		Attempt:   attempt,
		Binary:    binary,
		Args:      args,
		Files: []jailer.Descriptor{
			{Name: jailer.TAPName, File: nic.Tap},
			{Name: jailer.VhostNetName, File: nic.Vhost},
			{Name: jailer.KernelName, File: pins.Kernel},
			{Name: jailer.InitrdName, File: pins.Initrd},
			{Name: jailer.VhostVsockName, File: vsock},
			{Name: "base", File: pins.Base},
			{Name: "overlay", File: pins.Overlay},
		},
		Disks:      []string{"base", "overlay"},
		ControlDir: pins.Control,
		SerialLog:  pins.Console,
		Limits:     limits,
		Stdout:     io.Discard,
		Stderr:     stderr,
	}
}

// jailedProcess is a jailed QEMU the manager started. Its reaper is the only
// consumer of jailRun.Wait; Done closes after that Wait returned. The jail
// reservation outlives Done and is settled separately (settleJail).
type jailedProcess struct {
	id     string
	ref    jailer.Ref
	log    *slog.Logger
	run    jailRun
	stderr *jailTail
	done   chan struct{}
	// stopGrace is how long Stop waits after QMP quit before killing.
	stopGrace time.Duration

	mu       sync.Mutex
	exitErr  error
	client   *qmp.Client
	handleUp bool // the run handle is neither released nor detached
}

func newJailedProcess(id string, ref jailer.Ref, run jailRun, log *slog.Logger, stderr *jailTail) *jailedProcess {
	p := &jailedProcess{id: id, ref: ref, log: log, run: run, stderr: stderr, done: make(chan struct{}), handleUp: true}
	go p.reap()
	return p
}

// reap is the sole consuming Wait of the jailer child.
func (p *jailedProcess) reap() {
	err := p.run.Wait()
	var wrapped error
	if err != nil {
		wrapped = fmt.Errorf("qemu: %s: %w%s", p.id, err, p.stderr.suffix())
	}
	p.mu.Lock()
	p.exitErr = wrapped
	client := p.client
	p.mu.Unlock()
	if client != nil {
		client.Close()
	}
	close(p.done)
}

func (p *jailedProcess) PID() int              { return p.run.PID() }
func (p *jailedProcess) Done() <-chan struct{} { return p.done }

func (p *jailedProcess) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exitErr
}

func (p *jailedProcess) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// kill signals the child through the jailer handle while it is held.
func (p *jailedProcess) kill() error {
	p.mu.Lock()
	up := p.handleUp
	p.mu.Unlock()
	if !up || p.exited() {
		return nil
	}
	return p.run.Kill()
}

// Stop asks QEMU to quit through QMP, waits up to the grace, then kills it
// through the jailer handle and waits for the reap, bounded independently of
// ctx. It never releases the jail reservation.
func (p *jailedProcess) Stop(ctx context.Context) error {
	if p.exited() {
		return nil
	}
	if client := p.QMP(); client != nil {
		quitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_ = client.Execute(quitCtx, "quit", nil, nil)
		cancel()
	}
	grace := time.NewTimer(p.stopGrace)
	defer grace.Stop()
	select {
	case <-p.done:
		return nil
	case <-grace.C:
	case <-ctx.Done():
	}
	killErr := p.kill()
	wait := time.NewTimer(jailCleanupWait)
	defer wait.Stop()
	select {
	case <-p.done:
	case <-wait.C:
		return errors.Join(fmt.Errorf("qemu: %s: process %d did not exit after SIGKILL", p.id, p.PID()), killErr)
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	return nil
}

// Close releases the QMP connection without stopping QEMU.
func (p *jailedProcess) Close() error {
	if client := p.QMP(); client != nil {
		return client.Close()
	}
	return nil
}

// QMP exposes the verified control connection.
func (p *jailedProcess) QMP() *qmp.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client
}

func (p *jailedProcess) setClient(c *qmp.Client) {
	p.mu.Lock()
	p.client = c
	p.mu.Unlock()
}

// Pause and Resume use the shared strict QMP validators.
func (p *jailedProcess) Pause(ctx context.Context) error  { return qemu.Pause(ctx, p.QMP(), p.id) }
func (p *jailedProcess) Resume(ctx context.Context) error { return qemu.Resume(ctx, p.QMP(), p.id) }

// Release is the explicit owner-shutdown hook: it detaches the handle and
// keeps the reservation durable for the next node process.
func (p *jailedProcess) Release() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.handleUp {
		return nil
	}
	p.handleUp = false
	return p.run.Detach()
}

// releaseJail releases the reservation through the held handle after the
// reap. Whatever the result the handle is closed exactly once: by a
// successful Release, otherwise by Detach, after which settlement proceeds by
// attempt identity.
func (p *jailedProcess) releaseJail(ctx context.Context) error {
	if !p.exited() {
		return fmt.Errorf("node: %s: jail release before the QEMU reap", p.id)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.handleUp {
		return errJailHandleClosed
	}
	p.handleUp = false
	if err := p.run.Release(ctx); err != nil {
		return errors.Join(err, p.run.Detach())
	}
	return nil
}

// jailTail keeps the last bytes of a jailed QEMU's stderr.
type jailTail struct {
	mu   sync.Mutex
	data []byte
}

func (b *jailTail) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)
	if len(b.data) > 16<<10 {
		b.data = slices.Clone(b.data[len(b.data)-16<<10:])
	}
	return len(p), nil
}

func (b *jailTail) suffix() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s := strings.TrimSpace(string(b.data)); s != "" {
		return ": " + s
	}
	return ""
}

// jailLaunchConfig is launchConfig plus the admitted CPU facts: a jailed
// launch never re-detects or re-resolves the CPU the startup probe proved.
func (m *Manager) jailLaunchConfig(meta storage.VMMeta, nic *qemu.Network) qemu.Config {
	cfg := m.launchConfig(meta, nic)
	if m.cfg.HostCPU != nil {
		host := *m.cfg.HostCPU
		cfg.HostCPU = &host
	}
	return cfg
}

func (m *Manager) openJailVsock() (*os.File, error) {
	if m.jailVsock != nil {
		return m.jailVsock()
	}
	return os.OpenFile(qemu.VsockDevice, os.O_RDWR|syscall.O_CLOEXEC, 0)
}

func (m *Manager) cleanupWait() time.Duration {
	if m.jailCleanup > 0 {
		return m.jailCleanup
	}
	return jailCleanupWait
}

// launchJailed runs one jailed launch for v inside its boot worker (the VM
// operation is owned; QEMU is proven absent and any previous jail released).
// It returns nil only when no process exists and its reservation is settled
// or was never spawned; a non-nil process with an error must be attached and
// finished by the caller. Every pinned descriptor is closed before return,
// and immediately after the jailer call on both outcomes.
func (m *Manager) launchJailed(ctx context.Context, v *vm, img storage.Image, nic *qemu.Network) (VMProcess, error) {
	if nic == nil {
		return nil, errors.New("node: jailed launches require TAP networking")
	}
	if rec, ok := m.jail.Record(v.id); ok {
		return nil, fmt.Errorf("%w: %s attempt %s", errJailConflict, v.id, rec.Attempt)
	}
	// A maintenance QEMU killed mid-session leaves its NBD socket, which
	// the jailer's control-directory check refuses.
	if err := m.store.RemovePhase2Scratch(v.id); err != nil {
		return nil, err
	}
	v.mu.Lock()
	meta := v.meta
	v.mu.Unlock()
	limits, err := jailLimits(meta.Resources, m.cfg.JailIO)
	if err != nil {
		return nil, err
	}
	args, err := m.jailLaunchConfig(meta, nic).JailedArgs()
	if err != nil {
		return nil, err
	}
	pins, err := m.store.PinLaunch(ctx, v.id, img)
	if err != nil {
		return nil, fmt.Errorf("pin launch resources: %w", err)
	}
	defer pins.Close()
	vsock, err := m.openJailVsock()
	if err != nil {
		return nil, fmt.Errorf("open vhost-vsock: %w", err)
	}
	defer vsock.Close()
	attempt := uuid.NewV7().String()
	digest := argvDigest(append([]string{m.cfg.JailBinary}, args...))
	if err := m.store.WriteJailedLaunchIntent(ctx, v.id, attempt, digest); err != nil {
		return nil, fmt.Errorf("record launch intent: %w", err)
	}
	ref := jailer.Ref{SandboxID: v.id, Attempt: attempt}
	v.mu.Lock()
	v.jail = ref
	v.mu.Unlock()
	stderr := &jailTail{}
	run, err := m.jail.Start(ctx, buildJailSpec(v.id, attempt, m.cfg.JailBinary, args, nic, pins, vsock, limits, stderr))
	// QEMU holds its own references now; the node's copies close here on
	// both outcomes (the deferred closes are then no-ops).
	closeErr := errors.Join(pins.Close(), vsock.Close())
	if closeErr != nil && err == nil {
		m.log.Warn("node: close jailed launch descriptors", "sandbox", v.id, "error", closeErr)
	}
	if run == nil {
		if errors.Is(err, jailer.ErrRetained) {
			// Our attempt never got a record: the retained one is foreign
			// and stays unclaimed.
			v.mu.Lock()
			if v.jail == ref {
				v.jail = jailer.Ref{}
			}
			v.mu.Unlock()
		}
		return nil, fmt.Errorf("jail: %w", err)
	}
	proc := newJailedProcess(v.id, ref, run, m.log, stderr)
	proc.stopGrace = cmp.Or(m.jailStopGrace, qemu.DefaultStopGrace)
	if err != nil {
		return m.jailStartupFailure(v, proc, fmt.Errorf("jail: %w", err))
	}
	dial := m.jailDial
	if dial == nil {
		dial = dialJailQMP
	}
	qmpPath := filepath.Join(m.store.VMDir(v.id), storage.ControlDirName, qemu.QMPFilename)
	client, err := dial(ctx, qmpPath, run.PID(), proc.done)
	if err != nil {
		return m.jailStartupFailure(v, proc, err)
	}
	proc.setClient(client)
	if err := ctx.Err(); err != nil {
		// A proven connection is not success once the startup context
		// ended: the owned failure path kills, reaps and settles.
		client.Close()
		return m.jailStartupFailure(v, proc, fmt.Errorf("qemu: %s: startup canceled: %w", v.id, context.Cause(ctx)))
	}
	if proc.exited() {
		client.Close()
		return m.jailStartupFailure(v, proc, fmt.Errorf("qemu: %s exited during startup: %w", v.id, proc.Err()))
	}
	return proc, nil
}

// jailStartupFailure kills a post-spawn failure and settles it when it can:
// nil only when the reap AND the jail release are proven.
func (m *Manager) jailStartupFailure(v *vm, proc *jailedProcess, cause error) (VMProcess, error) {
	killErr := proc.kill()
	wait := time.NewTimer(m.cleanupWait())
	defer wait.Stop()
	select {
	case <-proc.done:
	case <-wait.C:
		return proc, errors.Join(cause, killErr, fmt.Errorf("node: %s: cleanup after failed jailed startup did not prove the child dead", v.id))
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(m.ctx), m.cleanupWait())
	defer cancel()
	if err := m.settleJail(ctx, v, proc); err != nil {
		return proc, errors.Join(cause, err)
	}
	return nil, cause
}

// dialJailQMP waits for the jailed QEMU's QMP socket and proves its peer.
func dialJailQMP(ctx context.Context, path string, pid int, done <-chan struct{}) (*qmp.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, qemu.DefaultQMPTimeout)
	defer cancel()
	var lastErr error
	for {
		if _, err := os.Lstat(path); err == nil {
			client, err := qmp.Dial(ctx, path, slog.New(slog.DiscardHandler))
			if err == nil {
				peer, perr := client.PeerPID()
				if perr == nil && peer == pid {
					return client, nil
				}
				client.Close()
				if perr == nil {
					perr = fmt.Errorf("QMP socket is owned by pid %d, not child pid %d", peer, pid)
				}
				return nil, fmt.Errorf("qemu: verify QMP peer: %w", perr)
			}
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("qemu: QMP not ready: %w", errors.Join(context.Cause(ctx), lastErr))
		case <-done:
			return nil, errors.New("qemu: exited during startup")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// jailSettleTimeout bounds one settlement on the detached path.
const jailSettleTimeout = 30 * time.Second

// settleJailDetached runs settleJail on a bounded context detached from any
// request or shutdown, like releaseNetwork: a settlement that started after a
// proven death must finish or fail, never be abandoned by cancellation.
func (m *Manager) settleJailDetached(v *vm, proc VMProcess) error {
	ctx, cancel := context.WithTimeout(context.Background(), jailSettleTimeout)
	defer cancel()
	return m.settleJail(ctx, v, proc)
}

// settleJail releases v's jail reservation after its QEMU is proven dead,
// before network, accounting or durable record changes. A nil error means
// v holds no reservation; any error retains everything. A record of another
// attempt is a conflict, never success. Without a jailer it removes the QMP
// socket a non-jailed proc left behind (removeStaleQMPSocket).
func (m *Manager) settleJail(ctx context.Context, v *vm, proc VMProcess) error {
	if m.jail == nil {
		if proc != nil {
			m.removeStaleQMPSocket(v.id)
		}
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("node: settle jail %s: %w", v.id, context.Cause(ctx))
	}
	v.mu.Lock()
	ref := v.jail
	v.mu.Unlock()
	if ref == (jailer.Ref{}) {
		if rec, ok := m.jail.Record(v.id); ok {
			return fmt.Errorf("%w: %s attempt %s", errJailConflict, v.id, rec.Attempt)
		}
		return nil
	}
	err := errJailHandleClosed
	if jp, ok := proc.(*jailedProcess); ok && jp.ref == ref {
		err = jp.releaseJail(ctx)
	}
	if errors.Is(err, errJailHandleClosed) {
		err = m.jail.ReleaseAttempt(ctx, ref)
	}
	if err != nil {
		return fmt.Errorf("node: release jail %s attempt %s: %w", v.id, ref.Attempt, err)
	}
	v.mu.Lock()
	if v.jail == ref {
		v.jail = jailer.Ref{}
	}
	v.mu.Unlock()
	return nil
}
