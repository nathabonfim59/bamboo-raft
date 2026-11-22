package node

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"path/filepath"
	"time"
	"uuid"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/nbd"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// Maintenance sessions (D3). A session is the VM's own QEMU (jailed in
// production) started paused (-S) on a disk no guest uses: no network, no
// vsock, no guest device, at most qemu.MaxMaintenanceMemoryMiB of memory. It
// is never continued. The node drives it over QMP and QEMU's NBD server and
// then quits it, so the persistent dirty bitmap is stored cleanly. SnapshotVM
// of a STOPPED VM and eager restore writes use it; a READY VM never does.
//
// A session holds the VM operation gate (acquireVMOperation) for its whole
// life, so it is serialized with StartVM's disk phase, StopVM, DeleteVM,
// SaveSnapshot and every later gate user. DeleteVM fences it like a boot:
// it cancels the session's context (vm.maintCancel) before waiting for the
// gate; the session then kills QEMU, settles the jail and returns.

var (
	// errMaintenanceFenced is the cancellation cause DeleteVM gives a
	// running session.
	errMaintenanceFenced = errors.New("node: maintenance session fenced by DeleteVM")
	// errMaintenanceShutdown is the cause when the manager shuts down.
	errMaintenanceShutdown = errors.New("node: maintenance session ended by node shutdown")
)

const (
	// maintQuitTimeout bounds the wait for a clean exit after QMP quit; a
	// QEMU still alive then is killed (and its bitmap is left in use).
	maintQuitTimeout = 10 * time.Second
	// maintTeardownTimeout bounds the NBD teardown and the quit command.
	maintTeardownTimeout = 5 * time.Second
)

// maintProcess is the QEMU of a session: a VMProcess with its verified QMP
// connection (*jailedProcess and *qemu.Process).
type maintProcess interface {
	VMProcess
	QMP() *qmp.Client
}

// maintSession is one running maintenance QEMU, handed to the session's fn.
// Its methods are not safe for concurrent use.
type maintSession struct {
	id   string
	proc maintProcess
	// nbdHost is the host path of the NBD socket, nbdServe the path QEMU
	// binds (the chroot path when jailed).
	nbdHost    string
	nbdServe   string
	nbdStarted bool
	clients    []*nbd.Client
}

// QMP returns the session's verified QMP connection.
func (s *maintSession) QMP() *qmp.Client { return s.proc.QMP() }

// PID is the maintenance QEMU's host pid.
func (s *maintSession) PID() int { return s.proc.PID() }

// StartNBD starts QEMU's NBD server on the session's unix socket
// (/control/nbd.sock when jailed) and returns its host path. Exports are
// added by the caller with block-export-add.
func (s *maintSession) StartNBD(ctx context.Context) (string, error) {
	if s.nbdStarted {
		return s.nbdHost, nil
	}
	args := map[string]any{"addr": map[string]any{"type": "unix", "data": map[string]any{"path": s.nbdServe}}}
	if err := s.QMP().Execute(ctx, "nbd-server-start", args, nil); err != nil {
		return "", fmt.Errorf("maintenance %s: nbd-server-start: %w", s.id, err)
	}
	s.nbdStarted = true
	info, err := os.Lstat(s.nbdHost)
	if err != nil {
		return "", fmt.Errorf("maintenance %s: nbd socket: %w", s.id, err)
	}
	if info.Mode().Type() != fs.ModeSocket {
		return "", fmt.Errorf("maintenance %s: %s is not a socket", s.id, s.nbdHost)
	}
	return s.nbdHost, nil
}

// DialNBD connects to an export of the session's NBD server. The peer
// credential (SO_PEERCRED) must be the maintenance QEMU's pid, before any
// caller-supplied CheckPeer runs. The session closes every client it dialed
// when it ends; the caller may close it earlier.
func (s *maintSession) DialNBD(ctx context.Context, opts nbd.Options) (*nbd.Client, error) {
	if !s.nbdStarted {
		return nil, fmt.Errorf("maintenance %s: NBD server is not started", s.id)
	}
	pid := s.proc.PID()
	extra := opts.CheckPeer
	opts.CheckPeer = func(c *net.UnixConn) error {
		peer, err := unixPeerPID(c)
		if err != nil {
			return err
		}
		if peer != pid {
			return fmt.Errorf("NBD socket is owned by pid %d, not maintenance QEMU pid %d", peer, pid)
		}
		if extra != nil {
			return extra(c)
		}
		return nil
	}
	client, err := nbd.Dial(ctx, s.nbdHost, opts)
	if err != nil {
		return nil, fmt.Errorf("maintenance %s: %w", s.id, err)
	}
	s.clients = append(s.clients, client)
	return client, nil
}

// StopNBD removes every block export (mode hard: remaining connections are
// dropped, so close or flush clients first), waits until none is left and
// stops the NBD server, which unlinks its socket. It is safe to call when
// the server was never started.
func (s *maintSession) StopNBD(ctx context.Context) error {
	client := s.QMP()
	var exports []struct {
		ID string `json:"id"`
	}
	if err := client.Execute(ctx, "query-block-exports", nil, &exports); err != nil {
		return fmt.Errorf("maintenance %s: query-block-exports: %w", s.id, err)
	}
	for _, e := range exports {
		if err := client.Execute(ctx, "block-export-del", map[string]any{"id": e.ID, "mode": "hard"}, nil); err != nil {
			return fmt.Errorf("maintenance %s: block-export-del %s: %w", s.id, e.ID, err)
		}
	}
	for len(exports) > 0 {
		if err := client.Execute(ctx, "query-block-exports", nil, &exports); err != nil {
			return fmt.Errorf("maintenance %s: query-block-exports: %w", s.id, err)
		}
		if len(exports) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("maintenance %s: exports not deleted: %w", s.id, context.Cause(ctx))
		case <-time.After(20 * time.Millisecond):
		}
	}
	if s.nbdStarted {
		if err := client.Execute(ctx, "nbd-server-stop", nil, nil); err != nil {
			return fmt.Errorf("maintenance %s: nbd-server-stop: %w", s.id, err)
		}
		s.nbdStarted = false
	}
	return nil
}

func (s *maintSession) closeClients() {
	for _, c := range s.clients {
		_ = c.Close()
	}
	s.clients = nil
}

// unixPeerPID is the SO_PEERCRED pid of a connected unix socket.
func unixPeerPID(c *net.UnixConn) (int, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("peer credential: %w", err)
	}
	var cred *unix.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return 0, fmt.Errorf("peer credential: %w", err)
	}
	if credErr != nil {
		return 0, fmt.Errorf("peer credential: %w", credErr)
	}
	return int(cred.Pid), nil
}

// maintClaim is what a session claimed on its VM.
type maintClaim struct {
	// admittedMiB is the memory added to Manager.reserved for the session
	// (zero when the VM already holds its compute).
	admittedMiB uint64
	memoryMiB   uint64
	// stopped: the VM is STOPPED and stoppedAt is its durable stop time;
	// otherwise it is a create in progress (BOOTING, no QEMU).
	stopped   bool
	stoppedAt int64
}

// maintenance runs fn on a paused maintenance QEMU of VM v's disk and quits
// it. v must be STOPPED, or BOOTING with no QEMU (a create whose disk exists:
// eager restore). The session holds the VM operation gate throughout; the
// caller must not hold it. fn's context ends when ctx ends, the manager shuts
// down or DeleteVM fences the session.
//
// Memory: a STOPPED VM holds no compute, so min(256 MiB, the VM's memory) is
// admitted against the node's capacity for the session (UNAVAILABLE "node
// busy" when there is no room) and released afterwards; a creating VM already
// holds its whole reservation and admits nothing more. The cgroup memory
// limit is the same amount.
//
// Every path ends with QEMU gone: quit when the session ends normally (a
// clean exit stores the bitmap), kill when fenced or when quit does not
// finish within maintQuitTimeout; then the jail is settled and a stale NBD
// socket removed. A QEMU whose death cannot be proven, or a jail that cannot
// be settled, leaves a STOPPED VM FAILED with the handle kept for DeleteVM.
func (m *Manager) maintenance(ctx context.Context, v *vm, fn func(ctx context.Context, s *maintSession) error) error {
	if err := m.beginNodeOperation(); err != nil {
		return err
	}
	defer m.wg.Done()
	if err := acquireVMOperation(ctx, v); err != nil {
		return err
	}
	defer releaseVMOperation(v)
	// A record whose restart recovery could not prove ownership never gets a
	// maintenance QEMU: its disk must be proven at rest first.
	if err := m.requireResolved(v); err != nil {
		return err
	}

	sessCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stopShutdown := context.AfterFunc(m.ctx, func() { cancel(errMaintenanceShutdown) })
	defer stopShutdown()

	claim, err := m.claimMaintenance(v, cancel)
	if err != nil {
		return err
	}
	defer m.releaseMaintenance(v, claim)
	if claim.stopped {
		meta, err := m.store.ReadVM(v.id)
		if err != nil {
			return status.Errorf(codes.Internal, "maintenance %s: read manifest: %v", v.id, err)
		}
		claim.stoppedAt = cmp.Or(meta.StoppedAtUnixMs, m.now().UnixMilli())
	}

	s, err := m.launchMaintenance(sessCtx, v, claim)
	if err != nil {
		return maintenanceError(v.id, sessCtx, err)
	}
	fnErr := m.ensureVMBitmap(sessCtx, v.id, s.QMP())
	if fnErr != nil {
		fnErr = fmt.Errorf("maintenance %s: dirty bitmap: %w", v.id, fnErr)
	} else {
		fnErr = fn(sessCtx, s)
	}
	endErr := m.endMaintenance(v, s, claim, context.Cause(sessCtx))
	if fnErr != nil {
		if sessCtx.Err() != nil {
			return maintenanceError(v.id, sessCtx, errors.Join(fnErr, endErr))
		}
		// fn's own error reaches the caller unchanged (its codes and
		// errors.Is targets), with any teardown failure joined.
		return errors.Join(fnErr, endErr)
	}
	if endErr != nil {
		return maintenanceError(v.id, sessCtx, endErr)
	}
	if err := sessCtx.Err(); err != nil {
		return maintenanceError(v.id, sessCtx, err)
	}
	return nil
}

// maintenanceError maps a session failure to a status: a fence is ABORTED,
// a shutdown UNAVAILABLE, a caller cancellation keeps its context code, an
// error that already carries a status keeps it, anything else is INTERNAL.
func maintenanceError(id string, ctx context.Context, err error) error {
	switch cause := context.Cause(ctx); {
	case errors.Is(cause, errMaintenanceFenced):
		return status.Errorf(codes.Aborted, "maintenance %s: sandbox deleted during the session: %v", id, err)
	case errors.Is(cause, errMaintenanceShutdown):
		return status.Errorf(codes.Unavailable, "maintenance %s: node is shutting down: %v", id, err)
	case ctx.Err() != nil:
		return status.FromContextError(ctx.Err()).Err()
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Errorf(codes.Internal, "%v", err)
}

// claimMaintenance checks v's state under the documented lock order (m.mu,
// then v.mu), admits the session's memory and publishes the fence.
func (m *Manager) claimMaintenance(v *vm, cancel context.CancelCauseFunc) (maintClaim, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.deleting || m.vms[v.id] != v {
		return maintClaim{}, status.Errorf(codes.NotFound, "sandbox %s not found", v.id)
	}
	var claim maintClaim
	switch {
	case v.process != nil:
		return maintClaim{}, status.Errorf(codes.FailedPrecondition, "sandbox %s has a running QEMU; maintenance needs its disk at rest", v.id)
	case v.state == nodev1.VMState_VM_STATE_STOPPED:
		claim.stopped = true
	case v.state == nodev1.VMState_VM_STATE_BOOTING:
	default:
		return maintClaim{}, status.Errorf(codes.FailedPrecondition, "sandbox %s is %s; maintenance needs a STOPPED VM or a create in progress", v.id, vmStateName(v.state))
	}
	claim.memoryMiB = qemu.MaintenanceMemoryMiB(v.meta.Resources.MemoryMiB)
	if !v.computeHeld {
		total, ok := m.reserved.add(resources{memoryMib: claim.memoryMiB})
		if !ok || !total.fits(m.capacity) {
			return maintClaim{}, status.Errorf(codes.Unavailable, "node busy: no memory for a maintenance session of sandbox %s", v.id)
		}
		m.reserved = total
		claim.admittedMiB = claim.memoryMiB
	}
	v.maintCancel = cancel
	return claim, nil
}

// releaseMaintenance returns the admitted memory and retires the fence.
func (m *Manager) releaseMaintenance(v *vm, claim maintClaim) {
	m.mu.Lock()
	m.reserved.memoryMib = subClamp(m.reserved.memoryMib, claim.admittedMiB)
	v.mu.Lock()
	v.maintCancel = nil
	v.mu.Unlock()
	m.mu.Unlock()
}

// launchMaintenance starts the session's QEMU. On error nothing is left
// running and the jail is settled, or v is marked failed with the handle
// kept (failMaintenance).
func (m *Manager) launchMaintenance(ctx context.Context, v *vm, claim maintClaim) (*maintSession, error) {
	v.mu.Lock()
	meta := v.meta
	v.mu.Unlock()
	img, ok := m.images[meta.Image]
	if !ok {
		return nil, status.Errorf(codes.Internal, "sandbox %s: image %q disappeared", v.id, meta.Image)
	}
	s := &maintSession{id: v.id}
	var cfg qemu.Config
	if m.jail != nil {
		cfg = m.jailLaunchConfig(meta, nil)
		s.nbdHost = filepath.Join(m.store.VMDir(v.id), storage.ControlDirName, storage.NBDSocketName)
		// The control directory is attached where the jailed QMP socket is.
		s.nbdServe = path.Join(path.Dir(qemu.JailQMPPath), storage.NBDSocketName)
	} else {
		cfg = m.launchConfig(meta, nil)
		s.nbdHost = filepath.Join(m.store.VMDir(v.id), storage.NBDSocketName)
		s.nbdServe = s.nbdHost
	}
	cfg.MemoryMiB = claim.memoryMiB
	cfg.VCPUs = 1
	// A QEMU killed during an NBD session leaves its socket, which the
	// jailer's control-directory check refuses; a non-socket is kept and
	// refuses the launch.
	if err := m.store.RemovePhase2Scratch(v.id); err != nil {
		return nil, err
	}
	var proc maintProcess
	var err error
	if m.jail != nil {
		proc, err = m.launchMaintenanceJailed(ctx, v, img, cfg)
	} else {
		proc, err = m.launchMaintenancePlain(ctx, v, cfg)
	}
	if err != nil {
		if proc != nil {
			// Absence unproven: the handle stays with the record.
			m.failMaintenance(v, proc, err)
			return nil, err
		}
		if serr := m.settleJailDetached(v, nil); serr != nil {
			m.failMaintenance(v, nil, serr)
			return nil, errors.Join(err, serr)
		}
		m.clearMaintenanceIntent(v, claim)
		return nil, err
	}
	s.proc = proc
	m.drains.Go(func() { drainEvents(proc) })
	return s, nil
}

// launchMaintenancePlain is the non-jailed launch (dev/test).
func (m *Manager) launchMaintenancePlain(ctx context.Context, v *vm, cfg qemu.Config) (maintProcess, error) {
	if err := m.store.WriteLaunchIntent(v.id); err != nil {
		return nil, fmt.Errorf("maintenance %s: record launch intent: %w", v.id, err)
	}
	p, err := m.launcher.Start(ctx, cfg, qemu.StartOptions{Logger: m.log, Maintenance: true})
	if err != nil {
		if p != nil {
			if serr := stopProcess(p); serr == nil && procDone(p) {
				return nil, err
			}
			return asMaintProcess(p), err
		}
		return nil, err
	}
	proc := asMaintProcess(p)
	if proc == nil || proc.QMP() == nil {
		if serr := stopProcess(p); serr != nil || !procDone(p) {
			return asMaintProcess(p), errors.Join(errors.New("maintenance: launcher process has no QMP control"), serr)
		}
		return nil, fmt.Errorf("maintenance %s: launcher process has no QMP control", v.id)
	}
	if err := m.store.CommitRunning(v.id, proc.PID()); err != nil {
		if serr := stopProcess(proc); serr != nil || !procDone(proc) {
			return proc, errors.Join(err, serr)
		}
		return nil, fmt.Errorf("maintenance %s: record process identity: %w", v.id, err)
	}
	return proc, nil
}

// asMaintProcess returns p's maintProcess view, or a nil interface.
func asMaintProcess(p VMProcess) maintProcess {
	if mp, ok := p.(maintProcess); ok {
		return mp
	}
	if p == nil {
		return nil
	}
	return noQMPProcess{p}
}

// noQMPProcess keeps a process without QMP control as a handle.
type noQMPProcess struct{ VMProcess }

func (noQMPProcess) QMP() *qmp.Client { return nil }

// buildMaintenanceJailSpec is buildJailSpec for the maintenance descriptor
// map (qemu.MaintenanceKernelFD..qemu.MaintenanceOverlayFD): kernel,
// initrd, base, overlay; no TAP, vhost-net or vhost-vsock.
func buildMaintenanceJailSpec(id, attempt, binary string, args []string, pins *storage.LaunchPins, limits jailer.Limits, stderr io.Writer) jailer.LaunchSpec {
	return jailer.LaunchSpec{
		SandboxID: id,
		Attempt:   attempt,
		Binary:    binary,
		Args:      args,
		Files: []jailer.Descriptor{
			{Name: jailer.KernelName, File: pins.Kernel},
			{Name: jailer.InitrdName, File: pins.Initrd},
			{Name: "base", File: pins.Base},
			{Name: "overlay", File: pins.Overlay},
		},
		Disks:       jailDisks(),
		ControlDir:  pins.Control,
		SerialLog:   pins.Console,
		Limits:      limits,
		Stdout:      io.Discard,
		Stderr:      stderr,
		Maintenance: true,
	}
}

// maintenanceLimits are the cgroup bounds of a maintenance QEMU: one CPU,
// memory.max equal to the admitted session memory.
func maintenanceLimits(memoryMiB uint64, io jailer.IOLimits) (jailer.Limits, error) {
	limits, err := jailLimits(storage.Resources{VCPUs: 1, MemoryMiB: memoryMiB}, io)
	if err != nil {
		return jailer.Limits{}, err
	}
	limits.MemoryBytes = memoryMiB << 20
	if err := limits.Validate(); err != nil {
		return jailer.Limits{}, err
	}
	return limits, nil
}

// launchMaintenanceJailed is launchJailed for the maintenance form: the same
// durable attempt intent, jail reservation, settlement and QMP proof.
func (m *Manager) launchMaintenanceJailed(ctx context.Context, v *vm, img storage.Image, cfg qemu.Config) (maintProcess, error) {
	if rec, ok := m.jail.Record(v.id); ok {
		return nil, fmt.Errorf("%w: %s attempt %s", errJailConflict, v.id, rec.Attempt)
	}
	limits, err := maintenanceLimits(cfg.MemoryMiB, m.cfg.JailIO)
	if err != nil {
		return nil, err
	}
	args, err := cfg.MaintenanceArgs()
	if err != nil {
		return nil, err
	}
	pins, err := m.store.PinLaunch(ctx, v.id, img)
	if err != nil {
		return nil, fmt.Errorf("pin launch resources: %w", err)
	}
	defer pins.Close()
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
	run, err := m.jail.Start(ctx, buildMaintenanceJailSpec(v.id, attempt, m.cfg.JailBinary, args, pins, limits, stderr))
	if cerr := pins.Close(); cerr != nil && err == nil {
		m.log.Warn("node: close maintenance launch descriptors", "sandbox", v.id, "error", cerr)
	}
	if run == nil {
		if errors.Is(err, jailer.ErrRetained) {
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
	fail := func(cause error) (maintProcess, error) {
		left, err := m.jailStartupFailure(v, proc, cause)
		if left == nil {
			return nil, err
		}
		return proc, err
	}
	if err != nil {
		return fail(fmt.Errorf("jail: %w", err))
	}
	dial := m.jailDial
	if dial == nil {
		dial = dialJailQMP
	}
	client, err := dial(ctx, m.jailControlQMP(v.id), run.PID(), proc.done)
	if err != nil {
		return fail(err)
	}
	proc.setClient(client)
	if err := ctx.Err(); err != nil {
		client.Close()
		return fail(fmt.Errorf("maintenance %s: startup canceled: %w", v.id, context.Cause(ctx)))
	}
	if proc.exited() {
		client.Close()
		return fail(fmt.Errorf("maintenance %s: QEMU exited during startup: %w", v.id, proc.Err()))
	}
	return proc, nil
}

// endMaintenance ends the session's QEMU and settles everything it held.
// cause is the session context's cause (nil while it is live).
func (m *Manager) endMaintenance(v *vm, s *maintSession, claim maintClaim, cause error) error {
	s.closeClients()
	proc := s.proc
	fenced := errors.Is(cause, errMaintenanceFenced)
	var errs []error
	killed := fenced
	if !fenced {
		ctx, cancel := context.WithTimeout(context.Background(), maintTeardownTimeout)
		if err := s.StopNBD(ctx); err != nil {
			m.log.Warn("node: maintenance NBD teardown", "sandbox", v.id, "error", err)
		}
		if client := s.QMP(); client != nil {
			// QEMU documents an EOF after quit as expected.
			_ = client.Execute(ctx, "quit", nil, nil)
		}
		cancel()
		select {
		case <-proc.Done():
		case <-time.After(cmp.Or(m.maintQuit, maintQuitTimeout)):
			killed = true
		}
	}
	if !procDone(proc) {
		// A canceled context makes Stop skip the quit grace and kill.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = proc.Stop(ctx)
		select {
		case <-proc.Done():
		case <-time.After(m.cleanupWait()):
		}
	}
	_ = proc.Close()
	if killed {
		// A killed QEMU leaves the bitmap in use (inconsistent): the next
		// snapshot must not trust it.
		m.log.Warn("node: maintenance QEMU killed; its dirty bitmap is not stored cleanly", "sandbox", v.id, "fenced", fenced)
		if err := m.store.SetFullScanNext(v.id); err != nil {
			m.log.Warn("node: record full scan next", "sandbox", v.id, "error", err)
		}
	}
	if !procDone(proc) {
		err := fmt.Errorf("maintenance %s: QEMU pid %d did not exit after SIGKILL", v.id, proc.PID())
		m.failMaintenance(v, proc, err)
		return err
	}
	if err := m.settleJailDetached(v, proc); err != nil {
		m.failMaintenance(v, nil, err)
		return err
	}
	if err := m.store.RemovePhase2Scratch(v.id); err != nil {
		errs = append(errs, err)
	}
	m.clearMaintenanceIntent(v, claim)
	return errors.Join(errs...)
}

// clearMaintenanceIntent removes a STOPPED VM's maintenance launch record
// once its QEMU is proven gone, keeping the original stop time. A creating
// VM keeps it: its boot writes its own launch intent next.
func (m *Manager) clearMaintenanceIntent(v *vm, claim maintClaim) {
	v.mu.Lock()
	deleting := v.deleting
	v.mu.Unlock()
	if !claim.stopped || deleting {
		return
	}
	if err := m.store.CommitStopped(v.id, claim.stoppedAt); err != nil {
		// A restart reads the leftover intent as an interrupted start of a
		// stopped VM and stops it again: safe, so only logged.
		m.log.Warn("node: clear maintenance launch intent", "sandbox", v.id, "error", err)
	}
}

// failMaintenance records a session whose QEMU or jail could not be settled.
// proc, when non-nil, is a process whose death is unproven: it becomes the
// record's process so DeleteVM (or a create's failBoot) stops it before
// anything is removed. A STOPPED VM turns FAILED; a creating VM's boot
// worker decides its state from the returned error.
func (m *Manager) failMaintenance(v *vm, proc VMProcess, cause error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if proc != nil {
		v.process = proc
	}
	if v.deleting || v.state != nodev1.VMState_VM_STATE_STOPPED {
		return
	}
	v.state = nodev1.VMState_VM_STATE_FAILED
	v.errMsg = "maintenance did not settle: " + cause.Error()
	if err := m.store.SetVMError(v.id, v.errMsg); err != nil {
		m.log.Warn("node: persist maintenance failure", "sandbox", v.id, "error", err)
	}
}
