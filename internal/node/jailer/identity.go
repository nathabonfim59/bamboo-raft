package jailer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func hostBootID() (string, error) {
	raw, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("jailer: read host boot id: %w", err)
	}
	id := strings.TrimSpace(string(raw))
	if id == "" {
		return "", errors.New("jailer: host boot id is empty")
	}
	return id, nil
}

type procIdentity struct {
	startTicks uint64
	state      byte
}

func readProcIdentity(pid int) (procIdentity, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return procIdentity{}, err
	}
	return parseProcStat(pid, raw)
}

// parseProcStat extracts the state and start ticks from /proc/pid/stat.
func parseProcStat(pid int, raw []byte) (procIdentity, error) {
	_, rest, ok := bytes.CutLast(raw, []byte(")"))
	if !ok {
		return procIdentity{}, fmt.Errorf("jailer: proc %d: malformed stat", pid)
	}
	fields := strings.Fields(string(rest))
	if len(fields) < 20 || len(fields[0]) != 1 {
		return procIdentity{}, fmt.Errorf("jailer: proc %d: short stat", pid)
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return procIdentity{}, fmt.Errorf("jailer: proc %d: invalid start ticks: %w", pid, err)
	}
	if ticks == 0 {
		return procIdentity{}, fmt.Errorf("jailer: proc %d: zero start ticks", pid)
	}
	return procIdentity{startTicks: ticks, state: fields[0][0]}, nil
}

func procStartTicks(pid int) (uint64, error) {
	identity, err := readProcIdentity(pid)
	return identity.startTicks, err
}

func procDead(state byte) bool { return state == 'Z' || state == 'X' || state == 'x' }

// recordAlive returns false only when death is proven. Errors retain every
// resource; a failed proc read other than nonexistence is never treated as
// death. A zombie with matching ticks is dead even before its parent's Wait.
func recordAlive(record Record) (bool, error) {
	if record.PID == 0 {
		return false, nil
	}
	if record.PID < 0 || record.StartTicks == 0 || record.BootID == "" {
		return false, errors.New("jailer: incomplete process identity")
	}
	bootID, err := hostBootID()
	if err != nil {
		return false, err
	}
	if record.BootID != bootID {
		return false, nil
	}
	identity, err := readProcIdentity(record.PID)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("jailer: inspect pid %d: %w", record.PID, err)
	}
	return identity.startTicks == record.StartTicks && !procDead(identity.state), nil
}

// openVerifiedPidfd pins a recovered process before any signal. Complete
// start-time identity is mandatory; zero ticks are a programmer/data error,
// not proof that the process is dead.
func openVerifiedPidfd(record Record) (*os.File, bool, error) {
	if record.PID == 0 {
		return nil, false, nil
	}
	if record.PID < 0 || record.StartTicks == 0 || record.BootID == "" {
		return nil, false, errors.New("jailer: cannot pin incomplete process identity")
	}
	fd, err := unix.PidfdOpen(record.PID, 0)
	if errors.Is(err, unix.ESRCH) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("jailer: pidfd open %d: %w", record.PID, err)
	}
	f := os.NewFile(uintptr(fd), "pidfd")
	bootID, err := hostBootID()
	if err != nil {
		f.Close()
		return nil, false, err
	}
	if record.BootID != bootID {
		f.Close()
		return nil, false, nil
	}
	identity, err := readProcIdentity(record.PID)
	if errors.Is(err, fs.ErrNotExist) {
		f.Close()
		return nil, false, nil
	}
	if err != nil {
		f.Close()
		return nil, false, err
	}
	if identity.startTicks != record.StartTicks || procDead(identity.state) {
		f.Close()
		return nil, false, nil
	}
	return f, true, nil
}

// processHandle owns a direct child's optional pidfd. Its mutex covers every
// pidfd syscall and the close, so a closed descriptor number can never be
// reused for a later poll or signal. process belongs to the caller's sole
// exec.Cmd.Wait; the handle uses it only for the no-pidfd SIGKILL fallback
// and never waits on, releases or closes it.
type processHandle struct {
	mu      sync.Mutex
	pid     int
	fd      int
	process *os.Process
	closed  bool
	// contained records that this direct child was verified a member of
	// its owned cgroup, into which clone3 placed it at birth.
	contained bool
	// exitHidden is the managerHooks.toolExitHidden test seam (nil in
	// production): while it is open the exit is reported as not yet
	// observable.
	exitHidden <-chan struct{}
}

// errChildExited marks an exec-proof failure whose only cause is a
// pidfd-proved exit (before the exe read, or after a matching one): the
// exit raced the proof, and the tool's fast-exit witness may decide.
var errChildExited = errors.New("its pidfd reports the exit")

// errExeVanished marks the exact exec-proof case of a child that was alive
// by its pidfd while its /proc/<pid>/exe was already gone: an exit between
// exit_mm and exit_notify, not yet waitable.
var errExeVanished = errors.New("its executable is gone while its exit is not yet observable")

// awaitExitObservable waits, bounded by ctx and limit, until the pinned
// child's exit is observable through its pidfd (POLLIN). It never reaps.
// Each poll runs under the handle lock with a short timeout, so the
// descriptor cannot be closed or reused meanwhile.
func (h *processHandle) awaitExitObservable(ctx context.Context, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("jailer: waiting for the tool's exit to become observable: %w", context.Cause(ctx))
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("jailer: the tool's exit did not become observable within %v", limit)
		}
		observable, err := h.pollExit(min(remaining, 50*time.Millisecond))
		if err != nil || observable {
			return err
		}
	}
}

// pollExit polls the pidfd for the exit once, for at most timeout.
func (h *processHandle) pollExit(timeout time.Duration) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false, errHandleReleased
	}
	if h.fd < 0 {
		return false, errors.New("jailer: no pidfd to observe the tool's exit")
	}
	if h.exitHiddenLocked() {
		h.mu.Unlock()
		select {
		case <-h.exitHidden:
		case <-time.After(timeout):
		}
		h.mu.Lock()
		return false, nil
	}
	// One syscall per call: an interrupted poll returns to the caller's
	// context and deadline checks, never restarting a full timeout here.
	poll := []unix.PollFd{{Fd: int32(h.fd), Events: unix.POLLIN}}
	n, err := unix.Poll(poll, max(1, int(timeout/time.Millisecond)))
	switch {
	case errors.Is(err, unix.EINTR):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("jailer: poll pidfd: %w", err)
	case n == 0:
		return false, nil
	case poll[0].Revents&(unix.POLLNVAL|unix.POLLERR) != 0:
		return false, fmt.Errorf("jailer: pidfd poll events %#x", poll[0].Revents)
	case poll[0].Revents&unix.POLLIN != 0:
		return true, nil
	default:
		return false, fmt.Errorf("jailer: unexpected pidfd events %#x", poll[0].Revents)
	}
}

// exitHiddenLocked reports the test seam's hidden-exit state (never in
// production, where exitHidden is nil).
func (h *processHandle) exitHiddenLocked() bool {
	if h.exitHidden == nil {
		return false
	}
	select {
	case <-h.exitHidden:
		return false
	default:
		return true
	}
}

// errDecideByCgroup reports that the handle holds no process evidence (no
// pidfd, no captured identity) but the child's birth containment was
// verified, so its owned cgroup alone decides whether it is gone.
var errDecideByCgroup = errors.New("jailer: process liveness is decided by the owned cgroup")

// markContained records verified birth membership in the owned cgroup.
func (h *processHandle) markContained() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.contained = true
}

// errHandleReleased reports use of a Child whose handle was released.
var errHandleReleased = errors.New("jailer: child process handle is released")

// newProcessHandle pins a direct, not yet waited-for child. pidfd is the
// descriptor clone3 returned through SysProcAttr.PidFD (CLONE_PIDFD), or -1;
// the handle takes sole ownership of it and closes it exactly once (the
// os.Process keeps its own duplicate). Only when the clone supplied none is
// pidfd_open (open) used: its pid is safe because the child cannot be
// reaped before Start returns. ENOSYS or EPERM select the identity-checked
// fallback; any other error is returned with a still-usable fallback handle
// so the child can be killed.
func newProcessHandle(process *os.Process, pidfd int, open func(int) (int, error)) (*processHandle, error) {
	h := &processHandle{pid: process.Pid, fd: -1, process: process}
	if pidfd >= 0 {
		h.fd = pidfd
		return h, nil
	}
	fd, err := open(process.Pid)
	switch {
	case err == nil:
		h.fd = fd
	case errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EPERM):
	default:
		return h, fmt.Errorf("jailer: pin direct child %d: %w", process.Pid, err)
	}
	return h, nil
}

// capture reads the direct child's start ticks and proves it is still alive
// after the read, so the identity cannot belong to a later process.
func (h *processHandle) capture() (uint64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, errHandleReleased
	}
	identity, err := readProcIdentity(h.pid)
	if err != nil {
		return 0, fmt.Errorf("jailer: capture direct child %d: %w", h.pid, err)
	}
	if procDead(identity.state) {
		return 0, fmt.Errorf("jailer: direct child %d died before identity commit", h.pid)
	}
	if h.fd >= 0 {
		alive, err := pollPidfd(h.fd)
		if err != nil {
			return 0, err
		}
		if !alive {
			return 0, fmt.Errorf("jailer: direct child %d died during identity capture", h.pid)
		}
	}
	return identity.startTicks, nil
}

func pollPidfd(fd int) (bool, error) {
	for {
		poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, err := unix.Poll(poll, 0)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("jailer: poll pidfd: %w", err)
		}
		if n == 0 {
			return true, nil
		}
		if poll[0].Revents&unix.POLLIN != 0 {
			return false, nil
		}
		return false, fmt.Errorf("jailer: unexpected pidfd events %#x", poll[0].Revents)
	}
}

// exited reports proven exit. Without a pidfd it requires a complete
// boot-id/start-tick identity; an incomplete identity is an error, never
// proof of death, except errDecideByCgroup for a verified-contained child
// with no captured identity.
func (h *processHandle) exited(record Record) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false, errHandleReleased
	}
	if h.fd >= 0 {
		alive, err := pollPidfd(h.fd)
		return !alive, err
	}
	if record.PID == 0 && h.contained {
		return false, errDecideByCgroup
	}
	if record.PID != h.pid {
		return false, fmt.Errorf("jailer: handle pid %d does not match record pid %d", h.pid, record.PID)
	}
	alive, err := recordAlive(record)
	return !alive, err
}

// kill sends SIGKILL through the pidfd or, without one, through the caller's
// unreaped os.Process. It is safe after Manager.Close and fails only after
// the handle was released.
func (h *processHandle) kill() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errHandleReleased
	}
	if h.fd >= 0 {
		if err := unix.PidfdSendSignal(h.fd, unix.SIGKILL, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
			return fmt.Errorf("jailer: pidfd SIGKILL: %w", err)
		}
		return nil
	}
	if err := h.process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("jailer: direct child SIGKILL: %w", err)
	}
	return nil
}

// hasPidfd reports whether the handle pins its child with a pidfd.
func (h *processHandle) hasPidfd() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fd >= 0
}

// close releases the pidfd. It never touches the caller's os.Process.
func (h *processHandle) close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	fd := h.fd
	h.fd = -1
	if fd >= 0 {
		if err := unix.Close(fd); err != nil {
			return fmt.Errorf("jailer: close pidfd: %w", err)
		}
	}
	return nil
}

// proveExec compares the direct child's /proc/pid/exe inode with the staged
// QEMU inode. EOF on the close-on-exec status pipe alone is not enough: a
// helper can close it and stay alive, or die before exec. Liveness is checked
// before and after the inode read so a recycled pid cannot satisfy it; the
// pid itself cannot be recycled before the caller's Wait.
func (h *processHandle) proveExec(expected inodeIdentity) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errHandleReleased
	}
	if err := h.aliveLocked("before"); err != nil {
		return err
	}
	info, err := os.Stat(fmt.Sprintf("/proc/%d/exe", h.pid))
	if err != nil {
		// An exiting task drops its executable (exit_mm) before its pidfd
		// reports the exit, so a missing exe link is an exit in progress.
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("jailer: child exited before executable proof: %w: %w", errExeVanished, err)
		}
		return fmt.Errorf("jailer: child exited before executable proof: %w", err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("jailer: executable has no Linux identity")
	}
	actual := inodeIdentity{Dev: uint64(st.Dev), Ino: st.Ino}
	if !actual.equal(expected) {
		return fmt.Errorf("jailer: child is not the staged QEMU (exe inode %d:%d, staged inode %d:%d)", actual.Dev, actual.Ino, expected.Dev, expected.Ino)
	}
	return h.aliveLocked("after")
}

func (h *processHandle) aliveLocked(when string) error {
	if h.fd >= 0 {
		if h.exitHiddenLocked() {
			return nil
		}
		alive, err := pollPidfd(h.fd)
		if err != nil {
			return err
		}
		if !alive {
			return fmt.Errorf("jailer: child exited %s executable proof: %w", when, errChildExited)
		}
		return nil
	}
	identity, err := readProcIdentity(h.pid)
	if err != nil {
		return fmt.Errorf("jailer: child exited %s executable proof: %w", when, err)
	}
	if procDead(identity.state) {
		return fmt.Errorf("jailer: child exited %s executable proof", when)
	}
	return nil
}

// pidfdKill signals a verified recovered process through its pidfd.
func pidfdKill(f *os.File) error {
	if err := unix.PidfdSendSignal(int(f.Fd()), unix.SIGKILL, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
		return fmt.Errorf("jailer: pidfd SIGKILL: %w", err)
	}
	return nil
}
