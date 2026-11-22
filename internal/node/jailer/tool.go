package jailer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// D1: qemu-img runs as a jailed tool attempt through the accepted pipeline:
// a durable record before any host mutation, a per-attempt pool uid, birth
// cgroup with a pidfd, a root helper that drops the bounding set before
// setuid and leaves all five capability sets zero with NNP, the staged
// read-only runtime, and the mandatory reclaim before the uid is freed.
// RunTool owns the only consuming Wait of a tool.

// KindTool is the record kind of one image tool attempt. Its id is
// ToolID(attempt), which no sandbox or probe id can equal.
const KindTool = "tool"

// ToolIDPrefix starts every tool record id; it contains 'o' and 'l', which
// the sandbox id alphabet excludes.
const ToolIDPrefix = "tool_"

// ToolID returns the record id of a tool attempt: ToolIDPrefix and the 32
// lowercase hex digits of the attempt uuid.
func ToolID(attempt string) string { return ToolIDPrefix + strings.ReplaceAll(attempt, "-", "") }

// TargetRoot names the confined directory a tool target is pinned under:
// never an absolute path.
type TargetRoot string

// Target roots.
const (
	TargetRootData   TargetRoot = "data"
	TargetRootImages TargetRoot = "images"
)

// TargetAccess is the kernel-enforced access a record grants its uid to its
// target inode.
type TargetAccess string

// Target access levels. Only AccessShared and AccessShared may share one
// inode; every other pairing conflicts in either order.
const (
	// AccessShared is a verified immutable image base: root-owned, 0444, a
	// read-only fd, never chowned or chmodded on any path.
	AccessShared TargetAccess = "shared-ro"
	// AccessGroupRO is info/check of an overlay or copy: root:<uid> 0640
	// with a read-only fd, so the uid can neither chmod nor reopen it RDWR.
	AccessGroupRO TargetAccess = "group-ro"
	// AccessOwnerRW is an operation meant to write the inode (resize,
	// create) and the VM overlay: <uid>:<uid> 0600.
	AccessOwnerRW TargetAccess = "owner-rw"
)

// ToolOp is one fixed qemu-img operation; the jailer builds its argv.
type ToolOp string

// Tool operations.
const (
	ToolInfo   ToolOp = "info"
	ToolCheck  ToolOp = "check"
	ToolResize ToolOp = "resize"
	ToolCreate ToolOp = "create"
)

// MaxToolTimeout bounds every tool run.
const MaxToolTimeout = 2 * time.Minute

// ToolLimits bounds one tool's cgroup (checked arithmetic); MemoryBytes is
// its memory.max. RLIMIT_CORE is 0.
type ToolLimits struct {
	CPUQuotaPercent uint32
	MemoryBytes     uint64
	Pids            uint32
	IO              IOLimits
}

// ToolSpec describes one jailed qemu-img run. The caller closes Target and
// Base after RunTool returns.
type ToolSpec struct {
	// Attempt is the caller-chosen uuid of this run.
	Attempt string
	// Binary is the trusted absolute qemu-img, staged like QEMU.
	Binary string
	Op     ToolOp
	// Driver is "qcow2" or "raw" (raw: info/check only); it selects
	// --image-opts driver=, or create -f qcow2.
	Driver string
	// Target is the pinned target: O_RDONLY for info/check, O_RDWR for
	// resize/create. At rest a group-ro or owner-rw target is root:root 0600.
	Target       *os.File
	TargetRoot   TargetRoot
	TargetPath   string // clean, relative to TargetRoot, no ".."
	TargetAccess TargetAccess
	// Base is an optional verified standalone root 0444 base, the explicit
	// graph's fd 4; the stored backing name is never followed.
	Base *os.File
	// BackingName is the exact stored backing name for create (-u). Empty
	// selects the standalone create (no backing), allowed only for a target
	// under StandaloneCreateDir in the data root.
	BackingName string
	Size        uint64
	Limits      ToolLimits
	// Timeout bounds the run (≤ MaxToolTimeout).
	Timeout time.Duration
	// Stdout and Stderr are caller-bounded.
	Stdout io.Writer
	Stderr io.Writer
}

// ToolProof names how the exec of a tool was proven.
type ToolProof string

// Tool exec proofs.
const (
	// ProofLiveExe is the staged executable inode of the live tool.
	ProofLiveExe ToolProof = "live-exe"
	// ProofExitedComm is the tool-only fast-exit proof: clean status EOF,
	// a WNOWAIT exit observation of the same unreaped child (recorded
	// ticks) and its comm equal to the truncated staged basename, captured
	// as distinct from the helper's own comm. It is weaker than inode proof.
	ProofExitedComm ToolProof = "exited-comm"
)

// ToolResult is a completed tool run. Success requires ExitCode 0.
type ToolResult struct {
	ExitCode int
	Proof    ToolProof
}

// ToolReport is what SettleTools did with the tool records of earlier
// processes.
type ToolReport struct {
	// Settled attempts were proven gone, reclaimed and released.
	Settled []Ref
	// Retained attempts are unverifiable or still live; their targets and
	// uids stay held (ErrTargetBusy for those inodes).
	Retained []Ref
}

// ErrToolRetained reports a tool attempt whose exit or settlement is
// unproven: its record, target lease and uid stay held, and the caller must
// keep every path that contains the target.
var ErrToolRetained = errors.New("jailer: tool attempt retained; its target and uid stay held")

// StandaloneCreateDir is the data-root directory whose files a create
// without a backing name may target (the Phase 2 fleece templates).
const StandaloneCreateDir = "phase2"

// Tool output and naming bounds.
const (
	maxToolBackingName = 1023
	// commLen is the kernel's comm length without its terminating NUL.
	commLen = 15
	// cldExited is siginfo's si_code for a child that exited normally.
	cldExited = 1
)

// validTargetPath requires a clean relative path below its root.
func validTargetPath(p string) bool {
	return p != "" && !path.IsAbs(p) && path.Clean(p) == p && p != ".." && !strings.HasPrefix(p, "../") &&
		!strings.ContainsRune(p, 0)
}

// validTarget checks a target's root, path and access together: an image
// base under the images root is only ever shared-ro.
func validTarget(root TargetRoot, p string, access TargetAccess) error {
	switch root {
	case TargetRootData, TargetRootImages:
	default:
		return fmt.Errorf("unknown target root %q", root)
	}
	switch access {
	case AccessShared, AccessGroupRO, AccessOwnerRW:
	default:
		return fmt.Errorf("unknown target access %q", access)
	}
	if !validTargetPath(p) {
		return fmt.Errorf("unsafe target path %q", p)
	}
	if root == TargetRootImages && access != AccessShared {
		return fmt.Errorf("an images-root target is only ever %s", AccessShared)
	}
	return nil
}

// limits converts tool limits into the cgroup limits.
func (l ToolLimits) limits() Limits { return Limits(l) }

// fdAccess returns f's O_ACCMODE.
func fdAccess(f *os.File) (int, error) {
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
	if err != nil {
		return 0, err
	}
	return flags & unix.O_ACCMODE, nil
}

// Validate checks a tool spec before any host change: the op/access/driver
// matrix, the target path and root, the descriptor modes, sizes, timeout
// and limits.
func (s ToolSpec) Validate() error {
	if !validAttempt(s.Attempt) {
		return fmt.Errorf("jailer: invalid tool attempt %q", s.Attempt)
	}
	if err := cleanAbsolute("tool binary", s.Binary); err != nil {
		return err
	}
	if _, err := runtimeTarget(s.Binary); err != nil {
		return err
	}
	if s.Target == nil {
		return errors.New("jailer: tool target is required")
	}
	if err := validTarget(s.TargetRoot, s.TargetPath, s.TargetAccess); err != nil {
		return fmt.Errorf("jailer: tool: %w", err)
	}
	writes := false
	switch s.Op {
	case ToolInfo, ToolCheck:
		if s.TargetAccess != AccessShared && s.TargetAccess != AccessGroupRO {
			return fmt.Errorf("jailer: tool %s needs shared-ro or group-ro access, not %s", s.Op, s.TargetAccess)
		}
		if s.Size != 0 {
			return fmt.Errorf("jailer: tool %s takes no size", s.Op)
		}
	case ToolResize, ToolCreate:
		writes = true
		if s.TargetAccess != AccessOwnerRW {
			return fmt.Errorf("jailer: tool %s needs owner-rw access, not %s", s.Op, s.TargetAccess)
		}
		if s.Size == 0 {
			return fmt.Errorf("jailer: tool %s needs a size", s.Op)
		}
	default:
		return fmt.Errorf("jailer: unknown tool op %q", s.Op)
	}
	switch s.Driver {
	case "qcow2":
	case "raw":
		if writes {
			return fmt.Errorf("jailer: tool %s is qcow2 only", s.Op)
		}
	default:
		return fmt.Errorf("jailer: unknown tool driver %q", s.Driver)
	}
	if s.Op == ToolCreate {
		switch {
		case s.BackingName == "":
			// A standalone create (no backing) makes only the Phase 2 fleece
			// templates under data/phase2/; a VM overlay always stores its
			// base's name.
			if s.TargetRoot != TargetRootData || !strings.HasPrefix(s.TargetPath, StandaloneCreateDir+"/") {
				return fmt.Errorf("jailer: a create without a backing name is only allowed under %s/, not %q", StandaloneCreateDir, s.TargetPath)
			}
		case len(s.BackingName) > maxToolBackingName || strings.ContainsAny(s.BackingName, ",\x00\n"):
			return fmt.Errorf("jailer: invalid create backing name %q", s.BackingName)
		}
		if s.Base != nil || s.Driver != "qcow2" {
			return errors.New("jailer: create names its backing with -u and opens no base")
		}
	} else if s.BackingName != "" {
		return errors.New("jailer: a backing name is only stored by create")
	}
	mode, err := fdAccess(s.Target)
	if err != nil {
		return fmt.Errorf("jailer: tool target: %w", err)
	}
	if want := map[bool]int{false: unix.O_RDONLY, true: unix.O_RDWR}[writes]; mode != want {
		return fmt.Errorf("jailer: tool %s target descriptor has access mode %#x, want %#x", s.Op, mode, want)
	}
	if s.Base != nil {
		// qemu-img info always opens without a backing (NO_BACKING): it
		// takes the target alone.
		if s.Op == ToolInfo {
			return errors.New("jailer: tool info opens no backing and takes no base")
		}
		if mode, err := fdAccess(s.Base); err != nil || mode != unix.O_RDONLY {
			return errors.New("jailer: tool base must be a read-only descriptor")
		}
	}
	if s.Timeout <= 0 || s.Timeout > MaxToolTimeout {
		return fmt.Errorf("jailer: tool timeout %s is outside (0, %s]", s.Timeout, MaxToolTimeout)
	}
	return s.Limits.limits().Validate()
}

// toolGraph is the closed --image-opts graph: the target at fd 3 and, for
// an overlay's check or resize (which open a backing), its verified base at
// fd 4. info opens no backing (qemu-img's fixed NO_BACKING for info), so its
// graph is the target alone. The stored backing name is never a graph input.
func toolGraph(spec ToolSpec) string {
	graph := "driver=" + spec.Driver + ",file.driver=file,file.filename=/proc/self/fd/3"
	if spec.Base != nil && spec.Op != ToolInfo {
		graph += ",backing.driver=qcow2,backing.file.driver=file,backing.file.filename=/proc/self/fd/4,backing.read-only=on"
	}
	return graph
}

// toolArgs returns the fixed argv (without argv0) for spec's op.
func toolArgs(spec ToolSpec) ([]string, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	size := strconv.FormatUint(spec.Size, 10)
	switch spec.Op {
	case ToolInfo, ToolCheck:
		return []string{string(spec.Op), "--output=json", "--image-opts", toolGraph(spec)}, nil
	case ToolResize:
		return []string{"resize", "--image-opts", toolGraph(spec), size}, nil
	default: // ToolCreate
		if spec.BackingName == "" {
			return []string{"create", "-f", "qcow2", "/proc/self/fd/3", size}, nil
		}
		return []string{"create", "-f", "qcow2", "-u", "-F", "qcow2", "-b", spec.BackingName, "/proc/self/fd/3", size}, nil
	}
}

// targetsConflict reports whether two leases on one inode conflict: only
// shared-ro with shared-ro may share.
func targetsConflict(held, want TargetAccess) bool {
	return held != AccessShared || want != AccessShared
}

// toolComm is the comm a process has right after it execs binary: the
// basename truncated to the kernel's 15 bytes.
func toolComm(binary string) string {
	base := filepath.Base(binary)
	if len(base) > commLen {
		base = base[:commLen]
	}
	return base
}

// toolCommAmbiguous reports that the helper's own comm equals the staged
// tool's (or is unknown), so comm cannot tell a pre-exec exit from the
// tool's.
func toolCommAmbiguous(helperComm, binary string) bool {
	return helperComm == "" || helperComm == toolComm(binary)
}

// launchSpec is the internal launch form of a tool: fd 3 is the target and
// fd 4 the optional base; there is no control directory or serial log.
func (s ToolSpec) launchSpec(args []string) LaunchSpec {
	files := []Descriptor{{Name: "target", File: s.Target}}
	if s.Base != nil {
		files = append(files, Descriptor{Name: "base", File: s.Base})
	}
	return LaunchSpec{
		SandboxID: ToolID(s.Attempt), Attempt: s.Attempt, Binary: s.Binary, Args: args,
		Files: files, Limits: s.Limits.limits(), Stdout: s.Stdout, Stderr: s.Stderr,
	}
}

// atRestUID is the owner a tool target must have before a run (root).
func (m *Manager) atRestUID() uint32 {
	if m.hooks.atRestOwner != nil {
		return m.hooks.atRestOwner()
	}
	return 0
}

// checkToolFiles proves, read-only, the at-rest policy of a tool's files
// before any reservation or change: a group-ro or owner-rw target is a
// root-owned 0600 regular file; a shared-ro target and any base are
// root-owned, not writable by anyone and readable by others (the tool uid).
func (m *Manager) checkToolFiles(spec ToolSpec) error {
	check := func(name string, f *os.File, shared bool) error {
		var st unix.Stat_t
		if err := unix.Fstat(int(f.Fd()), &st); err != nil {
			return fmt.Errorf("jailer: tool %s: %w", name, err)
		}
		perm := st.Mode & 0o7777
		switch {
		case st.Mode&unix.S_IFMT != unix.S_IFREG:
			return fmt.Errorf("jailer: tool %s is not a regular file", name)
		case st.Uid != m.atRestUID():
			return fmt.Errorf("jailer: tool %s is owned by uid %d, not root", name, st.Uid)
		case shared && (perm&0o7222 != 0 || perm&0o004 == 0):
			return fmt.Errorf("jailer: tool %s mode %#o is not an immutable readable base", name, perm)
		case !shared && perm != 0o600:
			return fmt.Errorf("jailer: tool %s mode %#o is not the at-rest 0600", name, perm)
		}
		return nil
	}
	if err := check("target", spec.Target, spec.TargetAccess == AccessShared); err != nil {
		return err
	}
	if spec.Base != nil {
		return check("base", spec.Base, true)
	}
	return nil
}

// chownTool changes a tool target's owner through the seam.
func (m *Manager) chownTool(f *os.File, uid, gid int) error {
	if m.hooks.toolChown != nil {
		return m.hooks.toolChown(f, uid, gid)
	}
	return f.Chown(uid, gid)
}

// ownToolTarget grants the tool uid its kernel-enforced access, after the
// durable intent and the conflict check: group-ro is root:<uid> 0640 (the
// uid can neither chmod nor reopen it read-write), owner-rw <uid>:<uid>
// 0600; a shared-ro base is never changed.
func (m *Manager) ownToolTarget(ctx context.Context, spec ToolSpec, record Record) error {
	if err := m.step(ctx, stageOwnFiles); err != nil {
		return err
	}
	switch spec.TargetAccess {
	case AccessGroupRO:
		if err := m.chownTool(spec.Target, int(m.atRestUID()), int(record.UID)); err != nil {
			return fmt.Errorf("jailer: group tool target: %w", err)
		}
		if err := spec.Target.Chmod(0o640); err != nil {
			return fmt.Errorf("jailer: protect tool target: %w", err)
		}
	case AccessOwnerRW:
		if err := spec.Target.Chmod(0o600); err != nil {
			return fmt.Errorf("jailer: protect tool target: %w", err)
		}
		if err := m.chownTool(spec.Target, int(record.UID), int(record.UID)); err != nil {
			return fmt.Errorf("jailer: own tool target: %w", err)
		}
	}
	return nil
}

// newToolRecord is the durable intent of one tool attempt.
func newToolRecord(spec ToolSpec, uid uint32) (Record, error) {
	target, err := fileInode(spec.Target)
	if err != nil {
		return Record{}, fmt.Errorf("jailer: tool target identity: %w", err)
	}
	bootID, err := hostBootID()
	if err != nil {
		return Record{}, err
	}
	record := Record{
		SchemaVersion: recordSchema, SandboxID: ToolID(spec.Attempt), Attempt: spec.Attempt, Kind: KindTool,
		UID: uid, BootID: bootID, TargetDev: target.Dev, TargetIno: target.Ino,
		TargetRoot: spec.TargetRoot, TargetPath: spec.TargetPath, TargetAccess: spec.TargetAccess,
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	return record, record.Validate()
}

// readComm returns pid's comm.
func readComm(pid int) (string, error) {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/comm")
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(raw), "\n"), nil
}

// sigchldInfo is siginfo_t's SIGCHLD view (linux, little or big endian
// alike: the fields are int32 at fixed offsets).
type sigchldInfo struct {
	Signo  int32
	Errno  int32
	Code   int32
	_      int32
	Pid    int32
	UID    uint32
	Status int32
	_      [100]byte
}

// observeExit reports, without reaping, whether the pinned child has
// exited, and how (waitid P_PIDFD with WEXITED|WNOWAIT|WNOHANG). A handle
// without a pidfd cannot observe and fails closed.
func (h *processHandle) observeExit() (exited bool, code, status int32, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false, 0, 0, errHandleReleased
	}
	if h.fd < 0 {
		return false, 0, 0, errors.New("jailer: no pidfd to observe the tool's exit")
	}
	if h.exitHiddenLocked() {
		return false, 0, 0, nil
	}
	var info sigchldInfo
	for {
		_, _, errno := unix.Syscall6(unix.SYS_WAITID, unix.P_PIDFD, uintptr(h.fd), uintptr(unsafe.Pointer(&info)),
			unix.WEXITED|unix.WNOWAIT|unix.WNOHANG, 0, 0)
		if errno == unix.EINTR {
			continue
		}
		if errno != 0 {
			return false, 0, 0, fmt.Errorf("jailer: waitid: %w", errno)
		}
		break
	}
	if info.Pid == 0 {
		return false, 0, 0, nil
	}
	return true, info.Code, info.Status, nil
}

// proveToolExec proves a tool child execed its staged binary: the live
// staged-inode proof, or, for a tool that already exited, the tool-only
// fast-exit proof: an unreaped CLD_EXITED observation of the same child
// (recorded start ticks) whose comm is the staged binary's, which the
// helper's own captured comm is not. Nothing is reaped here.
func (m *Manager) proveToolExec(ctx context.Context, handle *processHandle, staged inodeIdentity, record Record, helperComm, binary string) (ToolProof, error) {
	exited, _, _, err := handle.observeExit()
	if err != nil {
		return "", err
	}
	if !exited {
		liveErr := handle.proveExec(staged)
		if liveErr == nil {
			return ProofLiveExe, nil
		}
		if m.hooks.toolLiveResume != nil {
			select {
			case m.hooks.toolLiveRefused <- struct{}{}:
			default:
			}
			select {
			case <-m.hooks.toolLiveResume:
			case <-ctx.Done():
				return "", errors.Join(liveErr, context.Cause(ctx))
			}
		}
		// Only a pidfd-proved exit, or the exact window (alive by pidfd,
		// exe already gone; waited for, bounded), may reach the unchanged
		// fast-exit witness below. Anything else (an observed exe that is
		// not the staged inode, a stat, poll or handle error) refuses now,
		// even if the child becomes waitable later.
		switch {
		case errors.Is(liveErr, errExeVanished):
			if m.hooks.toolExitWaiting != nil {
				select {
				case m.hooks.toolExitWaiting <- struct{}{}:
				default:
				}
			}
			if err := handle.awaitExitObservable(ctx, toolExitObserveLimit); err != nil {
				return "", errors.Join(liveErr, err)
			}
		case errors.Is(liveErr, errChildExited):
		default:
			return "", liveErr
		}
		if exited, _, _, err = handle.observeExit(); err != nil || !exited {
			return "", errors.Join(liveErr, err)
		}
	}
	_, code, status, err := handle.observeExit()
	if err != nil {
		return "", err
	}
	if code != cldExited {
		return "", fmt.Errorf("jailer: tool %s ended by signal %d (code %d) before its exec proof", record.SandboxID, status, code)
	}
	if toolCommAmbiguous(helperComm, binary) {
		return "", fmt.Errorf("jailer: tool %s exited (exit status %d) before its live proof and the helper comm %q cannot be told from the tool's", record.SandboxID, status, helperComm)
	}
	identity, err := readProcIdentity(record.PID)
	if err != nil {
		return "", fmt.Errorf("jailer: tool %s exit proof: %w", record.SandboxID, err)
	}
	if identity.startTicks != record.StartTicks {
		return "", fmt.Errorf("jailer: tool %s exit proof: pid %d is another process", record.SandboxID, record.PID)
	}
	comm, err := readComm(record.PID)
	if err != nil {
		return "", fmt.Errorf("jailer: tool %s exit proof: %w", record.SandboxID, err)
	}
	if comm != toolComm(binary) {
		return "", fmt.Errorf("jailer: tool %s exited as %q (exit status %d), not the staged %q: no exec happened", record.SandboxID, comm, status, toolComm(binary))
	}
	return ProofExitedComm, nil
}

// pendingTool is an in-process tool attempt. While owned, its RunTool call
// still runs (its sole waiter owns Cmd.Wait, and the call settles it):
// SettleTools never touches it. A call that returned with the attempt
// retained clears owned; SettleTools then settles it once done is closed
// (the reap observed and the private handle closed).
type pendingTool struct {
	done  <-chan struct{}
	owned bool
}

// errToolTimeout is the cause of a run that exceeded its ToolSpec.Timeout.
var errToolTimeout = errors.New("jailer: tool run timed out")

// RunTool runs one jailed qemu-img attempt and owns its only consuming Wait.
// It locks the Manager for the launch and the release only. An unproven exit
// or settlement returns ErrToolRetained with the record, lease and uid kept;
// the child stays owned by its sole waiter and SettleTools releases it once
// that reap is observed.
func (m *Manager) RunTool(ctx context.Context, spec ToolSpec) (ToolResult, error) {
	if err := ctx.Err(); err != nil {
		return ToolResult{}, fmt.Errorf("jailer: tool: %w", context.Cause(ctx))
	}
	args, err := toolArgs(spec)
	if err != nil {
		return ToolResult{}, err
	}
	if m.hooks.toolArgs != nil {
		args = m.hooks.toolArgs(args)
	}
	// One budget bounds the whole run: helper setup, status, proof and the
	// tool itself. Settlement afterwards is detached and separately bounded.
	run, cancelRun := context.WithTimeoutCause(ctx, spec.Timeout, errToolTimeout)
	defer cancelRun()
	m.mu.Lock()
	if m.root == nil {
		m.mu.Unlock()
		return ToolResult{}, ErrClosed
	}
	if err := run.Err(); err != nil {
		m.mu.Unlock()
		return ToolResult{}, fmt.Errorf("jailer: tool: %w", context.Cause(run))
	}
	child, launchErr := m.launchLocked(run, spec.launchSpec(args), KindTool, &spec)
	done := make(chan struct{})
	if child != nil {
		// Published before the lock is released: SettleTools must see this
		// attempt as a current run, never as an earlier process's record.
		m.initMaps()
		m.pendingTools[child.record.SandboxID] = &pendingTool{done: done, owned: true}
	}
	m.mu.Unlock()
	if child == nil {
		return ToolResult{}, launchErr
	}
	return m.finishTool(ctx, run, spec, child, launchErr, done)
}

// finishTool is the tool's sole consuming Wait and its settlement. run is
// the run budget (its deadline is the ToolSpec.Timeout from before launch);
// done is the published completion of the sole waiter.
func (m *Manager) finishTool(ctx, run context.Context, spec ToolSpec, child *Child, launchErr error, done chan struct{}) (result ToolResult, returnErr error) {
	id := child.record.SandboxID
	var waitErr error
	reaped := m.hooks.toolReaped
	go func() {
		waitErr = child.Cmd.Wait()
		_ = child.handle.close()
		close(done)
		// The witness runs after done is closed: it is causally final.
		if reaped != nil {
			reaped(id, child.handle, waitErr)
		}
	}()
	// On return the call stops owning the attempt: a settled attempt is
	// forgotten, a retained one is left for SettleTools.
	defer func() {
		m.mu.Lock()
		if p, ok := m.pendingTools[id]; ok {
			if _, held := m.records[id]; held {
				p.owned = false
			} else {
				delete(m.pendingTools, id)
			}
		}
		m.mu.Unlock()
	}()
	var cause error
	if launchErr != nil {
		cause = launchErr
	} else {
		select {
		case <-done:
		case <-run.Done():
			if errors.Is(context.Cause(run), errToolTimeout) {
				cause = fmt.Errorf("jailer: tool %s timed out after %s", spec.Op, spec.Timeout)
			} else {
				cause = fmt.Errorf("jailer: tool %s: %w", spec.Op, context.Cause(run))
			}
		}
	}
	select {
	case <-done:
	default:
		killErr := m.killChild(child.handle)
		if errors.Is(killErr, errHandleReleased) {
			killErr = nil
		}
		select {
		case <-done:
		case <-time.After(cleanupTimeout):
			return ToolResult{}, errors.Join(cause, killErr,
				fmt.Errorf("jailer: tool %s was not reaped: %w", id, ErrToolRetained))
		}
	}
	if m.hooks.beforeToolSettle != nil {
		m.hooks.beforeToolSettle(id)
	}
	// The reap is proven: settle by the exact attempt, detached from ctx so
	// a settlement that started is never abandoned half-done.
	release, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	m.mu.Lock()
	var relErr error
	if m.root == nil {
		relErr = ErrClosed
	} else if record, ok := m.records[id]; ok && record.Attempt == spec.Attempt {
		relErr = m.releaseLocked(release, id, nil, &record)
	}
	m.mu.Unlock()
	if relErr != nil {
		relErr = fmt.Errorf("jailer: settle tool %s: %w: %w", id, relErr, ErrToolRetained)
	}
	result = ToolResult{ExitCode: -1, Proof: child.proof}
	if exitErr, ok := errors.AsType[*exec.ExitError](waitErr); ok {
		result.ExitCode = exitErr.ExitCode()
	} else if waitErr == nil {
		result.ExitCode = 0
	}
	switch {
	case cause != nil:
		return result, errors.Join(cause, relErr)
	case relErr != nil:
		return result, relErr
	case waitErr != nil:
		return result, fmt.Errorf("jailer: tool %s: %w", spec.Op, waitErr)
	}
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("jailer: tool %s settled, but the run was canceled: %w", spec.Op, context.Cause(ctx))
	}
	return result, nil
}

// SettleTools settles the tool records of earlier runs and processes: PID 0
// by the owned group's emptiness only; a dead process by its recorded
// identity; a live one only after VerifyProcess with tool rules, a pidfd
// kill and an observed exit; then the empty-group proof, the reclaim and
// the release. Anything unverifiable, and any in-process tool whose sole
// waiter has not reaped it, is retained.
func (m *Manager) SettleTools(ctx context.Context) (ToolReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return ToolReport{}, ErrClosed
	}
	m.initMaps()
	var report ToolReport
	for _, record := range m.sortedRecordsLocked() {
		if record.Kind != KindTool {
			continue
		}
		if err := ctx.Err(); err != nil {
			return report, context.Cause(ctx)
		}
		if err := m.settleToolLocked(ctx, record); err != nil {
			m.log.Warn("jailer: tool attempt retained", "tool", record.SandboxID, "error", err)
			report.Retained = append(report.Retained, record.Ref())
			// A canceled or expired call is a failed call, not a report.
			if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return report, fmt.Errorf("jailer: settle tools: %w", errors.Join(context.Cause(ctx), err))
			}
			continue
		}
		report.Settled = append(report.Settled, record.Ref())
	}
	if err := ctx.Err(); err != nil {
		return report, fmt.Errorf("jailer: settle tools: %w", context.Cause(ctx))
	}
	return report, nil
}

func (m *Manager) settleToolLocked(ctx context.Context, record Record) error {
	id := record.SandboxID
	if pending, ok := m.pendingTools[id]; ok {
		if pending.owned {
			return errors.New("its RunTool call is still running and owns it")
		}
		select {
		case <-pending.done:
			delete(m.pendingTools, id)
		default:
			return errors.New("its sole waiter has not reaped it")
		}
	}
	if record.PID != 0 {
		alive, err := recordAlive(record)
		if err != nil {
			return err
		}
		if alive {
			if err := m.verifyProcessLocked(ctx, record); err != nil {
				return fmt.Errorf("live and unverifiable: %w", err)
			}
			pidfd, alive, err := openVerifiedPidfd(record)
			if err != nil {
				return err
			}
			if alive {
				err := pidfdKill(pidfd)
				if err == nil {
					err = waitPidfdReadable(ctx, pidfd, cleanupTimeout)
				}
				pidfd.Close()
				if err != nil {
					return err
				}
			}
		}
	}
	return m.releaseLocked(ctx, id, nil, &record)
}

// waitPidfdReadable waits for a pidfd to report the exit, bounded by limit
// and by ctx (checked at least every 100ms).
func waitPidfdReadable(ctx context.Context, f *os.File, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("jailer: wait for the killed tool: %w", context.Cause(ctx))
		}
		left := time.Until(deadline)
		if left <= 0 {
			return errors.New("jailer: killed tool did not exit in time")
		}
		poll := []unix.PollFd{{Fd: int32(f.Fd()), Events: unix.POLLIN}}
		n, err := unix.Poll(poll, int(min(left, 100*time.Millisecond).Milliseconds())+1)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("jailer: poll pidfd: %w", err)
		}
		if n > 0 && poll[0].Revents&unix.POLLIN != 0 {
			return nil
		}
	}
}

// TargetHolder returns the attempt whose record holds the inode dev/ino
// (the strongest holder: a writer before a shared-ro reader).
func (m *Manager) TargetHolder(dev, ino uint64) (Ref, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var found *Record
	for _, record := range m.sortedRecordsLocked() {
		if record.TargetIno == 0 || record.TargetDev != dev || record.TargetIno != ino {
			continue
		}
		if found == nil || (recordAccess(*found) == AccessShared && recordAccess(record) != AccessShared) {
			found = &record
		}
	}
	if found == nil {
		return Ref{}, false
	}
	return found.Ref(), true
}

// toolExitObserveLimit caps the exec proof's wait for a fast tool's exit
// to become observable after its executable vanished (exit_mm before
// exit_notify); the run's own context bounds it too.
const toolExitObserveLimit = 2 * time.Second
