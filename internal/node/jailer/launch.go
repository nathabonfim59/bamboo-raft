package jailer

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	cleanupTimeout = 5 * time.Second
	// setupTimeout bounds the helper's mount/chroot/exec setup even when the
	// caller's context has no deadline, so Start cannot hold the Manager
	// forever on a stuck helper.
	setupTimeout = 30 * time.Second
	maxStatus    = 16 << 10
)

// Child is a launched helper that either has execed QEMU or failed during
// setup. Cmd.Process.Pid remains the QEMU host pid across the same-PID exec.
// The caller owns exactly one Cmd.Wait and its reaper; the jailer never calls
// Wait and never closes or releases Cmd.Process. After a post-spawn Start
// error the caller still receives Child, starts that reaper immediately and
// calls Release after Wait returns.
type Child struct {
	Cmd *exec.Cmd

	manager *Manager
	record  Record
	handle  *processHandle
	// proof is how a tool's exec was proven.
	proof ToolProof
}

// Identity returns the child's durable host identity. PID and StartTicks are
// zero when Start failed before the identity was captured.
func (c *Child) Identity() Record { return c.record }

// Kill sends SIGKILL through the child's pinned pidfd, or through the
// caller's not yet reaped os.Process on a host without pidfds. QEMU is PID 1
// in its namespace, so SIGKILL is the reliable fallback after a graceful QMP
// quit. Kill keeps working after Manager.Close and fails only after Release
// released the handle.
func (c *Child) Kill() error {
	if c == nil || c.handle == nil {
		return errHandleReleased
	}
	return c.handle.kill()
}

// Detach closes only the Child's process handle and leaves every resource
// reserved. It is the explicit owner-shutdown path: the durable record keeps
// the attempt's identity for a later Manager, which releases or adopts it by
// proof. Kill and Release then return an error.
func (c *Child) Detach() error {
	if c == nil || c.handle == nil {
		return nil
	}
	return c.handle.close()
}

// Release removes resources only after the handle proves exit and the cgroup
// is empty, then releases the handle. The Manager's reclaim policy runs
// between those proofs and the first removal. The caller invokes it after its sole
// Cmd.Wait returns. ErrAlive and other errors keep the handle and every
// resource, so Release can be retried. After Manager.Close, Release only
// releases this Child's handle and returns ErrClosed: the closed Manager
// never touches a newer Manager's reservations, which recover the record
// through Reconcile.
func (c *Child) Release(ctx context.Context) error {
	if c == nil || c.manager == nil {
		return nil
	}
	m := c.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return errors.Join(ErrClosed, c.handle.close())
	}
	if err := m.releaseLocked(ctx, c.record.SandboxID, c.handle, &c.record); err != nil {
		return err
	}
	return c.handle.close()
}

// Start reserves and launches a jailed QEMU. Each durable boundary precedes
// the host mutation it covers: the uid/control/log intent before the jail,
// the jail base identity before its contents, the full jail inventory before
// the cgroup, the cgroup identity before the spawn. One clone3 creates the
// blocked helper inside its owned cgroup (CLONE_INTO_CGROUP) and returns its
// pidfd (CLONE_PIDFD); the parent verifies that membership, captures the
// start ticks and commits the identity before releasing the helper. Exec is proven by
// the staged QEMU inode, not by the status pipe alone.
//
// ctx bounds only the startup and is checked at every boundary, between
// staged runtime files and copy chunks, and once more after the exec proof.
// Cancellation before the spawn rolls back with no process; after it, the
// child is killed but not reaped. A successful Child is detached from ctx.
// Every post-spawn error returns a nonnil Child so the caller can perform
// its sole Wait and then Release; no error path loses a live process. The
// helper's status wait is additionally capped at setupTimeout. Start holds
// the Manager lock until it returns.
func (m *Manager) Start(ctx context.Context, spec LaunchSpec) (*Child, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := spec.validate(); err != nil {
		return nil, err
	}
	checkDevices := m.checkDeviceRoles
	if spec.Maintenance {
		checkDevices = m.checkMaintenanceDevices
	}
	if err := checkDevices(spec); err != nil {
		return nil, err
	}
	return m.launchLocked(ctx, spec, KindVM, nil)
}

// launchLocked runs the durable reservation, jail, cgroup, spawn, identity
// and exec-proof sequence for an already validated spec of kind. m.mu is
// held by the caller for the whole launch.
//
// tool is non-nil for a tool attempt: no firmware, control directory or
// serial log; the target gets the access its spec grants after the durable
// intent and the conflict check; and the exec proof may be the tool-only
// fast-exit proof.
func (m *Manager) launchLocked(ctx context.Context, spec LaunchSpec, kind string, tool *ToolSpec) (*Child, error) {
	id := spec.SandboxID
	if retained, ok := m.records[id]; ok {
		// Never release here: only the caller's settlement of that exact
		// attempt runs the reclaim its resources need.
		return nil, fmt.Errorf("%w: %s attempt %s", ErrRetained, id, retained.Attempt)
	}
	firmware := m.cfg.FirmwareFiles
	if tool != nil {
		firmware = nil
	}
	runtimeFiles, err := resolveRuntime(ctx, spec.Binary, m.cfg.LibraryDirs, firmware)
	if err != nil {
		return nil, err
	}
	devices, err := m.fixedDevices()
	if err != nil {
		return nil, err
	}
	uid, err := m.allocateUID()
	if err != nil {
		return nil, err
	}
	var record Record
	if tool != nil {
		record, err = newToolRecord(*tool, uid)
	} else {
		record, err = newRecord(spec, kind, uid)
	}
	if err != nil {
		return nil, err
	}
	if err := m.checkControlCollision(record); err != nil {
		return nil, err
	}
	if tool != nil {
		// Read-only at-rest proof, after the conflict check and before any
		// reservation or ownership change.
		if err := m.checkToolFiles(*tool); err != nil {
			return nil, err
		}
	}

	// The reservation exists in memory before its first durable write, so
	// an uncertain write can never hand this uid to another sandbox.
	var own resourceOwnership
	m.setRecord(record, own)
	if err := m.writeRecord(record, own); err != nil {
		return nil, m.failIntent(id, err)
	}
	staged, err := m.prepareJail(ctx, record, &own, runtimeFiles)
	if err != nil {
		return nil, m.abandonPrespawn(ctx, id, err)
	}
	if err := m.prepareCgroup(ctx, spec, record, &own); err != nil {
		return nil, m.abandonPrespawn(ctx, id, err)
	}
	if tool != nil {
		err = m.ownToolTarget(ctx, *tool, record)
	} else {
		err = m.ownFiles(ctx, spec, record)
	}
	if err != nil {
		return nil, m.abandonPrespawn(ctx, id, err)
	}
	if err := m.step(ctx, stageSpawn); err != nil {
		return nil, m.abandonPrespawn(ctx, id, err)
	}
	// The helper is born inside its owned cgroup (CLONE_INTO_CGROUP): from
	// its first instruction it is visible to every by-id Release, Reconcile
	// and restart, which then keep the reservation until it is gone. There
	// is no spawn-then-attach fallback; without clone3 nothing is spawned.
	cgroupDir, err := m.cgroups.OpenOwned(id, own.Cgroup)
	if err != nil {
		return nil, m.abandonPrespawn(ctx, id, err)
	}
	defer cgroupDir.Close()
	// The helper lives in a new mount namespace and cannot bind the
	// caller's host-namespace descriptors. Hand it non-recursive detached
	// clones instead; Start owns these copies and closes them on return.
	var controlTree, logTree *os.File
	if tool == nil {
		if controlTree, err = m.detachedMount(spec.ControlDir); err != nil {
			return nil, m.abandonPrespawn(ctx, id, fmt.Errorf("jailer: detach control directory: %w", err))
		}
		defer controlTree.Close()
		if logTree, err = m.detachedMount(spec.SerialLog); err != nil {
			return nil, m.abandonPrespawn(ctx, id, fmt.Errorf("jailer: detach serial log: %w", err))
		}
		defer logTree.Close()
	}
	cmd, release, status, pidfd, err := m.spawn(spec, record, devices, cgroupDir, controlTree, logTree, tool)
	if err != nil {
		return nil, m.abandonPrespawn(ctx, id, err)
	}
	child := &Child{Cmd: cmd, manager: m, record: record}
	handle, pinErr := newProcessHandle(cmd.Process, pidfd, m.openPidfd)
	if tool != nil && handle != nil {
		handle.exitHidden = m.hooks.toolExitHidden
	}
	child.handle = handle
	fail := func(cause error) (*Child, error) {
		release.Close()
		status.Close()
		if err := m.killChild(child.handle); err != nil {
			cause = errors.Join(cause, err)
		}
		// Resources deliberately remain until the caller's Wait and Release.
		return child, cause
	}
	// Verify the birth membership first; on any failure below the child is
	// already contained and keeps the reservation until proven dead.
	joinErr := m.cgroups.Joined(id, cmd.Process.Pid, own.Cgroup)
	if joinErr == nil {
		handle.markContained()
	}
	if pinErr != nil {
		return fail(pinErr)
	}
	if err := m.step(ctx, stageAttach); err != nil {
		return fail(err)
	}
	if joinErr != nil {
		return fail(joinErr)
	}
	if err := m.step(ctx, stagePin); err != nil {
		return fail(err)
	}
	ticks, err := handle.capture()
	if err != nil {
		return fail(err)
	}
	record.PID = cmd.Process.Pid
	record.StartTicks = ticks
	child.record = record
	// A tool's helper comm is captured before release, while it is surely
	// the helper's: the fast-exit proof must be able to tell it apart.
	var helperComm string
	if tool != nil {
		helperComm, _ = readComm(record.PID)
		if m.hooks.toolHelperComm != nil {
			helperComm = m.hooks.toolHelperComm(helperComm)
		}
	}
	m.setRecord(record, own) // the identity is retained even if the commit fails
	if err := m.step(ctx, stageCommit); err != nil {
		return fail(err)
	}
	if err := m.writeRecord(record, own); err != nil {
		return fail(err)
	}
	if err := m.step(ctx, stageRelease); err != nil {
		return fail(err)
	}
	if _, err := release.Write([]byte{1}); err != nil {
		return fail(fmt.Errorf("jailer: release helper: %w", err))
	}
	if err := release.Close(); err != nil {
		return fail(fmt.Errorf("jailer: close helper release: %w", err))
	}
	if err := m.step(ctx, stageStatus); err != nil {
		return fail(err)
	}
	if err := readExecStatus(ctx, status); err != nil {
		return fail(err)
	}
	if err := status.Close(); err != nil {
		return fail(fmt.Errorf("jailer: close exec status: %w", err))
	}
	// A clean EOF can race a cancellation; the step re-checks ctx, and the
	// final check makes success impossible once ctx is done.
	if err := m.step(ctx, stageExecProof); err != nil {
		return fail(err)
	}
	if tool != nil {
		if !handle.hasPidfd() {
			return fail(errors.New("jailer: a tool needs a pidfd for its exec proof"))
		}
		proof, err := m.proveToolExec(ctx, handle, staged, record, helperComm, spec.Binary)
		if err != nil {
			return fail(err)
		}
		child.proof = proof
	} else if err := handle.proveExec(staged); err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return fail(fmt.Errorf("jailer: exec proof: %w", context.Cause(ctx)))
	}
	m.log.Debug("jailer: started", "sandbox", id, "pid", record.PID, "uid", record.UID)
	return child, nil
}

// prepareJail creates the attempt-owned jail. The base identity is durable
// before anything is created inside it, and the complete inventory is
// durable before Start continues. In-memory ownership is updated as each
// entry appears, so an in-process rollback removes exactly those entries.
func (m *Manager) prepareJail(ctx context.Context, record Record, own *resourceOwnership, files []runtimeFile) (inodeIdentity, error) {
	base := path.Join(jailsDir, record.SandboxID)
	if err := m.step(ctx, stageJailBase); err != nil {
		return inodeIdentity{}, err
	}
	if err := m.root.Mkdir(base, 0o700); err != nil {
		return inodeIdentity{}, fmt.Errorf("jailer: create private jail: %w", err)
	}
	identity, err := statDirectoryNoFollow(m.root, base)
	if err != nil {
		// The new base exists but cannot be proven ours: it is retained.
		return inodeIdentity{}, fmt.Errorf("jailer: identify private jail: %w", err)
	}
	own.Jail = identity
	m.setRecord(record, *own)
	if err := syncDirectory(m.root, jailsDir); err != nil {
		return inodeIdentity{}, err
	}
	if err := m.step(ctx, stageJailBaseRecord); err != nil {
		return inodeIdentity{}, err
	}
	if err := m.writeRecord(record, *own); err != nil {
		return inodeIdentity{}, err
	}
	if err := m.step(ctx, stageJailPopulate); err != nil {
		return inodeIdentity{}, err
	}
	builder := newJailBuilder(m.root, base, func(entry ownedEntry) {
		own.Entries = append(own.Entries, entry)
		m.setRecord(record, *own)
	}, m.hooks.jailOp)
	staged, err := populateJail(ctx, builder, files)
	if err != nil {
		return inodeIdentity{}, err
	}
	// The staged executable is now an owned entry; recovery proves a live
	// process's /proc/pid/exe against it.
	own.Exec = path.Join("root", files[0].target)
	m.setRecord(record, *own)
	if err := builder.verify(ctx, own.Entries); err != nil {
		return inodeIdentity{}, err
	}
	// Every new entry's parent directory is durable before the inventory
	// that claims it is committed.
	if err := builder.barrier(); err != nil {
		return inodeIdentity{}, err
	}
	if err := m.step(ctx, stageJailRecord); err != nil {
		return inodeIdentity{}, err
	}
	if err := m.writeRecord(record, *own); err != nil {
		return inodeIdentity{}, err
	}
	return staged, nil
}

// prepareCgroup creates the limited cgroup and persists its identity. An
// identity returned with an error is still recorded for rollback.
func (m *Manager) prepareCgroup(ctx context.Context, spec LaunchSpec, record Record, own *resourceOwnership) error {
	if err := m.step(ctx, stageCgroup); err != nil {
		return err
	}
	identity, err := m.cgroups.Prepare(spec.SandboxID, spec.Limits)
	if identity.valid() {
		own.Cgroup = identity
		m.setRecord(record, *own)
	}
	if err != nil {
		return err
	}
	if err := m.step(ctx, stageCgroupRecord); err != nil {
		return err
	}
	return m.writeRecord(record, *own)
}

// ownFiles gives the per-VM uid the pinned control directory, serial log and
// writable overlay. Read-only backing layers, kernel and initrd stay as the
// caller opened them.
func (m *Manager) ownFiles(ctx context.Context, spec LaunchSpec, record Record) error {
	if err := m.step(ctx, stageOwnFiles); err != nil {
		return err
	}
	if err := spec.ControlDir.Chmod(0o700); err != nil {
		return fmt.Errorf("jailer: protect control dir: %w", err)
	}
	if err := m.chownFile(spec.ControlDir, record.UID); err != nil {
		return fmt.Errorf("jailer: own control dir: %w", err)
	}
	if err := spec.SerialLog.Chmod(0o600); err != nil {
		return fmt.Errorf("jailer: protect serial log: %w", err)
	}
	if err := m.chownFile(spec.SerialLog, record.UID); err != nil {
		return fmt.Errorf("jailer: own serial log: %w", err)
	}
	if len(spec.Disks) == 0 {
		return nil
	}
	overlay := spec.descriptor(spec.Disks[len(spec.Disks)-1])
	// The overlay is private to its VM uid: no other uid may read the disk.
	if err := overlay.Chmod(0o600); err != nil {
		return fmt.Errorf("jailer: protect writable overlay: %w", err)
	}
	if err := m.chownFile(overlay, record.UID); err != nil {
		return fmt.Errorf("jailer: own writable overlay: %w", err)
	}
	return nil
}

// helperConfig is the helper's private, fd-passed launch description.
type helperConfig struct {
	// Root is the host path of the jail root; Binary is QEMU's path inside it.
	Root      string   `json:"root"`
	Binary    string   `json:"binary"`
	Args      []string `json:"args"`
	UID       uint32   `json:"uid"`
	ControlFD int      `json:"controlFD"`
	LogFD     int      `json:"logFD"`
	// Control and Log are the pinned inodes the detached mounts must root.
	Control    inodeIdentity `json:"control"`
	Log        inodeIdentity `json:"log"`
	KVMRdev    uint64        `json:"kvmRdev"`
	RandomRdev uint64        `json:"randomRdev"`
	NullRdev   uint64        `json:"nullRdev"`
	// Tool selects a tool jail: no control, serial log or KVM, and
	// RLIMIT_CORE 0 before the credential drop (memory is bounded by the
	// birth cgroup's memory.max).
	Tool bool `json:"tool,omitzero"`
}

// spawn starts the blocked same-PID helper. Child fds 3..3+len(Files)-1 are
// the caller's descriptors in order; the helper's config, release, status,
// control and log descriptors follow them.
//
// cgroupDir is the owned group's directory: clone3 places the helper in it
// (CLONE_INTO_CGROUP) and returns its pidfd (CLONE_PIDFD), which spawn hands
// back as the caller's sole copy, or -1. The cgroup fd is close-on-exec and
// is not in ExtraFiles, so the child never inherits it.
//
// controlTree and logTree are the detached pinned mounts from detachedMount;
// they occupy the ControlFD and LogFD slots.
func (m *Manager) spawn(spec LaunchSpec, record Record, devices privateDevices, cgroupDir, controlTree, logTree *os.File, tool *ToolSpec) (*exec.Cmd, *os.File, *os.File, int, error) {
	n := len(spec.Files)
	configFD := 3 + n
	cfg := helperConfig{
		Root:       filepath.Join(m.cfg.StateDir, jailsDir, record.SandboxID, "root"),
		Binary:     spec.Binary,
		Args:       append([]string(nil), spec.Args...),
		UID:        record.UID,
		RandomRdev: devices.Random,
		NullRdev:   devices.Null,
	}
	if tool == nil {
		cfg.ControlFD, cfg.LogFD = configFD+3, configFD+4
		cfg.Control = inodeIdentity{Dev: record.ControlDev, Ino: record.ControlIno}
		cfg.Log = inodeIdentity{Dev: record.LogDev, Ino: record.LogIno}
		cfg.KVMRdev = devices.KVM
	} else {
		cfg.Tool = true
	}
	configFile, err := helperConfigFile(cfg)
	if err != nil {
		return nil, nil, nil, -1, err
	}
	defer configFile.Close()
	releaseReader, releaseWriter, err := os.Pipe()
	if err != nil {
		return nil, nil, nil, -1, fmt.Errorf("jailer: release pipe: %w", err)
	}
	statusReader, statusWriter, err := os.Pipe()
	if err != nil {
		releaseReader.Close()
		releaseWriter.Close()
		return nil, nil, nil, -1, fmt.Errorf("jailer: status pipe: %w", err)
	}
	extra := make([]*os.File, 0, n+5)
	for _, d := range spec.Files {
		extra = append(extra, d.File)
	}
	extra = append(extra, configFile, releaseReader, statusWriter)
	if tool == nil {
		extra = append(extra, controlTree, logTree)
	}
	cmd := exec.Command("/proc/self/exe", HelperArg,
		strconv.Itoa(configFD), strconv.Itoa(configFD+1), strconv.Itoa(configFD+2))
	cmd.ExtraFiles = extra
	cmd.Env = []string{}
	cmd.Dir = "/"
	cmd.Stdout = spec.Stdout
	cmd.Stderr = spec.Stderr
	pidfd := -1 // set by a successful clone3 only
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:  unix.CLONE_NEWNS | unix.CLONE_NEWPID | unix.CLONE_NEWIPC,
		Setpgid:     true,
		UseCgroupFD: true,
		CgroupFD:    int(cgroupDir.Fd()),
		PidFD:       &pidfd,
	}
	if m.hooks.command != nil {
		m.hooks.command(cmd)
	}
	startErr := cmd.Start()
	// The child holds its own copies; close the parent's helper ends. The
	// caller's descriptors stay open: it closes them after Start returns.
	releaseReader.Close()
	statusWriter.Close()
	if startErr != nil {
		releaseWriter.Close()
		statusReader.Close()
		return nil, nil, nil, -1, fmt.Errorf("jailer: start helper: %w", startErr)
	}
	return cmd, releaseWriter, statusReader, pidfd, nil
}

// helperConfigFile returns a sealed anonymous memfd holding cfg, so no
// filesystem name is created, replaced or removed for it.
func helperConfigFile(cfg helperConfig) (*os.File, error) {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("jailer: encode helper config: %w", err)
	}
	fd, err := unix.MemfdCreate("bbraft-jailer-config", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, fmt.Errorf("jailer: helper config memfd: %w", err)
	}
	f := os.NewFile(uintptr(fd), "helper-config")
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return nil, fmt.Errorf("jailer: write helper config: %w", err)
	}
	if _, err := unix.FcntlInt(f.Fd(), unix.F_ADD_SEALS, unix.F_SEAL_SEAL|unix.F_SEAL_SHRINK|unix.F_SEAL_GROW|unix.F_SEAL_WRITE); err != nil {
		f.Close()
		return nil, fmt.Errorf("jailer: seal helper config: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("jailer: rewind helper config: %w", err)
	}
	return f, nil
}

// openTreeFlags clone exactly the descriptor's own mount, non-recursively
// (no AT_RECURSIVE, so no submount below it), as a detached tree whose fd
// is close-on-exec (OPEN_TREE_CLOEXEC is O_CLOEXEC).
const openTreeFlags = unix.AT_EMPTY_PATH | unix.OPEN_TREE_CLONE | unix.O_CLOEXEC

// pinnedMountAttr is applied to each detached pinned mount before the
// helper attaches it: nosuid, nodev, noexec and private propagation.
func pinnedMountAttr() unix.MountAttr {
	return unix.MountAttr{
		Attr_set:    unix.MOUNT_ATTR_NOSUID | unix.MOUNT_ATTR_NODEV | unix.MOUNT_ATTR_NOEXEC,
		Propagation: unix.MS_PRIVATE,
	}
}

// detachPinned clones f's mount into a detached tree rooted at f's inode
// and restricts it. It runs in the node's own mount namespace, where the
// caller's descriptor is valid. Any unavailable API fails closed.
func detachPinned(f *os.File) (*os.File, error) {
	fd, err := unix.OpenTree(int(f.Fd()), "", openTreeFlags)
	if err != nil {
		return nil, fmt.Errorf("open_tree: %w", err)
	}
	tree := os.NewFile(uintptr(fd), "pinned-mount")
	attr := pinnedMountAttr()
	if err := unix.MountSetattr(fd, "", unix.AT_EMPTY_PATH, &attr); err != nil {
		tree.Close()
		return nil, fmt.Errorf("mount_setattr: %w", err)
	}
	want, err := fileInode(f)
	if err == nil {
		err = checkFDIdentity(fd, want)
	}
	if err != nil {
		tree.Close()
		return nil, err
	}
	return tree, nil
}

func (m *Manager) detachedMount(f *os.File) (*os.File, error) {
	if m.hooks.detachMount != nil {
		return m.hooks.detachMount(f)
	}
	return detachPinned(f)
}

func fixedDevice(name string) (uint64, error) {
	var st unix.Stat_t
	if err := unix.Stat(name, &st); err != nil {
		return 0, fmt.Errorf("jailer: stat fixed device %s: %w", name, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFCHR {
		return 0, fmt.Errorf("jailer: fixed device %s is not a character device", name)
	}
	return st.Rdev, nil
}

// readExecStatus waits for EOF on the close-on-exec status pipe. Any bytes
// are a helper setup error. EOF alone is not success: it also happens when
// the helper dies or closes the pipe; Start proves the exec separately.
func readExecStatus(ctx context.Context, f *os.File) error {
	ctx, cancel := context.WithTimeoutCause(ctx, setupTimeout, errors.New("jailer: helper setup timed out"))
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = f.SetReadDeadline(time.Now()) })
	defer stop()
	raw, err := io.ReadAll(io.LimitReader(f, maxStatus))
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("jailer: helper setup: %w", context.Cause(ctx))
		}
		return fmt.Errorf("jailer: read helper exec status: %w", err)
	}
	if len(raw) != 0 {
		return fmt.Errorf("jailer: helper setup failed: %s", strings.TrimSpace(string(raw)))
	}
	return nil
}
