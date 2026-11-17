package jailer

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// Manager owns one node's durable jail reservations. Its methods are safe for
// concurrent use. A permanent no-follow lock file under StateDir serializes
// independent managers, including managers in different processes. Close
// releases only local directory/lock handles; live VMs and unresolved records
// retain their uids, cgroups and jail trees.
type Manager struct {
	cfg     Config
	log     *slog.Logger
	root    *os.Root
	cgroups cgroupOps
	lock    *os.File
	state   stateStore

	mu           sync.Mutex
	records      map[string]Record
	ownership    map[string]resourceOwnership
	stateEntries map[string]inodeIdentity
	// pendingTools are tool children whose sole waiter had not reaped them
	// when their run returned ErrToolRetained.
	pendingTools map[string]*pendingTool
	hooks        managerHooks
}

// launchStage names a Start boundary for fault injection and witnesses.
type launchStage string

const (
	stageJailBase       launchStage = "jail-base"
	stageJailBaseRecord launchStage = "jail-base-record"
	stageJailPopulate   launchStage = "jail-populate"
	stageJailRecord     launchStage = "jail-record"
	stageCgroup         launchStage = "cgroup"
	stageCgroupRecord   launchStage = "cgroup-record"
	stageOwnFiles       launchStage = "own-files"
	stageSpawn          launchStage = "spawn"
	stagePin            launchStage = "pin"
	stageAttach         launchStage = "attach"
	stageCommit         launchStage = "commit"
	stageRelease        launchStage = "release"
	stageStatus         launchStage = "status"
	stageExecProof      launchStage = "exec-proof"
	// stageReclaim is reached in every release after the death and
	// empty-group proof, immediately before the reclaim policy runs.
	stageReclaim launchStage = "reclaim"
)

// managerHooks are per-Manager test seams; production leaves them nil. They
// replace only privileged host operations (chown, device lookup, namespace
// clone flags) or inject failures, never ownership decisions.
type managerHooks struct {
	// stage runs before each launch boundary; an error fails that boundary.
	stage func(launchStage) error
	// command adjusts the helper command just before it starts.
	command func(*exec.Cmd)
	// pidfdOpen replaces pidfd_open(2).
	pidfdOpen func(pid int) (int, error)
	// chown replaces fchown of the caller's control dir, log and overlay.
	chown func(f *os.File, uid int) error
	// devices replaces the /dev/kvm, /dev/urandom and /dev/null rdev lookup.
	devices func() (privateDevices, error)
	// deviceRdev replaces the stat of a role's host device node
	// (/dev/net/tun, /dev/vhost-net, /dev/vhost-vsock).
	deviceRdev func(path string) (uint64, error)
	// tapFlags replaces TUNGETIFF on the caller's TAP descriptor.
	tapFlags func(*os.File) (uint16, error)
	// jailOp runs after each jail builder operation on rel; an error fails
	// that operation as if the syscall had failed at that point.
	jailOp func(op jailOp, rel string) error
	// kill replaces Start's post-failure SIGKILL of the child, so tests can
	// make that kill fail or be delayed.
	kill func(*processHandle) error
	// detachMount replaces detachPinned (open_tree + mount_setattr, which
	// need CAP_SYS_ADMIN) for the control directory and serial log.
	detachMount func(*os.File) (*os.File, error)
	// proc replaces the host /proc reader VerifyProcess uses.
	proc procFS
	// admitTimeout replaces the admission helper's setup bound.
	admitTimeout time.Duration
	// admitState injects failures at the admission journal's durable
	// boundaries and the admission cleanup barriers.
	admitState func(stateFault) error
	// admitNull replaces the private /dev/null node check (a character
	// device with /dev/null's rdev), which an unprivileged test cannot mknod.
	admitNull func(fs.FileInfo) bool
	// admitReaped runs in the admission helper's sole waiter after its
	// reap and the close of its process handle.
	admitReaped func(*processHandle)
	// atRestOwner replaces the uid a tool target must be owned by at rest
	// (root), which an unprivileged test cannot create.
	atRestOwner func() uint32
	// toolChown replaces fchown of a tool target (uid, gid).
	toolChown func(f *os.File, uid, gid int) error
	// toolHelperComm replaces the helper comm captured before release.
	toolHelperComm func(captured string) string
	// toolArgs replaces a tool's fixed argv (privileged suite only, to
	// keep a live tool for inspection).
	toolArgs func(args []string) []string
	// toolReaped runs in a tool's sole waiter after its consuming Wait and
	// the close of its private process handle.
	toolReaped func(id string, h *processHandle, waitErr error)
	// beforeToolSettle runs in RunTool after the reap is observed and
	// before its own settlement takes the Manager lock.
	beforeToolSettle func(id string)
	// toolExitHidden, while open, makes a tool handle report its exit as
	// not yet observable (pidfd poll alive, waitid nothing), standing in for
	// the kernel window between exit_mm (exe gone) and exit_notify. Data,
	// not a callback: it is read under the handle lock by a non-blocking
	// select only.
	toolExitHidden <-chan struct{}
	// toolExitWaiting (buffered) receives a non-blocking witness when a
	// tool's exec proof starts waiting for that exit to become observable.
	toolExitWaiting chan<- struct{}
	// toolLiveRefused (buffered) receives a non-blocking witness right
	// after a tool's live exec proof failed; when toolLiveResume is set the
	// proof then waits for it to close (or its context) before deciding,
	// so a test can make the child exit in exactly that window. Data only.
	toolLiveRefused chan<- struct{}
	toolLiveResume  <-chan struct{}
}

func (m *Manager) killChild(h *processHandle) error {
	if m.hooks.kill != nil {
		return m.hooks.kill(h)
	}
	return h.kill()
}

// step passes a launch boundary: the stage hook, then cancellation.
func (m *Manager) step(ctx context.Context, stage launchStage) error {
	if err := m.reach(stage); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("jailer: %s: %w", stage, context.Cause(ctx))
	}
	return nil
}

func (m *Manager) roleDeviceRdev(path string) (uint64, error) {
	if m.hooks.deviceRdev != nil {
		return m.hooks.deviceRdev(path)
	}
	return fixedDevice(path)
}

func (m *Manager) tapDeviceFlags(f *os.File) (uint16, error) {
	if m.hooks.tapFlags != nil {
		return m.hooks.tapFlags(f)
	}
	ifr, err := unix.NewIfreq("")
	if err != nil {
		return 0, err
	}
	if err := unix.IoctlIfreq(int(f.Fd()), unix.TUNGETIFF, ifr); err != nil {
		return 0, fmt.Errorf("jailer: TUNGETIFF: %w", err)
	}
	return ifr.Uint16(), nil
}

// checkDeviceRoles proves each device descriptor is the device its role
// names, using only the caller's descriptors: the live TAP is never reopened.
// It runs before any reservation, chmod or chown.
func (m *Manager) checkDeviceRoles(spec LaunchSpec) error {
	for _, role := range []struct{ name, path string }{
		{TAPName, "/dev/net/tun"},
		{VhostNetName, "/dev/vhost-net"},
		{VhostVsockName, "/dev/vhost-vsock"},
	} {
		want, err := m.roleDeviceRdev(role.path)
		if err != nil {
			return err
		}
		var st unix.Stat_t
		if err := unix.Fstat(int(spec.descriptor(role.name).Fd()), &st); err != nil {
			return fmt.Errorf("jailer: descriptor %q: %w", role.name, err)
		}
		if st.Mode&unix.S_IFMT != unix.S_IFCHR || st.Rdev != want {
			return fmt.Errorf("jailer: descriptor %q is not %s", role.name, role.path)
		}
	}
	flags, err := m.tapDeviceFlags(spec.descriptor(TAPName))
	if err != nil {
		return fmt.Errorf("jailer: descriptor %q: %w", TAPName, err)
	}
	const mode = unix.IFF_TUN | unix.IFF_TAP
	const want = unix.IFF_NO_PI | unix.IFF_VNET_HDR
	if flags&mode != unix.IFF_TAP || flags&want != want {
		return fmt.Errorf("jailer: descriptor %q is not an attached IFF_TAP|IFF_NO_PI|IFF_VNET_HDR device (flags %#x)", TAPName, flags)
	}
	return nil
}

func (m *Manager) reach(stage launchStage) error {
	if m.hooks.stage == nil {
		return nil
	}
	if err := m.hooks.stage(stage); err != nil {
		return fmt.Errorf("jailer: %s: %w", stage, err)
	}
	return nil
}

func (m *Manager) openPidfd(pid int) (int, error) {
	if m.hooks.pidfdOpen != nil {
		return m.hooks.pidfdOpen(pid)
	}
	return unix.PidfdOpen(pid, 0)
}

func (m *Manager) chownFile(f *os.File, uid uint32) error {
	if m.hooks.chown != nil {
		return m.hooks.chown(f, int(uid))
	}
	return f.Chown(int(uid), int(uid))
}

// privateDevices are the host rdevs of the package-fixed device nodes the
// helper creates (mknod, VM-owned 0600) inside the jail.
type privateDevices struct {
	KVM, Random, Null uint64
}

func (m *Manager) fixedDevices() (privateDevices, error) {
	if m.hooks.devices != nil {
		return m.hooks.devices()
	}
	var d privateDevices
	for _, dev := range []struct {
		name string
		rdev *uint64
	}{{"/dev/kvm", &d.KVM}, {"/dev/urandom", &d.Random}, {"/dev/null", &d.Null}} {
		rdev, err := fixedDevice(dev.name)
		if err != nil {
			return privateDevices{}, err
		}
		*dev.rdev = rdev
	}
	return d, nil
}

// New opens the state and delegated cgroup roots and loads all reservations.
// Invalid inventory is rejected before the cgroup root is opened or mutated.
func New(cfg Config) (_ *Manager, returnErr error) {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return nil, fmt.Errorf("jailer: create state dir: %w", err)
	}
	root, err := os.OpenRoot(cfg.StateDir)
	if err != nil {
		return nil, fmt.Errorf("jailer: open state dir: %w", err)
	}
	defer func() {
		if returnErr != nil {
			_ = root.Close()
		}
	}()
	if err := syncDirectory(root, "."); err != nil {
		return nil, err
	}
	if err := syncHostDirectory(filepath.Dir(cfg.StateDir)); err != nil {
		return nil, err
	}
	lock, err := lockStateDir(root)
	if err != nil {
		return nil, err
	}
	defer func() {
		if returnErr != nil {
			_ = lock.Close()
		}
	}()
	for _, dir := range []string{recordsDir, jailsDir} {
		if err := ensureStateSubdir(root, dir); err != nil {
			return nil, err
		}
	}
	records, ownership, stateEntries, err := loadRecords(root)
	if err != nil {
		return nil, err
	}
	if err := validateInventory(records, cfg.UIDs); err != nil {
		return nil, err
	}
	cgroup, err := os.OpenRoot(cfg.CgroupParent)
	if err != nil {
		return nil, fmt.Errorf("jailer: open cgroup parent: %w", err)
	}
	return &Manager{
		cfg: cfg, log: cfg.Log, root: root, cgroups: cgroupFS{root: cgroup}, lock: lock, state: fileStates{},
		records: records, ownership: ownership, stateEntries: stateEntries,
	}, nil
}

func syncHostDirectory(name string) error {
	d, err := os.Open(name)
	if err != nil {
		return fmt.Errorf("jailer: open state parent for sync: %w", err)
	}
	if err := errors.Join(d.Sync(), d.Close()); err != nil {
		return fmt.Errorf("jailer: sync state parent: %w", err)
	}
	return nil
}

func lockStateDir(root *os.Root) (*os.File, error) {
	f, err := root.OpenFile(stateLockName, os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, fmt.Errorf("jailer: open state lock: %w", err)
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		if err == nil {
			err = errors.New("state lock is not a regular file")
		}
		return nil, fmt.Errorf("jailer: inspect state lock: %w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("jailer: state directory is already managed: %w", err)
	}
	// The lock inode is permanent and is never unlinked at Close. Sync its
	// creation and every retry barrier while the exclusive flock is held.
	if err := syncDirectory(root, "."); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func ensureStateSubdir(root *os.Root, name string) error {
	if err := root.Mkdir(name, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("jailer: create %s: %w", name, err)
	}
	if _, err := statDirectoryNoFollow(root, name); err != nil {
		return fmt.Errorf("jailer: inspect %s: %w", name, err)
	}
	// This also supplies the parent barrier after an EEXIST left by a previous
	// creator; an existing regular file or symlink is never accepted.
	return syncDirectory(root, ".")
}

// Close closes local handles without changing any VM resource and releases
// the state lock so a new Manager can own StateDir. The lock inode itself is
// permanent. Live and unresolved reservations stay on disk for that Manager.
// A Child from this Manager keeps its own process handle: Child.Kill still
// works, and Child.Release closes that handle and returns ErrClosed without
// touching any other Manager's reservations.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return nil
	}
	var errs []error
	if m.cgroups != nil {
		errs = append(errs, m.cgroups.Close())
	}
	errs = append(errs, m.root.Close())
	if m.lock != nil {
		errs = append(errs, m.lock.Close())
	}
	m.cgroups = nil
	m.root = nil
	m.lock = nil
	return errors.Join(errs...)
}

// Record returns the durable reservation for id.
func (m *Manager) Record(id string) (Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.records[id]
	return record, ok
}

// Records returns all reservations ordered by uid.
func (m *Manager) Records() []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	return sortedRecords(m.records)
}

func (m *Manager) initMaps() {
	if m.pendingTools == nil {
		m.pendingTools = map[string]*pendingTool{}
	}
	if m.records == nil {
		m.records = make(map[string]Record)
	}
	if m.ownership == nil {
		m.ownership = make(map[string]resourceOwnership)
	}
	if m.stateEntries == nil {
		m.stateEntries = make(map[string]inodeIdentity)
	}
}

func (m *Manager) allocateUID() (uint32, error) {
	used := make(map[uint32]struct{}, len(m.records))
	for _, record := range m.records {
		used[record.UID] = struct{}{}
	}
	for uid := m.cfg.UIDs.Min; uid <= m.cfg.UIDs.Max; uid++ {
		if _, exists := used[uid]; !exists {
			return uid, nil
		}
	}
	return 0, ErrPoolExhausted
}

// checkControlCollision refuses a new record whose control directory, serial
// log or target is any resource another record holds, in any role: one inode
// is never two attempts' caller resource, so no attempt can be handed write
// access to another's overlay or log. The one exception is two shared-ro
// readers of a verified immutable base.
func (m *Manager) checkControlCollision(record Record) error {
	type holder struct {
		owner, role string
		access      TargetAccess
	}
	held := make(map[inodeIdentity]holder, 3*len(m.records))
	for _, other := range m.records {
		if other.SandboxID == record.SandboxID {
			continue
		}
		for _, r := range callerResources(other) {
			if prev, ok := held[r.identity]; ok && prev.role == "target" && targetsConflict(prev.access, AccessShared) {
				continue // keep the strongest holder of a target
			}
			held[r.identity] = holder{other.SandboxID, r.role, r.access}
		}
	}
	for _, r := range callerResources(record) {
		h, busy := held[r.identity]
		if !busy {
			continue
		}
		if r.role == "target" && h.role == "target" && !targetsConflict(h.access, r.access) {
			continue
		}
		switch r.role {
		case "control":
			return fmt.Errorf("jailer: control directory is already pinned to %s %s", h.owner, h.role)
		case "log":
			return fmt.Errorf("jailer: serial log is already pinned to %s %s", h.owner, h.role)
		default:
			return fmt.Errorf("%w: %s %s", ErrTargetBusy, h.owner, h.role)
		}
	}
	return nil
}

// callerResource is one caller-owned inode a record holds; role is
// "control", "log" or "target" (with the target's access).
type callerResource struct {
	role     string
	identity inodeIdentity
	access   TargetAccess
}

func callerResources(r Record) []callerResource {
	var out []callerResource
	if r.ControlIno != 0 {
		out = append(out, callerResource{role: "control", identity: inodeIdentity{Dev: r.ControlDev, Ino: r.ControlIno}})
	}
	if r.LogIno != 0 {
		out = append(out, callerResource{role: "log", identity: inodeIdentity{Dev: r.LogDev, Ino: r.LogIno}})
	}
	if r.TargetIno != 0 {
		out = append(out, callerResource{role: "target", identity: inodeIdentity{Dev: r.TargetDev, Ino: r.TargetIno}, access: recordAccess(r)})
	}
	return out
}

// recordAccess is the access a record holds its target with; a VM always
// holds its overlay owner-rw.
func recordAccess(r Record) TargetAccess {
	if r.Kind == KindVM || r.TargetAccess == "" {
		return AccessOwnerRW
	}
	return r.TargetAccess
}

// Reconcile releases only records whose process and cgroup are proven dead.
// Live and unresolved records are retained and reported.
func (m *Manager) Reconcile(ctx context.Context) (ReconcileReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return ReconcileReport{}, ErrClosed
	}
	var report ReconcileReport
	var errs []error
	for _, record := range m.sortedRecordsLocked() {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		err := m.releaseLocked(ctx, record.SandboxID, nil, nil)
		if errors.Is(err, ErrAlive) {
			report.Retained = append(report.Retained, record.SandboxID)
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("jailer: reconcile %s: %w", record.SandboxID, err))
			continue
		}
		report.Released = append(report.Released, record.SandboxID)
	}
	return report, errors.Join(errs...)
}

// ReconcileReport lists released dead reservations and retained live ones.
type ReconcileReport struct {
	Released []string
	Retained []string
}

func (m *Manager) sortedRecordsLocked() []Record {
	return sortedRecords(m.records)
}

// Release removes id's resources after proving QEMU death and an empty cgroup.
// An unknown id is already released. ErrAlive always preserves the record.
func (m *Manager) Release(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return ErrClosed
	}
	return m.releaseLocked(ctx, id, nil, nil)
}

func (m *Manager) releaseLocked(ctx context.Context, id string, handle *processHandle, expected *Record) error {
	m.initMaps()
	record, ok := m.records[id]
	if !ok {
		return nil
	}
	if expected != nil && record.Attempt != expected.Attempt {
		return fmt.Errorf("%w: %s attempt %s, not %s", ErrSuperseded, id, record.Attempt, expected.Attempt)
	}
	if expected != nil && record != *expected {
		return fmt.Errorf("jailer: child for %s no longer owns the current reservation", id)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var alive bool
	if handle != nil {
		exited, err := handle.exited(record)
		switch {
		case errors.Is(err, errDecideByCgroup):
			// No process evidence: the mandatory empty-cgroup check below
			// is the whole proof for this birth-contained child.
		case err != nil:
			return err
		default:
			alive = !exited
		}
	} else {
		var err error
		if alive, err = recordAlive(record); err != nil {
			return err
		}
	}
	if alive {
		return ErrAlive
	}
	ownership := m.ownership[id]
	empty, err := m.cgroups.Empty(id, ownership.Cgroup)
	if err != nil {
		return err
	}
	if !empty {
		return ErrAlive
	}
	// Death and an empty owned group are proven: the caller's resources go
	// back to their stopped ownership while the record still holds every
	// identity they are verified against, and before the uid can be reused.
	// step also re-checks cancellation after the boundary: a canceled
	// release never runs the policy, so nothing is left half-reclaimed.
	if err := m.step(ctx, stageReclaim); err != nil {
		return err
	}
	if err := m.cfg.Reclaim(ctx, record); err != nil {
		return fmt.Errorf("jailer: reclaim %s: %w", id, err)
	}
	// Remove resources first and the durable uid reservation last. Every
	// operation proves its recorded inode; any replacement or ambiguity keeps
	// the complete reservation retryable and preserves foreign content.
	if err := removeOwnedJail(ctx, m.root, id, ownership, record.UID); err != nil {
		return err
	}
	if err := m.cgroups.Remove(id, ownership.Cgroup); err != nil {
		return err
	}
	if err := m.state.Remove(m.root, id, m.stateEntries[id]); err != nil {
		return err
	}
	m.forget(id)
	m.log.Debug("jailer: released", "sandbox", id, "uid", record.UID)
	return nil
}

func (m *Manager) forget(id string) {
	delete(m.records, id)
	delete(m.ownership, id)
	delete(m.stateEntries, id)
}

// ReleaseAttempt releases exactly ref's reservation after proving its death
// and an empty owned cgroup, running the reclaim policy before any removal.
// An id with no record is already released. A record of another attempt
// returns ErrSuperseded and is not touched; ErrAlive and every other error
// retain the complete reservation.
func (m *Manager) ReleaseAttempt(ctx context.Context, ref Ref) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return ErrClosed
	}
	record, ok := m.records[ref.SandboxID]
	if !ok {
		return nil
	}
	if record.Attempt != ref.Attempt {
		return fmt.Errorf("%w: %s attempt %s, not %s", ErrSuperseded, ref.SandboxID, record.Attempt, ref.Attempt)
	}
	return m.releaseLocked(ctx, ref.SandboxID, nil, &record)
}

// RecordFor returns ref's record. It returns an fs.ErrNotExist error when the
// id has no record and ErrSuperseded when the record is another attempt's.
func (m *Manager) RecordFor(ref Ref) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.records[ref.SandboxID]
	if !ok {
		return Record{}, fmt.Errorf("jailer: %s: %w", ref.SandboxID, fs.ErrNotExist)
	}
	if record.Attempt != ref.Attempt {
		return Record{}, fmt.Errorf("%w: %s attempt %s, not %s", ErrSuperseded, ref.SandboxID, record.Attempt, ref.Attempt)
	}
	return record, nil
}

// Kill signals ref's attempt through a start-time-verified pidfd. On hosts
// without pidfd_open it uses the proven-owned cgroup's cgroup.kill after the
// same identity check. It never signals a bare recovered pid, and never
// another attempt's process: a different current attempt is ErrSuperseded.
func (m *Manager) Kill(ctx context.Context, ref Ref) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	id := ref.SandboxID
	record, ok := m.records[id]
	if !ok {
		return nil
	}
	if record.Attempt != ref.Attempt {
		return fmt.Errorf("%w: %s attempt %s, not %s", ErrSuperseded, id, record.Attempt, ref.Attempt)
	}
	if record.PID == 0 {
		// No identity was captured, but a helper may exist: it was born in
		// the owned cgroup, so kill whatever that cgroup still holds.
		if !m.ownership[id].Cgroup.valid() {
			return nil
		}
		err := m.cgroups.Kill(id, m.ownership[id].Cgroup)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	pidfd, alive, err := openVerifiedPidfd(record)
	if err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EPERM) {
			alive, aliveErr := recordAlive(record)
			if aliveErr != nil {
				return aliveErr
			}
			if !alive {
				return nil
			}
			return m.cgroups.Kill(id, m.ownership[id].Cgroup)
		}
		return err
	}
	if !alive {
		return nil
	}
	defer pidfd.Close()
	return pidfdKill(pidfd)
}

// abandonPrespawn removes a reservation whose launch failed before any
// process existed. Cleanup is bounded and detached from the caller's
// cancellation; whatever cannot be proven removable stays reserved.
func (m *Manager) abandonPrespawn(ctx context.Context, id string, cause error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	if err := m.releaseLocked(cleanupCtx, id, nil, nil); err != nil {
		return errors.Join(cause, fmt.Errorf("jailer: rollback retained reservation: %w", err))
	}
	return cause
}

// failIntent handles a failed first record write. The fresh reservation is
// dropped only when the writer proved it left nothing and no record is
// published for id; otherwise the uid/control/log reservation stays.
func (m *Manager) failIntent(id string, cause error) error {
	if errors.Is(cause, errRecordNotPublished) && !m.stateEntries[id].valid() {
		_, err := statRegularNoFollow(m.root, recordPath(id))
		if errors.Is(err, fs.ErrNotExist) {
			m.forget(id)
			return cause
		}
	}
	return fmt.Errorf("jailer: %s: launch intent outcome is uncertain; reservation retained: %w", id, cause)
}

func (m *Manager) setRecord(record Record, ownership resourceOwnership) {
	m.initMaps()
	m.records[record.SandboxID] = record
	m.ownership[record.SandboxID] = ownership
}

// writeRecord persists record and ownership, which must already be the
// in-memory reservation. A possibly published inode is always tracked so a
// later Release removes exactly it.
func (m *Manager) writeRecord(record Record, ownership resourceOwnership) error {
	identity, err := m.state.Write(m.root, record, ownership, m.stateEntries[record.SandboxID])
	if identity.valid() {
		m.stateEntries[record.SandboxID] = identity
	}
	return err
}
