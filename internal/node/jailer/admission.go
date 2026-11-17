package jailer

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// AdmissionKind is the journal kind of a startup admission attempt. It is
// not a Record kind: admission reserves no uid and is never released through
// the reclaim policy. Its resources are journaled and cleaned by the jailer
// itself, by recorded inode.
const AdmissionKind = "admission"

// AdmitSpec is one functional admission attempt.
type AdmitSpec struct {
	// Attempt is the caller-chosen uuid naming this attempt's journal
	// (StateDir/admission/<attempt>.json), scratch directory
	// (StateDir/admission/<attempt>.d) and cgroup (qemu-admit_<hex>).
	Attempt string
	// Limits bounds the admission helper's cgroup.
	Limits Limits
}

// AdmissionReport describes stale admission state found at the start of an
// attempt: entries of journaled earlier attempts proven ours and removed,
// and entries kept for operator cleanup because their ownership is unproven.
type AdmissionReport struct {
	Removed   []string
	Preserved []string
}

// admissionDir holds admission journals (<attempt>.json) and scratch
// directories (<attempt>.d) under StateDir.
const admissionDir = "admission"

// admissionID is the cgroup key of an attempt: cgroupName gives
// "qemu-admit_<32 hex>", which no sandbox or probe key can produce.
func admissionID(attempt string) string {
	return "admit_" + strings.ReplaceAll(attempt, "-", "")
}

// admissionJournal is the durable record of one admission attempt. Each
// identity is journaled right after its resource exists and before it is
// used. A journaled attempt is settled only when every one of its resources
// is proven by these identities; anything else fails admission loudly and
// is kept for the operator.
type admissionJournal struct {
	SchemaVersion int           `json:"schemaVersion"`
	Kind          string        `json:"kind"`
	Attempt       string        `json:"attempt"`
	BootID        string        `json:"bootId"`
	Cgroup        inodeIdentity `json:"cgroup,omitzero"`
	Scratch       inodeIdentity `json:"scratch,omitzero"`
	Mount         inodeIdentity `json:"mount,omitzero"`
	// Null is the child-created private null node, from the helper's report.
	Null       inodeIdentity `json:"null,omitzero"`
	PID        int           `json:"pid,omitzero"`
	StartTicks uint64        `json:"startTicks,omitzero"`
}

const (
	admissionSchema     = 1
	admitDefaultTimeout = 30 * time.Second
	// admitReportTimeout bounds the status read after the helper's reap.
	admitReportTimeout   = 5 * time.Second
	maxAdmissionEntries  = 1024
	maxAdmissionJournal  = 4096
	admissionNullName    = "null"
	admissionMountName   = "mnt"
	admissionJournalExt  = ".json"
	admissionScratchExt  = ".d"
	admissionStatusLimit = 4096
	// maxAdmissionScratch bounds the scratch enumeration: mnt, null and one
	// more, which is always refused.
	maxAdmissionScratch = 3
)

// Admission cleanup barriers (fault seam names).
const (
	faultAdmitScratchSync stateFault = "admit-scratch-sync"
	faultAdmitDirSync     stateFault = "admit-dir-sync"
)

// admitReport is the admission helper's one status report: OK after every
// functional proof, or Error; Null is the private null node it created, so
// the parent can journal and later remove exactly that inode.
type admitReport struct {
	OK    bool          `json:"ok"`
	Error string        `json:"error,omitzero"`
	Null  inodeIdentity `json:"null,omitzero"`
}

// admitHelperConfig is the admission helper's sealed launch description.
type admitHelperConfig struct {
	// Scratch is the host path of the attempt's scratch directory.
	Scratch string `json:"scratch"`
	// Mount is the inode the detached tree and its attached target must be.
	Mount inodeIdentity `json:"mount"`
	// TreeFD is the detached open_tree clone (from ExtraFiles).
	TreeFD   int    `json:"treeFD"`
	NullRdev uint64 `json:"nullRdev"`
}

// checkIdentityPair refuses a half-set identity.
func checkIdentityPair(name string, id inodeIdentity) error {
	if (id.Dev == 0) != (id.Ino == 0) {
		return fmt.Errorf("partial %s identity", name)
	}
	return nil
}

// validate proves the journal is complete and self-consistent for attempt:
// every identity is unset or whole, and each one implies those journaled
// before it.
func (j admissionJournal) validate(attempt string) error {
	if j.SchemaVersion != admissionSchema || j.Kind != AdmissionKind || j.Attempt != attempt || !validAttempt(attempt) {
		return errors.New("not this attempt's admission journal")
	}
	if j.BootID == "" || len(j.BootID) > 64 {
		return errors.New("invalid boot id")
	}
	for name, id := range map[string]inodeIdentity{"cgroup": j.Cgroup, "scratch": j.Scratch, "mount": j.Mount, "null": j.Null} {
		if err := checkIdentityPair(name, id); err != nil {
			return err
		}
	}
	switch {
	case j.PID < 0 || (j.PID == 0) != (j.StartTicks == 0):
		return errors.New("partial helper identity")
	case j.Scratch.valid() && !j.Cgroup.valid():
		return errors.New("scratch journaled before its cgroup")
	case j.Mount.valid() && !j.Scratch.valid():
		return errors.New("mount point journaled without its scratch")
	case j.PID != 0 && !j.Mount.valid():
		return errors.New("helper journaled before its mount point")
	case j.Null.valid() && j.PID == 0:
		return errors.New("null node journaled without its helper")
	}
	return nil
}

// Admit functionally proves, before any VM launch, every kernel facility a
// jailed launch needs: clone3 with CLONE_INTO_CGROUP and CLONE_PIDFD into an
// attempt-owned cgroup, fresh mount/PID/IPC namespaces, a detached
// non-recursive open_tree clone restricted by mount_setattr (nosuid, nodev,
// noexec, private) and attached by move_mount, and a private /dev/null that
// can be created, opened read-write, written and OFD-locked on the state
// filesystem. Every failure is fatal.
//
// Earlier journaled attempts are settled first, each only when every one of
// its resources is proven by journaled identity, its helper dead and its
// group empty; an attempt that cannot be proven fails admission and is kept.
// Unjournaled entries are never touched and are reported as Preserved.
func (m *Manager) Admit(ctx context.Context, spec AdmitSpec) (AdmissionReport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return AdmissionReport{}, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return AdmissionReport{}, context.Cause(ctx)
	}
	if !validAttempt(spec.Attempt) {
		return AdmissionReport{}, fmt.Errorf("jailer: invalid admission attempt %q", spec.Attempt)
	}
	if err := spec.Limits.Validate(); err != nil {
		return AdmissionReport{}, err
	}
	if err := ensureStateSubdir(m.root, admissionDir); err != nil {
		return AdmissionReport{}, err
	}
	report, err := m.cleanStaleAdmissions(ctx, spec.Attempt)
	if err != nil {
		return report, err
	}
	return report, m.admitAttempt(ctx, spec)
}

func (m *Manager) admitStates() fileStates { return fileStates{fault: m.hooks.admitState} }

func admissionJournalName(attempt string) string { return attempt + admissionJournalExt }

func admissionJournalPath(attempt string) string {
	return path.Join(admissionDir, admissionJournalName(attempt))
}

func admissionScratchPath(attempt string) string {
	return path.Join(admissionDir, attempt+admissionScratchExt)
}

// cleanStaleAdmissions settles earlier journaled attempts and reports every
// unjournaled entry it leaves alone.
func (m *Manager) cleanStaleAdmissions(ctx context.Context, current string) (AdmissionReport, error) {
	var report AdmissionReport
	dir, err := m.root.OpenFile(admissionDir, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return report, fmt.Errorf("jailer: open admission dir: %w", err)
	}
	entries, err := dir.ReadDir(maxAdmissionEntries + 1)
	dir.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		return report, fmt.Errorf("jailer: read admission dir: %w", err)
	}
	if len(entries) > maxAdmissionEntries {
		return report, fmt.Errorf("jailer: more than %d admission entries", maxAdmissionEntries)
	}
	journals := map[string]bool{}
	for _, e := range entries {
		if att, ok := strings.CutSuffix(e.Name(), admissionJournalExt); ok && validAttempt(att) {
			journals[att] = true
		}
	}
	for _, e := range entries {
		name := e.Name()
		att, isJournal := strings.CutSuffix(name, admissionJournalExt)
		if !isJournal {
			att, _ = strings.CutSuffix(name, admissionScratchExt)
		}
		if !journals[att] || (!isJournal && name != att+admissionScratchExt) {
			report.Preserved = append(report.Preserved, path.Join(admissionDir, name))
		}
	}
	groups, err := m.cgroups.List()
	if err != nil {
		return report, err
	}
	journaledGroups := map[string]bool{}
	for att := range journals {
		journaledGroups[cgroupName(admissionID(att))] = true
	}
	for _, g := range groups {
		if strings.HasPrefix(g, cgroupName("admit_")) && !journaledGroups[g] {
			report.Preserved = append(report.Preserved, "cgroup "+g)
		}
	}
	for _, att := range slices.Sorted(maps.Keys(journals)) {
		if err := ctx.Err(); err != nil {
			return report, context.Cause(ctx)
		}
		if att == current {
			return report, fmt.Errorf("jailer: admission attempt %s is already journaled", att)
		}
		raw, jid, err := readBoundedRegular(m.root, admissionJournalPath(att), maxAdmissionJournal)
		if err != nil {
			return report, fmt.Errorf("jailer: stale admission %s: read journal: %w", att, err)
		}
		var j admissionJournal
		if err := json.Unmarshal(raw, &j, json.RejectUnknownMembers(true)); err != nil {
			return report, fmt.Errorf("jailer: stale admission %s: decode journal: %w", att, err)
		}
		if err := j.validate(att); err != nil {
			return report, fmt.Errorf("jailer: stale admission %s: %w", att, err)
		}
		if err := m.settleAdmission(ctx, j, jid); err != nil {
			return report, fmt.Errorf("jailer: stale admission %s is kept: %w", att, err)
		}
		report.Removed = append(report.Removed, att)
	}
	return report, nil
}

// admissionLedger tracks the current attempt's journal: the inode this
// attempt last published (jid) and whether the published state is known.
type admissionLedger struct {
	j     admissionJournal
	jid   inodeIdentity
	known bool
}

// publish journals l.j. A failure that published nothing keeps the previous
// journal; a failure that published (with an uncertain barrier) adopts the
// new inode; any other failure leaves the journal unknown, and the attempt
// must then keep every resource.
func (m *Manager) publishAdmission(l *admissionLedger) error {
	if err := l.j.validate(l.j.Attempt); err != nil {
		return fmt.Errorf("jailer: admission journal: %w", err)
	}
	raw, err := json.Marshal(l.j)
	if err != nil {
		return fmt.Errorf("jailer: encode admission journal: %w", err)
	}
	if len(raw) > maxAdmissionJournal {
		return fmt.Errorf("jailer: admission journal exceeds %d bytes", maxAdmissionJournal)
	}
	id, err := m.admitStates().publish(m.root, admissionDir, admissionJournalName(l.j.Attempt), "admission journal", raw, l.jid)
	switch {
	case err == nil || id.valid():
		l.jid = id
	case !errors.Is(err, errRecordNotPublished):
		l.known = false
	}
	return err
}

// settleAdmission removes a journaled attempt's resources. Every proof is
// read-only and complete before the first mutation: the journal is still
// jid, the helper is dead, the group is empty or absent, and every scratch
// entry is the journaled inode. Each mutation re-proves its entry, every
// barrier runs even for names already absent, and the journal goes last.
// Any failure is an error and keeps what remains.
func (m *Manager) settleAdmission(ctx context.Context, j admissionJournal, jid inodeIdentity) error {
	id := admissionID(j.Attempt)
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if current, err := statRegularNoFollow(m.root, admissionJournalPath(j.Attempt)); err == nil {
		if !jid.valid() || !current.equal(jid) {
			return errors.New("the journal was replaced; preserving it")
		}
	} else if !errors.Is(err, fs.ErrNotExist) || jid.valid() {
		return fmt.Errorf("inspect journal: %w", err)
	}
	if j.PID != 0 {
		alive, err := recordAlive(Record{PID: j.PID, StartTicks: j.StartTicks, BootID: j.BootID})
		if err != nil {
			return fmt.Errorf("helper liveness unknown: %w", err)
		}
		if alive {
			return fmt.Errorf("%w: admission helper %d", ErrAlive, j.PID)
		}
	}
	// Empty refuses a group under this name that is not the journaled
	// inode, including one that was never journaled.
	empty, err := m.cgroups.Empty(id, j.Cgroup)
	if err != nil {
		return fmt.Errorf("cgroup proof unknown: %w", err)
	}
	if !empty {
		return fmt.Errorf("%w: admission cgroup %s is not empty", ErrAlive, cgroupName(id))
	}
	scratch := admissionScratchPath(j.Attempt)
	plan, scratchExists, err := m.planAdmissionScratch(scratch, j)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	for _, entry := range plan {
		if err := entry.remove(); err != nil {
			return err
		}
	}
	states := m.admitStates()
	if scratchExists {
		if err := states.inject(faultAdmitScratchSync); err != nil {
			return fmt.Errorf("sync scratch: %w", err)
		}
		if err := syncDirectory(m.root, scratch); err != nil {
			return err
		}
		current, err := statDirectoryNoFollow(m.root, scratch)
		if err != nil || !current.equal(j.Scratch) {
			return errors.New("the scratch directory changed during cleanup; preserving it")
		}
		if err := m.root.Remove(scratch); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove scratch: %w", err)
		}
	}
	if err := states.inject(faultAdmitDirSync); err != nil {
		return fmt.Errorf("sync admission dir: %w", err)
	}
	if err := syncDirectory(m.root, admissionDir); err != nil {
		return err
	}
	if j.Cgroup.valid() {
		if err := m.cgroups.Remove(id, j.Cgroup); err != nil {
			return fmt.Errorf("remove cgroup: %w", err)
		}
	}
	if err := states.removePublished(m.root, admissionDir, admissionJournalName(j.Attempt), jid); err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	return nil
}

// scratchRemoval is one proven scratch entry and its re-proving removal.
type scratchRemoval struct {
	remove func() error
}

// planAdmissionScratch proves the whole scratch directory read-only: it is
// absent, or the journaled inode holding at most the journaled mount point
// and the journaled null node. It returns their removals (null first).
func (m *Manager) planAdmissionScratch(scratch string, j admissionJournal) ([]scratchRemoval, bool, error) {
	dir, err := m.root.OpenFile(scratch, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("open scratch: %w", err)
	}
	defer dir.Close()
	current, err := fileInode(dir)
	if err != nil {
		return nil, false, err
	}
	if !j.Scratch.valid() || !current.equal(j.Scratch) {
		return nil, false, errors.New("the scratch directory is not the journaled inode; preserving it")
	}
	entries, err := dir.ReadDir(maxAdmissionScratch)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, false, fmt.Errorf("read scratch: %w", err)
	}
	var plan []scratchRemoval
	var mountPlan *scratchRemoval
	for _, e := range entries {
		name := path.Join(scratch, e.Name())
		switch e.Name() {
		case admissionMountName:
			prove := func() error {
				id, err := statDirectoryNoFollow(m.root, name)
				if err != nil {
					return err
				}
				if !j.Mount.valid() || !id.equal(j.Mount) {
					return errors.New("the mount point is not the journaled inode; preserving it")
				}
				return nil
			}
			if err := prove(); err != nil {
				return nil, false, err
			}
			mountPlan = &scratchRemoval{remove: func() error { return m.removeProven(name, prove) }}
		case admissionNullName:
			prove := func() error { return m.proveAdmissionNull(name, j.Null) }
			if err := prove(); err != nil {
				return nil, false, err
			}
			plan = append(plan, scratchRemoval{remove: func() error { return m.removeProven(name, prove) }})
		default:
			return nil, false, fmt.Errorf("foreign scratch entry %q; preserving the attempt", e.Name())
		}
	}
	if mountPlan != nil {
		plan = append(plan, *mountPlan)
	}
	return plan, true, nil
}

// removeProven re-proves an entry right before unlinking it; an entry
// already gone is fine (a retry after partial removal).
func (m *Manager) removeProven(name string, prove func() error) error {
	if err := prove(); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := m.root.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", name, err)
	}
	return nil
}

// proveAdmissionNull requires name to be the journaled null node: the
// private /dev/null device, owned by the node, with the journaled inode.
func (m *Manager) proveAdmissionNull(name string, want inodeIdentity) error {
	info, err := m.root.Lstat(name)
	if err != nil {
		return err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !want.valid() || uint64(st.Dev) != want.Dev || st.Ino != want.Ino {
		return errors.New("the null node is not the journaled inode; preserving it")
	}
	if !m.isAdmissionNull(info) || int(st.Uid) != os.Geteuid() {
		return errors.New("the null node is not a private /dev/null; preserving it")
	}
	return nil
}

func (m *Manager) isAdmissionNull(info fs.FileInfo) bool {
	if m.hooks.admitNull != nil {
		return m.hooks.admitNull(info)
	}
	devices, err := m.fixedDevices()
	st, ok := info.Sys().(*syscall.Stat_t)
	return err == nil && ok && info.Mode().Type() == fs.ModeDevice|fs.ModeCharDevice && uint64(st.Rdev) == devices.Null
}

func (m *Manager) admitTimeoutValue() time.Duration {
	if m.hooks.admitTimeout > 0 {
		return m.hooks.admitTimeout
	}
	return admitDefaultTimeout
}

// proveFreshNamespaces requires the helper's mount, PID and IPC namespaces
// to differ from the supervisor's.
func (m *Manager) proveFreshNamespaces(pid int) error {
	proc := m.procfs()
	for _, ns := range []string{"ns/mnt", "ns/pid", "ns/ipc"} {
		self, err := proc.Stat("self", ns)
		if err != nil {
			return fmt.Errorf("jailer: admission %s: %w", ns, err)
		}
		child, err := proc.Stat(strconv.Itoa(pid), ns)
		if err != nil {
			return fmt.Errorf("jailer: admission helper %s: %w", ns, err)
		}
		if child.equal(self) {
			return fmt.Errorf("jailer: admission helper shares the supervisor's %s namespace", strings.TrimPrefix(ns, "ns/"))
		}
	}
	return nil
}

// admitAttempt runs one journaled admission attempt.
func (m *Manager) admitAttempt(ctx context.Context, spec AdmitSpec) (returnErr error) {
	boot, err := hostBootID()
	if err != nil {
		return err
	}
	l := &admissionLedger{j: admissionJournal{SchemaVersion: admissionSchema, Kind: AdmissionKind, Attempt: spec.Attempt, BootID: boot}, known: true}
	// proven reports that no process of this attempt can exist any more.
	// Only a proven attempt whose journal is known is settled on return;
	// settling failures fail Admit and keep what remains.
	proven := true
	defer func() {
		switch {
		case !proven:
		case !l.known:
			returnErr = errors.Join(returnErr, fmt.Errorf("jailer: admission %s journal state is uncertain; its resources are kept", spec.Attempt))
		default:
			if err := m.settleAdmission(context.WithoutCancel(ctx), l.j, l.jid); err != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("jailer: admission %s cleanup failed; kept: %w", spec.Attempt, err))
			}
		}
	}()
	if err := m.publishAdmission(l); err != nil {
		return err
	}
	id := admissionID(spec.Attempt)
	identity, err := m.cgroups.Prepare(id, spec.Limits)
	if identity.valid() {
		l.j.Cgroup = identity
		if werr := m.publishAdmission(l); werr != nil {
			return errors.Join(err, werr)
		}
	}
	if err != nil {
		return err
	}
	scratch := admissionScratchPath(spec.Attempt)
	mnt := path.Join(scratch, admissionMountName)
	if err := m.root.Mkdir(scratch, 0o700); err != nil {
		return fmt.Errorf("jailer: create admission scratch: %w", err)
	}
	if l.j.Scratch, err = statDirectoryNoFollow(m.root, scratch); err != nil {
		l.known = false // an unidentified scratch directory exists
		return err
	}
	if err := m.publishAdmission(l); err != nil {
		return err
	}
	if err := m.root.Mkdir(mnt, 0o700); err != nil {
		return fmt.Errorf("jailer: create admission mount point: %w", err)
	}
	if l.j.Mount, err = statDirectoryNoFollow(m.root, mnt); err != nil {
		l.known = false
		return err
	}
	if err := syncDirectory(m.root, scratch); err != nil {
		return err
	}
	if err := m.publishAdmission(l); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	mntFile, err := m.root.OpenFile(mnt, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("jailer: open admission mount point: %w", err)
	}
	tree, err := m.detachedMount(mntFile)
	mntFile.Close()
	if err != nil {
		return fmt.Errorf("jailer: admission open_tree/mount_setattr: %w", err)
	}
	defer tree.Close()
	cgroupDir, err := m.cgroups.OpenOwned(id, l.j.Cgroup)
	if err != nil {
		return err
	}
	defer cgroupDir.Close()
	devices, err := m.fixedDevices()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(admitHelperConfig{Scratch: filepath.Join(m.cfg.StateDir, scratch), Mount: l.j.Mount, TreeFD: 6, NullRdev: devices.Null})
	if err != nil {
		return err
	}
	config, err := sealedConfig(raw, "bbraft-admission-config")
	if err != nil {
		return err
	}
	defer config.Close()
	releaseR, releaseW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer releaseW.Close()
	statusR, statusW, err := os.Pipe()
	if err != nil {
		releaseR.Close()
		return err
	}
	defer statusR.Close()
	cmd := exec.Command("/proc/self/exe", AdmitHelperArg, "3", "4", "5")
	cmd.ExtraFiles = []*os.File{config, releaseR, statusW, tree}
	cmd.Env = []string{}
	cmd.Dir = "/"
	pidfd := -1
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:  unix.CLONE_NEWNS | unix.CLONE_NEWPID | unix.CLONE_NEWIPC,
		Setpgid:     true,
		Pdeathsig:   syscall.SIGKILL,
		UseCgroupFD: true,
		CgroupFD:    int(cgroupDir.Fd()),
		PidFD:       &pidfd,
	}
	if m.hooks.command != nil {
		m.hooks.command(cmd)
	}
	startErr := cmd.Start()
	releaseR.Close()
	statusW.Close()
	if startErr != nil {
		return fmt.Errorf("jailer: start admission helper (clone3 into cgroup with pidfd): %w", startErr)
	}
	// A process exists. Its handle is owned by the sole waiter below, which
	// closes it exactly once after the reap; nothing is settled until that
	// reap is observed. A clone without a pidfd is refused (never the
	// pidfd_open fallback), and the child is then killed through its
	// unreaped os.Process.
	proven = false
	handle, _ := newProcessHandle(cmd.Process, pidfd, func(int) (int, error) { return -1, unix.ENOSYS })
	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		_ = handle.close()
		if m.hooks.admitReaped != nil {
			m.hooks.admitReaped(handle)
		}
		close(done)
	}()
	// released: the helper may have created its null node, whose identity
	// only its report carries.
	released := false
	// journalNull journals the null node a reaped, released helper reported,
	// so cleanup can prove and remove exactly that inode.
	journalNull := func(report admitReport) error {
		if !report.Null.valid() {
			return nil
		}
		l.j.Null = report.Null
		return m.publishAdmission(l)
	}
	reap := func(cause error) error {
		killErr := m.killChild(handle)
		if errors.Is(killErr, errHandleReleased) {
			killErr = nil
		}
		select {
		case <-done:
			proven = true
			if released {
				// Best effort: without a report an unjournaled null node
				// fails cleanup and the attempt is kept.
				if report, err := readAdmitReport(statusR); err == nil {
					killErr = errors.Join(killErr, journalNull(report))
				}
			}
			return errors.Join(cause, killErr)
		case <-time.After(cleanupTimeout):
			return errors.Join(cause, killErr, fmt.Errorf("jailer: admission helper %d was not reaped; its journal, cgroup and scratch are kept", cmd.Process.Pid))
		}
	}
	if pidfd < 0 {
		return reap(errors.New("jailer: admission: clone3 returned no pidfd (CLONE_PIDFD is required)"))
	}
	if err := m.cgroups.Joined(id, cmd.Process.Pid, l.j.Cgroup); err != nil {
		return reap(fmt.Errorf("jailer: admission helper was not born in its cgroup: %w", err))
	}
	handle.markContained()
	if err := m.proveFreshNamespaces(cmd.Process.Pid); err != nil {
		return reap(err)
	}
	ticks, err := handle.capture()
	if err != nil {
		return reap(err)
	}
	l.j.PID, l.j.StartTicks = cmd.Process.Pid, ticks
	if err := m.publishAdmission(l); err != nil {
		return reap(err)
	}
	released = true
	if _, err := releaseW.Write([]byte{1}); err != nil {
		return reap(fmt.Errorf("jailer: release admission helper: %w", err))
	}
	releaseW.Close()
	timer := time.NewTimer(m.admitTimeoutValue())
	defer timer.Stop()
	select {
	case <-done:
		proven = true
	case <-timer.C:
		return reap(errors.New("jailer: admission helper timed out"))
	case <-ctx.Done():
		return reap(fmt.Errorf("jailer: admission: %w", context.Cause(ctx)))
	}
	// The helper is reaped, so its report is complete or the pipe is at EOF.
	// The read is bounded by bytes and time; cancellation is checked after
	// it, so a reported null node is still journaled and removed by proof.
	report, reportErr := readAdmitReport(statusR)
	if reportErr == nil {
		if err := journalNull(report); err != nil {
			return err
		}
	}
	switch {
	case reportErr != nil:
		return reportErr
	case !report.OK:
		return fmt.Errorf("jailer: admission failed: %s", report.Error)
	case waitErr != nil:
		return fmt.Errorf("jailer: admission helper reported success but exited: %w", waitErr)
	case report.Error != "" || !report.Null.valid():
		return errors.New("jailer: admission helper report is incomplete")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("jailer: admission: %w", context.Cause(ctx))
	}
	return nil
}

// readAdmitReport reads the reaped helper's single report, bounded by bytes
// and by admitReportTimeout (a leaked write end cannot stall it). A read
// error, a timeout, an oversized, truncated or unknown report and a missing
// report are failures.
func readAdmitReport(r *os.File) (admitReport, error) {
	if err := r.SetReadDeadline(time.Now().Add(admitReportTimeout)); err != nil {
		return admitReport{}, fmt.Errorf("jailer: bound admission report read: %w", err)
	}
	raw, err := io.ReadAll(io.LimitReader(r, admissionStatusLimit+1))
	if err != nil {
		return admitReport{}, fmt.Errorf("jailer: read admission report: %w", err)
	}
	if len(raw) == 0 {
		return admitReport{}, errors.New("jailer: admission helper sent no report")
	}
	if len(raw) > admissionStatusLimit {
		return admitReport{}, fmt.Errorf("jailer: admission report exceeds %d bytes", admissionStatusLimit)
	}
	var report admitReport
	if err := json.Unmarshal(raw, &report, json.RejectUnknownMembers(true)); err != nil {
		return admitReport{}, fmt.Errorf("jailer: invalid admission report: %w", err)
	}
	if err := checkIdentityPair("null", report.Null); err != nil {
		return admitReport{}, fmt.Errorf("jailer: invalid admission report: %w", err)
	}
	return report, nil
}

// sealedConfig returns a sealed memfd holding raw.
func sealedConfig(raw []byte, name string) (*os.File, error) {
	fd, err := unix.MemfdCreate(name, unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, fmt.Errorf("jailer: config memfd: %w", err)
	}
	f := os.NewFile(uintptr(fd), name)
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := unix.FcntlInt(f.Fd(), unix.F_ADD_SEALS, unix.F_SEAL_SEAL|unix.F_SEAL_SHRINK|unix.F_SEAL_GROW|unix.F_SEAL_WRITE); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// checkAdmissionMount proves from mountinfo that the effective mount on
// target, found by mount id/parent topology rather than listing order (see
// effectiveMountAt), is private (no shared or master peer group) and
// mounted nosuid, nodev and noexec.
func checkAdmissionMount(mountinfo []byte, target string) error {
	top, err := effectiveMountAt(mountinfo, target)
	if err != nil {
		return fmt.Errorf("jailer: admission mount: %w", err)
	}
	if !top.separated {
		return errors.New("jailer: admission mount: malformed mountinfo entry")
	}
	for _, tag := range top.optional {
		if strings.HasPrefix(tag, "shared:") || strings.HasPrefix(tag, "master:") {
			return fmt.Errorf("jailer: admission mount propagation is not private (%s)", tag)
		}
	}
	opts := strings.Split(top.options, ",")
	for _, want := range []string{"nosuid", "nodev", "noexec"} {
		if !slices.Contains(opts, want) {
			return fmt.Errorf("jailer: admission mount lacks %s", want)
		}
	}
	return nil
}

// admitHelperMain is the admission helper, born in fresh mount/PID/IPC
// namespaces inside its cgroup. After the release byte it makes its mounts
// private, attaches the detached tree with move_mount and proves its flags
// and propagation, then creates (outside that nodev mount), opens, writes
// and OFD-locks a private /dev/null. Its single report goes to the status
// pipe: success is an explicit exit 0 after every proof; every error is
// reported and returned, so the helper exits HelperExitCode.
func admitHelperMain() (returnErr error) {
	if len(os.Args) != 5 {
		return errors.New("jailer: admission helper expects config, release and status fds")
	}
	fds := make([]int, 3)
	for i := range fds {
		fd, err := strconv.Atoi(os.Args[i+2])
		if err != nil || fd < 3 {
			return fmt.Errorf("jailer: invalid admission fd %q", os.Args[i+2])
		}
		fds[i] = fd
	}
	status := os.NewFile(uintptr(fds[2]), "status")
	var report admitReport
	defer func() {
		if returnErr != nil {
			report.Error = returnErr.Error()
			if raw, err := json.Marshal(report); err == nil {
				_, _ = status.Write(raw)
			}
		}
		status.Close()
	}()
	var cfg admitHelperConfig
	config := os.NewFile(uintptr(fds[0]), "config")
	err := json.UnmarshalRead(io.LimitReader(config, 1<<16), &cfg)
	config.Close()
	if err != nil {
		return fmt.Errorf("jailer: decode admission config: %w", err)
	}
	release := os.NewFile(uintptr(fds[1]), "release")
	var gate [1]byte
	_, err = io.ReadFull(release, gate[:])
	release.Close()
	if err != nil || gate[0] != 1 {
		return errors.New("jailer: admission canceled before setup")
	}
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		return fmt.Errorf("jailer: admission private propagation: %w", err)
	}
	mnt := filepath.Join(cfg.Scratch, admissionMountName)
	if err := attachPinned(cfg.TreeFD, mnt, cfg.Mount); err != nil {
		return fmt.Errorf("jailer: admission move_mount: %w", err)
	}
	var st unix.Statfs_t
	if err := unix.Statfs(mnt, &st); err != nil {
		return fmt.Errorf("jailer: admission statfs: %w", err)
	}
	const want = unix.ST_NOSUID | unix.ST_NODEV | unix.ST_NOEXEC
	if st.Flags&want != want {
		return fmt.Errorf("jailer: admission mount flags %#x lack nosuid/nodev/noexec", st.Flags)
	}
	mountinfo, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return fmt.Errorf("jailer: admission mountinfo: %w", err)
	}
	if err := checkAdmissionMount(mountinfo, mnt); err != nil {
		return err
	}
	null := filepath.Join(cfg.Scratch, admissionNullName)
	if err := unix.Mknod(null, unix.S_IFCHR|0o600, int(cfg.NullRdev)); err != nil {
		return fmt.Errorf("jailer: admission private /dev/null: %w", err)
	}
	var nst unix.Stat_t
	if err := unix.Lstat(null, &nst); err != nil {
		return fmt.Errorf("jailer: admission stat private /dev/null: %w", err)
	}
	report.Null = inodeIdentity{Dev: nst.Dev, Ino: nst.Ino}
	if nst.Mode&unix.S_IFMT != unix.S_IFCHR || nst.Rdev != cfg.NullRdev {
		return errors.New("jailer: admission private /dev/null is not the null device")
	}
	f, err := os.OpenFile(null, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("jailer: admission open private /dev/null: %w", err)
	}
	defer f.Close()
	if _, err := f.Write([]byte{0}); err != nil {
		return fmt.Errorf("jailer: admission write private /dev/null: %w", err)
	}
	lock := unix.Flock_t{Type: unix.F_WRLCK}
	if err := unix.FcntlFlock(f.Fd(), unix.F_OFD_SETLK, &lock); err != nil {
		return fmt.Errorf("jailer: admission OFD lock: %w", err)
	}
	report.OK = true
	raw, err := json.Marshal(report)
	if err != nil {
		report.OK = false
		return err
	}
	if _, err := status.Write(raw); err != nil {
		report.OK = false
		return fmt.Errorf("jailer: admission report: %w", err)
	}
	if err := status.Close(); err != nil {
		report.OK = false
		return fmt.Errorf("jailer: admission report: %w", err)
	}
	os.Exit(0)
	return nil
}
