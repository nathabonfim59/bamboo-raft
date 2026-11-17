package jailer

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

// procFS reads one process's /proc entries. pid is a decimal pid or "self".
// Production uses the host /proc; tests substitute fixed contents.
type procFS interface {
	ReadFile(pid, name string) ([]byte, error)
	// Stat follows name (so ns/mnt, root and exe resolve to their targets)
	// and returns the target's identity.
	Stat(pid, name string) (inodeIdentity, error)
}

type hostProcFS struct{}

func (hostProcFS) ReadFile(pid, name string) ([]byte, error) {
	return os.ReadFile("/proc/" + pid + "/" + name)
}

func (hostProcFS) Stat(pid, name string) (inodeIdentity, error) {
	info, err := os.Stat("/proc/" + pid + "/" + name)
	if err != nil {
		return inodeIdentity{}, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return inodeIdentity{}, errors.New("no Linux identity")
	}
	return inodeIdentity{Dev: uint64(st.Dev), Ino: st.Ino}, nil
}

func (m *Manager) procfs() procFS {
	if m.hooks.proc != nil {
		return m.hooks.proc
	}
	return hostProcFS{}
}

// VerifyProcess proves that expected's live process is the confined process
// this Manager launched for exactly that attempt, before a restarted node
// adopts it. The caller pins the pid with a pidfd first. It requires:
//
//   - the current record to equal expected (same attempt, every field);
//   - the host boot id, a live non-zombie pid with the recorded start ticks,
//     checked before and after every other read;
//   - every uid and gid equal to the record's uid, no supplementary groups,
//     all five capability sets empty, no_new_privs and seccomp filter mode;
//   - private mount, PID and IPC namespaces (not the node's own);
//   - its root directory to be the owned jail root, mounted read-only;
//   - membership of the owned cgroup inode;
//   - its executable to be the staged binary inode.
//
// Any failure is an error; it never signals, kills or releases anything.
func (m *Manager) VerifyProcess(ctx context.Context, expected Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.root == nil {
		return ErrClosed
	}
	return m.verifyProcessLocked(ctx, expected)
}

// verifyProcessLocked is VerifyProcess with m.mu held. A tool is verified
// by the same rules except seccomp, which is QEMU's own (-sandbox): an
// inherited service filter is valid for a tool and is not asserted.
func (m *Manager) verifyProcessLocked(ctx context.Context, expected Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id := expected.SandboxID
	current, ok := m.records[id]
	if !ok {
		return fmt.Errorf("jailer: verify %s: %w", id, fs.ErrNotExist)
	}
	if current.Attempt != expected.Attempt {
		return fmt.Errorf("%w: %s attempt %s, not %s", ErrSuperseded, id, current.Attempt, expected.Attempt)
	}
	if current != expected {
		return fmt.Errorf("jailer: verify %s: record differs from the expected attempt record", id)
	}
	if expected.PID <= 0 || expected.StartTicks == 0 {
		return fmt.Errorf("jailer: verify %s: no captured process identity", id)
	}
	bootID, err := hostBootID()
	if err != nil {
		return err
	}
	if bootID != expected.BootID {
		return fmt.Errorf("jailer: verify %s: record is from another host boot", id)
	}
	own := m.ownership[id]
	proc := m.procfs()
	pid := strconv.Itoa(expected.PID)
	// canceled is checked after every proof read: a verification whose
	// context ended is never success, and it has no side effect to undo.
	canceled := func() error {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("jailer: verify %s: %w", id, context.Cause(ctx))
		}
		return nil
	}
	alive := func(when string) error {
		raw, err := proc.ReadFile(pid, "stat")
		if err != nil {
			return fmt.Errorf("jailer: verify %s: read stat %s: %w", id, when, err)
		}
		identity, err := parseProcStat(expected.PID, raw)
		if err != nil {
			return err
		}
		if identity.startTicks != expected.StartTicks || procDead(identity.state) {
			return fmt.Errorf("jailer: verify %s: pid %d is not the recorded live process %s", id, expected.PID, when)
		}
		return nil
	}
	if err := alive("before"); err != nil {
		return err
	}
	if err := canceled(); err != nil {
		return err
	}
	status, err := proc.ReadFile(pid, "status")
	if err != nil {
		return fmt.Errorf("jailer: verify %s: read status: %w", id, err)
	}
	if err := checkConfinedStatus(status, expected.UID, expected.Kind != KindTool); err != nil {
		return fmt.Errorf("jailer: verify %s: %w", id, err)
	}
	if err := canceled(); err != nil {
		return err
	}
	for _, ns := range []string{"ns/mnt", "ns/pid", "ns/ipc"} {
		theirs, err := proc.Stat(pid, ns)
		if err != nil {
			return fmt.Errorf("jailer: verify %s: %s: %w", id, ns, err)
		}
		ours, err := proc.Stat("self", ns)
		if err != nil {
			return fmt.Errorf("jailer: verify %s: own %s: %w", id, ns, err)
		}
		if theirs.equal(ours) {
			return fmt.Errorf("jailer: verify %s: process shares the node's %s namespace", id, ns)
		}
		if err := canceled(); err != nil {
			return err
		}
	}
	rootEntry, ok := ownedEntryAt(own, "root")
	if !ok {
		return fmt.Errorf("jailer: verify %s: no owned jail root", id)
	}
	root, err := proc.Stat(pid, "root")
	if err != nil {
		return fmt.Errorf("jailer: verify %s: root: %w", id, err)
	}
	if !root.equal(rootEntry.identity()) {
		return fmt.Errorf("jailer: verify %s: process root is not the owned jail root", id)
	}
	if err := canceled(); err != nil {
		return err
	}
	mountinfo, err := proc.ReadFile(pid, "mountinfo")
	if err != nil {
		return fmt.Errorf("jailer: verify %s: read mountinfo: %w", id, err)
	}
	if err := checkReadOnlyRoot(mountinfo); err != nil {
		return fmt.Errorf("jailer: verify %s: %w", id, err)
	}
	if err := canceled(); err != nil {
		return err
	}
	if err := m.cgroups.Joined(id, expected.PID, own.Cgroup); err != nil {
		return fmt.Errorf("jailer: verify %s: %w", id, err)
	}
	if err := canceled(); err != nil {
		return err
	}
	execEntry, ok := ownedEntryAt(own, own.Exec)
	if own.Exec == "" || !ok {
		return fmt.Errorf("jailer: verify %s: no staged executable is recorded", id)
	}
	exe, err := proc.Stat(pid, "exe")
	if err != nil {
		return fmt.Errorf("jailer: verify %s: exe: %w", id, err)
	}
	if !exe.equal(execEntry.identity()) {
		return fmt.Errorf("jailer: verify %s: executable is not the staged binary", id)
	}
	if err := canceled(); err != nil {
		return err
	}
	if err := alive("after"); err != nil {
		return err
	}
	return canceled()
}

func ownedEntryAt(own resourceOwnership, p string) (ownedEntry, bool) {
	i := slices.IndexFunc(own.Entries, func(e ownedEntry) bool { return e.Path == p })
	if i < 0 {
		return ownedEntry{}, false
	}
	return own.Entries[i], true
}

// checkConfinedStatus requires the credential state the helper establishes
// before exec: every uid and gid equal to uid, no groups, all capability
// sets empty, no_new_privs and a seccomp filter. Every field must be present.
func checkConfinedStatus(raw []byte, uid uint32, requireSeccomp bool) error {
	want := strconv.FormatUint(uint64(uid), 10)
	required := map[string]bool{
		"Uid": false, "Gid": false, "Groups": false,
		"CapInh": false, "CapPrm": false, "CapEff": false, "CapBnd": false, "CapAmb": false,
		"NoNewPrivs": false, "Seccomp": false,
	}
	for line := range strings.SplitSeq(string(raw), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		if _, tracked := required[key]; !tracked {
			continue
		}
		required[key] = true
		value = strings.TrimSpace(value)
		switch key {
		case "Uid", "Gid":
			fields := strings.Fields(value)
			if len(fields) != 4 {
				return fmt.Errorf("%s has %d ids", key, len(fields))
			}
			for _, field := range fields {
				if field != want {
					return fmt.Errorf("%s is %q, want %s everywhere", key, value, want)
				}
			}
		case "Groups":
			if value != "" {
				return fmt.Errorf("supplementary groups %q are present", value)
			}
		case "NoNewPrivs":
			if value != "1" {
				return fmt.Errorf("no_new_privs is %q", value)
			}
		case "Seccomp":
			if requireSeccomp && value != "2" {
				return fmt.Errorf("seccomp mode is %q, want a filter (2)", value)
			}
		default:
			capabilities, err := strconv.ParseUint(value, 16, 64)
			if err != nil || capabilities != 0 {
				return fmt.Errorf("%s is %q, want empty", key, value)
			}
		}
	}
	for key, seen := range required {
		if !seen {
			return fmt.Errorf("status has no %s field", key)
		}
	}
	return nil
}

// checkReadOnlyRoot requires the effective mount at "/" in the process's
// view to be read-only (see effectiveMountAt).
func checkReadOnlyRoot(raw []byte) error {
	top, err := effectiveMountAt(raw, "/")
	if err != nil {
		return err
	}
	if !slices.Contains(strings.Split(top.options, ","), "ro") {
		return fmt.Errorf("effective root mount options %q are not read-only", top.options)
	}
	return nil
}

// mountInfoEntry is one mountinfo line mounted on the target point.
type mountInfoEntry struct {
	parent  int
	options string
	// optional holds the optional fields (propagation tags); separated
	// reports that the line had its " - " separator.
	optional  []string
	separated bool
}

// effectiveMountAt returns the effective mount on target. Line order is not
// authority: mounts stacked at one mount point form a chain through their
// parent ids, and the effective one is the target mount that no other
// target mount names as its parent (proc_pid_mountinfo(5)). Mount points are
// compared after undoing mountinfo's octal escapes. A missing, malformed,
// duplicated, branching or cyclic topology is refused.
func effectiveMountAt(raw []byte, target string) (mountInfoEntry, error) {
	ids := make(map[int]struct{})
	mounts := make(map[int]mountInfoEntry)
	for line := range strings.SplitSeq(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 6 {
			return mountInfoEntry{}, fmt.Errorf("malformed mountinfo line %q", line)
		}
		id, err := strconv.Atoi(fields[0])
		if err != nil || id < 0 {
			return mountInfoEntry{}, fmt.Errorf("malformed mount id in %q", line)
		}
		parent, err := strconv.Atoi(fields[1])
		if err != nil || parent < 0 {
			return mountInfoEntry{}, fmt.Errorf("malformed parent id in %q", line)
		}
		if _, dup := ids[id]; dup {
			return mountInfoEntry{}, fmt.Errorf("duplicate mount id %d", id)
		}
		ids[id] = struct{}{}
		point, err := unescapeMountPoint(fields[4])
		if err != nil {
			return mountInfoEntry{}, fmt.Errorf("%w in %q", err, line)
		}
		if point != target {
			continue
		}
		entry := mountInfoEntry{parent: parent, options: fields[5]}
		if sep := slices.Index(fields[6:], "-"); sep >= 0 {
			entry.optional, entry.separated = fields[6:6+sep], true
		}
		mounts[id] = entry
	}
	if len(mounts) == 0 {
		return mountInfoEntry{}, fmt.Errorf("no %q mount in mountinfo", target)
	}
	stackedOn := make(map[int]bool, len(mounts))
	for _, m := range mounts {
		if _, onTarget := mounts[m.parent]; onTarget {
			stackedOn[m.parent] = true
		}
	}
	top := -1
	for id := range mounts {
		if stackedOn[id] {
			continue
		}
		if top >= 0 {
			return mountInfoEntry{}, fmt.Errorf("ambiguous %q mount topology: more than one effective mount", target)
		}
		top = id
	}
	if top < 0 {
		return mountInfoEntry{}, fmt.Errorf("cyclic %q mount topology", target)
	}
	// The chain below the top must reach a parent off the target without
	// revisiting a mount, and must cover every target mount.
	seen := map[int]bool{}
	for id := top; ; id = mounts[id].parent {
		if seen[id] {
			return mountInfoEntry{}, fmt.Errorf("cyclic %q mount topology", target)
		}
		seen[id] = true
		if _, onTarget := mounts[mounts[id].parent]; !onTarget {
			break
		}
	}
	if len(seen) != len(mounts) {
		return mountInfoEntry{}, fmt.Errorf("ambiguous %q mount topology: a mount is not on the effective chain", target)
	}
	return mounts[top], nil
}

// unescapeMountPoint undoes mountinfo's \ooo octal escapes (space, tab,
// newline and backslash); any other backslash is malformed.
func unescapeMountPoint(field string) (string, error) {
	if !strings.Contains(field, `\`) {
		return field, nil
	}
	var b strings.Builder
	for i := 0; i < len(field); i++ {
		if field[i] != '\\' {
			b.WriteByte(field[i])
			continue
		}
		if i+3 >= len(field) {
			return "", errors.New("malformed mount point escape")
		}
		v, err := strconv.ParseUint(field[i+1:i+4], 8, 8)
		if err != nil {
			return "", errors.New("malformed mount point escape")
		}
		b.WriteByte(byte(v))
		i += 3
	}
	return b.String(), nil
}
