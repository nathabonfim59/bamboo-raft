package jailer

import (
	"encoding/json/v2"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
)

// Pinned control/log mounts: the helper is born in a new mount namespace, so
// it cannot bind the node's host-namespace descriptors (EINVAL). The parent
// hands it non-recursive detached clones instead, in the same ControlFD and
// LogFD slots, and owns and closes them on every path.

// jailerDetachRecorder stands in for open_tree+mount_setattr: it returns a
// fresh descriptor of each pinned inode and records what it handed out. The
// failAt-th call (1-based) fails instead.
type jailerDetachRecorder struct {
	mu     sync.Mutex
	clones []*os.File
	calls  int
	failAt int
}

func (r *jailerDetachRecorder) detach(f *os.File) (*os.File, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.calls == r.failAt {
		return nil, errJailerInjected
	}
	clone, err := os.Open(fdPath(int(f.Fd())))
	if err != nil {
		return nil, err
	}
	r.clones = append(r.clones, clone)
	return clone, nil
}

// assertClosed requires that Start closed every clone it was handed.
func (r *jailerDetachRecorder) assertClosed(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, clone := range r.clones {
		if _, err := clone.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("detached clone %d is still open after Start: %v", i, err)
		}
	}
}

// jailerCaptureSlots wraps the env command hook to record the helper-only
// descriptors following the caller's files: config, release, status,
// control, log.
func jailerCaptureSlots(m *Manager, files int) *[]*os.File {
	slots := new([]*os.File)
	inner := m.hooks.command
	m.hooks.command = func(cmd *exec.Cmd) {
		inner(cmd)
		*slots = append([]*os.File(nil), cmd.ExtraFiles[files:]...)
	}
	return slots
}

func TestSpawnPassesDetachedPinnedMounts(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	rec := &jailerDetachRecorder{}
	m.hooks.detachMount = rec.detach
	spec := jailerSpec(t, jailerLaunchID)
	slots := jailerCaptureSlots(m, len(spec.Files))
	child, owner, err := jailerStart(t, m, t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.clones) != 2 {
		t.Fatalf("detached %d mounts, want control and serial log", len(rec.clones))
	}
	if len(*slots) != 5 || (*slots)[3] != rec.clones[0] || (*slots)[4] != rec.clones[1] {
		t.Fatal("the helper's ControlFD/LogFD slots are not the detached clones")
	}
	for _, f := range []*os.File{spec.ControlDir, spec.SerialLog} {
		for _, slot := range *slots {
			if slot == f {
				t.Fatal("a host-namespace pinned descriptor reached the helper")
			}
		}
	}
	rec.assertClosed(t)
	if err := child.Kill(); err != nil {
		t.Fatal(err)
	}
	owner.waitKilled(t)
	if err := child.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// TestDetachedMountLifetime: Start closes every detached clone it created,
// whether the launch fails before the clones, between them, at the spawn
// itself, or after the spawn, and leaks no descriptor.
func TestDetachedMountLifetime(t *testing.T) {
	jailerNeedBinary(t)
	for _, tt := range []struct {
		name    string
		failAt  int // detach call that fails (0: none)
		clone   bool
		stage   launchStage
		clones  int
		spawned bool
	}{
		{name: "control detach fails", failAt: 1, clones: 0},
		{name: "log detach fails", failAt: 2, clones: 1},
		{name: "clone3 fails", clone: true, clones: 2},
		{name: "post-spawn failure", stage: stagePin, clones: 2, spawned: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			env := newJailerEnv(t)
			if tt.stage != "" {
				env.stages.failAt, env.stages.err = tt.stage, errJailerInjected
			}
			m := env.open(t)
			defer m.Close()
			rec := &jailerDetachRecorder{failAt: tt.failAt}
			m.hooks.detachMount = rec.detach
			if tt.clone {
				inner := m.hooks.command
				m.hooks.command = func(cmd *exec.Cmd) {
					inner(cmd)
					cmd.SysProcAttr.UseCgroupFD = true
					cmd.SysProcAttr.CgroupFD = -1
				}
			}
			spec := jailerSpec(t, jailerLaunchID)
			fdsBefore := jailerOpenFDs(t)
			child, owner, err := jailerStart(t, m, t.Context(), spec)
			if err == nil {
				t.Fatal("Start succeeded")
			}
			if (child != nil) != tt.spawned {
				t.Fatalf("child = %v, want spawned %v", child, tt.spawned)
			}
			if len(rec.clones) != tt.clones {
				t.Fatalf("%d clones made, want %d", len(rec.clones), tt.clones)
			}
			rec.assertClosed(t)
			if tt.spawned {
				owner.waitKilled(t)
				if err := child.Release(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			jailerAssertReleased(t, env, m, jailerLaunchID)
			if got := jailerOpenFDs(t); got != fdsBefore {
				t.Fatalf("descriptor count %d, want %d", got, fdsBefore)
			}
		})
	}
}

// TestPinnedMountFlags pins the exact flags: a non-recursive, close-on-exec
// detached clone; nosuid, nodev, noexec and private propagation; a move of
// the tree by fd alone.
func TestPinnedMountFlags(t *testing.T) {
	if openTreeFlags != unix.AT_EMPTY_PATH|unix.OPEN_TREE_CLONE|unix.O_CLOEXEC {
		t.Fatalf("open_tree flags %#x", openTreeFlags)
	}
	if openTreeFlags&unix.AT_RECURSIVE != 0 {
		t.Fatal("open_tree clones recursively and could expose submounts")
	}
	attr := pinnedMountAttr()
	want := unix.MountAttr{
		Attr_set:    unix.MOUNT_ATTR_NOSUID | unix.MOUNT_ATTR_NODEV | unix.MOUNT_ATTR_NOEXEC,
		Propagation: unix.MS_PRIVATE,
	}
	if attr != want {
		t.Fatalf("mount_setattr %+v, want %+v", attr, want)
	}
	if attr.Attr_set&unix.MOUNT_ATTR_RDONLY != 0 {
		t.Fatal("the control dir must stay writable for the QMP socket")
	}
	if moveMountFlags != unix.MOVE_MOUNT_F_EMPTY_PATH {
		t.Fatalf("move_mount flags %#x", moveMountFlags)
	}
}

// TestPinnedMountIdentity: the helper accepts a tree and a mounted target
// only when each is the expected pinned inode.
func TestPinnedMountIdentity(t *testing.T) {
	dir := t.TempDir()
	pinned := filepath.Join(dir, "pinned")
	other := filepath.Join(dir, "other")
	for _, p := range []string{pinned, other} {
		if err := os.WriteFile(p, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f := jailerOpen(t, pinned, os.O_RDONLY)
	want, err := fileInode(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkFDIdentity(int(f.Fd()), want); err != nil {
		t.Fatalf("matching tree rejected: %v", err)
	}
	if err := checkPathIdentity(pinned, want); err != nil {
		t.Fatalf("matching target rejected: %v", err)
	}
	g := jailerOpen(t, other, os.O_RDONLY)
	if err := checkFDIdentity(int(g.Fd()), want); err == nil || !strings.Contains(err.Error(), "want") {
		t.Fatalf("a different tree inode was accepted: %v", err)
	}
	if err := checkPathIdentity(other, want); err == nil {
		t.Fatal("a different mounted inode was accepted")
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(pinned, link); err != nil {
		t.Fatal(err)
	}
	if err := checkPathIdentity(link, want); err == nil {
		t.Fatal("a symlink to the pinned inode was accepted as the mounted target")
	}
}

// TestHelperConfigCarriesPinnedIdentity: the helper config names the pinned
// inodes the detached mounts must root, so a mixed-up slot fails closed.
func TestHelperConfigCarriesPinnedIdentity(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	spec := jailerSpec(t, jailerLaunchID)
	var cfg helperConfig
	inner := m.hooks.command
	m.hooks.command = func(cmd *exec.Cmd) {
		inner(cmd)
		configFile := cmd.ExtraFiles[len(spec.Files)]
		raw, err := os.ReadFile(fdPath(int(configFile.Fd())))
		if err == nil {
			err = json.Unmarshal(raw, &cfg)
		}
		if err != nil {
			t.Error(err)
		}
	}
	child, owner, err := jailerStart(t, m, t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	control, _ := fileInode(spec.ControlDir)
	log, _ := fileInode(spec.SerialLog)
	if !cfg.Control.equal(control) || !cfg.Log.equal(log) {
		t.Fatalf("helper config identities %v/%v, want %v/%v", cfg.Control, cfg.Log, control, log)
	}
	_ = child.Kill()
	owner.waitKilled(t)
	if err := child.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// TestDetachPinnedFailsClosedUnprivileged: without CAP_SYS_ADMIN the real
// open_tree/mount_setattr path returns an error and no descriptor.
func TestDetachPinnedFailsClosedUnprivileged(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root may succeed; the privileged suite covers the real path")
	}
	f := jailerOpen(t, t.TempDir(), os.O_RDONLY)
	fdsBefore := jailerOpenFDs(t)
	tree, err := detachPinned(f)
	if err == nil || tree != nil {
		if tree != nil {
			tree.Close()
		}
		t.Fatalf("detachPinned = %v, %v; want a closed failure", tree, err)
	}
	if got := jailerOpenFDs(t); got != fdsBefore {
		t.Fatalf("descriptor count %d, want %d", got, fdsBefore)
	}
}
