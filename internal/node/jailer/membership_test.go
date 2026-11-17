package jailer

import (
	"errors"
	"os"
	"os/exec"
	"slices"
	"testing"

	"golang.org/x/sys/unix"
)

// Spawn membership regressions: a child whose launch failed before its
// identity was captured (record PID 0) must keep every reservation until its
// death is proven, even when Start's SIGKILL fails, through by-id Release,
// Reconcile and a Manager restart.

var errJailerKillFailed = errors.New("injected kill failure")

// jailerPreCaptureChild starts a launch that fails before the identity
// capture with Start's kill failing, leaving a live, gated helper. how
// selects the pre-capture failure: a pin-stage fault or a pidfd error.
func jailerPreCaptureChild(t *testing.T, env *jailerEnv, how string) (*Manager, *Child, *jailerOwner) {
	t.Helper()
	env.mode = jailerModeGateHang
	switch how {
	case "pin":
		env.stages.failAt, env.stages.err = stagePin, errJailerInjected
	case "pidfd":
		env.pidfd = func(int) (int, error) { return -1, unix.EMFILE }
	}
	m := env.open(t)
	m.hooks.kill = func(*processHandle) error { return errJailerKillFailed }
	child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if child == nil || !errors.Is(err, errJailerKillFailed) {
		t.Fatalf("Start = %v, %v; want a returned child and the failed kill", child, err)
	}
	if record, _ := m.Record(jailerLaunchID); record.PID != 0 {
		t.Fatalf("record PID %d; the failure must precede identity capture", record.PID)
	}
	if exited, err := child.handle.exited(Record{}); err == nil && exited {
		t.Fatal("the helper is not alive; the test needs a live pre-capture child")
	}
	return m, child, owner
}

func TestPreCaptureChildRetainedByID(t *testing.T) {
	jailerNeedBinary(t)
	for _, how := range []string{"pin", "pidfd"} {
		t.Run(how, func(t *testing.T) {
			env := newJailerEnv(t)
			m, child, owner := jailerPreCaptureChild(t, env, how)
			defer m.Close()
			if err := m.Release(t.Context(), jailerLaunchID); !errors.Is(err, ErrAlive) {
				t.Fatalf("Release by id of a live pre-capture child = %v, want ErrAlive", err)
			}
			report, err := m.Reconcile(t.Context())
			if err != nil || !slices.Equal(report.Retained, []string{jailerLaunchID}) {
				t.Fatalf("Reconcile = %+v, %v; want retained", report, err)
			}
			jailerAssertRetained(t, m, jailerLaunchID)
			if !jailerExists(t, env.cfg.StateDir, recordPath(jailerLaunchID)) {
				t.Fatal("the durable reservation was removed while the child lives")
			}
			// The owner proves death: a by-id kill through the owned cgroup,
			// then the sole Wait.
			if err := m.Kill(t.Context(), jailerRefOf(m, jailerLaunchID)); err != nil {
				t.Fatal(err)
			}
			owner.waitKilled(t)
			report, err = m.Reconcile(t.Context())
			if err != nil || !slices.Equal(report.Released, []string{jailerLaunchID}) {
				t.Fatalf("Reconcile after death = %+v, %v; want released", report, err)
			}
			jailerAssertReleased(t, env, m, jailerLaunchID)
			_ = child
		})
	}
}

func TestPreCaptureChildRetainedAcrossRestart(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m, child, owner := jailerPreCaptureChild(t, env, "pin")
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m2 := env.open(t)
	defer m2.Close()
	report, err := m2.Reconcile(t.Context())
	if err != nil || !slices.Equal(report.Retained, []string{jailerLaunchID}) {
		t.Fatalf("Reconcile after restart = %+v, %v; want retained", report, err)
	}
	if err := m2.Release(t.Context(), jailerLaunchID); !errors.Is(err, ErrAlive) {
		t.Fatalf("Release by id after restart = %v, want ErrAlive", err)
	}
	jailerAssertRetained(t, m2, jailerLaunchID)
	if err := child.Kill(); err != nil {
		t.Fatal(err)
	}
	owner.waitKilled(t)
	report, err = m2.Reconcile(t.Context())
	if err != nil || !slices.Equal(report.Released, []string{jailerLaunchID}) {
		t.Fatalf("Reconcile after death = %+v, %v; want released", report, err)
	}
	jailerAssertReleased(t, env, m2, jailerLaunchID)
}

// TestStartUsesClonePidfd: the handle owns the pidfd clone3 returned, and
// pidfd_open is never consulted when the clone supplied one.
func TestStartUsesClonePidfd(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	m.hooks.pidfdOpen = func(int) (int, error) {
		t.Error("pidfd_open was used although clone3 returned a pidfd")
		return -1, unix.ENOSYS
	}
	child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if err != nil {
		t.Fatal(err)
	}
	if child.handle.fd < 0 {
		t.Fatal("the handle has no pidfd")
	}
	if got := env.cgroups.attached(jailerLaunchID); !slices.Equal(got, []int{child.Cmd.Process.Pid}) {
		t.Fatalf("cgroup members %v, want the child from birth", got)
	}
	if err := child.Kill(); err != nil {
		t.Fatal(err)
	}
	owner.waitKilled(t)
	if err := child.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if child.handle.fd != -1 {
		t.Fatal("Release did not close the clone pidfd")
	}
}

// TestStartMembershipVerifyFailure: a failed membership verification kills
// the already-contained child and keeps the reservation until its death.
func TestStartMembershipVerifyFailure(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	env.cgroups.fail["attach"] = errJailerInjected
	m := env.open(t)
	defer m.Close()
	child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if child == nil || !errors.Is(err, errJailerInjected) {
		t.Fatalf("Start = %v, %v", child, err)
	}
	if got := env.stages.last(); got != stageAttach {
		t.Fatalf("last stage %q, want %q", got, stageAttach)
	}
	jailerAssertRetained(t, m, jailerLaunchID)
	owner.waitKilled(t)
	if err := child.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	jailerAssertReleased(t, env, m, jailerLaunchID)
}

// TestStartSpawnFailuresNoChild: a cgroup that cannot be opened for the
// clone, or a failed clone itself, spawns nothing, leaks no descriptor and
// rolls the reservation back. There is no spawn-then-attach fallback.
func TestStartSpawnFailuresNoChild(t *testing.T) {
	jailerNeedBinary(t)
	for _, tt := range []struct {
		name string
		edit func(*jailerEnv, *Manager)
	}{
		{name: "owned cgroup open fails", edit: func(env *jailerEnv, _ *Manager) {
			env.cgroups.fail["open"] = errJailerInjected
		}},
		{name: "clone fails", edit: func(_ *jailerEnv, m *Manager) {
			inner := m.hooks.command
			m.hooks.command = func(cmd *exec.Cmd) {
				inner(cmd)
				// An invalid cgroup fd makes clone3 fail, as on a host
				// that refuses CLONE_INTO_CGROUP.
				cmd.SysProcAttr.UseCgroupFD = true
				cmd.SysProcAttr.CgroupFD = -1
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			env := newJailerEnv(t)
			m := env.open(t)
			defer m.Close()
			tt.edit(env, m)
			spec := jailerSpec(t, jailerLaunchID)
			fdsBefore := jailerOpenFDs(t)
			child, _, err := jailerStart(t, m, t.Context(), spec)
			if child != nil || err == nil {
				t.Fatalf("Start = %v, %v; want an error without a child", child, err)
			}
			if got := env.stages.last(); got != stageSpawn {
				t.Fatalf("last stage %q, want %q", got, stageSpawn)
			}
			jailerAssertReleased(t, env, m, jailerLaunchID)
			if got := jailerOpenFDs(t); got != fdsBefore {
				t.Fatalf("descriptor count %d, want %d", got, fdsBefore)
			}
		})
	}
}

// TestNoPidfdPreCaptureReleaseByContainment: with no pidfd and no captured
// identity, a child whose birth membership was verified is proven dead by
// its owned cgroup alone. While it lives, Child.Release keeps ErrAlive and
// the full reservation; once it is gone, Child.Release frees it in process.
func TestNoPidfdPreCaptureReleaseByContainment(t *testing.T) {
	jailerNeedBinary(t)
	t.Run("dead", func(t *testing.T) {
		env := newJailerEnv(t)
		env.pidfd = func(int) (int, error) { return -1, unix.EMFILE }
		m := env.open(t)
		defer m.Close()
		child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
		if child == nil || !errors.Is(err, unix.EMFILE) {
			t.Fatalf("Start = %v, %v", child, err)
		}
		owner.waitKilled(t)
		if err := child.Release(t.Context()); err != nil {
			t.Fatalf("Release after proven death = %v", err)
		}
		jailerAssertReleased(t, env, m, jailerLaunchID)
	})
	t.Run("live", func(t *testing.T) {
		env := newJailerEnv(t)
		m, child, owner := jailerPreCaptureChild(t, env, "pidfd")
		defer m.Close()
		if err := child.Release(t.Context()); !errors.Is(err, ErrAlive) {
			t.Fatalf("Release of a live contained child = %v, want ErrAlive", err)
		}
		jailerAssertRetained(t, m, jailerLaunchID)
		for _, p := range []string{recordPath(jailerLaunchID), "jails/" + jailerLaunchID} {
			if !jailerExists(t, env.cfg.StateDir, p) {
				t.Fatalf("%s was removed while the child lives", p)
			}
		}
		if !env.cgroups.has(jailerLaunchID) {
			t.Fatal("the cgroup was removed while the child lives")
		}
		if err := m.Kill(t.Context(), jailerRefOf(m, jailerLaunchID)); err != nil {
			t.Fatal(err)
		}
		owner.waitKilled(t)
		if err := child.Release(t.Context()); err != nil {
			t.Fatalf("Release after proven death = %v", err)
		}
		jailerAssertReleased(t, env, m, jailerLaunchID)
	})
}

// TestHandleExitedContainment: a no-pidfd handle with no captured identity
// defers to the cgroup only after verified containment; a handle with a
// captured identity or a pidfd keeps its own evidence.
func TestHandleExitedContainment(t *testing.T) {
	self, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	// FindProcess may hold a pidfd; release it now rather than letting a
	// finalizer close it during another test's descriptor count.
	t.Cleanup(func() { _ = self.Release() })
	h := &processHandle{pid: os.Getpid(), fd: -1, process: self}
	if _, err := h.exited(Record{}); err == nil || errors.Is(err, errDecideByCgroup) {
		t.Fatalf("uncontained PID-0 handle = %v, want a no-proof error", err)
	}
	h.markContained()
	if _, err := h.exited(Record{}); !errors.Is(err, errDecideByCgroup) {
		t.Fatalf("contained PID-0 handle = %v, want errDecideByCgroup", err)
	}
	// A captured identity is still checked by start time, not the cgroup.
	bootID, err := hostBootID()
	if err != nil {
		t.Fatal(err)
	}
	ticks, err := procStartTicks(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	exited, err := h.exited(Record{PID: os.Getpid(), BootID: bootID, StartTicks: ticks})
	if err != nil || exited {
		t.Fatalf("captured identity of a live process = %v, %v", exited, err)
	}
}
