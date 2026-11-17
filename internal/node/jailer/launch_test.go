package jailer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

const jailerLaunchID = "bx_cccccccc"

var errJailerInjected = errors.New("injected boundary failure")

// jailerAssertReleased proves a reservation left nothing behind: no record
// in memory or on disk, no jail, no cgroup, and its uid is free again.
func jailerAssertReleased(t *testing.T, env *jailerEnv, m *Manager, id string) {
	t.Helper()
	if _, ok := m.Record(id); ok {
		t.Fatal("reservation is still held in memory")
	}
	if jailerExists(t, env.cfg.StateDir, recordPath(id)) {
		t.Fatal("durable record still exists")
	}
	if jailerExists(t, env.cfg.StateDir, path.Join(jailsDir, id)) {
		t.Fatal("jail still exists")
	}
	if env.cgroups.has(id) {
		t.Fatal("cgroup still exists")
	}
	if uid, err := m.allocateUID(); err != nil || uid != env.cfg.UIDs.Min {
		t.Fatalf("uid pool = %d, %v; want %d free", uid, err, env.cfg.UIDs.Min)
	}
	if entries, err := os.ReadDir(filepath.Join(env.cfg.StateDir, recordsDir)); err != nil || len(entries) != 0 {
		t.Fatalf("records dir holds %d entries (%v); attempt temps must be gone", len(entries), err)
	}
}

// jailerAssertRetained proves a reservation is still held and its uid is not
// offered to another sandbox.
func jailerAssertRetained(t *testing.T, m *Manager, id string) Record {
	t.Helper()
	record, ok := m.Record(id)
	if !ok {
		t.Fatal("reservation was dropped")
	}
	if uid, err := m.allocateUID(); err == nil && uid == record.UID {
		t.Fatalf("retained uid %d is offered again", uid)
	}
	return record
}

func TestStartLifecycle(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	ctx, cancel := context.WithCancel(t.Context())
	spec := jailerSpec(t, jailerLaunchID)
	fdsBefore := jailerOpenFDs(t)
	child, owner, err := jailerStart(t, m, ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	// Startup context cancellation after the handoff must not kill QEMU.
	cancel()
	record := child.Identity()
	if record.PID != child.Cmd.Process.Pid || record.StartTicks == 0 || record.UID != env.cfg.UIDs.Min {
		t.Fatalf("identity %+v does not describe direct child %d", record, child.Cmd.Process.Pid)
	}
	if got := env.stages.last(); got != stageExecProof {
		t.Fatalf("last stage %q, want %q", got, stageExecProof)
	}
	// The durable record carries the committed identity and ownership.
	records, owners, _, err := loadRecords(m.root)
	if err != nil {
		t.Fatal(err)
	}
	if records[jailerLaunchID] != record {
		t.Fatalf("durable record %+v, want %+v", records[jailerLaunchID], record)
	}
	own := owners[jailerLaunchID]
	if !own.Jail.valid() || !own.Cgroup.valid() || len(own.Entries) == 0 {
		t.Fatalf("durable ownership incomplete: %+v", own)
	}
	if got := env.cgroups.attached(jailerLaunchID); !slices.Equal(got, []int{record.PID}) {
		t.Fatalf("cgroup members %v, want [%d]", got, record.PID)
	}
	// The running executable is the staged copy, at the declared path.
	staged := filepath.Join(env.cfg.StateDir, jailsDir, jailerLaunchID, "root", jailerTestBinary)
	exe, err := os.Stat(fmt.Sprintf("/proc/%d/exe", record.PID))
	if err != nil {
		t.Fatal(err)
	}
	stagedInfo, err := os.Stat(staged)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(exe, stagedInfo) {
		t.Fatal("QEMU is not running from its staged inode")
	}
	if stagedInfo.Mode().Perm() != 0o555 {
		t.Fatalf("staged binary mode %v, want 0555", stagedInfo.Mode().Perm())
	}
	if !slices.Contains(env.chowned, "control:50000") || !slices.Contains(env.chowned, "serial.log:50000") || !slices.Contains(env.chowned, "disk0:50000") {
		t.Fatalf("control/log/overlay not owned by the VM uid: %v", env.chowned)
	}
	// Still alive after the cancelled startup context, so Release retains.
	if err := child.Release(t.Context()); !errors.Is(err, ErrAlive) {
		t.Fatalf("Release of a live child = %v, want ErrAlive", err)
	}
	if err := child.Kill(); err != nil {
		t.Fatal(err)
	}
	owner.waitKilled(t)
	if err := child.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	jailerAssertReleased(t, env, m, jailerLaunchID)
	if err := child.Kill(); !errors.Is(err, errHandleReleased) {
		t.Fatalf("Kill after Release = %v, want the released-handle error", err)
	}
	if err := child.Release(t.Context()); err != nil {
		t.Fatalf("repeated Release = %v", err)
	}
	if got := jailerOpenFDs(t); got != fdsBefore {
		t.Fatalf("descriptor count %d after the lifecycle, want %d", got, fdsBefore)
	}
}

// TestStartFaultBeforeSpawn fails each pre-spawn boundary. No process may
// exist, the exact boundary is the last one reached, and the rollback
// removes everything the attempt created.
func TestStartFaultBeforeSpawn(t *testing.T) {
	jailerNeedBinary(t)
	for _, stage := range []launchStage{
		stageJailBase, stageJailBaseRecord, stageJailPopulate, stageJailRecord,
		stageCgroup, stageCgroupRecord, stageOwnFiles, stageSpawn,
	} {
		t.Run(string(stage), func(t *testing.T) {
			env := newJailerEnv(t)
			env.stages.failAt, env.stages.err = stage, errJailerInjected
			m := env.open(t)
			defer m.Close()
			spec := jailerSpec(t, jailerLaunchID)
			fdsBefore := jailerOpenFDs(t)
			child, _, err := jailerStart(t, m, t.Context(), spec)
			if !errors.Is(err, errJailerInjected) {
				t.Fatalf("Start = %v, want the injected failure", err)
			}
			if child != nil {
				t.Fatal("a pre-spawn failure returned a child")
			}
			if got := env.stages.last(); got != stage {
				t.Fatalf("last stage %q, want %q", got, stage)
			}
			jailerAssertReleased(t, env, m, jailerLaunchID)
			if got := jailerOpenFDs(t); got != fdsBefore {
				t.Fatalf("descriptor count %d, want %d", got, fdsBefore)
			}
		})
	}
}

// TestStartFaultAfterSpawn fails each post-spawn boundary. The child is a
// cgroup member from birth at every one of them. Start returns the child,
// kills it through its own handle, never waits, and keeps every resource
// until the caller's Wait and Release.
func TestStartFaultAfterSpawn(t *testing.T) {
	jailerNeedBinary(t)
	for _, tt := range []struct {
		stage    launchStage
		attached bool
	}{
		{stageAttach, true},
		{stagePin, true},
		{stageCommit, true},
		{stageRelease, true},
		{stageStatus, true},
		{stageExecProof, true},
	} {
		t.Run(string(tt.stage), func(t *testing.T) {
			env := newJailerEnv(t)
			env.stages.failAt, env.stages.err = tt.stage, errJailerInjected
			m := env.open(t)
			defer m.Close()
			child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
			if !errors.Is(err, errJailerInjected) {
				t.Fatalf("Start = %v, want the injected failure", err)
			}
			if child == nil || child.Cmd == nil || child.Cmd.Process == nil {
				t.Fatal("a post-spawn failure lost the child")
			}
			if got := env.stages.last(); got != tt.stage {
				t.Fatalf("last stage %q, want %q", got, tt.stage)
			}
			if got := len(env.cgroups.attached(jailerLaunchID)) == 1; got != tt.attached {
				t.Fatalf("attached = %v, want %v", got, tt.attached)
			}
			jailerAssertRetained(t, m, jailerLaunchID)
			// The caller's sole Wait observes the jailer's SIGKILL.
			owner.waitKilled(t)
			if err := child.Release(t.Context()); err != nil {
				t.Fatal(err)
			}
			jailerAssertReleased(t, env, m, jailerLaunchID)
		})
	}
}

// TestStartHelperOutcomes covers helpers that close the status pipe without
// a proven exec: pre-exec death, a live helper with a closed pipe, a setup
// error, an exec of an unstaged binary and a hang ended by cancellation.
func TestStartHelperOutcomes(t *testing.T) {
	jailerNeedBinary(t)
	for _, tt := range []struct {
		mode   string
		reason string
		cancel bool
	}{
		{mode: jailerModeDie, reason: "exited before executable proof"},
		{mode: jailerModeCloseAlive, reason: "not the staged QEMU"},
		{mode: jailerModeSetupError, reason: "injected setup failure"},
		{mode: jailerModeExecHost, reason: "not the staged QEMU"},
		{mode: jailerModeHang, reason: "context canceled", cancel: true},
	} {
		t.Run(tt.mode, func(t *testing.T) {
			env := newJailerEnv(t)
			env.mode = tt.mode
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.cancel {
				env.stages.on = map[launchStage]func(){stageStatus: cancel}
			}
			m := env.open(t)
			defer m.Close()
			child, owner, err := jailerStart(t, m, ctx, jailerSpec(t, jailerLaunchID))
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("Start = %v, want %q", err, tt.reason)
			}
			if child == nil {
				t.Fatal("helper failure lost the child")
			}
			jailerAssertRetained(t, m, jailerLaunchID)
			_ = owner.wait(t) // exits by itself or by the jailer's SIGKILL
			if err := child.Release(t.Context()); err != nil {
				t.Fatal(err)
			}
			jailerAssertReleased(t, env, m, jailerLaunchID)
		})
	}
}

// TestStartIntentWriteOutcomes distinguishes a proven-clean first write
// (reservation dropped) from every uncertain outcome (reservation kept).
func TestStartIntentWriteOutcomes(t *testing.T) {
	jailerNeedBinary(t)
	for _, tt := range []struct {
		fault     stateFault
		cleanup   bool // also fail the attempt's own cleanup
		retained  bool
		published bool
	}{
		{fault: faultStateCreate},
		{fault: faultStateWrite},
		{fault: faultStateSync},
		{fault: faultStateClose},
		{fault: faultStateWrite, cleanup: true, retained: true},
		{fault: faultStateIdentity, retained: true},
		{fault: faultStateRename, retained: true},
		{fault: faultStateParentSync, retained: true, published: true},
	} {
		name := string(tt.fault)
		if tt.cleanup {
			name += "+cleanup"
		}
		t.Run(name, func(t *testing.T) {
			env := newJailerEnv(t)
			m := env.open(t)
			defer m.Close()
			m.state = fileStates{fault: func(stage stateFault) error {
				if stage == tt.fault || (tt.cleanup && stage == faultStateCleanup) {
					return errJailerInjected
				}
				return nil
			}}
			child, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
			if !errors.Is(err, errJailerInjected) || child != nil {
				t.Fatalf("Start = %v, %v; want the injected failure without a child", child, err)
			}
			if got := env.stages.last(); got != "" {
				t.Fatalf("Start continued past the intent to %q", got)
			}
			if got := jailerExists(t, env.cfg.StateDir, recordPath(jailerLaunchID)); got != tt.published {
				t.Fatalf("record published = %v, want %v", got, tt.published)
			}
			if !tt.retained {
				jailerAssertReleased(t, env, m, jailerLaunchID)
				return
			}
			jailerAssertRetained(t, m, jailerLaunchID)
			// Restart: a published record is reloaded and reconciled away
			// (no process, jail or cgroup); an unpublished one is simply gone.
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			m2 := env.open(t)
			defer m2.Close()
			if _, ok := m2.Record(jailerLaunchID); ok != tt.published {
				t.Fatalf("reloaded = %v, want %v", ok, tt.published)
			}
			report, err := m2.Reconcile(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if tt.published && !slices.Equal(report.Released, []string{jailerLaunchID}) {
				t.Fatalf("Reconcile = %+v", report)
			}
			// Temps left by a failed cleanup stay: they are not records and
			// cannot be proven removable.
			if _, ok := m2.Record(jailerLaunchID); ok {
				t.Fatal("reservation survived reconcile")
			}
		})
	}
}

// TestStartCommitWriteUncertainKeepsIdentity fails the post-attach commit
// after rename: the live identity stays reserved, and after the caller's
// Wait the child is released exactly once.
func TestStartCommitWriteUncertainKeepsIdentity(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	writes := 0
	m.state = fileStates{fault: func(stage stateFault) error {
		if stage == faultStateParentSync {
			writes++
			if writes == 5 { // intent, jail base, jail, cgroup, commit
				return errJailerInjected
			}
		}
		return nil
	}}
	child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if !errors.Is(err, errJailerInjected) || child == nil {
		t.Fatalf("Start = %v, %v", child, err)
	}
	if got := env.stages.last(); got != stageCommit {
		t.Fatalf("last stage %q, want %q", got, stageCommit)
	}
	record := jailerAssertRetained(t, m, jailerLaunchID)
	if record.PID != child.Cmd.Process.Pid || record.StartTicks == 0 {
		t.Fatalf("in-memory identity %+v lost the child", record)
	}
	records, _, _, err := loadRecords(m.root)
	if err != nil {
		t.Fatal(err)
	}
	if records[jailerLaunchID].PID != record.PID {
		t.Fatal("the uncertain commit's published identity differs")
	}
	owner.waitKilled(t)
	if err := child.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	jailerAssertReleased(t, env, m, jailerLaunchID)
}

// TestRestartRetainsLiveChild closes the Manager with QEMU running. A new
// Manager reloads the reservation, Reconcile keeps it while alive, the old
// Child keeps its own handle, and only proven death releases it.
func TestRestartRetainsLiveChild(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerReviewID)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after Close = %v", err)
	}
	m2 := env.open(t)
	defer m2.Close()
	if _, ok := m2.Record(jailerLaunchID); !ok {
		t.Fatal("live reservation was not reloaded")
	}
	report, err := m2.Reconcile(t.Context())
	if err != nil || !slices.Equal(report.Retained, []string{jailerLaunchID}) {
		t.Fatalf("Reconcile = %+v, %v; want retained", report, err)
	}
	if err := m2.Release(t.Context(), jailerLaunchID); !errors.Is(err, ErrAlive) {
		t.Fatalf("Release of a live recovered record = %v", err)
	}
	// A recovered-identity kill by the new owner.
	if err := m2.Kill(t.Context(), jailerRefOf(m2, jailerLaunchID)); err != nil {
		t.Fatal(err)
	}
	owner.waitKilled(t)
	// The old Child releases only its own handle; the new owner reconciles.
	if err := child.Release(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Child.Release after Close = %v, want ErrClosed", err)
	}
	if _, ok := m2.Record(jailerLaunchID); !ok {
		t.Fatal("the closed Manager's Child touched the new owner's reservation")
	}
	if err := child.Kill(); !errors.Is(err, errHandleReleased) {
		t.Fatalf("Kill after handle release = %v", err)
	}
	report, err = m2.Reconcile(t.Context())
	if err != nil || !slices.Equal(report.Released, []string{jailerLaunchID}) {
		t.Fatalf("Reconcile = %+v, %v; want released", report, err)
	}
	jailerAssertReleased(t, env, m2, jailerLaunchID)
}

// TestChildKillAfterManagerClose keeps Child.Kill usable after Close.
func TestChildKillAfterManagerClose(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := child.Kill(); err != nil {
		t.Fatalf("Kill after Manager.Close = %v", err)
	}
	owner.waitKilled(t)
	if err := child.Release(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Release = %v, want ErrClosed", err)
	}
	m2 := env.open(t)
	defer m2.Close()
	if _, err := m2.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	jailerAssertReleased(t, env, m2, jailerLaunchID)
}

// TestNoPidfdFallbackPolarity forces pidfd_open to fail with ENOSYS and
// EPERM. A live owned child is not exited and Release retains it; after the
// caller's Wait it is exited and released. Kill uses the caller's unreaped
// os.Process, and closing the handle never touches that os.Process.
func TestNoPidfdFallbackPolarity(t *testing.T) {
	jailerNeedBinary(t)
	for _, errno := range []error{unix.ENOSYS, unix.EPERM} {
		t.Run(errno.Error(), func(t *testing.T) {
			env := newJailerEnv(t)
			env.pidfd = func(int) (int, error) { return -1, errno }
			m := env.open(t)
			defer m.Close()
			child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
			if err != nil {
				t.Fatal(err)
			}
			if child.handle.fd != -1 {
				t.Fatal("handle has a pidfd despite the forced failure")
			}
			exited, err := child.handle.exited(child.Identity())
			if err != nil || exited {
				t.Fatalf("live child exited = %v, %v; want false", exited, err)
			}
			if err := child.Release(t.Context()); !errors.Is(err, ErrAlive) {
				t.Fatalf("Release of a live child = %v, want ErrAlive", err)
			}
			jailerAssertRetained(t, m, jailerLaunchID)
			if err := child.Kill(); err != nil {
				t.Fatal(err)
			}
			owner.waitKilled(t)
			exited, err = child.handle.exited(child.Identity())
			if err != nil || !exited {
				t.Fatalf("dead child exited = %v, %v; want true", exited, err)
			}
			if err := child.Release(t.Context()); err != nil {
				t.Fatal(err)
			}
			jailerAssertReleased(t, env, m, jailerLaunchID)
			// The caller's os.Process stays usable: Release did not release it.
			if err := child.Cmd.Process.Signal(syscall.Signal(0)); !errors.Is(err, os.ErrProcessDone) {
				t.Fatalf("os.Process state after Release = %v", err)
			}
		})
	}
}

// TestPidfdOpenErrorStillKillable: an unexpected pidfd_open error fails the
// launch, but the child is returned and killed through the fallback.
//
// The birth membership is also left unverified here (the fake's Joined
// fails), so not even the owned cgroup proves this child's death in process:
// the reservation stays until a restart reconciles it.
func TestPidfdOpenErrorStillKillable(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	env.pidfd = func(int) (int, error) { return -1, unix.EMFILE }
	env.cgroups.fail["attach"] = errJailerInjected
	m := env.open(t)
	defer m.Close()
	child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if !errors.Is(err, unix.EMFILE) || child == nil {
		t.Fatalf("Start = %v, %v", child, err)
	}
	if got := env.stages.last(); got != stageSpawn {
		t.Fatalf("last stage %q, want %q", got, stageSpawn)
	}
	owner.waitKilled(t)
	// Without a pidfd, a captured identity or verified containment, death
	// cannot be proven in process.
	if err := child.Release(t.Context()); err == nil {
		t.Fatal("Release succeeded without proof of death")
	}
	jailerAssertRetained(t, m, jailerLaunchID)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m2 := env.open(t)
	defer m2.Close()
	if _, err := m2.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	jailerAssertReleased(t, env, m2, jailerLaunchID)
}

// TestConcurrentChildOperations races Kill, Release, Manager.Kill, Reconcile,
// and Records under -race. Release succeeds once death is proven, repeated
// Releases are no-ops, and a released handle never signals again.
func TestConcurrentChildOperations(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { _ = child.Kill() })
		wg.Go(func() { _ = child.Release(t.Context()) })
		wg.Go(func() { _ = m.Kill(t.Context(), jailerRefOf(m, jailerLaunchID)) })
		wg.Go(func() { _, _ = m.Reconcile(t.Context()) })
		wg.Go(func() { _ = m.Records() })
	}
	wg.Wait()
	owner.waitKilled(t)
	var released int
	var mu sync.Mutex
	for range 8 {
		wg.Go(func() {
			if child.Release(t.Context()) == nil {
				mu.Lock()
				released++
				mu.Unlock()
			}
		})
		wg.Go(func() { _ = child.Kill() })
	}
	wg.Wait()
	if released == 0 {
		t.Fatal("no Release succeeded after Wait")
	}
	jailerAssertReleased(t, env, m, jailerLaunchID)
	if child.handle.fd != -1 || !child.handle.closed {
		t.Fatal("handle still holds a descriptor after Release")
	}
}

// TestStartRefusesRetainedAndCollidingReservations: a live id is refused
// with ErrAlive; another id with the same pinned control directory is
// refused; the uid pool is bounded.
func TestStartRefusesRetainedAndCollidingReservations(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	m := env.open(t)
	defer m.Close()
	spec := jailerSpec(t, jailerLaunchID)
	child, owner, err := jailerStart(t, m, t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = child.Kill()
		_ = owner.wait(t)
		_ = child.Release(context.Background())
	}()
	again := jailerSpec(t, jailerLaunchID)
	if _, _, err := jailerStart(t, m, t.Context(), again); !errors.Is(err, ErrRetained) {
		t.Fatalf("Start of a live id = %v, want ErrRetained", err)
	}
	other := jailerSpec(t, jailerReviewID)
	other.ControlDir = spec.ControlDir
	if _, _, err := jailerStart(t, m, t.Context(), other); err == nil || !strings.Contains(err.Error(), "control directory is already pinned") {
		t.Fatalf("shared control dir = %v", err)
	}
	other = jailerSpec(t, jailerReviewID)
	other.SerialLog = spec.SerialLog
	if _, _, err := jailerStart(t, m, t.Context(), other); err == nil || !strings.Contains(err.Error(), "serial log is already pinned") {
		t.Fatalf("shared serial log = %v", err)
	}
	if got := len(m.Records()); got != 1 {
		t.Fatalf("%d reservations, want 1", got)
	}
}

func TestStartUIDPoolExhausted(t *testing.T) {
	jailerNeedBinary(t)
	env := newJailerEnv(t)
	env.cfg.UIDs = UIDRange{Min: 50000, Max: 50000}
	m := env.open(t)
	defer m.Close()
	child, owner, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerReviewID)); !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("Start = %v, want ErrPoolExhausted", err)
	}
	_ = child.Kill()
	_ = owner.wait(t)
	if err := child.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// TestStartPreservesForeignJailAndCgroup: a pre-existing jail directory or
// cgroup for the id is never adopted or removed, and the reservation stays
// to protect it.
func TestStartPreservesForeignJailAndCgroup(t *testing.T) {
	jailerNeedBinary(t)
	t.Run("jail", func(t *testing.T) {
		env := newJailerEnv(t)
		m := env.open(t)
		defer m.Close()
		foreign := filepath.Join(env.cfg.StateDir, jailsDir, jailerLaunchID)
		if err := os.MkdirAll(filepath.Join(foreign, "keep"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID)); err == nil {
			t.Fatal("Start adopted a foreign jail")
		}
		if _, err := os.Stat(filepath.Join(foreign, "keep")); err != nil {
			t.Fatalf("foreign jail content removed: %v", err)
		}
		jailerAssertRetained(t, m, jailerLaunchID)
	})
	t.Run("cgroup", func(t *testing.T) {
		env := newJailerEnv(t)
		env.cgroups.groups[jailerLaunchID] = &jailerFakeGroup{identity: inodeIdentity{Dev: 1, Ino: 1}}
		m := env.open(t)
		defer m.Close()
		if _, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID)); err == nil {
			t.Fatal("Start adopted a foreign cgroup")
		}
		if !env.cgroups.has(jailerLaunchID) {
			t.Fatal("foreign cgroup removed")
		}
		// An unowned group under the name blocks proof that the sandbox is
		// empty, so the whole reservation, including its own jail, stays.
		jailerAssertRetained(t, m, jailerLaunchID)
		if !jailerExists(t, env.cfg.StateDir, path.Join(jailsDir, jailerLaunchID)) {
			t.Fatal("the reservation's own jail vanished while retained")
		}
		// Once an operator removes the foreign group, Release completes.
		env.cgroups.mu.Lock()
		delete(env.cgroups.groups, jailerLaunchID)
		env.cgroups.mu.Unlock()
		if err := m.Release(t.Context(), jailerLaunchID); err != nil {
			t.Fatal(err)
		}
		jailerAssertReleased(t, env, m, jailerLaunchID)
	})
	t.Run("cgroup limits", func(t *testing.T) {
		env := newJailerEnv(t)
		env.cgroups.fail["limits"] = errJailerInjected
		m := env.open(t)
		defer m.Close()
		if _, _, err := jailerStart(t, m, t.Context(), jailerSpec(t, jailerLaunchID)); !errors.Is(err, errJailerInjected) {
			t.Fatalf("Start = %v", err)
		}
		// The attempt created the group, so its identity proves ownership.
		jailerAssertReleased(t, env, m, jailerLaunchID)
	})
}
