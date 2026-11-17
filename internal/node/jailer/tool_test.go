package jailer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// A10 D1: jailed image tools. The fake helper execs the staged test binary
// with "3600": /usr/bin/true and /usr/bin/false finish at once,
// /usr/bin/sleep runs until killed.

// jailerToolEnv opens a Manager whose tool targets may be owned by the test
// user (the at-rest owner seam) and returns the env.
func jailerToolEnv(t *testing.T, mode string) (*jailerEnv, *Manager) {
	t.Helper()
	env := jailerToolEnvOnly(t, mode)
	return env, jailerToolOpen(t, env)
}

// jailerToolOpen opens a Manager on env with the tool test seams.
func jailerToolOpen(t *testing.T, env *jailerEnv) *Manager {
	t.Helper()
	m := env.open(t)
	t.Cleanup(func() { m.Close() })
	m.hooks.atRestOwner = func() uint32 { return uint32(os.Getuid()) }
	m.hooks.toolChown = func(f *os.File, uid, gid int) error {
		env.mu.Lock()
		defer env.mu.Unlock()
		env.chowned = append(env.chowned, fmt.Sprintf("%s:%d:%d", filepath.Base(f.Name()), uid, gid))
		return nil
	}
	// Installed after the Manager's own Close cleanup, so it runs first:
	// every child is killed and joined before the Manager closes.
	env.toolOwner = jailerOwnTools(t, m)
	return m
}

// jailerToolOwner owns every tool child a test spawns: it captures each
// exec.Cmd through the command hook and counts the sole waiter's reaps
// through toolReaped. Its cleanup is installed before any spawn: it kills
// every captured child through its own os.Process (never a raw pid or a
// second Wait) and joins each reap, bounded, failing the test otherwise.
type jailerToolOwner struct {
	mu      sync.Mutex
	cmds    []*exec.Cmd
	reaps   chan jailerToolReap
	reaped  int
	handles []*processHandle
}

type jailerToolReap struct {
	id     string
	handle *processHandle
	err    error
}

func jailerOwnTools(t *testing.T, m *Manager) *jailerToolOwner {
	t.Helper()
	o := &jailerToolOwner{reaps: make(chan jailerToolReap, 64)}
	command := m.hooks.command
	m.hooks.command = func(cmd *exec.Cmd) {
		command(cmd)
		o.mu.Lock()
		o.cmds = append(o.cmds, cmd)
		o.mu.Unlock()
	}
	m.hooks.toolReaped = func(id string, h *processHandle, err error) {
		o.reaps <- jailerToolReap{id: id, handle: h, err: err}
	}
	t.Cleanup(func() {
		o.mu.Lock()
		spawned := 0
		for _, cmd := range o.cmds {
			if cmd.Process != nil {
				spawned++
				_ = cmd.Process.Kill()
			}
		}
		o.mu.Unlock()
		for o.reapedCount() < spawned {
			if _, ok := o.next(10 * time.Second); !ok {
				t.Errorf("a tool child was never reaped by its sole waiter (%d of %d)", o.reapedCount(), spawned)
				return
			}
		}
	})
	return o
}

func (o *jailerToolOwner) reapedCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.reaped
}

// next joins one reap notification, bounded.
func (o *jailerToolOwner) next(limit time.Duration) (jailerToolReap, bool) {
	select {
	case r := <-o.reaps:
		o.mu.Lock()
		o.reaped++
		o.handles = append(o.handles, r.handle)
		o.mu.Unlock()
		return r, true
	case <-time.After(limit):
		return jailerToolReap{}, false
	}
}

// forget drops a non-tool child (a VM the test reaps itself) from the
// owner's bookkeeping.
func (o *jailerToolOwner) forget(cmd *exec.Cmd) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.cmds = slices.DeleteFunc(o.cmds, func(c *exec.Cmd) bool { return c == cmd })
}

// spawned returns the captured children that started.
func (o *jailerToolOwner) spawned() []*exec.Cmd {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []*exec.Cmd
	for _, cmd := range o.cmds {
		if cmd.Process != nil {
			out = append(out, cmd)
		}
	}
	return out
}

// jailerToolEnvOnly is the env of a tool test, with no Manager open.
func jailerToolEnvOnly(t *testing.T, mode string) *jailerEnv {
	t.Helper()
	for _, bin := range []string{"/usr/bin/true", "/usr/bin/false", "/usr/bin/sleep"} {
		info, err := os.Stat(bin)
		if err != nil {
			t.Skipf("no %s: %v", bin, err)
		}
		if st, ok := info.Sys().(*syscall.Stat_t); !ok || st.Uid != 0 {
			t.Skipf("%s is not root-owned", bin)
		}
	}
	env := newJailerEnv(t)
	env.mode = mode
	return env
}

// jailerToolTarget creates a 0600 target file and opens it with flag.
func jailerToolTarget(t *testing.T, flag int) *os.File {
	t.Helper()
	p := filepath.Join(t.TempDir(), "disk.qcow2")
	if err := os.WriteFile(p, []byte("qcow2 stand-in"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(p, flag, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func jailerToolLimits() ToolLimits {
	return ToolLimits{CPUQuotaPercent: 100, MemoryBytes: 256 << 20, Pids: 64, IO: IOLimits{Device: BlockDevice{Major: 8}, ReadBPS: 1}}
}

// jailerToolSpec is a valid info run of binary on target.
func jailerToolSpec(binary string, target *os.File) ToolSpec {
	return ToolSpec{
		Attempt: jailerAttempt(), Binary: binary, Op: ToolInfo, Driver: "qcow2",
		Target: target, TargetRoot: TargetRootData, TargetPath: "vms/bx_11111111/disk.qcow2", TargetAccess: AccessGroupRO,
		Limits: jailerToolLimits(), Timeout: 10 * time.Second,
	}
}

func jailerInode(t *testing.T, f *os.File) (uint64, uint64) {
	t.Helper()
	id, err := fileInode(f)
	if err != nil {
		t.Fatal(err)
	}
	return id.Dev, id.Ino
}

// jailerAssertToolSettled requires the attempt released: no record, the
// reclaim run once with the tool's target fields, the target unheld.
func jailerAssertToolSettled(t *testing.T, env *jailerEnv, m *Manager, spec ToolSpec) {
	t.Helper()
	if _, ok := m.Record(ToolID(spec.Attempt)); ok {
		t.Fatal("a settled tool attempt kept its record")
	}
	dev, ino := jailerInode(t, spec.Target)
	if _, held := m.TargetHolder(dev, ino); held {
		t.Fatal("a settled tool attempt still holds its target")
	}
	var seen []Record
	for _, rec := range env.reclaims() {
		if rec.SandboxID == ToolID(spec.Attempt) {
			seen = append(seen, rec)
		}
	}
	if len(seen) != 1 {
		t.Fatalf("reclaim ran %d times for the tool attempt, want 1", len(seen))
	}
	rec := seen[0]
	if rec.Kind != KindTool || rec.TargetRoot != spec.TargetRoot || rec.TargetPath != spec.TargetPath ||
		rec.TargetAccess != spec.TargetAccess || rec.TargetDev != dev || rec.TargetIno != ino ||
		rec.UID < env.cfg.UIDs.Min || rec.UID > env.cfg.UIDs.Max {
		t.Fatalf("reclaimed tool record %+v", rec)
	}
}

func TestRunToolFastExitSucceeds(t *testing.T) {
	env, m := jailerToolEnv(t, jailerModeExec)
	spec := jailerToolSpec("/usr/bin/true", jailerToolTarget(t, os.O_RDONLY))
	res, err := m.RunTool(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if res.ExitCode != 0 || (res.Proof != ProofLiveExe && res.Proof != ProofExitedComm) {
		t.Fatalf("result %+v", res)
	}
	jailerAssertToolSettled(t, env, m, spec)
}

// TestRunToolCompletionBeforeProof: the tool has exited (observed by an
// event, its pidfd turning readable) before the exec proof runs; only the
// tool fast-exit proof can accept it, and it must.
func TestRunToolCompletionBeforeProof(t *testing.T) {
	env, m := jailerToolEnv(t, jailerModeExec)
	spec := jailerToolSpec("/usr/bin/true", jailerToolTarget(t, os.O_RDONLY))
	env.stages.on = map[launchStage]func(){stageExecProof: func() {
		pid := jailerRecordPID(t, env, ToolID(spec.Attempt))
		fd, err := unix.PidfdOpen(pid, 0)
		if err != nil {
			t.Errorf("pidfd_open(%d): %v", pid, err)
			return
		}
		defer unix.Close(fd)
		poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		if _, err := unix.Poll(poll, 10_000); err != nil || poll[0].Revents&unix.POLLIN == 0 {
			t.Errorf("tool never exited before its proof: %v", err)
		}
	}}
	res, err := m.RunTool(t.Context(), spec)
	if err != nil || res.ExitCode != 0 || res.Proof != ProofExitedComm {
		t.Fatalf("RunTool = %+v, %v; want the exited-comm proof", res, err)
	}
	jailerAssertToolSettled(t, env, m, spec)
}

// jailerRecordPID reads the committed pid of id from its durable record
// (the Manager lock is held by the stage hook's caller).
func jailerRecordPID(t *testing.T, env *jailerEnv, id string) int {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(env.cfg.StateDir, recordsDir, id+".json"))
	if err != nil {
		t.Fatal(err)
	}
	_, rest, ok := strings.Cut(string(raw), `"pid":`)
	if !ok {
		t.Fatalf("record %s has no pid", id)
	}
	end := strings.IndexAny(rest, ",}")
	pid, err := strconv.Atoi(rest[:end])
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func TestRunToolNonzeroExitIsFailure(t *testing.T) {
	env, m := jailerToolEnv(t, jailerModeExec)
	spec := jailerToolSpec("/usr/bin/false", jailerToolTarget(t, os.O_RDONLY))
	res, err := m.RunTool(t.Context(), spec)
	if err == nil || res.ExitCode != 1 || errors.Is(err, ErrToolRetained) {
		t.Fatalf("RunTool = %+v, %v; want a proven nonzero failure", res, err)
	}
	jailerAssertToolSettled(t, env, m, spec)
}

// TestRunToolPreExecFailures: every helper outcome before the exec is a
// failure, including a helper that exits 0 cleanly (its comm is not the
// staged tool's) and one that closes its status pipe but stays alive.
func TestRunToolPreExecFailures(t *testing.T) {
	for _, mode := range []string{jailerModeSetupError, jailerModeDie, jailerModeDieSignal, jailerModeExit0, jailerModeCloseAlive} {
		t.Run(mode, func(t *testing.T) {
			env, m := jailerToolEnv(t, mode)
			spec := jailerToolSpec("/usr/bin/true", jailerToolTarget(t, os.O_RDONLY))
			res, err := m.RunTool(t.Context(), spec)
			if err == nil {
				t.Fatalf("RunTool accepted %s: %+v", mode, res)
			}
			jailerAssertToolSettled(t, env, m, spec)
		})
	}
}

// TestRunToolRefusalReportsHelperExit: a refused fast exit names the
// observed exit status, for diagnosis; the refusal itself is unchanged.
func TestRunToolRefusalReportsHelperExit(t *testing.T) {
	env, m := jailerToolEnv(t, jailerModeDie)
	spec := jailerToolSpec("/usr/bin/true", jailerToolTarget(t, os.O_RDONLY))
	_, err := m.RunTool(t.Context(), spec)
	if err == nil {
		t.Fatal("a pre-exec helper exit was accepted")
	}
	if !strings.Contains(err.Error(), "exit status 3") {
		t.Fatalf("refusal %q does not name the observed exit status 3", err)
	}
	jailerAssertToolSettled(t, env, m, spec)
}

func TestRunToolTimeoutKillsAndReaps(t *testing.T) {
	env, m := jailerToolEnv(t, jailerModeExec)
	spec := jailerToolSpec("/usr/bin/sleep", jailerToolTarget(t, os.O_RDONLY))
	spec.Timeout = 200 * time.Millisecond
	if _, err := m.RunTool(t.Context(), spec); err == nil || errors.Is(err, ErrToolRetained) {
		t.Fatalf("RunTool = %v; want a proven timeout failure", err)
	}
	jailerAssertToolSettled(t, env, m, spec)
}

// TestRunToolUnprovenReapRetains: a tool whose kill never lands keeps its
// record, lease and uid; every conflicting run on that inode is refused.
// The test then kills the child through its own os.Process, joins the one
// sole waiter (whose private handle must be closed) and settles the attempt
// with exactly one SettleTools.
func TestRunToolUnprovenReapRetains(t *testing.T) {
	env, m := jailerToolEnv(t, jailerModeExec)
	owner := env.toolOwner
	m.hooks.kill = func(*processHandle) error { return nil } // set before any spawn, never changed
	target := jailerToolTarget(t, os.O_RDONLY)
	spec := jailerToolSpec("/usr/bin/sleep", target)
	spec.Timeout = 200 * time.Millisecond
	_, err := m.RunTool(t.Context(), spec)
	if !errors.Is(err, ErrToolRetained) {
		t.Fatalf("RunTool = %v, want ErrToolRetained", err)
	}
	rec, ok := m.Record(ToolID(spec.Attempt))
	if !ok {
		t.Fatal("an unproven tool lost its record")
	}
	dev, ino := jailerInode(t, target)
	if holder, held := m.TargetHolder(dev, ino); !held || holder != rec.Ref() {
		t.Fatalf("TargetHolder = %v, %v; want the retained attempt", holder, held)
	}
	if _, err := m.RunTool(t.Context(), jailerToolSpec("/usr/bin/true", target)); !errors.Is(err, ErrTargetBusy) {
		t.Fatalf("second run = %v, want ErrTargetBusy", err)
	}
	if len(env.reclaims()) != 0 {
		t.Fatal("an unproven tool was reclaimed")
	}
	children := owner.spawned()
	if len(children) != 1 {
		t.Fatalf("%d tool children spawned, want 1 (the busy run must not spawn)", len(children))
	}
	if err := children[0].Process.Kill(); err != nil {
		t.Fatalf("owned kill: %v", err)
	}
	reap, ok := owner.next(10 * time.Second)
	if !ok || reap.id != rec.SandboxID {
		t.Fatalf("the sole waiter did not report the reap of %s (%+v)", rec.SandboxID, reap)
	}
	reap.handle.mu.Lock()
	closed := reap.handle.closed
	reap.handle.mu.Unlock()
	if !closed {
		t.Fatal("the sole waiter left the private handle open")
	}
	report, err := m.SettleTools(t.Context())
	if err != nil || !slices.Equal(report.Settled, []Ref{rec.Ref()}) {
		t.Fatalf("SettleTools = %+v, %v; want the reaped attempt settled", report, err)
	}
	jailerAssertToolSettled(t, env, m, spec)
}

func TestRunToolCanceledCreatesNothing(t *testing.T) {
	env, m := jailerToolEnv(t, jailerModeExec)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	spec := jailerToolSpec("/usr/bin/true", jailerToolTarget(t, os.O_RDONLY))
	if _, err := m.RunTool(ctx, spec); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunTool = %v, want Canceled", err)
	}
	if len(m.Records()) != 0 || len(env.reclaims()) != 0 {
		t.Fatal("a canceled run created state")
	}
}

// TestRunToolMissingPidfdFailsClosed: a clone without a pidfd is never a
// fast success; the child stays owned and is reaped.
func TestRunToolMissingPidfdFailsClosed(t *testing.T) {
	env, m := jailerToolEnv(t, jailerModeExec)
	command := m.hooks.command
	m.hooks.command = func(cmd *exec.Cmd) {
		command(cmd)
		cmd.SysProcAttr.PidFD = nil
	}
	m.hooks.pidfdOpen = func(int) (int, error) { return -1, unix.ENOSYS }
	spec := jailerToolSpec("/usr/bin/true", jailerToolTarget(t, os.O_RDONLY))
	if _, err := m.RunTool(t.Context(), spec); err == nil {
		t.Fatalf("RunTool = %v; want a fail-closed refusal", err)
	}
	jailerAssertToolSettled(t, env, m, spec)
}

func TestToolSpecValidate(t *testing.T) {
	rd := jailerToolTarget(t, os.O_RDONLY)
	rw := jailerToolTarget(t, os.O_RDWR)
	roBase := jailerToolTarget(t, os.O_RDONLY) // a valid read-only base fd
	valid := map[string]ToolSpec{
		"info group-ro": jailerToolSpec("/usr/bin/qemu-img", rd),
		"check shared": func() ToolSpec {
			s := jailerToolSpec("/usr/bin/qemu-img", rd)
			s.Op, s.TargetAccess, s.TargetRoot, s.TargetPath = ToolCheck, AccessShared, TargetRootImages, "v1/root.qcow2"
			return s
		}(),
		"resize owner": func() ToolSpec {
			s := jailerToolSpec("/usr/bin/qemu-img", rw)
			s.Op, s.TargetAccess, s.Size = ToolResize, AccessOwnerRW, 20<<30
			return s
		}(),
		"create owner": func() ToolSpec {
			s := jailerToolSpec("/usr/bin/qemu-img", rw)
			s.Op, s.TargetAccess, s.Size, s.BackingName = ToolCreate, AccessOwnerRW, 10<<30, "/var/lib/bbraft/images/v1/root.qcow2"
			return s
		}(),
		"info raw": func() ToolSpec { s := jailerToolSpec("/usr/bin/qemu-img", rd); s.Driver = "raw"; return s }(),
	}
	for name, spec := range valid {
		t.Run("valid/"+name, func(t *testing.T) {
			if err := spec.Validate(); err != nil {
				t.Fatalf("Validate = %v", err)
			}
		})
	}
	invalid := map[string]func(*ToolSpec){
		"info owner-rw":     func(s *ToolSpec) { s.TargetAccess = AccessOwnerRW },
		"resize group-ro":   func(s *ToolSpec) { s.Op, s.Size = ToolResize, 1<<30 },
		"create shared":     func(s *ToolSpec) { s.Op, s.TargetAccess, s.Size, s.BackingName = ToolCreate, AccessShared, 1<<30, "b" },
		"resize zero size":  func(s *ToolSpec) { s.Op, s.TargetAccess, s.Target = ToolResize, AccessOwnerRW, rw },
		"create no backing": func(s *ToolSpec) { s.Op, s.TargetAccess, s.Target, s.Size = ToolCreate, AccessOwnerRW, rw, 1<<30 },
		"resize raw": func(s *ToolSpec) {
			s.Op, s.TargetAccess, s.Target, s.Size, s.Driver = ToolResize, AccessOwnerRW, rw, 1<<30, "raw"
		},
		"unknown driver":   func(s *ToolSpec) { s.Driver = "vmdk" },
		"unknown op":       func(s *ToolSpec) { s.Op = "convert" },
		"unknown root":     func(s *ToolSpec) { s.TargetRoot = "etc" },
		"unknown access":   func(s *ToolSpec) { s.TargetAccess = "rw" },
		"dotdot path":      func(s *ToolSpec) { s.TargetPath = "vms/../../etc/shadow" },
		"absolute path":    func(s *ToolSpec) { s.TargetPath = "/etc/shadow" },
		"empty path":       func(s *ToolSpec) { s.TargetPath = "" },
		"unclean path":     func(s *ToolSpec) { s.TargetPath = "vms//x" },
		"relative binary":  func(s *ToolSpec) { s.Binary = "qemu-img" },
		"invalid attempt":  func(s *ToolSpec) { s.Attempt = "x" },
		"nil target":       func(s *ToolSpec) { s.Target = nil },
		"rw fd for info":   func(s *ToolSpec) { s.Target = rw },
		"ro fd for resize": func(s *ToolSpec) { s.Op, s.TargetAccess, s.Size = ToolResize, AccessOwnerRW, 1<<30 },
		"timeout zero":     func(s *ToolSpec) { s.Timeout = 0 },
		"timeout too long": func(s *ToolSpec) { s.Timeout = MaxToolTimeout + time.Second },
		"no memory limit":  func(s *ToolSpec) { s.Limits.MemoryBytes = 0 },
		"backing with comma": func(s *ToolSpec) {
			s.Op, s.TargetAccess, s.Target, s.Size, s.BackingName = ToolCreate, AccessOwnerRW, rw, 1<<30, "a,b"
		},
		"backing on non-create": func(s *ToolSpec) { s.BackingName = "x" },
		"info with a base":      func(s *ToolSpec) { s.Base = roBase },
	}
	for name, edit := range invalid {
		t.Run("invalid/"+name, func(t *testing.T) {
			spec := jailerToolSpec("/usr/bin/qemu-img", rd)
			edit(&spec)
			if err := spec.Validate(); err == nil {
				t.Fatal("Validate accepted it")
			}
		})
	}
}

func TestToolArgsAreFixed(t *testing.T) {
	const fd3 = "file.driver=file,file.filename=/proc/self/fd/3"
	const graph = ",backing.driver=qcow2,backing.file.driver=file,backing.file.filename=/proc/self/fd/4,backing.read-only=on"
	rd := jailerToolTarget(t, os.O_RDONLY)
	rw := jailerToolTarget(t, os.O_RDWR)
	base := jailerToolTarget(t, os.O_RDONLY)
	spec := func(op ToolOp, edit func(*ToolSpec)) ToolSpec {
		s := jailerToolSpec("/usr/bin/qemu-img", rd)
		s.Op = op
		if edit != nil {
			edit(&s)
		}
		return s
	}
	cases := map[string]struct {
		spec ToolSpec
		want []string
	}{
		"info standalone": {spec(ToolInfo, nil), []string{"info", "--output=json", "--image-opts", "driver=qcow2," + fd3}},
		// info opens no backing (qemu-img's fixed NO_BACKING for info): the
		// graph names the target alone, with no base fd.
		"info overlay":  {spec(ToolInfo, nil), []string{"info", "--output=json", "--image-opts", "driver=qcow2," + fd3}},
		"check overlay": {spec(ToolCheck, func(s *ToolSpec) { s.Base = base }), []string{"check", "--output=json", "--image-opts", "driver=qcow2," + fd3 + graph}},
		"info raw":      {spec(ToolInfo, func(s *ToolSpec) { s.Driver = "raw" }), []string{"info", "--output=json", "--image-opts", "driver=raw," + fd3}},
		"resize overlay": {spec(ToolResize, func(s *ToolSpec) {
			s.Target, s.TargetAccess, s.Size, s.Base = rw, AccessOwnerRW, 21474836480, base
		}), []string{"resize", "--image-opts", "driver=qcow2," + fd3 + graph, "21474836480"}},
		"create": {spec(ToolCreate, func(s *ToolSpec) {
			s.Target, s.TargetAccess, s.Size, s.BackingName = rw, AccessOwnerRW, 10737418240, "/var/lib/bbraft/images/v1/root.qcow2"
		}), []string{"create", "-f", "qcow2", "-u", "-F", "qcow2", "-b", "/var/lib/bbraft/images/v1/root.qcow2", "/proc/self/fd/3", "10737418240"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := toolArgs(tc.spec)
			if err != nil || !slices.Equal(got, tc.want) {
				t.Fatalf("toolArgs = %q, %v\nwant %q", got, err, tc.want)
			}
		})
	}
}

// TestToolGraphInfoNamesTargetOnly pins the graph below Validate: even a
// spec carrying a base gets no backing.* for info (qemu-img info opens no
// backing and refuses leftover backing options).
func TestToolGraphInfoNamesTargetOnly(t *testing.T) {
	s := jailerToolSpec("/usr/bin/qemu-img", jailerToolTarget(t, os.O_RDONLY))
	s.Op, s.Base = ToolInfo, jailerToolTarget(t, os.O_RDONLY)
	if got, want := toolGraph(s), "driver=qcow2,file.driver=file,file.filename=/proc/self/fd/3"; got != want {
		t.Fatalf("toolGraph(info with a base) = %q, want %q", got, want)
	}
}

func TestTargetConflictMatrix(t *testing.T) {
	all := []TargetAccess{AccessShared, AccessGroupRO, AccessOwnerRW}
	for _, held := range all {
		for _, want := range all {
			wantConflict := !(held == AccessShared && want == AccessShared)
			if got := targetsConflict(held, want); got != wantConflict {
				t.Errorf("targetsConflict(%s, %s) = %v, want %v", held, want, got, wantConflict)
			}
		}
	}
}
