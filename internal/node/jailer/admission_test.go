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
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// A10 slice4: the reserved helper exit contract and functional admission
// with an attempt-owned journal, cgroup and scratch directory.

// Fake admission helper modes (BBRAFT_JAILER_FAKE_HELPER with
// AdmitHelperArg): the real one needs root for namespaces and mounts.
const (
	// jailerAdmitOK creates the private null stand-in (a FIFO: mknod of a
	// character device needs CAP_MKNOD), reports it and exits 0.
	jailerAdmitOK   = "admit-ok"
	jailerAdmitFail = "admit-fail"
	jailerAdmitHang = "admit-hang"
	// jailerAdmitSilent exits 0 without any report.
	jailerAdmitSilent = "admit-silent"
	// jailerAdmitTruncated exits 0 with a truncated report.
	jailerAdmitTruncated = "admit-truncated"
)

// jailerFakeAdmit follows the admission helper's fd protocol: config,
// release and status descriptors; it never mounts or mknods.
func jailerFakeAdmit() int {
	mode := os.Getenv(jailerFakeModeEnv)
	if len(os.Args) != 5 {
		return HelperExitCode
	}
	var fds [3]int
	for i := range fds {
		fd, err := strconv.Atoi(os.Args[i+2])
		if err != nil {
			return HelperExitCode
		}
		fds[i] = fd
	}
	unix.CloseOnExec(fds[2])
	status := os.NewFile(uintptr(fds[2]), "status")
	var cfg admitHelperConfig
	if err := json.UnmarshalRead(io.LimitReader(os.NewFile(uintptr(fds[0]), "config"), 1<<16), &cfg); err != nil {
		return HelperExitCode
	}
	var gate [1]byte
	if n, err := os.NewFile(uintptr(fds[1]), "release").Read(gate[:]); n != 1 || err != nil {
		return HelperExitCode
	}
	switch mode {
	case jailerAdmitOK:
		null := filepath.Join(cfg.Scratch, admissionNullName)
		if err := unix.Mkfifo(null, 0o600); err != nil {
			status.Write([]byte(`{"ok":false,"error":"mkfifo"}`))
			return HelperExitCode
		}
		info, err := os.Lstat(null)
		if err != nil {
			return HelperExitCode
		}
		st := info.Sys().(*syscall.Stat_t)
		raw, _ := json.Marshal(admitReport{OK: true, Null: inodeIdentity{Dev: uint64(st.Dev), Ino: st.Ino}})
		if _, err := status.Write(raw); err != nil {
			return HelperExitCode
		}
		return 0
	case jailerAdmitFail:
		status.Write([]byte(`{"ok":false,"error":"injected admission failure"}`))
		return HelperExitCode
	case jailerAdmitSilent:
		return 0
	case jailerAdmitTruncated:
		status.Write([]byte(`{"ok":tr`))
		return 0
	case jailerAdmitHang:
		for {
			unix.Pause()
		}
	}
	return HelperExitCode
}

// TestHelperExitsReservedCodeBeforeExec: every handled pre-exec return of
// the helper, whatever its cause, exits HelperExitCode.
func TestHelperExitsReservedCodeBeforeExec(t *testing.T) {
	config, err := helperConfigFile(helperConfig{
		Root: "/nonexistent", Binary: "/usr/bin/true", UID: 1, ControlFD: 9, LogFD: 10,
		Control: inodeIdentity{Dev: 1, Ino: 1}, Log: inodeIdentity{Dev: 1, Ino: 2},
		KVMRdev: 1, RandomRdev: 1, NullRdev: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer config.Close()
	// jailerPipe returns both ends of a pipe, each closed at cleanup.
	jailerPipe := func(t *testing.T) (r, w *os.File) {
		t.Helper()
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r.Close(); w.Close() })
		return r, w
	}
	cases := map[string]func(*testing.T, *exec.Cmd){
		"bad descriptor arguments": func(_ *testing.T, cmd *exec.Cmd) {
			cmd.Args = []string{"/proc/self/exe", HelperArg, "x", "y", "z"}
		},
		"release gate closed without the byte": func(t *testing.T, cmd *exec.Cmd) {
			release, releaseW := jailerPipe(t)
			releaseW.Close()
			_, statusW := jailerPipe(t)
			cmd.ExtraFiles = []*os.File{config, release, statusW}
			cmd.Args = []string{"/proc/self/exe", HelperArg, "3", "4", "5"}
		},
		"undecodable config": func(t *testing.T, cmd *exec.Cmd) {
			bad, badW := jailerPipe(t)
			badW.Write([]byte("{not json"))
			badW.Close()
			_, statusW := jailerPipe(t)
			cmd.ExtraFiles = []*os.File{bad, bad, statusW}
			cmd.Args = []string{"/proc/self/exe", HelperArg, "3", "4", "5"}
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command("/proc/self/exe")
			cmd.Env = []string{}
			setup(t, cmd)
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			var err error
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				_ = cmd.Process.Kill()
				<-done
				t.Fatal("the helper did not exit")
			}
			exitErr, ok := errors.AsType[*exec.ExitError](err)
			if !ok || exitErr.ExitCode() != HelperExitCode {
				t.Fatalf("helper exit = %v, want %d", err, HelperExitCode)
			}
		})
	}
}

// jailerAdmitEnv opens a Manager whose admission helper is the fake mode.
func jailerAdmitEnv(t *testing.T, mode string) (*jailerEnv, *Manager) {
	t.Helper()
	env := newJailerEnv(t)
	env.mode = mode
	m := env.open(t)
	t.Cleanup(func() { m.Close() })
	jailerAdmitHooks(m)
	return env, m
}

// jailerAdmitHooks installs the unprivileged admission stand-ins: a short
// setup bound, the FIFO null node and fresh namespaces reported for the
// helper (the fake cannot unshare).
func jailerAdmitHooks(m *Manager) {
	m.hooks.admitTimeout = 2 * time.Second
	m.hooks.admitNull = func(info fs.FileInfo) bool { return info.Mode().Type() == fs.ModeNamedPipe }
	m.hooks.proc = jailerAdmitFreshNS{}
}

// jailerAdmitReopen closes m and opens the env again, as a node restart.
func jailerAdmitReopen(t *testing.T, env *jailerEnv, m *Manager) *Manager {
	t.Helper()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	next := env.open(t)
	t.Cleanup(func() { next.Close() })
	jailerAdmitHooks(next)
	return next
}

// jailerAdmitFreshNS reports a distinct namespace for every pid but self.
type jailerAdmitFreshNS struct{}

func (jailerAdmitFreshNS) ReadFile(pid, name string) ([]byte, error) {
	return hostProcFS{}.ReadFile(pid, name)
}

func (jailerAdmitFreshNS) Stat(pid, name string) (inodeIdentity, error) {
	id, err := hostProcFS{}.Stat(pid, name)
	if err != nil || pid == "self" || !strings.HasPrefix(name, "ns/") {
		return id, err
	}
	n, _ := strconv.Atoi(pid)
	return inodeIdentity{Dev: id.Dev, Ino: id.Ino + 1<<40 + uint64(n)}, nil
}

func jailerAdmitSpec() AdmitSpec {
	return AdmitSpec{Attempt: jailerAttempt(), Limits: Limits{
		CPUQuotaPercent: 100, MemoryBytes: 64 << 20, Pids: 16,
		IO: IOLimits{Device: BlockDevice{Major: 8}, ReadBPS: 1},
	}}
}

func jailerAdmitLeft(t *testing.T, env *jailerEnv, attempt string) (journal, scratch, cgroup bool) {
	t.Helper()
	dir := filepath.Join(env.cfg.StateDir, admissionDir)
	journal = jailerExists(t, dir, attempt+".json")
	scratch = jailerExists(t, dir, attempt+".d")
	cgroup = env.cgroups.has(admissionID(attempt))
	return
}

func TestAdmitSuccessLeavesNothing(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitOK)
	var spawned *exec.Cmd
	command := m.hooks.command
	m.hooks.command = func(cmd *exec.Cmd) {
		attrs := *cmd.SysProcAttr
		spawned = &exec.Cmd{Args: slices.Clone(cmd.Args), SysProcAttr: &attrs}
		command(cmd)
	}
	spec := jailerAdmitSpec()
	report, err := m.Admit(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Removed) != 0 || len(report.Preserved) != 0 {
		t.Fatalf("report %+v on a clean state", report)
	}
	if j, s, c := jailerAdmitLeft(t, env, spec.Attempt); j || s || c {
		t.Fatalf("a successful admission left journal %v scratch %v cgroup %v", j, s, c)
	}
	if spawned == nil || len(spawned.Args) < 2 || spawned.Args[1] != AdmitHelperArg {
		t.Fatalf("admission helper argv %v", spawned)
	}
	flags := spawned.SysProcAttr.Cloneflags
	if flags&(unix.CLONE_NEWNS|unix.CLONE_NEWPID|unix.CLONE_NEWIPC) != unix.CLONE_NEWNS|unix.CLONE_NEWPID|unix.CLONE_NEWIPC ||
		!spawned.SysProcAttr.UseCgroupFD || spawned.SysProcAttr.PidFD == nil {
		t.Fatalf("admission helper is not born in fresh namespaces inside its cgroup with a pidfd: %+v", spawned.SysProcAttr)
	}
}

func TestAdmitHelperFailureIsFatal(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitFail)
	spec := jailerAdmitSpec()
	if _, err := m.Admit(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "injected admission failure") {
		t.Fatalf("Admit = %v, want the helper's failure", err)
	}
	// The helper is proven dead and its group empty: the attempt cleans up.
	if j, s, c := jailerAdmitLeft(t, env, spec.Attempt); j || s || c {
		t.Fatalf("a settled failed admission left journal %v scratch %v cgroup %v", j, s, c)
	}
}

func TestAdmitTimeoutKillsAndReapsOnce(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitHang)
	m.hooks.admitTimeout = 200 * time.Millisecond
	spec := jailerAdmitSpec()
	if _, err := m.Admit(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("Admit = %v, want a timeout", err)
	}
	if j, s, c := jailerAdmitLeft(t, env, spec.Attempt); j || s || c {
		t.Fatalf("a killed and reaped admission left journal %v scratch %v cgroup %v", j, s, c)
	}
}

func TestAdmitUnprovenReapRetainsEverything(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitHang)
	m.hooks.admitTimeout = 200 * time.Millisecond
	// The test owns the helper through its exec.Cmd (never a raw pid) and
	// joins the production sole waiter through admitReaped.
	var spawned *exec.Cmd
	command := m.hooks.command
	m.hooks.command = func(cmd *exec.Cmd) {
		command(cmd)
		spawned = cmd
	}
	reaped := make(chan *processHandle, 1)
	m.hooks.admitReaped = func(h *processHandle) { reaped <- h }
	var joined bool
	join := func() *processHandle {
		if joined || spawned == nil || spawned.Process == nil {
			return nil
		}
		joined = true
		_ = spawned.Process.Kill()
		select {
		case h := <-reaped:
			return h
		case <-time.After(10 * time.Second):
			t.Error("the admission helper's sole waiter never observed its exit")
			return nil
		}
	}
	t.Cleanup(func() { join() })
	var killed int
	m.hooks.kill = func(*processHandle) error {
		killed++
		return nil // the kill never lands: death stays unproven
	}
	spec := jailerAdmitSpec()
	if _, err := m.Admit(t.Context(), spec); err == nil {
		t.Fatal("Admit succeeded with a hung helper")
	}
	if killed == 0 {
		t.Fatal("the hung helper was never signaled")
	}
	if j, s, c := jailerAdmitLeft(t, env, spec.Attempt); !j || !s || !c {
		t.Fatalf("an unproven admission dropped journal %v scratch %v cgroup %v", j, s, c)
	}
	dir := filepath.Join(env.cfg.StateDir, admissionDir)
	before := jailerAdmitTree(t, dir)
	h := join()
	if h == nil {
		t.Fatal("no reaped handle")
	}
	h.mu.Lock()
	closed, fd := h.closed, h.fd
	h.mu.Unlock()
	if !closed || fd != -1 {
		t.Fatalf("the late reap left the process handle open (closed %v fd %d)", closed, fd)
	}
	if after := jailerAdmitTree(t, dir); !maps.Equal(before, after) {
		t.Fatalf("the late reap mutated retained state\nbefore %v\nafter  %v", before, after)
	}
	if !env.cgroups.has(admissionID(spec.Attempt)) {
		t.Fatal("the late reap removed the retained cgroup")
	}
}

// jailerAdmitTree maps each path under dir to its inode, type and content.
func jailerAdmitTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Lstat(p)
		if err != nil {
			return err
		}
		st := info.Sys().(*syscall.Stat_t)
		v := fmt.Sprintf("%d/%d %v", st.Dev, st.Ino, info.Mode())
		if info.Mode().IsRegular() {
			raw, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			v += " " + string(raw)
		}
		out[p] = v
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAdmitCanceledChangesNothing(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitOK)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	spec := jailerAdmitSpec()
	if _, err := m.Admit(ctx, spec); !errors.Is(err, context.Canceled) {
		t.Fatalf("Admit = %v, want Canceled", err)
	}
	if j, s, c := jailerAdmitLeft(t, env, spec.Attempt); j || s || c {
		t.Fatal("a canceled admission created state")
	}
}

func TestAdmitRefusesInvalidAttempt(t *testing.T) {
	_, m := jailerAdmitEnv(t, jailerAdmitOK)
	spec := jailerAdmitSpec()
	spec.Attempt = "../escape"
	if _, err := m.Admit(t.Context(), spec); err == nil {
		t.Fatal("Admit accepted an invalid attempt")
	}
}

// jailerStaleAdmission journals an earlier attempt with real scratch and a
// fake cgroup, as a crash after setup would leave them.
func jailerStaleAdmission(t *testing.T, env *jailerEnv, attempt string, livePID int) {
	t.Helper()
	jailerStaleAdmissionWith(t, env, attempt, jailerStaleOpts{livePID: livePID})
}

// jailerStaleOpts shapes a stale admission fixture.
type jailerStaleOpts struct {
	livePID int
	// null adds the child-created null stand-in (a FIFO) and journals it
	// with a dead helper identity from another boot.
	null bool
	// before runs inside the scratch directory before mnt is created, so
	// its entries come after mnt in tmpfs directory order.
	before func(scratch string)
}

// jailerStaleAdmissionWith journals an earlier attempt and returns its
// journal path.
func jailerStaleAdmissionWith(t *testing.T, env *jailerEnv, attempt string, opts jailerStaleOpts) string {
	t.Helper()
	dir := filepath.Join(env.cfg.StateDir, admissionDir)
	scratch := filepath.Join(dir, attempt+".d")
	if err := os.MkdirAll(scratch, 0o700); err != nil {
		t.Fatal(err)
	}
	if opts.before != nil {
		opts.before(scratch)
	}
	if err := os.Mkdir(filepath.Join(scratch, "mnt"), 0o700); err != nil {
		t.Fatal(err)
	}
	stat := func(p string) inodeIdentity {
		info, err := os.Lstat(p)
		if err != nil {
			t.Fatal(err)
		}
		st := info.Sys().(*syscall.Stat_t)
		return inodeIdentity{Dev: uint64(st.Dev), Ino: st.Ino}
	}
	cgroup, err := env.cgroups.Prepare(admissionID(attempt), jailerAdmitSpec().Limits)
	if err != nil {
		t.Fatal(err)
	}
	if opts.livePID != 0 {
		env.cgroups.mu.Lock()
		env.cgroups.groups[admissionID(attempt)].pids = []int{opts.livePID}
		env.cgroups.mu.Unlock()
	}
	boot, _ := hostBootID()
	j := admissionJournal{
		SchemaVersion: 1, Kind: AdmissionKind, Attempt: attempt, BootID: boot,
		Cgroup: cgroup, Scratch: stat(scratch), Mount: stat(filepath.Join(scratch, "mnt")),
	}
	if opts.null {
		null := filepath.Join(scratch, admissionNullName)
		if err := unix.Mkfifo(null, 0o600); err != nil {
			t.Fatal(err)
		}
		j.Null = stat(null)
		j.BootID = "00000000-0000-4000-8000-000000000000"
		j.PID, j.StartTicks = 4194000, 1
	}
	raw, err := json.Marshal(j)
	if err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(dir, attempt+".json")
	if err := os.WriteFile(journal, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return journal
}

func TestAdmitCleansProvenStaleJournal(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitOK)
	stale := jailerAttempt()
	jailerStaleAdmission(t, env, stale, 0)
	report, err := m.Admit(t.Context(), jailerAdmitSpec())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(report.Removed, stale) {
		t.Fatalf("report %+v does not name the removed stale attempt", report)
	}
	if j, s, c := jailerAdmitLeft(t, env, stale); j || s || c {
		t.Fatalf("stale journaled attempt left journal %v scratch %v cgroup %v", j, s, c)
	}
}

func TestAdmitPreservesUnprovenLeftovers(t *testing.T) {
	env, m := jailerAdmitEnv(t, jailerAdmitOK)
	dir := filepath.Join(env.cfg.StateDir, admissionDir)
	orphan := jailerAttempt()
	if err := os.MkdirAll(filepath.Join(dir, orphan+".d"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := env.cgroups.Prepare(admissionID(jailerAttempt()), jailerAdmitSpec().Limits); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(dir, "operator-note")
	if err := os.WriteFile(foreign, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	report, err := m.Admit(t.Context(), jailerAdmitSpec())
	if err != nil {
		t.Fatalf("unproven but empty leftovers blocked admission: %v", err)
	}
	if len(report.Preserved) < 3 || len(report.Removed) != 0 {
		t.Fatalf("report %+v, want the scratch dir, cgroup and foreign file preserved", report)
	}
	for _, p := range []string{filepath.Join(dir, orphan+".d"), foreign} {
		if _, err := os.Lstat(p); err != nil {
			t.Fatalf("unproven leftover %s removed: %v", p, err)
		}
	}
}

func TestAdmitStaleJournalWithLiveGroupIsFatal(t *testing.T) {
	jailerNeedBinary(t)
	env, m := jailerAdmitEnv(t, jailerAdmitOK)
	live := exec.Command(jailerTestBinary, "60")
	if err := live.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = live.Process.Kill(); _ = live.Wait() })
	stale := jailerAttempt()
	jailerStaleAdmission(t, env, stale, live.Process.Pid)
	if _, err := m.Admit(t.Context(), jailerAdmitSpec()); err == nil {
		t.Fatal("Admit proceeded past a stale admission group that still holds a process")
	}
	if j, s, c := jailerAdmitLeft(t, env, stale); !j || !s || !c {
		t.Fatal("a stale attempt with a live process was cleaned")
	}
	if syscall.Kill(live.Process.Pid, 0) != nil {
		t.Fatal("the stale process was killed")
	}
}
