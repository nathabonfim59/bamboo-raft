package jailer

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// A10 D1 review controls: terminal cancellation at each phase, the
// helper/staged comm collision, and the real ownership index shared by
// RunTool and Start.

func TestRunToolCancellationPhases(t *testing.T) {
	cases := map[string]func(env *jailerEnv, cancel context.CancelFunc){
		// Before the spawn: nothing is spawned and the attempt rolls back.
		"during setup": func(env *jailerEnv, cancel context.CancelFunc) {
			env.stages.on = map[launchStage]func(){stageCgroup: cancel}
		},
		// After the clean status EOF, before the proof.
		"after status EOF": func(env *jailerEnv, cancel context.CancelFunc) {
			env.stages.on = map[launchStage]func(){stageExecProof: cancel}
		},
		// During the final reclaim of a run that completed.
		"during final reclaim": func(env *jailerEnv, cancel context.CancelFunc) {
			env.onReclaim = func(context.Context, Record) error { cancel(); return nil }
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			env, m := jailerToolEnv(t, jailerModeExec)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			setup(env, cancel)
			spec := jailerToolSpec("/usr/bin/true", jailerToolTarget(t, os.O_RDONLY))
			res, err := m.RunTool(ctx, spec)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("RunTool = %+v, %v; want Canceled, never a nil success", res, err)
			}
			// Honest cleanup: the attempt is settled (no record, no lease).
			if _, ok := m.Record(ToolID(spec.Attempt)); ok {
				t.Fatal("a canceled, proven attempt kept its record")
			}
			dev, ino := jailerInode(t, spec.Target)
			if _, held := m.TargetHolder(dev, ino); held {
				t.Fatal("a canceled, proven attempt still holds its target")
			}
		})
	}
	t.Run("after success", func(t *testing.T) {
		_, m := jailerToolEnv(t, jailerModeExec)
		ctx, cancel := context.WithCancel(t.Context())
		spec := jailerToolSpec("/usr/bin/true", jailerToolTarget(t, os.O_RDONLY))
		res, err := m.RunTool(ctx, spec)
		cancel()
		if err != nil || res.ExitCode != 0 {
			t.Fatalf("RunTool = %+v, %v", res, err)
		}
		if _, ok := m.Record(ToolID(spec.Attempt)); ok {
			t.Fatal("a later cancellation resurrected the record")
		}
	})
}

func TestToolCommTruncationAndCollision(t *testing.T) {
	for binary, want := range map[string]string{
		"/usr/bin/qemu-img":                 "qemu-img",
		"/usr/bin/true":                     "true",
		"/opt/q/qemu-img-wrapper-long-name": "qemu-img-wrappe",
	} {
		if got := toolComm(binary); got != want {
			t.Errorf("toolComm(%q) = %q, want %q", binary, got, want)
		}
	}
	cases := []struct {
		helper, binary string
		ambiguous      bool
	}{
		{"jailer.test", "/usr/bin/true", false},
		{"bbraft-node", "/usr/bin/qemu-img", false},
		{"true", "/usr/bin/true", true},
		// Equal only after the kernel's 15-byte truncation.
		{"qemu-img-wrappe", "/opt/q/qemu-img-wrapper-long-name", true},
		{"", "/usr/bin/true", true}, // an unknown helper comm is never distinct
	}
	for _, tc := range cases {
		if got := toolCommAmbiguous(tc.helper, tc.binary); got != tc.ambiguous {
			t.Errorf("toolCommAmbiguous(%q, %q) = %v, want %v", tc.helper, tc.binary, got, tc.ambiguous)
		}
	}
}

// TestRunToolAmbiguousCommRefusesFastExit: when the helper's captured comm
// equals the staged tool's, an exit observed before the live proof cannot
// be told from a pre-exec helper exit, so the run is refused and settled.
func TestRunToolAmbiguousCommRefusesFastExit(t *testing.T) {
	env, m := jailerToolEnv(t, jailerModeExec)
	var captured []string
	m.hooks.toolHelperComm = func(c string) string {
		captured = append(captured, c)
		return "true"
	}
	spec := jailerToolSpec("/usr/bin/true", jailerToolTarget(t, os.O_RDONLY))
	env.stages.on = map[launchStage]func(){stageExecProof: func() {
		pid := jailerRecordPID(t, env, ToolID(spec.Attempt))
		fd, err := unix.PidfdOpen(pid, 0)
		if err != nil {
			t.Errorf("pidfd_open: %v", err)
			return
		}
		defer unix.Close(fd)
		poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		if _, err := unix.Poll(poll, 10_000); err != nil {
			t.Errorf("poll: %v", err)
		}
	}}
	res, err := m.RunTool(t.Context(), spec)
	if err == nil {
		t.Fatalf("RunTool accepted an ambiguous fast exit: %+v", res)
	}
	if len(captured) != 1 || captured[0] == "" || captured[0] == "true" {
		t.Fatalf("helper comm captured %q; want the helper's own comm, once, before release", captured)
	}
	jailerAssertToolSettled(t, env, m, spec)
}

// jailerToolHold runs a long tool on target with access in the background
// and returns once it holds the inode; stop kills it through its own
// os.Process and joins the run.
func jailerToolHold(t *testing.T, env *jailerEnv, m *Manager, target *os.File, access TargetAccess) (stop func()) {
	t.Helper()
	spec := jailerToolSpec("/usr/bin/sleep", target)
	jailerToolAccess(&spec, access)
	spec.Timeout = 30 * time.Second
	ready := make(chan struct{})
	var once sync.Once
	previous := env.stages.on
	env.stages.mu.Lock()
	env.stages.on = map[launchStage]func(){stageExecProof: func() { once.Do(func() { close(ready) }) }}
	env.stages.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		_, err := m.RunTool(context.Background(), spec)
		done <- err
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("holding tool ended early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("holding tool never started")
	}
	env.stages.mu.Lock()
	env.stages.on = previous
	env.stages.mu.Unlock()
	return func() {
		for _, cmd := range env.toolOwner.spawned() {
			_ = cmd.Process.Kill()
		}
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("holding tool run never returned")
		}
	}
}

// jailerToolAccess adapts a spec to access (op and fd mode).
func jailerToolAccess(spec *ToolSpec, access TargetAccess) {
	spec.TargetAccess = access
	switch access {
	case AccessOwnerRW:
		spec.Op, spec.Size = ToolResize, 1<<30
	case AccessShared:
		spec.Op, spec.TargetRoot, spec.TargetPath = ToolCheck, TargetRootImages, "v1/root.qcow2"
	}
}

// jailerAccessTargets creates one target inode and opens it twice: first
// for want (through a hard link when alias is set), then, after the inode
// takes the held access's at-rest mode (0444 for shared-ro), for held.
func jailerAccessTargets(t *testing.T, held, want TargetAccess, alias bool) (heldFile, wantFile *os.File) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "disk.qcow2")
	if err := os.WriteFile(p, []byte("qcow2 stand-in"), 0o600); err != nil {
		t.Fatal(err)
	}
	wantPath := p
	if alias {
		wantPath = p + ".link"
		if err := os.Link(p, wantPath); err != nil {
			t.Fatal(err)
		}
	}
	wantFile = jailerOpenFor(t, wantPath, want)
	if held == AccessShared {
		if err := os.Chmod(p, 0o444); err != nil {
			t.Fatal(err)
		}
	}
	return jailerOpenFor(t, p, held), wantFile
}

func jailerOpenFor(t *testing.T, p string, access TargetAccess) *os.File {
	t.Helper()
	flag := os.O_RDONLY
	if access == AccessOwnerRW {
		flag = os.O_RDWR
	}
	f, err := os.OpenFile(p, flag, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// TestRunToolOwnershipIndex: with a tool really holding an inode, only a
// second shared-ro reader is admitted; every other pairing is refused with
// ErrTargetBusy before any chown, including through a hard link.
func TestRunToolOwnershipIndex(t *testing.T) {
	all := []TargetAccess{AccessShared, AccessGroupRO, AccessOwnerRW}
	for _, held := range all {
		for _, want := range all {
			for _, alias := range []bool{false, true} {
				name := string(held) + "_then_" + string(want)
				if alias {
					name += "_via_hardlink"
				}
				t.Run(name, func(t *testing.T) {
					env, m := jailerToolEnv(t, jailerModeExec)
					heldFile, wantFile := jailerAccessTargets(t, held, want, alias)
					stop := jailerToolHold(t, env, m, heldFile, held)
					defer stop()
					env.mu.Lock()
					chownedBefore := len(env.chowned)
					env.mu.Unlock()
					spec := jailerToolSpec("/usr/bin/true", wantFile)
					jailerToolAccess(&spec, want)
					_, err := m.RunTool(t.Context(), spec)
					if held == AccessShared && want == AccessShared {
						if err != nil {
							t.Fatalf("a second shared reader was refused: %v", err)
						}
						return
					}
					if !errors.Is(err, ErrTargetBusy) {
						t.Fatalf("RunTool = %v, want ErrTargetBusy", err)
					}
					env.mu.Lock()
					chownedAfter := len(env.chowned)
					env.mu.Unlock()
					if chownedAfter != chownedBefore {
						t.Fatal("a refused run changed ownership before the refusal")
					}
				})
			}
		}
	}
}

// TestRunToolAndVMShareOneIndex: a VM's writable overlay blocks any tool on
// it, a retained tool blocks a VM launch on its target (whether or not the
// launch would grow it), and the hold survives a restart.
func TestRunToolAndVMShareOneIndex(t *testing.T) {
	jailerNeedBinary(t)
	t.Run("vm holds overlay", func(t *testing.T) {
		env, m := jailerToolEnv(t, jailerModeExec)
		spec := jailerSpec(t, "bx_jjjjjjj9")
		child, err := m.Start(t.Context(), spec)
		if err != nil {
			t.Fatal(err)
		}
		env.toolOwner.forget(child.Cmd)
		t.Cleanup(func() { _ = child.Kill(); _ = child.Cmd.Wait(); _ = child.Release(context.Background()) })
		overlay := spec.descriptor(spec.Disks[len(spec.Disks)-1])
		for _, access := range []TargetAccess{AccessShared, AccessGroupRO, AccessOwnerRW} {
			tool := jailerToolSpec("/usr/bin/true", jailerOpenFor(t, overlay.Name(), access))
			jailerToolAccess(&tool, access)
			if _, err := m.RunTool(t.Context(), tool); !errors.Is(err, ErrTargetBusy) {
				t.Fatalf("%s tool on a VM overlay = %v, want ErrTargetBusy", access, err)
			}
		}
	})
	t.Run("retained tool blocks vm launch and survives restart", func(t *testing.T) {
		env, m := jailerToolEnv(t, jailerModeExec)
		m.hooks.kill = func(*processHandle) error { return nil }
		vm := jailerSpec(t, "bx_jjjjjjj9")
		overlay := vm.descriptor(vm.Disks[len(vm.Disks)-1])
		tool := jailerToolSpec("/usr/bin/sleep", overlay)
		jailerToolAccess(&tool, AccessOwnerRW)
		tool.Timeout = 200 * time.Millisecond
		if _, err := m.RunTool(t.Context(), tool); !errors.Is(err, ErrToolRetained) {
			t.Fatalf("RunTool = %v, want ErrToolRetained", err)
		}
		// A VM launch that skips Grow still names the overlay as its target.
		if _, err := m.Start(t.Context(), vm); !errors.Is(err, ErrTargetBusy) {
			t.Fatalf("Start on a retained tool's target = %v, want ErrTargetBusy", err)
		}
		dev, ino := jailerInode(t, overlay)
		ref, held := m.TargetHolder(dev, ino)
		if !held {
			t.Fatal("the retained tool does not hold its target")
		}
		// Restart: the durable record keeps the hold (the live child is
		// unverifiable to the new Manager, which never guesses it absent).
		for _, cmd := range env.toolOwner.spawned() {
			_ = cmd.Process.Kill()
		}
		if _, ok := env.toolOwner.next(10 * time.Second); !ok {
			t.Fatal("the retained tool was never reaped")
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		m2 := jailerToolOpen(t, env)
		if got, held := m2.TargetHolder(dev, ino); !held || got != ref {
			t.Fatalf("after restart TargetHolder = %v, %v; want %v", got, held, ref)
		}
		if _, err := m2.Start(t.Context(), vm); !errors.Is(err, ErrTargetBusy) {
			t.Fatalf("Start after restart = %v, want ErrTargetBusy", err)
		}
		report, err := m2.SettleTools(t.Context())
		if err != nil || !slices.Equal(report.Settled, []Ref{ref}) {
			t.Fatalf("SettleTools = %+v, %v; want the dead attempt settled", report, err)
		}
	})
}

// TestProveToolExecRefusesSignaledExit: an exit observed before the live
// proof counts only as CLD_EXITED; a tool child killed by a signal is never
// proven, even with the staged comm and a distinct helper comm.
func TestProveToolExecRefusesSignaledExit(t *testing.T) {
	_, m := jailerToolEnv(t, jailerModeExec)
	pidfd := -1
	cmd := exec.Command("/usr/bin/sleep", "60")
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &pidfd}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	if pidfd < 0 {
		t.Skip("clone3 returned no pidfd")
	}
	handle, err := newProcessHandle(cmd.Process, pidfd, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.close()
	identity, err := readProcIdentity(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	poll := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
	if _, err := unix.Poll(poll, 10_000); err != nil {
		t.Fatal(err)
	}
	record := Record{SandboxID: ToolID(jailerAttempt()), PID: cmd.Process.Pid, StartTicks: identity.startTicks}
	if proof, err := m.proveToolExec(t.Context(), handle, inodeIdentity{Dev: 1, Ino: 1}, record, "jailer.test", "/usr/bin/sleep"); err == nil {
		t.Fatalf("a signal-killed tool was proven (%s)", proof)
	}
	waited = true
	_ = cmd.Wait()
}

// TestRunToolRefusesTargetsOffPolicy: a target or base outside the at-rest
// policy is refused before any reservation or ownership change.
func TestRunToolRefusesTargetsOffPolicy(t *testing.T) {
	cases := map[string]struct {
		access TargetAccess
		mode   os.FileMode
		owner  func() uint32
		base   os.FileMode
	}{
		"group-ro target not 0600":   {access: AccessGroupRO, mode: 0o644},
		"owner-rw target not 0600":   {access: AccessOwnerRW, mode: 0o640},
		"shared target writable":     {access: AccessShared, mode: 0o644},
		"shared target not readable": {access: AccessShared, mode: 0o440},
		"target of another owner":    {access: AccessGroupRO, mode: 0o600, owner: func() uint32 { return uint32(os.Getuid()) + 1 }},
		"base writable by its owner": {access: AccessGroupRO, mode: 0o600, base: 0o644},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env, m := jailerToolEnv(t, jailerModeExec)
			if tc.owner != nil {
				m.hooks.atRestOwner = tc.owner
			}
			p := filepath.Join(t.TempDir(), "disk.qcow2")
			if err := os.WriteFile(p, []byte("qcow2 stand-in"), 0o600); err != nil {
				t.Fatal(err)
			}
			target := jailerOpenFor(t, p, tc.access)
			if err := os.Chmod(p, tc.mode); err != nil {
				t.Fatal(err)
			}
			spec := jailerToolSpec("/usr/bin/true", target)
			jailerToolAccess(&spec, tc.access)
			if tc.base != 0 {
				b := filepath.Join(t.TempDir(), "root.qcow2")
				if err := os.WriteFile(b, []byte("base"), tc.base); err != nil {
					t.Fatal(err)
				}
				spec.Base = jailerOpenFor(t, b, AccessShared)
			}
			if _, err := m.RunTool(t.Context(), spec); err == nil {
				t.Fatal("RunTool accepted an off-policy target")
			}
			if len(m.Records()) != 0 || len(env.toolOwner.spawned()) != 0 || len(env.chowned) != 0 {
				t.Fatal("an off-policy run reserved, spawned or changed ownership")
			}
		})
	}
}
