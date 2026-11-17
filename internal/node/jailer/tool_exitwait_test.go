package jailer

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// jailerExitWaitRun starts one RunTool whose child is a REAL zombie before
// the exec proof (its /proc/<pid>/exe is gone) while toolExitHidden keeps
// the exit unobservable, reproducing the exit_mm..exit_notify window. kill
// signals the child first (a signaled exit); host runs the host binary
// instead of the staged one. It returns the hidden gate, the waiting
// witness, the cancel and the result channel.
func jailerExitWaitRun(t *testing.T, binary string, mode string, kill bool, helperComm ...string) (chan struct{}, chan struct{}, context.CancelFunc, <-chan jailerExitWaitResult) {
	t.Helper()
	env, m := jailerToolEnv(t, mode)
	if len(helperComm) == 1 {
		m.hooks.toolHelperComm = func(string) string { return helperComm[0] }
	}
	hidden, waiting := make(chan struct{}), make(chan struct{}, 1)
	m.hooks.toolExitHidden, m.hooks.toolExitWaiting = hidden, waiting
	spec := jailerToolSpec(binary, jailerToolTarget(t, os.O_RDONLY))
	env.stages.on = map[launchStage]func(){stageExecProof: func() {
		pid := jailerRecordPID(t, env, ToolID(spec.Attempt))
		fd, err := unix.PidfdOpen(pid, 0)
		if err != nil {
			t.Errorf("pidfd_open: %v", err)
			return
		}
		defer unix.Close(fd)
		if kill {
			if err := unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0); err != nil {
				t.Errorf("pidfd_send_signal: %v", err)
				return
			}
		} else if mode == jailerModeExecHost {
			return // stays alive: the live proof sees the host binary
		}
		// The real child becomes a zombie (exe gone), unreaped.
		poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		if n, err := unix.Poll(poll, 5000); err != nil || n != 1 {
			t.Errorf("the child did not exit: %d, %v", n, err)
			return
		}
		if _, err := os.Stat("/proc/" + strconv.Itoa(pid) + "/exe"); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("a zombie's exe is still readable: %v", err)
		}
	}}
	ctx, cancel := context.WithCancel(t.Context())
	out := make(chan jailerExitWaitResult, 1)
	done := make(chan struct{})
	// Registered before the goroutine starts and after the env's own
	// cleanups (LIFO: this runs first): every path, an early Fatal
	// included, cancels the run and joins its goroutine (bounded) before
	// the Manager is closed. out stays the single consumer's channel.
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("the RunTool goroutine did not finish")
		}
	})
	go func() {
		defer close(done)
		res, err := m.RunTool(ctx, spec)
		out <- jailerExitWaitResult{res, err}
	}()
	return hidden, waiting, cancel, out
}

type jailerExitWaitResult struct {
	res ToolResult
	err error
}

func jailerExitWaitGet(t *testing.T, out <-chan jailerExitWaitResult) jailerExitWaitResult {
	t.Helper()
	select {
	case r := <-out:
		return r
	case <-time.After(15 * time.Second):
		t.Fatal("RunTool did not return")
		return jailerExitWaitResult{}
	}
}

// TestRunToolExitTransitionWait: a successful fast tool caught between
// exit_mm (its exe already gone) and exit_notify (not yet waitable) is
// proven by the unchanged fast-exit witness once its exit becomes
// observable, within the 2s cap and the run's context; anything else in
// that window is refused.
func TestRunToolExitTransitionWait(t *testing.T) {
	t.Run("vanished exe of a clean exit is proven after the transition", func(t *testing.T) {
		hidden, waiting, _, out := jailerExitWaitRun(t, "/usr/bin/true", jailerModeExec, false)
		select {
		case <-waiting:
		case r := <-out:
			t.Fatalf("RunTool = %+v, %v before waiting for the exit transition", r.res, r.err)
		case <-time.After(10 * time.Second):
			t.Fatal("the proof never started waiting")
		}
		close(hidden)
		r := jailerExitWaitGet(t, out)
		if r.err != nil || r.res.ExitCode != 0 || r.res.Proof != ProofExitedComm {
			t.Fatalf("RunTool = %+v, %v; want exit 0 proven by %s", r.res, r.err, ProofExitedComm)
		}
	})
	t.Run("an exit that never becomes observable is refused at the cap", func(t *testing.T) {
		_, waiting, _, out := jailerExitWaitRun(t, "/usr/bin/true", jailerModeExec, false)
		start := time.Now()
		r := jailerExitWaitGet(t, out)
		if r.err == nil || !strings.Contains(r.err.Error(), "observable") {
			t.Fatalf("RunTool = %+v, %v; want a refusal at the bound", r.res, r.err)
		}
		if len(waiting) != 1 {
			t.Fatal("the refusal did not come from the bounded wait")
		}
		if d := time.Since(start); d < toolExitObserveLimit || d > toolExitObserveLimit+5*time.Second {
			t.Fatalf("refused after %v, want about %v", d, toolExitObserveLimit)
		}
	})
	t.Run("cancellation during the wait keeps its cause", func(t *testing.T) {
		_, waiting, cancel, out := jailerExitWaitRun(t, "/usr/bin/true", jailerModeExec, false)
		select {
		case <-waiting:
		case r := <-out:
			t.Fatalf("RunTool = %+v, %v before waiting", r.res, r.err)
		}
		cancel()
		if r := jailerExitWaitGet(t, out); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("RunTool = %+v, %v; want context.Canceled", r.res, r.err)
		}
	})
	t.Run("a signaled exit after the transition is refused", func(t *testing.T) {
		hidden, waiting, _, out := jailerExitWaitRun(t, "/usr/bin/sleep", jailerModeExec, true)
		select {
		case <-waiting:
		case r := <-out:
			t.Fatalf("RunTool = %+v, %v before waiting", r.res, r.err)
		}
		close(hidden)
		if r := jailerExitWaitGet(t, out); r.err == nil || !strings.Contains(r.err.Error(), "ended by signal") || !strings.Contains(r.err.Error(), "before its exec proof") {
			t.Fatalf("RunTool = %+v, %v; want the exec proof to refuse the signaled exit", r.res, r.err)
		}
	})
	t.Run("an ambiguous helper comm in the window is refused", func(t *testing.T) {
		hidden, waiting, _, out := jailerExitWaitRun(t, "/usr/bin/true", jailerModeExec, false, "true")
		select {
		case <-waiting:
		case r := <-out:
			t.Fatalf("RunTool = %+v, %v before waiting", r.res, r.err)
		}
		close(hidden)
		if r := jailerExitWaitGet(t, out); r.err == nil || !strings.Contains(r.err.Error(), "cannot be told") {
			t.Fatalf("RunTool = %+v, %v; want the fast-exit witness to refuse an ambiguous comm", r.res, r.err)
		}
	})
	t.Run("a live exe mismatch is refused without waiting", func(t *testing.T) {
		_, waiting, _, out := jailerExitWaitRun(t, "/usr/bin/sleep", jailerModeExecHost, false)
		r := jailerExitWaitGet(t, out)
		if r.err == nil || !strings.Contains(r.err.Error(), "not the staged") {
			t.Fatalf("RunTool = %+v, %v; want the exe mismatch refused", r.res, r.err)
		}
		if len(waiting) != 0 {
			t.Fatal("a mismatch waited for an exit transition")
		}
	})
}
