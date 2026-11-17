package qemu

import (
	"context"
	"errors"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// The startup-failure contract: after the child was spawned, a failure
// either proves the child dead (nil handle, the cause alone) or returns the
// live Process with the cause joined to the cleanup error, because only that
// handle can finish and prove the kill. The tests stage both outcomes
// deterministically: the proven-dead case reaps the child first (Done
// closed), and the unproven case holds the reaper back so Done never closes
// within the wait. No zero-grace race is involved: the wait simply ends
// without the exit being observed.

// reapedChild gives a spawned child exactly one Wait owner. wait reaps at
// most once however often it is called (test body, reaper simulation or
// cleanup), and join bounds the wait for that single reap.
type reapedChild struct {
	cmd  *exec.Cmd
	once sync.Once
	done chan struct{}
}

func (r *reapedChild) wait() {
	r.once.Do(func() {
		_ = r.cmd.Wait()
		close(r.done)
	})
}

func (r *reapedChild) join(t *testing.T) {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the single Wait owner never finished")
	}
}

// newStartupTestProcess spawns a real child and builds the Process around it
// without the reaper goroutine, so the test owns the one Wait. The cleanup
// kills and reaps through the shared owner: an early test failure can never
// leave a second Wait racing it.
func newStartupTestProcess(t *testing.T) (*Process, *reapedChild) {
	t.Helper()
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	child := &reapedChild{cmd: cmd, done: make(chan struct{})}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		child.wait()
		child.join(t)
	})
	p := &Process{
		cfg:  Config{SandboxID: "bx_22222222"},
		cmd:  cmd,
		log:  slog.New(slog.DiscardHandler),
		done: make(chan struct{}),
	}
	return p, child
}

// TestStartupFailureProvenDeadReturnsNilHandle: the child exited and was
// reaped (Done closed) before the settlement; absence is proven and the
// original cause travels unchanged.
func TestStartupFailureProvenDeadReturnsNilHandle(t *testing.T) {
	p, child := newStartupTestProcess(t)
	_ = p.cmd.Process.Kill()
	child.wait() // the sole owner reaps before Done is published
	close(p.done)

	cause := errors.New("qmp not ready")
	got, err := p.startupFailure(cause, cleanupTimeout)
	if got != nil {
		t.Fatalf("proven-dead settlement returned a handle: %+v", got)
	}
	if !errors.Is(err, cause) || err.Error() != cause.Error() {
		t.Fatalf("error = %v, want the original cause %v alone", err, cause)
	}
}

// TestStartupFailureUnprovenRetainsHandle: with the reaper held back, the
// bounded wait ends without an observed exit; the Process itself is returned
// with the cause joined to the cleanup error, and a later Stop of that
// handle finishes and proves the kill.
func TestStartupFailureUnprovenRetainsHandle(t *testing.T) {
	p, child := newStartupTestProcess(t)

	cause := errors.New("qmp not ready")
	got, err := p.startupFailure(cause, 20*time.Millisecond)
	if got == nil {
		t.Fatal("unproven settlement returned no handle")
	}
	if got != p {
		t.Fatalf("returned handle %+v is not the live process", got)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("error = %v, want it to wrap the original cause", err)
	}
	if !strings.Contains(err.Error(), "did not prove the child dead") {
		t.Fatalf("error = %v, want the absence-unproven explanation", err)
	}
	if p.exited() {
		t.Fatal("settlement claimed an exit it did not observe")
	}

	// The retained handle can finish the job: the reaper simulation (the one
	// Wait owner) publishes Done, and Stop waits for exactly that. Every
	// path joins the reaper, including an early failure above.
	go func() {
		child.wait()
		close(p.done)
	}()
	stopCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := p.Stop(stopCtx); err != nil {
		t.Fatalf("Stop of the retained handle: %v", err)
	}
	child.join(t)
	if !p.exited() {
		t.Fatal("handle still reports an unproven exit after Stop")
	}
}
