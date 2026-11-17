// Internal tests for the kill machinery's failure modes and ownership
// rules, using the service-scoped seams (groupScan, killProve, preSignal):
// a canceled caller stops waiting promptly, a group that cannot be proven
// ended is an error never a success, a kill that resumes after the process
// finalized on its own sends no signal, and the anchored state walk
// follows a renamed base directory.
package guest

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// procSpawnGroup starts a real process group: bash with a child, so the
// group has live members. The caller must kill it.
func procSpawnGroup(t *testing.T) (pgid int) {
	t.Helper()
	cmd := exec.Command("bash", "-c", "sleep 30 & wait")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
	return cmd.Process.Pid
}

// procKillTestService builds a minimal Service with just the fields
// killGroup and the state walk touch.
func procKillTestService() *Service {
	return &Service{
		log:      slog.New(slog.DiscardHandler),
		uid:      os.Getuid(),
		gid:      os.Getgid(),
		syncFile: func(*os.File) error { return nil },
	}
}

// TestKillGroupCanceledCaller: cancellation between TERM and the grace
// deadline returns the context error promptly instead of waiting out the
// grace period.
func TestKillGroupCanceledCaller(t *testing.T) {
	s := procKillTestService()
	pgid := procSpawnGroup(t)
	p := &detachedProc{
		rec:  &nodev1.Process{State: nodev1.ProcessState_PROCESS_STATE_RUNNING},
		pgid: pgid,
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := s.killGroup(ctx, p, 30*time.Second)
		errCh <- err
	}()
	time.Sleep(150 * time.Millisecond) // TERM sent, inside the grace wait
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("killGroup after cancel = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("killGroup kept waiting past its canceled context")
	}
	if d := time.Since(start); d > 1500*time.Millisecond {
		t.Fatalf("cancellation answered after %v, not promptly", d)
	}
}

// TestKillGroupUnprovable: with the group scan insisting the group is
// alive (an unkillable or unreadable /proc), killGroup must give up with
// an error after the proof bound instead of looping forever or claiming
// success.
func TestKillGroupUnprovable(t *testing.T) {
	s := procKillTestService()
	pgid := procSpawnGroup(t)
	p := &detachedProc{
		rec:  &nodev1.Process{State: nodev1.ProcessState_PROCESS_STATE_RUNNING},
		pgid: pgid,
	}
	s.groupScan = func(int, bool) bool { return true }
	s.killProve = 150 * time.Millisecond

	type result struct {
		sent bool
		err  error
	}
	errCh := make(chan result, 1)
	go func() {
		sent, err := s.killGroup(context.Background(), p, 100*time.Millisecond)
		errCh <- result{sent, err}
	}()
	select {
	case r := <-errCh:
		if !errors.Is(r.err, errGroupUnproven) {
			t.Fatalf("killGroup with a lying scan = %v, want errGroupUnproven", r.err)
		}
		if !r.sent {
			t.Fatal("killGroup reported no signal sent although TERM went out")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("killGroup never gave up on the unprovable group")
	}
}

// TestKillStagedAfterFinalize proves the ownership race is closed by
// ordering, not luck: a kill that has already passed its running check
// pauses, the process finalizes on its own, and the resumed kill sends no
// signal and reports sent=false — even though the pid could by then be
// reused by an unrelated process group.
func TestKillStagedAfterFinalize(t *testing.T) {
	base := t.TempDir()
	home := filepath.Join(base, "home/user")
	tmp := filepath.Join(base, "tmp")
	for _, d := range []string{home, tmp} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	svc, err := New(Config{
		Log:         slog.New(slog.DiscardHandler),
		UID:         os.Getuid(),
		GID:         os.Getgid(),
		NoSetGroups: true,
		HomeRoot:    home,
		TmpRoot:     tmp,
		Freezer:     &noopFreezer{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		svc.Shutdown(ctx)
	})

	const id = 901
	if _, err := svc.StartProcess(context.Background(), &guestv1.StartProcessRequest{
		ProcessId: id, Command: "echo quick",
	}); err != nil {
		t.Fatal(err)
	}

	entered, release := make(chan struct{}), make(chan struct{})
	svc.preSignal = func() {
		close(entered)
		<-release
	}
	type response struct {
		signaled bool
		err      error
	}
	killCh := make(chan response, 1)
	go func() {
		k, err := svc.KillProcess(context.Background(), &guestv1.KillProcessRequest{ProcessId: id, GraceSeconds: 5})
		killCh <- response{k.GetSignaled(), err}
	}()
	<-entered // the kill passed its running check and paused

	// Let the process finalize while the kill is parked.
	p := svc.lookupProc(id)
	if p == nil {
		t.Fatal("process vanished")
	}
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		t.Fatal("process never finalized while the kill was parked")
	}
	close(release)

	select {
	case r := <-killCh:
		if r.err != nil {
			t.Fatalf("resumed kill: %v", r.err)
		}
		if r.signaled {
			t.Fatal("resumed kill signaled a group it no longer owns")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resumed kill never returned")
	}
	if out := svc.processSnapshot(p); out.State != nodev1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("state after resumed kill = %s", out.State)
	}
}

// TestOpenStateDirFromAnchored proves the state walk anchors at the passed
// directory handle: renaming the underlying path does not detach or
// relocate the walk.
func TestOpenStateDirFromAnchored(t *testing.T) {
	base := t.TempDir()
	root, err := os.OpenRoot(base)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	anchor, err := root.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	defer anchor.Close()
	s := procKillTestService()

	renamed := filepath.Join(t.TempDir(), "moved")
	if err := os.Rename(base, renamed); err != nil {
		t.Fatal(err)
	}
	dir, err := s.openStateDirFrom(anchor, ".bbraft/processes", true)
	if err != nil {
		t.Fatalf("walk after the base path was renamed: %v", err)
	}
	defer dir.Close()
	if _, err := os.Stat(filepath.Join(renamed, ".bbraft", "processes")); err != nil {
		t.Fatalf("state did not follow the renamed directory: %v", err)
	}
}

// noopFreezer satisfies Freezer without touching anything.
type noopFreezer struct{}

func (noopFreezer) Freeze() error { return nil }
func (noopFreezer) Thaw() error   { return nil }
