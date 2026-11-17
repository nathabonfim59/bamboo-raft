// Deterministic finalization regressions: a detached process remains
// non-terminal until its logs and exit record have had their durability
// attempt, and reaping ends signal ownership before that attempt can block.
package guest

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// procFinalizeHarness is one directly called service over a caller-chosen
// base, allowing a second service to recover the same durable state.
type procFinalizeHarness struct {
	svc        *Service
	base       string
	procs      string
	finalizing <-chan struct{}
	releases   []func()
}

func newProcFinalizeHarness(t *testing.T, base string, mod func(*Config)) *procFinalizeHarness {
	t.Helper()
	home := filepath.Join(base, "home/user")
	tmp := filepath.Join(base, "tmp")
	for _, dir := range []string{home, tmp} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cfg := Config{
		Log:         slog.New(slog.DiscardHandler),
		UID:         os.Getuid(),
		GID:         os.Getgid(),
		NoSetGroups: true,
		HomeRoot:    home,
		TmpRoot:     tmp,
		Freezer:     noopFreezer{},
	}
	if mod != nil {
		mod(&cfg)
	}
	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("guest.New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := svc.Shutdown(ctx); err != nil {
			t.Logf("shutdown: %v", err)
		}
	})
	return &procFinalizeHarness{
		svc:   svc,
		base:  base,
		procs: filepath.Join(home, ".bbraft", "processes"),
	}
}

func (h *procFinalizeHarness) holdFinalization(gate *procFinalizeGate) {
	h.finalizing = gate.entered
	h.releases = append(h.releases, gate.release)
}

func (h *procFinalizeHarness) start(t *testing.T, id uint32, command string) *nodev1.Process {
	t.Helper()
	p, err := h.svc.StartProcess(t.Context(), &guestv1.StartProcessRequest{
		ProcessId: id,
		Command:   command,
	})
	if err != nil {
		t.Fatalf("StartProcess(%d): %v", id, err)
	}
	registered := h.svc.lookupProc(id)
	if registered == nil {
		t.Fatalf("StartProcess(%d) returned without a registered process", id)
	}
	// Registered after start, this runs before roots and TempDir disappear.
	// Always open the staged gates first. A cleanup signal is permitted only
	// while sigMu proves the leader is still unreaped and owns its pgid.
	t.Cleanup(func() {
		for _, release := range h.releases {
			release()
		}
		registered.sigMu.Lock()
		finalizing := false
		if h.finalizing != nil {
			select {
			case <-h.finalizing:
				finalizing = true // the gate is reached only after Wait
			default:
			}
		}
		registered.mu.Lock()
		owned := !finalizing && registered.rec != nil &&
			registered.rec.State == nodev1.ProcessState_PROCESS_STATE_RUNNING &&
			!registered.reaped && registered.pgid > 0
		pgid := registered.pgid
		registered.mu.Unlock()
		if owned {
			if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
				t.Errorf("cleanup kill of owned process group %d: %v", pgid, err)
			}
		}
		registered.sigMu.Unlock()
		select {
		case <-registered.done:
		case <-time.After(5 * time.Second):
			t.Errorf("process %d did not finalize during cleanup", id)
		}
	})
	return p
}

func (h *procFinalizeHarness) shutdown(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.svc.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func (h *procFinalizeHarness) restart(t *testing.T) *procFinalizeHarness {
	t.Helper()
	h.shutdown(t)
	return newProcFinalizeHarness(t, h.base, nil)
}

func (h *procFinalizeHarness) exitPath(id uint32) string {
	return filepath.Join(h.procs, strconv.FormatUint(uint64(id), 10), procExitName)
}

// procFinalizeBarrier identifies one finalization durability boundary.
type procFinalizeBarrier int

const (
	procFinalizeStdoutSync procFinalizeBarrier = iota
	procFinalizeExitFileSync
	procFinalizeExitDirSync
)

// procFinalizeGate holds one finalization barrier using only Config.SyncFile.
// The exit-directory barrier is armed by the final stdout sync, so earlier
// metadata-directory syncs cannot trip it. Release is idempotent for cleanup.
type procFinalizeGate struct {
	id         uint32
	barrier    procFinalizeBarrier
	syncErr    error
	entered    chan struct{}
	releaseCh  chan struct{}
	enterOnce  sync.Once
	releaseOne sync.Once
	armExitDir atomic.Bool
}

func newProcFinalizeGate(id uint32, barrier procFinalizeBarrier, syncErr error) *procFinalizeGate {
	return &procFinalizeGate{
		id:        id,
		barrier:   barrier,
		syncErr:   syncErr,
		entered:   make(chan struct{}),
		releaseCh: make(chan struct{}),
	}
}

func (g *procFinalizeGate) install(cfg *Config) {
	cfg.SyncFile = g.syncFile
}

func (g *procFinalizeGate) syncFile(f *os.File) error {
	id := strconv.FormatUint(uint64(g.id), 10)
	name := f.Name()
	isStdout := strings.HasSuffix(name, "/"+id+".log")
	if isStdout {
		g.armExitDir.Store(true)
	}
	hold := (g.barrier == procFinalizeStdoutSync && isStdout) ||
		(g.barrier == procFinalizeExitFileSync && strings.Contains(name, "/.tmp.exit.json.")) ||
		(g.barrier == procFinalizeExitDirSync && g.armExitDir.Load() && strings.HasSuffix(name, "/"+id))
	if !hold {
		return f.Sync()
	}
	g.enterOnce.Do(func() { close(g.entered) })
	<-g.releaseCh
	if g.syncErr != nil {
		return g.syncErr
	}
	return f.Sync()
}

func (g *procFinalizeGate) release() {
	g.releaseOne.Do(func() { close(g.releaseCh) })
}

func procFinalizeAwait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func procFinalizeRequireRunning(t *testing.T, h *procFinalizeHarness, id uint32, exitExists bool) {
	t.Helper()
	out, err := h.svc.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatalf("GetProcess(%d): %v", id, err)
	}
	if out.Process.State != nodev1.ProcessState_PROCESS_STATE_RUNNING ||
		out.Process.Exit != nil || out.Process.FinishedAtUnixMs != 0 {
		t.Fatalf("process became terminal before its durability attempt: %+v", out.Process)
	}
	_, statErr := os.Stat(h.exitPath(id))
	if exitExists && statErr != nil {
		t.Fatalf("post-rename barrier has no exit.json: %v", statErr)
	}
	if !exitExists && !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("exit record exists before its rename: %v", statErr)
	}
}

// procFinalizeStream is an in-memory AttachProcess stream. Sends are
// observable without gRPC and remain cancellation-responsive.
type procFinalizeStream struct {
	ctx    context.Context
	events chan *nodev1.ProcessEvent
}

func (s *procFinalizeStream) Send(event *nodev1.ProcessEvent) error {
	select {
	case s.events <- event:
		return nil
	case <-s.ctx.Done():
		return status.FromContextError(s.ctx.Err()).Err()
	}
}

func (*procFinalizeStream) SetHeader(metadata.MD) error  { return nil }
func (*procFinalizeStream) SendHeader(metadata.MD) error { return nil }
func (*procFinalizeStream) SetTrailer(metadata.MD)       {}
func (s *procFinalizeStream) Context() context.Context   { return s.ctx }
func (*procFinalizeStream) SendMsg(any) error            { return nil }
func (*procFinalizeStream) RecvMsg(any) error            { return nil }

func procFinalizeNextEvent(t *testing.T, stream *procFinalizeStream) *nodev1.ProcessEvent {
	t.Helper()
	select {
	case event := <-stream.events:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an attach frame")
		return nil
	}
}

func TestProcessFinalizationBlocksTerminalUntilDurable(t *testing.T) {
	const id = 901
	cases := []struct {
		name       string
		barrier    procFinalizeBarrier
		exitExists bool
	}{
		{name: "stdout-log-sync", barrier: procFinalizeStdoutSync},
		{name: "exit-record-file-sync", barrier: procFinalizeExitFileSync},
		{name: "exit-record-directory-sync", barrier: procFinalizeExitDirSync, exitExists: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gate := newProcFinalizeGate(id, tc.barrier, nil)
			h := newProcFinalizeHarness(t, t.TempDir(), gate.install)
			h.holdFinalization(gate)
			h.start(t, id, "printf final-out; printf final-err >&2; exit 7")
			procFinalizeAwait(t, gate.entered, tc.name)
			procFinalizeRequireRunning(t, h, id, tc.exitExists)

			// Attach exposes a process snapshot in its attached frame. That snapshot,
			// like GetProcess, must remain non-terminal while finalization is held.
			attachCtx, cancelAttach := context.WithCancel(t.Context())
			t.Cleanup(cancelAttach)
			stream := &procFinalizeStream{ctx: attachCtx, events: make(chan *nodev1.ProcessEvent, 8)}
			attachDone := make(chan error, 1)
			go func() {
				attachDone <- h.svc.AttachProcess(&guestv1.AttachProcessRequest{ProcessId: id}, stream)
			}()
			first := procFinalizeNextEvent(t, stream)
			attached := first.GetAttached()
			if attached == nil || attached.Process == nil {
				t.Fatalf("first attach frame = %+v, want attached process", first)
			}
			if attached.Process.State != nodev1.ProcessState_PROCESS_STATE_RUNNING || attached.Process.Exit != nil {
				t.Fatalf("attach exposed terminal state before durability: %+v", attached.Process)
			}
			cancelAttach()
			select {
			case err := <-attachDone:
				if status.Code(err) != codes.Canceled {
					t.Fatalf("canceled attach = %v, want Canceled", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("attach did not answer cancellation while finalization was blocked")
			}
		attachDrain:
			for {
				select {
				case event := <-stream.events:
					if event.GetExit() != nil {
						t.Fatal("attach emitted an exit frame before the durability attempt")
					}
				default:
					break attachDrain
				}
			}

			p := h.svc.lookupProc(id)
			if p == nil {
				t.Fatal("process vanished during finalization")
			}
			gate.release()
			procFinalizeAwait(t, p.done, "finalization")
			out, err := h.svc.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
			if err != nil {
				t.Fatal(err)
			}
			if out.Process.State != nodev1.ProcessState_PROCESS_STATE_EXITED ||
				out.Process.Exit == nil || out.Process.Exit.ExitCode == nil || *out.Process.Exit.ExitCode != 7 {
				t.Fatalf("final process = %+v, want EXITED(7)", out.Process)
			}
			if _, err := os.Stat(h.exitPath(id)); err != nil {
				t.Fatalf("terminal state is visible without exit.json: %v", err)
			}
			if string(out.Stdout) != "final-out" || string(out.Stderr) != "final-err" {
				t.Fatalf("final logs = %q, %q", out.Stdout, out.Stderr)
			}

			restarted := h.restart(t)
			recovered, err := restarted.svc.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
			if err != nil {
				t.Fatalf("GetProcess after restart: %v", err)
			}
			if recovered.Process.State != nodev1.ProcessState_PROCESS_STATE_EXITED ||
				recovered.Process.Exit == nil || recovered.Process.Exit.ExitCode == nil ||
				*recovered.Process.Exit.ExitCode != 7 || string(recovered.Stdout) != "final-out" ||
				string(recovered.Stderr) != "final-err" {
				t.Fatalf("recovered process = %+v, logs %q, %q", recovered.Process, recovered.Stdout, recovered.Stderr)
			}
		})
	}
}

func TestProcessFinalizationFailureRemainsExitedThenLost(t *testing.T) {
	const id = 902
	gate := newProcFinalizeGate(id, procFinalizeStdoutSync, errors.New("injected final stdout sync failure"))
	h := newProcFinalizeHarness(t, t.TempDir(), gate.install)
	h.holdFinalization(gate)
	h.start(t, id, "printf undurable")
	procFinalizeAwait(t, gate.entered, "the failing final stdout sync")
	procFinalizeRequireRunning(t, h, id, false)

	p := h.svc.lookupProc(id)
	if p == nil {
		t.Fatal("process vanished during finalization")
	}
	gate.release()
	procFinalizeAwait(t, p.done, "failed finalization")
	out, err := h.svc.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatal(err)
	}
	if out.Process.State != nodev1.ProcessState_PROCESS_STATE_EXITED ||
		out.Process.Exit == nil || out.Process.Exit.ExitCode == nil || *out.Process.Exit.ExitCode != 0 {
		t.Fatalf("in-memory process after sync failure = %+v, want EXITED(0)", out.Process)
	}
	if _, err := os.Stat(h.exitPath(id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sync failure unexpectedly wrote exit.json: %v", err)
	}

	restarted := h.restart(t)
	recovered, err := restarted.svc.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatalf("GetProcess after restart: %v", err)
	}
	if recovered.Process.State != nodev1.ProcessState_PROCESS_STATE_LOST || recovered.Process.Exit != nil {
		t.Fatalf("undurable exit recovered as %+v, want LOST", recovered.Process)
	}
}

func TestProcessFinalizationSignalAfterReap(t *testing.T) {
	const id = 903
	gate := newProcFinalizeGate(id, procFinalizeStdoutSync, nil)
	h := newProcFinalizeHarness(t, t.TempDir(), gate.install)
	var killCalls atomic.Int32
	h.svc.procFS.kill = func(pgid int, sig syscall.Signal) error {
		killCalls.Add(1)
		return nil
	}
	signalEntered := make(chan struct{})
	signalRelease := make(chan struct{})
	var signalOnce, releaseSignalOnce sync.Once
	h.svc.preSignal = func() {
		signalOnce.Do(func() { close(signalEntered) })
		<-signalRelease
	}
	releaseSignal := func() { releaseSignalOnce.Do(func() { close(signalRelease) }) }
	h.holdFinalization(gate)
	h.releases = append(h.releases, releaseSignal)

	started := h.start(t, id, "sleep 30")

	// Park an already-entered kill between its outer RUNNING check and
	// signalGroup, while the process is still genuinely alive.
	killCtx, cancelKill := context.WithCancel(t.Context())
	t.Cleanup(cancelKill)
	killDone := make(chan error, 1)
	go func() {
		_, err := h.svc.KillProcess(killCtx, &guestv1.KillProcessRequest{ProcessId: id})
		killDone <- err
	}()
	procFinalizeAwait(t, signalEntered, "the staged signal before reap")

	// End the live group independently of the service's signal seam. The
	// supervisor can now reap and reach its held final sync while the kill is
	// still parked. Once reaped, the old pgid may belong to somebody else.
	if err := syscall.Kill(-int(started.GetPid()), syscall.SIGTERM); err != nil {
		t.Fatalf("end the staged process group: %v", err)
	}
	procFinalizeAwait(t, gate.entered, "the final stdout sync after reap")
	procFinalizeRequireRunning(t, h, id, false)
	p := h.svc.lookupProc(id)
	if p == nil {
		t.Fatal("process vanished during finalization")
	}

	// Resume the already-entered kill only after the reap. Its fresh ownership
	// check must reject the signal without waiting for durable persistence.
	releaseSignal()
	cancelKill()
	select {
	case err := <-killDone:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("canceled post-reap kill = %v, want Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("post-reap kill did not answer cancellation while persistence was blocked")
	}
	if got := killCalls.Load(); got != 0 {
		t.Fatalf("post-reap kill made %d signal syscalls to a possibly reused pgid", got)
	}

	gate.release()
	procFinalizeAwait(t, p.done, "finalization after the staged kill")
	if got := h.svc.processSnapshot(p); got.State != nodev1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("state after releasing finalization = %s, want EXITED", got.State)
	}
}
