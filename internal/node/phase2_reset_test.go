// Phase 2 ResetVM: the QMP command order, the reboot-action restore on every
// path, the READY -> BOOTING -> READY transition, the preconditions and the
// cancellation/delete rules. The QMP peer is the node tests' fake server (with
// its set-action/system_reset surface); the guest is the real in-process guest
// service. The hardware reset itself is a KVM test: nodetest's Phase2 Reset
// group runs against a real VM.

package node

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// resetvmOptions configures one ResetVM test fixture.
type resetvmOptions struct {
	bootTimeout time.Duration
	wrapGuest   func(guestv1.GuestServer) guestv1.GuestServer
}

// resetvmEnv is a booted READY VM on the fake QMP server.
type resetvmEnv struct {
	h    *harness
	srv  *fakeQMPServer
	id   string
	proc *fakeProcess
	// wrapped is the QMP-decorated process handed to the manager; closing it
	// closes the QMP client, which the exit watcher otherwise leaves open
	// (the real reaper closes its own).
	wrapped VMProcess
}

// resetvmSetup builds the fixture. The harness clock advances one second per
// reading, so a reset's started_at is provably newer than the boot's.
func resetvmSetup(t *testing.T, opts resetvmOptions) *resetvmEnv {
	t.Helper()
	env := &resetvmEnv{srv: startFakeQMP(t, "guest"), id: ids.Sandbox()}
	launcher := &fakeLauncher{}
	launcher.wrapProcess = func(p VMProcess) VMProcess {
		client, err := qmp.Dial(t.Context(), env.srv.path, quietLog())
		if err != nil {
			t.Errorf("qmp dial: %v", err)
			return p
		}
		wrapped := &qmpFakeProcess{fakeProcess: p.(*fakeProcess), client: client}
		env.wrapped = wrapped
		return wrapped
	}
	var ticks atomic.Int64
	base := time.Unix(1_700_000_000, 0)
	env.h = newHarness(t, harnessOptions{
		launcher:    launcher,
		bootTimeout: opts.bootTimeout,
		wrapGuest:   opts.wrapGuest,
		configure: func(cfg *Config) {
			cfg.snapshotNow = func() time.Time {
				return base.Add(time.Duration(ticks.Add(1)) * time.Second)
			}
		},
	})
	return env
}

// create boots the fixture's VM through the public RPC.
func (e *resetvmEnv) create() *nodev1.VM {
	e.h.t.Helper()
	vm := e.h.create(e.id, nil)
	e.proc = e.h.launcher.lastProcess()
	return vm
}

// dialCount is how many guest connections the manager opened, boot included.
func (e *resetvmEnv) dialCount() int {
	e.h.dialer.mu.Lock()
	defer e.h.dialer.mu.Unlock()
	return len(e.h.dialer.conns)
}

// resetvmGuest forwards to the real guest until the fake QMP reports the
// hardware reset. Afterwards it holds every Ping (entered/release) or fails it
// (dead), emulating an agent that is not back yet.
type resetvmGuest struct {
	guestv1.GuestServer
	armed   atomic.Bool
	dead    bool
	entered chan struct{}
	release chan struct{}
}

func (g *resetvmGuest) arm() { g.armed.Store(true) }

func (g *resetvmGuest) Ping(ctx context.Context, req *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	if !g.armed.Load() {
		return g.GuestServer.Ping(ctx, req)
	}
	if g.dead {
		return nil, status.Error(codes.Unavailable, "the guest agent is restarting")
	}
	select {
	case g.entered <- struct{}{}:
	default:
	}
	select {
	case <-g.release:
		return g.GuestServer.Ping(ctx, req)
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// armWith makes every reset in the fixture arm g.
func (e *resetvmEnv) armWith(g *resetvmGuest) {
	e.srv.mu.Lock()
	e.srv.onReset = g.arm
	e.srv.mu.Unlock()
}

// resetvmState reads the VM record through GetVM.
func resetvmState(t *testing.T, m *Manager, id string) *nodev1.VM {
	t.Helper()
	vm, err := m.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatalf("GetVM(%s): %v", id, err)
	}
	return vm
}

// resetvmWaitState polls the VM record until it reads want.
func resetvmWaitState(t *testing.T, m *Manager, id string, want nodev1.VMState) *nodev1.VM {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		vm := resetvmState(t, m, id)
		if vm.GetState() == want {
			return vm
		}
		if time.Now().After(deadline) {
			t.Fatalf("sandbox %s is %s, want %s", id, vm.GetState(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// resetvmWantCode requires err to carry code.
func resetvmWantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("error = %v (code %s), want %s", err, got, want)
	}
}

func TestResetVMResetsAndReturnsReady(t *testing.T) {
	pings := &resetvmGuest{entered: make(chan struct{}, 1), release: make(chan struct{})}
	env := resetvmSetup(t, resetvmOptions{wrapGuest: func(g guestv1.GuestServer) guestv1.GuestServer {
		pings.GuestServer = g
		return pings
	}})
	before := env.create()
	env.armWith(pings)

	type result struct {
		vm  *nodev1.VM
		err error
	}
	done := make(chan result, 1)
	go func() {
		vm, err := env.h.mg.ResetVM(t.Context(), &nodev1.ResetVMRequest{SandboxId: env.id})
		done <- result{vm, err}
	}()
	select {
	case <-pings.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the guest agent was never pinged after the reset")
	}
	// The guest is back at this point: the record must already read BOOTING
	// and a second connection must have been opened for the restarted agent.
	if vm := resetvmState(t, env.h.mg, env.id); vm.GetState() != nodev1.VMState_VM_STATE_BOOTING {
		t.Fatalf("state during the reset = %s, want BOOTING", vm.GetState())
	}
	if got := env.dialCount(); got != 2 {
		t.Fatalf("guest dials = %d, want 2 (boot and reset)", got)
	}
	close(pings.release)

	res := <-done
	if res.err != nil {
		t.Fatalf("ResetVM: %v", res.err)
	}
	if res.vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("state after reset = %s, want READY", res.vm.GetState())
	}
	if res.vm.GetStartedAtUnixMs() <= before.GetStartedAtUnixMs() {
		t.Fatalf("started_at after reset = %d, boot = %d; want newer", res.vm.GetStartedAtUnixMs(), before.GetStartedAtUnixMs())
	}
	if res.vm.GetError() != "" {
		t.Fatalf("READY VM reports error %q", res.vm.GetError())
	}
	want := []string{"set-action:reset", "system_reset", "set-action:shutdown"}
	if got := env.srv.resetCommands(); !slices.Equal(got, want) {
		t.Fatalf("QMP reset commands = %v, want %v", got, want)
	}
	if got := env.srv.resetCount(); got != 1 {
		t.Fatalf("system_reset sent %d times, want 1", got)
	}
	// The replacement connection serves commands after the reset.
	if err := env.h.mg.Exec(&nodev1.ExecRequest{SandboxId: env.id, Command: "true"}, &testExecStream{ctx: t.Context()}); err != nil {
		t.Fatalf("Exec after reset: %v", err)
	}
}

func TestResetVMRestoresRebootShutdownOnFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(env *resetvmEnv, pings *resetvmGuest)
	}{
		{
			name: "system_reset fails",
			prepare: func(env *resetvmEnv, _ *resetvmGuest) {
				env.srv.mu.Lock()
				env.srv.resetErr = errors.New("injected reset failure")
				env.srv.mu.Unlock()
			},
		},
		{
			name: "agent never answers",
			prepare: func(env *resetvmEnv, pings *resetvmGuest) {
				pings.dead = true
				env.armWith(pings)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pings := &resetvmGuest{entered: make(chan struct{}, 1), release: make(chan struct{})}
			env := resetvmSetup(t, resetvmOptions{
				// Long enough for one retry of the fake agent, short enough to
				// keep the test fast.
				bootTimeout: 300 * time.Millisecond,
				wrapGuest: func(g guestv1.GuestServer) guestv1.GuestServer {
					pings.GuestServer = g
					return pings
				},
			})
			env.create()
			tc.prepare(env, pings)

			if _, err := env.h.mg.ResetVM(t.Context(), &nodev1.ResetVMRequest{SandboxId: env.id}); err == nil {
				t.Fatal("ResetVM succeeded, want an error")
			}
			// The restore runs on every path, as long as QEMU is alive.
			want := []string{"set-action:reset", "system_reset", "set-action:shutdown"}
			if got := env.srv.resetCommands(); !slices.Equal(got, want) {
				t.Fatalf("QMP reset commands = %v, want %v", got, want)
			}
			vm := resetvmState(t, env.h.mg, env.id)
			if vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
				t.Fatalf("state after a failed reset = %s, want FAILED", vm.GetState())
			}
			if vm.GetError() == "" {
				t.Fatal("FAILED VM reports no error")
			}
			// QEMU is kept, like a failed boot: stop or a retried reset must
			// still find the process.
			select {
			case <-env.proc.Done():
				t.Fatal("the failed reset stopped QEMU")
			default:
			}
		})
	}
}

func TestResetVMCancelledBeforeResetKeepsReady(t *testing.T) {
	env := resetvmSetup(t, resetvmOptions{})
	before := env.create()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	env.h.mg.beforeResetGate = func() {
		entered <- struct{}{}
		<-release
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := env.h.mg.ResetVM(ctx, &nodev1.ResetVMRequest{SandboxId: env.id})
		done <- err
	}()
	<-entered
	if vm := resetvmState(t, env.h.mg, env.id); vm.GetState() != nodev1.VMState_VM_STATE_BOOTING {
		t.Fatalf("state while the reset waits for the gate = %s, want BOOTING", vm.GetState())
	}
	cancel()
	select {
	case err := <-done:
		resetvmWantCode(t, err, codes.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("the canceled ResetVM did not return")
	}
	close(release)

	// The reset ended before the guest was reset: the record goes back to
	// READY, with its old started_at and its working connection.
	after := resetvmWaitState(t, env.h.mg, env.id, nodev1.VMState_VM_STATE_READY)
	if after.GetStartedAtUnixMs() != before.GetStartedAtUnixMs() {
		t.Fatalf("started_at = %d after an aborted reset, want the boot's %d", after.GetStartedAtUnixMs(), before.GetStartedAtUnixMs())
	}
	if got := env.dialCount(); got != 1 {
		t.Fatalf("guest dials = %d, want only the boot's", got)
	}
	if got := env.srv.resetCommands(); len(got) != 0 {
		t.Fatalf("QMP commands after an aborted reset = %v, want none", got)
	}
	if err := env.h.mg.Exec(&nodev1.ExecRequest{SandboxId: env.id, Command: "true"}, &testExecStream{ctx: t.Context()}); err != nil {
		t.Fatalf("Exec after an aborted reset: %v", err)
	}
}

func TestResetVMCancelledAfterResetLeavesFailed(t *testing.T) {
	pings := &resetvmGuest{entered: make(chan struct{}, 1), release: make(chan struct{})}
	env := resetvmSetup(t, resetvmOptions{wrapGuest: func(g guestv1.GuestServer) guestv1.GuestServer {
		pings.GuestServer = g
		return pings
	}})
	env.create()
	env.armWith(pings)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := env.h.mg.ResetVM(ctx, &nodev1.ResetVMRequest{SandboxId: env.id})
		done <- err
	}()
	select {
	case <-pings.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the reset never reached the guest agent")
	}
	// Past system_reset there is no way back: the record must end FAILED with
	// QEMU kept, and the reboot action must be restored all the same.
	cancel()
	select {
	case err := <-done:
		resetvmWantCode(t, err, codes.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("the canceled ResetVM did not return")
	}
	resetvmWaitState(t, env.h.mg, env.id, nodev1.VMState_VM_STATE_FAILED)
	want := []string{"set-action:reset", "system_reset", "set-action:shutdown"}
	if got := env.srv.resetCommands(); !slices.Equal(got, want) {
		t.Fatalf("QMP reset commands = %v, want %v", got, want)
	}
	select {
	case <-env.proc.Done():
		t.Fatal("the canceled reset stopped QEMU")
	default:
	}
}

func TestResetVMRecoversFailedVM(t *testing.T) {
	pings := &resetvmGuest{dead: true, entered: make(chan struct{}, 1), release: make(chan struct{})}
	env := resetvmSetup(t, resetvmOptions{
		bootTimeout: 300 * time.Millisecond,
		wrapGuest: func(g guestv1.GuestServer) guestv1.GuestServer {
			pings.GuestServer = g
			return pings
		},
	})
	env.create()
	env.armWith(pings)

	// The agent never comes back: the VM is FAILED with its QEMU kept.
	if _, err := env.h.mg.ResetVM(t.Context(), &nodev1.ResetVMRequest{SandboxId: env.id}); err == nil {
		t.Fatal("ResetVM with a dead agent succeeded")
	}
	resetvmWaitState(t, env.h.mg, env.id, nodev1.VMState_VM_STATE_FAILED)

	// A failed VM whose QEMU still runs is reset again, and this time the
	// agent answers.
	pings.dead = false
	close(pings.release)
	after, err := env.h.mg.ResetVM(t.Context(), &nodev1.ResetVMRequest{SandboxId: env.id})
	if err != nil {
		t.Fatalf("ResetVM of a FAILED VM: %v", err)
	}
	if after.GetState() != nodev1.VMState_VM_STATE_READY || after.GetError() != "" {
		t.Fatalf("after the retried reset = %+v, want READY without an error", after)
	}
	want := []string{"set-action:reset", "system_reset", "set-action:shutdown", "set-action:reset", "system_reset", "set-action:shutdown"}
	if got := env.srv.resetCommands(); !slices.Equal(got, want) {
		t.Fatalf("QMP reset commands = %v, want %v", got, want)
	}
}

// TestResetVMRestoreAfterDeadQEMUIsHarmless proves the deferred reboot-action
// restore cannot hurt a VM whose QEMU a delete just stopped: a dead process is
// skipped, and a client closed underneath it only produces a logged error.
func TestResetVMRestoreAfterDeadQEMUIsHarmless(t *testing.T) {
	srv := startFakeQMP(t, "guest")
	client, err := qmp.Dial(t.Context(), srv.path, quietLog())
	if err != nil {
		t.Fatalf("qmp dial: %v", err)
	}
	proc := newFakeProcess()
	wrapped := &qmpFakeProcess{fakeProcess: proc, client: client}
	h := newHarness(t, harnessOptions{})

	h.mg.restoreRebootShutdown(wrapped, client, "bx_restore0")
	if got := srv.rebootActions(); !slices.Equal(got, []string{"shutdown"}) {
		t.Fatalf("reboot actions = %v, want one shutdown", got)
	}
	// QEMU is gone: the restore is skipped instead of talking to a dead peer.
	proc.exit(nil)
	h.mg.restoreRebootShutdown(wrapped, client, "bx_restore0")
	if got := srv.rebootActions(); !slices.Equal(got, []string{"shutdown"}) {
		t.Fatalf("reboot actions after the exit = %v, want unchanged", got)
	}
	// A live handle whose control connection closed under it handles the
	// failure by logging it.
	proc = newFakeProcess()
	wrapped = &qmpFakeProcess{fakeProcess: proc, client: client}
	if err := client.Close(); err != nil {
		t.Fatalf("close QMP client: %v", err)
	}
	h.mg.restoreRebootShutdown(wrapped, client, "bx_restore0")
}

func TestResetVMDeleteDrainsPromptly(t *testing.T) {
	pings := &resetvmGuest{entered: make(chan struct{}, 1), release: make(chan struct{})}
	env := resetvmSetup(t, resetvmOptions{wrapGuest: func(g guestv1.GuestServer) guestv1.GuestServer {
		pings.GuestServer = g
		return pings
	}})
	env.create()
	env.armWith(pings)

	resetDone := make(chan error, 1)
	go func() {
		_, err := env.h.mg.ResetVM(t.Context(), &nodev1.ResetVMRequest{SandboxId: env.id})
		resetDone <- err
	}()
	select {
	case <-pings.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the reset never reached the guest agent")
	}
	// The reset is parked in the guest answer; a delete must fence it and not
	// wait for the boot timeout.
	start := time.Now()
	if _, err := env.h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: env.id}); err != nil {
		t.Fatalf("DeleteVM during reset: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("DeleteVM during a reset took %v", elapsed)
	}
	select {
	case err := <-resetDone:
		resetvmWantCode(t, err, codes.Internal)
	case <-time.After(5 * time.Second):
		t.Fatal("ResetVM did not return after its VM was deleted")
	}
	if _, err := env.h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: env.id}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetVM after delete = %v, want NotFound", err)
	}
	select {
	case <-env.proc.Done():
	default:
		t.Fatal("the deleted VM's QEMU was not stopped")
	}
	// The reboot action was restored before the record went away.
	if got := env.srv.rebootActions(); !slices.Equal(got, []string{"reset", "shutdown"}) {
		t.Fatalf("reboot actions = %v, want reset then shutdown", got)
	}
}

func TestResetVMPreconditions(t *testing.T) {
	t.Run("unknown id", func(t *testing.T) {
		env := resetvmSetup(t, resetvmOptions{})
		_, err := env.h.mg.ResetVM(t.Context(), &nodev1.ResetVMRequest{SandboxId: "bx_unknown0"})
		resetvmWantCode(t, err, codes.NotFound)
	})
	t.Run("stopped", func(t *testing.T) {
		env := resetvmSetup(t, resetvmOptions{})
		env.create()
		env.srv.mu.Lock()
		env.srv.onPowerdown = func() { env.proc.exit(nil) }
		env.srv.mu.Unlock()
		if _, err := env.h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: env.id, GraceSeconds: 5}); err != nil {
			t.Fatalf("StopVM: %v", err)
		}
		_, err := env.h.mg.ResetVM(t.Context(), &nodev1.ResetVMRequest{SandboxId: env.id})
		resetvmWantCode(t, err, codes.FailedPrecondition)
	})
	t.Run("booting", func(t *testing.T) {
		pings := &resetvmGuest{entered: make(chan struct{}, 1), release: make(chan struct{})}
		pings.armed.Store(true) // hold the boot's own Ping
		env := resetvmSetup(t, resetvmOptions{wrapGuest: func(g guestv1.GuestServer) guestv1.GuestServer {
			pings.GuestServer = g
			return pings
		}})
		created := make(chan error, 1)
		go func() {
			_, err := env.h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
				SandboxId: env.id,
				Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB},
			})
			created <- err
		}()
		select {
		case <-pings.entered:
		case err := <-created:
			t.Fatalf("CreateVM returned before the boot ping: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("the boot never reached its guest ping")
		}
		_, err := env.h.mg.ResetVM(t.Context(), &nodev1.ResetVMRequest{SandboxId: env.id})
		resetvmWantCode(t, err, codes.FailedPrecondition)
		close(pings.release)
		if err := <-created; err != nil {
			t.Fatalf("CreateVM: %v", err)
		}
	})
	t.Run("stopping", func(t *testing.T) {
		env := resetvmSetup(t, resetvmOptions{})
		env.create()
		stopped := make(chan error, 1)
		go func() {
			_, err := env.h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: env.id, GraceSeconds: 1})
			stopped <- err
		}()
		resetvmWaitState(t, env.h.mg, env.id, nodev1.VMState_VM_STATE_STOPPING)
		_, err := env.h.mg.ResetVM(t.Context(), &nodev1.ResetVMRequest{SandboxId: env.id})
		resetvmWantCode(t, err, codes.FailedPrecondition)
		if err := <-stopped; err != nil {
			t.Fatalf("StopVM: %v", err)
		}
	})
	t.Run("failed without a process", func(t *testing.T) {
		env := resetvmSetup(t, resetvmOptions{})
		env.create()
		env.proc.exit(errors.New("injected QEMU exit"))
		resetvmWaitState(t, env.h.mg, env.id, nodev1.VMState_VM_STATE_FAILED)
		_, err := env.h.mg.ResetVM(t.Context(), &nodev1.ResetVMRequest{SandboxId: env.id})
		resetvmWantCode(t, err, codes.FailedPrecondition)
		// The fake QEMU has no reaper: close its QMP client so the manager's
		// event drain ends and the harness can shut down.
		if err := env.wrapped.Close(); err != nil {
			t.Fatalf("close the exited process: %v", err)
		}
	})
	t.Run("deleting", func(t *testing.T) {
		env := resetvmSetup(t, resetvmOptions{})
		env.create()
		gate, entered := make(chan struct{}), make(chan struct{}, 1)
		env.proc.stageStop(gate, entered)
		deleted := make(chan error, 1)
		go func() {
			_, err := env.h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: env.id})
			deleted <- err
		}()
		<-entered
		_, err := env.h.mg.ResetVM(t.Context(), &nodev1.ResetVMRequest{SandboxId: env.id})
		close(gate)
		resetvmWantCode(t, err, codes.NotFound)
		if err := <-deleted; err != nil {
			t.Fatalf("DeleteVM: %v", err)
		}
	})
}
