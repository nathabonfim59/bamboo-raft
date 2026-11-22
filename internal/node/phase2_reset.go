// Phase 2 ResetVM: a hardware reset of a running VM through QMP, followed by
// a fresh guest connection and the same wait a boot does.

package node

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// errResetAborted marks a reset that ended before the guest was reset (its
// last caller left, or the manager shut down): the record goes back to the
// state the reset found and nothing is published as a failure.
var errResetAborted = errors.New("node: reset aborted before the guest was reset")

// errResetFenced marks a reset DeleteVM aborted: the record is being removed,
// so the state this reset would publish no longer matters.
var errResetFenced = errors.New("node: reset fenced by DeleteVM")

// ResetVM power-cycles a READY VM, or a FAILED VM whose QEMU process still
// runs, and blocks until its guest agent answers again. The disk is untouched
// (a hardware reset) and the VM keeps its process, network and reservations.
//
// The reset belongs to the manager like a boot: it claims a fresh boot
// generation, so a joining CreateVM waits on it exactly like on a boot, and it
// is bounded by BootTimeout, not by the caller's context. The VM operation gate
// covers the machine reset itself — arming the reboot action and issuing
// system_reset — so that never overlaps a snapshot, a stop, a start or a
// maintenance session; the wait for the guest does not hold it, and the record
// reads BOOTING throughout, so every other operation refuses or joins instead
// of touching the VM. A canceled caller only stops waiting. When it was the
// last waiter the reset aborts before the guest is reset and the record returns
// to the state it had; once the guest has been reset the record can only end
// READY or FAILED (QEMU kept). DeleteVM fences a reset like a boot. Detached
// guest processes end up LOST: the guest agent rebuilds its registry when it
// restarts with the rebooted machine.
func (m *Manager) ResetVM(ctx context.Context, req *nodev1.ResetVMRequest) (*nodev1.VM, error) {
	if err := m.beginNodeOperation(); err != nil {
		return nil, err
	}
	defer m.wg.Done()
	v, err := m.lookup(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	v.mu.Lock()
	deleting := v.deleting
	v.mu.Unlock()
	if deleting {
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", v.id)
	}
	// Count the attempt before waiting for the gate, so a concurrent DeleteVM
	// drains it before returning OK, exactly like a start. The claim below
	// rechecks the record under the gate; a delete that won that race leaves
	// no record to reset.
	m.mu.Lock()
	m.creating[v.id]++
	m.mu.Unlock()
	defer m.createDone(v.id)

	if err := acquireVMOperation(ctx, v); err != nil {
		return nil, err
	}
	// A record whose restart recovery could not prove ownership must resolve
	// before the reset touches its QEMU.
	if err := m.requireResolved(v); err != nil {
		releaseVMOperation(v)
		return nil, err
	}
	gen, err := m.claimReset(v)
	releaseVMOperation(v)
	if err != nil {
		return nil, err
	}
	// The worker owns the reset now; the caller is an ordinary ticket holder
	// of its boot generation, outside the gate.
	return m.waitBoot(ctx, v, gen)
}

// claimReset validates a reset on a record whose operation gate the caller
// holds and claims the record's next boot generation for the reset worker.
// Claiming under the gate publishes BOOTING before anything can observe a
// half-started reset: every other operation refuses or joins from there on.
func (m *Manager) claimReset(v *vm) (gen uint64, err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.deleting {
		return 0, status.Errorf(codes.NotFound, "sandbox %s not found", v.id)
	}
	switch v.state {
	case nodev1.VMState_VM_STATE_READY, nodev1.VMState_VM_STATE_FAILED:
	default:
		return 0, status.Errorf(codes.FailedPrecondition, "sandbox %s is %s", v.id, vmStateName(v.state))
	}
	if v.process == nil || procDone(v.process) {
		return 0, status.Errorf(codes.FailedPrecondition, "sandbox %s has no running QEMU process", v.id)
	}
	if cmpQMP(v.snapshotClient, qmpOf(v.process)) == nil {
		return 0, status.Errorf(codes.FailedPrecondition, "sandbox %s has no QMP connection to its QEMU", v.id)
	}
	prev := v.state
	bootCtx, cancel := context.WithCancel(m.ctx)
	done := make(chan struct{})
	v.state = nodev1.VMState_VM_STATE_BOOTING
	v.bootDone = done
	v.bootErr = nil
	v.bootCancel = cancel
	v.stopDone = nil
	v.waiters = 1 // the reset caller's ticket on this generation
	v.bootGen++
	gen = v.bootGen
	m.wg.Go(func() { m.resetWorker(bootCtx, v, gen, prev, done) })
	return gen, nil
}

// resetWorker runs one claimed reset and publishes its terminal state under
// the same generation guard a boot worker uses: a record replaced by a newer
// generation is left alone, and the worker closes the channel it created.
func (m *Manager) resetWorker(ctx context.Context, v *vm, gen uint64, prev nodev1.VMState, done chan struct{}) {
	err := m.runReset(ctx, v, gen, prev)
	v.mu.Lock()
	if v.bootGen == gen {
		switch {
		case err == nil, errors.Is(err, errResetAborted), errors.Is(err, errResetFenced):
			// No failure to publish: READY was published by runReset, or the
			// record went back to the state it had.
			v.bootErr = nil
		case v.state == nodev1.VMState_VM_STATE_BOOTING:
			v.bootErr = err
			v.state = nodev1.VMState_VM_STATE_FAILED
			v.errMsg = "reset did not complete: " + errorText(err)
		}
	}
	close(done)
	v.mu.Unlock()
}

// runReset performs one reset: reset the machine under the VM operation gate,
// then dial the restarted agent and wait for it. The guest connection the VM
// had is dead as soon as the machine resets, so it is detached and closed here
// and a fresh one replaces it on success.
//
// The gate covers the machine reset (arm the reboot action, system_reset) and
// nothing else: the wait for the guest can take a boot timeout, and DeleteVM
// only fences a reset — cancel + wait for bootDone — after it holds the gate.
// Holding the gate across the wait would make a delete wait the whole timeout.
// The record reads BOOTING throughout, so every other operation refuses or
// joins instead of touching the VM.
func (m *Manager) runReset(ctx context.Context, v *vm, gen uint64, prev nodev1.VMState) error {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.BootTimeout)
	defer cancel()
	if m.beforeResetGate != nil {
		// Test seam: the record reads BOOTING and the worker has not taken
		// the gate yet.
		m.beforeResetGate()
	}
	if err := acquireVMOperation(ctx, v); err != nil {
		return m.abortReset(v, gen, prev, err)
	}
	released := false
	release := func() {
		if !released {
			released = true
			releaseVMOperation(v)
		}
	}
	defer release()

	v.mu.Lock()
	if v.deleting {
		v.mu.Unlock()
		return errResetFenced
	}
	if v.bootGen != gen {
		// A newer generation owns the record; it has nothing to restore.
		v.mu.Unlock()
		return errResetAborted
	}
	if err := ctx.Err(); err != nil {
		v.mu.Unlock()
		return m.abortReset(v, gen, prev, err)
	}
	proc, conn := v.process, v.conn
	qmpClient := cmpQMP(v.snapshotClient, qmpOf(proc))
	if proc != nil && !procDone(proc) {
		// The reset kills the guest agent with the machine: this connection
		// cannot serve anything from here on.
		v.conn, v.client = nil, nil
	}
	v.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	if proc == nil || procDone(proc) {
		return errors.New("the QEMU process exited before the reset")
	}
	if qmpClient == nil {
		return errors.New("QEMU has no QMP connection")
	}
	// QEMU's reboot action must go back to shutdown on every path from here
	// on, success or failure, as long as QEMU is alive.
	defer m.restoreRebootShutdown(proc, qmpClient, v.id)

	if err := qemu.Reset(ctx, qmpClient, v.id); err != nil {
		return fmt.Errorf("reset: %w", err)
	}
	// The machine is reset: let a delete through, and let the rest of this
	// call wait for the agent without the gate.
	release()
	// Past this point the guest has been reset: the record must end READY or
	// FAILED, never in the state the reset found.
	guestConn, err := m.dialer.Dial(ctx, v.meta.VsockCID)
	if err != nil {
		return fmt.Errorf("dial guest after reset: %w", err)
	}
	guest := guestv1.NewGuestClient(guestConn)
	v.mu.Lock()
	if v.deleting {
		v.mu.Unlock()
		guestConn.Close()
		return errResetFenced
	}
	v.conn, v.client = guestConn, guest
	v.mu.Unlock()

	if err := m.pingGuest(ctx, guest); err != nil {
		v.mu.Lock()
		if v.conn == guestConn {
			v.conn, v.client = nil, nil
		}
		v.mu.Unlock()
		guestConn.Close()
		return fmt.Errorf("guest did not answer after reset: %w", err)
	}

	now := m.now().UnixMilli()
	v.mu.Lock()
	if v.deleting {
		v.mu.Unlock()
		return errResetFenced
	}
	// The durable ready metadata is written before READY is published: a
	// record that reads READY must survive a node restart.
	if err := m.store.CommitReady(v.id, now); err != nil {
		v.mu.Unlock()
		return fmt.Errorf("record ready metadata: %w", err)
	}
	v.state = nodev1.VMState_VM_STATE_READY
	v.readyCommitted(now)
	v.errMsg = ""
	v.mu.Unlock()
	m.log.Info("node: vm reset", "sandbox", v.id, "started_at_unix_ms", now)
	return nil
}

// abortReset puts back the state a reset found when the reset ends before the
// guest was reset: a caller that left and a shutting-down manager must leave a
// usable record, not one stuck in BOOTING. It reports errResetAborted with the
// cause.
func (m *Manager) abortReset(v *vm, gen uint64, prev nodev1.VMState, cause error) error {
	v.mu.Lock()
	if v.bootGen == gen && !v.deleting {
		v.state = prev
	}
	v.mu.Unlock()
	return fmt.Errorf("%w: %v", errResetAborted, cause)
}

// restoreRebootShutdown puts QEMU's default reboot action back after a reset.
// It runs on a background context bounded by the cleanup timeout: the restore
// must not depend on the caller that asked for the reset, and a QEMU that is
// gone needs no restore. A failed restore is logged, not returned: the guest
// works, and only a later guest-initiated reboot would notice.
func (m *Manager) restoreRebootShutdown(proc VMProcess, client *qmp.Client, id string) {
	if client == nil || proc == nil || procDone(proc) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.SnapshotCleanupTimeout)
	defer cancel()
	if err := qemu.SetRebootAction(ctx, client, id, qemu.RebootShutdown); err != nil {
		m.log.Warn("node: restore QEMU reboot action after reset", "sandbox", id, "error", err)
	}
}
