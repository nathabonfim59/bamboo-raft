package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// reconcile applies the conservative restart policy to everything under
// vms/. Only directories named like sandbox ids are touched. A VM whose
// process is provably still alive (pidfd, exact owned argv, QMP peer pid and
// guest name) is adopted and listed again; anything else becomes a FAILED
// record with its private disk preserved until DeleteVM. Foreign names and
// unreadable manifests are quarantined and never killed or deleted.
func (m *Manager) reconcile(ctx context.Context) error {
	entries, err := m.store.ListVMDirs()
	if err != nil {
		return err
	}
	bootID, err := storage.HostBootID()
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.Kind != storage.VMKindOwned {
			m.log.Warn("node: foreign entry under vms/; leaving it alone", "name", e.Name)
			continue
		}
		meta, err := m.store.ReadVM(e.Name)
		if err != nil {
			if jerr := m.jailRefuseCorrupt(e.Name, err); jerr != nil {
				return jerr
			}
			// Before forgetting corrupt state, prove no live QEMU still uses
			// the owned paths. If one might, startup fails conservatively:
			// its CID and capacity are unknowable, so offering them to new
			// VMs could collide with a running guest.
			if m.qemuExe == "" {
				return fmt.Errorf("node: corrupt vm %s (manifest: %w) and the QEMU binary was not found; refusing to guess ownership", e.Name, err)
			}
			live, ferr := findOwnedQEMU(e.Name, m.store.VMDir(e.Name), m.store.DiskPath(e.Name), m.qemuExe)
			if ferr != nil {
				return fmt.Errorf("node: corrupt vm %s (manifest: %w): %w", e.Name, err, ferr)
			}
			if live != nil {
				live.close()
				return fmt.Errorf("node: corrupt vm %s (manifest: %w) still has a live process using its paths; resolve it before starting", e.Name, err)
			}
			m.log.Warn("node: unreadable vm manifest; quarantining", "sandbox", e.Name, "error", err)
			if _, qerr := m.store.Quarantine(e.Name); qerr != nil {
				m.log.Error("node: quarantine failed", "sandbox", e.Name, "error", qerr)
			}
			m.mu.Lock()
			m.quarantined[e.Name] = struct{}{}
			m.mu.Unlock()
			continue
		}
		if err := m.recoverVM(ctx, meta, bootID); err != nil {
			return err
		}
	}
	// Every record's fate is decided: settle the live-cut markers no
	// adoption recovered (phase2_launch.go).
	m.recoverCutsWithoutQEMU()
	return m.jailRefuseOrphans()
}

// recoverVM decides the fate of one readable VM record.
func (m *Manager) recoverVM(ctx context.Context, meta storage.VMMeta, bootID string) error {
	if m.jail != nil {
		return m.recoverJailed(ctx, meta, bootID)
	}
	if m.recoverMaintenance(ctx, meta, bootID) {
		// A maintenance QEMU is never booted or adopted (it runs before
		// any verifyQMPAt path, which would continue it).
		return nil
	}
	if meta.QEMU == nil {
		if meta.Stopped() {
			// A durably stopped VM: its kept disk is real inventory. No
			// process record exists, so there is nothing to prove dead.
			m.recoverStopped(meta)
			return nil
		}
		// The node crashed after creating the box but before recording a
		// launch intent. No QEMU can own the disk, so this is a safe
		// owned orphan.
		if err := m.store.RemoveVM(meta.SandboxID); err != nil {
			m.log.Warn("node: remove never-launched vm", "sandbox", meta.SandboxID, "error", err)
		}
		return nil
	}
	if m.scanOwned && m.qemuExe == "" {
		return fmt.Errorf("node: vm %s has a QEMU record but the QEMU binary was not found", meta.SandboxID)
	}
	if _, ok := m.images[meta.Image]; !ok {
		// The manifest names an image that is not installed. Never launch or
		// free capacity for it; a live process using the owned paths still
		// has to be accounted for conservatively.
		if m.qemuExe != "" {
			live, ferr := findOwnedQEMU(meta.SandboxID, m.store.VMDir(meta.SandboxID), m.store.DiskPath(meta.SandboxID), m.qemuExe)
			if ferr != nil {
				return fmt.Errorf("node: vm %s names an uninstalled image and its process cannot be identified: %w", meta.SandboxID, ferr)
			}
			if live != nil {
				live.close()
				return fmt.Errorf("node: vm %s names an uninstalled image %q but a live process uses its paths", meta.SandboxID, meta.Image)
			}
		}
		m.recoverFailed(meta, fmt.Sprintf("image %q is not installed", meta.Image))
		return nil
	}
	if meta.QEMU.BootID != bootID {
		// The recorded process belongs to a previous host boot and is gone;
		// a start that never reached READY returns to STOPPED, anything
		// else is a FAILED VM with its disk kept. The dead QEMU's socket
		// would refuse the next start.
		m.removeStaleQMPSocket(meta.SandboxID)
		if meta.StoppedAtUnixMs > 0 {
			m.recoverStoppedAfterInterrupt(meta, "node restarted during a start")
		} else {
			m.recoverFailed(meta, "node restarted: the VM process from a previous host boot is gone")
		}
		return nil
	}
	pinned, err := m.findProcess(meta)
	if err != nil {
		m.log.Warn("node: ambiguous vm process; not touching any process", "sandbox", meta.SandboxID, "error", err)
		m.recoverUnresolved(meta, "cannot identify the VM process safely: "+err.Error(), nil)
		return nil
	}
	if pinned == nil {
		// No owned process remains: the recorded QEMU is proven gone.
		m.removeStaleQMPSocket(meta.SandboxID)
		if meta.StoppedAtUnixMs > 0 {
			m.recoverStoppedAfterInterrupt(meta, "the VM process exited while the node was down")
		} else {
			m.recoverFailed(meta, "the VM process exited while the node was down")
		}
		return nil
	}
	client, err := m.verifyProc(ctx, pinned, meta)
	if err != nil {
		m.log.Warn("node: vm process failed verification; not killing it", "sandbox", meta.SandboxID, "error", err)
		m.recoverUnresolved(meta, "cannot verify the VM process: "+err.Error(), pinned)
		return nil
	}
	if m.net != nil {
		alloc, ok := m.networkAllocation(meta.SandboxID)
		if !ok {
			// A live QEMU from the legacy user-network mode (before production
			// TAP networking) has no durable network identity. Conservatively
			// refuse startup: never advertise it READY without its isolation,
			// never stop a healthy machine merely to migrate it, and never
			// fabricate a lease. Only the verification handles close; the
			// process and its disk stay for an explicit operator migration via
			// dev mode (docs/node.md, A10).
			client.Close()
			pinned.close()
			return fmt.Errorf("node: vm %s is live without a durable network allocation "+
				"(legacy user-mode networking); refusing startup under TAP networking — "+
				"leave it running and migrate it explicitly via dev mode", meta.SandboxID)
		}
		if err := tapArgvIdentity(pinned.info.argv, alloc); err != nil {
			// An allocation exists but the machine does not run exactly on it:
			// a user-mode launch, an extra backend or a different MAC would be
			// adopted as READY without the isolation the allocation stands
			// for. The same conservative refusal applies: only the
			// verification handles close, the healthy process and its disk
			// stay, and no lease is fabricated or TAP reopened.
			client.Close()
			pinned.close()
			return fmt.Errorf("node: vm %s is live with a NIC that does not match its "+
				"durable allocation: %v; refusing startup under TAP networking — "+
				"leave it running and migrate it explicitly via dev mode", meta.SandboxID, err)
		}
	}
	if meta.QEMU.State == storage.QEMULaunching {
		if err := m.store.CommitRunning(meta.SandboxID, pinned.pid); err != nil {
			client.Close()
			m.log.Warn("node: cannot record recovered identity", "sandbox", meta.SandboxID, "error", err)
			m.recoverUnresolved(meta, "cannot record recovered process identity: "+err.Error(), pinned)
			return nil
		}
	}
	m.adopt(ctx, meta, pinned, client)
	return nil
}

// recoverStopped re-registers a durably stopped VM: the disk is kept and
// accounted, the compute is free, and StartVM may boot it again.
func (m *Manager) recoverStopped(meta storage.VMMeta) {
	v := &vm{
		id:       meta.SandboxID,
		meta:     meta,
		state:    nodev1.VMState_VM_STATE_STOPPED,
		bootDone: make(chan struct{}),
	}
	close(v.bootDone)
	m.mu.Lock()
	if m.vms[meta.SandboxID] != nil {
		m.mu.Unlock()
		return
	}
	m.reserveStoppedDiskLocked(v)
	m.vms[meta.SandboxID] = v
	m.mu.Unlock()
	m.log.Info("node: recovered stopped vm", "sandbox", meta.SandboxID, "disk_gib", meta.Resources.DiskGiB)
}

// recoverStoppedAfterInterrupt settles a start that a node restart
// interrupted: the process is proven gone, so the kept disk is flushed, the
// manifest's stop marker is restored and the VM is STOPPED again rather than
// FAILED — its disk was kept and nothing of the start survives. If the disk
// or the record cannot be made durably stopped the VM is FAILED with its
// kept disk still accounted, and a StopVM retry completes the settlement.
func (m *Manager) recoverStoppedAfterInterrupt(meta storage.VMMeta, why string) {
	var persistErr error
	if err := m.store.SyncVMData(meta.SandboxID); err != nil {
		persistErr = fmt.Errorf("flush disk: %w", err)
	}
	if persistErr == nil {
		if err := m.store.CommitStopped(meta.SandboxID, m.now().UnixMilli()); err != nil {
			persistErr = fmt.Errorf("record stopped: %w", err)
		}
	}
	if persistErr != nil {
		msg := "cannot record interrupted start as stopped: " + persistErr.Error()
		m.log.Warn("node: settle interrupted start", "sandbox", meta.SandboxID, "error", persistErr)
		m.recoverFailed(meta, msg)
		// The kept disk of this interrupted start stays accounted until it
		// is durably stopped or deleted.
		m.mu.Lock()
		if v := m.vms[meta.SandboxID]; v != nil {
			m.reserveStoppedDiskLocked(v)
		}
		m.mu.Unlock()
		return
	}
	meta.QEMU = nil
	m.recoverStopped(meta)
	m.log.Info("node: start interrupted; vm stopped again", "sandbox", meta.SandboxID, "reason", why)
}

// adopt takes ownership of a verified running QEMU and waits for its guest to
// answer. A guest that does not answer is stopped (identity is proven by now)
// and the disk is preserved as FAILED.
func (m *Manager) adopt(ctx context.Context, meta storage.VMMeta, pinned *pinnedProc, qmpClient *qmp.Client) {
	m.adoptRef(ctx, meta, pinned, qmpClient, jailer.Ref{})
}

// adoptRef is adopt for a process whose jail reservation ref owns (zero for
// an unjailed adoption).
func (m *Manager) adoptRef(ctx context.Context, meta storage.VMMeta, pinned *pinnedProc, qmpClient *qmp.Client, ref jailer.Ref) {
	proc := newAdoptedProcess(meta.SandboxID, pinned, qmpClient)
	v := &vm{
		id:       meta.SandboxID,
		meta:     meta,
		state:    nodev1.VMState_VM_STATE_BOOTING,
		process:  proc,
		bootDone: make(chan struct{}),
		jail:     ref,
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		stopProcess(proc)
		return
	}
	// A live VM is real usage even when the configured offer shrank. Force
	// its accounting so CreateVM offers nothing extra.
	m.reserveForceLocked(v)
	reserved := m.reserved
	m.vms[meta.SandboxID] = v
	m.mu.Unlock()
	if reserved.vcpus > m.capacity.vcpus || reserved.memoryMib > m.capacity.memoryMib || reserved.diskGiB > m.capacity.diskGiB {
		m.log.Warn("node: recovered vm exceeds configured capacity; refusing new work", "sandbox", meta.SandboxID)
	}

	m.wg.Go(func() {
		proc.watch(m.ctx)
	})
	m.wg.Go(func() {
		m.watch(v, proc)
	})
	m.drains.Go(func() {
		drainEvents(proc)
	})

	probeCtx, cancel := context.WithTimeout(m.ctx, m.cfg.BootTimeout)
	defer cancel()
	conn, err := m.dialer.Dial(probeCtx, meta.VsockCID)
	if err == nil {
		guest := guestv1.NewGuestClient(conn)
		v.mu.Lock()
		v.conn, v.client = conn, guest
		v.mu.Unlock()
		err = m.pingGuest(probeCtx, guest)
		if err == nil {
			// A node crash mid-save can leave the guest filesystem frozen
			// with a lease token this new process never saw. Ping still
			// succeeds while frozen, so READY requires proving a thaw first.
			err = m.recoverGuestFreeze(probeCtx, guest)
		}
	}
	if err != nil {
		m.settleAdoptedFailure(v, meta, proc, conn, err)
		return
	}
	// Commit the ready metadata durably before (and under the same lock as)
	// publishing READY, exactly like runBoot's terminal transition: a stop
	// that observes the adopted VM READY may then CommitStopped without a
	// late ready-write racing the stop marker away. A node restart is not a
	// new start, though: an adopted VM that was READY before the crash keeps
	// its recorded last-start time. Only an interrupted StartVM that becomes
	// READY just now stamps the fresh time, and an old record that predates
	// the timestamp is migrated to now.
	startedAt := meta.StartedAtUnixMs
	if meta.StoppedAtUnixMs > 0 || startedAt == 0 {
		startedAt = m.now().UnixMilli()
	}
	v.mu.Lock()
	if cerr := m.store.CommitReady(v.id, startedAt); cerr != nil {
		v.mu.Unlock()
		m.settleAdoptedFailure(v, meta, proc, conn, fmt.Errorf("record ready metadata: %w", cerr))
		return
	}
	v.state = nodev1.VMState_VM_STATE_READY
	v.readyCommitted(startedAt)
	close(v.bootDone)
	v.mu.Unlock()
	m.log.Info("node: recovered running vm", "sandbox", meta.SandboxID, "pid", pinned.pid)
}

// settleAdoptedFailure stops an adopted VM that did not become ready (or
// whose ready metadata could not be committed) and settles the record: back
// to STOPPED when the recovered start never completed and the kept disk is
// flushed and durably recorded, otherwise FAILED. A stop whose process
// death, disk flush or manifest write is unproven keeps the full
// reservation.
func (m *Manager) settleAdoptedFailure(v *vm, meta storage.VMMeta, proc VMProcess, conn *grpc.ClientConn, cause error) {
	if conn != nil {
		conn.Close()
	}
	v.mu.Lock()
	v.conn, v.client = nil, nil
	v.mu.Unlock()
	stopErr := stopProcess(proc)
	var jailErr error
	if stopErr == nil {
		// The adopted attempt's jail is released after its proven stop and
		// before any accounting moves; a retained jail keeps everything.
		jailErr = m.settleJailDetached(v, proc)
	}
	// A VM that was stopped before its interrupted start returns to STOPPED
	// (the start simply never completed); anything else is a FAILED record
	// with its disk kept.
	toStopped := meta.StoppedAtUnixMs > 0 && stopErr == nil && jailErr == nil
	v.mu.Lock()
	switch {
	case toStopped:
		v.state = nodev1.VMState_VM_STATE_STOPPED
		v.errMsg = ""
	case jailErr != nil:
		v.state = nodev1.VMState_VM_STATE_FAILED
		v.errMsg = fmt.Sprintf("recovered vm did not become ready: %v; releasing its jail failed (retry with StopVM or DeleteVM): %v", cause, jailErr)
	default:
		v.state = nodev1.VMState_VM_STATE_FAILED
		v.errMsg = fmt.Sprintf("recovered vm did not become ready: %v", cause)
	}
	if stopErr == nil {
		v.process = nil
	}
	// Persist under v.mu like the death watcher: an old recovery attempt
	// must never overwrite the manifest of a same-id replacement. The kept
	// disk is flushed before the stopped marker is committed — the adopted
	// QEMU was just killed and its writes must reach the disk first.
	if m.beforePersist != nil {
		m.beforePersist()
	}
	var persistErr error
	// settleFailed reports that flushing or recording STOPPED failed for a
	// VM whose stop WAS proven: like an unproven stop, the whole
	// reservation stays.
	settleFailed := false
	if toStopped {
		if err := m.store.SyncVMData(v.id); err != nil {
			persistErr = fmt.Errorf("flush disk: %w", err)
		}
		if persistErr == nil {
			if err := m.store.CommitStopped(v.id, m.now().UnixMilli()); err != nil {
				persistErr = fmt.Errorf("record stopped: %w", err)
			}
		}
		if persistErr != nil {
			// Not durably stopped: FAILED with everything retained, the
			// same durability rule as runStop.
			settleFailed = true
			v.state = nodev1.VMState_VM_STATE_FAILED
			v.errMsg = "recovered start could not be recorded as stopped: " + persistErr.Error()
		}
	} else {
		persistErr = m.store.SetVMError(v.id, v.errMsg)
	}
	close(v.bootDone)
	v.mu.Unlock()
	m.mu.Lock()
	switch {
	case stopErr != nil || jailErr != nil || settleFailed:
		// Death unproven, jail retained or stop not durably recorded: keep
		// the whole reservation; a StopVM retry or DeleteVM settles it.
	case !toStopped:
		m.releaseReservationLocked(v)
	default:
		m.releaseComputeLocked(v)
		m.reserveStoppedDiskLocked(v)
	}
	m.mu.Unlock()
	m.log.Warn("node: recovered vm stopped", "sandbox", meta.SandboxID, "error", cause, "stop", stopErr, "jail", jailErr, "returned_to_stopped", toStopped)
	if persistErr != nil {
		m.log.Warn("node: persist recovered failure", "sandbox", v.id, "error", persistErr)
	}
}

// recoverGuestFreeze discovers any freeze lease a surviving guest agent still
// holds and thaws it before the recovered VM becomes READY. Freeze is
// idempotent: an unfrozen agent gets a fresh lease that Thaw then clears, and a
// frozen one returns the surviving token. Bounded independent retries make a
// lost or ambiguous reply safe because readiness requires a proven Thaw.
func (m *Manager) recoverGuestFreeze(ctx context.Context, guest guestv1.GuestClient) error {
	cleanupCtx, cancel := context.WithTimeout(ctx, m.cfg.SnapshotCleanupTimeout)
	defer cancel()
	leaseMs := uint32(m.cfg.SnapshotLease / time.Millisecond)
	var token string
	var lastErr error
	for token == "" {
		resp, err := guest.Freeze(cleanupCtx, &guestv1.FreezeRequest{LeaseMs: leaseMs})
		if err == nil && resp.GetLeaseToken() != "" {
			token = resp.GetLeaseToken()
			break
		}
		if err == nil {
			err = errors.New("guest Freeze returned an empty lease token")
		}
		lastErr = err
		select {
		case <-cleanupCtx.Done():
			return fmt.Errorf("discover surviving freeze: %w", errors.Join(cleanupCtx.Err(), lastErr))
		case <-time.After(50 * time.Millisecond):
		}
	}
	for {
		_, err := guest.Thaw(cleanupCtx, &guestv1.ThawRequest{LeaseToken: token})
		if err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-cleanupCtx.Done():
			return fmt.Errorf("thaw recovered guest: %w", errors.Join(cleanupCtx.Err(), lastErr))
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// resolveUnresolved proves that no live owned process remains for a record
// whose recovery could not decide ownership. It never signals a process. A
// live or ambiguous owner blocks destructive cleanup.
func (m *Manager) resolveUnresolved(v *vm) error {
	if handled, err := m.resolveJailUnresolved(v); handled {
		return err
	}
	v.mu.Lock()
	unresolved := v.ownershipUnresolved
	pinned := v.unresolved
	meta := v.meta
	v.mu.Unlock()
	if !unresolved {
		return nil
	}
	if pinned != nil {
		// Only ESRCH from the pidfd proves the process is gone; any other
		// error leaves ownership unresolved and keeps the protection.
		alive, err := pinned.state()
		if err != nil {
			return fmt.Errorf("pidfd state: %w", err)
		}
		if alive {
			return fmt.Errorf("process %d still runs", pinned.pid)
		}
		pinned.close()
		v.mu.Lock()
		v.unresolved = nil
		v.ownershipUnresolved = false
		v.mu.Unlock()
		return nil
	}
	pinned, err := m.findProcess(meta)
	if err != nil {
		return err
	}
	if pinned != nil {
		pinned.close()
		return errors.New("a live process still claims this vm")
	}
	v.mu.Lock()
	v.ownershipUnresolved = false
	v.mu.Unlock()
	return nil
}

// recoverFailed records a FAILED VM without touching any process or removing
// its disk. GetVM and ListVMs report it until DeleteVM.
func (m *Manager) recoverFailed(meta storage.VMMeta, msg string) {
	m.addRecovered(meta, msg, nil, false)
}

// recoverUnresolved keeps the record (and any pinned handle) in a state that
// blocks DeleteVM's destructive path until the process is proven gone.
func (m *Manager) recoverUnresolved(meta storage.VMMeta, msg string, pinned *pinnedProc) {
	m.addRecovered(meta, msg, pinned, true)
}

func (m *Manager) addRecovered(meta storage.VMMeta, msg string, pinned *pinnedProc, unresolved bool) {
	m.addRecoveredRef(meta, msg, pinned, unresolved, jailer.Ref{})
}

func (m *Manager) addRecoveredRef(meta storage.VMMeta, msg string, pinned *pinnedProc, unresolved bool, ref jailer.Ref) {
	v := &vm{
		id:                  meta.SandboxID,
		meta:                meta,
		state:               nodev1.VMState_VM_STATE_FAILED,
		errMsg:              msg,
		bootDone:            make(chan struct{}),
		ownershipUnresolved: unresolved,
		unresolved:          pinned,
		jail:                ref,
	}
	close(v.bootDone)
	m.mu.Lock()
	if m.vms[meta.SandboxID] != nil {
		m.mu.Unlock()
		if pinned != nil {
			pinned.close()
		}
		return
	}
	if unresolved {
		// Account possible live usage conservatively; DeleteVM releases it
		// only after proving the process absent.
		m.reserveForceLocked(v)
	}
	m.vms[meta.SandboxID] = v
	m.mu.Unlock()
	if err := m.store.SetVMError(meta.SandboxID, msg); err != nil {
		m.log.Warn("node: persist failure", "sandbox", meta.SandboxID, "error", err)
	}
	m.log.Warn("node: vm marked failed", "sandbox", meta.SandboxID, "error", msg, "unresolved", unresolved)
}
