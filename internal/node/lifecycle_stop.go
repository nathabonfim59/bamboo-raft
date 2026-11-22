package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// DefaultStopGrace bounds StopVM's graceful phase when the request leaves
// grace_seconds at zero, matching the frozen contract's documented default.
const DefaultStopGrace = 30 * time.Second

// StopVM shuts a VM down and keeps its disk. The guest gets up to
// grace_seconds to honor an ACPI power-off request; then QEMU is killed and
// reaped, the disk is flushed and the STOPPED record is made durable. A
// STOPPED VM holds no vCPU or memory capacity, only its disk.
func (m *Manager) StopVM(ctx context.Context, req *nodev1.StopVMRequest) (*nodev1.VM, error) {
	grace := time.Duration(req.GetGraceSeconds()) * time.Second
	if req.GetGraceSeconds() == 0 {
		grace = DefaultStopGrace
	}
	if err := m.beginNodeOperation(); err != nil {
		return nil, err
	}
	defer m.wg.Done()
	v, err := m.lookup(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	for {
		v.mu.Lock()
		if v.deleting {
			v.mu.Unlock()
			return nil, status.Errorf(codes.NotFound, "sandbox %s not found", v.id)
		}
		switch v.state {
		case nodev1.VMState_VM_STATE_STOPPED:
			// A record whose restart recovery could not prove ownership is
			// never vouched for: it takes the gate and resolves first.
			if !v.ownershipUnresolved {
				out := v.cloneVM()
				v.mu.Unlock()
				return out, nil
			}
		case nodev1.VMState_VM_STATE_BOOTING:
			// CreateVM or StartVM is in flight; the frozen contract makes
			// the caller retry after the boot ends.
			v.mu.Unlock()
			return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s is BOOTING; retry after the boot ends", v.id)
		case nodev1.VMState_VM_STATE_STOPPING:
			stopDone := v.stopDone
			v.mu.Unlock()
			select {
			case <-stopDone:
			case <-ctx.Done():
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			// The attempt finished: re-evaluate (STOPPED, or a retryable
			// failure this caller may own next).
			continue
		}
		v.mu.Unlock()
		// READY, FAILED or a STOPPED-looking record whose ownership is
		// unresolved: serialize with snapshots, starts and deletes.
		if err := acquireVMOperation(ctx, v); err != nil {
			return nil, err
		}
		out, err := m.ownStop(v, grace)
		releaseVMOperation(v)
		return out, err
	}
}

// ownStop runs one stop attempt while holding the VM operation gate. The
// state is re-checked after the gate: a stop, start or save may have finished
// while this caller waited for it.
func (m *Manager) ownStop(v *vm, grace time.Duration) (*nodev1.VM, error) {
	// An unresolved record stops only after its QEMU is proven gone or owned;
	// otherwise a nil process would commit STOPPED while that QEMU may still
	// be writing the disk.
	if err := m.requireResolved(v); err != nil {
		return nil, err
	}
	v.mu.Lock()
	switch {
	case v.deleting:
		v.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", v.id)
	case v.state == nodev1.VMState_VM_STATE_STOPPED:
		out := v.cloneVM()
		v.mu.Unlock()
		return out, nil
	case v.state == nodev1.VMState_VM_STATE_BOOTING:
		v.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s is BOOTING; retry after the boot ends", v.id)
	case v.state == nodev1.VMState_VM_STATE_STOPPING:
		v.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "sandbox %s: another stop holds the operation gate", v.id)
	}
	// STOPPING is visible before any ACPI request, and stopDone exists
	// before the state flip, so a concurrent StopVM or StartVM that observes
	// STOPPING always has a channel to wait on. The owner closes exactly
	// the channel it created, never whatever a later attempt installed.
	stopDone := make(chan struct{})
	v.stopDone = stopDone
	v.state = nodev1.VMState_VM_STATE_STOPPING
	v.mu.Unlock()

	err := m.runStop(v, grace)

	v.mu.Lock()
	close(stopDone)
	state := v.state
	var out *nodev1.VM
	if state == nodev1.VMState_VM_STATE_STOPPED {
		out = v.cloneVM()
	}
	v.mu.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "stop %s: %v", v.id, err)
	}
	if out == nil {
		return nil, status.Errorf(codes.Internal, "stop %s: state is %s after the stop", v.id, vmStateName(state))
	}
	return out, nil
}

// runStop performs one stop. The process handles are detached under vm.mu
// first, so the QEMU-exit watcher can no longer mark the VM FAILED once the
// stop owns it; the guest then gets its grace window and QEMU is killed and
// reaped. Only after the disk and manifest are durably synced does the
// STOPPED state publish and the compute reservation release.
func (m *Manager) runStop(v *vm, grace time.Duration) error {
	v.mu.Lock()
	proc := v.process
	conn := v.conn
	snapshotClient := v.snapshotClient
	v.process = nil
	v.conn, v.client = nil, nil
	v.snapshotClient = nil
	v.mu.Unlock()
	if snapshotClient != nil {
		snapshotClient.Close()
	}
	if conn != nil {
		conn.Close()
	}
	if proc != nil {
		if err := gracefulStop(proc, grace); err != nil {
			// Absence is unproven. The guest connection is already closed, so
			// READY would promise a working proxy it cannot deliver; the
			// record goes to FAILED with the process handle and the whole
			// reservation kept (failBoot's stop-error precedent). A retry
			// stops it again; DeleteVM is unaffected.
			msg := "stop did not complete: " + err.Error()
			v.mu.Lock()
			v.process = proc
			v.state = nodev1.VMState_VM_STATE_FAILED
			v.errMsg = msg
			perr := m.store.SetVMError(v.id, msg)
			v.mu.Unlock()
			return errors.Join(err, perr)
		}
	}

	// QEMU is proven gone (or never ran). The jail and then the network go
	// before the durable stop record: a record that reads STOPPED never holds
	// jail or network resources, and a failed release keeps the FAILED record
	// with its whole reservation (a StopVM retry re-runs this settlement
	// idempotently, by attempt identity).
	if err := m.settleJailDetached(v, proc); err != nil {
		msg := "stop did not release jail: " + err.Error()
		v.mu.Lock()
		v.state = nodev1.VMState_VM_STATE_FAILED
		v.errMsg = msg
		perr := m.store.SetVMError(v.id, msg)
		v.mu.Unlock()
		return errors.Join(err, perr)
	}
	// A jailed image tool that still holds the disk keeps the FAILED record,
	// the disk, any allocation and the whole reservation, like a retained
	// jail; a StopVM retry settles once the tool is settled.
	if err := m.diskHeldByTool(v.id); err != nil {
		msg := "stop did not complete: " + err.Error()
		v.mu.Lock()
		v.state = nodev1.VMState_VM_STATE_FAILED
		v.errMsg = msg
		perr := m.store.SetVMError(v.id, msg)
		v.mu.Unlock()
		return errors.Join(err, perr)
	}
	if err := m.releaseNetwork(v.id); err != nil {
		msg := "stop did not release network: " + err.Error()
		v.mu.Lock()
		v.state = nodev1.VMState_VM_STATE_FAILED
		v.errMsg = msg
		perr := m.store.SetVMError(v.id, msg)
		v.mu.Unlock()
		return errors.Join(err, perr)
	}

	// QEMU is gone (or never ran). Flush the kept disk and persist STOPPED
	// before publishing the state or releasing compute: a caller must never
	// observe STOPPED, or reusable capacity, that a crash would forget.
	var persistErr error
	if err := m.store.SyncVMData(v.id); err != nil {
		persistErr = fmt.Errorf("flush disk: %w", err)
	}
	if persistErr == nil {
		if err := m.store.CommitStopped(v.id, m.now().UnixMilli()); err != nil {
			persistErr = fmt.Errorf("record stopped: %w", err)
		}
	}
	if persistErr != nil {
		// Not durably stopped: report a FAILED record with the reason and
		// keep the whole reservation, like any other unproven cleanup.
		// StopVM of a FAILED VM retries the whole stop.
		msg := "stop did not persist: " + persistErr.Error()
		v.mu.Lock()
		v.state = nodev1.VMState_VM_STATE_FAILED
		v.errMsg = msg
		perr := m.store.SetVMError(v.id, msg)
		v.mu.Unlock()
		return errors.Join(persistErr, perr)
	}

	m.mu.Lock()
	v.mu.Lock()
	// The exit watcher may have released everything while QEMU was dying; a
	// stopped VM keeps its disk accounted regardless of who released it.
	// Accounting moves before (and atomically with) the terminal publish, so
	// a record that reads STOPPED is always fully settled.
	m.releaseComputeLocked(v)
	m.reserveStoppedDiskLocked(v)
	v.state = nodev1.VMState_VM_STATE_STOPPED
	v.errMsg = ""
	// A completed stop settles the record: a boot waiter arriving now is a
	// joiner of a stopped VM, not a survivor of an aborted start.
	v.bootErr = nil
	v.mu.Unlock()
	m.mu.Unlock()
	m.log.Info("node: vm stopped", "sandbox", v.id, "grace", grace)
	return nil
}

// gracefulStop asks the guest for an ACPI power-off, waits out the grace and
// then kills and reaps QEMU. The ACPI request and the shutdown wait share one
// overall grace budget — the QMP attempt is capped by min(grace, 5s) and only
// the remaining time is waited — so a stop never exceeds the caller's grace.
// The kill fallback is bounded independently of any request context: once a
// stop owns the VM it runs to completion.
func gracefulStop(proc VMProcess, grace time.Duration) error {
	// Best effort: processes without a QMP control connection (test fakes,
	// an already-dead controller) skip straight to the forced stop.
	deadline := time.Now().Add(grace)
	powerCtx, cancel := context.WithTimeout(context.Background(), min(grace, 5*time.Second))
	_ = requestPowerDown(powerCtx, proc)
	cancel()
	if wait := time.Until(deadline); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-proc.Done():
			return stopProcess(proc)
		case <-timer.C:
		}
	}
	return stopProcess(proc)
}

// StartVM boots a STOPPED VM from its kept disk and blocks until its guest
// answers, with CreateVM's cancellation and DeleteVM-fencing rules.
func (m *Manager) StartVM(ctx context.Context, req *nodev1.StartVMRequest) (*nodev1.VM, error) {
	id := req.GetSandboxId()
	if err := m.beginNodeOperation(); err != nil {
		return nil, err
	}
	defer m.wg.Done()
	v, err := m.lookup(id)
	if err != nil {
		return nil, err
	}
	// Register as a launch attempt before doing any work, so a concurrent
	// DeleteVM fence drains this start before returning OK.
	m.mu.Lock()
	if _, fenced := m.fence[id]; fenced {
		m.mu.Unlock()
		return nil, status.Errorf(codes.Aborted, "sandbox %s is being deleted", id)
	}
	m.creating[id]++
	m.mu.Unlock()
	defer m.createDone(id)

	for {
		v.mu.Lock()
		if v.deleting {
			v.mu.Unlock()
			return nil, status.Errorf(codes.NotFound, "sandbox %s not found", id)
		}
		switch v.state {
		case nodev1.VMState_VM_STATE_READY:
			proc, client := v.process, v.client
			out := v.cloneVM()
			v.mu.Unlock()
			// The fast path never returns READY for a dead process: the exit
			// watcher may be serialized behind the operation gate (its
			// settlement holds it) or descheduled. The caller retries and
			// observes the published terminal state instead.
			if proc != nil && procDone(proc) {
				return nil, status.Errorf(codes.FailedPrecondition,
					"sandbox %s's process exited; its state is settling, retry", id)
			}
			if client == nil {
				return nil, status.Errorf(codes.FailedPrecondition,
					"sandbox %s is READY but has no guest connection", id)
			}
			return out, nil
		case nodev1.VMState_VM_STATE_BOOTING:
			v.mu.Unlock()
			return m.waitBoot(ctx, v, joinerTicket)
		case nodev1.VMState_VM_STATE_STOPPING:
			stopDone := v.stopDone
			v.mu.Unlock()
			select {
			case <-stopDone:
			case <-ctx.Done():
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			continue
		case nodev1.VMState_VM_STATE_FAILED:
			v.mu.Unlock()
			return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s is FAILED; stop it first", id)
		}
		v.mu.Unlock()
		if m.beforeStartGate != nil {
			// Test seam: this caller has observed STOPPED but does not hold
			// the operation gate yet.
			m.beforeStartGate()
		}
		if err := acquireVMOperation(ctx, v); err != nil {
			return nil, err
		}
		// A STOPPED-looking record whose restart recovery could not prove
		// ownership must resolve first: booting it could put a second QEMU
		// on the same disk.
		if err := m.requireResolved(v); err != nil {
			releaseVMOperation(v)
			return nil, err
		}
		out, claimGen, err := m.ownStart(v, req)
		releaseVMOperation(v)
		if errors.Is(err, errStartRetry) {
			continue
		}
		if err != nil || out != nil {
			return out, err
		}
		// The boot was claimed and its worker spawned: wait outside the
		// operation gate, exactly like a create, so deletes, stops, saves
		// and joining starters are never fenced behind this caller. The
		// claim bound this caller's ticket to claimGen, so even a starter
		// descheduled until after a whole later boot generation cannot
		// touch that generation's waiters.
		return m.waitBoot(ctx, v, claimGen)
	}
}

// errStartRetry tells StartVM the record changed while it waited for the
// operation gate (a stop finished, another start launched, a save marked the
// VM FAILED): the caller re-evaluates the state instead of guessing.
var errStartRetry = errors.New("node: start state changed")

// ownStart claims a STOPPED record and launches one boot, returning
// (nil, nil) once the boot worker exists (the caller then waits outside the
// operation gate). Validation, admission and the claim — the reservation
// apply (compute in full, the disk growth delta), the BOOTING flip and the
// boot channel reset — happen in one critical section BEFORE the durable
// disk phase: the irreversible growth only runs once its capacity is spent
// under the same lock every other admission uses, so a concurrent create or
// start can never take the room the growth needs. The durable phase
// (manifest replacement, growth) then runs with the claim held, and a
// failure leaves a durably STOPPED VM with its kept disk intact and nothing
// half-launched.
func (m *Manager) ownStart(v *vm, req *nodev1.StartVMRequest) (_ *nodev1.VM, claimGen uint64, err error) {
	m.mu.Lock()
	if _, fenced := m.fence[v.id]; fenced {
		m.mu.Unlock()
		return nil, 0, status.Errorf(codes.Aborted, "sandbox %s is being deleted", v.id)
	}
	v.mu.Lock()
	if v.deleting {
		v.mu.Unlock()
		m.mu.Unlock()
		return nil, 0, status.Errorf(codes.Aborted, "sandbox %s was deleted while starting", v.id)
	}
	if v.state != nodev1.VMState_VM_STATE_STOPPED || m.vms[v.id] != v {
		v.mu.Unlock()
		m.mu.Unlock()
		// Another starter won while this caller queued for the gate (or a
		// stop finished): the caller's state loop maps the outcome — a
		// joined BOOTING, the idempotent READY, a waited stop — instead of
		// guessing a precondition here.
		return nil, 0, errStartRetry
	}

	// Replacement fields are validated only on the owner path for a STOPPED
	// record — the frozen contract's existing-id precedence: READY returns
	// at once and BOOTING joins whatever the fields say, so their validity
	// is only this caller's business once it is about to apply them.
	if req.GetResources() != nil {
		if err := validateResources(req.GetResources()); err != nil {
			v.mu.Unlock()
			m.mu.Unlock()
			return nil, 0, err
		}
	}
	if err := validHostname(req.GetHostname()); err != nil {
		v.mu.Unlock()
		m.mu.Unlock()
		return nil, 0, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	// Sizing: unchanged when the request names no resources; a smaller disk
	// is rejected before anything is touched.
	res := v.meta.Resources
	if r := req.GetResources(); r != nil {
		if r.GetDiskGib() < v.meta.Resources.DiskGiB {
			v.mu.Unlock()
			m.mu.Unlock()
			return nil, 0, status.Errorf(codes.InvalidArgument,
				"disk_gib %d is smaller than the kept disk %d GiB", r.GetDiskGib(), v.meta.Resources.DiskGiB)
		}
		if r.GetVcpus() > m.hostVCPUs {
			v.mu.Unlock()
			m.mu.Unlock()
			return nil, 0, status.Errorf(codes.ResourceExhausted,
				"vcpus %d exceeds the host's %d CPUs", r.GetVcpus(), m.hostVCPUs)
		}
		res = storageResources(r)
	}
	want := resourcesOf(res)
	if _, ok := want.diskBytes(); !ok {
		v.mu.Unlock()
		m.mu.Unlock()
		return nil, 0, status.Errorf(codes.ResourceExhausted, "disk_gib %d is too large", res.DiskGiB)
	}
	// Admission compares what the node would hold after the start. The kept
	// disk is already counted in m.reserved (STOPPED VMs keep disk
	// accounting), so the compute is added in full and the disk only as the
	// growth delta.
	delta := resources{vcpus: want.vcpus, memoryMib: want.memoryMib, diskGiB: want.diskGiB - v.meta.Resources.DiskGiB}
	total, ok := m.reserved.add(delta)
	if !ok || !total.fits(m.capacity) {
		v.mu.Unlock()
		m.mu.Unlock()
		return nil, 0, status.Error(codes.ResourceExhausted, "no room for this VM")
	}
	free, ferr := m.diskFree()
	if ferr != nil {
		v.mu.Unlock()
		m.mu.Unlock()
		return nil, 0, status.Errorf(codes.Internal, "disk free: %v", ferr)
	}
	promise, ferr := m.diskPromiseBytes(free)
	if ferr != nil {
		v.mu.Unlock()
		m.mu.Unlock()
		return nil, 0, status.Errorf(codes.Internal, "disk promise: %v", ferr)
	}
	reservedBytes, ok := total.diskBytes()
	if !ok || reservedBytes > promise {
		v.mu.Unlock()
		m.mu.Unlock()
		return nil, 0, status.Error(codes.ResourceExhausted, "not enough free disk for this VM")
	}
	img, ok := m.images[v.meta.Image]
	if !ok {
		v.mu.Unlock()
		m.mu.Unlock()
		return nil, 0, status.Errorf(codes.Internal, "sandbox %s: image %q disappeared", v.id, v.meta.Image)
	}

	// Claim: nothing durable has changed yet, so the pre-claim meta is the
	// restore point for a durable phase that fails before anything
	// persisted.
	original := v.meta
	bootCtx, cancel := context.WithCancel(m.ctx)
	v.meta.Resources = res
	v.meta.Env = maps.Clone(req.GetEnv())
	if req.GetHostname() != "" {
		v.meta.Hostname = req.GetHostname()
	}
	v.state = nodev1.VMState_VM_STATE_BOOTING
	v.bootDone = make(chan struct{})
	v.bootErr = nil
	v.bootCancel = cancel
	v.stopDone = nil
	v.waiters = 1 // the starter's ticket on this generation
	v.bootGen++
	claimGen = v.bootGen
	v.startKeep = true
	v.startOriginal = original
	m.reserved = total
	v.computeHeld = true
	v.mu.Unlock()
	m.mu.Unlock()

	// Everything after the claim — the durable disk phase and the boot —
	// belongs to the bounded shared worker. The starter immediately becomes
	// an ordinary generation-ticketed waiter, so its cancellation returns
	// at once and only ever removes its ticket: the last waiter leaving
	// cancels a blocked Grow, a surviving waiter keeps the boot alive.
	m.wg.Go(func() {
		m.boot(bootCtx, v, img)
	})
	return nil, claimGen, nil
}

// startDiskPhase applies a StartVM's durable disk changes inside the boot
// worker: the manifest replacement and the growth of the kept disk, under
// the shared BootTimeout-bounded context and the VM operation gate (which
// excludes stopped-disk snapshot copies and deletes for the whole phase).
// persisted reports whether the new sizing is durably recorded even though a
// later step failed.
func (m *Manager) startDiskPhase(ctx context.Context, v *vm) (persisted bool, err error) {
	if err := acquireVMOperation(ctx, v); err != nil {
		return false, err
	}
	defer releaseVMOperation(v)
	v.mu.Lock()
	res := v.meta.Resources
	env := v.meta.Env
	hostname := v.meta.Hostname
	v.mu.Unlock()
	if err := m.store.PrepareStart(v.id, res, env, hostname); err != nil {
		// A failed directory sync after a successful rename leaves the new
		// manifest live with unproven durability: the commit's outcome is
		// unknown, so the growth must stay reserved and nothing reverted.
		return errors.Is(err, storage.ErrManifestDurabilityUnknown), fmt.Errorf("apply start manifest: %w", err)
	}
	if err := m.store.GrowVMDisk(ctx, v.id, res.DiskGiB); err != nil {
		// The manifest already records the new size durably: the logical
		// size and its reservation stay grown (the physical file catches up
		// on the next start; growth is idempotent) — recording a smaller
		// size would undercount the disk and let a later request shrink it.
		return true, fmt.Errorf("grow kept disk: %w", err)
	}
	return true, nil
}

// settleFailedStart unwinds a StartVM boot whose durable disk phase failed
// before anything was launched: the manifest revert and the reservation
// release complete while the record still reads BOOTING, and only then does
// it publish STOPPED — a record that reads STOPPED is always fully settled,
// so a new start can never interleave with this worker's cleanup. When the
// new sizing was already persisted — or its commit outcome is unknown — the
// logical disk size and its reservation stay grown. The boot worker owns the
// boot channel; this function never touches it. DeleteVM stays the only
// remover.
func (m *Manager) settleFailedStart(v *vm, cause error, persisted bool) {
	// The snapshot (current, deleting, the restore point) is taken under
	// the manager lock first and then the record lock — the documented
	// order — because reading the manager's VM map requires m.mu: a
	// concurrent create or delete of any sandbox mutates it under m.mu
	// alone.
	m.mu.Lock()
	v.mu.Lock()
	if m.inStartSettle != nil {
		m.inStartSettle()
	}
	deleting := v.deleting
	current := m.vms[v.id] == v
	original := v.startOriginal
	v.mu.Unlock()
	m.mu.Unlock()
	// A start whose disk tool is retained settles nothing: the record goes
	// FAILED with the disk, the grown sizing and the whole reservation kept,
	// retryable through StopVM or DeleteVM once the tool is settled.
	if heldErr := m.diskHeldByTool(v.id); heldErr != nil && current && !deleting {
		m.mu.Lock()
		v.mu.Lock()
		if m.vms[v.id] == v && !v.deleting {
			v.state = nodev1.VMState_VM_STATE_FAILED
			v.errMsg = "start did not complete: " + heldErr.Error()
			if perr := m.store.SetVMError(v.id, v.errMsg); perr != nil {
				m.log.Warn("node: persist start failure", "sandbox", v.id, "error", perr)
			}
		}
		v.mu.Unlock()
		m.mu.Unlock()
		return
	}
	// The manifest revert runs while the record still reads BOOTING, so no
	// new start can observe the half-settled state or interleave its own
	// manifest writes with this worker's.
	if current && !deleting && !persisted {
		if err := m.store.RevertStart(v.id, original.Resources, original.Env, original.Hostname); err != nil {
			// The manifest keeps the new fields: they are validated, non-
			// shrinking start fields and the VM stays STOPPED, so this is
			// an accounting mismatch, not a safety problem. A retry
			// re-applies whatever it asks for.
			m.log.Warn("node: revert start fields", "sandbox", v.id, "error", err, "cause", cause)
		}
	}
	m.mu.Lock()
	v.mu.Lock()
	if m.vms[v.id] == v && !v.deleting {
		m.releaseComputeLocked(v)
		if !persisted {
			// Nothing durable changed: hand the growth delta back and
			// restore the in-memory fields.
			m.releaseDiskDeltaLocked(v.meta.Resources.DiskGiB - original.Resources.DiskGiB)
			v.meta.Resources = original.Resources
			v.meta.Env = original.Env
			v.meta.Hostname = original.Hostname
		}
		if v.state == nodev1.VMState_VM_STATE_BOOTING {
			// Published last: a record that reads STOPPED is fully settled.
			v.state = nodev1.VMState_VM_STATE_STOPPED
		}
	}
	v.mu.Unlock()
	m.mu.Unlock()
}
