package node

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// errBootAborted marks a boot canceled by the last waiter leaving, DeleteVM or
// manager shutdown. It is not surfaced to clients as a FAILED record; the VM
// is removed so the id can be created again.
var errBootAborted = errors.New("node: boot aborted")

// CreateVM boots a VM and blocks until its guest answers Ping. Concurrent
// calls for one sandbox id share one boot. The boot itself belongs to the
// manager: a successful VM survives the caller's context and a nodelink
// reconnect.
func (m *Manager) CreateVM(ctx context.Context, req *nodev1.CreateVMRequest) (*nodev1.VM, error) {
	id := req.GetSandboxId()
	if !ids.ValidSandbox(id) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid sandbox id %q", id)
	}
	// Register this attempt before doing any work: DeleteVM waits for every
	// attempt that started before its fence to drain before returning OK.
	m.mu.Lock()
	if _, fenced := m.fence[id]; fenced {
		m.mu.Unlock()
		return nil, status.Errorf(codes.Aborted, "sandbox %s is being deleted", id)
	}
	m.creating[id]++
	m.mu.Unlock()
	defer m.createDone(id)

	// Existing-id idempotency takes precedence over every other request field:
	// join before waiting on an irrelevant from_snapshot reservation.
	v, joined, err := m.joinExisting(id)
	if err != nil {
		return nil, err
	}
	if joined {
		return m.waitExisting(ctx, v)
	}
	// Phase 2 restores are validated before any reservation or record exists:
	// a refused request leaves no VM.
	var restore *restoreSource
	if req.GetFromManifest() != "" {
		if err := m.checkFromManifest(req); err != nil {
			return nil, err
		}
		var err error
		if restore, err = m.loadRestore(ctx, req); err != nil {
			return nil, err
		}
	}
	var snapshotRelease func()
	if from := req.GetFromSnapshot(); from != "" {
		var err error
		snapshotRelease, err = m.acquireSnapshotID(ctx, from)
		if err != nil {
			return nil, err
		}
	}
	v, owner, err := m.joinCreate(req, snapshotRelease, restore)
	if err != nil {
		return nil, err
	}
	if !owner {
		return m.waitExisting(ctx, v)
	}
	// A fresh record's first boot generation is 0: the creator's ticket was
	// created with the record itself.
	return m.waitBoot(ctx, v, 0)
}

// waitExisting returns an existing record's answer to an idempotent caller:
// a settled STOPPED record returns at once — a canceled start's boot failure
// must not leak into existing-id idempotency — while a boot in flight is
// joined and a stop in flight is waited out.
func (m *Manager) waitExisting(ctx context.Context, v *vm) (*nodev1.VM, error) {
	v.mu.Lock()
	if v.state == nodev1.VMState_VM_STATE_STOPPED {
		out := v.cloneVM()
		v.mu.Unlock()
		return out, nil
	}
	v.mu.Unlock()
	return m.waitBoot(ctx, v, joinerTicket)
}

// createDone retires one CreateVM attempt.
func (m *Manager) createDone(id string) {
	m.mu.Lock()
	m.creating[id]--
	if m.creating[id] <= 0 {
		delete(m.creating, id)
	}
	m.mu.Unlock()
}

// joinExisting joins an existing VM without inspecting or blocking on any
// other create fields. The create attempt is already counted, so DeleteVM
// cannot finish its fence underneath this join.
func (m *Manager) joinExisting(id string) (*vm, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, false, status.Error(codes.Unavailable, "node is shutting down")
	}
	if _, fenced := m.fence[id]; fenced {
		return nil, false, status.Errorf(codes.Aborted, "sandbox %s is being deleted", id)
	}
	existing := m.vms[id]
	if existing == nil {
		return nil, false, nil
	}
	existing.mu.Lock()
	defer existing.mu.Unlock()
	if existing.deleting {
		return nil, false, status.Errorf(codes.Aborted, "sandbox %s is being deleted", id)
	}
	return existing, true, nil
}

// joinCreate finds the existing record for the sandbox id first — the frozen
// proto says an existing-id call returns that VM whatever its other fields
// say — and only validates and reserves for a genuinely new id. owner
// reports whether the caller claimed the record's boot (and holds its first
// ticket) or is joining an existing one.
// A from_manifest create passes its checked manifest (loadRestore) as
// restore; it decides the image and the smallest disk.
func (m *Manager) joinCreate(req *nodev1.CreateVMRequest, snapshotRelease func(), restore *restoreSource) (_ *vm, owner bool, err error) {
	// joinCreate owns the reservation on every path. A genuinely new child
	// transfers it to the boot worker; an existing id or an error releases it.
	defer func() {
		if snapshotRelease != nil {
			snapshotRelease()
		}
	}()
	id := req.GetSandboxId()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, false, status.Error(codes.Unavailable, "node is shutting down")
	}
	if _, fenced := m.fence[id]; fenced {
		return nil, false, status.Errorf(codes.Aborted, "sandbox %s is being deleted", id)
	}
	if existing := m.vms[id]; existing != nil {
		existing.mu.Lock()
		deleting := existing.deleting
		existing.mu.Unlock()
		if deleting {
			// A create racing a delete fence is fenced too.
			return nil, false, status.Errorf(codes.Aborted, "sandbox %s is being deleted", id)
		}
		return existing, false, nil
	}
	if _, bad := m.quarantined[id]; bad {
		// A corrupt record was quarantined this boot. A live QEMU may still
		// own that state, so a new launch for the same id could collide with
		// it. An operator must resolve the orphan first.
		return nil, false, status.Errorf(codes.Internal, "sandbox %s was quarantined after corrupt state", id)
	}

	var source *storage.Snapshot
	var img storage.Image
	var minimumDiskGiB uint64
	if from := req.GetFromSnapshot(); from != "" {
		snap, ok := m.snapshots[from]
		if !ok {
			return nil, false, status.Errorf(codes.NotFound, "snapshot %q not found", from)
		}
		if requested := req.GetImage(); requested != "" && requested != snap.Meta.Image {
			return nil, false, status.Errorf(codes.InvalidArgument, "image %q does not match snapshot image %q", requested, snap.Meta.Image)
		}
		img, ok = m.images[snap.Meta.Image]
		if !ok {
			return nil, false, status.Errorf(codes.NotFound, "snapshot image %q not found", snap.Meta.Image)
		}
		minimumDiskGiB = ceilGiB(snap.Meta.VirtualSize)
		source = &snap
	} else if restore != nil {
		// loadRestore proved the image is installed and matches.
		img = restore.image
		minimumDiskGiB = max(img.RootVirtualSizeGiB, ceilGiB(restore.manifest.DiskBytes))
	} else {
		var err error
		img, err = m.imageFor(req.GetImage())
		if err != nil {
			return nil, false, err
		}
		minimumDiskGiB = img.RootVirtualSizeGiB
	}
	if err := validateResources(req.GetResources()); err != nil {
		return nil, false, err
	}
	res := req.GetResources()
	// A structurally valid VM may never exceed the logical host CPU count,
	// even when aggregate capacity is overcommitted.
	if res.GetVcpus() > m.hostVCPUs {
		return nil, false, status.Errorf(codes.ResourceExhausted,
			"vcpus %d exceeds the host's %d CPUs", res.GetVcpus(), m.hostVCPUs)
	}
	if res.GetDiskGib() < minimumDiskGiB {
		return nil, false, status.Errorf(codes.InvalidArgument,
			"disk_gib %d is smaller than the source disk %d GiB", res.GetDiskGib(), minimumDiskGiB)
	}
	if err := validHostname(req.GetHostname()); err != nil {
		return nil, false, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	want := resourcesFrom(res)
	if _, ok := want.diskBytes(); !ok {
		return nil, false, status.Errorf(codes.ResourceExhausted, "disk_gib %d is too large", res.GetDiskGib())
	}

	total, ok := m.reserved.add(want)
	if !ok || !total.fits(m.capacity) {
		return nil, false, status.Error(codes.ResourceExhausted, "no room for this VM")
	}
	free, err := m.diskFree()
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "disk free: %v", err)
	}
	// Charge the whole outstanding logical reservation, including this VM,
	// against the checked live-free promise. Sparse files may look small
	// today; promising the same promised bytes to two disks is still an
	// overcommit. Device errors and arithmetic overflow stay hard failures.
	promise, err := m.diskPromiseBytes(free)
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "disk promise: %v", err)
	}
	reservedBytes, ok := total.diskBytes()
	if !ok || reservedBytes > promise {
		return nil, false, status.Error(codes.ResourceExhausted, "not enough free disk for this VM")
	}
	cid, ok := m.allocCIDLocked()
	if !ok {
		return nil, false, status.Error(codes.ResourceExhausted, "no vsock CID is free")
	}

	meta := storage.VMMeta{
		SandboxID:       id,
		Image:           img.ID,
		FromSnapshot:    req.GetFromSnapshot(),
		FromManifest:    restore.key(),
		RestoreChunks:   restore.chunks(),
		Resources:       storageResources(res),
		Env:             maps.Clone(req.GetEnv()),
		Hostname:        req.GetHostname(),
		VsockCID:        cid,
		CreatedAtUnixMs: time.Now().UnixMilli(),
	}
	v := &vm{
		id:              id,
		meta:            meta,
		state:           nodev1.VMState_VM_STATE_BOOTING,
		bootDone:        make(chan struct{}),
		waiters:         1,
		capacityHeld:    true,
		computeHeld:     true,
		snapshotRelease: snapshotRelease,
		snapshotSource:  source,
		restore:         restore,
	}
	snapshotRelease = nil // the boot worker now owns the source pin
	m.reserved = total
	m.vms[id] = v

	bootCtx, cancel := context.WithCancel(m.ctx)
	v.bootCancel = cancel
	m.wg.Go(func() {
		m.boot(bootCtx, v, img)
	})
	return v, true, nil
}

// validateResources rejects sizing a QEMU configuration cannot satisfy. It
// mirrors qemu.Config's limits so the error is INVALID_ARGUMENT rather than a
// boot failure.
func validateResources(res *nodev1.Resources) error {
	switch {
	case res == nil:
		return status.Error(codes.InvalidArgument, "resources are required")
	case res.GetVcpus() == 0 || res.GetVcpus() > 1024:
		return status.Errorf(codes.InvalidArgument, "vcpus %d out of range 1..1024", res.GetVcpus())
	case res.GetMemoryMib() < 64 || res.GetMemoryMib() > 1<<20:
		return status.Errorf(codes.InvalidArgument, "memory_mib %d out of range 64..%d", res.GetMemoryMib(), uint64(1<<20))
	case res.GetDiskGib() == 0 || res.GetDiskGib() > math.MaxUint64>>30:
		return status.Errorf(codes.InvalidArgument, "disk_gib %d out of range", res.GetDiskGib())
	}
	return nil
}

// validHostname mirrors qemu's kernel-command-line hostname rule. The value
// becomes systemd.hostname=, so spaces and kernel-parameter separators must be
// impossible.
func validHostname(s string) error {
	if s == "" {
		return nil
	}
	if len(s) > 64 || s[0] == '.' || s[0] == '-' || s[len(s)-1] == '.' || s[len(s)-1] == '-' {
		return fmt.Errorf("invalid hostname %q", s)
	}
	for label := range strings.SplitSeq(s, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return fmt.Errorf("invalid hostname %q", s)
		}
		for i := range len(label) {
			c := label[i]
			if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' {
				continue
			}
			return fmt.Errorf("invalid hostname %q", s)
		}
	}
	return nil
}

// joinerTicket is the waitBoot ticket of a caller that holds none yet: it
// takes one, atomically with the boot generation it observes. Every other
// ticket value is the boot generation a claim bound its first ticket to.
const joinerTicket = ^uint64(0)

// waitBoot waits for the VM's boot attempt and maps its outcome. A canceled
// caller only stops the boot when it was the last waiter. Owners pass the
// generation their claim captured, so a starter descheduled between claim
// and wait can never mistake a newer boot's ticket for its own — its stale
// cancellation is a generation-guarded no-op. A boot that ended in STOPPED
// (an aborted StartVM returning to its kept disk) is a result, not an error;
// a stop observed mid-flight is waited out first so the caller sees the
// state the stop produced.
func (m *Manager) waitBoot(ctx context.Context, v *vm, ticket uint64) (*nodev1.VM, error) {
	var ticketGen uint64
	haveTicket := false
	for {
		v.mu.Lock()
		if !haveTicket {
			switch {
			case ticket == joinerTicket:
				v.waiters++
				ticketGen = v.bootGen
			case ctx.Err() != nil:
				// An owner arriving already canceled: hand its ticket back
				// through the generation guard, outside v.mu. A stale
				// ticket was already discarded by the newer claim's reset,
				// so the guard is a no-op for it; a current ticket must be
				// returned, or the sole canceled caller would leave a
				// phantom waiter and the boot could never abort at zero.
				v.mu.Unlock()
				m.leaveBoot(v, ticket)
				return nil, status.FromContextError(ctx.Err()).Err()
			case ticket != v.bootGen:
				// A live owner whose whole boot generation was replaced
				// while it was descheduled (a stop and a newer start
				// happened): it is genuinely waiting for this record's
				// boot, so it joins the current generation with a real
				// ticket immediately instead of waiting ticketless.
				v.waiters++
				ticketGen = v.bootGen
			default:
				// An owner on the current generation: the claim's ticket.
				ticketGen = ticket
			}
			haveTicket = true
		} else if ticketGen != v.bootGen {
			// The boot generation changed under this waiter: the new claim's
			// reset discarded our ticket, so take a fresh one on it.
			v.waiters++
			ticketGen = v.bootGen
		}
		done := v.bootDone
		state := v.state
		var stopping chan struct{}
		if state == nodev1.VMState_VM_STATE_STOPPING {
			stopping = v.stopDone
		}
		v.mu.Unlock()
		if stopping != nil {
			select {
			case <-stopping:
			case <-ctx.Done():
				m.leaveBoot(v, ticketGen)
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			continue
		}
		select {
		case <-done:
		case <-ctx.Done():
			m.leaveBoot(v, ticketGen)
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		v.mu.Lock()
		switch v.state {
		case nodev1.VMState_VM_STATE_READY:
			out := v.cloneVM()
			v.mu.Unlock()
			return out, nil
		case nodev1.VMState_VM_STATE_STOPPED:
			bootErr := v.bootErr
			var out *nodev1.VM
			if bootErr == nil {
				out = v.cloneVM()
			}
			v.mu.Unlock()
			if bootErr != nil {
				// The boot ended by returning its kept disk to STOPPED (an
				// aborted StartVM). That is the boot's failure, not a
				// successful start: a live caller must never see success
				// here. The record and its disk are stable; a completed
				// stop clears bootErr, so later joiners of a settled
				// stopped VM still get the clone.
				return nil, status.Errorf(codes.Internal, "boot did not complete: %s",
					cmp.Or(errorText(bootErr), "the start was abandoned"))
			}
			return out, nil
		case nodev1.VMState_VM_STATE_FAILED:
			bootErr := v.bootErr
			errMsg := v.errMsg
			v.mu.Unlock()
			if errors.Is(bootErr, errBootAborted) {
				// DeleteVM fenced this boot: every caller that started before
				// the delete observes ABORTED, never a resurrected VM.
				return nil, status.Errorf(codes.Aborted, "sandbox %s was deleted while booting", v.id)
			}
			if re, ok := errors.AsType[*restoreError](bootErr); ok {
				// A failed restore is a failed boot with its own code
				// (object store outage UNAVAILABLE, a bad chunk INTERNAL).
				return nil, re.grpcStatus()
			}
			msg := cmp.Or(errorText(bootErr), errMsg, "boot failed")
			return nil, status.Errorf(codes.Internal, "boot failed: %s", msg)
		default:
			// bootDone closed while the state is BOOTING: a StartVM replaced
			// the channel and this waiter consumed the previous boot's
			// signal. The loop's generation check re-tickets us before we
			// wait on the current one.
			v.mu.Unlock()
			continue
		}
	}
}

// leaveBoot drops one waiter's ticket and aborts the boot when nobody is
// left. A ticket from an older boot generation was already discarded by that
// boot's reset, so it has nothing to give back and must not cancel the newer
// boot. The boot worker owns the cleanup; canceling it never leaves a
// half-built disk behind because cleanup happens before the id can be reused.
func (m *Manager) leaveBoot(v *vm, gen uint64) {
	v.mu.Lock()
	if gen != v.bootGen {
		v.mu.Unlock()
		return
	}
	v.waiters--
	last := v.waiters == 0
	cancel := v.bootCancel
	v.mu.Unlock()
	if last && cancel != nil {
		cancel()
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// boot runs one boot attempt and publishes its terminal state. The worker
// captures its own boot channel and generation at entry: a newer generation
// (a stop followed by a new start, while this worker finishes post-terminal
// work) owns the record's state and channels now, so a stale worker never
// overwrites them or closes a channel it does not own — it only closes its
// own, waking any waiter still parked on it. A state failBoot already
// resolved (an aborted StartVM back to STOPPED) is left alone.
func (m *Manager) boot(ctx context.Context, v *vm, img storage.Image) {
	v.mu.Lock()
	myDone := v.bootDone
	myGen := v.bootGen
	v.mu.Unlock()
	err := m.runBoot(ctx, v, img, myGen)
	v.mu.Lock()
	if v.bootGen == myGen {
		v.bootErr = err
		if err != nil && v.state != nodev1.VMState_VM_STATE_READY && v.state != nodev1.VMState_VM_STATE_STOPPED {
			v.state = nodev1.VMState_VM_STATE_FAILED
			v.errMsg = errorText(err)
		}
	}
	close(myDone)
	v.mu.Unlock()
}

// runBoot creates durable state, launches QEMU, waits for the guest and marks
// the VM READY. On failure it runs the cleanup policy before returning. A
// StartVM boot (startKeep) replaces disk creation with the durable start
// phase — manifest replacement and growth of the kept disk — inside this
// same BootTimeout-bounded worker, and an aborted one returns to STOPPED
// instead of removing the disk.
func (m *Manager) runBoot(ctx context.Context, v *vm, img storage.Image, myGen uint64) error {
	workerCtx := ctx
	deadline := time.Now().Add(m.cfg.BootTimeout)
	diskCtx, cancelDisk := context.WithDeadline(workerCtx, deadline)
	defer cancelDisk()

	if !v.startKeep {
		if err := m.createVMDisk(diskCtx, v, img); err != nil {
			return m.failBoot(v, fmt.Errorf("create disk: %w", err))
		}
		if v.restore != nil {
			// An eager restore is as long as the disk is large: only the
			// worker's own context (the last waiter, DeleteVM, shutdown)
			// bounds it, and BootTimeout starts again once it is done.
			if err := m.restoreDisk(workerCtx, v); err != nil {
				return m.failBoot(v, err)
			}
			deadline = time.Now().Add(m.cfg.BootTimeout)
		}
	} else {
		persisted, err := m.startDiskPhase(diskCtx, v)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// The last waiter left, a delete fence or the manager
				// shutdown stopped the shared boot: the abort path settles
				// the record durably.
				return m.failBoot(v, err)
			}
			m.settleFailedStart(v, err, persisted)
			return err
		}
	}
	ctx, cancel := context.WithDeadline(workerCtx, deadline)
	defer cancel()
	// The network lease belongs to the bounded shared boot worker, before the
	// launch intent: nothing durable about the machine exists until the
	// network it will run on is allocated, and a failure below releases it
	// through failBoot once the absence of any QEMU is proven.
	nic, lease, err := m.networkLaunchLease(ctx, v.id)
	if err != nil {
		return m.failBoot(v, err)
	}
	if lease != nil {
		// The node's only copies of the lease descriptors close on every
		// exit from here: the deferred backstop covers each pre-Start
		// failure (the launch intent included), the explicit close below
		// runs immediately after Start returned on both outcomes, and
		// Lease.Close is idempotent, so exactly one close takes effect.
		defer lease.Close()
	}
	var proc VMProcess
	if m.jail != nil {
		// The jailed launcher writes its own attempt-naming intent and
		// claims the attempt before the jailer runs (launchJailed).
		proc, err = m.launchJailed(ctx, v, img, nic)
	} else {
		if err := m.store.WriteLaunchIntent(v.id); err != nil {
			return m.failBoot(v, fmt.Errorf("record launch intent: %w", err))
		}
		proc, err = m.launcher.Start(ctx, m.launchConfig(v.meta, nic), qemu.StartOptions{Logger: m.log})
	}
	if lease != nil {
		// QEMU duplicated the descriptors into the child (fd 3/4); the
		// manager's copies go away on both outcomes, exactly here.
		if cerr := lease.Close(); cerr != nil {
			if err != nil {
				err = errors.Join(err, cerr)
			} else {
				m.log.Warn("node: close network lease descriptors", "sandbox", v.id, "error", cerr)
			}
		}
	}
	if err != nil {
		if proc != nil {
			// The launcher could not prove the child dead and returned the
			// live handle: attach it before the settlement, so failBoot's
			// stop decides absence and no lease is released early.
			v.mu.Lock()
			v.process = proc
			v.mu.Unlock()
		}
		return m.failBoot(v, fmt.Errorf("launch: %w", err))
	}
	m.drains.Go(func() {
		drainEvents(proc)
	})

	v.mu.Lock()
	v.process = proc
	deleting := v.deleting
	v.mu.Unlock()
	if deleting {
		return m.failBoot(v, errBootAborted)
	}
	m.wg.Go(func() {
		m.watch(v, proc)
	})
	if err := m.store.CommitRunning(v.id, proc.PID()); err != nil {
		return m.failBoot(v, fmt.Errorf("record process identity: %w", err))
	}
	// QEMU runs paused (-S) until the dirty bitmap is in place (D2).
	if err := m.startGuest(ctx, v.id, proc); err != nil {
		return m.failBoot(v, err)
	}

	conn, err := m.dialer.Dial(ctx, v.meta.VsockCID)
	if err != nil {
		return m.failBoot(v, fmt.Errorf("dial guest: %w", err))
	}
	client := guestv1.NewGuestClient(conn)
	v.mu.Lock()
	if v.deleting {
		v.mu.Unlock()
		conn.Close()
		return m.failBoot(v, errBootAborted)
	}
	v.conn, v.client = conn, client
	v.mu.Unlock()

	if err := m.pingGuest(ctx, client); err != nil {
		return m.failBoot(v, fmt.Errorf("guest did not answer: %w", err))
	}
	if m.beforeReady != nil {
		// Test seam: stage the window between a successful Ping and the
		// terminal READY transition.
		m.beforeReady()
	}
	// Terminal transition: commit the ready metadata durably BEFORE (and
	// under the same lock as) publishing READY. A stop that observes READY
	// may then CommitStopped safely: no late CommitReady can clear the stop
	// marker of a manifest whose QEMU record is already gone, which a
	// restart would misread as a never-launched orphan and delete. The
	// last waiter's cancellation and DeleteVM both take v.mu too, so the
	// waiter check stays atomic with respect to them. A superseded
	// generation (provably impossible before the flip, checked defensively)
	// never publishes READY over a newer boot.
	now := m.now().UnixMilli()
	v.mu.Lock()
	if v.deleting || v.waiters == 0 || ctx.Err() != nil || v.bootGen != myGen {
		v.mu.Unlock()
		return m.failBoot(v, errBootAborted)
	}
	if err := m.store.CommitReady(v.id, now); err != nil {
		v.mu.Unlock()
		return m.failBoot(v, fmt.Errorf("record ready metadata: %w", err))
	}
	v.state = nodev1.VMState_VM_STATE_READY
	v.readyCommitted(now)
	v.mu.Unlock()
	if m.afterReady != nil {
		// Test seam: the point where a late ready-write used to race a
		// concurrent stop's durable stopped record.
		m.afterReady()
	}
	return nil
}

// createVMDisk builds either a fresh overlay or a private snapshot child. The
// source reservation ends as soon as the copy is complete, before guest boot.
func (m *Manager) createVMDisk(ctx context.Context, v *vm, img storage.Image) error {
	if v.snapshotRelease != nil {
		defer v.snapshotRelease()
	}
	if v.snapshotSource != nil {
		return m.store.CreateVMFromSnapshot(ctx, v.meta, img, *v.snapshotSource)
	}
	return m.store.CreateVM(ctx, v.meta, img)
}

// failBoot applies the boot-failure policy: stop and detach the process, then
// either remove everything (aborted creates) or keep the FAILED record and its
// private disk for GetVM/ListVMs until DeleteVM. Capacity is released only
// after cleanup, so a re-create cannot race a live QEMU.
func (m *Manager) failBoot(v *vm, cause error) error {
	v.mu.Lock()
	proc := v.process
	conn := v.conn
	v.process, v.conn, v.client = nil, nil, nil
	deleting := v.deleting
	v.mu.Unlock()

	var errs []error
	var stopErr error
	if proc != nil {
		stopErr = stopProcess(proc)
		if stopErr != nil {
			errs = append(errs, stopErr)
		}
	}
	if conn != nil {
		errs = append(errs, conn.Close())
	}
	aborted := deleting || m.ctx.Err() != nil || errors.Is(cause, context.Canceled) || errors.Is(cause, errBootAborted)
	if stopErr != nil {
		// Absence is unproven for every failure cause: keep the live handle
		// and its reservation so DeleteVM can try again instead of removing
		// the disk of a running QEMU.
		aborted = false
		v.mu.Lock()
		v.process = proc
		v.mu.Unlock()
	}

	// The network release runs exactly when the machine's absence is proven:
	// no QEMU was ever launched, or its stop succeeded — including a Setup
	// that failed after retaining an uncertain allocation (nil lease). A
	// failed release keeps the record retryable: nothing is removed and the
	// whole reservation stays, so the address and port can never be reused
	// while their rules or TAP may still exist.
	// The jail reservation is settled after the proven stop and before the
	// network: a retained reservation keeps everything (no removal, no
	// network or capacity release), retryable through StopVM or DeleteVM.
	var netErr error
	if stopErr == nil {
		if err := m.settleJailDetached(v, proc); err != nil {
			netErr = err
			errs = append(errs, netErr)
			aborted = false
		}
	}
	if stopErr == nil && netErr == nil {
		// A retained image tool holding the disk keeps the whole
		// reservation, like an unsettled jail.
		if err := m.diskHeldByTool(v.id); err != nil {
			netErr = err
			errs = append(errs, netErr)
			aborted = false
		}
	}
	if stopErr == nil && netErr == nil {
		if err := m.releaseNetwork(v.id); err != nil {
			netErr = err
			errs = append(errs, netErr)
			aborted = false
		}
	}

	removalFailed := false
	if aborted && v.startKeep && !deleting {
		// An aborted StartVM discards only the boot attempt: the kept disk
		// returns to STOPPED. A DeleteVM-fenced abort (deleting) skips this
		// and loses the disk together with the delete below.
		return m.abortStart(v, cause, errs)
	}
	if aborted {
		// Remove the owned directory before releasing the CID and capacity:
		// a re-create of this id must never race the previous QEMU's
		// sockets or disk.
		if err := m.store.RemoveVM(v.id); err != nil {
			aborted = false
			removalFailed = true
			errs = append(errs, err)
		}
	}
	if aborted {
		// While a DeleteVM owner is active, it owns the map entry until its
		// cleanup returns: keeping the id mapped blocks a same-id create
		// from racing the old delete's RemoveVM and getting its new disk
		// deleted. The delete owner releases the reservation.
		m.mu.Lock()
		v.mu.Lock()
		if !v.deleting && m.vms[v.id] == v {
			delete(m.vms, v.id)
			m.releaseReservationLocked(v)
		}
		v.mu.Unlock()
		m.mu.Unlock()
		// A DeleteVM fence owns every waiter that started before it, even
		// when the canceled disk/machine child reports a signal death instead
		// of context.Canceled (for example qemu-img create: signal: killed).
		// Join the abort marker so waitBoot classifies ABORTED while the
		// original subprocess cause stays available for diagnostics.
		if deleting || errors.Is(cause, context.Canceled) {
			if !errors.Is(cause, errBootAborted) {
				cause = errors.Join(errBootAborted, cause)
			}
		}
		return errors.Join(append([]error{cause}, errs...)...)
	}

	persisted := cause
	if netErr != nil {
		persisted = errors.Join(cause, netErr)
	}
	errs = append(errs, m.store.SetVMError(v.id, persisted.Error()))
	if stopErr == nil && !removalFailed && netErr == nil {
		// Done is proven and no owned files remain: the runtime reservation
		// can be handed out again while the disk stays for GetVM/ListVMs.
		m.mu.Lock()
		m.releaseReservationLocked(v)
		m.mu.Unlock()
	}
	return errors.Join(append([]error{cause}, errs...)...)
}

// abortStart finalizes an aborted StartVM boot: the process is already
// stopped and reaped by failBoot, so the kept disk is flushed and the record
// returns to STOPPED — the record and its disk always survive, exactly what
// the frozen contract promises for a start whose callers gave up. When the
// disk or manifest cannot be made durably stopped, the record is FAILED
// with the full reservation retained (runStop's durability rule): the
// manifest's stop marker plus dead launch record let a restart and a StopVM
// retry settle it.
func (m *Manager) abortStart(v *vm, cause error, errs []error) error {
	var persistErr error
	if err := m.store.SyncVMData(v.id); err != nil {
		persistErr = fmt.Errorf("flush disk: %w", err)
	}
	if persistErr == nil {
		if err := m.store.CommitStopped(v.id, m.now().UnixMilli()); err != nil {
			persistErr = fmt.Errorf("record stopped: %w", err)
		}
	}
	if m.afterAbortPersist != nil {
		// Test seam: the settlement writes are done, the record still reads
		// BOOTING and no accounting has moved yet.
		m.afterAbortPersist()
	}
	if persistErr != nil {
		msg := "stop after aborted start did not persist: " + persistErr.Error()
		v.mu.Lock()
		v.state = nodev1.VMState_VM_STATE_FAILED
		v.errMsg = msg
		perr := m.store.SetVMError(v.id, msg)
		v.mu.Unlock()
		return errors.Join(append([]error{cause, persistErr, perr}, errs...)...)
	}
	m.mu.Lock()
	v.mu.Lock()
	// Compute goes back to the pool and the disk stays counted as stopped
	// inventory (restoring it if the exit watcher released everything while
	// QEMU was dying) — all before the record reads STOPPED, so a new start
	// can only ever observe a fully settled record and can never interleave
	// its reservation changes with this worker's.
	m.releaseComputeLocked(v)
	m.reserveStoppedDiskLocked(v)
	v.state = nodev1.VMState_VM_STATE_STOPPED
	v.errMsg = ""
	v.bootErr = nil
	v.mu.Unlock()
	m.mu.Unlock()
	return errors.Join(append([]error{cause}, errs...)...)
}

// launchConfig renders the QEMU launch parameters for a durable record. A
// nil NIC keeps QEMU's user-mode network, the explicit dev/test fallback
// (Config.AllowUserNetworking); production boots always carry the lease.
func (m *Manager) launchConfig(meta storage.VMMeta, nic *qemu.Network) qemu.Config {
	return qemu.Config{
		SandboxID:    meta.SandboxID,
		WorkDir:      m.store.VMDir(meta.SandboxID),
		ImageDir:     m.store.ImagePath(meta.Image),
		DiskPath:     m.store.DiskPath(meta.SandboxID),
		VCPUs:        meta.Resources.VCPUs,
		MemoryMiB:    meta.Resources.MemoryMiB,
		VsockCID:     meta.VsockCID,
		CPUModel:     m.cfg.CPUModel,
		AllowHostCPU: m.cfg.AllowHostCPU,
		Nested:       m.cfg.Nested,
		CPUFeatures:  m.cfg.CPUFeatures,
		Hostname:     meta.Hostname,
		Network:      nic,
	}
}

// pingGuest polls Ping until the guest answers or the boot deadline ends.
// Dialing is lazy, so the first Ping also establishes the vsock connection.
func (m *Manager) pingGuest(ctx context.Context, client guestv1.GuestClient) error {
	backoff := 50 * time.Millisecond
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := client.Ping(pingCtx, &guestv1.PingRequest{})
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return context.Cause(ctx)
		}
		backoff = min(backoff*2, 500*time.Millisecond)
	}
}

// watch marks a VM FAILED when its QEMU exits. It keeps the record and the
// disk: GetVM/ListVMs report the failure and DeleteVM is the only path that
// removes data.
//
// The exit is proven by the reaped Done channel. The per-VM operation gate is
// taken BEFORE anything is published and held through the network release and
// the reservation settlement: every stop, start and delete needs the same
// gate, so no operation can replace the record between this watcher's
// ownership check and its settlement — the publish-then-gate order let a
// stop+start install a newer FAILED generation (for example one keeping a
// retained live child) that the stale watcher would then release as its own.
// While the watcher waits for the gate the record can still read READY with a
// dead process; StartVM's fast path refuses exactly that. The publication —
// FAILED flip, connection detach, durable write — is one critical section
// under m.mu then v.mu, the network release runs with no manager locks, and
// the reservation settlement is one atomic critical section in which only a
// proven release may free anything. A failed release keeps everything
// (retryable via StopVM or DeleteVM); a stale watcher — record replaced,
// process detached by a stop, or a delete fence — retires at the ownership
// check without touching anything.
func (m *Manager) watch(v *vm, proc VMProcess) {
	select {
	case <-proc.Done():
	case <-m.ctx.Done():
		return
	}
	// The boot channel is replaced by StartVM claims, so it is read under
	// the same lock those claims hold.
	v.mu.Lock()
	bootDone := v.bootDone
	v.mu.Unlock()
	select {
	case <-bootDone:
	case <-m.ctx.Done():
		return
	}
	if m.beforeWatchGate != nil {
		// Test seam: hold this watcher before it takes the operation gate.
		m.beforeWatchGate(v, proc)
	}
	if err := acquireVMOperation(m.ctx, v); err != nil {
		// Manager shutdown: everything is preserved for the next process.
		return
	}
	defer releaseVMOperation(v)

	// Full ownership recheck under m.mu then v.mu, gate in hand, before the
	// record is touched at all.
	m.mu.Lock()
	v.mu.Lock()
	if m.vms[v.id] != v || v.process != proc || v.deleting {
		v.mu.Unlock()
		m.mu.Unlock()
		if m.watchRetired != nil {
			// Test seam: a stale watcher retired without touching anything.
			m.watchRetired(v, proc)
		}
		return
	}
	msg := "vm process exited"
	if err := proc.Err(); err != nil {
		msg = err.Error()
	}
	v.process = nil
	v.state = nodev1.VMState_VM_STATE_FAILED
	v.errMsg = msg
	conn := v.conn
	v.conn, v.client = nil, nil

	// Persist under both locks: v.mu keeps DeleteVM from replacing the manifest
	// during the write, while m.mu keeps stop/start accounting outside the
	// failure publication boundary.
	if m.beforePersist != nil {
		m.beforePersist()
	}
	persistErr := m.store.SetVMError(v.id, msg)
	if m.afterFailurePersist != nil {
		m.afterFailurePersist()
	}
	v.mu.Unlock()
	m.mu.Unlock()
	if m.afterFailurePublish != nil {
		// Test seam: the point between the failure publication and the
		// network release, with the operation gate held.
		m.afterFailurePublish()
	}

	// No manager locks across the jail or network call; the gate is held.
	// The jail goes first: a retained jail keeps the network too.
	jailErr := m.settleJailDetached(v, proc)
	var relErr error
	if jailErr == nil {
		relErr = m.releaseNetwork(v.id)
	}
	// The reservation settlement is one atomic critical section, and only a
	// proven release may free anything.
	m.mu.Lock()
	v.mu.Lock()
	if m.vms[v.id] == v && !v.deleting && v.state == nodev1.VMState_VM_STATE_FAILED {
		switch {
		case jailErr != nil:
			v.errMsg = "vm exited; releasing its jail failed (retry with StopVM or DeleteVM): " + jailErr.Error()
			if perr := m.store.SetVMError(v.id, v.errMsg); perr != nil {
				m.log.Warn("node: persist release failure", "sandbox", v.id, "error", perr)
			}
		case relErr == nil:
			m.releaseReservationLocked(v)
		default:
			v.errMsg = "vm exited; releasing its network failed (retry with StopVM or DeleteVM): " + relErr.Error()
			if perr := m.store.SetVMError(v.id, v.errMsg); perr != nil {
				m.log.Warn("node: persist release failure", "sandbox", v.id, "error", perr)
			}
		}
	}
	v.mu.Unlock()
	m.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	if persistErr != nil {
		m.log.Warn("node: persist vm failure", "sandbox", v.id, "error", persistErr)
	}
	if jailErr != nil {
		m.log.Error("node: release jail after vm exit", "sandbox", v.id, "error", jailErr)
	}
	if relErr != nil {
		m.log.Error("node: release network after vm exit", "sandbox", v.id, "error", relErr)
	}
	m.log.Warn("node: vm exited", "sandbox", v.id, "error", msg)
}

// GetVM returns one sandbox, including a FAILED record whose disk is kept.
func (m *Manager) GetVM(_ context.Context, req *nodev1.GetVMRequest) (*nodev1.VM, error) {
	v, err := m.lookup(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.cloneVM(), nil
}

// ListVMs returns every VM record, sorted by id.
func (m *Manager) ListVMs(context.Context, *nodev1.ListVMsRequest) (*nodev1.ListVMsResponse, error) {
	m.mu.Lock()
	vms := slices.Collect(maps.Values(m.vms))
	m.mu.Unlock()
	out := &nodev1.ListVMsResponse{Vms: make([]*nodev1.VM, 0, len(vms))}
	for _, v := range vms {
		v.mu.Lock()
		if !v.deleting {
			out.Vms = append(out.Vms, v.cloneVM())
		}
		v.mu.Unlock()
	}
	slices.SortFunc(out.Vms, func(a, b *nodev1.VM) int {
		return cmp.Compare(a.GetSandboxId(), b.GetSandboxId())
	})
	return out, nil
}

// DeleteVM stops the VM, removes its disk and releases its capacity and CID.
// It is idempotent: an unknown or malformed sandbox id is OK. Deleting while a
// boot is in flight waits for that boot's cleanup, then deletes.
func (m *Manager) DeleteVM(ctx context.Context, req *nodev1.DeleteVMRequest) (*nodev1.DeleteVMResponse, error) {
	id := req.GetSandboxId()
	if !ids.ValidSandbox(id) {
		return &nodev1.DeleteVMResponse{}, nil
	}
	if err := m.beginNodeOperation(); err != nil {
		return nil, err
	}
	defer m.wg.Done()
	m.mu.Lock()
	v := m.vms[id]
	if v == nil && m.creating[id] > 0 {
		// Fence creates that have not reached joinCreate yet (for example a
		// snapshot-pin wait). No VM can appear after this publication; each
		// earlier attempt drains with ABORTED.
		token := &fenceToken{}
		m.fence[id] = token
		m.mu.Unlock()
		err := m.waitCreateFence(ctx, id)
		if err == nil {
			m.mu.Lock()
			if m.fence[id] == token {
				delete(m.fence, id)
			}
			m.mu.Unlock()
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "delete %s: %v", id, err)
		}
		return &nodev1.DeleteVMResponse{}, nil
	}
	if v == nil {
		// No VM exists and no create is in flight (checked under the same
		// lock). A prior protected wait may have left its fence when its
		// budget expired; this successful unknown-id retry retires it so
		// later creates are not ABORTED forever. With a VM or an in-flight
		// create, the branch above owns the fence instead.
		if m.creating[id] == 0 {
			delete(m.fence, id)
		}
		m.mu.Unlock()
		return &nodev1.DeleteVMResponse{}, nil
	}
	m.mu.Unlock()
	// Publish this generation's attempt BEFORE waiting for the VM gate. A Save
	// can hold that gate; duplicates arriving meanwhile must join this attempt,
	// not become a second owner that silently retries a failed first one.
	m.mu.Lock()
	if m.vms[id] != v {
		m.mu.Unlock()
		return &nodev1.DeleteVMResponse{}, nil
	}
	v.mu.Lock()
	if v.deleting {
		att := v.del
		v.deleteWaiters++
		v.mu.Unlock()
		m.mu.Unlock()
		if err := m.waitDeleteAttempt(ctx, v, att); err != nil {
			return nil, err
		}
		return &nodev1.DeleteVMResponse{}, nil
	}
	token := &fenceToken{}
	m.fence[id] = token
	att := &deleteAttempt{done: make(chan struct{})}
	v.deleting = true
	v.del = att
	cancel := v.bootCancel
	bootDone := v.bootDone
	maintCancel := v.maintCancel
	snapCancel := v.snapCancel
	v.mu.Unlock()
	m.mu.Unlock()
	if maintCancel != nil {
		// A maintenance session holds the operation gate: fence it like a
		// boot so it kills its QEMU and releases the gate promptly.
		maintCancel(errMaintenanceFenced)
	}
	if snapCancel != nil {
		// So does a live snapshot's read: it tears its cut down (on its own
		// cleanup context) and releases the gate.
		snapCancel(errSnapshotFenced)
	}
	defer func() {
		m.mu.Lock()
		if m.fence[id] == token {
			delete(m.fence, id)
		}
		m.mu.Unlock()
	}()
	if err := acquireVMOperation(ctx, v); err != nil {
		finishDeleteAttempt(v, att, err)
		if m.afterCanceledDeleteAttempt != nil {
			m.afterCanceledDeleteAttempt()
		}
		return nil, err
	}
	defer releaseVMOperation(v)

	err := m.deleteOwned(ctx, v, cancel, bootDone)
	// Earlier CreateVM attempts may still be blocked on a snapshot pin and
	// have no bootDone yet. They are counted from RPC entry and must observe
	// the fence before DeleteVM can return OK.
	fenceErr := m.waitCreateFence(ctx, id)
	if fenceErr == nil {
		m.mu.Lock()
		if m.fence[id] == token {
			delete(m.fence, id)
		}
		m.mu.Unlock()
	}
	err = errors.Join(err, fenceErr)

	finishDeleteAttempt(v, att, err)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete %s: %v", id, err)
	}
	return &nodev1.DeleteVMResponse{}, nil
}

// finishDeleteAttempt publishes the result and closes the attempt channel
// atomically. A retry starts only after deleting=false is visible, and old
// waiters hold the previous attempt pointer, so they never observe the retry.
func finishDeleteAttempt(v *vm, att *deleteAttempt, err error) {
	v.mu.Lock()
	att.err = err
	if err != nil {
		v.deleting = false
	}
	close(att.done)
	v.mu.Unlock()
}

// waitDeleteAttempt joins one already-published attempt. A duplicate always
// waits for the exact owner it observed, including while that owner is still
// blocked behind a Save's snapshot gate.
func (m *Manager) waitDeleteAttempt(ctx context.Context, v *vm, att *deleteAttempt) error {
	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.cfg.BootTimeout+time.Minute)
	defer cancel()
	select {
	case <-att.done:
	case <-waitCtx.Done():
		v.mu.Lock()
		v.deleteWaiters--
		v.mu.Unlock()
		return status.Errorf(codes.Internal, "delete %s: waiting for cleanup: %v", v.id, context.Cause(waitCtx))
	}
	v.mu.Lock()
	v.deleteWaiters--
	err := att.err
	v.mu.Unlock()
	if err != nil {
		return status.Errorf(codes.Internal, "delete %s: %v", v.id, err)
	}
	return nil
}

// waitCreateFence drains every create attempt registered before the delete.
func (m *Manager) waitCreateFence(ctx context.Context, id string) error {
	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.cfg.BootTimeout+time.Minute)
	defer cancel()
	for {
		m.mu.Lock()
		remaining := m.creating[id]
		m.mu.Unlock()
		if remaining == 0 {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("waiting for %d fenced create attempts: %w", remaining, context.Cause(waitCtx))
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// deleteOwned performs the cleanup for the single owner of a VM generation.
func (m *Manager) deleteOwned(ctx context.Context, v *vm, cancel context.CancelFunc, bootDone <-chan struct{}) error {
	if cancel != nil {
		cancel()
	}
	// The boot worker owns its cleanup; wait for it. Delete must not be
	// canceled by a client that went away.
	waitCtx, waitCancel := context.WithTimeout(context.WithoutCancel(ctx), m.cfg.BootTimeout+time.Minute)
	defer waitCancel()
	select {
	case <-bootDone:
	case <-waitCtx.Done():
		return fmt.Errorf("waiting for boot cleanup: %w", context.Cause(waitCtx))
	}
	// The boot's own abort cleanup may have removed this generation and a
	// same-id create may already own the directory. Never touch a newer
	// generation's disk.
	if !m.isCurrent(v) {
		return nil
	}
	if err := m.resolveUnresolved(v); err != nil {
		return fmt.Errorf("ownership unresolved: %w", err)
	}
	if m.beforeDeleteRemove != nil {
		m.beforeDeleteRemove()
	}

	v.mu.Lock()
	proc := v.process
	conn := v.conn
	snapshotClient := v.snapshotClient
	v.snapshotClient = nil
	v.mu.Unlock()
	if snapshotClient != nil {
		snapshotClient.Close()
	}
	if proc != nil {
		if err := stopProcess(proc); err != nil {
			// Keep the handle and the reservation: the process may still be
			// alive, and removing its disk would corrupt a running VM.
			return err
		}
	}
	v.mu.Lock()
	v.process = nil
	v.conn, v.client = nil, nil
	v.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	// The machine is proven gone (or never ran): release its jail, then its
	// network, before anything durable is removed, so a failed release finds
	// record, disk and reservation untouched for this DeleteVM's retry.
	if err := m.settleJailDetached(v, proc); err != nil {
		v.mu.Lock()
		v.state = nodev1.VMState_VM_STATE_FAILED
		v.errMsg = "delete failed: " + err.Error()
		persistErr := m.store.SetVMError(v.id, v.errMsg)
		v.mu.Unlock()
		return errors.Join(fmt.Errorf("release jail: %w", err), persistErr)
	}
	// A jailed image tool that still holds the disk (an unproven tool exit)
	// keeps everything: disk, network and reservation, until it is settled.
	if err := m.diskHeldByTool(v.id); err != nil {
		v.mu.Lock()
		v.state = nodev1.VMState_VM_STATE_FAILED
		v.errMsg = "delete failed: " + err.Error()
		persistErr := m.store.SetVMError(v.id, v.errMsg)
		v.mu.Unlock()
		return errors.Join(err, persistErr)
	}
	if err := m.releaseNetwork(v.id); err != nil {
		v.mu.Lock()
		v.state = nodev1.VMState_VM_STATE_FAILED
		v.errMsg = "delete failed: " + err.Error()
		persistErr := m.store.SetVMError(v.id, v.errMsg)
		v.mu.Unlock()
		return errors.Join(fmt.Errorf("release network: %w", err), persistErr)
	}
	if err := m.store.RemoveVM(v.id); err != nil {
		// The process is gone but the owned files remain: keep a retryable
		// FAILED record (and its reservation) instead of pretending the VM
		// no longer exists.
		v.mu.Lock()
		v.state = nodev1.VMState_VM_STATE_FAILED
		v.errMsg = "delete failed: " + err.Error()
		persistErr := m.store.SetVMError(v.id, v.errMsg)
		v.mu.Unlock()
		return errors.Join(fmt.Errorf("remove disk: %w", err), persistErr)
	}

	m.mu.Lock()
	if m.vms[v.id] == v {
		delete(m.vms, v.id)
		m.releaseReservationLocked(v)
	}
	m.mu.Unlock()
	return nil
}
