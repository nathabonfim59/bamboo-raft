package node

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// snapshotOperation is a refcounted per-snapshot-id serialization gate. The
// map entry cannot disappear while an owner or waiter still refers to it.
type snapshotOperation struct {
	gate chan struct{}
	refs int
}

// snapshotRun records every ambiguous side effect that cleanup may need to
// resolve. In particular, Pause may have stopped vCPUs even when its RPC fails.
type snapshotRun struct {
	// stage is the Phase 1 staged copy; nil for a Phase 2 cut.
	stage *storage.SnapshotStage
	// lease is the guest freeze lease to ask for; zero is
	// Config.SnapshotLease.
	lease           time.Duration
	token           string
	leaseChanged    bool
	deadline        time.Time
	freezeAttempted bool
	pauseAttempted  bool
	resumed         bool
	thawed          bool
}

// beginNodeOperation joins SaveSnapshot, DeleteSnapshot and DeleteVM to the
// manager's shutdown barrier. Adding under m.mu prevents an Add racing Wait
// after shutdown has started.
func (m *Manager) beginNodeOperation() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return status.Error(codes.Unavailable, "node is shutting down")
	}
	m.wg.Add(1)
	return nil
}

// acquireVMOperation serializes snapshot save with VM deletion without holding
// the manager's global lock across I/O.
func acquireVMOperation(ctx context.Context, v *vm) error {
	v.mu.Lock()
	if v.snapshotGate == nil {
		v.snapshotGate = make(chan struct{}, 1)
		v.snapshotGate <- struct{}{}
	}
	gate := v.snapshotGate
	v.mu.Unlock()
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

func releaseVMOperation(v *vm) { v.snapshotGate <- struct{}{} }

// acquireSnapshotID serializes save/delete and pins source data through a
// child's private copy. The returned release function is safe to call once.
func (m *Manager) acquireSnapshotID(ctx context.Context, id string) (func(), error) {
	m.mu.Lock()
	op := m.snapshotOps[id]
	if op == nil {
		op = &snapshotOperation{gate: make(chan struct{}, 1)}
		op.gate <- struct{}{}
		m.snapshotOps[id] = op
	}
	op.refs++
	m.mu.Unlock()

	select {
	case <-op.gate:
	case <-ctx.Done():
		m.mu.Lock()
		op.refs--
		if op.refs == 0 {
			delete(m.snapshotOps, id)
		}
		m.mu.Unlock()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return sync.OnceFunc(func() {
		op.gate <- struct{}{}
		m.mu.Lock()
		op.refs--
		if op.refs == 0 {
			delete(m.snapshotOps, id)
		}
		m.mu.Unlock()
	}), nil
}

// SaveSnapshot freezes the guest, strictly pauses QEMU, privately copies and
// validates the stopped disk, resumes and thaws independently of cancellation,
// then durably publishes. No consistency, lease or cleanup failure can publish.
func (m *Manager) SaveSnapshot(ctx context.Context, req *nodev1.SaveSnapshotRequest) (*nodev1.Snapshot, error) {
	sandboxID, snapshotID := req.GetSandboxId(), req.GetSnapshotId()
	if snapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot id is required")
	}
	if err := m.beginNodeOperation(); err != nil {
		return nil, err
	}
	defer m.wg.Done()
	opCtx, cancelOperation := context.WithCancel(ctx)
	stopManagerCancel := context.AfterFunc(m.ctx, cancelOperation)
	defer func() {
		stopManagerCancel()
		cancelOperation()
	}()
	v, err := m.lookup(sandboxID)
	if err != nil {
		return nil, err
	}
	if err := acquireVMOperation(opCtx, v); err != nil {
		return nil, err
	}
	defer releaseVMOperation(v)
	// A record whose restart recovery could not prove ownership is resolved
	// before its disk is copied; otherwise UNAVAILABLE with nothing copied.
	if err := m.requireResolved(v); err != nil {
		return nil, err
	}
	releaseSnapshot, err := m.acquireSnapshotID(opCtx, snapshotID)
	if err != nil {
		return nil, err
	}
	defer releaseSnapshot()

	m.mu.Lock()
	_, duplicate := m.snapshots[snapshotID]
	m.mu.Unlock()
	if duplicate {
		return nil, status.Errorf(codes.AlreadyExists, "snapshot %q already exists", snapshotID)
	}
	v.mu.Lock()
	if v.deleting {
		v.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", sandboxID)
	}
	if v.state == nodev1.VMState_VM_STATE_STOPPED {
		meta := v.meta
		v.mu.Unlock()
		return m.saveStoppedSnapshot(opCtx, sandboxID, snapshotID, meta)
	}
	if v.state != nodev1.VMState_VM_STATE_READY || v.client == nil || v.process == nil {
		v.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s is not ready", sandboxID)
	}
	proc, client, meta := v.process, v.client, v.meta
	rescueClient := v.snapshotClient
	v.mu.Unlock()
	controller, ok := proc.(VMController)
	if rescueClient != nil {
		// A previous save rescued a dead QMP connection. Keep using the
		// verified replacement instead of the dead original controller.
		controller = snapshotQMPController{id: sandboxID, client: rescueClient}
		ok = true
	}
	if !ok {
		return nil, status.Error(codes.Internal, "QEMU process does not support pause/resume")
	}
	img := m.images[meta.Image]
	stage, err := m.store.BeginSnapshot(snapshotID)
	if err != nil {
		return nil, snapshotRPCError("save snapshot", err)
	}
	run := &snapshotRun{stage: stage, deadline: m.now().Add(m.cfg.SnapshotLease)}
	fail := func(cause error) (*nodev1.Snapshot, error) {
		cleanupErr := m.cleanupSnapshot(v, proc, controller, client, run)
		joined := errors.Join(cause, cleanupErr)
		if snapshotRuntimeUnsafe(run) {
			if markErr := m.markSnapshotRuntimeFailed(v, joined); markErr != nil {
				joined = errors.Join(joined, markErr)
			}
		}
		return nil, snapshotRPCError("save snapshot", joined)
	}

	if err := m.freezeSnapshotAttempt(opCtx, client, run); err != nil {
		return fail(err)
	}
	// Every request step after the first Freeze response is bounded by the
	// conservative host lease deadline as well as request/manager cancellation.
	leaseCtx, cancelLease := context.WithDeadline(opCtx, run.deadline)
	defer cancelLease()
	if m.cfg.afterSnapshotFreeze != nil {
		if err := m.cfg.afterSnapshotFreeze(leaseCtx); err != nil {
			return fail(err)
		}
	}
	run.pauseAttempted = true
	if err := controller.Pause(leaseCtx); err != nil {
		return fail(fmt.Errorf("pause vm: %w", err))
	}
	if err := leaseCtx.Err(); err != nil {
		return fail(err)
	}
	virtualSize, ok := resourcesOf(meta.Resources).diskBytes()
	if !ok {
		return fail(errors.New("invalid VM disk size"))
	}
	if err := stage.Copy(leaseCtx, sandboxID, img, virtualSize); err != nil {
		return fail(err)
	}
	if err := m.checkSnapshotDeadline(run); err != nil {
		return fail(err)
	}

	// Resume uses an independent bounded context even when the request ended.
	// The helper retries with a freshly verified QMP connection if necessary.
	if err := m.resumeSnapshot(v, proc, controller); err != nil {
		return fail(err)
	}
	run.resumed = true
	verifyCtx, cancel := context.WithTimeout(context.Background(), m.cfg.SnapshotCleanupTimeout)
	err = m.freezeSnapshotAttempt(verifyCtx, client, run)
	cancel()
	if err != nil {
		return fail(fmt.Errorf("verify freeze lease: %w", err))
	}
	if run.token == "" {
		return fail(errors.New("freeze verification returned no token"))
	}
	// A replacement token means the original lease expired or the agent
	// restarted; cleanup may thaw only that replacement, never an unrelated
	// later lease.
	if run.leaseChanged {
		return fail(errors.New("freeze lease changed during snapshot"))
	}
	if err := m.checkSnapshotDeadline(run); err != nil {
		return fail(err)
	}
	if err := m.thawSnapshot(client, run); err != nil {
		return fail(err)
	}
	if err := opCtx.Err(); err != nil {
		return fail(err)
	}
	if err := m.checkSnapshotDeadline(run); err != nil {
		return fail(err)
	}

	// The verification Freeze above may have shrunk run.deadline. Publishing
	// must use the latest conservative deadline, not the earlier copy lease.
	publishCtx, cancelPublish := context.WithDeadline(opCtx, run.deadline)
	defer cancelPublish()
	snap, published, err := stage.Publish(publishCtx, sandboxID, img, m.now().UnixMilli())
	if published {
		m.mu.Lock()
		m.snapshots[snapshotID] = snap
		m.mu.Unlock()
	}
	// A deadline that elapsed during manifest fsync/rename or immediately
	// after it is never reported as success, even when the rename already
	// made the snapshot truthfully visible in Info.
	if deadlineErr := m.checkSnapshotDeadline(run); err == nil {
		err = deadlineErr
	}
	if err != nil {
		return nil, snapshotRPCError("publish snapshot", errors.Join(err, stage.Abort()))
	}
	return snapshotProto(snap), nil
}

// saveStoppedSnapshot copies a stopped VM's kept disk into a snapshot. The
// disk is at rest (no QEMU runs), so no guest freeze or QEMU pause exists to
// clean up: copy, validate and publish. The operation gate held by the caller
// excludes StartVM's growth phase and DeleteVM for the whole copy.
func (m *Manager) saveStoppedSnapshot(ctx context.Context, sandboxID, snapshotID string, meta storage.VMMeta) (*nodev1.Snapshot, error) {
	img, ok := m.images[meta.Image]
	if !ok {
		return nil, status.Errorf(codes.Internal, "sandbox %s: image %q disappeared", sandboxID, meta.Image)
	}
	virtualSize, ok := resourcesOf(meta.Resources).diskBytes()
	if !ok {
		return nil, status.Error(codes.Internal, "invalid VM disk size")
	}
	stage, err := m.store.BeginSnapshot(snapshotID)
	if err != nil {
		return nil, snapshotRPCError("save snapshot", err)
	}
	if err := stage.Copy(ctx, sandboxID, img, virtualSize); err != nil {
		return nil, snapshotRPCError("save snapshot", errors.Join(err, stage.Abort()))
	}
	if err := ctx.Err(); err != nil {
		return nil, snapshotRPCError("save snapshot", errors.Join(err, stage.Abort()))
	}
	snap, published, err := stage.Publish(ctx, sandboxID, img, m.now().UnixMilli())
	if published {
		m.mu.Lock()
		m.snapshots[snapshotID] = snap
		m.mu.Unlock()
	}
	if err != nil {
		return nil, snapshotRPCError("publish snapshot", errors.Join(err, stage.Abort()))
	}
	return snapshotProto(snap), nil
}

// freezeSnapshotAttempt captures the returned token before validating the
// response. The original deadline can only shrink, never extend on retries.
func (m *Manager) freezeSnapshotAttempt(ctx context.Context, client guestv1.GuestClient, run *snapshotRun) error {
	run.freezeAttempted = true
	attemptStart := m.now()
	resp, err := client.Freeze(ctx, &guestv1.FreezeRequest{LeaseMs: m.freezeLeaseMs(run)})
	if resp != nil {
		if run.token != "" && resp.GetLeaseToken() != run.token {
			run.leaseChanged = true
		}
		run.token = resp.GetLeaseToken()
	}
	if err != nil {
		return err
	}
	if resp == nil || resp.GetLeaseToken() == "" {
		return errors.New("guest Freeze returned an empty lease token")
	}
	remaining := resp.GetLeaseRemainingMs()
	if remaining == 0 || remaining > uint64(time.Hour/time.Millisecond) {
		return fmt.Errorf("guest Freeze returned invalid remaining lease %d ms", remaining)
	}
	candidate := attemptStart.Add(time.Duration(remaining) * time.Millisecond)
	if candidate.Before(run.deadline) {
		run.deadline = candidate
	}
	return m.checkSnapshotDeadline(run)
}

func (m *Manager) checkSnapshotDeadline(run *snapshotRun) error {
	if !m.now().Before(run.deadline) {
		return errors.New("freeze lease expired before snapshot consistency and cleanup completed")
	}
	return nil
}

// snapshotRuntimeUnsafe reports that cleanup could not prove the VM is running
// with an unfrozen filesystem. A failed stage publish or stage cleanup alone is
// not a runtime failure.
func snapshotRuntimeUnsafe(run *snapshotRun) bool {
	if run.pauseAttempted && !run.resumed {
		return true
	}
	// A Freeze reply can be lost after the guest actually froze, and cleanup
	// discovery can fail too. Without a successful Thaw, readiness is unproven
	// even when no token was ever returned.
	return run.freezeAttempted && !run.thawed
}

// markSnapshotRuntimeFailed makes the VM unavailable without killing its
// process or releasing its disk, CID, capacity or reservation. Exec/GetVM then
// report FAILED instead of pretending the sandbox is ready. Transient save
// failures that fully recover never reach here.
func (m *Manager) markSnapshotRuntimeFailed(v *vm, cause error) error {
	v.mu.Lock()
	v.state = nodev1.VMState_VM_STATE_FAILED
	v.errMsg = "snapshot recovery failed: " + errorText(cause)
	// Persist while holding v.mu, exactly like the death watcher: the durable
	// manifest write is serialized per VM generation and DeleteVM cannot race
	// a same-id recreate between the state and manifest updates.
	if m.beforePersist != nil {
		m.beforePersist()
	}
	persistErr := m.store.SetVMError(v.id, v.errMsg)
	v.mu.Unlock()
	if persistErr != nil {
		return fmt.Errorf("persist snapshot recovery failure: %w", persistErr)
	}
	return nil
}

// cleanupSnapshot independently resumes an ambiguous Pause, discovers a token
// if the first Freeze reply was lost, thaws the current owned token, and removes
// only unpublished staging. Every cleanup result is joined truthfully.
func (m *Manager) cleanupSnapshot(v *vm, proc VMProcess, controller VMController, client guestv1.GuestClient, run *snapshotRun) error {
	var errs []error
	if run.pauseAttempted && !run.resumed {
		if err := m.resumeSnapshot(v, proc, controller); err != nil {
			errs = append(errs, fmt.Errorf("resume cleanup: %w", err))
		} else {
			run.resumed = true
		}
	}
	// Thaw RPCs cannot run while vCPUs remain paused. Startup recovery or the
	// guest watchdog is the only safe rescue if every Resume attempt failed.
	if run.resumed || !run.pauseAttempted {
		if !run.thawed && run.token == "" {
			ctx, cancel := context.WithTimeout(context.Background(), m.cfg.SnapshotCleanupTimeout)
			resp, err := client.Freeze(ctx, &guestv1.FreezeRequest{LeaseMs: m.freezeLeaseMs(run)})
			cancel()
			if resp != nil {
				run.token = resp.GetLeaseToken()
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("discover freeze lease for cleanup: %w", err))
			} else if run.token == "" {
				errs = append(errs, errors.New("cleanup Freeze returned no token"))
			}
		}
		if !run.thawed && run.token != "" {
			if err := m.thawSnapshot(client, run); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if run.stage != nil {
		if err := run.stage.Abort(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// freezeLeaseMs is the lease a Freeze of run asks for.
func (m *Manager) freezeLeaseMs(run *snapshotRun) uint32 {
	return uint32(cmp.Or(run.lease, m.cfg.SnapshotLease) / time.Millisecond)
}

func (m *Manager) thawSnapshot(client guestv1.GuestClient, run *snapshotRun) error {
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.SnapshotCleanupTimeout)
	defer cancel()
	for {
		_, err := client.Thaw(ctx, &guestv1.ThawRequest{LeaseToken: run.token})
		if err == nil {
			run.thawed = true
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("thaw cleanup: %w", errors.Join(context.Cause(ctx), err))
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// snapshotQMPController drives Pause/Resume through a rescue QMP connection
// that outlives the save which established it.
type snapshotQMPController struct {
	id     string
	client *qmp.Client
}

func (c snapshotQMPController) Pause(ctx context.Context) error {
	return qemu.Pause(ctx, c.client, c.id)
}

func (c snapshotQMPController) Resume(ctx context.Context) error {
	return qemu.Resume(ctx, c.client, c.id)
}

// installSnapshotClient adopts a verified rescue connection for later saves.
// The caller transfers ownership; the client is closed by DeleteVM/Shutdown.
func (m *Manager) installSnapshotClient(v *vm, client *qmp.Client) {
	v.mu.Lock()
	old := v.snapshotClient
	v.snapshotClient = client
	v.mu.Unlock()
	if old != nil {
		old.Close()
	}
	m.drains.Go(func() {
		drainQMPClient(client)
	})
}

// resumeSnapshot retries with independent cleanup time. If the original QMP
// connection died after stop, a rescue reuses the full A2 pinned-process
// verification, including pid incarnation, before it may issue cont, then
// keeps that connection for later saves.
func (m *Manager) resumeSnapshot(v *vm, proc VMProcess, controller VMController) error {
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.SnapshotCleanupTimeout)
	defer cancel()
	for {
		err := controller.Resume(ctx)
		if err == nil {
			return nil
		}
		rescueCtx, rescueCancel := context.WithTimeout(ctx, min(5*time.Second, m.cfg.SnapshotCleanupTimeout))
		rescueErr := m.rescueResume(rescueCtx, v, proc)
		rescueCancel()
		if rescueErr == nil {
			return nil
		}
		err = errors.Join(err, rescueErr)
		select {
		case <-ctx.Done():
			return fmt.Errorf("resume cleanup: %w", errors.Join(context.Cause(ctx), err))
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// rescueResume proves the live process is the recorded QEMU incarnation
// through a fresh pidfd and the shared QMP verification, resumes it, and
// installs the verified connection as the VM's persistent snapshot
// controller. PID equality alone never authorizes control.
func (m *Manager) rescueResume(ctx context.Context, v *vm, proc VMProcess) error {
	pinned, err := pinProc(proc.PID())
	if err != nil {
		return fmt.Errorf("QMP rescue pin: %w", err)
	}
	defer pinned.close()
	meta, err := m.store.ReadVM(v.id)
	if err != nil {
		return fmt.Errorf("QMP rescue read record: %w", err)
	}
	if meta.QEMU == nil || meta.QEMU.State != storage.QEMURunning {
		return errors.New("QMP rescue: VM has no running process record")
	}
	if meta.QEMU.PID != proc.PID() {
		return fmt.Errorf("QMP rescue: process pid %d does not match recorded pid %d", proc.PID(), meta.QEMU.PID)
	}
	// Provenance before any QMP connection or control: the pinned process must
	// be exactly our QEMU for this VM, with the owned executable, image,
	// workdir and disk argv. A plausible QMP socket alone is not identity.
	if m.qemuExe == "" {
		return errors.New("QMP rescue: expected QEMU executable is unknown")
	}
	if !ownedBy(pinned.info, meta, m.store.ImageDir(), m.store.VMDir(v.id), m.store.DiskPath(v.id), m.qemuExe) {
		return errors.New("QMP rescue: pinned process does not own this VM's executable, image, workdir and disk")
	}
	client, err := m.verifyQMPAt(ctx, pinned, meta, filepath.Join(m.store.VMDir(v.id), qemu.QMPFilename), false)
	if err != nil {
		return fmt.Errorf("QMP rescue verify: %w", err)
	}
	m.installSnapshotClient(v, client)
	return nil
}

// DeleteSnapshot is idempotent and serialized with saves and child copies of
// the same opaque id. Unknown and empty ids succeed without touching storage.
func (m *Manager) DeleteSnapshot(ctx context.Context, req *nodev1.DeleteSnapshotRequest) (*nodev1.DeleteSnapshotResponse, error) {
	id := req.GetSnapshotId()
	if id == "" {
		return &nodev1.DeleteSnapshotResponse{}, nil
	}
	if err := m.beginNodeOperation(); err != nil {
		return nil, err
	}
	defer m.wg.Done()
	release, err := m.acquireSnapshotID(ctx, id)
	if err != nil {
		return nil, err
	}
	defer release()
	removed, err := m.store.DeleteSnapshot(id)
	if removed {
		// A post-remove fsync error still leaves the disk proven absent. Info
		// and create-from must not advertise or open a path that is gone.
		m.mu.Lock()
		delete(m.snapshots, id)
		m.mu.Unlock()
	}
	if err != nil {
		return nil, snapshotRPCError("delete snapshot", err)
	}
	return &nodev1.DeleteSnapshotResponse{}, nil
}

func snapshotProto(snap storage.Snapshot) *nodev1.Snapshot {
	return &nodev1.Snapshot{
		SnapshotId:      snap.Meta.SnapshotID,
		SourceSandboxId: snap.Meta.SourceSandboxID,
		Image:           snap.Meta.Image,
		SizeBytes:       snap.Meta.SizeBytes,
		CreatedAtUnixMs: snap.Meta.CreatedAtUnixMs,
	}
}

// Pause and Resume make an adopted process a VMController. Both use the exact
// same strict QMP validation as a directly launched qemu.Process.
func (p *adoptedProcess) Pause(ctx context.Context) error {
	return qemu.Pause(ctx, p.client, p.id)
}

func (p *adoptedProcess) Resume(ctx context.Context) error {
	return qemu.Resume(ctx, p.client, p.id)
}

func ceilGiB(bytes uint64) uint64 {
	if bytes > math.MaxUint64-(1<<30-1) {
		return math.MaxUint64
	}
	return (bytes + (1<<30 - 1)) >> 30
}

func snapshotRPCError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrSnapshotExists) {
		return status.Errorf(codes.AlreadyExists, "%s: snapshot already exists", operation)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	return status.Errorf(codes.Internal, "%s: %v", operation, err)
}
