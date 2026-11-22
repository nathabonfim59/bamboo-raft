// Phase 2 SnapshotVM (D4, D5): a READY VM's live cut through a fleece image
// and QEMU's NBD export, read by disksnap into the object store.

package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/node/disksnap"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// cutFreezeLease is the guest freeze lease of a live cut: it covers one QMP
// transaction and the verification Freeze, a few milliseconds in practice.
const cutFreezeLease = 5 * time.Second

// errSnapshotSpace aborts a live snapshot whose fleece image outgrew its
// reservation or whose node fell below the free-space floor.
var errSnapshotSpace = errors.New("snapshot aborted: not enough disk for the fleece image")

// errSnapshotFenced is the cancellation cause DeleteVM gives a running live
// SnapshotVM.
var errSnapshotFenced = errors.New("node: snapshot fenced by DeleteVM")

// SnapshotVM writes the VM's disk as it is now to the object store as one
// manifest. The validation order is nodefake's — store, chain id, sandbox —
// so a node without an object store reports that before anything the request
// itself could be wrong about. A QEMU that lacks a Phase 2 command or
// argument (the capability gate) is a FAILED_PRECONDITION right after the
// store check. Calls for one VM are serialized with each other and with
// StopVM, StartVM's disk phase and DeleteVM through the VM operation gate.
func (m *Manager) SnapshotVM(ctx context.Context, req *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
	if err := m.beginNodeOperation(); err != nil {
		return nil, err
	}
	defer m.wg.Done()
	if m.cfg.Store == nil {
		return nil, status.Error(codes.FailedPrecondition, "this node has no object store")
	}
	if err := m.phase2Unavailable(); err != nil {
		return nil, err
	}
	if !snapformat.ValidChainID(req.GetChainId()) {
		return nil, status.Errorf(codes.InvalidArgument, "bad chain_id %q", req.GetChainId())
	}
	v, err := m.lookup(req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	opCtx, cancelOperation := context.WithCancel(ctx)
	stopManagerCancel := context.AfterFunc(m.ctx, cancelOperation)
	defer func() {
		stopManagerCancel()
		cancelOperation()
	}()

	// A STOPPED VM is read in a maintenance session, which takes the VM
	// operation gate itself; a READY VM is cut live under the gate. A VM
	// that stops while this call waits for the gate goes the STOPPED way.
	if vmStateOf(v) == nodev1.VMState_VM_STATE_STOPPED {
		return m.snapshotStopped(opCtx, v, req)
	}
	if err := acquireVMOperation(opCtx, v); err != nil {
		return nil, err
	}
	// A record whose restart recovery could not prove ownership is resolved
	// before it is read or cut, live or offline.
	if err := m.requireResolved(v); err != nil {
		releaseVMOperation(v)
		return nil, err
	}
	v.mu.Lock()
	if v.deleting {
		v.mu.Unlock()
		releaseVMOperation(v)
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", v.id)
	}
	switch v.state {
	case nodev1.VMState_VM_STATE_READY:
	case nodev1.VMState_VM_STATE_STOPPED:
		v.mu.Unlock()
		releaseVMOperation(v)
		return m.snapshotStopped(opCtx, v, req)
	default:
		state := v.state
		v.mu.Unlock()
		releaseVMOperation(v)
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s is %s", v.id, vmStateName(state))
	}
	defer releaseVMOperation(v)
	if v.process == nil || v.client == nil {
		v.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s is not ready", v.id)
	}
	job := liveCut{
		v: v, proc: v.process, guest: v.client, meta: v.meta,
		qmp: cmpQMP(v.snapshotClient, qmpOf(v.process)),
		req: req, freeze: req.Freeze == nil || req.GetFreeze(),
	}
	// Published after the deleting check under the same lock: a DeleteVM
	// either set deleting first (and the check above refused) or finds this
	// fence.
	cutCtx, cancelCut := context.WithCancelCause(opCtx)
	defer cancelCut(nil)
	v.snapCancel = cancelCut
	v.mu.Unlock()
	defer func() {
		v.mu.Lock()
		v.snapCancel = nil
		v.mu.Unlock()
	}()
	gen, err := m.snapshotLive(cutCtx, &job)
	if errors.Is(context.Cause(cutCtx), errSnapshotFenced) {
		return nil, status.Errorf(codes.Aborted, "sandbox %s deleted during the snapshot: %v", v.id, err)
	}
	if err != nil {
		return nil, snapshotVMError(err)
	}
	return gen, nil
}

// vmStateOf reads v's state.
func vmStateOf(v *vm) nodev1.VMState {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.state
}

// cmpQMP prefers a rescued QMP connection over the process's own.
func cmpQMP(rescue, own *qmp.Client) *qmp.Client {
	if rescue != nil {
		return rescue
	}
	return own
}

// liveCut is one live SnapshotVM's inputs and the side effects its cleanup
// must undo.
type liveCut struct {
	v      *vm
	proc   VMProcess
	guest  guestv1.GuestClient
	qmp    *qmp.Client
	meta   storage.VMMeta
	req    *nodev1.SnapshotVMRequest
	freeze bool

	budget uint64 // fleece reservation, bytes
	fleece *os.File
	run    *snapshotRun
}

// snapshotLive runs a READY VM's cut (spike method A): fleece, marker,
// freeze, transaction, thaw, export, read, teardown. Every failure after the
// marker tears the graph down and merges bbraft-cut back into bbraft-dirty,
// so the next generation still holds every change.
func (m *Manager) snapshotLive(ctx context.Context, c *liveCut) (*nodev1.Generation, error) {
	id := c.v.id
	if c.qmp == nil {
		return nil, status.Errorf(codes.Internal, "sandbox %s: QEMU has no QMP connection", id)
	}
	switch s, known := m.learnPhase2(ctx, c.qmp); {
	case !known:
		return nil, status.Error(codes.FailedPrecondition, "this node could not check its QEMU for Phase 2 support")
	case !s.Available():
		return nil, status.Error(codes.FailedPrecondition, s.Reason())
	}
	img, ok := m.images[c.meta.Image]
	if !ok {
		return nil, status.Errorf(codes.Internal, "sandbox %s: image %q disappeared", id, c.meta.Image)
	}
	size, ok := resourcesOf(c.meta.Resources).diskBytes()
	if !ok || size == 0 {
		return nil, status.Errorf(codes.Internal, "sandbox %s: invalid disk size", id)
	}

	// A cut an earlier call could not settle is merged back first: its
	// leftovers would refuse this cut's names.
	if st, err := m.store.ReadPhase2(id); err != nil || st.SnapshotInProgress {
		m.settleLeftoverCut(ctx, c)
	}
	// A VM launched while the capability gate was unknown has no bbraft-dirty:
	// the cut's transaction would merge from a missing bitmap on every call.
	// The ensure step adds or repairs it and records a full scan, so it runs
	// before previousManifest reads that record.
	if err := m.ensureVMBitmap(ctx, id, c.qmp); err != nil {
		return nil, fmt.Errorf("dirty bitmap: %w", err)
	}
	prev, full := m.previousManifest(id, c.meta.Image, img.RootSHA256, size)

	gib := fleeceBudgetGiB(c.meta.Resources.DiskGiB)
	c.budget = gib << 30
	m.holdFleece(c.v, gib)
	if err := m.store.SetSnapshotInProgress(id); err != nil {
		m.releaseFleece(c.v)
		return nil, fmt.Errorf("record snapshot in progress: %w", err)
	}
	// From here on every return goes through fail or the success path.
	if err := m.store.RemovePhase2Scratch(id); err != nil {
		return m.failCut(c, fmt.Errorf("remove snapshot scratch: %w", err))
	}
	fleece, err := m.store.PrepareFleece(ctx, id, size)
	if err != nil {
		return m.failCut(c, fmt.Errorf("fleece image: %w", err))
	}
	c.fleece = fleece
	var fdset struct {
		ID int64 `json:"fdset-id"`
	}
	if err := c.qmp.ExecuteWithFiles(ctx, "add-fd", map[string]any{"opaque": fleeceFDOpaque}, &fdset, fleece); err != nil {
		return m.failCut(c, fmt.Errorf("add-fd fleece: %w", err))
	}
	if err := c.qmp.Execute(ctx, "blockdev-add", map[string]any{
		"driver": "qcow2", "node-name": fleeceNode,
		"file":    map[string]any{"driver": "file", "filename": fmt.Sprintf("/dev/fdset/%d", fdset.ID)},
		"backing": qemu.RootDiskNode,
	}, nil); err != nil {
		return m.failCut(c, fmt.Errorf("blockdev-add fleece: %w", err))
	}

	cutAt, err := m.cutLive(ctx, c)
	if err != nil {
		return m.failCut(c, err)
	}

	qemuSock, hostSock := m.nbdSocketPaths(id)
	if err := c.qmp.Execute(ctx, "nbd-server-start", map[string]any{
		"addr": map[string]any{"type": "unix", "data": map[string]any{"path": qemuSock}},
	}, nil); err != nil {
		return m.failCut(c, fmt.Errorf("nbd-server-start: %w", err))
	}
	if err := c.qmp.Execute(ctx, "block-export-add", map[string]any{
		"type": "nbd", "id": cutExportID, "node-name": fleeceNode, "name": cutExportName,
		"writable": false, "bitmaps": []string{qemu.CutBitmap}, "allocation-depth": true,
	}, nil); err != nil {
		return m.failCut(c, fmt.Errorf("block-export-add: %w", err))
	}
	open := m.openCutDisk
	if open == nil {
		open = openNBDDisk
	}
	disk, err := open(ctx, cutDiskOptions{Socket: hostSock, Conns: disksnap.DefaultWorkers, MaxDepth: liveSandboxDepth, PeerPID: c.proc.PID()})
	if err != nil {
		return m.failCut(c, fmt.Errorf("open cut export: %w", err))
	}
	if disk.Size() != size {
		disk.Close()
		return m.failCut(c, fmt.Errorf("cut export is %d bytes, the disk %d", disk.Size(), size))
	}
	params := disksnap.Params{
		Prev: prev, Full: full,
		ChainID: c.req.GetChainId(), Generation: c.req.GetGeneration(),
		SandboxID: id, Image: c.meta.Image, ImageSHA256: img.RootSHA256,
		CreatedAt: cutAt, Workers: disksnap.DefaultWorkers,
	}
	manifest, stats, err := disksnap.Snapshot(ctx, snapshotStore{m.cfg.Store}, m.guardFleece(c, disk), params)
	if closeErr := disk.Close(); closeErr != nil {
		m.log.Debug("node: close cut export", "sandbox", id, "error", closeErr)
	}
	if err != nil {
		return m.failCut(c, err)
	}
	if err := m.finishCut(c, manifest); err != nil {
		return nil, err
	}
	return &nodev1.Generation{
		ManifestKey:     snapformat.ManifestKey(manifest.ChainID, manifest.Generation),
		ChainId:         manifest.ChainID,
		Generation:      manifest.Generation,
		Image:           manifest.Image,
		DiskBytes:       manifest.DiskBytes,
		Chunks:          stats.Chunks,
		ChunksUploaded:  stats.ChunksUploaded,
		BytesUploaded:   stats.BytesUploaded,
		ContentBytes:    stats.ContentBytes,
		CreatedAtUnixMs: cutAt.UnixMilli(),
		Frozen:          c.freeze,
	}, nil
}

// cutLive freezes the guest (unless the request says not to), runs the cut
// transaction, proves the same freeze lease held through it and thaws. It
// returns the cut time. A guest whose thaw cannot be proven is marked
// FAILED, as for a Phase 1 snapshot.
func (m *Manager) cutLive(ctx context.Context, c *liveCut) (time.Time, error) {
	transaction := func(ctx context.Context) error { return cutTransaction(ctx, c.qmp, true) }
	if !c.freeze {
		if err := transaction(ctx); err != nil {
			return time.Time{}, err
		}
		return m.now(), nil
	}

	run := &snapshotRun{lease: cutFreezeLease, deadline: m.now().Add(cutFreezeLease)}
	c.run = run
	fail := func(cause error) (time.Time, error) {
		if cleanupErr := m.cleanupSnapshot(c.v, c.proc, nil, c.guest, run); cleanupErr != nil {
			cause = errors.Join(cause, cleanupErr)
		}
		if snapshotRuntimeUnsafe(run) {
			if markErr := m.markSnapshotRuntimeFailed(c.v, cause); markErr != nil {
				cause = errors.Join(cause, markErr)
			}
		}
		return time.Time{}, cause
	}
	if err := m.freezeSnapshotAttempt(ctx, c.guest, run); err != nil {
		return fail(fmt.Errorf("freeze guest: %w", err))
	}
	leaseCtx, cancel := context.WithDeadline(ctx, run.deadline)
	err := transaction(leaseCtx)
	cancel()
	cutAt := m.now()
	if err != nil {
		return fail(err)
	}
	if err := m.checkSnapshotDeadline(run); err != nil {
		return fail(err)
	}
	// The same lease must still be held: a replacement token means the
	// guest thawed (lease expiry, agent restart) before or during the cut.
	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), m.cfg.SnapshotCleanupTimeout)
	err = m.freezeSnapshotAttempt(verifyCtx, c.guest, run)
	cancelVerify()
	if err != nil {
		return fail(fmt.Errorf("verify freeze lease: %w", err))
	}
	if run.leaseChanged {
		return fail(errors.New("freeze lease changed during the cut"))
	}
	if err := m.thawSnapshot(c.guest, run); err != nil {
		return fail(err)
	}
	return cutAt, nil
}

// cutTransaction atomically starts a cut: bbraft-cut takes bbraft-dirty's
// bits and bbraft-dirty starts over; a live cut also starts the fleece
// backup job (copy-before-write of every later guest write).
func cutTransaction(ctx context.Context, client *qmp.Client, live bool) error {
	actions := []any{
		map[string]any{"type": "block-dirty-bitmap-add", "data": map[string]any{
			"node": qemu.RootDiskNode, "name": qemu.CutBitmap, "disabled": true, "persistent": false,
			"granularity": qemu.DirtyBitmapGranularity,
		}},
		map[string]any{"type": "block-dirty-bitmap-merge", "data": map[string]any{
			"node": qemu.RootDiskNode, "target": qemu.CutBitmap, "bitmaps": []string{qemu.DirtyBitmap},
		}},
		map[string]any{"type": "block-dirty-bitmap-clear", "data": map[string]any{
			"node": qemu.RootDiskNode, "name": qemu.DirtyBitmap,
		}},
	}
	if live {
		actions = append(actions, map[string]any{"type": "blockdev-backup", "data": map[string]any{
			"job-id": fleeceNode, "device": qemu.RootDiskNode, "target": fleeceNode,
			"sync": "none", "filter-node-name": "bbraft-backup-top",
		}})
	}
	if err := client.Execute(ctx, "transaction", map[string]any{"actions": actions}, nil); err != nil {
		return fmt.Errorf("cut transaction: %w", err)
	}
	return nil
}

// snapshotStopped reads a STOPPED VM's kept disk in a maintenance session:
// the cut transaction without a backup job, a read-only export of rootdisk
// with bbraft-cut and allocation depths, the read, then bbraft-cut removed
// (after the last manifest is durable) or merged back. The session's quit
// stores bbraft-dirty cleanly. The marker covers a node restart during the
// session: bbraft-cut is not persistent, so a lost session forces a full
// scan.
func (m *Manager) snapshotStopped(ctx context.Context, v *vm, req *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
	var gen *nodev1.Generation
	err := m.maintenance(ctx, v, func(ctx context.Context, s *maintSession) error {
		v.mu.Lock()
		state, meta := v.state, v.meta
		v.mu.Unlock()
		if state != nodev1.VMState_VM_STATE_STOPPED {
			// The session also serves creates in progress; a VM that
			// started meanwhile is not this call's business.
			return status.Errorf(codes.FailedPrecondition, "sandbox %s is %s", v.id, vmStateName(state))
		}
		var err error
		gen, err = m.readStoppedCut(ctx, v, s, meta, req)
		return err
	})
	if err != nil {
		return nil, snapshotVMError(err)
	}
	return gen, nil
}

// readStoppedCut is snapshotStopped's work inside the session.
func (m *Manager) readStoppedCut(ctx context.Context, v *vm, s *maintSession, meta storage.VMMeta, req *nodev1.SnapshotVMRequest) (*nodev1.Generation, error) {
	id := v.id
	client := s.QMP()
	switch sup, known := m.learnPhase2(ctx, client); {
	case !known:
		return nil, status.Error(codes.FailedPrecondition, "this node could not check its QEMU for Phase 2 support")
	case !sup.Available():
		return nil, status.Error(codes.FailedPrecondition, sup.Reason())
	}
	img, ok := m.images[meta.Image]
	if !ok {
		return nil, status.Errorf(codes.Internal, "sandbox %s: image %q disappeared", id, meta.Image)
	}
	size, ok := resourcesOf(meta.Resources).diskBytes()
	if !ok || size == 0 {
		return nil, status.Errorf(codes.Internal, "sandbox %s: invalid disk size", id)
	}
	prev, full := m.previousManifest(id, meta.Image, img.RootSHA256, size)
	if err := m.store.SetSnapshotInProgress(id); err != nil {
		return nil, fmt.Errorf("record snapshot in progress: %w", err)
	}
	fail := func(cause error) (*nodev1.Generation, error) {
		cctx, cancel := context.WithTimeout(context.Background(), m.cfg.SnapshotCleanupTimeout)
		defer cancel()
		if err := s.StopNBD(cctx); err != nil {
			m.log.Warn("node: stopped snapshot NBD teardown", "sandbox", id, "error", err)
		}
		if err := teardownCut(cctx, client); err != nil {
			m.log.Warn("node: failed snapshot not merged back; next snapshot scans the whole disk", "sandbox", id, "error", err)
			if err := m.store.SetFullScanNext(id); err != nil {
				m.log.Warn("node: record full scan next", "sandbox", id, "error", err)
				// The marker stays: a restart forces the full scan.
				return nil, cause
			}
		}
		// bbraft-cut is not persistent: once this session quits there is
		// nothing left to recover.
		if err := m.store.ClearSnapshotInProgress(id); err != nil {
			m.log.Warn("node: clear snapshot marker", "sandbox", id, "error", err)
		}
		return nil, cause
	}
	if err := cutTransaction(ctx, client, false); err != nil {
		return fail(err)
	}
	cutAt := m.now()
	host, err := s.StartNBD(ctx)
	if err != nil {
		return fail(err)
	}
	if err := client.Execute(ctx, "block-export-add", map[string]any{
		"type": "nbd", "id": cutExportID, "node-name": qemu.RootDiskNode, "name": cutExportName,
		"writable": false, "bitmaps": []string{qemu.CutBitmap}, "allocation-depth": true,
	}, nil); err != nil {
		return fail(fmt.Errorf("block-export-add: %w", err))
	}
	open := m.openCutDisk
	if open == nil {
		open = openNBDDisk
	}
	disk, err := open(ctx, cutDiskOptions{Socket: host, Conns: disksnap.DefaultWorkers, MaxDepth: stoppedSandboxDepth, PeerPID: s.PID()})
	if err != nil {
		return fail(fmt.Errorf("open cut export: %w", err))
	}
	if disk.Size() != size {
		disk.Close()
		return fail(fmt.Errorf("cut export is %d bytes, the disk %d", disk.Size(), size))
	}
	manifest, stats, err := disksnap.Snapshot(ctx, snapshotStore{m.cfg.Store}, disk, disksnap.Params{
		Prev: prev, Full: full,
		ChainID: req.GetChainId(), Generation: req.GetGeneration(),
		SandboxID: id, Image: meta.Image, ImageSHA256: img.RootSHA256,
		CreatedAt: cutAt, Workers: disksnap.DefaultWorkers,
	})
	if closeErr := disk.Close(); closeErr != nil {
		m.log.Debug("node: close cut export", "sandbox", id, "error", closeErr)
	}
	if err != nil {
		return fail(err)
	}

	cctx, cancel := context.WithTimeout(context.Background(), m.cfg.SnapshotCleanupTimeout)
	defer cancel()
	if err := s.StopNBD(cctx); err != nil {
		// An export that would not go away may still hold bbraft-cut:
		// fail safe (merge back, or a full scan) rather than record a
		// manifest whose cut cannot be settled.
		return fail(err)
	}
	key := snapformat.ManifestKey(manifest.ChainID, manifest.Generation)
	encoded, err := snapformat.EncodeManifest(manifest)
	if err == nil {
		err = m.store.SetLastManifest(id, key, encoded)
	}
	if err != nil {
		return fail(fmt.Errorf("record last manifest %s: %w", key, err))
	}
	if err := m.store.ClearFullScanNext(id); err != nil {
		m.log.Warn("node: clear full scan next", "sandbox", id, "error", err)
	}
	if err := client.Execute(cctx, "block-dirty-bitmap-remove", map[string]string{"node": qemu.RootDiskNode, "name": qemu.CutBitmap}, nil); err != nil {
		// Not persistent: the session's quit drops it anyway.
		m.log.Warn("node: remove cut bitmap", "sandbox", id, "error", err)
	}
	if err := m.store.ClearSnapshotInProgress(id); err != nil {
		m.log.Warn("node: clear snapshot marker", "sandbox", id, "error", err)
	}
	return &nodev1.Generation{
		ManifestKey:     key,
		ChainId:         manifest.ChainID,
		Generation:      manifest.Generation,
		Image:           manifest.Image,
		DiskBytes:       manifest.DiskBytes,
		Chunks:          stats.Chunks,
		ChunksUploaded:  stats.ChunksUploaded,
		BytesUploaded:   stats.BytesUploaded,
		ContentBytes:    stats.ContentBytes,
		CreatedAtUnixMs: cutAt.UnixMilli(),
	}, nil
}

// finishCut settles a written manifest in the order that never loses a
// change: the graph is torn down, the local last manifest is made durable,
// and only then is bbraft-cut removed (before that, a crash merges it back).
// A failed local record merges the cut back and fails the call; a cut that
// cannot be removed, or a graph not proven gone, keeps the marker (the next
// SnapshotVM or a restart merges it back) and still succeeds: the manifest
// is written and correct.
func (m *Manager) finishCut(c *liveCut, manifest *snapformat.Manifest) error {
	id := c.v.id
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.SnapshotCleanupTimeout)
	defer cancel()
	graphErr := teardownCutGraph(ctx, c.qmp)
	if graphErr == nil {
		graphErr = cutGraphGone(ctx, c.qmp)
	}
	key := snapformat.ManifestKey(manifest.ChainID, manifest.Generation)
	encoded, err := snapformat.EncodeManifest(manifest)
	if err == nil {
		err = m.store.SetLastManifest(id, key, encoded)
	}
	if err != nil {
		_, ferr := m.failCut(c, fmt.Errorf("record last manifest %s: %w", key, err))
		return ferr
	}
	if err := m.store.ClearFullScanNext(id); err != nil {
		// A stale flag costs one more full scan, nothing else.
		m.log.Warn("node: clear full scan next", "sandbox", id, "error", err)
	}
	removeErr := c.qmp.Execute(ctx, "block-dirty-bitmap-remove", map[string]string{"node": qemu.RootDiskNode, "name": qemu.CutBitmap}, nil)
	if graphErr != nil || removeErr != nil {
		m.log.Warn("node: snapshot written but its cut is not fully torn down; the next snapshot merges it back",
			"sandbox", id, "manifest", key, "graph", errorText(graphErr), "remove_cut", errorText(removeErr))
		m.dropFleeceFile(c)
		if graphErr == nil {
			m.releaseFleece(c.v)
		}
		return nil
	}
	if err := m.store.ClearSnapshotInProgress(id); err != nil {
		m.log.Warn("node: clear snapshot marker", "sandbox", id, "error", err)
	}
	m.dropFleeceFile(c)
	m.releaseFleece(c.v)
	return nil
}

// failCut tears a failed cut down and merges bbraft-cut back. A merge that
// cannot be proven forces a full scan; a teardown that failed keeps the
// marker, so the next SnapshotVM (or a restart) retries it. The fleece
// reservation is released only once QEMU is proven to hold no fleece.
func (m *Manager) failCut(c *liveCut, cause error) (*nodev1.Generation, error) {
	id := c.v.id
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.SnapshotCleanupTimeout)
	defer cancel()
	tearErr := teardownCut(ctx, c.qmp)
	goneErr := cutGraphGone(ctx, c.qmp)
	if tearErr != nil {
		m.log.Warn("node: failed snapshot not merged back; next snapshot scans the whole disk", "sandbox", id, "error", tearErr)
		if err := m.store.SetFullScanNext(id); err != nil {
			m.log.Warn("node: record full scan next", "sandbox", id, "error", err)
		}
	} else if err := m.store.ClearSnapshotInProgress(id); err != nil {
		m.log.Warn("node: clear snapshot marker", "sandbox", id, "error", err)
	}
	m.dropFleeceFile(c)
	if goneErr == nil {
		m.releaseFleece(c.v)
	}
	return nil, cause
}

// settleLeftoverCut merges back a cut an earlier call or node process left
// behind, before a new one starts. The reservation it held is released
// once QEMU holds no fleece.
func (m *Manager) settleLeftoverCut(ctx context.Context, c *liveCut) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.cfg.SnapshotCleanupTimeout)
	defer cancel()
	m.recoverCutLive(cctx, c.qmp, c.v.id)
	if cutGraphGone(cctx, c.qmp) == nil {
		m.releaseFleece(c.v)
	}
}

// dropFleeceFile closes the node's fleece descriptor and removes the
// snapshot scratch (fleece file, NBD socket).
func (m *Manager) dropFleeceFile(c *liveCut) {
	if c.fleece != nil {
		c.fleece.Close()
		c.fleece = nil
	}
	if err := m.store.RemovePhase2Scratch(c.v.id); err != nil {
		m.log.Warn("node: remove snapshot scratch", "sandbox", c.v.id, "error", err)
	}
}

// previousManifest returns the VM's local last manifest for an incremental
// generation, or full = true when the next generation must scan the whole
// disk: no usable previous manifest, the bitmap is untrusted, or the
// previous manifest does not describe this disk (image, size, chunk size).
func (m *Manager) previousManifest(id, image, imageSHA string, size uint64) (*snapformat.Manifest, bool) {
	st, err := m.store.ReadPhase2(id)
	if err != nil || st.FullScanNext {
		return nil, true
	}
	_, encoded, ok, err := m.store.LastManifest(id)
	if err != nil || !ok {
		return nil, true
	}
	prev, err := snapformat.DecodeManifest(encoded)
	if err != nil {
		m.log.Warn("node: local last manifest unreadable; scanning the whole disk", "sandbox", id, "error", err)
		return nil, true
	}
	switch {
	case prev.Image != image,
		prev.ImageSHA256 != "" && imageSHA != "" && prev.ImageSHA256 != imageSHA,
		prev.DiskBytes > size,
		prev.ChunkSize != snapformat.ChunkSize:
		m.log.Info("node: last manifest does not describe this disk; scanning the whole disk", "sandbox", id,
			"prev_image", prev.Image, "image", image, "prev_disk_bytes", prev.DiskBytes, "disk_bytes", size)
		return nil, true
	}
	return prev, false
}

// nbdSocketPaths returns the NBD server socket of VM id as QEMU names it
// and as the node dials it: /control/nbd.sock inside a jail, the VM
// directory otherwise.
func (m *Manager) nbdSocketPaths(id string) (qemuPath, hostPath string) {
	if m.jail != nil {
		// The control directory is attached where the jailed QMP socket
		// is, as for a maintenance session.
		return path.Join(path.Dir(qemu.JailQMPPath), storage.NBDSocketName),
			filepath.Join(m.store.VMDir(id), storage.ControlDirName, storage.NBDSocketName)
	}
	p := filepath.Join(m.store.VMDir(id), storage.NBDSocketName)
	return p, p
}

// fleeceBudgetGiB is a fleece image's reservation for a disk of diskGiB:
// min(disk, max(1, ceil(disk/8))) GiB.
func fleeceBudgetGiB(diskGiB uint64) uint64 {
	return min(diskGiB, max(1, (diskGiB+7)/8))
}

// holdFleece force-reserves gib GiB of disk for v's fleece image (it never
// refuses: the free-space floor is the hard bound, the reservation keeps
// CreateVM from promising the same bytes). A reservation an unsettled
// earlier cut still holds is reused.
func (m *Manager) holdFleece(v *vm, gib uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v.fleeceGiB >= gib {
		return
	}
	delta := gib - v.fleeceGiB
	total, ok := m.reserved.add(resources{diskGiB: delta})
	if !ok {
		total = resources{vcpus: m.reserved.vcpus, memoryMib: m.reserved.memoryMib, diskGiB: ^uint64(0)}
	}
	m.reserved = total
	v.fleeceGiB = gib
}

// releaseFleece returns v's fleece reservation.
func (m *Manager) releaseFleece(v *vm) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseFleeceLocked(v)
}

// releaseFleeceLocked returns v's fleece reservation. m.mu must be held.
func (m *Manager) releaseFleeceLocked(v *vm) {
	m.reserved.diskGiB = subClamp(m.reserved.diskGiB, v.fleeceGiB)
	v.fleeceGiB = 0
}

// guardFleece wraps disk so every chunk read first checks the fleece's
// bounds: its allocated size within the reservation, and the data
// directory's free space above the floor.
func (m *Manager) guardFleece(c *liveCut, disk cutDisk) disksnap.Disk {
	return guardedDisk{cutDisk: disk, check: func() error {
		usage := m.fleeceUsage
		if usage == nil {
			usage = fleeceAllocated
		}
		used, err := usage(c.fleece)
		if err != nil {
			return fmt.Errorf("stat fleece: %w", err)
		}
		if used > c.budget {
			m.log.Warn("node: snapshot aborted: the fleece image outgrew its reservation", "sandbox", c.v.id,
				"fleece_bytes", used, "reserved_bytes", c.budget)
			return fmt.Errorf("%w: fleece holds %d bytes, reserved %d", errSnapshotSpace, used, c.budget)
		}
		free, err := m.diskFree()
		if err != nil {
			return fmt.Errorf("disk free: %w", err)
		}
		if free < m.cfg.SnapshotFreeFloor {
			m.log.Warn("node: snapshot aborted: node disk below the snapshot floor", "sandbox", c.v.id,
				"free_bytes", free, "floor_bytes", m.cfg.SnapshotFreeFloor)
			return fmt.Errorf("%w: %d bytes free, floor %d", errSnapshotSpace, free, m.cfg.SnapshotFreeFloor)
		}
		return nil
	}}
}

// fleeceAllocated is the fleece file's allocated size (st_blocks).
func fleeceAllocated(f *os.File) (uint64, error) {
	var st syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &st); err != nil {
		return 0, err
	}
	return uint64(st.Blocks) * 512, nil
}

// guardedDisk runs check before every chunk read.
type guardedDisk struct {
	cutDisk
	check func() error
}

func (g guardedDisk) ReadChunk(ctx context.Context, i uint64, buf []byte) ([]byte, error) {
	if err := g.check(); err != nil {
		return nil, err
	}
	return g.cutDisk.ReadChunk(ctx, i, buf)
}

// objectStoreError marks an error the object store returned, so SnapshotVM
// maps it to UNAVAILABLE whatever wraps it.
type objectStoreError struct{ err error }

func (e *objectStoreError) Error() string { return "object store: " + e.err.Error() }
func (e *objectStoreError) Unwrap() error { return e.err }

// snapshotStore marks the store's errors (objectStoreError). ErrNotFound and
// ErrExists keep their meaning through errors.Is.
type snapshotStore struct{ objstore.Store }

func markStoreError(err error) error {
	if err == nil {
		return nil
	}
	return &objectStoreError{err: err}
}

func (s snapshotStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	return markStoreError(s.Store.PutIfAbsent(ctx, key, r, size))
}

func (s snapshotStore) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	rc, err := s.Store.Get(ctx, key, offset, length)
	return rc, markStoreError(err)
}

func (s snapshotStore) Head(ctx context.Context, key string) (objstore.Info, error) {
	info, err := s.Store.Head(ctx, key)
	return info, markStoreError(err)
}

func (s snapshotStore) List(ctx context.Context, prefix string) iter.Seq2[objstore.Info, error] {
	return func(yield func(objstore.Info, error) bool) {
		for info, err := range s.Store.List(ctx, prefix) {
			if !yield(info, markStoreError(err)) {
				return
			}
		}
	}
}

// snapshotVMError maps a SnapshotVM failure to nodefake's codes: an existing
// manifest is ALREADY_EXISTS, context errors keep their codes, object store
// errors are UNAVAILABLE, a fleece out of room is RESOURCE_EXHAUSTED and
// everything else INTERNAL. Errors that already carry a code keep it.
func snapshotVMError(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, disksnap.ErrExists):
		return status.Errorf(codes.AlreadyExists, "%v", err)
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, errSnapshotSpace):
		return status.Errorf(codes.ResourceExhausted, "%v", err)
	}
	if _, ok := errors.AsType[*objectStoreError](err); ok {
		return status.Errorf(codes.Unavailable, "%v", err)
	}
	return status.Errorf(codes.Internal, "snapshot: %v", err)
}
