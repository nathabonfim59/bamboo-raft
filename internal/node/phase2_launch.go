// Phase 2 launch, adoption and restart recovery: the paused-launch dirty
// bitmap step (D2), the QMP capability gate and the recovery of a live
// snapshot cut a node restart interrupted.

package node

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

// Fixed QMP names of a live cut (D4, method A). A restarted node finds a
// cut's leftovers by these names.
const (
	// fleeceNode is the fleece qcow2's block node and the backup job id.
	fleeceNode = "bbraft-fleece"
	// cutExportID is the NBD export of the cut.
	cutExportID = "bbraft-cut"
	// fleeceFDOpaque tags the fleece image's fd set (add-fd opaque).
	fleeceFDOpaque = "bbraft-fleece"
)

// phase2Gate caches the result of the Phase 2 QMP capability check for the
// node's lifetime. It is filled at New from the startup CPU probe when the
// probe could read the schema, or by the first QMP connection otherwise.
type phase2Gate struct {
	mu      sync.Mutex
	known   bool
	support qemu.Phase2Support
}

// setPhase2Support records a check result once and logs an unavailable
// Phase 2 exactly then.
func (m *Manager) setPhase2Support(s qemu.Phase2Support) {
	m.phase2.mu.Lock()
	if m.phase2.known {
		m.phase2.mu.Unlock()
		return
	}
	m.phase2.known, m.phase2.support = true, s
	m.phase2.mu.Unlock()
	if !s.Available() {
		m.log.Warn("node: "+s.Reason()+"; snapshots and restores are refused, VMs run without dirty bitmaps",
			"qemu", s.QEMUVersion, "missing", s.Missing)
		return
	}
	m.log.Info("node: QEMU supports Phase 2 snapshots", "qemu", s.QEMUVersion)
}

// phase2Support returns the cached check result and whether there is one.
func (m *Manager) phase2Support() (qemu.Phase2Support, bool) {
	m.phase2.mu.Lock()
	defer m.phase2.mu.Unlock()
	return m.phase2.support, m.phase2.known
}

// learnPhase2 runs the capability check on client when no result is cached
// yet. A schema that cannot be read leaves the gate unknown for the next
// connection.
func (m *Manager) learnPhase2(ctx context.Context, client *qmp.Client) (qemu.Phase2Support, bool) {
	if s, ok := m.phase2Support(); ok {
		return s, true
	}
	s, err := qemu.CheckPhase2Schema(ctx, client)
	if err != nil {
		m.log.Warn("node: Phase 2 capability check failed; retrying on the next QEMU", "error", err)
		return qemu.Phase2Support{}, false
	}
	m.setPhase2Support(s)
	return m.phase2Support()
}

// phase2Unavailable is the FAILED_PRECONDITION for SnapshotVM and CreateVM
// from_manifest when this node's QEMU lacks something Phase 2 needs. An
// unknown result (no QEMU checked yet) is not a refusal: those operations
// connect to a QEMU and check it then.
func (m *Manager) phase2Unavailable() error {
	if s, ok := m.phase2Support(); ok && !s.Available() {
		return status.Error(codes.FailedPrecondition, s.Reason())
	}
	return nil
}

// qmpOf returns a process's QMP control connection; processes without one
// (test fakes) report nil.
func qmpOf(proc VMProcess) *qmp.Client {
	src, ok := proc.(interface{ QMP() *qmp.Client })
	if !ok {
		return nil
	}
	return src.QMP()
}

// ensureVMBitmap runs the dirty bitmap step for VM id on client only when
// this node's QEMU is known to support Phase 2. Otherwise the VM runs
// exactly as in Phase 1 (the caller just conts), so an old QEMU or a schema
// that could not be read never fails a boot. An unknown gate also records
// "full scan next": nothing checked the bitmap, so a snapshot taken once
// the gate is known must not trust it. A repair that loses the bitmap's
// history records "full scan next" durably before it changes anything.
func (m *Manager) ensureVMBitmap(ctx context.Context, id string, client *qmp.Client) error {
	s, known := m.learnPhase2(ctx, client)
	if !known {
		if err := m.store.SetFullScanNext(id); err != nil {
			return fmt.Errorf("record full scan next: %w", err)
		}
		return nil
	}
	if !s.Available() {
		return nil
	}
	res, err := qemu.EnsureDirtyBitmap(ctx, client, id, func(r qemu.BitmapResult) error {
		if err := m.store.SetFullScanNext(id); err != nil {
			return fmt.Errorf("record full scan next: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if res.Action != qemu.BitmapKept {
		m.log.Info("node: dirty bitmap repaired", "sandbox", id, "action", string(res.Action), "reason", res.Reason, "full_scan_next", res.FullScan)
	}
	return nil
}

// startGuest is the paused launch's last step (D2): with QEMU connected and
// still in prelaunch, make the dirty bitmap healthy, then cont. runBoot
// calls it before anything waits for the guest; its error is a boot
// failure. A process without QMP (a test fake) was never paused.
func (m *Manager) startGuest(ctx context.Context, id string, proc VMProcess) error {
	client := qmpOf(proc)
	if client == nil {
		return nil
	}
	if err := m.ensureVMBitmap(ctx, id, client); err != nil {
		return fmt.Errorf("dirty bitmap: %w", err)
	}
	if err := qemu.Resume(ctx, client, id); err != nil {
		return fmt.Errorf("start guest: %w", err)
	}
	return nil
}

// adoptPhase2 runs on a verified QEMU found after a node restart, in the QMP
// status the verification observed. It first
// recovers an interrupted live cut, then makes the dirty bitmap healthy and
// resumes a QEMU that is not running:
//
//   - prelaunch: the node died between the paused launch and cont. The
//     bitmap step runs exactly as at launch and its failure fails the
//     verification (the QEMU is never started without its bitmap).
//   - paused or running: the guest already ran. A Phase 1 QEMU adopted
//     after an upgrade gets its bitmap here (with a full scan next). A
//     failure is logged and only forces a full scan: the VM stays usable.
func (m *Manager) adoptPhase2(ctx context.Context, client *qmp.Client, id, qmpStatus string) error {
	m.recoverCutLive(ctx, client, id)
	if err := m.ensureVMBitmap(ctx, id, client); err != nil {
		if qmpStatus == "prelaunch" {
			return fmt.Errorf("dirty bitmap before first cont: %w", err)
		}
		m.log.Warn("node: dirty bitmap check on an adopted vm failed; forcing a full scan", "sandbox", id, "error", err)
		if serr := m.store.SetFullScanNext(id); serr != nil {
			m.log.Warn("node: record full scan next", "sandbox", id, "error", serr)
		}
	}
	if qmpStatus == "running" {
		return nil
	}
	return resumePaused(ctx, client)
}

// recoverCutLive settles a live cut the restart interrupted while QEMU kept
// running: it tears the cut's export, NBD server, backup job, fleece node
// and fd set down (best effort: each may already be gone), merges
// bbraft-cut back into bbraft-dirty and removes it, deletes the fleece file
// and clears the marker. If the merge cannot be proven, the next snapshot
// scans fully instead.
func (m *Manager) recoverCutLive(ctx context.Context, client *qmp.Client, id string) {
	st, err := m.store.ReadPhase2(id)
	if err != nil && !errors.Is(err, storage.ErrPhase2StateCorrupt) {
		m.log.Warn("node: read phase 2 state; forcing a full scan", "sandbox", id, "error", err)
		_ = m.store.SetFullScanNext(id)
		return
	}
	if err == nil && !st.SnapshotInProgress {
		return
	}
	m.log.Warn("node: a live snapshot was interrupted; merging its cut back", "sandbox", id)
	merged := teardownCut(ctx, client)
	if err := m.store.RemovePhase2Scratch(id); err != nil {
		m.log.Warn("node: remove snapshot scratch", "sandbox", id, "error", err)
	}
	if err := m.store.RecoverInterruptedSnapshot(id, merged != nil); err != nil {
		m.log.Warn("node: clear snapshot marker", "sandbox", id, "error", err)
		return
	}
	if merged != nil {
		m.log.Warn("node: interrupted cut not merged back; next snapshot scans the whole disk", "sandbox", id, "error", merged)
	}
}

// teardownCut removes every QMP object of a live cut, in the spike's order,
// and merges bbraft-cut back into bbraft-dirty. Steps whose object is
// already gone are fine. It returns nil only when no bbraft-cut is left and
// its dirty set (if it existed) is part of bbraft-dirty again.
func teardownCut(ctx context.Context, client *qmp.Client) error {
	if err := teardownCutGraph(ctx, client); err != nil {
		return err
	}
	nodes, err := qemu.QueryNamedBlockNodes(ctx, client)
	if err != nil {
		return err
	}
	if _, found, _ := qemu.FindBitmap(nodes, qemu.RootDiskNode, qemu.CutBitmap); !found {
		// The cut's transaction never ran, or its teardown finished: the
		// bitmap was never cleared, or the cut was already merged.
		return nil
	}
	_, dirty, _ := qemu.FindBitmap(nodes, qemu.RootDiskNode, qemu.DirtyBitmap)
	var mergeErr error
	if dirty {
		mergeErr = client.Execute(ctx, "block-dirty-bitmap-merge", map[string]any{
			"node": qemu.RootDiskNode, "target": qemu.DirtyBitmap, "bitmaps": []string{qemu.CutBitmap},
		}, nil)
	} else {
		mergeErr = fmt.Errorf("%s is missing; the cut cannot be merged back", qemu.DirtyBitmap)
	}
	removeErr := client.Execute(ctx, "block-dirty-bitmap-remove", map[string]string{"node": qemu.RootDiskNode, "name": qemu.CutBitmap}, nil)
	if mergeErr != nil {
		return fmt.Errorf("merge %s into %s: %w", qemu.CutBitmap, qemu.DirtyBitmap, mergeErr)
	}
	if removeErr != nil {
		// Merged, but a leftover cut bitmap would refuse the next cut's
		// transaction: reported as a failure, so the caller is conservative.
		return fmt.Errorf("remove %s: %w", qemu.CutBitmap, removeErr)
	}
	return nil
}

// teardownCutGraph removes a cut's export, NBD server, backup job, fleece
// node and fd set, in the spike's order, leaving the bitmaps alone. Objects
// already gone are fine. The export is waited out before the fleece node is
// deleted (block-export-del only requests the removal).
func teardownCutGraph(ctx context.Context, client *qmp.Client) error {
	_ = client.Execute(ctx, "block-export-del", map[string]any{"id": cutExportID, "mode": "hard"}, nil)
	if err := waitCutExportGone(ctx, client); err != nil {
		return err
	}
	_ = client.Execute(ctx, "nbd-server-stop", nil, nil)
	if err := cancelFleeceJob(ctx, client); err != nil {
		return err
	}
	_ = client.Execute(ctx, "blockdev-del", map[string]string{"node-name": fleeceNode}, nil)
	removeFleeceFDSets(ctx, client)
	return nil
}

// waitCutExportGone polls query-block-exports until the cut's export is no
// longer listed, bounded by ctx.
func waitCutExportGone(ctx context.Context, client *qmp.Client) error {
	type blockExport struct {
		ID string `json:"id"`
	}
	for {
		var exports []blockExport
		if err := client.Execute(ctx, "query-block-exports", nil, &exports); err != nil {
			return fmt.Errorf("query-block-exports: %w", err)
		}
		if !slices.ContainsFunc(exports, func(e blockExport) bool { return e.ID == cutExportID }) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for export %s to end: %w", cutExportID, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// cutGraphGone reports whether QEMU no longer holds the fleece image: no
// fleece block node and no fleece fd set.
func cutGraphGone(ctx context.Context, client *qmp.Client) error {
	nodes, err := qemu.QueryNamedBlockNodes(ctx, client)
	if err != nil {
		return err
	}
	if slices.ContainsFunc(nodes, func(n qemu.NamedBlockNode) bool { return n.NodeName == fleeceNode }) {
		return fmt.Errorf("block node %s is still present", fleeceNode)
	}
	var sets []struct {
		FDs []struct {
			Opaque string `json:"opaque"`
		} `json:"fds"`
	}
	if err := client.Execute(ctx, "query-fdsets", nil, &sets); err != nil {
		return fmt.Errorf("query-fdsets: %w", err)
	}
	for _, s := range sets {
		for _, fd := range s.FDs {
			if fd.Opaque == fleeceFDOpaque {
				return fmt.Errorf("fd set of %s is still present", fleeceNode)
			}
		}
	}
	return nil
}

// cancelFleeceJob cancels the cut's backup job and waits until QEMU no
// longer lists it, bounded by ctx.
func cancelFleeceJob(ctx context.Context, client *qmp.Client) error {
	type job struct {
		ID string `json:"id"`
	}
	present := func() (bool, error) {
		var jobs []job
		if err := client.Execute(ctx, "query-jobs", nil, &jobs); err != nil {
			return false, fmt.Errorf("query-jobs: %w", err)
		}
		return slices.ContainsFunc(jobs, func(j job) bool { return j.ID == fleeceNode }), nil
	}
	ok, err := present()
	if err != nil || !ok {
		return err
	}
	if err := client.Execute(ctx, "block-job-cancel", map[string]any{"device": fleeceNode, "force": true}, nil); err != nil {
		return fmt.Errorf("block-job-cancel %s: %w", fleeceNode, err)
	}
	for {
		ok, err := present()
		if err != nil || !ok {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %s to end: %w", fleeceNode, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// removeFleeceFDSets removes the fd sets add-fd created for a fleece image.
func removeFleeceFDSets(ctx context.Context, client *qmp.Client) {
	var sets []struct {
		ID  int64 `json:"fdset-id"`
		FDs []struct {
			Opaque string `json:"opaque"`
		} `json:"fds"`
	}
	if err := client.Execute(ctx, "query-fdsets", nil, &sets); err != nil {
		return
	}
	for _, s := range sets {
		for _, fd := range s.FDs {
			if fd.Opaque == fleeceFDOpaque {
				_ = client.Execute(ctx, "remove-fd", map[string]int64{"fdset-id": s.ID}, nil)
				break
			}
		}
	}
}

// recoverCutsWithoutQEMU settles, after every record's fate is decided, the
// live-cut markers no adoption cleared. A VM whose QEMU is proven gone lost
// the cut's dirty set with the process: the marker is cleared, a full scan
// is forced and the scratch removed. A VM whose process could not be
// resolved keeps its marker (a later restart may still reach that QEMU) and
// gets a full scan; so does an adopted VM whose live recovery failed.
func (m *Manager) recoverCutsWithoutQEMU() {
	m.mu.Lock()
	vms := make([]*vm, 0, len(m.vms))
	for _, v := range m.vms {
		vms = append(vms, v)
	}
	m.mu.Unlock()
	for _, v := range vms {
		st, err := m.store.ReadPhase2(v.id)
		if err != nil && !errors.Is(err, storage.ErrPhase2StateCorrupt) {
			m.log.Warn("node: read phase 2 state", "sandbox", v.id, "error", err)
			continue
		}
		if err == nil && !st.SnapshotInProgress {
			continue
		}
		v.mu.Lock()
		live := v.process != nil || v.ownershipUnresolved
		v.mu.Unlock()
		if live {
			if err := m.store.SetFullScanNext(v.id); err != nil {
				m.log.Warn("node: record full scan next", "sandbox", v.id, "error", err)
			}
			continue
		}
		m.log.Warn("node: a live snapshot was interrupted and its QEMU is gone; next snapshot scans the whole disk", "sandbox", v.id)
		if err := m.store.RemovePhase2Scratch(v.id); err != nil {
			m.log.Warn("node: remove snapshot scratch", "sandbox", v.id, "error", err)
		}
		if err := m.store.RecoverInterruptedSnapshot(v.id, true); err != nil {
			m.log.Warn("node: clear snapshot marker", "sandbox", v.id, "error", err)
		}
	}
}
