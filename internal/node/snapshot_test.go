package node

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// snapshotGuest wraps the real guest service with deterministic RPC faults.
type snapshotGuest struct {
	guestv1.GuestServer
	mu              sync.Mutex
	freezeErrs      []error
	freezeRemaining []uint64
	thawErrs        []error
	thawAlways      error
	replaceOnVerify bool
	freezeCalls     int
}

func (g *snapshotGuest) Freeze(ctx context.Context, req *guestv1.FreezeRequest) (*guestv1.FreezeResponse, error) {
	g.mu.Lock()
	g.freezeCalls++
	call := g.freezeCalls
	var injected error
	if len(g.freezeErrs) > 0 {
		injected = g.freezeErrs[0]
		g.freezeErrs = g.freezeErrs[1:]
	}
	var remaining uint64
	hasRemaining := false
	if len(g.freezeRemaining) > 0 {
		remaining = g.freezeRemaining[0]
		g.freezeRemaining = g.freezeRemaining[1:]
		hasRemaining = true
	}
	replace := g.replaceOnVerify && call == 2
	g.mu.Unlock()
	if injected != nil {
		return nil, injected
	}
	resp, err := g.GuestServer.Freeze(ctx, req)
	if replace && err == nil {
		if _, thawErr := g.GuestServer.Thaw(context.Background(), &guestv1.ThawRequest{LeaseToken: resp.GetLeaseToken()}); thawErr != nil {
			return nil, thawErr
		}
		resp, err = g.GuestServer.Freeze(ctx, req)
	}
	if err == nil && hasRemaining {
		resp.LeaseRemainingMs = remaining
	}
	return resp, err
}

func (g *snapshotGuest) Thaw(ctx context.Context, req *guestv1.ThawRequest) (*guestv1.ThawResponse, error) {
	g.mu.Lock()
	injected := g.thawAlways
	if injected == nil && len(g.thawErrs) > 0 {
		injected = g.thawErrs[0]
		g.thawErrs = g.thawErrs[1:]
	}
	g.mu.Unlock()
	if injected != nil {
		return nil, injected
	}
	return g.GuestServer.Thaw(ctx, req)
}

// droppedFreezeGuest performs the real Freeze (possibly freezing the guest)
// but loses both the reply and every cleanup-discovery reply.
type droppedFreezeGuest struct {
	guestv1.GuestServer
}

func (g droppedFreezeGuest) Freeze(ctx context.Context, req *guestv1.FreezeRequest) (*guestv1.FreezeResponse, error) {
	if _, err := g.GuestServer.Freeze(ctx, req); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unavailable, "freeze reply lost")
}

// thawFailingGuest performs real Freeze but permanently rejects Thaw, so
// restart adoption cannot prove the recovered filesystem is writable.
type thawFailingGuest struct {
	guestv1.GuestServer
}

func (g thawFailingGuest) Thaw(context.Context, *guestv1.ThawRequest) (*guestv1.ThawResponse, error) {
	return nil, status.Error(codes.Internal, "thaw permanently stuck")
}

func snapshotResources(gib uint64) *nodev1.Resources {
	return &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: gib}
}

func saveSnapshot(ctx context.Context, m *Manager, sandboxID, snapshotID string) error {
	_, err := m.SaveSnapshot(ctx, &nodev1.SaveSnapshotRequest{SandboxId: sandboxID, SnapshotId: snapshotID})
	return err
}

func createChild(t *testing.T, m *Manager, id, snapshotID string, gib uint64) *nodev1.VM {
	t.Helper()
	v, err := m.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: id, FromSnapshot: snapshotID, Resources: snapshotResources(gib),
	})
	if err != nil {
		t.Fatalf("CreateVM from snapshot: %v", err)
	}
	return v
}

func execWorks(t *testing.T, h *harness, id string) {
	t.Helper()
	client := startNodeClient(t, h)
	res, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{SandboxId: id, Command: "printf runnable"})
	if err != nil || res.Stdout != "runnable" || res.Exit.GetExitCode() != 0 {
		t.Fatalf("VM is not runnable: %+v, %v", res, err)
	}
}

func infoHasSnapshot(t *testing.T, m *Manager, id string) bool {
	t.Helper()
	info, err := m.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, snap := range info.GetSnapshots() {
		if snap.GetSnapshotId() == id {
			return true
		}
	}
	return false
}

// assertSnapshotRuntimeFailed checks the durable FAILED state and that the
// live process, disk, CID, capacity and reservation were retained after an
// exhausted Resume/Thaw cleanup.
func assertSnapshotRuntimeFailed(t *testing.T, h *harness, id string, res *nodev1.Resources) {
	t.Helper()
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatalf("GetVM after failed recovery: %v", err)
	}
	if vm.GetState() != nodev1.VMState_VM_STATE_FAILED || !strings.Contains(vm.GetError(), "snapshot recovery failed") {
		t.Fatalf("recovered VM = %+v, want FAILED with recovery error", vm)
	}
	client := startNodeClient(t, h)
	if _, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{SandboxId: id, Command: "true"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Exec on failed-recovery VM = %v, want FailedPrecondition", err)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id, "disk.qcow2")); err != nil {
		t.Fatalf("failed-recovery VM lost its disk: %v", err)
	}
	h.mg.mu.Lock()
	v := h.mg.vms[id]
	reserved := h.mg.reserved
	h.mg.mu.Unlock()
	if v == nil {
		t.Fatal("failed-recovery VM record was removed")
	}
	v.mu.Lock()
	proc, cid := v.process, v.meta.VsockCID
	v.mu.Unlock()
	if proc == nil || cid == 0 {
		t.Fatalf("failed-recovery VM lost process/CID: proc=%v cid=%d", proc, cid)
	}
	if reserved.vcpus != res.GetVcpus() || reserved.memoryMib != res.GetMemoryMib() || reserved.diskGiB != res.GetDiskGib() {
		t.Fatalf("failed-recovery reservation = %+v, want %+v", reserved, res)
	}
}

func TestSnapshotLifecycleOpaqueIDAndIndependentChild(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	source := h.create("bx_22222222", snapshotResources(1))
	const snapshotID = "opaque /../ snapshot ID"
	snap, err := h.mg.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{SandboxId: source.GetSandboxId(), SnapshotId: snapshotID})
	if err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if snap.GetSnapshotId() != snapshotID || snap.GetSourceSandboxId() != source.GetSandboxId() || snap.GetImage() != testImageID || snap.GetCreatedAtUnixMs() == 0 {
		t.Fatalf("snapshot = %+v", snap)
	}
	if freezes, thaws, frozen := h.freezer.state(); frozen || freezes == 0 || thaws == 0 {
		t.Fatalf("successful save freezer state: freezes=%d thaws=%d frozen=%t", freezes, thaws, frozen)
	}
	if err := saveSnapshot(t.Context(), h.mg, source.GetSandboxId(), snapshotID); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate save = %v, want AlreadyExists", err)
	}
	if !infoHasSnapshot(t, h.mg, snapshotID) {
		t.Fatal("Info misses committed snapshot")
	}
	key := storage.SnapshotKey(snapshotID)
	if strings.Contains(key, "/") || key == snapshotID {
		t.Fatalf("unsafe snapshot key %q", key)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "snapshots", "committed", key, "snapshot.json")); err != nil {
		t.Fatalf("committed manifest: %v", err)
	}

	child := createChild(t, h.mg, "bx_33333333", snapshotID, 1)
	h.delete(source.GetSandboxId())
	if _, err := h.mg.DeleteSnapshot(t.Context(), &nodev1.DeleteSnapshotRequest{SnapshotId: snapshotID}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.mg.DeleteSnapshot(t.Context(), &nodev1.DeleteSnapshotRequest{SnapshotId: snapshotID}); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
	execWorks(t, h, child.GetSandboxId())
}

func TestSnapshotChildGrowAndNoShrink(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	source := h.create("bx_22222222", snapshotResources(2))
	if err := saveSnapshot(t.Context(), h.mg, source.GetSandboxId(), "grow-source"); err != nil {
		t.Fatal(err)
	}
	child := createChild(t, h.mg, "bx_33333333", "grow-source", 3)
	if child.GetResources().GetDiskGib() != 3 {
		t.Fatalf("child resources = %+v", child.GetResources())
	}
	fake := h.mg.store.Imager().(*fakeImager)
	fake.mu.Lock()
	calls := strings.Join(fake.calls, "\n")
	fake.mu.Unlock()
	if !strings.Contains(calls, "grow "+h.mg.store.DiskPath(child.GetSandboxId())) {
		t.Fatalf("snapshot child was not grown: %s", calls)
	}
	_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: "bx_44444444", FromSnapshot: "grow-source", Resources: snapshotResources(1),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("shrinking create = %v, want InvalidArgument", err)
	}
}

func TestSnapshotFreezePauseCopyFaultsRecoverVM(t *testing.T) {
	for _, tc := range []struct {
		name      string
		wrap      func(*snapshotGuest)
		configure func(*Config)
		pause     bool
	}{
		{"freeze", func(g *snapshotGuest) { g.freezeErrs = []error{errors.New("freeze fault")} }, nil, false},
		{"pause", nil, nil, true},
		{"copy", nil, func(cfg *Config) {
			cfg.snapshotOptions.Copy = func(context.Context, *os.File, *os.File) error { return errors.New("copy fault") }
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &snapshotGuest{}
			h := newHarness(t, harnessOptions{
				wrapGuest: func(base guestv1.GuestServer) guestv1.GuestServer {
					g.GuestServer = base
					if tc.wrap != nil {
						tc.wrap(g)
					}
					return g
				},
				configure: tc.configure,
			})
			id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
			if tc.pause {
				h.launcher.lastProcess().setPauseResumeErrs(errors.New("pause fault"), nil)
			}
			if err := saveSnapshot(t.Context(), h.mg, id, "fault"); err == nil {
				t.Fatal("faulted save succeeded")
			}
			if infoHasSnapshot(t, h.mg, "fault") {
				t.Fatal("faulted save published a snapshot")
			}
			_, _, paused := h.launcher.lastProcess().pauseResumeCounts()
			if paused {
				t.Fatal("failed save left vCPUs paused")
			}
			if _, _, frozen := h.freezer.state(); frozen {
				t.Fatal("failed save left the guest filesystem frozen")
			}
			execWorks(t, h, id)
		})
	}
}

// TestSnapshotPermanentResumeFailureMarksVMUnavailable: after bounded Resume
// cleanup is exhausted, readiness is unproven. The VM becomes FAILED while its
// process, disk, CID and reservation stay protected; Delete retries cleanly.
func TestSnapshotPermanentResumeFailureMarksVMUnavailable(t *testing.T) {
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.SnapshotCleanupTimeout = 200 * time.Millisecond
	}})
	res := snapshotResources(1)
	id := h.create("bx_22222222", res).GetSandboxId()
	proc := h.launcher.lastProcess()
	proc.setResumeAlways(errors.New("resume permanently stuck"))
	if err := saveSnapshot(t.Context(), h.mg, id, "resume-failed"); err == nil {
		t.Fatal("save succeeded with permanently failing Resume")
	}
	assertSnapshotRuntimeFailed(t, h, id, res)
	if got := h.launcher.stopCount(); got != 0 {
		t.Fatalf("failed recovery killed the live process: %d Stop calls", got)
	}
	h.delete(id)
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("delete retry after resume failure: %v", err)
	}
}

// TestRecoveredFrozenGuestThawedBeforeReady mimics a node crash mid-save: the
// real test process has QEMU's argv/QMP, and a surviving real guest service is
// frozen with a lease the new manager never saw. Adoption must discover and thaw
// that lease before READY, after which a write Exec proves usability.
func TestRecoveredFrozenGuestThawedBeforeReady(t *testing.T) {
	const id = "bx_22222222"
	const cid = 14
	freezer := &countingFreezer{}
	homeRoot, tmpRoot := t.TempDir(), t.TempDir()
	svc, err := guest.New(guest.Config{
		Version: "test", UID: os.Getuid(), GID: os.Getgid(), NoSetGroups: true,
		HomeRoot: homeRoot, TmpRoot: tmpRoot, Freezer: freezer,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Freeze(t.Context(), &guestv1.FreezeRequest{LeaseMs: 120_000}); err != nil {
		t.Fatalf("surviving Freeze: %v", err)
	}
	if _, _, frozen := freezer.state(); !frozen {
		t.Fatal("surviving guest was not frozen")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = svc.Shutdown(ctx)
	})

	var exe string
	h := newHarness(t, harnessOptions{
		fallback:  svc,
		stubGuest: true,
		prepare: func(t *testing.T, dataDir, imageDir string) {
			prepareVM(t, dataDir, imageDir, id, cid, 0)
			cfg := qemu.Config{
				SandboxID: id,
				WorkDir:   filepath.Join(dataDir, "vms", id),
				ImageDir:  filepath.Join(imageDir, testImageID),
				DiskPath:  filepath.Join(dataDir, "vms", id, "disk.qcow2"),
				VCPUs:     2,
				MemoryMiB: 2048,
				VsockCID:  cid,
			}
			helper, helperExe := spawnNodeHelper(t, id, cfg)
			exe = helperExe
			store, err := storage.Open(storage.Config{
				DataDir: dataDir, ImageDir: imageDir, Imager: &fakeImager{diskGiB: defaultTestBaseGiB},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.CommitRunning(id, helper.Process.Pid); err != nil {
				store.Close()
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		},
		configure: func(cfg *Config) { cfg.qemuExe = exe },
	})
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("recovered frozen VM = %+v, %v", vm, err)
	}
	if _, _, frozen := freezer.state(); frozen {
		t.Fatal("adopted READY VM still has a frozen guest filesystem")
	}
	client := startNodeClient(t, h)
	res, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{
		SandboxId: id, Command: `printf thawed > "$HOME/restart-proof" && cat "$HOME/restart-proof"`,
	})
	if err != nil || res.Stdout != "thawed" {
		t.Fatalf("write Exec after restart = %+v, %v", res, err)
	}
	data, err := os.ReadFile(filepath.Join(homeRoot, "restart-proof"))
	if err != nil || string(data) != "thawed" {
		t.Fatalf("restart write proof = %q, %v", data, err)
	}
}

// TestRecoveredFrozenGuestThawFailureNeverReady: Ping succeeds while frozen,
// but a surviving lease that cannot be thawed must keep the VM FAILED and
// unavailable, with the disk protected for a clean Delete retry.
func TestRecoveredFrozenGuestThawFailureNeverReady(t *testing.T) {
	const id = "bx_22222223"
	const cid = 15
	freezer := &countingFreezer{}
	svc, err := guest.New(guest.Config{
		Version: "test", UID: os.Getuid(), GID: os.Getgid(), NoSetGroups: true,
		HomeRoot: t.TempDir(), TmpRoot: t.TempDir(), Freezer: freezer,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Freeze(t.Context(), &guestv1.FreezeRequest{LeaseMs: 120_000}); err != nil {
		t.Fatalf("surviving Freeze: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = svc.Shutdown(ctx)
	})

	var exe string
	h := newHarness(t, harnessOptions{
		fallback:  thawFailingGuest{GuestServer: svc},
		stubGuest: true,
		prepare: func(t *testing.T, dataDir, imageDir string) {
			prepareVM(t, dataDir, imageDir, id, cid, 0)
			cfg := qemu.Config{
				SandboxID: id,
				WorkDir:   filepath.Join(dataDir, "vms", id),
				ImageDir:  filepath.Join(imageDir, testImageID),
				DiskPath:  filepath.Join(dataDir, "vms", id, "disk.qcow2"),
				VCPUs:     2,
				MemoryMiB: 2048,
				VsockCID:  cid,
			}
			helper, helperExe := spawnNodeHelper(t, id, cfg)
			exe = helperExe
			store, err := storage.Open(storage.Config{
				DataDir: dataDir, ImageDir: imageDir, Imager: &fakeImager{diskGiB: defaultTestBaseGiB},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.CommitRunning(id, helper.Process.Pid); err != nil {
				store.Close()
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		},
		configure: func(cfg *Config) {
			cfg.qemuExe = exe
			cfg.SnapshotCleanupTimeout = 200 * time.Millisecond
		},
	})
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatalf("GetVM after failed thaw recovery: %v", err)
	}
	if vm.GetState() != nodev1.VMState_VM_STATE_FAILED || !strings.Contains(vm.GetError(), "thaw recovered guest") {
		t.Fatalf("recovered VM = %+v, want FAILED thaw error", vm)
	}
	client := startNodeClient(t, h)
	if _, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{SandboxId: id, Command: "true"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Exec on failed thaw recovery = %v, want FailedPrecondition", err)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id, "disk.qcow2")); err != nil {
		t.Fatalf("failed thaw recovery lost the disk: %v", err)
	}
	h.delete(id)
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("delete retry after failed thaw recovery: %v", err)
	}
}

// TestSnapshotRuntimeFailurePersistsUnderVMLock stages the snapshot failure
// writer holding v.mu across SetVMError while the death watcher and GetVM wait
// on the same lock. Both must serialize, not deadlock or observe READY.
func TestSnapshotRuntimeFailurePersistsUnderVMLock(t *testing.T) {
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.SnapshotCleanupTimeout = 100 * time.Millisecond
	}})
	id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	proc := h.launcher.lastProcess()
	persistEntered := make(chan struct{})
	persistRelease := make(chan struct{})
	var once sync.Once
	var releaseOnce sync.Once
	releasePersist := func() { releaseOnce.Do(func() { close(persistRelease) }) }
	defer releasePersist()
	h.mg.beforePersist = func() {
		once.Do(func() { close(persistEntered) })
		<-persistRelease
	}
	proc.setResumeAlways(errors.New("resume stuck"))
	saveDone := make(chan error, 1)
	go func() { saveDone <- saveSnapshot(context.Background(), h.mg, id, "writer-stage") }()
	select {
	case <-persistEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot failure never reached staged persistence")
	}
	proc.exit(errors.New("process died during staged persistence"))
	getDone := make(chan *nodev1.VM, 1)
	go func() {
		vm, _ := h.mg.GetVM(context.Background(), &nodev1.GetVMRequest{SandboxId: id})
		getDone <- vm
	}()
	select {
	case <-getDone:
		t.Fatal("GetVM escaped v.mu while failure persistence was staged")
	case <-time.After(100 * time.Millisecond):
	}
	releasePersist()
	select {
	case err := <-saveDone:
		if err == nil {
			t.Fatal("save with permanently failing Resume succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("staged snapshot failure did not return")
	}
	select {
	case vm := <-getDone:
		if vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
			t.Fatalf("staged concurrent writer left state %s", vm.GetState())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("GetVM did not return after staged persistence was released")
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id, "disk.qcow2")); err != nil {
		t.Fatalf("staged failure lost the disk: %v", err)
	}
}

// TestSnapshotLostFreezeRepliesMarkVMUnavailable: a real Freeze can take effect
// even when both its reply and cleanup's discovery reply are lost. Without a
// proven Thaw the VM must become FAILED rather than advertise READY.
func TestSnapshotLostFreezeRepliesMarkVMUnavailable(t *testing.T) {
	h := newHarness(t, harnessOptions{
		wrapGuest: func(base guestv1.GuestServer) guestv1.GuestServer {
			return droppedFreezeGuest{GuestServer: base}
		},
		configure: func(cfg *Config) {
			cfg.SnapshotCleanupTimeout = 200 * time.Millisecond
		},
	})
	res := snapshotResources(1)
	id := h.create("bx_22222222", res).GetSandboxId()
	if err := saveSnapshot(t.Context(), h.mg, id, "lost-freeze"); err == nil {
		t.Fatal("save succeeded when every Freeze reply was lost")
	}
	if _, _, frozen := h.freezer.state(); !frozen {
		t.Fatal("test did not leave the real guest freezer frozen")
	}
	assertSnapshotRuntimeFailed(t, h, id, res)
	if got := h.launcher.stopCount(); got != 0 {
		t.Fatalf("failed recovery killed the live process: %d Stop calls", got)
	}
	h.delete(id)
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("delete retry after lost Freeze replies: %v", err)
	}
}

// TestSnapshotPermanentThawFailureMarksVMUnavailable: Resume succeeds but the
// guest filesystem stays frozen. The VM becomes FAILED without killing the
// process or losing its disk/reservation; Delete retries cleanly.
func TestSnapshotPermanentThawFailureMarksVMUnavailable(t *testing.T) {
	g := &snapshotGuest{thawAlways: errors.New("thaw permanently stuck")}
	h := newHarness(t, harnessOptions{
		wrapGuest: func(base guestv1.GuestServer) guestv1.GuestServer {
			g.GuestServer = base
			return g
		},
		configure: func(cfg *Config) {
			cfg.SnapshotCleanupTimeout = 200 * time.Millisecond
		},
	})
	res := snapshotResources(1)
	id := h.create("bx_22222222", res).GetSandboxId()
	if err := saveSnapshot(t.Context(), h.mg, id, "thaw-failed"); err == nil {
		t.Fatal("save succeeded with permanently failing Thaw")
	}
	if _, _, paused := h.launcher.lastProcess().pauseResumeCounts(); paused {
		t.Fatal("resume was proven but the failed save left vCPUs paused")
	}
	assertSnapshotRuntimeFailed(t, h, id, res)
	if got := h.launcher.stopCount(); got != 0 {
		t.Fatalf("failed recovery killed the live process: %d Stop calls", got)
	}
	h.delete(id)
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("delete retry after thaw failure: %v", err)
	}
}

func TestSnapshotResumeAndThawRetryThenPublish(t *testing.T) {
	g := &snapshotGuest{thawErrs: []error{errors.New("transient thaw")}}
	h := newHarness(t, harnessOptions{wrapGuest: func(base guestv1.GuestServer) guestv1.GuestServer {
		g.GuestServer = base
		return g
	}})
	id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	h.launcher.lastProcess().setPauseResumeErrs(nil, errors.New("transient resume"))
	if err := saveSnapshot(t.Context(), h.mg, id, "retry"); err != nil {
		t.Fatalf("transient cleanup faults were not recovered: %v", err)
	}
	_, resumes, paused := h.launcher.lastProcess().pauseResumeCounts()
	if resumes < 2 || paused {
		t.Fatalf("resumes=%d paused=%t", resumes, paused)
	}
	if !infoHasSnapshot(t, h.mg, "retry") {
		t.Fatal("recovered save did not publish")
	}
	if _, _, frozen := h.freezer.state(); frozen {
		t.Fatal("recovered save left the guest filesystem frozen")
	}
}

func testExecutable(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		t.Fatal(err)
	}
	return exe
}

// TestSnapshotRescueInstallsPersistentController uses a genuine QEMU-shaped
// subprocess with the exact executable/argv/QMP socket. When its original QMP
// controller is dead, Resume must prove the whole process identity, resume it,
// and keep that verified connection for the next save.
func TestSnapshotRescueInstallsPersistentController(t *testing.T) {
	launcher := &fakeLauncher{newProcess: func(cfg qemu.Config) (*fakeProcess, error) {
		return startRescueHelper(t, cfg)
	}}
	h := newHarness(t, harnessOptions{launcher: launcher, qemuExe: testExecutable(t)})
	id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	proc := h.launcher.lastProcess()
	proc.setPauseResumeErrs(nil, errors.New("original QMP controller is dead"))
	if err := saveSnapshot(t.Context(), h.mg, id, "rescue-one"); err != nil {
		t.Fatalf("save with rescue: %v", err)
	}
	// The first save used the fake controller's Pause, failed its Resume and
	// rescued through the real QMP socket. The second save must use the
	// installed connection, not the dead original controller or another rescue.
	proc.setPauseResumeErrs(nil, errors.New("original QMP controller is still dead"))
	if err := saveSnapshot(t.Context(), h.mg, id, "rescue-two"); err != nil {
		t.Fatalf("save after rescue: %v", err)
	}
	if pauses, _, _ := proc.pauseResumeCounts(); pauses != 1 {
		t.Fatalf("later save used the dead original controller: pauses=%d", pauses)
	}
	execWorks(t, h, id)
}

// TestSnapshotRescueRejectsUnrelatedSameBirthProcess: the recorded pid/start
// ticks match, and a plausible QMP socket exists, but the executable and argv
// are not this VM's QEMU. No cont may be sent and no snapshot may publish.
func TestSnapshotRescueRejectsUnrelatedSameBirthProcess(t *testing.T) {
	launcher := &fakeLauncher{newProcess: func(cfg qemu.Config) (*fakeProcess, error) {
		return startUnrelatedHelper(t, cfg)
	}}
	h := newHarness(t, harnessOptions{launcher: launcher, qemuExe: testExecutable(t), configure: func(cfg *Config) {
		cfg.SnapshotCleanupTimeout = 200 * time.Millisecond
	}})
	id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	proc := h.launcher.lastProcess()
	proc.setResumeAlways(errors.New("original QMP controller is dead"))
	if err := saveSnapshot(t.Context(), h.mg, id, "unrelated"); err == nil {
		t.Fatal("save accepted an unrelated same-birth process")
	}
	if infoHasSnapshot(t, h.mg, "unrelated") {
		t.Fatal("unrelated process rescued a published snapshot")
	}
	if _, _, paused := proc.pauseResumeCounts(); !paused {
		t.Fatal("unproven process was resumed")
	}
	// Independent Resume was exhausted, so readiness is unproven: the VM
	// must be unavailable but keep its process, disk, CID and reservation.
	assertSnapshotRuntimeFailed(t, h, id, snapshotResources(1))
	if got := h.launcher.stopCount(); got != 0 {
		t.Fatalf("unverified owner was killed during recovery: %d Stop calls", got)
	}
	h.delete(id)
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("delete retry after failed recovery: %v", err)
	}
}

func TestSnapshotChangedLeaseRejectedAndReplacementThawed(t *testing.T) {
	g := &snapshotGuest{replaceOnVerify: true}
	h := newHarness(t, harnessOptions{wrapGuest: func(base guestv1.GuestServer) guestv1.GuestServer {
		g.GuestServer = base
		return g
	}})
	id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	if err := saveSnapshot(t.Context(), h.mg, id, "changed-lease"); err == nil {
		t.Fatal("save accepted a replacement lease")
	}
	if infoHasSnapshot(t, h.mg, "changed-lease") {
		t.Fatal("replacement-lease snapshot published")
	}
	execWorks(t, h, id)
}

func TestSnapshotPublishFaultsAreTruthful(t *testing.T) {
	for _, tc := range []struct {
		name        string
		afterRename bool
		published   bool
	}{
		{"rename refused", false, false},
		{"error after rename", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
				cfg.snapshotOptions.Rename = func(root *os.Root, old, new string) error {
					if !strings.HasPrefix(new, "snapshots/committed/") {
						return root.Rename(old, new)
					}
					if !tc.afterRename {
						return errors.New("injected rename refusal")
					}
					return errors.Join(root.Rename(old, new), errors.New("rename result lost"))
				}
			}})
			id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
			if err := saveSnapshot(t.Context(), h.mg, id, "publish"); err == nil {
				t.Fatal("publish fault succeeded")
			}
			if got := infoHasSnapshot(t, h.mg, "publish"); got != tc.published {
				t.Fatalf("Info published=%t, want %t after rename fault", got, tc.published)
			}
			execWorks(t, h, id)
		})
	}
}

func TestSnapshotCopyDeadlineCancelsAndCleansWithoutGateRelease(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCopy := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseCopy()
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.SnapshotLease = 100 * time.Millisecond
		cfg.SnapshotCleanupTimeout = time.Second
		cfg.snapshotOptions.Copy = func(ctx context.Context, _, _ *os.File) error {
			close(entered)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return errors.New("test cleanup released the copy")
			}
		}
	}})
	id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	saveDone := make(chan error, 1)
	go func() { saveDone <- saveSnapshot(context.Background(), h.mg, id, "expired") }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("copy fault block was never entered")
	}
	var err error
	select {
	case err = <-saveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("deadline save did not return promptly; test cleanup will unblock the copy")
	}
	releaseCopy()
	if !errors.Is(err, context.DeadlineExceeded) && status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("save error = %v, want DeadlineExceeded", err)
	}
	_, _, paused := h.launcher.lastProcess().pauseResumeCounts()
	if paused {
		t.Fatal("deadline left VM paused")
	}
	if _, _, frozen := h.freezer.state(); frozen {
		t.Fatal("deadline left the guest filesystem frozen")
	}
	if infoHasSnapshot(t, h.mg, "expired") {
		t.Fatal("expired save published")
	}
	execWorks(t, h, id)
}

// TestSnapshotPublishUsesVerificationShrunkDeadline: the verification Freeze
// may shrink the host deadline after the copy lease context was created.
// Publication must use the latest deadline and never report success when the
// manifest fsync crosses it before the rename.
func TestSnapshotPublishUsesVerificationShrunkDeadline(t *testing.T) {
	// The verification Freeze shrinks the host deadline to 1s. The manifest
	// temp-file sync then blocks past that real deadline, so Publish must see
	// its rebound context expire before the rename. The old copy lease was 1h
	// and would have allowed both the sync and the rename.
	g := &snapshotGuest{freezeRemaining: []uint64{3_600_000, 1_000}}
	var syncHit atomic.Bool
	h := newHarness(t, harnessOptions{
		wrapGuest: func(base guestv1.GuestServer) guestv1.GuestServer {
			g.GuestServer = base
			return g
		},
		configure: func(cfg *Config) {
			cfg.SnapshotLease = time.Hour
			cfg.snapshotOptions.SyncFile = func(f *os.File) error {
				if strings.HasSuffix(f.Name(), "snapshot.json.tmp") {
					syncHit.Store(true)
					time.Sleep(1500 * time.Millisecond)
				}
				return f.Sync()
			}
		},
	})
	id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	err := saveSnapshot(t.Context(), h.mg, id, "shrunk-deadline")
	if err == nil {
		t.Fatal("save succeeded after the verification-shrunk deadline crossed publication")
	}
	if !syncHit.Load() {
		t.Fatal("manifest temp-file sync callback was never entered")
	}
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("save error = %v, want DeadlineExceeded from the sync crossing", err)
	}
	if infoHasSnapshot(t, h.mg, "shrunk-deadline") {
		t.Fatal("expired publication renamed the staged snapshot")
	}
	if _, _, frozen := h.freezer.state(); frozen {
		t.Fatal("expired publication left the guest frozen")
	}
	execWorks(t, h, id)
}

// TestSnapshotPublishCrossAfterRenameNeverSucceeds: a deadline that crosses
// after the durable rename is still an error, but the committed snapshot stays
// truthfully visible instead of being deleted.
func TestSnapshotPublishCrossAfterRenameNeverSucceeds(t *testing.T) {
	base := time.Now()
	var nowMu sync.Mutex
	now := base
	g := &snapshotGuest{freezeRemaining: []uint64{3_600_000, 30_000}}
	h := newHarness(t, harnessOptions{
		wrapGuest: func(base guestv1.GuestServer) guestv1.GuestServer {
			g.GuestServer = base
			return g
		},
		configure: func(cfg *Config) {
			cfg.SnapshotLease = time.Hour
			cfg.snapshotNow = func() time.Time {
				nowMu.Lock()
				defer nowMu.Unlock()
				return now
			}
			cfg.snapshotOptions.SyncDir = func(root *os.Root, name string) error {
				err := syncTestDir(root, name)
				if name == "snapshots/committed" {
					nowMu.Lock()
					now = base.Add(time.Minute)
					nowMu.Unlock()
				}
				return err
			}
		},
	})
	id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	if err := saveSnapshot(t.Context(), h.mg, id, "crossed-after-rename"); err == nil {
		t.Fatal("save reported success after crossing the deadline during publication")
	}
	if !infoHasSnapshot(t, h.mg, "crossed-after-rename") {
		t.Fatal("committed snapshot was not truthfully visible after the rename")
	}
	if _, _, frozen := h.freezer.state(); frozen {
		t.Fatal("crossed publication left the guest frozen")
	}
	execWorks(t, h, id)
}

func syncTestDir(root *os.Root, name string) error {
	dir, err := root.Open(name)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	return errors.Join(syncErr, closeErr)
}

func TestSnapshotShutdownCancelsBackgroundCopyAndReleasesStore(t *testing.T) {
	entered := make(chan struct{})
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.SnapshotLease = time.Hour
		cfg.SnapshotCleanupTimeout = time.Second
		cfg.snapshotOptions.Copy = func(ctx context.Context, _, _ *os.File) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}
	}})
	id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	saveDone := make(chan error, 1)
	go func() { saveDone <- saveSnapshot(context.Background(), h.mg, id, "shutdown") }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("save did not reach copy")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.mg.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown with blocked save: %v", err)
	}
	select {
	case err := <-saveDone:
		if err == nil {
			t.Fatal("shutdown-canceled save succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown-canceled save did not return")
	}
	if _, _, frozen := h.freezer.state(); frozen {
		t.Fatal("shutdown-canceled save left the guest filesystem frozen")
	}
	s, err := storage.Open(storage.Config{DataDir: h.dataDir, ImageDir: h.imageDir, Imager: &fakeImager{diskGiB: 1}})
	if err != nil {
		t.Fatalf("store lock was not released: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func copyTestFile(ctx context.Context, source, destination *os.File) error {
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return err
	}
	data, err := io.ReadAll(source)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err = destination.Write(data)
	return err
}

func blockingTestCopy(entered chan struct{}, release chan struct{}, block *atomic.Bool) func(context.Context, *os.File, *os.File) error {
	var once sync.Once
	return func(ctx context.Context, source, destination *os.File) error {
		if block.Load() {
			once.Do(func() { close(entered) })
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return copyTestFile(ctx, source, destination)
	}
}

func TestSnapshotExistingCreateJoinsBeforeIrrelevantPin(t *testing.T) {
	var block atomic.Bool
	block.Store(true)
	entered, release := make(chan struct{}), make(chan struct{})
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.snapshotOptions.Copy = blockingTestCopy(entered, release, &block)
	}})
	id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	saveDone := make(chan error, 1)
	go func() { saveDone <- saveSnapshot(context.Background(), h.mg, id, "irrelevant") }()
	<-entered

	// A nil-resource retry with an in-flight, otherwise-invalid snapshot must
	// join the existing VM immediately rather than wait for its pin.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	v, err := h.mg.CreateVM(ctx, &nodev1.CreateVMRequest{SandboxId: id, FromSnapshot: "irrelevant"})
	if err != nil || v.GetSandboxId() != id {
		t.Fatalf("existing create = %+v, %v", v, err)
	}
	close(release)
	if err := <-saveDone; err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotDeleteFencesCreateBlockedOnPin(t *testing.T) {
	var block atomic.Bool
	block.Store(true)
	entered, release := make(chan struct{}), make(chan struct{})
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.snapshotOptions.Copy = blockingTestCopy(entered, release, &block)
	}})
	source := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	const child = "bx_33333333"
	saveDone := make(chan error, 1)
	go func() { saveDone <- saveSnapshot(context.Background(), h.mg, source, "pinned") }()
	<-entered
	createDone := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(context.Background(), &nodev1.CreateVMRequest{
			SandboxId: child, FromSnapshot: "pinned", Resources: snapshotResources(1),
		})
		createDone <- err
	}()
	waitFor(t, "create attempt registration", func() bool {
		h.mg.mu.Lock()
		defer h.mg.mu.Unlock()
		return h.mg.creating[child] == 1
	})
	deleteDone := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(context.Background(), &nodev1.DeleteVMRequest{SandboxId: child})
		deleteDone <- err
	}()
	waitFor(t, "delete fence before a VM exists", func() bool {
		h.mg.mu.Lock()
		defer h.mg.mu.Unlock()
		_, ok := h.mg.fence[child]
		return ok
	})
	close(release)
	if err := <-saveDone; err != nil {
		t.Fatal(err)
	}
	if err := <-createDone; status.Code(err) != codes.Aborted {
		t.Fatalf("fenced create = %v, want Aborted", err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatalf("DeleteVM of fenced create: %v", err)
	}
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: child}); status.Code(err) != codes.NotFound {
		t.Fatalf("fenced child exists: %v", err)
	}
}

func TestSnapshotDeleteWaitsForPrivateChildCopy(t *testing.T) {
	var block atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.snapshotOptions.Copy = blockingTestCopy(entered, release, &block)
	}})
	source := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	if err := saveSnapshot(t.Context(), h.mg, source, "child-pin"); err != nil {
		t.Fatal(err)
	}
	block.Store(true)
	const child = "bx_33333333"
	createDone := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(context.Background(), &nodev1.CreateVMRequest{
			SandboxId: child, FromSnapshot: "child-pin", Resources: snapshotResources(1),
		})
		createDone <- err
	}()
	<-entered
	deleteDone := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteSnapshot(context.Background(), &nodev1.DeleteSnapshotRequest{SnapshotId: "child-pin"})
		deleteDone <- err
	}()
	select {
	case err := <-deleteDone:
		t.Fatalf("DeleteSnapshot escaped the child pin: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-createDone; err != nil {
		t.Fatalf("child create: %v", err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatal(err)
	}
	execWorks(t, h, child)
}

// TestSnapshotQueuedDuplicateDeletesJoinFailedOwner stages both deletes behind
// a Save that holds the snapshot gate. Ownership must be published before the
// gate wait, so when the first owner later fails, the queued duplicate returns
// the same failure instead of silently becoming a second cleanup owner.
func TestSnapshotQueuedDuplicateDeletesJoinFailedOwner(t *testing.T) {
	var block atomic.Bool
	block.Store(true)
	entered, release := make(chan struct{}), make(chan struct{})
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.snapshotOptions.Copy = blockingTestCopy(entered, release, &block)
	}})
	id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	proc := h.launcher.lastProcess()
	proc.setStopErr(errors.New("stop fault"))
	saveDone := make(chan error, 1)
	go func() { saveDone <- saveSnapshot(context.Background(), h.mg, id, "queued-delete") }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("save never entered the blocking copy")
	}

	first := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(context.Background(), &nodev1.DeleteVMRequest{SandboxId: id})
		first <- err
	}()
	waitFor(t, "first delete attempt published before gate wait", func() bool {
		v, err := h.mg.lookup(id)
		if err != nil {
			return false
		}
		v.mu.Lock()
		defer v.mu.Unlock()
		return v.deleting && v.del != nil
	})
	second := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(context.Background(), &nodev1.DeleteVMRequest{SandboxId: id})
		second <- err
	}()
	waitFor(t, "duplicate joined the pre-gate attempt", func() bool {
		v, err := h.mg.lookup(id)
		if err != nil {
			return false
		}
		v.mu.Lock()
		defer v.mu.Unlock()
		return v.deleteWaiters == 1
	})
	select {
	case err := <-first:
		t.Fatalf("owner escaped the Save gate: %v", err)
	default:
	}

	close(release)
	if err := <-saveDone; err != nil {
		t.Fatalf("save holding the gate: %v", err)
	}
	firstErr, secondErr := <-first, <-second
	if status.Code(firstErr) != codes.Internal || status.Code(secondErr) != codes.Internal || firstErr.Error() != secondErr.Error() {
		t.Fatalf("queued deletes did not share the owner result: first=%v second=%v", firstErr, secondErr)
	}
	if got := h.launcher.stopCount(); got != 1 {
		t.Fatalf("queued duplicate retried cleanup: %d Stop calls", got)
	}
	proc.setStopErr(nil)
	h.delete(id)
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("explicit retry failed: %v", err)
	}
}

// TestSnapshotCanceledDeleteFenceCannotClearRetry stages a gate-canceled owner
// after finalization but before its deferred fence cleanup. A retry published
// in that window owns a new fence; the old completion must not delete it, and
// creates stay fenced until the retry finishes.
func TestSnapshotCanceledDeleteFenceCannotClearRetry(t *testing.T) {
	var block atomic.Bool
	block.Store(true)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseCopy := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseCopy()
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.snapshotOptions.Copy = blockingTestCopy(entered, release, &block)
	}})
	id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	saveDone := make(chan error, 1)
	go func() { saveDone <- saveSnapshot(context.Background(), h.mg, id, "fence-retry") }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("save never entered the blocking copy")
	}

	hookEntered := make(chan struct{})
	hookRelease := make(chan struct{})
	var hookOnce sync.Once
	releaseHook := func() { hookOnce.Do(func() { close(hookRelease) }) }
	defer releaseHook()
	h.mg.afterCanceledDeleteAttempt = func() {
		close(hookEntered)
		<-hookRelease
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: id})
		first <- err
	}()
	var firstAttempt *deleteAttempt
	var oldToken *fenceToken
	waitFor(t, "first delete attempt published", func() bool {
		// Lock order is Manager.mu then vm.mu, matching DeleteVM.
		h.mg.mu.Lock()
		v := h.mg.vms[id]
		token := h.mg.fence[id]
		h.mg.mu.Unlock()
		if v == nil {
			return false
		}
		v.mu.Lock()
		defer v.mu.Unlock()
		if !v.deleting || v.del == nil {
			return false
		}
		firstAttempt = v.del
		oldToken = token
		return true
	})
	cancel()
	select {
	case <-hookEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled owner never reached finalization window")
	}

	retry := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(context.Background(), &nodev1.DeleteVMRequest{SandboxId: id})
		retry <- err
	}()
	var retryToken *fenceToken
	waitFor(t, "retry published a new attempt and fence", func() bool {
		h.mg.mu.Lock()
		v := h.mg.vms[id]
		token := h.mg.fence[id]
		h.mg.mu.Unlock()
		if v == nil {
			return false
		}
		v.mu.Lock()
		defer v.mu.Unlock()
		if !v.deleting || v.del == nil || v.del == firstAttempt {
			return false
		}
		retryToken = token
		return token != nil && token != oldToken
	})
	releaseHook()
	var firstErr error
	select {
	case firstErr = <-first:
	case <-time.After(10 * time.Second):
		t.Fatal("canceled owner did not return after hook release")
	}
	if status.Code(firstErr) != codes.Canceled {
		t.Fatalf("canceled owner = %v, want Canceled", firstErr)
	}
	h.mg.mu.Lock()
	got := h.mg.fence[id]
	h.mg.mu.Unlock()
	if got != retryToken {
		t.Fatal("old canceled attempt's fence cleanup removed the retry fence")
	}

	createCtx, createCancel := context.WithTimeout(context.Background(), time.Second)
	defer createCancel()
	createDone := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(createCtx, &nodev1.CreateVMRequest{SandboxId: id, Resources: snapshotResources(1)})
		createDone <- err
	}()
	if err := <-createDone; status.Code(err) != codes.Aborted {
		t.Fatalf("create during retry fence = %v, want Aborted", err)
	}

	releaseCopy()
	select {
	case err := <-saveDone:
		if err != nil {
			t.Fatalf("save holding the gate: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("save holding the gate did not finish")
	}
	select {
	case err := <-retry:
		if err != nil {
			t.Fatalf("retry delete: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("retry delete did not finish")
	}
	h.mg.mu.Lock()
	_, fenced := h.mg.fence[id]
	h.mg.mu.Unlock()
	if fenced {
		t.Fatal("retry fence not cleared after success")
	}
	if vm := h.create(id, nil); vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("post-retry create = %v", vm)
	}
}

func TestSnapshotConcurrentFailedDeletesJoinOriginal(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := h.create("bx_22222222", snapshotResources(1)).GetSandboxId()
	proc := h.launcher.lastProcess()
	gate, entered := make(chan struct{}), make(chan struct{}, 1)
	proc.stageStop(gate, entered)
	proc.setStopErr(errors.New("stop fault"))
	firstDone := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(context.Background(), &nodev1.DeleteVMRequest{SandboxId: id})
		firstDone <- err
	}()
	<-entered
	secondDone := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(context.Background(), &nodev1.DeleteVMRequest{SandboxId: id})
		secondDone <- err
	}()
	waitFor(t, "duplicate delete join", func() bool {
		v, _ := h.mg.lookup(id)
		v.mu.Lock()
		defer v.mu.Unlock()
		return v.deleteWaiters == 1
	})
	close(gate)
	firstErr, secondErr := <-firstDone, <-secondDone
	if firstErr == nil || secondErr == nil || firstErr.Error() != secondErr.Error() {
		t.Fatalf("duplicate results differ: first=%v second=%v", firstErr, secondErr)
	}
	if got := h.launcher.stopCount(); got != 1 {
		t.Fatalf("concurrent duplicate retried Stop: %d calls", got)
	}
	proc.setStopErr(nil)
	if _, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("later explicit retry: %v", err)
	}
}

// TestSnapshotManagerRestartRecoversCommittedAndCleansStaging proves a new
// manager validates committed snapshots from disk, exposes them in Info and
// removes only owned incomplete staging after a restart.
func TestSnapshotManagerRestartRecoversCommittedAndCleansStaging(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	source := h.create("bx_22222222", snapshotResources(1))
	if err := saveSnapshot(t.Context(), h.mg, source.GetSandboxId(), "restart-committed"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.mg.store.BeginSnapshot("restart-incomplete"); err != nil {
		t.Fatal(err)
	}
	h.delete(source.GetSandboxId())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := h.mg.Shutdown(shutdownCtx); err != nil {
		cancel()
		t.Fatalf("first shutdown: %v", err)
	}
	cancel()

	dialer := newFakeGuestDialer(t)
	restarted, err := New(t.Context(), Config{
		NodeID:       "node-test",
		DataDir:      h.dataDir,
		ImageDir:     h.imageDir,
		DefaultImage: testImageID,
		Capacity:     &nodev1.Resources{Vcpus: 64, MemoryMib: 256 << 10, DiskGib: 1024},
		BootTimeout:  time.Second,
		Launcher:     &fakeLauncher{},
		GuestDialer:  dialer,
		Imager:       &fakeImager{diskGiB: defaultTestBaseGiB, basePath: filepath.Join(h.imageDir, testImageID, "root.qcow2")},
		// Test-factory construction: the dev/test user-mode network.
		AllowUserNetworking: true,
		diskFree:            func() (uint64, error) { return 1 << 40, nil },
	})
	if err != nil {
		t.Fatalf("restart manager: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := restarted.Shutdown(ctx); err != nil {
			t.Errorf("restarted shutdown: %v", err)
		}
		dialer.stop(t)
	})
	if !infoHasSnapshot(t, restarted, "restart-committed") {
		t.Fatal("restart did not recover the committed snapshot")
	}
	if _, err := restarted.store.ReadSnapshot(t.Context(), "restart-committed", restarted.images); err != nil {
		t.Fatalf("recovered snapshot invalid: %v", err)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "snapshots", "staging", storage.SnapshotKey("restart-incomplete"))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("incomplete staging survived restart: %v", err)
	}
}

// TestSnapshotUnknownDeleteRetiresStaleFence initializes the state left by a
// timed-out v==nil/create-wait protection: creating has drained but the fence
// remains. A successful unknown-id retry must clear it, and a same-id create
// must then boot instead of staying ABORTED forever.
// TestSnapshotDeleteFsyncAfterRemoveRetiresMetadata: once RemoveAll proves the
// committed path absent, a later fsync error is returned but Info, create-from
// and retries must all treat the snapshot as gone.
func TestSnapshotDeleteFsyncAfterRemoveRetiresMetadata(t *testing.T) {
	var failSync atomic.Bool
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) {
		cfg.snapshotOptions.SyncDir = func(root *os.Root, name string) error {
			err := syncTestDir(root, name)
			if failSync.Load() && name == "snapshots/committed" {
				return errors.New("fsync after remove")
			}
			return err
		}
	}})
	source := h.create("bx_22222222", snapshotResources(1))
	const snapshotID = "delete-fsync"
	if err := saveSnapshot(t.Context(), h.mg, source.GetSandboxId(), snapshotID); err != nil {
		t.Fatal(err)
	}
	failSync.Store(true)
	if _, err := h.mg.DeleteSnapshot(t.Context(), &nodev1.DeleteSnapshotRequest{SnapshotId: snapshotID}); err == nil {
		t.Fatal("delete with post-remove fsync error succeeded silently")
	}
	if infoHasSnapshot(t, h.mg, snapshotID) {
		t.Fatal("Info still advertises a proven-absent snapshot")
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "snapshots", "committed", storage.SnapshotKey(snapshotID))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("committed path still present after RemoveAll: %v", err)
	}
	_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: "bx_33333333", FromSnapshot: snapshotID, Resources: snapshotResources(1),
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("create from proven-absent snapshot = %v, want NotFound", err)
	}
	failSync.Store(false)
	if _, err := h.mg.DeleteSnapshot(t.Context(), &nodev1.DeleteSnapshotRequest{SnapshotId: snapshotID}); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
}

func TestSnapshotUnknownDeleteRetiresStaleFence(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	const id = "bx_22222222"
	h.mg.mu.Lock()
	h.mg.creating[id] = 1
	h.mg.fence[id] = &fenceToken{}
	h.mg.mu.Unlock()
	// The earlier protected create drains without clearing its expired wait's
	// fence, exactly like the timeout path.
	h.mg.mu.Lock()
	delete(h.mg.creating, id)
	h.mg.mu.Unlock()

	if _, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("unknown-id delete retry: %v", err)
	}
	h.mg.mu.Lock()
	_, fenced := h.mg.fence[id]
	h.mg.mu.Unlock()
	if fenced {
		t.Fatal("successful unknown-id retry left the stale fence")
	}
	if vm := h.create(id, nil); vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("create after stale-fence retirement = %v", vm)
	}
}

// TestDeleteFencesBootDuringDiskCreate blocks inside the real create-disk
// phase, then cancels it with DeleteVM. qemu-img reports a signal death, not
// ctx.Err; every pre-delete waiter must still see ABORTED, cleanup must finish
// before Delete returns, and no record/disk/reservation may survive.
func TestDeleteFencesBootDuringDiskCreate(t *testing.T) {
	entered := make(chan struct{})
	imager := &fakeImager{diskGiB: defaultTestBaseGiB}
	imager.createHook = func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return errors.New("qemu-img create: signal: killed")
	}
	h := newHarness(t, harnessOptions{configure: func(cfg *Config) { cfg.Imager = imager }})
	const id = "bx_22222222"
	res := snapshotResources(defaultTestBaseGiB)
	first := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(context.Background(), &nodev1.CreateVMRequest{SandboxId: id, Resources: res})
		first <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("boot never reached disk creation")
	}
	second := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(context.Background(), &nodev1.CreateVMRequest{SandboxId: id, Resources: res})
		second <- err
	}()
	waitFor(t, "both create waiters registered", func() bool {
		h.mg.mu.Lock()
		v := h.mg.vms[id]
		h.mg.mu.Unlock()
		if v == nil {
			return false
		}
		v.mu.Lock()
		defer v.mu.Unlock()
		return v.waiters == 2
	})
	deleteDone := make(chan error, 1)
	go func() {
		_, err := h.mg.DeleteVM(context.Background(), &nodev1.DeleteVMRequest{SandboxId: id})
		deleteDone <- err
	}()
	for i, done := range []chan error{first, second} {
		select {
		case err := <-done:
			if status.Code(err) != codes.Aborted {
				t.Fatalf("create waiter %d = %v, want Aborted", i, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("create waiter %d did not return", i)
		}
	}
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("DeleteVM: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("DeleteVM did not finish cleanup")
	}
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("fenced boot left a record: %v", err)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id, "disk.qcow2")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("fenced boot left a disk: %v", err)
	}
	h.mg.mu.Lock()
	reserved := h.mg.reserved
	h.mg.mu.Unlock()
	if reserved.vcpus != 0 || reserved.memoryMib != 0 || reserved.diskGiB != 0 {
		t.Fatalf("fenced boot leaked reservation: %+v", reserved)
	}
}

func TestSnapshotUnknownAndEmptyDelete(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	for _, id := range []string{"", "missing / snapshot"} {
		if _, err := h.mg.DeleteSnapshot(t.Context(), &nodev1.DeleteSnapshotRequest{SnapshotId: id}); err != nil {
			t.Fatalf("DeleteSnapshot(%q): %v", id, err)
		}
	}
	if err := saveSnapshot(t.Context(), h.mg, ids.Sandbox(), "missing-source"); status.Code(err) != codes.NotFound {
		t.Fatalf("missing source save = %v, want NotFound", err)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "snapshots", "staging")); err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
}
