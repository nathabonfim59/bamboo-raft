package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	nodev1 "github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// A10 D1: the standalone reclaimer binds both confined roots (data and
// images) and owns the tool kind's reclaim; the manager settles earlier
// tool attempts after the store lock and before any storage cleanup.

// jailImagesDir is an existing private image root for reclaimer fixtures.
func jailImagesDir(t *testing.T) string {
	t.Helper()
	return jailShortDir(t)
}

func TestNewJailReclaimerBindsExistingImagesRoot(t *testing.T) {
	data, images := filepath.Join(jailShortDir(t), "data"), jailImagesDir(t)
	rc, err := NewJailReclaimer(t.Context(), data, images)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if rc.images == nil {
		t.Fatal("the reclaimer has no images root")
	}
	refusals := map[string]func(t *testing.T) string{
		"missing (never created)": func(t *testing.T) string { return filepath.Join(jailShortDir(t), "images") },
		"group-writable": func(t *testing.T) string {
			dir := jailShortDir(t)
			if err := os.Chmod(dir, 0o775); err != nil {
				t.Fatal(err)
			}
			return dir
		},
		"symlink": func(t *testing.T) string {
			link := filepath.Join(jailShortDir(t), "images")
			if err := os.Symlink(jailImagesDir(t), link); err != nil {
				t.Fatal(err)
			}
			return link
		},
	}
	for name, setup := range refusals {
		t.Run(name, func(t *testing.T) {
			images := setup(t)
			if rc, err := NewJailReclaimer(t.Context(), filepath.Join(jailShortDir(t), "data"), images); err == nil {
				rc.Close()
				t.Fatalf("NewJailReclaimer accepted a %s images root", name)
			}
			if name == "missing (never created)" {
				if _, err := os.Lstat(images); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("the reclaimer created the images root")
				}
			}
		})
	}
}

// jailToolReclaimFixture builds a reclaimer over fresh data and images
// roots, with target files at data/vms/bx_11111111/disk.qcow2 (0640) and
// images/v1/root.qcow2 (0444).
type jailToolReclaimFixture struct {
	rc            *JailReclaimer
	data, images  string
	overlay, base string
	witness       *jailReclaimerSync
}

func newJailToolReclaimFixture(t *testing.T) *jailToolReclaimFixture {
	t.Helper()
	data, images := filepath.Join(jailShortDir(t), "data"), jailImagesDir(t)
	rc, err := NewJailReclaimer(t.Context(), data, images)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rc.Close() })
	f := &jailToolReclaimFixture{rc: rc, data: data, images: images, witness: &jailReclaimerSync{}}
	rc.sync = f.witness.sync
	f.overlay = filepath.Join(data, "vms", "bx_11111111", "disk.qcow2")
	f.base = filepath.Join(images, "v1", "root.qcow2")
	for p, mode := range map[string]os.FileMode{f.overlay: 0o640, f.base: 0o444} {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("qcow2"), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f *jailToolReclaimFixture) record(t *testing.T, root jailer.TargetRoot, rel string, access jailer.TargetAccess, file string) jailer.Record {
	t.Helper()
	info, err := os.Lstat(file)
	if err != nil {
		t.Fatal(err)
	}
	st := info.Sys().(*syscall.Stat_t)
	attempt := uuid.NewV7().String()
	return jailer.Record{
		SandboxID: jailer.ToolID(attempt), Attempt: attempt, Kind: jailer.KindTool, UID: uint32(os.Getuid()),
		TargetDev: uint64(st.Dev), TargetIno: st.Ino, TargetRoot: root, TargetPath: rel, TargetAccess: access,
	}
}

func TestJailReclaimerToolReclaim(t *testing.T) {
	for _, access := range []jailer.TargetAccess{jailer.AccessGroupRO, jailer.AccessOwnerRW} {
		t.Run(string(access)+" restores 0600 durably", func(t *testing.T) {
			f := newJailToolReclaimFixture(t)
			rec := f.record(t, jailer.TargetRootData, "vms/bx_11111111/disk.qcow2", access, f.overlay)
			if err := f.rc.Reclaim(t.Context(), rec); err != nil {
				t.Fatal(err)
			}
			info, err := os.Lstat(f.overlay)
			if err != nil || info.Mode().Perm() != 0o600 {
				t.Fatalf("target mode %v, %v; want 0600", info.Mode(), err)
			}
			want := []string{fmt.Sprintf("disk.qcow2 uid=%d mode=-rw-------", os.Geteuid())}
			if !slices.Equal(f.witness.seen, want) {
				t.Fatalf("barrier saw %v, want %v", f.witness.seen, want)
			}
			if err := f.rc.Reclaim(t.Context(), rec); err != nil {
				t.Fatalf("tool reclaim is not idempotent: %v", err)
			}
		})
	}
	t.Run("shared-ro base is never touched", func(t *testing.T) {
		f := newJailToolReclaimFixture(t)
		before := jailTreeWithContent(t, f.images, jailTree(t, f.images))
		rec := f.record(t, jailer.TargetRootImages, "v1/root.qcow2", jailer.AccessShared, f.base)
		if err := f.rc.Reclaim(t.Context(), rec); err != nil {
			t.Fatal(err)
		}
		if after := jailTreeWithContent(t, f.images, jailTree(t, f.images)); !maps.Equal(before, after) {
			t.Fatal("a shared-ro reclaim changed the image base")
		}
	})
	refusals := map[string]func(t *testing.T, f *jailToolReclaimFixture) jailer.Record{
		"replaced target": func(t *testing.T, f *jailToolReclaimFixture) jailer.Record {
			rec := f.record(t, jailer.TargetRootData, "vms/bx_11111111/disk.qcow2", jailer.AccessGroupRO, f.overlay)
			if err := os.Rename(f.overlay, f.overlay+".moved"); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f.overlay, []byte("foreign"), 0o640); err != nil {
				t.Fatal(err)
			}
			return rec
		},
		"symlinked target": func(t *testing.T, f *jailToolReclaimFixture) jailer.Record {
			rec := f.record(t, jailer.TargetRootData, "vms/bx_11111111/link", jailer.AccessGroupRO, f.overlay)
			if err := os.Symlink("disk.qcow2", filepath.Join(filepath.Dir(f.overlay), "link")); err != nil {
				t.Fatal(err)
			}
			return rec
		},
		"path escapes its root": func(t *testing.T, f *jailToolReclaimFixture) jailer.Record {
			return f.record(t, jailer.TargetRootData, "../escape", jailer.AccessGroupRO, f.overlay)
		},
		"unknown root": func(t *testing.T, f *jailToolReclaimFixture) jailer.Record {
			return f.record(t, "etc", "vms/bx_11111111/disk.qcow2", jailer.AccessGroupRO, f.overlay)
		},
		"unknown access": func(t *testing.T, f *jailToolReclaimFixture) jailer.Record {
			return f.record(t, jailer.TargetRootData, "vms/bx_11111111/disk.qcow2", "rw", f.overlay)
		},
		"writable access under images": func(t *testing.T, f *jailToolReclaimFixture) jailer.Record {
			return f.record(t, jailer.TargetRootImages, "v1/root.qcow2", jailer.AccessOwnerRW, f.base)
		},
	}
	for name, setup := range refusals {
		t.Run(name, func(t *testing.T) {
			f := newJailToolReclaimFixture(t)
			rec := setup(t, f)
			before := jailTreeWithContent(t, f.data, jailTree(t, f.data))
			beforeImages := jailTreeWithContent(t, f.images, jailTree(t, f.images))
			if err := f.rc.Reclaim(t.Context(), rec); err == nil {
				t.Fatalf("Reclaim = %v; want a refusal", err)
			}
			if after := jailTreeWithContent(t, f.data, jailTree(t, f.data)); !maps.Equal(before, after) {
				t.Fatal("a refused tool reclaim changed the data root")
			}
			if after := jailTreeWithContent(t, f.images, jailTree(t, f.images)); !maps.Equal(beforeImages, after) {
				t.Fatal("a refused tool reclaim changed the images root")
			}
		})
	}
}

// TestNewSettlesToolsBeforeStorageCleanup: SettleTools runs once, after the
// store lock and before snapshot staging recovery (and any other storage
// cleanup), and a retained tool's staged target survives startup.
func TestNewSettlesToolsBeforeStorageCleanup(t *testing.T) {
	fj := newFakeJail(t)
	var stage, staged string
	fj.settleTools = func(context.Context) (jailer.ToolReport, error) {
		if _, err := os.Lstat(stage); err != nil {
			t.Errorf("snapshot staging was cleaned before SettleTools: %v", err)
		}
		var report jailer.ToolReport
		for _, rec := range fj.Records() {
			report.Retained = append(report.Retained, rec.Ref())
		}
		return report, nil
	}
	newJailedHarness(t, jailedOptions{jail: fj, prepare: func(t *testing.T, dataDir, _ string) {
		stage = filepath.Join(dataDir, "snapshots", "staging", storage.SnapshotKey("in flight"))
		staged = filepath.Join(stage, "disk.qcow2")
		if err := os.MkdirAll(stage, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(staged, []byte("staged"), 0o600); err != nil {
			t.Fatal(err)
		}
		info, err := os.Lstat(staged)
		if err != nil {
			t.Fatal(err)
		}
		st := info.Sys().(*syscall.Stat_t)
		attempt := uuid.NewV7().String()
		fj.records[jailer.ToolID(attempt)] = jailer.Record{
			SchemaVersion: 2, SandboxID: jailer.ToolID(attempt), Attempt: attempt, Kind: jailer.KindTool, UID: 50000,
			TargetDev: uint64(st.Dev), TargetIno: st.Ino, TargetRoot: jailer.TargetRootData,
			TargetPath: "snapshots/staging/" + storage.SnapshotKey("in flight") + "/disk.qcow2", TargetAccess: jailer.AccessGroupRO,
			CreatedUnixMs: time.Now().UnixMilli(),
		}
	}})
	if fj.settled != 1 {
		t.Fatalf("SettleTools ran %d times at startup, want 1", fj.settled)
	}
	if _, err := os.Lstat(staged); err != nil {
		t.Fatalf("startup removed a retained tool's staged target: %v", err)
	}
}

func TestNewFailsWhenSettleToolsFails(t *testing.T) {
	fj := newFakeJail(t)
	fj.settleTools = func(context.Context) (jailer.ToolReport, error) {
		return jailer.ToolReport{}, errors.New("injected corrupt tool record")
	}
	jailNewFails(t, fj, "", nil, "injected corrupt tool record")
}

// jailRetainTool plants a retained tool record holding path's inode in fj,
// as a jailer whose tool exit was unproven would hold it.
func jailRetainTool(t *testing.T, fj *fakeJail, path, rel string) jailer.Ref {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	st := info.Sys().(*syscall.Stat_t)
	attempt := uuid.NewV7().String()
	rec := jailer.Record{
		SchemaVersion: 2, SandboxID: jailer.ToolID(attempt), Attempt: attempt, Kind: jailer.KindTool, UID: 50001,
		TargetDev: uint64(st.Dev), TargetIno: st.Ino, TargetRoot: jailer.TargetRootData, TargetPath: rel,
		TargetAccess: jailer.AccessOwnerRW, CreatedUnixMs: time.Now().UnixMilli(),
	}
	fj.mu.Lock()
	fj.records[rec.SandboxID] = rec
	fj.mu.Unlock()
	return rec.Ref()
}

func jailDropRecord(fj *fakeJail, ref jailer.Ref) {
	fj.mu.Lock()
	delete(fj.records, ref.SandboxID)
	fj.mu.Unlock()
}

// TestDeleteVMKeepsLeasedDiskAndReservation: while a retained tool holds a
// VM's overlay, DeleteVM fails before releasing anything: the disk, the
// network allocation, the capacity reservation and the VM entry all stay,
// and a retry after the tool is settled succeeds.
func TestDeleteVMKeepsLeasedDiskAndReservation(t *testing.T) {
	fj := newFakeJail(t)
	j := newJailedHarness(t, jailedOptions{jail: fj, fallback: pingOnlyGuest{}, stubGuest: true})
	id := "bx_77777777"
	j.create(id, nil)
	disk := j.mg.store.DiskPath(id)
	ref := jailRetainTool(t, fj, disk, "vms/"+id+"/disk.qcow2")
	reserved := j.reserved()
	if _, err := j.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err == nil {
		t.Fatal("DeleteVM removed a VM whose overlay a retained tool holds")
	}
	if _, err := os.Lstat(disk); err != nil {
		t.Fatalf("the leased disk is gone: %v", err)
	}
	if !j.netAllocated(id) {
		t.Fatal("the network allocation was released while the disk is leased")
	}
	if got := j.reserved(); got != reserved {
		t.Fatalf("capacity changed from %+v to %+v while the disk is leased", reserved, got)
	}
	if _, err := j.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("the VM entry was dropped: %v", err)
	}
	jailDropRecord(fj, ref)
	if _, err := j.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("retry after settlement: %v", err)
	}
	if _, err := os.Lstat(disk); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the retry left the disk")
	}
}

// retainingImager fails the overlay grow with a retained tool error and
// plants the jailer's hold on the created overlay.
type retainingImager struct {
	jailImager
	fj *fakeJail
	t  *testing.T
}

func (r retainingImager) Grow(_ context.Context, ref storage.ImageRef, _ uint64) error {
	file := ref.File.Name()
	jailRetainTool(r.t, r.fj, file, "vms/"+filepath.Base(filepath.Dir(file))+"/disk.qcow2")
	return fmt.Errorf("jailed resize: %w", storage.ErrToolRetained)
}

// TestFailedCreateKeepsRetainedToolDisk: a create whose grow tool is
// retained fails, keeps the disk and directory, and DeleteVM refuses until
// the tool is settled.
func TestFailedCreateKeepsRetainedToolDisk(t *testing.T) {
	fj := newFakeJail(t)
	j := newJailedHarness(t, jailedOptions{jail: fj, fallback: pingOnlyGuest{}, stubGuest: true, configure: func(cfg *Config) {
		cfg.Imager = retainingImager{fj: fj, t: t}
	}})
	id := "bx_88888888"
	if _, err := j.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 1024, DiskGib: defaultTestBaseGiB + 4}}); err == nil {
		t.Fatal("CreateVM succeeded with a retained grow")
	}
	disk := j.mg.store.DiskPath(id)
	if _, err := os.Lstat(disk); err != nil {
		t.Fatalf("the retained tool's disk was rolled back: %v", err)
	}
	if _, err := j.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err == nil {
		t.Fatal("DeleteVM removed a disk a retained tool holds")
	}
	if _, err := os.Lstat(disk); err != nil {
		t.Fatal("the refused delete removed the disk")
	}
	for _, rec := range fj.Records() {
		jailDropRecord(fj, rec.Ref())
	}
	if _, err := j.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("retry after settlement: %v", err)
	}
}

// TestSettleToolsRunsUnderTheStoreLock: when SettleTools runs, the store's
// exclusive node lock is already held.
func TestSettleToolsRunsUnderTheStoreLock(t *testing.T) {
	fj := newFakeJail(t)
	var dataDir string
	var locked error
	fj.settleTools = func(context.Context) (jailer.ToolReport, error) {
		f, err := os.OpenFile(filepath.Join(dataDir, "node.lock"), os.O_RDWR, 0)
		if err != nil {
			locked = err
			return jailer.ToolReport{}, nil
		}
		defer f.Close()
		locked = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if locked == nil {
			_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		}
		return jailer.ToolReport{}, nil
	}
	newJailedHarness(t, jailedOptions{jail: fj, prepare: func(_ *testing.T, d, _ string) { dataDir = d }})
	if fj.settled != 1 {
		t.Fatalf("SettleTools ran %d times", fj.settled)
	}
	if !errors.Is(locked, syscall.EWOULDBLOCK) {
		t.Fatalf("flock during SettleTools = %v, want EWOULDBLOCK (the store lock held)", locked)
	}
}

// TestFailedSettleToolsPreservesStorage: a SettleTools failure stops
// startup before any snapshot staging or orphan cleanup.
func TestFailedSettleToolsPreservesStorage(t *testing.T) {
	fj := newFakeJail(t)
	fj.settleTools = func(context.Context) (jailer.ToolReport, error) {
		return jailer.ToolReport{}, errors.New("injected corrupt tool record")
	}
	var stage string
	var before map[string]string
	jailNewFails(t, fj, "bx_99999999", func(t *testing.T, dataDir, _ string) {
		stage = filepath.Join(dataDir, "snapshots", "staging", storage.SnapshotKey("in flight"))
		if err := os.MkdirAll(stage, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(stage, "disk.qcow2"), []byte("staged"), 0o600); err != nil {
			t.Fatal(err)
		}
		orphan := filepath.Join(dataDir, "vms", "bx_99999999")
		if err := os.MkdirAll(orphan, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(orphan, "disk.qcow2"), []byte("orphan"), 0o600); err != nil {
			t.Fatal(err)
		}
		before = jailTreeWithContent(t, stage, jailTree(t, stage))
	}, "injected corrupt tool record")
	if after := jailTreeWithContent(t, stage, jailTree(t, stage)); !maps.Equal(before, after) {
		t.Fatal("a failed SettleTools let startup clean snapshot staging")
	}
}

// TestStopVMRetainedToolRetry: StopVM keeps the FAILED record, the disk and
// the full reservation while a tool holds the disk, with no network release
// and the hold named in the persisted error; after the tool is settled the
// retry stops it.
func TestStopVMRetainedToolRetry(t *testing.T) {
	fj := newFakeJail(t)
	j := newJailedHarness(t, jailedOptions{jail: fj, fallback: pingOnlyGuest{}, stubGuest: true, configure: func(cfg *Config) {
		cfg.Imager = retainingImager{fj: fj, t: t}
	}})
	id := "bx_88888888"
	if _, err := j.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 1024, DiskGib: defaultTestBaseGiB + 4}}); err == nil {
		t.Fatal("CreateVM succeeded")
	}
	before, releases, allocated := j.reserved(), j.netReleases(), j.netAllocated(id)
	if _, err := j.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err == nil {
		t.Fatal("StopVM succeeded while a tool holds the disk")
	}
	if got := j.state(t, id); got.GetState() != nodev1.VMState_VM_STATE_FAILED || !strings.Contains(got.GetError(), "held") {
		t.Fatalf("state %v error %q; want FAILED naming the hold", got.GetState(), got.GetError())
	}
	if j.reserved() != before || j.netReleases() != releases || j.netAllocated(id) != allocated {
		t.Fatal("StopVM released capacity or network while the disk is held")
	}
	for _, rec := range fj.Records() {
		jailDropRecord(fj, rec.Ref())
	}
	if _, err := j.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("retry after settlement: %v", err)
	}
	if got := j.state(t, id).GetState(); got != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state after retry %v", got)
	}
}

// switchRetainImager retains Grow only once enabled.
type switchRetainImager struct {
	retainingImager
	on *atomic.Bool
}

func (s switchRetainImager) Grow(ctx context.Context, ref storage.ImageRef, size uint64) error {
	if s.on.Load() {
		return s.retainingImager.Grow(ctx, ref, size)
	}
	return nil
}

// TestFailedStartKeepsRetainedToolReservation: a StartVM whose disk grow is
// retained does not publish STOPPED or free compute while the tool holds
// the disk.
func TestFailedStartKeepsRetainedToolReservation(t *testing.T) {
	fj := newFakeJail(t)
	on := &atomic.Bool{}
	j := newJailedHarness(t, jailedOptions{jail: fj, fallback: pingOnlyGuest{}, stubGuest: true, configure: func(cfg *Config) {
		cfg.Imager = switchRetainImager{retainingImager{fj: fj, t: t}, on}
	}})
	id := "bx_99999999"
	j.create(id, nil)
	if _, err := j.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatal(err)
	}
	on.Store(true)
	if _, err := j.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB + 4}}); err == nil {
		t.Fatal("StartVM succeeded with a retained grow")
	}
	got := j.state(t, id)
	if got.GetState() == nodev1.VMState_VM_STATE_STOPPED {
		t.Fatal("a start whose grow tool is retained published STOPPED")
	}
	if j.reserved().vcpus == 0 {
		t.Fatal("compute was released while the tool holds the disk")
	}
}

// TestReclaimVMRefusesSymlinkToRecordedInode: a VM entry renamed aside and
// replaced by a relative symlink to the same recorded inode is refused and
// left exactly as it is.
func TestReclaimVMRefusesSymlinkToRecordedInode(t *testing.T) {
	for _, name := range []string{storage.ControlDirName, storage.ConsoleLogName, "disk.qcow2"} {
		t.Run(name, func(t *testing.T) {
			f, rec := jailReclaimFixture(t)
			p := filepath.Join(f.m.store.VMDir(jailTestID), name)
			if err := os.Rename(p, p+".kept"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(name+".kept", p); err != nil {
				t.Fatal(err)
			}
			before := jailTreeWithContent(t, f.m.store.VMDir(jailTestID), jailTree(t, f.m.store.VMDir(jailTestID)))
			if err := jailNodeReclaimer(t, f, nil).Reclaim(t.Context(), rec); err == nil {
				t.Fatal("reclaim followed a symlink to the recorded inode")
			}
			if after := jailTreeWithContent(t, f.m.store.VMDir(jailTestID), jailTree(t, f.m.store.VMDir(jailTestID))); !maps.Equal(before, after) {
				t.Fatal("a refused reclaim changed files")
			}
		})
	}
}
