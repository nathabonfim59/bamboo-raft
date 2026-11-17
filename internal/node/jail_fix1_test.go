package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

// Slice2 fix round 1: cancellation after a proven QMP connection, and the
// durability barriers and bounded enumeration of the VM reclaim policy.

// TestLaunchJailedCanceledAfterQMP (lead regression): a context canceled
// after a valid QMP connection is not success; the owned failure path kills,
// reaps once, settles and reports Canceled.
func TestLaunchJailedCanceledAfterQMP(t *testing.T) {
	f := newJailFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	original := f.dial
	f.dial = func(ctx context.Context, path string, pid int) (*qmp.Client, error) {
		c, err := original(ctx, path, pid)
		if err != nil {
			return c, err
		}
		cancel()
		return c, nil
	}
	proc, err := f.m.launchJailed(ctx, f.v, f.img, f.nic)
	if proc != nil {
		t.Cleanup(func() { proc.Close() })
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("launch after QMP with a canceled context = %v, %v; want Canceled", proc, err)
	}
	if proc != nil {
		t.Fatalf("a settled canceled launch returned a process")
	}
	run := f.jail.lastRun()
	run.joinReaped(t)
	waits, _, releases := run.counts()
	if waits != 1 || releases != 1 {
		t.Fatalf("waits %d releases %d, want 1 and 1", waits, releases)
	}
	if _, ok := f.jail.Record(jailTestID); ok || f.ref() != (jailer.Ref{}) {
		t.Fatal("the canceled attempt's reservation was not settled")
	}
}

// TestLaunchJailedCanceledAfterQMPReleaseFails: the same cancellation with a
// failing release keeps the handle-closed process and the claimed attempt.
func TestLaunchJailedCanceledAfterQMPReleaseFails(t *testing.T) {
	f := newJailFixture(t)
	f.jail.releaseErr = errors.New("injected release failure")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	original := f.dial
	f.dial = func(ctx context.Context, path string, pid int) (*qmp.Client, error) {
		c, err := original(ctx, path, pid)
		cancel()
		return c, err
	}
	proc, err := f.m.launchJailed(ctx, f.v, f.img, f.nic)
	if !errors.Is(err, context.Canceled) || proc == nil {
		t.Fatalf("launch = %v, %v; want the retained process with Canceled", proc, err)
	}
	run := f.jail.lastRun()
	run.joinReaped(t)
	if _, detaches, _ := run.counts(); detaches != 1 {
		t.Fatalf("detaches %d, want 1", detaches)
	}
	if _, ok := f.jail.Record(jailTestID); !ok || f.ref() == (jailer.Ref{}) {
		t.Fatal("an unreleased reservation was dropped")
	}
}

// TestLaunchJailedSuccessDetachedFromLaterCancel: once launchJailed returned
// success, canceling its context does not touch the process.
func TestLaunchJailedSuccessDetachedFromLaterCancel(t *testing.T) {
	f := newJailFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	proc, err := f.m.launchJailed(ctx, f.v, f.img, f.nic)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	run := f.jail.lastRun()
	if run.isReaped() || procDone(proc) {
		t.Fatal("a successful launch died with its startup context")
	}
	if _, detaches, releases := run.counts(); detaches != 0 || releases != 0 {
		t.Fatal("a successful launch released its handle on cancel")
	}
}

// jailSyncRecorder is a store sync seam that records the synced entry names
// and fails the n-th sync when failAt is set.
type jailSyncRecorder struct {
	mu     sync.Mutex
	names  []string
	failAt int
}

func (r *jailSyncRecorder) sync(f *os.File) error {
	r.mu.Lock()
	r.names = append(r.names, filepath.Base(f.Name()))
	n, failAt := len(r.names), r.failAt
	r.mu.Unlock()
	if failAt != 0 && n == failAt {
		return fmt.Errorf("injected sync failure %d", n)
	}
	return f.Sync()
}

func (r *jailSyncRecorder) reset(failAt int) {
	r.mu.Lock()
	r.names, r.failAt = nil, failAt
	r.mu.Unlock()
}

func (r *jailSyncRecorder) synced() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.names)
}

func jailReclaimFixtureWith(t *testing.T, rec *jailSyncRecorder) (*jailFixture, jailer.Record) {
	t.Helper()
	f := newJailFixtureWith(t, rec.sync)
	pins, err := f.m.store.PinLaunch(t.Context(), jailTestID, f.img)
	if err != nil {
		t.Fatal(err)
	}
	defer pins.Close()
	id := func(file *os.File) (uint64, uint64) {
		info, err := file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		st := info.Sys().(*syscall.Stat_t)
		return uint64(st.Dev), st.Ino
	}
	record := jailer.Record{SandboxID: jailTestID, Attempt: "01a1d1c7-34c4-796f-a5e0-f6c4d7c51d7e", Kind: jailer.KindVM, UID: uint32(os.Getuid())}
	record.ControlDev, record.ControlIno = id(pins.Control)
	record.LogDev, record.LogIno = id(pins.Console)
	record.TargetDev, record.TargetIno = id(pins.Overlay)
	return f, record
}

// jailNodeReclaimer is the standalone jail reclaimer on the fixture's data
// directory, its sync seam set to sync (nil: File.Sync); it is closed at
// cleanup.
func jailNodeReclaimer(t *testing.T, f *jailFixture, sync func(*os.File) error) *JailReclaimer {
	t.Helper()
	rc, err := NewJailReclaimer(t.Context(), f.m.store.DataDir(), f.m.store.ImageDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rc.Close() })
	rc.sync = sync
	return rc
}

// TestReclaimVMDurabilityBarriers: every reclaimed entry is synced after its
// ownership and mode change (the control directory after its socket is
// removed) before reclaim reports success; a failing barrier is an error and
// a retry by the same record completes.
func TestReclaimVMDurabilityBarriers(t *testing.T) {
	want := []string{storage.ControlDirName, storage.ConsoleLogName, "disk.qcow2"}
	recorder := &jailSyncRecorder{}
	f, rec := jailReclaimFixtureWith(t, recorder)
	control := filepath.Join(f.m.store.VMDir(jailTestID), storage.ControlDirName)
	if err := syscall.Mknod(filepath.Join(control, qemu.QMPFilename), syscall.S_IFSOCK|0o600, 0); err != nil {
		t.Fatal(err)
	}
	recorder.reset(0)
	if err := jailNodeReclaimer(t, f, recorder.sync).Reclaim(t.Context(), rec); err != nil {
		t.Fatal(err)
	}
	if got := recorder.synced(); !slices.Equal(got, want) {
		t.Fatalf("reclaim barriers = %v, want %v", got, want)
	}
	for failAt := 1; failAt <= len(want); failAt++ {
		t.Run(fmt.Sprintf("barrier %d fails", failAt), func(t *testing.T) {
			recorder := &jailSyncRecorder{}
			f, rec := jailReclaimFixtureWith(t, recorder)
			recorder.reset(failAt)
			if err := jailNodeReclaimer(t, f, recorder.sync).Reclaim(t.Context(), rec); err == nil {
				t.Fatalf("reclaim succeeded although barrier %d failed", failAt)
			}
			recorder.reset(0)
			if err := jailNodeReclaimer(t, f, recorder.sync).Reclaim(t.Context(), rec); err != nil {
				t.Fatalf("retry: %v", err)
			}
			if got := recorder.synced(); !slices.Equal(got, want) {
				t.Fatalf("retry barriers = %v, want %v", got, want)
			}
		})
	}
}

// TestReclaimVMBoundedControlEnumeration: a control directory holding more
// than the one supported entry is refused unchanged.
func TestReclaimVMBoundedControlEnumeration(t *testing.T) {
	recorder := &jailSyncRecorder{}
	f, rec := jailReclaimFixtureWith(t, recorder)
	control := filepath.Join(f.m.store.VMDir(jailTestID), storage.ControlDirName)
	if err := syscall.Mknod(filepath.Join(control, qemu.QMPFilename), syscall.S_IFSOCK|0o600, 0); err != nil {
		t.Fatal(err)
	}
	for i := range 64 {
		if err := os.WriteFile(filepath.Join(control, fmt.Sprintf("flood%02d", i)), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	before := jailTree(t, f.m.store.VMDir(jailTestID))
	recorder.reset(0)
	if err := jailNodeReclaimer(t, f, recorder.sync).Reclaim(t.Context(), rec); err == nil {
		t.Fatal("reclaim accepted an oversized control directory")
	}
	if after := jailTree(t, f.m.store.VMDir(jailTestID)); !maps.Equal(before, after) {
		t.Fatal("a refused reclaim changed files")
	}
	if got := recorder.synced(); len(got) != 0 {
		t.Fatalf("a refused reclaim ran barriers %v", got)
	}
}
