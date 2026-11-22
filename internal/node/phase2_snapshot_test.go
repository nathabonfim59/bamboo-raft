package node

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// snapCutLocked models the cut commands of a live snapshot on the fake QMP
// server: add-fd, blockdev-add of the fleece, the cut transaction (atomic:
// a leftover bbraft-cut refuses it and changes nothing), the NBD server and
// the export.
func (b *bitmapFakeState) snapCutLocked(cmd string, args jsontext.Value) (any, string, bool) {
	find := func(name string) int {
		return slices.IndexFunc(b.bitmaps, func(bm qemu.BlockDirtyBitmap) bool { return bm.Name == name })
	}
	switch cmd {
	case "add-fd":
		if b.fdset {
			return nil, "fd set already exists", true
		}
		b.fdset = true
		return map[string]any{"fdset-id": 3, "fd": 54}, "", true
	case "blockdev-add":
		var a struct {
			NodeName string `json:"node-name"`
			Backing  string `json:"backing"`
			File     struct {
				Filename string `json:"filename"`
			} `json:"file"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return nil, err.Error(), true
		}
		if b.fleece {
			return nil, "Duplicate nodes with node-name='" + a.NodeName + "'", true
		}
		if a.NodeName != fleeceNode || a.Backing != qemu.RootDiskNode || a.File.Filename != "/dev/fdset/3" {
			return nil, "unexpected blockdev-add " + string(args), true
		}
		b.fleece = true
		return map[string]any{}, "", true
	case "transaction":
		var a struct {
			Actions []struct {
				Type string `json:"type"`
			} `json:"actions"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return nil, err.Error(), true
		}
		var types []string
		for _, act := range a.Actions {
			types = append(types, act.Type)
		}
		if !slices.Equal(types, []string{"block-dirty-bitmap-add", "block-dirty-bitmap-merge", "block-dirty-bitmap-clear", "blockdev-backup"}) {
			return nil, "unexpected transaction " + strings.Join(types, ","), true
		}
		d := find(qemu.DirtyBitmap)
		if find(qemu.CutBitmap) >= 0 || d < 0 || !b.fleece {
			return nil, "transaction refused", true
		}
		b.bitmaps = append(b.bitmaps, qemu.BlockDirtyBitmap{Name: qemu.CutBitmap, Count: b.bitmaps[d].Count, Granularity: qemu.DirtyBitmapGranularity})
		b.bitmaps[d].Count = 0
		b.job = true
		return map[string]any{}, "", true
	case "nbd-server-start":
		if b.nbd {
			return nil, "NBD server already running", true
		}
		b.nbd = true
		return map[string]any{}, "", true
	case "block-export-add":
		if b.export || !b.nbd || !b.fleece {
			return nil, "export refused", true
		}
		b.export = true
		return map[string]any{}, "", true
	}
	return nil, "", false
}

// CreateStandalone makes the fake imager write a minimal standalone qcow2
// v3 header (fleece templates).
func (f *fakeImager) CreateStandalone(_ context.Context, dst storage.ImageRef, virtualSize uint64) error {
	f.mu.Lock()
	f.calls = append(f.calls, "create-standalone "+dst.Path)
	f.mu.Unlock()
	return snapvmWriteQCOW(dst.File, virtualSize)
}

// snapvmWriteQCOW writes a qcow2 v3 header of virtualSize with no backing.
func snapvmWriteQCOW(f *os.File, virtualSize uint64) error {
	h := make([]byte, 104)
	copy(h, "QFI\xfb")
	binary.BigEndian.PutUint32(h[4:], 3)
	binary.BigEndian.PutUint32(h[20:], 16) // cluster bits
	binary.BigEndian.PutUint64(h[24:], virtualSize)
	binary.BigEndian.PutUint32(h[100:], 104)
	_, err := f.WriteAt(h, 0)
	return err
}

// snapvmDisk is an in-memory cut disk: chunk i holds content[i] (zero
// otherwise), Dirty yields dirty, Allocated yields allocated.
type snapvmDisk struct {
	size      uint64
	content   map[uint64]byte
	dirty     []uint64
	allocated []uint64
	readErr   error
	// block, when set, makes ReadChunk report on entered (once) and wait
	// for its context: a slow read.
	block     bool
	entered   chan struct{}
	enterOnce sync.Once
	reads     atomic.Int64
	closed    atomic.Bool
}

func (d *snapvmDisk) Size() uint64 { return d.size }

func (d *snapvmDisk) seq(list []uint64) iter.Seq2[uint64, error] {
	return func(yield func(uint64, error) bool) {
		for _, i := range list {
			if !yield(i, nil) {
				return
			}
		}
	}
}

func (d *snapvmDisk) Dirty(context.Context) iter.Seq2[uint64, error]     { return d.seq(d.dirty) }
func (d *snapvmDisk) Allocated(context.Context) iter.Seq2[uint64, error] { return d.seq(d.allocated) }

func (d *snapvmDisk) ReadChunk(ctx context.Context, i uint64, buf []byte) ([]byte, error) {
	d.reads.Add(1)
	if d.block {
		d.enterOnce.Do(func() { close(d.entered) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if d.readErr != nil {
		return nil, d.readErr
	}
	n := min(snapformat.ChunkSize, d.size-i*snapformat.ChunkSize)
	buf = slices.Grow(buf[:0], int(n))[:n]
	clear(buf)
	if b, ok := d.content[i]; ok {
		for j := range buf {
			buf[j] = b ^ byte(j)
		}
	}
	return buf, nil
}

func (d *snapvmDisk) Close() error { d.closed.Store(true); return nil }

// snapvmGuest logs the guest's Freeze and Thaw calls into the fake QMP
// command log, so the test sees their order against the cut.
type snapvmGuest struct {
	guestv1.GuestServer
	srv *fakeQMPServer
}

func (g snapvmGuest) Freeze(ctx context.Context, req *guestv1.FreezeRequest) (*guestv1.FreezeResponse, error) {
	g.srv.mu.Lock()
	g.srv.p2.cmds = append(g.srv.p2.cmds, "guest:freeze")
	g.srv.mu.Unlock()
	return g.GuestServer.Freeze(ctx, req)
}

func (g snapvmGuest) Thaw(ctx context.Context, req *guestv1.ThawRequest) (*guestv1.ThawResponse, error) {
	g.srv.mu.Lock()
	g.srv.p2.cmds = append(g.srv.p2.cmds, "guest:thaw")
	g.srv.mu.Unlock()
	return g.GuestServer.Thaw(ctx, req)
}

// snapvmStore fails PutIfAbsent of chunk objects after failAfter of them.
type snapvmStore struct {
	objstore.Store
	failAfter int64
	puts      atomic.Int64
}

func (s *snapvmStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	if strings.HasPrefix(key, "chunks/") && s.failAfter >= 0 && s.puts.Add(1) > s.failAfter {
		return errors.New("injected store outage")
	}
	return s.Store.PutIfAbsent(ctx, key, r, size)
}

// snapvmEnv is a booted READY VM on a fake QMP server with an in-memory
// cut disk.
type snapvmEnv struct {
	h     *harness
	srv   *fakeQMPServer
	id    string
	store objstore.Store
	free  atomic.Uint64
	disk  *snapvmDisk
	opens atomic.Int64
}

func snapvmSetup(t *testing.T, configure func(*Config)) *snapvmEnv {
	t.Helper()
	env := &snapvmEnv{srv: bitmapFakeQMP(t, "guest"), id: ids.Sandbox(), store: memstore.New()}
	env.free.Store(1 << 40)
	launcher := &fakeLauncher{}
	launcher.wrapProcess = func(p VMProcess) VMProcess {
		client, err := qmp.Dial(t.Context(), env.srv.path, quietLog())
		if err != nil {
			t.Errorf("qmp dial: %v", err)
			return p
		}
		return &qmpFakeProcess{fakeProcess: p.(*fakeProcess), client: client}
	}
	env.h = newHarness(t, harnessOptions{
		launcher:  launcher,
		wrapGuest: func(g guestv1.GuestServer) guestv1.GuestServer { return snapvmGuest{GuestServer: g, srv: env.srv} },
		diskFree:  func() (uint64, error) { return env.free.Load(), nil },
		configure: func(cfg *Config) {
			cfg.Store = env.store
			if configure != nil {
				configure(cfg)
			}
		},
	})
	size := uint64(defaultTestBaseGiB) << 30
	env.disk = &snapvmDisk{
		size:      size,
		content:   map[uint64]byte{0: 1, 3: 2, 7: 3},
		allocated: []uint64{0, 3, 5, 7}, // 5 is allocated but zero
	}
	env.h.mg.openCutDisk = func(_ context.Context, opts cutDiskOptions) (cutDisk, error) {
		env.opens.Add(1)
		if opts.MaxDepth != liveSandboxDepth || opts.PeerPID != os.Getpid() || opts.Conns <= 0 {
			return nil, errors.New("unexpected cut disk options")
		}
		return env.disk, nil
	}
	env.h.create(env.id, nil)
	env.setDirtyCount(12 << 20)
	return env
}

func (e *snapvmEnv) setDirtyCount(n uint64) {
	e.srv.mu.Lock()
	defer e.srv.mu.Unlock()
	for i := range e.srv.p2.bitmaps {
		if e.srv.p2.bitmaps[i].Name == qemu.DirtyBitmap {
			e.srv.p2.bitmaps[i].Count = n
		}
	}
}

func (e *snapvmEnv) resetCmds() {
	e.srv.mu.Lock()
	e.srv.p2.cmds = nil
	e.srv.mu.Unlock()
}

func (e *snapvmEnv) snapshot(t *testing.T, gen uint32, chain string, freeze *bool) (*nodev1.Generation, error) {
	t.Helper()
	return e.h.mg.SnapshotVM(t.Context(), &nodev1.SnapshotVMRequest{SandboxId: e.id, ChainId: chain, Generation: gen, Freeze: freeze})
}

func (e *snapvmEnv) reservedGiB() uint64 {
	e.h.mg.mu.Lock()
	defer e.h.mg.mu.Unlock()
	return e.h.mg.reserved.diskGiB
}

// assertSettled checks a finished SnapshotVM left nothing behind: no QMP
// cut object, no bbraft-cut, bbraft-dirty with wantDirty, no marker, no
// fleece file and the fleece reservation released.
func (e *snapvmEnv) assertSettled(t *testing.T, reservedBefore, wantDirty uint64) {
	t.Helper()
	e.srv.mu.Lock()
	p2 := e.srv.p2
	e.srv.mu.Unlock()
	if p2.job || p2.fleece || p2.fdset || p2.export || p2.nbd {
		t.Errorf("cut graph left behind: job=%v fleece=%v fdset=%v export=%v nbd=%v", p2.job, p2.fleece, p2.fdset, p2.export, p2.nbd)
	}
	if _, found := e.srv.p2Bitmap(qemu.CutBitmap); found {
		t.Errorf("%s left behind", qemu.CutBitmap)
	}
	if bm, found := e.srv.p2Bitmap(qemu.DirtyBitmap); !found || bm.Count != wantDirty {
		t.Errorf("%s = %+v (found %v), want count %d", qemu.DirtyBitmap, bm, found, wantDirty)
	}
	if st := phase2StateOf(t, e.h, e.id); st.SnapshotInProgress {
		t.Errorf("snapshot marker left set: %+v", st)
	}
	if _, err := os.Lstat(filepath.Join(e.h.mg.store.VMDir(e.id), storage.FleeceName)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("fleece file left behind: %v", err)
	}
	if got := e.reservedGiB(); got != reservedBefore {
		t.Errorf("reserved disk = %d GiB, want %d (fleece reservation released)", got, reservedBefore)
	}
}

func snapvmFilter(cmds []string) []string {
	skip := map[string]bool{"query-qmp-schema": true, "query-status": true, "cont": true}
	return slices.DeleteFunc(cmds, func(c string) bool { return skip[c] })
}

// TestSnapshotVMLiveSequence proves a live cut's order (spike method A,
// marker before add-fd), the success bookkeeping (last manifest recorded,
// bbraft-cut removed, marker cleared, fleece gone, reservation released)
// and the Generation's fields, for a full first generation and an
// incremental second one.
func TestSnapshotVMLiveSequence(t *testing.T) {
	env := snapvmSetup(t, nil)
	chain := uuid.NewV7().String()
	reserved := env.reservedGiB()
	env.resetCmds()

	gen, err := env.snapshot(t, 0, chain, nil)
	if err != nil {
		t.Fatalf("SnapshotVM gen 0: %v", err)
	}
	want := []string{
		"query-named-block-nodes", // the bitmap check before the cut (a healthy bitmap: nothing to repair)
		"add-fd", "blockdev-add", "guest:freeze", "transaction", "guest:freeze", "guest:thaw",
		"nbd-server-start", "block-export-add",
		"block-export-del", "query-block-exports", "nbd-server-stop", "query-jobs", "block-job-cancel", "query-jobs",
		"blockdev-del", "query-fdsets", "remove-fd", "query-named-block-nodes", "query-fdsets",
		"block-dirty-bitmap-remove",
	}
	if got := snapvmFilter(env.srv.p2Commands()); !slices.Equal(got, want) {
		t.Fatalf("commands:\n got %q\nwant %q", got, want)
	}
	if !gen.GetFrozen() || gen.GetChainId() != chain || gen.GetGeneration() != 0 || gen.GetImage() != testImageID {
		t.Fatalf("generation = %+v", gen)
	}
	// Full scan: the four allocated chunks, three with data.
	if gen.GetChunks() != 4 || gen.GetChunksUploaded() != 3 || gen.GetContentBytes() != 3*snapformat.ChunkSize {
		t.Fatalf("gen 0 counters = %+v", gen)
	}
	if gen.GetDiskBytes() != env.disk.size || gen.GetCreatedAtUnixMs() == 0 || gen.GetBytesUploaded() == 0 {
		t.Fatalf("gen 0 = %+v", gen)
	}
	key := snapformat.ManifestKey(chain, 0)
	if gen.GetManifestKey() != key {
		t.Fatalf("manifest key %q, want %q", gen.GetManifestKey(), key)
	}
	if _, err := env.store.Head(t.Context(), key); err != nil {
		t.Fatalf("manifest not in the store: %v", err)
	}
	gotKey, _, ok, err := env.h.mg.store.LastManifest(env.id)
	if err != nil || !ok || gotKey != key {
		t.Fatalf("LastManifest = %q, %v, %v; want %q", gotKey, ok, err, key)
	}
	if st := phase2StateOf(t, env.h, env.id); st.FullScanNext {
		t.Fatalf("full scan next still set after a full scan: %+v", st)
	}
	env.assertSettled(t, reserved, 0)
	if !env.disk.closed.Load() {
		t.Fatal("cut disk not closed")
	}

	// Gen 1 is incremental: only the dirty chunks are read; chunk 3 is
	// unchanged content and is not uploaded again.
	env.disk.dirty = []uint64{3, 9}
	env.disk.content[9] = 4
	env.setDirtyCount(8 << 20)
	before := env.disk.reads.Load()
	gen1, err := env.snapshot(t, 1, chain, nil)
	if err != nil {
		t.Fatalf("SnapshotVM gen 1: %v", err)
	}
	if reads := env.disk.reads.Load() - before; reads != 2 {
		t.Fatalf("gen 1 read %d chunks, want 2 (dirty only)", reads)
	}
	if gen1.GetChunks() != 5 || gen1.GetChunksUploaded() != 1 {
		t.Fatalf("gen 1 counters = %+v", gen1)
	}
	env.assertSettled(t, reserved, 0)
}

// TestSnapshotVMLiveNoFreeze: freeze=false never calls the guest and
// reports frozen=false.
func TestSnapshotVMLiveNoFreeze(t *testing.T) {
	env := snapvmSetup(t, nil)
	env.resetCmds()
	freezes, _, _ := env.h.freezer.state()
	gen, err := env.snapshot(t, 0, uuid.NewV7().String(), new(false))
	if err != nil {
		t.Fatalf("SnapshotVM: %v", err)
	}
	if gen.GetFrozen() {
		t.Fatal("frozen = true for freeze=false")
	}
	if after, _, _ := env.h.freezer.state(); after != freezes {
		t.Fatalf("guest froze %d times for freeze=false", after-freezes)
	}
	if slices.ContainsFunc(env.srv.p2Commands(), func(c string) bool { return strings.HasPrefix(c, "guest:") }) {
		t.Fatalf("guest called: %q", env.srv.p2Commands())
	}
}

// TestSnapshotVMLiveFailures: every failure point tears the graph down,
// merges bbraft-cut back (no dirty bit lost), clears the marker, removes
// the fleece and releases its reservation, with nodefake's codes; the
// guest always ends thawed.
func TestSnapshotVMLiveFailures(t *testing.T) {
	const dirty = 12 << 20
	tests := []struct {
		name  string
		setup func(t *testing.T, e *snapvmEnv, chain string)
		want  codes.Code
		// read reports that the cut disk was opened.
		read bool
	}{
		{name: "add-fd", setup: func(_ *testing.T, e *snapvmEnv, _ string) { e.srv.p2.failCmd = "add-fd" }, want: codes.Internal},
		{name: "blockdev-add", setup: func(_ *testing.T, e *snapvmEnv, _ string) { e.srv.p2.failCmd = "blockdev-add" }, want: codes.Internal},
		{name: "transaction", setup: func(_ *testing.T, e *snapvmEnv, _ string) { e.srv.p2.failCmd = "transaction" }, want: codes.Internal},
		{name: "nbd-server-start", setup: func(_ *testing.T, e *snapvmEnv, _ string) { e.srv.p2.failCmd = "nbd-server-start" }, want: codes.Internal},
		{name: "block-export-add", setup: func(_ *testing.T, e *snapvmEnv, _ string) { e.srv.p2.failCmd = "block-export-add" }, want: codes.Internal},
		{name: "disk read", setup: func(_ *testing.T, e *snapvmEnv, _ string) { e.disk.readErr = errors.New("nbd: read: EIO") }, want: codes.Internal, read: true},
		{name: "store outage mid-read", setup: func(_ *testing.T, e *snapvmEnv, _ string) {
			e.h.mg.cfg.Store = &snapvmStore{Store: e.store, failAfter: 1}
		}, want: codes.Unavailable, read: true},
		{name: "free space floor", setup: func(_ *testing.T, e *snapvmEnv, _ string) { e.free.Store(1 << 30) }, want: codes.ResourceExhausted, read: true},
		{name: "manifest exists", setup: func(t *testing.T, e *snapvmEnv, chain string) {
			key := snapformat.ManifestKey(chain, 0)
			if err := e.store.PutIfAbsent(t.Context(), key, bytes.NewReader([]byte("x")), 1); err != nil {
				t.Fatal(err)
			}
		}, want: codes.AlreadyExists, read: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := snapvmSetup(t, nil)
			chain := uuid.NewV7().String()
			reserved := env.reservedGiB()
			env.srv.mu.Lock()
			tt.setup(t, env, chain)
			env.srv.mu.Unlock()
			_, err := env.snapshot(t, 0, chain, nil)
			if got := status.Code(err); got != tt.want {
				t.Fatalf("SnapshotVM code = %s (%v), want %s", got, err, tt.want)
			}
			if opened := env.opens.Load() > 0; opened != tt.read {
				t.Fatalf("cut disk opened = %v, want %v", opened, tt.read)
			}
			env.srv.mu.Lock()
			env.srv.p2.failCmd = ""
			env.srv.mu.Unlock()
			env.assertSettled(t, reserved, dirty)
			if _, _, frozen := env.h.freezer.state(); frozen {
				t.Fatal("guest left frozen")
			}
			if _, _, ok, _ := env.h.mg.store.LastManifest(env.id); ok {
				t.Fatal("a failed snapshot recorded a last manifest")
			}
			if st, err := env.h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: env.id}); err != nil || st.GetState() != nodev1.VMState_VM_STATE_READY {
				t.Fatalf("VM after a failed snapshot = %v, %v; want READY", st.GetState(), err)
			}

			// The next generation succeeds and, being a full scan, holds
			// every change.
			env.h.mg.cfg.Store = env.store
			env.disk.readErr = nil
			env.free.Store(1 << 40)
			gen, err := env.snapshot(t, 1, chain, nil)
			if err != nil {
				t.Fatalf("next SnapshotVM: %v", err)
			}
			if gen.GetChunks() != 4 {
				t.Fatalf("next generation lists %d chunks, want 4", gen.GetChunks())
			}
			env.assertSettled(t, reserved, 0)
		})
	}
}

// TestSnapshotVMLeftoverCutMergedFirst: a cut an earlier call could not
// settle (marker set, bbraft-cut and the fleece node still in QEMU) is
// merged back before the next cut starts, which then succeeds.
func TestSnapshotVMLeftoverCutMergedFirst(t *testing.T) {
	env := snapvmSetup(t, nil)
	if err := env.h.mg.store.SetSnapshotInProgress(env.id); err != nil {
		t.Fatal(err)
	}
	env.srv.mu.Lock()
	env.srv.p2.bitmaps = append(env.srv.p2.bitmaps, qemu.BlockDirtyBitmap{Name: qemu.CutBitmap, Count: 4 << 20, Granularity: qemu.DirtyBitmapGranularity})
	env.srv.p2.fleece, env.srv.p2.fdset, env.srv.p2.job = true, true, true
	env.srv.mu.Unlock()
	reserved := env.reservedGiB()
	if _, err := env.snapshot(t, 0, uuid.NewV7().String(), nil); err != nil {
		t.Fatalf("SnapshotVM: %v", err)
	}
	env.assertSettled(t, reserved, 0)
}

// TestSnapshotVMStates: only READY is snapshotted live; STOPPED waits for
// the maintenance session; an unknown VM is NOT_FOUND.
func TestSnapshotVMStates(t *testing.T) {
	env := snapvmSetup(t, nil)
	chain := uuid.NewV7().String()
	if _, err := env.h.mg.SnapshotVM(t.Context(), &nodev1.SnapshotVMRequest{SandboxId: ids.Sandbox(), ChainId: chain}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown VM: %v, want NOT_FOUND", err)
	}
	v, err := env.h.mg.lookup(env.id)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range []nodev1.VMState{nodev1.VMState_VM_STATE_BOOTING, nodev1.VMState_VM_STATE_STOPPING, nodev1.VMState_VM_STATE_FAILED} {
		v.mu.Lock()
		v.state = st
		v.mu.Unlock()
		if _, err := env.snapshot(t, 0, chain, nil); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("%s: %v, want FAILED_PRECONDITION", st, err)
		}
	}
	v.mu.Lock()
	v.state = nodev1.VMState_VM_STATE_READY
	v.mu.Unlock()
}

// TestSnapshotVMSerializedWithStop: a StopVM that arrives while a snapshot
// holds the VM operation gate waits for it.
func TestSnapshotVMSerializedWithStop(t *testing.T) {
	env := snapvmSetup(t, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	inner := env.h.mg.openCutDisk
	env.h.mg.openCutDisk = func(ctx context.Context, opts cutDiskOptions) (cutDisk, error) {
		close(entered)
		<-release
		return inner(ctx, opts)
	}
	var wg sync.WaitGroup
	var snapErr error
	wg.Go(func() { _, snapErr = env.snapshot(t, 0, uuid.NewV7().String(), nil) })
	<-entered
	stopped := make(chan error, 1)
	go func() {
		_, err := env.h.mg.StopVM(context.Background(), &nodev1.StopVMRequest{SandboxId: env.id, GraceSeconds: 1})
		stopped <- err
	}()
	select {
	case err := <-stopped:
		t.Fatalf("StopVM returned during the snapshot: %v", err)
	default:
	}
	v, _ := env.h.mg.lookup(env.id)
	v.mu.Lock()
	state := v.state
	v.mu.Unlock()
	if state != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("state during the snapshot = %s, want READY", state)
	}
	close(release)
	wg.Wait()
	if snapErr != nil {
		t.Fatalf("SnapshotVM: %v", snapErr)
	}
	if err := <-stopped; err != nil {
		t.Fatalf("StopVM after the snapshot: %v", err)
	}
}

// TestFleeceBudget pins R = min(disk, max(1, ceil(disk/8))) GiB.
func TestFleeceBudget(t *testing.T) {
	for _, tc := range []struct{ disk, want uint64 }{{1, 1}, {2, 1}, {8, 1}, {9, 2}, {10, 2}, {80, 10}, {81, 11}} {
		if got := fleeceBudgetGiB(tc.disk); got != tc.want {
			t.Errorf("fleeceBudgetGiB(%d) = %d, want %d", tc.disk, got, tc.want)
		}
	}
}

// TestSnapshotVMErrorMapping pins the code of each error class.
func TestSnapshotVMErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want codes.Code
	}{
		{errors.New("boom"), codes.Internal},
		{context.Canceled, codes.Canceled},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
		{&objectStoreError{err: errors.New("503")}, codes.Unavailable},
		{errors.Join(errors.New("x"), errSnapshotSpace), codes.ResourceExhausted},
		{status.Error(codes.FailedPrecondition, "gate"), codes.FailedPrecondition},
	} {
		if got := status.Code(snapshotVMError(tc.err)); got != tc.want {
			t.Errorf("snapshotVMError(%v) = %s, want %s", tc.err, got, tc.want)
		}
	}
}

// TestSnapshotVMLiveFleeceOverBudget: a fleece image whose allocated size
// passes its reservation aborts the read (RESOURCE_EXHAUSTED) and settles
// like every other failure.
func TestSnapshotVMLiveFleeceOverBudget(t *testing.T) {
	env := snapvmSetup(t, nil)
	reserved := env.reservedGiB()
	budget := fleeceBudgetGiB(defaultTestBaseGiB) << 30
	env.h.mg.fleeceUsage = func(*os.File) (uint64, error) { return budget + 1, nil }
	_, err := env.snapshot(t, 0, uuid.NewV7().String(), nil)
	if status.Code(err) != codes.ResourceExhausted || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("SnapshotVM = %v, want RESOURCE_EXHAUSTED naming the reservation", err)
	}
	if env.disk.reads.Load() != 0 {
		t.Fatalf("read %d chunks past the budget check", env.disk.reads.Load())
	}
	env.assertSettled(t, reserved, 12<<20)

	// Within the budget the same call succeeds.
	env.h.mg.fleeceUsage = func(*os.File) (uint64, error) { return budget, nil }
	if _, err := env.snapshot(t, 1, uuid.NewV7().String(), nil); err != nil {
		t.Fatalf("SnapshotVM within the budget: %v", err)
	}
}

// TestSnapshotVMLiveLastManifestFailure: a manifest written to the store
// whose local record fails is a failed call: bbraft-cut is merged back (the
// next incremental generation still sees every change), no last manifest
// is recorded and the marker is cleared.
func TestSnapshotVMLiveLastManifestFailure(t *testing.T) {
	var failRecord atomic.Bool
	env := snapvmSetup(t, func(cfg *Config) {
		cfg.snapshotOptions = storage.SnapshotOptions{Rename: func(root *os.Root, oldName, newName string) error {
			if newName == "last-manifest" && failRecord.Load() {
				return errors.New("injected rename failure")
			}
			return root.Rename(oldName, newName)
		}}
	})
	reserved := env.reservedGiB()
	failRecord.Store(true)
	chain := uuid.NewV7().String()
	_, err := env.snapshot(t, 0, chain, nil)
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "record last manifest") {
		t.Fatalf("SnapshotVM = %v, want INTERNAL naming the local record", err)
	}
	if _, err := env.store.Head(t.Context(), snapformat.ManifestKey(chain, 0)); err != nil {
		t.Fatalf("the manifest itself was written first: %v", err)
	}
	env.assertSettled(t, reserved, 12<<20)
	if _, _, ok, _ := env.h.mg.store.LastManifest(env.id); ok {
		t.Fatal("a failed local record left a last manifest")
	}
	failRecord.Store(false)
	if _, err := env.snapshot(t, 1, chain, nil); err != nil {
		t.Fatalf("next SnapshotVM: %v", err)
	}
	env.assertSettled(t, reserved, 0)
}

// snapvmStoppedEnv is a STOPPED VM whose maintenance session runs on the
// scripted maintenance QEMU, extended with the cut commands: bbraft-cut
// next to the fake's bbraft-dirty.
type snapvmStoppedEnv struct {
	h     *harness
	ml    *maintTestLauncher
	fq    *maintFakeQMP
	store objstore.Store
	disk  *snapvmDisk
	cut   *qemu.BlockDirtyBitmap
	opts  []cutDiskOptions
	optMu sync.Mutex
}

func snapvmStoppedSetup(t *testing.T) *snapvmStoppedEnv {
	t.Helper()
	env := &snapvmStoppedEnv{fq: newMaintFakeQMP(t), store: memstore.New()}
	inner := &fakeLauncher{}
	env.ml = &maintTestLauncher{inner: inner, qmp: env.fq, t: t}
	env.h = newHarness(t, harnessOptions{launcher: inner, configure: func(cfg *Config) {
		cfg.Launcher = env.ml
		cfg.Store = env.store
	}})
	env.h.mg.maintQuit = 200 * time.Millisecond
	env.disk = &snapvmDisk{size: uint64(defaultTestBaseGiB) << 30, content: map[uint64]byte{2: 9}, allocated: []uint64{2}}
	env.h.mg.openCutDisk = func(_ context.Context, opts cutDiskOptions) (cutDisk, error) {
		env.optMu.Lock()
		env.opts = append(env.opts, opts)
		env.optMu.Unlock()
		return env.disk, nil
	}
	env.fq.hook = env.hook
	env.h.create(maintTestID, nil)
	if _, err := env.h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: maintTestID, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	return env
}

// hook models bbraft-cut on the maintenance fake (called under fq.mu).
func (e *snapvmStoppedEnv) hook(cmd string, args map[string]any) (any, string, bool) {
	f := e.fq
	switch cmd {
	case "transaction":
		actions, _ := args["actions"].([]any)
		if len(actions) != 3 {
			return nil, "a STOPPED cut has no backup job", true
		}
		if e.cut != nil || f.bitmap == nil {
			return nil, "transaction refused", true
		}
		e.cut = &qemu.BlockDirtyBitmap{Name: qemu.CutBitmap, Count: f.bitmap.Count, Granularity: qemu.DirtyBitmapGranularity}
		f.bitmap.Count = 0
		return map[string]any{}, "", true
	case "block-export-add":
		if args["node-name"] != qemu.RootDiskNode || args["writable"] != false {
			return nil, "unexpected export", true
		}
		return nil, "", false
	case "block-dirty-bitmap-merge":
		if e.cut == nil || f.bitmap == nil {
			return nil, "bitmap not found", true
		}
		f.bitmap.Count += e.cut.Count
		return map[string]any{}, "", true
	case "block-dirty-bitmap-remove":
		if args["name"] != qemu.CutBitmap {
			return nil, "", false
		}
		if e.cut == nil {
			return nil, "Dirty bitmap 'bbraft-cut' not found", true
		}
		e.cut = nil
		return map[string]any{}, "", true
	case "query-named-block-nodes":
		bitmaps := []any{}
		if f.bitmap != nil {
			bitmaps = append(bitmaps, *f.bitmap)
		}
		if e.cut != nil {
			bitmaps = append(bitmaps, *e.cut)
		}
		return []any{map[string]any{"node-name": qemu.RootDiskNode, "dirty-bitmaps": bitmaps}}, "", true
	case "query-jobs", "query-fdsets":
		return []any{}, "", true
	case "blockdev-del":
		return nil, "Failed to find node", true
	}
	return nil, "", false
}

func (e *snapvmStoppedEnv) dirtyCount() uint64 {
	e.fq.mu.Lock()
	defer e.fq.mu.Unlock()
	if e.fq.bitmap == nil {
		return 0
	}
	return e.fq.bitmap.Count
}

// TestSnapshotVMStopped: a STOPPED VM is read in a maintenance session:
// cut transaction without a backup job, a read-only rootdisk export at
// depth 1 with the maintenance QEMU's pid, bbraft-cut removed after the
// last manifest, quit, frozen=false, the VM still STOPPED. A failed read
// merges the cut back.
func TestSnapshotVMStopped(t *testing.T) {
	env := snapvmStoppedSetup(t)
	// The stored bitmap a clean stop left, with 8 MiB dirty.
	healthy := bitmapHealthy()
	env.fq.mu.Lock()
	env.fq.bitmap = &healthy
	env.fq.mu.Unlock()
	chain := uuid.NewV7().String()

	env.disk.readErr = errors.New("nbd: read: EIO")
	_, err := env.h.mg.SnapshotVM(t.Context(), &nodev1.SnapshotVMRequest{SandboxId: maintTestID, ChainId: chain})
	if status.Code(err) != codes.Internal {
		t.Fatalf("failed read: %v, want INTERNAL", err)
	}
	if got := env.dirtyCount(); got != 8<<20 {
		t.Fatalf("after a failed read bbraft-dirty counts %d, want 8 MiB merged back", got)
	}
	if st := phase2StateOf(t, env.h, maintTestID); st.SnapshotInProgress || st.LastManifestKey != "" {
		t.Fatalf("phase 2 state after a failed read: %+v", st)
	}

	env.disk.readErr = nil
	env.fq.mu.Lock()
	env.fq.commands = nil
	env.fq.mu.Unlock()
	gen, err := env.h.mg.SnapshotVM(t.Context(), &nodev1.SnapshotVMRequest{SandboxId: maintTestID, ChainId: chain, Generation: 1})
	if err != nil {
		t.Fatalf("SnapshotVM: %v", err)
	}
	if gen.GetFrozen() || gen.GetChunks() != 1 || gen.GetChunksUploaded() != 1 {
		t.Fatalf("generation = %+v", gen)
	}
	seen := env.fq.seen()
	order := []string{"transaction", "nbd-server-start", "block-export-add", "block-export-del", "nbd-server-stop", "block-dirty-bitmap-remove", "quit"}
	at := -1
	for _, c := range order {
		i := slices.Index(seen[at+1:], c)
		if i < 0 {
			t.Fatalf("command %q missing or out of order in %q", c, seen)
		}
		at += i + 1
	}
	if slices.Contains(seen, "add-fd") || slices.Contains(seen, "block-dirty-bitmap-merge") {
		t.Fatalf("STOPPED success sent a fleece or merge command: %q", seen)
	}
	proc, _ := env.ml.lastMaint()
	env.optMu.Lock()
	last := env.opts[len(env.opts)-1]
	env.optMu.Unlock()
	if last.MaxDepth != stoppedSandboxDepth || last.PeerPID != proc.PID() {
		t.Fatalf("cut disk options %+v, want depth %d and the maintenance pid %d", last, stoppedSandboxDepth, proc.PID())
	}
	if got := env.dirtyCount(); got != 0 {
		t.Fatalf("bbraft-dirty counts %d after a successful cut, want 0", got)
	}
	if env.cut != nil {
		t.Fatal("bbraft-cut left behind")
	}
	if key, _, ok, err := env.h.mg.store.LastManifest(maintTestID); err != nil || !ok || key != gen.GetManifestKey() {
		t.Fatalf("LastManifest = %q, %v, %v", key, ok, err)
	}
	if st := phase2StateOf(t, env.h, maintTestID); st.SnapshotInProgress || st.FullScanNext {
		t.Fatalf("phase 2 state: %+v", st)
	}
	got, err := env.h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: maintTestID})
	if err != nil || got.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("VM after a STOPPED snapshot = %v, %v", got.GetState(), err)
	}
}

// TestSnapshotVMLiveDeleteFences: DeleteVM during a slow read fences the
// snapshot instead of waiting for it: the call is ABORTED, the cut is torn
// down on its own cleanup context, the fleece is gone, the reservation is
// back and DeleteVM returns promptly.
func TestSnapshotVMLiveDeleteFences(t *testing.T) {
	env := snapvmSetup(t, nil)
	reservedWithVM := env.reservedGiB()
	env.disk.block = true
	env.disk.entered = make(chan struct{})
	var wg sync.WaitGroup
	var snapErr error
	wg.Go(func() { _, snapErr = env.snapshot(t, 0, uuid.NewV7().String(), nil) })
	<-env.disk.entered
	if got := env.reservedGiB(); got <= reservedWithVM {
		t.Fatalf("reserved %d GiB during the cut, want the fleece on top of %d", got, reservedWithVM)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := env.h.mg.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: env.id}); err != nil {
		t.Fatalf("DeleteVM during a snapshot: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("DeleteVM took %s: it waited for the read instead of fencing it", took)
	}
	wg.Wait()
	if status.Code(snapErr) != codes.Aborted {
		t.Fatalf("fenced SnapshotVM = %v, want ABORTED", snapErr)
	}
	env.srv.mu.Lock()
	p2 := env.srv.p2
	env.srv.mu.Unlock()
	if p2.job || p2.fleece || p2.fdset || p2.export || p2.nbd {
		t.Fatalf("fenced cut not torn down: job=%v fleece=%v fdset=%v export=%v nbd=%v", p2.job, p2.fleece, p2.fdset, p2.export, p2.nbd)
	}
	if _, found := env.srv.p2Bitmap(qemu.CutBitmap); found {
		t.Fatalf("%s left behind", qemu.CutBitmap)
	}
	if _, err := os.Lstat(filepath.Join(env.h.mg.store.VMDir(env.id), storage.FleeceName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("fleece file left behind: %v", err)
	}
	if got, want := env.reservedGiB(), reservedWithVM-defaultTestBaseGiB; got != want {
		t.Fatalf("reserved %d GiB after the delete, want %d (VM disk and fleece released)", got, want)
	}
	if _, err := env.h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: env.id}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetVM after delete: %v, want NOT_FOUND", err)
	}
}

// TestSnapshotVMLiveMissingBitmapFullScan: a VM launched while the capability
// gate was unknown never got bbraft-dirty. A live SnapshotVM adds the bitmap
// itself, records a full scan (the bitmap holds no history) and the cut then
// succeeds; without that step the cut's transaction would merge from a
// missing bitmap and fail with INTERNAL on every later call.
func TestSnapshotVMLiveMissingBitmapFullScan(t *testing.T) {
	env := snapvmSetup(t, nil)
	chain := uuid.NewV7().String()
	reserved := env.reservedGiB()
	if _, err := env.snapshot(t, 0, chain, nil); err != nil {
		t.Fatalf("SnapshotVM gen 0: %v", err)
	}

	// The launch left no bitmap and no full-scan record behind (the gate was
	// unknown and the flag was cleared by the first generation's full scan).
	env.srv.mu.Lock()
	env.srv.p2.bitmaps = nil
	env.srv.mu.Unlock()
	if st := phase2StateOf(t, env.h, env.id); st.FullScanNext {
		t.Fatalf("precondition: full scan next is already set: %+v", st)
	}
	env.disk.dirty = []uint64{3} // an incremental read would see only this
	env.resetCmds()
	before := env.disk.reads.Load()

	gen, err := env.snapshot(t, 1, chain, nil)
	if err != nil {
		t.Fatalf("SnapshotVM on a VM without bbraft-dirty: %v (commands %q)", err, env.srv.p2Commands())
	}
	cmds := env.srv.p2Commands()
	add, cut := slices.Index(cmds, "block-dirty-bitmap-add"), slices.Index(cmds, "transaction")
	if add < 0 || cut < 0 || add > cut {
		t.Fatalf("bbraft-dirty must be added before the cut: %q", cmds)
	}
	if bm, found := env.srv.p2Bitmap(qemu.DirtyBitmap); !found || !bm.Persistent || !bm.Recording {
		t.Fatalf("bbraft-dirty after the snapshot = %+v (found %v)", bm, found)
	}
	// The full scan ensureVMBitmap recorded is honoured by this very call:
	// every allocated chunk is read (4), not the one dirty chunk.
	if reads := env.disk.reads.Load() - before; reads != 4 || gen.GetChunks() != 4 {
		t.Fatalf("read %d chunks, generation %+v; want a full scan of the 4 allocated chunks", reads, gen)
	}
	if st := phase2StateOf(t, env.h, env.id); st.FullScanNext {
		t.Fatalf("full scan next still set after the full scan: %+v", st)
	}
	env.assertSettled(t, reserved, 0)
}

// TestSnapshotVMStoppedMissingBitmapFullScan: a STOPPED VM whose disk has no
// bbraft-dirty (launched while the capability gate was unknown) is snapshotted
// as a full scan: the maintenance session's bitmap step adds the bitmap before
// the cut, and the allocated chunks are read even with a previous manifest.
func TestSnapshotVMStoppedMissingBitmapFullScan(t *testing.T) {
	env := snapvmStoppedSetup(t)
	chain := uuid.NewV7().String()
	// One generation first, so a previous manifest exists and an incremental
	// read of the (empty) dirty set would upload nothing.
	healthy := bitmapHealthy()
	healthy.Count = 0
	env.fq.mu.Lock()
	env.fq.bitmap = &healthy
	env.fq.mu.Unlock()
	if _, err := env.h.mg.SnapshotVM(t.Context(), &nodev1.SnapshotVMRequest{SandboxId: maintTestID, ChainId: chain}); err != nil {
		t.Fatalf("SnapshotVM gen 0: %v", err)
	}

	// The bitmap is gone (never added). Make the chunk new content so a full
	// scan has something to upload that an empty incremental would miss.
	env.fq.mu.Lock()
	env.fq.bitmap = nil
	env.fq.commands = nil
	env.fq.mu.Unlock()
	env.disk.content[2] = 7
	gen, err := env.h.mg.SnapshotVM(t.Context(), &nodev1.SnapshotVMRequest{SandboxId: maintTestID, ChainId: chain, Generation: 1})
	if err != nil {
		t.Fatalf("SnapshotVM without bbraft-dirty: %v (commands %q)", err, env.fq.seen())
	}
	seen := env.fq.seen()
	add, cut := slices.Index(seen, "block-dirty-bitmap-add"), slices.Index(seen, "transaction")
	if add < 0 || cut < 0 || add > cut {
		t.Fatalf("bbraft-dirty must be added before the cut: %q", seen)
	}
	if gen.GetChunks() != 1 || gen.GetChunksUploaded() != 1 {
		t.Fatalf("generation %+v: a full scan reads the allocated chunk and uploads its new content", gen)
	}
	if st := phase2StateOf(t, env.h, maintTestID); st.FullScanNext || st.SnapshotInProgress {
		t.Fatalf("phase 2 state: %+v", st)
	}
}
