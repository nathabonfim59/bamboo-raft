package node

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// restoreTestStore wraps a memstore: it counts chunk and manifest GETs and
// can fail Gets of keys matching a prefix (an object store outage).
type restoreTestStore struct {
	*memstore.Store
	chunkGets    atomic.Int64
	manifestGets atomic.Int64

	mu         sync.Mutex
	failPrefix string
}

func (s *restoreTestStore) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if strings.HasPrefix(key, "chunks/") {
		s.chunkGets.Add(1)
	} else {
		s.manifestGets.Add(1)
	}
	s.mu.Lock()
	fail := s.failPrefix != "" && strings.HasPrefix(key, s.failPrefix)
	s.mu.Unlock()
	if fail {
		return nil, errors.New("connection refused")
	}
	return s.Store.Get(ctx, key, offset, length)
}

func (s *restoreTestStore) put(t *testing.T, key string, b []byte) {
	t.Helper()
	if err := s.Store.PutIfAbsent(t.Context(), key, bytes.NewReader(b), int64(len(b))); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

// restoreTestDisk records what a restore writes; Size is the export size.
type restoreTestDisk struct {
	size uint64

	mu      sync.Mutex
	writes  map[uint64][]byte
	zeroes  map[uint64]uint64
	flushes int
	closed  bool
	// writeErr fails every Write; writeHook runs first (outside mu).
	writeErr  error
	writeHook func(ctx context.Context) error
}

func (d *restoreTestDisk) Size() uint64 { return d.size }

func (d *restoreTestDisk) Write(ctx context.Context, off uint64, data []byte) error {
	if d.writeHook != nil {
		if err := d.writeHook(ctx); err != nil {
			return err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.writeErr != nil {
		return d.writeErr
	}
	if _, dup := d.writes[off]; dup {
		return errors.New("offset written twice")
	}
	d.writes[off] = bytes.Clone(data)
	return nil
}

func (d *restoreTestDisk) WriteZeroes(_ context.Context, off, length uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.zeroes[off] = length
	return nil
}

func (d *restoreTestDisk) Flush(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.flushes++
	return nil
}

func (d *restoreTestDisk) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	return nil
}

// restoreEnv is a manager whose maintenance launches answer QMP
// (maintTestLauncher), with a counting store, a chunk cache and a
// recording restore disk.
type restoreEnv struct {
	h      *harness
	qmp    *maintFakeQMP
	store  *restoreTestStore
	launch *maintTestLauncher

	mu      sync.Mutex
	disks   []*restoreTestDisk
	newDisk func() *restoreTestDisk
	// exportArgs are the block-export-add arguments seen.
	exportArgs []map[string]any
}

func restoreSetup(t *testing.T, configure func(*Config)) *restoreEnv {
	t.Helper()
	fq := newMaintFakeQMP(t)
	inner := &fakeLauncher{}
	ml := &maintTestLauncher{inner: inner, qmp: fq, t: t}
	store := &restoreTestStore{Store: memstore.New()}
	env := &restoreEnv{qmp: fq, store: store, launch: ml}
	env.h = newHarness(t, harnessOptions{
		launcher: inner,
		configure: func(cfg *Config) {
			cfg.Launcher = ml
			cfg.Store = store
			cfg.ChunkCacheBytes = 64 << 20
			if configure != nil {
				configure(cfg)
			}
		},
	})
	env.h.mg.maintQuit = 200 * time.Millisecond
	fq.mu.Lock()
	fq.hook = func(cmd string, args map[string]any) (any, string, bool) {
		if cmd == "block-export-add" {
			env.mu.Lock()
			env.exportArgs = append(env.exportArgs, args)
			env.mu.Unlock()
		}
		return nil, "", false
	}
	fq.mu.Unlock()
	env.h.mg.openRestoreDisk = func(_ context.Context, opts restoreDiskOptions) (restoreDisk, error) {
		if opts.Conns != restoreWorkers || opts.PeerPID <= 0 || opts.Socket == "" {
			return nil, errors.New("unexpected restore disk options")
		}
		env.mu.Lock()
		defer env.mu.Unlock()
		d := &restoreTestDisk{size: uint64(defaultTestBaseGiB) << 30}
		if env.newDisk != nil {
			d = env.newDisk()
		}
		d.writes, d.zeroes = map[uint64][]byte{}, map[uint64]uint64{}
		env.disks = append(env.disks, d)
		return d, nil
	}
	return env
}

func (e *restoreEnv) lastDisk(t *testing.T) *restoreTestDisk {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.disks) == 0 {
		t.Fatal("no restore disk was opened")
	}
	return e.disks[len(e.disks)-1]
}

// restoreChunkData is a deterministic, non-zero chunk payload.
func restoreChunkData(seed byte) []byte {
	b := make([]byte, snapformat.ChunkSize)
	for i := range b {
		b[i] = seed + byte(i%251)
	}
	return b
}

// restoreManifest stores chunks for data and a manifest over a disk of
// diskGiB, returning its key and manifest. data maps chunk index to bytes;
// zero lists Zero entries.
func (e *restoreEnv) manifest(t *testing.T, diskGiB uint64, data map[uint64][]byte, zero []uint64, edit func(*snapformat.Manifest)) (string, *snapformat.Manifest) {
	t.Helper()
	img := e.h.mg.images[testImageID]
	m := &snapformat.Manifest{
		Version: snapformat.Version, ChainID: uuid.NewV7().String(), Generation: 3,
		SandboxID: "bx_source00", Image: testImageID, ImageSHA256: img.RootSHA256,
		DiskBytes: diskGiB << 30, ChunkSize: snapformat.ChunkSize, CreatedAt: time.Now(),
	}
	for i, d := range data {
		h, obj, err := snapformat.EncodeChunk(d)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.store.Store.Head(t.Context(), snapformat.ChunkKey(h)); errors.Is(err, objstore.ErrNotFound) {
			e.store.put(t, snapformat.ChunkKey(h), obj)
		}
		m.Chunks = append(m.Chunks, snapformat.Entry{Index: i, Hash: h})
	}
	for _, i := range zero {
		m.Chunks = append(m.Chunks, snapformat.Entry{Index: i, Zero: true})
	}
	slices.SortFunc(m.Chunks, func(a, b snapformat.Entry) int { return int(a.Index) - int(b.Index) })
	if edit != nil {
		edit(m)
	}
	key := snapformat.ManifestKey(m.ChainID, m.Generation)
	enc, err := snapformat.EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	e.store.put(t, key, enc)
	return key, m
}

func (e *restoreEnv) restore(ctx context.Context, id, key string, diskGiB uint64) (*nodev1.VM, error) {
	return e.h.mg.CreateVM(ctx, &nodev1.CreateVMRequest{
		SandboxId: id, FromManifest: key,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: diskGiB},
	})
}

// TestCreateVMFromManifestRestore restores a manifest and checks every
// listed chunk is written once at its offset, Zero entries as zeroes,
// nothing else; the session clears bbraft-dirty after a writable export of
// rootdisk; the VM is READY, hydrated, with the manifest recorded as its
// last one and no full scan pending.
func TestCreateVMFromManifestRestore(t *testing.T) {
	env := restoreSetup(t, nil)
	data := map[uint64][]byte{0: restoreChunkData(1), 5: restoreChunkData(2), 255: restoreChunkData(3), 7: restoreChunkData(1)}
	key, man := env.manifest(t, 1, data, []uint64{3, 200}, nil)

	vm, err := env.restore(t.Context(), maintTestID, key, 1)
	if err != nil {
		t.Fatalf("CreateVM from_manifest: %v", err)
	}
	if vm.State != nodev1.VMState_VM_STATE_READY || vm.FromManifest != key || vm.Image != testImageID {
		t.Fatalf("restored VM = state %s from_manifest %q image %q", vm.State, vm.FromManifest, vm.Image)
	}
	h := vm.GetHydration()
	if !h.GetHydrated() || h.GetHydratedAtUnixMs() == 0 || h.GetChunksTotal() != uint64(len(man.Chunks)) || h.GetChunksDone() != h.GetChunksTotal() {
		t.Fatalf("restored VM hydration = %+v; want hydrated with %d chunks done", h, len(man.Chunks))
	}

	d := env.lastDisk(t)
	if len(d.writes) != len(data) {
		t.Errorf("restore wrote %d chunks, want %d", len(d.writes), len(data))
	}
	for i, want := range data {
		if got := d.writes[i*snapformat.ChunkSize]; !bytes.Equal(got, want) {
			t.Errorf("chunk %d: wrong bytes at offset %d (%d bytes written)", i, i*snapformat.ChunkSize, len(got))
		}
	}
	wantZero := map[uint64]uint64{3 * snapformat.ChunkSize: snapformat.ChunkSize, 200 * snapformat.ChunkSize: snapformat.ChunkSize}
	if len(d.zeroes) != len(wantZero) {
		t.Errorf("zero writes = %v, want %v", d.zeroes, wantZero)
	}
	for off, n := range wantZero {
		if d.zeroes[off] != n {
			t.Errorf("zero write at %d = %d bytes, want %d", off, d.zeroes[off], n)
		}
	}
	if d.flushes == 0 || !d.closed {
		t.Errorf("restore disk flushes %d closed %v; want flushed and closed", d.flushes, d.closed)
	}

	env.mu.Lock()
	exports := slices.Clone(env.exportArgs)
	env.mu.Unlock()
	if len(exports) != 1 || exports[0]["node-name"] != "rootdisk" || exports[0]["writable"] != true || exports[0]["id"] != restoreExportID {
		t.Errorf("block-export-add args = %v; want one writable export of rootdisk", exports)
	}
	seen := env.qmp.seen()
	order := []string{"nbd-server-start", "block-export-add", "nbd-server-stop", "block-dirty-bitmap-clear", "quit"}
	last := -1
	for _, cmd := range order {
		i := slices.Index(seen[last+1:], cmd)
		if i < 0 {
			t.Fatalf("maintenance commands %v lack %s after position %d (want order %v)", seen, cmd, last, order)
		}
		last += i + 1
	}
	if slices.Contains(seen, "cont") {
		t.Errorf("the maintenance QEMU was continued: %v", seen)
	}
	if env.launch.maintenanceStarts() != 1 {
		t.Errorf("maintenance launches = %d, want 1", env.launch.maintenanceStarts())
	}

	gotKey, gotEnc, ok, err := env.h.mg.store.LastManifest(maintTestID)
	if err != nil || !ok || gotKey != key {
		t.Fatalf("LastManifest = %q, %v, %v; want %q", gotKey, ok, err, key)
	}
	if dec, err := snapformat.DecodeManifest(gotEnc); err != nil || dec.ChainID != man.ChainID || len(dec.Chunks) != len(man.Chunks) {
		t.Errorf("recorded last manifest = %v, %v", dec, err)
	}
	if st, err := env.h.mg.store.ReadPhase2(maintTestID); err != nil || st.FullScanNext {
		t.Errorf("phase 2 state = %+v, %v; want no full scan next", st, err)
	}
	meta, err := env.h.mg.store.ReadVM(maintTestID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.FromManifest != key || meta.RestoreChunks != uint64(len(man.Chunks)) || meta.HydratedAtUnixMs != h.GetHydratedAtUnixMs() {
		t.Errorf("persisted meta from_manifest %q restore_chunks %d hydrated_at %d", meta.FromManifest, meta.RestoreChunks, meta.HydratedAtUnixMs)
	}
	got, err := env.h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: maintTestID})
	if err != nil || got.FromManifest != key || !got.GetHydration().GetHydrated() {
		t.Errorf("GetVM = %+v, %v", got, err)
	}
}

// TestCreateVMFromManifestGrowsDisk restores a manifest onto a larger disk:
// the export is the requested size and the chunks land where the manifest
// says.
func TestCreateVMFromManifestGrowsDisk(t *testing.T) {
	env := restoreSetup(t, nil)
	env.newDisk = func() *restoreTestDisk { return &restoreTestDisk{size: 3 << 30} }
	key, _ := env.manifest(t, 1, map[uint64][]byte{4: restoreChunkData(9)}, nil, nil)
	vm, err := env.restore(t.Context(), maintTestID, key, 3)
	if err != nil {
		t.Fatalf("CreateVM from_manifest onto 3 GiB: %v", err)
	}
	if vm.GetResources().GetDiskGib() != 3 {
		t.Errorf("disk = %d GiB, want 3", vm.GetResources().GetDiskGib())
	}
	if d := env.lastDisk(t); len(d.writes) != 1 || d.writes[4*snapformat.ChunkSize] == nil {
		t.Errorf("writes at %v, want only chunk 4", slices.Collect(func(yield func(uint64) bool) {
			for off := range d.writes {
				if !yield(off) {
					return
				}
			}
		}))
	}
}

// TestCreateVMFromManifestRefusals covers loadRestore's codes, each before
// any record exists: GetVM is NOT_FOUND after every refusal and nothing was
// launched.
func TestCreateVMFromManifestRefusals(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T, e *restoreEnv) (key string, req *nodev1.CreateVMRequest)
		want    codes.Code
		wantMsg string
	}{
		{
			name: "missing manifest",
			setup: func(t *testing.T, e *restoreEnv) (string, *nodev1.CreateVMRequest) {
				return snapformat.ManifestKey(uuid.NewV7().String(), 0), nil
			},
			want: codes.NotFound, wantMsg: "not found",
		},
		{
			name: "store outage",
			setup: func(t *testing.T, e *restoreEnv) (string, *nodev1.CreateVMRequest) {
				key, _ := e.manifest(t, 1, nil, nil, nil)
				e.store.failPrefix = "manifests/"
				return key, nil
			},
			want: codes.Unavailable, wantMsg: "object store",
		},
		{
			name: "undecodable",
			setup: func(t *testing.T, e *restoreEnv) (string, *nodev1.CreateVMRequest) {
				key := snapformat.ManifestKey(uuid.NewV7().String(), 1)
				e.store.put(t, key, []byte("not a manifest"))
				return key, nil
			},
			want: codes.Internal, wantMsg: "decode manifest",
		},
		{
			name: "other key's manifest",
			setup: func(t *testing.T, e *restoreEnv) (string, *nodev1.CreateVMRequest) {
				_, m := e.manifest(t, 1, nil, nil, nil)
				enc, err := snapformat.EncodeManifest(m)
				if err != nil {
					t.Fatal(err)
				}
				key := snapformat.ManifestKey(uuid.NewV7().String(), 9)
				e.store.put(t, key, enc)
				return key, nil
			},
			want: codes.Internal, wantMsg: "describes chain",
		},
		{
			name: "other image requested",
			setup: func(t *testing.T, e *restoreEnv) (string, *nodev1.CreateVMRequest) {
				key, _ := e.manifest(t, 1, nil, nil, nil)
				return key, &nodev1.CreateVMRequest{Image: "other-v0"}
			},
			want: codes.InvalidArgument, wantMsg: "differs from the manifest",
		},
		{
			name: "image not on node",
			setup: func(t *testing.T, e *restoreEnv) (string, *nodev1.CreateVMRequest) {
				key, _ := e.manifest(t, 1, nil, nil, func(m *snapformat.Manifest) { m.Image, m.ImageSHA256 = "gone-v9", "" })
				return key, nil
			},
			want: codes.FailedPrecondition, wantMsg: "not on this node",
		},
		{
			name: "image sha differs",
			setup: func(t *testing.T, e *restoreEnv) (string, *nodev1.CreateVMRequest) {
				key, _ := e.manifest(t, 1, nil, nil, func(m *snapformat.Manifest) { m.ImageSHA256 = strings.Repeat("ab", 32) })
				return key, nil
			},
			want: codes.FailedPrecondition, wantMsg: "not the build",
		},
		{
			name: "smaller disk",
			setup: func(t *testing.T, e *restoreEnv) (string, *nodev1.CreateVMRequest) {
				key, _ := e.manifest(t, 2, nil, nil, nil)
				return key, &nodev1.CreateVMRequest{Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1}}
			},
			want: codes.InvalidArgument, wantMsg: "smaller than the source disk",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := restoreSetup(t, nil)
			key, req := tt.setup(t, env)
			if req == nil {
				req = &nodev1.CreateVMRequest{}
			}
			req.SandboxId, req.FromManifest = maintTestID, key
			if req.Resources == nil {
				req.Resources = &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1}
			}
			_, err := env.h.mg.CreateVM(t.Context(), req)
			if got := status.Code(err); got != tt.want {
				t.Fatalf("CreateVM code = %s (%v), want %s", got, err, tt.want)
			}
			if msg := status.Convert(err).Message(); !strings.Contains(msg, tt.wantMsg) {
				t.Fatalf("CreateVM message = %q, want it to contain %q", msg, tt.wantMsg)
			}
			if _, err := env.h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: maintTestID}); status.Code(err) != codes.NotFound {
				t.Fatalf("GetVM after a refused restore = %v, want NOT_FOUND", err)
			}
			if n := env.launch.inner.count(); n != 0 {
				t.Fatalf("a refused restore launched %d QEMUs", n)
			}
		})
	}
}

// TestCreateVMFromManifestFailures covers failures during the restore: the
// CreateVM code is the restore's (a missing or corrupt chunk and an NBD
// write error INTERNAL, an object store outage UNAVAILABLE) and the VM is a
// failed boot: FAILED, not hydrated, never launched as a guest, no last
// manifest recorded.
func TestCreateVMFromManifestFailures(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T, e *restoreEnv) string
		want    codes.Code
		wantMsg string
	}{
		{
			name: "corrupt chunk",
			setup: func(t *testing.T, e *restoreEnv) string {
				data := restoreChunkData(4)
				h := snapformat.HashChunk(data)
				_, obj, err := snapformat.EncodeChunk(restoreChunkData(5))
				if err != nil {
					t.Fatal(err)
				}
				e.store.put(t, snapformat.ChunkKey(h), obj) // another chunk's bytes under h
				key, _ := e.manifest(t, 1, nil, nil, func(m *snapformat.Manifest) {
					m.Chunks = []snapformat.Entry{{Index: 2, Hash: h}}
				})
				return key
			},
			want: codes.Internal, wantMsg: "corrupt chunk",
		},
		{
			name: "missing chunk",
			setup: func(t *testing.T, e *restoreEnv) string {
				key, _ := e.manifest(t, 1, nil, nil, func(m *snapformat.Manifest) {
					m.Chunks = []snapformat.Entry{{Index: 2, Hash: snapformat.HashChunk(restoreChunkData(6))}}
				})
				return key
			},
			want: codes.Internal, wantMsg: "missing from the object store",
		},
		{
			name: "store outage",
			setup: func(t *testing.T, e *restoreEnv) string {
				key, _ := e.manifest(t, 1, map[uint64][]byte{1: restoreChunkData(7)}, nil, nil)
				e.store.failPrefix = "chunks/"
				return key
			},
			want: codes.Unavailable, wantMsg: "object store",
		},
		{
			name: "nbd write error",
			setup: func(t *testing.T, e *restoreEnv) string {
				e.newDisk = func() *restoreTestDisk {
					return &restoreTestDisk{size: 1 << 30, writeErr: errors.New("nbd: write: EIO")}
				}
				key, _ := e.manifest(t, 1, map[uint64][]byte{1: restoreChunkData(8)}, nil, nil)
				return key
			},
			want: codes.Internal, wantMsg: "EIO",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := restoreSetup(t, nil)
			key := tt.setup(t, env)
			_, err := env.restore(t.Context(), maintTestID, key, 1)
			if got := status.Code(err); got != tt.want {
				t.Fatalf("CreateVM code = %s (%v), want %s", got, err, tt.want)
			}
			if msg := status.Convert(err).Message(); !strings.Contains(msg, tt.wantMsg) {
				t.Fatalf("CreateVM message = %q, want it to contain %q", msg, tt.wantMsg)
			}
			vm, err := env.h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: maintTestID})
			if err != nil || vm.State != nodev1.VMState_VM_STATE_FAILED {
				t.Fatalf("GetVM after a failed restore = %v, %v; want FAILED (a failed boot)", vm.GetState(), err)
			}
			if h := vm.GetHydration(); h.GetHydrated() || h.GetHydratedAtUnixMs() != 0 || h.GetChunksDone() != 0 {
				t.Errorf("failed restore hydration = %+v; want not hydrated", h)
			}
			if starts, maint := env.launch.inner.count(), env.launch.maintenanceStarts(); starts != maint {
				t.Errorf("%d launches, %d of them maintenance: a failed restore booted the guest", starts, maint)
			}
			if _, _, ok, _ := env.h.mg.store.LastManifest(maintTestID); ok {
				t.Error("a failed restore recorded its manifest as the last one")
			}
			// The failed record is the caller's to delete, as after any
			// failed boot.
			env.h.delete(maintTestID)
			if _, err := env.h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: maintTestID}); status.Code(err) != codes.NotFound {
				t.Fatalf("GetVM after DeleteVM = %v, want NOT_FOUND", err)
			}
		})
	}
}

// TestCreateVMFromManifestCacheHit restores one manifest twice: the second
// restore reads every chunk from the chunk cache (no chunk GET).
func TestCreateVMFromManifestCacheHit(t *testing.T) {
	env := restoreSetup(t, nil)
	key, _ := env.manifest(t, 1, map[uint64][]byte{0: restoreChunkData(10), 9: restoreChunkData(11)}, []uint64{4}, nil)
	if _, err := env.restore(t.Context(), maintTestID, key, 1); err != nil {
		t.Fatalf("first restore: %v", err)
	}
	if got := env.store.chunkGets.Load(); got != 2 {
		t.Fatalf("first restore fetched %d chunks, want 2", got)
	}
	env.h.delete(maintTestID)
	if _, err := env.restore(t.Context(), maintTestID, key, 1); err != nil {
		t.Fatalf("second restore: %v", err)
	}
	if got := env.store.chunkGets.Load(); got != 2 {
		t.Errorf("second restore fetched %d more chunks, want 0 (cache hits)", got-2)
	}
	if d := env.lastDisk(t); len(d.writes) != 2 || len(d.zeroes) != 1 {
		t.Errorf("second restore wrote %d chunks and %d zero ranges, want 2 and 1", len(d.writes), len(d.zeroes))
	}
}

// TestCreateVMFromManifestOutsideBootTimeout proves a restore longer than
// BootTimeout still boots: the boot's timeout starts after the restore.
func TestCreateVMFromManifestOutsideBootTimeout(t *testing.T) {
	env := restoreSetup(t, func(cfg *Config) { cfg.BootTimeout = 300 * time.Millisecond })
	env.newDisk = func() *restoreTestDisk {
		return &restoreTestDisk{size: 1 << 30, writeHook: func(ctx context.Context) error {
			select {
			case <-time.After(250 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
	}
	data := map[uint64][]byte{}
	for i := range uint64(restoreWorkers * 2) { // two rounds: about 500ms
		data[i] = restoreChunkData(byte(i))
	}
	key, _ := env.manifest(t, 1, data, nil, nil)
	vm, err := env.restore(t.Context(), maintTestID, key, 1)
	if err != nil || vm.State != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("restore longer than BootTimeout = %v, %v; want READY", vm.GetState(), err)
	}
}

// TestCreateVMFromManifestDeleteFences deletes a VM while its restore
// writes: the CreateVM is ABORTED and nothing is left.
func TestCreateVMFromManifestDeleteFences(t *testing.T) {
	env := restoreSetup(t, nil)
	writing := make(chan struct{})
	var once sync.Once
	env.newDisk = func() *restoreTestDisk {
		return &restoreTestDisk{size: 1 << 30, writeHook: func(ctx context.Context) error {
			once.Do(func() { close(writing) })
			<-ctx.Done()
			return ctx.Err()
		}}
	}
	key, _ := env.manifest(t, 1, map[uint64][]byte{1: restoreChunkData(12)}, nil, nil)
	errc := make(chan error, 1)
	go func() {
		_, err := env.restore(t.Context(), maintTestID, key, 1)
		errc <- err
	}()
	select {
	case <-writing:
	case <-time.After(10 * time.Second):
		t.Fatal("restore never wrote")
	}
	env.h.delete(maintTestID)
	select {
	case err := <-errc:
		if status.Code(err) != codes.Aborted {
			t.Fatalf("CreateVM during DeleteVM = %v, want ABORTED", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CreateVM did not return after DeleteVM")
	}
	if _, err := env.h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: maintTestID}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetVM after DeleteVM = %v, want NOT_FOUND", err)
	}
}

// TestHydrationOf covers VM.hydration for both kinds of VM and old records.
func TestHydrationOf(t *testing.T) {
	tests := []struct {
		name string
		meta storage.VMMeta
		want *nodev1.Hydration
	}{
		{
			name: "fresh vm booting",
			meta: storage.VMMeta{CreatedAtUnixMs: 100},
			want: &nodev1.Hydration{Hydrated: true, HydratedAtUnixMs: 100},
		},
		{
			name: "fresh vm ready",
			meta: storage.VMMeta{CreatedAtUnixMs: 100, HydratedAtUnixMs: 150},
			want: &nodev1.Hydration{Hydrated: true, HydratedAtUnixMs: 150},
		},
		{
			name: "pre phase 2 record",
			meta: storage.VMMeta{CreatedAtUnixMs: 100, StartedAtUnixMs: 900},
			want: &nodev1.Hydration{Hydrated: true, HydratedAtUnixMs: 100},
		},
		{
			name: "restore in progress or failed",
			meta: storage.VMMeta{CreatedAtUnixMs: 100, FromManifest: "manifests/x/00000000", RestoreChunks: 7},
			want: &nodev1.Hydration{ChunksTotal: 7},
		},
		{
			name: "restored and ready",
			meta: storage.VMMeta{CreatedAtUnixMs: 100, FromManifest: "manifests/x/00000000", RestoreChunks: 7, HydratedAtUnixMs: 200},
			want: &nodev1.Hydration{Hydrated: true, HydratedAtUnixMs: 200, ChunksDone: 7, ChunksTotal: 7},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hydrationOf(tt.meta)
			if got.Hydrated != tt.want.Hydrated || got.HydratedAtUnixMs != tt.want.HydratedAtUnixMs ||
				got.ChunksDone != tt.want.ChunksDone || got.ChunksTotal != tt.want.ChunksTotal {
				t.Fatalf("hydrationOf = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestCreateVMHydratedAtFirstReady: a VM not from a manifest records its
// first READY as its hydration time, and a restart of it keeps that time.
func TestCreateVMHydratedAtFirstReady(t *testing.T) {
	h := phase2Harness(t, false)
	vm := h.create(maintTestID, nil)
	hy := vm.GetHydration()
	if !hy.GetHydrated() || hy.GetHydratedAtUnixMs() != vm.StartedAtUnixMs {
		t.Fatalf("hydration = %+v, started %d; want hydrated at the first READY", hy, vm.StartedAtUnixMs)
	}
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: maintTestID, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	again, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: maintTestID})
	if err != nil {
		t.Fatalf("StartVM: %v", err)
	}
	if again.StartedAtUnixMs <= vm.StartedAtUnixMs || again.GetHydration().GetHydratedAtUnixMs() != hy.GetHydratedAtUnixMs() {
		t.Fatalf("after restart: started %d hydrated_at %d; want a new start and the first hydration time %d",
			again.StartedAtUnixMs, again.GetHydration().GetHydratedAtUnixMs(), hy.GetHydratedAtUnixMs())
	}
}
