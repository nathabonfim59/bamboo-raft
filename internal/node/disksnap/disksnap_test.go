package disksnap

// Tests for the snapshot core: a fake Disk over in-memory byte slices with a
// write log (Dirty = written since the last snapshot, Allocated = ever
// written), memstore and counting/failing store wrappers. No QEMU, no
// services; everything here runs anywhere.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

const disksnapTestImage = "ubuntu-24.04-v0"

func disksnapTestChain(t *testing.T) string {
	t.Helper()
	return uuid.NewV7().String()
}

func disksnapTestParams(chain string, gen uint32) Params {
	return Params{
		ChainID:     chain,
		Generation:  gen,
		SandboxID:   "bx_test",
		Image:       disksnapTestImage,
		ImageSHA256: "sha256:abc",
		CreatedAt:   time.Unix(1_700_000_000+int64(gen), 0),
	}
}

// TestSnapshotFullAndRestore checks a full scan of a disk whose size is not a
// multiple of ChunkSize: entries only for allocated chunks, the last partial
// chunk is the right length, and the manifest restores the disk exactly.
func TestSnapshotFullAndRestore(t *testing.T) {
	ctx := t.Context()
	size := uint64(2*snapformat.ChunkSize + 1234)
	base := disksnapTestBytes(size, 11)
	d := newDisksnapTestDisk(size, base)
	// One full chunk and the last partial one, leaving chunk 1 to the base.
	d.Write(0, disksnapTestBytes(snapformat.ChunkSize, 7))
	d.Write(2*snapformat.ChunkSize, disksnapTestBytes(1234, 3))

	store := memstore.New()
	chain := disksnapTestChain(t)
	m, stats, err := Snapshot(ctx, store, d, disksnapTestParams(chain, 1))
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if got, want := len(m.Chunks), 2; got != want {
		t.Fatalf("chunks = %d, want %d (%v)", got, want, m.Chunks)
	}
	if m.Chunks[0].Index != 0 || m.Chunks[1].Index != 2 {
		t.Fatalf("chunks = %v, want indexes 0 and 2", m.Chunks)
	}
	if got, want := m.ChunkLen(2), uint64(1234); got != want {
		t.Fatalf("last chunk length = %d, want %d", got, want)
	}
	if stats.Chunks != 2 {
		t.Errorf("stats.Chunks = %d, want 2", stats.Chunks)
	}
	wantContent := uint64(snapformat.ChunkSize + 1234)
	if stats.ContentBytes != wantContent {
		t.Errorf("stats.ContentBytes = %d, want %d", stats.ContentBytes, wantContent)
	}
	if got := disksnapTestRestore(t, ctx, store, m, base); !bytes.Equal(got, d.Bytes()) {
		t.Fatalf("restored disk differs from the original")
	}
	if _, err := store.Head(ctx, snapformat.ManifestKey(chain, 1)); err != nil {
		t.Fatalf("manifest not in store: %v", err)
	}
}

// TestSnapshotZeroChunks: all-zero chunks are listed as zero entries, get no
// object, and do not count as content.
func TestSnapshotZeroChunks(t *testing.T) {
	ctx := t.Context()
	size := 2 * uint64(snapformat.ChunkSize)
	base := disksnapTestBytes(size, 5)
	d := newDisksnapTestDisk(size, base)
	d.Write(0, make([]byte, snapformat.ChunkSize)) // overwrite the base with zeroes

	store := memstore.New()
	m, stats, err := Snapshot(ctx, store, d, disksnapTestParams(disksnapTestChain(t), 1))
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(m.Chunks) != 1 || !m.Chunks[0].Zero {
		t.Fatalf("chunks = %v, want the single written chunk as a zero entry", m.Chunks)
	}
	if stats.Chunks != 1 || stats.ChunksUploaded != 0 || stats.ContentBytes != 0 {
		t.Errorf("stats = %+v, want one zero chunk and no uploads", stats)
	}
	if stats.BytesUploaded == 0 {
		t.Errorf("stats.BytesUploaded = 0, want the manifest's size")
	}
	if got := disksnapTestRestore(t, ctx, store, m, base); !bytes.Equal(got, d.Bytes()) {
		t.Fatalf("restored disk differs from the original")
	}
}

// TestIncrementalMatchesFull is the property test: random disks and write
// sequences, an incremental snapshot every round; each manifest must list
// exactly what a full scan of the same disk lists and must restore it.
func TestIncrementalMatchesFull(t *testing.T) {
	ctx := t.Context()
	rng := rand.New(rand.NewPCG(0x5eed_5eed, 0xc0ff_ee00))
	size := uint64(3*snapformat.ChunkSize + 7777)
	base := disksnapTestBytes(size, 29)
	d := newDisksnapTestDisk(size, base)
	store := memstore.New()
	chain := disksnapTestChain(t)

	var prev *snapformat.Manifest
	for round := range 8 {
		for range 1 + rng.IntN(4) {
			off := rng.IntN(int(size))
			n := min(1+rng.IntN(2*snapformat.ChunkSize), int(size)-off)
			p := make([]byte, n)
			if rng.IntN(4) != 0 { // every fourth write is all zeroes
				for i := range p {
					p[i] = byte(rng.UintN(256))
				}
			}
			d.Write(uint64(off), p)
		}

		gen := uint32(round + 1)
		p := disksnapTestParams(chain, gen)
		p.Prev = prev
		p.Workers = 1 + rng.IntN(4)
		inc, _, err := Snapshot(ctx, store, d, p)
		if err != nil {
			t.Fatalf("round %d: incremental Snapshot: %v", round, err)
		}

		fullParams := disksnapTestParams(chain, gen+1000)
		fullParams.Full = true
		fullParams.Workers = p.Workers
		full, _, err := Snapshot(ctx, store, d, fullParams)
		if err != nil {
			t.Fatalf("round %d: full Snapshot: %v", round, err)
		}
		if !slices.Equal(inc.Chunks, full.Chunks) {
			t.Fatalf("round %d: incremental chunks %v != full-scan chunks %v", round, inc.Chunks, full.Chunks)
		}
		if inc.DiskBytes != full.DiskBytes || inc.Image != full.Image {
			t.Fatalf("round %d: manifest header %+v != %+v", round, inc, full)
		}
		if got := disksnapTestRestore(t, ctx, store, inc, base); !bytes.Equal(got, d.Bytes()) {
			t.Fatalf("round %d: restored disk differs from the original", round)
		}
		prev = inc
		d.ClearDirty()
	}
}

// TestIncrementalGrownDiskKeepsPrev: a grown disk (disks only grow) keeps the
// previous manifest's entries and records the new size.
func TestIncrementalGrownDiskKeepsPrev(t *testing.T) {
	ctx := t.Context()
	d := newDisksnapTestDisk(2*snapformat.ChunkSize, disksnapTestBytes(2*snapformat.ChunkSize, 1))
	d.Write(0, disksnapTestBytes(snapformat.ChunkSize, 2))
	store := memstore.New()
	chain := disksnapTestChain(t)
	prev, _, err := Snapshot(ctx, store, d, disksnapTestParams(chain, 1))
	if err != nil {
		t.Fatalf("first Snapshot: %v", err)
	}
	d.ClearDirty()
	d.Grow(3*snapformat.ChunkSize + 99)

	p := disksnapTestParams(chain, 2)
	p.Prev = prev
	got, _, err := Snapshot(ctx, store, d, p)
	if err != nil {
		t.Fatalf("incremental Snapshot: %v", err)
	}
	if got.DiskBytes != 3*snapformat.ChunkSize+99 {
		t.Errorf("DiskBytes = %d, want the grown size", got.DiskBytes)
	}
	if !slices.Equal(got.Chunks, prev.Chunks) {
		t.Fatalf("chunks = %v, want the previous entries %v", got.Chunks, prev.Chunks)
	}

	fullParams := disksnapTestParams(chain, 3)
	fullParams.Full = true
	full, _, err := Snapshot(ctx, store, d, fullParams)
	if err != nil {
		t.Fatalf("full Snapshot: %v", err)
	}
	if !slices.Equal(got.Chunks, full.Chunks) {
		t.Fatalf("grown incremental chunks %v != full-scan chunks %v", got.Chunks, full.Chunks)
	}
}

// TestPrevMismatch: a previous manifest from another image, a bigger disk or
// with invalid fields is rejected before any disk read or store write.
func TestPrevMismatch(t *testing.T) {
	ctx := t.Context()
	size := 2 * uint64(snapformat.ChunkSize)
	valid := func() *snapformat.Manifest {
		return &snapformat.Manifest{
			Version:     snapformat.Version,
			ChainID:     "0191b2c3-d4e5-7f60-8a9b-0c1d2e3f4a5b",
			Generation:  1,
			SandboxID:   "bx_test",
			Image:       disksnapTestImage,
			ImageSHA256: "sha256:abc",
			DiskBytes:   size,
			ChunkSize:   snapformat.ChunkSize,
			CreatedAt:   time.Unix(1_700_000_000, 0),
			Chunks:      []snapformat.Entry{{Index: 0, Zero: true}},
		}
	}
	tests := []struct {
		name   string
		mutate func(*snapformat.Manifest)
	}{
		{name: "other image", mutate: func(m *snapformat.Manifest) { m.Image = "other" }},
		{name: "other image sha256", mutate: func(m *snapformat.Manifest) { m.ImageSHA256 = "sha256:other" }},
		{name: "bigger disk", mutate: func(m *snapformat.Manifest) { m.DiskBytes = size + snapformat.ChunkSize }},
		{name: "bad chunk size", mutate: func(m *snapformat.Manifest) { m.ChunkSize = 512 }},
		{name: "bad version", mutate: func(m *snapformat.Manifest) { m.Version = 99 }},
		{name: "unsorted entries", mutate: func(m *snapformat.Manifest) {
			m.Chunks = []snapformat.Entry{{Index: 1, Zero: true}, {Index: 0, Zero: true}}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newDisksnapTestDisk(size, disksnapTestBytes(size, 1))
			store := &disksnapCountingStore{Store: memstore.New()}
			prev := valid()
			tt.mutate(prev)
			p := disksnapTestParams(prev.ChainID, 2)
			p.Prev = prev
			if _, _, err := Snapshot(ctx, store, d, p); err == nil {
				t.Fatal("Snapshot accepted a mismatched previous manifest")
			}
			if got := d.Reads(); got != 0 {
				t.Errorf("disk reads = %d, want 0", got)
			}
			if store.Puts() != 0 {
				t.Errorf("store puts = %d, want 0", store.Puts())
			}
		})
	}
}

// TestImageSHA256OneSideUnset: only two different hashes are a mismatch; an
// older manifest without the hash (or a caller not pinning it) is fine.
func TestImageSHA256OneSideUnset(t *testing.T) {
	ctx := t.Context()
	size := uint64(snapformat.ChunkSize)
	d := newDisksnapTestDisk(size, make([]byte, size))
	d.Write(0, disksnapTestBytes(size, 12))
	tests := []struct {
		name, prevSHA, paramsSHA string
	}{
		{name: "previous unset", prevSHA: "", paramsSHA: "sha256:abc"},
		{name: "params unset", prevSHA: "sha256:abc", paramsSHA: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chain := disksnapTestChain(t)
			prev := &snapformat.Manifest{
				Version:     snapformat.Version,
				ChainID:     chain,
				Generation:  1,
				SandboxID:   "bx_test",
				Image:       disksnapTestImage,
				ImageSHA256: tt.prevSHA,
				DiskBytes:   size,
				ChunkSize:   snapformat.ChunkSize,
				CreatedAt:   time.Unix(1_700_000_000, 0),
			}
			p := disksnapTestParams(chain, 2)
			p.ImageSHA256 = tt.paramsSHA
			p.Prev = prev
			if _, _, err := Snapshot(ctx, memstore.New(), d, p); err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
		})
	}
}

// TestFullIgnoresPrev: a caller retrying a mismatched incremental snapshot
// with Full keeps its stale Prev and still gets a full scan.
func TestFullIgnoresPrev(t *testing.T) {
	ctx := t.Context()
	size := 2 * uint64(snapformat.ChunkSize)
	d := newDisksnapTestDisk(size, disksnapTestBytes(size, 8))
	d.Write(0, disksnapTestBytes(snapformat.ChunkSize, 9))
	stale := disksnapTestEmptyManifest(size)
	stale.Image = "some-other-image"
	stale.DiskBytes = size + snapformat.ChunkSize
	p := disksnapTestParams(disksnapTestChain(t), 2)
	p.Prev = stale
	p.Full = true
	m, stats, err := Snapshot(ctx, memstore.New(), d, p)
	if err != nil {
		t.Fatalf("Snapshot with Full and a stale Prev: %v", err)
	}
	if len(m.Chunks) != 1 || m.Chunks[0].Index != 0 || stats.Chunks != 1 {
		t.Errorf("chunks = %v, want only the allocated chunk 0", m.Chunks)
	}
}

// TestManifestExists: a generation already in the store fails fast with
// ErrExists and touches no chunk.
func TestManifestExists(t *testing.T) {
	ctx := t.Context()
	d := newDisksnapTestDisk(snapformat.ChunkSize, disksnapTestBytes(snapformat.ChunkSize, 1))
	d.Write(0, disksnapTestBytes(snapformat.ChunkSize, 2))
	store := &disksnapCountingStore{Store: memstore.New()}
	chain := disksnapTestChain(t)
	key := snapformat.ManifestKey(chain, 1)
	if err := store.PutIfAbsent(ctx, key, strings.NewReader("{}"), 2); err != nil {
		t.Fatalf("seed manifest: %v", err)
	}
	_, _, err := Snapshot(ctx, store, d, disksnapTestParams(chain, 1))
	if !errors.Is(err, ErrExists) {
		t.Fatalf("err = %v, want ErrExists", err)
	}
	if got := d.Reads(); got != 0 {
		t.Errorf("disk reads = %d, want 0", got)
	}
	if got := store.ChunkPuts(); got != 0 {
		t.Errorf("chunk puts = %d, want 0", got)
	}
}

// TestManifestPutExists: another writer putting the manifest between our Head
// and our put is ErrExists too.
func TestManifestPutExists(t *testing.T) {
	ctx := t.Context()
	d := newDisksnapTestDisk(snapformat.ChunkSize, disksnapTestBytes(snapformat.ChunkSize, 1))
	d.Write(0, disksnapTestBytes(snapformat.ChunkSize, 2))
	store := &disksnapCountingStore{Store: memstore.New(), ManifestPutExists: true}
	_, _, err := Snapshot(ctx, store, d, disksnapTestParams(disksnapTestChain(t), 1))
	if !errors.Is(err, ErrExists) {
		t.Fatalf("err = %v, want ErrExists", err)
	}
}

// TestPutFailureNoManifest: a chunk upload failing mid-way returns the error
// and leaves no manifest behind.
func TestPutFailureNoManifest(t *testing.T) {
	ctx := t.Context()
	size := 3 * uint64(snapformat.ChunkSize)
	d := newDisksnapTestDisk(size, disksnapTestBytes(size, 1))
	for i := range 3 {
		d.Write(uint64(i)*snapformat.ChunkSize, disksnapTestBytes(snapformat.ChunkSize, byte(i+2)))
	}
	store := &disksnapCountingStore{Store: memstore.New(), FailChunkPutAt: 2}
	p := disksnapTestParams(disksnapTestChain(t), 4)
	p.Workers = 1
	_, _, err := Snapshot(ctx, store, d, p)
	if err == nil {
		t.Fatal("Snapshot succeeded despite a failing chunk put")
	}
	if !strings.Contains(err.Error(), "injected put failure") {
		t.Errorf("err = %v, want the injected failure", err)
	}
	if errors.Is(err, ErrExists) {
		t.Errorf("err = %v, must not be ErrExists", err)
	}
	key := snapformat.ManifestKey(p.ChainID, p.Generation)
	if _, err := store.Head(ctx, key); !errors.Is(err, objstore.ErrNotFound) {
		t.Errorf("manifest Head = %v, want ErrNotFound", err)
	}
	if got := store.ChunkPuts(); got != 2 {
		t.Errorf("chunk puts = %d, want 2 (the second failed)", got)
	}
}

// TestUnchangedDiskUploadsNothing: a second snapshot of an unchanged disk
// uploads no chunks and only the manifest, and never calls Delete.
func TestUnchangedDiskUploadsNothing(t *testing.T) {
	ctx := t.Context()
	size := 2 * uint64(snapformat.ChunkSize)
	d := newDisksnapTestDisk(size, disksnapTestBytes(size, 9))
	d.Write(0, disksnapTestBytes(snapformat.ChunkSize, 4))
	d.Write(snapformat.ChunkSize, disksnapTestBytes(snapformat.ChunkSize, 5))
	store := &disksnapCountingStore{Store: memstore.New()}
	chain := disksnapTestChain(t)
	prev, first, err := Snapshot(ctx, store, d, disksnapTestParams(chain, 1))
	if err != nil {
		t.Fatalf("first Snapshot: %v", err)
	}
	if first.ChunksUploaded != 2 {
		t.Fatalf("first ChunksUploaded = %d, want 2", first.ChunksUploaded)
	}
	store.Reset()
	d.ClearDirty()

	p := disksnapTestParams(chain, 2)
	p.Prev = prev
	got, second, err := Snapshot(ctx, store, d, p)
	if err != nil {
		t.Fatalf("second Snapshot: %v", err)
	}
	if second.ChunksUploaded != 0 || store.ChunkPuts() != 0 {
		t.Errorf("second snapshot uploaded %d chunks (%d puts), want 0", second.ChunksUploaded, store.ChunkPuts())
	}
	manifestKey := snapformat.ManifestKey(chain, 2)
	info, err := store.Head(ctx, manifestKey)
	if err != nil {
		t.Fatalf("manifest Head: %v", err)
	}
	if second.BytesUploaded != uint64(info.Size) {
		t.Errorf("second BytesUploaded = %d, want the manifest size %d", second.BytesUploaded, info.Size)
	}
	if !slices.Equal(got.Chunks, prev.Chunks) {
		t.Errorf("chunks changed on an unchanged disk: %v != %v", got.Chunks, prev.Chunks)
	}
	if store.Deletes() != 0 {
		t.Errorf("store.Delete called %d times; nodes never delete objects", store.Deletes())
	}
}

// TestDuplicateContentUploadsOnce: identical chunks within one call are
// uploaded once.
func TestDuplicateContentUploadsOnce(t *testing.T) {
	ctx := t.Context()
	chunk := disksnapTestBytes(snapformat.ChunkSize, 21)
	size := 2 * uint64(snapformat.ChunkSize)
	d := newDisksnapTestDisk(size, make([]byte, size))
	d.Write(0, chunk)
	d.Write(snapformat.ChunkSize, chunk) // same bytes in the next chunk
	store := &disksnapCountingStore{Store: memstore.New()}
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			store.Reset()
			store.Store = memstore.New()
			p := disksnapTestParams(disksnapTestChain(t), 1)
			p.Workers = workers
			_, stats, err := Snapshot(ctx, store, d, p)
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			if stats.ChunksUploaded != 1 || store.ChunkPuts() != 1 {
				t.Errorf("uploaded %d chunks (%d puts), want 1", stats.ChunksUploaded, store.ChunkPuts())
			}
			if stats.ContentBytes != 2*snapformat.ChunkSize {
				t.Errorf("ContentBytes = %d, want both chunks counted", stats.ContentBytes)
			}
		})
	}
}

// TestConcurrentSameHashUploadsOnce: two workers that hold the same new hash
// at the same moment upload it once — the per-call claim is an in-flight set,
// not a done set — and ChunksUploaded counts only puts that returned nil.
func TestConcurrentSameHashUploadsOnce(t *testing.T) {
	ctx := t.Context()
	chunk := disksnapTestBytes(snapformat.ChunkSize, 61)
	size := 2 * uint64(snapformat.ChunkSize)
	d := &disksnapBarrierDisk{
		disksnapTestDisk: newDisksnapTestDisk(size, make([]byte, size)),
		ready:            make(chan struct{}, 2),
		release:          make(chan struct{}),
	}
	d.Write(0, chunk)
	d.Write(snapformat.ChunkSize, chunk)

	go func() {
		for range 2 {
			<-d.ready
		}
		close(d.release)
	}()

	store := &disksnapRaceStore{Store: memstore.New()}
	p := disksnapTestParams(disksnapTestChain(t), 1)
	p.Workers = 2
	type result struct {
		stats Stats
		err   error
	}
	done := make(chan result, 1)
	go func() {
		_, stats, err := Snapshot(ctx, store, d, p)
		done <- result{stats, err}
	}()
	var got result
	select {
	case got = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Snapshot did not finish")
	}
	if got.err != nil {
		t.Fatalf("Snapshot: %v", got.err)
	}
	if n := store.ChunkSuccesses(); n != 1 {
		t.Errorf("store put %d copies of the same chunk, want 1", n)
	}
	if got.stats.ChunksUploaded != 1 {
		t.Errorf("ChunksUploaded = %d, want 1", got.stats.ChunksUploaded)
	}
	if got.stats.ChunksUploaded != uint64(store.ChunkSuccesses()) {
		t.Errorf("ChunksUploaded = %d, successful chunk puts = %d", got.stats.ChunksUploaded, store.ChunkSuccesses())
	}
}

// TestChunkExistsNotCounted: a PutIfAbsent that loses the race (ErrExists) is
// present, not uploaded.
func TestChunkExistsNotCounted(t *testing.T) {
	ctx := t.Context()
	d := newDisksnapTestDisk(snapformat.ChunkSize, make([]byte, snapformat.ChunkSize))
	d.Write(0, disksnapTestBytes(snapformat.ChunkSize, 62))
	store := &disksnapCountingStore{Store: memstore.New(), ChunkPutExists: true}
	chain := disksnapTestChain(t)
	_, stats, err := Snapshot(ctx, store, d, disksnapTestParams(chain, 1))
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if stats.ChunksUploaded != 0 {
		t.Errorf("ChunksUploaded = %d, want 0 (the put lost the race)", stats.ChunksUploaded)
	}
	info, err := store.Head(ctx, snapformat.ManifestKey(chain, 1))
	if err != nil {
		t.Fatalf("manifest Head: %v", err)
	}
	if stats.BytesUploaded != uint64(info.Size) {
		t.Errorf("BytesUploaded = %d, want only the manifest's %d bytes", stats.BytesUploaded, info.Size)
	}
}

// TestHeadSkip: a full scan does not re-upload a chunk the store already
// holds, even without a previous manifest (the fork case).
func TestHeadSkip(t *testing.T) {
	ctx := t.Context()
	size := uint64(snapformat.ChunkSize)
	chunk := disksnapTestBytes(size, 71)
	store := &disksnapCountingStore{Store: memstore.New()}

	d := newDisksnapTestDisk(size, make([]byte, size))
	d.Write(0, chunk)
	if _, stats, err := Snapshot(ctx, store, d, disksnapTestParams(disksnapTestChain(t), 1)); err != nil {
		t.Fatalf("first Snapshot: %v", err)
	} else if stats.ChunksUploaded != 1 {
		t.Fatalf("first ChunksUploaded = %d, want 1", stats.ChunksUploaded)
	}
	store.Reset()

	d = newDisksnapTestDisk(size, make([]byte, size))
	d.Write(0, chunk)
	_, stats, err := Snapshot(ctx, store, d, disksnapTestParams(disksnapTestChain(t), 1))
	if err != nil {
		t.Fatalf("second Snapshot: %v", err)
	}
	if stats.ChunksUploaded != 0 || store.ChunkPuts() != 0 {
		t.Errorf("re-uploaded a chunk already in the store: %d uploaded, %d puts", stats.ChunksUploaded, store.ChunkPuts())
	}
	if stats.BytesUploaded == 0 {
		t.Errorf("BytesUploaded = 0, want the manifest's size")
	}
}

// TestStatsExact pins the Generation counters: zero chunks are listed but not
// content, uploads are the compressed bytes put, and the manifest's own bytes
// count.
func TestStatsExact(t *testing.T) {
	ctx := t.Context()
	size := uint64(2*snapformat.ChunkSize + 100)
	base := disksnapTestBytes(size, 31)
	d := newDisksnapTestDisk(size, base)
	d.Write(0, disksnapTestBytes(snapformat.ChunkSize, 32))
	d.Write(2*snapformat.ChunkSize, disksnapTestBytes(100, 33))
	store := memstore.New()
	chain := disksnapTestChain(t)
	m, stats, err := Snapshot(ctx, store, d, disksnapTestParams(chain, 1))
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if stats.Chunks != 2 {
		t.Errorf("Chunks = %d, want 2", stats.Chunks)
	}
	if stats.ChunksUploaded != 2 {
		t.Errorf("ChunksUploaded = %d, want 2", stats.ChunksUploaded)
	}
	if want := uint64(snapformat.ChunkSize + 100); stats.ContentBytes != want {
		t.Errorf("ContentBytes = %d, want %d", stats.ContentBytes, want)
	}
	var chunkBytes int64
	for _, e := range m.Chunks {
		info, err := store.Head(ctx, snapformat.ChunkKey(e.Hash))
		if err != nil {
			t.Fatalf("chunk Head: %v", err)
		}
		chunkBytes += info.Size
	}
	mi, err := store.Head(ctx, snapformat.ManifestKey(chain, 1))
	if err != nil {
		t.Fatalf("manifest Head: %v", err)
	}
	if want := uint64(chunkBytes + mi.Size); stats.BytesUploaded != want {
		t.Errorf("BytesUploaded = %d, want %d", stats.BytesUploaded, want)
	}
}

// TestCancelMidway: cancellation while chunks are being uploaded returns the
// context's error promptly, leaves no manifest and leaves no worker running.
func TestCancelMidway(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	size := 4 * uint64(snapformat.ChunkSize)
	d := newDisksnapTestDisk(size, make([]byte, size))
	for i := range 4 {
		d.Write(uint64(i)*snapformat.ChunkSize, disksnapTestBytes(snapformat.ChunkSize, byte(i+1)))
	}
	called := make(chan struct{})
	store := &disksnapBlockingStore{Store: memstore.New(), called: called}
	p := disksnapTestParams(disksnapTestChain(t), 1)
	p.Workers = 1

	done := make(chan error, 1)
	go func() {
		_, _, err := Snapshot(ctx, store, d, p)
		done <- err
	}()
	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("snapshot never reached the store")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Snapshot did not return after cancellation")
	}
	if _, err := store.Head(t.Context(), snapformat.ManifestKey(p.ChainID, p.Generation)); !errors.Is(err, objstore.ErrNotFound) {
		t.Errorf("manifest Head = %v, want ErrNotFound", err)
	}
	if got := store.Deletes(); got != 0 {
		t.Errorf("store.Delete called %d times", got)
	}
}

// TestDiskContractErrors: a Disk that breaks its contract fails the snapshot
// loudly (it is a bug, not something to drop silently).
func TestDiskContractErrors(t *testing.T) {
	ctx := t.Context()
	size := 2 * uint64(snapformat.ChunkSize)
	tests := []struct {
		name string
		disk Disk
		full bool
	}{
		{
			name: "dirty past the disk",
			disk: &disksnapSeqDisk{
				disksnapTestDisk: newDisksnapTestDisk(size, make([]byte, size)),
				dirty:            func(context.Context) iter.Seq2[uint64, error] { return disksnapTestSeq(2) },
			},
		},
		{
			name: "dirty not ascending",
			disk: &disksnapSeqDisk{
				disksnapTestDisk: newDisksnapTestDisk(size, make([]byte, size)),
				dirty:            func(context.Context) iter.Seq2[uint64, error] { return disksnapTestSeq(1, 0) },
			},
		},
		{
			name: "dirty repeated",
			disk: &disksnapSeqDisk{
				disksnapTestDisk: newDisksnapTestDisk(size, make([]byte, size)),
				dirty:            func(context.Context) iter.Seq2[uint64, error] { return disksnapTestSeq(1, 1) },
			},
		},
		{
			name: "allocated past the disk",
			disk: &disksnapSeqDisk{
				disksnapTestDisk: newDisksnapTestDisk(size, make([]byte, size)),
				allocated:        func(context.Context) iter.Seq2[uint64, error] { return disksnapTestSeq(2) },
			},
			full: true,
		},
		{
			name: "dirty iterator error",
			disk: &disksnapSeqDisk{
				disksnapTestDisk: newDisksnapTestDisk(size, make([]byte, size)),
				dirty: func(context.Context) iter.Seq2[uint64, error] {
					return func(yield func(uint64, error) bool) {
						yield(0, errors.New("bitmap gone"))
					}
				},
			},
		},
		{
			name: "short chunk",
			disk: &disksnapSeqDisk{
				disksnapTestDisk: newDisksnapTestDisk(size, make([]byte, size)),
				allocated:        func(context.Context) iter.Seq2[uint64, error] { return disksnapTestSeq(0) },
				readChunk:        func(context.Context, uint64, []byte) ([]byte, error) { return []byte{1}, nil },
			},
			full: true,
		},
		{
			name: "read error",
			disk: &disksnapSeqDisk{
				disksnapTestDisk: newDisksnapTestDisk(size, make([]byte, size)),
				allocated:        func(context.Context) iter.Seq2[uint64, error] { return disksnapTestSeq(0) },
				readChunk: func(context.Context, uint64, []byte) ([]byte, error) {
					return nil, errors.New("nbd: connection lost")
				},
			},
			full: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := memstore.New()
			p := disksnapTestParams(disksnapTestChain(t), 1)
			p.Full = tt.full
			p.Prev = disksnapTestEmptyManifest(size)
			if _, _, err := Snapshot(ctx, store, tt.disk, p); err == nil {
				t.Fatal("Snapshot accepted a broken Disk")
			}
		})
	}
}

// TestInvalidParams: bad arguments are rejected before any I/O.
func TestInvalidParams(t *testing.T) {
	ctx := t.Context()
	valid := disksnapTestParams(disksnapTestChain(t), 1)
	disk := newDisksnapTestDisk(snapformat.ChunkSize, make([]byte, snapformat.ChunkSize))
	tests := []struct {
		name  string
		store objstore.Store
		disk  Disk
		p     Params
	}{
		{name: "no store", store: nil, disk: disk, p: valid},
		{name: "no disk", store: memstore.New(), disk: nil, p: valid},
		{name: "bad chain", store: memstore.New(), disk: disk, p: func() Params { p := valid; p.ChainID = "nope"; return p }()},
		{name: "no image", store: memstore.New(), disk: disk, p: func() Params { p := valid; p.Image = ""; return p }()},
		{name: "no cut time", store: memstore.New(), disk: disk, p: func() Params { p := valid; p.CreatedAt = time.Time{}; return p }()},
		{name: "negative workers", store: memstore.New(), disk: disk, p: func() Params { p := valid; p.Workers = -1; return p }()},
		{name: "zero disk", store: memstore.New(), disk: newDisksnapTestDisk(0, nil), p: valid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := Snapshot(ctx, tt.store, tt.disk, tt.p); err == nil {
				t.Fatal("Snapshot accepted invalid params")
			}
		})
	}
}

// TestHeadErrorPropagates: a store error while checking for an existing
// manifest is not mistaken for "absent" or for ErrExists.
func TestHeadErrorPropagates(t *testing.T) {
	ctx := t.Context()
	d := newDisksnapTestDisk(snapformat.ChunkSize, make([]byte, snapformat.ChunkSize))
	store := &disksnapHeadErrorStore{Store: memstore.New(), err: errors.New("s3: 503")}
	_, _, err := Snapshot(ctx, store, d, disksnapTestParams(disksnapTestChain(t), 1))
	if err == nil || !strings.Contains(err.Error(), "s3: 503") {
		t.Fatalf("err = %v, want the store's error", err)
	}
	if errors.Is(err, ErrExists) {
		t.Errorf("err = %v, must not be ErrExists", err)
	}
}

// TestWorkersBound: a large Workers value is capped by the number of chunks
// and still produces the right manifest.
func TestWorkersBound(t *testing.T) {
	ctx := t.Context()
	size := 2 * uint64(snapformat.ChunkSize)
	d := newDisksnapTestDisk(size, disksnapTestBytes(size, 41))
	for i := range 2 {
		d.Write(uint64(i)*snapformat.ChunkSize, disksnapTestBytes(snapformat.ChunkSize, byte(i+50)))
	}
	store := memstore.New()
	p := disksnapTestParams(disksnapTestChain(t), 1)
	p.Workers = 1000
	m, stats, err := Snapshot(ctx, store, d, p)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if stats.ChunksUploaded != 2 || len(m.Chunks) != 2 {
		t.Errorf("chunks = %d uploaded %d, want 2 and 2", len(m.Chunks), stats.ChunksUploaded)
	}
}

// --- helpers -----------------------------------------------------------

// disksnapTestDisk is an in-memory Disk. Writes mark chunks dirty and
// allocated; Dirty yields the dirty ones, Allocated every allocated one.
type disksnapTestDisk struct {
	mu        sync.Mutex
	data      []byte
	allocated map[uint64]struct{}
	dirty     map[uint64]struct{}
	reads     atomic.Int64
}

func newDisksnapTestDisk(size uint64, base []byte) *disksnapTestDisk {
	data := make([]byte, size)
	copy(data, base)
	return &disksnapTestDisk{
		data:      data,
		allocated: make(map[uint64]struct{}),
		dirty:     make(map[uint64]struct{}),
	}
}

func (d *disksnapTestDisk) Size() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return uint64(len(d.data))
}

func (d *disksnapTestDisk) Dirty(ctx context.Context) iter.Seq2[uint64, error] {
	d.mu.Lock()
	idx := slices.Sorted(maps.Keys(d.dirty))
	d.mu.Unlock()
	return disksnapTestIndexes(ctx, idx)
}

func (d *disksnapTestDisk) Allocated(ctx context.Context) iter.Seq2[uint64, error] {
	d.mu.Lock()
	idx := slices.Sorted(maps.Keys(d.allocated))
	d.mu.Unlock()
	return disksnapTestIndexes(ctx, idx)
}

func (d *disksnapTestDisk) ReadChunk(ctx context.Context, i uint64, buf []byte) ([]byte, error) {
	d.reads.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	start := i * snapformat.ChunkSize
	if start >= uint64(len(d.data)) {
		return nil, fmt.Errorf("chunk %d past the %d byte disk", i, len(d.data))
	}
	end := min(start+snapformat.ChunkSize, uint64(len(d.data)))
	return append(buf[:0], d.data[start:end]...), nil
}

// Write copies p at off and marks the covered chunks dirty and allocated.
func (d *disksnapTestDisk) Write(off uint64, p []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	copy(d.data[off:], p)
	for i := off / snapformat.ChunkSize; i <= min((off+uint64(len(p))-1)/snapformat.ChunkSize, uint64(len(d.data)-1)/snapformat.ChunkSize); i++ {
		d.dirty[i] = struct{}{}
		d.allocated[i] = struct{}{}
	}
}

// Grow extends the disk with zeroes.
func (d *disksnapTestDisk) Grow(size uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.data = append(d.data, make([]byte, size-uint64(len(d.data)))...)
}

// ClearDirty forgets the writes, as a bitmap reset after a cut would.
func (d *disksnapTestDisk) ClearDirty() {
	d.mu.Lock()
	defer d.mu.Unlock()
	clear(d.dirty)
}

// Bytes returns a copy of the current disk contents.
func (d *disksnapTestDisk) Bytes() []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.data)
}

// Reads is the number of ReadChunk calls so far.
func (d *disksnapTestDisk) Reads() int64 { return d.reads.Load() }

// disksnapSeqDisk lets a test replace Dirty, Allocated or ReadChunk.
type disksnapSeqDisk struct {
	*disksnapTestDisk
	dirty     func(context.Context) iter.Seq2[uint64, error]
	allocated func(context.Context) iter.Seq2[uint64, error]
	readChunk func(context.Context, uint64, []byte) ([]byte, error)
}

func (d *disksnapSeqDisk) Dirty(ctx context.Context) iter.Seq2[uint64, error] {
	if d.dirty != nil {
		return d.dirty(ctx)
	}
	return d.disksnapTestDisk.Dirty(ctx)
}

func (d *disksnapSeqDisk) Allocated(ctx context.Context) iter.Seq2[uint64, error] {
	if d.allocated != nil {
		return d.allocated(ctx)
	}
	return d.disksnapTestDisk.Allocated(ctx)
}

func (d *disksnapSeqDisk) ReadChunk(ctx context.Context, i uint64, buf []byte) ([]byte, error) {
	if d.readChunk != nil {
		return d.readChunk(ctx, i, buf)
	}
	return d.disksnapTestDisk.ReadChunk(ctx, i, buf)
}

// disksnapShortDisk returns one byte for every chunk.

func disksnapTestIndexes(ctx context.Context, idx []uint64) iter.Seq2[uint64, error] {
	return func(yield func(uint64, error) bool) {
		for _, i := range idx {
			if err := ctx.Err(); err != nil {
				yield(0, err)
				return
			}
			if !yield(i, nil) {
				return
			}
		}
	}
}

func disksnapTestSeq(idx ...uint64) iter.Seq2[uint64, error] {
	return func(yield func(uint64, error) bool) {
		for _, i := range idx {
			if !yield(i, nil) {
				return
			}
		}
	}
}

func disksnapTestEmptyManifest(size uint64) *snapformat.Manifest {
	return &snapformat.Manifest{
		Version:    snapformat.Version,
		ChainID:    "0191b2c3-d4e5-7f60-8a9b-0c1d2e3f4a5b",
		Generation: 0,
		SandboxID:  "bx_test",
		Image:      disksnapTestImage,
		DiskBytes:  size,
		ChunkSize:  snapformat.ChunkSize,
		CreatedAt:  time.Unix(1_700_000_000, 0),
	}
}

// disksnapBarrierDisk makes the first two ReadChunk calls wait until both
// finished reading, so both chunks are in flight at the same time.
type disksnapBarrierDisk struct {
	*disksnapTestDisk
	ready   chan struct{}
	release chan struct{}
}

func (d *disksnapBarrierDisk) ReadChunk(ctx context.Context, i uint64, buf []byte) ([]byte, error) {
	data, err := d.disksnapTestDisk.ReadChunk(ctx, i, buf)
	if err != nil {
		return nil, err
	}
	d.ready <- struct{}{}
	select {
	case <-d.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return data, nil
}

// disksnapRaceStore accepts concurrent puts of one key and counts how many
// succeeded, like a service without conditional writes: a missing in-flight
// claim shows up as two successes.
type disksnapRaceStore struct {
	*memstore.Store

	mu      sync.Mutex
	chunks  int
	objects map[string][]byte
}

func (s *disksnapRaceStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	chunk := strings.HasPrefix(key, snapformat.ChunkPrefix)
	if chunk {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	data, err := io.ReadAll(io.LimitReader(r, size+1))
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return fmt.Errorf("race store: put %s: read %d bytes, want %d", key, len(data), size)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objects == nil {
		s.objects = make(map[string][]byte)
	}
	s.objects[key] = data
	if chunk {
		s.chunks++
	}
	return nil
}

func (s *disksnapRaceStore) Head(_ context.Context, key string) (objstore.Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return objstore.Info{}, objstore.ErrNotFound
	}
	return objstore.Info{Key: key, Size: int64(len(data))}, nil
}

// ChunkSuccesses is the number of chunk puts that returned nil.
func (s *disksnapRaceStore) ChunkSuccesses() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chunks
}

// disksnapTestBytes returns n bytes of a deterministic non-zero pattern.
func disksnapTestBytes(n uint64, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*131 + int(seed)*17) % 251)
	}
	return b
}

// disksnapTestRestore rebuilds a disk from the base image and a manifest,
// reading (and verifying) every chunk object.
func disksnapTestRestore(t *testing.T, ctx context.Context, store objstore.Store, m *snapformat.Manifest, base []byte) []byte {
	t.Helper()
	out := slices.Clone(base[:m.DiskBytes])
	buf := make([]byte, 0, snapformat.ChunkSize)
	for _, e := range m.Chunks {
		start := e.Index * snapformat.ChunkSize
		n := m.ChunkLen(e.Index)
		if e.Zero {
			clear(out[start : start+n])
			continue
		}
		rc, err := store.Get(ctx, snapformat.ChunkKey(e.Hash), 0, -1)
		if err != nil {
			t.Fatalf("get chunk %d: %v", e.Index, err)
		}
		obj, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read chunk %d: %v", e.Index, err)
		}
		data, err := snapformat.DecodeChunk(e.Hash, obj, buf)
		buf = data[:0]
		if err != nil {
			t.Fatalf("decode chunk %d: %v", e.Index, err)
		}
		if uint64(len(data)) != n {
			t.Fatalf("chunk %d is %d bytes, want %d", e.Index, len(data), n)
		}
		copy(out[start:start+n], data)
	}
	return out
}

// disksnapCountingStore counts puts and deletes, and injects failures.
type disksnapCountingStore struct {
	objstore.Store
	ManifestPutExists bool
	ChunkPutExists    bool
	FailChunkPutAt    int // 1-based chunk PutIfAbsent call that fails; 0 = off

	mu        sync.Mutex
	puts      map[string]int
	chunkPuts int
	deletes   int
}

func (s *disksnapCountingStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	chunk := strings.HasPrefix(key, snapformat.ChunkPrefix)
	s.mu.Lock()
	if s.puts == nil {
		s.puts = make(map[string]int)
	}
	s.puts[key]++
	if chunk {
		s.chunkPuts++
	}
	fail := chunk && s.FailChunkPutAt > 0 && s.chunkPuts == s.FailChunkPutAt
	exists := chunk && s.ChunkPutExists || !chunk && s.ManifestPutExists
	s.mu.Unlock()
	switch {
	case fail:
		return fmt.Errorf("injected put failure for %s", key)
	case exists:
		return objstore.ErrExists
	}
	return s.Store.PutIfAbsent(ctx, key, r, size)
}

func (s *disksnapCountingStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	s.deletes++
	s.mu.Unlock()
	return s.Store.Delete(ctx, key)
}

// Reset forgets the counters (not the objects).
func (s *disksnapCountingStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.puts)
	s.chunkPuts, s.deletes = 0, 0
}

func (s *disksnapCountingStore) Puts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.puts)
}

func (s *disksnapCountingStore) ChunkPuts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chunkPuts
}

func (s *disksnapCountingStore) Deletes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deletes
}

// disksnapBlockingStore blocks the first chunk put until its context ends.
type disksnapBlockingStore struct {
	objstore.Store
	called chan struct{}
	once   sync.Once

	mu      sync.Mutex
	deletes int
}

func (s *disksnapBlockingStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	if strings.HasPrefix(key, snapformat.ChunkPrefix) {
		s.once.Do(func() { close(s.called) })
		<-ctx.Done()
		return ctx.Err()
	}
	return s.Store.PutIfAbsent(ctx, key, r, size)
}

func (s *disksnapBlockingStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	s.deletes++
	s.mu.Unlock()
	return s.Store.Delete(ctx, key)
}

func (s *disksnapBlockingStore) Deletes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deletes
}

// disksnapHeadErrorStore fails every Head.
type disksnapHeadErrorStore struct {
	objstore.Store
	err error
}

func (s *disksnapHeadErrorStore) Head(context.Context, string) (objstore.Info, error) {
	return objstore.Info{}, s.err
}
