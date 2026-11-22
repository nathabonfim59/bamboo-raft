// Package disksnap builds snapshot manifests from a sandbox's disk
// (PLAN.md §5.2, D4, D5): it reads the chunks the disk says changed, uploads
// the ones the object store does not hold yet, and writes the generation's
// manifest last.
//
// A manifest describes the whole virtual disk over its base image, not a
// delta (see snapformat): the previous manifest plus the changed chunks is
// the next one, and content addressing makes unchanged data free.
//
// The package is QEMU-free. A Disk is whatever can tell which chunks changed
// (a dirty bitmap) or are allocated (a full scan) and can read a chunk; the
// node layer supplies one and maps this package's errors to gRPC codes.
package disksnap

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// DefaultWorkers is the worker count used when Params.Workers is zero.
const DefaultWorkers = 4

// Disk is one sandbox disk as the snapshot sees it: a frozen point in time.
// Implementations must be safe for concurrent use by up to Workers
// goroutines while the snapshot runs.
type Disk interface {
	// Size is the virtual disk size in bytes.
	Size() uint64
	// Dirty yields, in ascending order and each once, the indexes of
	// chunks that may have changed since the previous manifest's cut.
	Dirty(ctx context.Context) iter.Seq2[uint64, error]
	// Allocated yields, ascending and each once, the indexes of chunks
	// with any data in the sandbox's own layer (not the base image). Used
	// for a full scan.
	Allocated(ctx context.Context) iter.Seq2[uint64, error]
	// ReadChunk reads chunk i whole (snapformat.ChunkSize bytes, shorter
	// for the last chunk) through the base image, into buf[:0] or a new
	// buffer. The returned slice is only valid until the next call.
	ReadChunk(ctx context.Context, i uint64, buf []byte) ([]byte, error)
}

// Params describes the generation to write.
type Params struct {
	// Prev is the previous generation's manifest, or nil to scan the
	// whole disk. With Full set, Prev is ignored and everything Allocated
	// is read.
	Prev *snapformat.Manifest
	// Full forces a full scan even when Prev is set (the bitmap was lost).
	Full bool
	// ChainID, Generation name the manifest object to write.
	ChainID    string
	Generation uint32
	// SandboxID is informational in the manifest.
	SandboxID string
	// Image is the base image the disk sits on; ImageSHA256 pins it.
	Image       string
	ImageSHA256 string
	// CreatedAt is the cut (freeze) time.
	CreatedAt time.Time
	// Workers is the number of concurrent chunk readers; zero means
	// DefaultWorkers.
	Workers int
}

// Stats are the Generation counters of one Snapshot call.
type Stats struct {
	// Chunks is the number of entries the manifest lists (zero chunks
	// included).
	Chunks uint64
	// ChunksUploaded is the number of chunk objects this call actually
	// put (chunks already in the store are not).
	ChunksUploaded uint64
	// BytesUploaded is the compressed size of the chunk objects this call
	// put plus the manifest object's size.
	BytesUploaded uint64
	// ContentBytes is the uncompressed size of the listed non-zero
	// chunks (the sandbox's own data, base image excluded).
	ContentBytes uint64
}

// ErrExists: the manifest key is already in the store (the node maps it to
// ALREADY_EXISTS and merges its bitmap back).
var ErrExists = errors.New("disksnap: manifest generation already exists")

// Snapshot writes generation (p.ChainID, p.Generation) to store and returns
// its manifest and counters. The manifest is written last, so any earlier
// error leaves no manifest behind; objects uploaded before the failure stay
// (they are content addressed and valid for a later call).
func Snapshot(ctx context.Context, store objstore.Store, disk Disk, p Params) (*snapformat.Manifest, Stats, error) {
	size, err := validate(store, disk, p)
	if err != nil {
		return nil, Stats{}, err
	}
	key := snapformat.ManifestKey(p.ChainID, p.Generation)
	switch _, err := store.Head(ctx, key); {
	case err == nil:
		return nil, Stats{}, fmt.Errorf("%w: %s", ErrExists, key)
	case !errors.Is(err, objstore.ErrNotFound):
		return nil, Stats{}, fmt.Errorf("disksnap: head manifest %s: %w", key, err)
	}

	numChunks := (size + snapformat.ChunkSize - 1) / snapformat.ChunkSize
	incremental := p.Prev != nil && !p.Full
	idxs, err := scanIndexes(ctx, disk, incremental, numChunks)
	if err != nil {
		return nil, Stats{}, err
	}

	up := &uploader{store: store, claimed: make(map[snapformat.Hash]struct{}, len(idxs))}
	entries, err := snapChunks(ctx, disk, up, idxs, size, p.Workers)
	if err != nil {
		return nil, Stats{}, err
	}
	if incremental {
		entries = mergeEntries(p.Prev.Chunks, idxs, entries, numChunks)
	}

	m := &snapformat.Manifest{
		Version:     snapformat.Version,
		ChainID:     p.ChainID,
		Generation:  p.Generation,
		SandboxID:   p.SandboxID,
		Image:       p.Image,
		ImageSHA256: p.ImageSHA256,
		DiskBytes:   size,
		ChunkSize:   snapformat.ChunkSize,
		CreatedAt:   p.CreatedAt.UTC(),
		Chunks:      entries,
	}
	b, err := snapformat.EncodeManifest(m)
	if err != nil {
		return nil, Stats{}, fmt.Errorf("disksnap: encode manifest %s: %w", key, err)
	}
	switch err := store.PutIfAbsent(ctx, key, bytes.NewReader(b), int64(len(b))); {
	case errors.Is(err, objstore.ErrExists):
		return nil, Stats{}, fmt.Errorf("%w: %s", ErrExists, key)
	case err != nil:
		return nil, Stats{}, fmt.Errorf("disksnap: put manifest %s: %w", key, err)
	}
	return m, Stats{
		Chunks:         uint64(len(entries)),
		ChunksUploaded: up.chunks,
		BytesUploaded:  up.bytes + uint64(len(b)),
		ContentBytes:   m.ContentBytes(),
	}, nil
}

// validate checks everything that needs no I/O and returns the disk size.
func validate(store objstore.Store, disk Disk, p Params) (uint64, error) {
	switch {
	case store == nil:
		return 0, errors.New("disksnap: no object store")
	case disk == nil:
		return 0, errors.New("disksnap: no disk")
	case !snapformat.ValidChainID(p.ChainID):
		return 0, fmt.Errorf("disksnap: bad chain id %q", p.ChainID)
	case p.Image == "":
		return 0, errors.New("disksnap: no image")
	case p.CreatedAt.IsZero():
		return 0, errors.New("disksnap: no cut time")
	case p.Workers < 0:
		return 0, fmt.Errorf("disksnap: negative worker count %d", p.Workers)
	}
	size := disk.Size()
	if size == 0 || size > snapformat.MaxDiskBytes {
		return 0, fmt.Errorf("disksnap: disk size %d out of range", size)
	}
	if p.Prev == nil || p.Full {
		// A full scan does not use Prev; a caller retrying after an
		// incremental mismatch keeps its stale copy and must succeed.
		return size, nil
	}
	if err := p.Prev.Validate(); err != nil {
		return 0, fmt.Errorf("disksnap: previous manifest: %w", err)
	}
	if p.Prev.Image != p.Image {
		return 0, fmt.Errorf("disksnap: previous manifest is on image %q, not %q", p.Prev.Image, p.Image)
	}
	// Unlisted chunks read from the base image, so an incremental generation
	// on a different base would describe a different disk.
	if p.Prev.ImageSHA256 != "" && p.ImageSHA256 != "" && p.Prev.ImageSHA256 != p.ImageSHA256 {
		return 0, fmt.Errorf("disksnap: previous manifest is on image sha256 %q, not %q", p.Prev.ImageSHA256, p.ImageSHA256)
	}
	if p.Prev.DiskBytes > size {
		return 0, fmt.Errorf("disksnap: previous manifest's disk is %d bytes, the disk is %d (disks only grow)", p.Prev.DiskBytes, size)
	}
	return size, nil
}

// scanIndexes collects the chunk indexes the disk reports, checking the
// contract (ascending, unique, inside the disk) so a buggy Disk fails loudly.
func scanIndexes(ctx context.Context, disk Disk, incremental bool, numChunks uint64) ([]uint64, error) {
	seq := disk.Allocated(ctx)
	what := "allocated"
	if incremental {
		seq = disk.Dirty(ctx)
		what = "dirty"
	}
	var idxs []uint64
	for i, err := range seq {
		if err != nil {
			return nil, fmt.Errorf("disksnap: %s chunks: %w", what, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch {
		case i >= numChunks:
			return nil, fmt.Errorf("disksnap: disk reported %s chunk %d past its %d chunks", what, i, numChunks)
		case len(idxs) > 0 && i <= idxs[len(idxs)-1]:
			return nil, fmt.Errorf("disksnap: disk reported %s chunk %d after %d (must ascend, each once)", what, i, idxs[len(idxs)-1])
		}
		idxs = append(idxs, i)
	}
	return idxs, nil
}

// snapChunks reads and classifies the chunks at idxs with a bounded worker
// pool, uploading the non-zero ones that are new. Results keep idxs' order.
func snapChunks(ctx context.Context, disk Disk, up *uploader, idxs []uint64, size uint64, workers int) ([]snapformat.Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries := make([]snapformat.Entry, len(idxs))
	if len(idxs) == 0 {
		return entries, nil
	}
	workers = min(cmp.Or(workers, DefaultWorkers), len(idxs))

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			buf := make([]byte, 0, snapformat.ChunkSize)
			for pos := range jobs {
				if ctx.Err() != nil {
					return
				}
				idx := idxs[pos]
				e, next, err := up.chunk(ctx, disk, idx, size, buf)
				if err != nil {
					cancel(fmt.Errorf("disksnap: chunk %d: %w", idx, err))
					return
				}
				entries[pos] = e
				buf = next
			}
		})
	}

dispatch:
	for pos := range idxs {
		select {
		case jobs <- pos:
		case <-ctx.Done():
			break dispatch
		}
	}
	close(jobs)
	wg.Wait()
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	return entries, nil
}

// uploader shares the per-call chunk claims and counters between workers.
type uploader struct {
	store objstore.Store

	mu      sync.Mutex
	claimed map[snapformat.Hash]struct{}
	chunks  uint64
	bytes   uint64
}

// chunk reads chunk idx through the disk, classifies it and (for a new
// non-zero chunk) uploads it. It returns the entry and the buffer to reuse.
func (u *uploader) chunk(ctx context.Context, disk Disk, idx, size uint64, buf []byte) (snapformat.Entry, []byte, error) {
	data, err := disk.ReadChunk(ctx, idx, buf[:0])
	if err != nil {
		return snapformat.Entry{}, buf, err
	}
	want := min(snapformat.ChunkSize, size-idx*snapformat.ChunkSize)
	if uint64(len(data)) != want {
		return snapformat.Entry{}, buf, fmt.Errorf("disk returned %d bytes, want %d", len(data), want)
	}
	if snapformat.IsZero(data) {
		return snapformat.Entry{Index: idx, Zero: true}, data[:0], nil
	}
	h := snapformat.HashChunk(data)
	if err := u.put(ctx, h, data); err != nil {
		return snapformat.Entry{}, buf, err
	}
	return snapformat.Entry{Index: idx, Hash: h}, data[:0], nil
}

// put uploads a chunk object unless this call already claimed its hash or
// the store already holds it. ErrExists means another writer won and counts
// as present.
func (u *uploader) put(ctx context.Context, h snapformat.Hash, data []byte) error {
	u.mu.Lock()
	_, claimed := u.claimed[h]
	u.claimed[h] = struct{}{}
	u.mu.Unlock()
	if claimed {
		return nil
	}
	key := snapformat.ChunkKey(h)
	switch _, err := u.store.Head(ctx, key); {
	case err == nil:
		return nil
	case !errors.Is(err, objstore.ErrNotFound):
		return fmt.Errorf("head chunk %s: %w", key, err)
	}
	_, obj, err := snapformat.EncodeChunk(data)
	if err != nil {
		return fmt.Errorf("encode chunk %s: %w", key, err)
	}
	switch err := u.store.PutIfAbsent(ctx, key, bytes.NewReader(obj), int64(len(obj))); {
	case errors.Is(err, objstore.ErrExists):
		return nil
	case err != nil:
		return fmt.Errorf("put chunk %s: %w", key, err)
	}
	u.mu.Lock()
	u.chunks++
	u.bytes += uint64(len(obj))
	u.mu.Unlock()
	return nil
}

// mergeEntries folds the freshly read entries at idxs (ascending) into prev
// (ascending), replacing an index prev already lists and keeping the rest.
func mergeEntries(prev []snapformat.Entry, idxs []uint64, fresh []snapformat.Entry, numChunks uint64) []snapformat.Entry {
	capHint := len(prev) + len(idxs)
	if uint64(capHint) > numChunks {
		capHint = int(numChunks)
	}
	out := make([]snapformat.Entry, 0, capHint)
	next := 0
	for _, e := range prev {
		for next < len(idxs) && idxs[next] < e.Index {
			out = append(out, fresh[next])
			next++
		}
		if next < len(idxs) && idxs[next] == e.Index {
			out = append(out, fresh[next])
			next++
			continue
		}
		out = append(out, e)
	}
	for ; next < len(idxs); next++ {
		out = append(out, fresh[next])
	}
	return out
}
