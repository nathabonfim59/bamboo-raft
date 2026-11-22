// Package chunkcache implements a bounded, content-addressed on-disk LRU cache
// for snapshot chunk objects (PLAN.md §5.2).
//
// Chunks are immutable and keyed by the SHA-256 of their uncompressed bytes
// (snapformat.Hash). Cached files hold the compressed object bytes exactly as
// stored in the object store, laid out under <Dir>/<first 2 hex>/<64 hex>.
// Every read from disk is verified with snapformat.DecodeChunk before being
// returned; a corrupt cached file is removed and refetched from the store.
//
// Cache hits update the in-memory LRU order without modifying file mtime on
// disk, so after a process restart Open rebuilds the initial LRU order from
// file mtimes (write order).
package chunkcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

const (
	// maxObjectBytes bounds a compressed chunk object read from disk or the
	// object store (a 4 MiB chunk plus framing margin). Anything larger is
	// rejected as corrupt.
	maxObjectBytes = snapformat.ChunkSize + 64<<10

	tmpDir = "tmp"
)

var errClosed = errors.New("chunkcache: cache is closed")

// Options configures a Cache opened by Open.
type Options struct {
	// Dir is the cache directory (created 0700 if missing) opened via os.OpenRoot.
	// Required when MaxBytes > 0; untouched when MaxBytes == 0.
	Dir string
	// MaxBytes caps the total bytes of cached compressed objects on disk.
	// Zero disables disk caching (pass-through mode). Negative values are rejected.
	MaxBytes int64
	// Log receives operational warnings (such as corrupt cached files).
	// If nil, log output is discarded.
	Log *slog.Logger
}

// Cache is a concurrent on-disk LRU cache of snapshot chunk objects.
// All methods are safe for concurrent use.
type Cache struct {
	root     *os.Root
	maxBytes int64
	log      *slog.Logger

	wg sync.WaitGroup

	mu            sync.Mutex
	closed        bool
	totalBytes    int64
	entries       map[snapformat.Hash]*cacheEntry
	lru           lruList
	pins          map[snapformat.Hash]int
	pendingDelete map[snapformat.Hash]struct{}
	inflight      map[snapformat.Hash]*flight
}

type cacheEntry struct {
	hash snapformat.Hash
	size int64
	prev *cacheEntry
	next *cacheEntry
}

// lruList is an intrusive doubly-linked list ordered MRU (head) to LRU (tail).
type lruList struct {
	head *cacheEntry
	tail *cacheEntry
}

func (l *lruList) pushFront(e *cacheEntry) {
	e.prev = nil
	e.next = l.head
	if l.head != nil {
		l.head.prev = e
	} else {
		l.tail = e
	}
	l.head = e
}

func (l *lruList) remove(e *cacheEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		l.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		l.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (l *lruList) moveToFront(e *cacheEntry) {
	if l.head == e {
		return
	}
	l.remove(e)
	l.pushFront(e)
}

type flight struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int // guarded by Cache.mu
	data    []byte
	err     error
}

type scannedEntry struct {
	hash    snapformat.Hash
	size    int64
	modTime time.Time
}

// Open opens or initializes a chunk cache according to opts.
//
// When opts.MaxBytes == 0, Open returns a pass-through cache without touching
// opts.Dir. Otherwise Open creates opts.Dir (0700) if missing, removes any
// leftover files in tmp/, scans existing chunk files to rebuild the in-memory
// LRU index ordered by modification time, and evicts oldest entries if the
// existing directory exceeds opts.MaxBytes.
func Open(ctx context.Context, opts Options) (*Cache, error) {
	if opts.MaxBytes < 0 {
		return nil, fmt.Errorf("chunkcache: negative max bytes %d", opts.MaxBytes)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if opts.MaxBytes == 0 {
		return &Cache{
			maxBytes: 0,
			log:      log,
			inflight: make(map[snapformat.Hash]*flight),
		}, nil
	}
	if opts.Dir == "" {
		return nil, errors.New("chunkcache: dir is required when max bytes > 0")
	}
	if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("chunkcache: create dir: %w", err)
	}
	root, err := os.OpenRoot(opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("chunkcache: open root: %w", err)
	}
	if err := cleanTmpDir(ctx, root); err != nil {
		_ = root.Close()
		return nil, err
	}
	scanned, err := scanCacheDir(ctx, root)
	if err != nil {
		_ = root.Close()
		return nil, err
	}

	c := &Cache{
		root:          root,
		maxBytes:      opts.MaxBytes,
		log:           log,
		entries:       make(map[snapformat.Hash]*cacheEntry, len(scanned)),
		pins:          make(map[snapformat.Hash]int),
		pendingDelete: make(map[snapformat.Hash]struct{}),
		inflight:      make(map[snapformat.Hash]*flight),
	}
	// Oldest mtime first so pushing each to the front leaves oldest at tail (LRU).
	slices.SortFunc(scanned, func(a, b scannedEntry) int {
		if cmp := a.modTime.Compare(b.modTime); cmp != 0 {
			return cmp
		}
		return bytes.Compare(a.hash[:], b.hash[:])
	})
	for _, s := range scanned {
		e := &cacheEntry{hash: s.hash, size: s.size}
		c.entries[s.hash] = e
		c.lru.pushFront(e)
		c.totalBytes += s.size
	}
	c.evictLocked(nil)
	return c, nil
}

// Get returns chunk h's uncompressed bytes in dst[:0] (dst may be nil),
// verified against h. On a miss, Get fetches snapformat.ChunkKey(h) from store,
// verifies it, caches the compressed object bytes if they fit within MaxBytes,
// and returns the uncompressed bytes.
//
// Concurrent Gets for the same missing hash share a single store fetch while
// respecting each caller's ctx cancellation independently.
func (c *Cache) Get(ctx context.Context, store objstore.Store, h snapformat.Hash, dst []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for attempt := range 2 {
		data, hit, err := c.tryReadCached(h, dst)
		if err != nil {
			return nil, err
		}
		if hit {
			return data, nil
		}
		f, retryDisk, err := c.acquireFlight(ctx, store, h, attempt == 0)
		if err != nil {
			return nil, err
		}
		if retryDisk {
			continue
		}
		select {
		case <-f.done:
			c.mu.Lock()
			f.waiters--
			c.mu.Unlock()
			if f.err != nil {
				return nil, f.err
			}
			return append(dst[:0], f.data...), nil
		case <-ctx.Done():
			c.mu.Lock()
			f.waiters--
			if f.waiters == 0 {
				if c.inflight[h] == f {
					delete(c.inflight, h)
				}
				f.cancel()
			}
			c.mu.Unlock()
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("chunkcache: unexpected cache retry termination")
}

// Has reports whether chunk h is currently present in the cache index.
func (c *Cache) Has(h snapformat.Hash) bool {
	c.mu.Lock()
	_, ok := c.entries[h]
	c.mu.Unlock()
	return ok
}

// Size returns the total bytes of compressed chunk objects currently cached.
func (c *Cache) Size() int64 {
	c.mu.Lock()
	n := c.totalBytes
	c.mu.Unlock()
	return n
}

// Close cancels any in-flight fetches, waits for active cache I/O to finish,
// and closes the underlying directory root. Calling Close more than once is safe.
func (c *Cache) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	for _, f := range c.inflight {
		f.cancel()
	}
	c.mu.Unlock()

	c.wg.Wait()
	if c.root != nil {
		return c.root.Close()
	}
	return nil
}

func (c *Cache) tryReadCached(h snapformat.Hash, dst []byte) ([]byte, bool, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, false, errClosed
	}
	if c.maxBytes == 0 {
		c.mu.Unlock()
		return nil, false, nil
	}
	e, ok := c.entries[h]
	if !ok {
		c.mu.Unlock()
		return nil, false, nil
	}
	c.lru.moveToFront(e)
	c.pins[h]++
	c.wg.Add(1)
	c.mu.Unlock()

	var corrupt bool
	defer func() {
		c.unpin(h, e, corrupt)
		c.wg.Done()
	}()

	_, rel := chunkPath(h)
	f, err := c.root.Open(rel)
	if err != nil {
		corrupt = true
		c.log.Warn("chunkcache: corrupt cached chunk removed", "hash", h.String())
		return nil, false, nil
	}
	obj, okRead := readCachedFile(f)
	_ = f.Close()
	if !okRead {
		corrupt = true
		c.log.Warn("chunkcache: corrupt cached chunk removed", "hash", h.String())
		return nil, false, nil
	}
	data, err := snapformat.DecodeChunk(h, obj, dst)
	if err != nil {
		corrupt = true
		c.log.Warn("chunkcache: corrupt cached chunk removed", "hash", h.String())
		return nil, false, nil
	}
	return data, true, nil
}

func (c *Cache) unpin(h snapformat.Hash, e *cacheEntry, corrupt bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if corrupt {
		if cur, exists := c.entries[h]; exists && cur == e {
			c.lru.remove(cur)
			delete(c.entries, h)
			c.totalBytes -= cur.size
			c.pendingDelete[h] = struct{}{}
		}
	}
	c.pins[h]--
	if c.pins[h] == 0 {
		delete(c.pins, h)
		if _, pending := c.pendingDelete[h]; pending {
			delete(c.pendingDelete, h)
			if !c.closed && c.entries[h] == nil {
				_, rel := chunkPath(h)
				_ = c.root.Remove(rel)
			}
		}
	}
}

func readCachedFile(f *os.File) ([]byte, bool) {
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() || fi.Size() <= 0 || fi.Size() > maxObjectBytes {
		return nil, false
	}
	obj, tooLarge, err := readBounded(f, maxObjectBytes)
	if err != nil || tooLarge || len(obj) == 0 {
		return nil, false
	}
	return obj, true
}

func (c *Cache) acquireFlight(ctx context.Context, store objstore.Store, h snapformat.Hash, allowRetryDisk bool) (*flight, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, false, errClosed
	}
	if allowRetryDisk && c.maxBytes > 0 {
		if _, ok := c.entries[h]; ok {
			return nil, true, nil
		}
	}
	if f, ok := c.inflight[h]; ok {
		f.waiters++
		return f, false, nil
	}
	if store == nil {
		return nil, false, fmt.Errorf("chunkcache: get %s: nil object store", h)
	}
	fetchCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	f := &flight{
		done:    make(chan struct{}),
		cancel:  cancel,
		waiters: 1,
	}
	c.inflight[h] = f
	c.wg.Go(func() {
		c.runFlight(fetchCtx, store, h, f)
	})
	return f, false, nil
}

func (c *Cache) runFlight(ctx context.Context, store objstore.Store, h snapformat.Hash, f *flight) {
	defer func() {
		f.cancel()
		c.mu.Lock()
		if c.inflight[h] == f {
			delete(c.inflight, h)
		}
		c.mu.Unlock()
		close(f.done)
	}()

	obj, data, err := fetchAndVerify(ctx, store, h)
	if err != nil {
		f.err = err
		return
	}
	if c.maxBytes > 0 && int64(len(obj)) <= c.maxBytes {
		c.put(h, obj)
	}
	f.data = data
}

func fetchAndVerify(ctx context.Context, store objstore.Store, h snapformat.Hash) ([]byte, []byte, error) {
	rc, err := store.Get(ctx, snapformat.ChunkKey(h), 0, -1)
	if err != nil {
		return nil, nil, fmt.Errorf("chunkcache: get %s: %w", h, err)
	}
	obj, tooLarge, readErr := readBounded(rc, maxObjectBytes)
	closeErr := rc.Close()
	if readErr != nil {
		return nil, nil, fmt.Errorf("chunkcache: get %s: %w", h, readErr)
	}
	if tooLarge {
		return nil, nil, fmt.Errorf("chunkcache: get %s: %w: object exceeds %d bytes", h, snapformat.ErrCorruptChunk, maxObjectBytes)
	}
	if closeErr != nil {
		return nil, nil, fmt.Errorf("chunkcache: get %s: %w", h, closeErr)
	}
	data, err := snapformat.DecodeChunk(h, obj, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("chunkcache: get %s: %w", h, err)
	}
	return obj, data, nil
}

func (c *Cache) put(h snapformat.Hash, obj []byte) {
	shard, rel := chunkPath(h)
	tmpRel := tmpDir + "/" + uuid.NewV7().String()

	f, err := c.root.OpenFile(tmpRel, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		c.log.Warn("chunkcache: create temp file failed", "hash", h.String(), "err", err)
		return
	}
	_, err = f.Write(obj)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = c.root.Remove(tmpRel)
		c.log.Warn("chunkcache: write temp file failed", "hash", h.String(), "err", err)
		return
	}
	if err := ensureShardDir(c.root, shard); err != nil {
		_ = c.root.Remove(tmpRel)
		c.log.Warn("chunkcache: create shard dir failed", "hash", h.String(), "err", err)
		return
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = c.root.Remove(tmpRel)
		return
	}
	if existing, ok := c.entries[h]; ok {
		c.lru.moveToFront(existing)
		c.mu.Unlock()
		_ = c.root.Remove(tmpRel)
		return
	}
	if err := c.root.Rename(tmpRel, rel); err != nil {
		c.mu.Unlock()
		_ = c.root.Remove(tmpRel)
		c.log.Warn("chunkcache: install chunk failed", "hash", h.String(), "err", err)
		return
	}
	delete(c.pendingDelete, h)
	e := &cacheEntry{hash: h, size: int64(len(obj))}
	c.entries[h] = e
	c.lru.pushFront(e)
	c.totalBytes += e.size
	c.evictLocked(e)
	c.mu.Unlock()
}

// evictLocked evicts least-recently-used entries until totalBytes <= maxBytes,
// preserving keep (if non-nil). Must be called with c.mu held.
func (c *Cache) evictLocked(keep *cacheEntry) {
	// Pass 1: evict unpinned entries so files currently being read stay untouched.
	for e := c.lru.tail; e != nil && c.totalBytes > c.maxBytes; {
		prev := e.prev
		if e != keep && c.pins[e.hash] == 0 {
			c.evictEntryLocked(e)
		}
		e = prev
	}
	// Pass 2: if still over cap because every remaining older entry is pinned,
	// remove from the index and defer unlinking the file until the last reader unpins.
	for e := c.lru.tail; e != nil && c.totalBytes > c.maxBytes; {
		prev := e.prev
		if e != keep {
			c.evictEntryLocked(e)
		}
		e = prev
	}
}

func (c *Cache) evictEntryLocked(e *cacheEntry) {
	c.lru.remove(e)
	delete(c.entries, e.hash)
	c.totalBytes -= e.size
	if c.pins[e.hash] > 0 {
		c.pendingDelete[e.hash] = struct{}{}
		return
	}
	_, rel := chunkPath(e.hash)
	if err := c.root.Remove(rel); err != nil && !errors.Is(err, fs.ErrNotExist) {
		c.log.Warn("chunkcache: remove evicted chunk failed", "hash", e.hash.String(), "err", err)
	}
}

func cleanTmpDir(ctx context.Context, root *os.Root) error {
	f, err := root.Open(tmpDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			if mkErr := root.Mkdir(tmpDir, 0o700); mkErr != nil && !errors.Is(mkErr, fs.ErrExist) {
				return fmt.Errorf("chunkcache: create %s dir: %w", tmpDir, mkErr)
			}
			return nil
		}
		return fmt.Errorf("chunkcache: open %s dir: %w", tmpDir, err)
	}
	entries, err := f.ReadDir(-1)
	_ = f.Close()
	if err != nil {
		return fmt.Errorf("chunkcache: read %s dir: %w", tmpDir, err)
	}
	for _, de := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := root.RemoveAll(tmpDir + "/" + de.Name()); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("chunkcache: remove stray temp %s: %w", de.Name(), err)
		}
	}
	return nil
}

func scanCacheDir(ctx context.Context, root *os.Root) ([]scannedEntry, error) {
	top, err := root.Open(".")
	if err != nil {
		return nil, fmt.Errorf("chunkcache: open cache dir: %w", err)
	}
	shards, err := top.ReadDir(-1)
	_ = top.Close()
	if err != nil {
		return nil, fmt.Errorf("chunkcache: read cache dir: %w", err)
	}
	var scanned []scannedEntry
	for _, sde := range shards {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		shard := sde.Name()
		if shard == tmpDir || !sde.IsDir() || !isHex2(shard) {
			continue
		}
		sd, err := root.Open(shard)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("chunkcache: open shard %s: %w", shard, err)
		}
		files, err := sd.ReadDir(-1)
		_ = sd.Close()
		if err != nil {
			return nil, fmt.Errorf("chunkcache: read shard %s: %w", shard, err)
		}
		for _, fde := range files {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			name := fde.Name()
			h, err := snapformat.ParseHash(name)
			if err != nil || name[:2] != shard {
				continue
			}
			info, err := fde.Info()
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				return nil, fmt.Errorf("chunkcache: stat %s: %w", name, err)
			}
			if !info.Mode().IsRegular() {
				continue
			}
			scanned = append(scanned, scannedEntry{
				hash:    h,
				size:    info.Size(),
				modTime: info.ModTime(),
			})
		}
	}
	return scanned, nil
}

func readBounded(r io.Reader, limit int64) ([]byte, bool, error) {
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(data)) > limit {
		return nil, true, nil
	}
	return data, false, nil
}

func ensureShardDir(root *os.Root, shard string) error {
	err := root.Mkdir(shard, 0o700)
	if err == nil {
		return nil
	}
	if !errors.Is(err, fs.ErrExist) {
		return err
	}
	fi, statErr := root.Stat(shard)
	if statErr != nil {
		return statErr
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s exists and is not a directory", shard)
	}
	return nil
}

func chunkPath(h snapformat.Hash) (shard, rel string) {
	hexStr := h.String()
	shard = hexStr[:2]
	return shard, shard + "/" + hexStr
}

func isHex2(s string) bool {
	return len(s) == 2 && isLowerHex(s[0]) && isLowerHex(s[1])
}

func isLowerHex(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f')
}
