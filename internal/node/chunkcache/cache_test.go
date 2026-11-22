package chunkcache

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// countingStore wraps an objstore.Store and counts Get calls, with optional
// per-Get hook for concurrency and fault-injection tests.
type countingStore struct {
	base  objstore.Store
	gets  atomic.Int64
	onGet func(ctx context.Context, key string) (io.ReadCloser, error, bool)
}

func newCountingStore() (*countingStore, *memstore.Store) {
	m := memstore.New()
	return &countingStore{base: m}, m
}

func (s *countingStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	return s.base.PutIfAbsent(ctx, key, r, size)
}

func (s *countingStore) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	s.gets.Add(1)
	if s.onGet != nil {
		if rc, err, handled := s.onGet(ctx, key); handled {
			return rc, err
		}
	}
	return s.base.Get(ctx, key, offset, length)
}

func (s *countingStore) Head(ctx context.Context, key string) (objstore.Info, error) {
	return s.base.Head(ctx, key)
}

func (s *countingStore) Delete(ctx context.Context, key string) error {
	return s.base.Delete(ctx, key)
}

func (s *countingStore) List(ctx context.Context, prefix string) iter.Seq2[objstore.Info, error] {
	return s.base.List(ctx, prefix)
}

func (s *countingStore) Presign(ctx context.Context, method, key string, expires time.Duration) (string, error) {
	return s.base.Presign(ctx, method, key, expires)
}

type testChunk struct {
	hash snapformat.Hash
	raw  []byte
	obj  []byte
}

// makeChunk creates a random incompressible chunk of size n and stores it in m.
func makeChunk(t *testing.T, m *memstore.Store, n int) testChunk {
	t.Helper()
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	h, obj, err := snapformat.EncodeChunk(raw)
	if err != nil {
		t.Fatalf("EncodeChunk: %v", err)
	}
	if err := m.PutIfAbsent(t.Context(), snapformat.ChunkKey(h), bytes.NewReader(obj), int64(len(obj))); err != nil {
		t.Fatalf("PutIfAbsent: %v", err)
	}
	return testChunk{hash: h, raw: raw, obj: obj}
}

func openTestCache(t *testing.T, opts Options) *Cache {
	t.Helper()
	c, err := Open(t.Context(), opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

func TestGetMissThenHit(t *testing.T) {
	cs, ms := newCountingStore()
	ch := makeChunk(t, ms, 8<<10)
	dir := filepath.Join(t.TempDir(), "chunks")

	c := openTestCache(t, Options{Dir: dir, MaxBytes: 1 << 20})

	if c.Has(ch.hash) {
		t.Fatalf("Has(%s) = true before Get, want false", ch.hash)
	}
	if got := c.Size(); got != 0 {
		t.Fatalf("Size() = %d before Get, want 0", got)
	}

	// First Get: miss -> fetches from store, reuses dst[:0], and caches on disk.
	missDst := make([]byte, 12, len(ch.raw)+64)
	got, err := c.Get(t.Context(), cs, ch.hash, missDst)
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	if !bytes.Equal(got, ch.raw) {
		t.Fatalf("first Get bytes mismatch")
	}
	if &got[0] != &missDst[0] {
		t.Fatalf("first Get did not reuse dst[:0] backing array")
	}
	if cs.gets.Load() != 1 {
		t.Fatalf("store Get count = %d, want 1", cs.gets.Load())
	}
	if !c.Has(ch.hash) {
		t.Fatalf("Has(%s) = false after Get, want true", ch.hash)
	}
	if c.Size() != int64(len(ch.obj)) {
		t.Fatalf("Size() = %d, want %d", c.Size(), len(ch.obj))
	}

	// Verify on-disk layout <Dir>/<2hex>/<64hex> holds exact compressed object.
	hexStr := ch.hash.String()
	diskPath := filepath.Join(dir, hexStr[:2], hexStr)
	onDisk, err := os.ReadFile(diskPath)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", diskPath, err)
	}
	if !bytes.Equal(onDisk, ch.obj) {
		t.Fatalf("cached file bytes do not match compressed object")
	}
	tmpEntries, err := os.ReadDir(filepath.Join(dir, "tmp"))
	if err != nil {
		t.Fatalf("ReadDir(tmp): %v", err)
	}
	if len(tmpEntries) != 0 {
		t.Fatalf("tmp dir has %d leftover entries, want 0", len(tmpEntries))
	}

	// Second Get: hit -> no store call, reuses dst[:0] backing array.
	dst := make([]byte, 16, len(ch.raw)+64)
	for i := range dst {
		dst[i] = 0xAA
	}
	got2, err := c.Get(t.Context(), nil, ch.hash, dst)
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if !bytes.Equal(got2, ch.raw) {
		t.Fatalf("second Get bytes mismatch")
	}
	if &got2[0] != &dst[0] {
		t.Fatalf("second Get did not reuse dst[:0] backing array")
	}
	if cs.gets.Load() != 1 {
		t.Fatalf("store Get count = %d after cache hit, want 1", cs.gets.Load())
	}
}

func TestSingleflight(t *testing.T) {
	cs, ms := newCountingStore()
	ch := makeChunk(t, ms, 16<<10)

	started := make(chan struct{})
	release := make(chan struct{})
	cs.onGet = func(ctx context.Context, key string) (io.ReadCloser, error, bool) {
		close(started)
		<-release
		return nil, nil, false
	}

	c := openTestCache(t, Options{Dir: t.TempDir(), MaxBytes: 1 << 20})

	const n = 32
	results := make([][]byte, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			got, err := c.Get(t.Context(), cs, ch.hash, nil)
			if err == nil {
				// Verify returned slice is isolated per caller (mutating does not race).
				cp := bytes.Clone(got)
				got[0] ^= 0xff
				got = cp
			}
			results[i] = got
			errs[i] = err
		})
	}

	<-started
	// Wait until all other goroutines have joined the flight.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		f := c.inflight[ch.hash]
		waiters := 0
		if f != nil {
			waiters = f.waiters
		}
		c.mu.Unlock()
		if waiters == n {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()

	if got := cs.gets.Load(); got != 1 {
		t.Fatalf("store Get count = %d, want 1", got)
	}
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("goroutine %d error: %v", i, errs[i])
		}
		if !bytes.Equal(results[i], ch.raw) {
			t.Fatalf("goroutine %d bytes mismatch", i)
		}
	}
	if !c.Has(ch.hash) || c.Size() != int64(len(ch.obj)) {
		t.Fatalf("post-singleflight cache state: Has=%v Size=%d want true/%d", c.Has(ch.hash), c.Size(), len(ch.obj))
	}
}

func TestSingleflightContextCancel(t *testing.T) {
	t.Run("waiter_cancels", func(t *testing.T) {
		cs, ms := newCountingStore()
		ch := makeChunk(t, ms, 4<<10)

		started := make(chan struct{})
		release := make(chan struct{})
		cs.onGet = func(ctx context.Context, key string) (io.ReadCloser, error, bool) {
			close(started)
			select {
			case <-release:
				return nil, nil, false
			case <-ctx.Done():
				return nil, ctx.Err(), true
			}
		}

		c := openTestCache(t, Options{Dir: t.TempDir(), MaxBytes: 1 << 20})

		var g1Data []byte
		var g1Err error
		var wg sync.WaitGroup
		wg.Go(func() {
			g1Data, g1Err = c.Get(t.Context(), cs, ch.hash, nil)
		})
		<-started

		ctx2, cancel2 := context.WithCancel(t.Context())
		g2Done := make(chan error, 1)
		wg.Go(func() {
			_, err := c.Get(ctx2, cs, ch.hash, nil)
			g2Done <- err
		})
		waitForWaiters(t, c, ch.hash, 2)

		cancel2()
		select {
		case err := <-g2Done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("waiter err = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("cancelled waiter did not return promptly")
		}

		close(release)
		wg.Wait()
		if g1Err != nil {
			t.Fatalf("initiator Get failed: %v", g1Err)
		}
		if !bytes.Equal(g1Data, ch.raw) {
			t.Fatalf("initiator data mismatch")
		}
		if cs.gets.Load() != 1 {
			t.Fatalf("store Get count = %d, want 1", cs.gets.Load())
		}
	})

	t.Run("initiator_cancels_waiter_survives", func(t *testing.T) {
		cs, ms := newCountingStore()
		ch := makeChunk(t, ms, 4<<10)

		started := make(chan struct{})
		release := make(chan struct{})
		cs.onGet = func(ctx context.Context, key string) (io.ReadCloser, error, bool) {
			close(started)
			select {
			case <-release:
				return nil, nil, false
			case <-ctx.Done():
				return nil, ctx.Err(), true
			}
		}

		c := openTestCache(t, Options{Dir: t.TempDir(), MaxBytes: 1 << 20})

		ctx1, cancel1 := context.WithCancel(t.Context())
		g1Done := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			_, err := c.Get(ctx1, cs, ch.hash, nil)
			g1Done <- err
		})
		<-started

		var g2Data []byte
		var g2Err error
		wg.Go(func() {
			g2Data, g2Err = c.Get(t.Context(), cs, ch.hash, nil)
		})
		waitForWaiters(t, c, ch.hash, 2)

		cancel1()
		select {
		case err := <-g1Done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("initiator err = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("cancelled initiator did not return promptly")
		}

		close(release)
		wg.Wait()
		if g2Err != nil {
			t.Fatalf("surviving waiter Get failed: %v", g2Err)
		}
		if !bytes.Equal(g2Data, ch.raw) {
			t.Fatalf("surviving waiter data mismatch")
		}
		if cs.gets.Load() != 1 {
			t.Fatalf("store Get count = %d, want 1", cs.gets.Load())
		}
	})

	t.Run("all_cancel_then_fresh_succeeds", func(t *testing.T) {
		cs, ms := newCountingStore()
		ch := makeChunk(t, ms, 4<<10)

		firstStarted := make(chan struct{})
		firstCanceled := make(chan struct{})
		var calls atomic.Int32
		cs.onGet = func(ctx context.Context, key string) (io.ReadCloser, error, bool) {
			if calls.Add(1) == 1 {
				close(firstStarted)
				<-ctx.Done()
				close(firstCanceled)
				return nil, ctx.Err(), true
			}
			return nil, nil, false
		}

		c := openTestCache(t, Options{Dir: t.TempDir(), MaxBytes: 1 << 20})

		ctx1, cancel1 := context.WithCancel(t.Context())
		g1Done := make(chan error, 1)
		go func() {
			_, err := c.Get(ctx1, cs, ch.hash, nil)
			g1Done <- err
		}()
		<-firstStarted
		cancel1()
		if err := <-g1Done; !errors.Is(err, context.Canceled) {
			t.Fatalf("first Get err = %v, want context.Canceled", err)
		}

		// Immediately start a fresh Get; it must not inherit the cancelled flight.
		got, err := c.Get(t.Context(), cs, ch.hash, nil)
		if err != nil {
			t.Fatalf("fresh Get failed: %v", err)
		}
		if !bytes.Equal(got, ch.raw) {
			t.Fatalf("fresh Get data mismatch")
		}
		select {
		case <-firstCanceled:
		case <-time.After(2 * time.Second):
			t.Fatal("abandoned flight context was not cancelled")
		}
	})
}

func waitForWaiters(t *testing.T, c *Cache, h snapformat.Hash, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		f := c.inflight[h]
		got := 0
		if f != nil {
			got = f.waiters
		}
		c.mu.Unlock()
		if got >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d waiters on %s", want, h)
}

func TestCapAndLRU(t *testing.T) {
	cs, ms := newCountingStore()
	cA := makeChunk(t, ms, 8<<10)
	cB := makeChunk(t, ms, 8<<10)
	cC := makeChunk(t, ms, 8<<10)
	cBig := makeChunk(t, ms, 32<<10)

	// Cap fits any 2 of {A, B, C}, not 3, and cannot fit cBig.
	maxBytes := int64(len(cA.obj)) + int64(len(cB.obj)) + int64(len(cC.obj))/2
	if maxBytes >= int64(len(cBig.obj)) {
		t.Fatalf("test setup: maxBytes %d >= big chunk %d", maxBytes, len(cBig.obj))
	}
	dir := t.TempDir()
	c := openTestCache(t, Options{Dir: dir, MaxBytes: maxBytes})

	if _, err := c.Get(t.Context(), cs, cA.hash, nil); err != nil {
		t.Fatalf("Get A: %v", err)
	}
	if _, err := c.Get(t.Context(), cs, cB.hash, nil); err != nil {
		t.Fatalf("Get B: %v", err)
	}
	if !c.Has(cA.hash) || !c.Has(cB.hash) {
		t.Fatalf("expected A and B cached")
	}

	// Touch A so B becomes least recently used.
	if _, err := c.Get(t.Context(), cs, cA.hash, nil); err != nil {
		t.Fatalf("touch A: %v", err)
	}
	if cs.gets.Load() != 2 {
		t.Fatalf("touch A should hit cache; gets = %d", cs.gets.Load())
	}

	// Insert C -> B must be evicted, A and C survive.
	if _, err := c.Get(t.Context(), cs, cC.hash, nil); err != nil {
		t.Fatalf("Get C: %v", err)
	}
	if c.Size() > maxBytes {
		t.Fatalf("Size() = %d > maxBytes %d", c.Size(), maxBytes)
	}
	if !c.Has(cA.hash) || !c.Has(cC.hash) || c.Has(cB.hash) {
		t.Fatalf("LRU state wrong: Has(A)=%v Has(B)=%v Has(C)=%v, want true/false/true",
			c.Has(cA.hash), c.Has(cB.hash), c.Has(cC.hash))
	}
	bHex := cB.hash.String()
	if _, err := os.Stat(filepath.Join(dir, bHex[:2], bHex)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("evicted chunk B still on disk: err=%v", err)
	}

	// Chunk bigger than MaxBytes is returned, not cached, and does not evict existing entries.
	gotBig, err := c.Get(t.Context(), cs, cBig.hash, nil)
	if err != nil {
		t.Fatalf("Get big: %v", err)
	}
	if !bytes.Equal(gotBig, cBig.raw) {
		t.Fatalf("big chunk bytes mismatch")
	}
	if c.Has(cBig.hash) {
		t.Fatalf("oversized chunk should not be cached")
	}
	if !c.Has(cA.hash) || !c.Has(cC.hash) {
		t.Fatalf("oversized chunk unexpectedly evicted existing entries")
	}
}

func TestCapConcurrent(t *testing.T) {
	cs, ms := newCountingStore()
	const numChunks = 32
	chunks := make([]testChunk, numChunks)
	for i := range numChunks {
		chunks[i] = makeChunk(t, ms, 4<<10)
	}
	// Cap holds ~6 chunks.
	maxBytes := int64(len(chunks[0].obj)) * 6
	dir := t.TempDir()
	c := openTestCache(t, Options{Dir: dir, MaxBytes: maxBytes})

	var wg sync.WaitGroup
	const workers = 16
	for w := range workers {
		wg.Go(func() {
			for round := range 4 {
				idx := (w*3 + round*7) % numChunks
				ch := chunks[idx]
				got, err := c.Get(t.Context(), cs, ch.hash, nil)
				if err != nil {
					t.Errorf("worker %d Get(%d): %v", w, idx, err)
					return
				}
				if !bytes.Equal(got, ch.raw) {
					t.Errorf("worker %d Get(%d): bytes mismatch", w, idx)
					return
				}
				if sz := c.Size(); sz > maxBytes {
					t.Errorf("Size() = %d exceeded maxBytes %d after Get", sz, maxBytes)
					return
				}
			}
		})
	}
	wg.Wait()

	if sz := c.Size(); sz > maxBytes {
		t.Fatalf("final Size() = %d > maxBytes %d", sz, maxBytes)
	}

	// Verify on-disk chunk files match index and total size with no leftover tmp files.
	var diskBytes int64
	diskFiles := 0
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(dir, p)
		if strings.HasPrefix(rel, "tmp"+string(filepath.Separator)) {
			t.Errorf("unexpected leftover tmp file: %s", rel)
			return nil
		}
		h, parseErr := snapformat.ParseHash(d.Name())
		if parseErr != nil {
			t.Errorf("unexpected non-hash file %s: %v", rel, parseErr)
			return nil
		}
		if !c.Has(h) {
			t.Errorf("file %s on disk is not in cache index", rel)
		}
		fi, statErr := d.Info()
		if statErr != nil {
			return statErr
		}
		diskBytes += fi.Size()
		diskFiles++
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
	if diskBytes != c.Size() {
		t.Fatalf("on-disk bytes = %d, cache Size() = %d", diskBytes, c.Size())
	}
	if diskFiles == 0 {
		t.Fatalf("expected some cached files on disk")
	}
}

func TestPinPreventsRemovalWhileReading(t *testing.T) {
	cs, ms := newCountingStore()
	cA := makeChunk(t, ms, 8<<10)
	cB := makeChunk(t, ms, 8<<10)

	// Cap fits only one chunk at a time.
	maxBytes := int64(len(cA.obj)) + 64
	dir := t.TempDir()
	c := openTestCache(t, Options{Dir: dir, MaxBytes: maxBytes})

	if _, err := c.Get(t.Context(), cs, cA.hash, nil); err != nil {
		t.Fatalf("Get A: %v", err)
	}
	aHex := cA.hash.String()
	aPath := filepath.Join(dir, aHex[:2], aHex)

	// Simulate an active reader pinning A before B triggers eviction.
	c.mu.Lock()
	eA := c.entries[cA.hash]
	c.pins[cA.hash]++
	c.mu.Unlock()

	if _, err := c.Get(t.Context(), cs, cB.hash, nil); err != nil {
		t.Fatalf("Get B: %v", err)
	}
	if c.Size() > maxBytes {
		t.Fatalf("Size() = %d > maxBytes %d", c.Size(), maxBytes)
	}
	if c.Has(cA.hash) {
		t.Fatalf("A should be logically evicted from index")
	}
	// File for A must still exist on disk while pinned!
	if _, err := os.Stat(aPath); err != nil {
		t.Fatalf("pinned file A was removed while still pinned: %v", err)
	}

	// Unpin A: file must now be deleted.
	c.unpin(cA.hash, eA, false)
	if _, err := os.Stat(aPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("evicted file A still exists after unpin: err=%v", err)
	}

	// Pin B, evict B by re-inserting A, then re-insert B before unpinning the old reader:
	// the old reader's unpin must NOT delete the newly re-inserted file for B.
	if _, err := c.Get(t.Context(), cs, cB.hash, nil); err != nil {
		t.Fatalf("Get B again: %v", err)
	}
	bHex := cB.hash.String()
	bPath := filepath.Join(dir, bHex[:2], bHex)
	c.mu.Lock()
	eB := c.entries[cB.hash]
	c.pins[cB.hash]++
	c.mu.Unlock()

	if _, err := c.Get(t.Context(), cs, cA.hash, nil); err != nil {
		t.Fatalf("Get A to evict B: %v", err)
	}
	if _, err := c.Get(t.Context(), cs, cB.hash, nil); err != nil {
		t.Fatalf("re-insert B while old reader still pinned: %v", err)
	}
	c.unpin(cB.hash, eB, false)
	if !c.Has(cB.hash) {
		t.Fatalf("re-inserted B missing from index after old reader unpin")
	}
	if _, err := os.Stat(bPath); err != nil {
		t.Fatalf("re-inserted file B was deleted by old reader unpin: %v", err)
	}
}

func TestCorruption(t *testing.T) {
	t.Run("disk_flipped_byte_refetched_and_replaced", func(t *testing.T) {
		cs, ms := newCountingStore()
		ch := makeChunk(t, ms, 8<<10)
		dir := t.TempDir()

		var logBuf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logBuf, nil))
		c := openTestCache(t, Options{Dir: dir, MaxBytes: 1 << 20, Log: logger})

		if _, err := c.Get(t.Context(), cs, ch.hash, nil); err != nil {
			t.Fatalf("initial Get: %v", err)
		}
		if cs.gets.Load() != 1 {
			t.Fatalf("gets = %d, want 1", cs.gets.Load())
		}

		// Corrupt cached file on disk by flipping a byte.
		hexStr := ch.hash.String()
		diskPath := filepath.Join(dir, hexStr[:2], hexStr)
		corrupted := bytes.Clone(ch.obj)
		corrupted[len(corrupted)/2] ^= 0x55
		if err := os.WriteFile(diskPath, corrupted, 0o600); err != nil {
			t.Fatalf("WriteFile corrupt: %v", err)
		}

		got, err := c.Get(t.Context(), cs, ch.hash, nil)
		if err != nil {
			t.Fatalf("Get after disk corruption: %v", err)
		}
		if !bytes.Equal(got, ch.raw) {
			t.Fatalf("refetched bytes mismatch")
		}
		if cs.gets.Load() != 2 {
			t.Fatalf("gets = %d, want 2 (refetch after disk corruption)", cs.gets.Load())
		}
		if !strings.Contains(logBuf.String(), hexStr) {
			t.Fatalf("expected warning log mentioning hash %s, got %q", hexStr, logBuf.String())
		}

		// File on disk must now be repaired (third Get hits cache).
		got3, err := c.Get(t.Context(), cs, ch.hash, nil)
		if err != nil {
			t.Fatalf("third Get: %v", err)
		}
		if !bytes.Equal(got3, ch.raw) {
			t.Fatalf("third Get bytes mismatch")
		}
		if cs.gets.Load() != 2 {
			t.Fatalf("gets = %d after repair, want 2", cs.gets.Load())
		}
	})

	t.Run("disk_oversized_file_refetched", func(t *testing.T) {
		cs, ms := newCountingStore()
		ch := makeChunk(t, ms, 4<<10)
		dir := t.TempDir()
		c := openTestCache(t, Options{Dir: dir, MaxBytes: 1 << 20})

		if _, err := c.Get(t.Context(), cs, ch.hash, nil); err != nil {
			t.Fatalf("initial Get: %v", err)
		}

		hexStr := ch.hash.String()
		diskPath := filepath.Join(dir, hexStr[:2], hexStr)
		f, err := os.OpenFile(diskPath, os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		if err := f.Truncate(maxObjectBytes + 1); err != nil {
			_ = f.Close()
			t.Fatalf("Truncate: %v", err)
		}
		_ = f.Close()

		got, err := c.Get(t.Context(), cs, ch.hash, nil)
		if err != nil {
			t.Fatalf("Get after oversized disk file: %v", err)
		}
		if !bytes.Equal(got, ch.raw) {
			t.Fatalf("refetched bytes mismatch")
		}
		if cs.gets.Load() != 2 {
			t.Fatalf("gets = %d, want 2", cs.gets.Load())
		}
	})

	t.Run("disk_externally_removed_refetched", func(t *testing.T) {
		cs, ms := newCountingStore()
		ch := makeChunk(t, ms, 4<<10)
		dir := t.TempDir()
		c := openTestCache(t, Options{Dir: dir, MaxBytes: 1 << 20})

		if _, err := c.Get(t.Context(), cs, ch.hash, nil); err != nil {
			t.Fatalf("initial Get: %v", err)
		}
		hexStr := ch.hash.String()
		if err := os.Remove(filepath.Join(dir, hexStr[:2], hexStr)); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		got, err := c.Get(t.Context(), cs, ch.hash, nil)
		if err != nil {
			t.Fatalf("Get after external removal: %v", err)
		}
		if !bytes.Equal(got, ch.raw) {
			t.Fatalf("refetched bytes mismatch")
		}
		if cs.gets.Load() != 2 {
			t.Fatalf("gets = %d, want 2", cs.gets.Load())
		}
	})

	t.Run("store_corrupt_objects", func(t *testing.T) {
		otherRaw := []byte("valid zstd payload for a different hash")
		_, otherObj, err := snapformat.EncodeChunk(otherRaw)
		if err != nil {
			t.Fatalf("EncodeChunk: %v", err)
		}
		wantHash := snapformat.HashChunk([]byte("expected chunk data"))

		cases := []struct {
			name string
			body func() io.ReadCloser
		}{
			{
				name: "bad_zstd",
				body: func() io.ReadCloser { return io.NopCloser(bytes.NewReader([]byte("not a zstd frame"))) },
			},
			{
				name: "wrong_hash",
				body: func() io.ReadCloser { return io.NopCloser(bytes.NewReader(otherObj)) },
			},
			{
				name: "empty_object",
				body: func() io.ReadCloser { return io.NopCloser(bytes.NewReader(nil)) },
			},
			{
				name: "oversized_object",
				body: func() io.ReadCloser {
					return io.NopCloser(io.LimitReader(zeroReader{}, maxObjectBytes+1024))
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				cs, _ := newCountingStore()
				cs.onGet = func(ctx context.Context, key string) (io.ReadCloser, error, bool) {
					return tc.body(), nil, true
				}
				dir := t.TempDir()
				c := openTestCache(t, Options{Dir: dir, MaxBytes: 1 << 20})

				_, err := c.Get(t.Context(), cs, wantHash, nil)
				if !errors.Is(err, snapformat.ErrCorruptChunk) {
					t.Fatalf("Get err = %v, want ErrCorruptChunk", err)
				}
				if c.Has(wantHash) || c.Size() != 0 {
					t.Fatalf("corrupt store object was cached: Has=%v Size=%d", c.Has(wantHash), c.Size())
				}
				hexStr := wantHash.String()
				if _, statErr := os.Stat(filepath.Join(dir, hexStr[:2], hexStr)); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("corrupt store object file exists on disk: %v", statErr)
				}
			})
		}
	})
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestStoreErrors(t *testing.T) {
	cs, _ := newCountingStore()
	c := openTestCache(t, Options{Dir: t.TempDir(), MaxBytes: 1 << 20})
	missing := snapformat.HashChunk([]byte("does not exist"))

	_, err := c.Get(t.Context(), cs, missing, nil)
	if !errors.Is(err, objstore.ErrNotFound) {
		t.Fatalf("Get missing err = %v, want objstore.ErrNotFound", err)
	}
	if c.Has(missing) || c.Size() != 0 {
		t.Fatalf("missing hash should not be cached")
	}

	// Nil store on miss returns an error without panicking.
	if _, err := c.Get(t.Context(), nil, missing, nil); err == nil {
		t.Fatal("Get with nil store on miss succeeded, want error")
	}

	// Already cancelled context returns ctx error without calling store.
	before := cs.gets.Load()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Get(ctx, cs, missing, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get with cancelled ctx err = %v, want context.Canceled", err)
	}
	if cs.gets.Load() != before {
		t.Fatalf("cancelled Get still called store")
	}

	// Store body read error and Close error propagate cleanly without caching.
	ioErr := errors.New("boom read")
	cs.onGet = func(ctx context.Context, key string) (io.ReadCloser, error, bool) {
		return errReadCloser{readErr: ioErr}, nil, true
	}
	if _, err := c.Get(t.Context(), cs, missing, nil); !errors.Is(err, ioErr) {
		t.Fatalf("Get body read err = %v, want %v", err, ioErr)
	}
	closeErr := errors.New("boom close")
	cs.onGet = func(ctx context.Context, key string) (io.ReadCloser, error, bool) {
		return errReadCloser{closeErr: closeErr}, nil, true
	}
	if _, err := c.Get(t.Context(), cs, missing, nil); !errors.Is(err, closeErr) {
		t.Fatalf("Get body close err = %v, want %v", err, closeErr)
	}
	if c.Has(missing) || c.Size() != 0 {
		t.Fatalf("errored store reads should not be cached")
	}
}

type errReadCloser struct {
	readErr  error
	closeErr error
}

func (e errReadCloser) Read([]byte) (int, error) {
	if e.readErr != nil {
		return 0, e.readErr
	}
	return 0, io.EOF
}

func (e errReadCloser) Close() error { return e.closeErr }

func TestRestartRebuild(t *testing.T) {
	cs, ms := newCountingStore()
	cA := makeChunk(t, ms, 8<<10)
	cB := makeChunk(t, ms, 8<<10)
	cC := makeChunk(t, ms, 8<<10)
	cD := makeChunk(t, ms, 8<<10)

	dir := t.TempDir()
	c1, err := Open(t.Context(), Options{Dir: dir, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	for _, ch := range []testChunk{cA, cB, cC} {
		if _, err := c1.Get(t.Context(), cs, ch.hash, nil); err != nil {
			t.Fatalf("Get %s: %v", ch.hash, err)
		}
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Set distinct mtimes: A oldest, B middle, C newest.
	baseTime := time.Now().Add(-time.Hour)
	for i, ch := range []testChunk{cA, cB, cC} {
		h := ch.hash.String()
		p := filepath.Join(dir, h[:2], h)
		mt := baseTime.Add(time.Duration(i) * time.Minute)
		if err := os.Chtimes(p, mt, mt); err != nil {
			t.Fatalf("Chtimes: %v", err)
		}
	}

	// Drop stray tmp files and a foreign file inside a shard dir.
	strayTmp := filepath.Join(dir, "tmp", "leftover-upload")
	if err := os.WriteFile(strayTmp, []byte("partial"), 0o600); err != nil {
		t.Fatalf("write stray tmp: %v", err)
	}
	straySub := filepath.Join(dir, "tmp", "stray-dir")
	if err := os.Mkdir(straySub, 0o700); err != nil {
		t.Fatalf("mkdir stray sub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(straySub, "x"), []byte("y"), 0o600); err != nil {
		t.Fatalf("write stray sub file: %v", err)
	}
	foreignFile := filepath.Join(dir, cA.hash.String()[:2], "not-a-hash")
	if err := os.WriteFile(foreignFile, []byte("ignore"), 0o600); err != nil {
		t.Fatalf("write foreign file: %v", err)
	}
	// A subdirectory named like a valid hash inside a shard dir must be ignored.
	fakeHash := snapformat.HashChunk([]byte("subdir-not-a-file"))
	fakeHex := fakeHash.String()
	if err := os.MkdirAll(filepath.Join(dir, fakeHex[:2], fakeHex), 0o700); err != nil {
		t.Fatalf("mkdir fake hash subdir: %v", err)
	}

	// Reopen with cap that fits 3 chunks (not 4).
	wantSize := int64(len(cA.obj) + len(cB.obj) + len(cC.obj))
	maxBytes := wantSize + int64(len(cD.obj))/2
	c2 := openTestCache(t, Options{Dir: dir, MaxBytes: maxBytes})

	if _, err := os.Stat(strayTmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stray tmp file was not removed by Open: err=%v", err)
	}
	if _, err := os.Stat(straySub); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stray tmp subdir was not removed by Open: err=%v", err)
	}
	if c2.Size() != wantSize {
		t.Fatalf("rebuilt Size() = %d, want %d", c2.Size(), wantSize)
	}
	for _, ch := range []testChunk{cA, cB, cC} {
		if !c2.Has(ch.hash) {
			t.Fatalf("rebuilt cache missing %s", ch.hash)
		}
	}

	// Reading B and C must hit disk without touching the store (and leaves A as oldest!).
	beforeGets := cs.gets.Load()
	for _, ch := range []testChunk{cB, cC} {
		got, err := c2.Get(t.Context(), cs, ch.hash, nil)
		if err != nil {
			t.Fatalf("rebuilt Get %s: %v", ch.hash, err)
		}
		if !bytes.Equal(got, ch.raw) {
			t.Fatalf("rebuilt Get %s bytes mismatch", ch.hash)
		}
	}
	if cs.gets.Load() != beforeGets {
		t.Fatalf("rebuilt cache made store calls on warm entries")
	}

	// Inserting D must evict A (oldest mtime).
	if _, err := c2.Get(t.Context(), cs, cD.hash, nil); err != nil {
		t.Fatalf("Get D: %v", err)
	}
	if c2.Has(cA.hash) {
		t.Fatalf("expected oldest mtime chunk A to be evicted")
	}
	if !c2.Has(cB.hash) || !c2.Has(cC.hash) || !c2.Has(cD.hash) {
		t.Fatalf("expected B, C, D to remain cached")
	}

	// Reopen with smaller MaxBytes (fits only 1 chunk): Open must trim immediately.
	if err := c2.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}
	smallCap := int64(len(cD.obj)) + 64
	c3 := openTestCache(t, Options{Dir: dir, MaxBytes: smallCap})
	if c3.Size() > smallCap {
		t.Fatalf("Size() after reopening with smaller cap = %d > %d", c3.Size(), smallCap)
	}
}

func TestPassThrough(t *testing.T) {
	cs, ms := newCountingStore()
	ch := makeChunk(t, ms, 8<<10)
	dir := t.TempDir()

	c := openTestCache(t, Options{Dir: dir, MaxBytes: 0})

	got1, err := c.Get(t.Context(), cs, ch.hash, nil)
	if err != nil {
		t.Fatalf("pass-through Get 1: %v", err)
	}
	if !bytes.Equal(got1, ch.raw) {
		t.Fatalf("pass-through Get 1 bytes mismatch")
	}
	got2, err := c.Get(t.Context(), cs, ch.hash, nil)
	if err != nil {
		t.Fatalf("pass-through Get 2: %v", err)
	}
	if !bytes.Equal(got2, ch.raw) {
		t.Fatalf("pass-through Get 2 bytes mismatch")
	}
	if cs.gets.Load() != 2 {
		t.Fatalf("pass-through store Get count = %d, want 2", cs.gets.Load())
	}
	if c.Has(ch.hash) || c.Size() != 0 {
		t.Fatalf("pass-through reported Has=%v Size=%d, want false/0", c.Has(ch.hash), c.Size())
	}

	// Directory must stay completely empty.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("pass-through created %d entries in Dir, want 0", len(entries))
	}

	// Empty Dir is also valid in pass-through mode.
	cEmpty := openTestCache(t, Options{MaxBytes: 0})
	if _, err := cEmpty.Get(t.Context(), cs, ch.hash, nil); err != nil {
		t.Fatalf("pass-through empty Dir Get: %v", err)
	}
}

func TestOpenValidationAndClose(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(t.Context())
	cancel()

	regularFile := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(regularFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	badTmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(badTmpDir, "tmp"), []byte("not-a-dir"), 0o600); err != nil {
		t.Fatalf("WriteFile tmp: %v", err)
	}

	cases := []struct {
		name string
		ctx  context.Context
		opts Options
	}{
		{
			name: "negative_max_bytes",
			ctx:  t.Context(),
			opts: Options{Dir: t.TempDir(), MaxBytes: -1},
		},
		{
			name: "missing_dir_when_enabled",
			ctx:  t.Context(),
			opts: Options{Dir: "", MaxBytes: 1024},
		},
		{
			name: "cancelled_context",
			ctx:  cancelledCtx,
			opts: Options{Dir: t.TempDir(), MaxBytes: 1024},
		},
		{
			name: "dir_is_regular_file",
			ctx:  t.Context(),
			opts: Options{Dir: regularFile, MaxBytes: 1024},
		},
		{
			name: "tmp_is_regular_file",
			ctx:  t.Context(),
			opts: Options{Dir: badTmpDir, MaxBytes: 1024},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := Open(tc.ctx, tc.opts)
			if err == nil {
				_ = c.Close()
				t.Fatalf("Open(%+v) succeeded, want error", tc.opts)
			}
		})
	}

	t.Run("close_cancels_inflight_and_rejects_future_get", func(t *testing.T) {
		cs, ms := newCountingStore()
		ch := makeChunk(t, ms, 4<<10)

		started := make(chan struct{})
		cs.onGet = func(ctx context.Context, key string) (io.ReadCloser, error, bool) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err(), true
		}

		c, err := Open(t.Context(), Options{Dir: t.TempDir(), MaxBytes: 1 << 20})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}

		getErr := make(chan error, 1)
		go func() {
			_, err := c.Get(t.Context(), cs, ch.hash, nil)
			getErr <- err
		}()
		<-started

		if err := c.Close(); err != nil {
			t.Fatalf("first Close: %v", err)
		}
		if err := c.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if err := <-getErr; err == nil {
			t.Fatal("in-flight Get succeeded after Close, want error")
		}
		if _, err := c.Get(t.Context(), cs, ch.hash, nil); err == nil {
			t.Fatal("Get after Close succeeded, want error")
		}
	})
}

func TestPutFailurePaths(t *testing.T) {
	cases := []struct {
		name     string
		sabotage func(t *testing.T, dir string, h snapformat.Hash)
		checkTmp bool
	}{
		{
			name: "tmp_create_fails",
			sabotage: func(t *testing.T, dir string, _ snapformat.Hash) {
				if err := os.RemoveAll(filepath.Join(dir, "tmp")); err != nil {
					t.Fatalf("RemoveAll tmp: %v", err)
				}
			},
			checkTmp: false,
		},
		{
			name: "shard_squatted_by_file",
			sabotage: func(t *testing.T, dir string, h snapformat.Hash) {
				shardPath := filepath.Join(dir, h.String()[:2])
				if err := os.WriteFile(shardPath, []byte("squatter"), 0o600); err != nil {
					t.Fatalf("WriteFile shard squatter: %v", err)
				}
			},
			checkTmp: true,
		},
		{
			name: "rename_fails",
			sabotage: func(t *testing.T, dir string, h snapformat.Hash) {
				hexStr := h.String()
				destDir := filepath.Join(dir, hexStr[:2], hexStr)
				if err := os.MkdirAll(destDir, 0o700); err != nil {
					t.Fatalf("MkdirAll dest dir: %v", err)
				}
			},
			checkTmp: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs, ms := newCountingStore()
			seed := makeChunk(t, ms, 4<<10)
			target := makeChunk(t, ms, 4<<10)
			for target.hash.String()[:2] == seed.hash.String()[:2] {
				target = makeChunk(t, ms, 4<<10)
			}

			dir := t.TempDir()
			var logBuf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logBuf, nil))
			c := openTestCache(t, Options{Dir: dir, MaxBytes: 1 << 20, Log: logger})

			if _, err := c.Get(t.Context(), cs, seed.hash, nil); err != nil {
				t.Fatalf("Get seed: %v", err)
			}
			wantSize := c.Size()

			tc.sabotage(t, dir, target.hash)

			got, err := c.Get(t.Context(), cs, target.hash, nil)
			if err != nil {
				t.Fatalf("Get target failed on put error: %v", err)
			}
			if !bytes.Equal(got, target.raw) {
				t.Fatalf("Get target bytes mismatch")
			}
			if c.Has(target.hash) {
				t.Fatalf("Has(target) = true after put failure, want false")
			}
			if c.Size() != wantSize {
				t.Fatalf("Size() = %d after put failure, want unchanged %d", c.Size(), wantSize)
			}
			if !strings.Contains(logBuf.String(), target.hash.String()) {
				t.Fatalf("expected warning log containing %s, got %q", target.hash, logBuf.String())
			}
			if tc.checkTmp {
				entries, err := os.ReadDir(filepath.Join(dir, "tmp"))
				if err != nil {
					t.Fatalf("ReadDir(tmp): %v", err)
				}
				if len(entries) != 0 {
					t.Fatalf("tmp/ has %d leftover entries after put failure, want 0", len(entries))
				}
			}
		})
	}
}

func TestScanCacheDirSkipsForeignEntries(t *testing.T) {
	cs, ms := newCountingStore()
	valid := makeChunk(t, ms, 4<<10)
	dir := t.TempDir()

	c1, err := Open(t.Context(), Options{Dir: dir, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	if _, err := c1.Get(t.Context(), cs, valid.hash, nil); err != nil {
		t.Fatalf("Get valid: %v", err)
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// 1. Non-hex shard directory containing a file with a valid hash name.
	hNonHex := snapformat.HashChunk([]byte("non-hex-shard"))
	if err := os.MkdirAll(filepath.Join(dir, "zz"), 0o700); err != nil {
		t.Fatalf("mkdir zz: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "zz", hNonHex.String()), []byte("ignored"), 0o600); err != nil {
		t.Fatalf("write non-hex shard file: %v", err)
	}

	// 2. Misnamed file inside a valid shard directory.
	if err := os.WriteFile(filepath.Join(dir, valid.hash.String()[:2], "not-a-valid-hash"), []byte("ignored"), 0o600); err != nil {
		t.Fatalf("write misnamed file: %v", err)
	}

	// 3. Valid hash filename placed in the wrong 2-hex shard directory.
	hWrongShard := snapformat.HashChunk([]byte("wrong-shard"))
	wrongShard := "00"
	if hWrongShard.String()[:2] == wrongShard {
		wrongShard = "ff"
	}
	if err := os.MkdirAll(filepath.Join(dir, wrongShard), 0o700); err != nil {
		t.Fatalf("mkdir wrong shard: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, wrongShard, hWrongShard.String()), []byte("ignored"), 0o600); err != nil {
		t.Fatalf("write wrong shard file: %v", err)
	}

	// 4. Non-regular entry (directory) with a valid hash name in its matching shard.
	hNonRegular := snapformat.HashChunk([]byte("non-regular-entry"))
	if err := os.MkdirAll(filepath.Join(dir, hNonRegular.String()[:2], hNonRegular.String()), 0o700); err != nil {
		t.Fatalf("mkdir non-regular entry: %v", err)
	}

	c2 := openTestCache(t, Options{Dir: dir, MaxBytes: 1 << 20})
	if c2.Size() != int64(len(valid.obj)) {
		t.Fatalf("Size() = %d, want %d (only valid chunk)", c2.Size(), len(valid.obj))
	}
	if !c2.Has(valid.hash) {
		t.Fatalf("Has(valid) = false, want true")
	}
	for _, skipped := range []struct {
		name string
		h    snapformat.Hash
	}{
		{"non_hex_shard", hNonHex},
		{"wrong_shard", hWrongShard},
		{"non_regular", hNonRegular},
	} {
		if c2.Has(skipped.h) {
			t.Fatalf("Has(%s) = true, want false (should be skipped by scan)", skipped.name)
		}
	}
}
