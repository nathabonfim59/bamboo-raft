package gc_test

// Tests of the collector's mark, sweep and fence against a scripted Store and
// the in-memory object store; no PostgreSQL. The liveness SQL and the fence
// against real attempts are in gc_db_test.go.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/gc"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

var errBoom = errors.New("boom")

// eventLog records the order of store and object calls.
type eventLog struct {
	mu sync.Mutex
	ev []string
}

func (l *eventLog) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ev = append(l.ev, s)
}

func (l *eventLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.ev)
}

func (l *eventLog) count(s string) int {
	n := 0
	for _, e := range l.all() {
		if e == s {
			n++
		}
	}
	return n
}

// fakeStore scripts gc.Store.
type fakeStore struct {
	log *eventLog

	mu       sync.Mutex
	live     func() []db.GCLiveGeneration
	pending  func() []db.GCInFlightGeneration
	declined func(keys []string) []string
	refuse   bool // GCBeginSweep reports the fence held
	sweeping bool
	purged   [][]string
	recovers int
}

func (f *fakeStore) GCLiveGenerations(context.Context, time.Time) ([]db.GCLiveGeneration, error) {
	f.log.add("live")
	if f.live == nil {
		return nil, nil
	}
	return f.live(), nil
}

func (f *fakeStore) GCBeginSweep(context.Context, time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refuse {
		f.log.add("begin refused")
		return false, nil
	}
	f.sweeping = true
	f.log.add("begin")
	return true, nil
}

func (f *fakeStore) GCEndSweep(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sweeping = false
	f.log.add("end")
	return nil
}

func (f *fakeStore) GCRecoverStaleSweep(context.Context, time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recovers++
	return false, nil
}

func (f *fakeStore) GCInFlightGenerations(context.Context, time.Duration) ([]db.GCInFlightGeneration, error) {
	f.log.add("pending")
	if f.pending == nil {
		return nil, nil
	}
	return f.pending(), nil
}

func (f *fakeStore) GCPurgeGenerations(_ context.Context, _ time.Time, keys []string) ([]string, error) {
	f.log.add("purge")
	f.mu.Lock()
	f.purged = append(f.purged, slices.Clone(keys))
	f.mu.Unlock()
	if f.declined == nil {
		return nil, nil
	}
	return f.declined(keys), nil
}

func (f *fakeStore) isSweeping() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sweeping
}

// recStore wraps an object store to record deletes and inject failures.
type recStore struct {
	objstore.Store
	log        *eventLog
	failList   string // prefix whose listing fails
	failDelete bool
}

func kindOf(key string) string {
	if strings.HasPrefix(key, snapformat.ManifestPrefix) {
		return "manifest"
	}
	return "chunk"
}

func (r *recStore) Delete(ctx context.Context, key string) error {
	r.log.add("delete " + kindOf(key))
	if r.failDelete {
		return errBoom
	}
	return r.Store.Delete(ctx, key)
}

func (r *recStore) List(ctx context.Context, prefix string) iter.Seq2[objstore.Info, error] {
	if r.failList != "" && prefix == r.failList {
		return func(yield func(objstore.Info, error) bool) { yield(objstore.Info{}, errBoom) }
	}
	return r.Store.List(ctx, prefix)
}

// testEnv is one collector over a memstore with a controllable clock.
type testEnv struct {
	t     *testing.T
	mem   *memstore.Store
	objs  *recStore
	store *fakeStore
	log   *eventLog
	logs  *bytes.Buffer
	now   time.Time
}

func newEnv(t *testing.T) *testEnv {
	t.Helper()
	e := &testEnv{
		t:    t,
		mem:  memstore.New(),
		log:  &eventLog{},
		logs: &bytes.Buffer{},
		now:  time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC),
	}
	e.mem.SetClock(func() time.Time { return e.now })
	e.objs = &recStore{Store: e.mem, log: e.log}
	e.store = &fakeStore{log: e.log}
	return e
}

func (e *testEnv) options() gc.Options {
	return gc.Options{
		Store: e.store, Objects: e.objs,
		Logger: slog.New(slog.NewTextHandler(e.logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
		Grace:  24 * time.Hour, History: 24 * time.Hour,
		Now:          func() time.Time { return e.now },
		PollInterval: time.Millisecond,
	}
}

func (e *testEnv) collector(mod func(*gc.Options)) *gc.Collector {
	e.t.Helper()
	opts := e.options()
	if mod != nil {
		mod(&opts)
	}
	c, err := gc.New(opts)
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

// advance moves the clock.
func (e *testEnv) advance(d time.Duration) { e.now = e.now.Add(d) }

// chunk stores a chunk object and returns its hash.
func (e *testEnv) chunk(content string) snapformat.Hash {
	e.t.Helper()
	h := snapformat.HashChunk([]byte(content))
	if err := e.mem.PutIfAbsent(e.t.Context(), snapformat.ChunkKey(h), strings.NewReader(content), int64(len(content))); err != nil {
		e.t.Fatal(err)
	}
	return h
}

// manifest stores a manifest of the chain's generation naming the chunks.
func (e *testEnv) manifest(chain string, generation uint32, chunks ...snapformat.Hash) string {
	e.t.Helper()
	m := &snapformat.Manifest{
		Version: snapformat.Version, ChainID: chain, Generation: generation, SandboxID: "bx_test",
		Image: "img", DiskBytes: uint64(max(len(chunks), 1)) * snapformat.ChunkSize,
		ChunkSize: snapformat.ChunkSize, CreatedAt: e.now,
	}
	for i, h := range chunks {
		m.Chunks = append(m.Chunks, snapformat.Entry{Index: uint64(i), Hash: h})
	}
	raw, err := snapformat.EncodeManifest(m)
	if err != nil {
		e.t.Fatal(err)
	}
	key := snapformat.ManifestKey(chain, generation)
	if err := e.mem.PutIfAbsent(e.t.Context(), key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		e.t.Fatal(err)
	}
	return key
}

func (e *testEnv) has(key string) bool {
	e.t.Helper()
	_, err := e.mem.Head(e.t.Context(), key)
	if err != nil && !errors.Is(err, objstore.ErrNotFound) {
		e.t.Fatal(err)
	}
	return err == nil
}

func (e *testEnv) liveKeys(keys ...string) {
	e.store.live = func() []db.GCLiveGeneration {
		out := make([]db.GCLiveGeneration, len(keys))
		for i, k := range keys {
			out[i] = db.GCLiveGeneration{ManifestKey: k, Status: db.GenerationCompleted}
		}
		return out
	}
}

func newChain() string { return uuid.NewV7().String() }

// TestRoundSweepsOnlyWhatIsUnreferencedAndOld: an old unreferenced chunk goes,
// an old referenced one stays, a young unreferenced one stays (grace).
func TestRoundSweepsOnlyWhatIsUnreferencedAndOld(t *testing.T) {
	e := newEnv(t)
	referenced := e.chunk("referenced")
	orphan := e.chunk("orphan")
	manifest := e.manifest(newChain(), 0, referenced)
	e.advance(25 * time.Hour)
	young := e.chunk("young orphan")
	e.liveKeys(manifest)

	sum, err := e.collector(nil).Round(t.Context())
	if err != nil {
		t.Fatalf("round: %v", err)
	}
	if e.has(snapformat.ChunkKey(orphan)) {
		t.Error("old unreferenced chunk survived")
	}
	for name, key := range map[string]string{
		"referenced chunk": snapformat.ChunkKey(referenced), "young orphan": snapformat.ChunkKey(young), "live manifest": manifest,
	} {
		if !e.has(key) {
			t.Errorf("%s was deleted", name)
		}
	}
	want := gc.Summary{ManifestsMarked: 1, ChunksMarked: 1, ChunksKept: 2, ChunksDeleted: 1, BytesFreed: int64(len("orphan")), Batches: 1}
	if sum != want {
		t.Errorf("summary = %+v, want %+v", sum, want)
	}
}

// TestRoundGraceUsesTheObjectClock: nothing younger than the grace is
// deleted even when unreferenced and unreachable.
func TestRoundGraceUsesTheObjectClock(t *testing.T) {
	e := newEnv(t)
	young := e.chunk("young")
	e.advance(23 * time.Hour)
	sum, err := e.collector(nil).Round(t.Context())
	if err != nil || sum.ChunksDeleted != 0 || !e.has(snapformat.ChunkKey(young)) {
		t.Fatalf("round = %+v, %v; chunk kept = %v", sum, err, e.has(snapformat.ChunkKey(young)))
	}
	if e.log.count("begin") != 0 {
		t.Fatal("a round with nothing to sweep took the fence")
	}
	e.advance(2 * time.Hour)
	sum, err = e.collector(nil).Round(t.Context())
	if err != nil || sum.ChunksDeleted != 1 || e.has(snapformat.ChunkKey(young)) {
		t.Fatalf("round after the grace = %+v, %v", sum, err)
	}
}

// TestRoundManifestsFollowLivenessAndGrace: a dead manifest older than the
// grace is deleted with its row purged and so are chunks only it named; a dead
// manifest younger than the grace keeps its chunks (its generation is still
// listed and restorable); a manifest with no row at all is a dead one.
func TestRoundManifestsFollowLivenessAndGrace(t *testing.T) {
	e := newEnv(t)
	oldOnly := e.chunk("only the old manifest")
	shared := e.chunk("shared with a live one")
	deadOld := e.manifest(newChain(), 0, oldOnly, shared)
	e.advance(30 * time.Hour)
	youngOnly := e.chunk("only the young dead manifest")
	deadYoung := e.manifest(newChain(), 0, youngOnly)
	live := e.manifest(newChain(), 0, shared)
	e.liveKeys(live)

	sum, err := e.collector(nil).Round(t.Context())
	if err != nil {
		t.Fatalf("round: %v", err)
	}
	if e.has(deadOld) || e.has(snapformat.ChunkKey(oldOnly)) {
		t.Error("the dead old manifest or its exclusive chunk survived")
	}
	for name, key := range map[string]string{
		"shared chunk": snapformat.ChunkKey(shared), "young dead manifest": deadYoung,
		"its chunk": snapformat.ChunkKey(youngOnly), "live manifest": live,
	} {
		if !e.has(key) {
			t.Errorf("%s was deleted", name)
		}
	}
	if len(e.store.purged) != 1 || !slices.Equal(e.store.purged[0], []string{deadOld}) {
		t.Errorf("purged = %v, want only the dead old manifest", e.store.purged)
	}
	if sum.ManifestsDeleted != 1 || sum.ChunksDeleted != 1 {
		t.Errorf("summary = %+v", sum)
	}
}

// TestRoundKeepsWhatAPurgeRefuses: a row that turned live between the mark and
// the purge is refused, and its manifest and chunks stay.
func TestRoundKeepsWhatAPurgeRefuses(t *testing.T) {
	e := newEnv(t)
	chunk := e.chunk("pinned meanwhile")
	pinned := e.manifest(newChain(), 0, chunk)
	e.advance(48 * time.Hour)
	e.store.declined = func(keys []string) []string { return keys }

	sum, err := e.collector(nil).Round(t.Context())
	if err != nil {
		t.Fatalf("round: %v", err)
	}
	if !e.has(pinned) || !e.has(snapformat.ChunkKey(chunk)) {
		t.Fatal("a refused purge still lost its manifest or chunk")
	}
	if sum.ManifestsDeleted != 0 || sum.ChunksDeleted != 0 {
		t.Fatalf("summary = %+v", sum)
	}
}

// TestFenceOrder: under one batch the flag is set before anything else, the
// pending wait and the re-mark come before the purge, objects are deleted
// after the purge and the flag is cleared last.
func TestFenceOrder(t *testing.T) {
	e := newEnv(t)
	e.chunk("orphan")
	e.manifest(newChain(), 0)
	e.advance(48 * time.Hour)

	if _, err := e.collector(nil).Round(t.Context()); err != nil {
		t.Fatalf("round: %v", err)
	}
	want := []string{
		"live",                                                        // the mark
		"begin", "pending", "live", "purge", "delete manifest", "end", // the manifest batch
		"begin", "pending", "live", "delete chunk", "end", // the chunk batch
	}
	if got := e.log.all(); !slices.Equal(got, want) {
		t.Fatalf("events:\n got %v\nwant %v", got, want)
	}
}

// TestBatching: candidates are swept in fenced batches of the configured size.
func TestBatching(t *testing.T) {
	e := newEnv(t)
	for i := range 5 {
		e.chunk(fmt.Sprintf("orphan %d", i))
	}
	e.advance(48 * time.Hour)
	sum, err := e.collector(func(o *gc.Options) { o.Batch = 2 }).Round(t.Context())
	if err != nil || sum.ChunksDeleted != 5 || sum.Batches != 3 {
		t.Fatalf("round = %+v, %v; want 5 chunks in 3 batches", sum, err)
	}
	if b, en := e.log.count("begin"), e.log.count("end"); b != 3 || en != 3 {
		t.Fatalf("begins %d, ends %d; every batch takes and releases the fence", b, en)
	}
}

// TestRemarkProtectsWhatAnAttemptFinishedWhileTheSweepWaited is the skip race:
// a chunk is an orphan at the mark, an attempt that was pending skips
// uploading it and completes while the fence waits, and its manifest names the
// chunk. The re-mark reads that manifest and the chunk survives; the
// resulting generation restores.
func TestRemarkProtectsWhatAnAttemptFinishedWhileTheSweepWaited(t *testing.T) {
	e := newEnv(t)
	skipped := e.chunk("an existing chunk the attempt skips uploading")
	e.advance(48 * time.Hour)

	chain := newChain()
	var manifest string
	var finished bool
	e.store.pending = func() []db.GCInFlightGeneration {
		if finished {
			return nil
		}
		finished = true
		// The attempt completes: its manifest is written and its row becomes live.
		manifest = e.manifest(chain, 1, skipped)
		e.liveKeys(manifest)
		return []db.GCInFlightGeneration{{ID: uuid.NewV7(), SandboxID: "bx_a", Number: 1, DeadlineAt: e.now.Add(time.Minute)}}
	}

	sum, err := e.collector(nil).Round(t.Context())
	if err != nil {
		t.Fatalf("round: %v", err)
	}
	if !e.has(snapformat.ChunkKey(skipped)) || sum.ChunksDeleted != 0 {
		t.Fatalf("the skipped chunk was deleted: %+v", sum)
	}
	// The generation restores: its manifest and every chunk it names exist.
	if !e.has(manifest) {
		t.Fatal("manifest missing")
	}
}

// TestPendingWaitIsLoggedOnceAndCapped: a pending attempt whose owner is gone
// keeps the fence waiting; the wait is logged once per round with the
// generation it waits on, ends at the cap, deletes nothing and clears the
// flag.
func TestPendingWaitIsLoggedOnceAndCapped(t *testing.T) {
	e := newEnv(t)
	orphan := e.chunk("orphan")
	e.manifest(newChain(), 0)
	e.advance(48 * time.Hour)
	stuck := db.GCInFlightGeneration{ID: uuid.NewV7(), SandboxID: "bx_stuck", Number: 7, DeadlineAt: e.now.Add(time.Hour)}
	e.store.pending = func() []db.GCInFlightGeneration { return []db.GCInFlightGeneration{stuck} }

	c := e.collector(func(o *gc.Options) {
		o.SnapshotTimeout = 40 * time.Millisecond
		o.PendingMargin = 10 * time.Millisecond
	})
	start := time.Now()
	sum, err := c.Round(t.Context())
	if !errors.Is(err, gc.ErrPendingTimeout) {
		t.Fatalf("round error = %v, want ErrPendingTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the wait took %v, the cap is 50ms", elapsed)
	}
	if e.store.isSweeping() {
		t.Fatal("the sweeping flag is still set after the abort")
	}
	if e.log.count("delete manifest")+e.log.count("delete chunk") != 0 || !e.has(snapformat.ChunkKey(orphan)) {
		t.Fatal("the aborted batch deleted something")
	}
	if sum.ManifestsDeleted != 0 || sum.ChunksDeleted != 0 {
		t.Fatalf("summary = %+v", sum)
	}
	if n := strings.Count(e.logs.String(), "waiting for a pending snapshot attempt"); n != 1 {
		t.Fatalf("wait logged %d times, want once:\n%s", n, e.logs.String())
	}
	if !strings.Contains(e.logs.String(), "generation_id="+stuck.ID.String()) ||
		!strings.Contains(e.logs.String(), "sandbox=bx_stuck") {
		t.Fatalf("the wait log does not name the generation:\n%s", e.logs.String())
	}
	if e.log.count("begin") != e.log.count("end") {
		t.Fatalf("begins and ends differ: %v", e.log.all())
	}
}

// TestSecondCollectorSkips: a round that finds the fence held by another sweep
// stops without error, deletes nothing and does not release a flag it never
// took.
func TestSecondCollectorSkips(t *testing.T) {
	e := newEnv(t)
	orphan := e.chunk("orphan")
	e.advance(48 * time.Hour)
	e.store.refuse = true

	sum, err := e.collector(nil).Round(t.Context())
	if err != nil {
		t.Fatalf("round: %v", err)
	}
	if !sum.Skipped || sum.ChunksDeleted != 0 || !e.has(snapformat.ChunkKey(orphan)) {
		t.Fatalf("summary = %+v, chunk kept = %v", sum, e.has(snapformat.ChunkKey(orphan)))
	}
	if e.log.count("end") != 0 {
		t.Fatal("a skipped round cleared a flag it did not set")
	}
	if !strings.Contains(e.logs.String(), "skipped=true") {
		t.Fatalf("the skip is not in the log:\n%s", e.logs.String())
	}
}

// TestRoundAbortsOnErrors: a manifest that cannot be decoded, a failed
// listing and a failed delete each abort the round, delete nothing beyond what
// was proven dead before the error, and leave no flag behind.
func TestRoundAbortsOnErrors(t *testing.T) {
	t.Run("corrupt manifest", func(t *testing.T) {
		e := newEnv(t)
		orphan := e.chunk("orphan")
		key := snapformat.ManifestKey(newChain(), 0)
		if err := e.mem.PutIfAbsent(t.Context(), key, strings.NewReader("not a zstd frame"), 16); err != nil {
			t.Fatal(err)
		}
		e.liveKeys(key)
		e.advance(48 * time.Hour)
		if _, err := e.collector(nil).Round(t.Context()); err == nil {
			t.Fatal("round with a corrupt live manifest succeeded")
		}
		if !e.has(snapformat.ChunkKey(orphan)) {
			t.Fatal("a chunk was deleted although a manifest could not be read")
		}
	})
	for _, prefix := range []string{snapformat.ManifestPrefix, snapformat.ChunkPrefix} {
		t.Run("list "+prefix, func(t *testing.T) {
			e := newEnv(t)
			orphan := e.chunk("orphan")
			e.advance(48 * time.Hour)
			e.objs.failList = prefix
			if _, err := e.collector(nil).Round(t.Context()); !errors.Is(err, errBoom) {
				t.Fatalf("round error = %v, want the listing error", err)
			}
			if !e.has(snapformat.ChunkKey(orphan)) {
				t.Fatal("a chunk was deleted although a listing failed")
			}
		})
	}
	t.Run("delete", func(t *testing.T) {
		e := newEnv(t)
		e.chunk("orphan")
		e.advance(48 * time.Hour)
		e.objs.failDelete = true
		if _, err := e.collector(nil).Round(t.Context()); !errors.Is(err, errBoom) {
			t.Fatalf("round error = %v, want the delete error", err)
		}
		if e.store.isSweeping() {
			t.Fatal("flag left set after a failed delete")
		}
	})
	t.Run("live query", func(t *testing.T) {
		e := newEnv(t)
		orphan := e.chunk("orphan")
		e.advance(48 * time.Hour)
		failing := &failingLiveStore{fakeStore: e.store}
		c := e.collector(func(o *gc.Options) { o.Store = failing })
		if _, err := c.Round(t.Context()); !errors.Is(err, errBoom) {
			t.Fatalf("round error = %v, want the query error", err)
		}
		if !e.has(snapformat.ChunkKey(orphan)) {
			t.Fatal("a chunk was deleted without a liveness answer")
		}
	})
}

type failingLiveStore struct{ *fakeStore }

func (*failingLiveStore) GCLiveGenerations(context.Context, time.Time) ([]db.GCLiveGeneration, error) {
	return nil, errBoom
}

// TestForeignObjectsAreNeverTouched: keys that are not chunk or manifest keys
// are left alone, however old.
func TestForeignObjectsAreNeverTouched(t *testing.T) {
	e := newEnv(t)
	for _, key := range []string{"chunks/not-a-hash", "manifests/not-a-chain/0.json.zst", "chunks/" + strings.Repeat("A", 64)} {
		if err := e.mem.PutIfAbsent(t.Context(), key, strings.NewReader("x"), 1); err != nil {
			t.Fatal(err)
		}
	}
	e.advance(48 * time.Hour)
	sum, err := e.collector(nil).Round(t.Context())
	if err != nil || sum.ChunksDeleted+sum.ManifestsDeleted != 0 {
		t.Fatalf("round = %+v, %v", sum, err)
	}
	if e.mem.Len() != 3 {
		t.Fatalf("%d objects left, want 3", e.mem.Len())
	}
}

// TestRoundSummaryIsLoggedAtInfo: the summary carries counts and sizes and
// never an object's content.
func TestRoundSummaryIsLoggedAtInfo(t *testing.T) {
	e := newEnv(t)
	referenced := e.chunk("referenced content marker")
	e.chunk("orphan content marker")
	manifest := e.manifest(newChain(), 0, referenced)
	e.liveKeys(manifest)
	e.advance(48 * time.Hour)
	if _, err := e.collector(nil).Round(t.Context()); err != nil {
		t.Fatal(err)
	}
	out := e.logs.String()
	for _, want := range []string{"level=INFO", "gc round finished", "manifests_marked=1", "chunks_marked=1",
		"chunks_kept=1", "chunks_deleted=1", "manifests_deleted=0", "bytes_freed=21"} {
		if !strings.Contains(out, want) {
			t.Errorf("summary log lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "content marker") {
		t.Errorf("the log carries object content:\n%s", out)
	}
}

// TestNewValidates rejects missing parts and negative settings.
func TestNewValidates(t *testing.T) {
	e := newEnv(t)
	for name, mod := range map[string]func(*gc.Options){
		"no store":     func(o *gc.Options) { o.Store = nil },
		"no objects":   func(o *gc.Options) { o.Objects = nil },
		"no logger":    func(o *gc.Options) { o.Logger = nil },
		"negative":     func(o *gc.Options) { o.Grace = -time.Second },
		"negative bat": func(o *gc.Options) { o.Batch = -1 },
	} {
		opts := e.options()
		mod(&opts)
		if _, err := gc.New(opts); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
}

// TestRunRecoversAStaleFlagAtStartAndStops: Run clears a stale flag before its
// first round and returns when its context ends.
func TestRunRecoversAStaleFlagAtStartAndStops(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- e.collector(nil).Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		e.store.mu.Lock()
		n := e.store.recovers
		e.store.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Run did not look for a stale flag")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v", err)
	}
}
