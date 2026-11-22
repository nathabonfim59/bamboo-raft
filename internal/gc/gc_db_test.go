package gc_test

// Collector tests against PostgreSQL and the in-memory object store: the
// liveness rules end to end, the fence against real snapshot attempts and the
// crash recovery. They skip unless BBRAFT_TEST_DATABASE_URL is set (see
// dbtest.Open); every test creates its own schema.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
	"github.com/nathabonfim59/bamboo-raft/internal/gc"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// dbEnv is a migrated store, an owner, a node and an object store whose clock
// the test moves: objects are written at the clock, rows carry real database
// time, and the collector measures its grace on the clock.
type dbEnv struct {
	t   *testing.T
	s   *db.Store
	own uuid.UUID
	mem *memstore.Store

	mu  sync.Mutex
	now time.Time
}

func newDBEnv(t *testing.T) *dbEnv {
	t.Helper()
	s := dbtest.Open(t)
	if err := s.UpsertNodeInfo(t.Context(), db.NodeInfo{
		ID: "node-a", CPUModel: "cpu", QEMUVersion: "9", DefaultImage: "img",
		Capacity: db.Resources{VCPUs: 64, MemoryMiB: 65536}, DiskFreeGiB: 1000, SeenAt: time.Now(),
	}); err != nil {
		t.Fatalf("node: %v", err)
	}
	acct, err := s.EnsureAccount(t.Context(), "acct-"+uuid.NewV7().String())
	if err != nil {
		t.Fatalf("account: %v", err)
	}
	e := &dbEnv{t: t, s: s, own: acct.ID, mem: memstore.New(), now: time.Now()}
	e.mem.SetClock(e.clock)
	return e
}

func (e *dbEnv) clock() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.now
}

// age moves the object clock forward: everything written so far is older.
func (e *dbEnv) age(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.now = e.now.Add(d)
}

func (e *dbEnv) exec(sql string, args ...any) {
	e.t.Helper()
	if _, err := e.s.Pool().Exec(e.t.Context(), sql, args...); err != nil {
		e.t.Fatalf("exec %q: %v", sql, err)
	}
}

// collector builds a collector over the env's clock with a 24 h grace, no
// history and a fast fence poll.
func (e *dbEnv) collector(logs *bytes.Buffer, mod func(*gc.Options)) *gc.Collector {
	e.t.Helper()
	var sink bytes.Buffer
	if logs == nil {
		logs = &sink
	}
	opts := gc.Options{
		Store: e.s, Objects: e.mem,
		Logger: slog.New(slog.NewTextHandler(logs, nil)),
		Grace:  24 * time.Hour, PollInterval: 5 * time.Millisecond, Now: e.clock,
	}
	if mod != nil {
		mod(&opts)
	}
	c, err := gc.New(opts)
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

// round ages the store by 48 h (everything written so far is past the grace)
// and runs one round.
func (e *dbEnv) round() gc.Summary {
	e.t.Helper()
	e.age(48 * time.Hour)
	sum, err := e.collector(nil, nil).Round(e.t.Context())
	if err != nil {
		e.t.Fatalf("round: %v", err)
	}
	return sum
}

// sandbox creates a ready sandbox.
func (e *dbEnv) sandbox() db.Sandbox {
	e.t.Helper()
	res, err := e.s.CreateSandbox(e.t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: e.own, NodeID: "node-a",
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10},
	})
	if err != nil {
		e.t.Fatalf("create sandbox: %v", err)
	}
	e.exec(`UPDATE sandboxes SET state = 'ready' WHERE id = $1`, res.Sandbox.ID)
	return res.Sandbox
}

func (e *dbEnv) chunk(content string) snapformat.Hash {
	e.t.Helper()
	h := snapformat.HashChunk([]byte(content))
	if err := e.mem.PutIfAbsent(e.t.Context(), snapformat.ChunkKey(h), strings.NewReader(content), int64(len(content))); err != nil &&
		!errors.Is(err, objstore.ErrExists) {
		e.t.Fatal(err)
	}
	return h
}

// writeManifest stores the manifest of sbx's chain at number naming chunks.
func (e *dbEnv) writeManifest(sbx db.Sandbox, number uint32, chunks ...snapformat.Hash) string {
	e.t.Helper()
	m := &snapformat.Manifest{
		Version: snapformat.Version, ChainID: sbx.ChainID.String(), Generation: number, SandboxID: sbx.ID,
		Image: "img", DiskBytes: uint64(max(len(chunks), 1)) * snapformat.ChunkSize,
		ChunkSize: snapformat.ChunkSize, CreatedAt: e.clock(),
	}
	for i, h := range chunks {
		m.Chunks = append(m.Chunks, snapformat.Entry{Index: uint64(i), Hash: h})
	}
	raw, err := snapformat.EncodeManifest(m)
	if err != nil {
		e.t.Fatal(err)
	}
	key := snapformat.ManifestKey(sbx.ChainID.String(), number)
	if err := e.mem.PutIfAbsent(e.t.Context(), key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		e.t.Fatal(err)
	}
	return key
}

// completed writes a manifest and a completed row cut at cut.
func (e *dbEnv) completed(sbx db.Sandbox, number uint32, cut time.Time, chunks ...snapformat.Hash) db.Generation {
	e.t.Helper()
	key := e.writeManifest(sbx, number, chunks...)
	id := uuid.NewV7()
	kind := "incremental"
	if number == 0 {
		kind = "base"
	}
	e.exec(`
		INSERT INTO snapshots (id, owner_id, sandbox_id, chain_id, generation, kind, status,
			manifest_key, node_id, image, disk_bytes, chunks, chunks_uploaded, bytes_uploaded,
			content_bytes, frozen, cut_at, started_at, completed_at, deadline_at)
		VALUES ($1, $2, $3, $4, $5, $6, 'completed', $7, 'node-a', 'img', $8, 1, 1, 1, 1,
			true, $9, $9, $9, $9)`,
		id, e.own, sbx.ID, sbx.ChainID, int64(number), kind, key,
		int64(max(len(chunks), 1))*snapformat.ChunkSize, cut)
	return db.Generation{ID: id, SandboxID: sbx.ID, ChainID: sbx.ChainID, Number: number, ManifestKey: key}
}

// failed writes a manifest and a failed row: an attempt that uploaded and
// wrote its manifest but did not complete.
func (e *dbEnv) failed(sbx db.Sandbox, number uint32, chunks ...snapformat.Hash) db.Generation {
	e.t.Helper()
	key := e.writeManifest(sbx, number, chunks...)
	id := uuid.NewV7()
	e.exec(`
		INSERT INTO snapshots (id, owner_id, sandbox_id, chain_id, generation, kind, status,
			manifest_key, node_id, error, started_at, deadline_at)
		VALUES ($1, $2, $3, $4, $5, 'incremental', 'failed', $6, 'node-a', 'timeout',
			now() - interval '2 hours', now() - interval '1 hour')`,
		id, e.own, sbx.ID, sbx.ChainID, int64(number), key)
	return db.Generation{ID: id, SandboxID: sbx.ID, ChainID: sbx.ChainID, Number: number, ManifestKey: key}
}

// attempt is a snapshot attempt begun through the real store under a live job.
type attempt struct {
	gen db.Generation
}

// begin begins a generation of sbx under a live claim.
func (e *dbEnv) begin(sbx db.Sandbox) attempt {
	e.t.Helper()
	g, err := e.tryBegin(sbx)
	if err != nil {
		e.t.Fatalf("begin generation: %v", err)
	}
	return attempt{gen: g}
}

func (e *dbEnv) tryBegin(sbx db.Sandbox) (db.Generation, error) {
	e.t.Helper()
	jobID, token := uuid.NewV7(), uuid.NewV7()
	e.exec(`
		INSERT INTO jobs (id, kind, owner_id, sandbox_id, state, attempts, leased_by, leased_until, lease_token)
		VALUES ($1, 'sandbox.snapshot', $2, $3, 'running', 1, 'gc-test', now() + interval '1 hour', $4)`,
		jobID, e.own, sbx.ID, token)
	return e.s.BeginGeneration(e.t.Context(), db.BeginGenerationParams{
		Owner: e.own, SandboxID: sbx.ID, Timeout: time.Hour, JobID: jobID, LeaseToken: token,
	})
}

// complete writes the attempt's manifest naming chunks and completes its row.
func (e *dbEnv) complete(a attempt, sbx db.Sandbox, chunks ...snapformat.Hash) db.Generation {
	e.t.Helper()
	e.writeManifest(sbx, a.gen.Number, chunks...)
	done, err := e.s.CompleteGeneration(e.t.Context(), a.gen.ID, db.GenerationResult{
		Image: "img", DiskBytes: snapformat.ChunkSize, Chunks: 1, ChunksUploaded: 0,
		ContentBytes: 1, Frozen: true, CutAt: time.Now(),
	}, time.Now().Add(time.Hour))
	if err != nil {
		e.t.Fatalf("complete generation: %v", err)
	}
	return done
}

func (e *dbEnv) has(key string) bool {
	e.t.Helper()
	_, err := e.mem.Head(e.t.Context(), key)
	if err != nil && !errors.Is(err, objstore.ErrNotFound) {
		e.t.Fatal(err)
	}
	return err == nil
}

func (e *dbEnv) hasChunk(h snapformat.Hash) bool { return e.has(snapformat.ChunkKey(h)) }

// restorable reports whether the generation's manifest exists and every chunk
// it names does too: what a restore needs.
func (e *dbEnv) restorable(g db.Generation) bool {
	e.t.Helper()
	rc, err := e.mem.Get(e.t.Context(), g.ManifestKey, 0, -1)
	if err != nil {
		return false
	}
	defer rc.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		e.t.Fatal(err)
	}
	m, err := snapformat.DecodeManifest(buf.Bytes())
	if err != nil {
		e.t.Fatal(err)
	}
	for key := range m.ChunkKeys() {
		if !e.has(key) {
			return false
		}
	}
	return true
}

func (e *dbEnv) purged(g db.Generation) bool {
	e.t.Helper()
	var at *time.Time
	if err := e.s.Pool().QueryRow(e.t.Context(), `SELECT purged_at FROM snapshots WHERE id = $1`, g.ID).Scan(&at); err != nil {
		e.t.Fatal(err)
	}
	return at != nil
}

func (e *dbEnv) deleteSandbox(sbx db.Sandbox) {
	e.t.Helper()
	e.exec(`UPDATE sandboxes SET deleted_at = now() WHERE id = $1`, sbx.ID)
}

func (e *dbEnv) expireSandbox(sbx db.Sandbox) {
	e.t.Helper()
	e.exec(`UPDATE sandboxes SET data_expired_at = now() WHERE id = $1`, sbx.ID)
}

// eventually polls cond for up to ten seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// roundAsync runs a round in a goroutine.
func roundAsync(ctx context.Context, c *gc.Collector) <-chan roundResult {
	out := make(chan roundResult, 1)
	go func() {
		sum, err := c.Round(ctx)
		out <- roundResult{sum, err}
	}()
	return out
}

type roundResult struct {
	sum gc.Summary
	err error
}

func (e *dbEnv) sweeping() bool {
	e.t.Helper()
	v, err := e.s.GCSweeping(e.t.Context())
	if err != nil {
		e.t.Fatal(err)
	}
	return v
}

// TestForkSourceSurvivesTheSweepBeforeTheFirstGeneration: fork a sandbox's
// latest generation to B, delete the source, sweep before B has a generation
// of its own, then cut B's first generation: it names chunks the source's
// manifest named, and it restores. Once B has its own generation the source is
// released and collected.
func TestForkSourceSurvivesTheSweepBeforeTheFirstGeneration(t *testing.T) {
	e := newDBEnv(t)
	ctx := t.Context()
	a, b, c := e.chunk("chunk a"), e.chunk("chunk b"), e.chunk("chunk c")
	src := e.sandbox()
	gen0 := e.completed(src, 0, time.Now().Add(-2*time.Hour), a, b)
	gen1 := e.completed(src, 1, time.Now().Add(-time.Hour), b, c)

	// The fork: a manifest restore of A's latest generation, through the
	// provision job's READY transition.
	res, err := e.s.CreateSandbox(ctx, db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: e.own, NodeID: "node-a",
		Reserve:      db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10},
		FromManifest: gen1.ManifestKey, SourceGenerationID: &gen1.ID,
	})
	if err != nil {
		t.Fatalf("create fork: %v", err)
	}
	fork := res.Sandbox
	var job db.Job
	for range 20 {
		j, err := e.s.ClaimJob(ctx, "gc-worker", time.Minute)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if j.SandboxID != nil && *j.SandboxID == fork.ID {
			job = j
			break
		}
		if err := e.s.CompleteJob(ctx, j.ID, "gc-worker", j.LeaseToken); err != nil {
			t.Fatal(err)
		}
	}
	if job.ID == uuid.Nil() {
		t.Fatal("the fork's provision job never appeared")
	}
	if _, err := e.s.TransitionSandboxJob(ctx, job.ID, "gc-worker", job.LeaseToken, db.TransitionParams{
		SandboxID: fork.ID, Owner: e.own, FromStates: []db.SandboxState{db.StateProvisioning},
		ExpectedVersion: fork.Version, To: db.StateReady, Hydrated: true,
	}); err != nil {
		t.Fatalf("fork READY: %v", err)
	}
	if err := e.s.CompleteJob(ctx, job.ID, "gc-worker", job.LeaseToken); err != nil {
		t.Fatal(err)
	}
	e.deleteSandbox(src)

	// Sweep before the fork has a generation of its own.
	e.round()
	if !e.restorable(gen1) {
		t.Fatal("the fork's restore source lost its manifest or chunks to the sweep")
	}

	// The fork's first generation skips uploading the chunks that exist.
	att := e.begin(fork)
	first := e.complete(att, fork, b, c)
	if !e.restorable(first) {
		t.Fatal("the fork's first generation does not restore")
	}

	// With a generation of its own, the source is dead: its manifest goes
	// (the older one went in the first sweep, with its exclusive chunk), the
	// shared chunks stay.
	e.round()
	if e.hasChunk(a) || e.has(gen0.ManifestKey) || e.has(gen1.ManifestKey) {
		t.Error("the released source was not collected")
	}
	if !e.hasChunk(b) || !e.hasChunk(c) || !e.restorable(first) {
		t.Error("the fork's generation lost chunks it shares with the source")
	}
	if !e.purged(gen0) || !e.purged(gen1) || e.purged(first) {
		t.Error("purged_at does not match what was collected")
	}
}

// TestFailedButWrittenGenerationSurvivesAndTheNextOneRestores: an attempt
// newer than the latest completed generation failed after writing its
// manifest and chunks. With the clock far past the grace it survives the
// sweep, so the next attempt may skip uploading its chunks and still restore.
func TestFailedButWrittenGenerationSurvivesAndTheNextOneRestores(t *testing.T) {
	e := newDBEnv(t)
	base, written := e.chunk("base chunk"), e.chunk("chunk only the failed attempt uploaded")
	sbx := e.sandbox()
	e.completed(sbx, 0, time.Now().Add(-time.Hour), base)
	failed := e.failed(sbx, 1, base, written)

	e.round()
	if !e.hasChunk(written) || !e.has(failed.ManifestKey) {
		t.Fatal("a failed but written generation newer than the latest completed one was collected")
	}

	// The next attempt skips uploading the chunk that already exists.
	att := e.begin(sbx)
	if att.gen.Number != 2 {
		t.Fatalf("next generation number = %d, want 2", att.gen.Number)
	}
	next := e.complete(att, sbx, base, written)
	if !e.restorable(next) {
		t.Fatal("the generation built on the failed attempt does not restore")
	}

	// Now the failed attempt is older than the latest completed generation:
	// its manifest goes, its chunk stays because the new manifest names it.
	e.round()
	if e.has(failed.ManifestKey) || !e.purged(failed) {
		t.Error("the superseded failed attempt was not collected")
	}
	if !e.restorable(next) {
		t.Error("collecting the failed attempt broke the generation that reuses its chunk")
	}
}

// TestExpiredAndDeletedChainsAreCollectedExceptPins: the chains of deleted and
// expired sandboxes are collected; a generation a saved snapshot or a running
// job pins, and a live sandbox's latest, stay.
func TestExpiredAndDeletedChainsAreCollectedExceptPins(t *testing.T) {
	e := newDBEnv(t)
	cut := time.Now().Add(-time.Hour)
	type chain struct {
		sbx   db.Sandbox
		chunk snapformat.Hash
		gen   db.Generation
	}
	mk := func(name string) chain {
		sbx := e.sandbox()
		h := e.chunk("chunk of " + name)
		return chain{sbx: sbx, chunk: h, gen: e.completed(sbx, 0, cut, h)}
	}
	deleted, expired, savedPin, jobPin, live := mk("deleted"), mk("expired"), mk("saved"), mk("job"), mk("live")
	e.deleteSandbox(deleted.sbx)
	e.expireSandbox(expired.sbx)
	e.deleteSandbox(savedPin.sbx)
	e.exec(`
		INSERT INTO saved_snapshots (id, owner_id, name, sandbox_id, status, artifact_id, generation, generation_id)
		VALUES ($1, $2, 'keep', $3, 'ready', $4, 1, $5)`,
		uuid.NewV7(), e.own, savedPin.sbx.ID, "snap-"+strings.ReplaceAll(uuid.NewV7().String(), "-", ""), savedPin.gen.ID)
	e.deleteSandbox(jobPin.sbx)
	e.exec(`
		INSERT INTO jobs (id, kind, owner_id, sandbox_id, state, source_generation_id)
		VALUES ($1, 'sandbox.provision', $2, $3, 'running', $4)`,
		uuid.NewV7(), e.own, live.sbx.ID, jobPin.gen.ID)

	sum := e.round()
	for name, c := range map[string]chain{"deleted": deleted, "expired": expired} {
		if e.hasChunk(c.chunk) || e.has(c.gen.ManifestKey) || !e.purged(c.gen) {
			t.Errorf("%s sandbox's chain was not collected", name)
		}
	}
	for name, c := range map[string]chain{"saved-pinned": savedPin, "job-pinned": jobPin, "live": live} {
		if !e.restorable(c.gen) || e.purged(c.gen) {
			t.Errorf("%s generation was collected", name)
		}
	}
	if sum.ManifestsDeleted != 2 || sum.ChunksDeleted != 2 {
		t.Errorf("summary = %+v, want 2 manifests and 2 chunks", sum)
	}

	// The job finishes: its generation is no longer pinned.
	e.exec(`UPDATE jobs SET state = 'done' WHERE source_generation_id = $1`, jobPin.gen.ID)
	e.round()
	if e.hasChunk(jobPin.chunk) || e.has(jobPin.gen.ManifestKey) {
		t.Error("the generation of a finished job stayed")
	}
	// A deleted saved name releases its pin.
	e.exec(`UPDATE saved_snapshots SET deleted_at = now() WHERE generation_id = $1`, savedPin.gen.ID)
	e.round()
	if e.hasChunk(savedPin.chunk) || !e.purged(savedPin.gen) {
		t.Error("the generation of a deleted saved snapshot stayed")
	}
	if !e.restorable(live.gen) {
		t.Error("the live sandbox's generation was collected")
	}
}

// TestSkipRaceFence: an attempt that is pending when the sweep starts may skip
// uploading a chunk the sweep is about to delete. The sweep waits for it, no
// new attempt may begin meanwhile, and when the attempt completes the re-mark
// sees its manifest: the chunk stays and the generation restores.
func TestSkipRaceFence(t *testing.T) {
	e := newDBEnv(t)
	ctx := t.Context()
	skipped := e.chunk("a chunk only a dead generation names")
	keep := e.chunk("kept chunk")
	dead := e.sandbox()
	e.completed(dead, 0, time.Now().Add(-time.Hour), skipped)
	e.deleteSandbox(dead)

	sbx, other := e.sandbox(), e.sandbox()
	e.completed(sbx, 0, time.Now().Add(-time.Hour), keep)
	att := e.begin(sbx)

	e.age(48 * time.Hour)
	var logs bytes.Buffer
	done := roundAsync(ctx, e.collector(&logs, nil))
	eventually(t, "the sweep to take the fence", e.sweeping)

	// The fence refuses a new attempt while the sweep waits for the pending one.
	if _, err := e.tryBegin(other); !errors.Is(err, db.ErrGCSweeping) {
		t.Fatalf("begin while the sweep waits = %v, want ErrGCSweeping", err)
	}
	select {
	case r := <-done:
		t.Fatalf("the sweep finished with an attempt pending: %+v %v", r.sum, r.err)
	case <-time.After(100 * time.Millisecond):
	}

	// The attempt skips uploading the chunk that exists, and completes.
	next := e.complete(att, sbx, keep, skipped)
	r := <-done
	if r.err != nil {
		t.Fatalf("round: %v", r.err)
	}
	if !e.hasChunk(skipped) || !e.restorable(next) {
		t.Fatalf("the chunk the attempt skipped was deleted, or the generation does not restore (summary %+v)", r.sum)
	}
	if r.sum.ChunksDeleted != 0 || r.sum.ManifestsDeleted != 1 {
		t.Errorf("summary = %+v, want the dead manifest only", r.sum)
	}
	if e.sweeping() {
		t.Error("the flag is still set after the round")
	}
	if _, err := e.tryBegin(other); err != nil {
		t.Errorf("begin after the sweep: %v", err)
	}
	if n := strings.Count(logs.String(), "waiting for a pending snapshot attempt"); n != 1 {
		t.Errorf("wait logged %d times, want once", n)
	}
}

// TestAttemptThatBeginsAfterTheBatchSeesTheChunkGone: the other half of the
// fence. A chunk the sweep deleted is deleted before the flag clears, so an
// attempt that begins afterwards finds it absent and uploads it again.
func TestAttemptThatBeginsAfterTheBatchSeesTheChunkGone(t *testing.T) {
	e := newDBEnv(t)
	orphan := e.chunk("an orphan")
	sbx := e.sandbox()
	e.round()
	if e.hasChunk(orphan) {
		t.Fatal("the orphan survived")
	}
	att := e.begin(sbx)
	// The attempt re-uploads it (it is absent) and completes.
	re := e.chunk("an orphan")
	if g := e.complete(att, sbx, re); !e.restorable(g) {
		t.Fatal("the generation does not restore")
	}
}

// TestTwoCollectorsOneSweeps: while one collector holds the fence (waiting for
// a pending attempt) a second collector's round skips: it deletes nothing and
// leaves the first one's flag alone.
func TestTwoCollectorsOneSweeps(t *testing.T) {
	e := newDBEnv(t)
	ctx := t.Context()
	orphan := e.chunk("orphan")
	sbx := e.sandbox()
	att := e.begin(sbx)

	e.age(48 * time.Hour)
	first := roundAsync(ctx, e.collector(nil, nil))
	eventually(t, "the first collector to take the fence", e.sweeping)

	var logs bytes.Buffer
	sum, err := e.collector(&logs, nil).Round(ctx)
	if err != nil {
		t.Fatalf("second round: %v", err)
	}
	if !sum.Skipped || sum.ChunksDeleted != 0 {
		t.Fatalf("second round = %+v, want a skip", sum)
	}
	if !e.sweeping() {
		t.Fatal("the second collector cleared the first one's flag")
	}
	if !e.hasChunk(orphan) {
		t.Fatal("the second collector deleted a chunk")
	}

	e.complete(att, sbx, e.chunk("the attempt's own chunk"))
	r := <-first
	if r.err != nil || r.sum.Skipped || r.sum.ChunksDeleted != 1 {
		t.Fatalf("first round = %+v, %v; want it to finish and delete the orphan", r.sum, r.err)
	}
	if e.hasChunk(orphan) || e.sweeping() {
		t.Fatal("the first collector did not finish its sweep")
	}
}

// TestAbortedWaitDeletesNothingAndClearsTheFlag: a pending attempt whose owner
// is gone keeps the fence waiting past the cap. The round stops, the flag is
// cleared, nothing is deleted and snapshot attempts can begin again.
func TestAbortedWaitDeletesNothingAndClearsTheFlag(t *testing.T) {
	e := newDBEnv(t)
	orphan := e.chunk("orphan")
	dead := e.sandbox()
	deadManifest := e.completed(dead, 0, time.Now().Add(-time.Hour), e.chunk("dead sandbox chunk"))
	e.deleteSandbox(dead)
	sbx := e.sandbox()
	e.begin(sbx) // pending for an hour, owner never finishes it

	e.age(48 * time.Hour)
	var logs bytes.Buffer
	c := e.collector(&logs, func(o *gc.Options) {
		o.SnapshotTimeout = 60 * time.Millisecond
		o.PendingMargin = 20 * time.Millisecond
	})
	start := time.Now()
	sum, err := c.Round(t.Context())
	if !errors.Is(err, gc.ErrPendingTimeout) {
		t.Fatalf("round error = %v, want ErrPendingTimeout", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("the wait ignored its cap")
	}
	if e.sweeping() {
		t.Fatal("the flag is still set after the abort")
	}
	if !e.hasChunk(orphan) || !e.has(deadManifest.ManifestKey) || e.purged(deadManifest) {
		t.Fatal("the aborted round deleted or purged something")
	}
	if sum.ChunksDeleted != 0 || sum.ManifestsDeleted != 0 {
		t.Fatalf("summary = %+v", sum)
	}
	if _, err := e.tryBegin(e.sandbox()); err != nil {
		t.Fatalf("begin after the abort: %v", err)
	}
	if n := strings.Count(logs.String(), "waiting for a pending snapshot attempt"); n != 1 {
		t.Errorf("wait logged %d times, want once:\n%s", n, logs.String())
	}
}

// TestCrashMidSweepIsRecovered: a collector that dies with the flag set leaves
// snapshot attempts refused. A fresh flag is respected (the round skips), a
// stale one is cleared by Run at start and taken over by a round.
func TestCrashMidSweepIsRecovered(t *testing.T) {
	e := newDBEnv(t)
	ctx := t.Context()
	orphan := e.chunk("orphan")
	sbx := e.sandbox()

	// The crashed sweep's flag, fresh: another sweep may be running.
	if ok, err := e.s.GCBeginSweep(ctx, time.Hour); err != nil || !ok {
		t.Fatalf("begin = %v, %v", ok, err)
	}
	if _, err := e.tryBegin(sbx); !errors.Is(err, db.ErrGCSweeping) {
		t.Fatalf("begin generation under a crashed flag = %v, want ErrGCSweeping", err)
	}
	e.age(48 * time.Hour)
	sum, err := e.collector(nil, nil).Round(ctx)
	if err != nil || !sum.Skipped || !e.hasChunk(orphan) {
		t.Fatalf("round under a fresh flag = %+v, %v; want a skip", sum, err)
	}

	// Run recovers it once it is stale.
	e.exec(`UPDATE gc_state SET sweep_started_at = now() - interval '3 hours'`)
	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- e.collector(nil, nil).Run(runCtx) }()
	eventually(t, "Run to clear the stale flag", func() bool { return !e.sweeping() })
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("Run = %v", err)
	}
	g, err := e.tryBegin(sbx)
	if err != nil {
		t.Fatalf("begin generation after recovery: %v", err)
	}
	// Its owner gives up and its call's deadline has passed, so the takeover
	// round below has nothing to wait for (a failed attempt whose deadline is
	// still ahead holds the fence, see TestGCInFlight in internal/db).
	if _, err := e.s.FailGeneration(ctx, g.ID, "test", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	e.exec(`UPDATE snapshots SET deadline_at = now() - interval '1 hour' WHERE id = $1`, g.ID)

	// A round takes over a stale flag by itself.
	e.exec(`UPDATE gc_state SET sweeping = true, sweep_started_at = now() - interval '3 hours'`)
	sum, err = e.collector(nil, nil).Round(ctx)
	if err != nil || sum.Skipped || sum.ChunksDeleted != 1 {
		t.Fatalf("round over a stale flag = %+v, %v; want it to sweep", sum, err)
	}
	if e.sweeping() {
		t.Fatal("flag left set after the takeover round")
	}
}
