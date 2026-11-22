// Package gc is the chunk garbage collector: it deletes the chunk and
// manifest objects of the object store that no generation can still restore.
// Only the server deletes objects, and only this package does.
//
// # Liveness
//
// A generation is live (its manifest and chunks are kept) when
//
//  1. it is its chain's latest completed generation on a sandbox that is
//     neither deleted nor expired, or a completed one younger than the history
//     window;
//  2. it is the restore source of such a sandbox (the generation its disk was
//     restored from) until the sandbox has a completed generation of its own
//     cut after the restore;
//  3. it is newer than the latest completed generation of such a sandbox, in
//     any status, as long as its manifest object exists: a failed attempt may
//     have written a manifest whose chunks the next attempt skipped uploading;
//  4. a live saved snapshot or a queued or running job names it;
//  5. it is pending, for any sandbox: an attempt may still be writing it.
//
// db.Store.GCLiveGenerations computes the set in PostgreSQL.
//
// # A round
//
// The mark reads every manifest object that is not a sweep candidate (live,
// or younger than the grace period, so a generation that stopped being live
// but is still listed and restorable keeps its chunks) and unions their chunk
// hashes. The sweep candidates are the manifest objects of generations that
// are not live and are older than the grace period, and the chunk objects that
// no marked manifest names and that are older than the grace period. A
// listing, read or decode error aborts the round: nothing is deleted that the
// round could not prove dead.
//
// # The fence
//
// An attempt that skips uploading a chunk because it already exists races the
// sweep that is about to delete it. The sweep therefore works in batches, and
// each batch runs under the fence: gc_state.sweeping is set (serializing with
// the transaction that begins an attempt, which then refuses with
// db.ErrGCSweeping), the sweep waits until no attempt that could still be
// writing remains (pending, or failed by a takeover before its deadline),
// re-marks (reads the manifests of every generation that became live or was
// written since the mark, which covers attempts that finished while it waited
// and new pins), purges the batch's generation rows and only then deletes the
// objects, and clears the flag. An attempt whose owner died keeps the fence
// waiting until its deadline plus a margin, and so does a failed or
// taken-over one: only the attempt's own deadline proves its call ended. The
// wait is capped (the snapshot timeout plus the margin) and a round that hits
// the cap clears the flag and stops without deleting that batch.
//
// A crash leaves the flag set. The flag records when its sweep started, and
// a flag older than the snapshot timeout plus the margin plus ten minutes is
// stale: the collector clears it at start and then every minute, and a sweep
// that finds a stale flag takes it over.
package gc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

// Defaults of the collector.
const (
	// DefaultInterval is the time between rounds.
	DefaultInterval = 6 * time.Hour
	// DefaultGrace is how old an object must be before it can be deleted.
	DefaultGrace = 24 * time.Hour
	// DefaultHistory is how long completed generations other than the latest
	// stay live.
	DefaultHistory = 24 * time.Hour
	// DefaultBatch is the number of objects deleted per fenced batch.
	DefaultBatch = 1000
	// DefaultSnapshotTimeout bounds a snapshot attempt (the lifecycle
	// default); with the margin it caps how long a sweep waits.
	DefaultSnapshotTimeout = 600 * time.Second
	// DefaultPendingMargin is added to an in-flight attempt's deadline before
	// the sweep stops waiting for it.
	DefaultPendingMargin = time.Minute
	// DefaultPollInterval is how often the fence rechecks for in-flight
	// attempts.
	DefaultPollInterval = 2 * time.Second

	// staleSlack is added to the wait cap to get the age at which a left-over
	// sweeping flag is a crashed sweep's.
	staleSlack = 10 * time.Minute
	// recoverEvery is how often Run looks for a stale flag between rounds.
	recoverEvery = time.Minute
	// firstRoundDelay is the wait before the first round after start.
	firstRoundDelay = time.Minute
	// endSweepTimeout bounds clearing the flag after a sweep, which must
	// happen even when the round's context is gone.
	endSweepTimeout = 30 * time.Second
)

// Errors of a round.
var (
	// ErrPendingTimeout means an attempt that could still be writing kept the
	// fence waiting past the cap; the batch was not deleted and the flag was
	// cleared.
	ErrPendingTimeout = errors.New("gc: pending snapshot attempts did not finish within the wait cap")
)

// Store is the database surface the collector uses; *db.Store implements it.
type Store interface {
	GCLiveGenerations(ctx context.Context, historyCutoff time.Time) ([]db.GCLiveGeneration, error)
	GCBeginSweep(ctx context.Context, staleAfter time.Duration) (bool, error)
	GCEndSweep(ctx context.Context) error
	GCRecoverStaleSweep(ctx context.Context, staleAfter time.Duration) (bool, error)
	GCInFlightGenerations(ctx context.Context, margin time.Duration) ([]db.GCInFlightGeneration, error)
	GCPurgeGenerations(ctx context.Context, historyCutoff time.Time, manifestKeys []string) ([]string, error)
}

// Options configures a Collector. Store, Objects and Logger are required.
type Options struct {
	Store   Store
	Objects objstore.Store
	Logger  *slog.Logger

	// Interval is the time between rounds and Batch the objects deleted per
	// fenced batch; zero selects the default (DefaultInterval, DefaultBatch).
	// Grace is the minimum age of an object before it is deleted and History
	// how long completed generations other than a chain's latest stay live;
	// both are used as given, so the caller supplies DefaultGrace and
	// DefaultHistory (zero deletes anything unreferenced at once and is for
	// tests). Negative values are rejected.
	Interval, Grace, History time.Duration
	Batch                    int
	// SnapshotTimeout is the longest a snapshot attempt may run (the
	// lifecycle's -lifecycle.snapshot-timeout-seconds) and PendingMargin the
	// slack added to an attempt's deadline. Together they cap how long a
	// batch waits for in-flight attempts, and so how long the sweeping flag
	// stays set. PollInterval is how often the fence rechecks.
	SnapshotTimeout, PendingMargin, PollInterval time.Duration

	// Now is the clock the grace and history ages are measured on (compared
	// with the objects' modification times); nil uses time.Now. Tests set it.
	Now func() time.Time
}

// Summary is what one round did.
type Summary struct {
	// ManifestsMarked is the number of manifests read for the mark and
	// ChunksMarked the distinct chunks they name.
	ManifestsMarked, ChunksMarked int
	// ChunksKept is the number of listed chunks not deleted: marked or
	// younger than the grace period.
	ChunksKept int
	// ChunksDeleted and ManifestsDeleted count deleted objects and BytesFreed
	// their stored size.
	ChunksDeleted, ManifestsDeleted int
	BytesFreed                      int64
	// Batches is the number of fenced batches.
	Batches int
	// Skipped is true when another sweep held the fence and the round
	// stopped without finishing.
	Skipped bool
}

// Collector runs rounds. Construct it with New.
type Collector struct {
	store   Store
	objects objstore.Store
	log     *slog.Logger

	interval, grace, history time.Duration
	batch                    int
	maxWait, margin, poll    time.Duration
	staleAfter               time.Duration
	now                      func() time.Time
}

// New validates opts and returns a Collector.
func New(opts Options) (*Collector, error) {
	if opts.Store == nil || opts.Objects == nil || opts.Logger == nil {
		return nil, errors.New("gc: New needs a Store, an object store and a logger")
	}
	if opts.Interval < 0 || opts.Grace < 0 || opts.History < 0 || opts.Batch < 0 ||
		opts.SnapshotTimeout < 0 || opts.PendingMargin < 0 || opts.PollInterval < 0 {
		return nil, errors.New("gc: durations and batch must not be negative")
	}
	c := &Collector{
		store:    opts.Store,
		objects:  opts.Objects,
		log:      opts.Logger,
		interval: durationOr(opts.Interval, DefaultInterval),
		grace:    opts.Grace,
		history:  opts.History,
		batch:    opts.Batch,
		margin:   durationOr(opts.PendingMargin, DefaultPendingMargin),
		poll:     durationOr(opts.PollInterval, DefaultPollInterval),
		now:      opts.Now,
	}
	if c.batch == 0 {
		c.batch = DefaultBatch
	}
	c.maxWait = durationOr(opts.SnapshotTimeout, DefaultSnapshotTimeout) + c.margin
	c.staleAfter = c.maxWait + staleSlack
	if c.now == nil {
		c.now = time.Now
	}
	return c, nil
}

func durationOr(d, def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return d
}

// Run recovers a stale sweeping flag, then runs a round every interval (the
// first shortly after start) and looks for a stale flag every minute, until
// ctx ends. A failed round is logged and the next one starts on schedule.
func (c *Collector) Run(ctx context.Context) error {
	c.recoverStale(ctx)
	round := time.NewTimer(min(firstRoundDelay, c.interval))
	defer round.Stop()
	recoverTick := time.NewTicker(recoverEvery)
	defer recoverTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-recoverTick.C:
			c.recoverStale(ctx)
		case <-round.C:
			if _, err := c.Round(ctx); err != nil && ctx.Err() == nil {
				c.log.Warn("gc round failed", "error", err)
			}
			round.Reset(c.interval)
		}
	}
}

func (c *Collector) recoverStale(ctx context.Context) {
	cleared, err := c.store.GCRecoverStaleSweep(ctx, c.staleAfter)
	switch {
	case err != nil:
		if ctx.Err() == nil {
			c.log.Warn("gc stale sweep check failed", "error", err)
		}
	case cleared:
		c.log.Warn("gc cleared a stale sweeping flag left by a crashed sweep")
	}
}

// round is the state of one Round: the marked chunk hashes, the manifests
// already read and the running summary.
type round struct {
	c          *Collector
	cutoff     time.Time // history cutoff for liveness
	graceCut   time.Time // objects modified after this are too young to delete
	marked     map[snapformat.Hash]struct{}
	read       map[string]struct{}
	sum        Summary
	waitLogged bool
}

// Round runs one mark and sweep and logs its summary. A round that cannot
// finish returns its error together with the summary of what it did; a round
// that finds another sweep holding the fence returns Skipped and no error.
func (c *Collector) Round(ctx context.Context) (Summary, error) {
	now := c.now()
	r := &round{
		c:        c,
		cutoff:   now.Add(-c.history),
		graceCut: now.Add(-c.grace),
		marked:   map[snapformat.Hash]struct{}{},
		read:     map[string]struct{}{},
	}
	err := r.run(ctx)
	if errors.Is(err, errSkipped) {
		err = nil
	}
	r.sum.ChunksMarked = len(r.marked)
	attrs := []any{
		"manifests_marked", r.sum.ManifestsMarked, "chunks_marked", r.sum.ChunksMarked,
		"chunks_kept", r.sum.ChunksKept, "chunks_deleted", r.sum.ChunksDeleted,
		"manifests_deleted", r.sum.ManifestsDeleted, "bytes_freed", r.sum.BytesFreed,
		"batches", r.sum.Batches, "skipped", r.sum.Skipped,
	}
	if err != nil {
		c.log.Warn("gc round aborted", append(attrs, "error", err)...)
	} else {
		c.log.Info("gc round finished", attrs...)
	}
	return r.sum, err
}

func (r *round) run(ctx context.Context) error {
	live, err := r.c.store.GCLiveGenerations(ctx, r.cutoff)
	if err != nil {
		return err
	}
	liveKeys := make(map[string]struct{}, len(live))
	for _, g := range live {
		liveKeys[g.ManifestKey] = struct{}{}
	}

	// Mark every manifest that is not a sweep candidate; collect the rest.
	var candidates []objstore.Info
	for info, err := range r.c.objects.List(ctx, snapformat.ManifestPrefix) {
		if err != nil {
			return fmt.Errorf("gc: list manifests: %w", err)
		}
		if _, _, perr := snapformat.ParseManifestKey(info.Key); perr != nil {
			continue // not ours: never touched
		}
		if _, ok := liveKeys[info.Key]; !ok && info.Modified.Before(r.graceCut) {
			candidates = append(candidates, info)
			continue
		}
		if err := r.mark(ctx, info.Key); err != nil {
			return err
		}
	}

	for len(candidates) > 0 {
		n := min(r.c.batch, len(candidates))
		batch := candidates[:n]
		candidates = candidates[n:]
		if err := r.fenced(ctx, func() error { return r.sweepManifests(ctx, batch) }); err != nil {
			return err
		}
	}

	var batch []objstore.Info
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		b := batch
		batch = nil
		return r.fenced(ctx, func() error { return r.sweepChunks(ctx, b) })
	}
	for info, err := range r.c.objects.List(ctx, snapformat.ChunkPrefix) {
		if err != nil {
			return fmt.Errorf("gc: list chunks: %w", err)
		}
		hash, perr := snapformat.ParseChunkKey(info.Key)
		if perr != nil {
			continue // not ours: never touched
		}
		if _, ok := r.marked[hash]; ok || !info.Modified.Before(r.graceCut) {
			r.sum.ChunksKept++
			continue
		}
		batch = append(batch, info)
		if len(batch) >= r.c.batch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// mark reads one manifest object and adds its chunks to the mark. A missing
// object marks nothing (a pending or failed attempt that never wrote it, or a
// manifest deleted meanwhile) and is read again by a later re-mark.
func (r *round) mark(ctx context.Context, key string) error {
	if _, ok := r.read[key]; ok {
		return nil
	}
	rc, err := r.c.objects.Get(ctx, key, 0, -1)
	if errors.Is(err, objstore.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("gc: read manifest %s: %w", key, err)
	}
	defer rc.Close()
	// The object is a zstd frame; the decoder bounds the decoded size.
	raw, err := io.ReadAll(io.LimitReader(rc, snapformat.MaxManifestBytes+1))
	if err != nil {
		return fmt.Errorf("gc: read manifest %s: %w", key, err)
	}
	m, err := snapformat.DecodeManifest(raw)
	if err != nil {
		return fmt.Errorf("gc: manifest %s: %w", key, err)
	}
	for _, e := range m.Chunks {
		if !e.Zero {
			r.marked[e.Hash] = struct{}{}
		}
	}
	r.read[key] = struct{}{}
	r.sum.ManifestsMarked++
	return nil
}

// fenced runs fn as one batch under the sweep fence: flag set, pending
// attempts waited for, marks refreshed, then fn, then the flag cleared.
func (r *round) fenced(ctx context.Context, fn func() error) (err error) {
	ok, err := r.c.store.GCBeginSweep(ctx, r.c.staleAfter)
	if err != nil {
		return err
	}
	if !ok {
		r.sum.Skipped = true
		return errSkipped
	}
	r.sum.Batches++
	defer func() {
		endCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), endSweepTimeout)
		defer cancel()
		if endErr := r.c.store.GCEndSweep(endCtx); endErr != nil {
			err = errors.Join(err, fmt.Errorf("gc: clear sweeping flag: %w", endErr))
		}
	}()
	if err := r.waitInFlight(ctx); err != nil {
		return err
	}
	if err := r.remark(ctx); err != nil {
		return err
	}
	return fn()
}

// errSkipped stops a round that found the fence held; Round reports it as a
// skip, not a failure.
var errSkipped = errors.New("gc: another sweep holds the fence")

// waitInFlight blocks until no attempt that could still be writing remains,
// pending or failed, for at most the wait cap.
func (r *round) waitInFlight(ctx context.Context) error {
	timer := time.NewTimer(r.c.maxWait)
	defer timer.Stop()
	for {
		inFlight, err := r.c.store.GCInFlightGenerations(ctx, r.c.margin)
		if err != nil {
			return err
		}
		if len(inFlight) == 0 {
			return nil
		}
		if !r.waitLogged {
			r.waitLogged = true
			r.c.log.Info("gc sweep waiting for a pending snapshot attempt",
				"generation_id", inFlight[0].ID, "sandbox", inFlight[0].SandboxID,
				"generation", inFlight[0].Number, "deadline", inFlight[0].DeadlineAt,
				"pending", len(inFlight))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return ErrPendingTimeout
		case <-time.After(r.c.poll):
		}
	}
}

// remark reads the manifests of every generation that is live now and was not
// read yet: attempts that finished while the fence waited, manifests written
// since the mark and new pins.
func (r *round) remark(ctx context.Context) error {
	live, err := r.c.store.GCLiveGenerations(ctx, r.cutoff)
	if err != nil {
		return err
	}
	for _, g := range live {
		if err := r.mark(ctx, g.ManifestKey); err != nil {
			return err
		}
	}
	return nil
}

// sweepManifests deletes a batch of dead manifests: it purges their rows
// first (a row that is live again is refused and its manifest, and so its
// chunks, is kept), then deletes the objects. Runs under the fence, after the
// re-mark, so the batch's own liveness was just evaluated.
func (r *round) sweepManifests(ctx context.Context, batch []objstore.Info) error {
	keys := make([]string, len(batch))
	for i, info := range batch {
		keys[i] = info.Key
	}
	declined, err := r.c.store.GCPurgeGenerations(ctx, r.cutoff, keys)
	if err != nil {
		return err
	}
	kept := make(map[string]struct{}, len(declined))
	for _, key := range declined {
		kept[key] = struct{}{}
		if err := r.mark(ctx, key); err != nil {
			return err
		}
	}
	for _, info := range batch {
		if _, ok := kept[info.Key]; ok {
			continue
		}
		if err := r.c.objects.Delete(ctx, info.Key); err != nil {
			return fmt.Errorf("gc: delete manifest: %w", err)
		}
		r.sum.ManifestsDeleted++
		r.sum.BytesFreed += info.Size
	}
	return nil
}

// sweepChunks deletes a batch of chunk candidates that the re-mark did not
// claim.
func (r *round) sweepChunks(ctx context.Context, batch []objstore.Info) error {
	for _, info := range batch {
		hash, err := snapformat.ParseChunkKey(info.Key)
		if err != nil {
			return err
		}
		if _, ok := r.marked[hash]; ok {
			r.sum.ChunksKept++
			continue
		}
		if err := r.c.objects.Delete(ctx, info.Key); err != nil {
			return fmt.Errorf("gc: delete chunk: %w", err)
		}
		r.sum.ChunksDeleted++
		r.sum.BytesFreed += info.Size
	}
	return nil
}
