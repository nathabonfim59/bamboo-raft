// Package lifecycle runs the durable sandbox state machine: it claims
// PostgreSQL jobs, boots VMs through the internal node gateway, fences every
// state change with the job lease, cleans up terminal failures, accepts TTL
// deletions, reconciles node inventory and enqueues idempotent node deletes.
//
// Roles share nothing in process: workers talk to PostgreSQL, the gateway
// and the node only. Handlers are fixed when the Worker is constructed
// (Options.Handlers), so a running Worker never mutates its dispatch table.
package lifecycle

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// Defaults for the lifecycle role. They align with server configuration
// (lifecycle.retry-seconds=5, reaper-seconds=60, job-poll-seconds=15). The
// lease and boot deadline are internal, short enough to reclaim a crashed
// worker and long enough for a cold boot with lease renewal every third.
const (
	DefaultWorkers         = 2
	DefaultLease           = 30 * time.Second
	MinLease               = 30 * time.Millisecond
	DefaultBootTimeout     = 15 * time.Minute
	DefaultRetryBase       = 5 * time.Second
	DefaultMaxBackoff      = 2 * time.Minute
	DefaultJobPoll         = 15 * time.Second
	DefaultReaperInterval  = 60 * time.Second
	DefaultGuardReacquire  = 5 * time.Second
	DefaultCleanupTimeout  = 30 * time.Second
	DefaultReapGuardWait   = 10 * time.Second
	finalizeTimeout        = 5 * time.Second
	failedJobsPageSize     = 100
	deletionReaperPageSize = 100
	nodeInventoryTimeout   = 10 * time.Second
	provisionRetryMin      = time.Second

	// DefaultStoppedRetention is how long a stopped sandbox keeps its
	// retained disk before the reaper deletes the sandbox through the normal
	// deletion path (-lifecycle.stopped-retention-seconds, seven days).
	DefaultStoppedRetention = 7 * 24 * time.Hour

	// DefaultDataRetention is how long a stopped sandbox's stored data is
	// kept before the reaper expires it (-lifecycle.data-retention-seconds,
	// fourteen days).
	DefaultDataRetention = 14 * 24 * time.Hour
)

// Store is the database surface lifecycle consumes. *db.Store implements it;
// tests may wrap it, but no role shares a Store instance between processes.
type Store interface {
	ClaimJob(ctx context.Context, worker string, lease time.Duration) (db.Job, error)
	RenewJobLease(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, lease time.Duration) error
	CompleteJob(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID) error
	RetryJob(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, backoff time.Duration, cause string) error
	FailJob(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, reason string) error
	CancelJob(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID) error
	FailedJobs(ctx context.Context, after *db.FailedJobCursor, limit int) ([]db.Job, *db.FailedJobCursor, error)
	FailStaleJobs(ctx context.Context) (int64, error)
	RetrySandboxDeletion(ctx context.Context, owner uuid.UUID, opID, sandboxID string, in db.JobInput) (db.DeletionOperation, bool, error)

	SandboxAny(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error)
	TransitionSandboxJob(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p db.TransitionParams) (db.Sandbox, error)
	TransitionSandbox(ctx context.Context, p db.TransitionParams) (db.Sandbox, error)
	ReleaseSandboxReservation(ctx context.Context, owner uuid.UUID, sandboxID string) error
	// ReleaseSnapshotPin releases a restore's artifact pin exactly once,
	// only after a confirmed ready sandbox or confirmed cleanup/reservation
	// release. It is the durable backstop for failed best-effort releases.
	ReleaseSnapshotPin(ctx context.Context, owner uuid.UUID, sandboxID string) error
	DecryptSandboxEnvForNode(ctx context.Context, owner uuid.UUID, sandboxID string) (map[string]string, error)
	SandboxesExpiring(ctx context.Context, before time.Time, limit int) ([]db.Sandbox, error)
	AcceptSandboxDeletion(ctx context.Context, owner uuid.UUID, sandboxID string, reason db.DeletionReason) (db.DeletionOperation, db.Sandbox, error)
	// AcceptSandboxExpiryStop accepts the TTL auto-stop while the reaper's
	// candidate is still current: it rechecks the expected version and the
	// deadline under the row lock, so a concurrent extension or manual stop
	// wins (B1.2).
	AcceptSandboxExpiryStop(ctx context.Context, owner uuid.UUID, sandboxID string, expectedVersion int64, deadline time.Time, force bool) (db.StopOperation, db.Sandbox, db.Job, bool, error)
	// StoppedSandboxesBefore lists archived retention candidates by their
	// stopped timestamp; AcceptStoppedRetentionDeletion rechecks every one
	// transactionally before the normal deletion path runs (B1.2).
	StoppedSandboxesBefore(ctx context.Context, before time.Time, limit int) ([]db.Sandbox, error)
	AcceptStoppedRetentionDeletion(ctx context.Context, owner uuid.UUID, sandboxID string, stoppedBefore time.Time, reason db.DeletionReason) (db.DeletionOperation, db.Sandbox, bool, error)
	// StoppedSandboxesWithDataBefore lists archived sandboxes whose stored
	// data has not expired yet; AcceptDataExpiry rechecks and expires one
	// transactionally, evicting its local disk but keeping the row.
	StoppedSandboxesWithDataBefore(ctx context.Context, before time.Time, limit int) ([]db.Sandbox, error)
	AcceptDataExpiry(ctx context.Context, owner uuid.UUID, sandboxID string, stoppedBefore time.Time) (db.Sandbox, bool, error)
	SandboxesOnNode(ctx context.Context, nodeID string) ([]db.Sandbox, error)
	NodeIDs(ctx context.Context) ([]string, error)

	// Durable stop and resume operations (B1.2): workers reread the
	// canonical operation instead of trusting a job payload, record failed
	// attempts under their live claim and finalize only under that claim,
	// the row version and the lifecycle generation.
	StopOperation(ctx context.Context, owner uuid.UUID, sandboxID, stopID string) (db.StopOperation, error)
	LatestSandboxStop(ctx context.Context, owner uuid.UUID, sandboxID string) (db.StopOperation, error)
	FailSandboxStopAttempt(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, owner uuid.UUID, stopID, reason string) (db.StopOperation, error)
	FinalizeSandboxStop(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p db.FinalizeStopParams) (db.Sandbox, db.StopOperation, error)
	// FinalizeSandboxStopMissingDisk records the terminal missing-disk stop
	// outcome after the worker's DeleteVM fence: sandbox error, canonical
	// stop failing with the fixed vm_missing error, all remaining
	// reservations released exactly once; replays report applied=false.
	FinalizeSandboxStopMissingDisk(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p db.FinalizeStopParams) (db.Sandbox, db.StopOperation, bool, error)
	FinalizeSandboxResume(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p db.FinalizeResumeParams) (db.Sandbox, error)
	FinalizeSandboxResumeFailure(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p db.FinalizeResumeFailureParams, reason string) (db.Sandbox, error)
	FinalizeSandboxResumeMissingDisk(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p db.FinalizeResumeFailureParams) (db.Sandbox, error)
	// LatestSandboxJob exposes the canonical initial provision job so a stop
	// accepted during the first boot can order itself after that disk.
	LatestSandboxJob(ctx context.Context, owner uuid.UUID, sandboxID string, kind db.JobKind) (db.Job, error)
	// EnqueueJobIfAbsent deduplicates reaper-driven stop/resume retries.
	EnqueueJobIfAbsent(ctx context.Context, in db.JobInput) (db.Job, bool, error)

	DeletionOperation(ctx context.Context, owner uuid.UUID, opID string) (db.DeletionOperation, error)
	ActiveSandboxDeletion(ctx context.Context, owner uuid.UUID, sandboxID string) (db.DeletionOperation, error)
	BeginDeletionAttempt(ctx context.Context, owner uuid.UUID, opID string) (db.DeletionOperation, error)
	CompleteDeletion(ctx context.Context, owner uuid.UUID, opID string) (db.DeletionOperation, error)

	AcquireSandboxGuard(ctx context.Context, owner uuid.UUID, sandboxID string) (*db.SandboxGuard, error)
	ListenJobs(ctx context.Context, pollEvery time.Duration) (*db.JobListener, error)

	// Object-store snapshot generations: the snapshot worker and
	// the stop's final generation begin, complete and fail rows; the
	// scheduler loop enqueues due sandboxes; the reaper fails pending rows
	// whose deadline passed.
	BeginGeneration(ctx context.Context, p db.BeginGenerationParams) (db.Generation, error)
	CompleteGeneration(ctx context.Context, id uuid.UUID, r db.GenerationResult, next time.Time) (db.Generation, error)
	FailGeneration(ctx context.Context, id uuid.UUID, reason string, next time.Time) (db.Generation, error)
	PendingGenerations(ctx context.Context, limit int) ([]db.Generation, error)
	DueSnapshotSandboxes(ctx context.Context, now time.Time, limit int) ([]db.Sandbox, error)
	EnqueueSnapshotJob(ctx context.Context, owner uuid.UUID, sandboxID string) (bool, error)
	// Fork jobs resolve the source generation (its latest completed one, or
	// the one admission pinned) and hand the fork to its provision job.
	LatestGeneration(ctx context.Context, owner uuid.UUID, sandboxID string) (db.Generation, bool, error)
	GenerationByID(ctx context.Context, owner uuid.UUID, sandboxID string, id uuid.UUID) (db.Generation, error)
	StartFork(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p db.StartForkParams) (db.Job, error)

	// RecordRecoverEvent records a recovery outcome event under a live
	// sandbox.recover claim.
	RecordRecoverEvent(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, owner uuid.UUID, sandboxID string, e db.EventInput) error

	// Multi-node placement: a manifest restore refused by a node without an
	// object store marks that node and moves the boot to another node under
	// the provision job's claim; a successful snapshot or restore marks the
	// node usable again.
	MarkNodeObjectStore(ctx context.Context, nodeID string, ok bool) error
	ReadmitSandbox(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p db.ReadmitParams) (db.Sandbox, db.Job, error)

	// Resume on another node: a cross-node resume refused by a node
	// without an object store moves to another node under its claim; a
	// terminal restore failure leaves the sandbox archived with no local
	// disk. Evicted copies of a sandbox on a node are deleted by the
	// resume after READY or by VM reconciliation, then marked done.
	MoveResume(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p db.MoveResumeParams) (db.Sandbox, db.Job, error)
	FinalizeSandboxResumeElsewhereFailure(ctx context.Context, jobID uuid.UUID, worker string, token uuid.UUID, p db.FinalizeResumeFailureParams, reason string) (db.Sandbox, error)
	OpenEvictions(ctx context.Context, nodeID string) ([]db.Eviction, error)
	OpenEvictionNodes(ctx context.Context, owner uuid.UUID, sandboxID string) ([]string, error)
	MarkEvictionDone(ctx context.Context, nodeID, sandboxID string) error

	// Node loss: the reaper lists nodes not observed connected for the
	// grace and their sandboxes, then decides each one in a store
	// transaction that rechecks the row and the node: move it from its
	// latest generation, fail it, or keep only its generation.
	LostNodes(ctx context.Context, cutoff time.Time) ([]string, error)
	SandboxesOnLostNode(ctx context.Context, nodeID string) ([]db.Sandbox, error)
	MoveLostSandbox(ctx context.Context, p db.NodeLossParams, m db.NodeLossMove) (db.Sandbox, db.Job, error)
	FailLostSandbox(ctx context.Context, p db.NodeLossParams) (db.Sandbox, error)
	EvictLostDisk(ctx context.Context, p db.NodeLossParams) (db.Sandbox, error)
	FinishLostStop(ctx context.Context, p db.NodeLossParams) (db.Sandbox, db.StopOperation, error)

	// Disk pressure: the reaper lists nodes under their free-disk
	// threshold and the archived sandboxes they may evict, commits each
	// eviction and then confirms the copy's delete. DiskLowPercent 0
	// disables it.
	LowDiskNodes(ctx context.Context, percent int) ([]db.Node, error)
	DiskPressureCandidates(ctx context.Context, nodeID string, limit int) ([]db.Sandbox, error)
	EvictSandboxDisk(ctx context.Context, p db.DiskPressureParams) (db.Sandbox, error)
}

// Placer ranks nodes for a sandbox; *scheduler.Scheduler implements it. The
// provision job uses it to move a manifest restore off a node that cannot
// read the object store.
type Placer interface {
	Place(ctx context.Context, req scheduler.PlaceRequest) ([]scheduler.Candidate, error)
}

// NodeClient is the gateway-bound node surface workers use. Every side
// effect goes through the accepted internal gateway client; workers never
// touch the Hub or open node connections themselves.
type NodeClient interface {
	nodev1.NodeClient
}

// Dial opens a gateway-bound client for one node and returns its closer.
type Dial func(nodeID string) (NodeClient, io.Closer, error)

// Handler processes one claimed job. ctx is the worker lifetime (DB and
// cleanup work); lease.Context() is cancelled when the job claim is lost and
// must bound any blocking RPC. A RetryError reports a safe reason; any other
// error is classified without echoing node or driver text.
type Handler func(ctx context.Context, job db.Job, lease *Lease) error

// Options configures a Worker. Store, Dial and Logger are required.
type Options struct {
	Store  Store
	Dial   Dial
	Logger *slog.Logger

	WorkerID       string
	Workers        int
	Lease          time.Duration
	BootTimeout    time.Duration
	RetryBase      time.Duration
	MaxBackoff     time.Duration
	JobPoll        time.Duration
	ReaperInterval time.Duration
	CleanupTimeout time.Duration
	ReapGuardWait  time.Duration
	GuardReacquire time.Duration
	Handlers       map[db.JobKind]Handler

	// StoppedRetention is how long the reaper keeps an archived sandbox's
	// retained disk before deleting the sandbox through the normal deletion
	// path. Nil selects [DefaultStoppedRetention] (seven days); an explicit
	// zero disables retention deletion; a negative value is rejected.
	StoppedRetention *time.Duration

	// DataRetention is how long the reaper keeps an archived sandbox's stored
	// data before it expires: the local disk copy is evicted, data_expired_at
	// is set and resume, fork and save refuse it. Nil selects
	// [DefaultDataRetention] (fourteen days); an explicit zero never expires
	// data; a negative value is rejected. It applies only with ObjectStore:
	// Phase 1 mode has no stored data to expire, so it is always off there.
	DataRetention *time.Duration

	// ObjectStore is the installation's object store, carried for the
	// Phase 2 snapshot worker and the stop's final generation. Nil means
	// none is configured: Phase 1 mode, with no snapshot uploads and local
	// stop/resume behaviour.
	ObjectStore objstore.Store
	// SnapshotInterval is the period between generations of one sandbox
	// (±10 % jitter); zero selects [DefaultSnapshotInterval].
	SnapshotInterval time.Duration
	// SnapshotTimeout bounds one SnapshotVM call and is the deadline of its
	// pending generation row; zero selects [DefaultSnapshotTimeout].
	SnapshotTimeout time.Duration
	// SnapshotRand returns a value in [0, 1) that spreads the next snapshot
	// time over the jitter window; nil uses math/rand/v2. Tests make it
	// deterministic.
	SnapshotRand func() float64

	// Placer re-places a manifest restore whose node refused it with
	// FAILED_PRECONDITION. Nil means no re-placement: such a boot fails
	// with no_ready_machine. Node loss needs it to move a sandbox.
	Placer Placer

	// NodeLostAfter is how long a node may go without a connected inventory
	// observation before the reaper treats it as lost and moves what it
	// held (-lifecycle.node-lost-seconds). Zero disables node loss, and so
	// does a nil ObjectStore; a negative value is rejected.
	NodeLostAfter time.Duration

	// DiskLowPercent is the share of a node's total disk below which the
	// reaper gives up the retained disks of archived sandboxes on it
	// (-lifecycle.disk-low-percent). Zero disables disk-pressure eviction
	// and so does a nil ObjectStore; anything above 90 is rejected.
	DiskLowPercent int
}

// Worker owns one process's lifecycle loops. Construct it once; Handlers is
// copied so registration is frozen before Run or ProcessNext.
type Worker struct {
	store            Store
	dial             Dial
	log              *slog.Logger
	workerID         string
	workers          int
	lease            time.Duration
	bootTimeout      time.Duration
	retryBase        time.Duration
	maxBackoff       time.Duration
	jobPoll          time.Duration
	reaperInterval   time.Duration
	cleanupTimeout   time.Duration
	reapGuardWait    time.Duration
	guardReacquire   time.Duration
	stoppedRetention time.Duration
	dataRetention    time.Duration
	handlers         map[db.JobKind]Handler
	runOnce          atomic.Bool

	objectStore      objstore.Store
	snapshotInterval time.Duration
	snapshotTimeout  time.Duration
	snapshotRand     func() float64
	precondWarned    warnOnce
	placer           Placer

	nodeLostAfter  time.Duration
	diskLowPercent int
	started        time.Time // node loss waits nodeLostAfter from here
	lostWarned     warnOnce
	lowWarned      warnOnce // low nodes with nothing left to evict
}

// New validates opts, copies the handler map and installs the built-in
// provision, stop, resume, delete and snapshot handlers. Supplying a handler
// for the same kind replaces the default; unknown kinds have no handler and
// fail safely.
func New(opts Options) (*Worker, error) {
	if opts.Store == nil {
		return nil, errors.New("lifecycle: New needs a Store")
	}
	if opts.Dial == nil {
		return nil, errors.New("lifecycle: New needs a Dial function")
	}
	if opts.Logger == nil {
		return nil, errors.New("lifecycle: New needs a logger")
	}
	if opts.Lease < 0 || opts.BootTimeout < 0 || opts.RetryBase < 0 ||
		opts.MaxBackoff < 0 || opts.JobPoll < 0 || opts.ReaperInterval < 0 ||
		opts.CleanupTimeout < 0 || opts.ReapGuardWait < 0 || opts.GuardReacquire < 0 ||
		opts.SnapshotInterval < 0 || opts.SnapshotTimeout < 0 || opts.NodeLostAfter < 0 {
		return nil, errors.New("lifecycle: durations must not be negative")
	}
	if opts.Workers < 0 {
		return nil, errors.New("lifecycle: Workers must not be negative")
	}
	if opts.Lease > 0 && opts.Lease < MinLease {
		return nil, errors.New("lifecycle: lease is below the safe minimum")
	}
	if opts.StoppedRetention != nil && *opts.StoppedRetention < 0 {
		return nil, errors.New("lifecycle: StoppedRetention must not be negative")
	}
	if opts.DataRetention != nil && *opts.DataRetention < 0 {
		return nil, errors.New("lifecycle: DataRetention must not be negative")
	}
	if opts.DiskLowPercent < 0 || opts.DiskLowPercent > 90 {
		return nil, errors.New("lifecycle: disk low percent must be 0..90")
	}

	stoppedRetention := DefaultStoppedRetention
	if opts.StoppedRetention != nil {
		stoppedRetention = *opts.StoppedRetention
	}
	dataRetention := DefaultDataRetention
	if opts.DataRetention != nil {
		dataRetention = *opts.DataRetention
	}
	if opts.ObjectStore == nil {
		// Phase 1 mode keeps its behaviour exactly: with no object store a
		// stopped sandbox has no stored data to expire.
		dataRetention = 0
	}
	w := &Worker{
		store:            opts.Store,
		dial:             opts.Dial,
		log:              opts.Logger,
		workerID:         cmp.Or(opts.WorkerID, defaultWorkerID()),
		workers:          cmp.Or(opts.Workers, DefaultWorkers),
		lease:            cmp.Or(opts.Lease, DefaultLease),
		bootTimeout:      cmp.Or(opts.BootTimeout, DefaultBootTimeout),
		retryBase:        cmp.Or(opts.RetryBase, DefaultRetryBase),
		maxBackoff:       cmp.Or(opts.MaxBackoff, DefaultMaxBackoff),
		jobPoll:          cmp.Or(opts.JobPoll, DefaultJobPoll),
		reaperInterval:   cmp.Or(opts.ReaperInterval, DefaultReaperInterval),
		cleanupTimeout:   cmp.Or(opts.CleanupTimeout, DefaultCleanupTimeout),
		reapGuardWait:    cmp.Or(opts.ReapGuardWait, DefaultReapGuardWait),
		guardReacquire:   cmp.Or(opts.GuardReacquire, DefaultGuardReacquire),
		stoppedRetention: stoppedRetention,
		dataRetention:    dataRetention,
		handlers:         make(map[db.JobKind]Handler, 5+len(opts.Handlers)),
		objectStore:      opts.ObjectStore,
		snapshotInterval: cmp.Or(opts.SnapshotInterval, DefaultSnapshotInterval),
		snapshotTimeout:  cmp.Or(opts.SnapshotTimeout, DefaultSnapshotTimeout),
		snapshotRand:     opts.SnapshotRand,
		placer:           opts.Placer,
		nodeLostAfter:    opts.NodeLostAfter,
		diskLowPercent:   opts.DiskLowPercent,
		started:          time.Now(),
	}
	if w.snapshotRand == nil {
		w.snapshotRand = rand.Float64
	}
	w.handlers[db.JobProvision] = w.provision
	w.handlers[db.JobStop] = w.stopSandbox
	w.handlers[db.JobResume] = w.resumeSandbox
	w.handlers[db.JobDeleteVM] = w.deleteVM
	w.handlers[db.JobSnapshot] = w.snapshotSandbox
	w.handlers[db.JobRecover] = w.recoverSandbox
	w.handlers[db.JobFork] = w.forkSandbox
	for kind, h := range opts.Handlers {
		w.handlers[kind] = h
	}
	return w, nil
}

// defaultWorkerID identifies this process's claims without leaking host
// details into logs or job rows.
func defaultWorkerID() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%s", cmp.Or(host, "lifecycle"), os.Getpid(), uuid.NewV7())
}

// Run serves jobs, the reaper and (with an object store) the snapshot
// scheduler until ctx ends, then cancels in-flight attempts and joins every
// goroutine and the listener. It can be called once.
func (w *Worker) Run(ctx context.Context) error {
	if !w.runOnce.CompareAndSwap(false, true) {
		return errors.New("lifecycle: Run may only be called once")
	}

	listener, err := w.store.ListenJobs(ctx, w.jobPoll)
	if err != nil {
		return fmt.Errorf("lifecycle: start job listener: %w", err)
	}
	defer listener.Close()

	var wg sync.WaitGroup
	for range w.workers {
		wg.Go(func() { w.jobLoop(ctx, listener) })
	}
	wg.Go(func() { w.reaperLoop(ctx) })
	if w.objectStore != nil {
		wg.Go(func() { w.snapshotLoop(ctx) })
	}
	wg.Wait()
	return nil
}

// jobLoop claims jobs until ctx ends. A notification wakes it immediately;
// the listener's poll fallback keeps it moving when a NOTIFY is lost.
func (w *Worker) jobLoop(ctx context.Context, listener *db.JobListener) {
	for {
		if ctx.Err() != nil {
			return
		}
		worked, err := w.ProcessNext(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.log.Warn("job attempt failed", "reason", classify(err))
			if !sleepCtx(ctx, provisionRetryMin) {
				return
			}
			continue
		}
		if worked {
			continue
		}
		if err := listener.Await(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			w.log.Warn("job listener await failed", "reason", classify(err))
			if !sleepCtx(ctx, provisionRetryMin) {
				return
			}
		}
	}
}

// ProcessNext claims and runs at most one job. It reports false when the
// queue has no runnable job. Tests use it as the deterministic one-job seam;
// production runs it from Run's worker loops.
func (w *Worker) ProcessNext(ctx context.Context) (bool, error) {
	job, err := w.store.ClaimJob(ctx, w.workerID, w.lease)
	if errors.Is(err, db.ErrNoJobs) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lifecycle: claim job: %w", err)
	}
	if err := w.runJob(ctx, job); err != nil {
		return true, err
	}
	return true, nil
}

// RetryError lets a handler state a safe retry reason and backoff, or mark
// the failure permanent so ProcessNext fails the job without further tries.
type RetryError struct {
	Reason    string
	Backoff   time.Duration
	Permanent bool
}

// Error reports the safe reason only.
func (e *RetryError) Error() string { return e.Reason }

// retry asks for another attempt after backoff.
func retry(reason string, backoff time.Duration) *RetryError {
	return &RetryError{Reason: reason, Backoff: backoff}
}

// permanent asks for a terminal failure without retrying.
func permanent(reason string) *RetryError {
	return &RetryError{Reason: reason, Permanent: true}
}

// ErrLeaseLost reports that this attempt no longer owns the job. The
// redelivered attempt is responsible for the work; handlers must not retry or
// write state after seeing it.
var ErrLeaseLost = errors.New("lifecycle: job lease lost")

// leaseRenewDivisor keeps three renewals per lease, so one slow database call
// cannot expire a healthy claim.
const leaseRenewDivisor = 3

// Lease is one claim's fencing handle. Context is cancelled when renewal
// fails (the token was reclaimed) or the worker shuts down; RPCs must use it.
type Lease struct {
	store     Store
	job       db.Job
	worker    string
	lease     time.Duration
	ctx       context.Context
	cancel    context.CancelCauseFunc
	renewDone sync.WaitGroup
}

// Context is cancelled when the claim is lost or the worker stops. Blocking
// node calls use it so a reclaimed job cannot keep acting.
func (l *Lease) Context() context.Context { return l.ctx }

// Job returns the claimed row snapshot.
func (l *Lease) Job() db.Job { return l.job }

// Lost reports whether the claim was lost before the handler finished.
func (l *Lease) Lost() bool { return errors.Is(context.Cause(l.ctx), ErrLeaseLost) }

// runJob executes one claimed job under a lease-renewal loop and finalizes
// the queue row according to the handler's outcome.
func (w *Worker) runJob(ctx context.Context, job db.Job) error {
	leaseCtx, cancel := context.WithCancelCause(ctx)
	lease := &Lease{
		store:  w.store,
		job:    job,
		worker: w.workerID,
		lease:  w.lease,
		ctx:    leaseCtx,
		cancel: cancel,
	}
	lease.renewDone.Go(func() { w.renewLoop(lease) })

	h := w.handlers[job.Kind]
	var err error
	if h == nil {
		err = permanent("unsupported job kind")
	} else {
		err = h(ctx, job, lease)
	}
	cancel(nil)
	lease.renewDone.Wait()

	if lease.Lost() {
		// The row is owned elsewhere or already terminal. Its new attempt
		// owns any remaining side effects and compensation.
		return nil
	}
	if err == nil {
		if ctx.Err() != nil {
			// A handler that aborted because the worker is shutting down
			// must not look like success: requeue so redelivery can
			// finish the sandbox and its cleanup.
			return w.requeue(ctx, job, 0, "worker shutdown")
		}
		return w.complete(ctx, job)
	}
	if re, ok := errors.AsType[*RetryError](err); ok {
		if re.Permanent {
			return w.fail(ctx, job, re.Reason)
		}
		return w.requeue(ctx, job, re.Backoff, re.Reason)
	}
	if ctx.Err() != nil {
		return w.requeue(ctx, job, 0, "worker shutdown")
	}
	return w.requeue(ctx, job, w.backoff(job.Attempts), classify(err))
}

// renewLoop extends the claim until the handler ends or renewal fails. Any
// failure cancels the attempt: continuing without a live token would let a
// reclaimed worker mutate a sandbox it no longer owns.
func (w *Worker) renewLoop(l *Lease) {
	every := max(l.lease/leaseRenewDivisor, 10*time.Millisecond)
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			if l.ctx.Err() != nil {
				return // shutdown, not a lost claim
			}
			// A stalled renewal must not outlive the lease it protects.
			rctx, cancel := context.WithTimeout(l.ctx, l.lease/leaseRenewDivisor)
			err := w.store.RenewJobLease(rctx, l.job.ID, l.worker, l.job.LeaseToken, l.lease)
			cancel()
			if err != nil {
				if l.ctx.Err() != nil {
					return // parent cancellation, not a reclaim
				}
				l.cancel(fmt.Errorf("%w: %v", ErrLeaseLost, err))
				return
			}
		}
	}
}

// finalizeCtx detaches queue-row finalization from a shutdown cancellation,
// but never from a lost lease (whose token is invalid anyway).
func finalizeCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), finalizeTimeout)
}

// complete marks a successful job done. A lost token is not an error: the
// redelivered attempt sees the committed sandbox state and completes instead.
func (w *Worker) complete(ctx context.Context, job db.Job) error {
	fctx, cancel := finalizeCtx(ctx)
	defer cancel()
	err := w.store.CompleteJob(fctx, job.ID, w.workerID, job.LeaseToken)
	if errors.Is(err, db.ErrJobNotLeased) {
		w.log.Info("job completed elsewhere", "job", job.ID, "kind", job.Kind)
		return nil
	}
	if err != nil {
		return fmt.Errorf("lifecycle: complete job: %w", err)
	}
	return nil
}

// requeue retries a job with backoff; RetryJob fails it terminally once
// attempts are exhausted, which the reaper then reconciles.
func (w *Worker) requeue(ctx context.Context, job db.Job, backoff time.Duration, reason string) error {
	fctx, cancel := finalizeCtx(ctx)
	defer cancel()
	err := w.store.RetryJob(fctx, job.ID, w.workerID, job.LeaseToken, backoff, reason)
	if errors.Is(err, db.ErrJobNotLeased) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lifecycle: retry job: %w", err)
	}
	return nil
}

// fail terminally fails a job under this claim, without consuming more
// attempts. A lost token is benign.
func (w *Worker) fail(ctx context.Context, job db.Job, reason string) error {
	fctx, cancel := finalizeCtx(ctx)
	defer cancel()
	err := w.store.FailJob(fctx, job.ID, w.workerID, job.LeaseToken, reason)
	if errors.Is(err, db.ErrJobNotLeased) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lifecycle: fail job: %w", err)
	}
	return nil
}

// backoff is bounded exponential backoff based on the attempt number.
func (w *Worker) backoff(attempt int) time.Duration {
	d := w.retryBase
	for range max(attempt-1, 0) {
		if d >= w.maxBackoff/2 {
			return w.maxBackoff
		}
		d *= 2
	}
	return min(d, w.maxBackoff)
}

// classify maps errors to fixed, non-secret reasons. Raw node, driver and
// secret-bearing errors never reach job rows, logs or events.
func classify(err error) string {
	switch status.Code(err) {
	case codes.Unavailable:
		return "node unavailable"
	case codes.DeadlineExceeded:
		return "node timeout"
	case codes.ResourceExhausted:
		return "node out of capacity"
	case codes.InvalidArgument:
		return "node rejected request"
	case codes.NotFound:
		return "node not found"
	case codes.Aborted:
		return "sandbox deleted"
	case codes.FailedPrecondition:
		return "node precondition failed"
	case codes.Unauthenticated, codes.PermissionDenied:
		return "node authorization failed"
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, ErrLeaseLost):
		return "lease lost"
	case errors.Is(err, db.ErrSandboxGuardLost):
		return "sandbox guard lost"
	case errors.Is(err, db.ErrNotFound):
		return "not found"
	case errors.Is(err, db.ErrConflict):
		return "state conflict"
	default:
		return "internal error"
	}
}

// terminalProvisionReason maps a node failure to a fixed sandbox error
// reason. It is the only reason text written to error_reason.
func terminalProvisionReason(err error) string {
	switch status.Code(err) {
	case codes.ResourceExhausted:
		return "out_of_capacity"
	case codes.InvalidArgument, codes.NotFound:
		return "image_unavailable"
	default:
		return "boot_failed"
	}
}

// retryableProvision reports whether another CreateVM attempt can plausibly
// succeed. Request and image errors are terminal; transport and boot errors
// retry until the attempt budget is exhausted.
func retryableProvision(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.ResourceExhausted,
		codes.Unauthenticated, codes.PermissionDenied:
		return false
	default:
		return true
	}
}

// sleepCtx waits d unless ctx ends first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
