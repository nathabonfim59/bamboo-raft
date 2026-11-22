// Package node is the real nodev1.Node implementation: it owns QEMU VMs, the
// per-VM disks under the node data directory and the guest proxy.
package node

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/chunkcache"
	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/overcommit"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// DefaultBootTimeout bounds a boot when Config.BootTimeout is zero. It
// matches nodetest.Options.
const DefaultBootTimeout = 2 * time.Minute

// Snapshot defaults bound both the guest lease and independent cleanup.
const (
	DefaultSnapshotLease          = 2 * time.Minute
	DefaultSnapshotCleanupTimeout = 30 * time.Second
	// DefaultSnapshotFreeFloor is Config.SnapshotFreeFloor's default.
	DefaultSnapshotFreeFloor = 2 << 30
)

// DefaultMinCID and DefaultMaxCID bound vsock CIDs. 0..2 are reserved by the
// Linux vsock implementation; 0xffffffff is VMADDR_CID_ANY and never a guest
// CID.
const (
	DefaultMinCID uint32 = 3
	DefaultMaxCID uint32 = math.MaxUint32 - 1
)

// Config configures a [Manager]. NodeID, DataDir, ImageDir and the capacity
// come from the bbraft-node command (A2.3); the launcher, guest dialer and
// imager are injected for tests.
type Config struct {
	// NodeID is the name the node reports in Info.
	NodeID string
	// DataDir is the writable state root. It holds the exclusive node lock.
	DataDir string
	// ImageDir is the read-only root of verified image version directories.
	ImageDir string
	// DefaultImage is the image id used when CreateVM names none. When
	// empty, the first verified image (sorted by id) is the default.
	DefaultImage string
	// QemuVersion is reported in Info. Empty means detect it from the
	// binary (best effort).
	QemuVersion string
	// CPUModel is the fixed named CPU model passed to QEMU. Empty uses
	// qemu.DefaultCPUModel.
	CPUModel string
	// Nested is the -node.cpu-nested policy passed to every QEMU launch.
	// Empty means qemu.NestedAuto. The CPU policy itself lives in qemu.
	Nested qemu.NestedMode
	// CPUFeatures are the "+name"/"-name" CPU feature toggles every QEMU
	// launch appends (qemu.ParseCPUFeatures): the operator's
	// -node.cpu-features plus nested VMX features the startup probe found
	// missing. The node CLI passes exactly the probed option's features.
	CPUFeatures []string
	// AllowHostCPU permits CPUModel "host" for host-KVM test fixtures only.
	// Production node configuration never sets it; the default fixed model
	// and enforce stay unchanged. "max" is never permitted.
	AllowHostCPU bool
	// Capacity is the total resource offer after the host reserve. It is
	// cloned at New; the caller may reuse or mutate its message afterwards.
	Capacity *nodev1.Resources
	// HostVCPUs is the logical host CPU count (runtime.NumCPU) used for the
	// hard per-VM ceiling. Zero uses runtime.NumCPU. It is not the offered
	// aggregate capacity, which may be overcommitted.
	HostVCPUs uint32
	// DiskOvercommit scales the live free-space promise used by admission and
	// Info. Zero means 1.0; negative, NaN and infinite values are rejected.
	DiskOvercommit float64
	// MinCID and MaxCID bound vsock CID allocation. Zero uses the
	// package defaults (3 .. 2^32-2).
	MinCID uint32
	MaxCID uint32
	// localCID is the test seam for the one-shot startup probe. nil reads
	// VM_SOCKETS_GET_LOCAL_CID from /dev/vsock. Production leaves it nil.
	localCID func(context.Context) (uint32, error)
	// Store is the object store shared with the control plane: Phase 2
	// snapshots write to it and restores read from it. nil turns Phase 2
	// off; the snapshot RPCs then fail with FAILED_PRECONDITION.
	Store objstore.Store
	// Phase2 is the startup probe's Phase 2 QMP capability check
	// (qemu.CheckPhase2Schema on the probe QEMU). nil: the node checks the
	// first VM's QEMU and caches the result. Anything missing turns Phase 2
	// off with FAILED_PRECONDITION and VMs run without dirty bitmaps.
	Phase2 *qemu.Phase2Support
	// ChunkCacheBytes caps the local chunk cache under DataDir, in bytes.
	// Zero disables the cache (restores read every chunk from the store).
	// The cache lives under <DataDir>/chunks when a Store is configured.
	ChunkCacheBytes int64
	// BootTimeout bounds one boot from CreateVM until the guest answers
	// Ping. Zero uses DefaultBootTimeout.
	BootTimeout time.Duration
	// SnapshotLease is the explicit guest freeze lease. Zero uses two minutes.
	SnapshotLease time.Duration
	// SnapshotCleanupTimeout bounds independent resume/thaw/stage cleanup.
	// Zero uses 30 seconds.
	SnapshotCleanupTimeout time.Duration
	// SnapshotFreeFloor is the free space (bytes) the data directory's
	// file system must keep while a live SnapshotVM reads its cut: below it
	// the snapshot aborts, so the growing fleece image never fills the
	// node. Zero uses DefaultSnapshotFreeFloor.
	SnapshotFreeFloor uint64
	// Network manages per-VM networking (TAP allocation, isolation rules,
	// SSH ingress). It is borrowed, never closed by the manager: the command
	// that constructs it owns its lifetime. nil selects the explicit dev/test
	// user-networking mode and requires AllowUserNetworking; production always
	// sets it.
	Network VMNetwork
	// AllowUserNetworking is the clearly named dev/test fallback: QEMU's
	// user-mode network without TAP isolation (decision 20's known gap,
	// accepted only while our own jobs run). It cannot be combined with
	// Network, and production roles never set it.
	AllowUserNetworking bool
	// Log receives structured diagnostics. Never guest data.
	Log *slog.Logger
	// Launcher starts QEMU. nil uses the real launcher.
	Launcher Launcher
	// GuestDialer dials the guest agent. nil uses AF_VSOCK on GuestPort.
	GuestDialer GuestDialer
	// Imager creates qcow2 files. nil uses the host qemu-img. It is a test
	// seam; the node always passes nil.
	Imager storage.Imager

	// Jailer launches every production QEMU (TAP networking). It is
	// borrowed: the command constructs it with the manager's reclaim policy
	// and closes it after Shutdown. nil keeps the explicit dev/test unjailed
	// launcher (user networking only).
	Jailer *jailer.Manager
	// JailBinary is the trusted QEMU the jailer stages (absolute).
	JailBinary string
	// JailToolBinary is the trusted qemu-img the jailer stages for image
	// tools (absolute; empty: DefaultJailToolBinary).
	JailToolBinary string
	// JailIO bounds each VM's disk IO (cgroup io.max).
	JailIO jailer.IOLimits
	// HostCPU is the admitted host CPU fact set the startup probe proved,
	// carried into every jailed launch so the CPU option cannot drift.
	HostCPU *qemu.HostCPU
	// jail, jailDial, jailVsock, jailCleanup and jailStopGrace are test
	// seams replacing Jailer, the QMP dial, the /dev/vhost-vsock open, the
	// failed-startup reap bound and the QMP-quit grace.
	jail          jailRunner
	jailDial      jailQMPDial
	jailVsock     func() (*os.File, error)
	jailCleanup   time.Duration
	jailStopGrace time.Duration
	// toolSelfCheck is a test seam replacing the startup qemu-img
	// self-check (see toolSelfCheckFor).
	toolSelfCheck toolSelfCheckFunc

	// findProcess and verifyProc are recovery seams, set only by tests in
	// this package. nil uses /proc scanning and QMP verification.
	findProcess func(meta storage.VMMeta) (*pinnedProc, error)
	verifyProc  func(ctx context.Context, pinned *pinnedProc, meta storage.VMMeta) (*qmp.Client, error)
	// diskFree is a test seam for live free space. nil reads statfs.
	diskFree func() (uint64, error)
	// qemuExe is a test seam for the expected QEMU executable path.
	qemuExe string
	// snapshotOptions and snapshotNow are deterministic snapshot fault seams.
	snapshotOptions storage.SnapshotOptions
	snapshotNow     func() time.Time
	// afterSnapshotFreeze stages guest-agent restart after Freeze but before
	// Pause in real KVM recovery tests.
	afterSnapshotFreeze func(context.Context) error
}

// Manager implements nodev1.NodeServer. Its lifetime is independent of any
// RPC: every successful VM keeps running until DeleteVM or a node restart.
type Manager struct {
	nodev1.UnimplementedNodeServer

	cfg            Config
	log            *slog.Logger
	store          *storage.Store
	images         map[string]storage.Image
	net            VMNetwork
	launcher       Launcher
	dialer         GuestDialer
	capacity       resources
	hostVCPUs      uint32
	diskOvercommit float64
	// excludedCID is the host's own local vsock CID when it falls inside
	// MinCID..MaxCID; zero means nothing is excluded.
	excludedCID uint32
	qemuExe     string
	// scanOwned records whether findProcess is the real /proc scanner, which
	// needs a resolved QEMU executable path to prove ownership.
	scanOwned bool
	diskFree  func() (uint64, error)
	now       func() time.Time

	// beforePersist is a test hook used to stage the watcher/Delete race. It
	// runs while v.mu is held, immediately before a durable failure write.
	beforePersist func()
	// afterFailurePersist stages the watcher after its durable failure write
	// and before reservation release; tests only.
	afterFailurePersist func()
	// afterFailurePublish stages the exit watcher between its failure
	// publication and its network release, with the per-VM operation gate
	// held; tests only.
	afterFailurePublish func()
	// beforeReady stages the window between a successful Ping and the
	// terminal READY transition; tests only.
	beforeReady func()
	// beforeStartGate stages the window where a StartVM caller has observed
	// STOPPED but not yet acquired the VM operation gate; tests only.
	beforeStartGate func()
	// beforeResetGate stages the window where a ResetVM worker has claimed
	// its record (BOOTING) but not yet acquired the VM operation gate; tests
	// only.
	beforeResetGate func()
	// beforeWatchGate holds an exit watcher before it takes the per-VM
	// operation gate, and watchRetired reports a watcher that retired at its
	// ownership recheck; tests only.
	beforeWatchGate func(v *vm, proc VMProcess)
	watchRetired    func(v *vm, proc VMProcess)
	// afterReady fires after the terminal READY transition returns from
	// runBoot, where a late ready-write used to race a concurrent stop;
	// tests only.
	afterReady func()
	// afterAbortPersist fires inside an aborted start's settlement, after
	// its durable writes and before any accounting or state publication;
	// tests only.
	afterAbortPersist func()
	// inStartSettle fires inside a failed start's settlement at its
	// snapshot of the record, while the settlement's locks are held; tests
	// use it to race the snapshot against manager-map mutations.
	inStartSettle func()
	// beforeDeleteRemove stages DeleteVM after the boot barrier and before
	// the directory is removed; tests only.
	beforeDeleteRemove func()
	// afterCanceledDeleteAttempt stages the window after a gate-canceled delete
	// attempt is finalized but before its fence cleanup; tests only.
	afterCanceledDeleteAttempt func()

	// jail is the jailed launcher's runner, nil in the dev/test mode.
	// jailDial, jailVsock and jailCleanup are test seams (QMP dial,
	// /dev/vhost-vsock open, failed-startup reap bound).
	jail          jailRunner
	jailDial      jailQMPDial
	jailVsock     func() (*os.File, error)
	jailCleanup   time.Duration
	jailStopGrace time.Duration
	// maintQuit is a test seam for the clean-exit wait of a maintenance
	// session after QMP quit (maintQuitTimeout when zero).
	maintQuit time.Duration

	// phase2 caches the Phase 2 QMP capability check (phase2_launch.go).
	phase2 phase2Gate
	// openCutDisk opens a snapshot cut's NBD export (openNBDDisk); tests
	// inject an in-memory disk.
	openCutDisk func(ctx context.Context, opts cutDiskOptions) (cutDisk, error)
	// chunks is the local chunk cache restores read through; nil when no
	// object store is configured.
	chunks *chunkcache.Cache
	// openRestoreDisk opens a restore's writable NBD export
	// (openNBDRestoreDisk); tests inject a recording disk.
	openRestoreDisk func(ctx context.Context, opts restoreDiskOptions) (restoreDisk, error)
	// fleeceUsage reports a fleece image's allocated bytes
	// (fleeceAllocated); tests inject a size over the reservation.
	fleeceUsage func(*os.File) (uint64, error)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	// drains tracks QMP event consumers. It is separate from wg because
	// stopping a process (which closes its QMP client) is what ends them.
	drains sync.WaitGroup

	// recovery seams. They are explicit so restart reconciliation and the
	// crash-window fault injection are testable without a real QEMU.
	findProcess func(meta storage.VMMeta) (*pinnedProc, error)
	verifyProc  func(ctx context.Context, pinned *pinnedProc, meta storage.VMMeta) (*qmp.Client, error)

	mu          sync.Mutex
	vms         map[string]*vm
	snapshots   map[string]storage.Snapshot
	snapshotOps map[string]*snapshotOperation
	quarantined map[string]struct{}
	// fence marks an in-progress DeleteVM for an id; creates that were
	// already active are counted in creating and must drain before the fence
	// is lifted, so only creates begun after DeleteVM returns can relaunch.
	fence           map[string]*fenceToken
	creating        map[string]int
	reserved        resources
	closed          bool
	shutdownStarted bool
	shutdownDone    chan struct{}
	shutdownErr     error
}

// resources is the manager's private capacity arithmetic. Generated protobuf
// messages are never copied or stored: they carry internal state and
// copylocks would flag it.
type resources struct {
	vcpus     uint32
	memoryMib uint64
	diskGiB   uint64
}

func resourcesFrom(r *nodev1.Resources) resources {
	if r == nil {
		return resources{}
	}
	return resources{vcpus: r.GetVcpus(), memoryMib: r.GetMemoryMib(), diskGiB: r.GetDiskGib()}
}

func (r resources) proto() *nodev1.Resources {
	return &nodev1.Resources{Vcpus: r.vcpus, MemoryMib: r.memoryMib, DiskGib: r.diskGiB}
}

func (r resources) add(o resources) (resources, bool) {
	vcpus := uint64(r.vcpus) + uint64(o.vcpus)
	memory := r.memoryMib + o.memoryMib
	disk := r.diskGiB + o.diskGiB
	if vcpus > math.MaxUint32 || memory < r.memoryMib || disk < r.diskGiB {
		return resources{}, false
	}
	return resources{vcpus: uint32(vcpus), memoryMib: memory, diskGiB: disk}, true
}

func (r resources) fits(c resources) bool {
	return r.vcpus <= c.vcpus && r.memoryMib <= c.memoryMib && r.diskGiB <= c.diskGiB
}

func (r resources) diskBytes() (uint64, bool) {
	if r.diskGiB > math.MaxUint64>>30 {
		return 0, false
	}
	return r.diskGiB << 30, true
}

// deleteAttempt is one generation's cleanup. done closes when err is final.
type deleteAttempt struct {
	done chan struct{}
	err  error
}

// fenceToken identifies one published delete fence. It must not be a zero-sized
// type: distinct allocations of an empty struct can share one address. Only
// the attempt holding the current token may remove it, so an older attempt's
// cleanup can never clear a retry's replacement fence.
type fenceToken struct{ _ byte }

// vm is one sandbox record. The capacityHeld and computeHeld flags are
// guarded by Manager.mu; every other field by vm.mu.
//
// Lock order: Manager.mu may be held while taking vm.mu, never the reverse.
type vm struct {
	id string

	mu         sync.Mutex
	meta       storage.VMMeta
	state      nodev1.VMState
	errMsg     string
	process    VMProcess
	conn       *grpc.ClientConn
	client     guestv1.GuestClient
	bootDone   chan struct{}
	bootErr    error
	bootCancel context.CancelFunc
	waiters    int
	deleting   bool
	// startKeep marks a boot launched by StartVM over a kept stopped disk:
	// an aborted boot returns the disk to STOPPED instead of removing it.
	// Guarded by vm.mu; set once when the start owns the record.
	startKeep bool
	// startOriginal is the pre-claim manifest of a StartVM boot: the restore
	// point for a durable disk phase that fails before anything persisted.
	// Guarded by vm.mu; written at the claim, read by the boot worker.
	startOriginal storage.VMMeta
	// bootGen counts boot generations on this record. A StartVM claim
	// increments it together with the bootDone/bootCancel reset, so boot
	// waiters can tell whether their ticket still belongs to the current
	// boot. Guarded by vm.mu.
	bootGen uint64
	// stopDone is closed when the current stop attempt finishes (STOPPED or
	// rolled back). Guarded by vm.mu like bootDone: replaced and closed only
	// under the lock, so waiters can snapshot the channel safely.
	stopDone chan struct{}
	// del is the current delete attempt. Duplicate deletes capture the
	// pointer they joined, so a retry replacing it can never close their
	// channel or overwrite their result.
	del           *deleteAttempt
	deleteWaiters int

	// ownershipUnresolved marks a recovered record whose process could not
	// be proven dead or safely owned. DeleteVM refuses to remove its disk or
	// reuse its reservation until absence is proven; unresolved may keep the
	// pinned process handle used for that proof.
	ownershipUnresolved bool
	unresolved          *pinnedProc

	// snapshotGate serializes SaveSnapshot, StopVM, StartVM's disk phase and
	// DeleteVM. snapshotRelease pins a source snapshot until its private
	// child copy is complete. snapshotClient replaces a dead QMP controller
	// after a successful rescue so later saves do not fail forever against
	// the original connection.
	snapshotGate    chan struct{}
	snapshotRelease func()
	snapshotSource  *storage.Snapshot
	snapshotClient  *qmp.Client
	// restore is a CreateVM from_manifest's checked manifest: the create's
	// boot writes it into the new disk before the first launch. Set at
	// joinCreate and never changed.
	restore *restoreSource

	// jail names the attempt that owns this VM's jail reservation, or is
	// zero when it holds none. It is assigned before the jailer runs, so a
	// failed launch that retained a reservation is still settled, and is
	// cleared only by settlement of that exact attempt. Guarded by vm.mu.
	jail jailer.Ref

	// maintCancel fences a running maintenance session (maintenance.go):
	// set under vm.mu by the session after its deleting check, cleared when
	// it ends. DeleteVM cancels it before waiting for the operation gate.
	maintCancel context.CancelCauseFunc
	// snapCancel fences a running live SnapshotVM (phase2_snapshot.go): set
	// under vm.mu by the snapshot once it owns the operation gate and has
	// seen deleting false, cleared when it ends. DeleteVM cancels it before
	// waiting for the gate; the snapshot tears its cut down and returns.
	snapCancel context.CancelCauseFunc

	capacityHeld bool
	// fleeceGiB is the disk a live snapshot's fleece image holds in
	// Manager.reserved while it may exist (phase2_snapshot.go). Guarded by
	// Manager.mu.
	fleeceGiB uint64
	// computeHeld reports that this VM's vCPU and memory are counted in
	// Manager.reserved. A STOPPED VM holds only its disk (capacityHeld)
	// until StartVM re-acquires compute or DeleteVM removes everything.
	computeHeld bool
}

// New opens the node's durable state, verifies every image, recovers the VMs
// left by a previous node process and returns a ready manager. ctx should be
// the process lifetime; canceling it stops boot workers but does not stop
// running VMs.
func New(ctx context.Context, cfg Config) (*Manager, error) {
	cfg.Log = cmp.Or(cfg.Log, slog.New(slog.DiscardHandler))
	if err := normalizeConfig(&cfg); err != nil {
		return nil, err
	}
	excludedCID, err := startupLocalCID(ctx, cfg.Log, cfg.MinCID, cfg.MaxCID, cfg.localCID)
	if err != nil {
		return nil, fmt.Errorf("node: %w", err)
	}
	// With a jailer, every qemu-img run is a jailed tool attempt and storage
	// consults the jailer's ownership index before removing anything.
	jail := cfg.jail
	if jail == nil && cfg.Jailer != nil {
		jail = jailerRunner{cfg.Jailer}
	}
	storeCfg := storage.Config{
		DataDir:   cfg.DataDir,
		ImageDir:  cfg.ImageDir,
		Imager:    cfg.Imager,
		Snapshots: cfg.snapshotOptions,
		Log:       cfg.Log,
	}
	if jail != nil {
		storeCfg.Leases = jailLeases{jail}
	}
	if cfg.Jailer != nil && cfg.Imager == nil {
		storeCfg.Imager = storage.NewJailedImager(jailToolRunner{m: cfg.Jailer, binary: cmp.Or(cfg.JailToolBinary, DefaultJailToolBinary), io: cfg.JailIO})
	}
	store, err := storage.Open(storeCfg)
	if err != nil {
		return nil, err
	}
	if jail != nil {
		// Under the sole store lock and before any storage cleanup
		// (image verification, snapshot staging recovery, orphan removal),
		// earlier tool attempts are settled by proof; retained ones keep
		// their targets leased.
		report, err := jail.SettleTools(ctx)
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("node: settle image tools: %w", err)
		}
		for _, ref := range report.Retained {
			cfg.Log.Warn("node: image tool attempt retained; its target stays held", "tool", ref.SandboxID, "attempt", ref.Attempt)
		}
	}
	images, err := store.VerifyImages(ctx)
	if err != nil {
		store.Close()
		return nil, err
	}
	if len(images) == 0 {
		store.Close()
		return nil, fmt.Errorf("node: image dir %s holds no verified image", cfg.ImageDir)
	}
	byID := make(map[string]storage.Image, len(images))
	for _, img := range images {
		byID[img.ID] = img
	}
	if cfg.DefaultImage == "" {
		cfg.DefaultImage = images[0].ID
	} else if _, ok := byID[cfg.DefaultImage]; !ok {
		store.Close()
		return nil, fmt.Errorf("node: default image %q is not installed under %s", cfg.DefaultImage, cfg.ImageDir)
	}
	// The installed qemu-img's semantics are checked on the default image's
	// verified base, still under the sole store lock and after SettleTools,
	// before any snapshot recovery; a mismatch fails startup.
	if selfCheck := toolSelfCheckFor(cfg); selfCheck != nil {
		report, err := selfCheck(ctx, store, byID[cfg.DefaultImage])
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("node: qemu-img self-check: %w", err)
		}
		cfg.Log.Info("node: qemu-img self-check passed", "image", report.Image, "steps", report.Steps,
			"recovered", report.Recovered, "elapsed", report.Elapsed)
	}
	recoveredSnapshots, err := store.RecoverSnapshots(ctx, images)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("node: recover snapshots: %w", err)
	}
	snapshots := make(map[string]storage.Snapshot, len(recoveredSnapshots))
	for _, snap := range recoveredSnapshots {
		snapshots[snap.Meta.SnapshotID] = snap
	}

	var chunks *chunkcache.Cache
	if cfg.Store != nil {
		chunks, err = chunkcache.Open(ctx, chunkcache.Options{
			Dir:      filepath.Join(store.DataDir(), chunkCacheDir),
			MaxBytes: cfg.ChunkCacheBytes,
			Log:      cfg.Log,
		})
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("node: open chunk cache: %w", err)
		}
	}

	mctx, cancel := context.WithCancel(ctx)
	m := &Manager{
		cfg:            cfg,
		log:            cfg.Log,
		store:          store,
		images:         byID,
		net:            cfg.Network,
		launcher:       cfg.Launcher,
		dialer:         cfg.GuestDialer,
		capacity:       resourcesFrom(cfg.Capacity),
		hostVCPUs:      cfg.HostVCPUs,
		diskOvercommit: cfg.DiskOvercommit,
		excludedCID:    excludedCID,
		ctx:            mctx,
		cancel:         cancel,
		vms:            map[string]*vm{},
		snapshots:      snapshots,
		snapshotOps:    map[string]*snapshotOperation{},
		quarantined:    map[string]struct{}{},
		fence:          map[string]*fenceToken{},
		creating:       map[string]int{},
		chunks:         chunks,
	}
	if m.launcher == nil {
		m.launcher = qemuLauncher{}
	}
	if m.dialer == nil {
		m.dialer = vsockDialer{}
	}
	m.qemuExe = cmp.Or(cfg.qemuExe, qemuExecutable())
	m.diskFree = cfg.diskFree
	if m.diskFree == nil {
		m.diskFree = store.DiskFreeBytes
	}
	m.now = cfg.snapshotNow
	if m.now == nil {
		m.now = time.Now
	}
	// The jailed launcher is selected only by an explicit jailer (or the
	// test seam); lifecycle wiring consumes m.jail.
	m.jail = cfg.jail
	if m.jail == nil && cfg.Jailer != nil {
		m.jail = jailerRunner{cfg.Jailer}
	}
	m.jailDial = cfg.jailDial
	m.jailVsock = cfg.jailVsock
	m.jailCleanup = cfg.jailCleanup
	m.jailStopGrace = cfg.jailStopGrace
	m.findProcess = cfg.findProcess
	if m.findProcess == nil {
		m.scanOwned = true
		m.findProcess = func(meta storage.VMMeta) (*pinnedProc, error) {
			return findOwnedPID(meta, m.store.ImageDir(), m.store.VMDir(meta.SandboxID), m.store.DiskPath(meta.SandboxID), m.qemuExe)
		}
	}
	m.verifyProc = cfg.verifyProc
	if m.verifyProc == nil {
		m.verifyProc = m.verifyQMP
		if m.jail != nil {
			// A jailed QEMU's socket is in its chroot control directory.
			m.verifyProc = func(ctx context.Context, pinned *pinnedProc, meta storage.VMMeta) (*qmp.Client, error) {
				return m.verifyQMPAt(ctx, pinned, meta, m.jailControlQMP(meta.SandboxID), true)
			}
		}
	}
	cfg.QemuVersion = qemuVersion(mctx, cfg.QemuVersion, m.log)
	m.cfg.QemuVersion = cfg.QemuVersion
	if cfg.Phase2 != nil {
		m.setPhase2Support(*cfg.Phase2)
	}

	if m.net != nil {
		// The storage lock is held (store.Open) and no VM is exposed yet:
		// restore the owned rules for every durable allocation before any
		// adoption can serve traffic. A failure refuses startup and keeps
		// every potential live resource for the retry.
		if err := m.restoreNetwork(mctx); err != nil {
			return nil, errors.Join(err, m.shutdown())
		}
	}
	if err := m.reconcile(mctx); err != nil {
		// Partial recovery must release everything it owns: adopted QMP and
		// guest connections, watchers, drains and the data lock. Ready VMs
		// stay on disk for the next manager to adopt.
		return nil, errors.Join(err, m.shutdown())
	}
	if m.net != nil {
		// Recovery decided every record's fate: release exactly the
		// allocations nothing live can still claim. The manager never touches
		// the network manager itself (Config.Network is borrowed).
		if err := m.pruneNetwork(mctx); err != nil {
			return nil, errors.Join(err, m.shutdown())
		}
	}
	// Every m.vms access runs under m.mu, including this constructor-time
	// log line's count: take the snapshot under the lock, log after.
	m.mu.Lock()
	vmCount := len(m.vms)
	m.mu.Unlock()
	m.log.Info("node: manager ready",
		"node", cfg.NodeID,
		"images", len(images),
		"default_image", cfg.DefaultImage,
		"vms", vmCount,
	)
	return m, nil
}

// normalizeConfig applies defaults and validates the manager settings.
func normalizeConfig(cfg *Config) error {
	if cfg.NodeID == "" {
		return errors.New("node: NodeID is required")
	}
	if cfg.DataDir == "" {
		return errors.New("node: DataDir is required")
	}
	if cfg.ImageDir == "" {
		return errors.New("node: ImageDir is required")
	}
	if cfg.MinCID == 0 && cfg.MaxCID == 0 {
		cfg.MinCID, cfg.MaxCID = DefaultMinCID, DefaultMaxCID
	} else if cfg.MinCID == 0 || cfg.MaxCID == 0 {
		return errors.New("node: MinCID and MaxCID must both be set")
	}
	if cfg.MinCID < 3 || cfg.MaxCID == math.MaxUint32 || cfg.MinCID > cfg.MaxCID {
		return fmt.Errorf("node: invalid vsock CID range %d..%d", cfg.MinCID, cfg.MaxCID)
	}
	if cfg.BootTimeout <= 0 {
		cfg.BootTimeout = DefaultBootTimeout
	}
	if cfg.SnapshotLease <= 0 {
		cfg.SnapshotLease = DefaultSnapshotLease
	}
	if cfg.SnapshotLease > time.Hour || cfg.SnapshotLease/time.Millisecond == 0 {
		return fmt.Errorf("node: SnapshotLease %s is outside 1ms..1h", cfg.SnapshotLease)
	}
	if cfg.SnapshotCleanupTimeout <= 0 {
		cfg.SnapshotCleanupTimeout = DefaultSnapshotCleanupTimeout
	}
	if cfg.SnapshotFreeFloor == 0 {
		cfg.SnapshotFreeFloor = DefaultSnapshotFreeFloor
	}
	if cfg.Jailer != nil || cfg.jail != nil {
		// A jailed manager never detects or execs QEMU itself: the version
		// comes from the verified startup probe and every launch is TAP.
		switch {
		case cfg.Network == nil:
			return errors.New("node: the jailed launcher requires TAP networking")
		case cfg.QemuVersion == "":
			return errors.New("node: a jailed manager needs the QEMU version proven by the startup probe")
		case cfg.JailBinary == "" || !strings.HasPrefix(cfg.JailBinary, "/"):
			return errors.New("node: a jailed manager needs the absolute trusted QEMU binary")
		case cfg.Launcher != nil:
			return errors.New("node: Launcher cannot be combined with the jailed launcher")
		}
		if err := cfg.JailIO.Device; err.Major == 0 {
			return errors.New("node: a jailed manager needs the per-VM IO limit device")
		}
	}
	switch {
	case cfg.Network == nil && !cfg.AllowUserNetworking:
		return errors.New("node: Network is required for per-VM TAP networking; " +
			"AllowUserNetworking is the explicit dev/test fallback and never a production setting")
	case cfg.Network != nil && cfg.AllowUserNetworking:
		return errors.New("node: AllowUserNetworking cannot be combined with Network")
	}
	if cfg.CPUModel != "" {
		hostAllowed := cfg.AllowHostCPU && cfg.CPUModel == "host"
		if !hostAllowed {
			if err := validCPUModel(cfg.CPUModel); err != nil {
				return fmt.Errorf("node: %w", err)
			}
		}
	}
	nested, err := qemu.ParseNestedMode(string(cfg.Nested))
	if err != nil {
		return fmt.Errorf("node: %w", err)
	}
	cfg.Nested = nested
	if _, err := qemu.ParseCPUFeatures(strings.Join(cfg.CPUFeatures, ",")); err != nil {
		return fmt.Errorf("node: %w", err)
	}
	cfg.CPUFeatures = slices.Clone(cfg.CPUFeatures)
	if cfg.HostVCPUs == 0 {
		cfg.HostVCPUs = uint32(runtime.NumCPU())
	}
	if cfg.DiskOvercommit == 0 {
		cfg.DiskOvercommit = 1
	}
	if err := overcommit.Validate(cfg.DiskOvercommit); err != nil {
		return fmt.Errorf("node: disk overcommit: %w", err)
	}
	if cfg.Capacity == nil || cfg.Capacity.GetVcpus() == 0 ||
		cfg.Capacity.GetMemoryMib() == 0 || cfg.Capacity.GetDiskGib() == 0 {
		return errors.New("node: Capacity vcpus, memory_mib and disk_gib are required")
	}
	clone := resourcesFrom(cfg.Capacity)
	if clone.vcpus == 0 || clone.memoryMib == 0 || clone.diskGiB == 0 {
		return errors.New("node: Capacity vcpus, memory_mib and disk_gib are required")
	}
	// Clone immediately: the caller keeps ownership of its message.
	cfg.Capacity = clone.proto()
	return nil
}

// Register adds the manager to a gRPC server.
func (m *Manager) Register(s *grpc.Server) { nodev1.RegisterNodeServer(s, m) }

// Shutdown stops boot workers and watchers, closes guest connections and
// releases the data-directory lock. Running VMs keep running so the next node
// process can adopt them; a boot that never reached READY is cleaned up.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	if !m.shutdownStarted {
		m.shutdownStarted = true
		m.closed = true
		m.shutdownDone = make(chan struct{})
		for _, v := range m.vms {
			v.mu.Lock()
			if v.bootCancel != nil {
				v.bootCancel()
			}
			v.mu.Unlock()
		}
		// Cleanup runs to completion even if the first caller's deadline
		// expires; a later Shutdown call reports the real outcome.
		go func() {
			m.shutdownErr = m.shutdown()
			close(m.shutdownDone)
		}()
	}
	done := m.shutdownDone
	m.mu.Unlock()

	select {
	case <-done:
		return m.shutdownErr
	case <-ctx.Done():
		return fmt.Errorf("node: shutdown still running: %w", context.Cause(ctx))
	}
}

// shutdown cancels all workers, closes every control connection and then
// releases the data-directory lock. Running VMs are left running for the next
// node process to adopt; a boot that never reached READY is cleaned up by its
// worker before wg.Wait returns.
func (m *Manager) shutdown() error {
	m.cancel()
	m.wg.Wait()

	m.mu.Lock()
	vms := slices.Collect(maps.Values(m.vms))
	m.mu.Unlock()
	var errs []error
	for _, v := range vms {
		v.mu.Lock()
		proc := v.process
		conn := v.conn
		snapshotClient := v.snapshotClient
		unresolved := v.unresolved
		v.process, v.conn, v.client = nil, nil, nil
		v.snapshotClient = nil
		v.unresolved = nil
		v.mu.Unlock()
		if snapshotClient != nil {
			errs = append(errs, snapshotClient.Close())
		}
		if proc != nil {
			errs = append(errs, proc.Close())
			if rel, ok := proc.(interface{ Release() error }); ok {
				// Explicit owner shutdown is the only other place a pidfd
				// may be closed without a proven exit.
				errs = append(errs, rel.Release())
			}
		}
		if conn != nil {
			errs = append(errs, conn.Close())
		}
		// Release the pidfd without killing: the next node process rescans
		// and re-proves ownership.
		if unresolved != nil {
			errs = append(errs, unresolved.close())
		}
	}
	m.drains.Wait()
	if m.chunks != nil {
		errs = append(errs, m.chunks.Close())
	}
	errs = append(errs, m.store.Close())
	return errors.Join(errs...)
}

// validCPUModel mirrors the qemu package's fixed-model rule: a configured or
// qemu.DefaultCPUModel named model, never the host CPU.
func validCPUModel(s string) error {
	if strings.EqualFold(s, "host") || strings.EqualFold(s, "max") {
		return fmt.Errorf("CPU model %q is host-dependent; use a fixed named model", s)
	}
	for i := range len(s) {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			continue
		}
		switch c {
		case '.', '_', '+', '-':
			continue
		default:
			return fmt.Errorf("invalid CPU model %q", s)
		}
	}
	return nil
}

// diskPromiseBytes returns the checked logical disk promise for current live
// free bytes. Admission and Info share this exact arithmetic.
func (m *Manager) diskPromiseBytes(free uint64) (uint64, error) {
	ratio := m.diskOvercommit
	if ratio == 0 {
		ratio = 1 // direct Manager literals in tests; New always normalizes.
	}
	return overcommit.Scale(free, ratio)
}

// Info reports the node's identity, offer, installed images and committed
// snapshots. Every message is newly allocated for the caller.
func (m *Manager) Info(context.Context, *nodev1.InfoRequest) (*nodev1.InfoResponse, error) {
	free, err := m.diskFree()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "disk free: %v", err)
	}
	m.mu.Lock()
	reserved := m.reserved
	capacity := m.capacity
	committed := slices.Collect(maps.Values(m.snapshots))
	m.mu.Unlock()

	// Outstanding sparse-disk promises are charged against the same checked
	// live-free promise admission uses (double counting physical usage). A
	// promise that has not materialised yet still cannot be handed out twice.
	promise, err := m.diskPromiseBytes(free)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "disk promise: %v", err)
	}
	reservedBytes, ok := reserved.diskBytes()
	if !ok || reservedBytes >= promise {
		free = 0
	} else {
		free = promise - reservedBytes
	}
	capacityLeft := capacity.diskGiB
	if reserved.diskGiB >= capacityLeft {
		capacityLeft = 0
	} else {
		capacityLeft -= reserved.diskGiB
	}
	freeMib := free >> 20
	// Compare whole GiB first: capacityLeft can be larger than free space by
	// orders of magnitude, and shifting it blindly could overflow. The <= is
	// deliberate: an equal whole-GiB count must still discard a fractional
	// remainder (or a sub-GiB promise when the offer has no whole GiB left).
	if capacityLeft <= freeMib>>10 {
		freeMib = capacityLeft << 10
	}
	images := make([]*nodev1.Image, 0, len(m.images))
	for _, id := range slices.Sorted(maps.Keys(m.images)) {
		img := m.images[id]
		images = append(images, &nodev1.Image{Id: img.ID, Sha256: img.RootSHA256})
	}
	snapshotMessages := make([]*nodev1.Snapshot, 0, len(committed))
	for _, snap := range committed {
		snapshotMessages = append(snapshotMessages, snapshotProto(snap))
	}
	slices.SortFunc(snapshotMessages, func(a, b *nodev1.Snapshot) int {
		return cmp.Compare(a.GetSnapshotId(), b.GetSnapshotId())
	})
	return &nodev1.InfoResponse{
		NodeId:       m.cfg.NodeID,
		Vcpus:        capacity.vcpus,
		MemoryMib:    capacity.memoryMib,
		DiskFreeMib:  freeMib,
		CpuModel:     cmp.Or(m.cfg.CPUModel, qemu.DefaultCPUModel),
		QemuVersion:  m.cfg.QemuVersion,
		Images:       images,
		Snapshots:    snapshotMessages,
		DefaultImage: m.cfg.DefaultImage,
	}, nil
}

// imageFor returns the verified image a create request names.
func (m *Manager) imageFor(requested string) (storage.Image, error) {
	name := cmp.Or(requested, m.cfg.DefaultImage)
	img, ok := m.images[name]
	if !ok {
		return storage.Image{}, status.Errorf(codes.NotFound, "image %q not found", name)
	}
	return img, nil
}

// allocCIDLocked returns the first CID in the configured range not used by a
// known VM and not the host's own local CID. Every VM keeps its CID,
// including FAILED records whose disk is preserved, so a live QEMU can never
// collide with a new one. m.mu must be held.
func (m *Manager) allocCIDLocked() (uint32, bool) {
	used := make(map[uint32]struct{}, len(m.vms))
	for _, v := range m.vms {
		used[v.meta.VsockCID] = struct{}{}
	}
	for cid := m.cfg.MinCID; ; cid++ {
		if _, ok := used[cid]; !ok && cid != m.excludedCID {
			return cid, true
		}
		// Check exhaustion after the exclusion skip: an excluded MaxCID is
		// the last candidate, not a reason to wrap the uint32.
		if cid == m.cfg.MaxCID {
			return 0, false
		}
	}
}

// isCurrent reports whether v is still the map entry for its sandbox id. A
// delete owner must check this before touching the directory, because an
// aborted boot may already have removed the generation or a later create may
// own the id.
func (m *Manager) isCurrent(v *vm) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.vms[v.id] == v
}

// releaseReservationLocked returns v's capacity to the pool exactly once:
// compute first, then the disk. m.mu must be held.
func (m *Manager) releaseReservationLocked(v *vm) {
	m.releaseComputeLocked(v)
	if !v.capacityHeld {
		return
	}
	v.capacityHeld = false
	m.reserved.diskGiB = subClamp(m.reserved.diskGiB, v.meta.Resources.DiskGiB)
}

// releaseComputeLocked returns only v's vCPU and memory, keeping the disk
// accounted: a STOPPED VM frees compute while its kept disk stays in the
// stopped inventory. m.mu must be held.
func (m *Manager) releaseComputeLocked(v *vm) {
	// Without its QEMU no fleece image can be held open either.
	m.releaseFleeceLocked(v)
	if !v.computeHeld {
		return
	}
	v.computeHeld = false
	m.reserved.vcpus = subClamp(m.reserved.vcpus, v.meta.Resources.VCPUs)
	m.reserved.memoryMib = subClamp(m.reserved.memoryMib, v.meta.Resources.MemoryMiB)
}

// reserveStoppedDiskLocked accounts a kept disk of a stopped VM that is not
// counted yet (for example a FAILED VM whose watcher released everything
// before a stop completed). m.mu must be held.
func (m *Manager) reserveStoppedDiskLocked(v *vm) {
	if v.capacityHeld {
		return
	}
	total, ok := m.reserved.add(resources{diskGiB: v.meta.Resources.DiskGiB})
	if !ok {
		total = resources{vcpus: m.reserved.vcpus, memoryMib: m.reserved.memoryMib, diskGiB: math.MaxUint64}
	}
	m.reserved = total
	v.capacityHeld = true
}

// releaseDiskDeltaLocked hands back a growth delta a start applied but never
// made durable. The kept disk's base reservation is untouched. m.mu must be
// held.
func (m *Manager) releaseDiskDeltaLocked(deltaGiB uint64) {
	m.reserved.diskGiB = subClamp(m.reserved.diskGiB, deltaGiB)
}

// reserveForceLocked accounts a real live VM even when a reduced offer cannot
// cover it. The counters may exceed capacity (so CreateVM refuses new work)
// but never forget usage that exists on the host. m.mu must be held.
func (m *Manager) reserveForceLocked(v *vm) {
	total, ok := m.reserved.add(resourcesOf(v.meta.Resources))
	if !ok {
		total = resources{vcpus: math.MaxUint32, memoryMib: math.MaxUint64, diskGiB: math.MaxUint64}
	}
	m.reserved = total
	v.capacityHeld = true
	v.computeHeld = true
}

func subClamp[T uint32 | uint64](a, b T) T {
	if b >= a {
		return 0
	}
	return a - b
}

// resourcesOf converts durable sizing into the manager's counters.
func resourcesOf(r storage.Resources) resources {
	return resources{vcpus: r.VCPUs, memoryMib: r.MemoryMiB, diskGiB: r.DiskGiB}
}

func storageResources(r *nodev1.Resources) storage.Resources {
	return storage.Resources{VCPUs: r.GetVcpus(), MemoryMiB: r.GetMemoryMib(), DiskGiB: r.GetDiskGib()}
}

// cloneVM renders the RPC view of v with v.mu held.
func (v *vm) cloneVM() *nodev1.VM {
	vm := &nodev1.VM{
		SandboxId:       v.meta.SandboxID,
		State:           v.state,
		Image:           v.meta.Image,
		FromSnapshot:    v.meta.FromSnapshot,
		Resources:       resourcesOf(v.meta.Resources).proto(),
		CreatedAtUnixMs: v.meta.CreatedAtUnixMs,
		Error:           v.errMsg,
		StartedAtUnixMs: v.meta.StartedAtUnixMs,
		FromManifest:    v.meta.FromManifest,
		Hydration:       hydrationOf(v.meta),
	}
	return vm
}

// readyCommitted mirrors storage.Store.CommitReady in the in-memory record
// once that write succeeded. Callers hold v.mu.
func (v *vm) readyCommitted(startedAtUnixMs int64) {
	v.meta.StartedAtUnixMs = startedAtUnixMs
	v.meta.StoppedAtUnixMs = 0
	v.meta.HydratedAtUnixMs = cmp.Or(v.meta.HydratedAtUnixMs, startedAtUnixMs)
}

// hydrationOf is a VM's Hydration (always set, as the proto says). A VM not
// restored from a manifest is hydrated from the start; records written
// before Phase 2 have no hydration time and report their creation. Phase 2a
// restores are eager, so a restored VM is hydrated, with every chunk done,
// from its first READY on.
func hydrationOf(meta storage.VMMeta) *nodev1.Hydration {
	if meta.FromManifest == "" {
		return &nodev1.Hydration{
			Hydrated:         true,
			HydratedAtUnixMs: cmp.Or(meta.HydratedAtUnixMs, meta.CreatedAtUnixMs),
		}
	}
	h := &nodev1.Hydration{ChunksTotal: meta.RestoreChunks}
	if meta.HydratedAtUnixMs != 0 {
		h.Hydrated = true
		h.HydratedAtUnixMs = meta.HydratedAtUnixMs
		h.ChunksDone = meta.RestoreChunks
	}
	return h
}

// lookup returns the VM record, or a NotFound status for an unknown id.
func (m *Manager) lookup(id string) (*vm, error) {
	if !ids.ValidSandbox(id) {
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", id)
	}
	m.mu.Lock()
	v := m.vms[id]
	m.mu.Unlock()
	if v == nil {
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", id)
	}
	return v, nil
}

// ready returns the guest client of a READY VM. A VM that exists but cannot
// serve commands is FAILED_PRECONDITION, as the proto documents.
func (m *Manager) ready(id string) (guestv1.GuestClient, error) {
	v, err := m.lookup(id)
	if err != nil {
		return nil, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.deleting {
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", id)
	}
	if v.state != nodev1.VMState_VM_STATE_READY {
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s is %s", id, vmStateName(v.state))
	}
	if v.client == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "sandbox %s has no guest connection", id)
	}
	return v.client, nil
}

func vmStateName(s nodev1.VMState) string {
	switch s {
	case nodev1.VMState_VM_STATE_BOOTING:
		return "BOOTING"
	case nodev1.VMState_VM_STATE_READY:
		return "READY"
	case nodev1.VMState_VM_STATE_FAILED:
		return "FAILED"
	case nodev1.VMState_VM_STATE_STOPPING:
		return "STOPPING"
	case nodev1.VMState_VM_STATE_STOPPED:
		return "STOPPED"
	default:
		return "UNSPECIFIED"
	}
}
