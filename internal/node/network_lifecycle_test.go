// Network lifecycle integration tests: the VMNetwork consumer contract wired
// into boot, stop, start, delete, the exit watcher and restart recovery. The
// fake below stages deterministic faults (Setup failure, uncertain Setup
// commit, release failures, blocked Setup) and records every call so tests
// assert exact ordering, keep sets and absence proofs.
package node

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodenet "github.com/nathabonfim59/bamboo-raft/internal/node/net"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// fakeNetwork is a VMNetwork fake with deterministic allocation, staged
// faults and a full call log. Addresses come from 10.42.0.0/16 in allocation
// order and SSH ports from 22000 up, mirroring nodenet's lowest-free policy.
type fakeNetwork struct {
	mu     sync.Mutex
	allocs map[string]nodenet.Allocation
	seq    int
	port   uint16

	setups   []string
	releases []string
	keeps    [][]string
	files    []*os.File

	// setupErr, when set, fails Setup before any allocation is recorded.
	setupErr error
	// uncertainErr, when set, records the allocation and then fails Setup
	// with a nil lease: nodenet's uncertain-commit shape, where the
	// reservation is retained for an adopt retry or a Release.
	uncertainErr error
	// releaseFailures, when positive, fails that many Release calls (each
	// decrementing) before releases succeed again. A failed release keeps
	// the allocation reserved, like nodenet's retryable release.
	releaseFailures int
	// gate, when non-nil, blocks Setup until closed or the context ends.
	gate chan struct{}
	// releaseGate, when non-nil, blocks Release (after recording the
	// attempt) until closed: the deterministic window in which a settlement
	// has published its terminal state but its cleanup still runs.
	releaseGate chan struct{}

	// reconcileErr fails Reconcile calls while set, from the
	// reconcileErrFrom-th call onwards (1-based; zero means every call), so a
	// test can fail exactly the startup restore (1) or the prune (2).
	reconcileErr     error
	reconcileErrFrom int
	reconcileCalls   int
}

func newFakeNetwork() *fakeNetwork {
	return &fakeNetwork{allocs: map[string]nodenet.Allocation{}, port: 22000}
}

// seed pre-allocates an identity for id: the durable state a previous node
// process left behind.
func (f *fakeNetwork) seed(id string) nodenet.Allocation {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.allocLocked(id)
}

func (f *fakeNetwork) allocLocked(id string) nodenet.Allocation {
	f.seq++
	f.port++
	addr := netip.MustParseAddr(fmt.Sprintf("10.42.0.%d", f.seq+1))
	a := nodenet.Allocation{
		SandboxID:   id,
		GuestIP:     addr,
		Prefix:      netip.MustParsePrefix("10.42.0.0/16"),
		Gateway:     netip.MustParseAddr("10.42.0.1"),
		Bridge:      "bbraft0",
		MAC:         fmt.Sprintf("52:54:00:00:00:%02x", f.seq+2),
		TAPName:     fmt.Sprintf("btdead0%02x", f.seq),
		IngressAddr: netip.MustParseAddr("192.0.2.10"),
		SSHPort:     f.port,
	}
	f.allocs[id] = a
	return a
}

func (f *fakeNetwork) Setup(ctx context.Context, sandboxID string) (*nodenet.Lease, error) {
	f.mu.Lock()
	if f.setupErr != nil {
		err := f.setupErr
		f.mu.Unlock()
		return nil, err
	}
	a := f.allocLocked(sandboxID)
	f.setups = append(f.setups, sandboxID)
	gate := f.gate
	uncertain := f.uncertainErr
	f.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
	if uncertain != nil {
		// The allocation is durable, but no lease comes back.
		return nil, uncertain
	}
	tapR, tapW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	vhostR, vhostW, err := os.Pipe()
	if err != nil {
		tapR.Close()
		tapW.Close()
		return nil, err
	}
	tapW.Close()
	vhostW.Close()
	f.mu.Lock()
	f.files = append(f.files, tapR, vhostR)
	f.mu.Unlock()
	return &nodenet.Lease{
		Allocation:  a,
		Resolvers:   []netip.Addr{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("9.9.9.9")},
		SSHEndpoint: netip.MustParseAddrPort(fmt.Sprintf("192.0.2.10:%d", a.SSHPort)),
		Tap:         tapR,
		Vhost:       vhostR,
	}, nil
}

func (f *fakeNetwork) Release(_ context.Context, sandboxID string) error {
	f.mu.Lock()
	f.releases = append(f.releases, sandboxID)
	fail := f.releaseFailures > 0
	releaseGate := f.releaseGate
	if fail {
		f.releaseFailures--
	}
	f.mu.Unlock()
	if releaseGate != nil {
		<-releaseGate
	}
	if fail {
		return errors.New("fake network release failed")
	}
	f.mu.Lock()
	delete(f.allocs, sandboxID)
	f.mu.Unlock()
	return nil
}

func (f *fakeNetwork) SSHEndpoint(sandboxID string) (netip.AddrPort, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.allocs[sandboxID]
	if !ok {
		return netip.AddrPort{}, false
	}
	return netip.MustParseAddrPort(fmt.Sprintf("192.0.2.10:%d", a.SSHPort)), true
}

func (f *fakeNetwork) Allocations() []nodenet.Allocation {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]nodenet.Allocation, 0, len(f.allocs))
	for _, a := range f.allocs {
		out = append(out, a)
	}
	slices.SortFunc(out, func(a, b nodenet.Allocation) int {
		return a.GuestIP.Compare(b.GuestIP)
	})
	return out
}

func (f *fakeNetwork) Reconcile(_ context.Context, live []string) (nodenet.ReconcileReport, error) {
	f.mu.Lock()
	keep := slices.Compact(slices.Sorted(slices.Values(live)))
	f.keeps = append(f.keeps, slices.Clone(keep))
	f.reconcileCalls++
	err := f.reconcileErr
	if err != nil && f.reconcileErrFrom > 0 && f.reconcileCalls < f.reconcileErrFrom {
		err = nil
	}
	var report nodenet.ReconcileReport
	if err == nil {
		for _, a := range f.allocs {
			if slices.Contains(keep, a.SandboxID) {
				continue
			}
			delete(f.allocs, a.SandboxID)
			report.Released = append(report.Released, a.SandboxID)
		}
	}
	f.mu.Unlock()
	if err != nil {
		return nodenet.ReconcileReport{}, err
	}
	return report, nil
}

// Call-log accessors.

func (f *fakeNetwork) setupLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.setups)
}

func (f *fakeNetwork) allocationIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.allocs))
}

func (f *fakeNetwork) keepLog() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]string, len(f.keeps))
	for i, k := range f.keeps {
		out[i] = slices.Clone(k)
	}
	return out
}

func (f *fakeNetwork) lastLeaseFiles() []*os.File {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.files) < 2 {
		return nil
	}
	return slices.Clone(f.files[len(f.files)-2:])
}

// released reports whether Release was attempted for id (successful or not).
func (f *fakeNetwork) released(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.releases, id)
}

// releaseAttempts reports how many Release calls were attempted in total.
func (f *fakeNetwork) releaseAttempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.releases)
}

func awaitCond(t *testing.T, within time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
}

// statFails reports that the descriptor no longer works: closing is the only
// way a handed-out pipe reader stops stat-ing.
func statFails(f *os.File) bool {
	if f == nil {
		return false
	}
	_, err := f.Stat()
	return err != nil
}

// directNetworkConfig builds the Config newHarness would, for tests that call
// New directly because they expect it to fail.
func directNetworkConfig(t *testing.T, dataDir, imageDir string, net VMNetwork, fallback guestv1.GuestServer) Config {
	t.Helper()
	writeTestImage(t, imageDir, testImageID, defaultTestBaseGiB)
	dialer := newFakeGuestDialer(t)
	dialer.fallback = fallback
	t.Cleanup(func() { dialer.stop(t) })
	return Config{
		NodeID:       "node-net-test",
		DataDir:      dataDir,
		ImageDir:     imageDir,
		DefaultImage: testImageID,
		QemuVersion:  "test",
		Capacity:     &nodev1.Resources{Vcpus: 64, MemoryMib: 256 << 10, DiskGib: 1024},
		HostVCPUs:    64,
		MinCID:       3,
		MaxCID:       64,
		Launcher:     &fakeLauncher{},
		GuestDialer:  dialer,
		Imager:       &fakeImager{diskGiB: defaultTestBaseGiB, basePath: filepath.Join(imageDir, testImageID, "root.qcow2")},
		Network:      net,
		diskFree:     func() (uint64, error) { return 1 << 40, nil },
	}
}

// dataDirLockFree proves the failed New released the exclusive data lock.
func dataDirLockFree(t *testing.T, dataDir, imageDir string) {
	t.Helper()
	store, err := storage.Open(storage.Config{DataDir: dataDir, ImageDir: imageDir, Imager: &fakeImager{diskGiB: defaultTestBaseGiB}, Log: quietLog()})
	if err != nil {
		t.Fatalf("data dir still locked after failed New: %v", err)
	}
	store.Close()
}

// oneVMCapacity fills the node with exactly one default test VM, so a kept
// reservation measurably blocks a second create.
func oneVMCapacity() *nodev1.Resources {
	return &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB}
}

func createReq(id string) *nodev1.CreateVMRequest {
	return &nodev1.CreateVMRequest{SandboxId: id, Resources: oneVMCapacity()}
}

// TestNormalizeNetworkModeMatrix pins the Config boundary: production needs a
// network manager, the dev/test user-networking fallback is explicit and
// exclusive, and combining both is a configuration error.
func TestNormalizeNetworkModeMatrix(t *testing.T) {
	base := func(net VMNetwork, allow bool) Config {
		return Config{
			NodeID: "node-test", DataDir: t.TempDir(), ImageDir: t.TempDir(),
			Capacity: &nodev1.Resources{Vcpus: 1, MemoryMib: 64, DiskGib: 1},
			Network:  net, AllowUserNetworking: allow,
		}
	}
	check := func(net VMNetwork, allow bool, wantErr bool) {
		t.Helper()
		cfg := base(net, allow)
		err := normalizeConfig(&cfg)
		if gotErr := err != nil; gotErr != wantErr {
			t.Fatalf("normalizeConfig(network=%v, allow=%t) = %v, wantErr %t", net != nil, allow, err, wantErr)
		}
	}
	check(newFakeNetwork(), false, false)
	check(nil, true, false)
	check(nil, false, true)
	check(newFakeNetwork(), true, true)
}

// TestNetworkBootUsesLeaseAndClosesDescriptors covers the happy wiring: Setup
// runs inside the boot, the launch Config carries the lease identity on the
// fd 3/4 descriptors, and the manager closes its copies right after Start
// returned, on the success path too.
func TestNetworkBootUsesLeaseAndClosesDescriptors(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net})
	prevOnStart := h.launcher.onStart
	var launchNet *qemu.Network
	h.launcher.onStart = func(cfg qemu.Config) {
		if prevOnStart != nil {
			prevOnStart(cfg)
		}
		launchNet = cfg.Network
	}
	const id = "bx_33333333"
	h.create(id, nil)
	files := net.lastLeaseFiles()
	if len(files) != 2 {
		t.Fatalf("lease handed out %d descriptors, want 2", len(files))
	}
	if launchNet == nil {
		t.Fatal("launch ran without the lease network")
	}
	allocs := net.Allocations()
	if len(allocs) != 1 || allocs[0].SandboxID != id {
		t.Fatalf("allocations = %+v", allocs)
	}
	if launchNet.GuestIP != allocs[0].GuestIP || launchNet.MAC != allocs[0].MAC {
		t.Fatalf("launch network identity %+v does not match allocation %+v", launchNet, allocs[0])
	}
	for _, f := range files {
		if !statFails(f) {
			t.Fatal("lease descriptor still open after Start returned")
		}
	}
	if net.released(id) {
		t.Fatal("network released while the VM is READY")
	}
}

// TestNetworkSetupFailureKeepsFailedRecord: a Setup that fails before any
// allocation boots no QEMU; the record stays FAILED with its disk, and the
// release attempt still runs (absence trivially proven) so no residue keeps
// the address reserved.
func TestNetworkSetupFailureKeepsFailedRecord(t *testing.T) {
	net := newFakeNetwork()
	net.setupErr = errors.New("no free address")
	h := newHarness(t, harnessOptions{network: net})
	const id = "bx_33333333"
	_, err := h.mg.CreateVM(t.Context(), createReq(id))
	if status.Code(err) != codes.Internal {
		t.Fatalf("CreateVM error = %v, want Internal", err)
	}
	vm, gerr := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if gerr != nil || vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("record = %+v, %v; want FAILED", vm, gerr)
	}
	awaitCond(t, 2*time.Second, func() bool { return net.released(id) }, "release never attempted after failed setup")
	if got := net.allocationIDs(); len(got) != 0 {
		t.Fatalf("allocation residue: %v", got)
	}
	// The proven-absent failure frees the reservation: a new id boots.
	net.mu.Lock()
	net.setupErr = nil
	net.mu.Unlock()
	h.create("bx_44444444", nil)
}

// TestNetworkUncertainSetupReleaseCleanup pins the uncertain-commit shape:
// Setup can retain a durable allocation and still return an error with no
// lease. The boot must attempt the Release (no QEMU ever ran), which frees
// the retained reservation instead of leaving it to block reuse.
func TestNetworkUncertainSetupReleaseCleanup(t *testing.T) {
	net := newFakeNetwork()
	net.uncertainErr = errors.New("state commit outcome unknown")
	h := newHarness(t, harnessOptions{network: net})
	const id = "bx_33333333"
	if _, err := h.mg.CreateVM(t.Context(), createReq(id)); err == nil {
		t.Fatal("uncertain setup accepted")
	}
	awaitCond(t, 2*time.Second, func() bool { return net.released(id) }, "uncertain allocation never released")
	if slices.Contains(net.allocationIDs(), id) {
		t.Fatalf("uncertain allocation still reserved: %v", net.allocationIDs())
	}
}

// TestNetworkLaunchFailureReleases: a launch that never produced a QEMU is
// proven absent, so the lease descriptors are closed after Start returns and
// the network is released into the FAILED record.
func TestNetworkLaunchFailureReleases(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net})
	h.launcher.mu.Lock()
	h.launcher.fail = errors.New("exec failed")
	h.launcher.mu.Unlock()
	const id = "bx_33333333"
	if _, err := h.mg.CreateVM(t.Context(), createReq(id)); err == nil {
		t.Fatal("failed launch accepted")
	}
	for _, f := range net.lastLeaseFiles() {
		if !statFails(f) {
			t.Fatal("lease descriptor left open by a failed launch")
		}
	}
	awaitCond(t, 2*time.Second, func() bool { return net.released(id) }, "release never attempted after failed launch")
	if got := net.allocationIDs(); len(got) != 0 {
		t.Fatalf("allocation residue after failed launch: %v", got)
	}
}

// TestNetworkLaunchIntentFailureClosesLease: the descriptors are the node's
// only copies from Setup until QEMU duplicates them; a failure between Setup
// and Start (the durable launch intent) must close them too, not only the
// paths around Start itself.
func TestNetworkLaunchIntentFailureClosesLease(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{
		network: net,
		configure: func(cfg *Config) {
			renames := 0
			cfg.snapshotOptions = storage.SnapshotOptions{
				Rename: func(root *os.Root, old, next string) error {
					renames++
					if renames == 2 { // initial manifest, then the launch intent
						return errors.New("disk full")
					}
					return root.Rename(old, next)
				},
			}
		},
	})
	const id = "bx_33333333"
	_, err := h.mg.CreateVM(t.Context(), createReq(id))
	if err == nil || !strings.Contains(err.Error(), "record launch intent") {
		t.Fatalf("CreateVM error = %v, want the launch-intent failure", err)
	}
	for _, f := range net.lastLeaseFiles() {
		if !statFails(f) {
			t.Fatal("lease descriptor left open by the launch-intent failure")
		}
	}
	awaitCond(t, 2*time.Second, func() bool { return net.released(id) }, "release never attempted after the launch-intent failure")
}

// TestNetworkUncertainSetupReleaseFailure: an uncertain Setup (nil lease,
// retained allocation) whose cleanup Release fails keeps a FAILED record
// with its disk and full reservation; the allocation is not reusable, and a
// Stop/Delete retry completes the settlement.
func TestNetworkUncertainSetupReleaseFailure(t *testing.T) {
	net := newFakeNetwork()
	net.uncertainErr = errors.New("state commit outcome unknown")
	net.releaseFailures = 1
	h := newHarness(t, harnessOptions{network: net, capacity: oneVMCapacity()})
	const id = "bx_33333333"
	if _, err := h.mg.CreateVM(t.Context(), createReq(id)); err == nil {
		t.Fatal("uncertain setup accepted")
	}
	awaitCond(t, 2*time.Second, func() bool { return net.released(id) }, "uncertain cleanup never attempted")
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("record = %+v, %v; want FAILED", vm, err)
	}
	if _, serr := os.Stat(filepath.Join(h.dataDir, "vms", id, "disk.qcow2")); serr != nil {
		t.Fatalf("disk removed after a retryable release failure: %v", serr)
	}
	if !slices.Contains(net.allocationIDs(), id) {
		t.Fatal("allocation freed despite the failed release (premature reuse)")
	}
	if _, cerr := h.mg.CreateVM(t.Context(), createReq("bx_44444444")); status.Code(cerr) != codes.ResourceExhausted {
		t.Fatalf("create during the retryable failure = %v, want ResourceExhausted", cerr)
	}
	// The retry path: a stop retry proves absence trivially (no QEMU ever
	// ran), the release now succeeds, and the record settles. The transient
	// Setup fault is gone, so a new boot works again.
	net.mu.Lock()
	net.uncertainErr = nil
	net.mu.Unlock()
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("stop retry after the uncertain cleanup failure: %v", err)
	}
	h.delete(id)
	h.create("bx_44444444", nil)
}

// TestNetworkStaleWatcherSettlementGuard reaches the publish-then-gate gap:
// with the buggy order, a failed stop (or a stop+start onto a newer FAILED
// generation) completes between the watcher's publication and its gate
// acquisition, and the watcher's settlement guard — which did not recheck
// the process — then releases that newer generation's network and
// reservation. The afterFailurePublish hook fires in that exact window; a
// non-blocking probe of the operation gate distinguishes the topology (the
// fixed order already holds the gate there), and the hook stages the newer
// failed-stop generation inline on the buggy one.
func TestNetworkStaleWatcherSettlementGuard(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net, capacity: oneVMCapacity()})
	const id = "bx_33333333"
	h.create(id, nil)
	v, err := h.mg.lookup(id)
	if err != nil {
		t.Fatal(err)
	}
	hookEntered := make(chan struct{})
	staged := false
	h.mg.afterFailurePublish = func() {
		defer close(hookEntered)
		// Probe: in the fixed order the watcher holds the operation gate at
		// this point (the gate's token is out); in the buggy order the token
		// is still in and the gate is free.
		v.mu.Lock()
		g := v.snapshotGate
		v.mu.Unlock()
		held := true
		if g == nil {
			held = false
		} else {
			select {
			case <-g:
				g <- struct{}{} // put the token back
				held = false
			default:
			}
		}
		if held {
			return
		}
		// Buggy topology: stage the newer generation in the exact window —
		// a stop whose network release fails keeps a FAILED record with its
		// full reservation, exactly what the stale watcher must not settle.
		staged = true
		net.mu.Lock()
		net.releaseFailures = 1
		net.mu.Unlock()
		if _, serr := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); serr == nil {
			t.Errorf("staged failed stop succeeded")
		}
	}
	h.launcher.lastProcess().exit(errors.New("segfault"))
	// Every wait is bounded: a wedged watcher fails the test instead of
	// hanging it.
	bound, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	select {
	case <-hookEntered:
	case <-bound.Done():
		t.Fatal("the watcher never reached its publication hook")
	}
	// Join the watcher's settlement: it holds the operation gate through the
	// release and the accounting in both topologies.
	if gerr := acquireVMOperation(bound, v); gerr != nil {
		t.Fatal(gerr)
	}
	releaseVMOperation(v)
	h.mg.afterFailurePublish = nil
	if staged {
		// Buggy behavior: the record carries the failed stop's retryable
		// FAILED state, but the stale watcher freed its reservation anyway.
		vm, gerr := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		if gerr != nil || vm.GetState() != nodev1.VMState_VM_STATE_FAILED || !strings.Contains(vm.GetError(), "stop did not release network") {
			t.Fatalf("record = %+v, %v; want the staged failed stop", vm, gerr)
		}
		if _, cerr := h.mg.CreateVM(t.Context(), createReq("bx_44444444")); cerr == nil {
			t.Fatal("stale watcher freed the failed stop's reservation")
		}
		t.Fatal("stale watcher settled a newer failed-stop generation")
	}
	// Fixed behavior: the watcher settled only its own proven-dead
	// generation — release attempted once, reservation freed legitimately.
	vm, gerr := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if gerr != nil || vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("record = %+v, %v; want the watcher's FAILED", vm, gerr)
	}
	if got := net.releaseAttempts(); got != 1 {
		t.Fatalf("release attempts = %d, want exactly the watcher's own", got)
	}
	h.delete(id)
	h.create("bx_44444444", nil)
}

// TestNetworkRetainedLaunchHandleNoEarlyRelease pins the Launcher
// retained-handle contract: a launcher that cannot prove the child absent
// returns the live process with the error. The boot attaches it, failBoot's
// stop cannot prove absence, so no Release runs and the record keeps its full
// reservation; once a stop retry proves the death, the release completes.
func TestNetworkRetainedLaunchHandleNoEarlyRelease(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net, capacity: oneVMCapacity()})
	retained := newFakeProcess()
	retained.setStopErr(errors.New("kill raced, reap pending"))
	h.launcher.mu.Lock()
	h.launcher.fail = errors.New("startup failed, child status unknown")
	h.launcher.failWith = retained
	h.launcher.mu.Unlock()
	const id = "bx_33333333"
	if _, err := h.mg.CreateVM(t.Context(), createReq(id)); err == nil {
		t.Fatal("retained-handle launch accepted")
	}
	vm, gerr := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if gerr != nil || vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("record = %+v, %v; want FAILED", vm, gerr)
	}
	// The boot settlement finished before CreateVM returned its error (the
	// record only reads FAILED afterwards), so the absence of a release
	// attempt is already final, not a race.
	if net.released(id) {
		t.Fatal("network released while the retained process was not proven dead")
	}
	if !slices.Contains(net.allocationIDs(), id) {
		t.Fatal("allocation freed while the retained process was not proven dead")
	}
	// The whole reservation is retained, not only the allocation: a node
	// sized for this one VM offers nothing until the handle is proven dead.
	if _, cerr := h.mg.CreateVM(t.Context(), createReq("bx_44444444")); status.Code(cerr) != codes.ResourceExhausted {
		t.Fatalf("create while the retained handle keeps the reservation = %v, want ResourceExhausted", cerr)
	}
	// The stop retry proves the death (the handle now stops cleanly) and the
	// settlement completes with the network release.
	retained.setStopErr(nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM retry of retained handle: %v", err)
	}
	awaitCond(t, 2*time.Second, func() bool { return net.released(id) }, "release never attempted after proving the retained handle dead")
	if slices.Contains(net.allocationIDs(), id) {
		t.Fatalf("allocation kept after proven stop: %v", net.allocationIDs())
	}
}

// TestNetworkCommitFailureReleases stages a durable-write failure at
// CommitRunning: QEMU was started and its stop is proven, so the network is
// released into the FAILED record.
func TestNetworkCommitFailureReleases(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{
		network: net,
		configure: func(cfg *Config) {
			renames := 0
			cfg.snapshotOptions = storage.SnapshotOptions{
				Rename: func(root *os.Root, old, next string) error {
					renames++
					if renames == 3 { // initial manifest, launch intent, commit-running
						return errors.New("disk full")
					}
					return root.Rename(old, next)
				},
			}
		},
	})
	const id = "bx_33333333"
	_, err := h.mg.CreateVM(t.Context(), createReq(id))
	if err == nil || !strings.Contains(err.Error(), "record process identity") {
		t.Fatalf("CreateVM error = %v, want a record-process-identity failure", err)
	}
	awaitCond(t, 2*time.Second, func() bool { return net.released(id) }, "release never attempted after commit failure")
}

// TestNetworkPingFailureReleases: a guest that never answers fails the boot
// after its QEMU was stopped with proof, releasing the network.
func TestNetworkPingFailureReleases(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net, fallback: errorPingGuest{}, stubGuest: true, bootTimeout: 500 * time.Millisecond})
	const id = "bx_33333333"
	if _, err := h.mg.CreateVM(t.Context(), createReq(id)); err == nil {
		t.Fatal("unreachable guest accepted")
	}
	awaitCond(t, 3*time.Second, func() bool { return net.released(id) }, "release never attempted after ping failure")
}

// TestNetworkSoleWaiterCancelDuringSetupReleases: the last waiter leaving
// aborts a boot blocked in Setup; the abort's settlement releases the network
// with an independent context even though the boot context is canceled. The
// cancellation is armed only after Setup actually started (the call log is
// the witness), and the owner's Canceled result is joined with a bound.
func TestNetworkSoleWaiterCancelDuringSetupReleases(t *testing.T) {
	net := newFakeNetwork()
	gate := make(chan struct{})
	net.gate = gate
	openGate := sync.OnceFunc(func() { close(gate) })
	defer openGate() // any failure path still frees a parked setup
	h := newHarness(t, harnessOptions{network: net})
	const id = "bx_33333333"
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ownerDone := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(ctx, createReq(id))
		ownerDone <- err
	}()
	awaitCond(t, 2*time.Second, func() bool { return slices.Contains(net.setupLog(), id) }, "setup never started")
	// Capture the boot's own completion channel while the record is provably
	// alive: the release witness below fires mid-settlement, long before the
	// boot worker removes the record and closes bootDone.
	v, verr := h.mg.lookup(id)
	if verr != nil {
		t.Fatal(verr)
	}
	v.mu.Lock()
	bootDone := v.bootDone
	v.mu.Unlock()
	cancel()
	select {
	case err := <-ownerDone:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("canceled owner error = %v, want Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("canceled owner never returned")
	}
	// The aborted boot is terminal only when its own boot channel closes;
	// the release witness alone is not completion.
	select {
	case <-bootDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the aborted boot never settled")
	}
	if h.mg.isCurrent(v) {
		t.Fatal("the aborted record still owns the id")
	}
	h.mg.mu.Lock()
	reserved := h.mg.reserved
	h.mg.mu.Unlock()
	if reserved != (resources{}) {
		t.Fatalf("the aborted boot left reservation %+v behind", reserved)
	}
	if !net.released(id) {
		t.Fatal("the aborted boot never released the network")
	}
	if slices.Contains(net.allocationIDs(), id) {
		t.Fatalf("aborted boot kept its allocation: %v", net.allocationIDs())
	}
	openGate()
	h.create(id, nil) // one retry, READY: the id is reusable after the settlement
}

// TestNetworkEarlyWitnessRetryObservesAbortWindow is the deterministic
// demonstration of the stress-found hazard, kept separate from the fixture
// above: the network release witness fires while failBoot's settlement is
// still mid-flight — the fake's Release blocks right after logging, holding
// the boot worker between the release and the record removal. A retry
// started at that witness joins the still-BOOTING record (the join is
// witnessed through the waiter count), and when the settlement completes it
// correctly observes the boot's own abort: Aborted, "was deleted while
// booting". A fixture must join the boot's completion, not trust the
// release witness.
func TestNetworkEarlyWitnessRetryObservesAbortWindow(t *testing.T) {
	net := newFakeNetwork()
	gate := make(chan struct{})
	net.gate = gate
	openGate := sync.OnceFunc(func() { close(gate) })
	defer openGate()
	releaseBlock := make(chan struct{})
	openRelease := sync.OnceFunc(func() { close(releaseBlock) })
	defer openRelease()
	net.mu.Lock()
	net.releaseGate = releaseBlock
	net.mu.Unlock()
	h := newHarness(t, harnessOptions{network: net})
	const id = "bx_33333333"
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ownerDone := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(ctx, createReq(id))
		ownerDone <- err
	}()
	awaitCond(t, 2*time.Second, func() bool { return slices.Contains(net.setupLog(), id) }, "setup never started")
	v, verr := h.mg.lookup(id)
	if verr != nil {
		t.Fatal(verr)
	}
	cancel()
	select {
	case err := <-ownerDone:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("canceled owner error = %v, want Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("canceled owner never returned")
	}
	// The witness fires with the settlement provably not done: the release
	// is still blocked inside failBoot.
	awaitCond(t, 2*time.Second, func() bool { return net.released(id) }, "release witness never fired")
	// The retry joins the aborting boot while the settlement is held — the
	// waiter count is the join witness (it dropped to zero when the owner
	// left, which is what started the abort).
	retryDone := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(t.Context(), createReq(id))
		retryDone <- err
	}()
	awaitCond(t, 2*time.Second, func() bool {
		v.mu.Lock()
		waiters := v.waiters
		v.mu.Unlock()
		return waiters >= 1
	}, "the early-witness retry never joined the aborting boot")
	openRelease()
	select {
	case err := <-retryDone:
		if status.Code(err) != codes.Aborted || !strings.Contains(err.Error(), "deleted while booting") {
			t.Fatalf("early-witness retry = %v, want Aborted (deleted while booting)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the early-witness retry never returned")
	}
	// The window demonstration ends with a fully settled record.
	if h.mg.isCurrent(v) {
		t.Fatal("the aborted record still owns the id after the window")
	}
}

// TestNetworkSurvivingWaiterKeepsBoot: a canceled owner only drops its
// ticket; a surviving joiner keeps the boot alive through Setup, reaches
// READY and keeps its network until a real stop.
func TestNetworkSurvivingWaiterKeepsBoot(t *testing.T) {
	net := newFakeNetwork()
	gate := make(chan struct{})
	net.gate = gate
	openGate := sync.OnceFunc(func() { close(gate) })
	defer openGate() // any failure path still frees the parked boot
	h := newHarness(t, harnessOptions{network: net})
	const id = "bx_33333333"
	ownerCtx, cancelOwner := context.WithCancel(t.Context())
	defer cancelOwner()
	ownerDone := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(ownerCtx, createReq(id))
		ownerDone <- err
	}()
	awaitCond(t, 2*time.Second, func() bool { return len(net.setupLog()) == 1 }, "setup never started")
	// Park the joiner on the boot before the owner cancels: two tickets are
	// held, so the owner's cancellation can only drop its own.
	joinerDone := make(chan struct{})
	go func() {
		defer close(joinerDone)
		vm, jerr := h.mg.CreateVM(t.Context(), createReq(id))
		if jerr != nil || vm.GetState() != nodev1.VMState_VM_STATE_READY {
			t.Errorf("surviving joiner = %+v, %v; want READY", vm, jerr)
		}
	}()
	v, verr := h.mg.lookup(id)
	if verr != nil {
		t.Fatal(verr)
	}
	awaitCond(t, 2*time.Second, func() bool {
		v.mu.Lock()
		waiters := v.waiters
		v.mu.Unlock()
		return waiters == 2
	}, "joiner never parked on the boot")
	cancelOwner()
	select {
	case err := <-ownerDone:
		if err == nil {
			t.Fatal("canceled owner joined successfully")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("canceled owner never returned")
	}
	openGate()
	select {
	case <-joinerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("surviving joiner never finished")
	}
	if net.released(id) {
		t.Fatal("surviving boot's network released")
	}
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatal(err)
	}
	awaitCond(t, 2*time.Second, func() bool { return net.released(id) }, "stop never released the network")
}

// TestNetworkStopStartDeleteLifecycle walks the settlement chain: stop
// releases the lease, start allocates a fresh one, delete removes the record,
// disk and allocation.
func TestNetworkStopStartDeleteLifecycle(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net})
	const id = "bx_33333333"
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatal(err)
	}
	awaitCond(t, 2*time.Second, func() bool { return net.released(id) }, "stop never released")
	if slices.Contains(net.allocationIDs(), id) {
		t.Fatalf("stopped VM kept an allocation: %v", net.allocationIDs())
	}
	if _, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id}); err != nil {
		t.Fatal(err)
	}
	awaitCond(t, 2*time.Second, func() bool { return len(net.setupLog()) == 2 }, "start never allocated a fresh lease")
	for _, f := range net.lastLeaseFiles() {
		if !statFails(f) {
			t.Fatal("start lease descriptor left open")
		}
	}
	h.delete(id)
	awaitCond(t, 2*time.Second, func() bool {
		return !slices.Contains(net.allocationIDs(), id)
	}, "delete never released the allocation")
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("record survived delete: %v", err)
	}
}

// TestNetworkDistinctIdentities: concurrent VMs never share an address or SSH
// port, and one VM's settlement does not touch another's allocation.
func TestNetworkDistinctIdentities(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net})
	h.create("bx_33333333", nil)
	h.create("bx_44444444", nil)
	allocs := net.Allocations()
	if len(allocs) != 2 {
		t.Fatalf("allocations = %+v", allocs)
	}
	if allocs[0].GuestIP == allocs[1].GuestIP || allocs[0].SSHPort == allocs[1].SSHPort {
		t.Fatalf("shared identity: %+v", allocs)
	}
	h.delete("bx_33333333")
	awaitCond(t, 2*time.Second, func() bool {
		return !slices.Contains(net.allocationIDs(), "bx_33333333")
	}, "delete never released")
	if got := net.allocationIDs(); !slices.Equal(got, []string{"bx_44444444"}) {
		t.Fatalf("delete disturbed the other allocation: %v", got)
	}
}

// TestNetworkStopReleaseFailureRetryable pins the stop settlement: a failed
// release keeps a FAILED record with its full reservation (no premature
// capacity or address reuse), and a StopVM retry completes the settlement.
func TestNetworkStopReleaseFailureRetryable(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net, capacity: oneVMCapacity()})
	const id = "bx_33333333"
	h.create(id, nil)
	net.mu.Lock()
	net.releaseFailures = 1
	net.mu.Unlock()
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err == nil {
		t.Fatal("stop with a failed network release succeeded")
	}
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("record after failed release = %+v, %v; want FAILED", vm, err)
	}
	if !slices.Contains(net.allocationIDs(), id) {
		t.Fatal("release failure freed the allocation (premature reuse)")
	}
	if _, cerr := h.mg.CreateVM(t.Context(), createReq("bx_44444444")); status.Code(cerr) != codes.ResourceExhausted {
		t.Fatalf("create during retryable failure = %v, want ResourceExhausted", cerr)
	}
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("stop retry: %v", err)
	}
	h.delete(id)
	h.create("bx_44444444", nil)
}

// TestNetworkDeleteReleaseFailureKeepsRecord pins release-before-remove: a
// failed release must leave disk, record and reservation untouched for the
// DeleteVM retry.
func TestNetworkDeleteReleaseFailureKeepsRecord(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net})
	const id = "bx_33333333"
	h.create(id, nil)
	net.mu.Lock()
	net.releaseFailures = 1
	net.mu.Unlock()
	if _, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err == nil {
		t.Fatal("delete with a failed network release succeeded")
	}
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("record after failed delete release = %+v, %v; want FAILED", vm, err)
	}
	if _, serr := os.Stat(filepath.Join(h.dataDir, "vms", id, "disk.qcow2")); serr != nil {
		t.Fatalf("disk removed before the release succeeded: %v", serr)
	}
	if !slices.Contains(net.allocationIDs(), id) {
		t.Fatal("delete release failure freed the allocation")
	}
	h.delete(id) // retry: the release now succeeds, everything goes
	if slices.Contains(net.allocationIDs(), id) {
		t.Fatalf("allocation survived the delete retry: %v", net.allocationIDs())
	}
}

// TestNetworkWatcherReleasesAfterProvenExit: an unexpected QEMU exit is
// proven by the reaped Done channel; the watcher publishes FAILED and then
// releases the network and the reservation.
func TestNetworkWatcherReleasesAfterProvenExit(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net})
	const id = "bx_33333333"
	h.create(id, nil)
	h.launcher.lastProcess().exit(errors.New("segfault"))
	awaitCond(t, 5*time.Second, func() bool {
		vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		return err == nil && vm.GetState() == nodev1.VMState_VM_STATE_FAILED
	}, "watcher never published FAILED")
	awaitCond(t, 5*time.Second, func() bool {
		return net.released(id) && !slices.Contains(net.allocationIDs(), id)
	}, "watcher never released the network")
}

// TestNetworkWatcherReleaseFailureKeepsReservation: the watcher's release
// failure keeps the full reservation and the allocation; DeleteVM is the
// retryable path that eventually frees both.
func TestNetworkWatcherReleaseFailureKeepsReservation(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net, capacity: oneVMCapacity()})
	const id = "bx_33333333"
	h.create(id, nil)
	net.mu.Lock()
	net.releaseFailures = 1
	net.mu.Unlock()
	h.launcher.lastProcess().exit(errors.New("segfault"))
	awaitCond(t, 5*time.Second, func() bool {
		vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		return err == nil && vm.GetState() == nodev1.VMState_VM_STATE_FAILED
	}, "watcher never published FAILED")
	// The failed release attempt is not settlement completion: await the
	// worker's terminal settlement (its durable release-failure record)
	// before any retry, so no assertion races the settlement.
	awaitCond(t, 5*time.Second, func() bool {
		vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		return err == nil && strings.Contains(vm.GetError(), "releasing its network failed")
	}, "watcher never recorded its terminal settlement")
	if !slices.Contains(net.allocationIDs(), id) {
		t.Fatal("watcher's failed release freed the allocation")
	}
	if _, cerr := h.mg.CreateVM(t.Context(), createReq("bx_44444444")); status.Code(cerr) != codes.ResourceExhausted {
		t.Fatalf("create while the watcher settlement holds the reservation = %v", cerr)
	}
	h.delete(id)
	if got := net.allocationIDs(); len(got) != 0 {
		t.Fatalf("delete retry did not free the allocation: %v", got)
	}
	h.create("bx_44444444", nil)
}

// TestNetworkWatcherBlockedReleaseWindow pins the mid-settlement invariants
// with the release deterministically blocked: the death is already published
// (FAILED, no READY fast path for StartVM), the full reservation is still
// held, and only the release completing frees it.
func TestNetworkWatcherBlockedReleaseWindow(t *testing.T) {
	net := newFakeNetwork()
	releaseGate := make(chan struct{})
	// Registered before anything spawns: any failure path — a mid-window
	// assertion included — unblocks the watcher's Release, so the harness
	// shutdown can never hang on a stranded settlement.
	openRelease := sync.OnceFunc(func() { close(releaseGate) })
	defer openRelease()
	h := newHarness(t, harnessOptions{network: net, capacity: oneVMCapacity()})
	const id = "bx_33333333"
	h.create(id, nil)
	net.mu.Lock()
	net.releaseGate = releaseGate
	net.mu.Unlock()
	h.launcher.lastProcess().exit(errors.New("segfault"))
	awaitCond(t, 5*time.Second, func() bool {
		vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		return err == nil && vm.GetState() == nodev1.VMState_VM_STATE_FAILED
	}, "FAILED not published while the release is blocked")
	awaitCond(t, 2*time.Second, func() bool { return net.released(id) }, "release never started")
	v, verr := h.mg.lookup(id)
	if verr != nil {
		t.Fatal(verr)
	}
	// Mid-settlement: no capacity handed out, no READY from StartVM.
	if _, cerr := h.mg.CreateVM(t.Context(), createReq("bx_44444444")); status.Code(cerr) != codes.ResourceExhausted {
		t.Fatalf("create while the settlement holds the reservation = %v", cerr)
	}
	if _, serr := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id}); status.Code(serr) != codes.FailedPrecondition {
		t.Fatalf("StartVM mid-settlement = %v, want FailedPrecondition", serr)
	}
	if !slices.Contains(net.allocationIDs(), id) {
		t.Fatal("allocation freed while the release is still running")
	}
	openRelease()
	awaitCond(t, 5*time.Second, func() bool {
		return !slices.Contains(net.allocationIDs(), id)
	}, "release never completed after unblocking")
	// Join the worker's terminal settlement: the watcher holds the per-VM
	// operation gate until its reservation settlement finished, so acquiring
	// it proves the record is fully settled before the retry checks. The
	// join is bounded: a wedged settlement fails the test instead of it.
	joinCtx, cancelJoin := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelJoin()
	if err := acquireVMOperation(joinCtx, v); err != nil {
		t.Fatal(err)
	}
	releaseVMOperation(v)
	h.create("bx_44444444", nil) // the reservation settled
}

// TestNetworkStaleWatcherKeepsNewGeneration: the first generation's watcher
// must observe the replacement process and retire without publishing or
// releasing the new generation's lease. The old watch is invoked explicitly
// and joined through a completion channel once the replacement is READY, so
// the retirement is deterministic: a watcher missing its
// current-record/process guard would publish FAILED over the replacement,
// release its lease and fail every assertion below.
func TestNetworkStaleWatcherKeepsNewGeneration(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net})
	const id = "bx_33333333"
	h.create(id, nil)
	first := h.launcher.lastProcess()
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatal(err)
	}
	awaitCond(t, 2*time.Second, func() bool { return net.released(id) }, "stop never released")
	if _, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id}); err != nil {
		t.Fatal(err)
	}
	awaitCond(t, 2*time.Second, func() bool { return len(net.setupLog()) == 2 }, "start never allocated")

	// The first generation's process exit is already proven (the stop closed
	// its Done), and the record's boot channel belongs to the completed
	// replacement boot: the old watcher runs straight through to its
	// guard and must retire without a side effect.
	v, err := h.mg.lookup(id)
	if err != nil {
		t.Fatal(err)
	}
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		h.mg.watch(v, first)
	}()
	select {
	case <-watchDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the old watcher never retired")
	}
	vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("replacement generation disturbed: %+v, %v", vm, err)
	}
	if got := net.allocationIDs(); !slices.Equal(got, []string{id}) {
		t.Fatalf("replacement allocation disturbed: %v", got)
	}
	if got := net.releaseAttempts(); got != 1 {
		t.Fatalf("release attempts = %d, want only the stop's single release", got)
	}
	h.delete(id)
}

// TestStartVMRefusesReadyWithDeadProcess pins the fast-path guard: while the
// exit watcher is serialized behind the per-VM operation gate, StartVM must
// not hand out READY for a process whose exit is already proven. The state
// settles to FAILED once the gate frees.
func TestStartVMRefusesReadyWithDeadProcess(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net})
	const id = "bx_33333333"
	h.create(id, nil)
	v, err := h.mg.lookup(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := acquireVMOperation(t.Context(), v); err != nil {
		t.Fatal(err)
	}
	h.launcher.lastProcess().exit(errors.New("segfault"))
	_, serr := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if status.Code(serr) != codes.FailedPrecondition {
		t.Fatalf("StartVM of a dead READY process = %v, want FailedPrecondition", serr)
	}
	releaseVMOperation(v)
	awaitCond(t, 5*time.Second, func() bool {
		vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		return err == nil && vm.GetState() == nodev1.VMState_VM_STATE_FAILED
	}, "watcher never settled after the gate freed")
	h.delete(id)
}

// TestNetworkRestartKeepSets: the first reconcile restores rules for every
// durable allocation plus every owned VM directory; the second, after VM
// recovery, releases exactly the allocations of proven-dead or stopped
// records while live, unresolved and quarantined ids keep theirs.
func TestNetworkRestartKeepSets(t *testing.T) {
	t.Run("live adopted", func(t *testing.T) {
		const id = "bx_55555555"
		net := newFakeNetwork()
		alloc := net.seed(id)
		var pinned *pinnedProc
		var qmpPath string
		h := newHarness(t, harnessOptions{
			network:  net,
			fallback: pingOnlyGuest{},
			prepare: func(t *testing.T, dataDir, imageDir string) {
				pinned, qmpPath, _ = spawnIdentityHelper(t, dataDir, imageDir, id, alloc, nil)
			},
			findProcess: func(meta storage.VMMeta) (*pinnedProc, error) {
				if meta.SandboxID == id {
					return pinned, nil
				}
				return nil, nil
			},
			verifyProc: func(ctx context.Context, _ *pinnedProc, _ storage.VMMeta) (*qmp.Client, error) {
				return qmp.Dial(ctx, qmpPath, quietLog())
			},
		})
		vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_READY {
			t.Fatalf("adopted VM = %+v, %v", vm, err)
		}
		keeps := net.keepLog()
		if len(keeps) != 2 || !slices.Equal(keeps[0], []string{id}) || !slices.Equal(keeps[1], []string{id}) {
			t.Fatalf("keep sets = %v", keeps)
		}
		if got := net.setupLog(); len(got) != 0 {
			t.Fatalf("adoption re-opened network: %v", got)
		}
		if got := net.allocationIDs(); !slices.Equal(got, []string{id}) {
			t.Fatalf("live allocation disturbed: %v", got)
		}
	})

	t.Run("unresolved with nil handle", func(t *testing.T) {
		const id = "bx_55555555"
		net := newFakeNetwork()
		net.seed(id)
		h := newHarness(t, harnessOptions{
			network: net,
			prepare: func(t *testing.T, dataDir, imageDir string) {
				prepareVM(t, dataDir, imageDir, id, 7, 0)
			},
			findProcess: func(storage.VMMeta) (*pinnedProc, error) {
				// Ambiguous scan: the record is unresolved with no pinned
				// handle, and its possibly-live lease must survive.
				return nil, errors.New("scan raced")
			},
		})
		vm, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		if err != nil || vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
			t.Fatalf("unresolved record = %+v, %v", vm, err)
		}
		keeps := net.keepLog()
		if len(keeps) != 2 || !slices.Equal(keeps[1], []string{id}) {
			t.Fatalf("keep sets = %v; the nil-handle unresolved record lost its lease", keeps)
		}
		if got := net.allocationIDs(); !slices.Equal(got, []string{id}) {
			t.Fatalf("unresolved allocation released: %v", got)
		}
	})

	t.Run("quarantined keeps allocation", func(t *testing.T) {
		const id = "bx_55555555"
		net := newFakeNetwork()
		net.seed(id)
		h := newHarness(t, harnessOptions{
			network: net,
			qemuExe: "/bin/true",
			prepare: func(t *testing.T, dataDir, imageDir string) {
				prepareVM(t, dataDir, imageDir, id, 7, 0)
				if err := os.WriteFile(filepath.Join(dataDir, "vms", id, "vm.json"), []byte("{corrupt"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		})
		if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
			t.Fatalf("quarantined record exposed: %v", err)
		}
		keeps := net.keepLog()
		if len(keeps) != 2 || !slices.Equal(keeps[1], []string{id}) {
			t.Fatalf("keep sets = %v; quarantined allocation released", keeps)
		}
		if got := net.allocationIDs(); !slices.Equal(got, []string{id}) {
			t.Fatalf("quarantined allocation released: %v", got)
		}
	})

	t.Run("quarantine survives a second restart", func(t *testing.T) {
		const id = "bx_55555555"
		net := newFakeNetwork()
		net.seed(id)
		corrupt := func(t *testing.T, dataDir, imageDir string) {
			prepareVM(t, dataDir, imageDir, id, 7, 0)
			if err := os.WriteFile(filepath.Join(dataDir, "vms", id, "vm.json"), []byte("{corrupt"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		h := newHarness(t, harnessOptions{
			network: net,
			qemuExe: "/bin/true",
			prepare: corrupt,
		})
		keeps := net.keepLog()
		if len(keeps) != 2 || !slices.Equal(keeps[1], []string{id}) {
			t.Fatalf("first boot keep sets = %v", keeps)
		}
		// The second restart finds no VM directory (quarantine moved it to
		// orphans/) and no in-memory quarantine entry: only the durable
		// orphan-name inspection keeps the allocation reserved.
		h2 := stopStartReopen(t, h, "/bin/true", nil, pingOnlyGuest{})
		keeps = net.keepLog()
		if len(keeps) != 4 || !slices.Equal(keeps[3], []string{id}) {
			t.Fatalf("second boot keep sets = %v; the quarantined allocation was pruned", keeps)
		}
		if got := net.allocationIDs(); !slices.Equal(got, []string{id}) {
			t.Fatalf("quarantined allocation lost after reopen: %v", got)
		}
		_ = h2
	})

	t.Run("dead pruned", func(t *testing.T) {
		const id = "bx_55555555"
		net := newFakeNetwork()
		net.seed(id)
		newHarness(t, harnessOptions{
			network: net,
			prepare: func(t *testing.T, dataDir, imageDir string) {
				prepareVM(t, dataDir, imageDir, id, 7, 0)
			},
			findProcess: func(storage.VMMeta) (*pinnedProc, error) { return nil, nil },
		})
		keeps := net.keepLog()
		if len(keeps) != 2 || len(keeps[1]) != 0 {
			t.Fatalf("keep sets = %v", keeps)
		}
		if got := net.allocationIDs(); len(got) != 0 {
			t.Fatalf("dead allocation kept: %v", got)
		}
	})

	t.Run("stopped pruned", func(t *testing.T) {
		const id = "bx_55555555"
		net := newFakeNetwork()
		net.seed(id)
		newHarness(t, harnessOptions{
			network: net,
			prepare: func(t *testing.T, dataDir, imageDir string) {
				prepareVM(t, dataDir, imageDir, id, 7, 0)
				store, err := storage.Open(storage.Config{DataDir: dataDir, ImageDir: imageDir, Imager: &fakeImager{diskGiB: defaultTestBaseGiB}, Log: quietLog()})
				if err != nil {
					t.Fatal(err)
				}
				if err := store.CommitStopped(id, time.Now().UnixMilli()); err != nil {
					t.Fatal(err)
				}
				store.Close()
			},
		})
		keeps := net.keepLog()
		if len(keeps) != 2 || len(keeps[1]) != 0 {
			t.Fatalf("keep sets = %v", keeps)
		}
		if got := net.allocationIDs(); len(got) != 0 {
			t.Fatalf("stopped residue kept: %v", got)
		}
	})
}

// TestNetworkRestartLegacyUserNetworkFailsStartup pins the conservative
// legacy boundary: under production TAP networking, a live verified QEMU
// with no durable network allocation fails startup. The healthy process is
// left running (only verification handles close), no lease is created, and
// the data-dir lock is released for the operator's dev-mode migration.
func TestNetworkRestartLegacyUserNetworkFailsStartup(t *testing.T) {
	const id = "bx_55555555"
	pinned, cmd := pinnedHelper(t)
	qmpSrv := startFakeQMP(t, id)
	dataDir := t.TempDir()
	imageDir := t.TempDir()
	cfg := directNetworkConfig(t, dataDir, imageDir, newFakeNetwork(), pingOnlyGuest{})
	cfg.qemuExe = "/bin/true"
	cfg.findProcess = func(meta storage.VMMeta) (*pinnedProc, error) {
		if meta.SandboxID == id {
			return pinned, nil
		}
		return nil, nil
	}
	cfg.verifyProc = func(ctx context.Context, _ *pinnedProc, _ storage.VMMeta) (*qmp.Client, error) {
		return qmp.Dial(ctx, qmpSrv.path, quietLog())
	}
	prepareVM(t, dataDir, imageDir, id, 7, 0)
	_, err := New(t.Context(), cfg)
	if err == nil || !strings.Contains(err.Error(), "legacy user-mode networking") {
		t.Fatalf("New error = %v, want the legacy user-network refusal", err)
	}
	// The manager closed the verification handles (the pidfd below), but the
	// process itself must be untouched and still running.
	if _, err := pinned.state(); err == nil {
		t.Fatal("the verification pidfd was left open")
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("the legacy process was stopped by the refusal: %v", err)
	}
	dataDirLockFree(t, dataDir, imageDir)
}

// spawnIdentityHelper stages a genuine live helper process whose argv is a
// real launch rendering for alloc (or a user-mode launch when alloc is the
// zero value), plus a valid running-record manifest. mutate adjusts the
// rendered argv before the helper starts, staging identity violations. It
// returns the pinned helper, its QMP path and the executable path.
func spawnIdentityHelper(t *testing.T, dataDir, imageDir, id string, alloc nodenet.Allocation, mutate func([]string) []string) (*pinnedProc, string, string) {
	t.Helper()
	writeTestImage(t, imageDir, testImageID, 1)
	work := filepath.Join(dataDir, "vms", id)
	if err := os.MkdirAll(work, 0o700); err != nil {
		t.Fatal(err)
	}
	disk := filepath.Join(work, "disk.qcow2")
	if err := os.WriteFile(disk, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := qemu.Config{
		SandboxID: id,
		WorkDir:   work,
		ImageDir:  filepath.Join(imageDir, testImageID),
		DiskPath:  disk,
		VCPUs:     2,
		MemoryMiB: 2048,
		VsockCID:  7,
		HostCPU:   &qemu.HostCPU{Vendor: "AuthenticAMD", CPUs: 4},
	}
	if alloc.SandboxID != "" {
		tapR, tapW, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer tapR.Close()
		defer tapW.Close()
		vhostR, vhostW, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer vhostR.Close()
		defer vhostW.Close()
		cfg.Network = &qemu.Network{
			GuestIP:   alloc.GuestIP,
			PrefixLen: alloc.Prefix.Bits(),
			Gateway:   alloc.Gateway,
			MAC:       alloc.MAC,
			Resolvers: []netip.Addr{netip.MustParseAddr("1.1.1.1")},
			Tap:       tapR,
			Vhost:     vhostR,
		}
	}
	argv, err := helperLaunchArgs(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if mutate != nil {
		argv = mutate(argv)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, append([]string{"-test.run=TestNodeHelperProcess", "--"}, argv[1:]...)...)
	cmd.Env = append(os.Environ(),
		"GO_WANT_NODE_HELPER=1",
		"BBRAFT_HELPER_QMP="+cfg.QMPPath(),
		"BBRAFT_HELPER_NAME="+id,
	)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, serr := os.Stat(cfg.QMPPath()); serr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("identity helper never created %s", cfg.QMPPath())
		}
		time.Sleep(10 * time.Millisecond)
	}
	bootID, err := storage.HostBootID()
	if err != nil {
		t.Fatal(err)
	}
	ticks, err := storage.ProcStartTicks(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	meta := storage.VMMeta{
		SchemaVersion: 1, SandboxID: id, Image: testImageID,
		Resources:       storage.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 1},
		VsockCID:        7,
		CreatedAtUnixMs: time.Now().UnixMilli(),
		QEMU: &storage.QEMURecord{
			State: storage.QEMURunning, PID: cmd.Process.Pid, BootID: bootID,
			StartTicks: ticks, StartedAtUnixMs: time.Now().UnixMilli(),
		},
	}
	raw, merr := json.Marshal(meta)
	if merr != nil {
		t.Fatal(merr)
	}
	if err := os.WriteFile(filepath.Join(work, "vm.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	pinned, err := pinProc(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	return pinned, cfg.QMPPath(), exe
}

// TestNetworkRestartAdoptionIdentity pins the production adoption gate: a
// live QEMU is adopted under TAP networking only when its own argv runs
// exactly the network its durable allocation describes — one TAP netdev on
// the fixed descriptors, one virtio-net-pci with the allocation MAC, and no
// extra backend or legacy shortcut. Every refusal closes only the
// verification handles and preserves the healthy process, its disk and the
// allocation.
func TestNetworkRestartAdoptionIdentity(t *testing.T) {
	const id = "bx_55555555"
	for _, tc := range []struct {
		name   string
		user   bool // launch without a NIC at all (legacy user mode)
		mutate func([]string) []string
	}{
		{name: "user mode with stale allocation", user: true},
		{name: "tap plus extra user backend", mutate: func(argv []string) []string {
			return append(argv, "-netdev", "user,id=extra0")
		}},
		{name: "tap with wrong mac", mutate: func(argv []string) []string {
			for i, a := range argv {
				if strings.HasPrefix(a, "virtio-net-pci,netdev=net0,mac=") {
					argv[i] = "virtio-net-pci,netdev=net0,mac=52:54:00:aa:bb:cc"
				}
			}
			return argv
		}},
		{name: "legacy nic shortcut", mutate: func(argv []string) []string {
			return append(argv, "-nic", "user")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := networkShortDataDir(t)
			imageDir := t.TempDir()
			net := newFakeNetwork()
			alloc := net.seed(id)
			var pinned *pinnedProc
			var qmpPath string
			if tc.user {
				pinned, qmpPath, _ = spawnIdentityHelper(t, dataDir, imageDir, id, nodenet.Allocation{}, nil)
			} else {
				pinned, qmpPath, _ = spawnIdentityHelper(t, dataDir, imageDir, id, alloc, tc.mutate)
			}
			cfg := directNetworkConfig(t, dataDir, imageDir, net, pingOnlyGuest{})
			cfg.findProcess = func(m storage.VMMeta) (*pinnedProc, error) {
				if m.SandboxID == id {
					return pinned, nil
				}
				return nil, nil
			}
			cfg.verifyProc = func(ctx context.Context, _ *pinnedProc, _ storage.VMMeta) (*qmp.Client, error) {
				// The verification must succeed so recovery reaches the
				// identity gate, which is where each refusal fires.
				return qmp.Dial(ctx, qmpPath, quietLog())
			}
			_, nerr := New(t.Context(), cfg)
			if nerr == nil || !strings.Contains(nerr.Error(), "NIC") {
				t.Fatalf("New error = %v, want the NIC-identity refusal", nerr)
			}
			if serr := signal0(pinned); serr != nil {
				t.Fatalf("the healthy helper process was stopped by the refusal: %v", serr)
			}
			if got := net.setupLog(); len(got) != 0 {
				t.Fatalf("the refusal fabricated network work: %v", got)
			}
			if got := net.allocationIDs(); !slices.Equal(got, []string{id}) {
				t.Fatalf("allocation disturbed by the refusal: %v", got)
			}
			dataDirLockFree(t, dataDir, imageDir)
		})
	}

	t.Run("valid tap adopts", func(t *testing.T) {
		dataDir := networkShortDataDir(t)
		imageDir := t.TempDir()
		net := newFakeNetwork()
		alloc := net.seed(id)
		pinned, qmpPath, _ := spawnIdentityHelper(t, dataDir, imageDir, id, alloc, nil)
		cfg := directNetworkConfig(t, dataDir, imageDir, net, pingOnlyGuest{})
		cfg.findProcess = func(m storage.VMMeta) (*pinnedProc, error) {
			if m.SandboxID == id {
				return pinned, nil
			}
			return nil, nil
		}
		cfg.verifyProc = func(ctx context.Context, _ *pinnedProc, _ storage.VMMeta) (*qmp.Client, error) {
			return qmp.Dial(ctx, qmpPath, quietLog())
		}
		m, nerr := New(t.Context(), cfg)
		if nerr != nil {
			t.Fatalf("valid TAP adoption refused: %v", nerr)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			m.Shutdown(ctx)
		})
		vm, gerr := m.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
		if gerr != nil || vm.GetState() != nodev1.VMState_VM_STATE_READY {
			t.Fatalf("adopted VM = %+v, %v; want READY", vm, gerr)
		}
		if got := net.setupLog(); len(got) != 0 {
			t.Fatalf("adoption re-opened the network: %v", got)
		}
		if got := net.allocationIDs(); !slices.Equal(got, []string{id}) {
			t.Fatalf("live allocation disturbed: %v", got)
		}
	})
}

// networkShortDataDir returns a test-owned data directory directly under
// /tmp, independent of TMPDIR: the helper's QMP socket is
// <dataDir>/vms/<id>/qmp.sock and must fit sun_path (107 bytes plus NUL)
// whatever the test runner's TMPDIR or test name.
func networkShortDataDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "bbn")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove %s: %v", dir, err)
		}
	})
	return dir
}

// TestNetworkShortDataDirIgnoresLongTMPDIR: with a TMPDIR long enough to
// overflow sun_path, the identity fixture's socket still fits.
func TestNetworkShortDataDirIgnoresLongTMPDIR(t *testing.T) {
	long := filepath.Join(t.TempDir(), strings.Repeat("a", 60), strings.Repeat("b", 60))
	if err := os.MkdirAll(long, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", long)
	control, err := os.MkdirTemp("", "ctl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(control) })
	if len(filepath.Join(control, "vms", "bx_55555555", qemu.QMPFilename)) <= 107 {
		t.Fatal("the long TMPDIR control does not overflow sun_path")
	}
	dir := networkShortDataDir(t)
	if !strings.HasPrefix(dir, "/tmp/bbn") {
		t.Fatalf("data dir %s follows TMPDIR", dir)
	}
	if n := len(filepath.Join(dir, "vms", "bx_55555555", qemu.QMPFilename)); n > 107 {
		t.Fatalf("fixture QMP socket path is %d bytes", n)
	}
}

// signal0 proves the pinned helper's process is still running without going
// through the pidfd (the refusal closed it).
func signal0(pinned *pinnedProc) error {
	return syscall.Kill(pinned.pid, syscall.Signal(0))
}

// TestTapArgvIdentityBareExtraNIC is a pure argv regression for the identity
// rule's counting edge: a bare "-device virtio-net-pci" (no options) is a
// second NIC and must break the exactly-one rule even though it carries no
// comma-separated options; the expected TAP NIC plus such a bare NIC must
// not pass as exactly one.
func TestTapArgvIdentityBareExtraNIC(t *testing.T) {
	alloc := nodenet.Allocation{MAC: "52:54:00:00:00:03"}
	base := []string{
		"qemu-system-x86_64",
		"-netdev", tapNetdevArg(),
		"-device", "virtio-net-pci,netdev=net0,mac=" + alloc.MAC,
		"-device", "virtio-blk-pci,drive=rootdisk",
	}
	if err := tapArgvIdentity(base, alloc); err != nil {
		t.Fatalf("valid TAP argv rejected: %v", err)
	}
	withBare := append(slices.Clone(base), "-device", "virtio-net-pci")
	err := tapArgvIdentity(withBare, alloc)
	if err == nil || !strings.Contains(err.Error(), "virtio-net-pci") {
		t.Fatalf("bare extra NIC accepted: %v", err)
	}
	twoBare := []string{"qemu-system-x86_64", "-netdev", tapNetdevArg(), "-device", "virtio-net-pci", "-device", "virtio-net-pci"}
	if err := tapArgvIdentity(twoBare, alloc); err == nil || !strings.Contains(err.Error(), "virtio-net-pci") {
		t.Fatalf("two bare NICs accepted: %v", err)
	}
}

// TestNetworkRestoreFailureFailsStartup: a failed rule restore keeps every
// durable allocation (potential live resources) and refuses to start.
func TestNetworkRestoreFailureFailsStartup(t *testing.T) {
	dataDir := t.TempDir()
	imageDir := t.TempDir()
	net := newFakeNetwork()
	net.seed("bx_55555555")
	net.reconcileErr = errors.New("nft apply failed")
	cfg := directNetworkConfig(t, dataDir, imageDir, net, pingOnlyGuest{})
	if _, err := New(t.Context(), cfg); err == nil || !strings.Contains(err.Error(), "restore network rules") {
		t.Fatalf("New error = %v, want the restore failure", err)
	}
	if got := net.allocationIDs(); !slices.Equal(got, []string{"bx_55555555"}) {
		t.Fatalf("restore failure released allocations: %v", got)
	}
	dataDirLockFree(t, dataDir, imageDir)
}

// TestNetworkPruneFailureFailsStartup: a failed prune after VM recovery also
// refuses startup; every allocation and recovered record stays for the retry.
func TestNetworkPruneFailureFailsStartup(t *testing.T) {
	const id = "bx_55555555"
	dataDir := t.TempDir()
	imageDir := t.TempDir()
	net := newFakeNetwork()
	net.seed(id)
	net.reconcileErr = errors.New("nft apply failed")
	net.reconcileErrFrom = 2 // only the prune call fails
	cfg := directNetworkConfig(t, dataDir, imageDir, net, pingOnlyGuest{})
	cfg.findProcess = func(storage.VMMeta) (*pinnedProc, error) { return nil, nil }
	prepareVM(t, dataDir, imageDir, id, 7, 0)
	if _, err := New(t.Context(), cfg); err == nil || !strings.Contains(err.Error(), "prune network allocations") {
		t.Fatalf("New error = %v, want the prune failure", err)
	}
	if got := net.allocationIDs(); !slices.Equal(got, []string{id}) {
		t.Fatalf("prune failure released allocations: %v", got)
	}
	dataDirLockFree(t, dataDir, imageDir)
}

// TestNetworkShutdownKeepsLiveNetworking: shutdown preserves running VMs and
// their networking; live allocations are never released by Shutdown itself.
func TestNetworkShutdownKeepsLiveNetworking(t *testing.T) {
	net := newFakeNetwork()
	h := newHarness(t, harnessOptions{network: net})
	h.create("bx_33333333", nil)
	h.create("bx_44444444", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.mg.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	awaitCond(t, 2*time.Second, func() bool {
		return slices.Equal(net.allocationIDs(), []string{"bx_33333333", "bx_44444444"})
	}, "shutdown released a live allocation")
	for _, id := range []string{"bx_33333333", "bx_44444444"} {
		if net.released(id) {
			t.Fatalf("shutdown released %s's network", id)
		}
	}
}

// TestLauncherNilInterfaceOnProvenFailure pins the adapter's typed-nil rule:
// a proven-absent launch failure returns a nil VMProcess interface, never a
// nil *qemu.Process wrapped in the interface.
func TestLauncherNilInterfaceOnProvenFailure(t *testing.T) {
	var l qemuLauncher
	proc, err := l.Start(t.Context(), qemu.Config{Binary: "/nonexistent/qemu"}, qemu.StartOptions{})
	if err == nil {
		t.Fatal("missing binary accepted")
	}
	if proc != nil {
		t.Fatalf("proven-absent failure returned %+v, want a nil interface", proc)
	}
}
