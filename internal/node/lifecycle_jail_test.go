package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// A10 lifecycle wiring of the jailed launcher: every settlement path
// (watch, StopVM, a failed boot, DeleteVM, shutdown) releases the jail
// reservation after the QEMU's proven death and before the network,
// accounting or durable record change, and retains everything when the
// release fails.

// jailedHarness is a manager in production jailed mode over fakes: the fake
// jailer runs real sleep children, the fake network leases pipes, and the
// fake guest dialer serves each launched VM's CID.
type jailedHarness struct {
	*harness
	jail *fakeJail
	net  *fakeNetwork

	mu     sync.Mutex
	events []string
}

func (j *jailedHarness) event(e string) {
	j.mu.Lock()
	j.events = append(j.events, e)
	j.mu.Unlock()
}

// jailCID extracts the guest CID from a jailed argv.
func jailCID(args []string) uint32 {
	for _, a := range args {
		if rest, ok := strings.CutPrefix(a, "vhost-vsock-pci,guest-cid="); ok {
			v, _, _ := strings.Cut(rest, ",")
			n, _ := strconv.ParseUint(v, 10, 32)
			return uint32(n)
		}
	}
	return 0
}

type jailedOptions struct {
	prepare   func(t *testing.T, dataDir, imageDir string)
	fallback  guestv1.GuestServer
	stubGuest bool
	boot      time.Duration
	jail      *fakeJail
	net       *fakeNetwork
	configure func(*Config)
}

func newJailedHarness(t *testing.T, opts jailedOptions) *jailedHarness {
	t.Helper()
	j := &jailedHarness{jail: opts.jail, net: opts.net}
	if j.jail == nil {
		j.jail = newFakeJail(t)
	}
	if j.net == nil {
		j.net = newFakeNetwork()
	}
	launcher := &fakeLauncher{}
	host := qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 8}
	j.harness = newHarness(t, harnessOptions{
		launcher:    launcher,
		network:     j.net,
		fallback:    opts.fallback,
		stubGuest:   opts.stubGuest,
		bootTimeout: opts.boot,
		prepare: func(t *testing.T, dataDir, imageDir string) {
			jailWriteImage(t, imageDir, testImageID)
			if opts.prepare != nil {
				opts.prepare(t, dataDir, imageDir)
			}
		},
		configure: func(cfg *Config) {
			cfg.Launcher = nil
			cfg.Imager = jailImager{}
			cfg.jail = j.jail
			cfg.jailDial = func(context.Context, string, int, <-chan struct{}) (*qmp.Client, error) { return nil, nil }
			cfg.jailVsock = func() (*os.File, error) { return os.OpenFile("/dev/full", os.O_RDWR, 0) }
			cfg.jailCleanup = 2 * time.Second
			cfg.jailStopGrace = 50 * time.Millisecond
			cfg.JailBinary = "/usr/bin/qemu-system-x86_64"
			cfg.JailIO = jailer.IOLimits{Device: jailer.BlockDevice{Major: 8}, ReadBPS: 1 << 20}
			cfg.HostCPU = &host
			cfg.Nested = qemu.NestedOff
			if opts.configure != nil {
				opts.configure(cfg)
			}
		},
	})
	// Each jailed child stands in for a booted QEMU: its guest answers on
	// the CID in its argv, through the harness's fake guest registration.
	previous := j.jail.onStart
	j.jail.onStart = func(spec jailer.LaunchSpec) {
		j.event("start " + spec.Attempt)
		launcher.onStart(qemu.Config{WorkDir: j.mg.store.VMDir(spec.SandboxID), VsockCID: jailCID(spec.Args)})
		if previous != nil {
			previous(spec)
		}
	}
	j.jail.onRelease = func(ref jailer.Ref) {
		j.event("jail-release " + ref.SandboxID)
	}
	return j
}

func (j *jailedHarness) netReleases() int {
	j.net.mu.Lock()
	defer j.net.mu.Unlock()
	return len(j.net.releases)
}

func (j *jailedHarness) netAllocated(id string) bool {
	j.net.mu.Lock()
	defer j.net.mu.Unlock()
	_, ok := j.net.allocs[id]
	return ok
}

func (j *jailedHarness) vmOf(t *testing.T, id string) *vm {
	t.Helper()
	j.mg.mu.Lock()
	defer j.mg.mu.Unlock()
	v := j.mg.vms[id]
	if v == nil {
		t.Fatalf("no vm %s", id)
	}
	return v
}

func (j *jailedHarness) vmRef(id string) jailer.Ref {
	j.mg.mu.Lock()
	v := j.mg.vms[id]
	j.mg.mu.Unlock()
	if v == nil {
		return jailer.Ref{}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.jail
}

func (j *jailedHarness) state(t *testing.T, id string) *nodev1.VM {
	t.Helper()
	vm, err := j.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatal(err)
	}
	return vm
}

func (j *jailedHarness) reserved() resources {
	j.mg.mu.Lock()
	defer j.mg.mu.Unlock()
	return j.mg.reserved
}

// gateFree reports, without blocking, whether v's operation gate is free.
func gateFree(v *vm) bool {
	v.mu.Lock()
	if v.snapshotGate == nil {
		v.snapshotGate = make(chan struct{}, 1)
		v.snapshotGate <- struct{}{}
	}
	gate := v.snapshotGate
	v.mu.Unlock()
	select {
	case <-gate:
		gate <- struct{}{}
		return true
	default:
		return false
	}
}

// joinGate waits, bounded, until v's operation gate is free: a watcher or
// stop holds it through its whole settlement, so this is the settled
// acknowledgment, not a state poll.
func joinGate(t *testing.T, v *vm) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if err := acquireVMOperation(ctx, v); err != nil {
		t.Fatalf("operation gate of %s never freed: %v", v.id, err)
	}
	releaseVMOperation(v)
}

// releaseEntry installs a release-entry witness that reports each attempt on
// the returned channel and blocks until gate is closed (nil: no block).
func (j *jailedHarness) releaseEntry(gate chan struct{}) <-chan jailer.Ref {
	entered := make(chan jailer.Ref, 16)
	j.jail.mu.Lock()
	j.jail.onRelease = func(ref jailer.Ref) {
		entered <- ref
		if gate != nil {
			<-gate
		}
	}
	j.jail.mu.Unlock()
	return entered
}

func waitEntry(t *testing.T, entered <-chan jailer.Ref) jailer.Ref {
	t.Helper()
	select {
	case ref := <-entered:
		return ref
	case <-time.After(15 * time.Second):
		t.Fatal("no jail release was attempted")
		return jailer.Ref{}
	}
}

// killCurrent kills the VM's current jailed child and joins its reap.
func (j *jailedHarness) killCurrent(t *testing.T) *fakeJailRun {
	t.Helper()
	run := j.jail.lastRun()
	_ = run.cmd.Process.Kill()
	run.joinReaped(t)
	return run
}

func TestJailedCreateRecordsAttemptThroughCommitRunning(t *testing.T) {
	j := newJailedHarness(t, jailedOptions{})
	id := ids.Sandbox()
	if vm := j.create(id, nil); vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("CreateVM state %s", vm.GetState())
	}
	if len(j.jail.specs) != 1 || j.launcher.count() != 0 {
		t.Fatalf("jailed starts %d, unjailed starts %d", len(j.jail.specs), j.launcher.count())
	}
	spec := j.jail.specs[0]
	meta, err := j.mg.store.ReadVM(id)
	if err != nil {
		t.Fatal(err)
	}
	q := meta.QEMU
	if q == nil || q.State != storage.QEMURunning || q.JailAttempt != spec.Attempt ||
		q.ArgvSHA256 != argvDigest(append([]string{"/usr/bin/qemu-system-x86_64"}, spec.Args...)) || q.PID != j.jail.lastRun().PID() {
		t.Fatalf("committed record %+v, spec attempt %s", q, spec.Attempt)
	}
	if ref := j.vmRef(id); ref != (jailer.Ref{SandboxID: id, Attempt: spec.Attempt}) {
		t.Fatalf("v.jail = %+v", ref)
	}
}

// TestJailedWatchHoldsGateThroughSettlement: an exited VM's watcher holds
// the operation gate from before its jail release until accounting settles;
// while the release is blocked the full reservation, network and manifest
// stand.
func TestJailedWatchHoldsGateThroughSettlement(t *testing.T) {
	j := newJailedHarness(t, jailedOptions{})
	id := ids.Sandbox()
	j.create(id, nil)
	v := j.vmOf(t, id)
	full := j.reserved()
	ref := j.vmRef(id)
	block := make(chan struct{})
	// Registered before anything can fail: a failed assertion never strands
	// the watcher inside the release with the operation gate held.
	unblock := sync.OnceFunc(func() { close(block) })
	t.Cleanup(unblock)
	entered := j.releaseEntry(block)
	j.killCurrent(t)
	if got := waitEntry(t, entered); got != ref {
		t.Fatalf("released %+v, want %+v", got, ref)
	}
	// Inside the blocked release: nothing is settled yet.
	if gateFree(v) {
		t.Fatal("the watcher released the operation gate before its jail release finished")
	}
	if j.reserved() != full || !j.netAllocated(id) || j.netReleases() != 0 {
		t.Fatalf("during the release: reserved %+v (full %+v), network allocated %v", j.reserved(), full, j.netAllocated(id))
	}
	if meta, _ := j.mg.store.ReadVM(id); meta.QEMU == nil {
		t.Fatal("the manifest lost its launch record during the release")
	}
	unblock()
	joinGate(t, v)
	if _, ok := j.jail.Record(id); ok || j.vmRef(id) != (jailer.Ref{}) {
		t.Fatal("the exited VM's jail was not released")
	}
	if j.netReleases() != 1 || j.reserved() != (resources{}) {
		t.Fatalf("after settlement: network releases %d, reserved %+v", j.netReleases(), j.reserved())
	}
	if j.state(t, id).GetState() != nodev1.VMState_VM_STATE_FAILED {
		t.Fatal("exited VM is not FAILED")
	}
}

func TestJailedWatchReleaseFailureRetainsEverything(t *testing.T) {
	j := newJailedHarness(t, jailedOptions{})
	id := ids.Sandbox()
	j.create(id, nil)
	v := j.vmOf(t, id)
	full := j.reserved()
	j.jail.mu.Lock()
	j.jail.releaseErr = errors.New("injected jail release failure")
	j.jail.mu.Unlock()
	entered := j.releaseEntry(nil)
	j.killCurrent(t)
	waitEntry(t, entered)
	joinGate(t, v)
	vm := j.state(t, id)
	if vm.GetState() != nodev1.VMState_VM_STATE_FAILED || !strings.Contains(vm.GetError(), "jail") {
		t.Fatalf("state %s error %q", vm.GetState(), vm.GetError())
	}
	if j.netReleases() != 0 || !j.netAllocated(id) || j.reserved() != full {
		t.Fatalf("a failed jail release freed network %d / reservation %+v (was %+v)", j.netReleases(), j.reserved(), full)
	}
	if _, ok := j.jail.Record(id); !ok || j.vmRef(id) == (jailer.Ref{}) {
		t.Fatal("the retained reservation was dropped")
	}
	if meta, _ := j.mg.store.ReadVM(id); !strings.Contains(meta.LastError, "jail") || meta.QEMU == nil {
		t.Fatalf("persisted error %q, qemu %+v", meta.LastError, meta.QEMU)
	}
	// StopVM retries the settlement by identity.
	j.jail.mu.Lock()
	j.jail.releaseErr = nil
	j.jail.mu.Unlock()
	if _, err := j.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatal(err)
	}
	if _, ok := j.jail.Record(id); ok || j.netReleases() != 1 {
		t.Fatal("retry did not settle")
	}
}

// TestJailedStaleWatcherNeverTouchesNewerGeneration: a watcher of a dead
// generation that gets the gate only after StopVM and StartVM installed a
// newer generation retires without releasing the newer attempt or moving
// accounting, also when the newer generation itself has failed with a
// retained reservation.
func TestJailedStaleWatcherNeverTouchesNewerGeneration(t *testing.T) {
	for _, newerFailed := range []bool{false, true} {
		t.Run(fmt.Sprintf("newer generation failed %v", newerFailed), func(t *testing.T) {
			j := newJailedHarness(t, jailedOptions{})
			id := ids.Sandbox()
			j.create(id, nil)
			v := j.vmOf(t, id)
			v.mu.Lock()
			stale := v.process
			v.mu.Unlock()
			hold, held := make(chan struct{}), make(chan struct{}, 1)
			// Registered before any kill or Fatal: the held stale watcher is
			// always let go before Shutdown.
			release := sync.OnceFunc(func() { close(hold) })
			t.Cleanup(release)
			retired := make(chan struct{}, 1)
			j.mg.beforeWatchGate = func(_ *vm, proc VMProcess) {
				if proc == stale {
					held <- struct{}{}
					<-hold
				}
			}
			j.mg.watchRetired = func(_ *vm, proc VMProcess) {
				if proc == stale {
					retired <- struct{}{}
				}
			}
			j.killCurrent(t)
			select {
			case <-held:
			case <-time.After(15 * time.Second):
				t.Fatal("the stale watcher never reached its gate")
			}
			// The stop settles the dead generation; the start installs a new one.
			if _, err := j.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
				t.Fatal(err)
			}
			if _, err := j.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id}); err != nil {
				t.Fatal(err)
			}
			newer := j.vmRef(id)
			if newer == (jailer.Ref{}) || newer.Attempt == j.jail.specs[0].Attempt {
				t.Fatalf("newer generation ref %+v", newer)
			}
			if newerFailed {
				j.jail.mu.Lock()
				j.jail.releaseErr = errors.New("injected jail release failure")
				j.jail.mu.Unlock()
				entered := j.releaseEntry(nil)
				j.killCurrent(t)
				waitEntry(t, entered)
				joinGate(t, v)
			}
			reserved, attempts, netReleases := j.reserved(), j.jail.attempts, j.netReleases()
			release()
			select {
			case <-retired:
			case <-time.After(15 * time.Second):
				t.Fatal("the stale watcher did not retire")
			}
			joinGate(t, v)
			if rec, ok := j.jail.Record(id); !ok || rec.Attempt != newer.Attempt || j.vmRef(id) != newer {
				t.Fatal("the stale watcher released or replaced the newer attempt")
			}
			if j.reserved() != reserved || j.jail.attempts != attempts || j.netReleases() != netReleases {
				t.Fatalf("the stale watcher moved accounting %+v→%+v, jail attempts %d→%d, network %d→%d",
					reserved, j.reserved(), attempts, j.jail.attempts, netReleases, j.netReleases())
			}
		})
	}
}

func TestJailedStopSettlesBeforeNetworkAndStoppedRecord(t *testing.T) {
	j := newJailedHarness(t, jailedOptions{})
	id := ids.Sandbox()
	j.create(id, nil)
	full := j.reserved()
	var inside error
	j.jail.mu.Lock()
	j.jail.onRelease = func(jailer.Ref) {
		meta, err := j.mg.store.ReadVM(id)
		switch {
		case err != nil || meta.QEMU == nil || meta.Stopped():
			inside = errors.New("the stopped record was written before the jail release")
		case j.netReleases() != 0 || !j.netAllocated(id):
			inside = errors.New("network released before the jail")
		case j.reserved() != full:
			inside = fmt.Errorf("reservation %+v moved before the jail release (full %+v)", j.reserved(), full)
		}
	}
	j.jail.mu.Unlock()
	if _, err := j.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatal(err)
	}
	if inside != nil {
		t.Fatal(inside)
	}
	if vm := j.state(t, id); vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state %s", vm.GetState())
	}
	if _, ok := j.jail.Record(id); ok || j.netReleases() != 1 {
		t.Fatal("stop did not settle the jail and the network")
	}
	if waits, _, _ := j.jail.lastRun().counts(); waits != 1 {
		t.Fatalf("Wait called %d times", waits)
	}
}

func TestJailedStopReleaseFailureKeepsFailedRecord(t *testing.T) {
	j := newJailedHarness(t, jailedOptions{})
	id := ids.Sandbox()
	j.create(id, nil)
	before := j.reserved()
	j.jail.mu.Lock()
	j.jail.releaseErr = errors.New("injected jail release failure")
	j.jail.mu.Unlock()
	if _, err := j.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err == nil {
		t.Fatal("StopVM succeeded although the jail release failed")
	}
	vm := j.state(t, id)
	meta, _ := j.mg.store.ReadVM(id)
	if vm.GetState() != nodev1.VMState_VM_STATE_FAILED || meta.QEMU == nil || meta.Stopped() {
		t.Fatalf("state %s, durable qemu %+v stopped %v", vm.GetState(), meta.QEMU, meta.Stopped())
	}
	if j.netReleases() != 0 || j.reserved() != before {
		t.Fatal("a failed jail release freed the network or reservation")
	}
}

func TestJailedDeleteSettlesBeforeRemoval(t *testing.T) {
	j := newJailedHarness(t, jailedOptions{})
	id := ids.Sandbox()
	j.create(id, nil)
	j.jail.mu.Lock()
	j.jail.releaseErr = errors.New("injected jail release failure")
	j.jail.mu.Unlock()
	if _, err := j.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err == nil {
		t.Fatal("DeleteVM succeeded although the jail release failed")
	}
	if _, err := os.Stat(j.mg.store.VMDir(id)); err != nil {
		t.Fatalf("VM directory removed after a failed release: %v", err)
	}
	if j.netReleases() != 0 {
		t.Fatal("network released after a failed jail release")
	}
	j.jail.mu.Lock()
	j.jail.releaseErr = nil
	j.jail.mu.Unlock()
	var inside error
	j.jail.mu.Lock()
	j.jail.onRelease = func(jailer.Ref) {
		if _, err := os.Stat(j.mg.store.VMDir(id)); err != nil {
			inside = errors.New("the VM directory was removed before the jail release")
		}
	}
	j.jail.mu.Unlock()
	if _, err := j.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatal(err)
	}
	if inside != nil {
		t.Fatal(inside)
	}
	if _, ok := j.jail.Record(id); ok || j.netReleases() != 1 {
		t.Fatal("delete did not settle the jail and the network")
	}
}

func TestJailedBootFailureSettles(t *testing.T) {
	for _, releaseFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("release fails %v", releaseFails), func(t *testing.T) {
			j := newJailedHarness(t, jailedOptions{fallback: errorPingGuest{}, stubGuest: true, boot: time.Second})
			if releaseFails {
				j.jail.mu.Lock()
				j.jail.releaseErr = errors.New("injected jail release failure")
				j.jail.mu.Unlock()
			}
			id := ids.Sandbox()
			if _, err := j.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1}}); err == nil {
				t.Fatal("CreateVM succeeded with a guest that never answers")
			}
			if len(j.jail.specs) != 1 || j.launcher.count() != 0 {
				t.Fatalf("jailed starts %d, unjailed %d", len(j.jail.specs), j.launcher.count())
			}
			_, retained := j.jail.Record(id)
			if retained != releaseFails {
				t.Fatalf("record retained %v, want %v", retained, releaseFails)
			}
			if got := j.netReleases() == 0; got != releaseFails {
				t.Fatalf("network retained %v, want %v", got, releaseFails)
			}
			if releaseFails {
				if _, err := os.Stat(j.mg.store.VMDir(id)); err != nil {
					t.Fatal("the VM directory was removed while its jail is retained")
				}
				if j.reserved().vcpus == 0 {
					t.Fatal("the reservation was freed while the jail is retained")
				}
			}
		})
	}
}

func TestJailedNoSpawnRetainedBootFailure(t *testing.T) {
	for _, releaseFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("release fails %v", releaseFails), func(t *testing.T) {
			fj := newFakeJail(t)
			fj.noRunErr = errors.New("injected pre-spawn failure")
			fj.retainOnNoRun = true
			if releaseFails {
				fj.releaseErr = errors.New("injected jail release failure")
			}
			j := newJailedHarness(t, jailedOptions{jail: fj})
			id := ids.Sandbox()
			if _, err := j.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1}}); err == nil {
				t.Fatal("CreateVM succeeded")
			}
			if len(j.jail.specs) != 1 || j.launcher.count() != 0 {
				t.Fatalf("jailed starts %d, unjailed %d", len(j.jail.specs), j.launcher.count())
			}
			if _, retained := j.jail.Record(id); retained != releaseFails {
				t.Fatalf("record retained %v, want %v", retained, releaseFails)
			}
			if releaseFails {
				if _, err := os.Stat(j.mg.store.VMDir(id)); err != nil || j.netReleases() != 0 || j.reserved().vcpus == 0 {
					t.Fatal("a retained no-spawn reservation lost its VM directory, network or reservation")
				}
			}
		})
	}
}

func TestJailedShutdownDetachesOnly(t *testing.T) {
	j := newJailedHarness(t, jailedOptions{})
	id := ids.Sandbox()
	j.create(id, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := j.mg.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	run := j.jail.lastRun()
	if _, detaches, releases := run.counts(); detaches != 1 || releases != 0 {
		t.Fatalf("shutdown detaches %d releases %d", detaches, releases)
	}
	if _, ok := j.jail.Record(id); !ok || run.isReaped() || j.jail.attempts != 0 {
		t.Fatal("shutdown released the reservation or killed the VM")
	}
}

// --- Restart recovery in jailed mode ---------------------------------------
//
// The fake jailer's release here is a node ordering control: it stands in
// for the real jailer's death and owned-cgroup proof, which the jailer's own
// privileged tests establish. A fake release is never evidence of death.

// jailPrepare creates one VM through the store and applies state to it.
func jailPrepare(t *testing.T, dataDir, imageDir, id string, state func(s *storage.Store)) {
	t.Helper()
	store, err := storage.Open(storage.Config{DataDir: dataDir, ImageDir: imageDir, Imager: jailImager{}, Log: quietLog()})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	images, err := store.VerifyImages(t.Context())
	if err != nil || len(images) != 1 {
		t.Fatalf("VerifyImages = %v, %v", images, err)
	}
	meta := storage.VMMeta{SandboxID: id, Image: testImageID, Resources: storage.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 1}, VsockCID: 9, CreatedAtUnixMs: time.Now().UnixMilli()}
	if err := store.CreateVM(t.Context(), meta, images[0]); err != nil {
		t.Fatal(err)
	}
	if state != nil {
		state(store)
	}
}

func jailIntent(t *testing.T, id, attempt string) func(*storage.Store) {
	return func(s *storage.Store) {
		if err := s.WriteJailedLaunchIntent(t.Context(), id, attempt, strings.Repeat("ab", 32)); err != nil {
			t.Fatal(err)
		}
	}
}

func jailRecord(id, attempt string, pid int) jailer.Record {
	boot, _ := storage.HostBootID()
	return jailer.Record{SchemaVersion: 2, SandboxID: id, Attempt: attempt, Kind: jailer.KindVM, UID: 61000, PID: pid, BootID: boot, ControlIno: 1, LogIno: 2, TargetIno: 3}
}

// jailSetImage rewrites the VM manifest's image id (an uninstalled image).
func jailSetImage(t *testing.T, dataDir, id, image string) {
	t.Helper()
	p := filepath.Join(dataDir, "vms", id, "vm.json")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatal(err)
	}
	meta["image"] = image
	if raw, err = json.Marshal(meta); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// unresolved reports whether a recovered VM is FAILED with its ownership
// unresolved.
func (j *jailedHarness) unresolved(id string) bool {
	j.mg.mu.Lock()
	v := j.mg.vms[id]
	j.mg.mu.Unlock()
	if v == nil {
		return false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.ownershipUnresolved && v.state == nodev1.VMState_VM_STATE_FAILED
}

// TestJailedRecoveryPID0Record is a node ordering control: a claimed PID 0
// record is released only through the jailer, and while the jailer refuses
// (an owned cgroup that may still hold the helper) the VM stays unresolved
// under its ref and DeleteVM is refused.
func TestJailedRecoveryPID0Record(t *testing.T) {
	for _, alive := range []bool{false, true} {
		t.Run(fmt.Sprintf("cgroup alive %v", alive), func(t *testing.T) {
			id, attempt := ids.Sandbox(), uuid.NewV7().String()
			fj := newFakeJail(t)
			fj.records[id] = jailRecord(id, attempt, 0)
			if alive {
				fj.releaseErr = fmt.Errorf("%w: owned cgroup not empty", jailer.ErrAlive)
			}
			j := newJailedHarness(t, jailedOptions{jail: fj, prepare: func(t *testing.T, dataDir, imageDir string) {
				jailPrepare(t, dataDir, imageDir, id, jailIntent(t, id, attempt))
			}})
			_, retained := j.jail.Record(id)
			if retained != alive || j.jail.attempts != 1 {
				t.Fatalf("PID 0 record retained %v (want %v), release attempts %d", retained, alive, j.jail.attempts)
			}
			if vm := j.state(t, id); vm.GetState() != nodev1.VMState_VM_STATE_FAILED {
				t.Fatalf("state %s", vm.GetState())
			}
			if !alive {
				return
			}
			if !j.unresolved(id) || j.vmRef(id) != (jailer.Ref{SandboxID: id, Attempt: attempt}) {
				t.Fatal("a possibly live PID 0 attempt was not kept unresolved under its ref")
			}
			if j.reserved().vcpus == 0 {
				t.Fatal("an unresolved attempt's reservation is not forced")
			}
			if _, err := j.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err == nil {
				t.Fatal("DeleteVM removed a VM whose PID 0 helper may live")
			}
			if _, err := os.Stat(j.mg.store.VMDir(id)); err != nil {
				t.Fatal("a refused delete removed the VM directory")
			}
			j.jail.mu.Lock()
			j.jail.releaseErr = nil
			j.jail.mu.Unlock()
			if _, err := j.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
				t.Fatalf("DeleteVM after the group emptied: %v", err)
			}
		})
	}
}

// TestJailedRecoveryNeverRemovesWithUnclaimedRecord: a never-launched or
// STOPPED manifest, or a foreign attempt, with a retained jail record is
// never removed, restarted or released; it stays FAILED and unresolved with
// its directory unchanged.
func TestJailedRecoveryNeverRemovesWithUnclaimedRecord(t *testing.T) {
	cases := map[string]func(t *testing.T, id string) func(*storage.Store){
		"never launched": func(*testing.T, string) func(*storage.Store) { return nil },
		"stopped": func(t *testing.T, id string) func(*storage.Store) {
			return func(s *storage.Store) {
				if err := s.CommitStopped(id, time.Now().UnixMilli()); err != nil {
					t.Fatal(err)
				}
			}
		},
		"foreign attempt": func(t *testing.T, id string) func(*storage.Store) {
			return jailIntent(t, id, uuid.NewV7().String())
		},
	}
	for name, state := range cases {
		t.Run(name, func(t *testing.T) {
			id := ids.Sandbox()
			fj := newFakeJail(t)
			foreign := jailRecord(id, uuid.NewV7().String(), 0)
			fj.records[id] = foreign
			var before map[string]string
			j := newJailedHarness(t, jailedOptions{jail: fj, prepare: func(t *testing.T, dataDir, imageDir string) {
				jailPrepare(t, dataDir, imageDir, id, state(t, id))
				before = jailTree(t, filepath.Join(dataDir, "vms", id))
			}})
			// The FAILED reason is persisted in vm.json (a new inode by
			// atomic rename); every other entry, the disk included, stands.
			after := jailTree(t, j.mg.store.VMDir(id))
			for _, tree := range []map[string]string{before, after} {
				maps.DeleteFunc(tree, func(p string, _ string) bool { return filepath.Base(p) == "vm.json" })
			}
			if !maps.Equal(before, after) {
				t.Fatalf("recovery changed the VM directory although a jail record is retained\nbefore %v\nafter  %v", before, after)
			}
			if rec, ok := j.jail.Record(id); !ok || rec != foreign || j.jail.attempts != 0 {
				t.Fatal("the unclaimed record was touched")
			}
			if !j.unresolved(id) || j.vmRef(id) != (jailer.Ref{}) {
				t.Fatal("the VM is not unresolved, or claimed the foreign attempt")
			}
			if _, err := j.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); !errors.Is(err, errJailConflict) && (err == nil || !strings.Contains(err.Error(), "another attempt")) {
				t.Fatalf("DeleteVM of a VM blocked by a foreign record = %v", err)
			}
		})
	}
}

func TestJailedRecoveryWithoutRecord(t *testing.T) {
	t.Run("launch intent without record", func(t *testing.T) {
		id := ids.Sandbox()
		j := newJailedHarness(t, jailedOptions{prepare: func(t *testing.T, dataDir, imageDir string) {
			jailPrepare(t, dataDir, imageDir, id, jailIntent(t, id, uuid.NewV7().String()))
		}})
		if vm := j.state(t, id); vm.GetState() != nodev1.VMState_VM_STATE_FAILED || j.unresolved(id) {
			t.Fatalf("state %s unresolved %v", vm.GetState(), j.unresolved(id))
		}
	})
	t.Run("interrupted start returns to stopped", func(t *testing.T) {
		id := ids.Sandbox()
		j := newJailedHarness(t, jailedOptions{prepare: func(t *testing.T, dataDir, imageDir string) {
			jailPrepare(t, dataDir, imageDir, id, func(s *storage.Store) {
				if err := s.CommitStopped(id, time.Now().UnixMilli()); err != nil {
					t.Fatal(err)
				}
				jailIntent(t, id, uuid.NewV7().String())(s)
			})
		}})
		if vm := j.state(t, id); vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
			t.Fatalf("state %s", vm.GetState())
		}
	})
}

func TestJailedRecoveryUninstalledImageWithRecord(t *testing.T) {
	id, attempt := ids.Sandbox(), uuid.NewV7().String()
	fj := newFakeJail(t)
	fj.records[id] = jailRecord(id, attempt, 0)
	j := newJailedHarness(t, jailedOptions{jail: fj, prepare: func(t *testing.T, dataDir, imageDir string) {
		jailPrepare(t, dataDir, imageDir, id, jailIntent(t, id, attempt))
		jailSetImage(t, dataDir, id, "gone-v1")
	}})
	if _, ok := j.jail.Record(id); ok || j.jail.attempts != 1 {
		t.Fatal("the dead attempt of an uninstalled-image VM was not released first")
	}
	if vm := j.state(t, id); vm.GetState() != nodev1.VMState_VM_STATE_FAILED || !strings.Contains(vm.GetError(), "not installed") {
		t.Fatalf("state %s error %q", vm.GetState(), vm.GetError())
	}
}

// jailLiveHelper starts a live process whose /proc cmdline is exactly argv
// (the argv helper), with a sole bounded reaper.
func jailLiveHelper(t *testing.T, argv []string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), jailArgvHelperEnv+"=1")
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		readyW.Close()
		t.Fatal(err)
	}
	cmd.ExtraFiles = []*os.File{readyW}
	startErr := cmd.Start()
	readyW.Close()
	if startErr != nil {
		stdin.Close()
		t.Fatal(startErr)
	}
	reaped := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(reaped)
	}()
	t.Cleanup(func() {
		stdin.Close()
		_ = cmd.Process.Kill()
		select {
		case <-reaped:
		case <-time.After(10 * time.Second):
			t.Errorf("live helper %d was not reaped", cmd.Process.Pid)
		}
	})
	if err := readyR.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line := make([]byte, 6)
	if _, err := io.ReadFull(readyR, line); err != nil || string(line) != "ready\n" {
		t.Fatalf("live helper ready = %q, %v", line, err)
	}
	return cmd
}

// jailLiveFixture prepares a running jailed VM whose live process is a
// helper with the exact jailed argv for its durable network allocation.
type jailLiveFixture struct {
	id, attempt string
	helper      *exec.Cmd
	net         *fakeNetwork
	jail        *fakeJail
	qmp         *fakeQMPServer
	argvDigest  string
	// pins selects the durable caller-resource layout the prepared VM has:
	// "real" pins it through PinLaunch and records the attempt with those
	// inodes; "none" leaves the manifest without pins; "mismatch",
	// "replaced-console" and "symlink-disk" break one identity after pinning.
	pins string
	// before is the VM directory tree (metadata and bytes) after prepare.
	before map[string]string
}

func newJailLiveFixture(t *testing.T, mutateDigest bool) *jailLiveFixture {
	t.Helper()
	f := &jailLiveFixture{id: ids.Sandbox(), attempt: uuid.NewV7().String(), net: newFakeNetwork(), jail: newFakeJail(t), pins: "real"}
	alloc := f.net.seed(f.id)
	host := qemu.HostCPU{Vendor: "GenuineIntel"}
	cfg := qemu.Config{
		SandboxID: f.id, WorkDir: "/w", ImageDir: "/i", DiskPath: "/d", VCPUs: 2, MemoryMiB: 2048, VsockCID: 9,
		Nested: qemu.NestedOff, HostCPU: &host,
		Network: &qemu.Network{GuestIP: alloc.GuestIP, PrefixLen: alloc.Prefix.Bits(), Gateway: alloc.Gateway, MAC: alloc.MAC, Tap: os.Stdin, Vhost: os.Stdout},
	}
	args, err := cfg.JailedArgs()
	if err != nil {
		t.Fatal(err)
	}
	argv := append([]string{os.Args[0]}, args...)
	f.helper = jailLiveHelper(t, argv)
	f.argvDigest = argvDigest(argv)
	if mutateDigest {
		f.argvDigest = strings.Repeat("cd", 32)
	}
	f.qmp = startFakeQMP(t, f.id)
	return f
}

// record builds the attempt's jail record for the live helper with the
// given caller-resource identities.
func (f *jailLiveFixture) record(t *testing.T, control, console, disk storage.Identity) jailer.Record {
	t.Helper()
	rec := jailRecord(f.id, f.attempt, f.helper.Process.Pid)
	rec.StartTicks = mustProcStartTicks(t, f.helper.Process.Pid)
	rec.ControlDev, rec.ControlIno = control.Dev, control.Ino
	rec.LogDev, rec.LogIno = console.Dev, console.Ino
	rec.TargetDev, rec.TargetIno = disk.Dev, disk.Ino
	return rec
}

func jailIdentityOf(t *testing.T, f *os.File) storage.Identity {
	t.Helper()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	st := info.Sys().(*syscall.Stat_t)
	return storage.Identity{Dev: uint64(st.Dev), Ino: st.Ino}
}

func (f *jailLiveFixture) harness(t *testing.T) *jailedHarness {
	t.Helper()
	return newJailedHarness(t, jailedOptions{
		jail: f.jail, net: f.net, fallback: pingOnlyGuest{}, stubGuest: true,
		prepare: func(t *testing.T, dataDir, imageDir string) {
			jailPrepare(t, dataDir, imageDir, f.id, func(s *storage.Store) { f.prepareVM(t, s) })
		},
		configure: func(cfg *Config) {
			cfg.JailBinary = os.Args[0]
			cfg.verifyProc = func(ctx context.Context, _ *pinnedProc, _ storage.VMMeta) (*qmp.Client, error) {
				return qmp.Dial(ctx, f.qmp.path, quietLog())
			}
		},
	})
}

// prepareVM gives the prepared VM its launch record, pins (per f.pins) and
// the attempt's jail record, and captures the VM directory.
func (f *jailLiveFixture) prepareVM(t *testing.T, s *storage.Store) {
	rec := f.record(t, storage.Identity{Dev: 1, Ino: 1}, storage.Identity{Dev: 1, Ino: 2}, storage.Identity{Dev: 1, Ino: 3})
	if f.pins != "none" {
		images, err := s.VerifyImages(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		pins, err := s.PinLaunch(t.Context(), f.id, images[0])
		if err != nil {
			t.Fatal(err)
		}
		rec = f.record(t, jailIdentityOf(t, pins.Control), jailIdentityOf(t, pins.Console), jailIdentityOf(t, pins.Overlay))
		pins.Close()
	}
	if err := s.WriteJailedLaunchIntent(t.Context(), f.id, f.attempt, f.argvDigest); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitRunning(f.id, f.helper.Process.Pid); err != nil {
		t.Fatal(err)
	}
	dir := s.VMDir(f.id)
	switch f.pins {
	case "mismatch":
		rec.ControlIno++
	case "replaced-console":
		console := filepath.Join(dir, storage.ConsoleLogName)
		if err := os.Rename(console, console+".old"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(console, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	case "symlink-disk":
		disk := s.DiskPath(f.id)
		if err := os.Rename(disk, disk+".old"); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(disk+".old", disk); err != nil {
			t.Fatal(err)
		}
	}
	f.jail.records[f.id] = rec
	f.before = jailTreeWithContent(t, dir, jailTree(t, dir))
}

func jailAlive(cmd *exec.Cmd) bool { return syscall.Kill(cmd.Process.Pid, 0) == nil }

func TestJailedRecoveryAdoptsVerifiedLiveProcess(t *testing.T) {
	f := newJailLiveFixture(t, false)
	j := f.harness(t)
	if vm := j.state(t, f.id); vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("adopted state %s: %s", vm.GetState(), vm.GetError())
	}
	if j.vmRef(f.id) != (jailer.Ref{SandboxID: f.id, Attempt: f.attempt}) || j.jail.attempts != 0 || !jailAlive(f.helper) {
		t.Fatal("the adopted VM does not own its attempt, or recovery released/killed it")
	}
}

// TestJailedRecoveryRefusesUnverifiedLiveProcess: a live claimed process
// that fails any identity proof is never adopted, killed or released.
func TestJailedRecoveryRefusesUnverifiedLiveProcess(t *testing.T) {
	for name, setup := range map[string]func(f *jailLiveFixture){
		"jailer verification": func(f *jailLiveFixture) { f.jail.verifyErr = errors.New("injected confinement mismatch") },
		"argv digest":         func(*jailLiveFixture) {},
	} {
		t.Run(name, func(t *testing.T) {
			f := newJailLiveFixture(t, name == "argv digest")
			setup(f)
			j := f.harness(t)
			if !j.unresolved(f.id) || j.vmRef(f.id) != (jailer.Ref{SandboxID: f.id, Attempt: f.attempt}) {
				t.Fatalf("state %s unresolved %v", j.state(t, f.id).GetState(), j.unresolved(f.id))
			}
			if !jailAlive(f.helper) || j.jail.attempts != 0 {
				t.Fatal("an unverified live process was killed or its reservation released")
			}
		})
	}
}

func TestJailedRecoveryRefusesUnsafeStartup(t *testing.T) {
	t.Run("orphan record", func(t *testing.T) {
		fj := newFakeJail(t)
		orphan := ids.Sandbox()
		rec := jailRecord(orphan, uuid.NewV7().String(), 0)
		fj.records[orphan] = rec
		jailNewFails(t, fj, "", nil, "retained jail reservation")
		if got, ok := fj.Record(orphan); !ok || got != rec {
			t.Fatal("the orphan record was touched")
		}
	})
	t.Run("corrupt manifest with a record", func(t *testing.T) {
		id := ids.Sandbox()
		fj := newFakeJail(t)
		rec := jailRecord(id, uuid.NewV7().String(), 0)
		fj.records[id] = rec
		jailNewFails(t, fj, id, func(t *testing.T, dataDir, imageDir string) {
			jailPrepare(t, dataDir, imageDir, id, nil)
			if err := os.WriteFile(filepath.Join(dataDir, "vms", id, "vm.json"), []byte("{corrupt"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, "retained jail reservation")
		if got, ok := fj.Record(id); !ok || got != rec {
			t.Fatal("the record of the corrupt VM was touched")
		}
	})
	t.Run("legacy unjailed live QEMU", func(t *testing.T) {
		id := ids.Sandbox()
		pinned, cmd := pinnedHelper(t)
		jailNewFails(t, newFakeJail(t), id, func(t *testing.T, dataDir, imageDir string) {
			jailPrepare(t, dataDir, imageDir, id, func(s *storage.Store) {
				if err := s.WriteLaunchIntent(id); err != nil {
					t.Fatal(err)
				}
			})
		}, "legacy", func(cfg *Config) {
			cfg.findProcess = func(meta storage.VMMeta) (*pinnedProc, error) {
				if meta.SandboxID == id {
					p, err := pinProc(pinned.pid)
					return p, err
				}
				return nil, nil
			}
		})
		if !jailAlive(cmd) {
			t.Fatal("the legacy process was killed")
		}
	})
	t.Run("uninstalled image with a live verified process", func(t *testing.T) {
		f := newJailLiveFixture(t, false)
		jailNewFails(t, f.jail, f.id, func(t *testing.T, dataDir, imageDir string) {
			jailPrepare(t, dataDir, imageDir, f.id, func(s *storage.Store) { f.prepareVM(t, s) })
			jailSetImage(t, dataDir, f.id, "gone-v1")
		}, "uninstalled image", func(cfg *Config) {
			cfg.Network = f.net
			cfg.JailBinary = os.Args[0]
			cfg.verifyProc = func(ctx context.Context, _ *pinnedProc, _ storage.VMMeta) (*qmp.Client, error) {
				return qmp.Dial(ctx, f.qmp.path, quietLog())
			}
		})
		if !jailAlive(f.helper) {
			t.Fatal("the live process was killed")
		}
	})
}

// jailNewFails builds a jailed manager config and requires New to fail
// naming reason, with zero jail releases, the named VM directory unchanged
// and the data directory lock freed.
func jailNewFails(t *testing.T, fj *fakeJail, id string, prepare func(t *testing.T, dataDir, imageDir string), reason string, configure ...func(*Config)) {
	t.Helper()
	dataDir, imageDir := t.TempDir(), t.TempDir()
	jailWriteImage(t, imageDir, testImageID)
	if prepare != nil {
		prepare(t, dataDir, imageDir)
	}
	var before map[string]string
	if id != "" {
		before = jailTree(t, filepath.Join(dataDir, "vms", id))
		before = jailTreeWithContent(t, filepath.Join(dataDir, "vms", id), before)
	}
	host := qemu.HostCPU{Vendor: "GenuineIntel"}
	cfg := Config{
		NodeID: "node-test", DataDir: dataDir, ImageDir: imageDir, DefaultImage: testImageID, QemuVersion: "test",
		Capacity: &nodev1.Resources{Vcpus: 8, MemoryMib: 8192, DiskGib: 100}, HostVCPUs: 8, MinCID: 3, MaxCID: 64,
		localCID: func(context.Context) (uint32, error) { return 2, nil },
		Network:  newFakeNetwork(), Imager: jailImager{}, GuestDialer: newFakeGuestDialer(t),
		jail: fj, JailBinary: "/usr/bin/qemu-system-x86_64", HostCPU: &host, Nested: qemu.NestedOff,
		JailIO:   jailer.IOLimits{Device: jailer.BlockDevice{Major: 8}, ReadBPS: 1},
		diskFree: func() (uint64, error) { return 1 << 40, nil },
	}
	for _, c := range configure {
		c(&cfg)
	}
	m, err := New(t.Context(), cfg)
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		m.Shutdown(ctx)
		t.Fatalf("New succeeded; want a %q refusal", reason)
	}
	if !strings.Contains(err.Error(), reason) {
		t.Fatalf("New = %v, want %q", err, reason)
	}
	if fj.attempts != 0 {
		t.Fatalf("a refused startup made %d jail releases", fj.attempts)
	}
	if id != "" {
		after := jailTreeWithContent(t, filepath.Join(dataDir, "vms", id), jailTree(t, filepath.Join(dataDir, "vms", id)))
		if !maps.Equal(before, after) {
			t.Fatalf("a refused startup changed the VM directory\nbefore %v\nafter  %v", before, after)
		}
	}
	// The data-directory lock is free again.
	store, err := storage.Open(storage.Config{DataDir: dataDir, ImageDir: imageDir, Imager: jailImager{}, Log: quietLog()})
	if err != nil {
		t.Fatalf("data directory still locked after a refused startup: %v", err)
	}
	store.Close()
}

// jailTreeWithContent adds each regular file's sha256 to a jailTree map.
func jailTreeWithContent(t *testing.T, dir string, tree map[string]string) map[string]string {
	t.Helper()
	out := maps.Clone(tree)
	for p := range tree {
		info, err := os.Lstat(p)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		out[p] += " " + hex.EncodeToString(sum[:])
	}
	return out
}
