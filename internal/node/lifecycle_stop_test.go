package node

import (
	"context"
	"encoding/binary"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// --- helpers -------------------------------------------------------------------

// stopStartReserved snapshots the manager's current reservation.
func stopStartReserved(t *testing.T, m *Manager) resources {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reserved
}

// stopStartManifest reads one VM's durable manifest straight from disk.
func stopStartManifest(t *testing.T, dataDir, id string) storage.VMMeta {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dataDir, "vms", id, "vm.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var meta storage.VMMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	return meta
}

// stopStartDiskMarker returns the fake disk's recorded virtual size.
func stopStartDiskMarker(t *testing.T, dataDir, id string) uint64 {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dataDir, "vms", id, "disk.qcow2"))
	if err != nil {
		t.Fatalf("read disk marker: %v", err)
	}
	if len(raw) < 8 {
		t.Fatalf("disk marker truncated: %d bytes", len(raw))
	}
	return binary.LittleEndian.Uint64(raw[:8])
}

// stopStartVMState returns the in-memory state of one VM record.
func stopStartVMState(t *testing.T, m *Manager, id string) (nodev1.VMState, *nodev1.VM) {
	t.Helper()
	v, err := m.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		return nodev1.VMState_VM_STATE_UNSPECIFIED, nil
	}
	return v.GetState(), v
}

// gatePing is a Ping gate that can be armed per boot: while armed every Ping
// blocks until release, so a create or a start can be held exactly where the
// terminal READY transition happens. Everything but Ping delegates to the
// wrapped service.
type gatePing struct {
	guestv1.GuestServer
	mu      sync.Mutex
	armed   bool
	entered chan struct{}
	release chan struct{}
}

func newGatePing() *gatePing {
	return &gatePing{entered: make(chan struct{}, 8), release: make(chan struct{})}
}

func (g *gatePing) arm() {
	g.mu.Lock()
	g.armed = true
	g.release = make(chan struct{})
	// A fresh entered channel drops signals from boots that already passed
	// through the unarmed gate, so a test that arms after a create observes
	// exactly the next boot's Ping.
	g.entered = make(chan struct{}, 8)
	g.mu.Unlock()
}

func (g *gatePing) disarm() {
	g.mu.Lock()
	if g.armed {
		g.armed = false
		close(g.release)
	}
	g.mu.Unlock()
}

func (g *gatePing) Ping(context.Context, *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	g.mu.Lock()
	release := g.release
	armed := g.armed
	g.mu.Unlock()
	select {
	case g.entered <- struct{}{}:
	default:
	}
	if armed {
		<-release
	}
	return &guestv1.PingResponse{Version: "test"}, nil
}

// flakyPing serves Ping from the real guest unless failing is set, which
// makes a whole boot fail and a later one succeed.
type flakyPing struct {
	guestv1.GuestServer
	mu      sync.Mutex
	failing bool
}

func (f *flakyPing) setFailing(v bool) {
	f.mu.Lock()
	f.failing = v
	f.mu.Unlock()
}

func (f *flakyPing) Ping(ctx context.Context, req *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	f.mu.Lock()
	failing := f.failing
	f.mu.Unlock()
	if failing {
		return nil, status.Error(codes.Unavailable, "guest not ready")
	}
	return f.GuestServer.Ping(ctx, req)
}

// stopStartReopen shuts the harness's manager down and opens a fresh one on
// the same data and image directories, with its own launcher and guest
// dialer wired like newHarness does. fallback, when set, serves guest RPCs
// for CIDs with no registered service (adopted VMs). configure, when set,
// adapts the reopened manager's Config (test seams and faults).
func stopStartReopen(t *testing.T, h *harness, qemuExe string, configure func(*Config), fallback guestv1.GuestServer) *harness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.mg.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown before reopen: %v", err)
	}
	launcher := &fakeLauncher{}
	dialer := newFakeGuestDialer(t)
	dialer.fallback = fallback
	freezer := &countingFreezer{}
	launcher.onStart = func(cfg qemu.Config) {
		home := filepath.Join(cfg.WorkDir, "guest", "home", "user")
		tmp := filepath.Join(cfg.WorkDir, "guest", "tmp")
		if err := os.MkdirAll(home, 0o755); err != nil {
			t.Errorf("create fake home: %v", err)
			return
		}
		if err := os.MkdirAll(tmp, 0o755); err != nil {
			t.Errorf("create fake tmp: %v", err)
			return
		}
		svc, err := guest.New(guest.Config{
			Version:     "test",
			UID:         os.Getuid(),
			GID:         os.Getgid(),
			NoSetGroups: true,
			HomeRoot:    home,
			TmpRoot:     tmp,
			Freezer:     freezer,
		})
		if err != nil {
			t.Errorf("guest.New: %v", err)
			return
		}
		dialer.register(cfg.VsockCID, svc)
	}
	free := uint64(1) << 40
	cfg := Config{
		NodeID:       "node-reopened",
		DataDir:      h.dataDir,
		ImageDir:     h.imageDir,
		DefaultImage: testImageID,
		QemuVersion:  "test",
		Capacity:     &nodev1.Resources{Vcpus: 64, MemoryMib: 256 << 10, DiskGib: 1024},
		HostVCPUs:    64,
		MinCID:       3,
		MaxCID:       64,
		// Unit tests must not depend on the review host's own vsock CID.
		localCID:    func(context.Context) (uint32, error) { return 2, nil },
		Launcher:    launcher,
		GuestDialer: dialer,
		Imager:      &fakeImager{diskGiB: defaultTestBaseGiB, basePath: filepath.Join(h.imageDir, testImageID, "root.qcow2")},
		// The reopened manager sees the same network the harness was built
		// with: a restart must reconcile the same durable network state. No
		// network means the explicit dev/test user-networking mode.
		Network:             h.net,
		AllowUserNetworking: h.net == nil,
		diskFree:            func() (uint64, error) { return free, nil },
		qemuExe:             qemuExe,
	}
	if configure != nil {
		configure(&cfg)
	}
	if cfg.Network != nil {
		cfg.AllowUserNetworking = false
	}
	// h2.net carries the network the reopened manager really runs with, so
	// a further reopen reconciles the same durable state.
	cfgForHarness := cfg.Network
	m, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	h2 := &harness{
		t:        t,
		mg:       m,
		launcher: launcher,
		dialer:   dialer,
		freezer:  freezer,
		dataDir:  h.dataDir,
		imageDir: h.imageDir,
		net:      cfgForHarness,
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := m.Shutdown(ctx); err != nil {
			t.Errorf("reopened Shutdown: %v", err)
		}
		launcher.killAll()
		dialer.stop(t)
	})
	return h2
}

// stopStartEditManifest rewrites one VM manifest between two manager
// generations, the way a crash mid-start would leave it.
func stopStartEditManifest(t *testing.T, h *harness, id string, edit func(*storage.VMMeta)) {
	t.Helper()
	path := filepath.Join(h.dataDir, "vms", id, "vm.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var meta storage.VMMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	edit(&meta)
	out, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
}

// --- stop ----------------------------------------------------------------------

func TestStopVMKeepsDiskFreesComputeAndPersists(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	created := h.create(id, nil)
	if created.GetStartedAtUnixMs() == 0 {
		t.Fatalf("CreateVM reported no started_at: %v", created)
	}
	full := stopStartReserved(t, h.mg)
	if full.vcpus != 2 || full.diskGiB != defaultTestBaseGiB {
		t.Fatalf("reserved after create = %+v", full)
	}

	stopped, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	if stopped.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM state = %s", stopped.GetState())
	}
	if stopped.GetStartedAtUnixMs() != created.GetStartedAtUnixMs() {
		t.Fatalf("started_at changed across the stop: %d -> %d", created.GetStartedAtUnixMs(), stopped.GetStartedAtUnixMs())
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); err != nil {
		t.Fatalf("kept disk directory vanished: %v", err)
	}
	meta := stopStartManifest(t, h.dataDir, id)
	if !meta.Stopped() || meta.StoppedAtUnixMs <= 0 {
		t.Fatalf("manifest after stop = %+v", meta)
	}
	if meta.StartedAtUnixMs != created.GetStartedAtUnixMs() {
		t.Fatalf("manifest started_at = %d, want %d", meta.StartedAtUnixMs, created.GetStartedAtUnixMs())
	}
	res := stopStartReserved(t, h.mg)
	if res.vcpus != 0 || res.memoryMib != 0 {
		t.Fatalf("compute not freed: %+v", res)
	}
	if res.diskGiB != defaultTestBaseGiB {
		t.Fatalf("stopped disk not accounted: %+v", res)
	}

	// Idempotent: a second stop returns the same record at once.
	again, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id})
	if err != nil || again.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM again = %v, %v", again, err)
	}

	// CreateVM never boots an existing VM, stopped or not.
	seen, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 9, MemoryMib: 999, DiskGib: 9}})
	if err != nil || seen.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("CreateVM of stopped = %v, %v", seen, err)
	}
	if got := h.launcher.count(); got != 1 {
		t.Fatalf("launcher ran %d times after create-of-stopped", got)
	}
	list, err := h.mg.ListVMs(t.Context(), &nodev1.ListVMsRequest{})
	if err != nil {
		t.Fatalf("ListVMs: %v", err)
	}
	found := false
	for _, vm := range list.GetVms() {
		if vm.GetSandboxId() == id {
			found = vm.GetState() == nodev1.VMState_VM_STATE_STOPPED
		}
	}
	if !found {
		t.Fatalf("ListVMs does not report %s STOPPED: %v", id, list)
	}
	// A stopped sandbox is not usable through the ready-gated RPCs.
	if err := h.mg.Exec(&nodev1.ExecRequest{SandboxId: id, Command: "true"}, &testExecStream{ctx: t.Context()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Exec of stopped = %v, want FailedPrecondition", err)
	}
}

func TestStopVMUsesACPIThenKill(t *testing.T) {
	server := startFakeQMP(t, "guest")
	launcher := &fakeLauncher{}
	launcher.wrapProcess = func(p VMProcess) VMProcess {
		fp := p.(*fakeProcess)
		client, err := qmp.Dial(t.Context(), server.path, slog.New(slog.DiscardHandler))
		if err != nil {
			t.Errorf("qmp dial: %v", err)
			return p
		}
		server.mu.Lock()
		server.onPowerdown = func() { fp.exit(nil) }
		server.mu.Unlock()
		return &qmpFakeProcess{fakeProcess: fp, client: client}
	}
	h := newHarness(t, harnessOptions{launcher: launcher})
	id := ids.Sandbox()
	h.create(id, nil)

	done := make(chan error, 1)
	go func() {
		_, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 10})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StopVM: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StopVM did not finish; the ACPI power-off was not honored")
	}
	if got := server.powerdownCount(); got != 1 {
		t.Fatalf("system_powerdown sent %d times, want 1", got)
	}
	state, _ := stopStartVMState(t, h.mg, id)
	if state != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state after ACPI stop = %s", state)
	}
}

// qmpFakeProcess adds a QMP control connection to the fake process.
type qmpFakeProcess struct {
	*fakeProcess
	client *qmp.Client
}

func (p *qmpFakeProcess) QMP() *qmp.Client { return p.client }

// Close also closes the QMP connection so the manager's event drain ends,
// the way a real qemu.Process's reaper closes its client on exit.
func (p *qmpFakeProcess) Close() error {
	return errors.Join(p.client.Close(), p.fakeProcess.Close())
}

func TestStopVMGraceBoundsTheACPIToo(t *testing.T) {
	// A guest that never answers the ACPI request must not extend the stop
	// beyond the caller's grace: the QMP attempt is capped by the same
	// budget, not added on top of it.
	server := startFakeQMP(t, "guest")
	launcher := &fakeLauncher{}
	launcher.wrapProcess = func(p VMProcess) VMProcess {
		fp := p.(*fakeProcess)
		client, err := qmp.Dial(t.Context(), server.path, slog.New(slog.DiscardHandler))
		if err != nil {
			t.Errorf("qmp dial: %v", err)
			return p
		}
		return &qmpFakeProcess{fakeProcess: fp, client: client}
	}
	server.mu.Lock()
	server.onPowerdown = func() { time.Sleep(3 * time.Second) } // reply later than the grace
	server.mu.Unlock()
	h := newHarness(t, harnessOptions{launcher: launcher})
	id := ids.Sandbox()
	h.create(id, nil)

	begin := time.Now()
	stopped, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	if elapsed := time.Since(begin); elapsed > 2500*time.Millisecond {
		t.Fatalf("StopVM with a stuck ACPI reply took %s; the grace budget was not shared", elapsed)
	}
	if stopped.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state = %s", stopped.GetState())
	}
	if got := server.powerdownCount(); got != 1 {
		t.Fatalf("system_powerdown sent %d times, want 1", got)
	}
}

func TestStopVMFailureMarksFailedKeepsHandleAndReservation(t *testing.T) {
	launcher := &fakeLauncher{stopErr: errors.New("cannot stop")}
	h := newHarness(t, harnessOptions{launcher: launcher})
	id := ids.Sandbox()
	h.create(id, nil)
	full := stopStartReserved(t, h.mg)

	_, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if status.Code(err) != codes.Internal {
		t.Fatalf("StopVM = %v, want Internal", err)
	}
	state, vm := stopStartVMState(t, h.mg, id)
	if state != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("state after failed stop = %s (%v), want FAILED", state, vm)
	}
	// The guest connection is gone with the failed stop, so the proxy must
	// gate on the state instead of a half-alive connection.
	if err := h.mg.Exec(&nodev1.ExecRequest{SandboxId: id, Command: "true"}, &testExecStream{ctx: t.Context()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Exec after failed stop = %v, want FailedPrecondition", err)
	}
	if got := stopStartReserved(t, h.mg); got != full {
		t.Fatalf("reservation changed after failed stop: %+v, want %+v", got, full)
	}
	// The retry, with the process stoppable again, succeeds: a FAILED VM
	// stops too.
	h.launcher.lastProcess().setStopErr(nil)
	stopped, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if err != nil || stopped.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM retry = %v, %v", stopped, err)
	}
}

func TestStopVMDuringBootIsPrecondition(t *testing.T) {
	gate := newGatePing()
	gate.arm()
	defer gate.disarm()
	h := newHarness(t, harnessOptions{fallback: gate, stubGuest: true})
	id := ids.Sandbox()
	booted := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
		})
		booted <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("boot never reached Ping")
	}
	_, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StopVM during boot = %v, want FailedPrecondition", err)
	}
	gate.disarm()
	if err := <-booted; err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
}

func TestConcurrentStopVMCallersShareOneStop(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	stopGate := make(chan struct{})
	entered := make(chan struct{}, 1)
	h.launcher.lastProcess().stageStop(stopGate, entered)

	const callers = 3
	results := make(chan *nodev1.VM, callers)
	errs := make(chan error, callers)
	for range callers {
		go func() {
			vm, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
			results <- vm
			errs <- err
		}()
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the stop owner never reached the process stop")
	}
	// Give the waiters time to observe STOPPING; then let the stop finish.
	waitFor(t, "stopping state", func() bool {
		state, _ := stopStartVMState(t, h.mg, id)
		return state == nodev1.VMState_VM_STATE_STOPPING
	})
	close(stopGate)
	for range callers {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent StopVM: %v", err)
		}
		vm := <-results
		if vm.GetState() != nodev1.VMState_VM_STATE_STOPPED {
			t.Fatalf("concurrent StopVM state = %s", vm.GetState())
		}
	}
	if got := h.launcher.stopCount(); got != 1 {
		t.Fatalf("process stopped %d times, want 1", got)
	}
}

func TestStopVMPersistFailureKeepsReservation(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	full := stopStartReserved(t, h.mg)
	vmDir := filepath.Join(h.dataDir, "vms", id)
	if err := os.Chmod(vmDir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(vmDir, 0o700) //nolint:errcheck // best-effort restore

	_, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if status.Code(err) != codes.Internal {
		t.Fatalf("StopVM = %v, want Internal", err)
	}
	state, vm := stopStartVMState(t, h.mg, id)
	if state != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("state after persist failure = %s (%v)", state, vm)
	}
	if got := stopStartReserved(t, h.mg); got != full {
		t.Fatalf("reservation changed on persist failure: %+v, want %+v", got, full)
	}
	// The failure is retryable: with the directory writable again the same
	// call completes the stop.
	if err := os.Chmod(vmDir, 0o700); err != nil {
		t.Fatal(err)
	}
	stopped, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if err != nil || stopped.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM retry = %v, %v", stopped, err)
	}
	if got := stopStartReserved(t, h.mg); got.vcpus != 0 || got.diskGiB != defaultTestBaseGiB {
		t.Fatalf("reserved after retried stop = %+v", got)
	}
}

// --- start ----------------------------------------------------------------------

func TestStartVMReplacesEnvGrowsDiskAndUpdatesTimestamps(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)

	stopped, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	started, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB + 1},
		Env:       map[string]string{"P1": "after"},
		Hostname:  "renamed",
	})
	if err != nil {
		t.Fatalf("StartVM: %v", err)
	}
	if started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM state = %s", started.GetState())
	}
	if started.GetStartedAtUnixMs() < stopped.GetStartedAtUnixMs() {
		t.Fatalf("started_at went backwards: %d < %d", started.GetStartedAtUnixMs(), stopped.GetStartedAtUnixMs())
	}
	meta := stopStartManifest(t, h.dataDir, id)
	if len(meta.Env) != 1 || meta.Env["P1"] != "after" {
		t.Fatalf("env not replaced: %v", meta.Env)
	}
	if meta.Hostname != "renamed" {
		t.Fatalf("hostname not updated: %q", meta.Hostname)
	}
	if meta.Resources.DiskGiB != defaultTestBaseGiB+1 {
		t.Fatalf("manifest disk = %d GiB", meta.Resources.DiskGiB)
	}
	if marker := stopStartDiskMarker(t, h.dataDir, id); marker != (defaultTestBaseGiB+1)<<30 {
		t.Fatalf("disk not grown: marker = %d bytes", marker)
	}
	if meta.StartedAtUnixMs != started.GetStartedAtUnixMs() || meta.StoppedAtUnixMs != 0 {
		t.Fatalf("manifest timestamps = started %d stopped %d", meta.StartedAtUnixMs, meta.StoppedAtUnixMs)
	}
	// The started VM answers ready-gated RPCs again.
	if err := h.mg.Exec(&nodev1.ExecRequest{SandboxId: id, Command: "true"}, &testExecStream{ctx: t.Context()}); err != nil {
		t.Fatalf("Exec after start: %v", err)
	}
	// Idempotent: a second StartVM returns the READY record at once.
	again, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || again.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM again = %v, %v", again, err)
	}
	if got := h.launcher.count(); got != 2 {
		t.Fatalf("launcher ran %d times, want 2 (create + start)", got)
	}
}

func TestStartVMValidationAndShrink(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	cases := []struct {
		name string
		req  *nodev1.StartVMRequest
		want codes.Code
	}{
		{"shrink", &nodev1.StartVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB - 1}}, codes.InvalidArgument},
		{"zero vcpus", &nodev1.StartVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 0, MemoryMib: 2048, DiskGib: defaultTestBaseGiB}}, codes.InvalidArgument},
		{"bad hostname", &nodev1.StartVMRequest{SandboxId: id, Hostname: "not a host"}, codes.InvalidArgument},
		{"exceeds host cpus", &nodev1.StartVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 65, MemoryMib: 2048, DiskGib: defaultTestBaseGiB}}, codes.ResourceExhausted},
	}
	for _, tc := range cases {
		_, err := h.mg.StartVM(t.Context(), tc.req)
		if status.Code(err) != tc.want {
			t.Fatalf("%s: StartVM = %v, want %s", tc.name, err, tc.want)
		}
		state, vm := stopStartVMState(t, h.mg, id)
		if state != nodev1.VMState_VM_STATE_STOPPED {
			t.Fatalf("%s: state after rejection = %s (%v)", tc.name, state, vm)
		}
	}
	// Unknown ids are NotFound for both RPCs.
	for _, err := range []error{
		func() error {
			_, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: "bx_unknown0"})
			return err
		}(),
		func() error {
			_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: "bx_unknown0"})
			return err
		}(),
	} {
		if status.Code(err) != codes.NotFound {
			t.Fatalf("unknown id = %v, want NotFound", err)
		}
	}
}

func TestStartVMAdmitsComputeAndOnlyTheDiskDelta(t *testing.T) {
	// Compute reuse: a stopped VM's vCPUs and memory are free for new VMs,
	// and starting it again must re-admit them.
	h := newHarness(t, harnessOptions{capacity: &nodev1.Resources{Vcpus: 8, MemoryMib: 4096, DiskGib: 10}})
	a, b := ids.Sandbox(), ids.Sandbox()
	h.create(a, &nodev1.Resources{Vcpus: 2, MemoryMib: 1024, DiskGib: 1})
	h.create(b, &nodev1.Resources{Vcpus: 2, MemoryMib: 1024, DiskGib: 1})
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: a, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	c := ids.Sandbox()
	h.create(c, &nodev1.Resources{Vcpus: 2, MemoryMib: 1024, DiskGib: 1}) // a's freed compute
	if got := stopStartReserved(t, h.mg); got.vcpus != 4 || got.diskGiB != 3 {
		t.Fatalf("reserved = %+v, want 4 vcpus / 3 disk", got)
	}
	_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{
		SandboxId: a,
		Resources: &nodev1.Resources{Vcpus: 5, MemoryMib: 1024, DiskGib: 1}, // 4 + 5 > 8
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("StartVM without room = %v, want ResourceExhausted", err)
	}
	state, vm := stopStartVMState(t, h.mg, a)
	if state != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state after exhausted start = %s (%v)", state, vm)
	}
	// The start admits exactly the compute it needs plus the disk delta.
	started, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{
		SandboxId: a,
		Resources: &nodev1.Resources{Vcpus: 3, MemoryMib: 1024, DiskGib: 2},
	})
	if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM with room = %v, %v", started, err)
	}
	if got := stopStartReserved(t, h.mg); got.vcpus != 7 || got.diskGiB != 4 {
		t.Fatalf("reserved after growth start = %+v, want 7 vcpus / 4 disk", got)
	}

	// Disk growth beyond capacity is refused and keeps the VM stopped.
	d := ids.Sandbox()
	h.create(d, &nodev1.Resources{Vcpus: 1, MemoryMib: 1024, DiskGib: 1})
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: d, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM d: %v", err)
	}
	_, err = h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{
		SandboxId: d,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 1024, DiskGib: 8}, // 4 + 7 > 10
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("StartVM growth beyond capacity = %v, want ResourceExhausted", err)
	}
	state, _ = stopStartVMState(t, h.mg, d)
	if state != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("d state = %s", state)
	}
}

func TestStartVMWaitsForAnInProgressStop(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	stopGate := make(chan struct{})
	entered := make(chan struct{}, 1)
	h.launcher.lastProcess().stageStop(stopGate, entered)

	stopDone := make(chan *nodev1.VM, 1)
	stopErr := make(chan error, 1)
	go func() {
		vm, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
		stopDone <- vm
		stopErr <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("stop never reached the process")
	}
	var order sync.Mutex
	sequence := []string{}
	startDone := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
		order.Lock()
		sequence = append(sequence, "start")
		order.Unlock()
		startDone <- err
	}()
	waitFor(t, "stopping observed", func() bool {
		state, _ := stopStartVMState(t, h.mg, id)
		return state == nodev1.VMState_VM_STATE_STOPPING
	})
	close(stopGate)
	if err := <-stopErr; err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	(<-stopDone)
	order.Lock()
	sequence = append(sequence, "stop-returned")
	order.Unlock()
	if err := <-startDone; err != nil {
		t.Fatalf("StartVM after a stop: %v", err)
	}
	state, vm := stopStartVMState(t, h.mg, id)
	if state != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("state after start-during-stop = %s (%v)", state, vm)
	}
	// The start booted exactly once more.
	if got := h.launcher.count(); got != 2 {
		t.Fatalf("launcher ran %d times, want 2", got)
	}
}

func TestStartVMOfFailedVMRequiresStopFirst(t *testing.T) {
	flaky := &flakyPing{}
	h := newHarness(t, harnessOptions{
		wrapGuest:   func(s guestv1.GuestServer) guestv1.GuestServer { return flaky.wrap(s) },
		bootTimeout: 300 * time.Millisecond,
	})
	flaky.setFailing(true)
	id := ids.Sandbox()
	created := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
		})
		created <- err
	}()
	if err := <-created; status.Code(err) != codes.Internal {
		t.Fatalf("CreateVM = %v, want Internal", err)
	}
	state, _ := stopStartVMState(t, h.mg, id)
	if state != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("state = %s, want FAILED", state)
	}
	_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartVM of FAILED = %v, want FailedPrecondition", err)
	}
	// A FAILED VM stops too: its QEMU was never proven gone here, so the
	// stop performs the kill path and lands on STOPPED.
	stopped, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if err != nil || stopped.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM of FAILED = %v, %v", stopped, err)
	}
	flaky.setFailing(false)
	started, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM after stop = %v, %v", started, err)
	}
	if started.GetStartedAtUnixMs() == 0 {
		t.Fatalf("started_at not set: %v", started)
	}
}

// wrap adapts flakyPing to harnessOptions.wrapGuest.
func (f *flakyPing) wrap(s guestv1.GuestServer) guestv1.GuestServer {
	f.GuestServer = s
	return f
}

func TestCanceledStartReturnsToStoppedAndKeepsDisk(t *testing.T) {
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}

	gate.arm()
	ctx, cancel := context.WithCancel(t.Context())
	startErr := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(ctx, &nodev1.StartVMRequest{SandboxId: id})
		startErr <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("start never reached Ping")
	}
	cancel()
	if err := <-startErr; status.Code(err) != codes.Canceled {
		t.Fatalf("StartVM = %v, want Canceled", err)
	}
	waitFor(t, "aborted start to settle", func() bool {
		state, _ := stopStartVMState(t, h.mg, id)
		return state == nodev1.VMState_VM_STATE_STOPPED
	})
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); err != nil {
		t.Fatalf("kept disk removed by an aborted start: %v", err)
	}
	meta := stopStartManifest(t, h.dataDir, id)
	if !meta.Stopped() {
		t.Fatalf("manifest after aborted start = %+v", meta)
	}
	if got := stopStartReserved(t, h.mg); got.vcpus != 0 || got.diskGiB != defaultTestBaseGiB {
		t.Fatalf("reserved after aborted start = %+v", got)
	}
	// The stopped VM starts again on retry.
	gate.disarm()
	started, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM retry = %v, %v", started, err)
	}
}

func TestStartWaiterCancellationDoesNotAbortOwnerBoot(t *testing.T) {
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}

	gate.arm()
	ctx, cancel := context.WithCancel(t.Context())
	owner := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(ctx, &nodev1.StartVMRequest{SandboxId: id})
		owner <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("owner start never reached Ping")
	}
	waiter := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
		waiter <- err
	}()
	waitFor(t, "waiter joined", func() bool { return waiterCount(h, id) == 2 })
	cancel()
	waitFor(t, "owner waiter left", func() bool { return waiterCount(h, id) == 1 })
	gate.disarm()
	if err := <-waiter; err != nil {
		t.Fatalf("surviving waiter: %v", err)
	}
	<-owner // Canceled or Ready, but never an aborted boot
	state, vm := stopStartVMState(t, h.mg, id)
	if state != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("state after waiter cancellation = %s (%v)", state, vm)
	}
	if got := h.launcher.count(); got != 2 {
		t.Fatalf("launcher ran %d times, want 2", got)
	}
}

func TestConcurrentStartVMCallersShareOneBoot(t *testing.T) {
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	gate.arm()
	const callers = 3
	started := make(chan *nodev1.VM, callers)
	errs := make(chan error, callers)
	for range callers {
		go func() {
			vm, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id, Env: map[string]string{"K": "v"}})
			started <- vm
			errs <- err
		}()
	}
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("start never reached Ping")
	}
	waitFor(t, "all callers joined", func() bool { return waiterCount(h, id) == callers })
	gate.disarm()
	var firstStarted int64
	for range callers {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent StartVM: %v", err)
		}
		vm := <-started
		if vm.GetState() != nodev1.VMState_VM_STATE_READY {
			t.Fatalf("concurrent StartVM state = %s", vm.GetState())
		}
		if firstStarted == 0 {
			firstStarted = vm.GetStartedAtUnixMs()
		} else if vm.GetStartedAtUnixMs() != firstStarted {
			t.Fatalf("callers disagree on started_at: %d vs %d", vm.GetStartedAtUnixMs(), firstStarted)
		}
	}
	if got := h.launcher.count(); got != 2 {
		t.Fatalf("launcher ran %d times, want 2", got)
	}
}

// --- delete races ----------------------------------------------------------------

func TestDeleteDuringStartAbortsTheStart(t *testing.T) {
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	gate.arm()
	defer gate.disarm()
	startErr := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
		startErr <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("start never reached Ping")
	}
	if _, err := h.mg.DeleteVM(t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		t.Fatalf("DeleteVM during start: %v", err)
	}
	if err := <-startErr; status.Code(err) != codes.Aborted {
		t.Fatalf("StartVM during delete = %v, want Aborted", err)
	}
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetVM after delete-during-start = %v, want NotFound", err)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disk directory survived the delete: %v", err)
	}
	if got := stopStartReserved(t, h.mg); got != (resources{}) {
		t.Fatalf("reservation leaked: %+v", got)
	}
}

func TestDeleteStoppedVMRemovesDiskAndReservation(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	h.delete(id)
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetVM after delete = %v, want NotFound", err)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disk directory survived: %v", err)
	}
	if got := stopStartReserved(t, h.mg); got != (resources{}) {
		t.Fatalf("reservation leaked: %+v", got)
	}
}

// --- restart recovery --------------------------------------------------------------

func TestRestartRecoversStoppedVM(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	created := h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	h2 := stopStartReopen(t, h, "", nil, nil)

	state, vm := stopStartVMState(t, h2.mg, id)
	if state != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state after restart = %s (%v)", state, vm)
	}
	if vm.GetStartedAtUnixMs() != created.GetStartedAtUnixMs() {
		t.Fatalf("started_at lost across restart: %d, want %d", vm.GetStartedAtUnixMs(), created.GetStartedAtUnixMs())
	}
	if got := stopStartReserved(t, h2.mg); got.vcpus != 0 || got.diskGiB != defaultTestBaseGiB {
		t.Fatalf("reserved after restart = %+v, want disk only", got)
	}
	started, err := h2.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM after restart = %v, %v", started, err)
	}
	if started.GetStartedAtUnixMs() < created.GetStartedAtUnixMs() {
		t.Fatalf("started_at went backwards across restart start")
	}
	h2.delete(id)
}

func TestRestartTreatsInterruptedStartAsStopped(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state storage.QEMUState
	}{
		{"launching", storage.QEMULaunching},
		{"running", storage.QEMURunning},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{})
			id := ids.Sandbox()
			h.create(id, nil)
			if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
				t.Fatalf("StopVM: %v", err)
			}
			// A crash mid-start leaves a launch record whose process is
			// gone (the boot id is from a previous host boot here).
			stopStartEditManifest(t, h, id, func(m *storage.VMMeta) {
				m.QEMU = &storage.QEMURecord{
					State:           tc.state,
					PID:             4242,
					BootID:          "an-old-host-boot",
					StartTicks:      99,
					StartedAtUnixMs: m.StoppedAtUnixMs,
				}
			})
			h2 := stopStartReopen(t, h, "/nonexistent/qemu-system-x86_64", nil, nil)
			state, vm := stopStartVMState(t, h2.mg, id)
			if state != nodev1.VMState_VM_STATE_STOPPED {
				t.Fatalf("state after restart = %s (%v), want STOPPED", state, vm)
			}
			meta := stopStartManifest(t, h.dataDir, id)
			if !meta.Stopped() {
				t.Fatalf("manifest not settled to stopped: %+v", meta)
			}
			started, err := h2.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
			if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
				t.Fatalf("StartVM after interrupted start = %v, %v", started, err)
			}
			h2.delete(id)
		})
	}
}

func TestRestartStillRemovesNeverLaunchedOrphans(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	// Erase the stop marker: the record becomes a never-launched orphan.
	stopStartEditManifest(t, h, id, func(m *storage.VMMeta) {
		m.StoppedAtUnixMs = 0
	})
	h2 := stopStartReopen(t, h, "", nil, nil)
	if _, err := h2.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetVM after restart = %v, want NotFound", err)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan directory survived: %v", err)
	}
}

// --- snapshots of stopped VMs -------------------------------------------------------

func TestSaveSnapshotOfStoppedAndFailedVMs(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	snap, err := h.mg.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{SandboxId: id, SnapshotId: "snap-stopped"})
	if err != nil {
		t.Fatalf("SaveSnapshot of stopped: %v", err)
	}
	if snap.GetSnapshotId() != "snap-stopped" || snap.GetSourceSandboxId() != id {
		t.Fatalf("snapshot = %+v", snap)
	}
	// A VM created from the stopped disk boots normally.
	child := ids.Sandbox()
	created, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId:    child,
		Resources:    &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB},
		FromSnapshot: "snap-stopped",
	})
	if err != nil || created.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("CreateVM from stopped snapshot = %v, %v", created, err)
	}
	h.delete(child)
	h.delete(id)

	// A FAILED VM cannot be snapshotted.
	flaky := &flakyPing{}
	hf := newHarness(t, harnessOptions{
		wrapGuest:   func(s guestv1.GuestServer) guestv1.GuestServer { return flaky.wrap(s) },
		bootTimeout: 300 * time.Millisecond,
	})
	flaky.setFailing(true)
	failed := ids.Sandbox()
	booted := make(chan error, 1)
	go func() {
		_, err := hf.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
			SandboxId: failed,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
		})
		booted <- err
	}()
	if err := <-booted; status.Code(err) != codes.Internal {
		t.Fatalf("CreateVM = %v, want Internal", err)
	}
	_, err = hf.mg.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{SandboxId: failed, SnapshotId: "snap-failed"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SaveSnapshot of FAILED = %v, want FailedPrecondition", err)
	}
}

// --- review regressions --------------------------------------------------------

func TestCommitReadyFailureFailsTheBoot(t *testing.T) {
	// Create variant: a boot whose ready metadata cannot be committed never
	// reports READY.
	gate := newGatePing()
	h := newHarness(t, harnessOptions{fallback: gate, stubGuest: true})
	id := ids.Sandbox()
	gate.arm()
	booted := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
		})
		booted <- err
	}()
	<-gate.entered
	if err := os.Chmod(filepath.Join(h.dataDir, "vms", id), 0o500); err != nil {
		t.Fatal(err)
	}
	gate.disarm()
	if err := <-booted; status.Code(err) != codes.Internal {
		t.Fatalf("CreateVM with a failing ready-write = %v, want Internal", err)
	}
	state, vm := stopStartVMState(t, h.mg, id)
	if state != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("state after failed ready-write = %s (%v)", state, vm)
	}
	if err := os.Chmod(filepath.Join(h.dataDir, "vms", id), 0o700); err != nil {
		t.Fatal(err)
	}
	h.delete(id)
}

func TestStartCommitReadyFailureKeepsStoppedAcrossRestart(t *testing.T) {
	// Start variant with the full stop/start/delete staging: the failed
	// ready-write must leave a durably stopped record whose disk survives a
	// restart, because no late ready-write can clear the stop marker of a
	// manifest whose QEMU record is gone.
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	gate.arm()
	startErr := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
		startErr <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("start never reached Ping")
	}
	// The durable start phase (manifest, growth) has completed by the time
	// the guest answers; only the ready-write is left to fail.
	if err := os.Chmod(filepath.Join(h.dataDir, "vms", id), 0o500); err != nil {
		t.Fatal(err)
	}
	gate.disarm()
	if err := <-startErr; status.Code(err) != codes.Internal {
		t.Fatalf("StartVM with a failing ready-write = %v, want Internal", err)
	}
	state, _ := stopStartVMState(t, h.mg, id)
	if state != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("state after failed ready-write = %s", state)
	}
	meta := stopStartManifest(t, h.dataDir, id)
	if meta.StoppedAtUnixMs <= 0 {
		t.Fatalf("failed ready-write left no stop marker: %+v", meta)
	}
	if err := os.Chmod(filepath.Join(h.dataDir, "vms", id), 0o700); err != nil {
		t.Fatal(err)
	}
	h2 := stopStartReopen(t, h, "/nonexistent/qemu-system-x86_64", nil, nil)
	state, vm := stopStartVMState(t, h2.mg, id)
	if state != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state after restart = %s (%v), want STOPPED: the disk must survive", state, vm)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); err != nil {
		t.Fatalf("kept disk vanished: %v", err)
	}
	started, err := h2.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM after recovery = %v, %v", started, err)
	}
	h2.delete(id)
}

func TestStartVMBootTimeoutWithLiveCallerFails(t *testing.T) {
	flaky := &flakyPing{}
	h := newHarness(t, harnessOptions{
		wrapGuest:   func(s guestv1.GuestServer) guestv1.GuestServer { return flaky.wrap(s) },
		bootTimeout: 300 * time.Millisecond,
	})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	flaky.setFailing(true)
	// The caller's context stays alive: a boot deadline is a boot failure,
	// never a silent return-to-stopped.
	_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if status.Code(err) != codes.Internal {
		t.Fatalf("StartVM on boot timeout = %v, want Internal", err)
	}
	state, vm := stopStartVMState(t, h.mg, id)
	if state != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("state after boot timeout = %s (%v)", state, vm)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); err != nil {
		t.Fatalf("disk removed by a failed start: %v", err)
	}
	flaky.setFailing(false)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM of the failed start: %v", err)
	}
	started, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM retry = %v, %v", started, err)
	}
}

func TestConcurrentStartGrowthAgainstLimitedDisk(t *testing.T) {
	free := uint64(4) << 30 // the whole disk promise
	h := newHarness(t, harnessOptions{
		capacity:  &nodev1.Resources{Vcpus: 8, MemoryMib: 8192, DiskGib: 100},
		freeBytes: &free,
	})
	a, b := ids.Sandbox(), ids.Sandbox()
	h.create(a, &nodev1.Resources{Vcpus: 1, MemoryMib: 256, DiskGib: 1})
	h.create(b, &nodev1.Resources{Vcpus: 1, MemoryMib: 256, DiskGib: 1})
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: a, GraceSeconds: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: b, GraceSeconds: 1}); err != nil {
		t.Fatal(err)
	}
	grow := &nodev1.Resources{Vcpus: 1, MemoryMib: 256, DiskGib: 3}
	results := map[string]codes.Code{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, id := range []string{a, b} {
		wg.Go(func() {
			_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id, Resources: grow})
			mu.Lock()
			results[id] = status.Code(err)
			mu.Unlock()
		})
	}
	wg.Wait()
	if results[a] == codes.OK && results[b] == codes.OK {
		t.Fatalf("both growths admitted against a 4 GiB promise: %v", results)
	}
	// Exactly one grew to 3 GiB and stays accounted; the loser is intact.
	if got := stopStartReserved(t, h.mg); got.diskGiB != 4 {
		t.Fatalf("reserved disk = %d GiB, want 4", got.diskGiB)
	}
	grown, kept := 0, 0
	for _, id := range []string{a, b} {
		if results[id] == codes.OK {
			grown++
			if marker := stopStartDiskMarker(t, h.dataDir, id); marker != 3<<30 {
				t.Fatalf("%s admitted but disk marker = %d", id, marker)
			}
		} else {
			kept++
			if results[id] != codes.ResourceExhausted {
				t.Fatalf("%s rejected with %s", id, results[id])
			}
			state, _ := stopStartVMState(t, h.mg, id)
			if state != nodev1.VMState_VM_STATE_STOPPED {
				t.Fatalf("%s state after rejection = %s", id, state)
			}
			if marker := stopStartDiskMarker(t, h.dataDir, id); marker != 1<<30 {
				t.Fatalf("%s rejected but disk marker = %d", id, marker)
			}
			if meta := stopStartManifest(t, h.dataDir, id); meta.Resources.DiskGiB != 1 {
				t.Fatalf("%s manifest disk = %d GiB after rejection", id, meta.Resources.DiskGiB)
			}
		}
	}
	if grown != 1 || kept != 1 {
		t.Fatalf("grown=%d kept=%d", grown, kept)
	}
}

func TestAbortedStartAfterGrowthKeepsGrownSize(t *testing.T) {
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1})
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	gate.arm()
	ctx, cancel := context.WithCancel(t.Context())
	startErr := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(ctx, &nodev1.StartVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 3},
		})
		startErr <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("start never reached Ping")
	}
	cancel()
	if err := <-startErr; status.Code(err) != codes.Canceled {
		t.Fatalf("StartVM = %v, want Canceled", err)
	}
	waitFor(t, "aborted start to settle", func() bool {
		state, _ := stopStartVMState(t, h.mg, id)
		return state == nodev1.VMState_VM_STATE_STOPPED
	})
	// The completed growth is retained: logical size, physical marker and
	// accounting all say 3 GiB, and a later smaller request is a shrink.
	meta := stopStartManifest(t, h.dataDir, id)
	if meta.Resources.DiskGiB != 3 || !meta.Stopped() {
		t.Fatalf("manifest after aborted start = %+v", meta)
	}
	if marker := stopStartDiskMarker(t, h.dataDir, id); marker != 3<<30 {
		t.Fatalf("disk marker after aborted start = %d", marker)
	}
	if got := stopStartReserved(t, h.mg); got.diskGiB != 3 || got.vcpus != 0 {
		t.Fatalf("reserved after aborted start = %+v", got)
	}
	_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 2},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("StartVM below the grown size = %v, want InvalidArgument", err)
	}
	// The grown size also survives a restart.
	h2 := stopStartReopen(t, h, "", nil, nil)
	state, vm := stopStartVMState(t, h2.mg, id)
	if state != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state after restart = %s (%v)", state, vm)
	}
	if got := stopStartReserved(t, h2.mg); got.diskGiB != 3 {
		t.Fatalf("reserved after restart = %+v", got)
	}
	gate.disarm()
	started, err := h2.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM retry after growth = %v, %v", started, err)
	}
	h2.delete(id)
}

func TestAbortedStartPersistFailureKeepsReservation(t *testing.T) {
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	full := stopStartReserved(t, h.mg) // STOPPED: disk only
	withCompute := resources{vcpus: 2, memoryMib: 2048, diskGiB: defaultTestBaseGiB}
	gate.arm()
	ctx, cancel := context.WithCancel(t.Context())
	startErr := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(ctx, &nodev1.StartVMRequest{SandboxId: id})
		startErr <- err
	}()
	<-gate.entered
	if err := os.Chmod(filepath.Join(h.dataDir, "vms", id), 0o500); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-startErr; status.Code(err) != codes.Canceled {
		t.Fatalf("StartVM = %v, want Canceled", err)
	}
	waitFor(t, "aborted start to settle", func() bool {
		state, _ := stopStartVMState(t, h.mg, id)
		return state == nodev1.VMState_VM_STATE_FAILED || state == nodev1.VMState_VM_STATE_STOPPED
	})
	state, vm := stopStartVMState(t, h.mg, id)
	if state != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("an unpersistable abort published %s (%v), want FAILED", state, vm)
	}
	if got := stopStartReserved(t, h.mg); got != withCompute {
		t.Fatalf("reservation after unpersistable abort = %+v, want the full %+v (was %+v before the start)", got, withCompute, full)
	}
	if err := os.Chmod(filepath.Join(h.dataDir, "vms", id), 0o700); err != nil {
		t.Fatal(err)
	}
	stopped, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if err != nil || stopped.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM retry = %v, %v", stopped, err)
	}
	if got := stopStartReserved(t, h.mg); got != full {
		t.Fatalf("reserved after retried stop = %+v, want %+v", got, full)
	}
	gate.disarm()
}

func TestRestartInterruptedStartPersistFailureKeepsDisk(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	stopStartEditManifest(t, h, id, func(m *storage.VMMeta) {
		m.QEMU = &storage.QEMURecord{
			State:           storage.QEMULaunching,
			BootID:          "an-old-host-boot",
			StartedAtUnixMs: m.StoppedAtUnixMs,
		}
	})
	if err := os.Chmod(filepath.Join(h.dataDir, "vms", id), 0o500); err != nil {
		t.Fatal(err)
	}
	h2 := stopStartReopen(t, h, "/nonexistent/qemu-system-x86_64", nil, nil)
	state, vm := stopStartVMState(t, h2.mg, id)
	if state != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("state after unpersistable interrupted start = %s (%v), want FAILED", state, vm)
	}
	if got := stopStartReserved(t, h2.mg); got.diskGiB != defaultTestBaseGiB || got.vcpus != 0 {
		t.Fatalf("kept disk not accounted after failed settlement: %+v", got)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); err != nil {
		t.Fatalf("kept disk vanished: %v", err)
	}
	if err := os.Chmod(filepath.Join(h.dataDir, "vms", id), 0o700); err != nil {
		t.Fatal(err)
	}
	stopped, err := h2.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if err != nil || stopped.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM after failed settlement = %v, %v", stopped, err)
	}
	started, err := h2.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM after settlement = %v, %v", started, err)
	}
	h2.delete(id)
}

// --- second-review regressions -------------------------------------------------

// TestStartGateRecheckJoinsTheEarlierStarter stages three StartVM callers
// that all observed STOPPED before acquiring the operation gate. The winner
// claims the boot; the others must join it (or see the idempotent result),
// never FailedPrecondition.
func TestStartGateRecheckJoinsTheEarlierStarter(t *testing.T) {
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	// Turnstile: each starter parks after observing STOPPED, before racing
	// for the gate.
	parked := make(chan chan struct{}, 8)
	h.mg.mu.Lock()
	h.mg.beforeStartGate = func() {
		c := make(chan struct{})
		parked <- c
		<-c
	}
	h.mg.mu.Unlock()

	const callers = 3
	errs := make(chan error, callers)
	vms := make(chan *nodev1.VM, callers)
	for range callers {
		go func() {
			vm, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
			vms <- vm
			errs <- err
		}()
	}
	tickets := make([]chan struct{}, 0, callers)
	for range callers {
		select {
		case c := <-parked:
			tickets = append(tickets, c)
		case <-time.After(5 * time.Second):
			t.Fatal("a starter never observed STOPPED")
		}
	}
	// Let exactly one through; it claims the boot and parks in Ping.
	close(tickets[0])
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the winning starter never reached Ping")
	}
	// The losers observed STOPPED before the claim; now they race for the
	// gate the winner has already released.
	close(tickets[1])
	close(tickets[2])
	for range callers {
		if err := <-errs; err != nil {
			t.Fatalf("StartVM after losing the gate race = %v, want a joined boot", err)
		}
		vm := <-vms
		if vm.GetState() != nodev1.VMState_VM_STATE_READY {
			t.Fatalf("joined starter saw state %s, want READY", vm.GetState())
		}
	}
	gate.disarm()
	if got := h.launcher.count(); got != 2 {
		t.Fatalf("launcher ran %d times, want 2", got)
	}
}

func TestOwnerCancelDuringGrowthKeepsBootForWaiters(t *testing.T) {
	var imager *fakeImager
	h := newHarness(t, harnessOptions{configure: func(c *Config) {
		imager = c.Imager.(*fakeImager)
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	growEntered := make(chan struct{}, 1)
	growRelease := make(chan struct{})
	imager.growHook = func(ctx context.Context) error {
		growEntered <- struct{}{}
		<-growRelease
		return ctx.Err() // like qemu-img dying with its context
	}

	ownerCtx, cancel := context.WithCancel(t.Context())
	ownerErr := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(ownerCtx, &nodev1.StartVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 2},
		})
		ownerErr <- err
	}()
	select {
	case <-growEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("the starter never reached the growth phase")
	}
	// A second starter joins the claimed boot while growth is in flight.
	waiterErr := make(chan error, 1)
	waiterVM := make(chan *nodev1.VM, 1)
	go func() {
		vm, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
		waiterVM <- vm
		waiterErr <- err
	}()
	waitFor(t, "waiter to join the boot", func() bool { return waiterCount(h, id) == 2 })
	// The owner gives up mid-growth and must return at once, while the
	// shared growth stays blocked: its cancellation only removes its
	// ticket, and the surviving waiter keeps the boot alive.
	cancel()
	select {
	case err := <-ownerErr:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("owner StartVM = %v, want Canceled while growth stays blocked", err)
		}
	case <-time.After(500 * time.Millisecond):
		close(growRelease)
		<-ownerErr
		t.Fatal("canceled owner did not return while shared growth remained blocked")
	}
	close(growRelease)

	if err := <-waiterErr; err != nil {
		t.Fatalf("waiter StartVM = %v, want the surviving boot to finish", err)
	}
	vm := <-waiterVM
	if vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("state after owner cancel = %s, want READY", vm.GetState())
	}
	if marker := stopStartDiskMarker(t, h.dataDir, id); marker != 2<<30 {
		t.Fatalf("disk marker = %d, want 2 GiB", marker)
	}
	if meta := stopStartManifest(t, h.dataDir, id); meta.Resources.DiskGiB != 2 {
		t.Fatalf("manifest disk = %d GiB", meta.Resources.DiskGiB)
	}
}

func TestOwnerCancelAloneDuringGrowthAbortsAndReturns(t *testing.T) {
	var imager *fakeImager
	h := newHarness(t, harnessOptions{configure: func(c *Config) {
		imager = c.Imager.(*fakeImager)
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	growEntered := make(chan struct{}, 1)
	growRelease := make(chan struct{})
	imager.growHook = func(ctx context.Context) error {
		growEntered <- struct{}{}
		<-growRelease
		return ctx.Err()
	}
	ownerCtx, cancel := context.WithCancel(t.Context())
	ownerErr := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(ownerCtx, &nodev1.StartVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 2},
		})
		ownerErr <- err
	}()
	select {
	case <-growEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("the starter never reached the growth phase")
	}
	// The only waiter leaves: the blocked growth must be canceled and the
	// owner must return promptly, before the test releases the gate.
	cancel()
	select {
	case err := <-ownerErr:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("owner StartVM = %v, want Canceled while growth stays blocked", err)
		}
	case <-time.After(500 * time.Millisecond):
		close(growRelease)
		<-ownerErr
		t.Fatal("canceled owner did not return while shared growth remained blocked")
	}
	close(growRelease)
	waitFor(t, "aborted start to settle", func() bool {
		state, _ := stopStartVMState(t, h.mg, id)
		return state == nodev1.VMState_VM_STATE_STOPPED
	})
	meta := stopStartManifest(t, h.dataDir, id)
	if !meta.Stopped() {
		t.Fatalf("manifest after aborted growth = %+v", meta)
	}
	if marker := stopStartDiskMarker(t, h.dataDir, id); marker != 1<<30 {
		t.Fatalf("canceled growth still wrote the disk marker = %d", marker)
	}
	// The manifest durably records the grown size (PrepareStart committed
	// before the growth was canceled), so the disk stays accounted at that
	// size; the physical file catches up on the next start.
	if got := stopStartReserved(t, h.mg); got.diskGiB != 2 || got.vcpus != 0 {
		t.Fatalf("reserved after aborted growth = %+v", got)
	}
	// The retry completes the growth normally.
	started, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 2},
	})
	if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM retry = %v, %v", started, err)
	}
	if marker := stopStartDiskMarker(t, h.dataDir, id); marker != 2<<30 {
		t.Fatalf("disk marker after retry = %d", marker)
	}
}

func TestCreateVMOfStoppedAfterCanceledStartIsIdempotent(t *testing.T) {
	var imager *fakeImager
	h := newHarness(t, harnessOptions{configure: func(c *Config) {
		imager = c.Imager.(*fakeImager)
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	growEntered := make(chan struct{}, 1)
	growRelease := make(chan struct{})
	imager.growHook = func(ctx context.Context) error {
		growEntered <- struct{}{}
		<-growRelease
		return ctx.Err()
	}
	ownerCtx, cancel := context.WithCancel(t.Context())
	startErr := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(ownerCtx, &nodev1.StartVMRequest{
			SandboxId: id,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 2},
		})
		startErr <- err
	}()
	<-growEntered
	cancel()
	if err := <-startErr; status.Code(err) != codes.Canceled {
		t.Fatalf("StartVM = %v, want Canceled", err)
	}
	close(growRelease)
	waitFor(t, "aborted start to settle", func() bool {
		state, _ := stopStartVMState(t, h.mg, id)
		return state == nodev1.VMState_VM_STATE_STOPPED
	})
	// The record is settled STOPPED: CreateVM returns it unchanged — the
	// aborted boot's failure must not leak into existing-id idempotency.
	seen, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 9, MemoryMib: 999, DiskGib: 9},
	})
	if err != nil || seen.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("CreateVM after a canceled start = %v, %v; want the STOPPED record", seen, err)
	}
	if got := h.launcher.count(); got != 1 {
		t.Fatalf("launcher ran %d times", got)
	}
}

func TestUncertainManifestCommitKeepsGrowthReserved(t *testing.T) {
	var syncFail atomic.Bool
	h := newHarness(t, harnessOptions{configure: func(c *Config) {
		c.snapshotOptions = storage.SnapshotOptions{
			SyncDir: func(root *os.Root, name string) error {
				if syncFail.Load() {
					return errors.New("directory sync fault")
				}
				d, err := root.Open(name)
				if err != nil {
					return err
				}
				defer d.Close()
				return d.Sync()
			},
		}
	}})
	id := ids.Sandbox()
	h.create(id, &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1})
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}

	// The manifest rename succeeds but its directory sync fails: the commit
	// outcome is unknown, so the growth must stay reserved.
	syncFail.Store(true)
	_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 2},
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("StartVM with an uncertain manifest commit = %v, want Internal", err)
	}
	state, vm := stopStartVMState(t, h.mg, id)
	if state != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state after uncertain commit = %s (%v)", state, vm)
	}
	meta := stopStartManifest(t, h.dataDir, id)
	if meta.Resources.DiskGiB != 2 {
		t.Fatalf("durable manifest disk = %d GiB; the rename already published the new size", meta.Resources.DiskGiB)
	}
	if got := stopStartReserved(t, h.mg); got.diskGiB != 2 || got.vcpus != 0 {
		t.Fatalf("reserved after uncertain commit = %+v; the growth must stay accounted until the outcome is known", got)
	}
	// The retry, with durability working again, completes the growth.
	syncFail.Store(false)
	started, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 2},
	})
	if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM retry = %v, %v", started, err)
	}
	if marker := stopStartDiskMarker(t, h.dataDir, id); marker != 2<<30 {
		t.Fatalf("disk marker after retry = %d", marker)
	}
}

func TestAdoptedStopSettleFailureKeepsReservation(t *testing.T) {
	sleep := helperSleep(t)
	var syncFail atomic.Bool
	faultySync := storage.SnapshotOptions{SyncDir: func(root *os.Root, name string) error {
		if syncFail.Load() {
			return errors.New("directory sync fault")
		}
		d, err := root.Open(name)
		if err != nil {
			return err
		}
		defer d.Close()
		return d.Sync()
	}}
	h := newHarness(t, harnessOptions{configure: func(c *Config) {
		c.snapshotOptions = faultySync
	}})
	id := ids.Sandbox()
	h.create(id, &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB})
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	ticks, err := storage.ProcStartTicks(sleep.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	bootID, err := storage.HostBootID()
	if err != nil {
		t.Fatal(err)
	}
	stopStartEditManifest(t, h, id, func(m *storage.VMMeta) {
		m.QEMU = &storage.QEMURecord{
			State:           storage.QEMURunning,
			PID:             sleep.Process.Pid,
			BootID:          bootID,
			StartTicks:      ticks,
			StartedAtUnixMs: m.StoppedAtUnixMs,
		}
	})

	server := startFakeQMP(t, id)
	quitSeen := make(chan struct{})
	server.mu.Lock()
	server.quit = quitSeen
	server.mu.Unlock()
	go func() {
		<-quitSeen
		_ = sleep.Process.Kill()
	}()

	// The adopted process's stop is proven (the QMP quit lands and the
	// process dies), but recording STOPPED fails: the full reservation must
	// survive the failed settlement.
	syncFail.Store(true)
	h2 := stopStartReopen(t, h, "", func(c *Config) {
		c.findProcess = func(meta storage.VMMeta) (*pinnedProc, error) {
			return pinProc(sleep.Process.Pid)
		}
		c.verifyProc = func(ctx context.Context, p *pinnedProc, meta storage.VMMeta) (*qmp.Client, error) {
			return qmp.Dial(ctx, server.path, slog.New(slog.DiscardHandler))
		}
		c.snapshotOptions = faultySync
	}, nil)
	state, vm := stopStartVMState(t, h2.mg, id)
	if state != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("state after failed settlement = %s (%v), want FAILED", state, vm)
	}
	full := resources{vcpus: 2, memoryMib: 2048, diskGiB: defaultTestBaseGiB}
	if got := stopStartReserved(t, h2.mg); got != full {
		t.Fatalf("reservation after failed settlement = %+v, want the full %+v retained", got, full)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); err != nil {
		t.Fatalf("kept disk vanished: %v", err)
	}
	syncFail.Store(false)
	stopped, err := h2.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if err != nil || stopped.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM retry after failed settlement = %v, %v", stopped, err)
	}
	if got := stopStartReserved(t, h2.mg); got.diskGiB != defaultTestBaseGiB || got.vcpus != 0 {
		t.Fatalf("reserved after retried stop = %+v", got)
	}
	h2.delete(id)
}

func TestReadyWriteVersusConcurrentStopOrdering(t *testing.T) {
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	inReady := make(chan struct{})
	releaseReady := make(chan struct{})
	h.mg.mu.Lock()
	h.mg.afterReady = func() {
		close(inReady)
		<-releaseReady
	}
	h.mg.mu.Unlock()

	startErr := make(chan error, 1)
	startVM := make(chan *nodev1.VM, 1)
	go func() {
		vm, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
		startVM <- vm
		startErr <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("start never reached Ping")
	}
	gate.disarm()
	select {
	case <-inReady:
	case <-time.After(5 * time.Second):
		t.Fatal("the boot never reached its READY boundary")
	}
	// A stop that observes READY must be able to record STOPPED without a
	// late ready-write clearing the marker afterwards.
	stopDone := make(chan *nodev1.VM, 1)
	stopErrc := make(chan error, 1)
	go func() {
		vm, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
		stopDone <- vm
		stopErrc <- err
	}()
	if err := <-stopErrc; err != nil {
		t.Fatalf("concurrent StopVM: %v", err)
	}
	stopped := <-stopDone
	if stopped.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("concurrent stop state = %s", stopped.GetState())
	}
	close(releaseReady)
	if err := <-startErr; err != nil {
		t.Fatalf("StartVM = %v", err)
	}
	(<-startVM)
	// The durable record is stopped; a restart must keep the disk instead of
	// mistaking it for a never-launched orphan.
	meta := stopStartManifest(t, h.dataDir, id)
	if !meta.Stopped() {
		t.Fatalf("manifest after the ready/stop race = %+v, want durably stopped", meta)
	}
	h2 := stopStartReopen(t, h, "", nil, nil)
	state, vm := stopStartVMState(t, h2.mg, id)
	if state != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state after restart = %s (%v), want STOPPED", state, vm)
	}
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); err != nil {
		t.Fatalf("kept disk vanished across the restart: %v", err)
	}
	h2.delete(id)
}

// --- generation-race regressions ----------------------------------------------

// stopStartVMRecord fetches one in-memory record for tests that drive the
// package-private lifecycle directly.
func stopStartVMRecord(t *testing.T, m *Manager, id string) *vm {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.vms[id]
	if v == nil {
		t.Fatalf("no in-memory record for %s", id)
	}
	return v
}

// TestOldWorkerCannotPublishAcrossGenerations parks the old start's worker
// past its READY transition, runs a full Stop and a new Start (which reset
// the boot channel and generation), and only then releases the old worker.
// The old worker must not close or overwrite the new generation's channel
// (which used to panic on the second close) nor disturb its state.
func TestOldWorkerCannotPublishAcrossGenerations(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	// afterReady fires for every boot; the hook is installed after the
	// create, so the first start's boot is its first invocation. Only that
	// one parks.
	var calls atomic.Int64
	parked := make(chan struct{})
	release := make(chan struct{})
	h.mg.mu.Lock()
	h.mg.afterReady = func() {
		if calls.Add(1) == 1 {
			close(parked)
			<-release
		}
	}
	h.mg.mu.Unlock()

	firstErr := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
		firstErr <- err
	}()
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("the first start never reached its post-READY boundary")
	}
	// The READY VM is stopped, then a new start claims a fresh boot
	// generation while the old worker is still parked.
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	second, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || second.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("second StartVM = %v, %v", second, err)
	}
	// Release the old worker: with the generation guard it closes only its
	// own channel; without it, it closed the new generation's already-closed
	// channel and the test binary died on the panic.
	close(release)
	time.Sleep(200 * time.Millisecond)
	if err := <-firstErr; err != nil {
		t.Fatalf("first StartVM = %v", err)
	}
	state, vm := stopStartVMState(t, h.mg, id)
	if state != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("state after the cross-generation release = %s (%v)", state, vm)
	}
	if got := stopStartReserved(t, h.mg); got.vcpus != 2 || got.diskGiB != defaultTestBaseGiB {
		t.Fatalf("reserved after the cross-generation release = %+v", got)
	}
	h.delete(id)
}

// TestNewStartWaitsForAbortingWorkerSettlement parks an aborting start's
// worker inside its settlement (durable writes done, accounting and STOPPED
// publish pending) and issues a new start against it. The new start must
// never claim or move reservations underneath the settling worker: the
// record only reads STOPPED once the settlement is complete.
func TestNewStartWaitsForAbortingWorkerSettlement(t *testing.T) {
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	inSettle := make(chan struct{})
	releaseSettle := make(chan struct{})
	h.mg.mu.Lock()
	h.mg.afterAbortPersist = func() {
		close(inSettle)
		<-releaseSettle
	}
	h.mg.mu.Unlock()

	gate.arm()
	ownerCtx, cancel := context.WithCancel(t.Context())
	firstErr := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(ownerCtx, &nodev1.StartVMRequest{SandboxId: id})
		firstErr <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the start never reached Ping")
	}
	cancel()
	if err := <-firstErr; status.Code(err) != codes.Canceled {
		t.Fatalf("StartVM = %v, want Canceled", err)
	}
	select {
	case <-inSettle:
	case <-time.After(5 * time.Second):
		t.Fatal("the aborting worker never reached its settlement boundary")
	}
	// A new start arrives while the old worker is mid-settlement. It may
	// not claim until the record reads STOPPED, which the settlement
	// publishes last.
	joinErr := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
		joinErr <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if state, _ := stopStartVMState(t, h.mg, id); state == nodev1.VMState_VM_STATE_STOPPED {
		t.Fatal("STOPPED published before the settlement finished accounting")
	}
	close(releaseSettle)
	// The waiting starter observes the aborted boot's failure...
	if err := <-joinErr; status.Code(err) != codes.Internal {
		t.Fatalf("start during settlement = %v, want the aborted boot's failure", err)
	}
	state, _ := stopStartVMState(t, h.mg, id)
	if state != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state after settlement = %s", state)
	}
	if got := stopStartReserved(t, h.mg); got.vcpus != 0 || got.diskGiB != defaultTestBaseGiB {
		t.Fatalf("reserved after settlement = %+v; the old worker's accounting leaked", got)
	}
	// ...and a fresh start claims the settled record and boots cleanly, with
	// exactly one boot's reservations.
	gate.disarm()
	started, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("fresh StartVM after settlement = %v, %v", started, err)
	}
	if got := stopStartReserved(t, h.mg); got.vcpus != 2 || got.diskGiB != defaultTestBaseGiB {
		t.Fatalf("reserved after fresh start = %+v", got)
	}
	h.delete(id)
}

// TestStaleOwnerCancellationCannotAbortNewerBoot drives a starter that
// claims a boot but never enters waitBoot (simulating descheduling) across a
// whole READY -> Stop -> new Start cycle. Its cancellation carries its
// claim-time generation and must be a no-op against the newer boot.
func TestStaleOwnerCancellationCannotAbortNewerBoot(t *testing.T) {
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	v := stopStartVMRecord(t, h.mg, id)

	// The old owner claims but never waits; its boot finishes on its own.
	out, claimGen, err := h.mg.ownStart(v, &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || out != nil {
		t.Fatalf("ownStart = %v, %d, %v", out, claimGen, err)
	}
	waitFor(t, "old boot to finish", func() bool {
		state, _ := stopStartVMState(t, h.mg, id)
		return state == nodev1.VMState_VM_STATE_READY
	})
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}

	// A new start claims a fresh generation and parks in Ping with its owner
	// waiting on it.
	gate.arm()
	newErr := make(chan error, 1)
	newVM := make(chan *nodev1.VM, 1)
	go func() {
		vm, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
		newVM <- vm
		newErr <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the new start never reached Ping")
	}
	waitFor(t, "new owner to wait", func() bool { return waiterCount(h, id) == 1 })

	// The stale owner finally runs, already canceled. Its ticket belongs to
	// the old generation: the newer boot's single waiter must survive.
	staleCtx, staleCancel := context.WithCancel(t.Context())
	staleCancel()
	staleErr := make(chan error, 1)
	go func() {
		_, err := h.mg.waitBoot(staleCtx, v, claimGen)
		staleErr <- err
	}()
	if err := <-staleErr; status.Code(err) != codes.Canceled {
		t.Fatalf("stale owner waitBoot = %v, want Canceled", err)
	}
	if got := waiterCount(h, id); got != 1 {
		t.Fatalf("waiters after stale cancellation = %d, want the new owner's 1", got)
	}
	gate.disarm()
	if err := <-newErr; err != nil {
		t.Fatalf("new StartVM survived the stale cancellation = %v, want READY", err)
	}
	vm := <-newVM
	if vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("state = %s, want READY", vm.GetState())
	}
	h.delete(id)
}

// --- recovery disk-sync regressions --------------------------------------------

// stopStartWriteDisk restores a fake disk file so a retried settlement can
// flush it.
func stopStartWriteDisk(t *testing.T, h *harness, id string) {
	t.Helper()
	var marker [8]byte
	binary.LittleEndian.PutUint64(marker[:], defaultTestBaseGiB<<30)
	if err := os.WriteFile(filepath.Join(h.dataDir, "vms", id, "disk.qcow2"), marker[:], 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRestartInterruptedStartSyncFailureSkipsCommitStopped(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	stopStartEditManifest(t, h, id, func(m *storage.VMMeta) {
		m.QEMU = &storage.QEMURecord{
			State:           storage.QEMULaunching,
			BootID:          "an-old-host-boot",
			StartedAtUnixMs: m.StoppedAtUnixMs,
		}
	})
	// The kept disk cannot be flushed: the settlement must keep the stale
	// launch record (CommitStopped never runs) and leave a retryable FAILED
	// record with the disk still accounted.
	if err := os.Remove(filepath.Join(h.dataDir, "vms", id, "disk.qcow2")); err != nil {
		t.Fatal(err)
	}
	h2 := stopStartReopen(t, h, "/nonexistent/qemu-system-x86_64", nil, nil)
	state, vm := stopStartVMState(t, h2.mg, id)
	if state != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("state after unflushable settlement = %s (%v), want FAILED", state, vm)
	}
	meta := stopStartManifest(t, h.dataDir, id)
	if meta.QEMU == nil || meta.QEMU.State != storage.QEMULaunching {
		t.Fatalf("manifest = %+v; CommitStopped must not run on a failed disk sync", meta)
	}
	if got := stopStartReserved(t, h2.mg); got.diskGiB != defaultTestBaseGiB || got.vcpus != 0 {
		t.Fatalf("kept disk not accounted after failed settlement: %+v", got)
	}
	// With the disk back, the StopVM retry settles and the VM boots.
	stopStartWriteDisk(t, h, id)
	stopped, err := h2.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if err != nil || stopped.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM retry = %v, %v", stopped, err)
	}
	started, err := h2.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || started.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM after settlement = %v, %v", started, err)
	}
	h2.delete(id)
}

func TestAdoptedSyncFailureSkipsCommitStopped(t *testing.T) {
	sleep := helperSleep(t)
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB})
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	ticks, err := storage.ProcStartTicks(sleep.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	bootID, err := storage.HostBootID()
	if err != nil {
		t.Fatal(err)
	}
	stopStartEditManifest(t, h, id, func(m *storage.VMMeta) {
		m.QEMU = &storage.QEMURecord{
			State:           storage.QEMURunning,
			PID:             sleep.Process.Pid,
			BootID:          bootID,
			StartTicks:      ticks,
			StartedAtUnixMs: m.StoppedAtUnixMs,
		}
	})
	server := startFakeQMP(t, id)
	quitSeen := make(chan struct{})
	server.mu.Lock()
	server.quit = quitSeen
	server.mu.Unlock()
	go func() {
		<-quitSeen
		_ = sleep.Process.Kill()
	}()
	// The adopted QEMU is killed, but the kept disk cannot be flushed: the
	// settlement must not commit a stopped marker over unflushed writes.
	if err := os.Remove(filepath.Join(h.dataDir, "vms", id, "disk.qcow2")); err != nil {
		t.Fatal(err)
	}
	h2 := stopStartReopen(t, h, "", func(c *Config) {
		c.findProcess = func(meta storage.VMMeta) (*pinnedProc, error) {
			return pinProc(sleep.Process.Pid)
		}
		c.verifyProc = func(ctx context.Context, p *pinnedProc, meta storage.VMMeta) (*qmp.Client, error) {
			return qmp.Dial(ctx, server.path, slog.New(slog.DiscardHandler))
		}
	}, nil)
	state, vm := stopStartVMState(t, h2.mg, id)
	if state != nodev1.VMState_VM_STATE_FAILED {
		t.Fatalf("state after unflushable settlement = %s (%v), want FAILED", state, vm)
	}
	meta := stopStartManifest(t, h.dataDir, id)
	if meta.QEMU == nil || meta.QEMU.State != storage.QEMURunning {
		t.Fatalf("manifest = %+v; CommitStopped must not run on a failed disk sync", meta)
	}
	full := resources{vcpus: 2, memoryMib: 2048, diskGiB: defaultTestBaseGiB}
	if got := stopStartReserved(t, h2.mg); got != full {
		t.Fatalf("reservation after unflushable settlement = %+v, want the full %+v", got, full)
	}
	stopStartWriteDisk(t, h, id)
	stopped, err := h2.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
	if err != nil || stopped.GetState() != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("StopVM retry = %v, %v", stopped, err)
	}
	h2.delete(id)
}

// TestFailedStartSettlementSnapshotsUnderManagerLock parks a failed start's
// settlement at its record snapshot while another sandbox is created. The
// snapshot must be ordered by the manager lock, or the race detector flags
// the settlement's map read against the create's map write.
func TestFailedStartSettlementSnapshotsUnderManagerLock(t *testing.T) {
	var imager *fakeImager
	h := newHarness(t, harnessOptions{configure: func(c *Config) {
		imager = c.Imager.(*fakeImager)
	}})
	a := ids.Sandbox()
	h.create(a, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: a, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	h.mg.mu.Lock()
	h.mg.inStartSettle = func() {
		close(entered)
		<-release
	}
	h.mg.mu.Unlock()
	growErr := errors.New("grow fault")
	imager.growHook = func(context.Context) error { return growErr }

	startErr := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{
			SandboxId: a,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 2},
		})
		startErr <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the settlement never reached its snapshot")
	}
	// Mutate the manager map while the settlement is parked in its
	// snapshot: properly ordered (m.mu first) the create waits; reading the
	// map under v.mu alone is a race the detector reports.
	b := ids.Sandbox()
	created := make(chan error, 1)
	go func() {
		_, err := h.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
			SandboxId: b,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
		})
		created <- err
	}()
	time.Sleep(200 * time.Millisecond)
	close(release)
	if err := <-created; err != nil {
		t.Fatalf("concurrent CreateVM: %v", err)
	}
	if err := <-startErr; status.Code(err) != codes.Internal {
		t.Fatalf("StartVM with a grow fault = %v, want Internal", err)
	}
	state, vm := stopStartVMState(t, h.mg, a)
	if state != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("settled state = %s (%v)", state, vm)
	}
}

// TestLateLiveOwnerJoinsNewGenerationTicket stages a live owner that only
// enters waitBoot after a whole newer boot generation exists, then cancels
// the newer owner: the surviving caller must hold a real ticket and keep
// the shared boot alive.
func TestLateLiveOwnerJoinsNewGenerationTicket(t *testing.T) {
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	v := stopStartVMRecord(t, h.mg, id)
	out, claimGen, err := h.mg.ownStart(v, &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || out != nil {
		t.Fatalf("ownStart = %v, %d, %v", out, claimGen, err)
	}
	waitFor(t, "old boot to finish", func() bool {
		state, _ := stopStartVMState(t, h.mg, id)
		return state == nodev1.VMState_VM_STATE_READY
	})
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}

	// The new owner claims a fresh generation and parks in Ping.
	gate.arm()
	newCtx, newCancel := context.WithCancel(t.Context())
	newErr := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(newCtx, &nodev1.StartVMRequest{SandboxId: id})
		newErr <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the new start never reached Ping")
	}
	waitFor(t, "new owner to wait", func() bool { return waiterCount(h, id) == 1 })

	// The old owner, still live, finally enters with its claim-time
	// generation. It must join the newer generation with a real ticket.
	lateErr := make(chan error, 1)
	lateVM := make(chan *nodev1.VM, 1)
	go func() {
		vm, err := h.mg.waitBoot(t.Context(), v, claimGen)
		lateVM <- vm
		lateErr <- err
	}()
	waitFor(t, "late owner to join the new generation", func() bool { return waiterCount(h, id) == 2 })

	// The newer owner gives up: the late owner's ticket keeps the boot.
	newCancel()
	if err := <-newErr; status.Code(err) != codes.Canceled {
		t.Fatalf("new owner StartVM = %v, want Canceled", err)
	}
	if got := waiterCount(h, id); got != 1 {
		t.Fatalf("waiters after the newer owner left = %d, want the late owner's 1", got)
	}
	gate.disarm()
	if err := <-lateErr; err != nil {
		t.Fatalf("late owner waitBoot = %v, want the surviving boot", err)
	}
	vm := <-lateVM
	if vm.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("late owner saw state %s, want READY", vm.GetState())
	}
	h.delete(id)
}

// TestRestartOfRunningVMPreservesLastStartTime stamps a running VM's
// last-start time at create, restarts the node around the still-running
// process (adoption), and checks the time survived: a restart is not a new
// start. An interrupted StartVM that only now becomes READY stamps fresh.
func TestRestartOfRunningVMPreservesLastStartTime(t *testing.T) {
	sleep := helperSleep(t)
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	created := h.create(id, &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB})
	firstStart := created.GetStartedAtUnixMs()
	if firstStart == 0 {
		t.Fatal("create stamped no start time")
	}
	// The manifest's QEMU record now points at the fake process; repoint it
	// at a real live pid so the reopened manager adopts it.
	ticks, err := storage.ProcStartTicks(sleep.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	bootID, err := storage.HostBootID()
	if err != nil {
		t.Fatal(err)
	}
	stopStartEditManifest(t, h, id, func(m *storage.VMMeta) {
		m.QEMU.PID = sleep.Process.Pid
		m.QEMU.StartTicks = ticks
		m.QEMU.BootID = bootID
	})
	server := startFakeQMP(t, id)
	quitSeen := make(chan struct{})
	server.mu.Lock()
	server.quit = quitSeen
	server.mu.Unlock()
	go func() {
		<-quitSeen
		_ = sleep.Process.Kill()
	}()
	h2 := stopStartReopen(t, h, "", func(c *Config) {
		c.findProcess = func(meta storage.VMMeta) (*pinnedProc, error) {
			return pinProc(sleep.Process.Pid)
		}
		c.verifyProc = func(ctx context.Context, p *pinnedProc, meta storage.VMMeta) (*qmp.Client, error) {
			return qmp.Dial(ctx, server.path, slog.New(slog.DiscardHandler))
		}
	}, pingOnlyGuest{})
	waitFor(t, "adopted VM to become READY", func() bool {
		state, _ := stopStartVMState(t, h2.mg, id)
		return state == nodev1.VMState_VM_STATE_READY
	})
	vm, err := h2.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatal(err)
	}
	if vm.GetStartedAtUnixMs() != firstStart {
		t.Fatalf("last-start time changed across a restart: %d, want the preserved %d", vm.GetStartedAtUnixMs(), firstStart)
	}
	meta := stopStartManifest(t, h.dataDir, id)
	if meta.StartedAtUnixMs != firstStart || meta.StoppedAtUnixMs != 0 {
		t.Fatalf("durable timestamps after adoption = started %d stopped %d, want %d/0",
			meta.StartedAtUnixMs, meta.StoppedAtUnixMs, firstStart)
	}
	// An interrupted StartVM (stop marker still set) that becomes READY on
	// adoption stamps the fresh start time.
	stopStartEditManifest(t, h2, id, func(m *storage.VMMeta) {
		m.StoppedAtUnixMs = firstStart // the stop predating the interrupted start
	})
	h3 := stopStartReopen(t, h2, "", func(c *Config) {
		c.findProcess = func(meta storage.VMMeta) (*pinnedProc, error) {
			return pinProc(sleep.Process.Pid)
		}
		c.verifyProc = func(ctx context.Context, p *pinnedProc, meta storage.VMMeta) (*qmp.Client, error) {
			return qmp.Dial(ctx, server.path, slog.New(slog.DiscardHandler))
		}
	}, pingOnlyGuest{})
	waitFor(t, "interrupted start to become READY", func() bool {
		state, _ := stopStartVMState(t, h3.mg, id)
		return state == nodev1.VMState_VM_STATE_READY
	})
	vm3, err := h3.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatal(err)
	}
	if vm3.GetStartedAtUnixMs() <= firstStart {
		t.Fatalf("interrupted start kept the old time %d; the fresh start must stamp a later time", vm3.GetStartedAtUnixMs())
	}
	h3.delete(id)
}

// TestStartVMValidationOnlyAppliesToTheOwnerPath checks the frozen
// contract's existing-id precedence: invalid replacement fields never
// change what READY, BOOTING or FAILED do; only a STOPPED owner path
// validates them, with no side effects on rejection.
func TestStartVMValidationOnlyAppliesToTheOwnerPath(t *testing.T) {
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	badResources := &nodev1.Resources{Vcpus: 0, MemoryMib: 2048, DiskGib: 1}

	// READY returns at once, whatever the fields say.
	ready, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id, Resources: badResources, Hostname: "not a host"})
	if err != nil || ready.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("StartVM of READY with bad fields = %v, %v", ready, err)
	}
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}

	// BOOTING joins the original boot, whatever the fields say.
	gate.arm()
	booted := make(chan error, 1)
	bootedVM := make(chan *nodev1.VM, 1)
	go func() {
		vm, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id})
		bootedVM <- vm
		booted <- err
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the start never reached Ping")
	}
	joined := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id, Resources: badResources, Hostname: "not a host"})
		joined <- err
	}()
	waitFor(t, "the invalid-fields caller to join", func() bool { return waiterCount(h, id) == 2 })
	gate.disarm()
	if err := <-booted; err != nil {
		t.Fatalf("original start: %v", err)
	}
	if err := <-joined; err != nil {
		t.Fatalf("joined start with bad fields = %v, want the shared boot", err)
	}
	(<-bootedVM)

	// FAILED still preconditions, whatever the fields say.
	flaky := &flakyPing{}
	hf := newHarness(t, harnessOptions{
		wrapGuest:   func(s guestv1.GuestServer) guestv1.GuestServer { return flaky.wrap(s) },
		bootTimeout: 300 * time.Millisecond,
	})
	flaky.setFailing(true)
	failed := ids.Sandbox()
	failedBoot := make(chan error, 1)
	go func() {
		_, err := hf.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
			SandboxId: failed,
			Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 1},
		})
		failedBoot <- err
	}()
	if err := <-failedBoot; status.Code(err) != codes.Internal {
		t.Fatalf("CreateVM = %v, want Internal", err)
	}
	if _, err := hf.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: failed, Resources: badResources, Hostname: "not a host"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartVM of FAILED with bad fields = %v, want FailedPrecondition", err)
	}

	// STOPPED validates the fields, with no side effects on rejection.
	if _, err := hf.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: failed, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM of FAILED: %v", err)
	}
	before := stopStartManifest(t, hf.dataDir, failed)
	_, err = hf.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: failed, Resources: badResources, Hostname: "not a host"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("StartVM of STOPPED with bad fields = %v, want InvalidArgument", err)
	}
	state, vm := stopStartVMState(t, hf.mg, failed)
	if state != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state after rejected fields = %s (%v)", state, vm)
	}
	after := stopStartManifest(t, hf.dataDir, failed)
	if after.Resources != before.Resources || after.Hostname != before.Hostname || after.StoppedAtUnixMs != before.StoppedAtUnixMs || len(after.Env) != len(before.Env) {
		t.Fatalf("manifest changed on a rejected start: %+v -> %+v", before, after)
	}
	hf.delete(failed)
	h.delete(id)
}

// TestCanceledCurrentOwnerReleasesItsClaimTicket claims a boot directly,
// cancels its owner before waitBoot, and enters waitBoot with the current
// ticket: the ticket must be returned (waiters reach zero, the shared boot
// context is canceled, the boot aborts to a settled STOPPED record) instead
// of lingering as a phantom waiter.
func TestCanceledCurrentOwnerReleasesItsClaimTicket(t *testing.T) {
	gate := newGatePing()
	h := newHarness(t, harnessOptions{wrapGuest: func(s guestv1.GuestServer) guestv1.GuestServer {
		gate.GuestServer = s
		return gate
	}})
	id := ids.Sandbox()
	h.create(id, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	v := stopStartVMRecord(t, h.mg, id)
	gate.arm()

	out, claimGen, err := h.mg.ownStart(v, &nodev1.StartVMRequest{SandboxId: id})
	if err != nil || out != nil {
		t.Fatalf("ownStart = %v, %d, %v", out, claimGen, err)
	}
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the boot never reached Ping")
	}
	if got := waiterCount(h, id); got != 1 {
		t.Fatalf("waiters after claim = %d, want the owner's 1", got)
	}

	ownerCtx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := h.mg.waitBoot(ownerCtx, v, claimGen); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled owner waitBoot = %v, want Canceled", err)
	}
	if got := waiterCount(h, id); got != 0 {
		t.Fatalf("waiters after the canceled owner = %d, want 0: the claim ticket must be released", got)
	}
	// The shared boot context was canceled by the last waiter leaving: the
	// gated boot aborts and settles to a stable STOPPED record with the
	// kept disk and stopped-inventory accounting.
	waitFor(t, "aborted boot to settle", func() bool {
		state, _ := stopStartVMState(t, h.mg, id)
		return state == nodev1.VMState_VM_STATE_STOPPED
	})
	if _, err := os.Stat(filepath.Join(h.dataDir, "vms", id)); err != nil {
		t.Fatalf("kept disk vanished: %v", err)
	}
	meta := stopStartManifest(t, h.dataDir, id)
	if !meta.Stopped() {
		t.Fatalf("manifest after the aborted boot = %+v", meta)
	}
	if got := stopStartReserved(t, h.mg); got.vcpus != 0 || got.diskGiB != defaultTestBaseGiB {
		t.Fatalf("reserved after the aborted boot = %+v", got)
	}
	gate.disarm()
	h.delete(id)
}

// stopStartStageWatcher installs a package-local process so the test owns one
// watch call and learns exactly when it returns. The launcher's real fake stays
// alive, so its original watcher cannot interfere with the staged interleaving.
func stopStartStageWatcher(t *testing.T, h *harness, id string) (*vm, *fakeProcess, <-chan struct{}) {
	t.Helper()
	v := stopStartVMRecord(t, h.mg, id)
	proc := newFakeProcess()
	v.mu.Lock()
	v.process = proc
	v.mu.Unlock()
	done := make(chan struct{})
	go func() {
		h.mg.watch(v, proc)
		close(done)
	}()
	return v, proc, done
}

// TestWatcherFailureAccountingIsAtomicWithStopAndStart parks an exited
// process's watcher after FAILED is persisted. A concurrent stop, and optionally
// a fresh start, must never interleave with that watcher's reservation release.
func TestWatcherFailureAccountingIsAtomicWithStopAndStart(t *testing.T) {
	for _, tc := range []struct {
		name    string
		restart bool
	}{
		{name: "stopped disk only"},
		{name: "restarted full reservation", restart: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{})
			id := ids.Sandbox()
			h.create(id, nil)
			v, proc, watcherDone := stopStartStageWatcher(t, h, id)

			persisted := make(chan struct{})
			releaseWatcher := make(chan struct{})
			h.mg.afterFailurePersist = func() {
				close(persisted)
				<-releaseWatcher
			}
			proc.exit(errors.New("staged process exit"))
			<-persisted

			// The old watcher parks with m.mu free; the fixed watcher keeps it
			// held. TryLock only identifies that boundary: when held continuously,
			// StopVM cannot even pass beginNodeOperation until the release below.
			oldWindow := h.mg.mu.TryLock()
			if oldWindow {
				h.mg.mu.Unlock()
			}
			stopCalled := make(chan struct{})
			stopDone := make(chan error, 1)
			go func() {
				close(stopCalled)
				_, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: id, GraceSeconds: 1})
				stopDone <- err
			}()
			<-stopCalled

			if oldWindow {
				// Fail-before ordering: the stop and optional restart complete
				// before the old watcher resumes its unlocked release.
				if err := <-stopDone; err != nil {
					t.Fatalf("StopVM in the old window: %v", err)
				}
				if tc.restart {
					if _, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id}); err != nil {
						t.Fatalf("StartVM in the old window: %v", err)
					}
				}
				close(releaseWatcher)
			} else {
				select {
				case err := <-stopDone:
					t.Fatalf("StopVM crossed the watcher's held manager lock: %v", err)
				default:
				}
				close(releaseWatcher)
				if err := <-stopDone; err != nil {
					t.Fatalf("StopVM after watcher release: %v", err)
				}
			}
			<-watcherDone
			if tc.restart && !oldWindow {
				if _, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: id}); err != nil {
					t.Fatalf("StartVM after watcher release: %v", err)
				}
			}

			wantState := nodev1.VMState_VM_STATE_STOPPED
			want := resources{diskGiB: defaultTestBaseGiB}
			if tc.restart {
				wantState = nodev1.VMState_VM_STATE_READY
				want = resources{vcpus: 2, memoryMib: 2048, diskGiB: defaultTestBaseGiB}
			}
			if state, current := stopStartVMState(t, h.mg, id); state != wantState {
				t.Fatalf("state = %s (%v), want %s", state, current, wantState)
			}
			if got := stopStartReserved(t, h.mg); got != want {
				t.Fatalf("reservation after stale watcher = %+v, want %+v", got, want)
			}
			h.mg.mu.Lock()
			capacityHeld, computeHeld := v.capacityHeld, v.computeHeld
			h.mg.mu.Unlock()
			if !capacityHeld || computeHeld != tc.restart {
				t.Fatalf("reservation flags = capacity %t, compute %t; want true, %t", capacityHeld, computeHeld, tc.restart)
			}
		})
	}
}

// TestWatcherSkipsReplacedRecord stages a watcher behind the old record lock,
// replaces the map entry and on-disk generation, then lets it run. Neither the
// old record nor the replacement's manifest and reservation may be disturbed.
func TestWatcherSkipsReplacedRecord(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	old, proc, watcherDone := stopStartStageWatcher(t, h, id)

	h.mg.mu.Lock()
	old.mu.Lock()
	delete(h.mg.vms, id)
	h.mg.releaseReservationLocked(old)
	h.mg.mu.Unlock()
	if err := h.mg.store.RemoveVM(id); err != nil {
		t.Fatalf("remove replaced generation: %v", err)
	}
	h.create(id, nil)
	proc.exit(errors.New("old process exited"))
	old.mu.Unlock()
	<-watcherDone

	old.mu.Lock()
	oldState, oldProcess := old.state, old.process
	old.mu.Unlock()
	if oldState != nodev1.VMState_VM_STATE_READY || oldProcess != proc {
		t.Fatalf("stale watcher changed replaced record: state %s, process preserved %t", oldState, oldProcess == proc)
	}
	if got := stopStartManifest(t, h.dataDir, id).LastError; got != "" {
		t.Fatalf("stale watcher wrote the replacement manifest: %q", got)
	}
	if state, current := stopStartVMState(t, h.mg, id); state != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("replacement state = %s (%v), want READY", state, current)
	}
	want := resources{vcpus: 2, memoryMib: 2048, diskGiB: defaultTestBaseGiB}
	if got := stopStartReserved(t, h.mg); got != want {
		t.Fatalf("replacement reservation = %+v, want %+v", got, want)
	}
}
