package node

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const (
	testImageID = "test-v0"
	// defaultTestBaseGiB keeps the fake suites inside a small tmpfs. The
	// env-gated conformance run uses a 10 GiB base because nodetest asks for
	// 10 GiB disks.
	defaultTestBaseGiB = 1
)

// fakeImager satisfies storage.Imager without qemu-img. It reports every file
// as a standalone qcow2 of the test base size and creates a placeholder
// overlay.
type fakeImager struct {
	diskGiB  uint64
	basePath string
	// createHook runs during CreateOverlay; tests use it to block the exact
	// disk-creation phase and return a subprocess-style cancellation error.
	createHook func(context.Context) error
	// growHook runs during Grow, blocking the growth phase of a StartVM the
	// same way.
	growHook func(context.Context) error
	mu       sync.Mutex
	calls    []string
}

func (f *fakeImager) CreateOverlay(ctx context.Context, dst storage.ImageRef, virtualSize uint64) error {
	backing, overlay := dst.BackingName, dst.File.Name()
	f.mu.Lock()
	f.calls = append(f.calls, "create "+backing+" "+overlay)
	hook := f.createHook
	f.mu.Unlock()
	if hook != nil {
		if err := hook(ctx); err != nil {
			return err
		}
	}
	return writeFakeDiskSize(overlay, virtualSize)
}

func writeFakeDiskSize(path string, size uint64) error {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], size)
	return os.WriteFile(path, data[:], 0o600)
}

func (f *fakeImager) Grow(ctx context.Context, ref storage.ImageRef, size uint64) error {
	file := ref.File.Name()
	f.mu.Lock()
	f.calls = append(f.calls, "grow "+file)
	hook := f.growHook
	f.mu.Unlock()
	if hook != nil {
		if err := hook(ctx); err != nil {
			return err
		}
	}
	return writeFakeDiskSize(file, size)
}

func (f *fakeImager) Info(_ context.Context, ref storage.ImageRef) (storage.QemuInfo, error) {
	file := ref.File.Name()
	info := storage.QemuInfo{Format: "qcow2", VirtualSize: f.diskGiB << 30}
	if filepath.Base(file) != "root.qcow2" {
		info.BackingFile = f.basePath
		data, err := os.ReadFile(file)
		if err == nil && len(data) == 8 {
			info.VirtualSize = binary.LittleEndian.Uint64(data)
		}
	}
	return info, nil
}

func (f *fakeImager) Check(context.Context, storage.ImageRef) error { return nil }

// writeTestImage builds a verified image directory the way images/build.sh
// does: four artifacts plus a manifest with their hashes.
func writeTestImage(t *testing.T, root, id string, diskGiB uint64) {
	t.Helper()
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{
		"kernel":        []byte("test kernel"),
		"initrd":        []byte("test initrd"),
		"root.qcow2":    []byte("test qcow2"),
		"packages.json": []byte("{}"),
	}
	artifacts := map[string]any{}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		artifacts[name] = map[string]any{
			"sha256": hex.EncodeToString(sum[:]),
			"size":   len(data),
		}
	}
	manifest := map[string]any{
		"schemaVersion": 1,
		"id":            id,
		"root": map[string]any{
			"filesystem":       "ext4",
			"format":           "qcow2",
			"diskGiB":          diskGiB,
			"virtualSizeBytes": diskGiB << 30,
		},
		"artifacts": artifacts,
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// countingFreezer never touches the host filesystem but records whether the
// guest service actually froze and thawed. A successful Exec alone cannot
// prove a thaw: the fake command runner keeps working while the state says
// frozen.
type countingFreezer struct {
	mu      sync.Mutex
	freezes int
	thaws   int
	frozen  bool
}

func (f *countingFreezer) Freeze() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.freezes++
	f.frozen = true
	return nil
}

func (f *countingFreezer) Thaw() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.thaws++
	f.frozen = false
	return nil
}

func (f *countingFreezer) state() (freezes, thaws int, frozen bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.freezes, f.thaws, f.frozen
}

// fakeProcess is one QEMU child. It reports the test process's pid so
// CommitRunning can read /proc, and Done closes when Stop is called or the
// test ends it explicitly.
type fakeProcess struct {
	pid     int
	done    chan struct{}
	once    sync.Once
	mu      sync.Mutex
	err     error
	stopErr error
	// stopGate stages Stop: when set, Stop signals stopEntered and waits for
	// the gate to close (or the test to finish) before returning.
	stopGate     chan struct{}
	stopEntered  chan struct{}
	onStop       func()
	pauseErr     error
	resumeErr    error
	resumeAlways error
	paused       bool
	pauses       int
	resumes      int
}

func newFakeProcess() *fakeProcess {
	return &fakeProcess{pid: os.Getpid(), done: make(chan struct{})}
}

func (p *fakeProcess) PID() int              { return p.pid }
func (p *fakeProcess) Done() <-chan struct{} { return p.done }
func (p *fakeProcess) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}
func (p *fakeProcess) Close() error { return nil }

// Pause marks the fake paused before returning any injected error, matching
// the ambiguous side effect of a lost QMP stop reply.
func (p *fakeProcess) Pause(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pauses++
	p.paused = true
	return p.pauseErr
}

func (p *fakeProcess) Resume(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resumes++
	if p.resumeAlways != nil {
		return p.resumeAlways
	}
	if p.resumeErr != nil {
		err := p.resumeErr
		p.resumeErr = nil
		return err
	}
	p.paused = false
	return nil
}

func (p *fakeProcess) setPauseResumeErrs(pauseErr, resumeErr error) {
	p.mu.Lock()
	p.pauseErr, p.resumeErr = pauseErr, resumeErr
	p.mu.Unlock()
}

func (p *fakeProcess) setResumeAlways(err error) {
	p.mu.Lock()
	p.resumeAlways = err
	p.mu.Unlock()
}

func (p *fakeProcess) pauseResumeCounts() (pauses, resumes int, paused bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pauses, p.resumes, p.paused
}

func (p *fakeProcess) exit(err error) {
	p.once.Do(func() {
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		close(p.done)
	})
}
func (p *fakeProcess) Stop(ctx context.Context) error {
	if p.onStop != nil {
		p.onStop()
	}
	p.mu.Lock()
	stopErr, gate, entered := p.stopErr, p.stopGate, p.stopEntered
	p.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
		}
	}
	if stopErr != nil {
		return stopErr
	}
	p.exit(nil)
	return nil
}

func (p *fakeProcess) setStopErr(err error) {
	p.mu.Lock()
	p.stopErr = err
	p.mu.Unlock()
}

func (p *fakeProcess) stageStop(gate chan struct{}, entered chan struct{}) {
	p.mu.Lock()
	p.stopGate = gate
	p.stopEntered = entered
	p.mu.Unlock()
}

type fakeLauncher struct {
	mu       sync.Mutex
	starts   int
	stops    int
	block    bool
	gate     chan struct{}
	fail     error
	failOnce error
	// failWith, together with fail, returns a non-nil process with the error:
	// the retained-handle contract of Launcher (absence unproven). The test
	// pre-loads the process's stopErr to decide whether the manager's cleanup
	// can prove the stop.
	failWith *fakeProcess
	stopErr  error
	onStart  func(qemu.Config)
	// newProcess overrides the default fake process, for tests that need a
	// real /proc identity (genuine argv and QMP socket).
	newProcess func(qemu.Config) (*fakeProcess, error)
	// wrapProcess decorates the fake process before it is handed to the
	// manager, for tests that add QMP control to it (StopVM's ACPI path).
	wrapProcess func(VMProcess) VMProcess
	processes   []*fakeProcess
}

func (l *fakeLauncher) Start(ctx context.Context, cfg qemu.Config, _ qemu.StartOptions) (VMProcess, error) {
	l.mu.Lock()
	l.starts++
	block, gate, fail, stopErr, onStart, newProcess := l.block, l.gate, l.fail, l.stopErr, l.onStart, l.newProcess
	failWith := l.failWith
	if l.failOnce != nil {
		fail = l.failOnce
		l.failOnce = nil
	}
	l.mu.Unlock()
	if onStart != nil {
		onStart(cfg)
	}
	if fail != nil {
		if failWith != nil {
			// The retained-handle failure shape: the launcher cannot prove
			// the child absent, so the live handle travels with the error.
			failWith.onStop = func() {
				l.mu.Lock()
				l.stops++
				l.mu.Unlock()
			}
			l.mu.Lock()
			l.processes = append(l.processes, failWith)
			wrap := l.wrapProcess
			l.mu.Unlock()
			if wrap != nil {
				return wrap(failWith), fail
			}
			return failWith, fail
		}
		return nil, fail
	}
	if block {
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
	var p *fakeProcess
	if newProcess != nil {
		var err error
		p, err = newProcess(cfg)
		if err != nil {
			return nil, err
		}
	} else {
		p = newFakeProcess()
	}
	p.stopErr = stopErr
	p.onStop = func() {
		l.mu.Lock()
		l.stops++
		l.mu.Unlock()
	}
	l.mu.Lock()
	wrap := l.wrapProcess
	l.processes = append(l.processes, p)
	l.mu.Unlock()
	if wrap != nil {
		return wrap(p), nil
	}
	return p, nil
}

func (l *fakeLauncher) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.starts
}

func (l *fakeLauncher) stopCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stops
}

func (l *fakeLauncher) setGate(gate chan struct{}) {
	l.mu.Lock()
	l.gate = gate
	l.mu.Unlock()
}

func (l *fakeLauncher) lastProcess() *fakeProcess {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.processes) == 0 {
		return nil
	}
	return l.processes[len(l.processes)-1]
}

func (l *fakeLauncher) setBlock(block bool) {
	l.mu.Lock()
	l.block = block
	l.mu.Unlock()
}

// killAll ends the fake QEMU processes the manager intentionally left
// running at shutdown (a real restart adopts them).
func (l *fakeLauncher) killAll() {
	l.mu.Lock()
	procs := slices.Clone(l.processes)
	l.mu.Unlock()
	for _, p := range procs {
		p.exit(errors.New("test cleanup"))
	}
}

// fakeGuestDialer serves one real internal/guest Service per vsock CID, on
// per-VM home and tmp roots. That exercises the production Exec/PutFile
// implementations without a VM.
type fakeGuestDialer struct {
	mu       sync.Mutex
	guest    map[uint32]guestv1.GuestServer
	fallback guestv1.GuestServer
	services []*guest.Service
	conns    []*grpc.ClientConn
	servers  []*grpc.Server
}

func newFakeGuestDialer(*testing.T) *fakeGuestDialer {
	return &fakeGuestDialer{guest: map[uint32]guestv1.GuestServer{}}
}

func (d *fakeGuestDialer) register(cid uint32, g guestv1.GuestServer) {
	d.mu.Lock()
	d.guest[cid] = g
	d.mu.Unlock()
}

func (d *fakeGuestDialer) Dial(ctx context.Context, cid uint32) (*grpc.ClientConn, error) {
	d.mu.Lock()
	g := d.guest[cid]
	if g == nil {
		g = d.fallback
	}
	d.mu.Unlock()
	if g == nil {
		return nil, fmt.Errorf("test: no guest for cid %d", cid)
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(8<<20), grpc.MaxSendMsgSize(nodelink.MaxResponseBytes))
	guestv1.RegisterGuestServer(srv, g)
	d.mu.Lock()
	d.servers = append(d.servers, srv)
	d.mu.Unlock()
	go func() { _ = srv.Serve(lis) }()
	// The harness must use the same guest response cap as the production
	// dialer, or large Phase 1 responses would stop at this test seam.
	conn, err := guestConn(func(ctx context.Context) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.conns = append(d.conns, conn)
	d.mu.Unlock()
	return conn, nil
}

// addService records a real guest service for shutdown.
func (d *fakeGuestDialer) addService(s *guest.Service) {
	d.mu.Lock()
	d.services = append(d.services, s)
	d.mu.Unlock()
}

func (d *fakeGuestDialer) stop(t *testing.T) {
	d.mu.Lock()
	servers := slices.Clone(d.servers)
	conns := slices.Clone(d.conns)
	services := slices.Clone(d.services)
	d.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
	for _, s := range servers {
		s.Stop()
	}
	for _, s := range services {
		if err := s.Shutdown(context.Background()); err != nil {
			t.Errorf("guest Shutdown: %v", err)
		}
	}
}

// harness wires a Manager to the fakes.
type harness struct {
	t        *testing.T
	mg       *Manager
	launcher *fakeLauncher
	dialer   *fakeGuestDialer
	freezer  *countingFreezer
	dataDir  string
	imageDir string
	// net is the fake VMNetwork the harness was built with, or nil in the
	// dev/test user-networking mode. A reopened manager (stopStartReopen)
	// reuses it so restart tests see the same durable network state.
	net VMNetwork
}

type harnessOptions struct {
	capacity    *nodev1.Resources
	baseGiB     uint64
	bootTimeout time.Duration
	launcher    *fakeLauncher
	prepare     func(t *testing.T, dataDir, imageDir string)
	findProcess func(meta storage.VMMeta) (*pinnedProc, error)
	verifyProc  func(ctx context.Context, pinned *pinnedProc, meta storage.VMMeta) (*qmp.Client, error)
	fallback    guestv1.GuestServer
	wrapGuest   func(guestv1.GuestServer) guestv1.GuestServer
	freeBytes   *uint64
	diskFree    func() (uint64, error)
	qemuExe     string
	// network injects a fake VMNetwork. nil keeps the explicit dev/test
	// user-networking mode (Config.AllowUserNetworking).
	network VMNetwork
	// configure runs after prepare and the defaults, just before New.
	configure func(*Config)
	// stubGuest skips the real guest service registration so fallback (a
	// scripted probe) serves the VM.
	stubGuest bool
}

func newHarness(t *testing.T, opts harnessOptions) *harness {
	t.Helper()
	baseGiB := opts.baseGiB
	if baseGiB == 0 {
		baseGiB = defaultTestBaseGiB
	}
	dataDir := t.TempDir()
	imageDir := t.TempDir()
	writeTestImage(t, imageDir, testImageID, baseGiB)
	capacity := &nodev1.Resources{Vcpus: 64, MemoryMib: 256 << 10, DiskGib: 1024}
	if opts.capacity != nil {
		capacity = opts.capacity
	}
	launcher := opts.launcher
	if launcher == nil {
		launcher = &fakeLauncher{}
	}
	dialer := newFakeGuestDialer(t)
	dialer.fallback = opts.fallback
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
		if opts.stubGuest {
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
		dialer.addService(svc)
		var registered guestv1.GuestServer = svc
		if opts.wrapGuest != nil {
			registered = opts.wrapGuest(registered)
		}
		dialer.register(cfg.VsockCID, registered)
	}
	if opts.prepare != nil {
		opts.prepare(t, dataDir, imageDir)
	}
	diskFree := opts.diskFree
	if diskFree == nil {
		// Unit tests must not depend on the physical filesystem. A terabyte
		// of injected free space exercises the reservation math.
		free := uint64(1) << 40
		if opts.freeBytes != nil {
			free = *opts.freeBytes
		}
		diskFree = func() (uint64, error) { return free, nil }
	}
	cfg := Config{
		NodeID:       "node-test",
		DataDir:      dataDir,
		ImageDir:     imageDir,
		DefaultImage: testImageID,
		QemuVersion:  "test",
		Capacity:     capacity,
		// Deterministic fake host CPU count: unit tests must not depend on the
		// CI machine's thread count. Individual tests override as needed.
		HostVCPUs: 64,
		MinCID:    3,
		MaxCID:    64,
		// Unit tests must not depend on the review host's own vsock CID.
		localCID:    func(context.Context) (uint32, error) { return 2, nil },
		BootTimeout: opts.bootTimeout,
		Launcher:    launcher,
		GuestDialer: dialer,
		Imager:      &fakeImager{diskGiB: baseGiB, basePath: filepath.Join(imageDir, testImageID, "root.qcow2")},
		// Explicit dev/test network mode: a test that does not inject a fake
		// VMNetwork runs QEMU's user-mode fallback, exactly what
		// Config.AllowUserNetworking names.
		Network:             opts.network,
		AllowUserNetworking: opts.network == nil,
		findProcess:         opts.findProcess,
		verifyProc:          opts.verifyProc,
		diskFree:            diskFree,
		qemuExe:             opts.qemuExe,
	}
	if opts.configure != nil {
		opts.configure(&cfg)
	}
	if cfg.Network != nil {
		// A configure hook that injects a network replaces the dev/test
		// user-networking fallback rather than conflicting with it.
		cfg.AllowUserNetworking = false
	}
	m, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := &harness{
		t:        t,
		mg:       m,
		launcher: launcher,
		dialer:   dialer,
		freezer:  freezer,
		dataDir:  dataDir,
		imageDir: imageDir,
		// The network New really ran with, so a reopened manager reconciles
		// what this manager actually owned (a configure hook may replace the
		// option).
		net: cfg.Network,
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := m.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		launcher.killAll()
		dialer.stop(t)
	})
	return h
}

// create boots one VM through the public RPC.
func (h *harness) create(id string, res *nodev1.Resources) *nodev1.VM {
	h.t.Helper()
	if res == nil {
		res = &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: defaultTestBaseGiB}
	}
	vm, err := h.mg.CreateVM(h.t.Context(), &nodev1.CreateVMRequest{SandboxId: id, Resources: res})
	if err != nil {
		h.t.Fatalf("CreateVM(%s): %v", id, err)
	}
	return vm
}

func (h *harness) delete(id string) {
	h.t.Helper()
	if _, err := h.mg.DeleteVM(h.t.Context(), &nodev1.DeleteVMRequest{SandboxId: id}); err != nil {
		h.t.Fatalf("DeleteVM(%s): %v", id, err)
	}
}

// helperSleep starts a real, long-lived process so recovery tests have a
// genuine pidfd and /proc identity to pin.
func helperSleep(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd
}

// fakeQMPServer is the smallest QMP peer that verifyProc tests can dial. It
// answers the handshake and the commands recovery uses. The first
// query-status reports "paused" until cont is executed.
type fakeQMPServer struct {
	path        string
	lis         net.Listener
	name        string
	mu          sync.Mutex
	cont        int
	contErr     error
	startPaused bool
	paused      bool
	powerdown   int
	// onPowerdown runs when system_powerdown arrives (under mu); StopVM
	// tests use it to emulate a guest honoring the ACPI request.
	onPowerdown func()
	// reboots lists every set-action reboot value in arrival order, and
	// resets counts system_reset commands; resetErr makes system_reset fail.
	// order records set-action and system_reset as they arrive, so a reset
	// test can assert their interleaving.
	reboots  []string
	resets   int
	resetErr error
	order    []string
	// onReset runs after a successful system_reset, outside mu; ResetVM tests
	// use it to emulate the guest machine restarting.
	onReset  func()
	quit     chan struct{}
	quitOnce sync.Once
	// p2 models the Phase 2 QMP surface: rootdisk's dirty bitmaps, the
	// prelaunch state of a paused launch and the schema (phase2_launch_test.go).
	p2 bitmapFakeState
}

func startFakeQMP(t *testing.T, name string) *fakeQMPServer {
	t.Helper()
	// Unix socket paths are limited to 107 bytes; a short /tmp directory
	// keeps the test independent of the (possibly long) TMPDIR.
	dir, err := os.MkdirTemp("/tmp", "bbraft-qmp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return startFakeQMPAt(t, filepath.Join(dir, qemu.QMPFilename), name)
}

func startFakeQMPAt(t *testing.T, path, name string) *fakeQMPServer {
	t.Helper()
	lis, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeQMPServer{path: path, lis: lis, name: name}
	t.Cleanup(func() { lis.Close() })
	go s.serve()
	return s
}

func (s *fakeQMPServer) serve() {
	for {
		conn, err := s.lis.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *fakeQMPServer) contCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cont
}

func (s *fakeQMPServer) powerdownCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.powerdown
}

func (s *fakeQMPServer) handle(conn net.Conn) {
	defer conn.Close()
	fmt.Fprintln(conn, `{"QMP":{"version":{"qemu":{"major":10,"minor":0,"micro":0},"package":"test"},"capabilities":[]}}`)
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var req struct {
			Execute   string         `json:"execute"`
			Arguments jsontext.Value `json:"arguments"`
			ID        uint64         `json:"id"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			return
		}
		// The reset surface is answered here, before the general lock, so the
		// command order stays exact and onReset can call back into the
		// manager without holding the fake's lock.
		switch req.Execute {
		case "set-action":
			var a struct {
				Reboot string `json:"reboot"`
			}
			if len(req.Arguments) > 0 {
				if err := json.Unmarshal(req.Arguments, &a); err != nil {
					writeQMPError(conn, req.ID, "bad set-action arguments: "+err.Error())
					return
				}
			}
			s.mu.Lock()
			s.reboots = append(s.reboots, a.Reboot)
			s.order = append(s.order, "set-action:"+a.Reboot)
			s.mu.Unlock()
			writeQMPResult(conn, req.ID, map[string]any{})
			continue
		case "system_reset":
			s.mu.Lock()
			resetErr, onReset := s.resetErr, s.onReset
			s.order = append(s.order, "system_reset")
			if resetErr == nil {
				s.resets++
			}
			s.mu.Unlock()
			if resetErr != nil {
				writeQMPError(conn, req.ID, resetErr.Error())
				continue
			}
			if onReset != nil {
				onReset()
			}
			writeQMPResult(conn, req.ID, map[string]any{})
			continue
		}
		var result any = map[string]any{}
		status := "running"
		s.mu.Lock()
		if s.paused || s.startPaused && s.cont == 0 {
			status = "paused"
		}
		if s.p2.prelaunch && s.cont == 0 {
			status = "prelaunch"
		}
		if r, qerr, ok := s.p2.handleLocked(req.Execute, req.Arguments); ok {
			s.mu.Unlock()
			msg := map[string]any{"return": r, "id": req.ID}
			if qerr != "" {
				msg = map[string]any{"error": map[string]any{"class": "GenericError", "desc": qerr}, "id": req.ID}
			}
			raw, _ := json.Marshal(msg)
			if _, err := conn.Write(append(raw, '\n')); err != nil {
				return
			}
			continue
		}
		switch req.Execute {
		case "stop":
			s.paused = true
			status = "paused"
		case "system_powerdown":
			s.powerdown++
			if s.onPowerdown != nil {
				s.onPowerdown()
			}
		case "cont":
			if s.contErr != nil {
				s.mu.Unlock()
				raw, _ := json.Marshal(map[string]any{
					"error": map[string]any{"class": "GenericError", "desc": s.contErr.Error()},
					"id":    req.ID,
				})
				conn.Write(append(raw, '\n'))
				return
			}
			s.cont++
			s.paused = false
			status = "running"
		}
		s.mu.Unlock()
		switch req.Execute {
		case "query-name":
			result = map[string]any{"name": s.name}
		case "query-status":
			result = map[string]any{"running": status == "running", "status": status}
		case "query-cpus-fast":
			result = []any{map[string]any{"cpu-index": 0, "thread-id": 4242, "qom-path": "/machine/unattached/device[0]"}}
		case "query-block":
			result = []any{map[string]any{
				"device":    "rootdisk",
				"io-status": "ok",
				"inserted":  map[string]any{"node-name": "rootdisk"},
			}}
		}
		raw, _ := json.Marshal(map[string]any{"return": result, "id": req.ID})
		if _, err := conn.Write(append(raw, '\n')); err != nil {
			return
		}
		if req.Execute == "quit" {
			s.quitOnce.Do(func() {
				if s.quit != nil {
					close(s.quit)
				}
			})
			return
		}
	}
}

// writeQMPResult answers one QMP request with a result.
func writeQMPResult(conn net.Conn, id uint64, result any) {
	raw, _ := json.Marshal(map[string]any{"return": result, "id": id})
	conn.Write(append(raw, '\n'))
}

// writeQMPError answers one QMP request with one GenericError.
func writeQMPError(conn net.Conn, id uint64, desc string) {
	raw, _ := json.Marshal(map[string]any{"error": map[string]any{"class": "GenericError", "desc": desc}, "id": id})
	conn.Write(append(raw, '\n'))
}

// resetCommands returns the set-action and system_reset commands in arrival
// order.
func (s *fakeQMPServer) resetCommands() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.order)
}

// resetCount returns how many system_reset commands succeeded.
func (s *fakeQMPServer) resetCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resets
}

// rebootActions returns every set-action reboot value in arrival order.
func (s *fakeQMPServer) rebootActions() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.reboots)
}

// gatedPingGuest blocks Ping until the test releases it, so the terminal
// READY transition can be raced against waiter cancellation.
type gatedPingGuest struct {
	guestv1.UnimplementedGuestServer
	entered chan struct{}
	release chan struct{}
}

func (g *gatedPingGuest) Ping(context.Context, *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	<-g.release
	return &guestv1.PingResponse{Version: "test"}, nil
}

// errorPingGuest never becomes ready.
type errorPingGuest struct {
	guestv1.UnimplementedGuestServer
}

func (errorPingGuest) Ping(context.Context, *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	return nil, status.Error(codes.Unavailable, "guest not ready")
}

// TestNodeHelperProcess is not a real test: spawnNodeHelper re-executes the
// test binary in this mode with a QEMU-shaped argv and a QMP listener, which
// lets recovery tests drive the real /proc and QMP code paths.
func TestNodeHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_NODE_HELPER") != "1" {
		return
	}
	path := os.Getenv("BBRAFT_HELPER_QMP")
	name := os.Getenv("BBRAFT_HELPER_NAME")
	lis, err := net.Listen("unix", path)
	if err != nil {
		os.Exit(3)
	}
	s := &fakeQMPServer{
		path: path, lis: lis, name: name,
		startPaused: os.Getenv("BBRAFT_HELPER_START_PAUSED") == "1",
		quit:        make(chan struct{}),
	}
	go s.serve()
	<-s.quit
	os.Exit(0)
}

// spawnNodeHelper starts a test-owned process whose exact executable and argv
// match a manager-owned QEMU, with a working QMP socket. It returns the
// process and the expected executable path.
func spawnNodeHelper(t *testing.T, id string, cfg qemu.Config) (*exec.Cmd, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		t.Fatal(err)
	}
	argv, err := helperLaunchArgs(cfg)
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
		if _, err := os.Stat(cfg.QMPPath()); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("helper never created %s", cfg.QMPPath())
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cmd, exe
}

// helperLaunchArgs mimics qemu.Start's one-time host CPU detection so helper
// argv can be built without a real qemu.Start call.
func helperLaunchArgs(cfg qemu.Config) ([]string, error) {
	if cfg.HostCPU == nil {
		host, err := qemu.DetectHostCPU("")
		if err != nil {
			host = qemu.HostCPU{}
		}
		cfg.HostCPU = &host
	}
	return cfg.Args()
}

// startRescueHelper starts a genuine QEMU-shaped subprocess with the exact
// executable/argv/QMP socket this VM would own. It returns errors instead of
// calling t.Fatal because launchers run from the boot goroutine.
func startRescueHelper(t *testing.T, cfg qemu.Config) (*fakeProcess, error) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		return nil, err
	}
	argv, err := helperLaunchArgs(cfg)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(exe, append([]string{"-test.run=TestNodeHelperProcess", "--"}, argv[1:]...)...)
	cmd.Env = append(os.Environ(),
		"GO_WANT_NODE_HELPER=1",
		"BBRAFT_HELPER_QMP="+cfg.QMPPath(),
		"BBRAFT_HELPER_NAME="+cfg.SandboxID,
	)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(cfg.QMPPath()); err == nil {
			break
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("rescue helper never created %s", cfg.QMPPath())
		}
		time.Sleep(10 * time.Millisecond)
	}
	p := newFakeProcess()
	p.pid = cmd.Process.Pid
	return p, nil
}

// startUnrelatedHelper starts a process with the recorded pid/start ticks and
// a live QMP socket but deliberately no owned QEMU argv. Its executable is the
// test binary, so only the exact argv/image/workdir/disk proof can reject it.
func startUnrelatedHelper(t *testing.T, cfg qemu.Config) (*fakeProcess, error) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(exe, "-test.run=TestNodeHelperProcess", "--")
	cmd.Env = append(os.Environ(),
		"GO_WANT_NODE_HELPER=1",
		"BBRAFT_HELPER_QMP="+cfg.QMPPath(),
		"BBRAFT_HELPER_NAME="+cfg.SandboxID,
		"BBRAFT_HELPER_START_PAUSED=1",
	)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(cfg.QMPPath()); err == nil {
			break
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("unrelated helper never created %s", cfg.QMPPath())
		}
		time.Sleep(10 * time.Millisecond)
	}
	p := newFakeProcess()
	p.pid = cmd.Process.Pid
	return p, nil
}

// pinnedHelper pins a helper process for recovery tests.
func pinnedHelper(t *testing.T) (*pinnedProc, *exec.Cmd) {
	t.Helper()
	cmd := helperSleep(t)
	pinned, err := pinProc(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pinned.close() })
	return pinned, cmd
}

// pidfdWait reports whether the pinned process disappears within the window.
func pidfdWait(pinned *pinnedProc, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if !pinned.alive() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return !pinned.alive()
}
