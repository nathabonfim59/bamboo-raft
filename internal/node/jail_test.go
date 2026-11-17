package node

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

// A10 jailed launch: fd map, limits, admitted CPU, the sole-Wait/settlement
// failure contract and the VM reclaim policy, against a fake jailer backed by
// real child processes.

const jailTestID = "bx_jjjjjjjj"

// jailQCOW returns a qcow2 v3 header naming backing (empty: standalone).
func jailQCOW(backing string) []byte {
	raw := make([]byte, 512)
	copy(raw, []byte{'Q', 'F', 'I', 0xfb})
	binary.BigEndian.PutUint32(raw[4:8], 3)
	if backing != "" {
		binary.BigEndian.PutUint64(raw[8:16], 256)
		binary.BigEndian.PutUint32(raw[16:20], uint32(len(backing)))
		copy(raw[256:], backing)
	}
	binary.BigEndian.PutUint32(raw[100:104], 104)
	return raw
}

// jailImager writes overlays with a real qcow2 header naming their backing.
type jailImager struct{}

func (jailImager) CreateOverlay(_ context.Context, dst storage.ImageRef, _ uint64) error {
	return os.WriteFile(dst.File.Name(), jailQCOW(dst.BackingName), 0o600)
}
func (jailImager) Grow(context.Context, storage.ImageRef, uint64) error { return nil }
func (jailImager) Info(_ context.Context, ref storage.ImageRef) (storage.QemuInfo, error) {
	return storage.QemuInfo{Format: "qcow2", VirtualSize: 1 << 30, BackingFile: ref.ExpectedBacking}, nil
}
func (jailImager) Check(context.Context, storage.ImageRef) error { return nil }

// jailWriteImage writes a verified image whose root.qcow2 is a standalone
// qcow2 header.
func jailWriteImage(t *testing.T, root, id string) {
	t.Helper()
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{"kernel": []byte("k"), "initrd": []byte("i"), "root.qcow2": jailQCOW(""), "packages.json": []byte("{}")}
	artifacts := map[string]any{}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o444); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		artifacts[name] = map[string]any{"sha256": hex.EncodeToString(sum[:]), "size": len(data)}
	}
	raw, err := json.Marshal(map[string]any{
		"schemaVersion": 1, "id": id,
		"root":      map[string]any{"filesystem": "ext4", "format": "qcow2", "diskGiB": 1, "virtualSizeBytes": 1 << 30},
		"artifacts": artifacts,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// fakeJailRun is one fake jailer child: a real sleep process, so Wait, Kill
// and death are genuine. reaped closes when the production owner's sole
// Wait returned: the fake jailer releases a record only after it.
type fakeJailRun struct {
	cmd    *exec.Cmd
	record jailer.Record
	jail   *fakeJail
	reaped chan struct{}

	mu       sync.Mutex
	waits    int
	detaches int
	releases int
	killNoop bool
}

func (r *fakeJailRun) PID() int { return r.cmd.Process.Pid }

func (r *fakeJailRun) Wait() error {
	r.mu.Lock()
	r.waits++
	r.mu.Unlock()
	err := r.cmd.Wait()
	close(r.reaped)
	return err
}

func (r *fakeJailRun) Identity() jailer.Record { return r.record }

func (r *fakeJailRun) Kill() error {
	r.mu.Lock()
	noop := r.killNoop
	r.mu.Unlock()
	if noop {
		return nil
	}
	if err := r.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}

func (r *fakeJailRun) Release(ctx context.Context) error {
	r.mu.Lock()
	r.releases++
	r.mu.Unlock()
	return r.jail.ReleaseAttempt(ctx, r.record.Ref())
}

func (r *fakeJailRun) Detach() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.detaches++
	return nil
}

func (r *fakeJailRun) counts() (waits, detaches, releases int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.waits, r.detaches, r.releases
}

func (r *fakeJailRun) isReaped() bool {
	select {
	case <-r.reaped:
		return true
	default:
		return false
	}
}

// joinReaped waits, bounded, for the production owner's Wait to complete.
func (r *fakeJailRun) joinReaped(t *testing.T) {
	t.Helper()
	select {
	case <-r.reaped:
	case <-time.After(10 * time.Second):
		t.Fatalf("child %d was not reaped by its sole Wait owner", r.PID())
	}
}

// fakeJail is a jailRunner with the jailer's attempt rules: Start refuses a
// retained id, ReleaseAttempt is exact-attempt and refuses a child its owner
// has not reaped yet.
type fakeJail struct {
	t       *testing.T
	mu      sync.Mutex
	records map[string]jailer.Record
	runOf   map[string]*fakeJailRun
	specs   []jailer.LaunchSpec
	runs    []*fakeJailRun
	// noRunErr fails Start before any spawn; retainOnNoRun keeps a record
	// anyway (an uncertain durable write).
	noRunErr      error
	retainOnNoRun bool
	// postErr is returned with a live run.
	postErr error
	// releaseErr fails releases while set.
	releaseErr error
	killNoop   bool
	// beforeStart runs before the retained check (to race a record in);
	// onStart runs once the child exists, before Start returns.
	beforeStart func(spec jailer.LaunchSpec)
	onStart     func(spec jailer.LaunchSpec)
	// onRelease is the release-entry witness: it runs at the start of every
	// ReleaseAttempt, before any decision, outside the fake's lock.
	onRelease func(ref jailer.Ref)
	verifyErr error
	// attempts counts ReleaseAttempt calls; releases counts successful ones.
	attempts int
	releases int
	// reclaim, when set, is the reclaim policy a release runs.
	reclaim func(context.Context, jailer.Record) error
	// probeSpecs records StartProbe calls; probeErr is returned with a live
	// probe child; onStartProbe runs once it exists.
	probeSpecs   []jailer.ProbeSpec
	probeErr     error
	onStartProbe func(spec jailer.ProbeSpec)
	// settleTools, when set, answers SettleTools; settled counts calls.
	settleTools func(ctx context.Context) (jailer.ToolReport, error)
	settled     int
}

// SettleTools answers through the settleTools hook (nothing to settle by
// default).
func (f *fakeJail) SettleTools(ctx context.Context) (jailer.ToolReport, error) {
	f.mu.Lock()
	f.settled++
	hook := f.settleTools
	f.mu.Unlock()
	if hook != nil {
		return hook(ctx)
	}
	return jailer.ToolReport{}, nil
}

// TargetHolder returns the record holding dev/ino, of any kind.
func (f *fakeJail) TargetHolder(dev, ino uint64) (jailer.Ref, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, rec := range f.records {
		if rec.TargetIno != 0 && rec.TargetDev == dev && rec.TargetIno == ino {
			return rec.Ref(), true
		}
	}
	return jailer.Ref{}, false
}

// StartProbe follows the jailer's probe rules with a real sleep child.
func (f *fakeJail) StartProbe(_ context.Context, spec jailer.ProbeSpec) (jailRun, error) {
	f.mu.Lock()
	f.probeSpecs = append(f.probeSpecs, spec)
	if _, ok := f.records[jailer.ProbeID]; ok {
		f.mu.Unlock()
		return nil, fmt.Errorf("%w: fake probe", jailer.ErrRetained)
	}
	control, err := identityFile(spec.ControlDir)
	if err != nil {
		f.mu.Unlock()
		return nil, err
	}
	serial, err := identityFile(spec.SerialLog)
	if err != nil {
		f.mu.Unlock()
		return nil, err
	}
	rec := jailer.Record{SchemaVersion: 2, SandboxID: jailer.ProbeID, Attempt: spec.Attempt, Kind: jailer.KindProbe, UID: uint32(os.Getuid()),
		ControlDev: control.Dev, ControlIno: control.Ino, LogDev: serial.Dev, LogIno: serial.Ino}
	if err := f.noRunErr; err != nil {
		if f.retainOnNoRun {
			f.records[jailer.ProbeID] = rec
		}
		f.mu.Unlock()
		return nil, err
	}
	probeErr, onStart, killNoop := f.probeErr, f.onStartProbe, f.killNoop
	f.mu.Unlock()
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		f.t.Fatal(err)
	}
	rec.PID = cmd.Process.Pid
	run := &fakeJailRun{cmd: cmd, record: rec, jail: f, killNoop: killNoop, reaped: make(chan struct{})}
	f.t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-run.reaped:
		case <-time.After(10 * time.Second):
			f.t.Errorf("probe child %d was never reaped by its sole Wait owner", cmd.Process.Pid)
		}
	})
	f.mu.Lock()
	f.records[jailer.ProbeID] = rec
	f.runOf[jailer.ProbeID] = run
	f.runs = append(f.runs, run)
	f.mu.Unlock()
	if onStart != nil {
		onStart(spec)
	}
	return run, probeErr
}

// identityFile returns f's inode identity.
func identityFile(f *os.File) (storage.Identity, error) {
	if f == nil {
		return storage.Identity{}, errors.New("nil file")
	}
	info, err := f.Stat()
	if err != nil {
		return storage.Identity{}, err
	}
	st := info.Sys().(*syscall.Stat_t)
	return storage.Identity{Dev: uint64(st.Dev), Ino: st.Ino}, nil
}

func newFakeJail(t *testing.T) *fakeJail {
	return &fakeJail{t: t, records: map[string]jailer.Record{}, runOf: map[string]*fakeJailRun{}}
}

func (f *fakeJail) Start(ctx context.Context, spec jailer.LaunchSpec) (jailRun, error) {
	f.mu.Lock()
	before := f.beforeStart
	f.mu.Unlock()
	if before != nil {
		before(spec)
	}
	f.mu.Lock()
	f.specs = append(f.specs, spec)
	if _, ok := f.records[spec.SandboxID]; ok {
		f.mu.Unlock()
		return nil, fmt.Errorf("%w: fake", jailer.ErrRetained)
	}
	rec := jailer.Record{SchemaVersion: 2, SandboxID: spec.SandboxID, Attempt: spec.Attempt, Kind: jailer.KindVM, UID: uint32(os.Getuid())}
	if err := f.noRunErr; err != nil {
		if f.retainOnNoRun {
			f.records[spec.SandboxID] = rec
		}
		f.mu.Unlock()
		return nil, err
	}
	postErr, onStart, killNoop := f.postErr, f.onStart, f.killNoop
	f.mu.Unlock()
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		f.t.Fatal(err)
	}
	rec.PID = cmd.Process.Pid
	run := &fakeJailRun{cmd: cmd, record: rec, jail: f, killNoop: killNoop, reaped: make(chan struct{})}
	// The production owner holds the only Wait: cleanup kills, then joins
	// that owner's completion, bounded, even after an early test failure.
	f.t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-run.reaped:
		case <-time.After(10 * time.Second):
			f.t.Errorf("child %d was never reaped by its sole Wait owner", cmd.Process.Pid)
		}
	})
	f.mu.Lock()
	f.records[spec.SandboxID] = rec
	f.runOf[spec.SandboxID] = run
	f.runs = append(f.runs, run)
	f.mu.Unlock()
	if onStart != nil {
		onStart(spec)
	}
	return run, postErr
}

func (f *fakeJail) Record(id string) (jailer.Record, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.records[id]
	return rec, ok
}

func (f *fakeJail) ReleaseAttempt(_ context.Context, ref jailer.Ref) error {
	f.mu.Lock()
	hook := f.onRelease
	f.attempts++
	f.mu.Unlock()
	if hook != nil {
		// The entry witness runs without the fake's lock, so it may block
		// and the test may inspect the fake meanwhile.
		hook(ref)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.records[ref.SandboxID]
	if !ok {
		return nil
	}
	if rec.Attempt != ref.Attempt {
		return fmt.Errorf("%w: fake", jailer.ErrSuperseded)
	}
	if run := f.runOf[ref.SandboxID]; run != nil && run.record.Attempt == ref.Attempt && !run.isReaped() {
		return fmt.Errorf("%w: fake child %d is not reaped", jailer.ErrAlive, run.PID())
	}
	if f.releaseErr != nil {
		return f.releaseErr
	}
	if f.reclaim != nil {
		// The jailer runs its reclaim policy after the proofs and before
		// removing the record.
		if err := f.reclaim(context.Background(), rec); err != nil {
			return err
		}
	}
	f.releases++
	delete(f.records, ref.SandboxID)
	delete(f.runOf, ref.SandboxID)
	return nil
}

func (f *fakeJail) VerifyProcess(_ context.Context, expected jailer.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.verifyErr != nil {
		return f.verifyErr
	}
	if rec, ok := f.records[expected.SandboxID]; !ok || rec != expected {
		return fmt.Errorf("%w: fake verify", jailer.ErrSuperseded)
	}
	return nil
}

func (f *fakeJail) Records() []jailer.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]jailer.Record, 0, len(f.records))
	for _, rec := range f.records {
		out = append(out, rec)
	}
	return out
}

func (f *fakeJail) lastRun() *fakeJailRun {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.runs) == 0 {
		return nil
	}
	return f.runs[len(f.runs)-1]
}

// jailFixture is a minimal Manager with a store holding one created VM
// whose overlay names the verified base, a fake jailer and a fake QMP dial.
type jailFixture struct {
	m    *Manager
	jail *fakeJail
	v    *vm
	img  storage.Image
	nic  *qemu.Network
	dial func(ctx context.Context, path string, pid int) (*qmp.Client, error)
}

func newJailFixture(t *testing.T) *jailFixture {
	t.Helper()
	return newJailFixtureWith(t, nil)
}

// newJailFixtureWith builds the fixture with syncFile as the store's file
// sync seam (nil: File.Sync).
func newJailFixtureWith(t *testing.T, syncFile func(*os.File) error) *jailFixture {
	t.Helper()
	// Short roots: the QMP socket path must fit sun_path (108 bytes).
	dataDir, imageDir := jailShortDir(t), jailShortDir(t)
	jailWriteImage(t, imageDir, testImageID)
	store, err := storage.Open(storage.Config{
		DataDir: dataDir, ImageDir: imageDir, Imager: jailImager{}, Log: slog.New(slog.DiscardHandler),
		Snapshots: storage.SnapshotOptions{SyncFile: syncFile},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	images, err := store.VerifyImages(t.Context())
	if err != nil || len(images) != 1 {
		t.Fatalf("VerifyImages = %v, %v", images, err)
	}
	meta := storage.VMMeta{SandboxID: jailTestID, Image: testImageID, Resources: storage.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 1}, VsockCID: 7, CreatedAtUnixMs: 1}
	if err := store.CreateVM(t.Context(), meta, images[0]); err != nil {
		t.Fatal(err)
	}
	meta, err = store.ReadVM(jailTestID)
	if err != nil {
		t.Fatal(err)
	}
	f := &jailFixture{jail: newFakeJail(t), img: images[0]}
	f.jail.onStart = func(spec jailer.LaunchSpec) {
		startFakeQMPAt(t, filepath.Join(store.VMDir(spec.SandboxID), storage.ControlDirName, qemu.QMPFilename), spec.SandboxID)
	}
	f.dial = func(ctx context.Context, path string, _ int) (*qmp.Client, error) {
		return qmp.Dial(ctx, path, slog.New(slog.DiscardHandler))
	}
	host := qemu.HostCPU{Vendor: "GenuineIntel", Vmx: true, CPUs: 8}
	f.m = &Manager{
		cfg: Config{
			NodeID: "node-test", Nested: qemu.NestedAuto, CPUFeatures: []string{"-vmx-apicv-vid"},
			JailBinary: "/usr/bin/qemu-system-x86_64", HostCPU: &host,
			JailIO: jailer.IOLimits{Device: jailer.BlockDevice{Major: 8, Minor: 0}, ReadBPS: 1 << 20},
		},
		log:   slog.New(slog.DiscardHandler),
		store: store,
		jail:  f.jail,
		jailDial: func(ctx context.Context, path string, pid int, _ <-chan struct{}) (*qmp.Client, error) {
			return f.dial(ctx, path, pid)
		},
		// /dev/full stands in for /dev/vhost-vsock, which tests cannot open.
		jailVsock:   func() (*os.File, error) { return os.OpenFile("/dev/full", os.O_RDWR, 0) },
		jailCleanup: 300 * time.Millisecond,
		ctx:         t.Context(),
		vms:         map[string]*vm{},
	}
	f.v = &vm{id: jailTestID, meta: meta, bootDone: make(chan struct{})}
	f.m.vms[jailTestID] = f.v
	tap, vhost := jailPipeFile(t), jailPipeFile(t)
	f.nic = &qemu.Network{
		GuestIP: netip.MustParseAddr("10.42.0.2"), PrefixLen: 16, Gateway: netip.MustParseAddr("10.42.0.1"),
		MAC: "52:54:00:00:00:02", Resolvers: []netip.Addr{netip.MustParseAddr("1.1.1.1")}, Tap: tap, Vhost: vhost,
	}
	return f
}

// jailShortDir returns a test-owned directory directly under /tmp,
// independent of TMPDIR: fixture QMP sockets such as
// <dir>/vms/<id>/control/qmp.sock must fit sun_path (107 bytes plus NUL)
// whatever the runner's TMPDIR or test name.
func jailShortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "bj")
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

// TestJailShortDirIgnoresLongTMPDIR: with a TMPDIR long enough to overflow
// sun_path, the jailed fixtures' control socket still fits.
func TestJailShortDirIgnoresLongTMPDIR(t *testing.T) {
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
	socket := func(dir string) string {
		return filepath.Join(dir, "vms", jailTestID, storage.ControlDirName, qemu.QMPFilename)
	}
	if len(socket(control)) <= 107 {
		t.Fatal("the long TMPDIR control does not overflow sun_path")
	}
	dir := jailShortDir(t)
	if !strings.HasPrefix(dir, "/tmp/bj") {
		t.Fatalf("fixture dir %s follows TMPDIR", dir)
	}
	if n := len(socket(dir)); n > 107 {
		t.Fatalf("fixture control socket path is %d bytes", n)
	}
}

func jailPipeFile(t *testing.T) *os.File {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	t.Cleanup(func() { r.Close() })
	return r
}

func (f *jailFixture) intent(t *testing.T) *storage.QEMURecord {
	t.Helper()
	meta, err := f.m.store.ReadVM(jailTestID)
	if err != nil {
		t.Fatal(err)
	}
	return meta.QEMU
}

func (f *jailFixture) ref() jailer.Ref {
	f.v.mu.Lock()
	defer f.v.mu.Unlock()
	return f.v.jail
}

// jailOpenFDs counts this process's open descriptors.
func jailOpenFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

// jailClosed reports whether every node-owned launch descriptor in spec
// (all but the borrowed TAP/vhost lease) is closed.
func jailClosed(spec jailer.LaunchSpec) error {
	files := []*os.File{spec.ControlDir, spec.SerialLog}
	for _, d := range spec.Files {
		if d.Name != jailer.TAPName && d.Name != jailer.VhostNetName {
			files = append(files, d.File)
		}
	}
	for _, f := range files {
		if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
			return fmt.Errorf("%s is still open (%v)", f.Name(), err)
		}
	}
	return nil
}

func TestJailLimits(t *testing.T) {
	io := jailer.IOLimits{Device: jailer.BlockDevice{Major: 8}, WriteBPS: 1}
	tests := []struct {
		res     storage.Resources
		want    jailer.Limits
		wantErr bool
	}{
		{storage.Resources{VCPUs: 1, MemoryMiB: 512}, jailer.Limits{CPUQuotaPercent: 100, MemoryBytes: (512 + 256) << 20, Pids: 258, IO: io}, false},
		{storage.Resources{VCPUs: 4, MemoryMiB: 8192}, jailer.Limits{CPUQuotaPercent: 400, MemoryBytes: (8192 + 1024) << 20, Pids: 264, IO: io}, false},
		{storage.Resources{VCPUs: 20000, MemoryMiB: 512}, jailer.Limits{}, true},
		{storage.Resources{VCPUs: 1, MemoryMiB: 1 << 50}, jailer.Limits{}, true},
		{storage.Resources{VCPUs: 0, MemoryMiB: 512}, jailer.Limits{}, true},
	}
	for _, tt := range tests {
		got, err := jailLimits(tt.res, io)
		if (err != nil) != tt.wantErr || (!tt.wantErr && got != tt.want) {
			t.Fatalf("jailLimits(%+v) = %+v, %v; want %+v, err %v", tt.res, got, err, tt.want, tt.wantErr)
		}
	}
}

// jailArgvHelperEnv selects the argv helper in a re-executed test binary.
const jailArgvHelperEnv = "BBRAFT_NODE_ARGV_HELPER"

// jailArgvHelper is the argv helper TestMain runs when the env is set: it
// reports ready on fd 3 once running its final image, then blocks on stdin.
// Its process argv is exactly what the parent exec'd, with no test flags.
func jailArgvHelper() {
	ready := os.NewFile(3, "ready")
	_, _ = ready.Write([]byte("ready\n"))
	ready.Close()
	_, _ = io.Copy(io.Discard, os.Stdin)
}

// TestArgvDigestMatchesProcCmdline: the intent digest equals the sha256 of
// a live process's actual /proc cmdline, including empty and space
// arguments, read after the process reports it is running its final image.
func TestArgvDigestMatchesProcCmdline(t *testing.T) {
	argv := []string{os.Args[0], "with space", "", "last"}
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
	// The sole reaper is registered before any assertion can fail.
	reaped := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(reaped)
	}()
	t.Cleanup(func() {
		stdin.Close()
		select {
		case <-reaped:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			select {
			case <-reaped:
			case <-time.After(10 * time.Second):
				t.Errorf("argv helper %d was not reaped", cmd.Process.Pid)
			}
		}
	})
	if err := readyR.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line := make([]byte, 6)
	if _, err := io.ReadFull(readyR, line); err != nil || string(line) != "ready\n" {
		t.Fatalf("helper ready = %q, %v", line, err)
	}
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(cmd.Process.Pid) + "/cmdline")
	if err != nil {
		t.Fatal(err)
	}
	var want []byte
	for _, arg := range argv {
		want = append(append(want, arg...), 0)
	}
	if string(raw) != string(want) {
		t.Fatalf("cmdline = %q, want %q", raw, want)
	}
	sum := sha256.Sum256(raw)
	if got := argvDigest(argv); got != hex.EncodeToString(sum[:]) {
		t.Fatalf("argvDigest = %s, sha256(cmdline) = %x", got, sum)
	}
}

// TestBuildJailSpecAcceptedByJailer: the production argv and descriptor map
// pass the jailer's own validator, in the fixed fd order.
func TestBuildJailSpecAcceptedByJailer(t *testing.T) {
	f := newJailFixture(t)
	pins, err := f.m.store.PinLaunch(t.Context(), jailTestID, f.img)
	if err != nil {
		t.Fatal(err)
	}
	defer pins.Close()
	// Character devices stand in for the TAP, vhost-net and vhost-vsock
	// descriptors (the validator checks the file type only).
	nic := *f.nic
	nic.Tap, nic.Vhost = jailOpenDev(t, "/dev/null"), jailOpenDev(t, "/dev/zero")
	args, err := f.m.jailLaunchConfig(f.v.meta, &nic).JailedArgs()
	if err != nil {
		t.Fatal(err)
	}
	limits, err := jailLimits(f.v.meta.Resources, f.m.cfg.JailIO)
	if err != nil {
		t.Fatal(err)
	}
	spec := buildJailSpec(jailTestID, uuid.NewV7().String(), f.m.cfg.JailBinary, args, &nic, pins, jailOpenDev(t, "/dev/full"), limits, io.Discard)
	var names []string
	for _, d := range spec.Files {
		names = append(names, d.Name)
	}
	if want := []string{jailer.TAPName, jailer.VhostNetName, jailer.KernelName, jailer.InitrdName, jailer.VhostVsockName, "base", "overlay"}; !slices.Equal(names, want) {
		t.Fatalf("descriptor order = %v, want %v", names, want)
	}
	if !slices.Equal(spec.Disks, []string{"base", "overlay"}) {
		t.Fatalf("disk chain = %v", spec.Disks)
	}
	if err := spec.Validate(); err != nil {
		t.Fatalf("jailer refused the production spec: %v", err)
	}
}

func jailOpenDev(t *testing.T, name string) *os.File {
	t.Helper()
	f, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// TestJailLaunchConfigCarriesAdmittedCPU: the jailed launch uses exactly the
// admitted host facts and feature toggles, so nested auto keeps the probed
// option instead of failing or silently dropping +vmx.
func TestJailLaunchConfigCarriesAdmittedCPU(t *testing.T) {
	f := newJailFixture(t)
	args, err := f.m.jailLaunchConfig(f.v.meta, f.nic).JailedArgs()
	if err != nil {
		t.Fatal(err)
	}
	i := slices.Index(args, "-cpu")
	if i < 0 || args[i+1] != "Haswell-noTSX,+vmx,-vmx-apicv-vid,enforce" {
		t.Fatalf("-cpu = %q", args[i+1:i+2])
	}
	f.m.cfg.HostCPU = nil
	if _, err := f.m.jailLaunchConfig(f.v.meta, f.nic).JailedArgs(); err == nil {
		t.Fatal("a jailed launch without admitted host facts re-resolved the CPU")
	}
}

// TestLaunchJailedSuccess is the positive control: inside Start the intent
// names the attempt and the VM already claims it; the sole Wait completes
// only after the child dies; every node-owned descriptor is closed.
func TestLaunchJailedSuccess(t *testing.T) {
	f := newJailFixture(t)
	qmpStart := f.jail.onStart
	var insideErr error
	f.jail.onStart = func(spec jailer.LaunchSpec) {
		intent := f.intent(t)
		want := jailer.Ref{SandboxID: jailTestID, Attempt: spec.Attempt}
		switch {
		case intent == nil || intent.State != storage.QEMULaunching || intent.JailAttempt != spec.Attempt:
			insideErr = fmt.Errorf("intent inside Start = %+v, want attempt %s", intent, spec.Attempt)
		case intent.ArgvSHA256 != argvDigest(append([]string{f.m.cfg.JailBinary}, spec.Args...)):
			insideErr = errors.New("intent digest does not match the launched argv")
		case f.ref() != want:
			insideErr = fmt.Errorf("v.jail inside Start = %+v, want %+v", f.ref(), want)
		}
		qmpStart(spec)
	}
	proc, err := f.m.launchJailed(t.Context(), f.v, f.img, f.nic)
	if err != nil {
		t.Fatal(err)
	}
	if insideErr != nil {
		t.Fatal(insideErr)
	}
	run := f.jail.lastRun()
	if err := jailClosed(f.jail.specs[0]); err != nil {
		t.Fatalf("after a successful Start: %v", err)
	}
	if proc.PID() != run.PID() || proc.(*jailedProcess).QMP() == nil {
		t.Fatal("process does not expose the run's pid and a QMP client")
	}
	if run.isReaped() || procDone(proc) {
		t.Fatal("reaped or Done while the child runs")
	}
	if err := f.jail.ReleaseAttempt(t.Context(), f.ref()); !errors.Is(err, jailer.ErrAlive) {
		t.Fatalf("release before the reap = %v, want ErrAlive", err)
	}
	_ = run.cmd.Process.Kill()
	run.joinReaped(t)
	select {
	case <-proc.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Done did not close after the reap")
	}
	if waits, _, _ := run.counts(); waits != 1 {
		t.Fatalf("Wait called %d times, want exactly 1", waits)
	}
	if proc.Err() == nil {
		t.Fatal("a killed QEMU reports no exit error")
	}
	if err := f.m.settleJail(t.Context(), f.v, proc); err != nil || f.ref() != (jailer.Ref{}) {
		t.Fatalf("settle after the reap = %v, ref %+v", err, f.ref())
	}
}

func TestLaunchJailedRefusesConflictingRetainedRecord(t *testing.T) {
	f := newJailFixture(t)
	foreign := jailer.Record{SandboxID: jailTestID, Attempt: uuid.NewV7().String()}
	f.jail.records[jailTestID] = foreign
	fds := jailOpenFDs(t)
	proc, err := f.m.launchJailed(t.Context(), f.v, f.img, f.nic)
	if proc != nil || !errors.Is(err, errJailConflict) {
		t.Fatalf("launchJailed = %v, %v; want errJailConflict", proc, err)
	}
	if len(f.jail.specs) != 0 || f.intent(t) != nil || f.ref() != (jailer.Ref{}) {
		t.Fatal("a conflicting record still started, wrote an intent or claimed the attempt")
	}
	if rec, _ := f.jail.Record(jailTestID); rec != foreign {
		t.Fatal("the foreign record was touched")
	}
	if got := jailOpenFDs(t); got != fds {
		t.Fatalf("open descriptors %d, want %d", got, fds)
	}
	if err := f.m.settleJail(t.Context(), f.v, nil); !errors.Is(err, errJailConflict) {
		t.Fatalf("settle of an unclaimed foreign record = %v, want errJailConflict", err)
	}
}

// TestLaunchJailedRetainedAtStartStaysUnclaimed: a record that appears
// between the absence check and Start makes Start return ErrRetained; our
// attempt never existed, so the foreign reservation is not claimed.
func TestLaunchJailedRetainedAtStartStaysUnclaimed(t *testing.T) {
	f := newJailFixture(t)
	foreign := jailer.Record{SandboxID: jailTestID, Attempt: uuid.NewV7().String()}
	f.jail.beforeStart = func(jailer.LaunchSpec) {
		f.jail.mu.Lock()
		f.jail.records[jailTestID] = foreign
		f.jail.mu.Unlock()
	}
	proc, err := f.m.launchJailed(t.Context(), f.v, f.img, f.nic)
	if proc != nil || !errors.Is(err, jailer.ErrRetained) {
		t.Fatalf("launchJailed = %v, %v; want ErrRetained", proc, err)
	}
	if f.ref() != (jailer.Ref{}) {
		t.Fatalf("v.jail claimed %+v", f.ref())
	}
	if err := jailClosed(f.jail.specs[0]); err != nil {
		t.Fatal(err)
	}
	if err := f.m.settleJail(t.Context(), f.v, nil); !errors.Is(err, errJailConflict) {
		t.Fatalf("settle = %v, want errJailConflict", err)
	}
	if rec, _ := f.jail.Record(jailTestID); rec != foreign {
		t.Fatal("the foreign record was released")
	}
}

// TestLaunchJailedNoSpawnRetainedRetry: Start failed before any spawn but
// kept a record (an uncertain durable write); the VM names the attempt and a
// settlement by identity releases it.
func TestLaunchJailedNoSpawnRetainedRetry(t *testing.T) {
	f := newJailFixture(t)
	f.jail.noRunErr = errors.New("injected pre-spawn failure")
	f.jail.retainOnNoRun = true
	proc, err := f.m.launchJailed(t.Context(), f.v, f.img, f.nic)
	if proc != nil || !errors.Is(err, f.jail.noRunErr) {
		t.Fatalf("launchJailed = %v, %v", proc, err)
	}
	spec := f.jail.specs[0]
	if err := jailClosed(spec); err != nil {
		t.Fatal(err)
	}
	ref := jailer.Ref{SandboxID: jailTestID, Attempt: spec.Attempt}
	if f.ref() != ref {
		t.Fatalf("v.jail = %+v, want %+v", f.ref(), ref)
	}
	f.jail.releaseErr = errors.New("injected release failure")
	if err := f.m.settleJail(t.Context(), f.v, nil); err == nil || f.ref() != ref {
		t.Fatalf("failed settle = %v, ref %+v", err, f.ref())
	}
	f.jail.releaseErr = nil
	if err := f.m.settleJail(t.Context(), f.v, nil); err != nil || f.ref() != (jailer.Ref{}) {
		t.Fatalf("retry = %v, ref %+v", err, f.ref())
	}
	if _, ok := f.jail.Record(jailTestID); ok {
		t.Fatal("the retained record survived its settlement")
	}
}

// TestLaunchJailedPreStartErrorsCloseEverything: each pre-Start failure
// leaves no descriptor, no intent and no claimed attempt.
func TestLaunchJailedPreStartErrorsCloseEverything(t *testing.T) {
	for name, setup := range map[string]func(f *jailFixture){
		"vsock open": func(f *jailFixture) {
			f.m.jailVsock = func() (*os.File, error) { return nil, errors.New("injected vsock failure") }
		},
		"unverified overlay": func(f *jailFixture) {
			if err := os.WriteFile(f.m.store.DiskPath(jailTestID), jailQCOW("/etc/shadow"), 0o600); err != nil {
				panic(err)
			}
		},
		"limits": func(f *jailFixture) { f.v.meta.Resources.VCPUs = 20000 },
		"canceled": func(f *jailFixture) {
			ctx, cancel := context.WithCancel(f.m.ctx)
			cancel()
			f.m.ctx = ctx
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newJailFixture(t)
			setup(f)
			fds := jailOpenFDs(t)
			proc, err := f.m.launchJailed(f.m.ctx, f.v, f.img, f.nic)
			if proc != nil || err == nil {
				t.Fatalf("launchJailed = %v, %v", proc, err)
			}
			if got := jailOpenFDs(t); got != fds {
				t.Fatalf("open descriptors %d, want %d", got, fds)
			}
			if len(f.jail.specs) != 0 || f.intent(t) != nil || f.ref() != (jailer.Ref{}) {
				t.Fatal("a pre-Start failure started, wrote an intent or claimed an attempt")
			}
		})
	}
}

// TestLaunchJailedPostSpawnFailures: a post-spawn error returns nil only
// when death AND the jail release are proven; otherwise the live or
// unreleased handle travels with the error.
func TestLaunchJailedPostSpawnFailures(t *testing.T) {
	cases := []struct {
		name       string
		setup      func(f *jailFixture)
		wantProc   bool
		wantDone   bool
		wantDetach int
		wantRef    bool
	}{
		{"death and release proven", func(f *jailFixture) {
			f.jail.postErr = errors.New("injected exec proof failure")
		}, false, true, 0, false},
		{"release fails", func(f *jailFixture) {
			f.jail.postErr = errors.New("injected exec proof failure")
			f.jail.releaseErr = errors.New("injected release failure")
		}, true, true, 1, true},
		{"death unproven", func(f *jailFixture) {
			f.jail.postErr = errors.New("injected exec proof failure")
			f.jail.killNoop = true
		}, true, false, 0, true},
		{"qmp peer refused", func(f *jailFixture) {
			f.dial = func(context.Context, string, int) (*qmp.Client, error) {
				return nil, errors.New("qmp peer is another pid")
			}
		}, false, true, 0, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			f := newJailFixture(t)
			tt.setup(f)
			proc, err := f.m.launchJailed(t.Context(), f.v, f.img, f.nic)
			if err == nil {
				t.Fatal("launchJailed succeeded")
			}
			run := f.jail.lastRun()
			if run == nil {
				t.Fatalf("no child was started (err %v)", err)
			}
			if err := jailClosed(f.jail.specs[0]); err != nil {
				t.Fatalf("after a failed Start: %v", err)
			}
			if (proc != nil) != tt.wantProc {
				t.Fatalf("process = %v, want non-nil %v (err %v)", proc, tt.wantProc, err)
			}
			if got := run.isReaped(); got != tt.wantDone {
				t.Fatalf("reaped on return = %v, want %v", got, tt.wantDone)
			}
			if proc != nil && procDone(proc) != tt.wantDone {
				t.Fatalf("Done on return = %v, want %v", procDone(proc), tt.wantDone)
			}
			_ = run.cmd.Process.Kill()
			run.joinReaped(t)
			if waits, detaches, _ := run.counts(); waits != 1 || detaches != tt.wantDetach {
				t.Fatalf("waits %d detaches %d, want 1 and %d", waits, detaches, tt.wantDetach)
			}
			if got := f.ref() != (jailer.Ref{}); got != tt.wantRef {
				t.Fatalf("v.jail retained = %v, want %v", got, tt.wantRef)
			}
		})
	}
}

func TestSettleJail(t *testing.T) {
	t.Run("absent and unclaimed", func(t *testing.T) {
		f := newJailFixture(t)
		if err := f.m.settleJail(t.Context(), f.v, nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("superseded attempt is a conflict", func(t *testing.T) {
		f := newJailFixture(t)
		ref := jailer.Ref{SandboxID: jailTestID, Attempt: uuid.NewV7().String()}
		f.v.jail = ref
		newer := jailer.Record{SandboxID: jailTestID, Attempt: uuid.NewV7().String()}
		f.jail.records[jailTestID] = newer
		if err := f.m.settleJail(t.Context(), f.v, nil); !errors.Is(err, jailer.ErrSuperseded) {
			t.Fatalf("settleJail = %v, want ErrSuperseded", err)
		}
		if f.ref() != ref {
			t.Fatal("a conflict cleared the VM's attempt")
		}
		if rec, _ := f.jail.Record(jailTestID); rec != newer {
			t.Fatal("the newer record was released")
		}
	})
	t.Run("canceled context changes nothing", func(t *testing.T) {
		f := newJailFixture(t)
		ref := jailer.Ref{SandboxID: jailTestID, Attempt: uuid.NewV7().String()}
		f.v.jail = ref
		f.jail.records[jailTestID] = jailer.Record{SandboxID: jailTestID, Attempt: ref.Attempt}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := f.m.settleJail(ctx, f.v, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("settleJail = %v, want Canceled", err)
		}
		if _, ok := f.jail.Record(jailTestID); !ok || f.ref() != ref || f.jail.releases != 0 {
			t.Fatal("a canceled settlement released something")
		}
	})
	t.Run("release before the reap is refused", func(t *testing.T) {
		f := newJailFixture(t)
		proc, err := f.m.launchJailed(t.Context(), f.v, f.img, f.nic)
		if err != nil {
			t.Fatal(err)
		}
		ref := f.ref()
		if err := f.m.settleJail(t.Context(), f.v, proc); err == nil || f.ref() != ref {
			t.Fatalf("settle of a live jailed process = %v", err)
		}
		run := f.jail.lastRun()
		if _, _, releases := run.counts(); releases != 0 {
			t.Fatal("a live process was released")
		}
		_ = run.cmd.Process.Kill()
		run.joinReaped(t)
		<-proc.Done()
		if err := f.m.settleJail(t.Context(), f.v, proc); err != nil {
			t.Fatal(err)
		}
	})
}

// TestJailedProcessShutdownDetaches: owner shutdown closes only the handle.
func TestJailedProcessShutdownDetaches(t *testing.T) {
	f := newJailFixture(t)
	proc, err := f.m.launchJailed(t.Context(), f.v, f.img, f.nic)
	if err != nil {
		t.Fatal(err)
	}
	rel, ok := proc.(interface{ Release() error })
	if !ok {
		t.Fatal("jailed process has no owner-shutdown Release")
	}
	if err := rel.Release(); err != nil {
		t.Fatal(err)
	}
	run := f.jail.lastRun()
	if _, detaches, releases := run.counts(); detaches != 1 || releases != 0 {
		t.Fatalf("shutdown detaches %d releases %d, want 1 and 0", detaches, releases)
	}
	if _, ok := f.jail.Record(jailTestID); !ok {
		t.Fatal("shutdown released the reservation")
	}
}

// jailReclaimFixture pins the layout once and returns the record a jailed
// attempt would hold for it.
func jailReclaimFixture(t *testing.T) (*jailFixture, jailer.Record) {
	t.Helper()
	f := newJailFixture(t)
	pins, err := f.m.store.PinLaunch(t.Context(), jailTestID, f.img)
	if err != nil {
		t.Fatal(err)
	}
	defer pins.Close()
	id := func(file *os.File) (uint64, uint64) {
		info, err := file.Stat()
		if err != nil {
			t.Fatal(err)
		}
		st := info.Sys().(*syscall.Stat_t)
		return uint64(st.Dev), st.Ino
	}
	rec := jailer.Record{SandboxID: jailTestID, Attempt: uuid.NewV7().String(), Kind: jailer.KindVM, UID: uint32(os.Getuid())}
	rec.ControlDev, rec.ControlIno = id(pins.Control)
	rec.LogDev, rec.LogIno = id(pins.Console)
	rec.TargetDev, rec.TargetIno = id(pins.Overlay)
	return f, rec
}

func TestReclaimVM(t *testing.T) {
	control := func(f *jailFixture) string {
		return filepath.Join(f.m.store.VMDir(jailTestID), storage.ControlDirName)
	}
	console := func(f *jailFixture) string {
		return filepath.Join(f.m.store.VMDir(jailTestID), storage.ConsoleLogName)
	}
	t.Run("removes the attempt's socket and restores ownership", func(t *testing.T) {
		f, rec := jailReclaimFixture(t)
		sock := filepath.Join(control(f), qemu.QMPFilename)
		if err := syscall.Mknod(sock, syscall.S_IFSOCK|0o600, 0); err != nil {
			t.Fatal(err)
		}
		jailLoosen(t, control(f), console(f), f.m.store.DiskPath(jailTestID))
		if err := jailNodeReclaimer(t, f, nil).Reclaim(t.Context(), rec); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(sock); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("socket survived: %v", err)
		}
		for p, mode := range map[string]os.FileMode{control(f): 0o700, console(f): 0o600, f.m.store.DiskPath(jailTestID): 0o600} {
			info, err := os.Lstat(p)
			if err != nil || info.Mode().Perm() != mode {
				t.Fatalf("%s mode %v, want %v (%v)", p, info.Mode().Perm(), mode, err)
			}
		}
		if err := jailNodeReclaimer(t, f, nil).Reclaim(t.Context(), rec); err != nil {
			t.Fatalf("reclaim is not idempotent: %v", err)
		}
	})
	// Each refusal leaves every file exactly as it was.
	refusals := map[string]func(t *testing.T, f *jailFixture, rec *jailer.Record){
		"foreign control entry": func(t *testing.T, f *jailFixture, _ *jailer.Record) {
			if err := os.WriteFile(filepath.Join(control(f), "foreign"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"socket of another uid": func(t *testing.T, f *jailFixture, rec *jailer.Record) {
			rec.UID++
			if err := syscall.Mknod(filepath.Join(control(f), qemu.QMPFilename), syscall.S_IFSOCK|0o600, 0); err != nil {
				t.Fatal(err)
			}
		},
		"replaced overlay": func(t *testing.T, f *jailFixture, _ *jailer.Record) {
			disk := f.m.store.DiskPath(jailTestID)
			if err := os.Rename(disk, disk+".moved"); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(disk, []byte("replacement"), 0o640); err != nil {
				t.Fatal(err)
			}
		},
		"replaced console": func(t *testing.T, f *jailFixture, _ *jailer.Record) {
			if err := os.Rename(console(f), console(f)+".moved"); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(console(f), nil, 0o640); err != nil {
				t.Fatal(err)
			}
		},
		"canceled": func(t *testing.T, f *jailFixture, _ *jailer.Record) {},
	}
	for name, setup := range refusals {
		t.Run(name, func(t *testing.T) {
			f, rec := jailReclaimFixture(t)
			jailLoosen(t, control(f), console(f), f.m.store.DiskPath(jailTestID))
			setup(t, f, &rec)
			before := jailTree(t, f.m.store.VMDir(jailTestID))
			ctx := t.Context()
			if name == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			err := jailNodeReclaimer(t, f, nil).Reclaim(ctx, rec)
			if err == nil {
				t.Fatalf("reclaim accepted %s", name)
			}
			if name == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("reclaim = %v, want Canceled", err)
			}
			if after := jailTree(t, f.m.store.VMDir(jailTestID)); !maps.Equal(before, after) {
				t.Fatalf("a refused reclaim changed files\nbefore %v\nafter  %v", before, after)
			}
		})
	}
}

// jailLoosen stands in for the jailer's chown/chmod to the VM uid: modes
// the reclaim must restore (the directory stays traversable for the test).
func jailLoosen(t *testing.T, dir string, files ...string) {
	t.Helper()
	if err := os.Chmod(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	for _, p := range files {
		if err := os.Chmod(p, 0o640); err != nil {
			t.Fatal(err)
		}
	}
}

// jailTree records path → inode/mode for every entry under dir.
func jailTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, _ os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Lstat(p)
		if err != nil {
			return err
		}
		out[p] = fmt.Sprintf("%d %v", info.Sys().(*syscall.Stat_t).Ino, info.Mode())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
