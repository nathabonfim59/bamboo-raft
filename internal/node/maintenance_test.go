package node

import (
	"bufio"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/nbd"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const maintTestID = "bx_mmmmmmmm"

// maintFakeQMP is a scripted maintenance QEMU: it answers the commands a
// session sends, keeps a fake bbraft-dirty bitmap and block exports, and
// binds a real unix socket for nbd-server-start (the NBD handshake itself is
// not served: accepted connections are closed).
type maintFakeQMP struct {
	t    *testing.T
	path string
	lis  net.Listener

	mu       sync.Mutex
	commands []string
	bitmap   *qemu.BlockDirtyBitmap
	schema   jsontext.Value
	exports  []string
	nbd      net.Listener
	nbdPath  string
	// controlDir, when set, is the host directory a jailed QEMU sees as
	// /control: nbd-server-start paths there are mapped to it.
	controlDir string
	// ignoreQuit makes quit answer without ending the process.
	ignoreQuit bool
	onQuit     func()
	// hook, when set, answers a command first (ok true), under mu.
	hook func(cmd string, args map[string]any) (result any, qerr string, ok bool)
}

func newMaintFakeQMP(t *testing.T) *maintFakeQMP {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "bbraft-maint-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, qemu.QMPFilename)
	lis, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	// The Phase 2 schema passes the capability gate, so the shared bitmap
	// step runs (bitmapSchema, phase2_launch_test.go).
	f := &maintFakeQMP{t: t, path: path, lis: lis, schema: jsontext.Value(bitmapSchema(t))}
	t.Cleanup(func() {
		lis.Close()
		f.mu.Lock()
		if f.nbd != nil {
			f.nbd.Close()
		}
		f.mu.Unlock()
	})
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go f.handle(conn)
		}
	}()
	return f
}

func (f *maintFakeQMP) dial(t *testing.T) *qmp.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	c, err := qmp.Dial(ctx, f.path, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// seen lists the commands received, without the capabilities handshake.
func (f *maintFakeQMP) seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.DeleteFunc(slices.Clone(f.commands), func(c string) bool { return c == "qmp_capabilities" })
}

func (f *maintFakeQMP) handle(conn net.Conn) {
	defer conn.Close()
	fmt.Fprintln(conn, `{"QMP":{"version":{"qemu":{"major":10,"minor":0,"micro":0},"package":"test"},"capabilities":[]}}`)
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var req struct {
			Execute   string         `json:"execute"`
			ID        uint64         `json:"id"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			return
		}
		result, qerr := f.exec(req.Execute, req.Arguments)
		reply := map[string]any{"return": result, "id": req.ID}
		if qerr != "" {
			reply = map[string]any{"error": map[string]any{"class": "GenericError", "desc": qerr}, "id": req.ID}
		}
		raw, _ := json.Marshal(reply)
		if _, err := conn.Write(append(raw, '\n')); err != nil {
			return
		}
		if req.Execute == "quit" {
			f.mu.Lock()
			ignore, onQuit := f.ignoreQuit, f.onQuit
			f.mu.Unlock()
			if !ignore {
				if onQuit != nil {
					onQuit()
				}
				return
			}
		}
	}
}

func (f *maintFakeQMP) exec(cmd string, args map[string]any) (any, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commands = append(f.commands, cmd)
	if f.hook != nil {
		if r, qerr, ok := f.hook(cmd, args); ok {
			return r, qerr
		}
	}
	switch cmd {
	case "query-qmp-schema":
		return f.schema, ""
	case "query-name":
		return map[string]any{"name": maintTestID}, ""
	case "query-named-block-nodes":
		var bitmaps []any
		if f.bitmap != nil {
			bitmaps = append(bitmaps, *f.bitmap)
		}
		return []any{
			map[string]any{"node-name": "base", "dirty-bitmaps": []any{}},
			map[string]any{"node-name": "rootdisk", "dirty-bitmaps": bitmaps},
		}, ""
	case "block-dirty-bitmap-add":
		if f.bitmap != nil {
			return nil, "Bitmap already exists: bbraft-dirty"
		}
		persistent, _ := args["persistent"].(bool)
		granularity, _ := args["granularity"].(float64)
		f.bitmap = &qemu.BlockDirtyBitmap{Name: qemu.DirtyBitmap, Recording: true, Persistent: persistent, Granularity: uint64(granularity)}
	case "block-dirty-bitmap-remove":
		f.bitmap = nil
	case "block-dirty-bitmap-enable":
		if f.bitmap != nil {
			f.bitmap.Recording = true
		}
	case "nbd-server-start":
		addr, _ := args["addr"].(map[string]any)
		data, _ := addr["data"].(map[string]any)
		path, _ := data["path"].(string)
		if rest, ok := strings.CutPrefix(path, "/control/"); ok && f.controlDir != "" {
			path = filepath.Join(f.controlDir, rest)
		}
		lis, err := net.Listen("unix", path)
		if err != nil {
			return nil, err.Error()
		}
		f.nbd, f.nbdPath = lis, path
		go func() {
			for {
				c, err := lis.Accept()
				if err != nil {
					return
				}
				c.Close()
			}
		}()
	case "block-export-add":
		id, _ := args["id"].(string)
		f.exports = append(f.exports, id)
	case "query-block-exports":
		out := []any{}
		for _, id := range f.exports {
			out = append(out, map[string]any{"id": id})
		}
		return out, ""
	case "block-export-del":
		id, _ := args["id"].(string)
		f.exports = slices.DeleteFunc(f.exports, func(e string) bool { return e == id })
	case "nbd-server-stop":
		if f.nbd == nil {
			return nil, "NBD server not running"
		}
		f.nbd.Close() // unlinks the socket, like QEMU
		f.nbd = nil
	}
	return map[string]any{}, ""
}

// maintFakeProc gives a fake process the QMP control a maintenance session
// needs.
type maintFakeProc struct {
	*fakeProcess
	client *qmp.Client
}

func (p *maintFakeProc) QMP() *qmp.Client { return p.client }
func (p *maintFakeProc) Close() error     { return p.client.Close() }

// maintTestLauncher records every StartOptions and wraps maintenance
// launches with the fake QMP; VM boots stay plain fake processes.
type maintTestLauncher struct {
	inner *fakeLauncher
	qmp   *maintFakeQMP
	t     *testing.T

	mu    sync.Mutex
	opts  []qemu.StartOptions
	cfgs  []qemu.Config
	procs []*maintFakeProc
	// pid, when set, overrides the maintenance fake's pid.
	pid int
}

func (l *maintTestLauncher) Start(ctx context.Context, cfg qemu.Config, opts qemu.StartOptions) (VMProcess, error) {
	l.mu.Lock()
	l.opts = append(l.opts, opts)
	l.cfgs = append(l.cfgs, cfg)
	l.mu.Unlock()
	p, err := l.inner.Start(ctx, cfg, opts)
	if err != nil || !opts.Maintenance {
		return p, err
	}
	fp := p.(*fakeProcess)
	l.mu.Lock()
	if l.pid != 0 {
		fp.pid = l.pid
	}
	l.mu.Unlock()
	l.qmp.mu.Lock()
	l.qmp.onQuit = func() { fp.exit(nil) }
	l.qmp.mu.Unlock()
	mp := &maintFakeProc{fakeProcess: fp, client: l.qmp.dial(l.t)}
	l.mu.Lock()
	l.procs = append(l.procs, mp)
	l.mu.Unlock()
	return mp, nil
}

func (l *maintTestLauncher) maintenanceStarts() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, o := range l.opts {
		if o.Maintenance {
			n++
		}
	}
	return n
}

func (l *maintTestLauncher) lastMaint() (*maintFakeProc, qemu.Config) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var cfg qemu.Config
	for i, o := range l.opts {
		if o.Maintenance {
			cfg = l.cfgs[i]
		}
	}
	if len(l.procs) == 0 {
		return nil, cfg
	}
	return l.procs[len(l.procs)-1], cfg
}

type maintHarness struct {
	*harness
	launcher *maintTestLauncher
	qmp      *maintFakeQMP
}

// newMaintHarness boots and stops one VM (2 vCPU, 2048 MiB) so it is
// STOPPED with its disk kept and no compute held.
func newMaintHarness(t *testing.T, capacity *nodev1.Resources) *maintHarness {
	t.Helper()
	fq := newMaintFakeQMP(t)
	inner := &fakeLauncher{}
	ml := &maintTestLauncher{inner: inner, qmp: fq, t: t}
	h := newHarness(t, harnessOptions{
		launcher:  inner,
		capacity:  capacity,
		configure: func(cfg *Config) { cfg.Launcher = ml },
	})
	h.mg.maintQuit = 200 * time.Millisecond
	h.create(maintTestID, nil)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: maintTestID, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	return &maintHarness{harness: h, launcher: ml, qmp: fq}
}

func (h *maintHarness) vm(t *testing.T) *vm {
	t.Helper()
	v, err := h.mg.lookup(maintTestID)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func (h *maintHarness) reservedMemory() uint64 {
	h.mg.mu.Lock()
	defer h.mg.mu.Unlock()
	return h.mg.reserved.memoryMib
}

func (h *maintHarness) state(t *testing.T) nodev1.VMState {
	t.Helper()
	got, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: maintTestID})
	if err != nil {
		t.Fatal(err)
	}
	return got.GetState()
}

func TestMaintenanceStoppedSession(t *testing.T) {
	h := newMaintHarness(t, nil)
	v := h.vm(t)
	before, err := h.mg.store.ReadVM(maintTestID)
	if err != nil {
		t.Fatal(err)
	}
	baseMem := h.reservedMemory()
	var during uint64
	err = h.mg.maintenance(t.Context(), v, func(ctx context.Context, s *maintSession) error {
		during = h.reservedMemory()
		meta, err := h.mg.store.ReadVM(maintTestID)
		if err != nil {
			return err
		}
		if meta.QEMU == nil || meta.QEMU.PID != s.PID() {
			return fmt.Errorf("launch intent during the session = %+v, want running pid %d", meta.QEMU, s.PID())
		}
		v.mu.Lock()
		fence := v.maintCancel != nil
		proc := v.process
		v.mu.Unlock()
		if !fence || proc != nil {
			return fmt.Errorf("during the session fence=%t process=%v", fence, proc)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	if during != baseMem+256 {
		t.Fatalf("reserved memory during the session = %d, want %d", during, baseMem+256)
	}
	if got := h.reservedMemory(); got != baseMem {
		t.Fatalf("reserved memory after the session = %d, want %d", got, baseMem)
	}
	_, cfg := h.launcher.lastMaint()
	if cfg.MemoryMiB != 256 || cfg.VCPUs != 1 || cfg.Network != nil {
		t.Fatalf("maintenance config = %dx%dMiB network=%v, want 1x256MiB without network", cfg.VCPUs, cfg.MemoryMiB, cfg.Network)
	}
	if h.launcher.maintenanceStarts() != 1 {
		t.Fatalf("maintenance launches = %d, want 1", h.launcher.maintenanceStarts())
	}
	seen := h.qmp.seen()
	for _, want := range []string{"query-named-block-nodes", "block-dirty-bitmap-add", "quit"} {
		if !slices.Contains(seen, want) {
			t.Fatalf("QMP commands %q lack %s", seen, want)
		}
	}
	if slices.Contains(seen, "cont") {
		t.Fatalf("a maintenance QEMU was continued: %q", seen)
	}
	if got := h.state(t); got != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state after the session = %s", got)
	}
	after, err := h.mg.store.ReadVM(maintTestID)
	if err != nil {
		t.Fatal(err)
	}
	if after.QEMU != nil || after.StoppedAtUnixMs != before.StoppedAtUnixMs || !after.Stopped() {
		t.Fatalf("manifest after the session: QEMU=%+v stoppedAt=%d (was %d)", after.QEMU, after.StoppedAtUnixMs, before.StoppedAtUnixMs)
	}
	v.mu.Lock()
	fence := v.maintCancel
	v.mu.Unlock()
	if fence != nil {
		t.Fatal("the fence outlived the session")
	}
	// The kept disk boots again.
	if _, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: maintTestID}); err != nil {
		t.Fatalf("StartVM after the session: %v", err)
	}
}

func TestMaintenanceNBDServer(t *testing.T) {
	h := newMaintHarness(t, nil)
	v := h.vm(t)
	nbdPath := filepath.Join(h.mg.store.VMDir(maintTestID), storage.NBDSocketName)
	err := h.mg.maintenance(t.Context(), v, func(ctx context.Context, s *maintSession) error {
		if _, err := s.DialNBD(ctx, nbd.Options{}); err == nil {
			return errors.New("DialNBD before StartNBD succeeded")
		}
		path, err := s.StartNBD(ctx)
		if err != nil {
			return err
		}
		if path != nbdPath {
			return fmt.Errorf("StartNBD = %q, want %q", path, nbdPath)
		}
		if info, err := os.Lstat(path); err != nil || info.Mode().Type() != os.ModeSocket {
			return fmt.Errorf("nbd socket: %v %v", info, err)
		}
		if again, err := s.StartNBD(ctx); err != nil || again != path {
			return fmt.Errorf("second StartNBD = %q, %v", again, err)
		}
		// The peer is the fake QEMU (this process): the peer check passes
		// and the fake's missing handshake fails the dial afterwards.
		_, err = s.DialNBD(ctx, nbd.Options{})
		if err == nil || strings.Contains(err.Error(), "peer check") {
			return fmt.Errorf("DialNBD = %v, want a handshake failure after the peer check", err)
		}
		if err := s.QMP().Execute(ctx, "block-export-add", map[string]any{"id": "restore", "type": "nbd", "node-name": "rootdisk"}, nil); err != nil {
			return err
		}
		if err := s.StopNBD(ctx); err != nil {
			return err
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("nbd socket after StopNBD: %v", err)
		}
		// A second session start after a stop is fine.
		_, err = s.StartNBD(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	seen := h.qmp.seen()
	if !slices.Contains(seen, "block-export-del") || strings.Count(strings.Join(seen, " "), "nbd-server-stop") != 2 {
		t.Fatalf("teardown commands %q", seen)
	}
	if _, err := os.Lstat(nbdPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("nbd socket after the session: %v", err)
	}
}

func TestMaintenanceNBDPeerCheck(t *testing.T) {
	h := newMaintHarness(t, nil)
	h.launcher.pid = 1 // the NBD socket's peer (this process) is not "QEMU"
	err := h.mg.maintenance(t.Context(), h.vm(t), func(ctx context.Context, s *maintSession) error {
		if _, err := s.StartNBD(ctx); err != nil {
			return err
		}
		_, err := s.DialNBD(ctx, nbd.Options{})
		if err == nil || !strings.Contains(err.Error(), "not maintenance QEMU pid 1") {
			return fmt.Errorf("DialNBD = %v, want a peer mismatch", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("maintenance: %v", err)
	}
}

func TestMaintenanceStaleSocket(t *testing.T) {
	h := newMaintHarness(t, nil)
	nbdPath := filepath.Join(h.mg.store.VMDir(maintTestID), storage.NBDSocketName)
	lis, err := net.Listen("unix", nbdPath)
	if err != nil {
		t.Fatal(err)
	}
	// Leave the socket file behind, as a killed QEMU would.
	lis.(*net.UnixListener).SetUnlinkOnClose(false)
	lis.Close()
	if err := h.mg.maintenance(t.Context(), h.vm(t), func(context.Context, *maintSession) error { return nil }); err != nil {
		t.Fatalf("maintenance over a stale socket: %v", err)
	}
	if _, err := os.Lstat(nbdPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale socket kept: %v", err)
	}

	// A non-socket is never removed, and nothing is launched.
	if err := os.WriteFile(nbdPath, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	starts := h.launcher.maintenanceStarts()
	mem := h.reservedMemory()
	err = h.mg.maintenance(t.Context(), h.vm(t), func(context.Context, *maintSession) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "leaving it alone") {
		t.Fatalf("maintenance over a regular file = %v", err)
	}
	if h.launcher.maintenanceStarts() != starts {
		t.Fatal("a session launched over a non-socket nbd.sock")
	}
	if raw, err := os.ReadFile(nbdPath); err != nil || string(raw) != "keep" {
		t.Fatalf("non-socket entry changed: %q %v", raw, err)
	}
	if got := h.reservedMemory(); got != mem {
		t.Fatalf("memory after a refused launch = %d, want %d", got, mem)
	}
}

func TestMaintenanceRefusals(t *testing.T) {
	h := newMaintHarness(t, &nodev1.Resources{Vcpus: 64, MemoryMib: 2048 + 100, DiskGib: 1024})
	v := h.vm(t)
	noop := func(context.Context, *maintSession) error { return nil }

	// A READY VM's disk is in use.
	if _, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: maintTestID}); err != nil {
		t.Fatal(err)
	}
	wantCode(t, h.mg.maintenance(t.Context(), v, noop), codes.FailedPrecondition)
	if _, err := h.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: maintTestID, GraceSeconds: 1}); err != nil {
		t.Fatal(err)
	}

	// No room: the capacity leaves 100 MiB, the session needs 256 MiB.
	h.create("bx_nnnnnnnn", &nodev1.Resources{Vcpus: 1, MemoryMib: 2048, DiskGib: defaultTestBaseGiB})
	mem := h.reservedMemory()
	err := h.mg.maintenance(t.Context(), v, noop)
	wantCode(t, err, codes.Unavailable)
	if !strings.Contains(err.Error(), "node busy") {
		t.Fatalf("no-room error = %v", err)
	}
	if h.launcher.maintenanceStarts() != 0 {
		t.Fatal("a refused session launched QEMU")
	}
	if got := h.reservedMemory(); got != mem {
		t.Fatalf("memory after a refused session = %d, want %d", got, mem)
	}

	// A deleting VM is gone.
	v.mu.Lock()
	v.deleting = true
	v.mu.Unlock()
	wantCode(t, h.mg.maintenance(t.Context(), v, noop), codes.NotFound)
	v.mu.Lock()
	v.deleting = false
	v.mu.Unlock()
}

// TestMaintenanceFnErrorQuitsAndReleases proves a failing fn still quits
// QEMU, releases memory and the fence, and keeps the VM STOPPED.
func TestMaintenanceFnErrorQuitsAndReleases(t *testing.T) {
	h := newMaintHarness(t, nil)
	mem := h.reservedMemory()
	boom := errors.New("boom")
	err := h.mg.maintenance(t.Context(), h.vm(t), func(context.Context, *maintSession) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("maintenance = %v, want boom", err)
	}
	proc, _ := h.launcher.lastMaint()
	if !procDone(proc) || !slices.Contains(h.qmp.seen(), "quit") {
		t.Fatalf("QEMU not quit after fn failure: done=%t commands=%q", procDone(proc), h.qmp.seen())
	}
	if got := h.reservedMemory(); got != mem {
		t.Fatalf("memory after a failed session = %d, want %d", got, mem)
	}
	if got := h.state(t); got != nodev1.VMState_VM_STATE_STOPPED {
		t.Fatalf("state after a failed session = %s", got)
	}
}

// TestMaintenanceQuitIgnoredIsKilled proves a QEMU that does not exit after
// quit is killed within the bound.
func TestMaintenanceQuitIgnoredIsKilled(t *testing.T) {
	h := newMaintHarness(t, nil)
	h.qmp.mu.Lock()
	h.qmp.bitmap = &qemu.BlockDirtyBitmap{Name: qemu.DirtyBitmap, Granularity: qemu.DirtyBitmapGranularity, Recording: true, Persistent: true}
	h.qmp.mu.Unlock()
	if err := h.mg.store.ClearFullScanNext(maintTestID); err != nil {
		t.Fatal(err)
	}
	// Control: a clean quit with a healthy bitmap trusts it.
	if err := h.mg.maintenance(t.Context(), h.vm(t), func(context.Context, *maintSession) error { return nil }); err != nil {
		t.Fatalf("clean maintenance: %v", err)
	}
	if phase2StateOf(t, h.harness, maintTestID).FullScanNext {
		t.Fatal("a cleanly quit session recorded full scan next")
	}
	h.qmp.mu.Lock()
	h.qmp.ignoreQuit = true
	h.qmp.mu.Unlock()
	start := time.Now()
	if err := h.mg.maintenance(t.Context(), h.vm(t), func(context.Context, *maintSession) error { return nil }); err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	proc, _ := h.launcher.lastMaint()
	if !procDone(proc) {
		t.Fatal("an ignoring QEMU is still alive")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("kill took %s", took)
	}
	if h.launcher.inner.stopCount() == 0 {
		t.Fatal("the ignoring QEMU was never stopped")
	}
	if !phase2StateOf(t, h.harness, maintTestID).FullScanNext {
		t.Fatal("a killed maintenance QEMU did not record full scan next")
	}
}

// TestMaintenanceSerializesStartVM proves StartVM waits for a running
// session instead of booting its disk under it.
func TestMaintenanceSerializesStartVM(t *testing.T) {
	h := newMaintHarness(t, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	sessionDone := make(chan error, 1)
	go func() {
		sessionDone <- h.mg.maintenance(t.Context(), h.vm(t), func(context.Context, *maintSession) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	boots := h.launcher.inner.count()
	startDone := make(chan error, 1)
	go func() {
		_, err := h.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: maintTestID})
		startDone <- err
	}()
	time.Sleep(200 * time.Millisecond)
	if got := h.launcher.inner.count(); got != boots {
		t.Fatalf("StartVM launched %d QEMUs during the session", got-boots)
	}
	select {
	case err := <-startDone:
		t.Fatalf("StartVM returned during the session: %v", err)
	default:
	}
	close(release)
	if err := <-sessionDone; err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	if err := <-startDone; err != nil {
		t.Fatalf("StartVM after the session: %v", err)
	}
	proc, _ := h.launcher.lastMaint()
	if !procDone(proc) {
		t.Fatal("StartVM booted while the maintenance QEMU lived")
	}
	if got := h.state(t); got != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("state = %s, want READY", got)
	}
}

// TestMaintenanceDeleteFences proves DeleteVM fences a running session like a
// boot: fn's context ends with the fence cause, QEMU is killed without a
// quit, DeleteVM returns promptly and nothing is left.
func TestMaintenanceDeleteFences(t *testing.T) {
	h := newMaintHarness(t, nil)
	mem := h.reservedMemory()
	entered := make(chan struct{})
	var cause error
	sessionDone := make(chan error, 1)
	go func() {
		sessionDone <- h.mg.maintenance(context.Background(), h.vm(t), func(ctx context.Context, s *maintSession) error {
			if _, err := s.StartNBD(ctx); err != nil {
				return err
			}
			close(entered)
			<-ctx.Done()
			cause = context.Cause(ctx)
			return ctx.Err()
		})
	}()
	<-entered
	start := time.Now()
	h.delete(maintTestID)
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("DeleteVM took %s with a session running", took)
	}
	err := <-sessionDone
	wantCode(t, err, codes.Aborted)
	if !errors.Is(cause, errMaintenanceFenced) {
		t.Fatalf("fn context cause = %v, want the fence", cause)
	}
	proc, _ := h.launcher.lastMaint()
	if !procDone(proc) {
		t.Fatal("the fenced QEMU is alive")
	}
	if slices.Contains(h.qmp.seen(), "quit") {
		t.Fatal("a fenced session quit instead of killing")
	}
	if _, err := h.mg.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: maintTestID}); err == nil {
		t.Fatal("the VM survived DeleteVM")
	}
	if _, err := os.Stat(h.mg.store.VMDir(maintTestID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("VM directory after DeleteVM: %v", err)
	}
	if got := h.reservedMemory(); got != mem {
		t.Fatalf("memory after the fence = %d, want %d", got, mem)
	}
}

// TestMaintenanceSessionBitmap proves every session runs the shared bitmap
// step (m.ensureVMBitmap) before fn: a missing, disabled, inconsistent or
// non-persistent bbraft-dirty is repaired and records "full scan next"; a
// healthy one is kept and records nothing.
func TestMaintenanceSessionBitmap(t *testing.T) {
	healthy := qemu.BlockDirtyBitmap{Name: qemu.DirtyBitmap, Granularity: qemu.DirtyBitmapGranularity, Recording: true, Persistent: true}
	disabled, inconsistent, transient := healthy, healthy, healthy
	disabled.Recording = false
	inconsistent.Recording, inconsistent.Inconsistent = false, true
	transient.Persistent = false
	for _, tt := range []struct {
		name     string
		bitmap   *qemu.BlockDirtyBitmap
		repair   []string
		fullScan bool
	}{
		{name: "missing", repair: []string{"block-dirty-bitmap-add"}, fullScan: true},
		{name: "healthy", bitmap: &healthy},
		{name: "disabled", bitmap: &disabled, repair: []string{"block-dirty-bitmap-enable"}, fullScan: true},
		{name: "inconsistent", bitmap: &inconsistent, repair: []string{"block-dirty-bitmap-remove", "block-dirty-bitmap-add"}, fullScan: true},
		{name: "not persistent", bitmap: &transient, repair: []string{"block-dirty-bitmap-remove", "block-dirty-bitmap-add"}, fullScan: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newMaintHarness(t, nil)
			h.qmp.mu.Lock()
			if tt.bitmap != nil {
				b := *tt.bitmap
				h.qmp.bitmap = &b
			}
			h.qmp.mu.Unlock()
			if err := h.mg.store.ClearFullScanNext(maintTestID); err != nil {
				t.Fatal(err)
			}
			var before []string
			err := h.mg.maintenance(t.Context(), h.vm(t), func(context.Context, *maintSession) error {
				before = h.qmp.seen()
				return nil
			})
			if err != nil {
				t.Fatalf("maintenance: %v", err)
			}
			var repairs []string
			for _, c := range before {
				if strings.HasPrefix(c, "block-dirty-bitmap-") {
					repairs = append(repairs, c)
				}
			}
			if !slices.Equal(repairs, tt.repair) {
				t.Fatalf("bitmap commands before fn = %q, want %q (all: %q)", repairs, tt.repair, before)
			}
			if !slices.Contains(before, "query-named-block-nodes") {
				t.Fatalf("the bitmap step did not run before fn: %q", before)
			}
			h.qmp.mu.Lock()
			b := h.qmp.bitmap
			h.qmp.mu.Unlock()
			if b == nil || !b.Recording || !b.Persistent || b.Inconsistent || b.Granularity != qemu.DirtyBitmapGranularity {
				t.Fatalf("bitmap after the session = %+v", b)
			}
			if got := phase2StateOf(t, h.harness, maintTestID).FullScanNext; got != tt.fullScan {
				t.Fatalf("fullScanNext = %t, want %t", got, tt.fullScan)
			}
		})
	}
}

// TestMaintenanceArgvForms proves the node's two launch forms are told apart
// only by the explicit marker: the production VM argv with -S is never a
// maintenance argv and the maintenance argv never passes the VM grammar.
func TestMaintenanceArgvForms(t *testing.T) {
	if got, want := qemu.MaintenanceName(maintTestID), jailer.MaintenanceName(maintTestID); got != want {
		t.Fatalf("qemu.MaintenanceName = %q, jailer.MaintenanceName = %q", got, want)
	}
	tap, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer tap.Close()
	vhost, err := os.Open("/dev/zero")
	if err != nil {
		t.Fatal(err)
	}
	defer vhost.Close()
	cfg := qemu.Config{
		SandboxID: maintTestID, WorkDir: t.TempDir(), ImageDir: t.TempDir(), DiskPath: "/var/lib/x/disk.qcow2",
		VCPUs: 2, MemoryMiB: 2048, VsockCID: 7, Nested: qemu.NestedOff,
		Network: &qemu.Network{Tap: tap, Vhost: vhost, MAC: "52:54:00:00:00:01",
			GuestIP: netip.MustParseAddr("198.51.100.2"), PrefixLen: 24, Gateway: netip.MustParseAddr("198.51.100.1")},
	}
	vmArgs, err := cfg.JailedArgs()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(vmArgs, "-S") {
		t.Fatalf("the production VM argv is not paused: %q", vmArgs)
	}
	maint := cfg
	maint.Network = nil
	maint.MemoryMiB, maint.VCPUs = qemu.MaintenanceMemoryMiB(cfg.MemoryMiB), 1
	maintArgs, err := maint.MaintenanceArgs()
	if err != nil {
		t.Fatal(err)
	}
	if err := jailer.ValidateArgv(maintTestID, vmArgs, jailDisks()); err != nil {
		t.Fatalf("VM argv with -S: %v", err)
	}
	if jailer.IsMaintenanceArgv(maintTestID, vmArgs) || jailer.ValidateMaintenanceArgv(maintTestID, vmArgs, jailDisks()) == nil {
		t.Fatal("the VM argv is recognised as maintenance")
	}
	if err := jailer.ValidateMaintenanceArgv(maintTestID, maintArgs, jailDisks()); err != nil {
		t.Fatalf("maintenance argv: %v", err)
	}
	if !jailer.IsMaintenanceArgv(maintTestID, maintArgs) || jailer.ValidateArgv(maintTestID, maintArgs, jailDisks()) == nil {
		t.Fatal("the maintenance argv passes as a VM argv")
	}

	// Non-jailed: the VM ownership predicate never matches a maintenance
	// argv and the maintenance predicate never matches a VM argv.
	exe := "/usr/bin/qemu-system-x86_64"
	plainVM, err := cfg.Args()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(plainVM, "-S") {
		t.Fatalf("the non-jailed VM argv is not paused: %q", plainVM)
	}
	plainMaint, err := maint.PlainMaintenanceArgs()
	if err != nil {
		t.Fatal(err)
	}
	vmInfo := procInfo{exe: exe, argv: plainVM}
	maintInfo := procInfo{exe: exe, argv: plainMaint}
	if !ownedPaths(vmInfo, maintTestID, cfg.WorkDir, cfg.DiskPath, exe, "", "") || maintOwnedPaths(vmInfo, maintTestID, cfg.WorkDir, cfg.DiskPath, exe) {
		t.Fatal("non-jailed VM argv misclassified")
	}
	if ownedPaths(maintInfo, maintTestID, cfg.WorkDir, cfg.DiskPath, exe, "", "") || !maintOwnedPaths(maintInfo, maintTestID, cfg.WorkDir, cfg.DiskPath, exe) {
		t.Fatal("non-jailed maintenance argv misclassified")
	}
	if !jailer.IsMaintenanceArgv(maintTestID, plainMaint[1:]) || jailer.IsMaintenanceArgv(maintTestID, plainVM[1:]) {
		t.Fatal("IsMaintenanceArgv misclassifies the plain forms")
	}
}

// TestMaintenanceJailedSession runs a session through the jailed launcher
// (fake jailer with real child processes): the maintenance spec passes the
// jailer's own validation, carries no TAP/vhost/vsock descriptor, limits
// memory to the admitted 256 MiB, and the attempt is released afterwards.
func TestMaintenanceJailedSession(t *testing.T) {
	fq := newMaintFakeQMP(t)
	var maint atomic.Bool
	j := newJailedHarness(t, jailedOptions{configure: func(cfg *Config) {
		cfg.jailDial = func(ctx context.Context, path string, pid int, done <-chan struct{}) (*qmp.Client, error) {
			if !maint.Load() {
				return nil, nil
			}
			return qmp.Dial(ctx, fq.path, nil)
		}
	}})
	j.mg.maintQuit = 200 * time.Millisecond
	j.create(maintTestID, nil)
	if _, err := j.mg.StopVM(t.Context(), &nodev1.StopVMRequest{SandboxId: maintTestID, GraceSeconds: 1}); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	var validateErr error
	j.jail.mu.Lock()
	j.jail.beforeStart = func(spec jailer.LaunchSpec) {
		if spec.Maintenance {
			validateErr = spec.Validate()
		}
	}
	j.jail.mu.Unlock()
	fq.mu.Lock()
	fq.controlDir = filepath.Join(j.mg.store.VMDir(maintTestID), "control")
	fq.onQuit = func() {
		// A clean QEMU exit: the jailed child ends on its own.
		j.jail.mu.Lock()
		run := j.jail.runOf[maintTestID]
		j.jail.mu.Unlock()
		if run != nil {
			_ = run.cmd.Process.Kill()
		}
	}
	fq.mu.Unlock()
	maint.Store(true)
	v, err := j.mg.lookup(maintTestID)
	if err != nil {
		t.Fatal(err)
	}
	var pid int
	err = j.mg.maintenance(t.Context(), v, func(ctx context.Context, s *maintSession) error {
		pid = s.PID()
		path, err := s.StartNBD(ctx)
		if err != nil {
			return err
		}
		if want := filepath.Join(j.mg.store.VMDir(maintTestID), "control", storage.NBDSocketName); path != want {
			return fmt.Errorf("jailed NBD host path %q, want %q", path, want)
		}
		return nil
	})
	maint.Store(false)
	if err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	if validateErr != nil {
		t.Fatalf("the maintenance spec fails jailer validation: %v", validateErr)
	}
	j.jail.mu.Lock()
	spec := j.jail.specs[len(j.jail.specs)-1]
	_, retained := j.jail.records[maintTestID]
	j.jail.mu.Unlock()
	if !spec.Maintenance {
		t.Fatal("the last jail launch is not a maintenance spec")
	}
	var names []string
	for _, d := range spec.Files {
		names = append(names, d.Name)
	}
	if want := []string{jailer.KernelName, jailer.InitrdName, "base", "overlay"}; !slices.Equal(names, want) {
		t.Fatalf("maintenance descriptors %q, want %q", names, want)
	}
	if spec.Limits.MemoryBytes != 256<<20 || spec.Limits.CPUQuotaPercent != 100 {
		t.Fatalf("maintenance limits %+v", spec.Limits)
	}
	if err := jailer.ValidateMaintenanceArgv(maintTestID, spec.Args, jailDisks()); err != nil {
		t.Fatalf("maintenance argv: %v", err)
	}
	if !slices.Contains(fq.seen(), "quit") || slices.Contains(fq.seen(), "cont") {
		t.Fatalf("QMP commands %q", fq.seen())
	}
	if retained {
		t.Fatal("the maintenance attempt's jail reservation is retained")
	}
	if pid == 0 {
		t.Fatal("no session pid")
	}
	v.mu.Lock()
	ref := v.jail
	v.mu.Unlock()
	if ref != (jailer.Ref{}) {
		t.Fatalf("v.jail after the session = %+v", ref)
	}
	meta, err := j.mg.store.ReadVM(maintTestID)
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Stopped() {
		t.Fatalf("manifest after a jailed session is not stopped: %+v", meta.QEMU)
	}
	// The kept disk boots jailed again (the jail check finds no stale entry).
	if _, err := j.mg.StartVM(t.Context(), &nodev1.StartVMRequest{SandboxId: maintTestID}); err != nil {
		t.Fatalf("StartVM after the session: %v", err)
	}
}

// TestReclaimVMMaintenanceSocket proves the VM reclaim removes a maintenance
// session's leftover nbd.sock together with qmp.sock, and still refuses a
// non-socket nbd.sock, a socket of another uid or a third entry.
func TestReclaimVMMaintenanceSocket(t *testing.T) {
	control := func(f *jailFixture) string {
		return filepath.Join(f.m.store.VMDir(jailTestID), storage.ControlDirName)
	}
	mknod := func(t *testing.T, path string) {
		t.Helper()
		if err := syscall.Mknod(path, syscall.S_IFSOCK|0o600, 0); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("removes both sockets", func(t *testing.T) {
		f, rec := jailReclaimFixture(t)
		qmpSock := filepath.Join(control(f), qemu.QMPFilename)
		nbdSock := filepath.Join(control(f), storage.NBDSocketName)
		mknod(t, qmpSock)
		mknod(t, nbdSock)
		if err := jailNodeReclaimer(t, f, nil).Reclaim(t.Context(), rec); err != nil {
			t.Fatal(err)
		}
		for _, p := range []string{qmpSock, nbdSock} {
			if _, err := os.Lstat(p); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s survived: %v", p, err)
			}
		}
	})
	t.Run("removes a lone nbd socket", func(t *testing.T) {
		f, rec := jailReclaimFixture(t)
		nbdSock := filepath.Join(control(f), storage.NBDSocketName)
		mknod(t, nbdSock)
		if err := jailNodeReclaimer(t, f, nil).Reclaim(t.Context(), rec); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(nbdSock); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("nbd.sock survived: %v", err)
		}
	})
	refusals := map[string]func(t *testing.T, f *jailFixture, rec *jailer.Record){
		"nbd.sock regular file": func(t *testing.T, f *jailFixture, _ *jailer.Record) {
			if err := os.WriteFile(filepath.Join(control(f), storage.NBDSocketName), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"nbd socket of another uid": func(t *testing.T, f *jailFixture, rec *jailer.Record) {
			rec.UID++
			mknod(t, filepath.Join(control(f), storage.NBDSocketName))
		},
		"third entry": func(t *testing.T, f *jailFixture, _ *jailer.Record) {
			mknod(t, filepath.Join(control(f), qemu.QMPFilename))
			mknod(t, filepath.Join(control(f), storage.NBDSocketName))
			mknod(t, filepath.Join(control(f), "other.sock"))
		},
	}
	for name, setup := range refusals {
		t.Run(name, func(t *testing.T) {
			f, rec := jailReclaimFixture(t)
			setup(t, f, &rec)
			before := jailTree(t, f.m.store.VMDir(jailTestID))
			if err := jailNodeReclaimer(t, f, nil).Reclaim(t.Context(), rec); err == nil {
				t.Fatalf("reclaim accepted %s", name)
			}
			if after := jailTree(t, f.m.store.VMDir(jailTestID)); !maps.Equal(before, after) {
				t.Fatalf("a refused reclaim changed files\nbefore %v\nafter  %v", before, after)
			}
		})
	}
}
