package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mdlayher/vsock"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
)

// GuestPort is the fixed AF_VSOCK port bbraft-guest listens on (A1 direction).
const GuestPort = 10789

// VMProcess is the part of a running QEMU process the manager owns. It is
// deliberately small: the manager waits for Done, stops on delete and closes
// the control connection on shutdown. qemu.Process implements it.
type VMProcess interface {
	// PID is the QEMU process id.
	PID() int
	// Done is closed when QEMU has exited and been reaped.
	Done() <-chan struct{}
	// Err reports the exit status after Done is closed, nil while running.
	Err() error
	// Stop asks QEMU to quit and falls back to killing it.
	Stop(ctx context.Context) error
	// Close releases the control connection without stopping QEMU.
	Close() error
}

// VMController is implemented by processes that can pause and resume guest
// execution. A3 snapshots use it; the manager itself does not.
type VMController interface {
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
}

// Launcher starts one QEMU process. The default implementation is
// [qemuLauncher]; tests inject a fake.
//
// Failure contract (mirroring qemu.Start's post-spawn contract): on error the
// returned process is nil when the launcher proved the child absent — it was
// never spawned, or its kill and reap completed — and may be non-nil when
// absence is unproven. A non-nil process with an error is a live handle: the
// caller must attach it, finish the stop and only then settle resources that
// depend on the machine being gone. Implementations must never return a nil
// concrete pointer wrapped in a non-nil VMProcess.
type Launcher interface {
	Start(ctx context.Context, cfg qemu.Config, opts qemu.StartOptions) (VMProcess, error)
}

type qemuLauncher struct{}

func (qemuLauncher) Start(ctx context.Context, cfg qemu.Config, opts qemu.StartOptions) (VMProcess, error) {
	p, err := qemu.Start(ctx, cfg, opts)
	if p == nil {
		// Every proven-absent failure, and no typed-nil interface: an absent
		// machine reports as a nil VMProcess.
		return nil, err
	}
	return p, err
}

// GuestDialer opens a gRPC connection to the guest agent of one VM. The
// default dials AF_VSOCK on [GuestPort]; tests use an in-process listener.
type GuestDialer interface {
	Dial(ctx context.Context, cid uint32) (*grpc.ClientConn, error)
}

// vsockDialer is the default [GuestDialer]. gRPC keeps the connection lazy,
// so Dial itself never blocks: the first Ping performs the vsock connect and
// is bounded by the boot deadline.
type vsockDialer struct{}

func (vsockDialer) Dial(_ context.Context, cid uint32) (*grpc.ClientConn, error) {
	return guestConn(func(ctx context.Context) (net.Conn, error) {
		return vsock.DialContext(ctx, cid, GuestPort, nil)
	})
}

// guestConn opens one guest connection with the response size the contract
// needs. gRPC's client default is 4 MiB, below GetProcess's two 8 MiB windows
// and a full ListFiles; MaxResponseBytes is the same cap the control plane
// applies to nodev1 responses. It only raises the response limit: requests to
// the guest stay on gRPC's default receive limit.
func guestConn(dial func(context.Context) (net.Conn, error)) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		"passthrough:///bbraft-guest",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return dial(ctx)
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(nodelink.MaxResponseBytes)),
	)
}

// drainEvents consumes QMP asynchronous events. The client's event queue is
// bounded and fails the connection when its single consumer falls behind, so
// every process the manager owns must keep reading until the connection
// closes.
func drainEvents(p VMProcess) {
	src, ok := p.(interface{ QMP() *qmp.Client })
	if !ok {
		return
	}
	client := src.QMP()
	if client == nil {
		return
	}
	for range client.Events() {
	}
}

// drainQMPClient keeps consuming asynchronous events on a rescue connection.
// The client queue is bounded, so every live connection needs one consumer.
func drainQMPClient(client *qmp.Client) {
	if client == nil {
		return
	}
	for range client.Events() {
	}
}

// stopProcess stops and closes a process with an independent bounded context:
// cleanup must finish even when the request or boot context is gone.
func stopProcess(p VMProcess) error {
	if p == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stopErr := p.Stop(ctx)
	closeErr := p.Close()
	return errors.Join(stopErr, closeErr)
}

// procDone reports without blocking whether a process's exit has already
// been proven (its Done channel closed and reaped). It never waits.
func procDone(p VMProcess) bool {
	select {
	case <-p.Done():
		return true
	default:
		return false
	}
}

// requestPowerDown sends QEMU's ACPI power-down request, which a guest with
// an ACPI-aware init turns into a clean shutdown inside StopVM's grace
// window. Processes without a QMP control connection (test fakes) report
// errNoPowerDown; callers treat that as "skip the graceful phase" and never
// as a failure of the stop itself.
func requestPowerDown(ctx context.Context, proc VMProcess) error {
	src, ok := proc.(interface{ QMP() *qmp.Client })
	if !ok {
		return errNoPowerDown
	}
	client := src.QMP()
	if client == nil {
		return errNoPowerDown
	}
	if err := client.Execute(ctx, "system_powerdown", nil, nil); err != nil {
		return fmt.Errorf("node: acpi power down: %w", err)
	}
	return nil
}

// errNoPowerDown reports that a process exposes no QMP control connection,
// so no ACPI request could be made.
var errNoPowerDown = errors.New("node: process has no QMP control connection")

// procInfo is the /proc identity of one process.
type procInfo struct {
	pid        int
	bootID     string
	startTicks uint64
	exe        string
	argv       []string
}

// readProc reads the identity fields of pid.
func readProc(pid int) (procInfo, error) {
	bootID, err := storage.HostBootID()
	if err != nil {
		return procInfo{}, err
	}
	ticks, err := storage.ProcStartTicks(pid)
	if err != nil {
		return procInfo{}, err
	}
	exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return procInfo{}, err
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return procInfo{}, err
	}
	var argv []string
	for part := range strings.SplitSeq(string(raw), "\x00") {
		if part != "" {
			argv = append(argv, part)
		}
	}
	if len(argv) == 0 {
		return procInfo{}, fmt.Errorf("node: proc %d has an empty command line", pid)
	}
	return procInfo{pid: pid, bootID: bootID, startTicks: ticks, exe: exe, argv: argv}, nil
}

// ownedBy reports whether info looks like this manager's QEMU for meta. It
// compares the exact executable and the exact argv values that carry paths
// the node owns; a different QEMU, even one with the same sandbox id in
// another data root, never matches. The image path uses the persisted image
// id, never the sandbox id.
func ownedBy(info procInfo, meta storage.VMMeta, imageDir, workDir, diskPath, expectedExe string) bool {
	imagePath := filepath.Join(imageDir, meta.Image)
	return ownedPaths(info, meta.SandboxID, workDir, diskPath, expectedExe,
		filepath.Join(imagePath, "kernel"), filepath.Join(imagePath, "initrd"))
}

// ownedPaths is the shared identity predicate. An empty kernel or initrd
// skips that check: a corrupt manifest has no trustworthy image id, but the
// work dir, disk and sandbox id are still derived from the directory name.
func ownedPaths(info procInfo, id, workDir, diskPath, expectedExe, kernel, initrd string) bool {
	if expectedExe == "" || info.exe != expectedExe {
		return false
	}
	if argAfter(info.argv, "-name") != "guest="+id+",process="+id {
		return false
	}
	if argAfter(info.argv, "-qmp") != "unix:"+filepath.Join(workDir, qemu.QMPFilename)+",server=on,wait=off" {
		return false
	}
	if !strings.Contains(argAfter(info.argv, "-blockdev"), ",file.filename="+diskPath+",") {
		return false
	}
	if kernel != "" && argAfter(info.argv, "-kernel") != kernel {
		return false
	}
	if initrd != "" && argAfter(info.argv, "-initrd") != initrd {
		return false
	}
	return true
}

// qemuExecutable resolves the QEMU binary once. /proc/<pid>/exe reports the
// real path, so the expected value must be resolved the same way; comparing
// only the basename would accept any binary called qemu-system-x86_64.
func qemuExecutable() string {
	path, err := exec.LookPath(qemu.DefaultBinary)
	if err != nil {
		return ""
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return resolved
	}
	return path
}

// argAfter returns the argv element following the first flag, or "" when the
// flag is absent. QEMU is launched with a fixed argv shape, so a positional
// index is unambiguous here; a missing value never matches an expected path.
func argAfter(argv []string, flag string) string {
	i := slices.Index(argv, flag)
	if i < 0 || i+1 >= len(argv) {
		return ""
	}
	return argv[i+1]
}

// pinnedProc pins a pid with a pidfd before any validation. Later checks and
// signals go through that fd, so pid reuse cannot redirect them to an
// unrelated process.
type pinnedProc struct {
	pid    int
	fd     int
	info   procInfo
	mu     sync.Mutex
	closed bool

	// Test-only hooks. afterLock runs while p.mu is held, immediately before
	// a pidfd syscall; pollErr injects an unknown poll failure.
	afterLock func()
	pollErr   func() error
}

// pinProc opens a pidfd for pid and reads its identity through it. The
// confirm call catches a pid that exited and was reused between the numeric
// lookup and the pidfd open: the pidfd then pins a dead process while /proc
// describes the replacement.
func pinProc(pid int) (*pinnedProc, error) {
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, fmt.Errorf("node: pin pid %d: %w", pid, err)
	}
	var info procInfo
	// A process that just exec'd can briefly show an empty cmdline. Retry
	// only that transient case; real identity errors still fail fast.
	for range 50 {
		info, err = readProc(pid)
		if err == nil || !strings.Contains(err.Error(), "empty command line") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err != nil {
		unix.Close(fd)
		return nil, err
	}
	p := &pinnedProc{pid: pid, fd: fd, info: info}
	if err := p.confirm(); err != nil {
		unix.Close(fd)
		return nil, err
	}
	return p, nil
}

// confirm proves the pinned process is still the same incarnation the
// identity was read from. A vanished process never authorizes control of its
// replacement: the start ticks must still match and the pidfd must be alive.
func (p *pinnedProc) confirm() error {
	alive, err := p.state()
	if err != nil {
		return err
	}
	if !alive {
		return fmt.Errorf("node: pid %d no longer exists", p.pid)
	}
	ticks, err := storage.ProcStartTicks(p.pid)
	if err != nil {
		return fmt.Errorf("node: re-read pid %d: %w", p.pid, err)
	}
	if ticks != p.info.startTicks {
		return fmt.Errorf("node: pid %d was reused (start ticks %d, was %d)", p.pid, ticks, p.info.startTicks)
	}
	return nil
}

// state reports whether the pinned process still exists. Only ESRCH is proof
// of exit; EPERM, EBADF and every other error leave the answer unknown and
// must never be treated as death.
func (p *pinnedProc) state() (alive bool, err error) {
	// Hold the lock across the syscall: close may otherwise recycle this fd
	// number and the check would target an unrelated descriptor.
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false, fmt.Errorf("node: pidfd for pid %d is closed", p.pid)
	}
	if p.afterLock != nil {
		p.afterLock()
	}
	err = unix.PidfdSendSignal(p.fd, 0, nil, 0)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, unix.ESRCH):
		return false, nil
	default:
		return false, fmt.Errorf("node: pidfd state for pid %d: %w", p.pid, err)
	}
}

// alive reports "not proven dead": a definite live process or an unknown
// pidfd error both protect the VM. Only an explicit ESRCH returns false.
func (p *pinnedProc) alive() bool {
	alive, err := p.state()
	return alive || err != nil
}

// signal sends sig through the pidfd.
func (p *pinnedProc) signal(sig unix.Signal) error {
	// Hold the lock through the syscall so close cannot recycle the fd in
	// between and cause a signal to an unrelated process.
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("node: pidfd for pid %d is closed", p.pid)
	}
	if p.afterLock != nil {
		p.afterLock()
	}
	if err := unix.PidfdSendSignal(p.fd, sig, nil, 0); err != nil {
		return fmt.Errorf("node: signal pid %d: %w", p.pid, err)
	}
	return nil
}

// close releases the pidfd. It does not affect the process, and is
// idempotent so cleanup paths can always call it.
func (p *pinnedProc) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return unix.Close(p.fd)
}

// findOwnedPID scans /proc for this manager's QEMU for meta. It returns nil
// when none exists; more than one match is reported as an error so the caller
// never picks a process arbitrarily.
func findOwnedPID(meta storage.VMMeta, imageDir, workDir, diskPath, expectedExe string) (*pinnedProc, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("node: scan /proc: %w", err)
	}
	var matches []*pinnedProc
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid <= 0 {
			continue
		}
		info, err := readProc(pid)
		if err != nil || !ownedBy(info, meta, imageDir, workDir, diskPath, expectedExe) {
			continue
		}
		// Pin before the final validation; a candidate that changed
		// between the scan and the pin simply fails the re-read.
		pinned, err := pinProc(pid)
		if err != nil {
			continue
		}
		if !ownedBy(pinned.info, meta, imageDir, workDir, diskPath, expectedExe) {
			pinned.close()
			continue
		}
		matches = append(matches, pinned)
	}
	switch len(matches) {
	case 0:
		return nil, nil
	case 1:
		return matches[0], nil
	default:
		for _, p := range matches {
			unix.Close(p.fd)
		}
		return nil, fmt.Errorf("node: %d processes claim sandbox %s; refusing to choose", len(matches), meta.SandboxID)
	}
}

// verifyQMP proves through the VM's own QMP socket that the pinned process is
// the QEMU for meta. It rechecks the peer credential and the guest name, then
// returns the connected client. A failure is not proof the process is
// unrelated, so callers never kill on it.
func (m *Manager) verifyQMPAt(ctx context.Context, pinned *pinnedProc, meta storage.VMMeta, qmpPath string) (*qmp.Client, error) {
	if pinned.info.bootID != meta.QEMU.BootID {
		return nil, fmt.Errorf("node: vm %s: process belongs to a previous host boot", meta.SandboxID)
	}
	if meta.QEMU.State == storage.QEMURunning && meta.QEMU.StartTicks != 0 && pinned.info.startTicks != meta.QEMU.StartTicks {
		return nil, fmt.Errorf("node: vm %s: pid %d start ticks %d do not match record %d", meta.SandboxID, pinned.pid, pinned.info.startTicks, meta.QEMU.StartTicks)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client, err := qmp.Dial(ctx, qmpPath, m.log)
	if err != nil {
		return nil, fmt.Errorf("node: vm %s: %w", meta.SandboxID, err)
	}
	peer, err := client.PeerPID()
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("node: vm %s: qmp peer: %w", meta.SandboxID, err)
	}
	if peer != pinned.pid {
		client.Close()
		return nil, fmt.Errorf("node: vm %s: qmp peer is pid %d, not %d", meta.SandboxID, peer, pinned.pid)
	}
	var name struct {
		Name string `json:"name"`
	}
	if err := client.Execute(ctx, "query-name", nil, &name); err != nil {
		client.Close()
		return nil, fmt.Errorf("node: vm %s: query-name: %w", meta.SandboxID, err)
	}
	if name.Name != meta.SandboxID && name.Name != "guest="+meta.SandboxID {
		client.Close()
		return nil, fmt.Errorf("node: vm %s: qmp reports guest name %q", meta.SandboxID, name.Name)
	}
	var status struct {
		Running bool   `json:"running"`
		Status  string `json:"status"`
	}
	if err := client.Execute(ctx, "query-status", nil, &status); err != nil {
		client.Close()
		return nil, fmt.Errorf("node: vm %s: query-status: %w", meta.SandboxID, err)
	}
	if status.Status != "running" && status.Status != "paused" {
		client.Close()
		return nil, fmt.Errorf("node: vm %s: qmp status %q", meta.SandboxID, status.Status)
	}
	// Recheck liveness after all identity and QMP observations: a pid that
	// vanished mid-verification must never be trusted.
	if err := pinned.confirm(); err != nil {
		client.Close()
		return nil, fmt.Errorf("node: vm %s: %w", meta.SandboxID, err)
	}
	if status.Status == "paused" {
		if err := resumePaused(ctx, client); err != nil {
			client.Close()
			return nil, fmt.Errorf("node: vm %s: %w", meta.SandboxID, err)
		}
	}
	return client, nil
}

// pollForExit performs one bounded pidfd poll with p.mu held, so a concurrent
// Close cannot recycle the descriptor between the check and the poll. exited
// is true only for POLLIN, the kernel's pidfd-exit indication; any other
// revents or error is unknown and keeps the handle.
func (p *pinnedProc) pollForExit(wait time.Duration) (exited bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false, fmt.Errorf("node: pidfd for pid %d is closed", p.pid)
	}
	if p.pollErr != nil {
		if err := p.pollErr(); err != nil {
			return false, err
		}
	}
	if p.afterLock != nil {
		p.afterLock()
	}
	fds := []unix.PollFd{{Fd: int32(p.fd), Events: unix.POLLIN}}
	n, err := unix.Poll(fds, int(wait.Milliseconds()))
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil
	}
	if fds[0].Revents&unix.POLLIN != 0 {
		return true, nil
	}
	return false, fmt.Errorf("node: pidfd for pid %d poll revents %#x", p.pid, fds[0].Revents)
}

// findOwnedQEMU finds a process whose argv names this sandbox's work dir and
// disk. It is the conservative corrupt-manifest check: the image paths are
// unknown, but a live QEMU using the owned paths must still block startup.
func findOwnedQEMU(id, workDir, diskPath, expectedExe string) (*pinnedProc, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("node: scan /proc: %w", err)
	}
	var matches []*pinnedProc
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid <= 0 {
			continue
		}
		info, err := readProc(pid)
		if err != nil || !ownedPaths(info, id, workDir, diskPath, expectedExe, "", "") {
			continue
		}
		pinned, err := pinProc(pid)
		if err != nil {
			continue
		}
		if !ownedPaths(pinned.info, id, workDir, diskPath, expectedExe, "", "") {
			pinned.close()
			continue
		}
		matches = append(matches, pinned)
	}
	switch len(matches) {
	case 0:
		return nil, nil
	case 1:
		return matches[0], nil
	default:
		for _, p := range matches {
			unix.Close(p.fd)
		}
		return nil, fmt.Errorf("node: %d processes use sandbox %s's owned paths", len(matches), id)
	}
}

// verifyQMP proves the pinned process owns its VM's fixed QMP socket.
func (m *Manager) verifyQMP(ctx context.Context, pinned *pinnedProc, meta storage.VMMeta) (*qmp.Client, error) {
	return m.verifyQMPAt(ctx, pinned, meta, filepath.Join(m.store.VMDir(meta.SandboxID), qemu.QMPFilename))
}

// resumePaused safely resumes a VM found paused after a restart. A guest
// frozen by QMP cannot answer Ping, so recovery would otherwise wait for the
// whole probe deadline and then stop a healthy VM.
func resumePaused(ctx context.Context, client *qmp.Client) error {
	if err := client.Execute(ctx, "cont", nil, nil); err != nil {
		return fmt.Errorf("resume paused vm: %w", err)
	}
	var status struct {
		Running bool   `json:"running"`
		Status  string `json:"status"`
	}
	if err := client.Execute(ctx, "query-status", nil, &status); err != nil {
		return fmt.Errorf("query-status after resume: %w", err)
	}
	if !status.Running || status.Status != "running" {
		return fmt.Errorf("resume paused vm: status %q (running=%t)", status.Status, status.Running)
	}
	return nil
}

// adoptedProcess is a QEMU process found after a node restart. It was not
// started by this process, so it has no os/exec state: the pidfd is the
// authority for liveness and signalling, and QMP for control.
type adoptedProcess struct {
	id     string
	pinned *pinnedProc
	client *qmp.Client

	done chan struct{}

	mu  sync.Mutex
	err error
}

func newAdoptedProcess(id string, pinned *pinnedProc, client *qmp.Client) *adoptedProcess {
	return &adoptedProcess{id: id, pinned: pinned, client: client, done: make(chan struct{})}
}

// watch waits for the process to exit and closes Done. Until the process is
// gone the pidfd matches one specific incarnation, so liveness cannot be
// confused by pid reuse. Cancellation (node shutdown) abandons the watcher
// without closing Done: the process may still be running and the next node
// process adopts it.
func (p *adoptedProcess) watch(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			// Explicit owner shutdown: leave the pidfd open for Release.
			return
		}
		exited, err := p.pinned.pollForExit(500 * time.Millisecond)
		switch {
		case errors.Is(err, unix.EINTR):
			continue
		case err != nil:
			// Only PIDFD readability proves exit. An unknown poll failure
			// keeps the pidfd and the VM's protection until the manager
			// closes this process explicitly; Done stays open.
			p.setErr(fmt.Errorf("qemu: %s: poll pid %d unknown: %w", p.id, p.pinned.pid, err))
			return
		case exited:
			p.setErr(fmt.Errorf("qemu: %s: process %d exited", p.id, p.pinned.pid))
			close(p.done)
			// Only proven death releases the pidfd.
			p.pinned.close()
			return
		}
	}
}

func (p *adoptedProcess) setErr(err error) {
	p.mu.Lock()
	p.err = err
	p.mu.Unlock()
}

func (p *adoptedProcess) PID() int { return p.pinned.pid }

func (p *adoptedProcess) Done() <-chan struct{} { return p.done }

func (p *adoptedProcess) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// Stop asks QEMU to quit, waits, then kills through the pidfd. The pidfd is
// proof the signal reaches only the verified process. Cancellation of ctx
// never leaves the process behind: the kill and reap fallback use an
// independent bounded cleanup context.
func (p *adoptedProcess) Stop(ctx context.Context) error {
	select {
	case <-p.done:
		return nil
	default:
	}
	quitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	err := p.client.Execute(quitCtx, "quit", nil, nil)
	cancel()
	if err != nil && ctx.Err() == nil {
		// QEMU may close the socket before answering quit.
		p.client.Close()
	}
	grace := min(qemu.DefaultStopGrace, 10*time.Second)
	timer := time.NewTimer(grace)
	defer timer.Stop()

	waitCtx, waitCancel := context.WithTimeout(context.WithoutCancel(ctx), grace+10*time.Second)
	defer waitCancel()
	select {
	case <-p.done:
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		return nil
	case <-timer.C:
	case <-ctx.Done():
	case <-waitCtx.Done():
	}
	if err := p.pinned.signal(unix.SIGKILL); err != nil && p.pinned.alive() {
		return err
	}
	select {
	case <-p.done:
	case <-waitCtx.Done():
		return fmt.Errorf("qemu: %s: process %d did not exit after SIGKILL", p.id, p.pinned.pid)
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return nil
}

func (p *adoptedProcess) Close() error { return p.client.Close() }

// QMP exposes the verified control connection so the manager can drain
// asynchronous events (the queue is bounded and fails the connection when
// its single consumer falls behind).
func (p *adoptedProcess) QMP() *qmp.Client { return p.client }

// Release closes the pidfd without signalling. It is called only on explicit
// owner shutdown, after every watcher has been joined; the next node process
// rescans and re-proves ownership if the VM is still alive.
func (p *adoptedProcess) Release() error { return p.pinned.close() }

// qemuVersion returns the configured version, or detects it from the QEMU
// binary. Detection is best effort: Info still works on a host without QEMU
// (tests), just with an empty version.
func qemuVersion(ctx context.Context, configured string, log *slog.Logger) string {
	if configured != "" {
		return configured
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, qemu.DefaultBinary, "--version").Output()
	if err != nil {
		log.Debug("qemu: version detection failed", "error", err)
		return ""
	}
	// "QEMU emulator version 10.0.13 (Debian ...)"
	fields := strings.Fields(string(out))
	for i, f := range fields {
		if f == "version" && i+1 < len(fields) {
			return fields[i+1]
		}
	}
	return ""
}
