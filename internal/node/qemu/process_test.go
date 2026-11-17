package qemu

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/node/qmp"
)

// TestMain doubles as the fake QEMU process. When QEMU_HELPER_MODE is set the
// test binary re-executes itself as the child Start would launch, before the
// testing package parses any flags.
func TestMain(m *testing.M) {
	if mode := os.Getenv("QEMU_HELPER_MODE"); mode != "" {
		helperMain(mode)
		return
	}
	os.Exit(m.Run())
}

func helperMain(mode string) {
	if pidfile := os.Getenv("QEMU_HELPER_PIDFILE"); pidfile != "" {
		_ = os.WriteFile(pidfile, []byte(strconv.Itoa(os.Getpid())), 0o600)
	}
	if fdFile := os.Getenv("QEMU_HELPER_FD_FILE"); fdFile != "" {
		var b strings.Builder
		for _, name := range []string{strconv.Itoa(TapFD), strconv.Itoa(VhostFD)} {
			target, err := os.Readlink("/proc/self/fd/" + name)
			if err != nil {
				fmt.Fprintf(&b, "%s=error:%v\n", name, err)
				continue
			}
			fmt.Fprintf(&b, "%s=%s\n", name, target)
		}
		_ = os.WriteFile(fdFile, []byte(b.String()), 0o600)
	}
	path := helperQMPPath()
	switch mode {
	case "exit":
		fmt.Fprintln(os.Stderr, "helper: boom")
		os.Exit(3)
	case "cpu-refused":
		if file := os.Getenv("QEMU_HELPER_CPU_FILE"); file != "" {
			_ = os.WriteFile(file, []byte(helperCPUArg()), 0o600)
		}
		fmt.Fprintln(os.Stderr, "qemu-system-x86_64: Host doesn't support requested features")
		os.Exit(1)
	case "nosocket":
		time.Sleep(30 * time.Second)
		os.Exit(0)
	case "slowgreet":
		ln, err := net.Listen("unix", path)
		if err != nil {
			os.Exit(2)
		}
		conn, err := ln.Accept()
		if err == nil {
			time.Sleep(30 * time.Second)
			conn.Close()
		}
		os.Exit(0)
	case "externalpeer":
		// Launch a separate process that owns the QMP socket after this child
		// started, so Start sees a peer pid different from its own child.
		peer := exec.Command(os.Args[0], os.Args[1:]...)
		peer.Env = append(helperEnvWithout("QEMU_HELPER_PIDFILE", "QEMU_HELPER_PEER_PIDFILE"), "QEMU_HELPER_MODE=peerchild")
		peer.Stdout = io.Discard
		peer.Stderr = io.Discard
		peer.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := peer.Start(); err != nil {
			os.Exit(2)
		}
		if peerFile := os.Getenv("QEMU_HELPER_PEER_PIDFILE"); peerFile != "" {
			_ = os.WriteFile(peerFile, []byte(strconv.Itoa(peer.Process.Pid)), 0o600)
		}
		time.Sleep(30 * time.Second)
		os.Exit(0)
	case "peerchild":
		helperServe(path, "normal")
		// Stay alive after the client disconnects so the test can prove the
		// unrelated socket owner was not killed.
		time.Sleep(30 * time.Second)
		os.Exit(0)
	default:
		helperServe(path, mode)
	}
}

func helperEnvWithout(keys ...string) []string {
	drop := make(map[string]bool, len(keys))
	for _, k := range keys {
		drop[k] = true
	}
	var env []string
	for _, kv := range os.Environ() {
		if key, _, _ := strings.Cut(kv, "="); drop[key] {
			continue
		}
		env = append(env, kv)
	}
	return env
}

func helperQMPPath() string {
	args := os.Args[1:]
	for i := 0; i+1 < len(args); i++ {
		if args[i] != "-qmp" {
			continue
		}
		spec := args[i+1]
		rest, ok := strings.CutPrefix(spec, "unix:")
		if !ok {
			return ""
		}
		if path, _, _ := strings.Cut(rest, ","); path != "" {
			return path
		}
	}
	return ""
}

func helperCPUArg() string {
	args := os.Args[1:]
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "-cpu" {
			return args[i+1]
		}
	}
	return ""
}

func helperServe(path, mode string) {
	ln, err := net.Listen("unix", path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: listen %s: %v\n", path, err)
		os.Exit(2)
	}
	conn, err := ln.Accept()
	if err != nil {
		os.Exit(2)
	}
	write := func(v any) {
		b, err := json.Marshal(v)
		if err != nil {
			os.Exit(2)
		}
		b = append(b, '\n')
		if _, err := conn.Write(b); err != nil {
			os.Exit(0)
		}
	}
	write(map[string]any{"QMP": map[string]any{
		"version": map[string]any{
			"qemu":    map[string]int{"major": 10, "minor": 0, "micro": 0},
			"package": "",
		},
		"capabilities": []string{"oob"},
	}})

	dec := jsontext.NewDecoder(conn)
	paused := false
	for {
		value, err := dec.ReadValue()
		if err != nil {
			return
		}
		var req struct {
			Execute string `json:"execute"`
			ID      uint64 `json:"id"`
		}
		if err := json.Unmarshal(value, &req); err != nil {
			os.Exit(2)
		}
		ret := map[string]any{"id": req.ID}
		switch req.Execute {
		case "qmp_capabilities":
			ret["return"] = map[string]any{}
		case "quit":
			if mode == "ignorequit" {
				continue
			}
			ret["return"] = map[string]any{}
			write(ret)
			os.Exit(0)
		case "stop":
			paused = true
			ret["return"] = map[string]any{}
		case "cont":
			paused = false
			ret["return"] = map[string]any{}
		case "query-status":
			status := "running"
			if paused {
				status = "paused"
			}
			if mode == "stopdesync" && paused {
				status = "running"
			}
			if mode == "contdesync" && !paused {
				status = "paused"
			}
			reply := map[string]any{"running": status == "running"}
			omitStatus := mode == "emptystatus" || (mode == "contemptystatus" && !paused)
			if !omitStatus {
				reply["status"] = status
			}
			ret["return"] = reply
		case "query-block":
			node := "rootdisk"
			if mode == "norootdisk" {
				node = "otherdisk"
			}
			ioStatus := "ok"
			switch mode {
			case "ioerror":
				ioStatus = "failed"
			case "noiostatus":
				ioStatus = ""
			}
			entry := map[string]any{
				"device":   "virtio-disk0",
				"inserted": map[string]any{"node-name": node},
			}
			if ioStatus != "" {
				entry["io-status"] = ioStatus
			}
			ret["return"] = []any{entry}
		default:
			ret["error"] = map[string]any{"class": "CommandNotFound", "desc": "helper: " + req.Execute}
		}
		write(ret)
	}
}

func helperConfig(t *testing.T, mode string) Config {
	t.Helper()
	cfg := testConfig(t)
	t.Setenv("QEMU_HELPER_MODE", mode)
	t.Setenv("QEMU_HELPER_PIDFILE", filepath.Join(t.TempDir(), "pid"))
	return cfg
}

func helperPID(t *testing.T) int {
	t.Helper()
	b, err := os.ReadFile(os.Getenv("QEMU_HELPER_PIDFILE"))
	if err != nil {
		t.Fatalf("read helper pid: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatalf("parse helper pid: %v", err)
	}
	return pid
}

func waitProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d still exists", pid)
}

// processState reads the state field from /proc/<pid>/stat. kill(pid, 0) alone
// also succeeds for a zombie, so liveness checks need this.
func processState(pid int) (byte, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, err
	}
	i := strings.LastIndexByte(string(b), ')')
	if i < 0 || i+2 >= len(b) {
		return 0, fmt.Errorf("process %d: malformed /proc stat", pid)
	}
	return b[i+2], nil
}

func waitProcessNotRunning(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		state, err := processState(pid)
		if errors.Is(err, os.ErrNotExist) || (err == nil && state == 'Z') {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d still running after kill", pid)
}

func startHelper(t *testing.T, mode string, opts StartOptions) (*Process, Config, int) {
	t.Helper()
	cfg := helperConfig(t, mode)
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	p, err := Start(t.Context(), cfg, opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Kill(ctx)
		_ = p.Close()
	})
	return p, cfg, p.PID()
}

func TestStartPassesNetworkDescriptors(t *testing.T) {
	cfg := helperConfig(t, "normal")
	cfg.Network = testNetwork(t)
	fdFile := filepath.Join(t.TempDir(), "child-fds")
	t.Setenv("QEMU_HELPER_FD_FILE", fdFile)
	pid, err := func() (int, error) {
		p, err := Start(t.Context(), cfg, StartOptions{})
		if err != nil {
			return 0, err
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = p.Kill(ctx)
			_ = p.Close()
		})
		return p.PID(), nil
	}()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	raw, err := os.ReadFile(fdFile)
	if err != nil {
		t.Fatalf("read child descriptor table: %v", err)
	}
	got := string(raw)
	tapWant := strconv.Itoa(TapFD) + "=" + cfg.Network.Tap.Name() + "\n"
	vhostWant := strconv.Itoa(VhostFD) + "=" + cfg.Network.Vhost.Name() + "\n"
	if want := tapWant + vhostWant; got != want {
		t.Fatalf("child descriptors = %q, want %q", got, want)
	}

	// Start never closes the caller's files: they stay valid until the caller
	// closes them, and the child keeps its own references afterwards.
	if _, err := cfg.Network.Tap.Stat(); err != nil {
		t.Fatalf("Start closed the caller's tap descriptor: %v", err)
	}
	if err := cfg.Network.Tap.Close(); err != nil {
		t.Fatalf("close tap after Start: %v", err)
	}
	if err := cfg.Network.Vhost.Close(); err != nil {
		t.Fatalf("close vhost after Start: %v", err)
	}
	for _, fd := range []string{strconv.Itoa(TapFD), strconv.Itoa(VhostFD)} {
		if _, err := os.Stat("/proc/" + strconv.Itoa(pid) + "/fd/" + fd); err != nil {
			t.Fatalf("child lost fd %s after the caller closed its copy: %v", fd, err)
		}
	}
}

func TestProcessLifecycle(t *testing.T) {
	p, cfg, pid := startHelper(t, "normal", StartOptions{})
	if pid <= 0 {
		t.Fatalf("pid = %d", pid)
	}
	if p.QMPPath() != cfg.QMPPath() || p.SerialPath() != cfg.SerialPath() {
		t.Errorf("paths = %q/%q, want %q/%q", p.QMPPath(), p.SerialPath(), cfg.QMPPath(), cfg.SerialPath())
	}
	client := p.QMP()
	if client == nil {
		t.Fatal("QMP client is nil")
	}
	if got := client.Greeting().Version.String(); got != "10.0.0" {
		t.Errorf("QEMU version = %q", got)
	}

	if err := p.Pause(t.Context()); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if err := p.Resume(t.Context()); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	if err := p.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-p.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done did not close after Stop")
	}
	if err := p.Err(); err != nil {
		t.Fatalf("Err after clean stop: %v", err)
	}
	if err := client.Err(); !errors.Is(err, qmp.ErrClosed) {
		t.Errorf("QMP Err after exit = %v, want ErrClosed", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("Close after exit: %v", err)
	}
}

func TestPauseDetectsDesync(t *testing.T) {
	p, _, _ := startHelper(t, "stopdesync", StartOptions{})
	err := p.Pause(t.Context())
	if err == nil || !strings.Contains(err.Error(), "status is") {
		t.Fatalf("Pause = %v, want status error", err)
	}
}

func TestPauseDetectsBlockError(t *testing.T) {
	p, _, _ := startHelper(t, "ioerror", StartOptions{})
	err := p.Pause(t.Context())
	if err == nil || !strings.Contains(err.Error(), "io-status") {
		t.Fatalf("Pause = %v, want io-status error", err)
	}
}

func TestPauseMissingStatus(t *testing.T) {
	p, _, _ := startHelper(t, "emptystatus", StartOptions{})
	err := p.Pause(t.Context())
	if err == nil || !strings.Contains(err.Error(), "no status") {
		t.Fatalf("Pause = %v, want missing status error", err)
	}
}

func TestPauseMissingRootDisk(t *testing.T) {
	p, _, _ := startHelper(t, "norootdisk", StartOptions{})
	err := p.Pause(t.Context())
	if err == nil || !strings.Contains(err.Error(), "rootdisk") {
		t.Fatalf("Pause = %v, want missing rootdisk error", err)
	}
}

func TestPauseMissingIOStatus(t *testing.T) {
	p, _, _ := startHelper(t, "noiostatus", StartOptions{})
	err := p.Pause(t.Context())
	if err == nil || !strings.Contains(err.Error(), "io-status") {
		t.Fatalf("Pause = %v, want missing io-status error", err)
	}
}

func TestResumeDetectsBadStatus(t *testing.T) {
	p, _, _ := startHelper(t, "contdesync", StartOptions{})
	err := p.Resume(t.Context())
	if err == nil || !strings.Contains(err.Error(), "status is") {
		t.Fatalf("Resume = %v, want status error", err)
	}
}

func TestResumeMissingStatus(t *testing.T) {
	p, _, _ := startHelper(t, "contemptystatus", StartOptions{})
	err := p.Resume(t.Context())
	if err == nil || !strings.Contains(err.Error(), "no status") {
		t.Fatalf("Resume = %v, want missing status error", err)
	}
}

func TestStopKillFallback(t *testing.T) {
	p, _, pid := startHelper(t, "ignorequit", StartOptions{StopGrace: 100 * time.Millisecond})
	start := time.Now()
	if err := p.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("Stop took %v", elapsed)
	}
	waitProcessGone(t, pid)
}

func TestStopCancelledStillKills(t *testing.T) {
	p, _, pid := startHelper(t, "ignorequit", StartOptions{StopGrace: 5 * time.Second})
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	err := p.Stop(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want deadline exceeded", err)
	}
	waitProcessGone(t, pid)
}

func TestKill(t *testing.T) {
	p, _, pid := startHelper(t, "ignorequit", StartOptions{})
	if err := p.Kill(t.Context()); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	waitProcessGone(t, pid)
}

func TestStartupCancelledReapsChild(t *testing.T) {
	cfg := helperConfig(t, "nosocket")
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Start(ctx, cfg, StartOptions{Logger: slog.New(slog.DiscardHandler)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("Start took %v", elapsed)
	}
	pid := helperPID(t)
	waitProcessGone(t, pid)
}

func TestStartupGreetingTimeoutReapsChild(t *testing.T) {
	cfg := helperConfig(t, "slowgreet")
	_, err := Start(t.Context(), cfg, StartOptions{
		Logger:     slog.New(slog.DiscardHandler),
		QMPTimeout: 150 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("Start succeeded, want timeout")
	}
	pid := helperPID(t)
	waitProcessGone(t, pid)
}

func TestStartupExitErrorIncludesStderr(t *testing.T) {
	cfg := helperConfig(t, "exit")
	_, err := Start(t.Context(), cfg, StartOptions{Logger: slog.New(slog.DiscardHandler)})
	if err == nil {
		t.Fatal("Start succeeded, want exit error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "exited during startup") || !strings.Contains(msg, "boom") {
		t.Fatalf("Start error = %v, want exit and stderr", err)
	}
}

func TestStartRefusesExistingSocket(t *testing.T) {
	cfg := helperConfig(t, "normal")
	if err := os.WriteFile(cfg.QMPPath(), []byte("someone else"), 0o600); err != nil {
		t.Fatalf("create socket file: %v", err)
	}
	_, err := Start(t.Context(), cfg, StartOptions{Logger: slog.New(slog.DiscardHandler)})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("Start = %v, want already exists", err)
	}
	if _, err := os.Stat(os.Getenv("QEMU_HELPER_PIDFILE")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("helper was started despite existing socket (pidfile err=%v)", err)
	}
}

func TestLifetimeIndependentOfStartContext(t *testing.T) {
	cfg := helperConfig(t, "normal")
	startCtx, cancel := context.WithCancel(t.Context())
	p, err := Start(startCtx, cfg, StartOptions{Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = p.Kill(ctx)
		_ = p.Close()
	})
	cancel()

	select {
	case <-p.Done():
		t.Fatal("VM died when the Start context was canceled")
	case <-time.After(150 * time.Millisecond):
	}
	if err := p.Pause(t.Context()); err != nil {
		t.Fatalf("Pause after Start context cancel: %v", err)
	}
	if err := p.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStartupPeerPIDMismatch(t *testing.T) {
	cfg := helperConfig(t, "externalpeer")
	peerFile := filepath.Join(t.TempDir(), "peer-pid")
	t.Setenv("QEMU_HELPER_PEER_PIDFILE", peerFile)

	_, err := Start(t.Context(), cfg, StartOptions{Logger: slog.New(slog.DiscardHandler)})
	if err == nil || !strings.Contains(err.Error(), "owned by pid") {
		t.Fatalf("Start = %v, want peer pid mismatch", err)
	}
	owner := helperPID(t)
	waitProcessGone(t, owner)

	b, err := os.ReadFile(peerFile)
	if err != nil {
		t.Fatalf("read peer pid: %v", err)
	}
	peer, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatalf("parse peer pid: %v", err)
	}
	// Register cleanup immediately: every later failure still kills only this
	// unrelated fixture process.
	t.Cleanup(func() {
		_ = syscall.Kill(peer, syscall.SIGKILL)
		waitProcessNotRunning(t, peer)
	})
	if pgid, err := syscall.Getpgid(peer); err != nil || pgid != peer {
		t.Fatalf("unrelated socket owner is not in its own process group: pgid=%d err=%v", pgid, err)
	}
	state, err := processState(peer)
	if err != nil {
		t.Fatalf("unrelated socket owner pid %d is gone after owned child cleanup: %v", peer, err)
	}
	if state == 'Z' {
		t.Fatalf("unrelated socket owner pid %d is a zombie", peer)
	}
}

type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestStartEnforcedDefaultRefusalReaps(t *testing.T) {
	tests := []struct {
		name string
		mode NestedMode
		want string
	}{
		{name: "default auto adds vmx", mode: NestedAuto, want: "Haswell-noTSX,+vmx,enforce"},
		{name: "default off has no nested feature", mode: NestedOff, want: "Haswell-noTSX,enforce"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := helperConfig(t, "cpu-refused")
			cfg.Nested = tt.mode
			if cfg.AllowHostCPU {
				t.Fatal("production-default refusal test must not enable AllowHostCPU")
			}
			cpuFile := filepath.Join(t.TempDir(), "cpu-arg")
			t.Setenv("QEMU_HELPER_CPU_FILE", cpuFile)

			_, err := Start(t.Context(), cfg, StartOptions{Logger: slog.New(slog.DiscardHandler)})
			if err == nil || !strings.Contains(err.Error(), "Host doesn't support requested features") {
				t.Fatalf("Start = %v, want QEMU CPU enforce error", err)
			}
			pid := helperPID(t)
			waitProcessGone(t, pid)

			b, err := os.ReadFile(cpuFile)
			if err != nil {
				t.Fatalf("helper did not record its -cpu argument: %v", err)
			}
			got := strings.TrimSpace(string(b))
			if got != tt.want {
				t.Fatalf("QEMU -cpu = %q, want enforced default %q", got, tt.want)
			}
			if strings.Contains(got, "host") {
				t.Fatalf("production default accidentally used the test host model: %q", got)
			}
		})
	}
}

func TestStartAllowHostCPULogsTestOverride(t *testing.T) {
	cfg := helperConfig(t, "normal")
	cfg.CPUModel = "host"
	cfg.AllowHostCPU = true
	cfg.Nested = NestedAuto
	cfg.HostCPU = &HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 8}
	var log syncLogBuffer
	p, err := Start(t.Context(), cfg, StartOptions{
		Logger: slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelInfo})),
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Kill(ctx)
		_ = p.Close()
	})
	if err := p.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	text := log.String()
	if !strings.Contains(text, "test CPU model override") || !strings.Contains(text, "model=host") {
		t.Fatalf("missing test CPU override log:\n%s", text)
	}
	if strings.Contains(text, "fixed Intel CPU model") {
		t.Fatalf("test host still logged the fixed-Intel AMD reason:\n%s", text)
	}
}

func TestStartAMDAutoLogsInfo(t *testing.T) {
	cfg := helperConfig(t, "normal")
	cfg.Nested = NestedAuto
	cfg.HostCPU = &HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 8}
	var log syncLogBuffer
	p, err := Start(t.Context(), cfg, StartOptions{
		Logger: slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelInfo})),
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Kill(ctx)
		_ = p.Close()
	})
	if err := p.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	text := log.String()
	if !strings.Contains(text, "nested virtualization disabled") || !strings.Contains(text, "AuthenticAMD") {
		t.Fatalf("AMD auto log does not explain the disabled nesting:\n%s", text)
	}
}

func TestStartNestedOnAMDRefused(t *testing.T) {
	cfg := helperConfig(t, "normal")
	cfg.Nested = NestedOn
	cfg.HostCPU = &HostCPU{Vendor: "AuthenticAMD", Svm: true, CPUs: 8}
	_, err := Start(t.Context(), cfg, StartOptions{Logger: slog.New(slog.DiscardHandler)})
	if err == nil || !strings.Contains(err.Error(), "AuthenticAMD") {
		t.Fatalf("Start = %v, want clear AMD refusal", err)
	}
	if _, statErr := os.Stat(os.Getenv("QEMU_HELPER_PIDFILE")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("helper started despite nested-on refusal (pidfile err=%v)", statErr)
	}
}

func TestStartNestedOffNeedsNoHostFacts(t *testing.T) {
	cfg := helperConfig(t, "normal")
	cfg.Nested = NestedOff
	cfg.HostCPU = nil
	p, err := Start(t.Context(), cfg, StartOptions{Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Kill(ctx)
		_ = p.Close()
	})
	if err := p.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStartupRejectsMissingDisk(t *testing.T) {
	cfg := helperConfig(t, "normal")
	if err := os.Remove(cfg.DiskPath); err != nil {
		t.Fatalf("remove disk: %v", err)
	}
	_, err := Start(t.Context(), cfg, StartOptions{Logger: slog.New(slog.DiscardHandler)})
	if err == nil {
		t.Fatal("Start succeeded without a disk")
	}
	if _, statErr := os.Stat(os.Getenv("QEMU_HELPER_PIDFILE")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("helper started despite invalid files (pidfile err=%v)", statErr)
	}
}
