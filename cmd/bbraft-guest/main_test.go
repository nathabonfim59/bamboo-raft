package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
)

func TestEnvName(t *testing.T) {
	for flagName, want := range map[string]string{
		"guest.vsock-port":   "BBRAFT_GUEST_VSOCK_PORT",
		"guest.vsock-cid":    "BBRAFT_GUEST_VSOCK_CID",
		"guest.home":         "BBRAFT_GUEST_HOME",
		"guest.freeze-lease": "BBRAFT_GUEST_FREEZE_LEASE",
	} {
		if got := envName(flagName); got != want {
			t.Errorf("envName(%q) = %q, want %q", flagName, got, want)
		}
	}
}

func TestParseFlagsDefaults(t *testing.T) {
	f, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if f.port != 10789 || f.cid != 0 || f.uid != 1000 || f.gid != 1000 {
		t.Fatalf("flags %+v", f)
	}
	if f.home != "/home/user" || f.tmp != "/tmp" {
		t.Fatalf("flags %+v", f)
	}
	if f.freezeLease != 2*time.Minute || f.watchdogRetry != 5*time.Second {
		t.Fatalf("flags %+v", f)
	}
}

func TestParseFlagsEnvFallback(t *testing.T) {
	t.Setenv("BBRAFT_GUEST_VSOCK_PORT", "12345")
	t.Setenv("BBRAFT_GUEST_UID", "1500")
	t.Setenv("BBRAFT_GUEST_HOME", "/x/home")
	t.Setenv("BBRAFT_GUEST_FREEZE_LEASE", "90s")
	f, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if f.port != 12345 || f.uid != 1500 || f.home != "/x/home" || f.freezeLease != 90*time.Second {
		t.Fatalf("flags %+v", f)
	}
}

// A valid flag always wins over a valid environment default.
func TestParseFlagsFlagOverridesEnv(t *testing.T) {
	t.Setenv("BBRAFT_GUEST_VSOCK_PORT", "12345")
	t.Setenv("BBRAFT_GUEST_UID", "1500")
	t.Setenv("BBRAFT_GUEST_FREEZE_LEASE", "90s")
	f, err := parseFlags([]string{
		"-guest.vsock-port=23456",
		"-guest.uid=1600",
		"-guest.freeze-lease=45s",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if f.port != 23456 || f.uid != 1600 || f.freezeLease != 45*time.Second {
		t.Fatalf("flags %+v", f)
	}
}

// Malformed environment values are configuration errors, not silent
// fallbacks, and the message names the variable without echoing the value.
func TestParseFlagsRejectsMalformedEnv(t *testing.T) {
	for _, tt := range []struct {
		env   string
		value string
	}{
		{"BBRAFT_GUEST_VSOCK_PORT", "not-a-number"},
		{"BBRAFT_GUEST_VSOCK_CID", "-1"},
		{"BBRAFT_GUEST_UID", "user"},
		{"BBRAFT_GUEST_GID", "1.5"},
		{"BBRAFT_GUEST_FREEZE_LEASE", "soon"},
		{"BBRAFT_GUEST_WATCHDOG_RETRY", "-"},
	} {
		t.Run(tt.env, func(t *testing.T) {
			t.Setenv(tt.env, tt.value)
			_, err := parseFlags(nil, io.Discard)
			if err == nil {
				t.Fatalf("%s=%q accepted", tt.env, tt.value)
			}
			if !strings.Contains(err.Error(), tt.env) {
				t.Fatalf("error %q does not name %s", err, tt.env)
			}
			if strings.Contains(err.Error(), tt.value) {
				t.Fatalf("error %q echoes the malformed value", err)
			}
		})
	}
}

func TestParseFlagsRejectsBadValues(t *testing.T) {
	for _, args := range [][]string{
		{"-guest.vsock-port=0"},
		{"-guest.vsock-port=4294967296"},
		{"-guest.vsock-cid=4294967296"},
		{"-guest.uid=0"},
		{"-guest.uid=-1"},
		{"-guest.uid=4294967295"}, // setuid "no change" sentinel
		{"-guest.gid=4294967295"},
		{"-guest.uid=4294967294", "extra-arg"},
		{"-guest.freeze-lease=0s"},
		{"-guest.watchdog-retry=-1s"},
	} {
		if _, err := parseFlags(args, io.Discard); err == nil {
			t.Errorf("parseFlags(%v) accepted bad values", args)
		}
	}
}

// A flag is authoritative: an explicit valid value overrides even a
// malformed BBRAFT_* fallback for the same setting.
func TestParseFlagsValidFlagOverridesMalformedEnv(t *testing.T) {
	for _, tt := range []struct {
		env   string
		value string
		args  []string
		check func(*flags) bool
	}{
		{"BBRAFT_GUEST_VSOCK_CID", "junk", []string{"-guest.vsock-cid=7"}, func(f *flags) bool { return f.cid == 7 }},
		{"BBRAFT_GUEST_VSOCK_PORT", "junk", []string{"-guest.vsock-port=23456"}, func(f *flags) bool { return f.port == 23456 }},
		{"BBRAFT_GUEST_UID", "user", []string{"-guest.uid=1600"}, func(f *flags) bool { return f.uid == 1600 }},
		{"BBRAFT_GUEST_GID", "1.5", []string{"-guest.gid=1700"}, func(f *flags) bool { return f.gid == 1700 }},
		{"BBRAFT_GUEST_FREEZE_LEASE", "soon", []string{"-guest.freeze-lease=45s"}, func(f *flags) bool { return f.freezeLease == 45*time.Second }},
		{"BBRAFT_GUEST_WATCHDOG_RETRY", "-", []string{"-guest.watchdog-retry=2s"}, func(f *flags) bool { return f.watchdogRetry == 2*time.Second }},
	} {
		t.Run(tt.env, func(t *testing.T) {
			t.Setenv(tt.env, tt.value)
			f, err := parseFlags(tt.args, io.Discard)
			if err != nil {
				t.Fatalf("%s overridden by %v: %v", tt.env, tt.args, err)
			}
			if !tt.check(f) {
				t.Fatalf("flags %+v did not take %v", f, tt.args)
			}
		})
	}
}

func TestRunHelpSkipsRootRecovery(t *testing.T) {
	recoverRoot := func(context.Context, time.Duration) error {
		t.Fatal("help issued a root thaw ioctl")
		return nil
	}
	var out bytes.Buffer
	if err := runWithDeps([]string{"-h"}, &out, recoverRoot); err != nil {
		t.Fatalf("help: %v", err)
	}
	if !strings.Contains(out.String(), "Usage") {
		t.Fatalf("help output %q", out.String())
	}
}

func TestRunBuffersParseErrorUntilRootRecovery(t *testing.T) {
	var out bytes.Buffer
	outputBeforeRecovery := "sentinel"
	recovered := false
	recoverRoot := func(context.Context, time.Duration) error {
		// The parse diagnostic must still be buffered here: writing to a
		// possibly frozen root before FITHAW can block forever.
		outputBeforeRecovery = out.String()
		recovered = true
		return nil
	}
	if err := runWithDeps([]string{"-not-a-flag"}, &out, recoverRoot); err == nil {
		t.Fatal("invalid flags succeeded")
	}
	if outputBeforeRecovery != "" {
		t.Fatalf("parse diagnostics written before root recovery: %q", outputBeforeRecovery)
	}
	if !recovered || out.Len() == 0 {
		t.Fatalf("recovered=%t, buffered output=%q", recovered, out.String())
	}
}

// TestGuestSignalHelper is a real subprocess helper. It blocks startup FITHAW
// on marker files so the parent can deliver SIGTERM during recovery.
func TestGuestSignalHelper(t *testing.T) {
	if os.Getenv("BBRAFT_GUEST_SIGNAL_HELPER") != "1" {
		return
	}
	entered := os.Getenv("BBRAFT_GUEST_SIGNAL_ENTERED")
	release := os.Getenv("BBRAFT_GUEST_SIGNAL_RELEASE")
	recoverRoot := func(context.Context, time.Duration) error {
		if err := os.WriteFile(entered, []byte("entered"), 0o600); err != nil {
			return err
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			if _, err := os.Stat(release); err == nil {
				return nil
			}
			if time.Now().After(deadline) {
				return errors.New("helper release timeout")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err := runWithDeps(nil, os.Stderr, recoverRoot); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

// TestShutdownSignalDuringRecoveryDoesNotKillAgent proves SIGTERM is caught
// before root recovery, recovery continues on Background until thaw is proven,
// and a queued shutdown exits cleanly before logging or service startup.
func TestShutdownSignalDuringRecoveryDoesNotKillAgent(t *testing.T) {
	dir := t.TempDir()
	entered := filepath.Join(dir, "entered")
	release := filepath.Join(dir, "release")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestGuestSignalHelper$")
	cmd.Env = append(os.Environ(),
		"BBRAFT_GUEST_SIGNAL_HELPER=1",
		"BBRAFT_GUEST_SIGNAL_ENTERED="+entered,
		"BBRAFT_GUEST_SIGNAL_RELEASE="+release,
	)
	var stderr syncLogBuffer
	cmd.Stderr = &stderr
	cmd.Stdout = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(entered); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("helper never entered recovery; stderr=%q", stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-waitDone:
		t.Fatalf("agent exited on SIGTERM during recovery: %v; stderr=%q", err, stderr.String())
	default:
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("agent logged before thaw was proven: %q", got)
	}
	if err := os.WriteFile(release, []byte("release"), 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("queued shutdown exit = %v; stderr=%q", err, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("agent did not exit after thaw release; stderr=%q", stderr.String())
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("queued shutdown wrote diagnostics before exiting: %q", got)
	}
}

// syncLogBuffer collects service logs without racing the shutdown helper.
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

func (b *syncLogBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

// mainTestFreezer fails every thaw until recover is called.
type mainTestFreezer struct {
	mu         sync.Mutex
	failures   int
	thawCalls  int
	closeCalls int
}

func (f *mainTestFreezer) Freeze() error { return nil }

func (f *mainTestFreezer) Thaw() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.thawCalls++
	if f.thawCalls <= f.failures {
		return errors.New("still frozen")
	}
	return nil
}

func (f *mainTestFreezer) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	return nil
}

func (f *mainTestFreezer) recover() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = f.thawCalls
}

func (f *mainTestFreezer) closes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeCalls
}

// TestShutdownThawDoesNotReturnOrLogWhileFrozen: the shutdown helper must
// keep the process alive and silent while thaw retries fail, then finish
// once a thaw succeeds.
func TestShutdownThawDoesNotReturnOrLogWhileFrozen(t *testing.T) {
	fz := &mainTestFreezer{failures: 1 << 20}
	var logs syncLogBuffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	svc, err := guest.New(guest.Config{
		Log:           log,
		HomeRoot:      t.TempDir(),
		TmpRoot:       t.TempDir(),
		WatchdogRetry: 20 * time.Millisecond,
		Freezer:       fz,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Freeze(t.Context(), &guestv1.FreezeRequest{LeaseMs: 10_000}); err != nil {
		t.Fatal(err)
	}
	logs.Reset() // drop the pre-freeze startup line; only shutdown logs matter

	done := make(chan struct{})
	go func() {
		shutdownThaw(svc, log)
		close(done)
	}()

	time.Sleep(300 * time.Millisecond)
	if out := logs.String(); out != "" {
		t.Fatalf("logged while the guest was still frozen: %q", out)
	}
	select {
	case <-done:
		t.Fatal("shutdownThaw returned while the guest was still frozen")
	default:
	}

	fz.recover()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdownThaw never returned after the thaw succeeded")
	}
	if got := fz.closes(); got != 1 {
		t.Fatalf("freezer closes = %d, want 1", got)
	}
	if out := logs.String(); out != "" {
		t.Fatalf("unexpected log after a clean thaw: %q", out)
	}
}
