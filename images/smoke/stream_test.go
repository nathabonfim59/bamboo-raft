package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// scriptedStream implements grpc.ServerStreamingClient for protocol tests.
type scriptedStream struct {
	events []*nodev1.ExecEvent
	index  int
	ctx    context.Context
}

func (s *scriptedStream) Recv() (*nodev1.ExecEvent, error) {
	if s.index >= len(s.events) {
		return nil, io.EOF
	}
	event := s.events[s.index]
	s.index++
	return event, nil
}

func (s *scriptedStream) Header() (metadata.MD, error) { return nil, nil }
func (s *scriptedStream) Trailer() metadata.MD         { return nil }
func (s *scriptedStream) CloseSend() error             { return nil }
func (s *scriptedStream) Context() context.Context     { return s.ctx }
func (s *scriptedStream) RecvMsg(any) error            { return io.EOF }
func (s *scriptedStream) SendMsg(any) error            { return nil }

// scriptedClient only implements Exec; execGuest uses nothing else.
type scriptedClient struct {
	stream  *scriptedStream
	execErr error
}

func (c *scriptedClient) Exec(ctx context.Context, _ *guestv1.ExecRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ExecEvent], error) {
	c.stream.ctx = ctx
	return c.stream, c.execErr
}

func int32Ptr(v int32) *int32 { return &v }

func startedEvent(pid int32) *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Started_{Started: &nodev1.ExecEvent_Started{Pid: pid}}}
}

func stdoutEvent(text string) *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Stdout{Stdout: []byte(text)}}
}

func stderrEvent(text string) *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Stderr{Stderr: []byte(text)}}
}

func exitEvent(code *int32) *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Exit_{Exit: &nodev1.ExecEvent_Exit{ExitCode: code}}}
}

func TestExecGuestFrameContract(t *testing.T) {
	tests := []struct {
		name    string
		events  []*nodev1.ExecEvent
		wantErr string // empty: success
		check   func(t *testing.T, res *execResult)
	}{
		{
			name:   "valid",
			events: []*nodev1.ExecEvent{startedEvent(7), stdoutEvent("out"), stderrEvent("err"), exitEvent(int32Ptr(0))},
			check: func(t *testing.T, res *execResult) {
				if res.stdout != "out" || res.stderr != "err" {
					t.Fatalf("stdout=%q stderr=%q", res.stdout, res.stderr)
				}
				if !res.exitOK() {
					t.Fatal("valid exit not accepted")
				}
			},
		},
		{name: "missing started", events: []*nodev1.ExecEvent{exitEvent(int32Ptr(0))}, wantErr: "exit before started"},
		{name: "zero pid", events: []*nodev1.ExecEvent{startedEvent(0), exitEvent(int32Ptr(0))}, wantErr: "pid 0"},
		{name: "duplicate started", events: []*nodev1.ExecEvent{startedEvent(1), startedEvent(2), exitEvent(int32Ptr(0))}, wantErr: "duplicate started"},
		{name: "stdout before started", events: []*nodev1.ExecEvent{stdoutEvent("x"), startedEvent(1), exitEvent(int32Ptr(0))}, wantErr: "stdout before started"},
		{name: "stderr before started", events: []*nodev1.ExecEvent{stderrEvent("x"), startedEvent(1), exitEvent(int32Ptr(0))}, wantErr: "stderr before started"},
		{name: "missing exit", events: []*nodev1.ExecEvent{startedEvent(1), stdoutEvent("x")}, wantErr: "without an exit"},
		{name: "duplicate exit", events: []*nodev1.ExecEvent{startedEvent(1), exitEvent(int32Ptr(0)), exitEvent(int32Ptr(0))}, wantErr: "frame after exit"},
		{name: "output after exit", events: []*nodev1.ExecEvent{startedEvent(1), exitEvent(int32Ptr(0)), stdoutEvent("late")}, wantErr: "frame after exit"},
		{name: "started after exit", events: []*nodev1.ExecEvent{startedEvent(1), exitEvent(int32Ptr(0)), startedEvent(2)}, wantErr: "frame after exit"},
		{name: "nil frame", events: []*nodev1.ExecEvent{{}}, wantErr: "unknown stream frame"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &scriptedClient{stream: &scriptedStream{events: tc.events}}
			res, err := execGuest(t.Context(), client, &guestv1.ExecRequest{Command: "test"})
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got success", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.check != nil {
				tc.check(t, res)
			}
		})
	}
}

func TestExecGuestCancelsContextOnProtocolError(t *testing.T) {
	client := &scriptedClient{stream: &scriptedStream{events: []*nodev1.ExecEvent{stdoutEvent("x")}}}
	if _, err := execGuest(t.Context(), client, &guestv1.ExecRequest{Command: "test"}); err == nil {
		t.Fatal("expected a protocol error")
	}
	if err := client.stream.ctx.Err(); err == nil {
		t.Fatal("per-command context was not canceled after the protocol error")
	}
}

func TestExitOKRequiresExitCode(t *testing.T) {
	if (&execResult{exit: &nodev1.ExecEvent_Exit{}}).exitOK() {
		t.Fatal("nil ExitCode must not be treated as success")
	}
	res := &execResult{exit: &nodev1.ExecEvent_Exit{ExitCode: int32Ptr(0)}}
	if !res.exitOK() {
		t.Fatal("exit code 0 must be success")
	}
	if err := mustExitZero("test", &execResult{exit: &nodev1.ExecEvent_Exit{}}, nil); err == nil {
		t.Fatal("mustExitZero accepted a nil ExitCode")
	}
}

func TestValidateVsock(t *testing.T) {
	tests := []struct {
		name    string
		cid     uint
		port    uint
		wantErr string
	}{
		{name: "default", cid: 42, port: 10789},
		{name: "lowest guest cid", cid: 3, port: 1},
		{name: "highest guest cid", cid: 0xfffffffe, port: 0xffffffff},
		{name: "hypervisor cid", cid: 0, port: 10789, wantErr: "invalid vsock cid"},
		{name: "local cid", cid: 1, port: 10789, wantErr: "invalid vsock cid"},
		{name: "host cid", cid: 2, port: 10789, wantErr: "invalid vsock cid"},
		{name: "reserved any cid", cid: 0xffffffff, port: 10789, wantErr: "reserved"},
		{name: "zero port", cid: 42, port: 0, wantErr: "invalid vsock port"},
		{name: "port overflow", cid: 42, port: 0x100000000, wantErr: "invalid vsock port"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateVsock(tc.cid, tc.port)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %v does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestCheckGroupGoneRejectsMissingSetup(t *testing.T) {
	tests := []struct {
		name    string
		output  string
		wantErr string
	}{
		{name: "gone", output: "gone\n"},
		{name: "missing ready", output: "missing-ready\n", wantErr: "child-ready marker is missing"},
		{name: "missing pid", output: "missing-pid\n", wantErr: "PID file is missing"},
		{name: "bad pid", output: "bad-pid\n", wantErr: "canonical positive PID"},
		{name: "unexpected", output: "surprise\n", wantErr: "unexpected group check output"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &scriptedClient{stream: &scriptedStream{events: []*nodev1.ExecEvent{
				startedEvent(1), stdoutEvent(tc.output), exitEvent(int32Ptr(0)),
			}}}
			err := checkGroupGone(t.Context(), client, "/tmp/bbraft-smoke-test", "test")
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %v does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestContainsExactLine rejects prefixes, suffixes and partial lines.
func TestContainsExactLine(t *testing.T) {
	tests := []struct {
		name string
		text string
		want bool
	}{
		{name: "exact", text: childReadyToken + "\n", want: true},
		{name: "after chatter", text: "profile noise\n" + childReadyToken + "\n", want: true},
		{name: "prefix", text: "x" + childReadyToken + "\n", want: false},
		{name: "suffix", text: childReadyToken + "x\n", want: false},
		{name: "partial", text: childReadyToken, want: false},
		{name: "partial after chatter", text: "x\n" + childReadyToken, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := containsExactLine(tc.text, childReadyToken); got != tc.want {
				t.Fatalf("containsExactLine(%q) = %v, want %v", tc.text, got, tc.want)
			}
		})
	}
}

// TestChildSetupCommandWritesReady runs the real timeout/cancel setup in its
// own process group, verifies the ready file is non-empty and exactly the
// token, and verifies the PID file is a canonical live PID before killing the
// group. It also proves the previous ": > ready" construction fails the check.
func TestChildSetupCommandWritesReady(t *testing.T) {
	for _, tc := range []struct {
		name string
		emit bool
	}{
		{name: "timeout", emit: false},
		{name: "cancel", emit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "probe")
			cmd := exec.CommandContext(t.Context(), "bash", "-c", childSetupCommand(base, tc.emit))
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			cmd.Stdout = io.Discard
			cmd.Stderr = io.Discard
			if err := cmd.Start(); err != nil {
				t.Fatalf("start setup: %v", err)
			}
			defer func() {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				_ = cmd.Wait()
			}()

			deadline := time.Now().Add(5 * time.Second)
			var ready, pid []byte
			for {
				ready, _ = os.ReadFile(base + ".ready")
				pid, _ = os.ReadFile(base + ".pid")
				if len(ready) > 0 && len(pid) > 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("setup wrote ready=%q pid=%q within 5s", ready, pid)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if got := string(ready); got != childReadyToken+"\n" {
				t.Fatalf("ready file = %q, want exact token", got)
			}
			pidText := strings.TrimSpace(string(pid))
			if pidText == "" || pidText == "0" || strings.TrimLeft(pidText, "0123456789") != "" {
				t.Fatalf("pid file = %q, want a canonical positive PID", pidText)
			}

			// With a valid setup the group probe reaches the liveness branch.
			out, err := exec.CommandContext(t.Context(), "bash", "-c", groupCheckCommand(base, 0)).CombinedOutput()
			if err != nil {
				t.Fatalf("group check: %v\n%s", err, out)
			}
			if got := strings.TrimSpace(string(out)); got != "alive" {
				t.Fatalf("group check = %q, want alive for a running child", got)
			}

			// The previous cancellation construction created an empty ready
			// file; the real probe must reject it as missing setup.
			oldBase := filepath.Join(t.TempDir(), "old")
			if err := os.WriteFile(oldBase+".ready", nil, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(oldBase+".pid", []byte("12345\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			out, err = exec.CommandContext(t.Context(), "bash", "-c", groupCheckCommand(oldBase, 0)).CombinedOutput()
			if err != nil {
				t.Fatalf("old group check: %v\n%s", err, out)
			}
			if got := strings.TrimSpace(string(out)); got != "missing-ready" {
				t.Fatalf("empty ready probe = %q, want missing-ready", got)
			}
		})
	}
}

// TestGroupCheckCommandRequiresSetup executes the real shell probe. The old
// probe (kept here as oldProbe) treated a missing PID file as "gone"; the new
// one must reject missing/malformed setup first.
func TestGroupCheckCommandRequiresSetup(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "probe")
	run := func(command string) string {
		t.Helper()
		out, err := exec.CommandContext(t.Context(), "bash", "-c", command).CombinedOutput()
		if err != nil {
			t.Fatalf("probe failed: %v\n%s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	remove := func(name string) {
		t.Helper()
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}

	newProbe := groupCheckCommand(base, 0)
	oldProbe := fmt.Sprintf(
		`sleep 0; if [ -e %[1]s.done ]; then echo done; elif [ -f %[1]s.pid ] && kill -0 "$(cat %[1]s.pid)" 2>/dev/null; then echo alive; else echo gone; fi`,
		base)

	// Setup missing entirely: the old probe says gone; the new one says why.
	if got := run(oldProbe); got != "gone" {
		t.Fatalf("old probe = %q, want gone (demonstrating the old false acceptance)", got)
	}
	if got := run(newProbe); got != "missing-ready" {
		t.Fatalf("new probe with no setup = %q, want missing-ready", got)
	}
	write("probe.ready", childReadyToken+"\n")
	if got := run(newProbe); got != "missing-pid" {
		t.Fatalf("new probe without pid = %q, want missing-pid", got)
	}
	for _, pid := range []string{"abc", "0", "07"} {
		write("probe.pid", pid+"\n")
		if got := run(newProbe); got != "bad-pid" {
			t.Fatalf("new probe with pid %q = %q, want bad-pid", pid, got)
		}
	}
	write("probe.pid", "999999\n")
	if got := run(newProbe); got != "gone" {
		t.Fatalf("new probe with dead pid = %q, want gone", got)
	}
	write("probe.done", "survived\n")
	if got := run(newProbe); got != "done" {
		t.Fatalf("new probe with completion marker = %q, want done", got)
	}
	remove("probe.done")
	remove("probe.pid")
	if got := run(oldProbe); got != "gone" {
		t.Fatalf("old probe with missing pid = %q, want gone", got)
	}
}
