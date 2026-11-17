package guest_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// execResult is the collected outcome of one Exec stream.
type execResult struct {
	Pid    int32
	Stdout string
	Stderr string
	Exit   *nodev1.ExecEvent_Exit
}

// execCollect runs one Exec and collects its stream, checking the frame
// order exactly as nodev1 specifies: one started, then output, then exit.
func execCollect(ctx context.Context, h *harness, req *guestv1.ExecRequest) (*execResult, error) {
	st, err := h.c.Exec(ctx, req)
	if err != nil {
		return nil, err
	}
	res := &execResult{}
	started := false
	for {
		ev, err := st.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if res.Exit != nil {
			return nil, errors.New("frame after exit")
		}
		switch e := ev.Event.(type) {
		case *nodev1.ExecEvent_Started_:
			if started {
				return nil, errors.New("two started frames")
			}
			started = true
			res.Pid = e.Started.GetPid()
		case *nodev1.ExecEvent_Stdout:
			if !started {
				return nil, errors.New("stdout before started")
			}
			res.Stdout += string(e.Stdout)
		case *nodev1.ExecEvent_Stderr:
			if !started {
				return nil, errors.New("stderr before started")
			}
			res.Stderr += string(e.Stderr)
		case *nodev1.ExecEvent_Exit_:
			if !started {
				return nil, errors.New("exit before started")
			}
			res.Exit = e.Exit
		default:
			return nil, errors.New("empty event")
		}
	}
	if !started {
		return nil, errors.New("stream ended without started")
	}
	if res.Exit == nil {
		return nil, errors.New("stream ended without exit")
	}
	return res, nil
}

// exec is the simple path: a command with a 60 s limit.
func (h *harness) exec(t *testing.T, cmd string) *execResult {
	t.Helper()
	res, err := execCollect(t.Context(), h, &guestv1.ExecRequest{Command: cmd, TimeoutSeconds: 60})
	if err != nil {
		t.Fatalf("Exec(%q): %v", cmd, err)
	}
	return res
}

func TestExecStartedOutputExitOrder(t *testing.T) {
	h := newHarness(t, nil)
	res := h.exec(t, "echo hello; printf 'a\\nb'")
	if res.Stdout != "hello\na\nb" || res.Stderr != "" {
		t.Fatalf("stdout %q stderr %q", res.Stdout, res.Stderr)
	}
	if res.Pid <= 0 {
		t.Fatalf("pid %d", res.Pid)
	}
	exit := res.Exit
	if exit.ExitCode == nil || *exit.ExitCode != 0 || exit.GetTimedOut() || exit.GetSignal() != "" {
		t.Fatalf("exit %v", exit)
	}
	if exit.GetDurationMs() < 0 {
		t.Fatalf("duration %d", exit.GetDurationMs())
	}
}

func TestExecStderr(t *testing.T) {
	h := newHarness(t, nil)
	res := h.exec(t, "echo out; echo err >&2")
	if res.Stdout != "out\n" || res.Stderr != "err\n" {
		t.Fatalf("stdout %q stderr %q", res.Stdout, res.Stderr)
	}
}

func TestExecNonzeroExit(t *testing.T) {
	h := newHarness(t, nil)
	res := h.exec(t, "exit 7")
	if res.Exit.ExitCode == nil || *res.Exit.ExitCode != 7 {
		t.Fatalf("exit %v, want code 7", res.Exit)
	}
}

func TestExecLargeOutput(t *testing.T) {
	h := newHarness(t, nil)
	res := h.exec(t, "head -c 3000000 /dev/zero | tr '\\0' a; head -c 1000000 /dev/zero | tr '\\0' b 1>&2")
	if len(res.Stdout) != 3000000 || strings.TrimLeft(res.Stdout, "a") != "" {
		t.Fatalf("stdout: %d bytes, trailing %q", len(res.Stdout), strings.TrimLeft(res.Stdout, "a"))
	}
	if len(res.Stderr) != 1000000 || strings.TrimLeft(res.Stderr, "b") != "" {
		t.Fatalf("stderr: %d bytes, trailing %q", len(res.Stderr), strings.TrimLeft(res.Stderr, "b"))
	}
}

// TestExecNoLimit proves timeout 0 leaves the command alone.
func TestExecNoLimit(t *testing.T) {
	h := newHarness(t, nil)
	start := time.Now()
	res, err := execCollect(t.Context(), h, &guestv1.ExecRequest{Command: "sleep 0.3; echo done", TimeoutSeconds: 0})
	if err != nil {
		t.Fatal(err)
	}
	if res.Stdout != "done\n" || res.Exit.GetTimedOut() {
		t.Fatalf("stdout %q exit %v", res.Stdout, res.Exit)
	}
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Fatalf("finished in %v; the sleep was cut short", elapsed)
	}
}

func TestExecTimeout(t *testing.T) {
	h := newHarness(t, nil)
	start := time.Now()
	res, err := execCollect(t.Context(), h, &guestv1.ExecRequest{
		Command:        "echo before; sleep 30 & sleep 30; echo after",
		TimeoutSeconds: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	exit := res.Exit
	if !exit.GetTimedOut() || exit.ExitCode != nil {
		t.Fatalf("exit %v, want timed out without exit code", exit)
	}
	if !strings.Contains(res.Stdout, "before") || strings.Contains(res.Stdout, "after") {
		t.Fatalf("stdout %q", res.Stdout)
	}
	if d := time.Since(start); d > 15*time.Second {
		t.Fatalf("timeout took %v (background child kept the stream open?)", d)
	}
}

// pidFromFile reads a pid the command wrote, retrying until it appears.
func pidFromFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		b, err := os.ReadFile(path)
		if err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && pid > 0 {
				return pid
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no pid in %s", path)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitPidGone polls until kill(pid, 0) fails, meaning the process was
// reaped and no longer exists.
func waitPidGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("pid %d still exists", pid)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestExecTimeoutKillsGrandchildren: a grandchild spawned into the group
// dies with the group kill on timeout.
func TestExecTimeoutKillsGrandchildren(t *testing.T) {
	h := newHarness(t, nil)
	res, err := execCollect(t.Context(), h, &guestv1.ExecRequest{
		Command:        "bash -c 'sleep 30' & echo $! > gc.pid; sleep 30",
		TimeoutSeconds: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Exit.GetTimedOut() {
		t.Fatalf("exit %v", res.Exit)
	}
	waitPidGone(t, pidFromFile(t, h.home+"/gc.pid"))
}

// TestExecCancelKillsGroup cancels the RPC mid-run: the stream errors and
// the whole process group dies without finishing its work.
func TestExecCancelKillsGroup(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	st, err := h.c.Exec(ctx, &guestv1.ExecRequest{
		Command:        "echo $$ > cancel.pid; sleep 30; touch cancel-finished",
		TimeoutSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recv(); err != nil { // started
		t.Fatal(err)
	}
	pid := pidFromFile(t, h.home+"/cancel.pid")
	cancel()
	for {
		if _, err := st.Recv(); err != nil {
			break // the client sees the stream end with an error
		}
	}
	waitPidGone(t, pid)
	if _, err := os.Stat(h.home + "/cancel-finished"); !os.IsNotExist(err) {
		t.Fatalf("canceled command finished its work: %v", err)
	}
}

// TestExecEnv proves the base environment, that request values win, and
// that the environment never changes the effective uid.
func TestExecEnv(t *testing.T) {
	h := newHarness(t, nil)
	res, err := execCollect(t.Context(), h, &guestv1.ExecRequest{
		Command: `printf '%s\n' "$USER" "$LOGNAME" "$SHELL" "$LANG" "$TERM" "$BBRAFT_REQ_VAR"; id -u`,
		Env:     map[string]string{"BBRAFT_REQ_VAR": "from-req", "USER": "hax"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "hax\nuser\n/bin/bash\nC.UTF-8\ndumb\nfrom-req\n" + strconv.Itoa(h.uid) + "\n"
	if res.Stdout != want {
		t.Fatalf("stdout %q, want %q", res.Stdout, want)
	}
	if res.Exit.ExitCode == nil || *res.Exit.ExitCode != 0 {
		t.Fatalf("exit %v", res.Exit)
	}
}

func TestExecCwd(t *testing.T) {
	h := newHarness(t, nil)
	h.exec(t, "mkdir -p cwdtest/sub && echo x > cwdtest/sub/f")

	for _, tt := range []struct {
		cwd      string
		command  string
		want     string
		wantCode codes.Code
	}{
		{cwd: "cwdtest/sub", command: "cat f", want: "x\n"},
		{cwd: "/home/user/cwdtest/sub", command: "cat f", want: "x\n"},
		{cwd: "", command: "test -f cwdtest/sub/f && echo yes", want: "yes\n"},
		{cwd: "/tmp", command: "pwd", want: h.tmp + "\n"},
		{cwd: "/etc", command: "true", wantCode: codes.InvalidArgument},
		{cwd: "../escape", command: "true", wantCode: codes.InvalidArgument},
		{cwd: "cwdtest/nope", command: "true", wantCode: codes.InvalidArgument},
		{cwd: "/home/user/../../etc", command: "true", wantCode: codes.InvalidArgument},
	} {
		res, err := execCollect(t.Context(), h, &guestv1.ExecRequest{Command: tt.command, Cwd: tt.cwd, TimeoutSeconds: 60})
		if tt.wantCode != 0 {
			wantCode(t, err, tt.wantCode)
			continue
		}
		if err != nil {
			t.Fatalf("cwd %q: %v", tt.cwd, err)
		}
		if res.Stdout != tt.want {
			t.Fatalf("cwd %q: stdout %q, want %q", tt.cwd, res.Stdout, tt.want)
		}
	}

	// A symlink out of the home root is rejected by os.Root; one that
	// stays inside keeps working.
	if err := os.Symlink("/etc", h.home+"/etclink"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("cwdtest", h.home+"/inlink"); err != nil {
		t.Fatal(err)
	}
	_, err := execCollect(t.Context(), h, &guestv1.ExecRequest{Command: "true", Cwd: "etclink", TimeoutSeconds: 60})
	wantCode(t, err, codes.InvalidArgument)
	res, err := execCollect(t.Context(), h, &guestv1.ExecRequest{Command: "cat sub/f", Cwd: "inlink", TimeoutSeconds: 60})
	if err != nil {
		t.Fatalf("cwd inlink: %v", err)
	}
	if res.Stdout != "x\n" {
		t.Fatalf("cwd inlink: stdout %q", res.Stdout)
	}
}

// TestExecConcurrentStreams races the single send owner: several streams
// at once must each stay coherent (run under -race by make check).
func TestExecConcurrentStreams(t *testing.T) {
	h := newHarness(t, nil)
	markers := []string{"zero", "one", "two", "three"}
	var wg sync.WaitGroup
	for i, marker := range markers {
		wg.Go(func() {
			res, err := execCollect(t.Context(), h, &guestv1.ExecRequest{
				Command:        "echo " + marker + "; sleep 0.2; echo " + marker + "-done",
				TimeoutSeconds: 30,
			})
			if err != nil {
				t.Errorf("stream %d: %v", i, err)
				return
			}
			if res.Stdout != marker+"\n"+marker+"-done\n" {
				t.Errorf("stream %d: stdout %q", i, res.Stdout)
			}
		})
	}
	wg.Wait()
}

// TestExecHomeIsProtected: the request environment cannot move HOME out
// of the confined home directory.
func TestExecHomeIsProtected(t *testing.T) {
	h := newHarness(t, nil)
	res, err := execCollect(t.Context(), h, &guestv1.ExecRequest{
		Command: `printf '%s' "$HOME"`,
		Env:     map[string]string{"HOME": "/"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Stdout != h.home {
		t.Fatalf("HOME moved to %q, want %q", res.Stdout, h.home)
	}
}

// TestExecRejectsMalformedEnv: keys with "=" or NUL, empty keys and NUL
// values are refused before anything is spawned.
func TestExecRejectsMalformedEnv(t *testing.T) {
	h := newHarness(t, nil)
	for name, env := range map[string]map[string]string{
		"empty key":       {"": "v"},
		"key with equals": {"A=B": "v"},
		"key with NUL":    {"A\x00B": "v"},
		"value with NUL":  {"A": "v\x00w"},
	} {
		_, err := execCollect(t.Context(), h, &guestv1.ExecRequest{Command: "true", Env: env})
		wantCode(t, err, codes.InvalidArgument)
		_ = name
	}
}

// renameExchange atomically swaps two paths with Linux renameat2(2)
// RENAME_EXCHANGE, so the cwd path flips between a real directory and an
// escaping symlink without ever being absent.
func renameExchange(a, b string) error {
	return unix.Renameat2(unix.AT_FDCWD, a, unix.AT_FDCWD, b, unix.RENAME_EXCHANGE)
}

// TestExecCwdSymlinkSwapRace performs a real rename swap: a nonempty
// directory containing a marker trades places with a symlink to /etc. The
// old path-based cwd can therefore land in /etc while a pinned descriptor
// always reaches the marker. Both outcomes are counted: rejected symlink
// accesses and successful executions of the pinned inode.
func TestExecCwdSymlinkSwapRace(t *testing.T) {
	h := newHarness(t, nil)
	const dir, other = "swapdir", "swapdir-other"
	if err := os.MkdirAll(filepath.Join(h.home, dir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.home, dir, "marker"), []byte("inside\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc", filepath.Join(h.home, other)); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	swapped := make(chan error, 1)
	var swaps int
	go func() {
		for {
			select {
			case <-stop:
				swapped <- nil
				return
			default:
			}
			if err := renameExchange(filepath.Join(h.home, dir), filepath.Join(h.home, other)); err != nil {
				swapped <- err
				return
			}
			swaps++
			time.Sleep(100 * time.Microsecond) // keep the exchange window realistic
		}
	}()
	var stopOnce sync.Once
	var swapErr error
	stopSwap := func() {
		stopOnce.Do(func() {
			close(stop)
			swapErr = <-swapped
		})
	}
	defer stopSwap()

	deadline := time.Now().Add(5 * time.Second)
	runs := 0
	rejected := 0
	for time.Now().Before(deadline) {
		res, err := execCollect(t.Context(), h, &guestv1.ExecRequest{
			Command:        "pwd; cat marker 2>/dev/null",
			Cwd:            dir,
			TimeoutSeconds: 30,
		})
		if err != nil {
			wantCode(t, err, codes.InvalidArgument) // caught the symlink: fine
			rejected++
			continue
		}
		lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
		if len(lines) != 2 || lines[1] != "inside" {
			t.Fatalf("exec did not run in the pinned directory: %q", res.Stdout)
		}
		if lines[0] != filepath.Join(h.home, dir) && lines[0] != filepath.Join(h.home, other) {
			t.Fatalf("exec escaped into %q", lines[0])
		}
		runs++
	}
	stopSwap()
	if swapErr != nil {
		t.Fatalf("rename exchange: %v", swapErr)
	}
	if swaps == 0 {
		t.Fatal("the directory was never swapped")
	}
	if runs == 0 {
		t.Fatal("no execution reached the pinned directory")
	}
	if rejected == 0 {
		t.Fatal("no execution observed and rejected the escaping symlink")
	}
	t.Logf("%d swaps, %d pinned runs, %d symlink rejections", swaps, runs, rejected)
}

// procStart reads the kernel start time of pid, used to avoid killing a
// recycled pid.
func procStart(pid int) (string, bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return "", false
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return "", false
	}
	fields := strings.Fields(s[i+1:])
	if len(fields) < 20 {
		return "", false
	}
	return fields[19], true // field 22, starttime
}

// killTestProc kills only the process it recorded, not a recycled pid.
func killTestProc(pid int, start string) {
	if got, ok := procStart(pid); ok && got == start {
		syscall.Kill(-pid, syscall.SIGKILL)
		syscall.Kill(pid, syscall.SIGKILL)
	}
}

// TestExecEscapedPipeHolder: a descendant that escapes the process group
// and holds the pipes open cannot hang the stream; WaitDelay closes them
// and the terminal exit frame still arrives. The escaped child is the
// test's own: its pid and start time are recorded and only that process
// group is killed, including on failure.
func TestExecEscapedPipeHolder(t *testing.T) {
	h := newHarness(t, nil)
	start := time.Now()
	res, err := execCollect(t.Context(), h, &guestv1.ExecRequest{
		Command:        "setsid bash -c 'echo $$ > escaped.pid; exec sleep 30' & sleep 0.3; echo done",
		TimeoutSeconds: 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	pid := pidFromFile(t, filepath.Join(h.home, "escaped.pid"))
	procIdent, ok := procStart(pid)
	if !ok {
		t.Fatalf("no start time for escaped pid %d", pid)
	}
	t.Cleanup(func() { killTestProc(pid, procIdent) })
	if !strings.Contains(res.Stdout, "done") {
		t.Fatalf("stdout %q", res.Stdout)
	}
	if res.Exit.ExitCode == nil || *res.Exit.ExitCode != 0 {
		t.Fatalf("exit %v", res.Exit)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("stream took %v; the escaped pipe holder hung it", d)
	}
	killTestProc(pid, procIdent)
	waitPidGone(t, pid)
}

// backgroundChildCommand starts a same-group background child that keeps
// the inherited stdout pipe open after the direct bash process exits.
const backgroundChildCommand = "sleep 30 & echo $! > bg.pid; echo parent-done"

// recvUntilContains drains the stream until marker appeared, proving the
// parent wrote its state before the RPC ended.
func recvUntilContains(t *testing.T, st guestv1.Guest_ExecClient, marker string) string {
	t.Helper()
	var out strings.Builder
	for {
		ev, err := st.Recv()
		if err != nil {
			t.Fatalf("stream ended before %q: %v", marker, err)
		}
		if exit := ev.GetExit(); exit != nil {
			t.Fatalf("exit before %q", marker)
		}
		out.Write(ev.GetStdout())
		out.Write(ev.GetStderr())
		if strings.Contains(out.String(), marker) {
			return out.String()
		}
	}
}

// backgroundChildPID reads the child pid and its start identity before the
// request ends, so the test can prove the process was alive and only the
// cancellation killed it.
func backgroundChildPID(t *testing.T, h *harness) (int, string) {
	t.Helper()
	pid := pidFromFile(t, filepath.Join(h.home, "bg.pid"))
	ident, ok := procStart(pid)
	if !ok {
		t.Fatalf("background child %d died before the request ended; cancellation was not what killed it", pid)
	}
	t.Cleanup(func() { killTestProc(pid, ident) })
	return pid, ident
}

// TestExecTimeoutKillsBackgroundChildAfterParentExit: the direct bash exits
// after starting a child that holds stdout; the timeout must still kill the
// group and return promptly, not rely on WaitDelay.
func TestExecTimeoutKillsBackgroundChildAfterParentExit(t *testing.T) {
	h := newHarness(t, nil)
	start := time.Now()
	st, err := h.c.Exec(t.Context(), &guestv1.ExecRequest{
		Command:        backgroundChildCommand,
		TimeoutSeconds: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	recvUntilContains(t, st, "parent-done")
	pid, _ := backgroundChildPID(t, h)

	var exit *nodev1.ExecEvent_Exit
	for {
		ev, err := st.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e := ev.GetExit(); e != nil {
			exit = e
		}
	}
	if exit == nil || !exit.GetTimedOut() {
		t.Fatalf("exit %v, want timed out", exit)
	}
	waitPidGone(t, pid)
	if d := time.Since(start); d > 4*time.Second {
		t.Fatalf("timeout took %v; the background child kept the stream open", d)
	}
}

// TestExecCancelKillsBackgroundChildAfterParentExit: same setup, but the
// RPC is canceled long after the parent exited; the child must die with the
// group and the client stream must end promptly.
func TestExecCancelKillsBackgroundChildAfterParentExit(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	st, err := h.c.Exec(ctx, &guestv1.ExecRequest{
		Command:        backgroundChildCommand,
		TimeoutSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	recvUntilContains(t, st, "parent-done")
	pid, _ := backgroundChildPID(t, h)

	canceledAt := time.Now()
	cancel()
	for {
		if _, err := st.Recv(); err != nil {
			break
		}
	}
	waitPidGone(t, pid)
	if d := time.Since(canceledAt); d > 4*time.Second {
		t.Fatalf("cancel took %v; the background child kept the stream open", d)
	}
}

// TestExecNormalCompletionLeavesBackgroundChild: without a request end, a
// deliberately backgrounded process keeps running.
func TestExecNormalCompletionLeavesBackgroundChild(t *testing.T) {
	h := newHarness(t, nil)
	h.exec(t, "sleep 30 >/dev/null 2>&1 & echo $! > bg-normal.pid; echo done")
	pid := pidFromFile(t, filepath.Join(h.home, "bg-normal.pid"))
	ident, ok := procStart(pid)
	if !ok {
		t.Fatalf("background child %d did not survive the command", pid)
	}
	t.Cleanup(func() { killTestProc(pid, ident) })
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("background child was killed on normal completion: %v", err)
	}
}

// startedSendFailStream fails the started frame after the command has had
// time to fill the bounded writer channel.
type startedSendFailStream struct {
	grpc.ServerStream
	ctx   context.Context
	gate  chan struct{}
	calls int
}

func (s *startedSendFailStream) Context() context.Context { return s.ctx }

func (s *startedSendFailStream) Send(*nodev1.ExecEvent) error {
	s.calls++
	if s.calls == 1 {
		<-s.gate
		return errors.New("started send failed")
	}
	return nil
}

// TestExecStartedSendFailureDrainsLargeOutput: when the started frame
// cannot be sent, a high-output command has already blocked exec's copy
// goroutine on the bounded writer channel. Every Send-failure path must
// drain that channel before Wait, or Wait waits for the blocked copy
// goroutine forever.
func TestExecStartedSendFailureDrainsLargeOutput(t *testing.T) {
	h := newHarness(t, nil)
	st := &startedSendFailStream{ctx: t.Context(), gate: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- h.svc.Exec(&guestv1.ExecRequest{
			Command:        "head -c 4000000 /dev/zero; sleep 30",
			TimeoutSeconds: 60,
		}, st)
	}()
	// Let the command fill the writer channel and block exec's copy
	// goroutine, then fail the started frame.
	time.Sleep(500 * time.Millisecond)
	close(st.gate)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Exec reported success after the started frame failed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Exec hung: the blocked output writer was never drained before Wait")
	}
}

// TestExecNoCwdDescriptorLeak pins cwd with the close-on-exec descriptor
// Go opened, not a dup(2) copy: a child must see no descriptor pointing at
// its own or a concurrent exec's working directory.
func TestExecNoCwdDescriptorLeak(t *testing.T) {
	h := newHarness(t, nil)
	dirs := []string{"leak-a", "leak-b"}
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(h.home, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	scan := `for fd in /proc/self/fd/*; do
  target=$(readlink "$fd" 2>/dev/null) || continue
  case "$target" in
    *leak-a*|*leak-b*) echo "leaked $fd -> $target" ;;
  esac
done
printf 'scan-done\n'`

	var wg sync.WaitGroup
	for _, d := range dirs {
		for range 4 {
			wg.Go(func() {
				res, err := execCollect(t.Context(), h, &guestv1.ExecRequest{
					Command:        scan,
					Cwd:            d,
					TimeoutSeconds: 30,
				})
				if err != nil {
					t.Errorf("cwd %q: %v", d, err)
					return
				}
				if strings.Contains(res.Stdout, "leaked") {
					t.Errorf("cwd descriptor leaked into a child: %q", res.Stdout)
				}
				if !strings.Contains(res.Stdout, "scan-done") {
					t.Errorf("cwd %q: scan did not finish: %q", d, res.Stdout)
				}
			})
		}
	}
	wg.Wait()
}
