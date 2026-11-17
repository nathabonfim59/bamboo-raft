// Detached-process tests over real gRPC (bufconn): start/idempotency,
// durable logs and records, restart recovery (LOST versus EXITED), kill
// semantics for whole groups, attach byte-exactness and cancellation, and
// the fault paths (durability, hostile symlinks, invalid state).
//
// Long-running commands are always killed by the test itself: a detached
// process survives the RPC and would otherwise outlive the test run.
package guest_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// procHarness is one service on bufconn whose confinement roots live under
// a caller-chosen base directory, so a second service can be started over
// the same state to simulate a guest-service restart.
type procHarness struct {
	t    *testing.T
	c    guestv1.GuestClient
	svc  *guest.Service
	home string
	tmp  string
}

// newProcHarness starts a service rooted at base (creating home/user and
// tmp under it); mod adjusts the config before New.
func newProcHarness(t *testing.T, base string, mod func(*guest.Config)) *procHarness {
	t.Helper()
	home := filepath.Join(base, "home/user")
	tmp := filepath.Join(base, "tmp")
	for _, d := range []string{home, tmp} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	fz := &fakeFreezer{}
	cfg := guest.Config{
		Log:         slog.New(slog.DiscardHandler),
		Version:     "test",
		UID:         os.Getuid(),
		GID:         os.Getgid(),
		NoSetGroups: true,
		HomeRoot:    home,
		TmpRoot:     tmp,
		Freezer:     fz,
	}
	if mod != nil {
		mod(&cfg)
	}
	svc, err := guest.New(cfg)
	if err != nil {
		t.Fatalf("guest.New: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	svc.Register(g)
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &procHarness{t: t, c: guestv1.NewGuestClient(conn), svc: svc, home: home, tmp: tmp}
}

// newProcTempHarness is a harness over its own fresh base directory.
func newProcTempHarness(t *testing.T, mod func(*guest.Config)) *procHarness {
	t.Helper()
	return newProcHarness(t, t.TempDir(), mod)
}

// shutdown stops the service explicitly (restart tests); the bufconn
// server and connection are cleaned up by the harness.
func (h *procHarness) shutdown() {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.svc.Shutdown(ctx); err != nil {
		h.t.Logf("shutdown: %v", err)
	}
}

// start begins a detached process and registers a test cleanup that kills
// its whole group, so nothing leaks past the test.
func (h *procHarness) start(t *testing.T, id uint32, command string, env map[string]string) *nodev1.Process {
	t.Helper()
	p, err := h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{
		ProcessId: id, Command: command, Env: env,
	})
	if err != nil {
		t.Fatalf("StartProcess(%d): %v", id, err)
	}
	if p.GetPid() > 0 {
		t.Cleanup(func() { syscall.Kill(-int(p.GetPid()), syscall.SIGKILL) })
	}
	return p
}

// waitState polls GetProcess until the process reaches one of want (nil
// exit tolerated) and returns the final response, failing on timeout.
func (h *procHarness) waitState(t *testing.T, id uint32, want ...nodev1.ProcessState) *nodev1.ProcessOutput {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		out, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
		if err != nil {
			t.Fatalf("GetProcess(%d): %v", id, err)
		}
		for _, w := range want {
			if out.Process.State == w {
				return out
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %d stuck in state %s, want %v", id, out.Process.State, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// procAttached collects one AttachProcess stream to its end, checking the
// contract's frame order and contiguous per-stream offsets.
type procAttached struct {
	First          *nodev1.ProcessEvent_Attached
	Stdout, Stderr string
	Exit           *nodev1.Process
}

func procAttach(ctx context.Context, c guestv1.GuestClient, req *guestv1.AttachProcessRequest) (*procAttached, error) {
	st, err := c.AttachProcess(ctx, req)
	if err != nil {
		return nil, err
	}
	res := &procAttached{}
	var out, errb bytes.Buffer
	var outNext, errNext int64
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
		case *nodev1.ProcessEvent_Attached_:
			if res.First != nil {
				return nil, errors.New("second attached frame")
			}
			res.First = e.Attached
			outNext, errNext = e.Attached.StdoutOffset, e.Attached.StderrOffset
		case *nodev1.ProcessEvent_Stdout:
			if res.First == nil {
				return nil, errors.New("stdout before attached")
			}
			if e.Stdout.NextOffset != outNext+int64(len(e.Stdout.Data)) {
				return nil, errors.New("stdout chunk offsets are not contiguous")
			}
			outNext = e.Stdout.NextOffset
			out.Write(e.Stdout.Data)
		case *nodev1.ProcessEvent_Stderr:
			if res.First == nil {
				return nil, errors.New("stderr before attached")
			}
			if e.Stderr.NextOffset != errNext+int64(len(e.Stderr.Data)) {
				return nil, errors.New("stderr chunk offsets are not contiguous")
			}
			errNext = e.Stderr.NextOffset
			errb.Write(e.Stderr.Data)
		case *nodev1.ProcessEvent_Exit:
			if res.First == nil {
				return nil, errors.New("exit before attached")
			}
			res.Exit = e.Exit
		}
	}
	if res.Exit == nil {
		return nil, errors.New("stream ended without exit")
	}
	res.Stdout, res.Stderr = out.String(), errb.String()
	return res, nil
}

// procGroupEmpty reports whether no non-zombie process remains in pgid.
// It is an independent /proc scan, not the service's own helper.
func procGroupEmpty(pgid int) bool {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}
	for _, e := range entries {
		if _, err := strconv.Atoi(e.Name()); err != nil {
			continue
		}
		data, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		_, rest, ok := strings.CutLast(string(data), ")")
		if !ok {
			continue
		}
		f := strings.Fields(rest)
		if len(f) < 3 || f[0] == "Z" {
			continue
		}
		if pgrp, err := strconv.Atoi(f[2]); err == nil && pgrp == pgid {
			return false
		}
	}
	return true
}

// procDead polls until pgid has no live members (descendants included).
func procDead(t *testing.T, pgid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !procGroupEmpty(pgid) {
		if time.Now().After(deadline) {
			t.Fatalf("process group %d still has live members", pgid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestProcessStartRecordAndLogs(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const id = 101
	p := h.start(t, id, `echo out-1; echo err-1 >&2; echo "env=$P1_VAR home-ok"; exit 3`,
		map[string]string{"P1_VAR": "from-start"})
	if p.ProcessId != id || p.Pid <= 0 || p.Command == "" {
		t.Fatalf("StartProcess = %v", p)
	}
	if p.Cwd != homeSpecCanonical {
		t.Fatalf("cwd = %q, want %q (empty cwd defaults to the home)", p.Cwd, homeSpecCanonical)
	}
	if p.State != nodev1.ProcessState_PROCESS_STATE_RUNNING && p.State != nodev1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("state = %s", p.State)
	}
	if p.LogPath != "/home/user/.bbraft/processes/101.log" || p.ErrLogPath != "/home/user/.bbraft/processes/101.err.log" || p.LogPath == p.ErrLogPath {
		t.Fatalf("log paths = %q, %q", p.LogPath, p.ErrLogPath)
	}

	out := h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	rec := out.Process
	if rec.Exit == nil || rec.Exit.ExitCode == nil || *rec.Exit.ExitCode != 3 || rec.Exit.Signal != "" {
		t.Fatalf("exit = %v", rec.Exit)
	}
	if rec.FinishedAtUnixMs < rec.StartedAtUnixMs || rec.FinishedAtUnixMs == 0 {
		t.Fatalf("finished %d before started %d", rec.FinishedAtUnixMs, rec.StartedAtUnixMs)
	}
	want := "out-1\nenv=from-start home-ok\n"
	if string(out.Stdout) != want || rec.StdoutSize != int64(len(want)) {
		t.Fatalf("stdout %q (%d), want %q", out.Stdout, rec.StdoutSize, want)
	}
	if string(out.Stderr) != "err-1\n" || rec.StderrSize != int64(len("err-1\n")) {
		t.Fatalf("stderr %q", out.Stderr)
	}

	// The durable state lives under the home root, owned by the sandbox
	// user, with the logs beside the private metadata directory.
	procs := filepath.Join(h.home, ".bbraft", "processes")
	for _, name := range []string{"101.log", "101.err.log", filepath.Join("101", "meta.json"), filepath.Join("101", "exit.json")} {
		fi, err := os.Stat(filepath.Join(procs, name))
		if err != nil {
			t.Fatalf("state file %s: %v", name, err)
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok || st.Uid != uint32(os.Getuid()) || st.Gid != uint32(os.Getgid()) {
			t.Fatalf("state file %s owned by %d:%d, want the sandbox user", name, st.Uid, st.Gid)
		}
	}
}

// homeSpecCanonical mirrors the v1 API home prefix the service reports.
const homeSpecCanonical = "/home/user"

func TestProcessDuplicateStarts(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const id = 110
	p := h.start(t, id, "echo once; sleep 1", nil)

	// Sequential duplicate: whatever the other fields say.
	again, err := h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{
		ProcessId: id, Command: "echo other", Cwd: "/tmp",
	})
	if err != nil || again.Pid != p.Pid || again.Command != p.Command || again.Cwd != p.Cwd {
		t.Fatalf("duplicate StartProcess = %v, %v; want the original process", again, err)
	}
	h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)

	// Still idempotent after the process ended: the record comes back.
	after, err := h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: id, Command: "different"})
	if err != nil || after.Pid != p.Pid || after.State != nodev1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("duplicate after exit = %v, %v", after, err)
	}

	// Concurrent duplicates: exactly one bash runs, all callers agree.
	const cid = 111
	var wg sync.WaitGroup
	var mu sync.Mutex
	pids := map[int32]int{}
	for range 4 {
		wg.Go(func() {
			p, err := h.c.StartProcess(context.Background(), &guestv1.StartProcessRequest{
				ProcessId: cid, Command: "echo sole", Env: map[string]string{"K": "v"},
			})
			if err != nil {
				t.Errorf("concurrent StartProcess: %v", err)
				return
			}
			mu.Lock()
			pids[p.Pid]++
			mu.Unlock()
		})
	}
	wg.Wait()
	if len(pids) != 1 {
		t.Fatalf("concurrent starts produced %d distinct processes", len(pids))
	}
	a, err := procAttach(t.Context(), h.c, &guestv1.AttachProcessRequest{ProcessId: cid})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if a.Stdout != "sole\n" {
		t.Fatalf("concurrent-start output = %q, want exactly one run", a.Stdout)
	}
}

func TestProcessNewSessionAsUser(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const id = 115
	p := h.start(t, id, `printf 'uid=%s sid=%s pgid=%s\n' "$(id -u)" "$(ps -o sess= -p $$)" "$(ps -o pgid= -p $$)"`, nil)
	h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	out, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatal(err)
	}
	if err := procCheckSessionReport(string(out.Stdout), os.Getuid(), int(p.Pid)); err != nil {
		t.Fatalf("session report = %q: %v (new session and group led by the recorded pid)", out.Stdout, err)
	}
}

// procCheckSessionReport validates the uid/sid/pgid report printed by
// TestProcessNewSessionAsUser. ps right-aligns its numeric columns on some
// distributions (`sid=  12127`), so the three values are read after
// splitting on whitespace, but the configured uid and the recorded pid are
// still checked exactly.
func procCheckSessionReport(report string, uid, pid int) error {
	prefixes := [3]string{"uid=", "sid=", "pgid="}
	var got [3]int
	rest := report
	for i, prefix := range prefixes {
		rest = strings.TrimLeft(rest, " \t\r\n")
		value, ok := strings.CutPrefix(rest, prefix)
		if !ok {
			return fmt.Errorf("field %d: missing %q in %q", i, prefix, rest)
		}
		// ps may right-align the number, so whitespace can sit between the
		// key and its value; the first non-digit ends it.
		value = strings.TrimLeft(value, " \t")
		end := 0
		for end < len(value) && value[end] >= '0' && value[end] <= '9' {
			end++
		}
		if end == 0 {
			return fmt.Errorf("%q has no numeric value", prefix)
		}
		n, err := strconv.Atoi(value[:end])
		if err != nil {
			return fmt.Errorf("%q: %w", prefix, err)
		}
		got[i] = n
		rest = value[end:]
	}
	if trailing := strings.TrimSpace(rest); trailing != "" {
		return fmt.Errorf("unexpected trailing %q", trailing)
	}
	if got[0] != uid {
		return fmt.Errorf("uid = %d, want %d", got[0], uid)
	}
	if got[1] != pid {
		return fmt.Errorf("sid = %d, want the process pid %d", got[1], pid)
	}
	if got[2] != pid {
		return fmt.Errorf("pgid = %d, want the process pid %d", got[2], pid)
	}
	return nil
}

// TestProcSessionReportValidation pins the two ps layouts this host family
// produces and proves wrong values are still rejected: the padded Ubuntu
// report fails the pre-fix exact-string assertion while the parser accepts
// it, and a wrong uid, sid or pgid is never accepted.
func TestProcSessionReportValidation(t *testing.T) {
	const (
		padded = "uid=1000 sid=  12127 pgid=  12127\n"
		packed = "uid=1000 sid=12127 pgid=12127\n"
	)
	t.Run("legacy exact assertion rejects padded ps output", func(t *testing.T) {
		legacy := fmt.Sprintf("uid=%d sid=%d pgid=%d\n", 1000, 12127, 12127)
		if padded == legacy {
			t.Fatal("padded fixture unexpectedly equals the legacy unpadded expectation")
		}
		if err := procCheckSessionReport(padded, 1000, 12127); err != nil {
			t.Fatalf("whitespace-independent validation rejected the padded report: %v", err)
		}
	})
	cases := []struct {
		name   string
		report string
		uid    int
		pid    int
		ok     bool
	}{
		{"ubuntu ps padding", padded, 1000, 12127, true},
		{"unpadded", packed, 1000, 12127, true},
		{"wrong uid", "uid=1001 sid=  12127 pgid=  12127\n", 1000, 12127, false},
		{"wrong sid", "uid=1000 sid=  12128 pgid=  12127\n", 1000, 12127, false},
		{"wrong pgid", "uid=1000 sid=  12127 pgid=  12128\n", 1000, 12127, false},
		{"missing field", "uid=1000 sid=12127\n", 1000, 12127, false},
		{"non numeric", "uid=1000 sid=abc pgid=12127\n", 1000, 12127, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := procCheckSessionReport(tc.report, tc.uid, tc.pid)
			if tc.ok && err != nil {
				t.Fatalf("valid report rejected: %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("invalid report accepted")
			}
		})
	}
}

func TestProcessBadRequests(t *testing.T) {
	h := newProcTempHarness(t, nil)
	ctx := t.Context()
	_, err := h.c.StartProcess(ctx, &guestv1.StartProcessRequest{ProcessId: 0, Command: "true"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.c.StartProcess(ctx, &guestv1.StartProcessRequest{ProcessId: 199, Command: "true", Cwd: "/etc"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.c.StartProcess(ctx, &guestv1.StartProcessRequest{ProcessId: 199, Command: "true", Cwd: "missing-dir"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.c.StartProcess(ctx, &guestv1.StartProcessRequest{ProcessId: 199, Command: "true", Env: map[string]string{"bad=key": "v"}})
	wantCode(t, err, codes.InvalidArgument)
	// A rejected start does not consume the id.
	h.start(t, 199, "echo ok", nil)

	for _, call := range []string{"get", "kill", "attach"} {
		var err error
		switch call {
		case "get":
			_, err = h.c.GetProcess(ctx, &guestv1.GetProcessRequest{ProcessId: 9999})
		case "kill":
			_, err = h.c.KillProcess(ctx, &guestv1.KillProcessRequest{ProcessId: 9999})
		case "attach":
			_, err = procAttach(ctx, h.c, &guestv1.AttachProcessRequest{ProcessId: 9999})
		}
		wantCode(t, err, codes.NotFound)
	}
	_, err = h.c.GetProcess(ctx, &guestv1.GetProcessRequest{ProcessId: 199, LimitBytes: 8<<20 + 1})
	wantCode(t, err, codes.InvalidArgument)
}

func TestProcessCallerCancellationDoesNotTouchIt(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const id = 120
	ctx, cancel := context.WithCancel(context.Background())
	p, err := h.c.StartProcess(ctx, &guestv1.StartProcessRequest{
		ProcessId: id, Command: "echo early; sleep 2; echo late",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syscall.Kill(-int(p.Pid), syscall.SIGKILL) })
	cancel() // the caller gives up immediately

	time.Sleep(300 * time.Millisecond)
	out, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatal(err)
	}
	if out.Process.State != nodev1.ProcessState_PROCESS_STATE_RUNNING {
		t.Fatalf("state = %s after caller cancel, want RUNNING", out.Process.State)
	}
	final := h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	if string(final.Stdout) != "early\nlate\n" {
		t.Fatalf("stdout = %q, want the process to run to completion", final.Stdout)
	}
}

func TestProcessGetWindows(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const id = 125
	h.start(t, id, "printf 0123456789; printf abc >&2", nil)
	h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	get := func(req *guestv1.GetProcessRequest) *nodev1.ProcessOutput {
		t.Helper()
		req.ProcessId = id
		out, err := h.c.GetProcess(t.Context(), req)
		if err != nil {
			t.Fatalf("GetProcess(%v): %v", req, err)
		}
		return out
	}
	i := func(v int64) *int64 { return &v }

	o := get(&guestv1.GetProcessRequest{LimitBytes: 4})
	if string(o.Stdout) != "6789" || o.StdoutOffset != 6 || !o.StdoutTruncated ||
		string(o.Stderr) != "abc" || o.StderrTruncated || o.More {
		t.Fatalf("tail window = %+v", o)
	}
	o = get(&guestv1.GetProcessRequest{})
	if string(o.Stdout) != "0123456789" || o.StdoutTruncated || o.StderrTruncated || o.More ||
		o.Process.State != nodev1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("full tail = %+v", o)
	}
	o = get(&guestv1.GetProcessRequest{StdoutOffset: i(2), StderrOffset: i(0), LimitBytes: 5})
	if string(o.Stdout) != "23456" || o.StdoutOffset != 2 || !o.More || string(o.Stderr) != "abc" {
		t.Fatalf("offset window = %+v", o)
	}
	o = get(&guestv1.GetProcessRequest{StdoutOffset: i(7), StderrOffset: i(3)})
	if string(o.Stdout) != "789" || o.More || len(o.Stderr) != 0 || o.StderrOffset != 3 {
		t.Fatalf("offset to end = %+v", o)
	}
	o = get(&guestv1.GetProcessRequest{StdoutOffset: i(99), StderrOffset: i(99)})
	if len(o.Stdout) != 0 || o.StdoutOffset != 10 || o.More || o.StderrOffset != 3 {
		t.Fatalf("offset past end = %+v", o)
	}
	o = get(&guestv1.GetProcessRequest{StdoutOffset: i(-5), LimitBytes: 3})
	if string(o.Stdout) != "012" || o.StdoutOffset != 0 {
		t.Fatalf("negative offset = %+v, want clamped to 0", o)
	}
}

func TestProcessAttachByteExact(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const id = 130
	h.start(t, id, `for i in 1 2 3 4 5; do printf "out$i "; printf "e$i " >&2; sleep 0.05; done; echo end`, nil)

	a, err := procAttach(t.Context(), h.c, &guestv1.AttachProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatalf("attach while running: %v", err)
	}
	if want := "out1 out2 out3 out4 out5 end\n"; a.Stdout != want {
		t.Fatalf("stdout = %q, want %q", a.Stdout, want)
	}
	if want := "e1 e2 e3 e4 e5 "; a.Stderr != want {
		t.Fatalf("stderr = %q, want %q", a.Stderr, want)
	}
	if a.First.StdoutOffset != 0 || a.First.StderrOffset != 0 {
		t.Fatalf("attached offsets = %d, %d", a.First.StdoutOffset, a.First.StderrOffset)
	}
	if a.Exit.Exit == nil || a.Exit.Exit.ExitCode == nil || *a.Exit.Exit.ExitCode != 0 {
		t.Fatalf("exit = %+v", a.Exit)
	}

	// Re-attach from a mid offset: no repeats before, no gaps after.
	a2, err := procAttach(t.Context(), h.c, &guestv1.AttachProcessRequest{ProcessId: id, StdoutOffset: 10, StderrOffset: 1 << 20})
	if err != nil {
		t.Fatalf("re-attach: %v", err)
	}
	if a2.First.StdoutOffset != 10 || a2.First.StderrOffset != int64(len("e1 e2 e3 e4 e5 ")) {
		t.Fatalf("re-attach offsets = %d, %d", a2.First.StdoutOffset, a2.First.StderrOffset)
	}
	if a2.Stdout != "out3 out4 out5 end\n" || a2.Stderr != "" {
		t.Fatalf("re-attach content = %q, %q", a2.Stdout, a2.Stderr)
	}

	// A stream larger than one chunk stays contiguous.
	const bid = 131
	blob := strings.Repeat("x", 3<<20) // 3 MiB, over the 1 MiB chunk size
	if err := os.WriteFile(filepath.Join(h.tmp, "blob.txt"), []byte(blob), 0o644); err != nil {
		t.Fatal(err)
	}
	h.start(t, bid, "cat "+filepath.Join(h.tmp, "blob.txt"), nil)
	ab, err := procAttach(t.Context(), h.c, &guestv1.AttachProcessRequest{ProcessId: bid})
	if err != nil {
		t.Fatalf("attach big: %v", err)
	}
	if ab.Stdout != blob {
		t.Fatalf("big attach: got %d bytes, want %d", len(ab.Stdout), len(blob))
	}
}

func TestProcessAttachToEndedProcess(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const id = 135
	h.start(t, id, "printf hi; printf bye >&2", nil)
	h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	a, err := procAttach(t.Context(), h.c, &guestv1.AttachProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatalf("attach to ended: %v", err)
	}
	if a.Stdout != "hi" || a.Stderr != "bye" || a.Exit.State != nodev1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("attach to ended = %+v", a)
	}
}

func TestProcessAttachCancellationOnlyEndsStream(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const id = 140
	p := h.start(t, id, "sleep 10", nil)

	ctx, cancel := context.WithCancel(context.Background())
	st, err := h.c.AttachProcess(ctx, &guestv1.AttachProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatal(err)
	}
	first, err := st.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := first.Event.(*nodev1.ProcessEvent_Attached_); !ok {
		t.Fatalf("first frame = %T, want attached", first.Event)
	}
	cancel()
	if _, err := st.Recv(); status.Code(err) != codes.Canceled {
		t.Fatalf("recv after cancel = %v, want Canceled", err)
	}

	// The process itself was never signaled.
	out, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatal(err)
	}
	if out.Process.State != nodev1.ProcessState_PROCESS_STATE_RUNNING {
		t.Fatalf("state = %s after attach cancel, want RUNNING", out.Process.State)
	}
	if err := syscall.Kill(int(p.Pid), 0); err != nil {
		t.Fatalf("leader %d died on an attach cancellation: %v", p.Pid, err)
	}
}

func TestProcessKillWholeGroup(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const id = 145
	h.start(t, id, "sleep 300 & sleep 300; wait", nil)
	k, err := h.c.KillProcess(t.Context(), &guestv1.KillProcessRequest{ProcessId: id, GraceSeconds: 2})
	if err != nil {
		t.Fatalf("KillProcess: %v", err)
	}
	if !k.Signaled || k.Process.State != nodev1.ProcessState_PROCESS_STATE_EXITED ||
		k.Process.Exit == nil || k.Process.Exit.ExitCode != nil || k.Process.Exit.Signal == "" {
		t.Fatalf("KillProcess = %v; want signaled, EXITED by a signal", k)
	}
	procDead(t, int(k.Process.Pid)) // descendants included

	k2, err := h.c.KillProcess(t.Context(), &guestv1.KillProcessRequest{ProcessId: id})
	if err != nil || k2.Signaled || k2.Process.State != nodev1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("KillProcess again = %v, %v; want OK, not signaled", k2, err)
	}
}

func TestProcessKillIgnoresTERM(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const id = 150
	h.start(t, id, "trap '' TERM; sleep 300", nil)
	time.Sleep(300 * time.Millisecond) // let bash install the trap
	start := time.Now()
	k, err := h.c.KillProcess(t.Context(), &guestv1.KillProcessRequest{ProcessId: id, GraceSeconds: 1})
	if err != nil || !k.Signaled || k.Process.Exit.GetSignal() != "SIGKILL" {
		t.Fatalf("KillProcess TERM-ignoring = %v, %v; want SIGKILL", k, err)
	}
	if d := time.Since(start); d < 900*time.Millisecond {
		t.Fatalf("SIGKILL after %v, before the 1 s grace", d)
	}
	procDead(t, int(k.Process.Pid))
}

func TestProcessRestartLostVersusExited(t *testing.T) {
	base := t.TempDir()
	a := newProcHarness(t, base, nil)
	const (
		lostID  = 160
		doneID  = 161
		deadPID = 4711 // a PID nothing runs under anymore
	)
	a.start(t, lostID, "echo before-stop; sleep 300", nil)
	// Wait until the survivor has actually written its line: its bash may
	// be scheduled arbitrarily late relative to the finished process.
	zero := int64(0)
	deadline := time.Now().Add(10 * time.Second)
	for {
		out, err := a.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{
			ProcessId: lostID, StdoutOffset: &zero, LimitBytes: 64,
		})
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(out.Stdout), "before-stop") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("surviving process never wrote its line")
		}
		time.Sleep(20 * time.Millisecond)
	}
	a.start(t, doneID, "echo hi", nil)
	a.waitState(t, doneID, nodev1.ProcessState_PROCESS_STATE_EXITED)
	a.shutdown() // the guest service dies; its processes keep running

	b := newProcHarness(t, base, nil)
	lost, err := b.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: lostID})
	if err != nil {
		t.Fatal(err)
	}
	lr := lost.Process
	if lr.State != nodev1.ProcessState_PROCESS_STATE_LOST || lr.Exit != nil || lr.FinishedAtUnixMs != 0 {
		t.Fatalf("running process after restart = %+v, want LOST", lr)
	}
	if string(lost.Stdout) != "before-stop\n" {
		t.Fatalf("LOST process logs unreadable: %q", lost.Stdout)
	}

	done, err := b.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: doneID})
	if err != nil {
		t.Fatal(err)
	}
	if done.Process.State != nodev1.ProcessState_PROCESS_STATE_EXITED ||
		done.Process.Exit == nil || done.Process.Exit.ExitCode == nil || *done.Process.Exit.ExitCode != 0 ||
		string(done.Stdout) != "hi\n" || done.Process.FinishedAtUnixMs == 0 {
		t.Fatalf("finished process after restart = %+v", done.Process)
	}

	// The recovered LOST record is never signaled, and the id stays taken.
	k, err := b.c.KillProcess(t.Context(), &guestv1.KillProcessRequest{ProcessId: lostID})
	if err != nil || k.Signaled || k.Process.State != nodev1.ProcessState_PROCESS_STATE_LOST {
		t.Fatalf("kill of a LOST record = %v, %v; want no signal", k, err)
	}
	again, err := b.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: doneID, Command: "echo other"})
	if err != nil || again.State != nodev1.ProcessState_PROCESS_STATE_EXITED || again.Command != "echo hi" {
		t.Fatalf("duplicate start of a recovered EXITED process = %v, %v", again, err)
	}
	_ = deadPID
}

// procWriteState lays a metadata directory and logs on disk by hand, to
// stage recovery scenarios (valid, corrupt, foreign).
func procWriteState(t *testing.T, home string, id uint32, meta string, exit string, out, errLog string) {
	t.Helper()
	dir := filepath.Join(home, ".bbraft", "processes", strconv.FormatUint(uint64(id), 10))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		filepath.Join(dir, "meta.json"): meta,
		filepath.Join(dir, "exit.json"): exit,
	} {
		if content == "" {
			continue
		}
		if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if out != "" || errLog != "" {
		if err := os.WriteFile(filepath.Join(home, ".bbraft", "processes", strconv.FormatUint(uint64(id), 10)+".log"), []byte(out), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(home, ".bbraft", "processes", strconv.FormatUint(uint64(id), 10)+".err.log"), []byte(errLog), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProcessRecoveryValidation(t *testing.T) {
	base := t.TempDir()
	home := filepath.Join(base, "home/user")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	const started = int64(1731300000000)
	meta := func(id, pid int) string {
		return fmt.Sprintf(`{"version":1,"process_id":%d,"command":"recorded","cwd":"/tmp","pid":%d,"started_at_unix_ms":%d}`, id, pid, started)
	}
	exit := func(code int) string {
		return fmt.Sprintf(`{"version":1,"exit_code":%d,"duration_ms":12,"finished_at_unix_ms":%d}`, code, started+12)
	}
	procWriteState(t, home, 201, "{not json", "", "", "")                                                                                         // corrupt meta: invisible
	procWriteState(t, home, 202, meta(999, 50), "", "", "")                                                                                       // id mismatch: invisible
	procWriteState(t, home, 203, meta(203, 50), "{broken", "", "")                                                                                // corrupt exit: LOST
	procWriteState(t, home, 204, meta(204, 50), exit(7), "kept-out", "kept-err")                                                                  // valid: EXITED
	procWriteState(t, home, 205, meta(205, 50), `{"version":9,"exit_code":0,"finished_at_unix_ms":1}`, "", "")                                    // future record: LOST
	procWriteState(t, home, 206, meta(206, 50), `{"version":1,"exit_code":7,"signal":"SIGKILL","duration_ms":1,"finished_at_unix_ms":9}`, "", "") // both code and signal: LOST

	h := newProcHarness(t, base, nil)
	for _, id := range []uint32{201, 202} {
		_, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
		wantCode(t, err, codes.NotFound)
	}
	for _, id := range []uint32{203, 205, 206} {
		out, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
		if err != nil {
			t.Fatalf("%d: %v", id, err)
		}
		if out.Process.State != nodev1.ProcessState_PROCESS_STATE_LOST || out.Process.Exit != nil {
			t.Fatalf("process %d recovered as %+v, want LOST", id, out.Process)
		}
	}
	out, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: 204})
	if err != nil {
		t.Fatal(err)
	}
	r := out.Process
	if r.State != nodev1.ProcessState_PROCESS_STATE_EXITED || r.Exit == nil || r.Exit.ExitCode == nil ||
		*r.Exit.ExitCode != 7 || r.Command != "recorded" || r.Cwd != "/tmp" ||
		r.StartedAtUnixMs != started || r.FinishedAtUnixMs != started+12 ||
		string(out.Stdout) != "kept-out" || string(out.Stderr) != "kept-err" {
		t.Fatalf("recovered EXITED record = %+v", r)
	}
	a, err := procAttach(t.Context(), h.c, &guestv1.AttachProcessRequest{ProcessId: 204})
	if err != nil || a.Stdout != "kept-out" || a.Exit.State != nodev1.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("attach to recovered process = %+v, %v", a, err)
	}
}

func TestProcessStartDurabilityFailureReleasesID(t *testing.T) {
	h := newProcTempHarness(t, func(c *guest.Config) {
		c.SyncFile = func(*os.File) error { return errors.New("injected sync failure") }
	})
	const id = 170
	_, err := h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: id, Command: "echo x"})
	wantCode(t, err, codes.Internal)

	// The failed attempt consumed nothing: a working service can use the id.
	h2 := newProcTempHarness(t, nil)
	p, err := h2.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: id, Command: "echo retry"})
	if err != nil {
		t.Fatalf("retry after failed start: %v", err)
	}
	t.Cleanup(func() { syscall.Kill(-int(p.Pid), syscall.SIGKILL) })
	h2.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	out, err := h2.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	if err != nil || string(out.Stdout) != "retry\n" {
		t.Fatalf("retry output = %q, %v", out.Stdout, err)
	}
}

func TestProcessExitSyncFailureRecoversLost(t *testing.T) {
	base := t.TempDir()
	a := newProcHarness(t, base, func(c *guest.Config) {
		// Fail only the exit-time log syncs: file names carry ".log".
		c.SyncFile = func(f *os.File) error {
			if strings.HasSuffix(f.Name(), ".log") {
				return errors.New("injected log sync failure")
			}
			return nil
		}
	})
	const id = 175
	a.start(t, id, "echo durable", nil)
	fin := a.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	if fin.Process.Exit == nil || fin.Process.Exit.ExitCode == nil || *fin.Process.Exit.ExitCode != 0 {
		t.Fatalf("in-memory exit after sync failure = %+v", fin.Process.Exit)
	}
	a.shutdown()

	b := newProcHarness(t, base, nil)
	out, err := b.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatal(err)
	}
	if out.Process.State != nodev1.ProcessState_PROCESS_STATE_LOST {
		t.Fatalf("undurable exit recovered as %s, want LOST (logs were never synced)", out.Process.State)
	}
	if string(out.Stdout) != "durable\n" {
		t.Fatalf("logs lost with the record: %q", out.Stdout)
	}
}

func TestProcessHostileSymlinks(t *testing.T) {
	base := t.TempDir()
	a := newProcHarness(t, base, nil)
	const id = 180
	a.start(t, id, "printf real", nil)
	a.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	a.shutdown()

	// A symlink planted at a log name must not be read through.
	procs := filepath.Join(base, "home/user/.bbraft/processes")
	outside := filepath.Join(base, "outside")
	if err := os.WriteFile(outside, []byte("poison"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(procs, "180.log")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(procs, "180.log")); err != nil {
		t.Fatal(err)
	}
	b := newProcHarness(t, base, nil)
	_, err := b.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	wantCode(t, err, codes.Internal)
	if got := readFileOrEmpty(outside); got != "poison" {
		t.Fatalf("symlink target modified through the planted link: %q", got)
	}

	// A symlink at .bbraft must not redirect new state writes either.
	home := filepath.Join(base, "home/user")
	if err := os.RemoveAll(filepath.Join(home, ".bbraft")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(base, "evil"), filepath.Join(home, ".bbraft")); err != nil {
		t.Fatal(err)
	}
	c := newProcHarness(t, base, nil)
	_, err = c.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: 181, Command: "echo x"})
	wantCode(t, err, codes.Internal)
	if _, err := os.Stat(filepath.Join(base, "evil", "processes")); !os.IsNotExist(err) {
		t.Fatalf("state written through the hostile .bbraft symlink")
	}
}

func readFileOrEmpty(p string) string {
	b, _ := os.ReadFile(p)
	return string(b)
}

func TestProcessConcurrentMixed(t *testing.T) {
	h := newProcTempHarness(t, nil)
	ids := []uint32{301, 302, 303, 304, 305, 306}

	// Phase 1: concurrent starts (each also racing duplicate callers).
	var starts sync.WaitGroup
	for _, id := range ids {
		starts.Go(func() {
			p, err := h.c.StartProcess(context.Background(), &guestv1.StartProcessRequest{
				ProcessId: id,
				Command:   fmt.Sprintf("echo start-%d; sleep 0.5; echo end-%d", id, id),
				Env:       map[string]string{"PROC": strconv.FormatUint(uint64(id), 10)},
			})
			if err != nil {
				t.Errorf("start %d: %v", id, err)
				return
			}
			t.Cleanup(func() { syscall.Kill(-int(p.Pid), syscall.SIGKILL) })
			if _, err := h.c.StartProcess(context.Background(), &guestv1.StartProcessRequest{
				ProcessId: id, Command: "echo duplicate",
			}); err != nil {
				t.Errorf("duplicate %d: %v", id, err)
			}
		})
	}
	starts.Wait()

	// Phase 2: concurrent attach and windows while the processes run.
	var readers sync.WaitGroup
	for _, id := range ids {
		readers.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err := procAttach(ctx, h.c, &guestv1.AttachProcessRequest{ProcessId: id}); err != nil {
				t.Errorf("attach %d: %v", id, err)
			}
		})
		readers.Go(func() {
			if _, err := h.c.GetProcess(context.Background(), &guestv1.GetProcessRequest{
				ProcessId: id, LimitBytes: 16,
			}); err != nil {
				t.Errorf("window %d: %v", id, err)
			}
		})
	}
	readers.Wait()

	// Phase 3: kill two of them (they may have ended already; both
	// outcomes are valid, the record must stay consistent).
	for _, id := range ids[:2] {
		if _, err := h.c.KillProcess(t.Context(), &guestv1.KillProcessRequest{ProcessId: id, GraceSeconds: 1}); err != nil {
			t.Errorf("kill %d: %v", id, err)
		}
	}
	for _, id := range ids {
		out := h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
		want := fmt.Sprintf("start-%d\nend-%d\n", id, id)
		if string(out.Stdout) != want {
			t.Errorf("process %d stdout = %q, want %q", id, out.Stdout, want)
		}
		if rec, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id}); err != nil ||
			rec.Process.StdoutSize != int64(len(want)) {
			t.Errorf("process %d record inconsistent: %v, %v", id, rec.Process, err)
		}
	}
}

// TestProcessRunsAsUserWithGroups is the detached-process half of the
// credential regression: it runs only in throwaway VMs
// (BBRAFT_TEST_GROUPS_ROOT=1 and euid 0, with a guest-layout "user"
// account as in the v1 image). Like TestExecExtraGroupRoot it adds a
// group listing the user to /etc/group and checks that `id -G` reported
// in the process logs matches what id(1) says the account belongs to —
// the same credential builder, now on the StartProcess path.
// TestProcessStateDirSymlinkRefused plants a symlink at the private
// metadata directory name: the state creation must fail closed instead of
// writing through it, and the id must survive for a later attempt.
func TestProcessStateDirSymlinkRefused(t *testing.T) {
	base := t.TempDir()
	home := filepath.Join(base, "home/user")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(base, "elsewhere")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	procs := filepath.Join(home, ".bbraft", "processes")
	if err := os.MkdirAll(procs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(procs, "210")); err != nil {
		t.Fatal(err)
	}
	h := newProcHarness(t, base, nil)
	_, err := h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: 210, Command: "echo x"})
	wantCode(t, err, codes.Internal)
	if _, err := os.Stat(filepath.Join(target, "meta.json")); !os.IsNotExist(err) {
		t.Fatalf("metadata written through the symlinked state directory")
	}
	// The id was not consumed: without the symlink it starts fine.
	if err := os.Remove(filepath.Join(procs, "210")); err != nil {
		t.Fatal(err)
	}
	h.start(t, 210, "echo ok", nil)
	h.waitState(t, 210, nodev1.ProcessState_PROCESS_STATE_EXITED)
}

// TestProcessStartInFlight holds one start attempt mid-creation: a
// duplicate caller whose context ends gets Canceled, a reader gets
// DeadlineExceeded while the id is reserved, and once the attempt fails on
// purpose the id is released (NotFound, then a clean retry succeeds).
func TestProcessStartInFlight(t *testing.T) {
	gate := make(chan struct{})
	var hold, fail atomic.Bool
	h := newProcTempHarness(t, func(c *guest.Config) {
		c.SyncFile = func(*os.File) error {
			if hold.Load() {
				<-gate // hold start attempts inside their durable writes
			}
			if fail.Load() {
				return errors.New("injected sync failure")
			}
			return nil
		}
	})
	hold.Store(true) // construction may sync; start attempts may not pass
	const id = 215

	started := make(chan error, 1)
	go func() {
		_, err := h.c.StartProcess(context.Background(), &guestv1.StartProcessRequest{
			ProcessId: id, Command: "echo x",
		})
		started <- err
	}()

	// Wait until the id is reserved: a GetProcess then blocks on the
	// attempt and times out instead of saying NotFound.
	reserved := time.Now().Add(5 * time.Second)
	for {
		pctx, pcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, perr := h.c.GetProcess(pctx, &guestv1.GetProcessRequest{ProcessId: id})
		pcancel()
		if status.Code(perr) == codes.DeadlineExceeded {
			break // reserved, still starting
		}
		if status.Code(perr) != codes.NotFound {
			t.Fatalf("probe while starting: %v", perr)
		}
		if time.Now().After(reserved) {
			t.Fatal("start attempt never reserved the id")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A caller that gives up while waiting on the same id: Canceled.
	dupCtx, dupCancel := context.WithCancel(context.Background())
	dupErr := make(chan error, 1)
	go func() {
		_, err := h.c.StartProcess(dupCtx, &guestv1.StartProcessRequest{ProcessId: id, Command: "echo y"})
		dupErr <- err
	}()
	time.Sleep(50 * time.Millisecond) // let it reach the wait
	dupCancel()
	if err := <-dupErr; status.Code(err) != codes.Canceled {
		t.Fatalf("duplicate during in-flight start = %v, want Canceled", err)
	}

	// Fail the held attempt: its caller sees Internal, readers NotFound.
	fail.Store(true)
	close(gate)
	if err := <-started; status.Code(err) != codes.Internal {
		t.Fatalf("held start = %v, want Internal", err)
	}
	_, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	wantCode(t, err, codes.NotFound)

	// The id was not consumed: a clean retry succeeds (gate closed, so
	// syncs pass through again).
	fail.Store(false)
	h.start(t, id, "echo ok", nil)
	h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
}

// TestProcessOversizedMetadata stages a metadata file beyond the recovery
// bound: it is corruption, not state, so the process stays invisible.
func TestProcessOversizedMetadata(t *testing.T) {
	base := t.TempDir()
	procs := filepath.Join(base, "home/user/.bbraft/processes/220")
	if err := os.MkdirAll(procs, 0o755); err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("x", 70*1024)
	if err := os.WriteFile(filepath.Join(procs, "meta.json"), []byte(big), 0o644); err != nil {
		t.Fatal(err)
	}
	h := newProcHarness(t, base, nil)
	_, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: 220})
	wantCode(t, err, codes.NotFound)
}

// TestProcessAfterShutdown: the state directory closes with the service.
// New starts fail closed, and reads through the shared pin fail rather
// than serving data from a re-opened path.
func TestProcessAfterShutdown(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const id = 225
	h.start(t, id, "echo x", nil)
	h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	h.shutdown()

	_, err := h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: 226, Command: "echo y"})
	wantCode(t, err, codes.Internal)
	_, err = h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	wantCode(t, err, codes.Internal)
}

// --- Review round: group lifetime, rollback, FIFOs, durability order,
// bounded kill, FD hygiene, exact reads, pinned roots ------------------------

// procRunWithDeadline runs call and fails the test when it does not answer
// within d, catching the hangs this round's bugs caused.
func procRunWithDeadline(t *testing.T, d time.Duration, name string, call func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- call() }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatalf("%s did not answer within %v (hang)", name, d)
		return nil
	}
}

// TestProcessDescendantsAfterLeader: the group, not the bash leader, owns
// the process's lifetime. A child that outlives the leader keeps the
// record RUNNING (so kill still reaches it), its late output still lands
// in the logs, and the exit frame follows all of it.
func TestProcessDescendantsAfterLeader(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const lateID, killID = 240, 241
	h.start(t, lateID, "( sleep 0.6; echo late ) & exit 0", nil)
	time.Sleep(300 * time.Millisecond) // the leader is gone; the child is not

	out, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: lateID})
	if err != nil {
		t.Fatal(err)
	}
	if out.Process.State != nodev1.ProcessState_PROCESS_STATE_RUNNING {
		t.Fatalf("state = %s while a group descendant still runs, want RUNNING", out.Process.State)
	}

	// Attach from zero: the exit frame must arrive after the child's
	// output, and the final record must account for it.
	a, err := procAttach(t.Context(), h.c, &guestv1.AttachProcessRequest{ProcessId: lateID})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if !strings.Contains(a.Stdout, "late\n") {
		t.Fatalf("late descendant output missing from attach: %q", a.Stdout)
	}
	if a.Exit.State != nodev1.ProcessState_PROCESS_STATE_EXITED ||
		a.Exit.Exit == nil || a.Exit.Exit.ExitCode == nil || *a.Exit.Exit.ExitCode != 0 {
		t.Fatalf("exit = %+v", a.Exit)
	}
	if a.Exit.StdoutSize < int64(len(a.Stdout)) {
		t.Fatalf("exit sizes %d below streamed output %d", a.Exit.StdoutSize, len(a.Stdout))
	}

	// Kill while only a descendant runs: TERM must reach it.
	h.start(t, killID, "( sleep 30 ) & exit 0", nil)
	time.Sleep(300 * time.Millisecond)
	k, err := h.c.KillProcess(t.Context(), &guestv1.KillProcessRequest{ProcessId: killID, GraceSeconds: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !k.Signaled {
		t.Fatalf("kill of a leader-less group = %+v, want signaled", k)
	}
	procDead(t, int(k.Process.Pid))
}

// TestProcessFailedStartRollback proves a failed start leaves nothing
// behind: no metadata (so a restart does not resurrect a LOST process), no
// log debris for the retry to append after, no temporary files.
func TestProcessFailedStartRollback(t *testing.T) {
	base := t.TempDir()
	var failSync atomic.Bool
	h := newProcHarness(t, base, func(c *guest.Config) {
		c.SyncFile = func(f *os.File) error {
			// Failing only the metadata sync leaves the state creation
			// intact, so the rollback has real files to remove.
			if failSync.Load() && strings.Contains(f.Name(), "meta.json") {
				return errors.New("injected sync failure")
			}
			return nil
		}
	})
	procs := filepath.Join(base, "home/user/.bbraft/processes")
	noDebris := func(stage string) {
		t.Helper()
		entries, err := os.ReadDir(procs)
		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			t.Fatal(err)
		}
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), ".tmp.") || e.Name() == "250" ||
				e.Name() == "250.log" || e.Name() == "250.err.log" || e.Name() == "251" {
				t.Fatalf("%s left debris: %s", stage, e.Name())
			}
		}
	}

	// Metadata write failure (before the fork, after the state was
	// created): INTERNAL, nothing left.
	failSync.Store(true)
	_, err := h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: 250, Command: "echo x"})
	wantCode(t, err, codes.Internal)
	noDebris("metadata failure")
	if _, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: 250}); status.Code(err) != codes.NotFound {
		t.Fatalf("failed start left a record: %v", err)
	}

	// A directory planted at the stderr log name fails the state creation
	// after the stdout log exists: the rollback must remove both logs and
	// the metadata directory.
	if err := os.MkdirAll(filepath.Join(procs, "251.err.log"), 0o755); err != nil {
		t.Fatal(err)
	}
	failSync.Store(false)
	_, err = h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: 251, Command: "echo x"})
	wantCode(t, err, codes.Internal)
	noDebris("partial state failure")
	if err := os.Remove(filepath.Join(procs, "251.err.log")); err != nil {
		t.Fatal(err)
	}

	// A regular file planted at a fresh id's log name is refused — a fresh
	// process never appends after someone else's bytes — and the plant
	// survives untouched.
	if err := os.WriteFile(filepath.Join(procs, "253.log"), []byte("foreign"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: 253, Command: "echo x"})
	wantCode(t, err, codes.Internal)
	if got := readFileOrEmpty(filepath.Join(procs, "253.log")); got != "foreign" {
		t.Fatalf("foreign log not preserved: %q", got)
	}

	// A pre-existing metadata directory is refused as well, preserved.
	if err := os.MkdirAll(filepath.Join(procs, "252"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err = h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: 252, Command: "echo x"})
	wantCode(t, err, codes.Internal)
	if fi, err := os.Stat(filepath.Join(procs, "252")); err != nil || !fi.IsDir() {
		t.Fatalf("pre-existing metadata directory not preserved: %v", err)
	}

	// An invalid request must not touch foreign state either: a bad cwd
	// fails validation before anything is created or removed.
	if err := os.WriteFile(filepath.Join(procs, "254.log"), []byte("foreign-cwd"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: 254, Command: "echo x", Cwd: "/etc"})
	wantCode(t, err, codes.InvalidArgument)
	if got := readFileOrEmpty(filepath.Join(procs, "254.log")); got != "foreign-cwd" {
		t.Fatalf("bad cwd erased foreign state: %q", got)
	}
	if err := os.Remove(filepath.Join(procs, "253.log")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(procs, "252")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(procs, "254.log")); err != nil {
		t.Fatal(err)
	}

	// A restart must not resurrect either failed attempt.
	a := newProcHarness(t, base, nil)
	for _, id := range []uint32{250, 251} {
		_, err := a.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
		wantCode(t, err, codes.NotFound)
	}

	// Both ids start cleanly on the original service, with no stale bytes
	// in their fresh logs.
	h.start(t, 250, "echo fresh250", nil)
	h.start(t, 251, "echo fresh251", nil)
	for _, id := range []uint32{250, 251} {
		out := h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
		if want := fmt.Sprintf("fresh%d\n", id); string(out.Stdout) != want {
			t.Fatalf("process %d stdout = %q, want %q (stale log bytes survived the rollback)", id, out.Stdout, want)
		}
	}
}

// TestProcessRollbackRetention: when a failed start cannot prove its
// rollback durable, the reservation is retained — the id answers INTERNAL
// mentioning the cleanup — and the next start retries the cleanup and
// proceeds once it proves out.
func TestProcessRollbackRetention(t *testing.T) {
	base := t.TempDir()
	var failMeta, rollbackBroken atomic.Bool
	h := newProcHarness(t, base, func(c *guest.Config) {
		c.SyncFile = func(f *os.File) error {
			name := f.Name()
			if failMeta.Load() && strings.Contains(name, "meta.json") {
				rollbackBroken.Store(true) // from here on, the rollback's parent sync fails too
				return errors.New("injected metadata sync failure")
			}
			if rollbackBroken.Load() && strings.HasSuffix(name, "/processes") {
				return errors.New("injected rollback sync failure")
			}
			return nil
		}
	})
	const id = 258

	failMeta.Store(true)
	_, err := h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: id, Command: "echo x"})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "cleanup also failed") {
		t.Fatalf("start with a broken rollback = %v, want INTERNAL mentioning the failed cleanup", err)
	}

	// The reservation is retained: a retry still cannot prove the cleanup,
	// so the id is not handed out for a fresh start.
	_, err = h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: id, Command: "echo y"})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "not durably removed") {
		t.Fatalf("retry with the cleanup still broken = %v, want INTERNAL about the retained state", err)
	}

	// Once the cleanup proves out, the same id starts fresh and clean.
	failMeta.Store(false)
	rollbackBroken.Store(false)
	h.start(t, id, "echo after-retention", nil)
	out := h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	if string(out.Stdout) != "after-retention\n" {
		t.Fatalf("stdout after retained rollback = %q", out.Stdout)
	}
}

// TestProcessLeaderPIDOwnedWhileDescendantsRun: the bash leader stays
// un-reaped (its /proc entry persists) while group descendants still run,
// keeping the pid — and the group id — owned; the reap happens when the
// process finalizes.
func TestProcessLeaderPIDOwnedWhileDescendantsRun(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const id = 280
	p := h.start(t, id, "( sleep 1 ) & exit 0", nil)
	leader := fmt.Sprintf("/proc/%d", p.Pid)

	// The leader exits within milliseconds; the descendant holds the group
	// for a second. During that window the leader must remain owned.
	time.Sleep(400 * time.Millisecond)
	if _, err := os.Stat(leader); err != nil {
		t.Fatalf("leader pid %d was reaped while its group still runs: %v", p.Pid, err)
	}
	out, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatal(err)
	}
	if out.Process.State != nodev1.ProcessState_PROCESS_STATE_RUNNING {
		t.Fatalf("state = %s while the group still runs", out.Process.State)
	}

	h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(leader); os.IsNotExist(err) {
			return // reaped with finalization: ownership truly ended
		}
		if time.Now().After(deadline) {
			t.Fatal("leader was not reaped after the process finalized")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestProcessSpecialFilesRefused: FIFOs (and other special files) planted
// at process state names must be rejected without hanging — on the live
// start path, on reads, and at recovery.
func TestProcessSpecialFilesRefused(t *testing.T) {
	base := t.TempDir()
	home := filepath.Join(base, "home/user")
	procs := filepath.Join(home, ".bbraft/processes")
	if err := os.MkdirAll(procs, 0o755); err != nil {
		t.Fatal(err)
	}
	// A recovered process whose stdout log is a FIFO.
	dir250 := filepath.Join(procs, "250")
	if err := os.MkdirAll(dir250, 0o755); err != nil {
		t.Fatal(err)
	}
	meta := `{"version":1,"process_id":250,"command":"c","cwd":"/tmp","pid":9,"started_at_unix_ms":1731300000000}`
	if err := os.WriteFile(filepath.Join(dir250, "meta.json"), []byte(meta), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(procs, "250.log"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A FIFO in place of another process's metadata, and one at a fresh
	// id's stdout log for the start path.
	if err := syscall.Mkfifo(filepath.Join(procs, "251"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(procs, "252.log"), 0o644); err != nil {
		t.Fatal(err)
	}

	h := newProcHarness(t, base, nil)
	// Recovery: the FIFO metadata is invisible; the FIFO log fails the
	// read fast instead of blocking until a writer appears.
	if _, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: 251}); status.Code(err) != codes.NotFound {
		t.Fatalf("FIFO metadata recovered as a process: %v", err)
	}
	err := procRunWithDeadline(t, 3*time.Second, "GetProcess with FIFO log", func() error {
		_, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: 250})
		return err
	})
	wantCode(t, err, codes.Internal)
	// Start: the FIFO at the fresh id's log name fails fast.
	err = procRunWithDeadline(t, 3*time.Second, "StartProcess with FIFO log", func() error {
		_, err := h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: 252, Command: "echo x"})
		return err
	})
	wantCode(t, err, codes.Internal)
}

// TestProcessDurabilityOrder records the order SyncFile is asked to make
// things durable: the processes directory entry exists durably before any
// metadata, and the exit record only after both logs.
func TestProcessDurabilityOrder(t *testing.T) {
	var mu sync.Mutex
	var synced []string
	h := newProcTempHarness(t, func(c *guest.Config) {
		c.SyncFile = func(f *os.File) error {
			mu.Lock()
			synced = append(synced, f.Name())
			mu.Unlock()
			return nil
		}
	})
	const id = 255
	h.start(t, id, "printf out; printf err >&2", nil)
	h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)

	mu.Lock()
	defer mu.Unlock()
	index := func(sub string) int {
		for i, name := range synced {
			if strings.Contains(name, sub) {
				return i
			}
		}
		return -1
	}
	if i, j := index("/processes"), index("meta.json"); i < 0 || j < 0 || i > j {
		t.Fatalf("processes parent (index %d) must sync before metadata (index %d): %v", i, j, synced)
	}
	if i, j := index("255.log"), index("exit.json"); i < 0 || j < 0 || i > j {
		t.Fatalf("stdout log (index %d) must sync before the exit record (index %d): %v", i, j, synced)
	}
	if i, j := index("255.err.log"), index("exit.json"); i < 0 || j < 0 || i > j {
		t.Fatalf("stderr log (index %d) must sync before the exit record (index %d): %v", i, j, synced)
	}
}

// TestProcessShrunkLog: a log truncated underneath the service yields the
// bytes actually on disk, never fabricated zero padding.
func TestProcessShrunkLog(t *testing.T) {
	base := t.TempDir()
	h := newProcHarness(t, base, nil)
	const id = 260
	h.start(t, id, "printf 0123456789", nil)
	h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	log := filepath.Join(base, "home/user/.bbraft/processes/260.log")
	if err := os.Truncate(log, 3); err != nil {
		t.Fatal(err)
	}
	out, err := h.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Stdout) != "012" {
		t.Fatalf("shrunk log read as %q, want the 3 bytes on disk", out.Stdout)
	}
}

// TestProcessAttachBusyStdout: a stdout stream that grows without end
// must not starve stderr — the marker written to stderr mid-stream has to
// arrive while stdout is still flooding — nor may the collected frames be
// corrupted by buffer reuse.
func TestProcessAttachBusyStdout(t *testing.T) {
	h := newProcTempHarness(t, nil)
	const id = 265
	// An endless stdout flood from a subshell; the leader writes the
	// stderr marker, then stays alive so the stream keeps pumping.
	h.start(t, id, `( while :; do printf 0123456789abcdef; done ) & sleep 0.4; printf stderr-marker >&2; sleep 30`, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	st, err := h.c.AttachProcess(ctx, &guestv1.AttachProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(8 * time.Second)
	var stderr bytes.Buffer
	for time.Now().Before(deadline) {
		ev, err := st.Recv()
		if err != nil {
			t.Fatalf("attach ended before the stderr marker: %v", err)
		}
		if e, ok := ev.Event.(*nodev1.ProcessEvent_Stderr); ok {
			stderr.Write(e.Stderr.Data)
			if bytes.Contains(stderr.Bytes(), []byte("stderr-marker")) {
				cancel()
				return // marker observed while stdout floods: pass
			}
		}
	}
	t.Fatalf("stderr starved behind endless stdout: %q", stderr.String())
}

// TestProcessFDBounded: completed and recovered processes release their
// private directory handles; the count of open descriptors stays bounded
// across many ids.
func TestProcessFDBounded(t *testing.T) {
	base := t.TempDir()
	countFDs := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
	settle := func() int {
		time.Sleep(100 * time.Millisecond)
		return countFDs()
	}

	h := newProcHarness(t, base, nil)
	before := settle()
	for id := range uint32(40) {
		h.start(t, 300+id, fmt.Sprintf("echo %d", 300+id), nil)
	}
	for id := range uint32(40) {
		h.waitState(t, 300+id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	}
	afterRun := settle()
	h.shutdown()

	r := newProcHarness(t, base, nil) // recovers all 40 as EXITED
	_ = r
	afterRecover := settle()
	if delta := afterRun - before; delta > 10 {
		t.Fatalf("%d descriptors leaked by 40 completed processes", delta)
	}
	if delta := afterRecover - before; delta > 10 {
		t.Fatalf("%d descriptors leaked by 40 recovered processes", delta)
	}
}

// TestProcessRenamedHome: the state traversal is anchored to the pinned
// home root, so renaming the configured home directory cannot detach the
// process state or silently relocate it.
func TestProcessRenamedHome(t *testing.T) {
	base := t.TempDir()
	h := newProcHarness(t, base, nil)
	const id = 270
	h.start(t, id, "echo before-rename", nil)
	h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)

	home := filepath.Join(base, "home/user")
	renamed := filepath.Join(base, "home/user-moved")
	if err := os.Rename(home, renamed); err != nil {
		t.Fatal(err)
	}
	p, err := h.c.StartProcess(t.Context(), &guestv1.StartProcessRequest{ProcessId: 271, Command: "echo after-rename"})
	if err != nil {
		t.Fatalf("start after the home was renamed: %v", err)
	}
	t.Cleanup(func() { syscall.Kill(-int(p.Pid), syscall.SIGKILL) })
	out := h.waitState(t, 271, nodev1.ProcessState_PROCESS_STATE_EXITED)
	if string(out.Stdout) != "after-rename\n" {
		t.Fatalf("stdout after rename = %q", out.Stdout)
	}
	if _, err := os.Stat(filepath.Join(renamed, ".bbraft/processes/271.log")); err != nil {
		t.Fatalf("state did not follow the renamed home inode: %v", err)
	}
	// No fresh tree appeared at the old path.
	if _, err := os.Stat(filepath.Join(home, ".bbraft")); !os.IsNotExist(err) {
		t.Fatalf("state written to a re-created old home path")
	}
}

// TestProcessRunsAsUserWithGroups is the detached-process half of the
// credential regression: it runs only in throwaway VMs
// (BBRAFT_TEST_GROUPS_ROOT=1 and euid 0, with a guest-layout "user"
// account as in the v1 image). Like TestExecExtraGroupRoot it adds a
// group listing the user to /etc/group and checks that `id -G` reported
// in the process logs matches what id(1) says the account belongs to —
// the same credential builder, now on the StartProcess path.
func TestProcessRunsAsUserWithGroups(t *testing.T) {
	if os.Getenv("BBRAFT_TEST_GROUPS_ROOT") != "1" {
		t.Skip("set BBRAFT_TEST_GROUPS_ROOT=1 in a throwaway VM to run the real /etc/group fixture")
	}
	if os.Geteuid() != 0 {
		t.Fatal("BBRAFT_TEST_GROUPS_ROOT=1 requires root")
	}
	u, err := user.Lookup("user")
	if err != nil {
		t.Fatalf("no guest-layout user account: %v", err)
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		t.Fatal(err)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		t.Fatal(err)
	}
	const extra = 60514
	backup, err := os.ReadFile("/etc/group")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.WriteFile("/etc/group", backup, 0o644); err != nil {
			t.Errorf("restore /etc/group: %v", err)
		}
	})
	line := "bbraft-proctest:x:" + strconv.Itoa(extra) + ":user\n"
	if err := os.WriteFile("/etc/group", append(bytes.Clone(backup), []byte(line)...), 0o644); err != nil {
		t.Fatal(err)
	}
	oracle, err := exec.Command("id", "-G", u.Username).Output()
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Fields(string(oracle))
	slices.Sort(want)

	base := t.TempDir()
	h := newProcHarness(t, base, func(c *guest.Config) {
		c.UID, c.GID, c.NoSetGroups = uid, gid, false
	})
	const id = 190
	h.start(t, id, "id -G", nil)
	out := h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	got := strings.Fields(string(out.Stdout))
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("detached id -G = %v, want the account's set %v", got, want)
	}
}

// TestLeadReviewLongCommandSurvivesRestart: a command longer than the old
// 64 KiB metadata read cap (90k bytes, still legal as a Linux argument) is
// written and must read back after a restart. The write and read share one
// bound, so a successful start can never leave an unreadable record.
func TestLeadReviewLongCommandSurvivesRestart(t *testing.T) {
	base := t.TempDir()
	h := newProcHarness(t, base, nil)
	const id = uint32(9901)
	command := "true #" + strings.Repeat("x", 90_000)
	h.start(t, id, command, nil)
	h.waitState(t, id, nodev1.ProcessState_PROCESS_STATE_EXITED)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		_, err := os.Stat(filepath.Join(h.home, ".bbraft/processes/9901/exit.json"))
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		select {
		case <-deadline.C:
			t.Fatal("completed process did not persist its exit")
		case <-time.After(5 * time.Millisecond):
		}
	}
	h.shutdown()
	restarted := newProcHarness(t, base, nil)
	got, err := restarted.c.GetProcess(t.Context(), &guestv1.GetProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatalf("completed long-command record disappeared after restart: %v", err)
	}
	if got.GetProcess().GetState() != nodev1.ProcessState_PROCESS_STATE_EXITED || got.GetProcess().GetCommand() != command {
		t.Fatalf("completed long-command record changed after restart: state %s, command bytes %d", got.GetProcess().GetState(), len(got.GetProcess().GetCommand()))
	}
}

// TestLeadReviewAttachDrainsEndedLargeLog: attaching to an already-ended
// process sends every byte of its logs before the terminal frame, even when
// a log is many pass budgets long (96 MiB here, three times the old drain).
func TestLeadReviewAttachDrainsEndedLargeLog(t *testing.T) {
	base := t.TempDir()
	home := filepath.Join(base, "home/user")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	const id = uint32(9902)
	const size = int64(96 << 20)
	meta := `{"version":1,"process_id":9902,"command":"large completed output","cwd":"/home/user","pid":42,"started_at_unix_ms":1731300000000}`
	exit := `{"version":1,"exit_code":0,"duration_ms":1,"finished_at_unix_ms":1731300000001}`
	procWriteState(t, home, id, meta, exit, "x", "")
	if err := os.Truncate(filepath.Join(home, ".bbraft/processes/9902.log"), size); err != nil {
		t.Fatal(err)
	}
	h := newProcHarness(t, base, nil)
	stream, err := h.c.AttachProcess(t.Context(), &guestv1.AttachProcessRequest{ProcessId: id})
	if err != nil {
		t.Fatal(err)
	}
	var received int64
	for {
		event, err := stream.Recv()
		if err != nil {
			t.Fatalf("attach ended before its terminal frame: %v", err)
		}
		if chunk := event.GetStdout(); chunk != nil {
			received += int64(len(chunk.GetData()))
			if chunk.GetNextOffset() != received {
				t.Fatalf("chunk offset %d does not follow received bytes %d", chunk.GetNextOffset(), received)
			}
		}
		if event.GetExit() != nil {
			if received != size {
				t.Fatalf("terminal frame after %d stdout bytes, want all %d bytes", received, size)
			}
			return
		}
	}
}
