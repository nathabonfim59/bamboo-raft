package guest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// baseExecEnv is the documented base environment of every exec, before
// the request environment is applied on top. Request values win for every
// variable except HOME — the confined home never moves — and no
// environment value ever changes the effective uid.
func (s *Service) baseExecEnv() map[string]string {
	return map[string]string{
		"PATH":    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"HOME":    s.homeDir,
		"USER":    "user",
		"LOGNAME": "user",
		"SHELL":   "/bin/bash",
		"LANG":    "C.UTF-8",
		"TERM":    "dumb",
	}
}

// execEventBuffer bounds the stream frames waiting between the output
// writers and the sender goroutine, so one exec uses bounded memory no
// matter how much the command writes.
const execEventBuffer = 16

// Exec runs the command as the sandbox user and streams its output with
// the nodev1 frame order: one started, stdout/stderr chunks in write
// order, then one exit. Timeout, RPC cancellation and a failing stream
// send all kill the command's whole process group; output is drained, the
// process is reaped, and a terminal exit frame is emitted whenever the
// stream is still usable.
func (s *Service) Exec(req *guestv1.ExecRequest, stream grpc.ServerStreamingServer[nodev1.ExecEvent]) error {
	if err := validateEnv(req.GetEnv()); err != nil {
		return err
	}
	// Open the working directory through the confinement root before
	// starting anything: a bad path or a symlink escape never spawns a
	// process, and the opened file pins the directory inode.
	dirFile, _, err := s.openWorkdir(req.GetCwd())
	if err != nil {
		return err
	}
	defer dirFile.Close()
	ctx := stream.Context()
	if t := req.GetTimeoutSeconds(); t > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(t)*time.Second)
		defer cancel()
	}

	envList := s.commandEnvList(req.GetEnv())
	cred, err := s.credential()
	if err != nil {
		return status.Errorf(codes.Internal, "credential: %v", err)
	}

	cmd := exec.CommandContext(ctx, "bash", "-lc", req.GetCommand())
	// The child chdirs via this descriptor before execve: syscall's child
	// code performs the chdir before it shuffles the fd table, and the
	// close-on-exec flag Go sets on dirFile only takes effect at execve.
	// Using the original descriptor (never a dup(2) copy, which would clear
	// close-on-exec and leak a globally inheritable fd into unrelated
	// concurrent execs) pins the inode even if the path is swapped for a
	// symlink afterwards.
	cmd.Dir = "/proc/self/fd/" + strconv.Itoa(int(dirFile.Fd()))
	cmd.Env = envList
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid:    true,
		Credential: cred,
	}
	// The whole process group dies on timeout or cancellation, and Wait
	// still returns (closing the pipes) if an escaped descendant holds
	// them past WaitDelay.
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 5 * time.Second

	// One goroutine owns stream.Send from here on. The command's output
	// reaches it through a bounded channel: exec's own copy goroutines
	// write into it, Wait drains them, and the frames keep their write
	// order.
	events := make(chan *nodev1.ExecEvent, execEventBuffer)
	sendErr := make(chan error, 1)
	cmd.Stdout = streamWriter{events: events, wrap: func(b []byte) *nodev1.ExecEvent {
		return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Stdout{Stdout: b}}
	}}
	cmd.Stderr = streamWriter{events: events, wrap: func(b []byte) *nodev1.ExecEvent {
		return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Stderr{Stderr: b}}
	}}

	if err := cmd.Start(); err != nil {
		return status.Errorf(codes.Internal, "start: %v", err)
	}
	pgid := cmd.Process.Pid
	killGroup := func() { syscall.Kill(-pgid, syscall.SIGKILL) }
	start := time.Now() // near the actual process start, before stream traffic

	// Independent cancellation supervision. exec.CommandContext's own
	// watcher stops as soon as the direct child is reaped, but a background
	// child can keep the inherited stdout/stderr pipes open long after
	// that: a later cancel or timeout would then never reach the group, and
	// only WaitDelay would bound the stream while the child survived. This
	// supervisor lives until every output and Send goroutine has been
	// joined, so a request ending always kills the group, while a normal
	// completion leaves deliberate background children alone.
	supervisorStop := make(chan struct{})
	supervisorDone := make(chan struct{})
	go func() {
		defer close(supervisorDone)
		select {
		case <-ctx.Done():
			killGroup()
		case <-supervisorStop:
		}
	}()
	defer func() {
		close(supervisorStop)
		<-supervisorDone
	}()

	// The started frame is sent by the handler itself, before the sender
	// goroutine exists: never two owners of stream.Send at once.
	if err := stream.Send(&nodev1.ExecEvent{Event: &nodev1.ExecEvent_Started_{
		Started: &nodev1.ExecEvent_Started{Pid: int32(pgid)},
	}}); err != nil {
		// Nothing consumes events yet. A command that already filled the
		// bounded channel would leave exec's copy goroutines blocked, and
		// Wait waits for them: discard everything until Wait has joined
		// them and closed the channel.
		killGroup()
		drained := make(chan struct{})
		go func() {
			for range events {
			}
			close(drained)
		}()
		cmd.Wait()
		close(events)
		<-drained
		return err
	}
	go func() {
		var err error
		for ev := range events {
			if err == nil {
				if err = stream.Send(ev); err != nil {
					// The client is gone: kill the group, keep
					// draining so the writers never block.
					killGroup()
				}
			}
		}
		sendErr <- err
	}()

	waitErr := cmd.Wait()

	// The exit frame goes through the same channel, so it lands after
	// every output frame.
	events <- exitEvent(ctx, cmd, start)
	close(events)
	if err := <-sendErr; err != nil {
		return err
	}
	if ctxErr := stream.Context().Err(); ctxErr != nil {
		return status.FromContextError(ctxErr).Err()
	}
	if cmd.ProcessState == nil {
		return status.Errorf(codes.Internal, "wait: %v", waitErr)
	}
	return nil
}

// openWorkdir resolves and opens a request working directory (empty: the
// home directory) through the confinement roots, rejecting bad paths,
// symlink escapes and non-directories before anything is spawned. The
// returned file pins the directory inode: a child chdirs through it before
// execve, so the path cannot be swapped for a symlink underneath. canonical
// is the v1 API path (/home/user/... or /tmp/...) for process records.
func (s *Service) openWorkdir(wd string) (dirFile *os.File, canonical string, err error) {
	if wd == "" {
		wd = homeSpec
	}
	rt, rel, canonical, err := s.resolve(wd)
	if err != nil {
		return nil, "", err
	}
	dirFile, err = rt.r.Open(rel)
	if err != nil {
		return nil, "", status.Errorf(codes.InvalidArgument, "cwd: %v", err)
	}
	if fi, serr := dirFile.Stat(); serr != nil || !fi.IsDir() {
		dirFile.Close()
		return nil, "", status.Errorf(codes.InvalidArgument, "cwd %q is not a directory", wd)
	}
	return dirFile, canonical, nil
}

// commandEnvList merges a request's environment over the base exec
// environment, keys sorted. HOME is protected: requests cannot move the
// confined home, and no value changes the effective uid.
func (s *Service) commandEnvList(env map[string]string) []string {
	merged := s.baseExecEnv()
	maps.Copy(merged, env)
	merged["HOME"] = s.homeDir
	list := make([]string, 0, len(merged))
	for _, k := range slices.Sorted(maps.Keys(merged)) {
		list = append(list, k+"="+merged[k])
	}
	return list
}

// streamWriter turns exec's output copies into channel sends, cloning
// every chunk: the sender owns the slice once it is queued.
type streamWriter struct {
	events chan<- *nodev1.ExecEvent
	wrap   func([]byte) *nodev1.ExecEvent
}

func (w streamWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		w.events <- w.wrap(slices.Clone(p))
	}
	return len(p), nil
}

// exitEvent reports how the command ended. timed_out is set only for the
// command's own deadline, not for an explicit stream cancellation. The
// oom_killed field stays false: the guest gathers no OOM evidence in this
// phase and must not guess.
func exitEvent(ctx context.Context, cmd *exec.Cmd, start time.Time) *nodev1.ExecEvent {
	exit := &nodev1.ExecEvent_Exit{DurationMs: time.Since(start).Milliseconds()}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		exit.TimedOut = true
	}
	if ps := cmd.ProcessState; ps != nil {
		ws, ok := ps.Sys().(syscall.WaitStatus)
		if ok {
			switch {
			case ws.Exited() && !exit.TimedOut:
				exit.ExitCode = new(int32(ws.ExitStatus()))
			case ws.Signaled():
				exit.Signal = signalName(ws.Signal())
			}
		}
	}
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Exit_{Exit: exit}}
}

// validateEnv rejects environment keys and values that cannot form a
// valid entry ("a=b", NUL bytes); values are never echoed, only keys.
func validateEnv(env map[string]string) error {
	for k := range env {
		if k == "" || strings.ContainsAny(k, "=\x00") || strings.ContainsRune(env[k], '\x00') {
			return status.Errorf(codes.InvalidArgument, "invalid environment key %q", k)
		}
	}
	return nil
}

func signalName(s syscall.Signal) string {
	switch s {
	case syscall.SIGKILL:
		return "SIGKILL"
	case syscall.SIGTERM:
		return "SIGTERM"
	case syscall.SIGINT:
		return "SIGINT"
	case syscall.SIGSEGV:
		return "SIGSEGV"
	case syscall.SIGABRT:
		return "SIGABRT"
	}
	return fmt.Sprintf("SIG%d", int(s))
}

var _ io.Writer = streamWriter{}
