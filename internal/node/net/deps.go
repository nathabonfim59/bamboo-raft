package nodenet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"time"
)

// Runner executes host networking commands. The real implementation is
// [execRunner]; tests inject a recorder so every command, argument and nft
// batch can be asserted without privileges.
type Runner interface {
	// Run executes name with args and returns an error when it cannot start or
	// exits non-zero. stdin, when non-nil, is the command's standard input.
	Run(ctx context.Context, name string, args []string, stdin []byte) error
	// Output executes name with args and returns its standard output.
	Output(ctx context.Context, name string, args []string) ([]byte, error)
}

// CommandError reports a failed host command. Stderr is captured for the error
// message and for callers that must distinguish "does not exist" from other
// failures; it never contains secrets, only ip/nft diagnostics.
type CommandError struct {
	// Name is the executable and Args its arguments, without quoting.
	Name string
	Args []string
	// Stderr is the captured standard error, truncated by the runner.
	Stderr string
	// ExitCode is the process exit status, or -1 when the command could not be
	// started.
	ExitCode int
	// Err is the underlying *exec.ExitError or start error.
	Err error
}

// Error implements error.
func (e *CommandError) Error() string {
	msg := fmt.Sprintf("nodenet: %s %s: %v", e.Name, strings.Join(e.Args, " "), e.Err)
	if stderr := strings.TrimSpace(e.Stderr); stderr != "" {
		msg += ": " + stderr
	}
	return msg
}

// Unwrap returns the underlying process error for errors.Is/As.
func (e *CommandError) Unwrap() error { return e.Err }

// exitCode returns the process exit code of err, or -1 when err is not a
// command error with a status.
func exitCode(err error) int {
	if cmdErr, ok := errors.AsType[*CommandError](err); ok {
		return cmdErr.ExitCode
	}
	return -1
}

// stderrOf returns the captured stderr of err, or the empty string.
func stderrOf(err error) string {
	if cmdErr, ok := errors.AsType[*CommandError](err); ok {
		return cmdErr.Stderr
	}
	return ""
}

// maxStderr bounds captured diagnostics; ip and nft never emit more than a few
// lines, and the bound keeps a broken tool from filling logs.
const maxStderr = 4 << 10

// maxStdout bounds captured standard output. `ip -j addr show` on a large host
// is kilobytes, not megabytes.
const maxStdout = 1 << 20

// commandWaitDelay bounds how long Wait waits for a pipe a grandchild holds
// open after the process itself was killed.
const commandWaitDelay = 5 * time.Second

type execRunner struct{}

// Run implements [Runner].
func (execRunner) Run(ctx context.Context, name string, args []string, stdin []byte) error {
	cmd := hostCommand(ctx, name, args...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	stderr := &limitedBuffer{limit: maxStderr}
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return newCommandError(name, args, err, stderr.Bytes())
	}
	return nil
}

// Output implements [Runner].
func (execRunner) Output(ctx context.Context, name string, args []string) ([]byte, error) {
	cmd := hostCommand(ctx, name, args...)
	stdout := &limitedBuffer{limit: maxStdout}
	stderr := &limitedBuffer{limit: maxStderr}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return nil, newCommandError(name, args, err, stderr.Bytes())
	}
	return stdout.Bytes(), nil
}

// hostCommand builds an ip/nft invocation that dies with its context. The
// process gets its own process group so cancellation kills any child it may
// have started, and WaitDelay bounds a pipe held open by a grandchild.
func hostCommand(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = commandWaitDelay
	return cmd
}

// limitedBuffer keeps at most limit bytes and silently discards the rest, so a
// misbehaving tool cannot exhaust memory or logs. It always reports a full
// write so the child is never killed by EPIPE.
type limitedBuffer struct {
	buf   bytes.Buffer
	limit int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if room := b.limit - b.buf.Len(); room > 0 {
		b.buf.Write(p[:min(room, len(p))])
	}
	return len(p), nil
}

func (b *limitedBuffer) Bytes() []byte { return b.buf.Bytes() }

func newCommandError(name string, args []string, err error, stderr []byte) error {
	code := -1
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		code = exitErr.ExitCode()
	}
	text := stderr
	if len(text) > maxStderr {
		text = text[:maxStderr]
	}
	return &CommandError{
		Name:     name,
		Args:     slices.Clone(args),
		Stderr:   string(text),
		ExitCode: code,
		Err:      err,
	}
}

// Deps are the injectable host facilities. The zero value uses the real
// runner, real TAP devices and a discarded logger.
type Deps struct {
	// Runner executes ip and nft. nil means the real host commands.
	Runner Runner
	// Taps opens TAP and vhost-net file descriptors. nil means LinuxTaps.
	Taps Tapper
	// Logger receives debug and info events. nil discards them.
	Logger *slog.Logger
}

func (d Deps) runner() Runner {
	if d.Runner != nil {
		return d.Runner
	}
	return execRunner{}
}

func (d Deps) taps() Tapper {
	if d.Taps != nil {
		return d.Taps
	}
	return LinuxTaps{}
}

func (d Deps) logger() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.New(slog.DiscardHandler)
}
