package cli

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

// Explicit exec modes. The zero value keeps the historical command execution.
type execMode int

const (
	execModeRun execMode = iota
	execModeDetach
	execModeStatus
	execModeKill
	execModeAttach
)

// Request bounds of the status and kill calls. Neither waits for a sandbox to
// become ready, so a short bound is enough.
const (
	execStatusTimeout = 30 * time.Second
	execKillTimeout   = 30 * time.Second
)

// execResolveMode validates the explicit mode flags. It runs before any API
// client exists, so a conflicted invocation never reaches the server. A
// --detach=false flag is presence for the conflict rules but keeps the
// foreground mode: only an explicit true selects detached start.
func execResolveMode(cmd *cobra.Command) (execMode, uint32, error) {
	changed := cmd.Flags().Changed
	detachSet := changed("detach")
	detachValue := false
	if detachSet {
		value, err := cmd.Flags().GetBool("detach")
		if err != nil {
			return execModeRun, 0, errors.New("--detach must be a boolean")
		}
		detachValue = value
	}
	explicit := 0
	if detachSet {
		explicit++
	}
	for _, name := range []string{"status", "kill", "attach"} {
		if changed(name) {
			explicit++
		}
	}
	if explicit > 1 {
		return execModeRun, 0, errors.New("--detach, --status, --kill and --attach are mutually exclusive")
	}
	for _, mode := range []struct {
		name string
		kind execMode
	}{
		{"status", execModeStatus},
		{"kill", execModeKill},
		{"attach", execModeAttach},
	} {
		if !changed(mode.name) {
			continue
		}
		for _, other := range []string{"cwd", "timeout", "stream", "detach"} {
			if changed(other) {
				return execModeRun, 0, fmt.Errorf("--%s cannot be used with --%s", other, mode.name)
			}
		}
		raw, err := cmd.Flags().GetString(mode.name)
		if err != nil {
			return execModeRun, 0, fmt.Errorf("--%s needs a positive process id", mode.name)
		}
		processID, err := execParseProcessID(raw)
		if err != nil {
			return execModeRun, 0, fmt.Errorf("--%s: %w", mode.name, err)
		}
		return mode.kind, processID, nil
	}
	if detachSet && detachValue {
		return execModeDetach, 0, nil
	}
	return execModeRun, 0, nil
}

// execParseProcessID parses a positive uint32 process id.
func execParseProcessID(raw string) (uint32, error) {
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || value == 0 {
		return 0, errors.New("process id must be a positive 32-bit integer")
	}
	return uint32(value), nil
}

// execDetach starts a command in the background and prints its process id.
func (a *App) execDetach(ctx context.Context, client APIClient, sandboxID, command, cwd string) error {
	startCtx, cancel := context.WithTimeout(ctx, execDetachedStartBound())
	started, err := execProcessAPI{client: client}.startDetached(startCtx, sandboxID, command, cwd)
	cancel()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return execStartFailure(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		// The command started but the caller cancelled before it learned
		// the process id. Stop it instead of orphaning it.
		execStopKnownProcess(ctx, client, sandboxID, started.ProcessID,
			ExecutionOutput{Stderr: a.opts.Stderr})
		return ctxErr
	}
	if a.out.JSON {
		return a.out.JSONLine(started)
	}
	return a.out.Humanf("process %d started", started.ProcessID)
}

// execStatus prints one detached process status and its log tails.
func (a *App) execStatus(ctx context.Context, client APIClient, sandboxID string, processID uint32) error {
	statusCtx, cancel := context.WithTimeout(ctx, execStatusTimeout)
	defer cancel()
	query := url.Values{}
	if !a.out.JSON {
		// A terminal gets a readable tail; JSON keeps the pinned 8 MiB
		// default that machine consumers can page through with offsets.
		query.Set("tailBytes", "65536")
	}
	status, err := execProcessAPI{client: client}.status(statusCtx, sandboxID, processID, query)
	if err != nil {
		return err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		// No output after cancellation, even if the answer arrived.
		return ctxErr
	}
	if a.out.JSON {
		return a.out.JSONLine(status)
	}
	header := fmt.Sprintf("process %d %s", processID, status.Status)
	if status.Signal != nil {
		header += " signal " + *status.Signal
	} else if status.Status == "exited" && status.ExitCode != nil {
		header += fmt.Sprintf(" %d", *status.ExitCode)
	}
	if err := a.out.Humanf("%s", header); err != nil {
		return err
	}
	if err := execWriteString(a.out.Stdout, status.Stdout); err != nil {
		return err
	}
	return execWriteString(a.out.Stderr, status.Stderr)
}

// execKill stops a detached process and every process it started.
func (a *App) execKill(ctx context.Context, client APIClient, sandboxID string, processID uint32) error {
	killCtx, cancel := context.WithTimeout(ctx, execKillTimeout)
	defer cancel()
	stopped, err := execProcessAPI{client: client}.kill(killCtx, sandboxID, processID)
	if err != nil {
		return err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if a.out.JSON {
		return a.out.JSONLine(stopped)
	}
	if stopped.Signaled {
		return a.out.Humanf("process %d stopped", processID)
	}
	return a.out.Humanf("process %d was not running", processID)
}

// execAttach follows an existing detached process from the start of its logs.
// Cancelling the viewer leaves the process running.
func (a *App) execAttach(ctx context.Context, client APIClient, sandboxID string, processID uint32) error {
	out := ExecutionOutput{Stdout: a.out.Stdout, Stderr: a.opts.Stderr}
	if a.out.JSON {
		out.JSON = a.out.Stdout
	}
	exit, err := execAttachStream(ctx, client, execAttachOptions{
		SandboxID: sandboxID,
		ProcessID: processID,
	}, out)
	if err != nil {
		if ctx.Err() != nil {
			// The viewer stopped; the detached process keeps running.
			return &ExitError{Code: 130}
		}
		if frameErr, ok := errors.AsType[*execAttachFrameError](err); ok {
			message := execAttachMessage(frameErr)
			if out.JSON != nil {
				_ = execWriteJSONLine(out.JSON, execAttachErrorFrame(frameErr.Code, message))
				return &ExitError{Code: 1}
			}
			return &ExitError{Code: 1, Err: &CLIError{Code: frameErr.Code, Message: message}}
		}
		return err
	}
	if ctx.Err() != nil {
		return &ExitError{Code: 130}
	}
	sink := &execStreamSink{out: out}
	if err := sink.emit(execAttachExitFrame(exit)); err != nil {
		return err
	}
	if code := execAttachStatusExitCode(exit); code != 0 {
		return &ExitError{Code: code}
	}
	return nil
}

// execModeName is the flag name of an explicit mode for error text.
func execModeName(mode execMode) string {
	switch mode {
	case execModeDetach:
		return "detach"
	case execModeStatus:
		return "status"
	case execModeKill:
		return "kill"
	case execModeAttach:
		return "attach"
	default:
		return "run"
	}
}

// execModeNeedsCommand reports whether the mode sends a command and therefore
// needs a command word after the sandbox id.
func execModeNeedsCommand(mode execMode) bool {
	return mode == execModeRun || mode == execModeDetach
}
