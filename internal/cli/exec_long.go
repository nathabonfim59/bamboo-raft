package cli

import (
	"context"
	"errors"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// Detached long-command policy. A detached start may wait for sandbox
// readiness before it answers, and a cancellation or timeout must stop the
// known remote process with its own bounded context so the cleanup survives
// the cancelled parent.
const (
	execDetachedStartGrace = 30 * time.Second
	execKillCleanupTimeout = 15 * time.Second
)

// execDetachedStartBound bounds the detached start request. The server may
// first wait for sandbox readiness, so the bound includes that wait.
func execDetachedStartBound() time.Duration {
	return api.DefaultCommandReadyDeadline + execDetachedStartGrace
}

// execDetachedCapability reports whether this server advertises the detached
// command operations. Only a valid metadata answer with commands.detached
// true selects the detached path: an absent or false member is legacy, while
// a malformed answer, auth failure or transport failure is an error and no
// command is sent.
func execDetachedCapability(ctx context.Context, client APIClient) (bool, error) {
	limits, err := execDiscoverLimits(ctx, client)
	if errors.Is(err, errExecLegacyServer) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return limits.Detached, nil
}

// execRunDetachedLong starts a long command detached, prints its legacy stream
// frames through attach, and returns its exit status. The command is started
// exactly once: no failure after the start answer is ever retried as a new
// command, because the process may already be running.
func execRunDetachedLong(ctx context.Context, client APIClient, request ExecutionRequest, out ExecutionOutput, timeout time.Duration, result ExecutionResult) (ExecutionResult, error) {
	startCtx, cancelStart := context.WithTimeout(ctx, execDetachedStartBound())
	started, err := execProcessAPI{client: client}.startDetached(startCtx, request.SandboxID, request.Command, request.Cwd)
	cancelStart()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return result, ctxErr
		}
		return result, execStartFailure(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		// The command started, but the caller cancelled before any output.
		// Stop it exactly once instead of orphaning it.
		execStopKnownProcess(ctx, client, request.SandboxID, started.ProcessID, out)
		return result, ctxErr
	}

	// The started frame keeps the legacy stream shape every JSON consumer of
	// exec already parses. A failed write cancels the run and stops the process.
	sink := &execStreamSink{out: out}
	if err := sink.emit(api.CommandStreamFrame{Type: api.CommandFrameStarted, CommandID: started.CommandID}); err != nil {
		execStopKnownProcess(ctx, client, request.SandboxID, started.ProcessID, out)
		return result, err
	}

	attachCtx := ctx
	if timeout > 0 {
		var cancelAttach context.CancelFunc
		attachCtx, cancelAttach = context.WithTimeout(ctx, timeout)
		defer cancelAttach()
	}
	exit, attachErr := newExecAttachRunner(client, out).run(attachCtx, execAttachOptions{
		SandboxID:    request.SandboxID,
		ProcessID:    started.ProcessID,
		StdoutOffset: 0,
		StderrOffset: 0,
	})
	if attachErr != nil {
		// A broken output writer is the caller's failure: stop the command
		// exactly like the legacy stream does when its request is cancelled.
		if execIsWriteFailure(attachErr) {
			execStopKnownProcess(ctx, client, request.SandboxID, started.ProcessID, out)
			return result, attachErr
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			execStopKnownProcess(ctx, client, request.SandboxID, started.ProcessID, out)
			return result, ctxErr
		}
		if errors.Is(attachCtx.Err(), context.DeadlineExceeded) {
			stopped := execStopKnownProcess(ctx, client, request.SandboxID, started.ProcessID, out)
			if ctxErr := ctx.Err(); ctxErr != nil {
				// The caller cancelled while the bounded stop ran. The
				// foreground cancellation wins: no second stop, no terminal
				// frame and no completed result.
				return result, ctxErr
			}
			if out.JSON != nil {
				if err := execWriteJSONLine(out.JSON, execAttachTimeoutFrame()); err != nil {
					// A broken final render is an output failure, not a
					// confirmed timeout result.
					return result, err
				}
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				// The render itself, or another goroutine around it,
				// cancelled the caller. Keep the foreground status and do
				// not publish a completed timeout.
				return result, ctxErr
			}
			result.ExitCode = 124
			result.TimedOut = true
			result.Completed = stopped
			return result, &ExitError{Code: 124}
		}
		if frameErr, ok := errors.AsType[*execAttachFrameError](attachErr); ok {
			message := execAttachMessage(frameErr)
			if out.JSON != nil {
				_ = execWriteJSONLine(out.JSON, execAttachErrorFrame(frameErr.Code, message))
				return result, &ExitError{Code: 1}
			}
			return result, &ExitError{Code: 1, Err: &CLIError{Code: frameErr.Code, Message: message}}
		}
		return result, attachErr
	}

	// A validated attach exit, including LOST, confirms that the process is
	// gone, but the consumer bit is published only after the final render
	// and the cancellation checks.
	if ctxErr := ctx.Err(); ctxErr != nil {
		// The process ended, but the caller cancelled before the terminal
		// frame. Never report a success status after cancellation.
		execStopKnownProcess(ctx, client, request.SandboxID, started.ProcessID, out)
		return result, ctxErr
	}
	code := execAttachStatusExitCode(exit)
	result.ExitCode = code
	if err := sink.emit(execAttachExitFrame(exit)); err != nil {
		return result, err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		execStopKnownProcess(ctx, client, request.SandboxID, started.ProcessID, out)
		return result, ctxErr
	}
	result.Completed = true
	if code != 0 {
		return result, &ExitError{Code: code}
	}
	return result, nil
}

// execAttachStream runs an explicit attach and returns its remote status. The
// caller owns the cancellation policy.
func execAttachStream(ctx context.Context, client APIClient, opts execAttachOptions, out ExecutionOutput) (execAttachExit, error) {
	return newExecAttachRunner(client, out).run(ctx, opts)
}

// execStartFailure classifies a detached start failure. Only a validated API
// error envelope with a 4xx status proves the command never started. Every 5xx
// (including the pinned 502 sandbox_direct_failed) and every transport failure
// may have started it, so it is reported as ambiguous and never retried.
func execStartFailure(err error) error {
	if apiErr, ok := errors.AsType[*APIError](err); ok &&
		apiErr.Envelope && apiErr.Status >= 400 && apiErr.Status < 500 {
		return err
	}
	return &CLIError{
		Code:    "command_ambiguous",
		Message: "the command start could not be confirmed; it may be running and was not retried",
	}
}

// execKillDetachedProcess stops a known process once with an independent
// bounded context. It never retries and never guesses an id. The returned
// response is validated (running:false and a terminal record) by the process
// client before it is returned.
func execKillDetachedProcess(parent context.Context, client execHTTPClient, sandboxID string, processID uint32) (api.CommandStopResponse, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), execKillCleanupTimeout)
	defer cancel()
	return execProcessAPI{client: client}.kill(ctx, sandboxID, processID)
}

// execStopKnownProcess stops a known process once and reports whether the
// single validated stop answer confirmed that it is no longer running.
func execStopKnownProcess(parent context.Context, client execHTTPClient, sandboxID string, processID uint32, out ExecutionOutput) bool {
	_, err := execKillDetachedProcess(parent, client, sandboxID, processID)
	execKillWarning(out, err)
	return err == nil
}

// execKillWarning reports a failed cleanup without failing the command's own
// exit status and without echoing node or transport text.
func execKillWarning(out ExecutionOutput, err error) {
	if err == nil {
		return
	}
	_ = execWriteString(out.Stderr, "bbraft: the command may still be running; stopping it failed\n")
}
