package cli

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// Attach timing and retry policy. A reconnect is the pinned 10-minute boundary
// and is retried immediately at its offsets. Real failures (transport cuts,
// idle half-open connections, retryable HTTP answers and whitelisted
// post-header error frames) back off and share one continuous-failure budget
// that only real output or a heartbeat resets, so an attach/error loop cannot
// renew it forever.
const (
	execAttachFailureBudget  = 5 * time.Minute
	execAttachInitialBackoff = 250 * time.Millisecond
	execAttachMaxBackoff     = 30 * time.Second
	execAttachNoProgress     = 3
	execAttachIdleTimeout    = 60 * time.Second
)

// errExecAttachIdle marks a stream that produced no complete frame inside the
// idle watchdog, including waiting for the first HTTP headers. It is retried
// from the committed cursors like any other transport cut.
var errExecAttachIdle = errors.New("api: attach stream idle")

// execAttachOptions is one attach request. Offsets are raw log byte offsets.
type execAttachOptions struct {
	SandboxID    string
	ProcessID    uint32
	StdoutOffset int64
	StderrOffset int64
}

// execAttachExit is the validated terminal process state of an attach stream.
type execAttachExit struct {
	Status       string
	ExitCode     *int
	Signal       string
	StdoutOffset int64
	StderrOffset int64
}

// execAttachFrameError is a terminal error frame received after the attached
// frame. Retryable ones are handled inside the runner; a permanent one is
// rendered as the legacy command error frame.
type execAttachFrameError struct {
	Code    string
	Message string
}

// Error implements error.
func (e *execAttachFrameError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

// execAttachRunner reads one detached process's output with reconnects.
type execAttachRunner struct {
	client        execHTTPClient
	out           ExecutionOutput
	failureBudget time.Duration
	initialDelay  time.Duration
	maxDelay      time.Duration
	idleTimeout   time.Duration
	now           func() time.Time
	sleep         func(context.Context, time.Duration) error
}

// newExecAttachRunner builds a runner with the production retry policy.
func newExecAttachRunner(client execHTTPClient, out ExecutionOutput) *execAttachRunner {
	return &execAttachRunner{
		client:        client,
		out:           out,
		failureBudget: execAttachFailureBudget,
		initialDelay:  execAttachInitialBackoff,
		maxDelay:      execAttachMaxBackoff,
		idleTimeout:   execAttachIdleTimeout,
		now:           time.Now,
		sleep:         sleepCtx,
	}
}

// execAttachAttemptKind is how one open stream ended.
type execAttachAttemptKind int

const (
	execAttachAttemptRetry execAttachAttemptKind = iota
	execAttachAttemptReconnect
	execAttachAttemptExit
)

// execAttachAttempt is the result of one attach connection.
type execAttachAttempt struct {
	kind      execAttachAttemptKind
	exit      execAttachExit
	stdout    int64
	stderr    int64
	progress  bool
	retryable bool
	writeErr  bool
	err       error
}

// run reads the stream until the process exits. It returns the terminal exit,
// a permanent error, a write failure or the context error. Every retry resumes
// at the last fully emitted frame, so output is neither lost nor repeated.
func (r *execAttachRunner) run(ctx context.Context, opts execAttachOptions) (execAttachExit, error) {
	stdout, stderr := opts.StdoutOffset, opts.StderrOffset
	noProgress := 0
	var failingSince time.Time
	delay := r.initialDelay
	for {
		if err := ctx.Err(); err != nil {
			return execAttachExit{}, err
		}
		attempt := r.session(ctx, opts.SandboxID, opts.ProcessID, stdout, stderr)
		if attempt.err != nil {
			if ctx.Err() != nil {
				return execAttachExit{}, ctx.Err()
			}
			if attempt.writeErr {
				return execAttachExit{}, attempt.err
			}
			if !attempt.retryable {
				return execAttachExit{}, attempt.err
			}
			if attempt.progress {
				failingSince = time.Time{}
				noProgress = 0
				delay = r.initialDelay
			}
			if failingSince.IsZero() {
				failingSince = r.now()
			}
			if r.now().Sub(failingSince) >= r.failureBudget {
				return execAttachExit{}, r.failure(opts, "the command output stream could not be reconnected")
			}
			if err := r.sleep(ctx, delay); err != nil {
				return execAttachExit{}, err
			}
			delay = min(delay*2, r.maxDelay)
			stdout, stderr = attempt.stdout, attempt.stderr
			continue
		}

		if attempt.progress {
			failingSince = time.Time{}
			noProgress = 0
			delay = r.initialDelay
		}
		switch attempt.kind {
		case execAttachAttemptExit:
			return attempt.exit, nil
		case execAttachAttemptReconnect:
			stdout, stderr = attempt.stdout, attempt.stderr
			if !attempt.progress {
				noProgress++
			}
			if noProgress >= execAttachNoProgress {
				return execAttachExit{}, r.failure(opts, "the sandbox machine kept reconnecting without output")
			}
		default:
			// A session that reports no terminal frame and no error can
			// never be resumed safely.
			return execAttachExit{}, r.failure(opts, "the command output stream ended unexpectedly")
		}
	}
}

// failure is the safe terminal stream error. It names the known process and
// how to reattach, and never includes the command text or a native error.
func (r *execAttachRunner) failure(opts execAttachOptions, reason string) *CLIError {
	return &CLIError{
		Code: "attach_failed",
		Message: fmt.Sprintf("%s; process %d may still be running: bbraft exec %s --status %d",
			reason, opts.ProcessID, opts.SandboxID, opts.ProcessID),
	}
}

// session reads one HTTP attach stream from offset until it ends. The returned
// stdout/stderr are the offsets to resume from; the frame's nextOffset is
// authoritative because the server already repaired invalid UTF-8 while it
// still counts original raw log bytes. A watchdog cancels a connection that
// produces no complete frame, including while waiting for HTTP headers.
func (r *execAttachRunner) session(ctx context.Context, sandboxID string, processID uint32, stdoutOffset, stderrOffset int64) execAttachAttempt {
	attempt := execAttachAttempt{stdout: stdoutOffset, stderr: stderrOffset}
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var idle atomic.Bool
	resetIdle, stopIdle := r.watchIdle(cancel, &idle)
	defer stopIdle()
	resp, err := execProcessAPI{client: r.client}.attach(attemptCtx, sandboxID, processID, stdoutOffset, stderrOffset)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			attempt.err = ctxErr
			return attempt
		}
		if idle.Load() {
			attempt.err = errExecAttachIdle
			attempt.retryable = true
			return attempt
		}
		attempt.err = err
		attempt.retryable = execAttachRetryable(err)
		return attempt
	}
	defer resp.Body.Close()

	sink := &execStreamSink{out: r.out, cancel: cancel, body: resp.Body}
	reader := bufio.NewReaderSize(resp.Body, 64<<10)
	attached := false
	cursorOut, cursorErr := stdoutOffset, stderrOffset
	for {
		line, complete, readErr := execReadCompleteNDJSONLine(reader, execMaxFrameBytes)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				attempt.err = ctxErr
				return attempt
			}
			if idle.Load() {
				attempt.err = errExecAttachIdle
				attempt.retryable = true
				return attempt
			}
			if errors.Is(readErr, errExecFrameTooLarge) {
				attempt.err = execInvalidStream("frame is too large")
				return attempt
			}
			attempt.err = readErr
			attempt.retryable = true
			return attempt
		}
		if complete {
			frame, decodeErr := execDecodeAttachFrame(line)
			if decodeErr != nil {
				attempt.err = decodeErr
				return attempt
			}
			// A complete protocol frame, including a heartbeat, proves
			// the connection is alive.
			resetIdle()
			if err := r.applyFrame(sink, &attempt, &attached, &cursorOut, &cursorErr, processID, stdoutOffset, stderrOffset, frame); err != nil {
				return attempt
			}
			if attempt.kind == execAttachAttemptExit || attempt.kind == execAttachAttemptReconnect {
				return attempt
			}
			if attempt.err != nil {
				return attempt
			}
		}
		if errors.Is(readErr, io.EOF) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				attempt.err = ctxErr
				return attempt
			}
			if idle.Load() {
				attempt.err = errExecAttachIdle
				attempt.retryable = true
				return attempt
			}
			// A final line without a newline is a cut frame. Discard it:
			// reopening at the last emitted offset replays it whole.
			attempt.err = io.EOF
			attempt.retryable = true
			return attempt
		}
	}
}

// watchIdle starts the idle watchdog. cancel aborts the in-flight request and
// body read; the returned stop joins the watchdog goroutine.
func (r *execAttachRunner) watchIdle(cancel context.CancelFunc, fired *atomic.Bool) (reset func(), stop func()) {
	if r.idleTimeout <= 0 {
		return func() {}, func() {}
	}
	resetCh := make(chan struct{}, 1)
	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		timer := time.NewTimer(r.idleTimeout)
		defer timer.Stop()
		for {
			select {
			case <-resetCh:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(r.idleTimeout)
			case <-timer.C:
				fired.Store(true)
				cancel()
				return
			case <-stopCh:
				return
			}
		}
	}()
	return func() {
			select {
			case resetCh <- struct{}{}:
			default:
			}
		}, func() {
			close(stopCh)
			<-done
		}
}

// applyFrame validates one decoded frame against the stream state, renders its
// output and updates the resume offsets.
func (r *execAttachRunner) applyFrame(sink *execStreamSink, attempt *execAttachAttempt, attached *bool, cursorOut, cursorErr *int64, processID uint32, startOut, startErr int64, frame api.CommandAttachFrame) error {
	switch frame.Type {
	case api.CommandAttachAttached:
		// processId is optional in the pinned frame schema, but a
		// different nonzero id is a protocol violation.
		if *attached || frame.ProcessID != 0 && frame.ProcessID != processID ||
			frame.StdoutOffset == nil || frame.StderrOffset == nil ||
			*frame.StdoutOffset < 0 || *frame.StderrOffset < 0 ||
			*frame.StdoutOffset > startOut || *frame.StderrOffset > startErr {
			attempt.err = execInvalidStream("invalid attached frame")
			return attempt.err
		}
		*attached = true
		*cursorOut, *cursorErr = *frame.StdoutOffset, *frame.StderrOffset
		attempt.stdout, attempt.stderr = *cursorOut, *cursorErr
	case api.CommandAttachStdout, api.CommandAttachStderr:
		cursor := cursorOut
		kind := api.CommandFrameStdout
		if frame.Type == api.CommandAttachStderr {
			cursor = cursorErr
			kind = api.CommandFrameStderr
		}
		// A data frame must carry real output: an absent or empty chunk
		// with a positive offset would silently skip log bytes.
		if !*attached || frame.NextOffset == nil || frame.Data == "" || *frame.NextOffset <= *cursor {
			attempt.err = execInvalidStream("invalid output frame")
			return attempt.err
		}
		if err := sink.emit(api.CommandStreamFrame{Type: kind, Data: frame.Data}); err != nil {
			attempt.err = err
			attempt.writeErr = true
			return err
		}
		*cursor = *frame.NextOffset
		if frame.Type == api.CommandAttachStdout {
			attempt.stdout = *cursorOut
		} else {
			attempt.stderr = *cursorErr
		}
		attempt.progress = true
	case api.CommandAttachHeartbeat:
		if !*attached {
			attempt.err = execInvalidStream("heartbeat before attached")
			return attempt.err
		}
		attempt.progress = true
	case api.CommandAttachReconnect:
		// A control frame reports exactly the server's fully emitted
		// cursors. Any other value would skip or repeat output.
		if !*attached || frame.StdoutOffset == nil || frame.StderrOffset == nil ||
			*frame.StdoutOffset != *cursorOut || *frame.StderrOffset != *cursorErr {
			attempt.err = execInvalidStream("invalid reconnect frame")
			return attempt.err
		}
		attempt.kind = execAttachAttemptReconnect
		attempt.stdout, attempt.stderr = *cursorOut, *cursorErr
	case api.CommandAttachExit:
		exit, err := execAttachExitFromFrame(frame)
		if err != nil || !*attached || exit.StdoutOffset != *cursorOut || exit.StderrOffset != *cursorErr {
			attempt.err = execInvalidStream("invalid exit frame")
			return attempt.err
		}
		attempt.kind = execAttachAttemptExit
		attempt.exit = exit
		attempt.stdout, attempt.stderr = exit.StdoutOffset, exit.StderrOffset
	case api.CommandAttachError:
		if !*attached || frame.Error == "" || frame.StdoutOffset == nil || frame.StderrOffset == nil ||
			*frame.StdoutOffset != *cursorOut || *frame.StderrOffset != *cursorErr {
			attempt.err = execInvalidStream("invalid error frame")
			return attempt.err
		}
		attempt.stdout, attempt.stderr = *cursorOut, *cursorErr
		frameErr := &execAttachFrameError{
			Code:    frame.Error,
			Message: cmp.Or(frame.Message, "the command output stream failed"),
		}
		if execAttachFrameRetryable(frame.Error) {
			attempt.err = frameErr
			attempt.retryable = true
			return attempt.err
		}
		attempt.err = frameErr
	default:
		attempt.err = execInvalidStream("unknown frame type")
		return attempt.err
	}
	return nil
}

// execAttachExitFromFrame validates the terminal exit members and converts a
// numeric code or signal into the legacy process status.
func execAttachExitFromFrame(frame api.CommandAttachFrame) (execAttachExit, error) {
	exit := execAttachExit{
		Status: frame.Status,
		Signal: "",
	}
	if frame.StdoutOffset == nil || frame.StderrOffset == nil ||
		frame.Status != "exited" && frame.Status != "lost" ||
		!frame.ExitCode.Present || !frame.Signal.Present {
		return exit, errors.New("invalid exit frame")
	}
	exit.StdoutOffset, exit.StderrOffset = *frame.StdoutOffset, *frame.StderrOffset
	if exit.StdoutOffset < 0 || exit.StderrOffset < 0 {
		return exit, errors.New("negative exit offset")
	}
	if frame.ExitCode.Value != nil {
		code := *frame.ExitCode.Value
		if code < 0 || code > 255 {
			return exit, errors.New("exit code out of range")
		}
		exit.ExitCode = new(code)
	}
	if frame.Signal.Value != nil {
		exit.Signal = *frame.Signal.Value
	}
	if exit.ExitCode != nil && exit.Signal != "" {
		return exit, errors.New("exit code and signal are mutually exclusive")
	}
	switch frame.Status {
	case "lost":
		// A lost process has no trustworthy result and must never claim
		// exit 0.
		if exit.ExitCode != nil || exit.Signal != "" {
			return exit, errors.New("lost exit carries a result")
		}
	case "exited":
		if (exit.ExitCode == nil) == (exit.Signal == "") {
			return exit, errors.New("exited status needs exactly one of exit code and signal")
		}
	}
	return exit, nil
}

// execAttachRetryable classifies a failure before the attached frame. A local
// protocol error is permanent. A transport failure is retryable. For an HTTP
// answer the status allowlist runs first: every 4xx is permanent even with an
// explicit retryable=true; 500 retries only with retryable=true; 502/503/504
// retry unless retryable=false; every other status is permanent.
func execAttachRetryable(err error) bool {
	if _, ok := errors.AsType[*CLIError](err); ok {
		return false
	}
	apiErr, ok := errors.AsType[*APIError](err)
	if !ok {
		return true
	}
	if apiErr.Status >= 400 && apiErr.Status < 500 {
		return false
	}
	switch apiErr.Status {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		if apiErr.Retryable != nil {
			return *apiErr.Retryable
		}
		return true
	case http.StatusInternalServerError:
		return apiErr.Retryable != nil && *apiErr.Retryable
	default:
		return false
	}
}

// execAttachFrameRetryable reports whether a post-header error code is a known
// transient node or store fault. Every other code is permanent.
func execAttachFrameRetryable(code string) bool {
	return code == "sandbox_direct_failed" || code == "http_500"
}

// execReadCompleteNDJSONLine reads one newline-terminated frame within limit.
// A final fragment without a newline reports complete=false with io.EOF so the
// caller can discard a frame cut by a network failure instead of parsing it.
func execReadCompleteNDJSONLine(reader *bufio.Reader, limit int) (line []byte, complete bool, err error) {
	var buffered []byte
	for {
		fragment, readErr := reader.ReadSlice('\n')
		if len(buffered)+len(fragment) > limit {
			return nil, false, errExecFrameTooLarge
		}
		buffered = append(buffered, fragment...)
		switch {
		case readErr == nil:
			return buffered, true, nil
		case errors.Is(readErr, bufio.ErrBufferFull):
			continue
		case errors.Is(readErr, io.EOF):
			return buffered, false, io.EOF
		default:
			return buffered, false, readErr
		}
	}
}

// execDecodeAttachFrame parses one pinned attach frame, rejecting unknown or
// wrongly typed members.
func execDecodeAttachFrame(line []byte) (api.CommandAttachFrame, error) {
	var frame api.CommandAttachFrame
	dec := jsontext.NewDecoder(bytes.NewReader(line))
	raw, err := dec.ReadValue()
	if err != nil {
		return frame, execInvalidStream("malformed frame")
	}
	raw = raw.Clone()
	if _, err := dec.ReadValue(); !errors.Is(err, io.EOF) {
		return frame, execInvalidStream("trailing data in frame")
	}
	var members map[string]jsontext.Value
	if err := json.Unmarshal(raw, &members); err != nil {
		return frame, execInvalidStream("malformed frame")
	}
	if !execValidAttachMembers(members) {
		return frame, execInvalidStream("malformed frame")
	}
	if err := json.Unmarshal(raw, &frame, json.RejectUnknownMembers(true)); err != nil {
		return frame, execInvalidStream("malformed frame")
	}
	return frame, nil
}

// execValidAttachMembers rejects nulls and wrong JSON kinds before a typed
// decode can hide them in zero values.
func execValidAttachMembers(members map[string]jsontext.Value) bool {
	for name, value := range members {
		switch name {
		case "type", "status", "data", "error", "message":
			if !execJSONString(value) {
				return false
			}
		case "processId", "nextOffset", "stdoutOffset", "stderrOffset":
			if !execJSONInteger(value) {
				return false
			}
		case "exitCode":
			if value.Kind() == jsontext.KindNull {
				continue
			}
			if !execJSONInteger(value) {
				return false
			}
		case "signal":
			if value.Kind() == jsontext.KindNull {
				continue
			}
			if !execJSONString(value) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// execAttachExitFrame builds the legacy terminal exit frame. The pinned stream
// frame has no signal member, so a signal exit is a null code.
func execAttachExitFrame(exit execAttachExit) api.CommandStreamFrame {
	frame := api.CommandStreamFrame{
		Type:     api.CommandFrameExit,
		TimedOut: api.Some(false),
		ExitCode: api.Null[int](),
		Success:  api.Some(false),
	}
	if exit.ExitCode != nil {
		frame.ExitCode = api.Some(*exit.ExitCode)
		frame.Success = api.Some(*exit.ExitCode == 0)
	}
	return frame
}

// execAttachTimeoutFrame is the local-watchdog equivalent of a server timeout.
func execAttachTimeoutFrame() api.CommandStreamFrame {
	return api.CommandStreamFrame{
		Type:     api.CommandFrameExit,
		TimedOut: api.Some(true),
		ExitCode: api.Null[int](),
		Success:  api.Some(false),
	}
}

// execAttachErrorFrame builds the legacy error frame of a permanent attach
// failure.
func execAttachErrorFrame(code, message string) api.CommandStreamFrame {
	return api.CommandStreamFrame{
		Type:    api.CommandFrameError,
		Error:   code,
		Message: message,
	}
}

// execIsWriteFailure reports whether err is the safe broken-writer error.
func execIsWriteFailure(err error) bool {
	cliErr, ok := errors.AsType[*CLIError](err)
	return ok && cliErr.Code == "write_failed"
}

// execAttachStatusExitCode is the process status of a terminal attach result.
// A null exit code (signal or lost) is status 1, matching the legacy stream.
func execAttachStatusExitCode(exit execAttachExit) int {
	if exit.ExitCode != nil {
		return *exit.ExitCode
	}
	return 1
}

// execAttachMessage keeps a safe fallback for a permanent frame error.
func execAttachMessage(frameErr *execAttachFrameError) string {
	if frameErr.Message != "" {
		return frameErr.Message
	}
	return "the command output stream failed"
}
