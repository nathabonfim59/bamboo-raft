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
	"math"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
)

// Command limits and bounds. The v1 API document keeps synchronous
// commands at 600 seconds. Streamed commands may use the server's advertised
// streamMaxSeconds instead; the CLI never invents it and never sends zero.
const (
	execDefaultTimeoutSeconds int64 = api.DefaultCommandTimeoutSeconds
	execMaxSyncSeconds        int64 = api.MaxCommandTimeoutSeconds
	execMaxTimeoutSeconds     int64 = math.MaxUint32
	execFallbackStreamSeconds int64 = 600

	execInfoPath = "/api/bbraft/info"
	execInfoType = "bbraft.info"
)

// Client-side bounds. Node output is delivered in chunks; 32 MiB leaves room
// for the worst-case JSON escaping of one gRPC message while still bounding a
// hostile frame. The metadata response is tiny by contract.
const (
	execMetadataTimeout  = 5 * time.Second
	execMetadataMaxBytes = 64 << 10
	execSyncMaxBytes     = 32 << 20
	execMaxFrameBytes    = 32 << 20

	// execClientGrace waits past the server's own execution grace so a healthy
	// server always gets to send its terminal timeout envelope first.
	execClientGrace = api.DefaultCommandExecutionGrace + 5*time.Second
)

// errExecFrameTooLarge reports a stream frame outside the bounded reader.
var errExecFrameTooLarge = errors.New("exec: stream frame is too large")

// ExecutionRequest is one command sent over the v1 command API. Command is
// the exact shell command line (the node runs it with `bash -lc`); callers
// that have argv must quote and join it themselves. TimeoutSeconds is a
// resolved number of seconds: zero asks ExecuteCommand to discover the
// server's streamed cap and is never sent to /api/v1, unless DetachedAllowed
// already confirmed the detached capability.
type ExecutionRequest struct {
	SandboxID      string
	Command        string
	Cwd            string
	TimeoutSeconds int64
	Stream         bool
	// DetachedAllowed reports that the caller already confirmed this server
	// advertises commands.detached. With a zero TimeoutSeconds it selects an
	// unlimited detached start with no metadata request; above the pinned
	// synchronous cap it selects the detached path with a local timeout. A
	// false value keeps the legacy path, and a positive timeout at or below
	// the cap stays synchronous even when true.
	DetachedAllowed bool
}

// ExecutionOutput receives a command's output. In human mode Stdout and
// Stderr receive the raw command output. JSON, when non-nil, selects JSON
// mode: validated pinned frames or the finished envelope go there and raw
// output is not written.
type ExecutionOutput struct {
	Stdout io.Writer
	Stderr io.Writer
	JSON   io.Writer
}

// ExecutionResult is the outcome of one command. ExitCode is 124 when the
// command timed out and otherwise the remote exit status. TimedOut reports the
// timeout, Streamed reports whether NDJSON frames were used and Detached
// reports that a long command ran through detached start plus attach.
//
// Completed reports that remote termination was confirmed and the terminal
// output was rendered. It is published only after the final render and the
// cancellation checks: a fully validated synchronous result, a clean legacy
// stream terminal exit followed by a clean EOF, a rendered detached attach
// exit, or a local detached timeout whose terminal frame was written and whose
// single stopCommand answer confirmed running:false. A LOST attach exit counts
// as completed because the process is gone even though its result is unknown.
// It stays false after an error frame, a trailing malformed frame, a transport
// failure, a local watchdog, a broken output writer or a failed stop, so a
// consumer can tell an ambiguous outcome from an ended command without a
// second request.
type ExecutionResult struct {
	ExitCode       int
	TimedOut       bool
	Streamed       bool
	Detached       bool
	Completed      bool
	TimeoutSeconds int64
}

// ExecuteCommand sends exactly one POST /api/v1/sandboxes/{id}/commands and
// renders its result. It never retries: a transport or node failure can mean
// the command already ran.
//
// A zero TimeoutSeconds normally resolves the server's streamed cap and
// detached capability through the authenticated /api/bbraft/info extension.
// That request happens at most once per call. A 404 means the endpoint is
// missing (a v1-only server), so the command falls back to 600 seconds and
// writes one note to out.Stderr; every other metadata failure is an error. A
// valid commands.detached=true starts the command detached and follows it with
// attach; an absent or false member keeps the legacy streamed path with the
// exact resolved cap, and zero is never sent to the v1 API. A caller that
// already confirmed the capability (run reads the same metadata itself) sets
// DetachedAllowed: a zero timeout then starts detached immediately with no
// metadata request and no local timeout, and a timeout above the pinned
// synchronous cap uses detached with that local timeout. Otherwise the legacy
// stream with the exact timeout is kept. A positive timeout at or below the
// cap stays synchronous even when DetachedAllowed is true.
//
// A remote non-zero exit returns an *ExitError with a nil Err after the output
// was rendered, so the caller prints no second error line. Output write
// failures cancel the request and close the response body.
func ExecuteCommand(ctx context.Context, client APIClient, request ExecutionRequest, out ExecutionOutput) (ExecutionResult, error) {
	result := ExecutionResult{}
	if client == nil {
		return result, &UsageError{Err: errors.New("exec: no API client")}
	}
	if request.SandboxID == "" {
		return result, &UsageError{Err: errors.New("exec: sandbox id is required")}
	}
	if strings.TrimSpace(request.Command) == "" {
		return result, &UsageError{Err: errors.New("exec: command is required")}
	}
	if request.TimeoutSeconds < 0 || request.TimeoutSeconds > execMaxTimeoutSeconds {
		return result, &UsageError{Err: fmt.Errorf("exec: timeout must be between 0 and %d seconds", execMaxTimeoutSeconds)}
	}
	if request.TimeoutSeconds == 0 {
		if request.DetachedAllowed {
			// The caller already confirmed the detached capability (run
			// does its own metadata read), so zero stays unlimited: no
			// second metadata request and no local timeout.
			result.Streamed = true
			result.Detached = true
			return execRunDetachedLong(ctx, client, request, out, 0, result)
		}
		limits, err := execDiscoverLimits(ctx, client)
		switch {
		case errors.Is(err, errExecLegacyServer):
			request.TimeoutSeconds, err = execFallbackStreamCap(out.Stderr)
			if err != nil {
				return result, err
			}
			request.Stream = true
		case err != nil:
			return result, err
		case limits.Detached:
			result.Streamed = true
			result.Detached = true
			return execRunDetachedLong(ctx, client, request, out, 0, result)
		default:
			request.TimeoutSeconds = limits.StreamMaxSeconds
			request.Stream = true
		}
	} else if request.TimeoutSeconds > execMaxSyncSeconds {
		request.Stream = true
		if request.DetachedAllowed {
			result.TimeoutSeconds = request.TimeoutSeconds
			result.Streamed = true
			result.Detached = true
			return execRunDetachedLong(ctx, client, request, out, time.Duration(request.TimeoutSeconds)*time.Second, result)
		}
	}
	if request.TimeoutSeconds < 1 {
		return result, &UsageError{Err: errors.New("exec: timeout must be positive")}
	}
	result.TimeoutSeconds = request.TimeoutSeconds
	result.Streamed = request.Stream

	timeout := int(request.TimeoutSeconds)
	body := api.CommandRequest{
		Command:        request.Command,
		TimeoutSeconds: new(timeout),
		Stream:         new(request.Stream),
	}
	if request.Cwd != "" {
		body.Cwd = new(request.Cwd)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return result, fmt.Errorf("exec: encode request: %w", err)
	}

	requestCtx, cancel := context.WithTimeout(ctx, execClientBound(request.TimeoutSeconds))
	defer cancel()
	path := "/api/v1/sandboxes/" + url.PathEscape(request.SandboxID) + "/commands"
	req, err := client.NewRequest(requestCtx, http.MethodPost, path, bytes.NewReader(raw))
	if err != nil {
		return result, fmt.Errorf("exec: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if request.Stream {
		req.Header.Set("Accept", "application/x-ndjson")
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			return result, execTimeoutFailure()
		}
		return result, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		rawErr, readErr := execReadLimited(resp.Body, execSyncMaxBytes)
		if readErr != nil {
			if failure := execBodyReadFailure(ctx, requestCtx, readErr); failure != nil {
				return result, failure
			}
			return result, execInvalidResponse()
		}
		return result, decodeAPIError(resp.StatusCode, rawErr)
	}
	wantType := "application/json"
	if request.Stream {
		wantType = "application/x-ndjson"
	}
	if !execHasMediaType(resp.Header, wantType) {
		return result, execInvalidContentType()
	}
	if request.Stream {
		return execRunStream(ctx, requestCtx, resp, out, result, cancel)
	}
	return execRunSync(ctx, requestCtx, resp, out, result)
}

// execClientBound derives the request deadline from the resolved cap. The
// server may first wait up to its readiness deadline for a starting sandbox,
// and then runs the command for the full requested timeout under its own
// execution grace. Include all three so a readiness wait never steals time
// from the command itself.
func execClientBound(seconds int64) time.Duration {
	return api.DefaultCommandReadyDeadline +
		time.Duration(seconds)*time.Second +
		execClientGrace
}

// execRunSync validates and renders a synchronous command envelope.
func execRunSync(ctx, requestCtx context.Context, resp *http.Response, out ExecutionOutput, result ExecutionResult) (ExecutionResult, error) {
	raw, err := execReadLimited(resp.Body, execSyncMaxBytes)
	if err != nil {
		if failure := execBodyReadFailure(ctx, requestCtx, err); failure != nil {
			return result, failure
		}
		return result, execInvalidResponse()
	}
	response, exitNull, err := execDecodeFinished(raw)
	if err != nil {
		return result, err
	}
	result.ExitCode, result.TimedOut, err = execValidateFinished(response, exitNull)
	if err != nil {
		return result, err
	}
	if out.JSON != nil {
		if err := execWriteJSONLine(out.JSON, response); err != nil {
			return result, err
		}
	} else {
		if err := execWriteString(out.Stdout, response.Stdout); err != nil {
			return result, err
		}
		if err := execWriteString(out.Stderr, response.Stderr); err != nil {
			return result, err
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	// Completed is published only after the validated result was rendered
	// and the caller is still active.
	result.Completed = true
	if result.ExitCode != 0 {
		return result, &ExitError{Code: result.ExitCode}
	}
	return result, nil
}

// execDecodeFinished decodes the pinned synchronous envelope. exitNull reports
// an explicit `"exitCode": null`; an absent member is malformed.
func execDecodeFinished(raw []byte) (api.CommandResponse, bool, error) {
	var response api.CommandResponse
	var members map[string]jsontext.Value
	if err := json.Unmarshal(raw, &members); err != nil {
		return response, false, execInvalidResponse()
	}
	for _, name := range []string{"ok", "type", "success", "exitCode", "stdout", "stderr", "timedOut"} {
		if _, ok := members[name]; !ok {
			return response, false, execInvalidResponse()
		}
	}
	if !execJSONBool(members["ok"]) || !execJSONString(members["type"]) ||
		!execJSONBool(members["success"]) || !execJSONBool(members["timedOut"]) ||
		!execJSONString(members["stdout"]) || !execJSONString(members["stderr"]) {
		return response, false, execInvalidResponse()
	}
	exitNull := members["exitCode"].Kind() == jsontext.KindNull
	if !exitNull && !execJSONInteger(members["exitCode"]) {
		return response, false, execInvalidResponse()
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return response, false, execInvalidResponse()
	}
	if !response.OK || response.Type != api.TypeCommandFinished {
		return response, false, execInvalidResponse()
	}
	return response, exitNull, nil
}

// execValidateFinished checks the exitCode/success/timedOut relation described
// by the v1 API document and returns the process status.
func execValidateFinished(response api.CommandResponse, exitNull bool) (int, bool, error) {
	if response.TimedOut {
		if !exitNull || response.Success {
			return 0, false, execInvalidResponse()
		}
		return 124, true, nil
	}
	if exitNull {
		// A signal exit carries no numeric status. It cannot be a success.
		if response.Success {
			return 0, false, execInvalidResponse()
		}
		return 1, false, nil
	}
	if response.ExitCode == nil {
		return 0, false, execInvalidResponse()
	}
	code := *response.ExitCode
	if code < 0 || code > 255 || response.Success != (code == 0) {
		return 0, false, execInvalidResponse()
	}
	return code, false, nil
}

// execRunStream parses NDJSON frames as they arrive and renders each one. Any
// protocol violation is a failed execution; it is never retried.
func execRunStream(ctx, requestCtx context.Context, resp *http.Response, out ExecutionOutput, result ExecutionResult, cancel context.CancelFunc) (ExecutionResult, error) {
	reader := bufio.NewReaderSize(resp.Body, 64<<10)
	sink := &execStreamSink{out: out, cancel: cancel, body: resp.Body}
	state := execStreamExpectStarted
	var terminalErr *CLIError
	terminal := false

	for {
		line, readErr := execReadNDJSONLine(reader, execMaxFrameBytes)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			if sink.err != nil {
				return result, sink.err
			}
			if failure := execBodyReadFailure(ctx, requestCtx, readErr); failure != nil {
				return result, failure
			}
			if errors.Is(readErr, errExecFrameTooLarge) {
				return result, execInvalidStream("frame is too large")
			}
			return result, &CLIError{Code: "transport_error", Message: "command stream read failed"}
		}
		if len(bytes.TrimSpace(line)) > 0 {
			if terminal {
				return result, execInvalidStream("frame after the terminal frame")
			}
			frame, err := execDecodeStreamFrame(line)
			if err != nil {
				return result, err
			}
			switch frame.Type {
			case api.CommandFrameStarted:
				if state != execStreamExpectStarted {
					return result, execInvalidStream("duplicate started frame")
				}
				state = execStreamExpectOutput
			case api.CommandFrameStdout, api.CommandFrameStderr:
				if state != execStreamExpectOutput {
					return result, execInvalidStream("output frame is out of order")
				}
			case api.CommandFrameExit:
				if state != execStreamExpectOutput {
					return result, execInvalidStream("exit frame is out of order")
				}
				code, timedOut, err := execValidateExitFrame(frame)
				if err != nil {
					return result, err
				}
				result.ExitCode = code
				result.TimedOut = timedOut
				terminal = true
			case api.CommandFrameError:
				if state != execStreamExpectOutput {
					return result, execInvalidStream("error frame is out of order")
				}
				if frame.Error == "" {
					return result, execInvalidStream("error frame has no code")
				}
				terminalErr = &CLIError{
					Code:    frame.Error,
					Message: cmp.Or(frame.Message, "remote command failed"),
				}
				result.ExitCode = 1
				terminal = true
			default:
				return result, execInvalidStream("unknown frame type")
			}
			if err := sink.emit(frame); err != nil {
				return result, err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
	}
	if !terminal {
		return result, execInvalidStream("stream ended before a terminal frame")
	}
	if terminalErr != nil {
		if out.JSON != nil {
			// The pinned error frame is already the command's error line.
			return result, &ExitError{Code: 1}
		}
		return result, &ExitError{Code: 1, Err: terminalErr}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	// A clean terminal exit, a clean EOF and successful rendering are all
	// required before the consumer may treat the command as ended.
	result.Completed = true
	if result.ExitCode != 0 {
		return result, &ExitError{Code: result.ExitCode}
	}
	return result, nil
}

// execStreamState is the frame-order state machine of a command stream.
type execStreamState int

const (
	execStreamExpectStarted execStreamState = iota
	execStreamExpectOutput
)

// execStreamSink renders validated frames. A failed write cancels the request
// and closes the body so a broken consumer never leaves the command attached.
type execStreamSink struct {
	out    ExecutionOutput
	cancel context.CancelFunc
	body   io.Closer
	err    error
}

// emit writes one frame in the output mode selected by the caller.
func (s *execStreamSink) emit(frame api.CommandStreamFrame) error {
	var err error
	switch {
	case s.out.JSON != nil:
		err = execWriteJSONLine(s.out.JSON, frame)
	case frame.Type == api.CommandFrameStdout:
		err = execWriteString(s.out.Stdout, frame.Data)
	case frame.Type == api.CommandFrameStderr:
		err = execWriteString(s.out.Stderr, frame.Data)
	}
	if err == nil {
		return nil
	}
	s.err = err
	if s.cancel != nil {
		s.cancel()
	}
	if s.body != nil {
		_ = s.body.Close()
	}
	return err
}

// execDecodeStreamFrame parses one NDJSON frame with jsontext and rejects
// unknown members, trailing tokens and values that are not frames.
func execDecodeStreamFrame(line []byte) (api.CommandStreamFrame, error) {
	var frame api.CommandStreamFrame
	dec := jsontext.NewDecoder(bytes.NewReader(line))
	raw, err := dec.ReadValue()
	if err != nil {
		return frame, execInvalidStream("malformed frame")
	}
	// ReadValue may alias the decoder buffer, and the trailing check reads
	// again. Keep our copy before that second read.
	raw = raw.Clone()
	if _, err := dec.ReadValue(); !errors.Is(err, io.EOF) {
		return frame, execInvalidStream("trailing data in frame")
	}
	var members map[string]jsontext.Value
	if err := json.Unmarshal(raw, &members); err != nil {
		return frame, execInvalidStream("malformed frame")
	}
	if !execValidStreamMembers(members) {
		return frame, execInvalidStream("malformed frame")
	}
	if err := json.Unmarshal(raw, &frame, json.RejectUnknownMembers(true)); err != nil {
		return frame, execInvalidStream("malformed frame")
	}
	return frame, nil
}

// execValidStreamMembers rejects explicit nulls and wrong JSON kinds for the
// pinned frame members before a typed decode can hide them in zero values.
func execValidStreamMembers(members map[string]jsontext.Value) bool {
	for name, value := range members {
		switch name {
		case "type", "commandId", "data", "error", "message":
			if !execJSONString(value) {
				return false
			}
		case "exitCode":
			if value.Kind() == jsontext.KindNull {
				continue
			}
			if !execJSONInteger(value) {
				return false
			}
		case "success", "timedOut", "retryable":
			if !execJSONBool(value) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// execValidateExitFrame checks the required terminal members. A timeout has an
// explicit null exitCode; an exit must agree with success.
func execValidateExitFrame(frame api.CommandStreamFrame) (int, bool, error) {
	if !frame.ExitCode.Present || !frame.Success.Present || !frame.TimedOut.Present {
		return 0, false, execInvalidStream("exit frame is missing required fields")
	}
	if frame.Success.Value == nil || frame.TimedOut.Value == nil {
		return 0, false, execInvalidStream("exit frame is missing required fields")
	}
	success := *frame.Success.Value
	timedOut := *frame.TimedOut.Value
	switch {
	case timedOut:
		if frame.ExitCode.Value != nil || success {
			return 0, false, execInvalidStream("timeout exit frame is inconsistent")
		}
		return 124, true, nil
	case frame.ExitCode.Value == nil:
		if success {
			return 0, false, execInvalidStream("null exit code cannot be a success")
		}
		return 1, false, nil
	default:
		code := *frame.ExitCode.Value
		if code < 0 || code > 255 || success != (code == 0) {
			return 0, false, execInvalidStream("exit frame is inconsistent")
		}
		return code, false, nil
	}
}

// execReadNDJSONLine reads one newline-delimited frame without Scanner's
// 64 KiB cap and without letting a hostile frame grow without bound.
func execReadNDJSONLine(reader *bufio.Reader, limit int) ([]byte, error) {
	var line []byte
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(line)+len(fragment) > limit {
			return nil, errExecFrameTooLarge
		}
		line = append(line, fragment...)
		switch {
		case err == nil:
			return line, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			return line, io.EOF
		default:
			return line, err
		}
	}
}

// execInfoResponse is the CLI-local shape of the authenticated extension. It
// is deliberately not an api package type: the core runner owns the handler.
// Detached is kept raw so an absent member, an explicit null and a boolean are
// distinguishable: only a JSON boolean is a valid capability answer.
type execInfoResponse struct {
	OK       bool   `json:"ok"`
	Type     string `json:"type"`
	Version  string `json:"version"`
	Commands *struct {
		StreamMaxSeconds *uint64        `json:"streamMaxSeconds"`
		SyncMaxSeconds   *uint64        `json:"syncMaxSeconds"`
		Detached         jsontext.Value `json:"detached,omitzero"`
	} `json:"commands"`
}

// execCommandLimits are the command limits this server actually enforces.
type execCommandLimits struct {
	StreamMaxSeconds int64
	Detached         bool
}

// errExecLegacyServer reports that the server has no /api/bbraft/info endpoint
// (the v1-only or pre-extension case). It is not a transport failure and the
// caller keeps its own legacy behaviour.
var errExecLegacyServer = errors.New("server has no command metadata")

// execReadInfo reads the metadata envelope once. A missing endpoint is
// errExecLegacyServer; every other protocol, auth, server or transport failure
// is an error, so a capability is never silently inferred.
func execReadInfo(ctx context.Context, client APIClient) (execInfoResponse, error) {
	var info execInfoResponse
	metaCtx, cancel := context.WithTimeout(ctx, execMetadataTimeout)
	defer cancel()
	req, err := client.NewRequest(metaCtx, http.MethodGet, execInfoPath, nil)
	if err != nil {
		return info, &CLIError{Code: "invalid_response", Message: "server metadata request is invalid"}
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return info, ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(metaCtx.Err(), context.DeadlineExceeded) {
			return info, &CLIError{Code: "timeout", Message: "server metadata request timed out"}
		}
		return info, &CLIError{Code: "metadata_unavailable", Message: "server metadata request failed"}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return info, errExecLegacyServer
	}
	if resp.StatusCode != http.StatusOK {
		raw, readErr := execReadLimited(resp.Body, execMetadataMaxBytes)
		if readErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return info, ctxErr
			}
			if errors.Is(metaCtx.Err(), context.DeadlineExceeded) || errors.Is(readErr, context.DeadlineExceeded) {
				return info, &CLIError{Code: "timeout", Message: "server metadata request timed out"}
			}
			return info, execInvalidMetadata()
		}
		return info, decodeAPIError(resp.StatusCode, raw)
	}
	if !execHasMediaType(resp.Header, "application/json") {
		return info, execInvalidMetadata()
	}
	raw, readErr := execReadLimited(resp.Body, execMetadataMaxBytes)
	if readErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return info, ctxErr
		}
		if errors.Is(metaCtx.Err(), context.DeadlineExceeded) || errors.Is(readErr, context.DeadlineExceeded) {
			return info, &CLIError{Code: "timeout", Message: "server metadata request timed out"}
		}
		return info, execInvalidMetadata()
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return info, execInvalidMetadata()
	}
	if !info.OK || info.Type != execInfoType || info.Version == "" || info.Commands == nil {
		return info, execInvalidMetadata()
	}
	return info, nil
}

// execDiscoverLimits validates the metadata and resolves the stream cap plus
// the detached capability. An absent or false detached member is legacy; an
// explicit null or any other JSON kind is malformed and never means legacy.
func execDiscoverLimits(ctx context.Context, client APIClient) (execCommandLimits, error) {
	var limits execCommandLimits
	info, err := execReadInfo(ctx, client)
	if err != nil {
		return limits, err
	}
	streamMax := info.Commands.StreamMaxSeconds
	if streamMax == nil || *streamMax == 0 || *streamMax > math.MaxUint32 {
		return limits, execInvalidMetadata()
	}
	syncMax := info.Commands.SyncMaxSeconds
	if syncMax == nil || *syncMax == 0 || *syncMax > uint64(execMaxSyncSeconds) {
		return limits, execInvalidMetadata()
	}
	limits.StreamMaxSeconds = int64(*streamMax)
	if len(info.Commands.Detached) == 0 {
		return limits, nil
	}
	switch info.Commands.Detached.Kind() {
	case jsontext.KindTrue:
		limits.Detached = true
	case jsontext.KindFalse:
	default:
		// An explicit null, string, number, array or object is a malformed
		// capability and never means legacy.
		return limits, execInvalidMetadata()
	}
	return limits, nil
}

// execFallbackStreamCap writes the single stderr note for a missing endpoint.
func execFallbackStreamCap(notes io.Writer) (int64, error) {
	if notes == nil {
		notes = io.Discard
	}
	note := fmt.Sprintf("bbraft: server has no %s; using the %d-second stream timeout\n", execInfoPath, execFallbackStreamSeconds)
	if err := execWriteString(notes, note); err != nil {
		return 0, &CLIError{Code: "write_failed", Message: "write fallback note failed"}
	}
	return execFallbackStreamSeconds, nil
}

// execReadLimited reads at most limit bytes and refuses a larger response.
func execReadLimited(reader io.Reader, limit int) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > limit {
		return nil, errExecFrameTooLarge
	}
	return raw, nil
}

// execWriteString writes raw command output and reports a broken writer
// without echoing its error, which can contain output or token text.
func execWriteString(w io.Writer, data string) error {
	if w == nil || data == "" {
		return nil
	}
	n, err := io.WriteString(w, data)
	if err != nil || n != len(data) {
		return execWriteFailure()
	}
	return nil
}

// execWriteJSONLine writes one pinned JSON value followed by a newline.
func execWriteJSONLine(w io.Writer, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("exec: encode command output: %w", err)
	}
	raw = append(raw, '\n')
	n, err := w.Write(raw)
	if err != nil || n != len(raw) {
		return execWriteFailure()
	}
	return nil
}

// execWriteFailure is the safe output-writer error. The writer's own text is
// never included: it can quote the command output or a credential.
func execWriteFailure() error {
	return &CLIError{Code: "write_failed", Message: "write command output failed"}
}

// execHasMediaType reports whether header carries want as its base media type.
// Parameters such as charset are accepted.
func execHasMediaType(header http.Header, want string) bool {
	mediaType, _, err := mime.ParseMediaType(header.Get("Content-Type"))
	return err == nil && strings.EqualFold(mediaType, want)
}

// execJSONBool reports whether v is a JSON boolean literal.
func execJSONBool(v jsontext.Value) bool {
	kind := v.Kind()
	return kind == jsontext.KindTrue || kind == jsontext.KindFalse
}

// execJSONString reports whether v is a JSON string.
func execJSONString(v jsontext.Value) bool { return v.Kind() == jsontext.KindString }

// execJSONInteger reports whether v is a JSON integer without a fraction or
// exponent.
func execJSONInteger(v jsontext.Value) bool {
	if v.Kind() != jsontext.KindNumber {
		return false
	}
	_, err := strconv.ParseInt(string(bytes.TrimSpace(v)), 10, 64)
	return err == nil
}

// execBodyReadFailure preserves caller cancel/deadline classifications and
// turns the client-side execution watchdog into the same exit status a remote
// timeout uses. It returns nil when the failure is a protocol problem.
func execBodyReadFailure(parent, request context.Context, readErr error) error {
	if err := parent.Err(); err != nil {
		return err
	}
	if errors.Is(request.Err(), context.DeadlineExceeded) || errors.Is(readErr, context.DeadlineExceeded) {
		return execTimeoutFailure()
	}
	if err := request.Err(); err != nil {
		return err
	}
	return nil
}

// execTimeoutFailure is the local watchdog's timeout result. The safe message
// is printed once; the exit status matches a remote timeout.
func execTimeoutFailure() error {
	return &ExitError{Code: 124, Err: &CLIError{Code: "timeout", Message: "command timed out"}}
}

// execInvalidResponse is the safe malformed sync-envelope error.
func execInvalidResponse() error {
	return &CLIError{Code: "invalid_response", Message: "command response is invalid"}
}

// execInvalidContentType is the safe wrong-media-type error.
func execInvalidContentType() error {
	return &CLIError{Code: "invalid_response", Message: "command response has an unexpected media type"}
}

// execInvalidMetadata is the safe malformed metadata error.
func execInvalidMetadata() error {
	return &CLIError{Code: "invalid_response", Message: "server metadata response is invalid"}
}

// execInvalidStream reports a protocol violation without echoing frame data.
func execInvalidStream(reason string) error {
	return &CLIError{Code: "invalid_response", Message: "invalid command stream: " + reason}
}

// execShellJoin quotes argv for a POSIX shell. It is the inverse of splitting
// a shell line into words for `bash -lc`.
func execShellJoin(args []string) string {
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = "'" + strings.ReplaceAll(arg, "'", `'\''`) + "'"
	}
	return strings.Join(quoted, " ")
}
