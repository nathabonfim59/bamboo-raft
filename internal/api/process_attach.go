package api

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// CommandAttachPath is the API path pattern of a detached command attach
// stream. The server adapter registers "GET "+CommandAttachPath.
const CommandAttachPath = "/api/v1/sandboxes/{sandboxId}/commands/{processId}/stream"

// Attach timing defaults. The pin sends a heartbeat after 15 seconds without
// another line and ends the stream with reconnect after 10 minutes, so clients
// can continue with the returned offsets. The bootstrap limit covers only the
// first attached event; it is not an RPC deadline on the whole stream. The
// write deadline is refreshed for each frame and cleared while the handler
// waits for the node, so an idle connection is never killed by a stale
// deadline.
const (
	DefaultCommandAttachHeartbeat      = 15 * time.Second
	DefaultCommandAttachReconnect      = 10 * time.Minute
	DefaultCommandAttachWriteTimeout   = 30 * time.Second
	DefaultCommandAttachBootstrapLimit = 30 * time.Second
	commandAttachMaxFrameBytes         = 1 << 20
	commandAttachErrorMessage          = "the sandbox machine could not complete the call"
	commandAttachProtocolMessage       = "the sandbox machine sent an invalid stream"
)

// errAttachProtocol marks a node frame that violates the frozen AttachProcess
// contract. It is terminal after the attached frame and never a writer fault.
var errAttachProtocol = errors.New("api: malformed process attach frame")

// errAttachReconnect reports that the absolute attach lifetime ended while a
// chunk was being written. The reconnect frame is already flushed; the caller
// must return without another terminal frame.
var errAttachReconnect = errors.New("api: attach reconnect due")

// CommandAttachOptions are the dependencies and test seams of an attach
// handler. CommandsOptions supplies the lookup, node-client factory, command
// store and logger. The durations default when zero or negative.
type CommandAttachOptions struct {
	CommandsOptions
	// Heartbeat is the quiet interval after which one heartbeat frame is
	// sent. Default DefaultCommandAttachHeartbeat (15 seconds).
	Heartbeat time.Duration
	// ReconnectAfter is the absolute stream lifetime after which one
	// reconnect frame ends the stream. Default DefaultCommandAttachReconnect
	// (10 minutes).
	ReconnectAfter time.Duration
	// WriteTimeout bounds one frame write. Default
	// DefaultCommandAttachWriteTimeout (30 seconds).
	WriteTimeout time.Duration
	// BootstrapTimeout bounds the wait for the first attached event. It is
	// stopped after that event and never limits the rest of the stream.
	// Default DefaultCommandAttachBootstrapLimit (30 seconds).
	BootstrapTimeout time.Duration
	// Now is a test seam for the absolute reconnect deadline only. Nil uses
	// time.Now; native heartbeat timers and write deadlines always use the
	// real clock.
	Now func() time.Time
}

// commandAttachHandler serves GET CommandAttachPath.
type commandAttachHandler struct {
	commands         *commandsHandler
	heartbeat        time.Duration
	reconnect        time.Duration
	writeTimeout     time.Duration
	bootstrapTimeout time.Duration
	now              func() time.Time
}

// NewCommandAttachHandler returns the v1 detached command attach
// handler. The caller registers it for "GET "+CommandAttachPath. The store is
// required: every observation is validated and recorded before it reaches the
// stream.
func NewCommandAttachHandler(opts CommandAttachOptions) (http.Handler, error) {
	if opts.Store == nil {
		return nil, errors.New("api: command attach handler needs a command store")
	}
	commands, err := newCommandsHandler(opts.CommandsOptions)
	if err != nil {
		return nil, err
	}
	handler := &commandAttachHandler{
		commands:         commands,
		heartbeat:        opts.Heartbeat,
		reconnect:        opts.ReconnectAfter,
		writeTimeout:     opts.WriteTimeout,
		bootstrapTimeout: opts.BootstrapTimeout,
		now:              opts.Now,
	}
	if handler.heartbeat <= 0 {
		handler.heartbeat = DefaultCommandAttachHeartbeat
	}
	if handler.reconnect <= 0 {
		handler.reconnect = DefaultCommandAttachReconnect
	}
	if handler.writeTimeout <= 0 {
		handler.writeTimeout = DefaultCommandAttachWriteTimeout
	}
	if handler.bootstrapTimeout <= 0 {
		handler.bootstrapTimeout = DefaultCommandAttachBootstrapLimit
	}
	if handler.now == nil {
		handler.now = time.Now
	}
	return handler, nil
}

// ServeHTTP streams one detached process's output.
func (h *commandAttachHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	identity, ok := auth.IdentityFrom(r.Context())
	if !ok || identity.Owner == "" {
		apierr.WriteError(w, r, apierr.New(http.StatusUnauthorized, apierr.CodeUnauthorized, "unauthorized"))
		return
	}
	if r.Context().Err() != nil {
		return
	}
	sandboxID := r.PathValue(ParamSandboxID)
	processID, ok := parseProcessPathID(r.PathValue(ParamProcessID))
	if sandboxID == "" || !ok {
		apierr.WriteError(w, r, commandNotFound())
		return
	}
	owner, err := uuid.Parse(identity.Owner)
	if err != nil || owner == uuid.Nil() {
		apierr.WriteError(w, r, apierr.New(http.StatusUnauthorized, apierr.CodeUnauthorized, "unauthorized"))
		return
	}
	stdoutOffset, stderrOffset, apiErr := parseCommandAttachQuery(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	record, apiErr := h.commands.readCommandRecord(r.Context(), owner, sandboxID, processID)
	if apiErr != nil {
		if r.Context().Err() != nil {
			return
		}
		apierr.WriteError(w, r, apiErr)
		return
	}
	if r.Context().Err() != nil {
		return
	}
	if record.Status == db.CommandFailed {
		// A definite start failure never created a node process.
		apierr.WriteError(w, r, commandNotFound())
		return
	}

	target, apiErr, proceed := h.commands.resolveTarget(r.Context(), identity.Owner, sandboxID)
	if !proceed {
		return
	}
	if r.Context().Err() != nil {
		return
	}
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	client, closeClient, apiErr := h.commands.dial(target.NodeID)
	if apiErr != nil {
		if r.Context().Err() != nil {
			return
		}
		apierr.WriteError(w, r, apiErr)
		return
	}
	defer h.commands.closeClient(closeClient)
	if r.Context().Err() != nil {
		return
	}

	h.serveAttach(w, r, client, target.State, owner, sandboxID, processID, stdoutOffset, stderrOffset)
}

// attachResult is one pump result. final means the event is a validated exit
// that was followed by the stream's clean EOF; it is the only exit the handler
// may emit.
type attachResult struct {
	event *nodev1.ProcessEvent
	err   error
	final bool
}

// serveAttach starts AttachProcess, validates the first attached event before
// any response bytes, then streams frames until a terminal event, reconnect or
// client cancellation. One pump goroutine owns Recv and is joined before
// return. The HTTP context only cancels this RPC; it never signals the process.
func (h *commandAttachHandler) serveAttach(w http.ResponseWriter, r *http.Request, client nodev1.NodeClient, state SandboxState, owner uuid.UUID, sandboxID string, processID uint32, stdoutOffset, stderrOffset int64) {
	streamCtx, cancelStream := context.WithCancel(r.Context())
	results, pumpDone := h.startAttachPump(streamCtx, client, &nodev1.AttachProcessRequest{
		SandboxId:    sandboxID,
		ProcessId:    processID,
		StdoutOffset: stdoutOffset,
		StderrOffset: stderrOffset,
	})
	defer func() {
		cancelStream()
		<-pumpDone
	}()

	bootstrap := time.NewTimer(h.bootstrapTimeout)
	defer bootstrap.Stop()
	var first attachResult
	select {
	case <-r.Context().Done():
		return
	case <-bootstrap.C:
		h.commands.logger.WarnContext(r.Context(), "api: command attach bootstrap timed out",
			"process_id", processID)
		apierr.WriteError(w, r, commandDirectFailed())
		return
	case res, ok := <-results:
		if !ok {
			if r.Context().Err() != nil {
				return
			}
			h.commands.logger.WarnContext(r.Context(), "api: command attach ended before attached",
				"process_id", processID)
			apierr.WriteError(w, r, commandDirectFailed())
			return
		}
		first = res
	}
	bootstrap.Stop()
	if r.Context().Err() != nil {
		return
	}
	if first.err != nil {
		h.writeAttachSetupError(w, r, processID, state, first.err)
		return
	}
	if first.final {
		h.writeAttachProtocolError(w, r, processID)
		return
	}
	attachedEvent, ok := first.event.GetEvent().(*nodev1.ProcessEvent_Attached_)
	if !ok || attachedEvent == nil || attachedEvent.Attached == nil || attachedEvent.Attached.Process == nil {
		h.writeAttachProtocolError(w, r, processID)
		return
	}
	attached := attachedEvent.Attached
	if err := validateProcessRecord(processID, attached.Process, true); err != nil {
		h.commands.logger.WarnContext(r.Context(), "api: command attach returned a malformed process",
			"process_id", processID, "reason", err.Error())
		apierr.WriteError(w, r, commandDirectFailed())
		return
	}
	if attached.StdoutOffset < 0 || attached.StdoutOffset > stdoutOffset ||
		attached.StderrOffset < 0 || attached.StderrOffset > stderrOffset {
		h.commands.logger.WarnContext(r.Context(), "api: command attach returned an invalid start offset",
			"process_id", processID,
			"stdout_offset", attached.StdoutOffset, "stderr_offset", attached.StderrOffset)
		apierr.WriteError(w, r, commandDirectFailed())
		return
	}
	if r.Context().Err() != nil {
		return
	}
	observation := commandObservationFromProcess(attached.Process)
	updated, err := h.commands.store.ObserveCommand(r.Context(), owner, sandboxID, processID, observation)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		h.commands.logger.ErrorContext(r.Context(), "api: command attach observation failed",
			"process_id", processID, "error", commandErrorName(err))
		apierr.WriteError(w, r, commandInternal())
		return
	}
	if err := validateStoredCommand(owner, sandboxID, processID, updated); err != nil {
		h.commands.logger.ErrorContext(r.Context(), "api: command attach observation is malformed",
			"process_id", processID, "reason", err.Error())
		apierr.WriteError(w, r, commandInternal())
		return
	}
	if r.Context().Err() != nil {
		return
	}

	w.Header().Set("Content-Type", commandNDJSONMediaType)
	encoder := jsontext.NewEncoder(w)
	controller := http.NewResponseController(w)
	stdout := attachStreamOffset{emitted: attached.StdoutOffset, raw: attached.StdoutOffset}
	stderr := attachStreamOffset{emitted: attached.StderrOffset, raw: attached.StderrOffset}
	attachedFrame := CommandAttachFrame{
		Type:         CommandAttachAttached,
		ProcessID:    processID,
		StdoutOffset: new(stdout.emitted),
		StderrOffset: new(stderr.emitted),
	}
	if err := h.writeFrame(encoder, controller, attachedFrame); err != nil {
		return
	}

	reconnectAt := h.now().Add(h.reconnect)
	heartbeat := time.NewTimer(h.heartbeat)
	defer heartbeat.Stop()
	reconnect := time.NewTimer(h.reconnect)
	defer reconnect.Stop()
	for {
		if !h.now().Before(reconnectAt) {
			_ = h.writeAttachReconnect(encoder, controller, stdout, stderr)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-reconnect.C:
			_ = h.writeAttachReconnect(encoder, controller, stdout, stderr)
			return
		case <-heartbeat.C:
			if err := h.writeFrame(encoder, controller, CommandAttachFrame{
				Type: CommandAttachHeartbeat,
			}); err != nil {
				return
			}
			heartbeat.Reset(h.heartbeat)
		case res, ok := <-results:
			if r.Context().Err() != nil {
				return
			}
			if !ok {
				h.writeAttachStreamError(encoder, controller, processID, stdout, stderr, commandDirectFailed())
				return
			}
			if res.err != nil {
				h.commands.logger.WarnContext(r.Context(), "api: command attach stream failed",
					"process_id", processID, "error", commandErrorName(res.err))
				h.writeAttachStreamError(encoder, controller, processID, stdout, stderr, commandDirectFailed())
				return
			}
			if res.final {
				exitEvent, ok := res.event.GetEvent().(*nodev1.ProcessEvent_Exit)
				if !ok || exitEvent == nil || exitEvent.Exit == nil {
					h.writeAttachStreamError(encoder, controller, processID, stdout, stderr, commandDirectFailed())
					return
				}
				h.finishAttach(encoder, controller, r, owner, sandboxID, processID, exitEvent.Exit, &stdout, &stderr, reconnectAt)
				return
			}
			switch event := res.event.GetEvent().(type) {
			case *nodev1.ProcessEvent_Stdout:
				if event == nil || event.Stdout == nil {
					h.writeAttachStreamError(encoder, controller, processID, stdout, stderr, commandDirectFailed())
					return
				}
				wrote, err := h.writeAttachChunk(encoder, controller, CommandAttachStdout, &stdout, &stderr, event.Stdout.Data, event.Stdout.NextOffset, reconnectAt)
				if err != nil {
					h.writeAttachChunkError(err, encoder, controller, processID, stdout, stderr)
					return
				}
				if wrote {
					heartbeat.Reset(h.heartbeat)
				}
			case *nodev1.ProcessEvent_Stderr:
				if event == nil || event.Stderr == nil {
					h.writeAttachStreamError(encoder, controller, processID, stdout, stderr, commandDirectFailed())
					return
				}
				wrote, err := h.writeAttachChunk(encoder, controller, CommandAttachStderr, &stdout, &stderr, event.Stderr.Data, event.Stderr.NextOffset, reconnectAt)
				if err != nil {
					h.writeAttachChunkError(err, encoder, controller, processID, stdout, stderr)
					return
				}
				if wrote {
					heartbeat.Reset(h.heartbeat)
				}
			default:
				// A duplicate attached, a non-final exit, an empty event
				// or an unknown event breaks the frozen frame order.
				h.writeAttachStreamError(encoder, controller, processID, stdout, stderr, commandDirectFailed())
				return
			}
		}
	}
}

// writeAttachChunkError classifies a chunk write failure. A reconnect frame was
// already flushed for errAttachReconnect; any other error either happened on
// the writer (no second terminal frame allowed) or was a protocol violation.
func (h *commandAttachHandler) writeAttachChunkError(err error, encoder *jsontext.Encoder, controller *http.ResponseController, processID uint32, stdout, stderr attachStreamOffset) {
	if errors.Is(err, errAttachProtocol) {
		h.writeAttachStreamError(encoder, controller, processID, stdout, stderr, commandDirectFailed())
	}
}

// writeAttachReconnect writes the one reconnect frame at the last fully flushed
// offsets. A fresh write deadline is set by writeFrame.
func (h *commandAttachHandler) writeAttachReconnect(encoder *jsontext.Encoder, controller *http.ResponseController, stdout, stderr attachStreamOffset) error {
	return h.writeFrame(encoder, controller, CommandAttachFrame{
		Type:         CommandAttachReconnect,
		StdoutOffset: new(stdout.emitted),
		StderrOffset: new(stderr.emitted),
	})
}

// finishAttach validates and records the final process, flushes the last
// incomplete rune bytes (a validated exit is a definite log EOF), then writes
// the exit frame. A store fault after the headers is one fixed http_500 error
// frame: the process may have ended, so nothing reruns.
func (h *commandAttachHandler) finishAttach(encoder *jsontext.Encoder, controller *http.ResponseController, r *http.Request, owner uuid.UUID, sandboxID string, processID uint32, process *nodev1.Process, stdout, stderr *attachStreamOffset, reconnectAt time.Time) {
	if r.Context().Err() != nil {
		return
	}
	if err := validateProcessRecord(processID, process, true); err != nil ||
		process.State == nodev1.ProcessState_PROCESS_STATE_RUNNING {
		h.commands.logger.WarnContext(r.Context(), "api: command attach exit is malformed",
			"process_id", processID)
		h.writeAttachStreamError(encoder, controller, processID, *stdout, *stderr, commandDirectFailed())
		return
	}
	// Native sizes that are larger than the raw cursor prove output the
	// node never delivered in this stream. A smaller size is a lagging or
	// replaced snapshot and is allowed.
	if process.StdoutSize > stdout.raw || process.StderrSize > stderr.raw {
		h.commands.logger.WarnContext(r.Context(), "api: command attach exit reports undelivered output",
			"process_id", processID,
			"stdout_size", process.StdoutSize, "stdout_offset", stdout.raw,
			"stderr_size", process.StderrSize, "stderr_offset", stderr.raw)
		h.writeAttachStreamError(encoder, controller, processID, *stdout, *stderr, commandDirectFailed())
		return
	}
	observation := commandObservationFromProcess(process)
	// The node's process snapshot can be older than the output just read.
	// Record at least the raw end of everything delivered to the client.
	observation.StdoutBytes = max(observation.StdoutBytes, stdout.raw)
	observation.StderrBytes = max(observation.StderrBytes, stderr.raw)
	updated, err := h.commands.store.ObserveCommand(r.Context(), owner, sandboxID, processID, observation)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		h.commands.logger.ErrorContext(r.Context(), "api: command attach exit observation failed",
			"process_id", processID, "error", commandErrorName(err))
		h.writeAttachStreamError(encoder, controller, processID, *stdout, *stderr, commandInternal())
		return
	}
	if err := validateStoredCommand(owner, sandboxID, processID, updated); err != nil {
		h.commands.logger.ErrorContext(r.Context(), "api: command attach exit observation is malformed",
			"process_id", processID, "reason", err.Error())
		h.writeAttachStreamError(encoder, controller, processID, *stdout, *stderr, commandInternal())
		return
	}
	if r.Context().Err() != nil {
		return
	}
	if !h.now().Before(reconnectAt) {
		_ = h.writeAttachReconnect(encoder, controller, *stdout, *stderr)
		return
	}
	if err := h.flushAttachPending(encoder, controller, stdout, stderr, reconnectAt); err != nil {
		if errors.Is(err, errAttachReconnect) {
			return
		}
		return
	}
	if !h.now().Before(reconnectAt) {
		_ = h.writeAttachReconnect(encoder, controller, *stdout, *stderr)
		return
	}
	if err := h.writeFrame(encoder, controller, commandAttachExitFrame(process, stdout.emitted, stderr.emitted)); err != nil {
		return
	}
}

// writeAttachChunk validates one node chunk's raw offset and writes its
// complete UTF-8 prefix as one or more bounded frames. The incomplete tail
// stays buffered (at most three bytes) until a later chunk, exit, reconnect or
// error frame. Offsets always advance by raw bytes consumed, not by the length
// of the decoded string.
func (h *commandAttachHandler) writeAttachChunk(encoder *jsontext.Encoder, controller *http.ResponseController, kind CommandAttachFrameType, stdout, stderr *attachStreamOffset, data []byte, nextOffset int64, reconnectAt time.Time) (bool, error) {
	stream := stdout
	if kind == CommandAttachStderr {
		stream = stderr
	}
	if nextOffset < stream.raw || int64(len(data)) > maxInt64-stream.raw ||
		nextOffset != stream.raw+int64(len(data)) {
		return false, errAttachProtocol
	}
	if len(data) == 0 {
		return false, nil
	}
	buffer := make([]byte, 0, len(stream.pending)+len(data))
	buffer = append(buffer, stream.pending...)
	buffer = append(buffer, data...)
	stream.raw = nextOffset
	cut := completeUTF8Prefix(buffer)
	if cut == 0 {
		stream.pending = bytes.Clone(buffer)
		return false, nil
	}
	base := stream.emitted
	prefix := buffer[:cut]
	wrote := false
	for start := 0; start < len(prefix); {
		if !h.now().Before(reconnectAt) {
			if err := h.writeAttachReconnect(encoder, controller, *stdout, *stderr); err != nil {
				return wrote, err
			}
			return wrote, errAttachReconnect
		}
		end := attachFramePieceEnd(prefix, start, commandAttachMaxFrameBytes)
		next := base + int64(end)
		frame := CommandAttachFrame{
			Type:       kind,
			Data:       strings.ToValidUTF8(string(prefix[start:end]), "\uFFFD"),
			NextOffset: &next,
		}
		if err := h.writeFrame(encoder, controller, frame); err != nil {
			return wrote, err
		}
		// Only a fully flushed piece advances the client-visible offset.
		// If the absolute reconnect deadline interrupts a large chunk, the
		// next attach starts exactly after the last piece that went out.
		stream.emitted = next
		wrote = true
		start = end
	}
	stream.pending = bytes.Clone(buffer[cut:])
	return wrote, nil
}

// attachFramePieceEnd returns the raw end of one output piece beginning at
// start, at most limit bytes unless a single rune is larger (at most four
// bytes). It never cuts a rune, so each frame's data is valid UTF-8 while the
// frame's nextOffset still counts raw bytes.
func attachFramePieceEnd(data []byte, start, limit int) int {
	end := start
	for end < len(data) {
		_, size := utf8.DecodeRune(data[end:])
		if size <= 0 {
			size = 1
		}
		if end > start && end-start+size > limit {
			break
		}
		end += size
	}
	if end == start {
		_, size := utf8.DecodeRune(data[start:])
		if size <= 0 {
			size = 1
		}
		end = start + size
	}
	return end
}

// flushAttachPending emits the last incomplete rune bytes as replacement
// characters at a validated exit, when no later chunk can complete them, and
// advances each offset to its raw end. It is never used on an error or
// reconnect: those preserve the pending bytes and report the emitted offsets.
func (h *commandAttachHandler) flushAttachPending(encoder *jsontext.Encoder, controller *http.ResponseController, stdout, stderr *attachStreamOffset, reconnectAt time.Time) error {
	streams := []struct {
		kind   CommandAttachFrameType
		stream *attachStreamOffset
	}{
		{CommandAttachStdout, stdout},
		{CommandAttachStderr, stderr},
	}
	for _, pending := range streams {
		if len(pending.stream.pending) == 0 {
			continue
		}
		if !h.now().Before(reconnectAt) {
			if err := h.writeAttachReconnect(encoder, controller, *stdout, *stderr); err != nil {
				return err
			}
			return errAttachReconnect
		}
		next := pending.stream.raw
		frame := CommandAttachFrame{
			Type:       pending.kind,
			Data:       strings.ToValidUTF8(string(pending.stream.pending), "\uFFFD"),
			NextOffset: &next,
		}
		if err := h.writeFrame(encoder, controller, frame); err != nil {
			return err
		}
		pending.stream.pending = nil
		pending.stream.emitted = next
	}
	return nil
}

// writeAttachStreamError writes the one terminal error frame after the
// attached frame. It preserves pending rune bytes and reports the emitted
// offsets, so opening the stream again at those offsets replays them once. A
// writer failure is returned silently: the caller must not write a second
// terminal frame.
func (h *commandAttachHandler) writeAttachStreamError(encoder *jsontext.Encoder, controller *http.ResponseController, processID uint32, stdout, stderr attachStreamOffset, apiErr *apierr.Error) {
	frame := CommandAttachFrame{
		Type:         CommandAttachError,
		ProcessID:    processID,
		Error:        apiErr.Code,
		Message:      apiErr.Message,
		StdoutOffset: new(stdout.emitted),
		StderrOffset: new(stderr.emitted),
	}
	_ = h.writeFrame(encoder, controller, frame)
}

// writeAttachSetupError writes a pre-header failure as a normal API error
// envelope, mapping the node status like the status endpoint.
func (h *commandAttachHandler) writeAttachSetupError(w http.ResponseWriter, r *http.Request, processID uint32, state SandboxState, err error) {
	if r.Context().Err() != nil {
		return
	}
	h.commands.logger.WarnContext(r.Context(), "api: command attach node call failed",
		"process_id", processID, "error", commandErrorName(err))
	apierr.WriteError(w, r, processNodeErrorForState(err, state))
}

// writeAttachProtocolError writes a pre-header 502 for a first event that is
// not one valid attached frame.
func (h *commandAttachHandler) writeAttachProtocolError(w http.ResponseWriter, r *http.Request, processID uint32) {
	if r.Context().Err() != nil {
		return
	}
	h.commands.logger.WarnContext(r.Context(), "api: command attach stream has no valid attached frame",
		"process_id", processID)
	apierr.WriteError(w, r, commandDirectFailed())
}

// writeFrame encodes one NDJSON frame, flushes it and clears the write
// deadline before the handler waits for the node again. ErrNotSupported is the
// standard ResponseWriter behaviour for httptest and wrappers without
// deadlines. A genuine deadline-set, encode, flush or deadline-clear error is a
// writer failure: the caller cancels the RPC, joins the pump and writes no
// second terminal frame.
func (h *commandAttachHandler) writeFrame(encoder *jsontext.Encoder, controller *http.ResponseController, frame CommandAttachFrame) (err error) {
	deadlineSet := false
	if h.writeTimeout > 0 {
		setErr := controller.SetWriteDeadline(time.Now().Add(h.writeTimeout))
		switch {
		case setErr == nil:
			deadlineSet = true
		case errors.Is(setErr, http.ErrNotSupported):
		default:
			return setErr
		}
	}
	if deadlineSet {
		defer func() {
			clearErr := controller.SetWriteDeadline(time.Time{})
			if clearErr != nil && !errors.Is(clearErr, http.ErrNotSupported) && err == nil {
				err = clearErr
			}
		}()
	}
	if err = json.MarshalEncode(encoder, frame); err != nil {
		return err
	}
	if err = controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

// startAttachPump starts the one goroutine that calls AttachProcess and reads
// events. The channel has room for one result, so a slow HTTP writer never
// builds an unbounded queue. A native exit is held until the next Recv returns
// the stream's clean EOF: an extra event or any other error after exit is a
// protocol failure, never a successful terminal state. Cancelling ctx unblocks
// a pending Recv, makes the pump stop, and closes results after pumpDone.
func (h *commandAttachHandler) startAttachPump(ctx context.Context, client nodev1.NodeClient, request *nodev1.AttachProcessRequest) (<-chan attachResult, <-chan struct{}) {
	results := make(chan attachResult, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(results)
		events, err := client.AttachProcess(ctx, request)
		if err != nil {
			sendAttachResult(ctx, results, attachResult{err: err})
			return
		}
		if events == nil {
			sendAttachResult(ctx, results, attachResult{err: errors.New("api: attach returned no stream")})
			return
		}
		for {
			event, err := events.Recv()
			if err != nil {
				sendAttachResult(ctx, results, attachResult{err: err})
				return
			}
			if isNativeExit(event) {
				next, nextErr := events.Recv()
				switch {
				case nextErr == nil:
					// Data, a duplicate exit or any event after
					// the terminal exit breaks the frozen order.
					_ = next
					sendAttachResult(ctx, results, attachResult{err: errAttachProtocol})
				case errors.Is(nextErr, io.EOF):
					sendAttachResult(ctx, results, attachResult{event: event, final: true})
				default:
					sendAttachResult(ctx, results, attachResult{err: nextErr})
				}
				return
			}
			if !sendAttachResult(ctx, results, attachResult{event: event}) {
				return
			}
		}
	}()
	return results, done
}

// isNativeExit reports whether event's oneof is the exit arm without
// dereferencing a typed-nil wrapper.
func isNativeExit(event *nodev1.ProcessEvent) bool {
	switch event.GetEvent().(type) {
	case *nodev1.ProcessEvent_Exit:
		return true
	default:
		return false
	}
}

// sendAttachResult delivers one result unless the stream context ended.
func sendAttachResult(ctx context.Context, results chan<- attachResult, result attachResult) bool {
	select {
	case results <- result:
		return true
	case <-ctx.Done():
		return false
	}
}

// attachStreamOffset is one stream's raw cursor. emitted is the raw offset of
// the next byte no client has seen; pending holds at most three raw bytes of
// an incomplete rune already received. Both streams always satisfy
// raw == emitted + len(pending).
type attachStreamOffset struct {
	emitted int64
	pending []byte
	raw     int64
}

// commandAttachExitFrame builds the terminal exit frame. status is one of the
// pinned running/exited/lost values (never running here). exitCode and signal
// are present as null or a value and are mutually exclusive.
func commandAttachExitFrame(process *nodev1.Process, stdoutOffset, stderrOffset int64) CommandAttachFrame {
	frame := CommandAttachFrame{
		Type:         CommandAttachExit,
		Status:       processWireStatus(process.State),
		ExitCode:     Null[int](),
		Signal:       Null[string](),
		StdoutOffset: &stdoutOffset,
		StderrOffset: &stderrOffset,
	}
	if process.State == nodev1.ProcessState_PROCESS_STATE_EXITED && process.Exit != nil {
		if process.Exit.ExitCode != nil {
			frame.ExitCode = Some(int(*process.Exit.ExitCode))
		}
		if process.Exit.Signal != "" {
			frame.Signal = Some(process.Exit.Signal)
		}
	}
	return frame
}

// parseCommandAttachQuery strictly parses the two pinned nonnegative offset
// parameters. Unknown names, repeats and out-of-range values are invalid_query.
func parseCommandAttachQuery(r *http.Request) (stdout, stderr int64, apiErr *apierr.Error) {
	values, apiErr := coreParseQuery(r)
	if apiErr != nil {
		return 0, 0, apiErr
	}
	for _, name := range slices.Sorted(maps.Keys(values)) {
		if name != "stdoutOffset" && name != "stderrOffset" {
			return 0, 0, coreInvalidQuery(name, "unsupported query parameter")
		}
		if len(values[name]) != 1 {
			return 0, 0, coreInvalidQuery(name, "must appear at most once")
		}
	}
	parsed, apiErr := parseStatusOffset(values, "stdoutOffset")
	if apiErr != nil {
		return 0, 0, apiErr
	}
	if parsed != nil {
		stdout = *parsed
	}
	parsed, apiErr = parseStatusOffset(values, "stderrOffset")
	if apiErr != nil {
		return 0, 0, apiErr
	}
	if parsed != nil {
		stderr = *parsed
	}
	return stdout, stderr, nil
}
