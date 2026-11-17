package api

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// CommandsPath is the API path pattern of the command endpoint. It carries no
// HTTP method: the server adapter must register "POST "+CommandsPath. The
// handler does not re-check the method because the mux pattern already does.
const CommandsPath = "/api/v1/sandboxes/{sandboxId}/commands"

// Command readiness defaults. The v1 API document says a command sent while a
// sandbox starts waits at most 60 seconds; the state is observed through
// durable storage, so the interval only bounds the polling latency.
const (
	DefaultCommandReadyDeadline = 60 * time.Second
	DefaultCommandReadyPoll     = time.Second
)

// DefaultCommandOutputLimit bounds each of stdout and stderr in a synchronous
// response. The stream keeps flowing after the bound: the command keeps
// running, the excess output is drained and the truncated flag is set.
const DefaultCommandOutputLimit = 1 << 20

// DefaultCommandExecutionGrace is added to the requested command timeout for
// the handler's own watchdog. The node enforces the real timeout and sends its
// terminal event first; the grace only covers a hung node call.
const DefaultCommandExecutionGrace = 5 * time.Second

// commandCapacityRetrySeconds is the retry hint on the command's 503
// out_of_capacity answer.
const commandCapacityRetrySeconds = 5

// ErrCommandTargetNotFound reports that an owner-scoped command target does
// not exist. A CommandTargetLookup returns it (possibly wrapped); the handler
// answers the API 404 not_found error and never reveals another tenant's
// sandbox.
var ErrCommandTargetNotFound = errors.New("api: command target not found")

// CommandTarget is the owner-scoped routing information of a sandbox that can
// run commands. State is the durable sandbox state; NodeID is the node that
// owns the running machine and is required once State is ready, idle or
// running.
type CommandTarget struct {
	NodeID string
	State  SandboxState
}

// CommandTargetLookup resolves one sandbox for one owner. Implementations must
// scope by owner: a sandbox owned by someone else is ErrCommandTargetNotFound,
// never a target. The handler calls it again while the sandbox is starting,
// so it must read durable state rather than a process-local cache.
type CommandTargetLookup interface {
	TargetLookup(ctx context.Context, owner, sandboxID string) (CommandTarget, error)
}

// NodeClientFactory returns a nodev1 client bound to nodeID and a function
// that releases it. A nil close function is treated as a no-op. The factory is
// the only seam into the node gateway: the command handler never dials a node
// itself.
type NodeClientFactory func(nodeID string) (nodev1.NodeClient, func() error, error)

// CommandsOptions are the dependencies and limits of a Commands handler.
type CommandsOptions struct {
	// Lookup resolves the owner-scoped sandbox target. Required.
	Lookup CommandTargetLookup
	// Clients returns a client for the target's node. Required.
	Clients NodeClientFactory
	// Store persists detached command records and process ids. It is
	// optional for the existing synchronous and streamed modes, and
	// required for detached starts and the status endpoint.
	Store CommandStore
	// Logger receives operational failures. Nil discards them.
	Logger *slog.Logger
	// StreamMaxSeconds caps streamed command timeouts. Zero uses
	// DefaultStreamMaxSeconds (7200), the approved Phase 0.5 difference
	// from the pinned 600-second maximum. A negative value or one that
	// cannot be represented as a gRPC uint32 timeout is a configuration
	// error.
	StreamMaxSeconds int
	// ReadyDeadline bounds how long a command waits for a starting sandbox.
	// Zero or negative uses DefaultCommandReadyDeadline (60 seconds).
	ReadyDeadline time.Duration
	// ReadyPoll is the interval between durable target lookups while a
	// sandbox is starting. Zero or negative uses DefaultCommandReadyPoll.
	ReadyPoll time.Duration
	// OutputLimit bounds each of stdout and stderr in a synchronous
	// response. Zero or negative uses DefaultCommandOutputLimit (1 MiB).
	OutputLimit int
	// ExecutionGrace is added to the requested timeout for the handler's
	// watchdog. Zero uses DefaultCommandExecutionGrace; the watchdog only
	// covers a node that never sends a terminal event.
	ExecutionGrace time.Duration
	// ExecutionBudget derives the local watchdog from the requested
	// timeout. Nil means timeout plus ExecutionGrace. It is a test seam;
	// production callers leave it nil.
	ExecutionBudget func(timeout time.Duration) time.Duration
}

// commandsHandler serves POST /api/v1/sandboxes/{sandboxId}/commands. It is
// deliberately not part of the Handlers struct: it carries its own narrow
// lookup, node-client and logging dependencies.
type commandsHandler struct {
	lookup        CommandTargetLookup
	clients       NodeClientFactory
	store         CommandStore
	logger        *slog.Logger
	streamMax     int
	readyDeadline time.Duration
	readyPoll     time.Duration
	outputLimit   int
	execution     func(timeout time.Duration) time.Duration
}

// NewCommandsHandler returns the v1 command handler. The caller
// registers it for "POST "+CommandsPath. The handler authenticates the tenant
// through auth.IdentityFrom, resolves the sandbox through Lookup, and runs the
// command over the node client returned by Clients.
func NewCommandsHandler(opts CommandsOptions) (http.Handler, error) {
	return newCommandsHandler(opts)
}

// newCommandsHandler builds the configuration shared by the start and status
// handlers. The constructors keep the public signatures feature-specific.
func newCommandsHandler(opts CommandsOptions) (*commandsHandler, error) {
	if opts.Lookup == nil {
		return nil, errors.New("api: command handler needs a target lookup")
	}
	if opts.Clients == nil {
		return nil, errors.New("api: command handler needs a node-client factory")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if opts.StreamMaxSeconds < 0 || uint64(opts.StreamMaxSeconds) > math.MaxUint32 {
		return nil, fmt.Errorf("api: stream max seconds %d is not representable as a command timeout", opts.StreamMaxSeconds)
	}
	streamMax := opts.StreamMaxSeconds
	if streamMax == 0 {
		streamMax = DefaultStreamMaxSeconds
	}
	readyDeadline := opts.ReadyDeadline
	if readyDeadline <= 0 {
		readyDeadline = DefaultCommandReadyDeadline
	}
	readyPoll := opts.ReadyPoll
	if readyPoll <= 0 {
		readyPoll = DefaultCommandReadyPoll
	}
	outputLimit := opts.OutputLimit
	if outputLimit <= 0 {
		outputLimit = DefaultCommandOutputLimit
	}
	grace := opts.ExecutionGrace
	if grace <= 0 {
		grace = DefaultCommandExecutionGrace
	}
	execution := opts.ExecutionBudget
	if execution == nil {
		execution = func(timeout time.Duration) time.Duration { return timeout + grace }
	}
	return &commandsHandler{
		lookup:        opts.Lookup,
		clients:       opts.Clients,
		store:         opts.Store,
		logger:        logger,
		streamMax:     streamMax,
		readyDeadline: readyDeadline,
		readyPoll:     readyPoll,
		outputLimit:   outputLimit,
		execution:     execution,
	}, nil
}

// ServeHTTP runs one command request.
func (h *commandsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	identity, ok := auth.IdentityFrom(r.Context())
	if !ok || identity.Owner == "" {
		apierr.WriteError(w, r, apierr.New(http.StatusUnauthorized, apierr.CodeUnauthorized, "unauthorized"))
		return
	}
	sandboxID := r.PathValue(ParamSandboxID)
	if sandboxID == "" {
		apierr.WriteError(w, r, commandNotFound())
		return
	}
	if apiErr := requireCommandJSONMedia(r); apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}

	request, apiErr := decodeCommandRequest(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	if request.Detached != nil && *request.Detached {
		// A detached command has no time limit. Validate the pinned
		// 1-600 range so an out-of-range value is still invalid_timeout,
		// then ignore it. stream is ignored too: detached wins.
		if _, apiErr := h.commandTimeout(request, false); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
			return
		}
		h.serveDetached(w, r, identity.Owner, sandboxID, request)
		return
	}
	stream := request.Stream != nil && *request.Stream
	timeout, apiErr := h.commandTimeout(request, stream)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}

	target, apiErr, proceed := h.resolveTarget(r.Context(), identity.Owner, sandboxID)
	if !proceed {
		return
	}
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}

	client, closeClient, apiErr := h.dial(target.NodeID)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	defer h.closeClient(closeClient)

	execRequest := &nodev1.ExecRequest{
		SandboxId:      sandboxID,
		Command:        request.Command,
		Cwd:            cwdOf(request),
		TimeoutSeconds: timeout,
	}
	if stream {
		h.serveStream(w, r, client, execRequest)
		return
	}
	h.serveSync(w, r, client, execRequest, request)
}

// commandRequestSyntax is the strict member shape. Every member is kept as raw
// JSON so explicit null can be distinguished from omission: the pinned request
// requires command, and null is not a valid value for any member, including
// timeoutSeconds.
type commandRequestSyntax struct {
	Command        jsontext.Value `json:"command,omitzero"`
	Cwd            jsontext.Value `json:"cwd,omitzero"`
	TimeoutSeconds jsontext.Value `json:"timeoutSeconds,omitzero"`
	Detached       jsontext.Value `json:"detached,omitzero"`
	Stream         jsontext.Value `json:"stream,omitzero"`
}

// decodeCommandRequest parses and validates the command body. It reads the
// body once, validates member presence and types, and only then decodes the
// accepted DTO. A timeout member of the wrong type or explicit null is the
// pinned invalid_timeout; every other bad body is invalid_json.
func decodeCommandRequest(r *http.Request) (CommandRequest, *apierr.Error) {
	var request CommandRequest
	raw, err := readCommandBody(r)
	if err != nil {
		return request, invalidCommandJSON()
	}
	var syntax commandRequestSyntax
	if err := json.Unmarshal(raw, &syntax, json.RejectUnknownMembers(true)); err != nil {
		return request, invalidCommandJSON()
	}
	if apiErr := validateCommandSyntax(syntax); apiErr != nil {
		return request, apiErr
	}
	if err := DecodeJSON(r, &request); err != nil {
		return request, invalidCommandJSON()
	}
	return request, nil
}

// validateCommandSyntax enforces the pinned required member and nullability
// rules before the DTO decode. command must be a string; cwd, detached and
// stream may be omitted but never null; timeoutSeconds must be an integer and
// explicit null is invalid_timeout.
func validateCommandSyntax(syntax commandRequestSyntax) *apierr.Error {
	command := bytes.TrimSpace(syntax.Command)
	switch {
	case len(command) == 0 || bytes.Equal(command, []byte("null")):
		return apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON, "command is required")
	case !isJSONString(command):
		return apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON, "command must be a string")
	}
	if !jsonStringOrAbsent(syntax.Cwd) {
		return apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON, "cwd must be a string")
	}
	if !jsonBoolOrAbsent(syntax.Detached) {
		return apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON, "detached must be a boolean")
	}
	if !jsonBoolOrAbsent(syntax.Stream) {
		return apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON, "stream must be a boolean")
	}
	if commandTimeoutTypeInvalid(syntax.TimeoutSeconds) {
		return invalidCommandTimeout("timeoutSeconds must be an integer within the allowed range")
	}
	return nil
}

// jsonStringOrAbsent reports whether v is omitted or a JSON string.
func jsonStringOrAbsent(v jsontext.Value) bool {
	return jsonAbsent(v) || isJSONString(v)
}

// jsonBoolOrAbsent reports whether v is omitted or a JSON boolean literal.
func jsonBoolOrAbsent(v jsontext.Value) bool {
	v = bytes.TrimSpace(v)
	return len(v) == 0 || bytes.Equal(v, []byte("true")) || bytes.Equal(v, []byte("false"))
}

// jsonAbsent reports whether the member was not present at all.
func jsonAbsent(v jsontext.Value) bool {
	return len(bytes.TrimSpace(v)) == 0
}

// readCommandBody reads the bounded JSON body and restores it for DecodeJSON.
func readCommandBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, ErrEmptyBody
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, MaxJSONBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxJSONBodyBytes {
		return nil, ErrBodyTooLarge
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	return raw, nil
}

// requireCommandJSONMedia rejects a declared non-JSON media type. A missing
// Content-Type is accepted because the body is still decoded strictly; a
// declared type must be application/json so an obvious client bug is not
// mistaken for a JSON syntax error.
func requireCommandJSONMedia(r *http.Request) *apierr.Error {
	raw := r.Header.Get("Content-Type")
	if raw == "" {
		return nil
	}
	mediaType, _, err := mime.ParseMediaType(raw)
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		return apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON, "Content-Type must be application/json")
	}
	return nil
}

// isJSONString reports whether v is a JSON string (including the empty one).
func isJSONString(v jsontext.Value) bool {
	v = bytes.TrimSpace(v)
	return len(v) > 0 && v[0] == '"'
}

// commandTimeoutTypeInvalid reports whether a present timeoutSeconds member is
// not a JSON integer. null is invalid too: the pin makes the member an integer
// with a default, not a nullable member.
func commandTimeoutTypeInvalid(v jsontext.Value) bool {
	v = bytes.TrimSpace(v)
	if len(v) == 0 {
		return false
	}
	if bytes.Equal(v, []byte("null")) {
		return true
	}
	switch v[0] {
	case '"', 't', 'f', '[', '{':
		return true
	}
	if bytes.ContainsAny(v, ".eE") {
		return true
	}
	_, err := strconv.ParseInt(string(v), 10, 64)
	return err != nil
}

// commandTimeout validates the requested timeout and returns it in seconds.
func (h *commandsHandler) commandTimeout(request CommandRequest, stream bool) (uint32, *apierr.Error) {
	seconds := DefaultCommandTimeoutSeconds
	maximum := MaxCommandTimeoutSeconds
	if stream {
		maximum = h.streamMax
	}
	if request.TimeoutSeconds != nil {
		seconds = *request.TimeoutSeconds
		if stream && seconds > maximum {
			return 0, invalidCommandTimeout(fmt.Sprintf("timeoutSeconds must be an integer from 1 to %d", maximum))
		}
	} else if stream && seconds > maximum {
		seconds = maximum
	}
	if seconds < 1 || seconds > maximum {
		return 0, invalidCommandTimeout(fmt.Sprintf("timeoutSeconds must be an integer from 1 to %d", maximum))
	}
	return uint32(seconds), nil
}

// cwdOf returns the request working directory, or "" for the node default.
func cwdOf(request CommandRequest) string {
	if request.Cwd == nil {
		return ""
	}
	return *request.Cwd
}

// resolveTarget waits for a starting sandbox and returns its routing target.
// The lookup and the sleep both run under a deadline context, so a
// context-aware lookup that blocks cannot exceed the readiness deadline and a
// poll interval longer than the remaining budget cannot overshoot it.
//
// proceed is false only when the request context ended; a readiness deadline
// is the retryable sandbox_starting conflict.
func (h *commandsHandler) resolveTarget(ctx context.Context, owner, sandboxID string) (CommandTarget, *apierr.Error, bool) {
	deadline := time.Now().Add(h.readyDeadline)
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	lastState := SandboxState("")
	for {
		target, err := h.lookup.TargetLookup(waitCtx, owner, sandboxID)
		if err != nil {
			if ctx.Err() != nil {
				return CommandTarget{}, nil, false
			}
			if waitCtx.Err() != nil {
				return CommandTarget{}, commandStarting(lastState), true
			}
			if errors.Is(err, ErrCommandTargetNotFound) {
				return CommandTarget{}, commandNotFound(), true
			}
			if apiErr, ok := errors.AsType[*apierr.Error](err); ok {
				// A lookup only distinguishes found from missing. Do
				// not forward its message: storage errors can echo
				// query text or a credential.
				h.logger.ErrorContext(ctx, "api: command target lookup failed",
					"sandbox_id", sandboxID, "status", apiErr.Status, "code", apiErr.Code)
				if apiErr.Status == http.StatusNotFound {
					return CommandTarget{}, commandNotFound(), true
				}
				return CommandTarget{}, commandInternal(), true
			}
			h.logger.ErrorContext(ctx, "api: command target lookup failed",
				"sandbox_id", sandboxID, "error", commandErrorName(err))
			return CommandTarget{}, commandInternal(), true
		}
		lastState = target.State
		switch {
		case commandStateReady(target.State):
			if target.NodeID == "" {
				h.logger.ErrorContext(ctx, "api: ready sandbox has no node",
					"sandbox_id", sandboxID, "state", target.State)
				return CommandTarget{}, commandNotReady(target.State), true
			}
			return target, nil, true
		case commandStateStarting(target.State):
			if ctx.Err() != nil {
				return CommandTarget{}, nil, false
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return CommandTarget{}, commandStarting(lastState), true
			}
			timer := time.NewTimer(min(h.readyPoll, remaining))
			select {
			case <-ctx.Done():
				timer.Stop()
				return CommandTarget{}, nil, false
			case <-waitCtx.Done():
				timer.Stop()
				if ctx.Err() != nil {
					return CommandTarget{}, nil, false
				}
				return CommandTarget{}, commandStarting(lastState), true
			case <-timer.C:
			}
		default:
			return CommandTarget{}, commandNotReady(target.State), true
		}
	}
}

// commandStateReady reports whether a sandbox accepts commands.
func commandStateReady(state SandboxState) bool {
	switch state {
	case StateReady, StateIdle, StateRunning:
		return true
	default:
		return false
	}
}

// commandStateStarting reports whether a sandbox is still coming up and a
// command should wait for it.
func commandStateStarting(state SandboxState) bool {
	switch state {
	case StateInit, StateProvisioning, StateProvisioned, StateCloning:
		return true
	default:
		return false
	}
}

// dial returns the node client of target and releases it when done.
func (h *commandsHandler) dial(nodeID string) (nodev1.NodeClient, func() error, *apierr.Error) {
	client, closeClient, err := h.clients(nodeID)
	if err != nil {
		h.logger.Error("api: command node client", "node_id", nodeID, "error", commandErrorName(err))
		return nil, nil, commandDirectFailed()
	}
	if client == nil {
		h.logger.Error("api: command node client is nil", "node_id", nodeID)
		return nil, nil, commandInternal()
	}
	if closeClient == nil {
		closeClient = func() error { return nil }
	}
	return client, closeClient, nil
}

// closeClient releases a node client, logging a failure without failing the
// already-finished response.
func (h *commandsHandler) closeClient(closeClient func() error) {
	if err := closeClient(); err != nil {
		h.logger.Debug("api: close command node client", "error", commandErrorName(err))
	}
}

// commandErrorName classifies an error for logs without echoing its text. RPC
// text can contain a command, path or environment value, and lookup text can
// contain credentials; only the gRPC code or Go type is safe.
func commandErrorName(err error) string {
	if st, ok := status.FromError(err); ok {
		return "grpc/" + st.Code().String()
	}
	switch {
	case errors.Is(err, ErrCommandTargetNotFound):
		return "not_found"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	}
	return fmt.Sprintf("%T", err)
}

// serveSync runs a synchronous command and writes its finished envelope.
//
// The node enforces the requested timeout and sends its terminal exit event;
// the local watchdog only fires after the timeout plus a small grace so it
// never races a healthy node's timeout. If it fires, the result is the same
// exitCode:null timeout envelope, not an ambiguous node failure.
func (h *commandsHandler) serveSync(w http.ResponseWriter, r *http.Request, client nodev1.NodeClient, execRequest *nodev1.ExecRequest, request CommandRequest) {
	ctx, cancel := h.executionContext(r, execRequest)
	defer cancel()
	startedAt := time.Now().UTC()
	events, err := client.Exec(ctx, execRequest)
	if err != nil {
		h.writeNodeError(w, r, err)
		return
	}
	stdout := commandCappedBuffer{limit: h.outputLimit}
	stderr := commandCappedBuffer{limit: h.outputLimit}
	first, err := events.Recv()
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		// Nothing proved the command reached a terminal state; this is
		// the ambiguous node failure, watchdog or not.
		h.writeNodeError(w, r, err)
		return
	}
	if first.GetStarted() == nil {
		h.writeNodeProtocolError(w, r)
		return
	}

	var exit *nodev1.ExecEvent_Exit
	for exit == nil {
		event, recvErr := events.Recv()
		if recvErr != nil {
			if r.Context().Err() != nil {
				return
			}
			if commandWatchdogExpired(ctx) {
				exit = &nodev1.ExecEvent_Exit{TimedOut: true}
				break
			}
			h.writeNodeError(w, r, recvErr)
			return
		}
		switch e := event.GetEvent().(type) {
		case *nodev1.ExecEvent_Started_:
			h.writeNodeProtocolError(w, r)
			return
		case *nodev1.ExecEvent_Stdout:
			stdout.Write(e.Stdout)
		case *nodev1.ExecEvent_Stderr:
			stderr.Write(e.Stderr)
		case *nodev1.ExecEvent_Exit_:
			if e.Exit == nil {
				h.writeNodeProtocolError(w, r)
				return
			}
			exit = e.Exit
		default:
			h.writeNodeProtocolError(w, r)
			return
		}
	}
	finishedAt := time.Now().UTC()
	response := commandResponse(request, exit, stdout, stderr, startedAt, finishedAt)
	apierr.Write(w, http.StatusOK, TypeCommandFinished, response)
}

// executionContext bounds one node Exec call with the requested timeout plus
// the configured grace. Parent cancellation is preserved.
func (h *commandsHandler) executionContext(r *http.Request, execRequest *nodev1.ExecRequest) (context.Context, context.CancelFunc) {
	timeout := time.Duration(execRequest.GetTimeoutSeconds()) * time.Second
	budget := h.execution(timeout)
	return context.WithTimeout(r.Context(), budget)
}

// commandWatchdogExpired reports whether the handler's own execution budget
// ended. A parent cancellation is reported separately and writes nothing.
func commandWatchdogExpired(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.DeadlineExceeded)
}

// commandResponse builds the synchronous API envelope from the node's exit
// event. A timed-out command carries exitCode null, as the v1 API document
// requires.
func commandResponse(request CommandRequest, exit *nodev1.ExecEvent_Exit, stdout, stderr commandCappedBuffer, startedAt, finishedAt time.Time) CommandResponse {
	timedOut := exit.GetTimedOut()
	var exitCode *int
	success := false
	if !timedOut && exit.ExitCode != nil {
		exitCode = new(int(*exit.ExitCode))
		success = *exit.ExitCode == 0 && exit.GetSignal() == "" && !exit.GetOomKilled()
	}
	response := NewCommandResponse(success, exitCode, stdout.String(), stderr.String(), timedOut)
	if signal := exit.GetSignal(); signal != "" {
		response.Signal = new(signal)
	}
	if exit.GetOomKilled() {
		response.OOMKilled = new(true)
	}
	if stdout.truncated {
		response.StdoutTruncated = new(true)
	}
	if stderr.truncated {
		response.StderrTruncated = new(true)
	}
	response.Cwd = cwdOf(request)
	response.StartedAt = &startedAt
	response.FinishedAt = &finishedAt
	return response
}

// writeNodeError writes a node RPC failure before any response bytes went out.
func (h *commandsHandler) writeNodeError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	h.logger.WarnContext(r.Context(), "api: command node call failed", "error", commandErrorName(err))
	apierr.WriteError(w, r, commandNodeError(err))
}

// writeNodeProtocolError writes a 502 when a node violates the Exec frame
// order before the stream started.
func (h *commandsHandler) writeNodeProtocolError(w http.ResponseWriter, r *http.Request) {
	if r.Context().Err() != nil {
		return
	}
	h.logger.WarnContext(r.Context(), "api: command node stream is malformed")
	apierr.WriteError(w, r, commandDirectFailed())
}

// commandNodeError maps an Exec failure to the pinned command error. EOF before
// an exit event means the node ended the stream early; both are the ambiguous
// sandbox_direct_failed, never a silent success. A capacity refusal carries the
// API retry hint clients read on a 503.
func commandNodeError(err error) *apierr.Error {
	if errors.Is(err, io.EOF) {
		return commandDirectFailed()
	}
	apiErr := commandNodeMap().Map(err)
	if st, ok := status.FromError(err); ok && st.Code() == codes.ResourceExhausted {
		apiErr.RetryAfterSeconds(commandCapacityRetrySeconds)
	}
	return apiErr
}

// commandNodeMap is the node error table for Exec.
func commandNodeMap() apierr.NodeMap {
	return apierr.NodeMap{
		NotFound:        apierr.CodeNotFound,
		InvalidArgument: apierr.CodeInvalidPath,
		Retryable:       false,
	}
}

// commandNotFound is the owner-scoped missing-sandbox error.
func commandNotFound() *apierr.Error {
	return apierr.New(http.StatusNotFound, apierr.CodeNotFound, "not found")
}

// commandNotReady is the pinned state conflict for a sandbox that cannot run
// commands. The state detail mirrors the API error model.
func commandNotReady(state SandboxState) *apierr.Error {
	return apierr.New(http.StatusConflict, apierr.CodeSandboxNotReady, "sandbox is not ready").
		WithDetail("state", string(state)).
		Retryable(false)
}

// commandStarting is the retryable 409 returned after a sandbox stayed
// starting for the readiness deadline.
func commandStarting(state SandboxState) *apierr.Error {
	apiErr := apierr.New(http.StatusConflict, apierr.CodeSandboxStarting, "sandbox is still starting").Retryable(true)
	if state != "" {
		apiErr.WithDetail("state", string(state))
	}
	return apiErr
}

// commandDirectFailed is the ambiguous node failure: a command may already be
// running, so it is never retryable automatically.
func commandDirectFailed() *apierr.Error {
	return apierr.New(http.StatusBadGateway, apierr.CodeSandboxDirectFailed,
		"the sandbox machine could not complete the call").Retryable(false)
}

// commandInternal is the safe 500 for failures that are not the node's answer.
func commandInternal() *apierr.Error {
	return apierr.New(http.StatusInternalServerError, apierr.CodeInternal, "internal server error")
}

// invalidCommandJSON is the pinned bad-body error.
func invalidCommandJSON() *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidJSON, "request body is not valid JSON")
}

// invalidCommandTimeout is the pinned timeout error.
func invalidCommandTimeout(message string) *apierr.Error {
	return apierr.New(http.StatusBadRequest, apierr.CodeInvalidTimeout, message)
}
