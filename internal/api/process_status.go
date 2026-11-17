package api

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// commandStatusQueryName reports whether name is one of the pinned
// commandStatus query parameters. Every other name is invalid_query.
func commandStatusQueryName(name string) bool {
	switch name {
	case "tailBytes", "stdoutOffset", "stderrOffset":
		return true
	default:
		return false
	}
}

// commandStatusQuery is the validated query of one status request.
type commandStatusQuery struct {
	tailBytes    int64
	stdoutOffset *int64
	stderrOffset *int64
}

// commandStatusHandler serves GET ProcessStatusPath. It shares the start
// handler's target lookup, node-client factory, store and logger.
type commandStatusHandler struct {
	commands *commandsHandler
}

// NewCommandStatusHandler returns the v1 detached command status
// handler. The caller registers it for "GET "+ProcessStatusPath. Store is
// required here even though the start handler keeps it optional for the
// existing synchronous and streamed modes.
func NewCommandStatusHandler(opts CommandsOptions) (http.Handler, error) {
	if opts.Store == nil {
		return nil, errors.New("api: command status handler needs a command store")
	}
	commands, err := newCommandsHandler(opts)
	if err != nil {
		return nil, err
	}
	return &commandStatusHandler{commands: commands}, nil
}

// ServeHTTP runs one status request.
func (h *commandStatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.commands.serveStatus(w, r)
}

// serveStatus reads the owner-scoped record, asks the node for the requested
// log window and merges the observation back into the record.
func (h *commandsHandler) serveStatus(w http.ResponseWriter, r *http.Request) {
	identity, ok := auth.IdentityFrom(r.Context())
	if !ok || identity.Owner == "" {
		apierr.WriteError(w, r, apierr.New(http.StatusUnauthorized, apierr.CodeUnauthorized, "unauthorized"))
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
	query, apiErr := parseCommandStatusQuery(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	if h.store == nil {
		h.logger.ErrorContext(r.Context(), "api: command status has no command store")
		apierr.WriteError(w, r, commandInternal())
		return
	}
	target, apiErr := h.statusTarget(r.Context(), identity.Owner, sandboxID)
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

	record, err := h.store.CommandRecord(r.Context(), owner, sandboxID, processID)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		if errors.Is(err, db.ErrNotFound) {
			apierr.WriteError(w, r, commandNotFound())
			return
		}
		h.logger.ErrorContext(r.Context(), "api: command status record failed",
			"sandbox_id", sandboxID, "process_id", processID, "error", commandErrorName(err))
		apierr.WriteError(w, r, commandInternal())
		return
	}
	if err := validateStoredCommand(owner, sandboxID, processID, record); err != nil {
		h.logger.ErrorContext(r.Context(), "api: command status record is malformed",
			"sandbox_id", sandboxID, "process_id", processID, "reason", err.Error())
		apierr.WriteError(w, r, commandInternal())
		return
	}
	if record.Status == db.CommandFailed {
		// The command never started, so no node process exists to report.
		apierr.WriteError(w, r, commandNotFound())
		return
	}
	if r.Context().Err() != nil {
		return
	}

	client, closeClient, apiErr := h.dial(target.NodeID)
	if apiErr != nil {
		if r.Context().Err() != nil {
			return
		}
		apierr.WriteError(w, r, apiErr)
		return
	}
	defer h.closeClient(closeClient)

	offsetMode := query.stdoutOffset != nil || query.stderrOffset != nil
	request := &nodev1.GetProcessRequest{SandboxId: sandboxID, ProcessId: processID}
	if offsetMode {
		stdoutOffset := int64(0)
		if query.stdoutOffset != nil {
			stdoutOffset = *query.stdoutOffset
		}
		stderrOffset := int64(0)
		if query.stderrOffset != nil {
			stderrOffset = *query.stderrOffset
		}
		request.StdoutOffset = new(stdoutOffset)
		request.StderrOffset = new(stderrOffset)
		request.LimitBytes = CommandStatusOffsetBytes
	} else {
		request.LimitBytes = uint32(query.tailBytes)
	}

	output, err := client.GetProcess(r.Context(), request)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		h.logger.WarnContext(r.Context(), "api: command status node call failed",
			"process_id", processID, "error", commandErrorName(err))
		apierr.WriteError(w, r, processNodeErrorForState(err, target.State))
		return
	}
	if err := validateProcessOutput(processID, request, output); err != nil {
		h.logger.WarnContext(r.Context(), "api: command status returned a malformed process",
			"process_id", processID, "reason", err.Error())
		apierr.WriteError(w, r, commandDirectFailed())
		return
	}
	observation := commandObservationFromProcess(output.Process)
	// The node's size snapshot can lag its log read. Record at least the
	// raw end of every returned window so a late stop command cannot
	// under-report bytes the client already saw.
	observation.StdoutBytes = max(observation.StdoutBytes, output.StdoutOffset+int64(len(output.Stdout)))
	observation.StderrBytes = max(observation.StderrBytes, output.StderrOffset+int64(len(output.Stderr)))
	updated, err := h.store.ObserveCommand(r.Context(), owner, sandboxID, processID, observation)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		h.logger.ErrorContext(r.Context(), "api: command status observation failed",
			"process_id", processID, "error", commandErrorName(err))
		apierr.WriteError(w, r, commandInternal())
		return
	}
	if err := validateStoredCommand(owner, sandboxID, processID, updated); err != nil {
		h.logger.ErrorContext(r.Context(), "api: command status observation is malformed",
			"process_id", processID, "reason", err.Error())
		apierr.WriteError(w, r, commandInternal())
		return
	}
	if r.Context().Err() != nil {
		return
	}

	apierr.Write(w, http.StatusOK, TypeCommandStatus,
		commandStatusResponse(processID, updated, output, offsetMode))
}

// statusTarget classifies a sandbox for a status read. Unlike a command start it
// does not wait: a starting sandbox answers the retryable 409 sandbox_starting,
// and every other non-ready state answers 409 sandbox_not_ready.
func (h *commandsHandler) statusTarget(ctx context.Context, owner, sandboxID string) (CommandTarget, *apierr.Error) {
	target, err := h.lookup.TargetLookup(ctx, owner, sandboxID)
	if err != nil {
		if errors.Is(err, ErrCommandTargetNotFound) {
			return CommandTarget{}, commandNotFound()
		}
		if apiErr, ok := errors.AsType[*apierr.Error](err); ok {
			h.logger.ErrorContext(ctx, "api: command status target lookup failed",
				"sandbox_id", sandboxID, "status", apiErr.Status, "code", apiErr.Code)
			if apiErr.Status == http.StatusNotFound {
				return CommandTarget{}, commandNotFound()
			}
			return CommandTarget{}, commandInternal()
		}
		h.logger.ErrorContext(ctx, "api: command status target lookup failed",
			"sandbox_id", sandboxID, "error", commandErrorName(err))
		return CommandTarget{}, commandInternal()
	}
	switch {
	case commandStateReady(target.State):
		if target.NodeID == "" {
			h.logger.ErrorContext(ctx, "api: ready sandbox has no node",
				"sandbox_id", sandboxID, "state", target.State)
			return CommandTarget{}, commandNotReady(target.State)
		}
		return target, nil
	case commandStateStarting(target.State):
		return CommandTarget{}, commandStarting(target.State)
	default:
		return CommandTarget{}, commandNotReady(target.State)
	}
}

// parseCommandStatusQuery strictly parses the three pinned query parameters.
// Unknown names, repeats and out-of-range values are invalid_query. tailBytes
// is still validated when offsets are present, then ignored as the pin says.
func parseCommandStatusQuery(r *http.Request) (commandStatusQuery, *apierr.Error) {
	query := commandStatusQuery{tailBytes: DefaultCommandStatusTailBytes}
	values, apiErr := coreParseQuery(r)
	if apiErr != nil {
		return query, apiErr
	}
	for _, name := range slices.Sorted(maps.Keys(values)) {
		if !commandStatusQueryName(name) {
			return query, coreInvalidQuery(name, "unsupported query parameter")
		}
		if len(values[name]) != 1 {
			return query, coreInvalidQuery(name, "must appear at most once")
		}
	}
	if raw, ok := statusQueryValue(values, "tailBytes"); ok {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 1 || n > MaxCommandStatusTailBytes {
			return query, coreInvalidQuery("tailBytes", "must be an integer from 1 to 8388608")
		}
		query.tailBytes = n
	}
	offset, apiErr := parseStatusOffset(values, "stdoutOffset")
	if apiErr != nil {
		return query, apiErr
	}
	query.stdoutOffset = offset
	offset, apiErr = parseStatusOffset(values, "stderrOffset")
	if apiErr != nil {
		return query, apiErr
	}
	query.stderrOffset = offset
	return query, nil
}

// statusQueryValue returns one query value and whether the parameter is
// present.
func statusQueryValue(values url.Values, name string) (string, bool) {
	raw, ok := values[name]
	if !ok || len(raw) == 0 {
		return "", false
	}
	return raw[0], true
}

// parseStatusOffset parses one optional nonnegative byte offset.
func parseStatusOffset(values url.Values, name string) (*int64, *apierr.Error) {
	raw, ok := statusQueryValue(values, name)
	if !ok {
		return nil, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return nil, coreInvalidQuery(name, "must be a nonnegative integer")
	}
	return new(n), nil
}

// parseProcessPathID parses the pinned ProcessId path parameter. A missing,
// non-numeric or zero id is the owner-scoped 404, never another tenant's row.
func parseProcessPathID(raw string) (uint32, bool) {
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || value == 0 {
		return 0, false
	}
	return uint32(value), true
}

// commandStatusResponse builds the pinned status envelope from the node's
// authoritative state and the merged record.
func commandStatusResponse(processID uint32, record db.CommandRecord, output *nodev1.ProcessOutput, offsetMode bool) CommandStatusResponse {
	process := output.Process
	finalEOF := process.State != nodev1.ProcessState_PROCESS_STATE_RUNNING && !output.More
	stdout := processOutputWindow(commandWindowInput{
		data: output.Stdout, start: output.StdoutOffset,
		offsetMode: offsetMode, finalEOF: finalEOF, truncated: output.StdoutTruncated,
	})
	stderr := processOutputWindow(commandWindowInput{
		data: output.Stderr, start: output.StderrOffset,
		offsetMode: offsetMode, finalEOF: finalEOF, truncated: output.StderrTruncated,
	})

	response := CommandStatusResponse{
		SuccessBase: success(TypeCommandStatus),
		Success:     commandStatusSucceeded(process),
		ProcessID:   processID,
		Status:      processWireStatus(process.State),
		Running:     process.State == nodev1.ProcessState_PROCESS_STATE_RUNNING,
		ExitCode:    processExitCode(process),
		Stdout:      stdout.text,
		Stderr:      stderr.text,
	}
	if process.Pid > 0 {
		response.PID = new(process.Pid)
	}
	known := true
	response.Known = &known
	if process.State == nodev1.ProcessState_PROCESS_STATE_EXITED && process.Exit != nil {
		if process.Exit.Signal != "" {
			response.Signal = new(process.Exit.Signal)
		}
		if process.Exit.OomKilled {
			response.OOMKilled = new(true)
		}
	}
	command := process.Command
	if command == "" {
		command = record.Command
	}
	response.Command = &command
	if process.Cwd != "" {
		response.Cwd = new(process.Cwd)
	} else if record.Cwd != nil {
		response.Cwd = new(*record.Cwd)
	}
	if process.StartedAtUnixMs > 0 {
		started := time.UnixMilli(process.StartedAtUnixMs).UTC()
		response.StartedAt = &started
	} else if !record.StartedAt.IsZero() {
		started := record.StartedAt
		response.StartedAt = &started
	}
	if process.State == nodev1.ProcessState_PROCESS_STATE_EXITED {
		if process.FinishedAtUnixMs > 0 {
			finished := time.UnixMilli(process.FinishedAtUnixMs).UTC()
			response.FinishedAt = &finished
		} else if record.FinishedAt != nil {
			response.FinishedAt = new(*record.FinishedAt)
		}
	}
	response.LogPath = process.LogPath
	if response.LogPath == "" {
		response.LogPath = record.LogPath
	}
	response.ErrLogPath = process.ErrLogPath
	if response.ErrLogPath == "" {
		response.ErrLogPath = record.ErrLogPath
	}

	if offsetMode {
		response.StdoutOffset = new(stdout.start)
		response.StderrOffset = new(stderr.start)
		response.StdoutNextOffset = new(stdout.next)
		response.StderrNextOffset = new(stderr.next)
		// Native More reflects the actual log snapshot; the captured
		// sizes cannot prove continuation or its absence because the log
		// may grow, shrink or be replaced.
		response.More = new(output.More)
		return response
	}
	if stdout.truncated {
		response.StdoutTruncated = new(true)
	}
	if stderr.truncated {
		response.StderrTruncated = new(true)
	}
	return response
}

// commandStatusSucceeded mirrors the synchronous command's success meaning for
// the node's final state. A running or lost process has not succeeded.
func commandStatusSucceeded(process *nodev1.Process) bool {
	if process.State != nodev1.ProcessState_PROCESS_STATE_EXITED || process.Exit == nil {
		return false
	}
	return process.Exit.ExitCode != nil && *process.Exit.ExitCode == 0 &&
		process.Exit.Signal == "" && !process.Exit.OomKilled
}

// processExitCode is the pinned nullable exitCode. It is non-null only for an
// exited process with a recorded numeric code.
func processExitCode(process *nodev1.Process) *int {
	if process.State != nodev1.ProcessState_PROCESS_STATE_EXITED ||
		process.Exit == nil || process.Exit.ExitCode == nil {
		return nil
	}
	code := int(*process.Exit.ExitCode)
	return &code
}
