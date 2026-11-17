package api

import (
	"context"
	"errors"
	"net/http"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// commandStopHandler serves DELETE ProcessStatusPath, the detached process's
// path with the GET status operation on it.
type commandStopHandler struct {
	commands *commandsHandler
}

// NewCommandStopHandler returns the v1 detached command stop
// handler. The caller registers it for "DELETE "+ProcessStatusPath. The store
// is required: the response carries the durable record with its final state.
func NewCommandStopHandler(opts CommandsOptions) (http.Handler, error) {
	if opts.Store == nil {
		return nil, errors.New("api: command stop handler needs a command store")
	}
	commands, err := newCommandsHandler(opts)
	if err != nil {
		return nil, err
	}
	return &commandStopHandler{commands: commands}, nil
}

// ServeHTTP stops one detached process.
//
// An already ended record is answered from durable storage without a node
// call: the pinned response says signaled false when the process had already
// ended. A failed-start record never had a process and is not_found. Starting
// and running records need the node's KillProcess.
func (h *commandStopHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	if h.commands.store == nil {
		h.commands.logger.ErrorContext(r.Context(), "api: command stop has no command store")
		apierr.WriteError(w, r, commandInternal())
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
	switch {
	case commandProcessTerminal(record.Status):
		// Nothing is running: answer the durable final record and its
		// native result, with signaled false because this call sent no
		// signal.
		apierr.Write(w, http.StatusOK, TypeCommandStopped, commandStoppedResponse(record, false))
		return
	case record.Status == db.CommandFailed:
		// A definite start failure never created a process.
		apierr.WriteError(w, r, commandNotFound())
		return
	case record.Status == db.CommandStarting || record.Status == db.CommandRunning:
		// The process may exist on the node; ask it to stop.
	default:
		h.commands.logger.ErrorContext(r.Context(), "api: command stop record has an unknown status",
			"sandbox_id", sandboxID, "process_id", processID, "status", record.Status)
		apierr.WriteError(w, r, commandInternal())
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

	// grace_seconds 0 keeps the node's own default (5 seconds), so the
	// server does not hardcode the escalation window from the pin.
	response, err := client.KillProcess(r.Context(), &nodev1.KillProcessRequest{
		SandboxId: sandboxID,
		ProcessId: processID,
	})
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		h.commands.logger.WarnContext(r.Context(), "api: command stop node call failed",
			"process_id", processID, "error", commandErrorName(err))
		apierr.WriteError(w, r, processNodeErrorForState(err, target.State))
		return
	}
	if err := validateKillResponse(processID, response); err != nil {
		// KillProcess may already have signaled the process. Never retry
		// or observe a malformed answer.
		h.commands.logger.WarnContext(r.Context(), "api: command stop returned a malformed process",
			"process_id", processID, "reason", err.Error())
		apierr.WriteError(w, r, commandDirectFailed())
		return
	}
	observation := killObservation(response)
	if r.Context().Err() != nil {
		return
	}
	updated, err := h.commands.store.ObserveCommand(r.Context(), owner, sandboxID, processID, observation)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		// The node already stopped the process. A storage fault is a 500,
		// never a retryable answer that could send a second kill.
		h.commands.logger.ErrorContext(r.Context(), "api: command stop observation failed",
			"process_id", processID, "error", commandErrorName(err))
		apierr.WriteError(w, r, commandInternalUnretryable())
		return
	}
	if err := validateStoredCommand(owner, sandboxID, processID, updated); err != nil {
		h.commands.logger.ErrorContext(r.Context(), "api: command stop observation is malformed",
			"process_id", processID, "reason", err.Error())
		apierr.WriteError(w, r, commandInternalUnretryable())
		return
	}
	if !commandRecordStatusWireable(updated.Status) {
		h.commands.logger.ErrorContext(r.Context(), "api: command stop observation has an internal status",
			"process_id", processID, "status", updated.Status)
		apierr.WriteError(w, r, commandInternalUnretryable())
		return
	}
	if r.Context().Err() != nil {
		return
	}
	apierr.Write(w, http.StatusOK, TypeCommandStopped, commandStoppedResponse(updated, response.GetSignaled()))
}

// validateKillResponse checks that the node answered for this process with a
// final state. KillProcess sends a signal, so a still-running answer is a
// protocol failure; LOST is a valid final state when the VM stopped.
func validateKillResponse(processID uint32, response *nodev1.KillProcessResponse) error {
	if response == nil || response.Process == nil {
		return errors.New("kill returned no process")
	}
	if err := validateProcessRecord(processID, response.Process, true); err != nil {
		return err
	}
	switch response.Process.State {
	case nodev1.ProcessState_PROCESS_STATE_EXITED,
		nodev1.ProcessState_PROCESS_STATE_LOST:
		return nil
	default:
		return errors.New("kill returned a non-final process")
	}
}

// killObservation merges the node's final state with the stop's native signal
// flag. LOST always stays lost with an unknown finish time, even when the node
// also reports signaled. A native EXITED result stays exited unless this stop
// signaled it; a process that had already ended naturally keeps its exit code
// or signal.
func killObservation(response *nodev1.KillProcessResponse) db.CommandObservation {
	process := response.Process
	observation := commandObservationFromProcess(process)
	if process.State == nodev1.ProcessState_PROCESS_STATE_EXITED && response.GetSignaled() {
		observation.Status = db.CommandKilled
	}
	return observation
}

// commandProcessTerminal reports whether a record's process has already ended
// and can be answered without a node call.
func commandProcessTerminal(status db.CommandStatus) bool {
	switch status {
	case db.CommandExited, db.CommandKilled, db.CommandLost, db.CommandTimedOut:
		return true
	default:
		return false
	}
}

// commandRecordStatusWireable reports whether status is one of the pinned
// CommandRecord values. The internal starting state never reaches the wire.
func commandRecordStatusWireable(status db.CommandStatus) bool {
	switch status {
	case db.CommandRunning, db.CommandExited, db.CommandTimedOut,
		db.CommandFailed, db.CommandKilled, db.CommandLost:
		return true
	default:
		return false
	}
}

// readCommandRecord reads and validates one owner-scoped detached command
// record. *db.Store's read joins the sandbox row, so an unknown, deleted or
// foreign sandbox is indistinguishable from an unknown process. A record that
// names another owner, sandbox, process or mode is a safe internal error and
// never reaches the wire or a node call.
func (h *commandsHandler) readCommandRecord(ctx context.Context, owner uuid.UUID, sandboxID string, processID uint32) (db.CommandRecord, *apierr.Error) {
	if h.store == nil {
		h.logger.ErrorContext(ctx, "api: command process has no command store",
			"sandbox_id", sandboxID, "process_id", processID)
		return db.CommandRecord{}, commandInternal()
	}
	record, err := h.store.CommandRecord(ctx, owner, sandboxID, processID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return db.CommandRecord{}, commandNotFound()
		}
		h.logger.ErrorContext(ctx, "api: command record failed",
			"sandbox_id", sandboxID, "process_id", processID, "error", commandErrorName(err))
		return db.CommandRecord{}, commandInternal()
	}
	if err := validateStoredCommand(owner, sandboxID, processID, record); err != nil {
		h.logger.ErrorContext(ctx, "api: command record is malformed",
			"sandbox_id", sandboxID, "process_id", processID, "reason", err.Error())
		return db.CommandRecord{}, commandInternal()
	}
	return record, nil
}
