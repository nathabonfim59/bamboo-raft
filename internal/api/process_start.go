package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// serveDetached allocates a never-reused process id, commits its starting
// record, starts the process on the node and answers command.started. The full
// command text is sent to StartProcess and returned on the wire; the store may
// keep only its 64 KiB prefix.
func (h *commandsHandler) serveDetached(w http.ResponseWriter, r *http.Request, ownerText, sandboxID string, request CommandRequest) {
	owner, err := uuid.Parse(ownerText)
	if err != nil || owner == uuid.Nil() {
		apierr.WriteError(w, r, apierr.New(http.StatusUnauthorized, apierr.CodeUnauthorized, "unauthorized"))
		return
	}
	if h.store == nil {
		h.logger.ErrorContext(r.Context(), "api: detached command has no command store")
		apierr.WriteError(w, r, commandInternal())
		return
	}
	target, apiErr, proceed := h.resolveTarget(r.Context(), ownerText, sandboxID)
	if !proceed {
		return
	}
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}

	record, err := h.store.AllocateCommand(r.Context(), owner, db.AllocateCommandParams{
		SandboxID: sandboxID,
		Command:   request.Command,
		Cwd:       request.Cwd,
	})
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		h.writeCommandAllocationError(w, r, sandboxID, target.State, err)
		return
	}
	if err := validateAllocatedCommand(owner, sandboxID, record); err != nil {
		h.logger.ErrorContext(r.Context(), "api: command allocation returned a malformed record",
			"sandbox_id", sandboxID, "reason", err.Error())
		apierr.WriteError(w, r, commandInternal())
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

	startRequest := &nodev1.StartProcessRequest{
		SandboxId: sandboxID,
		ProcessId: record.ProcessID,
		Command:   request.Command,
		Cwd:       cwdOf(request),
	}
	process, ambiguous, err := h.startProcess(r.Context(), client, startRequest)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		// A first ambiguous attempt may have created the process; a later
		// definite answer from the same-id retry does not prove it never
		// ran. Only a definite first refusal becomes a failed record.
		if ambiguous {
			// The first attempt may already have created the process,
			// so even a definite retry answer cannot be treated as a
			// safe first-start failure. Keep the record starting and
			// answer the ambiguous 502.
			h.logger.WarnContext(r.Context(), "api: detached command start stayed ambiguous",
				"process_id", record.ProcessID, "error", commandErrorName(err))
			apierr.WriteError(w, r, commandDirectFailed())
			return
		}
		if processStartDefinite(err) {
			h.observeStartFailure(r.Context(), owner, sandboxID, record.ProcessID)
		}
		h.logger.WarnContext(r.Context(), "api: detached command start failed",
			"process_id", record.ProcessID, "error", commandErrorName(err))
		apierr.WriteError(w, r, processNodeErrorForState(err, target.State))
		return
	}
	if err := validateStartedProcess(record.ProcessID, request.Command, process); err != nil {
		// The process may be running, so this is the ambiguous 502 and the
		// record stays starting. Never observe a malformed result.
		h.logger.WarnContext(r.Context(), "api: detached command start returned a malformed process",
			"process_id", record.ProcessID, "reason", err.Error())
		apierr.WriteError(w, r, commandDirectFailed())
		return
	}
	if _, err := h.store.ObserveCommand(r.Context(), owner, sandboxID, record.ProcessID,
		commandObservationFromProcess(process)); err != nil {
		if r.Context().Err() != nil {
			return
		}
		// The node already started the process. A storage fault is a 500,
		// never the node 502, and the command is not restarted or
		// reallocated.
		h.logger.ErrorContext(r.Context(), "api: detached command observation failed",
			"process_id", record.ProcessID, "error", commandErrorName(err))
		apierr.WriteError(w, r, commandInternalUnretryable())
		return
	}
	if r.Context().Err() != nil {
		return
	}

	apierr.Write(w, http.StatusOK, TypeCommandStarted, CommandStartedResponse{
		SuccessBase: success(TypeCommandStarted),
		Success:     true,
		CommandID:   record.CommandID,
		ProcessID:   record.ProcessID,
		PID:         process.Pid,
		Command:     request.Command,
		Cwd:         process.Cwd,
		StartedAt:   time.UnixMilli(process.StartedAtUnixMs).UTC(),
		LogPath:     process.LogPath,
		ErrLogPath:  process.ErrLogPath,
	})
}

// startProcess calls StartProcess and retries the exact same request once when
// the first failure is transport-ambiguous. The id and command never change.
// ambiguous reports that the first attempt may already have had a side effect,
// so a definite answer from the retry must not mark the record failed.
func (h *commandsHandler) startProcess(ctx context.Context, client nodev1.NodeClient, request *nodev1.StartProcessRequest) (process *nodev1.Process, ambiguous bool, err error) {
	process, err = client.StartProcess(ctx, request)
	if err == nil || ctx.Err() != nil || !processStartAmbiguous(err) {
		return process, false, err
	}
	h.logger.WarnContext(ctx, "api: retrying ambiguous detached command start",
		"process_id", request.ProcessId)
	process, err = client.StartProcess(ctx, request)
	return process, true, err
}

// observeStartFailure marks a definitely failed start as failed. The command
// never ran, so it carries no exit result. This is best effort: the original
// node error still shapes the response.
func (h *commandsHandler) observeStartFailure(ctx context.Context, owner uuid.UUID, sandboxID string, processID uint32) {
	finished := time.Now().UTC()
	_, err := h.store.ObserveCommand(ctx, owner, sandboxID, processID, db.CommandObservation{
		Status:     db.CommandFailed,
		FinishedAt: &finished,
	})
	if err != nil {
		h.logger.WarnContext(ctx, "api: detached command failure observation failed",
			"process_id", processID, "error", commandErrorName(err))
	}
}

// writeCommandAllocationError maps the durable allocation contract to the
// pinned command errors. A storage fault is the standing 500.
func (h *commandsHandler) writeCommandAllocationError(w http.ResponseWriter, r *http.Request, sandboxID string, state SandboxState, err error) {
	switch {
	case errors.Is(err, db.ErrNotFound):
		apierr.WriteError(w, r, commandNotFound())
	case errors.Is(err, db.ErrInvalidState):
		apierr.WriteError(w, r, commandNotReady(state))
	case errors.Is(err, db.ErrCommandIDExhausted):
		apierr.WriteError(w, r, commandProcessIDsExhausted())
	case errors.Is(err, db.ErrInvalidCommand):
		apierr.WriteError(w, r, invalidCommandJSON())
	case errors.Is(err, db.ErrNoOwner):
		apierr.WriteError(w, r, apierr.New(http.StatusUnauthorized, apierr.CodeUnauthorized, "unauthorized"))
	default:
		h.logger.ErrorContext(r.Context(), "api: command allocation failed",
			"sandbox_id", sandboxID, "error", commandErrorName(err))
		apierr.WriteError(w, r, commandInternal())
	}
}
