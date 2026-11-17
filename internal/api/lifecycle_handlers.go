package api

import (
	"net/http"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// updateSandbox serves PATCH /api/v1/sandboxes/{sandboxId}. Name, TTL and
// subdomain are each three-state where the pin defines them; the response is
// the updated pinned sandbox object.
func (h *lifecycleHandlers) updateSandbox(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	sandboxID := r.PathValue(ParamSandboxID)
	if sandboxID == "" {
		apierr.WriteError(w, r, coreNotFound())
		return
	}
	params, apiErr := parseUpdateSandbox(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	params.Owner = owner
	params.SandboxID = sandboxID
	row, err := h.store.PatchSandbox(r.Context(), params)
	if err != nil {
		if apiErr := updateFailure(r.Context(), h.logger, err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	sandbox, err := sandboxView(r.Context(), h.store, owner, row)
	if err != nil {
		if apiErr := lifecycleInternal(r.Context(), h.logger, "update sandbox stop metadata", err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	apierr.Write(w, http.StatusOK, TypeSandboxInfo, NewSandboxInfoResponse(sandbox))
}

// stopSandbox serves POST /api/v1/sandboxes/{sandboxId}/stop. A new or pending
// stop is 202 with the canonical operation; an already archived sandbox is 200
// with its last completed stop.
func (h *lifecycleHandlers) stopSandbox(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	sandboxID := r.PathValue(ParamSandboxID)
	if sandboxID == "" {
		apierr.WriteError(w, r, coreNotFound())
		return
	}
	force, apiErr := parseStopSandbox(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	operation, row, _, _, err := h.store.AcceptSandboxStop(r.Context(), owner, sandboxID, force)
	if err != nil {
		if apiErr := stopFailure(r.Context(), h.logger, err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	operationView := operationFromRow(operation, false)
	sandbox := sandboxFromRow(row)
	sandbox.Stop = &operationView
	status := http.StatusAccepted
	responseType := TypeSandboxStopping
	statusValue := "archiving"
	if row.State == db.StateArchived {
		status = http.StatusOK
		responseType = TypeSandboxStopped
		statusValue = "archived"
	}
	response := NewStopActionResponse(
		responseType, row.ID, statusValue, sandbox, operationView,
		stopStatusPath(row.ID, operation.ID), operation.Forced || operation.ForceRequested,
	)
	apierr.Write(w, status, responseType, response)
}

// getSandboxStop serves GET /api/v1/sandboxes/{sandboxId}/stops/{stopId}. The
// lookup binds owner, sandbox and stop id; an unknown or foreign stop is 404.
func (h *lifecycleHandlers) getSandboxStop(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	sandboxID := r.PathValue(ParamSandboxID)
	stopID := r.PathValue(ParamStopID)
	if sandboxID == "" || stopID == "" {
		apierr.WriteError(w, r, coreNotFound())
		return
	}
	operation, err := h.store.StopOperation(r.Context(), owner, sandboxID, stopID)
	if err != nil {
		if apiErr := stopReadFailure(r.Context(), h.logger, err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	apierr.Write(w, http.StatusOK, TypeStopStatus,
		NewStopOperationResponse(sandboxID, operationFromRow(operation, true)))
}

// resumeSandbox serves POST /api/v1/sandboxes/{sandboxId}/resume. The
// admission seam owns capacity and the durable accept; the handler only reads
// the current state to pass the active stop id and choose the pinned refusal.
func (h *lifecycleHandlers) resumeSandbox(w http.ResponseWriter, r *http.Request) {
	owner, apiErr := coreOwner(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	sandboxID := r.PathValue(ParamSandboxID)
	if sandboxID == "" {
		apierr.WriteError(w, r, coreNotFound())
		return
	}
	input, apiErr := parseResumeSandbox(r)
	if apiErr != nil {
		apierr.WriteError(w, r, apiErr)
		return
	}
	row, err := h.store.Sandbox(r.Context(), owner, sandboxID)
	if err != nil {
		if apiErr := stopReadFailure(r.Context(), h.logger, err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	// An error sandbox has no resumable disk. Refuse before admission so
	// no start, CreateVM fallback or job can be attempted.
	if row.State == db.StateError {
		apierr.WriteError(w, r, sandboxNotReady())
		return
	}
	supersede := ""
	if row.State == db.StateArchiving {
		if row.StopOperationID == nil || *row.StopOperationID == "" {
			apierr.WriteError(w, r, sandboxStopping(row))
			return
		}
		supersede = *row.StopOperationID
	}
	result, err := h.resume.Resume(r.Context(), ResumeRequest{
		Owner:           owner,
		SandboxID:       sandboxID,
		MachineType:     input.machineType,
		Resources:       input.resources,
		Env:             input.env,
		TTL:             input.ttl,
		SupersedeStopID: supersede,
		FailFast:        input.failFast,
	})
	if err != nil {
		current := row
		if fresh, getErr := h.store.Sandbox(r.Context(), owner, sandboxID); getErr == nil {
			current = fresh
		}
		if apiErr := resumeFailure(r.Context(), h.logger, h.capacityRetry, current, input.changes, err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	if result.Sandbox.ID == "" || result.Sandbox.ID != sandboxID || result.Sandbox.OwnerID != owner {
		if apiErr := lifecycleInternal(r.Context(), h.logger, "resume result",
			errResumeResult); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	sandbox, err := sandboxView(r.Context(), h.store, owner, result.Sandbox)
	if err != nil {
		if apiErr := lifecycleInternal(r.Context(), h.logger, "resume stop metadata", err); apiErr != nil {
			apierr.WriteError(w, r, apiErr)
		}
		return
	}
	apierr.Write(w, http.StatusAccepted, TypeSandboxResuming, NewResumeActionResponse(sandboxID, sandbox))
}
