package lifecycle

import (
	"context"
	"encoding/json/v2"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const (
	recoverResetTimeout       = 5 * time.Minute
	eventSandboxRecovered     = "sandbox.recovered"
	eventSandboxRecoverFailed = "sandbox.recover_failed"
	recoverFailedEventData    = `{"code":"reset_failed"}`
)

// recoverSandbox power-cycles one unresponsive sandbox VM via ResetVM under
// the per-sandbox guard and fences its outcome event with the job lease. If
// the sandbox left ready-ish, was deleted, moved nodes or booted a newer
// generation before the attempt runs, the job finishes without touching the
// node. UNAVAILABLE retries with backoff up to the job's max attempts;
// FAILED_PRECONDITION and other failures record sandbox.recover_failed with
// the fixed reset_failed code and finish without retrying.
func (w *Worker) recoverSandbox(ctx context.Context, job db.Job, lease *Lease) error {
	if job.SandboxID == nil || *job.SandboxID == "" {
		return permanent("missing sandbox id")
	}
	owner, sandboxID := job.OwnerID, *job.SandboxID
	var p db.RecoverPayload
	if err := json.Unmarshal([]byte(job.Payload), &p); err != nil || p.NodeID == "" {
		return permanent("invalid recover payload")
	}

	sbx, err := w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return permanent("sandbox missing")
	}
	if err != nil {
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if !recoverTargetMatches(sbx, p) {
		return nil
	}

	guard, opCtx, stopGuard, err := w.acquireGuarded(ctx, lease, owner, sandboxID)
	if err != nil {
		if lease.Lost() || ctx.Err() != nil {
			return nil
		}
		return retry("sandbox guard unavailable", w.backoff(job.Attempts))
	}
	defer func() {
		stopGuard()
		guard.Release(ctx)
	}()

	sbx, err = w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return permanent("sandbox missing")
	}
	if err != nil {
		return retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if !recoverTargetMatches(sbx, p) {
		return nil
	}

	client, closer, err := w.dial(*sbx.NodeID)
	if err != nil {
		return w.recoverUnavailable(ctx, job, lease, sbx, "node gateway unavailable")
	}
	defer closer.Close()

	resetCtx, resetCancel := context.WithTimeout(opCtx, recoverResetTimeout)
	vm, err := client.ResetVM(resetCtx, &nodev1.ResetVMRequest{SandboxId: sbx.ID})
	resetCancel()

	if guard.Err() != nil {
		return retry("sandbox guard lost", w.backoff(job.Attempts))
	}
	if lease.Lost() || ctx.Err() != nil {
		return nil
	}

	if err != nil {
		if status.Code(err) == codes.Unavailable {
			return w.recoverUnavailable(ctx, job, lease, sbx, classify(err))
		}
		w.log.Warn("sandbox recover reset failed",
			"sandbox", sbx.ID, "node", *sbx.NodeID, "reason", classify(err))
		return w.recordRecoverOutcome(ctx, job, lease, owner, sandboxID, db.EventInput{
			Type: eventSandboxRecoverFailed,
			Data: recoverFailedEventData,
		})
	}

	if vm.GetSandboxId() != sbx.ID || vm.GetState() != nodev1.VMState_VM_STATE_READY {
		w.log.Warn("sandbox recover returned non-ready VM",
			"sandbox", sbx.ID, "node", *sbx.NodeID, "vm_state", vm.GetState().String())
		return w.recordRecoverOutcome(ctx, job, lease, owner, sandboxID, db.EventInput{
			Type: eventSandboxRecoverFailed,
			Data: recoverFailedEventData,
		})
	}

	return w.recordRecoverOutcome(ctx, job, lease, owner, sandboxID, db.EventInput{
		Type: eventSandboxRecovered,
	})
}

// recoverUnavailable retries a transient node/gateway failure with backoff,
// and records sandbox.recover_failed when the final attempt is exhausted.
func (w *Worker) recoverUnavailable(ctx context.Context, job db.Job, lease *Lease, sbx db.Sandbox, reason string) error {
	if job.Attempts < job.MaxAttempts {
		return retry(reason, w.backoff(job.Attempts))
	}
	w.log.Warn("sandbox recover exhausted retries",
		"sandbox", sbx.ID, "node", derefNode(sbx.NodeID), "reason", reason)
	if evErr := w.recordRecoverOutcome(ctx, job, lease, sbx.OwnerID, sbx.ID, db.EventInput{
		Type: eventSandboxRecoverFailed,
		Data: recoverFailedEventData,
	}); evErr != nil {
		return evErr
	}
	return retry(reason, w.backoff(job.Attempts))
}

// recordRecoverOutcome writes one recovery event fenced by the worker's live
// job lease. A lost claim or deleted sandbox finishes quietly without writing
// a success event; a transient database error retries.
func (w *Worker) recordRecoverOutcome(ctx context.Context, job db.Job, lease *Lease, owner uuid.UUID, sandboxID string, ev db.EventInput) error {
	if lease.Lost() || ctx.Err() != nil {
		return nil
	}
	err := w.store.RecordRecoverEvent(ctx, job.ID, w.workerID, job.LeaseToken, owner, sandboxID, ev)
	switch {
	case err == nil, errors.Is(err, db.ErrJobNotLeased), errors.Is(err, db.ErrNotFound):
		return nil
	default:
		return retry("recover event write failed", w.backoff(job.Attempts))
	}
}

// recoverTargetMatches reports whether the canonical sandbox row is still the
// same live, ready-ish boot on the node that recover was accepted for.
func recoverTargetMatches(sbx db.Sandbox, p db.RecoverPayload) bool {
	if sbx.DeletedAt != nil || !readyState(sbx.State) {
		return false
	}
	if sbx.NodeID == nil || *sbx.NodeID != p.NodeID {
		return false
	}
	return sbx.LifecycleGeneration == p.Generation
}
