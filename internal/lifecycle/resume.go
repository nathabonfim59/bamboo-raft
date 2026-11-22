package lifecycle

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// resumeSandbox boots one retained disk back to READY. Every state change is
// fenced by the job claim and the lifecycle generation of the payload, and
// every node side effect runs under the shared per-sandbox guard. Retries
// never clean up the node: the retained disk must survive every attempt, so
// there is no partial-boot DeleteVM on this path. Only two outcomes delete
// anything: a row deleted meanwhile (the deletion owns the disk), and the
// confirmed missing disk, where DeleteVM is the fence that makes the release
// safe.
func (w *Worker) resumeSandbox(ctx context.Context, job db.Job, lease *Lease) error {
	if job.SandboxID == nil {
		return permanent("missing sandbox id")
	}
	owner, sandboxID := job.OwnerID, *job.SandboxID
	var p db.ResumePayload
	if err := json.Unmarshal([]byte(job.Payload), &p); err != nil ||
		p.Generation <= 0 || p.VMID != sandboxID || p.NodeID == "" {
		return permanent("invalid resume payload")
	}

	sbx, err := w.readResumeTarget(ctx, job, owner, sandboxID, p)
	if err != nil {
		return err
	}
	if sbx.State != db.StateProvisioning {
		return nil // decided by another delivery or superseding actor
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

	// Canonical reread under the guard.
	sbx, err = w.readResumeTarget(ctx, job, owner, sandboxID, p)
	if err != nil {
		return err
	}
	if sbx.State != db.StateProvisioning {
		return nil
	}
	if p.NodeID != derefNode(sbx.NodeID) || p.Resources != sbx.Desired || p.MachineType != sbx.MachineType {
		return permanent("job payload does not match sandbox")
	}
	if p.FromManifest != "" {
		return w.resumeElsewhere(ctx, job, lease, guard, opCtx, sbx, p)
	}

	env, err := w.store.DecryptSandboxEnvForNode(ctx, owner, sandboxID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil // deleted between the reads
		}
		return retry("env unavailable", w.backoff(job.Attempts))
	}
	client, closer, err := w.dial(p.NodeID)
	if err != nil {
		return retry("node gateway unavailable", w.backoff(job.Attempts))
	}
	defer closer.Close()

	bootCtx, bootCancel := context.WithTimeout(opCtx, w.bootTimeout)
	defer bootCancel()
	vm, err := client.StartVM(bootCtx, &nodev1.StartVMRequest{
		SandboxId: sbx.ID,
		Resources: resourcesOf(sbx.Desired),
		Env:       env,
		// Hostname stays empty: unchanged, like CreateVM's canonical default.
		Hostname: "",
	})
	bootCancel()
	if gerr := guard.Err(); gerr != nil {
		// The RPC outcome is unproven; redelivery's idempotent StartVM
		// resolves it under a fresh guard.
		return retry("sandbox guard lost", w.backoff(job.Attempts))
	}
	if lease.Lost() || ctx.Err() != nil {
		// The boot may still complete remotely; StartVM is idempotent, so
		// redelivery observes READY (or the failure) and finalizes.
		return nil
	}
	if err != nil {
		return w.resumeBootFailed(ctx, job, lease, guard, opCtx, sbx, p, err)
	}
	if vm.GetSandboxId() != sbx.ID || vm.GetState() != nodev1.VMState_VM_STATE_READY {
		return w.resumeBootFailed(ctx, job, lease, guard, opCtx, sbx, p,
			status.Error(codes.Internal, "StartVM returned a non-ready VM"))
	}

	_, err = w.finalizeResumeReady(ctx, job, guard, opCtx, p)
	return err
}

// finalizeResumeReady finalizes a resume whose VM answered READY: it verifies
// nothing superseded this generation while the RPC ran, then finalizes under
// the claim. finalized reports that this call committed the READY state.
func (w *Worker) finalizeResumeReady(ctx context.Context, job db.Job, guard *db.SandboxGuard, opCtx context.Context, p db.ResumePayload) (finalized bool, _ error) {
	owner, sandboxID := job.OwnerID, *job.SandboxID
	sbx, err := w.readResumeTarget(ctx, job, owner, sandboxID, p)
	if err != nil {
		return false, err
	}
	if sbx.State != db.StateProvisioning {
		return false, nil
	}
	if sbx.DeletedAt != nil {
		// The VM is running for a deleted row: the deletion owns everything,
		// including the disk. Best-effort cleanup under our guard; the delete
		// job repeats it idempotently.
		if err := w.deleteVMUnderGuard(opCtx, guard, sbx); err != nil {
			w.log.Warn("deleted-sandbox compensation failed", "sandbox", sbx.ID, "reason", classify(err))
		}
		return false, nil
	}
	_, err = w.store.FinalizeSandboxResume(ctx, job.ID, w.workerID, job.LeaseToken, db.FinalizeResumeParams{
		Owner: owner, SandboxID: sandboxID, Generation: p.Generation, Version: sbx.Version,
	})
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, db.ErrJobNotLeased), errors.Is(err, db.ErrNotFound):
		return false, nil
	case errors.Is(err, db.ErrConflict):
		// A racing PATCH can bump the version while the row still wants this
		// boot: retry so a fresh attempt finalizes, and only stop for an
		// authoritative newer outcome.
		decided, derr := w.resumeConflictDecided(ctx, job, owner, sandboxID, p.Generation)
		if derr != nil {
			return false, derr
		}
		if decided {
			return false, nil
		}
		return false, retry("resume finalization conflict", w.backoff(job.Attempts))
	default:
		return false, retry("resume finalization failed", w.backoff(job.Attempts))
	}
}

// readResumeTarget loads the canonical row and applies the resume fences: a
// deleted row (returning a provisioning state) means the deletion owns the
// disk, a generation mismatch is a stale attempt, and only provisioning at
// the payload's generation still belongs to this boot.
func (w *Worker) readResumeTarget(ctx context.Context, job db.Job, owner uuid.UUID, sandboxID string, p db.ResumePayload) (db.Sandbox, error) {
	sbx, err := w.store.SandboxAny(ctx, owner, sandboxID)
	if errors.Is(err, db.ErrNotFound) {
		return sbx, permanent("sandbox missing")
	}
	if err != nil {
		return sbx, retry("sandbox read failed", w.backoff(job.Attempts))
	}
	if sbx.DeletedAt != nil {
		// Report a non-provisioning state so the caller stops quietly; the
		// delete job owns VM and disk.
		sbx.State = db.StateCancelled
		return sbx, nil
	}
	if sbx.LifecycleGeneration != p.Generation {
		return sbx, permanent("stale resume generation")
	}
	switch sbx.State {
	case db.StateProvisioning:
		return sbx, nil
	case db.StateReady, db.StateIdle, db.StateRunning:
		// A duplicate delivery finalized this boot already.
		sbx.State = db.StateReady
		return sbx, nil
	default:
		return sbx, permanent("sandbox not resumable")
	}
}

// resumeConflictDecided rereads the canonical row after a fenced resume
// finalize answered [db.ErrConflict]. decided reports an authoritative newer
// outcome — deletion, a newer lifecycle generation, or a state this attempt
// no longer owns; the same generation still provisioning with only a newer
// version (a racing PATCH, or a duplicate delivery that has not committed)
// stays undecided and retries. A reread failure retries too: it proves
// nothing about the row.
func (w *Worker) resumeConflictDecided(ctx context.Context, job db.Job, owner uuid.UUID, sandboxID string, generation int64) (bool, error) {
	cur, err := w.store.SandboxAny(ctx, owner, sandboxID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return true, nil
		}
		return false, retry("sandbox reread failed", w.backoff(job.Attempts))
	}
	if cur.DeletedAt != nil || cur.LifecycleGeneration != generation {
		return true, nil
	}
	return cur.State != db.StateProvisioning, nil
}

// resumeBootFailed handles a failed StartVM. Retryable failures (transport,
// capacity, timeout) get bounded attempts with no node cleanup at all: the
// retained disk must survive every retry. Definitive failures end the resume
// cleanly through the designed finalization:
//
//   - a missing disk (StartVM and the GetVM recheck both NOT_FOUND) is fenced
//     by DeleteVM and then releases everything — there is never a CreateVM
//     fallback;
//   - a FAILED precondition means the retained VM is in FAILED state and will
//     never start: StopVM must confirm it is off with the disk kept before
//     the sandbox returns to archived with a compute-only release;
//   - any other terminal outcome runs the same StopVM-confirmed failure path.
//
// ABORTED means a DeleteVM fenced this boot: only a canonical reread may
// decide it — the deletion (or newer intent) owns the disk, while a still
// live current generation retries into the missing-disk or boot path instead
// of stranding the row.
func (w *Worker) resumeBootFailed(ctx context.Context, job db.Job, lease *Lease, guard *db.SandboxGuard, opCtx context.Context, sbx db.Sandbox, p db.ResumePayload, cause error) error {
	reason := classify(cause)
	switch status.Code(cause) {
	case codes.Aborted:
		decided, err := w.resumeConflictDecided(ctx, job, sbx.OwnerID, sbx.ID, p.Generation)
		if err != nil {
			return err
		}
		if decided {
			return nil // a deletion or newer intent owns the machine
		}
		// The row is still ours and the disk was deleted underneath it: the
		// next attempt resolves it through the confirmed missing-disk path.
		return retry("resume fenced by deletion", w.backoff(job.Attempts))
	case codes.NotFound:
		return w.resumeDiskMissing(ctx, job, lease, guard, opCtx, sbx, p)
	case codes.FailedPrecondition:
		// The retained VM is FAILED and will never start; end the resume now
		// instead of burning attempts. StopVM confirms the disk is kept
		// before anything is released.
		return w.terminateResumeFailure(ctx, job, lease, guard, opCtx, sbx, p, terminalResumeReason(cause))
	}
	if retryableResume(cause) && job.Attempts < job.MaxAttempts {
		return retry(reason, w.backoff(job.Attempts))
	}
	return w.terminateResumeFailure(ctx, job, lease, guard, opCtx, sbx, p, terminalResumeReason(cause))
}

// resumeDiskMissing ends a resume whose retained disk the node confirmed
// missing: DeleteVM fences any ambiguous earlier start, then the fenced store
// method releases every remaining reservation exactly once and fails the
// sandbox with the fixed vm_missing classification. There is never a
// CreateVM fallback that would silently boot an empty replacement.
func (w *Worker) resumeDiskMissing(ctx context.Context, job db.Job, lease *Lease, guard *db.SandboxGuard, opCtx context.Context, sbx db.Sandbox, p db.ResumePayload) error {
	// StartVM's NOT_FOUND alone is not proof: confirm with the canonical
	// probe while holding the guard.
	if _, err := w.getVM(opCtx, derefNode(sbx.NodeID), sbx.ID); err == nil {
		// The target reappeared (a raced boot finished); retry the resume.
		return retry("resume target reappeared", w.backoff(job.Attempts))
	} else if status.Code(err) != codes.NotFound {
		return retry(classify(err), w.backoff(job.Attempts))
	}
	// DeleteVM fences earlier starts before anything is released.
	if err := w.deleteVMUnderGuard(opCtx, guard, sbx); err != nil {
		return retry(classify(err), w.backoff(job.Attempts))
	}
	if guard.Err() != nil || opCtx.Err() != nil {
		return retry("sandbox guard lost", w.backoff(job.Attempts))
	}
	if lease.Lost() || ctx.Err() != nil {
		return nil // redelivery replays the fenced outcome idempotently
	}
	_, err := w.store.FinalizeSandboxResumeMissingDisk(ctx, job.ID, w.workerID, job.LeaseToken,
		db.FinalizeResumeFailureParams{
			Owner: sbx.OwnerID, SandboxID: sbx.ID, Generation: p.Generation, Version: sbx.Version,
		})
	switch {
	case err == nil:
		return permanent(missingDiskResume)
	case errors.Is(err, db.ErrJobNotLeased), errors.Is(err, db.ErrNotFound):
		return nil
	case errors.Is(err, db.ErrConflict):
		decided, derr := w.resumeConflictDecided(ctx, job, sbx.OwnerID, sbx.ID, p.Generation)
		if derr != nil {
			return derr
		}
		if decided {
			return nil // a newer intent or the persisted outcome owns the row
		}
		return retry("missing-disk finalization conflict", w.backoff(job.Attempts))
	default:
		return retry("missing-disk finalization failed", w.backoff(job.Attempts))
	}
}

// terminateResumeFailure ends a failed resume cleanly: StopVM confirms the VM
// is off with its disk retained (retrying briefly while an ambiguous boot
// resolves), and only then does the fenced store method return the sandbox to
// archived with a safe error and a compute-only release. The initial-create
// cleanup path never runs here: DeleteVM would destroy the retained disk.
func (w *Worker) terminateResumeFailure(ctx context.Context, job db.Job, lease *Lease, guard *db.SandboxGuard, opCtx context.Context, sbx db.Sandbox, p db.ResumePayload, reason string) error {
	// StopVM, never DeleteVM: the disk is kept. A VM still BOOTING (an
	// abandoned StartVM) is refused and waited out inside the same guard.
	if err := w.stopVMConfirmed(opCtx, derefNode(sbx.NodeID), sbx.ID, 0); err != nil {
		return retry(classify(err), w.backoff(job.Attempts))
	}
	if guard.Err() != nil || opCtx.Err() != nil {
		return retry("sandbox guard lost", w.backoff(job.Attempts))
	}
	if lease.Lost() || ctx.Err() != nil {
		return nil // redelivery replays idempotently
	}
	_, err := w.store.FinalizeSandboxResumeFailure(ctx, job.ID, w.workerID, job.LeaseToken,
		db.FinalizeResumeFailureParams{
			Owner: sbx.OwnerID, SandboxID: sbx.ID, Generation: p.Generation, Version: sbx.Version,
		}, reason)
	switch {
	case err == nil:
		return permanent(reason)
	case errors.Is(err, db.ErrJobNotLeased), errors.Is(err, db.ErrNotFound):
		return nil
	case errors.Is(err, db.ErrConflict):
		// A newer intent (a stop accepted while we stopped the VM, a resume
		// generation bump) decides; a racing PATCH under the same generation
		// retries with the fresh version.
		decided, derr := w.resumeConflictDecided(ctx, job, sbx.OwnerID, sbx.ID, p.Generation)
		if derr != nil {
			return derr
		}
		if decided {
			return nil
		}
		return retry("resume failure finalization conflict", w.backoff(job.Attempts))
	default:
		return retry("resume failure finalization failed", w.backoff(job.Attempts))
	}
}

// retryableResume reports whether another StartVM attempt can plausibly
// succeed. The retained disk is never cleaned up between attempts, so only
// transport, capacity and boot-time failures retry; definitive node verdicts
// go to the designed failure paths.
func retryableResume(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.Unauthenticated,
		codes.PermissionDenied, codes.Aborted, codes.FailedPrecondition:
		return false
	default:
		return true
	}
}

// terminalResumeReason maps a terminal resume failure to a fixed, safe reason
// for the archived row. It is the only reason text this path writes.
func terminalResumeReason(err error) string {
	switch status.Code(err) {
	case codes.ResourceExhausted:
		return "out_of_capacity"
	default:
		return "boot_failed"
	}
}

// missingDiskResume is the fixed safe reason a missing-disk resume fails with.
const missingDiskResume = "vm_missing"

// derefStopID reads an optional stop operation id.
func derefStopID(s *string) string {
	return cmp.Or(derefNode(s), "")
}
