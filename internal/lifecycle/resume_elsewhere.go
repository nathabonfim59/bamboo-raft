package lifecycle

// Resume on another node. The admission moved the sandbox to
// a new node with no local disk; this job restores the chain's last completed
// generation there with CreateVM from_manifest. CreateVM is idempotent per
// sandbox id on a node, so a redelivery after a lost lease gets the same VM
// back and never boots a second one. After READY the disk is local on the new
// node (FinalizeSandboxResume records it with the hydrated event) and the old
// node's copy is deleted, or left to VM reconciliation as an open eviction
// when that node is away.
//
// Every failure path deletes whatever the new node holds before it releases
// anything: that node has nothing the object store does not, and DeleteVM is
// the fence against an earlier ambiguous CreateVM.

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// resumeElsewhere runs one attempt of a cross-node resume. The caller holds
// the guard and has reread the row: provisioning at the payload's generation,
// on the payload's node.
func (w *Worker) resumeElsewhere(ctx context.Context, job db.Job, lease *Lease, guard *db.SandboxGuard, opCtx context.Context, sbx db.Sandbox, p db.ResumePayload) error {
	env, err := w.store.DecryptSandboxEnvForNode(ctx, sbx.OwnerID, sbx.ID)
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
	vm, err := client.CreateVM(bootCtx, &nodev1.CreateVMRequest{
		SandboxId:    sbx.ID,
		Resources:    resourcesOf(sbx.Desired),
		Env:          env,
		FromManifest: p.FromManifest,
	})
	bootCancel()
	if guard.Err() != nil {
		// The RPC outcome is unproven; redelivery's idempotent CreateVM
		// resolves it under a fresh guard.
		return retry("sandbox guard lost", w.backoff(job.Attempts))
	}
	if lease.Lost() || ctx.Err() != nil {
		return nil // redelivery's CreateVM returns this same VM
	}
	if err != nil {
		return w.resumeElsewhereFailed(ctx, job, lease, guard, opCtx, sbx, p, client, err)
	}
	if vm.GetSandboxId() != sbx.ID || vm.GetState() != nodev1.VMState_VM_STATE_READY {
		// CreateVM returns an existing VM unchanged: a FAILED (or otherwise
		// not ready) copy on this node would come back forever. The node
		// holds nothing the object store does not, so delete it and let the
		// next attempt restore again.
		if err := w.deleteVMUnderGuard(opCtx, guard, sbx); err != nil {
			return retry(classify(err), w.backoff(job.Attempts))
		}
		if job.Attempts < job.MaxAttempts {
			return retry("restored VM not ready", w.backoff(job.Attempts))
		}
		return w.terminateResumeElsewhere(ctx, job, lease, guard, opCtx, sbx, p,
			status.Error(codes.Internal, "CreateVM returned a non-ready VM"))
	}

	finalized, err := w.finalizeResumeReady(ctx, job, guard, opCtx, p)
	if err != nil || !finalized {
		return err
	}
	w.markNodeObjectStoreOK(ctx, p.NodeID)
	w.evictCopy(ctx, p.EvictNode, sbx.ID)
	return nil
}

// resumeElsewhereFailed handles a failed CreateVM from_manifest:
//
//   - FAILED_PRECONDITION: the node cannot read the object store (placement
//     already required the image). It is marked and the resume moves to
//     another node.
//   - NOT_FOUND: the manifest is gone; INVALID_ARGUMENT: the node refused the
//     restore. Both end the attempt (snapshot_not_found, restore_rejected).
//   - ABORTED: a DeleteVM fenced the boot; only a canonical reread decides.
//   - anything else retries within the job's attempts, then ends it.
func (w *Worker) resumeElsewhereFailed(ctx context.Context, job db.Job, lease *Lease, guard *db.SandboxGuard, opCtx context.Context, sbx db.Sandbox, p db.ResumePayload, client NodeClient, cause error) error {
	switch status.Code(cause) {
	case codes.Aborted:
		decided, err := w.resumeConflictDecided(ctx, job, sbx.OwnerID, sbx.ID, p.Generation)
		if err != nil {
			return err
		}
		if decided {
			return nil
		}
		return retry("resume fenced by deletion", w.backoff(job.Attempts))
	case codes.FailedPrecondition:
		return w.moveResume(ctx, job, lease, guard, opCtx, sbx, p, client)
	case codes.NotFound, codes.InvalidArgument:
		return w.terminateResumeElsewhere(ctx, job, lease, guard, opCtx, sbx, p, cause)
	}
	if retryableResume(cause) && job.Attempts < job.MaxAttempts {
		return retry(classify(cause), w.backoff(job.Attempts))
	}
	return w.terminateResumeElsewhere(ctx, job, lease, guard, opCtx, sbx, p, cause)
}

// moveResume marks the payload's node as lacking an object store (only when
// its live inventory lists the image, like the provision path), fences it
// with DeleteVM and moves the resume to the best other node (never the node
// it left, nor one holding an evicted copy). Without another node the resume
// ends with no_ready_machine; the next resume request then answers 503 while
// the node stays marked.
func (w *Worker) moveResume(ctx context.Context, job db.Job, lease *Lease, guard *db.SandboxGuard, opCtx context.Context, sbx db.Sandbox, p db.ResumePayload, client NodeClient) error {
	if w.nodeHasImage(opCtx, client, p.Image) {
		w.log.Warn("node cannot restore from the object store; resume moves to another node",
			"node", p.NodeID, "sandbox", sbx.ID)
		if err := w.store.MarkNodeObjectStore(ctx, p.NodeID, false); err != nil {
			w.log.Warn("node object store mark failed", "node", p.NodeID, "reason", classify(err))
		}
	}
	if err := w.deleteVMUnderGuard(opCtx, guard, sbx); err != nil {
		return retry(classify(err), w.backoff(job.Attempts))
	}
	if guard.Err() != nil || opCtx.Err() != nil {
		return retry("sandbox guard lost", w.backoff(job.Attempts))
	}
	if lease.Lost() || ctx.Err() != nil {
		return nil
	}
	if w.placer == nil {
		return w.terminateResumeElsewhere(ctx, job, lease, guard, opCtx, sbx, p, errNoReadyMachine)
	}
	exclude, err := w.store.OpenEvictionNodes(ctx, sbx.OwnerID, sbx.ID)
	if err != nil {
		return retry("eviction nodes unavailable", w.backoff(job.Attempts))
	}
	exclude = append(exclude, p.NodeID)
	if p.EvictNode != "" {
		exclude = append(exclude, p.EvictNode)
	}
	cands, err := w.placer.Place(opCtx, scheduler.PlaceRequest{
		Reserve:         sbx.Reserved,
		Image:           p.Image,
		Exclude:         exclude,
		NeedObjectStore: true,
	})
	switch {
	case errors.Is(err, scheduler.ErrNoCapacity), errors.Is(err, scheduler.ErrNoNode):
		return w.terminateResumeElsewhere(ctx, job, lease, guard, opCtx, sbx, p, errNoReadyMachine)
	case err != nil:
		return retry("placement failed", w.backoff(job.Attempts))
	}
	for _, c := range cands {
		_, _, err := w.store.MoveResume(ctx, job.ID, w.workerID, job.LeaseToken, db.MoveResumeParams{
			Owner:      sbx.OwnerID,
			SandboxID:  sbx.ID,
			Generation: p.Generation,
			Version:    sbx.Version,
			FromNode:   p.NodeID,
			ToNode:     c.Node.ID,
		})
		switch {
		case err == nil, errors.Is(err, db.ErrJobNotLeased):
			return nil // the new job owns the boot (or a redelivery does)
		case errors.Is(err, db.ErrNoCapacity), errors.Is(err, db.ErrNotFound):
			continue // the node filled up or vanished: next candidate
		case errors.Is(err, db.ErrConflict):
			decided, derr := w.resumeConflictDecided(ctx, job, sbx.OwnerID, sbx.ID, p.Generation)
			if derr != nil {
				return derr
			}
			if decided {
				return nil
			}
			return retry("resume move conflict", w.backoff(job.Attempts))
		default:
			return retry("resume move failed", w.backoff(job.Attempts))
		}
	}
	return w.terminateResumeElsewhere(ctx, job, lease, guard, opCtx, sbx, p, errNoReadyMachine)
}

// terminateResumeElsewhere ends a cross-node resume: DeleteVM on the new node
// fences any boot there, then the fenced store method returns the sandbox to
// archived with no local disk and every reservation released, so the next
// resume restores the chain again. The reason is the manifest restore's
// fixed classification of cause (bootFailureReason): snapshot_not_found,
// restore_rejected, no_ready_machine, out_of_capacity or boot_failed.
func (w *Worker) terminateResumeElsewhere(ctx context.Context, job db.Job, lease *Lease, guard *db.SandboxGuard, opCtx context.Context, sbx db.Sandbox, p db.ResumePayload, cause error) error {
	restore := sbx
	restore.FromManifestKey = p.FromManifest // classify as a manifest restore
	reason := bootFailureReason(restore, cause)
	if err := w.deleteVMUnderGuard(opCtx, guard, sbx); err != nil {
		return retry(classify(err), w.backoff(job.Attempts))
	}
	if guard.Err() != nil || opCtx.Err() != nil {
		return retry("sandbox guard lost", w.backoff(job.Attempts))
	}
	if lease.Lost() || ctx.Err() != nil {
		return nil // redelivery replays idempotently
	}
	_, err := w.store.FinalizeSandboxResumeElsewhereFailure(ctx, job.ID, w.workerID, job.LeaseToken,
		db.FinalizeResumeFailureParams{
			Owner: sbx.OwnerID, SandboxID: sbx.ID, Generation: p.Generation, Version: sbx.Version,
		}, reason)
	switch {
	case err == nil:
		return permanent(reason)
	case errors.Is(err, db.ErrJobNotLeased), errors.Is(err, db.ErrNotFound):
		return nil
	case errors.Is(err, db.ErrConflict):
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

// evictCopy deletes nodeID's evicted copy of sandboxID and closes its
// eviction. A node that is away keeps the row open: VM reconciliation deletes
// the copy when it comes back.
func (w *Worker) evictCopy(ctx context.Context, nodeID, sandboxID string) {
	if nodeID == "" {
		return
	}
	if err := w.deleteVMOnNode(ctx, nodeID, sandboxID); err != nil {
		w.log.Info("evicted VM left for reconciliation", "node", nodeID, "sandbox", sandboxID, "reason", classify(err))
		return
	}
	if err := w.store.MarkEvictionDone(ctx, nodeID, sandboxID); err != nil {
		w.log.Warn("eviction close failed", "node", nodeID, "sandbox", sandboxID, "reason", classify(err))
	}
}

// reconcileEvictions deletes nodeID's evicted VM copies before anything else
// reconciles that node, and returns the sandbox ids they belong to: the VM
// pass skips them, so an evicted copy is never reported unknown nor treated
// as a sandbox's VM. ok is false when the evictions could not be read; the
// caller then leaves the node for the next round.
func (w *Worker) reconcileEvictions(ctx context.Context, nodeID string) (evicted map[string]bool, ok bool) {
	evs, err := w.store.OpenEvictions(ctx, nodeID)
	if err != nil {
		w.log.Warn("VM reconciliation eviction list failed", "node", nodeID, "reason", classify(err))
		return nil, false
	}
	evicted = make(map[string]bool, len(evs))
	for _, e := range evs {
		if ctx.Err() != nil {
			return evicted, false
		}
		if e.Current {
			// Admission never points a sandbox at a node holding its evicted
			// copy; if it happened anyway, the copy is the live VM and the
			// normal pass reconciles it.
			w.log.Warn("evicted VM is the sandbox's current VM; kept", "node", nodeID, "sandbox", e.SandboxID)
			continue
		}
		evicted[e.SandboxID] = true
		if err := w.deleteVMOnNode(ctx, nodeID, e.SandboxID); err != nil {
			w.log.Warn("evicted VM cleanup pending", "node", nodeID, "sandbox", e.SandboxID, "reason", classify(err))
			continue
		}
		if err := w.store.MarkEvictionDone(ctx, nodeID, e.SandboxID); err != nil {
			w.log.Warn("eviction close failed", "node", nodeID, "sandbox", e.SandboxID, "reason", classify(err))
		}
	}
	return evicted, true
}
