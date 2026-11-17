package scheduler

// Same-node resume admission (B1.2). A resume boots the retained disk on the
// node that keeps it: the node is pinned, there is no fallback node and never
// a fresh-image fallback. The scheduler's job is to refuse cleanly and fast
// when that node is unreachable, then let [db.Store.AcceptSandboxResume] do
// the atomic admission — re-reserving compute on the pinned node under its
// row lock, superseding an in-flight stop and enqueueing the resume job.

import (
	"context"
	"errors"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// ResumeRequest is one owner-scoped resume of a stopped (or stopping) sandbox,
// already validated by the API layer. The fields pass through to
// [db.ResumeParams]: Resources and MachineType are either both empty (keep
// the stored shape) or both set (a new shape); SupersedeStopID must name the
// active stop when the sandbox is still archiving. ExpectedVersion and
// MaxAttempts may stay zero.
type ResumeRequest struct {
	Owner           uuid.UUID
	SandboxID       string
	ExpectedVersion int64
	MachineType     string
	Resources       db.Resources
	Env             db.EnvChange
	TTL             db.TTLOption
	SupersedeStopID string
	MaxAttempts     int
	FailFast        bool
}

// Resume admits one same-node resume. It reads the sandbox for its pinned
// node, probes that node live through the gateway and then runs the atomic
// resume acceptance. With FailFast the whole admission is bounded by
// FailFastBudget, and the caller's own cancellation always wins over the
// internal deadline. An unreachable, stale or identity-mismatched node
// answers [ErrNoNode]; every store error — capacity, state, machine shape,
// conflict, a missing or foreign sandbox — passes through unchanged for the
// API to map, never relabelled as a connectivity refusal.
func (s *Scheduler) Resume(ctx context.Context, req ResumeRequest) (db.ResumeAcceptance, error) {
	if req.Owner == uuid.Nil() {
		return db.ResumeAcceptance{}, db.ErrNoOwner
	}
	if req.SandboxID == "" {
		return db.ResumeAcceptance{}, errors.New("scheduler: resume needs a sandbox id")
	}
	callerCtx := ctx
	if req.FailFast {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.failFastBudget)
		defer cancel()
	}

	sbx, err := s.store.Sandbox(ctx, req.Owner, req.SandboxID)
	if err != nil {
		// A missing or foreign sandbox is the store's answer, and a caller
		// cancellation is returned as such — neither is a node verdict.
		return db.ResumeAcceptance{}, err
	}
	if sbx.NodeID == nil || *sbx.NodeID == "" {
		return db.ResumeAcceptance{}, db.ErrNodeUnavailable
	}
	nodeID := *sbx.NodeID
	if _, err := s.probe(ctx, nodeID); err != nil {
		s.log.Warn("resume probe failed", "node", nodeID, "reason", probeReason(err))
		switch {
		case callerCtx.Err() != nil:
			// The caller's own cancellation or deadline wins.
			return db.ResumeAcceptance{}, callerCtx.Err()
		case ctx.Err() != nil:
			// The internal failFast budget ended while the caller still
			// waits: the same admission refusal a create gets.
			return db.ResumeAcceptance{}, errors.Join(ErrNoNode, ctx.Err())
		default:
			return db.ResumeAcceptance{}, ErrNoNode
		}
	}
	return s.store.AcceptSandboxResume(ctx, db.ResumeParams{
		Owner:           req.Owner,
		SandboxID:       req.SandboxID,
		ExpectedVersion: req.ExpectedVersion,
		MachineType:     req.MachineType,
		Resources:       req.Resources,
		Env:             req.Env,
		TTL:             req.TTL,
		SupersedeStopID: req.SupersedeStopID,
		MaxAttempts:     req.MaxAttempts,
	})
}
