package scheduler

// Resume admission. Same node (B1.2): a resume boots the retained disk on the
// node that keeps it, and never a fresh-image fallback. The scheduler refuses
// cleanly and fast when that node is unreachable, then lets
// [db.Store.AcceptSandboxResume] do the atomic admission — re-reserving
// compute on the pinned node under its row lock, superseding an in-flight stop
// and enqueueing the resume job.
//
// Another node (only with an object store): an archived
// sandbox whose disk node is full, unreachable or unknown restores the chain's
// last completed generation on a placed node through
// [db.Store.AcceptSandboxResumeElsewhere]. Without an object store
// (Options.ResumeElsewhere nil) resume stays same-node only.

import (
	"cmp"
	"context"
	"errors"
	"fmt"

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

// Resume admits one resume. It reads the sandbox for its pinned node, probes
// that node live through the gateway and then runs the atomic resume
// acceptance; with Options.ResumeElsewhere set, an archived sandbox the disk
// node cannot take is restored on another node instead (resumeArchived).
// With FailFast the whole admission is bounded by FailFastBudget, and the
// caller's own cancellation always wins over the internal deadline. An unreachable, stale or identity-mismatched node
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
	if s.elsewhere != nil && sbx.State == db.StateArchived {
		return s.resumeArchived(ctx, callerCtx, req, sbx)
	}
	return s.resumeSameNode(ctx, callerCtx, req, sbx)
}

// resumeSameNode is the same-node admission: probe the node that keeps the
// disk, then accept atomically on it.
func (s *Scheduler) resumeSameNode(ctx, callerCtx context.Context, req ResumeRequest, sbx db.Sandbox) (db.ResumeAcceptance, error) {
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
	return s.store.AcceptSandboxResume(ctx, resumeParams(req))
}

// resumeParams is the store input of req.
func resumeParams(req ResumeRequest) db.ResumeParams {
	return db.ResumeParams{
		Owner:           req.Owner,
		SandboxID:       req.SandboxID,
		ExpectedVersion: req.ExpectedVersion,
		MachineType:     req.MachineType,
		Resources:       req.Resources,
		Env:             req.Env,
		TTL:             req.TTL,
		SupersedeStopID: req.SupersedeStopID,
		MaxAttempts:     req.MaxAttempts,
	}
}

// resumeArchived admits the resume of an archived sandbox with an object
// store configured. The node in disk_node_id goes first, through the
// same-node path; when it is full, unreachable or unknown — or there is no
// local disk at all — the chain's last completed generation is restored on
// another placed node. Store errors other than capacity and node
// availability pass through from the same-node attempt unchanged.
//
// Without a generation the answer depends on the disk: a disk node that is
// only full or away keeps the same-node refusal (503, retry later), and no
// local disk at all is [db.ErrNoRestorableData]. Expired data is
// [db.ErrSnapshotDataExpired] before anything else.
func (s *Scheduler) resumeArchived(ctx, callerCtx context.Context, req ResumeRequest, sbx db.Sandbox) (db.ResumeAcceptance, error) {
	if sbx.DataExpiredAt != nil {
		return db.ResumeAcceptance{}, db.ErrSnapshotDataExpired
	}
	diskNode := derefNodeID(sbx.DiskNodeID)
	var local error // why the disk node could not take the resume
	if diskNode != "" && diskNode == derefNodeID(sbx.NodeID) {
		acc, err := s.resumeSameNode(ctx, callerCtx, req, sbx)
		if err == nil || !movableResume(err) {
			return acc, err
		}
		local = err
		s.log.Info("resume moves off the disk node", "sandbox", sbx.ID, "node", diskNode, "reason", probeReason(err))
	}
	if callerCtx.Err() != nil {
		return db.ResumeAcceptance{}, callerCtx.Err()
	}
	if ctx.Err() != nil {
		return db.ResumeAcceptance{}, errors.Join(ErrNoNode, ctx.Err())
	}
	if !sbx.SnapshotsEnabled {
		return db.ResumeAcceptance{}, cmp.Or(local, db.ErrSnapshotsDisabled)
	}

	gen, ok, err := s.elsewhere.LatestGeneration(ctx, req.Owner, req.SandboxID)
	if err != nil {
		return db.ResumeAcceptance{}, fmt.Errorf("scheduler: latest generation: %w", err)
	}
	if !ok {
		return db.ResumeAcceptance{}, cmp.Or(local, db.ErrNoRestorableData)
	}
	exclude, err := s.elsewhere.OpenEvictionNodes(ctx, req.Owner, req.SandboxID)
	if err != nil {
		return db.ResumeAcceptance{}, fmt.Errorf("scheduler: eviction nodes: %w", err)
	}
	if diskNode != "" {
		exclude = append(exclude, diskNode)
	}
	reserve := sbx.Desired
	if req.Resources != (db.Resources{}) {
		reserve = req.Resources
	}
	reserve.DiskGiB = max(reserve.DiskGiB, db.DiskGiBFor(gen.DiskBytes))
	cands, err := s.Place(ctx, PlaceRequest{
		Reserve:         reserve,
		Image:           gen.Image,
		Prefer:          []string{gen.NodeID},
		Exclude:         exclude,
		NeedObjectStore: true,
	})
	if err != nil {
		if callerCtx.Err() != nil {
			return db.ResumeAcceptance{}, callerCtx.Err()
		}
		return db.ResumeAcceptance{}, err
	}
	for _, c := range cands {
		acc, err := s.elsewhere.AcceptSandboxResumeElsewhere(ctx, db.ResumeElsewhereParams{
			ResumeParams: resumeParams(req),
			ToNode:       c.Node.ID,
			GenerationID: gen.ID,
			ManifestKey:  gen.ManifestKey,
		})
		if err == nil {
			return acc, nil
		}
		if !errors.Is(err, db.ErrNoCapacity) && !errors.Is(err, db.ErrNodeUnavailable) {
			return db.ResumeAcceptance{}, err
		}
		// Another admission took this node's last room, or its row
		// vanished: try the next candidate.
		s.log.Warn("resume reservation lost", "node", c.Node.ID, "reason", probeReason(err))
	}
	return db.ResumeAcceptance{}, ErrNoCapacity
}

// movableResume reports whether a same-node refusal lets the resume move to
// another node: the disk node is full, unreachable or gone.
func movableResume(err error) bool {
	return errors.Is(err, ErrNoNode) || errors.Is(err, db.ErrNoCapacity) || errors.Is(err, db.ErrNodeUnavailable)
}

// derefNodeID reads an optional node id.
func derefNodeID(id *string) string {
	if id == nil {
		return ""
	}
	return *id
}
