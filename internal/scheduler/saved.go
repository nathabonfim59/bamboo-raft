package scheduler

// Create-from a saved snapshot for a batch (the v1 create `count` member).
// The single-create path resolves the name inside [Scheduler.Admit]; a batch
// needs the same replay-before-resolution order, so this file owns the small
// amount of admission the batch entry point (AdmitBatch) does not: resolution after
// the keyed replay, then either the generation path (any node) or the legacy
// node-local path (one pinned node, one atomic transaction).

import (
	"context"
	"errors"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// AdmitBatchFrom admits a batch of creates from one saved-snapshot name or
// share code. The keyed replay is checked first, so a duplicate answers the
// original batch even after the name was deleted or replaced. Every request
// must carry the same owner and From, no Restore, no Source, and a unique id;
// only the first request's idempotency key and request hash are used.
//
// A generation-backed source places the batch on any nodes with the object
// store and the manifest's image, spread round-robin (Scheduler.AdmitBatch).
// A legacy node-local source keeps its pinned-node rule: every sandbox is
// created on the artifact's node in one transaction, so the batch is
// all-or-nothing there too.
func (s *Scheduler) AdmitBatchFrom(ctx context.Context, reqs []AdmitRequest) ([]db.CreateSandboxResult, error) {
	if len(reqs) == 0 {
		return nil, errors.New("scheduler: batch admission needs at least one request")
	}
	reqs = append([]AdmitRequest(nil), reqs...)
	first := reqs[0]
	if first.Owner == uuid.Nil() {
		return nil, db.ErrNoOwner
	}
	for i, r := range reqs {
		switch {
		case r.Owner != first.Owner:
			return nil, errors.New("scheduler: a batch belongs to one owner")
		case r.From == "" || r.From != first.From:
			return nil, errors.New("scheduler: a batch restores one saved snapshot")
		case r.Restore != nil || r.Source != nil:
			return nil, errors.New("scheduler: a batch resolves its own source")
		case r.ID == "":
			return nil, errors.New("scheduler: admit needs a sandbox id")
		case i > 0 && r.IdempotencyKey != "":
			return nil, errors.New("scheduler: only the first request of a batch carries the idempotency key")
		}
	}
	if res, found, err := s.replayBatch(ctx, first); err != nil || found {
		return res, err
	}
	if s.restore == nil {
		return nil, errors.New("scheduler: create-from needs a restore resolver")
	}
	source, machine, reserve, err := s.restore.ResolveRestore(ctx, first.Owner, first.From,
		first.TypeExplicit, first.MachineType)
	if err != nil {
		// A duplicate may have committed while this request resolved a
		// deleted or replaced name.
		if res, found, rerr := s.replayBatch(ctx, first); rerr == nil && found {
			return res, nil
		}
		return nil, err
	}
	for i := range reqs {
		reqs[i].MachineType = machine
		reqs[i].Reserve = reserve
		reqs[i].ShareID = source.ShareID
		if reqs[i].Name == "" {
			reqs[i].Name = source.Name
		}
	}
	if source.GenerationBacked() {
		gen := generationSourceFromSnapshot(source)
		for i := range reqs {
			reqs[i].Source = gen
			reqs[i].From = ""
		}
		res, err := s.AdmitBatch(ctx, reqs)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	return s.admitLegacyBatch(ctx, reqs, source)
}

// admitLegacyBatch creates a batch from one node-local artifact on the node
// that holds it. Node capacity is reserved per sandbox inside one database
// transaction, so a batch that does not fit creates nothing.
func (s *Scheduler) admitLegacyBatch(ctx context.Context, reqs []AdmitRequest, source db.SnapshotSource) ([]db.CreateSandboxResult, error) {
	if source.NodeID == "" {
		return nil, db.ErrSnapshotNotFound
	}
	if _, err := s.probe(ctx, source.NodeID); err != nil {
		if ctx.Err() != nil {
			return nil, errors.Join(ErrNoNode, ctx.Err())
		}
		s.log.Warn("restore probe failed", "node", source.NodeID, "reason", probeReason(err))
		return nil, ErrNoNode
	}
	params := make([]db.CreateSandboxParams, len(reqs))
	for i, r := range reqs {
		reserve := r.Reserve
		reserve.DiskGiB = max(reserve.DiskGiB, source.DiskGiB)
		params[i] = db.CreateSandboxParams{
			ID:               r.ID,
			Owner:            r.Owner,
			MachineType:      r.MachineType,
			Name:             r.Name,
			Labels:           r.Labels,
			Env:              r.Env,
			ExpiresAt:        r.ExpiresAt,
			TTLSeconds:       r.TTLSeconds,
			NodeID:           source.NodeID,
			Reserve:          reserve,
			MaxAttempts:      r.MaxAttempts,
			IdempotencyKey:   r.IdempotencyKey,
			RequestSHA256:    r.RequestSHA256,
			Restore:          &source,
			SnapshotsEnabled: r.SnapshotsEnabled,
			ShareID:          r.ShareID,
		}
	}
	res, err := s.store.CreateSandboxes(ctx, params)
	switch {
	case err == nil:
		return res, nil
	case errors.Is(err, db.ErrNoCapacity), errors.Is(err, db.ErrNotFound):
		if res, found, rerr := s.replayBatch(ctx, reqs[0]); rerr == nil && found {
			return res, nil
		}
		return nil, ErrNoCapacity
	default:
		return nil, err
	}
}

// generationSourceFromSnapshot adapts a generation-backed saved-snapshot
// source to the placement input. The source's node is only the locality
// preference.
func generationSourceFromSnapshot(source db.SnapshotSource) *GenerationSource {
	if source.GenerationID == nil {
		return nil
	}
	gen := &GenerationSource{
		GenerationID: *source.GenerationID,
		ManifestKey:  source.ManifestKey,
		Image:        source.Image,
		DiskBytes:    source.DiskBytes,
	}
	if source.NodeID != "" {
		gen.Prefer = []string{source.NodeID}
	}
	return gen
}
