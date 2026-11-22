package server

// The fork admission adapter: the API fork seam places the fork's node through
// the scheduler and writes the durable admission through the store. It is the
// only bridge between the two, so the API never sees a node, hub or gateway.

import (
	"context"
	"errors"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/scheduler"
)

// forkPlacer is the scheduler's placement surface. *scheduler.Scheduler
// implements it.
type forkPlacer interface {
	Place(ctx context.Context, req scheduler.PlaceRequest) ([]scheduler.Candidate, error)
}

// forkStore is the store surface a fork admission needs: the source node's
// default image, the keyed replay and the durable admission transaction.
type forkStore interface {
	Node(ctx context.Context, id string) (db.Node, error)
	ReplayFork(ctx context.Context, owner uuid.UUID, key string, requestSHA256 []byte) (db.CreateSandboxResult, bool, error)
	AdmitFork(ctx context.Context, p db.ForkAdmitParams) (db.CreateSandboxResult, error)
}

// forkStoreContract keeps the accepted database facade inside the adapter.
var _ forkStore = (*db.Store)(nil)

// apiForkAdmission wires the API fork seam to ranked placement and the store.
// It passes only the validated request: no node, hub or gateway handle crosses
// into the API layer.
type apiForkAdmission struct {
	placer forkPlacer
	store  forkStore
}

// Fork implements [api.ForkAdmission]. Placement prefers the node the source
// runs on (or that wrote its latest generation), keeps only nodes with the
// source image and a usable object store, and never shrinks the disk below the
// source's. A node that loses the reservation race is skipped for the next
// ranked candidate, like a create.
func (a apiForkAdmission) Fork(ctx context.Context, request api.ForkRequest) (api.ForkResult, error) {
	callerCtx := ctx
	if request.FailFast {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, scheduler.DefaultFailFastBudget)
		defer cancel()
	}
	// A duplicate is answered before any placement, so a retry after a lost
	// response never competes for capacity again.
	if result, found, err := a.replay(ctx, request); err != nil {
		if callerCtx.Err() != nil {
			return api.ForkResult{}, callerCtx.Err()
		}
		return api.ForkResult{}, err
	} else if found {
		return result, nil
	}
	image, prefer := forkLocality(ctx, a.store, request)
	reserve := request.Resources
	if request.Latest != nil {
		reserve.DiskGiB = max(reserve.DiskGiB, db.DiskGiBFor(request.Latest.DiskBytes))
	}
	cands, err := a.placer.Place(ctx, scheduler.PlaceRequest{
		Reserve:         reserve,
		Image:           image,
		Prefer:          prefer,
		NeedObjectStore: true,
	})
	if err != nil {
		if callerCtx.Err() != nil {
			return api.ForkResult{}, callerCtx.Err()
		}
		// Another duplicate may have committed while placement ran.
		if result, found, rerr := a.replay(callerCtx, request); rerr == nil && found {
			return result, nil
		}
		return forkPlacementResult(err)
	}
	var sourceGeneration *uuid.UUID
	if request.Source.State == db.StateArchived && request.Latest != nil {
		// The fork restores exactly the generation the client was answered
		// for: its chain takes no new generation.
		sourceGeneration = new(request.Latest.ID)
	}
	for _, cand := range cands {
		result, err := a.store.AdmitFork(ctx, db.ForkAdmitParams{
			ID:                 request.SandboxID,
			Owner:              request.Owner,
			SourceID:           request.SourceID,
			MachineType:        request.MachineType,
			Name:               forkDisplayName(request.Source),
			Labels:             request.Labels,
			Env:                request.Env,
			InheritEnv:         request.InheritEnv,
			TTLSeconds:         request.TTLSeconds,
			NodeID:             cand.Node.ID,
			Reserve:            reserve,
			IdempotencyKey:     request.IdempotencyKey,
			RequestSHA256:      request.RequestSHA256[:],
			SourceGenerationID: sourceGeneration,
		})
		switch {
		case err == nil:
			return api.ForkResult{Sandbox: result.Sandbox, Replayed: result.Replayed}, nil
		case errors.Is(err, db.ErrNoCapacity), errors.Is(err, db.ErrNodeGone):
			// Another admission took this node's last slot (or its row
			// vanished): try the next ranked candidate.
			continue
		default:
			return api.ForkResult{}, err
		}
	}
	if result, found, err := a.replay(callerCtx, request); err == nil && found {
		return result, nil
	}
	return forkPlacementResult(scheduler.ErrNoCapacity)
}

// replay is the keyed fast path; no key means none.
func (a apiForkAdmission) replay(ctx context.Context, request api.ForkRequest) (api.ForkResult, bool, error) {
	if request.IdempotencyKey == "" {
		return api.ForkResult{}, false, nil
	}
	result, found, err := a.store.ReplayFork(ctx, request.Owner, request.IdempotencyKey, request.RequestSHA256[:])
	if err != nil || !found {
		return api.ForkResult{}, false, err
	}
	return api.ForkResult{Sandbox: result.Sandbox, Replayed: true}, true, nil
}

// forkDisplayName is the fork's generated display name: the source's name plus
// a suffix, so a list shows where the copy came from. An empty source name
// (never written by admission) falls back to the pinned default shape.
func forkDisplayName(source db.Sandbox) string {
	if source.Name == "" {
		return "Sandbox fork"
	}
	return source.Name + " fork"
}

// forkLocality resolves the placement hint and image filter of one fork. The
// source node comes first, then the node that wrote the latest generation. The
// image is the latest generation's; a source with no generation yet falls back
// to its node's inventory default. A failed inventory read only relaxes the
// filter: the provision job's manifest restore re-places a node without the
// image.
func forkLocality(ctx context.Context, store forkStore, request api.ForkRequest) (string, []string) {
	var prefer []string
	if sourceNode := derefString(request.Source.NodeID); sourceNode != "" {
		prefer = append(prefer, sourceNode)
	}
	image := ""
	if request.Latest != nil {
		image = request.Latest.Image
		if request.Latest.NodeID != "" && request.Latest.NodeID != derefString(request.Source.NodeID) {
			prefer = append(prefer, request.Latest.NodeID)
		}
	}
	if image == "" {
		if sourceNode := derefString(request.Source.NodeID); sourceNode != "" {
			if node, err := store.Node(ctx, sourceNode); err == nil {
				image = node.DefaultImage
			}
		}
	}
	return image, prefer
}

// forkPlacementResult maps a scheduler placement refusal to the API's pinned
// sentinels, keeping the scheduler classification alongside them so wrapped
// and joined forms keep errors.Is working for both.
func forkPlacementResult(err error) (api.ForkResult, error) {
	switch {
	case errors.Is(err, scheduler.ErrNoCapacity):
		return api.ForkResult{}, errors.Join(db.ErrNoCapacity, err)
	case errors.Is(err, scheduler.ErrNoNode):
		return api.ForkResult{}, errors.Join(api.ErrNoReadyMachine, err)
	default:
		return api.ForkResult{}, err
	}
}

// ForkAdmissionContract keeps the adapter inside the API seam.
var _ api.ForkAdmission = apiForkAdmission{}
