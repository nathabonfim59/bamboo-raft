package scheduler

// Multi-node placement: ranked candidates, creates from an
// object-store generation and batch admission.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// PlaceRequest describes the node a sandbox needs.
type PlaceRequest struct {
	// Reserve is the reservation the sandbox takes on the node.
	Reserve db.Resources
	// Image, when not empty, keeps only nodes whose live Info lists it (a
	// manifest restore runs on the manifest's image).
	Image string
	// Prefer lists node ids to rank first, in this order (locality: the
	// node holding the disk or that wrote the source generation).
	Prefer []string
	// Exclude lists node ids never to return.
	Exclude []string
	// NeedObjectStore skips nodes marked without a usable object store
	// ([db.Node.ObjectStoreOK] false); set for manifest restores.
	NeedObjectStore bool
}

// Candidate is one node that passed placement: fresh, connected, with room
// and the requested image. Node is the inventory row read for this placement
// (its Free capacity is what ranking used); Info is the live probe answer.
type Candidate struct {
	Node db.Node
	Info *nodev1.InfoResponse
}

// Place lists the nodes that can take req, best first. Candidates are fresh
// inventory rows with capacity for req.Reserve, minus req.Exclude and, with
// NeedObjectStore, minus nodes marked without an object store; each is
// probed live through the gateway (concurrently, each probe bounded by the
// probe timeout and all of them by ctx, so a failFast deadline set by the
// caller holds) and must still fit and, with req.Image, list that image.
// Under a caller deadline shorter than the probe timeout (failFast), once a
// node qualifies and every preferred node answered, slower probes get a
// short grace and are then cut off, so one hung node cannot spend the
// caller's budget; otherwise Place waits for every probe.
// Ranking: nodes in req.Prefer first in that order, then the most free
// memory, then the most free vCPUs, then node id.
//
// No fresh node with room answers [ErrNoCapacity]. Nodes with room that none
// passed the probe, the image or the object-store filter answer [ErrNoNode]
// (joined with ctx's error when its deadline cut the probes); probed nodes
// that are all full answer [ErrNoCapacity]. The reservation itself is made
// later by the caller's transaction, which re-verifies capacity.
func (s *Scheduler) Place(ctx context.Context, req PlaceRequest) ([]Candidate, error) {
	nodes, err := s.store.NodesWithCapacity(ctx, req.Reserve, s.freshWithin)
	if err != nil {
		return nil, fmt.Errorf("scheduler: list nodes: %w", err)
	}
	nodes = slices.DeleteFunc(nodes, func(n db.Node) bool { return slices.Contains(req.Exclude, n.ID) })
	if len(nodes) == 0 {
		return nil, ErrNoCapacity
	}

	infos := s.probeAll(ctx, nodes, req.NeedObjectStore, req.Prefer, func(info *nodev1.InfoResponse) bool {
		return (req.Image == "" || hasImage(info, req.Image)) && fits(info, req.Reserve)
	})

	var (
		out    []Candidate
		probed int
	)
	for i, info := range infos {
		if info == nil {
			continue
		}
		node := nodes[i]
		if req.Image != "" && !hasImage(info, req.Image) {
			s.log.Debug("placement skipped node", "node", node.ID, "reason", "image missing")
			continue
		}
		probed++
		if !fits(info, req.Reserve) {
			s.logNoRoom("placement", node.ID, capacityShortfalls(info, req.Reserve))
			continue
		}
		out = append(out, Candidate{Node: node, Info: info})
	}
	if len(out) > 0 {
		slices.SortFunc(out, func(a, b Candidate) int { return compareCandidates(req.Prefer, a, b) })
		return out, nil
	}
	if probed == 0 {
		if ctx.Err() != nil {
			return nil, errors.Join(ErrNoNode, ctx.Err())
		}
		return nil, ErrNoNode
	}
	return nil, ErrNoCapacity
}

// placementRankGrace is how long Place keeps waiting for the remaining
// probes, under a short caller deadline, once a usable node and every
// preferred node answered: long enough for every healthy node on the
// internal network, short enough that one hung node cannot spend a failFast
// budget the create transaction still needs.
const placementRankGrace = 100 * time.Millisecond

// probeAll probes every node concurrently (skipping nodes marked without an
// object store when needObjectStore) and returns their answers by index; nil
// marks a skipped, failed or cut-off probe. Without a caller deadline
// shorter than the probe timeout it waits for every probe (each is bounded
// by the probe timeout). Under such a deadline (failFast) it stops waiting
// placementRankGrace after a usable answer arrived AND every node in prefer
// answered or failed, cancelling and joining the rest, so locality is never
// lost to the cut.
func (s *Scheduler) probeAll(ctx context.Context, nodes []db.Node, needObjectStore bool, prefer []string,
	usable func(*nodev1.InfoResponse) bool,
) []*nodev1.InfoResponse {
	deadline, hasDeadline := ctx.Deadline()
	mayCut := hasDeadline && time.Until(deadline) < s.probeTimeout

	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type answer struct {
		i    int
		info *nodev1.InfoResponse
	}
	answers := make(chan answer, len(nodes))
	var wg sync.WaitGroup
	for i, node := range nodes {
		if needObjectStore && !node.ObjectStoreOK {
			s.log.Debug("placement skipped node", "node", node.ID, "reason", "no object store")
			answers <- answer{i: i}
			continue
		}
		wg.Go(func() {
			info, err := s.probe(pctx, node.ID)
			if err != nil {
				if pctx.Err() == nil || ctx.Err() != nil {
					s.log.Warn("placement probe failed", "node", node.ID, "reason", probeReason(err))
				}
				info = nil
			}
			answers <- answer{i: i, info: info}
		})
	}
	defer func() {
		cancel() // cut off the probes still running, then join them
		wg.Wait()
	}()

	pendingPrefer := 0
	for _, n := range nodes {
		if slices.Contains(prefer, n.ID) {
			pendingPrefer++
		}
	}
	infos := make([]*nodev1.InfoResponse, len(nodes))
	var (
		haveUsable bool
		grace      <-chan time.Time
	)
	for range nodes {
		select {
		case a := <-answers:
			infos[a.i] = a.info
			if slices.Contains(prefer, nodes[a.i].ID) {
				pendingPrefer--
			}
			haveUsable = haveUsable || (a.info != nil && usable(a.info))
			if mayCut && grace == nil && haveUsable && pendingPrefer == 0 {
				t := time.NewTimer(placementRankGrace)
				defer t.Stop()
				grace = t.C
			}
		case <-grace:
			return infos
		}
	}
	return infos
}

// compareCandidates orders by preference rank, then free memory and vCPUs
// (most first), then id.
func compareCandidates(prefer []string, a, b Candidate) int {
	rank := func(id string) int {
		if i := slices.Index(prefer, id); i >= 0 {
			return i
		}
		return len(prefer)
	}
	fa, fb := a.Node.Free(), b.Node.Free()
	return cmp.Or(
		cmp.Compare(rank(a.Node.ID), rank(b.Node.ID)),
		cmp.Compare(fb.MemoryMiB, fa.MemoryMiB),
		cmp.Compare(fb.VCPUs, fa.VCPUs),
		cmp.Compare(a.Node.ID, b.Node.ID),
	)
}

// hasImage reports whether the live inventory lists image.
func hasImage(info *nodev1.InfoResponse, image string) bool {
	return slices.ContainsFunc(info.GetImages(), func(img *nodev1.Image) bool { return img.GetId() == image })
}

// GenerationSource is a create that restores its disk from one completed
// object-store generation (fork, saved snapshot on a manifest, count). The
// caller resolved and authorized it; the create transaction still refuses a
// generation that is no longer completed or was purged.
type GenerationSource struct {
	// GenerationID is the snapshots row; the provision job records it so
	// the chunk collector keeps its chunks while the job is active.
	GenerationID uuid.UUID
	// ManifestKey is that row's manifest (snapformat.ManifestKey).
	ManifestKey string
	// Image is the manifest's image: only nodes listing it are candidates.
	Image string
	// DiskBytes is the manifest's disk size: the reservation's disk is never
	// smaller than it.
	DiskBytes uint64
	// Prefer lists node ids to try first (the node that wrote the
	// generation, the source sandbox's node).
	Prefer []string
}

// sourceReserve is req.Reserve with the disk grown to hold the source
// generation's disk: a restore never shrinks it.
func sourceReserve(req AdmitRequest) db.Resources {
	r := req.Reserve
	r.DiskGiB = max(r.DiskGiB, db.DiskGiBFor(req.Source.DiskBytes))
	return r
}

// sourcePlacement is the placement of a create from a generation.
func sourcePlacement(req AdmitRequest) PlaceRequest {
	return PlaceRequest{
		Reserve:         req.Reserve,
		Image:           req.Source.Image,
		Prefer:          req.Source.Prefer,
		NeedObjectStore: true,
	}
}

// createParams is the store input of one admitted create on nodeID.
func createParams(req AdmitRequest, nodeID string) db.CreateSandboxParams {
	p := db.CreateSandboxParams{
		ID:               req.ID,
		Owner:            req.Owner,
		MachineType:      req.MachineType,
		Name:             req.Name,
		Labels:           req.Labels,
		Env:              req.Env,
		ExpiresAt:        req.ExpiresAt,
		TTLSeconds:       req.TTLSeconds,
		NodeID:           nodeID,
		Reserve:          req.Reserve,
		MaxAttempts:      req.MaxAttempts,
		IdempotencyKey:   req.IdempotencyKey,
		RequestSHA256:    req.RequestSHA256,
		Restore:          req.Restore,
		SnapshotsEnabled: req.SnapshotsEnabled,
		ShareID:          req.ShareID,
	}
	if req.Source != nil {
		p.FromManifest = req.Source.ManifestKey
		p.SourceGenerationID = new(req.Source.GenerationID)
	}
	return p
}

// AdmitBatch places and atomically creates a batch of sandboxes from one
// generation (a create with count): all of them or none. Every request must
// carry the same owner and the same Source; only reqs[0]'s idempotency key
// and request hash are used, and they key the whole batch: a replay returns
// every sandbox of it in creation order, checked before any placement like
// [Scheduler.Admit]. FailFast is refused (the v1 API rejects count with
// failFast). Sandboxes are spread round-robin over the ranked candidates,
// with each node's free capacity accounted as the batch fills it; if the
// batch does not fit, nothing is created and [ErrNoCapacity] (or
// [ErrNoNode]) is returned.
func (s *Scheduler) AdmitBatch(ctx context.Context, reqs []AdmitRequest) ([]db.CreateSandboxResult, error) {
	if len(reqs) == 0 {
		return nil, errors.New("scheduler: batch admission needs at least one request")
	}
	reqs = slices.Clone(reqs) // defaults below never write the caller's slice
	first := reqs[0]
	if first.Owner == uuid.Nil() {
		return nil, db.ErrNoOwner
	}
	for i := range reqs {
		r := &reqs[i]
		switch {
		case r.Owner != first.Owner:
			return nil, errors.New("scheduler: a batch belongs to one owner")
		case r.Source == nil || r.Source.GenerationID != first.Source.GenerationID:
			return nil, errors.New("scheduler: a batch creates from one generation")
		case r.FailFast:
			return nil, errors.New("scheduler: batch admission does not take failFast")
		case r.Restore != nil || r.From != "":
			return nil, errors.New("scheduler: batch admission restores generations only")
		case r.ID == "":
			return nil, errors.New("scheduler: admit needs a sandbox id")
		case i > 0 && r.IdempotencyKey != "":
			return nil, errors.New("scheduler: only the first request of a batch carries the idempotency key")
		}
		if r.TTLSeconds != nil && r.ExpiresAt != nil {
			return nil, db.ErrInvalidTTL
		}
		if err := db.ValidateTTLSeconds(r.TTLSeconds); err != nil {
			return nil, err
		}
		r.MachineType = cmp.Or(r.MachineType, "default")
		r.Reserve = sourceReserve(*r)
	}

	if res, found, err := s.replayBatch(ctx, first); err != nil || found {
		return res, err
	}

	cands, err := s.Place(ctx, sourcePlacement(reqs[0]))
	if err != nil {
		if res, found, rerr := s.replayBatch(ctx, first); rerr == nil && found {
			return res, nil
		}
		return nil, err
	}
	nodes, ok := spread(cands, reqs)
	if !ok {
		s.log.Warn("batch does not fit", "sandboxes", len(reqs), "candidates", len(cands))
		if res, found, rerr := s.replayBatch(ctx, first); rerr == nil && found {
			return res, nil
		}
		return nil, ErrNoCapacity
	}
	params := make([]db.CreateSandboxParams, len(reqs))
	for i, r := range reqs {
		params[i] = createParams(r, nodes[i])
	}
	res, err := s.store.CreateSandboxes(ctx, params)
	switch {
	case err == nil:
		return res, nil
	case errors.Is(err, db.ErrNoCapacity), errors.Is(err, db.ErrNotFound):
		// A concurrent create took room the placement counted on, or a
		// node row vanished: nothing was created.
		if res, found, rerr := s.replayBatch(ctx, first); rerr == nil && found {
			return res, nil
		}
		return nil, ErrNoCapacity
	default:
		return nil, err
	}
}

// replayBatch is the idempotent fast path of a batch; no key means none.
func (s *Scheduler) replayBatch(ctx context.Context, first AdmitRequest) ([]db.CreateSandboxResult, bool, error) {
	if first.IdempotencyKey == "" {
		return nil, false, nil
	}
	return s.store.ReplayCreateSandboxBatch(ctx, first.Owner, first.IdempotencyKey, first.RequestSHA256)
}

// spread assigns every request a node, round-robin over the ranked
// candidates, skipping a node once the batch filled its free capacity (the
// inventory row's free, capped by the live offer). ok is false when some
// request fits nowhere.
func spread(cands []Candidate, reqs []AdmitRequest) ([]string, bool) {
	free := make([]db.Resources, len(cands))
	for i, c := range cands {
		f := c.Node.Free()
		free[i] = db.Resources{
			VCPUs:     f.VCPUs,
			MemoryMiB: f.MemoryMiB,
			DiskGiB:   min(f.DiskGiB, c.Info.GetDiskFreeMib()/1024),
		}
	}
	out := make([]string, len(reqs))
	next := 0
	for i, r := range reqs {
		placed := false
		for step := range len(cands) {
			j := (next + step) % len(cands)
			if !holds(free[j], r.Reserve) {
				continue
			}
			free[j] = db.Resources{
				VCPUs:     free[j].VCPUs - r.Reserve.VCPUs,
				MemoryMiB: free[j].MemoryMiB - r.Reserve.MemoryMiB,
				DiskGiB:   free[j].DiskGiB - r.Reserve.DiskGiB,
			}
			out[i] = cands[j].Node.ID
			next = j + 1
			placed = true
			break
		}
		if !placed {
			return nil, false
		}
	}
	return out, true
}

// holds reports whether free covers need in every dimension.
func holds(free, need db.Resources) bool {
	return free.VCPUs >= need.VCPUs && free.MemoryMiB >= need.MemoryMiB && free.DiskGiB >= need.DiskGiB
}
