// Package scheduler decides which node hosts a new sandbox. It combines the
// fresh capacity inventory in PostgreSQL with a live connectivity probe
// through the internal node gateway, then lets [db.Store.CreateSandbox] make
// the reservation, sandbox row, first event and provision job durable in one
// transaction.
//
// The scheduler opens no node connection itself and knows nothing about the
// Hub: a Dial function bound to the accepted gateway client factory is the
// only node surface it consumes. API roles call Admit and never share worker
// state with the lifecycle role.
package scheduler

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// DefaultFreshWithin is how old a node's inventory may be and still receive
// placement. The inventory poller refreshes every 30 seconds; three missed
// rounds mark a node stale.
const DefaultFreshWithin = 90 * time.Second

// DefaultProbeTimeout bounds the gateway Info call made before reserving.
const DefaultProbeTimeout = 5 * time.Second

// DefaultFailFastBudget bounds a whole failFast admission: replay, inventory,
// gateway probes and the atomic create together, not each candidate.
const DefaultFailFastBudget = 1500 * time.Millisecond

// failFastReplayBudget is the small independent allowance for the final
// keyed replay recheck after the failFast admission budget ended. A duplicate
// request must replay rather than report a capacity/connectivity error.
const failFastReplayBudget = 500 * time.Millisecond

// Errors reported by Admit. Both map to the API's 503 out_of_capacity family at
// the API edge; the distinction stays available for logs and tests.
var (
	// ErrNoCapacity means fresh nodes exist but none has room, or every
	// candidate lost its reservation race. Callers answer 503
	// out_of_capacity.
	ErrNoCapacity = errors.New("scheduler: no node with capacity")

	// ErrNoNode means no candidate could be probed through the gateway:
	// nodes were stale, disconnected or reported inconsistent inventory.
	// Callers answer 503 no_ready_machine.
	ErrNoNode = errors.New("scheduler: no connected node available")
)

// Store is the database surface placement needs. *db.Store implements it.
type Store interface {
	NodesWithCapacity(ctx context.Context, need db.Resources, freshWithin time.Duration) ([]db.Node, error)
	CreateSandbox(ctx context.Context, p db.CreateSandboxParams) (db.CreateSandboxResult, error)
	ReplayCreateSandbox(ctx context.Context, owner uuid.UUID, key string, requestSHA256 []byte) (db.CreateSandboxResult, bool, error)
	// Sandbox reads one owner-visible sandbox row; resume admission uses it
	// to find the node that holds the retained disk.
	Sandbox(ctx context.Context, owner uuid.UUID, id string) (db.Sandbox, error)
	// AcceptSandboxResume performs the atomic same-node resume admission.
	AcceptSandboxResume(ctx context.Context, p db.ResumeParams) (db.ResumeAcceptance, error)
}

// NodeClient is the narrow node surface placement probes. The accepted
// gateway client implements it.
type NodeClient interface {
	Info(ctx context.Context, in *nodev1.InfoRequest, opts ...grpc.CallOption) (*nodev1.InfoResponse, error)
}

// Dial opens a gateway-bound client for one node and returns its closer.
// Callers (server wiring) adapt nodegateway.ClientFactory.Dial to this shape.
type Dial func(nodeID string) (NodeClient, io.Closer, error)

// RestoreResolver resolves an owner-scoped saved-snapshot name to one
// immutable generation and the machine shape it implies. The server adapts
// api.ResolveSnapshotRestore to this interface, so the scheduler owns the
// admission budget while the API package owns the wire policy.
type RestoreResolver interface {
	ResolveRestore(ctx context.Context, owner uuid.UUID, name string, typeExplicit bool, requestedType string) (db.SnapshotSource, string, db.Resources, error)
}

// Options configures a Scheduler. Store, Dial and Log are required.
type Options struct {
	Store          Store
	Dial           Dial
	Log            *slog.Logger
	Restore        RestoreResolver
	FreshWithin    time.Duration // DefaultFreshWithin when zero
	ProbeTimeout   time.Duration // DefaultProbeTimeout when zero
	FailFastBudget time.Duration // DefaultFailFastBudget when zero
}

// Scheduler performs placement and atomic admission. It is safe for
// concurrent use and holds no per-request state.
type Scheduler struct {
	store          Store
	dial           Dial
	log            *slog.Logger
	restore        RestoreResolver
	freshWithin    time.Duration
	probeTimeout   time.Duration
	failFastBudget time.Duration
}

// New validates opts and returns a scheduler.
func New(opts Options) (*Scheduler, error) {
	if opts.Store == nil {
		return nil, errors.New("scheduler: New needs a Store")
	}
	if opts.Dial == nil {
		return nil, errors.New("scheduler: New needs a Dial function")
	}
	if opts.Log == nil {
		return nil, errors.New("scheduler: New needs a logger")
	}
	if opts.FreshWithin < 0 || opts.ProbeTimeout < 0 || opts.FailFastBudget < 0 {
		return nil, errors.New("scheduler: durations must not be negative")
	}
	return &Scheduler{
		store:          opts.Store,
		dial:           opts.Dial,
		log:            opts.Log,
		restore:        opts.Restore,
		freshWithin:    cmp.Or(opts.FreshWithin, DefaultFreshWithin),
		probeTimeout:   cmp.Or(opts.ProbeTimeout, DefaultProbeTimeout),
		failFastBudget: cmp.Or(opts.FailFastBudget, DefaultFailFastBudget),
	}, nil
}

// AdmitRequest is one owner's create, already validated and sized by the API
// layer. ID must be a fresh ids.Sandbox value; env is plaintext input only
// and never leaves this call except sealed inside the database.
type AdmitRequest struct {
	ID          string
	Owner       uuid.UUID
	MachineType string
	Name        string
	Labels      map[string]string
	Env         map[string]string
	ExpiresAt   *time.Time
	// TTLSeconds is the API-facing duration source; nil means unlimited.
	// It is mutually exclusive with the legacy absolute ExpiresAt. The
	// database captures the TTL at insert time, not here.
	TTLSeconds  *int
	Reserve     db.Resources
	MaxAttempts int

	// FailFast bounds the whole admission to FailFastBudget (about 1.5s
	// by default): placement answers from nodes that are reachable now,
	// never waiting 5s per candidate. The final keyed replay recheck still
	// runs, so a duplicate is not hidden by the deadline.
	FailFast bool

	IdempotencyKey string
	RequestSHA256  []byte

	// Restore, when set, places the create on the immutable artifact's
	// recorded node only. The API's restore resolver may instead fill it
	// from From/TypeExplicit inside the same admission budget.
	Restore *db.SnapshotSource

	// From is the saved-snapshot name to restore, empty for a fresh create.
	// TypeExplicit reports whether the raw create carried a type; when it
	// did not, the resolver inherits the snapshot's canonical type.
	From         string
	TypeExplicit bool
}

// Admit places and atomically creates one sandbox. The idempotency replay is
// checked before any placement or connectivity decision: a duplicate request
// returns the original sandbox even while the cluster is full or every node
// is disconnected. A repeated key with a different request hash answers
// [db.ErrIdempotencyReused]; a concurrent same-key create answers
// [db.ErrIdempotencyInProgress] (the API maps both to 409).
func (s *Scheduler) Admit(ctx context.Context, req AdmitRequest) (db.CreateSandboxResult, error) {
	if req.Owner == uuid.Nil() {
		return db.CreateSandboxResult{}, db.ErrNoOwner
	}
	if req.ID == "" {
		return db.CreateSandboxResult{}, errors.New("scheduler: admit needs a sandbox id")
	}
	if req.MachineType == "" && req.From == "" {
		req.MachineType = "default"
	}
	if req.TTLSeconds != nil && req.ExpiresAt != nil {
		return db.CreateSandboxResult{}, db.ErrInvalidTTL
	}
	if err := db.ValidateTTLSeconds(req.TTLSeconds); err != nil {
		return db.CreateSandboxResult{}, err
	}
	callerCtx := ctx
	if req.FailFast {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.failFastBudget)
		defer cancel()
	}

	if res, found, err := s.replay(ctx, req); err != nil {
		// A failFast deadline can expire inside the replay read itself;
		// still give a keyed duplicate its detached final chance, but
		// never detach from the caller's own cancellation.
		if req.IdempotencyKey != "" && ctx.Err() != nil {
			if res, found, ferr := s.finalReplay(callerCtx, ctx, req); ferr == nil && found {
				return res, nil
			}
		}
		return db.CreateSandboxResult{}, err
	} else if found {
		return res, nil
	}

	// A create-from name is resolved inside the same failFast budget as the
	// replay and placement, never before it.
	if req.Restore == nil && req.From != "" {
		if s.restore == nil {
			return db.CreateSandboxResult{}, errors.New("scheduler: create-from needs a restore resolver")
		}
		source, machine, reserve, err := s.restore.ResolveRestore(ctx, req.Owner, req.From, req.TypeExplicit, req.MachineType)
		if err != nil {
			// Another duplicate may have committed while this request
			// resolved a deleted or replaced name. Give the keyed replay
			// one last bounded chance before returning the resolver error.
			if res, found, rerr := s.finalReplay(callerCtx, ctx, req); rerr != nil {
				return db.CreateSandboxResult{}, rerr
			} else if found {
				return res, nil
			}
			switch {
			case callerCtx.Err() != nil:
				// The caller's own cancellation/deadline wins and must
				// reach the API as such, never as a lookup 500.
				return db.CreateSandboxResult{}, callerCtx.Err()
			case ctx.Err() != nil:
				// The internal failFast budget ended while the caller is
				// still waiting: this is the existing no-ready-machine
				// admission refusal, not a control-plane fault.
				return db.CreateSandboxResult{}, errors.Join(ErrNoNode, ctx.Err())
			default:
				return db.CreateSandboxResult{}, err
			}
		}
		req.Restore = &source
		req.MachineType = machine
		req.Reserve = reserve
	}

	if req.Restore != nil {
		return s.admitRestore(ctx, callerCtx, req)
	}

	nodes, err := s.store.NodesWithCapacity(ctx, req.Reserve, s.freshWithin)
	if err != nil {
		if req.IdempotencyKey != "" {
			if res, found, rerr := s.finalReplay(callerCtx, ctx, req); rerr == nil && found {
				return res, nil
			}
		}
		return db.CreateSandboxResult{}, fmt.Errorf("scheduler: list nodes: %w", err)
	}

	var probed int
	for _, node := range nodes {
		if ctx.Err() != nil {
			break // failFast budget spent; do not start another probe
		}
		info, err := s.probe(ctx, node.ID)
		if err != nil {
			s.log.Warn("placement probe failed", "node", node.ID, "reason", probeReason(err))
			continue
		}
		probed++
		if !fits(info, req.Reserve) {
			s.logNoRoom("placement", node.ID, capacityShortfalls(info, req.Reserve))
			continue
		}
		res, err := s.store.CreateSandbox(ctx, db.CreateSandboxParams{
			ID:             req.ID,
			Owner:          req.Owner,
			MachineType:    req.MachineType,
			Name:           req.Name,
			Labels:         req.Labels,
			Env:            req.Env,
			ExpiresAt:      req.ExpiresAt,
			TTLSeconds:     req.TTLSeconds,
			NodeID:         node.ID,
			Reserve:        req.Reserve,
			MaxAttempts:    req.MaxAttempts,
			IdempotencyKey: req.IdempotencyKey,
			RequestSHA256:  req.RequestSHA256,
		})
		if err == nil {
			return res, nil
		}
		if !errors.Is(err, db.ErrNoCapacity) && !errors.Is(err, db.ErrNotFound) {
			return db.CreateSandboxResult{}, err
		}
		// Another create won this node's last slot (or the node row
		// vanished). Try the next candidate.
		s.log.Warn("reservation lost", "node", node.ID, "reason", probeReason(err))
	}

	// A duplicate may have committed while placement ran; replay before
	// answering capacity or connectivity failure. When a failFast deadline
	// already fired, this last check gets its own small budget: hiding a
	// replay behind the admission deadline would create a second sandbox
	// on a client retry.
	if res, found, err := s.finalReplay(callerCtx, ctx, req); err != nil {
		return db.CreateSandboxResult{}, err
	} else if found {
		return res, nil
	}
	if len(nodes) == 0 {
		return db.CreateSandboxResult{}, ErrNoCapacity
	}
	if probed == 0 {
		if ctx.Err() != nil {
			return db.CreateSandboxResult{}, errors.Join(ErrNoNode, ctx.Err())
		}
		return db.CreateSandboxResult{}, ErrNoNode
	}
	return db.CreateSandboxResult{}, ErrNoCapacity
}

// admitRestore places a create-from on the saved artifact's recorded node. It
// has exactly one candidate: the node that holds the immutable generation. A
// disconnected or full source node is a capacity/placement refusal, never a
// reason to boot the same artifact elsewhere or to fall back to a fresh disk.
func (s *Scheduler) admitRestore(ctx, callerCtx context.Context, req AdmitRequest) (db.CreateSandboxResult, error) {
	source := req.Restore
	if source == nil || source.NodeID == "" || req.MachineType == "" {
		return db.CreateSandboxResult{}, db.ErrSnapshotNotFound
	}
	info, err := s.probe(ctx, source.NodeID)
	if err != nil {
		s.log.Warn("restore probe failed", "node", source.NodeID, "reason", probeReason(err))
		if res, found, rerr := s.finalReplay(callerCtx, ctx, req); rerr != nil {
			return db.CreateSandboxResult{}, rerr
		} else if found {
			return res, nil
		}
		if ctx.Err() != nil {
			return db.CreateSandboxResult{}, errors.Join(ErrNoNode, ctx.Err())
		}
		return db.CreateSandboxResult{}, ErrNoNode
	}
	if !fits(info, req.Reserve) {
		s.logNoRoom("restore", source.NodeID, capacityShortfalls(info, req.Reserve))
		if res, found, rerr := s.finalReplay(callerCtx, ctx, req); rerr != nil {
			return db.CreateSandboxResult{}, rerr
		} else if found {
			return res, nil
		}
		return db.CreateSandboxResult{}, ErrNoCapacity
	}
	res, err := s.store.CreateSandbox(ctx, db.CreateSandboxParams{
		ID:             req.ID,
		Owner:          req.Owner,
		MachineType:    req.MachineType,
		Name:           req.Name,
		Labels:         req.Labels,
		Env:            req.Env,
		ExpiresAt:      req.ExpiresAt,
		TTLSeconds:     req.TTLSeconds,
		NodeID:         source.NodeID,
		Reserve:        req.Reserve,
		MaxAttempts:    req.MaxAttempts,
		IdempotencyKey: req.IdempotencyKey,
		RequestSHA256:  req.RequestSHA256,
		Restore:        source,
	})
	switch {
	case err == nil:
		return res, nil
	case errors.Is(err, db.ErrNoCapacity):
		if res, found, rerr := s.finalReplay(callerCtx, ctx, req); rerr != nil {
			return db.CreateSandboxResult{}, rerr
		} else if found {
			return res, nil
		}
		return db.CreateSandboxResult{}, ErrNoCapacity
	case errors.Is(err, db.ErrNotFound):
		// The artifact node row vanished; it is not a source-row miss.
		if res, found, rerr := s.finalReplay(callerCtx, ctx, req); rerr != nil {
			return db.CreateSandboxResult{}, rerr
		} else if found {
			return res, nil
		}
		return db.CreateSandboxResult{}, ErrNoNode
	default:
		// Snapshot identity/disk errors and driver faults pass through so
		// the API can map them without a second guess.
		return db.CreateSandboxResult{}, err
	}
}

// replay is the idempotent fast path; no key means no replay.
func (s *Scheduler) replay(ctx context.Context, req AdmitRequest) (db.CreateSandboxResult, bool, error) {
	if req.IdempotencyKey == "" {
		return db.CreateSandboxResult{}, false, nil
	}
	return s.store.ReplayCreateSandbox(ctx, req.Owner, req.IdempotencyKey, req.RequestSHA256)
}

// finalReplay rechecks a keyed admission after placement failed. It only
// detaches from the internal failFast deadline: a caller cancellation or
// deadline still wins and is never replaced by a fresh context.
func (s *Scheduler) finalReplay(callerCtx, admitCtx context.Context, req AdmitRequest) (db.CreateSandboxResult, bool, error) {
	if req.IdempotencyKey == "" || admitCtx.Err() == nil {
		return s.replay(admitCtx, req)
	}
	if callerCtx.Err() != nil {
		return s.replay(callerCtx, req)
	}
	rctx, cancel := context.WithTimeout(callerCtx, failFastReplayBudget)
	defer cancel()
	return s.replay(rctx, req)
}

// probe dials the node through the gateway and asks for its live inventory.
// It closes the client before returning. A probe error is never echoed raw:
// the caller logs the classification from the gateway and moves on.
func (s *Scheduler) probe(ctx context.Context, nodeID string) (*nodev1.InfoResponse, error) {
	client, closer, err := s.dial(nodeID)
	if err != nil {
		return nil, fmt.Errorf("dial gateway-bound node: %w", err)
	}
	defer closer.Close()
	pctx, cancel := context.WithTimeout(ctx, s.probeTimeout)
	defer cancel()
	info, err := client.Info(pctx, &nodev1.InfoRequest{})
	if err != nil {
		return nil, fmt.Errorf("info: %w", err)
	}
	if info.GetNodeId() != nodeID {
		return nil, fmt.Errorf("info identifies node %q, want %q", info.GetNodeId(), nodeID)
	}
	return info, nil
}

// fits checks that the live node still offers at least the requested totals.
// The authoritative reservation check is inside CreateSandbox; this only
// rejects a stale inventory row before a pointless transaction.
func fits(info *nodev1.InfoResponse, need db.Resources) bool {
	return len(capacityShortfalls(info, need)) == 0
}

// resourceShortfall is one dimension where the live node offers less than the
// sandbox needs. Need and Offer share Unit.
type resourceShortfall struct {
	Resource string
	Need     uint64
	Offer    uint64
	Unit     string
}

// capacityShortfalls lists every resource dimension the live inventory cannot
// cover, in stable vcpus, memory, disk order. Disk offers are MiB so need and
// offer use the same unit.
func capacityShortfalls(info *nodev1.InfoResponse, need db.Resources) []resourceShortfall {
	var short []resourceShortfall
	if info.GetVcpus() < need.VCPUs {
		short = append(short, resourceShortfall{
			Resource: "vcpus", Need: uint64(need.VCPUs), Offer: uint64(info.GetVcpus()), Unit: "vcpus",
		})
	}
	if info.GetMemoryMib() < need.MemoryMiB {
		short = append(short, resourceShortfall{
			Resource: "memory", Need: need.MemoryMiB, Offer: info.GetMemoryMib(), Unit: "MiB",
		})
	}
	if diskNeed := need.DiskGiB * 1024; info.GetDiskFreeMib() < diskNeed {
		short = append(short, resourceShortfall{
			Resource: "disk", Need: diskNeed, Offer: info.GetDiskFreeMib(), Unit: "MiB",
		})
	}
	return short
}

// logNoRoom records each failed resource with fixed fields. Raw inventory,
// errors and env never reach the log.
func (s *Scheduler) logNoRoom(operation, nodeID string, short []resourceShortfall) {
	for _, item := range short {
		s.log.Warn("node has no room",
			"node", nodeID,
			"operation", operation,
			"resource", item.Resource,
			"need", item.Need,
			"offer", item.Offer,
			"unit", item.Unit,
		)
	}
}

// probeReason maps placement failures to fixed, non-secret log reasons. Raw
// gateway/driver text (which may carry credentials or host details) never
// reaches a log line.
func probeReason(err error) string {
	switch status.Code(err) {
	case codes.Unavailable:
		return "node unavailable"
	case codes.DeadlineExceeded:
		return "node probe timeout"
	case codes.Unauthenticated, codes.PermissionDenied:
		return "gateway unauthorized"
	case codes.ResourceExhausted:
		return "node out of capacity"
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "probe timeout"
	case errors.Is(err, context.Canceled):
		return "probe cancelled"
	case errors.Is(err, db.ErrNoCapacity):
		return "no capacity"
	case errors.Is(err, db.ErrNotFound):
		return "node missing"
	default:
		return "probe failed"
	}
}
