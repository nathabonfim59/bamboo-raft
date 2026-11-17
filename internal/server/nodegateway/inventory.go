package nodegateway

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// DefaultInventoryInterval is how often the poller refreshes every
// connected node.
const DefaultInventoryInterval = 30 * time.Second

// inventoryTimeout bounds each Info call and each writer call, so a stuck
// node or adapter cannot stall the poller for good.
const inventoryTimeout = 10 * time.Second

// writeQueueDepth bounds the observations waiting for the writer. Full
// queue backpressures the poll instead of growing memory.
const writeQueueDepth = 64

// NodeState is one node inventory observation. ConnectedAt identifies the
// connection epoch: every write names the epoch it observed.
type NodeState struct {
	// NodeID is the node's registered id.
	NodeID string
	// Connected reports whether the node's link was live at observation.
	Connected bool
	// ConnectedAt is when that connection was established.
	ConnectedAt time.Time
	// Info is the node's InfoResponse. Nil when Connected is false, or
	// when the link is live but this round's Info call failed (the
	// connection is fresh; its capacity is unknown this round).
	Info *nodev1.InfoResponse
}

// InventoryWriter is the interface nodegateway consumes; the PostgreSQL
// inventory adapter (internal/db) implements it.
//
// Ordering guarantee, published for adapters: WriteNode is called from a
// single goroutine, and per node the writes follow connection epochs —
// once a newer epoch's state is written, no write for an older epoch
// follows it. Adapters may treat ConnectedAt as a per-node monotonic
// epoch; a compare-and-swap is not required. The context is bounded; a
// write that overruns it is abandoned and the next poll round retries.
type InventoryWriter interface {
	// WriteNode stores st.
	WriteNode(ctx context.Context, st NodeState) error
}

// Inventory polls connected nodes and writes their state, so placement can
// pick a fresh connected node with capacity. It runs with
// the nodehub role, next to the hub and the gateway. A hub change wakes it
// immediately; the interval is only the periodic safety refresh.
type Inventory struct {
	// Interval between periodic poll rounds; DefaultInventoryInterval when
	// zero. Set before Run.
	Interval time.Duration

	hub    InventoryHub
	writer InventoryWriter
	log    *slog.Logger
}

// NewInventory returns a poller for hub's links reporting to writer. hub
// must expose change notification: without it a late connection would wait
// for the next interval. log is required: the poller reports Info and write
// failures on it.
func NewInventory(hub InventoryHub, writer InventoryWriter, log *slog.Logger) (*Inventory, error) {
	if hub == nil {
		return nil, errors.New("nodegateway: NewInventory needs a hub")
	}
	if writer == nil {
		return nil, errors.New("nodegateway: NewInventory needs an InventoryWriter")
	}
	if log == nil {
		return nil, errors.New("nodegateway: NewInventory needs a logger")
	}
	return &Inventory{hub: hub, writer: writer, log: log}, nil
}

// Run polls every connected node once at startup, immediately whenever the
// hub's links change, and once per Interval as the periodic safety refresh,
// until ctx ends. It then joins the writer and the disconnect watchers and
// returns nil. A negative Interval is rejected up front (time.NewTicker
// panics on it); zero means DefaultInventoryInterval. The change
// subscription is taken before each Links snapshot, so a connection or
// replacement landing during the poll still wakes the next round.
// Cancelling ctx unblocks a poll stuck in Info (the hub snapshot and Info
// respect it), a writer stuck in WriteNode (bounded by the adapter honoring
// the write context) and every watcher.
func (inv *Inventory) Run(ctx context.Context) error {
	if inv.Interval < 0 {
		return fmt.Errorf("nodegateway: inventory interval %s is negative", inv.Interval)
	}
	events := make(chan inventoryEvent, writeQueueDepth)
	var wg sync.WaitGroup
	wg.Go(func() { inv.writeLoop(ctx, events) })

	ticker := time.NewTicker(cmp.Or(inv.Interval, DefaultInventoryInterval))
	defer ticker.Stop()
	watched := map[*nodelink.Link]string{} // links with a disconnect watcher
	for {
		// Subscribe before the snapshot: a link added, removed or replaced
		// while poll runs closes this channel, so the select below falls
		// through to an immediate poll instead of sleeping to the ticker.
		changed := inv.hub.Changes()
		inv.poll(ctx, events, watched, &wg)
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil
		case <-changed:
		case <-ticker.C:
		}
	}
}

// pruneWatched drops watcher entries for links the hub no longer holds. A
// link that leaves the hub never comes back (every dial creates a new
// link), so an entry for a departed link only pins its memory — without
// pruning, repeated reconnects would grow the map without bound.
func pruneWatched(hub HubView, watched map[*nodelink.Link]string) {
	for l := range watched {
		if cur, ok := hub.Get(l.NodeID); !ok || cur != l {
			delete(watched, l)
		}
	}
}

// inventoryEvent is one queued observation; link is the epoch it was
// observed on. A connected event's link must be a real hub link: the
// writer validates it against the link's Done channel. Disconnect events
// carry the link only for identity (the hub check), so internal tests may
// build them with synthetic links.
type inventoryEvent struct {
	link *nodelink.Link
	st   NodeState
}

// linkAlive reports whether l's transport is still up. The hub keeps
// naming a link whose transport died until its own watcher removes it —
// under load that window is arbitrarily wide — and a connected observation
// validated inside it would claim a connection that is already gone, so
// observe and writeLoop drop connected observations whose transport died.
//
// The checks do not close every check-then-write race: nodelink.Link.Close
// fails pending gRPC calls before it closes Done, so an Info failure can
// race through both checks in that gap while the hub still names the link.
// Production closers never expose it — a replacement swaps the hub's entry
// before closing the old link, and a transport that dies on its own closes
// Done before the failure is visible to Info — and the per-node epoch
// ordering published to adapters holds regardless: such a write is always
// a pre-replacement write.
func linkAlive(l *nodelink.Link) bool {
	select {
	case <-l.Done():
		return false
	default:
		return true
	}
}

// poll refreshes every connected node once and starts a disconnect watcher
// for each link seen for the first time, pruning watcher entries whose
// link the hub has dropped.
func (inv *Inventory) poll(ctx context.Context, events chan<- inventoryEvent, watched map[*nodelink.Link]string, wg *sync.WaitGroup) {
	pruneWatched(inv.hub, watched)
	for _, l := range inv.hub.Links() {
		if _, ok := watched[l]; !ok {
			watched[l] = l.NodeID
			wg.Go(func() { inv.watchDisconnect(ctx, events, l) })
		}
		inv.observe(ctx, events, l)
	}
}

// observe polls one link's Info and queues its state. A reconnect during
// the call drops the queue offer here; the writer re-validates anyway, so
// a reconnect after it cannot publish the old epoch either. An Info call
// that returns over a transport that already died is dropped too — the
// disconnect watcher owns that transition — even while the hub still names
// the link (its removal is asynchronous).
func (inv *Inventory) observe(ctx context.Context, events chan<- inventoryEvent, l *nodelink.Link) {
	infoCtx, cancel := context.WithTimeout(ctx, inventoryTimeout)
	defer cancel()
	info, err := l.Client.Info(infoCtx, &nodev1.InfoRequest{})
	if ctx.Err() != nil {
		return // shutting down
	}
	if !linkAlive(l) {
		return // transport died mid-call; the watcher reports it
	}
	if cur, ok := inv.hub.Get(l.NodeID); !ok || cur != l {
		return // replaced or gone while the call ran
	}
	st := NodeState{NodeID: l.NodeID, Connected: true, ConnectedAt: l.ConnectedAt, Info: info}
	if err != nil {
		inv.log.Warn("node info failed", "node", l.NodeID, "err", err)
		st.Info = nil
	}
	inv.enqueue(ctx, events, inventoryEvent{link: l, st: st})
}

// watchDisconnect queues the link's disconnect once it is done, unless a
// newer link already replaced it.
func (inv *Inventory) watchDisconnect(ctx context.Context, events chan<- inventoryEvent, l *nodelink.Link) {
	select {
	case <-ctx.Done():
		return
	case <-l.Done():
	}
	if ctx.Err() != nil {
		return
	}
	if cur, ok := inv.hub.Get(l.NodeID); ok && cur != l {
		return // a newer epoch is connected; its own report covers it
	}
	inv.enqueue(ctx, events, inventoryEvent{
		link: l,
		st:   NodeState{NodeID: l.NodeID, Connected: false, ConnectedAt: l.ConnectedAt},
	})
}

// enqueue offers one observation to the writer; on shutdown it is dropped.
func (inv *Inventory) enqueue(ctx context.Context, events chan<- inventoryEvent, ev inventoryEvent) {
	select {
	case events <- ev:
	case <-ctx.Done():
	}
}

// writeLoop is the only goroutine calling WriteNode, FIFO. Before each
// write it re-checks the observation's link against the hub: a connected
// state needs its link still current and its transport still alive (the
// hub names a link until its own watcher removes it, so hub currency alone
// does not mean the transport survived to the write; see linkAlive for the
// gap this does not cover), and a disconnect is skipped once a newer link
// is registered. A per-node watermark of the newest VALIDATED observation
// backs the same guarantee while the hub holds no link at all: queued or
// retried observations from an older epoch are dropped even then, and
// because the watermark advances at validation — write success or not —
// the newest state survives an adapter outage in pending while delayed
// older observations are rejected.
//
// A disconnect whose write fails would never be re-enqueued (polls only
// visit live links), so it is held as the node's single pending
// observation and retried on each interval tick. Retry revalidates the
// epoch, and any newer successful write drops it, so a reconnect always
// supersedes a pending old disconnect; connected states need no pending
// slot because every round re-observes live links.
func (inv *Inventory) writeLoop(ctx context.Context, events <-chan inventoryEvent) {
	ticker := time.NewTicker(cmp.Or(inv.Interval, DefaultInventoryInterval))
	defer ticker.Stop()
	pending := map[string]inventoryEvent{} // failed disconnects, latest per node
	written := map[string]time.Time{}      // newest epoch written per node
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for node := range pending {
				inv.retryPending(ctx, pending, written, node)
			}
		case ev := <-events:
			if ev.st.Connected && !linkAlive(ev.link) {
				// The transport died after enqueue; the watcher reports it.
				// Disconnect events are exempt: they are queued precisely
				// because Done fired.
				continue
			}
			cur, ok := inv.hub.Get(ev.st.NodeID)
			current := ok && cur == ev.link
			if ev.st.Connected && !current {
				continue // stale epoch: replaced or gone
			}
			if !ev.st.Connected && ok && cur != ev.link {
				continue // replaced: the new epoch's report covers it
			}
			if ev.st.ConnectedAt.Before(written[ev.st.NodeID]) {
				continue // an older epoch observed after a newer validated one
			}
			// The watermark advances on validation, not on write success:
			// a newest observation whose write fails must still supersede
			// delayed older ones — it survives in pending across the
			// outage. Equal epochs (poll refreshes, retries) still pass.
			written[ev.st.NodeID] = ev.st.ConnectedAt
			if inv.write(ctx, ev.st) {
				delete(pending, ev.st.NodeID) // a newer write superseded it
				continue
			}
			if !ev.st.Connected {
				pending[ev.st.NodeID] = ev // bounded: latest per node
			}
		}
	}
}

// retryPending re-attempts one held disconnect: dropped when a newer link
// or a newer validated epoch superseded it, kept when the write fails
// again. Equal epochs pass, so a held disconnect keeps its retries.
func (inv *Inventory) retryPending(ctx context.Context, pending map[string]inventoryEvent, written map[string]time.Time, node string) {
	ev, ok := pending[node]
	if !ok {
		return
	}
	if cur, alive := inv.hub.Get(node); alive && cur != ev.link {
		delete(pending, node) // reconnected: the new epoch supersedes it
		return
	}
	if ev.st.ConnectedAt.Before(written[node]) {
		delete(pending, node) // a newer epoch was written meanwhile
		return
	}
	if inv.write(ctx, ev.st) {
		written[node] = ev.st.ConnectedAt
		delete(pending, node)
	}
}

// write hands one observation to the writer with a bounded context,
// reporting success. A failing write is logged; whether it is retried is
// the caller's business (see writeLoop).
func (inv *Inventory) write(ctx context.Context, st NodeState) bool {
	wctx, cancel := context.WithTimeout(ctx, inventoryTimeout)
	defer cancel()
	if err := inv.writer.WriteNode(wctx, st); err != nil {
		inv.log.Warn("inventory write failed", "node", st.NodeID, "err", err)
		return false
	}
	return true
}
