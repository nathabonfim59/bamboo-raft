package nodegateway

// Internal tests: (a) the inventory watcher bookkeeping — entries for
// links the hub no longer holds are pruned each round, so reconnect churn
// cannot pin dead links in the watched map; (b) the writer loop's epoch
// ordering, driven directly with an empty hub and synthetic links, which
// makes the delayed-observation races deterministic.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestPruneWatchedDropsDeadLinks churns one node's links and checks the
// map only ever keeps entries the hub still holds.
func TestPruneWatchedDropsDeadLinks(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hub := nodelink.NewHub(func(context.Context, nodelink.Hello) error { return nil }, quietLog())
	served := make(chan struct{})
	// t.Context() ends before this test's cleanups run, which is fine
	// here: nothing in a cleanup needs the hub or the node alive.
	go func() { hub.Serve(t.Context(), ln); close(served) }()
	t.Cleanup(func() { <-served })

	node := grpc.NewServer(nodelink.ServerOptions()...)
	nodefake.New("node-pr", t.TempDir()).Register(node)
	dialCtx, dialCancel := context.WithCancel(context.Background())
	dialed := make(chan struct{})
	go func() {
		nodelink.Serve(dialCtx, nodelink.DialConfig{
			Addr: ln.Addr().String(), NodeID: "node-pr", Log: quietLog(),
			MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		}, node)
		close(dialed)
	}()
	t.Cleanup(func() { dialCancel(); <-dialed })

	watched := map[*nodelink.Link]string{}
	link := waitHubLink(t, hub, "node-pr")
	watched[link] = link.NodeID
	pruneWatched(hub, watched)
	if len(watched) != 1 {
		t.Fatalf("prune dropped the live link: %d entries", len(watched))
	}

	for range 5 {
		link.Close() // replaced by a fresh dial
		link = waitNewHubLink(t, hub, "node-pr", link)
		watched[link] = link.NodeID
		pruneWatched(hub, watched)
		if len(watched) != 1 {
			t.Fatalf("after a replacement the map holds %d entries, want 1", len(watched))
		}
	}
}

func waitHubLink(t *testing.T, hub *nodelink.Hub, id string) *nodelink.Link {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	l, err := hub.Wait(ctx, id)
	if err != nil {
		t.Fatalf("node %s never connected: %v", id, err)
	}
	return l
}

func waitNewHubLink(t *testing.T, hub *nodelink.Hub, id string, old *nodelink.Link) *nodelink.Link {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if l, ok := hub.Get(id); ok && l != old {
			return l
		}
		if time.Now().After(deadline) {
			t.Fatal("node did not reconnect with a new link")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// startInternalNode runs a hub and one nodefake dialing into it and returns
// the hub; both stop with the test.
func startInternalNode(t *testing.T, id string) *nodelink.Hub {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hub := nodelink.NewHub(func(context.Context, nodelink.Hello) error { return nil }, quietLog())
	served := make(chan struct{})
	// t.Context() ends before this test's cleanups run, which is fine
	// here: nothing in a cleanup needs the hub or the node alive.
	go func() { hub.Serve(t.Context(), ln); close(served) }()
	t.Cleanup(func() { <-served })

	node := grpc.NewServer(nodelink.ServerOptions()...)
	nodefake.New(id, t.TempDir()).Register(node)
	dialCtx, dialCancel := context.WithCancel(t.Context())
	dialed := make(chan struct{})
	go func() {
		nodelink.Serve(dialCtx, nodelink.DialConfig{
			Addr: ln.Addr().String(), NodeID: id, Log: quietLog(),
			MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		}, node)
		close(dialed)
	}()
	t.Cleanup(func() { dialCancel(); <-dialed })
	return hub
}

// deadLink returns id's link from hub with its transport already closed:
// Done is closed and the connection is gone, while the link value and its
// epoch stay usable.
func deadLink(t *testing.T, hub *nodelink.Hub, id string) *nodelink.Link {
	t.Helper()
	l := waitHubLink(t, hub, id)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	<-l.Done() // the transport death is observed before anything races
	return l
}

// pinnedHub reports l as the node's current link forever, however the real
// hub sees it. The real Hub keeps returning a link whose transport died
// until its own watcher goroutine removes it — under load that window is
// arbitrarily wide — so a writer-loop regression must not depend on the
// removal racing anyone.
type pinnedHub struct{ l *nodelink.Link }

func (h pinnedHub) Get(string) (*nodelink.Link, bool) { return h.l, true }
func (h pinnedHub) Links() []*nodelink.Link           { return []*nodelink.Link{h.l} }
func (h pinnedHub) Changes() <-chan struct{}          { return nil }

// emptyHub is a HubView with no links at all: every observation must
// order itself by epoch alone. Changes returns nil because the writer-loop
// tests drive writeLoop directly and never subscribe; a nil channel simply
// never fires if one ever did.
type emptyHub struct{}

func (emptyHub) Get(string) (*nodelink.Link, bool) { return nil, false }
func (emptyHub) Links() []*nodelink.Link           { return nil }
func (emptyHub) Changes() <-chan struct{}          { return nil }

// internalRecorder records every WriteNode.
type internalRecorder struct {
	mu     sync.Mutex
	states []NodeState
}

func (r *internalRecorder) WriteNode(_ context.Context, st NodeState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states = append(r.states, st)
	return nil
}

func (r *internalRecorder) snapshot() []NodeState {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]NodeState, len(r.states))
	copy(out, r.states)
	return out
}

var errAdapterDown = errors.New("adapter down")

// failNWriter fails the first n writes, then records: an adapter outage
// around specific observations.
type failNWriter struct {
	*internalRecorder
	n atomic.Int64
}

func (w *failNWriter) WriteNode(ctx context.Context, st NodeState) error {
	if w.n.Add(-1) >= 0 {
		return errAdapterDown
	}
	return w.internalRecorder.WriteNode(ctx, st)
}

// discEvent builds a disconnect observation at epoch at. The link is
// synthetic: an empty hub never compares it beyond pointer inequality.
func discEvent(node string, at time.Time) inventoryEvent {
	return inventoryEvent{
		link: &nodelink.Link{NodeID: node, ConnectedAt: at},
		st:   NodeState{NodeID: node, Connected: false, ConnectedAt: at},
	}
}

// runWriteLoop drives one writeLoop over events for the test's lifetime.
func runWriteLoop(t *testing.T, hub InventoryHub, w InventoryWriter) chan<- inventoryEvent {
	t.Helper()
	inv := &Inventory{hub: hub, writer: w, log: quietLog(), Interval: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan inventoryEvent, 8)
	done := make(chan struct{})
	go func() { inv.writeLoop(ctx, events); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return events
}

// waitInternalState waits for a recorded state matching pred.
func waitInternalState(t *testing.T, rec *internalRecorder, pred func(NodeState) bool) NodeState {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, st := range rec.snapshot() {
			if pred(st) {
				return st
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no inventory state matching; recorded %+v", rec.snapshot())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestWriteLoopRejectsOlderEpochWhileHubEmpty: with no link at the hub at
// all, a delayed disconnect of an older epoch must not follow a newer
// epoch's write.
func TestWriteLoopRejectsOlderEpochWhileHubEmpty(t *testing.T) {
	rec := &internalRecorder{}
	events := runWriteLoop(t, emptyHub{}, rec)
	newer := time.Now()
	events <- discEvent("n", newer)
	waitInternalState(t, rec, func(st NodeState) bool { return st.ConnectedAt.Equal(newer) })

	events <- discEvent("n", newer.Add(-time.Minute)) // delayed, hub empty
	time.Sleep(200 * time.Millisecond)                // ample time to be (wrongly) written
	for i, st := range rec.snapshot() {
		if !st.ConnectedAt.Equal(newer) {
			t.Fatalf("state %d is not the newest epoch: %+v", i, st)
		}
	}
}

// TestWriteLoopNewerDisconnectSurvivesOutageOverDelayedOlder: the newest
// disconnect's write fails (adapter outage) and pends; a delayed older
// disconnect arriving while the hub is empty must be dropped — it must
// not replace the pending newest state — and once the adapter recovers
// the newest epoch is what gets written.
func TestWriteLoopNewerDisconnectSurvivesOutageOverDelayedOlder(t *testing.T) {
	rec := &internalRecorder{}
	w := &failNWriter{internalRecorder: rec}
	w.n.Store(1) // the first write (the newest disconnect) fails
	events := runWriteLoop(t, emptyHub{}, w)

	newer := time.Now()
	older := newer.Add(-time.Minute)
	events <- discEvent("n", newer) // validated, watermark set, write fails, pends
	events <- discEvent("n", older) // delayed older: must be dropped, not pended

	st := waitInternalState(t, rec, func(st NodeState) bool { return st.ConnectedAt.Equal(newer) })
	if st.Connected {
		t.Fatalf("newest state recorded as connected: %+v", st)
	}
	time.Sleep(200 * time.Millisecond)
	for i, st := range rec.snapshot() {
		if !st.ConnectedAt.Equal(newer) {
			t.Fatalf("state %d is not the newest epoch: %+v", i, st)
		}
	}
}

// TestPendingDisconnectRetrySupersededByNewerOffline: a pending failed
// disconnect older than a newer observed offline epoch is never written.
func TestPendingDisconnectRetrySupersededByNewerOffline(t *testing.T) {
	rec := &internalRecorder{}
	w := &failNWriter{internalRecorder: rec}
	w.n.Store(1) // the older disconnect's write fails, pending it
	events := runWriteLoop(t, emptyHub{}, w)

	older := time.Now().Add(-2 * time.Minute)
	newer := time.Now().Add(-time.Minute)
	events <- discEvent("n", older) // fails, pends
	events <- discEvent("n", newer) // validated and written; supersedes it

	waitInternalState(t, rec, func(st NodeState) bool { return st.ConnectedAt.Equal(newer) })
	time.Sleep(300 * time.Millisecond) // retry ticks with the adapter healthy
	for i, st := range rec.snapshot() {
		if st.ConnectedAt.Equal(older) {
			t.Fatalf("state %d wrote the superseded older epoch: %+v", i, st)
		}
	}
}

// TestWriteLoopDropsConnectedWhoseTransportDied: the hub keeps naming a
// link whose transport died until its own watcher removes it — under load
// that window is arbitrarily wide — so a connected observation can reach
// the writer while the map still names the dead link (pinnedHub keeps that
// window open forever). Writing it would claim a connection that is already
// gone; the writer must drop it. This is the deterministic core of the
// TestInventoryDropsStalePoll flake, where the scheduler decided whether
// the hub's removal or the writer's validation ran first.
//
// A trailing disconnect on the same link is the barrier: the writer is one
// FIFO consumer, so once the sentinel is recorded the connected observation
// was processed (written or dropped) — no sleep decides the absence.
func TestWriteLoopDropsConnectedWhoseTransportDied(t *testing.T) {
	hub := startInternalNode(t, "node-dd")
	first := deadLink(t, hub, "node-dd")

	rec := &internalRecorder{}
	events := runWriteLoop(t, pinnedHub{first}, rec)
	events <- inventoryEvent{
		link: first,
		st:   NodeState{NodeID: first.NodeID, Connected: true, ConnectedAt: first.ConnectedAt},
	}
	// The sentinel disconnect keeps the link's own epoch: once it is
	// recorded the writer drained the queue past the connected
	// observation (equal epochs still pass validation).
	sentinel := inventoryEvent{
		link: first,
		st:   NodeState{NodeID: first.NodeID, ConnectedAt: first.ConnectedAt},
	}
	events <- sentinel
	waitInternalState(t, rec, func(st NodeState) bool {
		return !st.Connected && st.ConnectedAt.Equal(first.ConnectedAt)
	})

	for i, st := range rec.snapshot() {
		if st.Connected && st.ConnectedAt.Equal(first.ConnectedAt) {
			t.Fatalf("state %d writes a connected observation whose transport died: %+v", i, st)
		}
	}
}

// TestObserveDropsDeadTransport: a poll whose Info returns over a transport
// that already died enqueues nothing — the disconnect watcher owns that
// transition — even while the hub still names the link (pinnedHub keeps the
// async-removal window open, so the original code cannot drop the
// observation at its hub check and pass for the wrong reason). observe is
// called directly and returns before the channel is checked, so the
// empty-queue check needs no timing.
func TestObserveDropsDeadTransport(t *testing.T) {
	hub := startInternalNode(t, "node-od")
	first := deadLink(t, hub, "node-od")

	inv := &Inventory{hub: pinnedHub{first}, log: quietLog()}
	events := make(chan inventoryEvent, 1)
	inv.observe(t.Context(), events, first)
	select {
	case ev := <-events:
		t.Fatalf("observed %+v over a dead transport", ev)
	default:
	}
}
