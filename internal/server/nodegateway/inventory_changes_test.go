package nodegateway_test

// Change-driven inventory regressions. Every case keeps the periodic
// interval at one hour, far beyond the test deadline, so a state is only
// observed if the hub's change signal woke the poller. The observing hub
// wrapper records the subscription and snapshot calls, making the
// subscribe-before-snapshot ordering a deterministic barrier instead of a
// sleep.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

// inventoryObservingHub wraps the real hub and records when Inventory
// subscribes to changes and when it snapshots links. firstLinks closes after
// the first links snapshot is taken, which is the deterministic
// initial-empty-pass barrier the late-connection test needs.
type inventoryObservingHub struct {
	*nodelink.Hub

	mu sync.Mutex
	// changes is how many Changes subscriptions the poller took.
	changes int
	// links is how many Links snapshots it took.
	links int
	// changesAtFirstSnapshot records whether the subscription preceded the
	// first snapshot: the ordering the poller must guarantee.
	changesAtFirstSnapshot int
	// afterFirstSnapshot, when set before Run, runs once inside the first
	// Links call; the change-during-snapshot test uses it to connect a node
	// exactly between the subscription and the snapshot completing.
	afterFirstSnapshot func()

	firstLinks chan struct{}
	firstOnce  sync.Once
}

func newInventoryObservingHub(hub *nodelink.Hub) *inventoryObservingHub {
	return &inventoryObservingHub{Hub: hub, firstLinks: make(chan struct{})}
}

func (h *inventoryObservingHub) Changes() <-chan struct{} {
	h.mu.Lock()
	h.changes++
	h.mu.Unlock()
	return h.Hub.Changes()
}

func (h *inventoryObservingHub) Links() []*nodelink.Link {
	links := h.Hub.Links()
	h.mu.Lock()
	h.links++
	if h.links == 1 {
		h.changesAtFirstSnapshot = h.changes
	}
	hook := h.afterFirstSnapshot
	if h.links == 1 {
		h.afterFirstSnapshot = nil
	} else {
		hook = nil
	}
	h.mu.Unlock()
	h.firstOnce.Do(func() { close(h.firstLinks) })
	if hook != nil {
		hook()
	}
	return links
}

// subscribedBeforeFirstSnapshot reports whether a change subscription was
// taken before the first links snapshot.
func (h *inventoryObservingHub) subscribedBeforeFirstSnapshot() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.links > 0 && h.changesAtFirstSnapshot > 0
}

// inventoryRun starts one default-configured poller under a one-hour
// periodic interval. The cleanup cancels and genuinely joins Run, reporting
// any error; it never abandons the goroutine on a timeout.
func inventoryRun(t *testing.T, hub nodegateway.InventoryHub, writer nodegateway.InventoryWriter) {
	t.Helper()
	inv, err := nodegateway.NewInventory(hub, writer, quiet())
	if err != nil {
		t.Fatal(err)
	}
	inv.Interval = time.Hour
	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() { errCh <- inv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Errorf("inventory Run = %v", err)
		}
	})
}

// TestInventoryPublishesLateConnectionImmediately: the poller starts before
// any node exists, completes its initial empty pass, then a node connects.
// The first state must be published by the change signal, long before the
// one-hour periodic tick.
func TestInventoryPublishesLateConnectionImmediately(t *testing.T) {
	hub, addr := startHub(t)
	obs := newInventoryObservingHub(hub)
	rec := &recorder{}
	inventoryRun(t, obs, rec)

	select {
	case <-obs.firstLinks:
	case <-time.After(5 * time.Second):
		t.Fatal("inventory never completed its initial poll")
	}
	if !obs.subscribedBeforeFirstSnapshot() {
		t.Fatal("inventory did not subscribe to hub changes before its first snapshot")
	}
	if links := hub.Links(); len(links) != 0 {
		t.Fatalf("initial pass saw %d links, want the empty hub", len(links))
	}
	if states := rec.snapshot(); len(states) != 0 {
		t.Fatalf("initial empty pass wrote %d states", len(states))
	}

	connected := time.Now()
	startFake(t, addr, "node-late")
	state := waitState(t, rec.snapshot, func(st nodegateway.NodeState) bool {
		return st.NodeID == "node-late" && st.Connected && st.Info != nil
	})
	if elapsed := time.Since(connected); elapsed >= 5*time.Second {
		t.Fatalf("late connection took %s to publish", elapsed)
	}
	if state.ConnectedAt.IsZero() {
		t.Fatal("published state has no connection epoch")
	}
}

// TestInventoryWakesWhenChangeLandsBetweenSubscriptionAndSnapshot connects
// a node exactly after the subscription is taken and before the links
// snapshot returns. The snapshot is therefore empty while the change is
// already pending; only a subscription taken first can wake the next poll.
func TestInventoryWakesWhenChangeLandsBetweenSubscriptionAndSnapshot(t *testing.T) {
	hub, addr := startHub(t)
	obs := newInventoryObservingHub(hub)
	rec := &recorder{}

	reached := make(chan struct{})
	proceed := make(chan struct{})
	var release sync.Once
	releaseProceed := func() { release.Do(func() { close(proceed) }) }
	obs.afterFirstSnapshot = func() {
		close(reached)
		<-proceed
	}
	inventoryRun(t, obs, rec)
	// Registered after the runner join so a fatal before the release still
	// unblocks the poll first, then the runner is joined.
	t.Cleanup(releaseProceed)

	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("inventory never reached its first links snapshot")
	}
	// The change lands after the subscription and while the snapshot is
	// still in flight.
	startFake(t, addr, "node-missed")
	waitLink(t, hub, "node-missed")
	releaseProceed()

	waitState(t, rec.snapshot, func(st nodegateway.NodeState) bool {
		return st.NodeID == "node-missed" && st.Connected && st.Info != nil
	})
}

// TestInventoryReconnectPublishesNewEpochImmediately: after a replacement
// connection the new epoch must be published by the change signal without
// waiting for the periodic interval, and it must be the newest epoch.
func TestInventoryReconnectPublishesNewEpochImmediately(t *testing.T) {
	hub, addr := startHub(t)
	obs := newInventoryObservingHub(hub)
	rec := &recorder{}
	inventoryRun(t, obs, rec)
	select {
	case <-obs.firstLinks:
	case <-time.After(5 * time.Second):
		t.Fatal("inventory never completed its initial poll")
	}

	stop := startFake(t, addr, "node-reconnect")
	first := waitState(t, rec.snapshot, func(st nodegateway.NodeState) bool {
		return st.NodeID == "node-reconnect" && st.Connected
	})
	stop()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := hub.Get("node-reconnect"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("old link never left the hub")
		}
		time.Sleep(5 * time.Millisecond)
	}

	reconnected := time.Now()
	startFake(t, addr, "node-reconnect")
	newest := waitState(t, rec.snapshot, func(st nodegateway.NodeState) bool {
		return st.NodeID == "node-reconnect" && st.Connected && st.ConnectedAt.After(first.ConnectedAt)
	})
	if elapsed := time.Since(reconnected); elapsed >= 5*time.Second {
		t.Fatalf("reconnect took %s to publish", elapsed)
	}
	if !newest.ConnectedAt.After(first.ConnectedAt) {
		t.Fatalf("published epoch %s is not newer than %s", newest.ConnectedAt, first.ConnectedAt)
	}
}

// TestInventoryKeepsPeriodicRefresh: without any change signal the poller
// still refreshes live links on its interval. This is the behaviour the
// change signal must not replace.
func TestInventoryKeepsPeriodicRefresh(t *testing.T) {
	hub, addr := startHub(t)
	obs := newInventoryObservingHub(hub)
	rec := &recorder{}
	inv, err := nodegateway.NewInventory(obs, rec, quiet())
	if err != nil {
		t.Fatal(err)
	}
	inv.Interval = 25 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() { errCh <- inv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Errorf("inventory Run = %v", err)
		}
	})

	startFake(t, addr, "node-refresh")
	inventoryWaitForStates(t, rec.snapshot, 3, func(st nodegateway.NodeState) bool {
		return st.NodeID == "node-refresh" && st.Connected
	})
}

// inventoryWaitForStates waits for at least want matching observations. The
// writer is serialized, so repeated matches prove multiple poll rounds.
func inventoryWaitForStates(t *testing.T, snapshot func() []nodegateway.NodeState, want int, pred func(nodegateway.NodeState) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := 0
		for _, st := range snapshot() {
			if pred(st) {
				n++
			}
		}
		if n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("observed %d matching states, want %d", n, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
