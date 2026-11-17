package nodelink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// MaxResponseBytes bounds one nodev1 response a client accepts. gRPC's
// default (4 MiB) is below what the contract allows: GetProcess returns two
// windows of up to 8 MiB each and ListFiles up to 10000 entries. Every
// client of nodev1.Node (the hub's, and the node gateway's) uses this.
const MaxResponseBytes = 64 << 20

// Authenticator decides whether a dialing node may join. Return an error to
// refuse it; the error text is sent back to the node.
type Authenticator func(ctx context.Context, h Hello) error

// Link is one connected node.
type Link struct {
	NodeID      string
	Version     string
	ConnectedAt time.Time
	// Client calls the node. It fails once Done is closed.
	Client nodev1.NodeClient

	cc   *grpc.ClientConn
	conn *trackedConn
}

// Done is closed when the connection to the node is gone.
func (l *Link) Done() <-chan struct{} { return l.conn.done }

// Close drops the connection; the node will dial again.
func (l *Link) Close() error {
	err := l.cc.Close()
	l.conn.Close()
	return err
}

// Hub accepts node connections on the server side and keeps the live links,
// one per node id. A node that connects again replaces its old link.
type Hub struct {
	auth Authenticator
	log  *slog.Logger

	mu      sync.Mutex
	links   map[string]*Link
	changed chan struct{} // closed and replaced whenever links change
}

func NewHub(auth Authenticator, log *slog.Logger) *Hub {
	if auth == nil {
		panic("nodelink: NewHub needs an Authenticator")
	}
	return &Hub{
		auth:    auth,
		log:     log,
		links:   map[string]*Link{},
		changed: make(chan struct{}),
	}
}

// Serve accepts node connections on ln until ctx ends or ln fails. It closes
// ln and every link when it returns.
func (h *Hub) Serve(ctx context.Context, ln net.Listener) error {
	stop := context.AfterFunc(ctx, func() { ln.Close() })
	defer stop()
	defer h.closeAll()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			if isTimeout(err) {
				continue
			}
			return err
		}
		wg.Go(func() {
			if err := h.accept(ctx, c); err != nil {
				h.log.Warn("node connection refused", "remote", c.RemoteAddr().String(), "err", err)
				c.Close()
			}
		})
	}
}

func (h *Hub) accept(ctx context.Context, c net.Conn) error {
	c.SetDeadline(time.Now().Add(handshakeTimeout))
	var hello Hello
	if err := readFrame(c, &hello); err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	if hello.NodeID == "" {
		writeFrame(c, Welcome{Error: "nodeId is required"})
		return errors.New("hello without nodeId")
	}
	if err := h.auth(ctx, hello); err != nil {
		writeFrame(c, Welcome{Error: err.Error()})
		return fmt.Errorf("node %q: %w", hello.NodeID, err)
	}
	if err := writeFrame(c, Welcome{OK: true}); err != nil {
		return fmt.Errorf("write welcome: %w", err)
	}
	c.SetDeadline(time.Time{})

	tc := track(c)
	var dialed atomic.Bool
	cc, err := grpc.NewClient("passthrough:///"+hello.NodeID,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // the conn itself carries TLS when configured
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			if dialed.Swap(true) {
				return nil, errors.New("nodelink: link closed; waiting for the node to dial again")
			}
			return tc, nil
		}),
		grpc.WithKeepaliveParams(clientKeepalive),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxResponseBytes)),
		grpc.WithIdleTimeout(0), // an idle client would close the only connection
	)
	if err != nil {
		return err
	}
	cc.Connect()
	l := &Link{
		NodeID:      hello.NodeID,
		Version:     hello.Version,
		ConnectedAt: time.Now(),
		Client:      nodev1.NewNodeClient(cc),
		cc:          cc,
		conn:        tc,
	}
	h.add(l)
	h.log.Info("node connected", "node", l.NodeID, "version", l.Version, "remote", c.RemoteAddr().String())
	go func() {
		<-l.Done()
		cc.Close()
		if h.remove(l) {
			h.log.Info("node disconnected", "node", l.NodeID)
		}
	}()
	return nil
}

func (h *Hub) add(l *Link) {
	h.mu.Lock()
	old := h.links[l.NodeID]
	h.links[l.NodeID] = l
	h.notifyLocked()
	h.mu.Unlock()
	if old != nil {
		old.Close()
	}
}

// remove drops l if it is still the current link for its node.
func (h *Hub) remove(l *Link) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.links[l.NodeID] != l {
		return false
	}
	delete(h.links, l.NodeID)
	h.notifyLocked()
	return true
}

func (h *Hub) notifyLocked() {
	close(h.changed)
	h.changed = make(chan struct{})
}

func (h *Hub) closeAll() {
	h.mu.Lock()
	links := slices.Collect(maps.Values(h.links))
	h.mu.Unlock()
	for _, l := range links {
		l.Close()
	}
}

// Get returns the live link of a node.
func (h *Hub) Get(nodeID string) (*Link, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	l, ok := h.links[nodeID]
	return l, ok
}

// Links returns the live links, sorted by node id.
func (h *Hub) Links() []*Link {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.SortedFunc(maps.Values(h.links), func(a, b *Link) int {
		return strings.Compare(a.NodeID, b.NodeID)
	})
}

// Changes returns a channel that is closed the next time the set of links
// changes: a node connects, disconnects or replaces its link. Take the
// channel BEFORE reading Links, then wait on it, so no change is missed;
// after it closes, call Changes again for the next one.
func (h *Hub) Changes() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.changed
}

// Wait blocks until the node is connected, or ctx ends.
func (h *Hub) Wait(ctx context.Context, nodeID string) (*Link, error) {
	for {
		h.mu.Lock()
		l, ok := h.links[nodeID]
		ch := h.changed
		h.mu.Unlock()
		if ok {
			return l, nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
}
