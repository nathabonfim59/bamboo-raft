package nodelink_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func tokenAuth(want string) nodelink.Authenticator {
	return func(_ context.Context, h nodelink.Hello) error {
		if h.Token != want {
			return errors.New("bad token")
		}
		return nil
	}
}

type env struct {
	hub  *nodelink.Hub
	addr string
}

func startHub(t *testing.T, auth nodelink.Authenticator) env {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hub := nodelink.NewHub(auth, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { hub.Serve(ctx, ln); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return env{hub: hub, addr: ln.Addr().String()}
}

func startNode(t *testing.T, addr, id, token string) context.CancelFunc {
	t.Helper()
	srv := grpc.NewServer(nodelink.ServerOptions()...)
	fake := nodefake.New(id, t.TempDir())
	fake.BootDelay = 200 * time.Millisecond
	fake.Register(srv)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		nodelink.Serve(ctx, nodelink.DialConfig{
			Addr: addr, NodeID: id, Token: token, Log: quiet(),
			MinBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond,
		}, srv)
		close(done)
	}()
	stop := func() { cancel(); <-done }
	t.Cleanup(stop)
	return stop
}

func waitLink(t *testing.T, hub *nodelink.Hub, id string) *nodelink.Link {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	l, err := hub.Wait(ctx, id)
	if err != nil {
		t.Fatalf("node %s never connected: %v", id, err)
	}
	return l
}

func TestConformanceOverLink(t *testing.T) {
	e := startHub(t, tokenAuth("s3cret"))
	startNode(t, e.addr, "node-a", "s3cret")
	l := waitLink(t, e.hub, "node-a")
	nodetest.Run(t, l.Client, nodetest.Options{Phase1: nodetest.AllPhase1})
}

func TestRefusedToken(t *testing.T) {
	e := startHub(t, tokenAuth("s3cret"))
	startNode(t, e.addr, "node-bad", "wrong")
	time.Sleep(300 * time.Millisecond)
	if _, ok := e.hub.Get("node-bad"); ok {
		t.Fatal("node with a wrong token got a link")
	}
}

func TestReconnect(t *testing.T) {
	e := startHub(t, tokenAuth("t"))
	startNode(t, e.addr, "node-r", "t")
	first := waitLink(t, e.hub, "node-r")
	if _, err := first.Client.Info(t.Context(), &nodev1.InfoRequest{}); err != nil {
		t.Fatal(err)
	}

	first.Close() // the server drops the connection
	select {
	case <-first.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed after Close")
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		l, ok := e.hub.Get("node-r")
		if ok && l != first {
			info, err := l.Client.Info(t.Context(), &nodev1.InfoRequest{})
			if err != nil {
				t.Fatalf("Info over the new link: %v", err)
			}
			if info.NodeId != "node-r" {
				t.Fatalf("node id %q", info.NodeId)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node did not reconnect")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := first.Client.Info(t.Context(), &nodev1.InfoRequest{}); err == nil {
		t.Fatal("old link still answers")
	}
}

func TestNodeStopRemovesLink(t *testing.T) {
	e := startHub(t, tokenAuth("t"))
	stop := startNode(t, e.addr, "node-s", "t")
	l := waitLink(t, e.hub, "node-s")
	stop()
	select {
	case <-l.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("link not done after the node stopped")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := e.hub.Get("node-s"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("hub still lists a stopped node")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := e.hub.Links(); len(got) != 0 {
		t.Fatalf("links %v", got)
	}
}

func TestTwoNodes(t *testing.T) {
	e := startHub(t, tokenAuth("t"))
	startNode(t, e.addr, "n2", "t")
	startNode(t, e.addr, "n1", "t")
	waitLink(t, e.hub, "n1")
	waitLink(t, e.hub, "n2")
	links := e.hub.Links()
	if len(links) != 2 || links[0].NodeID != "n1" || links[1].NodeID != "n2" {
		t.Fatalf("links %v", links)
	}
	for _, l := range links {
		info, err := l.Client.Info(t.Context(), &nodev1.InfoRequest{})
		if err != nil || info.NodeId != l.NodeID {
			t.Fatalf("%s: %v %v", l.NodeID, info, err)
		}
	}
}

func TestChanges(t *testing.T) {
	e := startHub(t, tokenAuth("t"))
	ch := e.hub.Changes()
	if len(e.hub.Links()) != 0 {
		t.Fatal("links before any node")
	}
	stop := startNode(t, e.addr, "node-c", "t")
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("Changes not closed when a node connected")
	}
	if _, ok := e.hub.Get("node-c"); !ok {
		t.Fatal("changed fired before the link was visible")
	}
	ch = e.hub.Changes()
	select {
	case <-ch:
		t.Fatal("fresh Changes channel already closed")
	default:
	}
	stop()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("Changes not closed when the node disconnected")
	}
}
