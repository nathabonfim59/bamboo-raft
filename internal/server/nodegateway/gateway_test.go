// Tests for the internal node gateway. Every proxy test traverses the full
// stack: nodefake serving over nodelink to a real Hub, the gateway serving
// nodev1.Node on a loopback listener, and callers reaching it only through
// the client factory or raw gRPC metadata.
package nodegateway_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

const (
	nodeSecret    = "node-link-secret"
	gatewaySecret = "gateway-secret"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func tokenAuth(want string) nodelink.Authenticator {
	return func(_ context.Context, h nodelink.Hello) error {
		if subtle.ConstantTimeCompare([]byte(h.Token), []byte(want)) != 1 {
			return errors.New("bad node token")
		}
		return nil
	}
}

// startHub runs a hub nodes dial into and returns it with its address.
// The serve context is Background-derived on purpose, not t.Context():
// t.Context() is canceled before cleanup functions run, and later
// cleanups (nodetest's DeleteVM above all) need the hub, gateway and node
// still alive. The same holds for the other keep-alive helpers below.
func startHub(t *testing.T) (*nodelink.Hub, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hub := nodelink.NewHub(tokenAuth(nodeSecret), quiet())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { hub.Serve(ctx, ln); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return hub, ln.Addr().String()
}

// startNode runs node over nodelink against the hub at addr, redialing
// until ctx ends. opts are appended to nodelink.ServerOptions.
func startNode(t *testing.T, addr, id, token string, node nodev1.NodeServer, opts ...grpc.ServerOption) context.CancelFunc {
	t.Helper()
	srv := grpc.NewServer(append(nodelink.ServerOptions(), opts...)...)
	nodev1.RegisterNodeServer(srv, node)
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

// startFake starts a nodefake under node id and returns nothing else: the
// hub is the way to reach it.
func startFake(t *testing.T, addr, id string) context.CancelFunc {
	t.Helper()
	return startNode(t, addr, id, nodeSecret, nodefake.New(id, t.TempDir()))
}

// startGateway serves the gateway on a fresh loopback port and returns its
// address. opts are passed to Gateway.Serve.
func startGateway(t *testing.T, hub *nodelink.Hub, opts ...grpc.ServerOption) string {
	t.Helper()
	gw, err := nodegateway.New(hub, gatewaySecret)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gw.Serve(ctx, ln, opts...) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("gateway Serve: %v", err)
		}
	})
	return ln.Addr().String()
}

// dialGateway returns a factory client bound to nodeID, closed on cleanup.
func dialGateway(t *testing.T, addr, nodeID string) *nodegateway.Client {
	t.Helper()
	client, err := nodegateway.NewClientFactory(addr, gatewaySecret, nil).Dial(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// rawClient returns a plain nodev1 client, for calls with hand-built
// metadata.
func rawClient(t *testing.T, addr string) nodev1.NodeClient {
	t.Helper()
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return nodev1.NewNodeClient(cc)
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

// waitNoLink waits until the hub drops the node's link.
func waitNoLink(t *testing.T, hub *nodelink.Hub, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := hub.Get(id); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %s still connected", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitNewLink waits until the node's current link differs from old.
func waitNewLink(t *testing.T, hub *nodelink.Hub, id string, old *nodelink.Link) *nodelink.Link {
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

// TestConformanceThroughGateway runs the whole nodev1 conformance suite
// through the gateway over a real nodelink connection: all nine RPCs, Exec
// ordering/cancel (cancellation must cross the gateway and kill the
// command's process group at the node) and 16 MiB PutFile in 1 MiB chunks.
func TestConformanceThroughGateway(t *testing.T) {
	hub, hubAddr := startHub(t)
	fake := nodefake.New("node-a", t.TempDir())
	startNode(t, hubAddr, "node-a", nodeSecret, fake)
	waitLink(t, hub, "node-a")
	gwAddr := startGateway(t, hub)
	client := dialGateway(t, gwAddr, "node-a")
	nodetest.Run(t, client, nodetest.Options{
		Phase1: nodetest.AllPhase1,
		Phase2: nodetest.Phase2{Snapshots: true, Reset: true, Store: fake.Store},
	})
}

func TestNewValidation(t *testing.T) {
	hub, _ := startHub(t)
	for _, tc := range []struct {
		name    string
		hub     nodegateway.HubView
		token   string
		wantErr bool
	}{
		{name: "ok", hub: hub, token: "t"},
		{name: "no hub", hub: nil, token: "t", wantErr: true},
		{name: "empty token", hub: hub, token: "", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := nodegateway.New(tc.hub, tc.token)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("New() error = %v, want error: %v", err, tc.wantErr)
			}
		})
	}
}

// TestAuthentication covers credential checks: exactly one
// "authorization: Bearer <token>" value, and never the token in the error.
func TestAuthentication(t *testing.T) {
	hub, hubAddr := startHub(t)
	startFake(t, hubAddr, "node-a")
	waitLink(t, hub, "node-a")
	gwAddr := startGateway(t, hub)
	client := rawClient(t, gwAddr)

	for _, tc := range []struct {
		name string
		md   metadata.MD
	}{
		{name: "no metadata", md: nil},
		{name: "missing authorization", md: metadata.Pairs(nodegateway.MetadataNodeID, "node-a")},
		{name: "wrong token", md: authMD("Bearer wrong")},
		{name: "raw token without Bearer", md: authMD(gatewaySecret)},
		{name: "empty Bearer", md: authMD("Bearer ")},
		{name: "two authorization values", md: twoAuthMD()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := metadata.NewOutgoingContext(t.Context(), tc.md)
			_, err := client.Info(ctx, &nodev1.InfoRequest{})
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("Info() = %v, want Unauthenticated", err)
			}
			if err != nil && strings.Contains(err.Error(), gatewaySecret) {
				t.Fatalf("error leaks the token: %v", err)
			}
		})
	}
}

func authMD(v string) metadata.MD {
	return metadata.Pairs(nodegateway.MetadataAuthorization, v)
}

func twoAuthMD() metadata.MD {
	md := metadata.Pairs(nodegateway.MetadataAuthorization, "Bearer a")
	md.Append(nodegateway.MetadataAuthorization, "Bearer b")
	return md
}

// TestNodeIDMetadata covers node selection: exactly one nonempty
// bbraft-node-id, and a connected target.
func TestNodeIDMetadata(t *testing.T) {
	hub, hubAddr := startHub(t)
	startFake(t, hubAddr, "node-a")
	waitLink(t, hub, "node-a")
	gwAddr := startGateway(t, hub)
	client := rawClient(t, gwAddr)

	dup := metadata.Pairs(nodegateway.MetadataNodeID, "node-a")
	dup.Append(nodegateway.MetadataNodeID, "node-a")
	for _, tc := range []struct {
		name     string
		md       metadata.MD
		wantCode codes.Code
	}{
		{name: "missing", md: nil, wantCode: codes.InvalidArgument},
		{name: "empty", md: metadata.Pairs(nodegateway.MetadataNodeID, ""), wantCode: codes.InvalidArgument},
		{name: "two values", md: dup, wantCode: codes.InvalidArgument},
		{name: "unknown node", md: metadata.Pairs(nodegateway.MetadataNodeID, "ghost"), wantCode: codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			md := tc.md.Copy()
			md.Set(nodegateway.MetadataAuthorization, "Bearer "+gatewaySecret)
			ctx := metadata.NewOutgoingContext(t.Context(), md)
			_, err := client.Info(ctx, &nodev1.InfoRequest{})
			if status.Code(err) != tc.wantCode {
				t.Fatalf("Info() = %v, want %s", err, tc.wantCode)
			}
		})
	}
}

// TestStreamingRequiresNodeID proves the stream interceptor authenticates
// and resolves the node too, not only unary calls.
func TestStreamingRequiresNodeID(t *testing.T) {
	hub, hubAddr := startHub(t)
	startFake(t, hubAddr, "node-a")
	waitLink(t, hub, "node-a")
	gwAddr := startGateway(t, hub)
	client := rawClient(t, gwAddr)

	ctx := metadata.NewOutgoingContext(t.Context(), authMD("Bearer "+gatewaySecret))
	exec, err := client.Exec(ctx, &nodev1.ExecRequest{SandboxId: "bx_none", Command: "true"})
	if err != nil {
		t.Fatal(err)
	}
	// Stream rejection surfaces on the first Recv, not at stream creation.
	if _, err := exec.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Exec() first Recv = %v, want InvalidArgument", err)
	}
	upload, err := client.PutFile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = upload.Send(&nodev1.PutFileRequest{Part: &nodev1.PutFileRequest_Header_{
		Header: &nodev1.PutFileRequest_Header{SandboxId: "bx_none", Path: "x"},
	}})
	if _, err := upload.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("PutFile() CloseAndRecv = %v, want InvalidArgument", err)
	}
}

// TestDisconnectedNode: once the node is gone from the hub, calls through
// the gateway fail with Unavailable.
func TestDisconnectedNode(t *testing.T) {
	hub, hubAddr := startHub(t)
	stop := startFake(t, hubAddr, "node-d")
	waitLink(t, hub, "node-d")
	gwAddr := startGateway(t, hub)
	client := dialGateway(t, gwAddr, "node-d")

	stop()
	waitNoLink(t, hub, "node-d")
	if _, err := client.Info(t.Context(), &nodev1.InfoRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Info() = %v, want Unavailable", err)
	}
}

// TestReplacedNode: a node that re-dials gets a new link; calls through
// the same gateway client keep working because the link is resolved per
// call.
func TestReplacedNode(t *testing.T) {
	hub, hubAddr := startHub(t)
	startFake(t, hubAddr, "node-r")
	first := waitLink(t, hub, "node-r")
	gwAddr := startGateway(t, hub)
	client := dialGateway(t, gwAddr, "node-r")

	info, err := client.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil || info.NodeId != "node-r" {
		t.Fatalf("Info before replace: %v %v", info, err)
	}

	first.Close() // the server drops the link; the node dials again
	second := waitNewLink(t, hub, "node-r", first)
	info, err = client.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil || info.NodeId != "node-r" {
		t.Fatalf("Info after replace: %v %v", info, err)
	}
	if !second.ConnectedAt.After(first.ConnectedAt) {
		t.Fatalf("new link epoch %v not after %v", second.ConnectedAt, first.ConnectedAt)
	}
}

// TestMetadataFlow pins what travels gateway → node: the caller's extra
// metadata and both gateway credentials never reach the node.
func TestMetadataFlow(t *testing.T) {
	hub, hubAddr := startHub(t)
	var mu sync.Mutex
	var seen []metadata.MD
	capture := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		mu.Lock()
		seen = append(seen, md.Copy())
		mu.Unlock()
		return handler(ctx, req)
	}
	startNode(t, hubAddr, "node-m", nodeSecret, nodefake.New("node-m", t.TempDir()), grpc.ChainUnaryInterceptor(capture))
	waitLink(t, hub, "node-m")
	gwAddr := startGateway(t, hub)
	client := dialGateway(t, gwAddr, "node-m")

	ctx := metadata.AppendToOutgoingContext(t.Context(), "x-bbraft-test", "trace-1")
	if _, err := client.Info(ctx, &nodev1.InfoRequest{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node never saw the Info call")
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	last := seen[len(seen)-1]
	for _, key := range []string{nodegateway.MetadataNodeID, nodegateway.MetadataAuthorization, "x-bbraft-test"} {
		if vals := last.Get(key); len(vals) > 0 {
			t.Fatalf("node saw %s=%v; the gateway must not forward caller metadata", key, vals)
		}
	}
}

// putFileSignalNode signals when its PutFile handler returned, so a test
// knows the node has rejected the stream before sending more chunks.
type putFileSignalNode struct {
	*nodefake.Node
	rejected chan struct{}
	once     sync.Once
}

func (n *putFileSignalNode) PutFile(stream grpc.ClientStreamingServer[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
	err := n.Node.PutFile(stream)
	n.once.Do(func() { close(n.rejected) })
	return err
}

// TestPutFileEarlyRejection: when the node rejects the stream early —
// here an INVALID_ARGUMENT path, with more chunks still coming — the
// gateway must surface the node's status. A rejected client stream gives
// Send io.EOF, and forwarding that would lose the status.
func TestPutFileEarlyRejection(t *testing.T) {
	hub, hubAddr := startHub(t)
	node := &putFileSignalNode{Node: nodefake.New("node-p", t.TempDir()), rejected: make(chan struct{})}
	startNode(t, hubAddr, "node-p", nodeSecret, node)
	waitLink(t, hub, "node-p")
	gwAddr := startGateway(t, hub)
	client := dialGateway(t, gwAddr, "node-p")

	vm, err := client.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: "bx_earlyrej",
		Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 2048, DiskGib: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		client.DeleteVM(ctx, &nodev1.DeleteVMRequest{SandboxId: vm.SandboxId})
	})

	stream, err := client.PutFile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	header := &nodev1.PutFileRequest_Header{SandboxId: vm.SandboxId, Path: "../escape"}
	if err := stream.Send(&nodev1.PutFileRequest{Part: &nodev1.PutFileRequest_Header_{Header: header}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&nodev1.PutFileRequest{Part: &nodev1.PutFileRequest_Data{Data: []byte("chunk")}}); err != nil {
		t.Fatal(err)
	}
	<-node.rejected                    // the node handler has returned INVALID_ARGUMENT
	time.Sleep(100 * time.Millisecond) // let the rejection reach the gateway
	_ = stream.Send(&nodev1.PutFileRequest{Part: &nodev1.PutFileRequest_Data{Data: []byte("chunk")}})
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("PutFile rejected early: %v, want InvalidArgument", err)
	}
}

// TestTLS serves the gateway behind a self-signed certificate and reaches
// it through a TLS-configured factory; plaintext fails.
func TestTLS(t *testing.T) {
	hub, hubAddr := startHub(t)
	startFake(t, hubAddr, "node-t")
	waitLink(t, hub, "node-t")

	certFile, keyFile := selfSignedCert(t)
	creds, err := nodegateway.ServerCredentials(t.Context(), certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	gwAddr := startGateway(t, hub, grpc.Creds(creds))

	pool := x509.NewCertPool()
	pemCert, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	if !pool.AppendCertsFromPEM(pemCert) {
		t.Fatal("test CA certificate not parsed")
	}
	client, err := nodegateway.NewClientFactory(gwAddr, gatewaySecret, &tls.Config{RootCAs: pool}).Dial("node-t")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	info, err := client.Info(t.Context(), &nodev1.InfoRequest{})
	if err != nil || info.NodeId != "node-t" {
		t.Fatalf("Info over TLS: %v %v", info, err)
	}

	plain := dialGateway(t, gwAddr, "node-t") // no TLS: must not get through
	if _, err := plain.Info(t.Context(), &nodev1.InfoRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("plaintext Info() = %v, want Unavailable", err)
	}
}

// TestServeReturnsNilOnCancel: ending the context stops the server and
// Serve returns nil.
func TestServeReturnsNilOnCancel(t *testing.T) {
	hub, _ := startHub(t)
	gw, err := nodegateway.New(hub, gatewaySecret)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gw.Serve(ctx, ln) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve after cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
}

// recorder is a test InventoryWriter keeping every observation.
type recorder struct {
	mu     sync.Mutex
	states []nodegateway.NodeState
}

func (r *recorder) WriteNode(_ context.Context, st nodegateway.NodeState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states = append(r.states, st)
	return nil
}

func (r *recorder) snapshot() []nodegateway.NodeState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.states)
}

// waitState waits for a recorded state matching pred. snapshot is polled
// for the current observations.
func waitState(t *testing.T, snapshot func() []nodegateway.NodeState, pred func(nodegateway.NodeState) bool) nodegateway.NodeState {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, st := range snapshot() {
			if pred(st) {
				return st
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no inventory state matching; recorded %+v", snapshot())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// startInventory runs the poller and returns a stop function.
func startInventory(t *testing.T, hub *nodelink.Hub, rec nodegateway.InventoryWriter) context.CancelFunc {
	t.Helper()
	inv, err := nodegateway.NewInventory(hub, rec, quiet())
	if err != nil {
		t.Fatal(err)
	}
	inv.Interval = 40 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { inv.Run(ctx); close(done) }()
	stop := func() { cancel(); <-done }
	t.Cleanup(stop)
	return stop
}

// TestNewInventoryValidation covers the constructor's required inputs.
func TestNewInventoryValidation(t *testing.T) {
	hub, _ := startHub(t)
	rec := &recorder{}
	for _, tc := range []struct {
		name   string
		hub    nodegateway.InventoryHub
		writer nodegateway.InventoryWriter
		log    *slog.Logger
	}{
		{name: "no hub", hub: nil, writer: rec, log: quiet()},
		{name: "no writer", hub: hub, writer: nil, log: quiet()},
		{name: "no logger", hub: hub, writer: rec, log: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := nodegateway.NewInventory(tc.hub, tc.writer, tc.log); err == nil {
				t.Fatal("NewInventory succeeded, want error")
			}
		})
	}
}

// TestInventoryRejectsNegativeInterval: a negative Interval is an error,
// not a ticker panic.
func TestInventoryRejectsNegativeInterval(t *testing.T) {
	hub, _ := startHub(t)
	inv, err := nodegateway.NewInventory(hub, &recorder{}, quiet())
	if err != nil {
		t.Fatal(err)
	}
	inv.Interval = -time.Second
	if err := inv.Run(t.Context()); err == nil {
		t.Fatal("Run with a negative interval succeeded, want error")
	}
}

// TestInventoryChurn: repeated reconnects keep the newest epoch
// authoritative, and each round's watcher bookkeeping only tracks live
// links (TestPruneWatchedDropsDeadLinks pins the map side).
func TestInventoryChurn(t *testing.T) {
	hub, hubAddr := startHub(t)
	rec := &recorder{}
	startInventory(t, hub, rec)
	startFake(t, hubAddr, "node-c")
	link := waitLink(t, hub, "node-c")

	for range 8 {
		link.Close()
		link = waitNewLink(t, hub, "node-c", link)
		waitState(t, rec.snapshot, func(st nodegateway.NodeState) bool {
			return st.Connected && st.ConnectedAt.Equal(link.ConnectedAt)
		})
	}

	// Give any stale write ample time to land, then the newest epoch must
	// be authoritative: from its FIRST write on, no older epoch may
	// appear.
	time.Sleep(300 * time.Millisecond)
	states := rec.snapshot()
	last := states[len(states)-1]
	if !last.Connected || !last.ConnectedAt.Equal(link.ConnectedAt) {
		t.Fatalf("last state %+v is not the newest epoch %v", last, link.ConnectedAt)
	}
	firstIdx := slices.IndexFunc(states, func(st nodegateway.NodeState) bool {
		return st.ConnectedAt.Equal(link.ConnectedAt)
	})
	for i, st := range states[firstIdx:] {
		if !st.ConnectedAt.Equal(link.ConnectedAt) {
			t.Fatalf("state %d writes an older epoch after the newest's first write: %+v", firstIdx+i, st)
		}
	}
}

// TestInventoryTracksNodes: a connected node is written with its Info and
// epoch, and its disconnect is written when the link drops.
func TestInventoryTracksNodes(t *testing.T) {
	hub, hubAddr := startHub(t)
	rec := &recorder{}
	startInventory(t, hub, rec)
	stop := startFake(t, hubAddr, "node-i")
	link := waitLink(t, hub, "node-i")

	st := waitState(t, rec.snapshot, func(st nodegateway.NodeState) bool {
		return st.NodeID == "node-i" && st.Connected
	})
	if !st.ConnectedAt.Equal(link.ConnectedAt) {
		t.Fatalf("connected epoch %v, want the link's %v", st.ConnectedAt, link.ConnectedAt)
	}
	if st.Info == nil || st.Info.NodeId != "node-i" || st.Info.Vcpus == 0 {
		t.Fatalf("state %+v has no usable Info", st)
	}

	stop()
	waitNoLink(t, hub, "node-i")
	st = waitState(t, rec.snapshot, func(st nodegateway.NodeState) bool {
		return st.NodeID == "node-i" && !st.Connected
	})
	if !st.ConnectedAt.Equal(link.ConnectedAt) {
		t.Fatalf("disconnect epoch %v, want the dead link's %v", st.ConnectedAt, link.ConnectedAt)
	}
	if st.Info != nil {
		t.Fatalf("disconnected state carries Info: %+v", st)
	}
}

// TestInventoryReconnectEpoch: after a replacement, the new epoch is
// written and no write for the old epoch lands after it.
func TestInventoryReconnectEpoch(t *testing.T) {
	hub, hubAddr := startHub(t)
	rec := &recorder{}
	startInventory(t, hub, rec)
	startFake(t, hubAddr, "node-e")
	first := waitLink(t, hub, "node-e")

	waitState(t, rec.snapshot, func(st nodegateway.NodeState) bool {
		return st.Connected && st.ConnectedAt.Equal(first.ConnectedAt)
	})

	first.Close()
	second := waitNewLink(t, hub, "node-e", first)
	if !second.ConnectedAt.After(first.ConnectedAt) {
		t.Fatalf("epochs not ordered: %v then %v", first.ConnectedAt, second.ConnectedAt)
	}
	waitState(t, rec.snapshot, func(st nodegateway.NodeState) bool {
		return st.Connected && st.ConnectedAt.Equal(second.ConnectedAt)
	})

	// Give any stale write ample time to land, then check ordering.
	time.Sleep(300 * time.Millisecond)
	states := rec.snapshot()
	newIdx := slices.IndexFunc(states, func(st nodegateway.NodeState) bool {
		return st.ConnectedAt.Equal(second.ConnectedAt)
	})
	for i, st := range states[newIdx:] {
		if st.ConnectedAt.Equal(first.ConnectedAt) {
			t.Fatalf("state %d writes the replaced epoch after the new one: %+v", newIdx+i, st)
		}
	}
}

// slowInfoNode delays Info calls through gate, so a poll races a reconnect.
type slowInfoNode struct {
	*nodefake.Node
	gate    func(context.Context) error
	entered chan struct{}
}

func (n *slowInfoNode) Info(ctx context.Context, req *nodev1.InfoRequest) (*nodev1.InfoResponse, error) {
	n.entered <- struct{}{}
	if err := n.gate(ctx); err != nil {
		return nil, err
	}
	return n.Node.Info(ctx, req)
}

// TestInventoryDropsStalePoll: a poll that started on the old link must
// not write after the node reconnected on a new link.
//
// first.Close() here stands in for the node's transport dying. In
// production a replacement closes the old link only after the hub's map
// already names the new one (Hub.add swaps the entry first), so the epoch
// check alone drops a stale observe. A raw out-of-band close, as here,
// instead leaves a window — Link.Close fails the in-flight Info while the
// hub still names the dead link, until its own watcher removes it — during
// which a first-epoch write that lands BEFORE the replacement is valid.
// Observations whose transport already closed Done are rejected by the
// liveness checks in observe and writeLoop (pinned by the internal tests);
// the cc.Close-to-conn.Close gap inside Link.Close is not covered, and a
// write that squeezes through it is still a pre-replacement write. The
// assertion therefore inspects only records after the new epoch's first
// write, where a first-epoch record is always stale.
func TestInventoryDropsStalePoll(t *testing.T) {
	hub, hubAddr := startHub(t)
	rec := &recorder{}
	startInventory(t, hub, rec)

	release := make(chan struct{})
	node := &slowInfoNode{
		Node: nodefake.New("node-s", t.TempDir()),
		gate: func(ctx context.Context) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		},
		entered: make(chan struct{}, 1),
	}
	startNode(t, hubAddr, "node-s", nodeSecret, node)
	first := waitLink(t, hub, "node-s")

	<-node.entered // the poller's Info on the first link is now blocked
	first.Close()
	second := waitNewLink(t, hub, "node-s", first)
	if !second.ConnectedAt.After(first.ConnectedAt) {
		t.Fatalf("epochs not ordered: %v then %v", first.ConnectedAt, second.ConnectedAt)
	}
	close(release) // the node-side handler finishes — against a dead link

	// The second epoch's first write is the ordering barrier. The writer
	// is one FIFO goroutine, and the stale POLL observation was enqueued
	// or dropped before it: polls are serial, the stale Info failed when
	// first.Close() tore down the connection (before the replacement link
	// existed), and no later poll can observe the first link once the hub
	// names the new one. So a first-epoch POLL record after the barrier
	// is always a stale write; a straggler disconnect watcher enqueuing
	// later is dropped by the writer's replaced-epoch check. No sleep is
	// needed to prove the absence.
	st := waitState(t, rec.snapshot, func(st nodegateway.NodeState) bool {
		return st.Connected && st.ConnectedAt.Equal(second.ConnectedAt)
	})
	if st.Info == nil {
		t.Fatalf("new epoch state without Info: %+v", st)
	}
	states := rec.snapshot()
	newIdx := slices.IndexFunc(states, func(st nodegateway.NodeState) bool {
		return st.ConnectedAt.Equal(second.ConnectedAt)
	})
	for i, st := range states[newIdx:] {
		if st.ConnectedAt.Equal(first.ConnectedAt) {
			t.Fatalf("state %d writes the replaced epoch after the new one: %+v", newIdx+i, st)
		}
	}
}

// flakyInfoNode fails its first Info call, then reports normally.
type flakyInfoNode struct {
	*nodefake.Node
	failed atomic.Bool
}

func (n *flakyInfoNode) Info(ctx context.Context, req *nodev1.InfoRequest) (*nodev1.InfoResponse, error) {
	if n.failed.CompareAndSwap(false, true) {
		return nil, status.Error(codes.Unavailable, "info unavailable")
	}
	return n.Node.Info(ctx, req)
}

// TestInventoryInfoFailureOnLiveLinkWritesWithoutInfo: an Info failure on a
// link that is still live must keep publishing — connected with unknown
// capacity (Info nil, the designed fresh-connection state) — and the next
// round refreshes it. The dead-transport drop must not swallow this write.
func TestInventoryInfoFailureOnLiveLinkWritesWithoutInfo(t *testing.T) {
	hub, hubAddr := startHub(t)
	rec := &recorder{}
	startInventory(t, hub, rec)
	node := &flakyInfoNode{Node: nodefake.New("node-lf", t.TempDir())}
	startNode(t, hubAddr, "node-lf", nodeSecret, node)
	link := waitLink(t, hub, "node-lf")

	st := waitState(t, rec.snapshot, func(st nodegateway.NodeState) bool {
		return st.NodeID == "node-lf" && st.Connected && st.Info == nil
	})
	if !st.ConnectedAt.Equal(link.ConnectedAt) {
		t.Fatalf("connected epoch %v, want the link's %v", st.ConnectedAt, link.ConnectedAt)
	}

	// The next round refreshes the capacity over the still-live link.
	st = waitState(t, rec.snapshot, func(st nodegateway.NodeState) bool {
		return st.NodeID == "node-lf" && st.Connected && st.Info != nil
	})
	if !st.ConnectedAt.Equal(link.ConnectedAt) {
		t.Fatalf("refreshed epoch %v, want the link's %v", st.ConnectedAt, link.ConnectedAt)
	}
}

// TestInventoryWriteErrorTolerated: a failing writer is logged and the
// poller keeps running.
func TestInventoryWriteErrorTolerated(t *testing.T) {
	hub, hubAddr := startHub(t)
	failing := failWriter{}
	startInventory(t, hub, &failing)
	stop := startFake(t, hubAddr, "node-f")
	waitLink(t, hub, "node-f")

	time.Sleep(150 * time.Millisecond) // several poll rounds hit the error
	if failing.calls.Load() == 0 {
		t.Fatal("writer never called")
	}
	stop()
	waitNoLink(t, hub, "node-f")
}

type failWriter struct{ calls atomic.Int64 }

func (w *failWriter) WriteNode(context.Context, nodegateway.NodeState) error {
	w.calls.Add(1)
	return errWriteFailed
}

var errWriteFailed = status.Error(codes.Unavailable, "adapter down")

// blockingRecorder records like recorder but holds each WriteNode until
// release closes (or the write context ends), so tests can stall the
// writer loop.
type blockingRecorder struct {
	mu      sync.Mutex
	states  []nodegateway.NodeState
	entered chan struct{}
	release chan struct{}
}

func (r *blockingRecorder) WriteNode(ctx context.Context, st nodegateway.NodeState) error {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	select {
	case <-r.release:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states = append(r.states, st)
	return nil
}

func (r *blockingRecorder) snapshot() []nodegateway.NodeState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.states)
}

// TestInventorySlowWriterKeepsNewEpoch: while WriteNode is stuck, the node
// reconnects; the queued old-epoch observations must never land after the
// new epoch's write — the new epoch stays authoritative.
func TestInventorySlowWriterKeepsNewEpoch(t *testing.T) {
	hub, hubAddr := startHub(t)
	rec := &blockingRecorder{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	startInventory(t, hub, rec)
	startFake(t, hubAddr, "node-w")
	first := waitLink(t, hub, "node-w")

	<-rec.entered // the writer is stuck inside the first WriteNode
	first.Close()
	second := waitNewLink(t, hub, "node-w", first)
	if !second.ConnectedAt.After(first.ConnectedAt) {
		t.Fatalf("epochs not ordered: %v then %v", first.ConnectedAt, second.ConnectedAt)
	}
	close(rec.release) // the whole backlog flushes through one writer, FIFO

	waitState(t, rec.snapshot, func(st nodegateway.NodeState) bool {
		return st.Connected && st.ConnectedAt.Equal(second.ConnectedAt)
	})

	// No observation of the first epoch may be written after the first
	// observation of the new one.
	time.Sleep(300 * time.Millisecond)
	states := rec.snapshot()
	newIdx := slices.IndexFunc(states, func(st nodegateway.NodeState) bool {
		return st.ConnectedAt.Equal(second.ConnectedAt)
	})
	for i, st := range states[newIdx:] {
		if st.ConnectedAt.Equal(first.ConnectedAt) {
			t.Fatalf("state %d writes the replaced epoch after the new one: %+v", newIdx+i, st)
		}
	}
	if last := states[len(states)-1]; !last.Connected || !last.ConnectedAt.Equal(second.ConnectedAt) {
		t.Fatalf("last state is not the new connected epoch: %+v", last)
	}
}

// flakyWriter records like recorder but refuses disconnect writes while
// allowDisconnects is false, counting the refusals.
type flakyWriter struct {
	recorder
	allowDisconnects atomic.Bool
	disconnectFails  atomic.Int64
}

func (w *flakyWriter) WriteNode(ctx context.Context, st nodegateway.NodeState) error {
	if !st.Connected && !w.allowDisconnects.Load() {
		w.disconnectFails.Add(1)
		return errWriteFailed
	}
	return w.recorder.WriteNode(ctx, st)
}

// TestInventoryRetriesFailedDisconnect: a disconnect write that fails must
// be retried on a later tick once the adapter recovers — polls never
// revisit dead links, so nothing else would re-enqueue it.
func TestInventoryRetriesFailedDisconnect(t *testing.T) {
	hub, hubAddr := startHub(t)
	fw := &flakyWriter{}
	fw.allowDisconnects.Store(false)
	startInventory(t, hub, fw)
	stop := startFake(t, hubAddr, "node-rd")
	link := waitLink(t, hub, "node-rd")

	waitState(t, fw.snapshot, func(st nodegateway.NodeState) bool {
		return st.Connected && st.NodeID == "node-rd"
	})

	stop() // the disconnect write fails and is held pending
	waitNoLink(t, hub, "node-rd")
	deadline := time.Now().Add(5 * time.Second)
	for fw.disconnectFails.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the failed disconnect was never attempted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	fw.allowDisconnects.Store(true) // the adapter recovers

	st := waitState(t, fw.snapshot, func(st nodegateway.NodeState) bool {
		return !st.Connected && st.NodeID == "node-rd"
	})
	if !st.ConnectedAt.Equal(link.ConnectedAt) {
		t.Fatalf("retry wrote epoch %v, want the dead link's %v", st.ConnectedAt, link.ConnectedAt)
	}
}

// TestInventoryReconnectBeforeRetry: a pending disconnect that lost its
// race against a reconnect must never be written afterwards — the newer
// epoch stays authoritative. The node is stopped fully first, so the
// failed disconnect attempt is guaranteed to happen before any
// replacement link exists.
func TestInventoryReconnectBeforeRetry(t *testing.T) {
	hub, hubAddr := startHub(t)
	fw := &flakyWriter{}
	fw.allowDisconnects.Store(false) // disconnects fail and stay pending
	startInventory(t, hub, fw)
	stop := startFake(t, hubAddr, "node-rr")
	first := waitLink(t, hub, "node-rr")

	waitState(t, fw.snapshot, func(st nodegateway.NodeState) bool {
		return st.Connected && st.ConnectedAt.Equal(first.ConnectedAt)
	})

	stop() // node gone: the disconnect is attempted, not skipped
	waitNoLink(t, hub, "node-rr")
	deadline := time.Now().Add(5 * time.Second)
	for fw.disconnectFails.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the failed disconnect was never attempted")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A replacement node takes the id with a fresh epoch.
	startFake(t, hubAddr, "node-rr")
	second := waitLink(t, hub, "node-rr")
	waitState(t, fw.snapshot, func(st nodegateway.NodeState) bool {
		return st.Connected && st.ConnectedAt.Equal(second.ConnectedAt)
	})

	// Even with the adapter accepting disconnects again, the superseded
	// pending write must not land.
	fw.allowDisconnects.Store(true)
	time.Sleep(300 * time.Millisecond) // several retry ticks
	for i, st := range fw.snapshot() {
		if !st.Connected && st.ConnectedAt.Equal(first.ConnectedAt) {
			t.Fatalf("state %d wrote the superseded disconnect: %+v", i, st)
		}
	}
}

// TestInventoryRunJoinsOnCancel: cancelling Run's context unblocks a poll
// stuck in Info (and the writer and watchers) and Run returns.
func TestInventoryRunJoinsOnCancel(t *testing.T) {
	hub, hubAddr := startHub(t)
	gate := make(chan struct{}) // never released
	node := &slowInfoNode{
		Node: nodefake.New("node-j", t.TempDir()),
		gate: func(ctx context.Context) error {
			select {
			case <-gate:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		},
		entered: make(chan struct{}, 1),
	}
	startNode(t, hubAddr, "node-j", nodeSecret, node)
	waitLink(t, hub, "node-j")

	inv, err := nodegateway.NewInventory(hub, &blockingRecorder{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}, quiet())
	if err != nil {
		t.Fatal(err)
	}
	inv.Interval = 40 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { inv.Run(ctx); close(done) }()

	<-node.entered // a poll is blocked inside Info
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not join its goroutines after cancel")
	}
}

// selfSignedCert writes a throwaway self-signed certificate good for
// 127.0.0.1/localhost and returns its PEM file paths.
func selfSignedCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "bbraft-nodegateway-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

// TestLargeResponsesThroughGateway: a GetProcess answer with two full 8 MiB
// windows (the contract's maximum, 16 MiB in one message) crosses both
// client hops — the gateway client and the hub's link client — whose gRPC
// default receive limit is 4 MiB.
func TestLargeResponsesThroughGateway(t *testing.T) {
	hub, hubAddr := startHub(t)
	startFake(t, hubAddr, "node-a")
	waitLink(t, hub, "node-a")
	client := dialGateway(t, startGateway(t, hub), "node-a")
	ctx := t.Context()

	id := "bx_large001"
	if _, err := client.CreateVM(ctx, &nodev1.CreateVMRequest{SandboxId: id, Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 512, DiskGib: 1}}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}
	const window = 8 << 20
	cmd := "head -c 9000000 /dev/zero; head -c 9000000 /dev/zero >&2"
	if _, err := client.StartProcess(ctx, &nodev1.StartProcessRequest{SandboxId: id, ProcessId: 1, Command: cmd}); err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	if _, err := nodetest.Attach(ctx, client, &nodev1.AttachProcessRequest{SandboxId: id, ProcessId: 1}); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	out, err := client.GetProcess(ctx, &nodev1.GetProcessRequest{SandboxId: id, ProcessId: 1, LimitBytes: window})
	if err != nil {
		t.Fatalf("GetProcess with two 8 MiB windows: %v", err)
	}
	if len(out.Stdout) != window || len(out.Stderr) != window {
		t.Fatalf("got %d stdout and %d stderr bytes, want %d each", len(out.Stdout), len(out.Stderr), window)
	}
}
