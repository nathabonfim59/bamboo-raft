package node

import (
	"context"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
)

// The conformance tests run the frozen nodetest suite against the real
// Manager with a fake launcher and the real internal/guest Service on the
// host. The fake qcow2 disk is not connected to the guest HOME used by those
// host commands, so a Snapshot child would see an empty HOME rather than the
// saved data. Keep this host harness on the non-Snapshot subset:
//
//	BBRAFT_TEST_FAKE_NODETEST=1 go test ./internal/node \
//	  -run 'TestConformance(Direct|Link)/(Info|Lifecycle|UnknownSandbox|DeleteFences.*|Exec.*|PutFile.*)$'
//
// The real full Snapshot conformance gate is KVM (BBRAFT_TEST_KVM=1 with a
// built image). Free space is injected here, so no large temporary filesystem
// is needed. The fence cases are part of the host non-Snapshot suite and must
// not be filtered out.
func requireFakeConformance(t *testing.T) {
	t.Helper()
	if os.Getenv("BBRAFT_TEST_FAKE_NODETEST") != "1" {
		t.Skip("host fake harness excludes Snapshot (fake disk does not back guest HOME); set BBRAFT_TEST_FAKE_NODETEST=1 and -run 'TestConformance(Direct|Link)/(Info|Lifecycle|UnknownSandbox|DeleteFences.*|Exec.*|PutFile.*)$'; real Snapshot gate is KVM")
	}
}

func TestConformanceDirect(t *testing.T) {
	requireFakeConformance(t)
	h := newHarness(t, harnessOptions{baseGiB: 10, bootTimeout: 2 * time.Minute})
	client := startNodeClient(t, h)
	nodetest.Run(t, client, nodetest.Options{FenceWatch: time.Second})
}

func TestConformanceLink(t *testing.T) {
	requireFakeConformance(t)
	h := newHarness(t, harnessOptions{baseGiB: 10, bootTimeout: 2 * time.Minute})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.DiscardHandler)
	hub := nodelink.NewHub(func(context.Context, nodelink.Hello) error { return nil }, log)
	hubCtx, hubCancel := context.WithCancel(context.Background())
	hubDone := make(chan struct{})
	go func() {
		_ = hub.Serve(hubCtx, ln)
		close(hubDone)
	}()
	t.Cleanup(func() {
		hubCancel()
		<-hubDone
	})

	srv := grpc.NewServer(nodelink.ServerOptions()...)
	h.mg.Register(srv)
	nodeCtx, nodeCancel := context.WithCancel(context.Background())
	nodeDone := make(chan struct{})
	go func() {
		_ = nodelink.Serve(nodeCtx, nodelink.DialConfig{
			Addr:       ln.Addr().String(),
			NodeID:     "node-test",
			Token:      "test-token",
			Log:        log,
			MinBackoff: 20 * time.Millisecond,
			MaxBackoff: 50 * time.Millisecond,
		}, srv)
		close(nodeDone)
	}()
	t.Cleanup(func() {
		nodeCancel()
		<-nodeDone
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	link, err := hub.Wait(ctx, "node-test")
	if err != nil {
		t.Fatalf("node never connected: %v", err)
	}
	nodetest.Run(t, link.Client, nodetest.Options{})
}
