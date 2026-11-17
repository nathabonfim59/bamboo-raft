package nodefake_test

import (
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func TestConformance(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	fake := nodefake.New("fake-1", t.TempDir())
	fake.BootDelay = 300 * time.Millisecond // a real window for the DeleteVM fence cases
	fake.Register(srv)
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)

	cc, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	nodetest.Run(t, nodev1.NewNodeClient(cc), nodetest.Options{Phase1: nodetest.AllPhase1})
}
