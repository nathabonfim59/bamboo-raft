package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/nodetest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// execProbeGuest blocks until its context ends, optionally sending one event
// first.
type execProbeGuest struct {
	guestv1.UnimplementedGuestServer
	sendFirst bool
	canceled  chan struct{}
}

func (g *execProbeGuest) Ping(context.Context, *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	return &guestv1.PingResponse{Version: "probe"}, nil
}

func (g *execProbeGuest) Exec(_ *guestv1.ExecRequest, stream grpc.ServerStreamingServer[nodev1.ExecEvent]) error {
	if g.sendFirst {
		// A valid prefix lets the test reach the node's downstream Send
		// failure instead of tripping frame validation.
		if err := stream.Send(&nodev1.ExecEvent{Event: &nodev1.ExecEvent_Started_{Started: &nodev1.ExecEvent_Started{Pid: 1}}}); err != nil {
			close(g.canceled)
			return err
		}
		if err := stream.Send(&nodev1.ExecEvent{Event: &nodev1.ExecEvent_Stdout{Stdout: []byte("x")}}); err != nil {
			close(g.canceled)
			return err
		}
	}
	<-stream.Context().Done()
	close(g.canceled)
	return status.FromContextError(stream.Context().Err()).Err()
}

// putProbeGuest blocks in PutFile until the stream context ends.
type putProbeGuest struct {
	guestv1.UnimplementedGuestServer
	received chan struct{}
	canceled chan struct{}
}

func (g *putProbeGuest) Ping(context.Context, *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	return &guestv1.PingResponse{Version: "probe"}, nil
}

func (g *putProbeGuest) PutFile(stream grpc.ClientStreamingServer[guestv1.PutFileRequest, nodev1.PutFileResponse]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	close(g.received)
	<-stream.Context().Done()
	close(g.canceled)
	return status.FromContextError(stream.Context().Err()).Err()
}

// scriptedExecGuest emits a fixed frame sequence. For malformed sequences it
// waits for the node to cancel the operation, so the test sees the cancel.
type scriptedExecGuest struct {
	guestv1.UnimplementedGuestServer
	events     []*nodev1.ExecEvent
	waitCancel bool
	canceled   chan struct{}
	once       sync.Once
}

func (g *scriptedExecGuest) Ping(context.Context, *guestv1.PingRequest) (*guestv1.PingResponse, error) {
	return &guestv1.PingResponse{Version: "scripted"}, nil
}

func (g *scriptedExecGuest) signalCanceled() { g.once.Do(func() { close(g.canceled) }) }

func (g *scriptedExecGuest) Exec(_ *guestv1.ExecRequest, stream grpc.ServerStreamingServer[nodev1.ExecEvent]) error {
	for _, ev := range g.events {
		if err := stream.Send(ev); err != nil {
			g.signalCanceled()
			return err
		}
	}
	if g.waitCancel {
		<-stream.Context().Done()
		g.signalCanceled()
		return status.FromContextError(stream.Context().Err()).Err()
	}
	return nil
}

func execStarted() *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Started_{Started: &nodev1.ExecEvent_Started{Pid: 1}}}
}

func execOut(text string) *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Stdout{Stdout: []byte(text)}}
}

func execErr(text string) *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Stderr{Stderr: []byte(text)}}
}

func execExit(code *int32, timedOut bool) *nodev1.ExecEvent {
	return &nodev1.ExecEvent{Event: &nodev1.ExecEvent_Exit_{Exit: &nodev1.ExecEvent_Exit{
		ExitCode: code, TimedOut: timedOut,
	}}}
}

// validExecPrefix re-checks captured frames independently of the production
// validator: a malformed stream may have forwarded a valid prefix, never a
// bad frame.
func validExecPrefix(events []*nodev1.ExecEvent) bool {
	started, exited := false, false
	for _, ev := range events {
		if ev == nil || ev.GetEvent() == nil || exited {
			return false
		}
		switch ev.GetEvent().(type) {
		case *nodev1.ExecEvent_Started_:
			if started {
				return false
			}
			started = true
		case *nodev1.ExecEvent_Stdout, *nodev1.ExecEvent_Stderr:
			if !started {
				return false
			}
		case *nodev1.ExecEvent_Exit_:
			if !started {
				return false
			}
			exited = true
		}
	}
	return true
}

type testExecStream struct {
	grpc.ServerStream
	ctx       context.Context
	sendErr   error
	failAfter int // forwarded frames allowed before sendErr applies
	events    []*nodev1.ExecEvent
}

func (s *testExecStream) Context() context.Context { return s.ctx }

func (s *testExecStream) Send(ev *nodev1.ExecEvent) error {
	if s.sendErr != nil && len(s.events) >= s.failAfter {
		return s.sendErr
	}
	s.events = append(s.events, ev)
	return nil
}

// useGuest installs a guest for the next launches, replacing the harness's
// default fakeGuest factory.
func useGuest[T guestv1.GuestServer](h *harness, g T) {
	h.launcher.mu.Lock()
	onStart := h.launcher.onStart
	h.launcher.onStart = func(cfg qemu.Config) {
		onStart(cfg)
		h.dialer.register(cfg.VsockCID, g)
	}
	h.launcher.mu.Unlock()
}

func TestExecSendFailureCancelsGuestCommand(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	g := &execProbeGuest{sendFirst: true, canceled: make(chan struct{})}
	useGuest(h, g)
	id := ids.Sandbox()
	h.create(id, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err := h.mg.Exec(&nodev1.ExecRequest{SandboxId: id, Command: "sleep 100"}, &testExecStream{
		ctx:       ctx,
		sendErr:   errors.New("client went away"),
		failAfter: 1, // let valid started through, fail on the stdout Send
	})
	if err == nil {
		t.Fatal("Exec reported success after a failed Send")
	}
	select {
	case <-g.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("guest command was not canceled after the node stream failed")
	}
}

func TestExecValidatesGuestFrameOrder(t *testing.T) {
	zero := int32(0)
	cases := []struct {
		name       string
		events     []*nodev1.ExecEvent
		waitCancel bool
		wantCancel bool
		wantErr    bool
	}{
		{"valid", []*nodev1.ExecEvent{execStarted(), execOut("a"), execErr("b"), execExit(&zero, false)}, false, false, false},
		{"timeout exit without code", []*nodev1.ExecEvent{execStarted(), execExit(nil, true)}, false, false, false},
		{"output before started", []*nodev1.ExecEvent{execOut("x")}, true, true, true},
		{"stderr before started", []*nodev1.ExecEvent{execErr("x")}, true, true, true},
		{"duplicate started", []*nodev1.ExecEvent{execStarted(), execStarted()}, true, true, true},
		{"duplicate exit", []*nodev1.ExecEvent{execStarted(), execExit(&zero, false), execExit(&zero, false)}, true, true, true},
		{"output after exit", []*nodev1.ExecEvent{execStarted(), execExit(&zero, false), execOut("late")}, true, true, true},
		{"empty event", []*nodev1.ExecEvent{execStarted(), {Event: nil}}, true, true, true},
		{"eof without exit", []*nodev1.ExecEvent{execStarted(), execOut("x")}, false, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			guest := &scriptedExecGuest{
				events:     tc.events,
				waitCancel: tc.waitCancel,
				canceled:   make(chan struct{}),
			}
			h := newHarness(t, harnessOptions{fallback: guest, stubGuest: true})
			id := ids.Sandbox()
			h.create(id, nil)
			// Bound each case: if frame validation regresses, the scripted
			// malformed guest waits for cancellation and the case must fail
			// with a wrong status instead of hanging the package.
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			stream := &testExecStream{ctx: ctx}
			err := h.mg.Exec(&nodev1.ExecRequest{SandboxId: id, Command: "true"}, stream)
			if tc.wantErr {
				if status.Code(err) != codes.Internal {
					t.Fatalf("Exec = %v, want Internal protocol error", err)
				}
			} else if err != nil {
				t.Fatalf("Exec = %v", err)
			}
			if !validExecPrefix(stream.events) {
				t.Fatalf("forwarded an invalid frame order: %v", stream.events)
			}
			if !tc.wantErr && len(stream.events) != len(tc.events) {
				t.Fatalf("forwarded %d frames, want %d", len(stream.events), len(tc.events))
			}
			if tc.wantCancel {
				select {
				case <-guest.canceled:
				case <-time.After(5 * time.Second):
					t.Fatal("guest operation was not canceled after a protocol error")
				}
			}
		})
	}
}

func TestExecUnknownAndNotReady(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	err := h.mg.Exec(&nodev1.ExecRequest{SandboxId: ids.Sandbox(), Command: "true"}, &testExecStream{ctx: t.Context()})
	wantCode(t, err, codes.NotFound)

	launcher := &fakeLauncher{failOnce: errors.New("no boot")}
	h2 := newHarness(t, harnessOptions{launcher: launcher})
	id := ids.Sandbox()
	if _, err := h2.mg.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: id,
		Resources: &nodev1.Resources{Vcpus: 1, MemoryMib: 128, DiskGib: 1},
	}); err == nil {
		t.Fatal("expected boot failure")
	}
	err = h2.mg.Exec(&nodev1.ExecRequest{SandboxId: id, Command: "true"}, &testExecStream{ctx: t.Context()})
	wantCode(t, err, codes.FailedPrecondition)
}

// startNodeClient serves the manager over bufconn and returns a node client.
// The server's send limit and the client's receive limit are the production
// response cap; request limits stay at gRPC's default.
func startNodeClient(t *testing.T, h *harness) nodev1.NodeClient {
	t.Helper()
	lis := bufconn.Listen(4 << 20)
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(nodelink.MaxResponseBytes))
	h.mg.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(
		"passthrough:///node",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(nodelink.MaxResponseBytes)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return nodev1.NewNodeClient(conn)
}

func TestPutFileRejectsMalformedStreams(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	client := startNodeClient(t, h)
	ctx := t.Context()
	header := func() *nodev1.PutFileRequest {
		return &nodev1.PutFileRequest{Part: &nodev1.PutFileRequest_Header_{Header: &nodev1.PutFileRequest_Header{
			SandboxId: id, Path: "f",
		}}}
	}

	cases := []struct {
		name string
		send func(st grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse]) error
	}{
		{"missing header", func(st grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
			return nil // CloseAndRecv with no messages
		}},
		{"data first", func(st grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
			return st.Send(&nodev1.PutFileRequest{Part: &nodev1.PutFileRequest_Data{Data: []byte("x")}})
		}},
		{"duplicate header", func(st grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
			if err := st.Send(header()); err != nil {
				return err
			}
			return st.Send(header())
		}},
		{"empty part", func(st grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
			if err := st.Send(header()); err != nil {
				return err
			}
			return st.Send(&nodev1.PutFileRequest{})
		}},
		{"oversized chunk", func(st grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse]) error {
			if err := st.Send(header()); err != nil {
				return err
			}
			return st.Send(&nodev1.PutFileRequest{Part: &nodev1.PutFileRequest_Data{
				Data: make([]byte, MaxPutFileChunk+1),
			}})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := client.PutFile(ctx)
			if err != nil {
				t.Fatal(err)
			}
			sendErr := tc.send(st)
			_, err = st.CloseAndRecv()
			if err == nil && sendErr != nil {
				err = sendErr
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("got %v, want InvalidArgument (send error: %v)", err, sendErr)
			}
		})
	}
}

func TestPutFileForwardsChunks(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	id := ids.Sandbox()
	h.create(id, nil)
	client := startNodeClient(t, h)
	st, err := client.PutFile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&nodev1.PutFileRequest{Part: &nodev1.PutFileRequest_Header_{Header: &nodev1.PutFileRequest_Header{
		SandboxId: id, Path: "dir/file", Mode: 0o755,
	}}}); err != nil {
		t.Fatal(err)
	}
	want := ""
	for i := range 4 {
		chunk := []byte(fmt.Sprintf("chunk-%d;", i))
		want += string(chunk)
		if err := st.Send(&nodev1.PutFileRequest{Part: &nodev1.PutFileRequest_Data{Data: chunk}}); err != nil {
			t.Fatal(err)
		}
	}
	resp, err := st.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPath() != "/home/user/dir/file" || resp.GetSize() != int64(len(want)) {
		t.Fatalf("response %v", resp)
	}
	res, err := nodetest.Exec(t.Context(), client, &nodev1.ExecRequest{SandboxId: id, Command: "cat dir/file"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Stdout != want {
		t.Fatalf("read back %q, want %q", res.Stdout, want)
	}
}

func TestPutFileCancelPropagatesToGuest(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	g := &putProbeGuest{received: make(chan struct{}), canceled: make(chan struct{})}
	useGuest(h, g)
	id := ids.Sandbox()
	h.create(id, nil)
	client := startNodeClient(t, h)
	ctx, cancel := context.WithCancel(t.Context())
	st, err := client.PutFile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&nodev1.PutFileRequest{Part: &nodev1.PutFileRequest_Header_{Header: &nodev1.PutFileRequest_Header{
		SandboxId: id, Path: "file",
	}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-g.received:
	case <-time.After(5 * time.Second):
		t.Fatal("guest never received the forwarded header")
	}
	cancel()
	_, _ = st.CloseAndRecv()
	select {
	case <-g.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("guest upload was not canceled with the client")
	}
}
