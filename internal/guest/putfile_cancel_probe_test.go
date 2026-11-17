package guest_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// These tests observe the guest side of a PutFile cancellation: which event
// reaches the service first (a clean end of stream or a canceled stream
// context) and whether the explicit commit ran. The probe is test-only; the
// production handler is untouched.

// cancelProbeEvent is one sampled guest-side event. ctxErr and ctxDone are
// read from the stream context at that exact moment instead of inferred from a
// callback's scheduling.
type cancelProbeEvent struct {
	name    string
	ctxErr  error
	ctxDone bool
	code    codes.Code
}

// cancelProbeStream wraps one server stream and records its receive and
// response events.
type cancelProbeStream struct {
	grpc.ServerStream
	probe *cancelProbe

	mu      sync.Mutex
	events  []cancelProbeEvent
	notify  chan struct{}
	done    chan struct{}
	handler error
}

// cancelProbe is the stream interceptor state for one harness. Each PutFile
// RPC publishes its stream on streams so the test can wait for its witness.
type cancelProbe struct {
	streams chan *cancelProbeStream

	mu   sync.Mutex
	gate chan struct{}
	once sync.Once
}

// interceptor records every RPC's events and publishes the stream before the
// service handler runs.
func (p *cancelProbe) interceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	sp := &cancelProbeStream{
		ServerStream: ss,
		probe:        p,
		notify:       make(chan struct{}),
		done:         make(chan struct{}),
	}
	p.streams <- sp
	err := handler(srv, sp)
	sp.mu.Lock()
	sp.handler = err
	sp.mu.Unlock()
	sp.record("handler-return", status.Code(err))
	close(sp.done)
	return err
}

// setCommitGate makes the next response send block until releaseCommit, after
// the handler has already committed the file and reached SendAndClose.
func (p *cancelProbe) setCommitGate() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gate = make(chan struct{})
}

// commitGate returns the armed gate, if any.
func (p *cancelProbe) commitGate() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.gate
}

// releaseCommit unblocks a gated response send; it is idempotent and safe to
// register as cleanup before the handler can reach the gate.
func (p *cancelProbe) releaseCommit() {
	p.mu.Lock()
	gate := p.gate
	p.gate = nil
	p.mu.Unlock()
	if gate != nil {
		p.once.Do(func() { close(gate) })
	}
}

// waitStream waits for the next PutFile stream to reach the interceptor.
func (p *cancelProbe) waitStream(t *testing.T) *cancelProbeStream {
	t.Helper()
	select {
	case sp := <-p.streams:
		return sp
	case <-time.After(5 * time.Second):
		t.Fatal("no PutFile stream reached the guest interceptor")
		return nil
	}
}

// record appends one event with an immediate context sample.
func (sp *cancelProbeStream) record(name string, code codes.Code) {
	ctx := sp.Context()
	ev := cancelProbeEvent{
		name:    name,
		ctxErr:  ctx.Err(),
		ctxDone: contextDone(ctx),
		code:    code,
	}
	sp.mu.Lock()
	sp.events = append(sp.events, ev)
	close(sp.notify)
	sp.notify = make(chan struct{})
	sp.mu.Unlock()
}

// contextDone reports whether done is already closed, without blocking.
func contextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// RecvMsg records a clean end of stream separately from a canceled or failed
// receive, sampling the stream context at the instant RecvMsg returns.
func (sp *cancelProbeStream) RecvMsg(m any) error {
	err := sp.ServerStream.RecvMsg(m)
	switch {
	case errors.Is(err, io.EOF):
		sp.record("recv-eof", codes.OK)
	case err != nil:
		sp.record("recv-error", status.Code(err))
	}
	return err
}

// SendMsg is the exact lower-level witness for ClientStreamingServer's
// SendAndClose: the generated wrapper calls this after PutFile's explicit
// commit succeeded. A gate here blocks the response, not the commit.
func (sp *cancelProbeStream) SendMsg(m any) error {
	sp.record("response-send", codes.OK)
	if gate := sp.probe.commitGate(); gate != nil {
		<-gate
	}
	return sp.ServerStream.SendMsg(m)
}

// snapshot copies the recorded events.
func (sp *cancelProbeStream) snapshot() []cancelProbeEvent {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return append([]cancelProbeEvent(nil), sp.events...)
}

// waitFor waits until an event with the given name was recorded, or fails.
func (sp *cancelProbeStream) waitFor(t *testing.T, name string) cancelProbeEvent {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		sp.mu.Lock()
		for _, ev := range sp.events {
			if ev.name == name {
				sp.mu.Unlock()
				return ev
			}
		}
		notify := sp.notify
		sp.mu.Unlock()
		select {
		case <-notify:
		case <-sp.done:
			if ev, ok := sp.find(name); ok {
				return ev
			}
			t.Fatalf("handler finished without a %q event; events: %v", name, sp.snapshot())
		case <-timer.C:
			t.Fatalf("timed out waiting for a %q event; events: %v", name, sp.snapshot())
		}
	}
}

// find reports the first event with the given name.
func (sp *cancelProbeStream) find(name string) (cancelProbeEvent, bool) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	for _, ev := range sp.events {
		if ev.name == name {
			return ev, true
		}
	}
	return cancelProbeEvent{}, false
}

// index reports the position of the first event with the given name, or -1.
func (sp *cancelProbeStream) index(name string) int {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	for i, ev := range sp.events {
		if ev.name == name {
			return i
		}
	}
	return -1
}

// join waits, bounded, for the real PutFile handler to return. Its deferred
// abort cleanup has run by then, so no-commit assertions are final.
func (sp *cancelProbeStream) join(t *testing.T) error {
	t.Helper()
	select {
	case <-sp.done:
		sp.mu.Lock()
		defer sp.mu.Unlock()
		return sp.handler
	case <-time.After(10 * time.Second):
		t.Fatalf("PutFile handler did not return; events: %v", sp.snapshot())
		return nil
	}
}

// newCancelProbeHarness starts the credential-dropped helper service with the
// event interceptor installed.
func newCancelProbeHarness(t *testing.T) (*harness, *cancelProbe) {
	t.Helper()
	base := t.TempDir()
	home := filepath.Join(base, "home/user")
	tmp := filepath.Join(base, "tmp")
	for _, d := range []string{home, tmp} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	fz := &fakeFreezer{}
	probe := &cancelProbe{streams: make(chan *cancelProbeStream, 16)}
	cfg := guest.Config{
		Log:             slog.New(slog.DiscardHandler),
		Version:         "test",
		UID:             os.Getuid(),
		GID:             os.Getgid(),
		NoSetGroups:     true,
		ForceFileHelper: true,
		HomeRoot:        home,
		TmpRoot:         tmp,
		DefaultLease:    2 * time.Minute,
		WatchdogRetry:   5 * time.Second,
		Freezer:         fz,
	}
	svc, err := guest.New(cfg)
	if err != nil {
		t.Fatalf("guest.New: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer(grpc.ChainStreamInterceptor(probe.interceptor))
	svc.Register(g)
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := svc.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &harness{
		t:    t,
		c:    guestv1.NewGuestClient(conn),
		fz:   fz,
		svc:  svc,
		home: home,
		tmp:  tmp,
		uid:  os.Getuid(),
		gid:  os.Getgid(),
	}, probe
}

// TestPutFileCancelProbeCleanEOFCommitsLast proves the server-side ordering
// behind the old racy assertion: when complete data and a clean end of stream
// reach the guest while its context is live, the explicit commit runs before a
// later client cancellation. The client then observes Canceled while the new
// bytes are legitimately on disk.
func TestPutFileCancelProbeCleanEOFCommitsLast(t *testing.T) {
	h, probe := newCancelProbeHarness(t)
	dir := filepath.Join(h.home, "probe")
	fileWrite(t, h.home, "probe/f", "old", 0o644)

	probe.setCommitGate()
	t.Cleanup(probe.releaseCommit)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	st, err := h.c.PutFile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "probe/f"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Data{Data: []byte("new")}}); err != nil {
		t.Fatal(err)
	}
	waitStagedFile(t, dir, 3)

	// Intentionally send a clean end of stream for complete data.
	if err := st.CloseSend(); err != nil {
		t.Fatal(err)
	}
	sp := probe.waitStream(t)

	eof := sp.waitFor(t, "recv-eof")
	if eof.ctxErr != nil || eof.ctxDone {
		t.Fatalf("guest saw a clean EOF only after cancellation: %+v", eof)
	}
	// SendMsg is entered after PutFile's explicit commit succeeded. Await that
	// causal witness, then prove the target already changed before canceling.
	send := sp.waitFor(t, "response-send")
	if send.ctxErr != nil || send.ctxDone {
		t.Fatalf("commit/response witness ran after cancellation: %+v", send)
	}
	if got, err := os.ReadFile(filepath.Join(h.home, "probe/f")); err != nil || string(got) != "new" {
		t.Fatalf("target before cancel = %q, %v; want the committed bytes", got, err)
	}

	cancel()
	// Observe the client's canceled status before releasing the gated
	// response, so releasing cannot let the server response race cancellation.
	var resp nodev1.PutFileResponse
	if err := st.RecvMsg(&resp); status.Code(err) != codes.Canceled {
		t.Fatalf("client after cancel = %v, want Canceled", err)
	}
	probe.releaseCommit()
	if err := sp.join(t); err != nil && status.Code(err) != codes.Canceled {
		t.Fatalf("handler returned %v after the gated response send", err)
	}
	if got, err := os.ReadFile(filepath.Join(h.home, "probe/f")); err != nil || string(got) != "new" {
		t.Fatalf("committed target after cancel = %q, %v; want the new bytes", got, err)
	}
	order := []string{"recv-eof", "response-send", "handler-return"}
	last := -1
	for _, name := range order {
		i := sp.index(name)
		if i <= last {
			t.Fatalf("event %q is missing or out of order; events: %v", name, sp.snapshot())
		}
		last = i
	}
	assertNoTempFiles(t, h.home)
}

// TestPutFileCancelProbeBeforeLastChunk proves the required cancellation
// contract: a client that cancels before sending all data, without ever
// sending a clean end of stream, reaches the guest as cancellation. The
// staging file is discarded and the old target stays, with no commit witness.
func TestPutFileCancelProbeBeforeLastChunk(t *testing.T) {
	h, probe := newCancelProbeHarness(t)
	dir := filepath.Join(h.home, "probe")
	fileWrite(t, h.home, "probe/f", "old", 0o644)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	st, err := h.c.PutFile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "probe/f"},
	}}); err != nil {
		t.Fatal(err)
	}
	waitStagedFile(t, dir, 0)
	// The client intended "new" but sends only its first two bytes: the last
	// chunk is still outstanding when it cancels.
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Data{Data: []byte("ne")}}); err != nil {
		t.Fatal(err)
	}
	waitStagedFile(t, dir, 2)
	sp := probe.waitStream(t)

	// No CloseSend: cancellation is the only terminal event the guest sees.
	cancel()
	var resp nodev1.PutFileResponse
	if err := st.RecvMsg(&resp); status.Code(err) != codes.Canceled {
		t.Fatalf("client after cancel = %v, want Canceled", err)
	}
	if err := sp.join(t); status.Code(err) != codes.Canceled {
		t.Fatalf("handler returned %v, want Canceled", err)
	}
	if ev, ok := sp.find("recv-eof"); ok {
		t.Fatalf("guest saw a clean EOF on a canceled stream: %+v", ev)
	}
	if ev, ok := sp.find("response-send"); ok {
		t.Fatalf("guest sent a response (commit witness) on a canceled stream: %+v", ev)
	}
	recv, ok := sp.find("recv-error")
	if !ok || recv.ctxErr == nil || !recv.ctxDone {
		t.Fatalf("guest cancellation was not observed as a canceled stream context: %+v", recv)
	}
	if got, err := os.ReadFile(filepath.Join(h.home, "probe/f")); err != nil || string(got) != "old" {
		t.Fatalf("canceled target = %q, %v; want the old bytes", got, err)
	}
	assertNoTempFiles(t, h.home)
}

// oldPatternVerdict judges the historical cancel-then-CloseAndRecv
// observation against the commit contract. A changed target is only
// acceptable when the guest saw a clean end of stream while its context was
// still live; response-send is diagnostic, not required, because cancellation
// can interrupt the response after the rename. A response witness with an
// unchanged target is never acceptable, and so is a committed target without
// clean pre-cancellation EOF evidence.
func oldPatternVerdict(target string, eof cancelProbeEvent, sawEOF, sawResponse bool) error {
	switch target {
	case "new":
		if !sawEOF {
			return errors.New("target was committed without clean EOF evidence")
		}
		if eof.ctxErr != nil || eof.ctxDone {
			return errors.New("target was committed with a canceled EOF sample")
		}
	case "old":
		if sawResponse {
			return errors.New("response/commit witness ran but the target stayed old")
		}
	default:
		return fmt.Errorf("target = %q, want old or new", target)
	}
	return nil
}

// TestPutFileCancelProbeOldPatternVerdict pins the old-pattern verdict without
// transport nondeterminism: a committed target needs clean pre-cancel EOF
// evidence even if the response witness is absent, while a canceled or absent
// EOF never justifies a committed target.
func TestPutFileCancelProbeOldPatternVerdict(t *testing.T) {
	clean := cancelProbeEvent{name: "recv-eof"}
	canceled := cancelProbeEvent{name: "recv-eof", ctxErr: context.Canceled, ctxDone: true}
	for _, tc := range []struct {
		name        string
		target      string
		eof         cancelProbeEvent
		sawEOF      bool
		sawResponse bool
		wantErr     bool
	}{
		{"committed with clean EOF and no response", "new", clean, true, false, false},
		{"committed with clean EOF and response", "new", clean, true, true, false},
		{"committed without EOF", "new", clean, false, false, true},
		{"committed with canceled EOF", "new", canceled, true, false, true},
		{"aborted without response", "old", cancelProbeEvent{}, false, false, false},
		{"aborted with response", "old", cancelProbeEvent{}, false, true, true},
		{"unexpected target", "other", clean, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := oldPatternVerdict(tc.target, tc.eof, tc.sawEOF, tc.sawResponse)
			if (err != nil) != tc.wantErr {
				t.Fatalf("oldPatternVerdict(%q, %v, %v, %v) = %v, wantErr %v",
					tc.target, tc.eof, tc.sawEOF, tc.sawResponse, err, tc.wantErr)
			}
		})
	}
}

// TestPutFileCancelProbeOldCloseRecvOrder keeps the historical
// cancel-then-CloseAndRecv pattern under observation. Either outcome is
// decided by which terminal event reaches the guest first; the test never
// accepts a committed target without clean, pre-cancellation EOF evidence.
// The stress matrix runs this test with -count in a throwaway VM.
func TestPutFileCancelProbeOldCloseRecvOrder(t *testing.T) {
	h, probe := newCancelProbeHarness(t)
	fileWrite(t, h.home, "probe/f", "old", 0o644)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	st, err := h.c.PutFile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "probe/f"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Data{Data: []byte("new")}}); err != nil {
		t.Fatal(err)
	}
	waitStagedFile(t, filepath.Join(h.home, "probe"), 3)
	sp := probe.waitStream(t)

	// The old fixture's exact pattern: cancel, then half-close and receive.
	cancel()
	_, clientErr := st.CloseAndRecv()
	if err := sp.join(t); err != nil && status.Code(err) != codes.Canceled {
		t.Fatalf("handler returned %v", err)
	}
	got, readErr := os.ReadFile(filepath.Join(h.home, "probe/f"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	eof, sawEOF := sp.find("recv-eof")
	_, sawResponse := sp.find("response-send")
	t.Logf("old pattern: client=%v target=%q eof=%+v sawEOF=%v sawResponse=%v events=%v",
		clientErr, got, eof, sawEOF, sawResponse, sp.snapshot())

	if err := oldPatternVerdict(string(got), eof, sawEOF, sawResponse); err != nil {
		t.Fatalf("%v; events: %v", err, sp.snapshot())
	}
	assertNoTempFiles(t, h.home)
}
