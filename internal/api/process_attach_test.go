package api_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const processAttachTestProcessID = uint32(7)

// processAttachTestUUID parses a test account id.
func processAttachTestUUID(value string) uuid.UUID { return uuid.MustParse(value) }

// processAttachTestClient scripts AttachProcess and KillProcess and records
// every call, so a test can prove the RPC path ran (or did not).
type processAttachTestClient struct {
	nodev1.NodeClient
	mu          sync.Mutex
	attach      func(context.Context, *nodev1.AttachProcessRequest) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error)
	kill        func(context.Context, *nodev1.KillProcessRequest) (*nodev1.KillProcessResponse, error)
	attachCalls int
	killCalls   int
	attachReqs  []*nodev1.AttachProcessRequest
	killReqs    []*nodev1.KillProcessRequest
}

func (c *processAttachTestClient) AttachProcess(ctx context.Context, request *nodev1.AttachProcessRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[nodev1.ProcessEvent], error) {
	c.mu.Lock()
	c.attachCalls++
	c.attachReqs = append(c.attachReqs, request)
	attach := c.attach
	c.mu.Unlock()
	if attach == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return attach(ctx, request)
}

func (c *processAttachTestClient) KillProcess(ctx context.Context, request *nodev1.KillProcessRequest, _ ...grpc.CallOption) (*nodev1.KillProcessResponse, error) {
	c.mu.Lock()
	c.killCalls++
	c.killReqs = append(c.killReqs, request)
	kill := c.kill
	c.mu.Unlock()
	if kill == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return kill(ctx, request)
}

func (c *processAttachTestClient) counts() (attach, kill int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.attachCalls, c.killCalls
}

func (c *processAttachTestClient) attachRequest() (*nodev1.AttachProcessRequest, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.attachReqs) == 0 {
		return nil, false
	}
	return c.attachReqs[len(c.attachReqs)-1], true
}

func (c *processAttachTestClient) killRequest() (*nodev1.KillProcessRequest, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.killReqs) == 0 {
		return nil, false
	}
	return c.killReqs[len(c.killReqs)-1], true
}

// processAttachTestStream delivers prepared events, then blocks or returns
// finalErr. recvDone closes when Recv returns for the last time; blocked
// closes as soon as Recv has to wait, the controlled receiver barrier timer
// tests use instead of sleeping.
type processAttachTestStream struct {
	grpc.ClientStream
	ctx       context.Context
	mu        sync.Mutex
	events    []*nodev1.ProcessEvent
	finalErr  error
	block     bool
	release   <-chan struct{}
	index     int
	blocked   chan struct{}
	blockOnce sync.Once
	done      chan struct{}
	doneOnce  sync.Once
}

func newProcessAttachTestStream(ctx context.Context, events []*nodev1.ProcessEvent, finalErr error, block bool) *processAttachTestStream {
	return &processAttachTestStream{
		ctx:      ctx,
		events:   events,
		finalErr: finalErr,
		block:    block,
		blocked:  make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (s *processAttachTestStream) Context() context.Context { return s.ctx }

func (s *processAttachTestStream) Recv() (*nodev1.ProcessEvent, error) {
	s.mu.Lock()
	if s.index < len(s.events) {
		event := s.events[s.index]
		s.index++
		s.mu.Unlock()
		return event, nil
	}
	block, release, finalErr := s.block, s.release, s.finalErr
	s.mu.Unlock()
	if block {
		s.blockOnce.Do(func() { close(s.blocked) })
		if release != nil {
			select {
			case <-s.ctx.Done():
				s.finish()
				return nil, s.ctx.Err()
			case <-release:
			}
		} else {
			<-s.ctx.Done()
			s.finish()
			return nil, s.ctx.Err()
		}
	}
	s.finish()
	if finalErr != nil {
		return nil, finalErr
	}
	return nil, io.EOF
}

func (s *processAttachTestStream) finish() {
	s.doneOnce.Do(func() { close(s.done) })
}

func processAttachTestAttached(processID uint32, offset int64) *nodev1.ProcessEvent {
	return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Attached_{Attached: &nodev1.ProcessEvent_Attached{
		Process:      processTestRunningProcess(processID, "attached-command"),
		StdoutOffset: offset,
		StderrOffset: offset,
	}}}
}

func processAttachTestAttachedProcess(processID uint32, stdoutOffset, stderrOffset int64, process *nodev1.Process) *nodev1.ProcessEvent {
	return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Attached_{Attached: &nodev1.ProcessEvent_Attached{
		Process:      process,
		StdoutOffset: stdoutOffset,
		StderrOffset: stderrOffset,
	}}}
}

func processAttachTestChunk(kind api.CommandAttachFrameType, data []byte, nextOffset int64) *nodev1.ProcessEvent {
	chunk := &nodev1.ProcessEvent_Chunk{Data: data, NextOffset: nextOffset}
	if kind == api.CommandAttachStderr {
		return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Stderr{Stderr: chunk}}
	}
	return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Stdout{Stdout: chunk}}
}

func processAttachTestExit(processID uint32, code *int32, signal string) *nodev1.ProcessEvent {
	process := processTestExitedProcess(processID, "attached-command", code, signal)
	// Sizes may lag the output already delivered; the terminal size guard
	// only rejects a native size larger than the raw cursor.
	process.StdoutSize = 0
	process.StderrSize = 0
	return &nodev1.ProcessEvent{Event: &nodev1.ProcessEvent_Exit{Exit: process}}
}

func processAttachTestLookup() *commandTestLookup {
	return commandTestWithReadyLookup(processTestNodeID)
}

func processAttachTestHandler(t *testing.T, store api.CommandStore, client nodev1.NodeClient, opts ...func(*api.CommandAttachOptions)) http.Handler {
	t.Helper()
	options := api.CommandAttachOptions{
		CommandsOptions: api.CommandsOptions{
			Lookup:  processAttachTestLookup(),
			Clients: processTestFactory(client),
			Store:   store,
			Logger:  commandTestLogger(),
		},
		Heartbeat:        time.Second,
		ReconnectAfter:   10 * time.Second,
		WriteTimeout:     100 * time.Millisecond,
		BootstrapTimeout: time.Second,
	}
	for _, opt := range opts {
		opt(&options)
	}
	handler, err := api.NewCommandAttachHandler(options)
	if err != nil {
		t.Fatalf("NewCommandAttachHandler: %v", err)
	}
	return handler
}

func processAttachTestStopHandler(t *testing.T, store api.CommandStore, client nodev1.NodeClient, opts ...func(*api.CommandsOptions)) http.Handler {
	t.Helper()
	options := api.CommandsOptions{
		Lookup:  processAttachTestLookup(),
		Clients: processTestFactory(client),
		Store:   store,
		Logger:  commandTestLogger(),
	}
	for _, opt := range opts {
		opt(&options)
	}
	handler, err := api.NewCommandStopHandler(options)
	if err != nil {
		t.Fatalf("NewCommandStopHandler: %v", err)
	}
	return handler
}

func processAttachTestTarget(sandboxID, processID, query string) string {
	target := "/api/v1/sandboxes/" + sandboxID + "/commands/" + processID + "/stream"
	if query != "" {
		target += "?" + query
	}
	return target
}

func processAttachTestServe(t *testing.T, handler http.Handler, owner, sandboxID, processID, query string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET "+api.CommandAttachPath, handler)
	request := httptest.NewRequest(http.MethodGet, processAttachTestTarget(sandboxID, processID, query), nil)
	if owner != "" {
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner, KeyID: "key_test"}))
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return recorder
}

// processAttachTestWriter is an http.ResponseWriter whose frame writes are
// observable through a channel. Tests assert on frames, not on sleeps.
type processAttachTestWriter struct {
	mu        sync.Mutex
	header    http.Header
	status    int
	body      bytes.Buffer
	wrote     chan struct{}
	deadlines []time.Time
}

func newProcessAttachTestWriter() *processAttachTestWriter {
	return &processAttachTestWriter{header: http.Header{}, wrote: make(chan struct{}, 1)}
}

func (w *processAttachTestWriter) Header() http.Header { return w.header }

func (w *processAttachTestWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = status
	}
}

func (w *processAttachTestWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.body.Write(data)
	w.mu.Unlock()
	select {
	case w.wrote <- struct{}{}:
	default:
	}
	return len(data), nil
}

func (w *processAttachTestWriter) FlushError() error { return nil }

func (w *processAttachTestWriter) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	w.deadlines = append(w.deadlines, deadline)
	w.mu.Unlock()
	return nil
}

func (w *processAttachTestWriter) snapshot() (int, []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status, bytes.Clone(w.body.Bytes())
}

// headerSnapshot is safe to call after the handler returned. Header itself is
// written by the handler without the writer lock, so tests must not clone it
// while frames are still being produced.
func (w *processAttachTestWriter) headerSnapshot() http.Header {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.header.Clone()
}

func (w *processAttachTestWriter) frames(t *testing.T) []api.CommandAttachFrame {
	t.Helper()
	_, body := w.snapshot()
	return processAttachTestParseFrames(t, body)
}

func (w *processAttachTestWriter) waitType(t *testing.T, kind api.CommandAttachFrameType, occurrence int) api.CommandAttachFrame {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		frames := w.frames(t)
		found := 0
		for _, frame := range frames {
			if frame.Type != kind {
				continue
			}
			found++
			if found == occurrence {
				return frame
			}
		}
		select {
		case <-w.wrote:
		case <-deadline.C:
			t.Fatalf("stream has %d %s frames, want occurrence %d\nbody: %s", found, kind, occurrence, w.rawBody())
		}
	}
}

func (w *processAttachTestWriter) waitDeadlineCleared(t *testing.T) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		w.mu.Lock()
		cleared := len(w.deadlines) == 0 || w.deadlines[len(w.deadlines)-1].IsZero()
		w.mu.Unlock()
		if cleared {
			return
		}
		select {
		case <-w.wrote:
		case <-deadline.C:
			t.Fatal("write deadline was never cleared")
		}
	}
}

func (w *processAttachTestWriter) rawBody() []byte {
	_, body := w.snapshot()
	return body
}

// processAttachTestClock is a controllable clock for the absolute reconnect
// deadline. Only that comparison uses it; heartbeat timers and write deadlines
// stay on the real clock.
type processAttachTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newProcessAttachTestClock() *processAttachTestClock {
	return &processAttachTestClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *processAttachTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *processAttachTestClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// processAttachGateWriter blocks one chosen frame write until release closes.
// It gives timer tests a receiver barrier without sleeping for ordering.
type processAttachGateWriter struct {
	inner   *processAttachTestWriter
	gateAt  int
	release <-chan struct{}
	blocked chan struct{}
	mu      sync.Mutex
	writes  int
	once    sync.Once
}

func newProcessAttachGateWriter(gateAt int, release <-chan struct{}) *processAttachGateWriter {
	return &processAttachGateWriter{
		inner:   newProcessAttachTestWriter(),
		gateAt:  gateAt,
		release: release,
		blocked: make(chan struct{}),
	}
}

func (w *processAttachGateWriter) Header() http.Header { return w.inner.Header() }

func (w *processAttachGateWriter) WriteHeader(status int) { w.inner.WriteHeader(status) }

func (w *processAttachGateWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	w.writes++
	atGate := w.writes == w.gateAt
	w.mu.Unlock()
	if atGate {
		w.once.Do(func() { close(w.blocked) })
		<-w.release
	}
	return w.inner.Write(data)
}

func (w *processAttachGateWriter) FlushError() error { return w.inner.FlushError() }

func (w *processAttachGateWriter) SetWriteDeadline(deadline time.Time) error {
	return w.inner.SetWriteDeadline(deadline)
}

// processAttachTestBegin starts one attach request in the test goroutine's
// lifetime and returns its writer plus a done channel.
func processAttachTestBegin(t *testing.T, handler http.Handler, ctx context.Context, owner, sandboxID, processID, query string) (*processAttachTestWriter, <-chan struct{}) {
	t.Helper()
	writer := newProcessAttachTestWriter()
	done := processAttachTestBeginWriter(t, handler, ctx, owner, sandboxID, processID, query, writer)
	return writer, done
}

// processAttachTestBeginWriter is processAttachTestBegin with a caller-owned
// writer, for gate/failure writers.
func processAttachTestBeginWriter(t *testing.T, handler http.Handler, ctx context.Context, owner, sandboxID, processID, query string, writer http.ResponseWriter) <-chan struct{} {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET "+api.CommandAttachPath, handler)
	request := httptest.NewRequest(http.MethodGet, processAttachTestTarget(sandboxID, processID, query), nil)
	childCtx, cancel := context.WithCancel(ctx)
	request = request.WithContext(childCtx)
	if owner != "" {
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner, KeyID: "key_test"}))
	}
	done := make(chan struct{})
	go func() {
		mux.ServeHTTP(writer, request)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("attach handler did not return during cleanup")
		}
	})
	return done
}

func processAttachTestParseFrames(t *testing.T, body []byte) []api.CommandAttachFrame {
	t.Helper()
	var frames []api.CommandAttachFrame
	for _, line := range bytes.Split(body, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var frame api.CommandAttachFrame
		if err := json.Unmarshal(line, &frame); err != nil {
			t.Fatalf("decode attach frame %s: %v", line, err)
		}
		frames = append(frames, frame)
	}
	return frames
}

func processAttachTestValidate(t *testing.T, status int, header http.Header, body []byte) {
	t.Helper()
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200\nbody: %s", status, body)
	}
	if got := header.Get("Content-Type"); got != "application/x-ndjson" {
		t.Fatalf("Content-Type = %q, want application/x-ndjson", got)
	}
	if err := compat.Load(t).NDJSON("attachCommand", status, header, body); err != nil {
		t.Errorf("attach stream does not validate against the pin: %v\nbody: %s", err, body)
	}
	subset := compat.Load(t)
	if err := subset.NDJSON("attachCommand", status, header, body); err != nil {
		t.Errorf("attach stream does not validate against the subset: %v\nbody: %s", err, body)
	}
}

func processAttachTestValidateWriter(t *testing.T, writer *processAttachTestWriter) {
	t.Helper()
	status, body := writer.snapshot()
	processAttachTestValidate(t, status, writer.headerSnapshot(), body)
}

func processAttachTestWaitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("attach handler did not return")
	}
}

func processAttachTestWaitClosed(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not happen", what)
	}
}
