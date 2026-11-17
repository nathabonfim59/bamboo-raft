package qmp

import (
	"bufio"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// qmpServer is a fake QMP server on a unix socket. Its goroutine sends the
// greeting, answers qmp_capabilities and then hands the connection to the
// test, which drives every further message.
type qmpServer struct {
	path string
	ln   net.Listener
}

type serverConn struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
	mu   sync.Mutex
}

func startQPMServer(t *testing.T, capabilities ...string) (*qmpServer, <-chan *serverConn) {
	t.Helper()
	path := t.TempDir() + "/qmp.sock"
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	caps, err := json.Marshal(capabilities)
	if err != nil {
		t.Fatalf("marshal capabilities: %v", err)
	}
	greeting := fmt.Sprintf(
		`{"QMP":{"version":{"qemu":{"major":10,"minor":0,"micro":0},"package":""},"capabilities":%s}}`,
		caps,
	)

	accepted := make(chan *serverConn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		sc := &serverConn{t: t, conn: conn, r: bufio.NewReaderSize(conn, 64<<10)}
		sc.writeRaw(greeting + "\n")
		// Dial always sends qmp_capabilities once it accepted the greeting.
		line, err := sc.r.ReadBytes('\n')
		if err != nil {
			return
		}
		var capsReq struct {
			ID uint64 `json:"id"`
		}
		if err := json.Unmarshal(line, &capsReq); err != nil {
			return
		}
		sc.writeRaw(fmt.Sprintf(`{"return":{},"id":%d}`+"\n", capsReq.ID))
		accepted <- sc
	}()
	return &qmpServer{path: path, ln: ln}, accepted
}

// startRawServer sends raw as the first frame and does not negotiate
// capabilities. It is used for greeting and framing failures.
func startRawServer(t *testing.T, raw string) string {
	t.Helper()
	path := t.TempDir() + "/qmp.sock"
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = conn.Write([]byte(raw))
		// Keep the connection open until the client closes it, so the
		// client reads the frame instead of an immediate EOF.
		buf := make([]byte, 1024)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	}()
	return path
}

func (s *serverConn) writeRaw(b string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.conn.Write([]byte(b))
}

func (s *serverConn) send(v any) {
	s.t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		s.t.Fatalf("marshal server message: %v", err)
	}
	s.writeRaw(string(b) + "\n")
}

func (s *serverConn) sendEvent(name string, data any) {
	s.t.Helper()
	if data == nil {
		s.send(map[string]any{"event": name})
		return
	}
	b, err := json.Marshal(data)
	if err != nil {
		s.t.Fatalf("marshal event data: %v", err)
	}
	s.writeRaw(fmt.Sprintf(`{"event":%q,"data":%s}`+"\n", name, b))
}

func (s *serverConn) sendReturn(id uint64, result any) {
	s.t.Helper()
	if result == nil {
		result = struct{}{}
	}
	s.send(map[string]any{"return": result, "id": id})
}

func (s *serverConn) sendError(id uint64, class, desc string) {
	s.t.Helper()
	s.send(map[string]any{"error": map[string]string{"class": class, "desc": desc}, "id": id})
}

type command struct {
	name string
	id   uint64
	raw  map[string]jsontext.Value
}

func (s *serverConn) readCommand() command {
	s.t.Helper()
	line, err := s.r.ReadBytes('\n')
	if err != nil {
		s.t.Fatalf("read command: %v", err)
	}
	var raw map[string]jsontext.Value
	if err := json.Unmarshal(line, &raw); err != nil {
		s.t.Fatalf("decode command %q: %v", line, err)
	}
	var c command
	c.raw = raw
	if err := json.Unmarshal(raw["execute"], &c.name); err != nil {
		s.t.Fatalf("decode command name: %v", err)
	}
	if err := json.Unmarshal(raw["id"], &c.id); err != nil {
		s.t.Fatalf("decode command id: %v", err)
	}
	return c
}

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func waitChannel[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for value")
	}
	var zero T
	return zero
}

func waitDone(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for close")
	}
}

func dialForTest(t *testing.T, srv *qmpServer, accepted <-chan *serverConn, opts ...Option) (*Client, *serverConn) {
	t.Helper()
	client, err := Dial(t.Context(), srv.path, discardLogger(), opts...)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	sc := waitChannel(t, accepted)
	t.Cleanup(func() { sc.conn.Close() })
	return client, sc
}

func TestDialGreetingAndCapabilities(t *testing.T) {
	srv, accepted := startQPMServer(t, "oob")
	client, err := Dial(t.Context(), srv.path, discardLogger(), WithCapabilities("oob"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	g := client.Greeting()
	if got, want := g.Version.String(), "10.0.0"; got != want {
		t.Errorf("version = %q, want %q", got, want)
	}
	if len(g.Capabilities) != 1 || g.Capabilities[0] != "oob" {
		t.Errorf("capabilities = %v, want [oob]", g.Capabilities)
	}

	// The handshake command was already consumed by the fake server. Read the
	// next command to check that ordinary commands carry no arguments when
	// none were given.
	sc := waitChannel(t, accepted)
	t.Cleanup(func() { sc.conn.Close() })
	done := make(chan error, 1)
	go func() {
		done <- client.Execute(t.Context(), "query-test", nil, nil)
	}()
	cmd := sc.readCommand()
	if _, ok := cmd.raw["arguments"]; ok {
		t.Error("command with nil args carried arguments")
	}
	sc.sendReturn(cmd.id, map[string]string{"ok": "yes"})
	if err := waitChannel(t, done); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	pid, err := client.PeerPID()
	if err != nil {
		t.Fatalf("PeerPID: %v", err)
	}
	if pid != os.Getpid() {
		t.Errorf("PeerPID = %d, want %d", pid, os.Getpid())
	}

	client.Close()
	waitDone(t, client.Done())
	if _, ok := <-client.Events(); ok {
		t.Error("events channel still open after Close")
	}
	if err := client.Execute(t.Context(), "query-test", nil, nil); !errors.Is(err, ErrClosed) {
		t.Errorf("Execute after Close = %v, want ErrClosed", err)
	}
}

func TestDialCapabilityNotAdvertised(t *testing.T) {
	srv, _ := startQPMServer(t) // advertises no capabilities
	_, err := Dial(t.Context(), srv.path, discardLogger(), WithCapabilities("oob"))
	if err == nil {
		t.Fatal("Dial succeeded, want capability error")
	}
	if !strings.Contains(err.Error(), "not advertised") {
		t.Errorf("error = %v, want capability error", err)
	}
}

func TestDialBadGreeting(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "not json", raw: "hello\n", want: "decode greeting"},
		{name: "no QMP object", raw: `{}` + "\n", want: "not a greeting"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := startRawServer(t, tt.raw)
			_, err := Dial(t.Context(), path, discardLogger())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Dial error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestDialGreetingObeysContext(t *testing.T) {
	path := startRawServer(t, "") // accepts and never writes
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Dial(ctx, path, discardLogger())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dial error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Dial took %v with a 100ms context", elapsed)
	}
}

func TestDialFrameTooLarge(t *testing.T) {
	big := `{"QMP":"` + strings.Repeat("x", MaxInboundFrame) + `"}` + "\n"
	path := startRawServer(t, big)
	_, err := Dial(t.Context(), path, discardLogger())
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Dial error = %v, want ErrFrameTooLarge", err)
	}
}

func TestExecuteConcurrentRepliesMatchedByID(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, sc := dialForTest(t, srv, accepted)

	type result struct {
		Value int `json:"value"`
	}
	var wg sync.WaitGroup
	results := make([]int, 2)
	wg.Go(func() {
		var r result
		if err := client.Execute(t.Context(), "query-a", nil, &r); err != nil {
			t.Errorf("query-a: %v", err)
			return
		}
		results[0] = r.Value
	})
	wg.Go(func() {
		var r result
		if err := client.Execute(t.Context(), "query-b", nil, &r); err != nil {
			t.Errorf("query-b: %v", err)
			return
		}
		results[1] = r.Value
	})

	// Read both commands before replying, then answer in reverse order. The
	// values are keyed by command name so write order does not matter.
	first := sc.readCommand()
	second := sc.readCommand()
	reply := func(c command) {
		value := 2
		if c.name == "query-a" {
			value = 1
		}
		sc.sendReturn(c.id, map[string]int{"value": value})
	}
	reply(second)
	reply(first)

	wg.Wait()
	if results[0] != 1 || results[1] != 2 {
		t.Errorf("results = %v, want [1 2]", results)
	}
}

func TestEventInterleavedWithReplies(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, sc := dialForTest(t, srv, accepted)

	done := make(chan error, 1)
	go func() {
		var r struct {
			OK bool `json:"ok"`
		}
		done <- client.Execute(t.Context(), "query-test", nil, &r)
	}()

	cmd := sc.readCommand()
	sc.sendEvent("STOP", map[string]bool{"paused": true})
	sc.sendEvent("RESET", nil)
	sc.sendReturn(cmd.id, map[string]bool{"ok": true})

	if err := waitChannel(t, done); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	ev := waitChannel(t, client.Events())
	if ev.Name != "STOP" {
		t.Fatalf("first event = %q, want STOP", ev.Name)
	}
	var data struct {
		Paused bool `json:"paused"`
	}
	if err := ev.Decode(&data); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !data.Paused {
		t.Error("event data not decoded")
	}
	ev = waitChannel(t, client.Events())
	if ev.Name != "RESET" {
		t.Fatalf("second event = %q, want RESET", ev.Name)
	}
}

func TestEventsDoNotBlockReplies(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, sc := dialForTest(t, srv, accepted)

	done := make(chan error, 1)
	go func() {
		done <- client.Execute(t.Context(), "query-test", nil, nil)
	}()
	cmd := sc.readCommand()
	for i := range 100 {
		sc.sendEvent("RESET", map[string]int{"n": i})
	}
	sc.sendReturn(cmd.id, nil)
	if err := waitChannel(t, done); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for range 100 {
		ev := waitChannel(t, client.Events())
		if ev.Name != "RESET" {
			t.Fatalf("event = %q", ev.Name)
		}
	}
}

func TestEventOverflowFailsClient(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, sc := dialForTest(t, srv, accepted)

	// No Events consumer at all: exceed the queue and prove the client fails
	// instead of silently dropping an event or blocking the reader.
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.Execute(t.Context(), "query-test", nil, nil)
	}()
	sc.readCommand()
	for i := range MaxEventQueue + 4 {
		sc.sendEvent("RESET", map[string]int{"n": i})
	}
	err := waitChannel(t, errCh)
	if !errors.Is(err, ErrEventOverflow) || !errors.Is(err, ErrClosed) {
		t.Fatalf("Execute error = %v, want ErrEventOverflow and ErrClosed", err)
	}

	// Close must terminate the pump even though nobody ever read Events.
	waitDone(t, client.Done())
	if _, ok := <-client.Events(); ok {
		t.Error("events channel still open after overflow")
	}
	client.Close()
}

func TestQMPErrorLeavesClientUsable(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, sc := dialForTest(t, srv, accepted)

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- client.Execute(t.Context(), "query-bad", nil, nil)
	}()
	first := sc.readCommand()
	sc.sendError(first.id, "GenericError", "boom")
	err := waitChannel(t, firstDone)
	qerr, ok := errors.AsType[*Error](err)
	if !ok {
		t.Fatalf("error = %v, want *Error", err)
	}
	if qerr.Class != "GenericError" || qerr.Description != "boom" {
		t.Errorf("QMP error = %+v", qerr)
	}

	secondDone := make(chan error, 1)
	go func() {
		var result struct {
			OK string `json:"ok"`
		}
		secondDone <- client.Execute(t.Context(), "query-good", nil, &result)
	}()
	second := sc.readCommand()
	sc.sendReturn(second.id, map[string]string{"ok": "yes"})
	if err := waitChannel(t, secondDone); err != nil {
		t.Fatalf("Execute after error: %v", err)
	}
}

func TestCancelWaitingForReply(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, sc := dialForTest(t, srv, accepted)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	execDone := make(chan error, 1)
	start := time.Now()
	go func() {
		execDone <- client.Execute(ctx, "query-slow", nil, nil)
	}()
	cmd := sc.readCommand()
	err := waitChannel(t, execDone)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Execute took %v with a 100ms context", elapsed)
	}

	// A late reply for the cancelled request is ignored, and the client stays
	// usable for the next command.
	sc.sendReturn(cmd.id, map[string]bool{"late": true})
	done := make(chan error, 1)
	go func() {
		var r struct {
			OK bool `json:"ok"`
		}
		done <- client.Execute(t.Context(), "query-next", nil, &r)
	}()
	next := sc.readCommand()
	sc.sendReturn(next.id, map[string]bool{"ok": true})
	if err := waitChannel(t, done); err != nil {
		t.Fatalf("Execute after cancel: %v", err)
	}
}

// TestCancelBlockedSocketWrite makes the server stop reading, fills the
// connection with a large command, and proves the call returns on its context
// instead of waiting for the peer.
func TestCancelBlockedSocketWrite(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, _ := dialForTest(t, srv, accepted)
	// From here on the server never reads again.

	big := strings.Repeat("x", 4<<20)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := client.Execute(ctx, "query-big", struct {
		Data string `json:"data"`
	}{Data: big}, nil)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrClosed) {
		t.Fatalf("Execute = %v, want deadline or closed", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("Execute took %v, want cancellation near 100ms", elapsed)
	}
}

// TestCancelWhileWaitingForWriter proves that an Execute blocked on the
// writer queue (not yet serialized) still honours its context.
func TestCancelWhileWaitingForWriter(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, _ := dialForTest(t, srv, accepted)
	// From here on the server never reads again.

	// Occupy the writer with a command that cannot fit in the socket buffer.
	blockerCtx, stopBlocker := context.WithCancel(t.Context())
	defer stopBlocker()
	go func() {
		_ = client.Execute(blockerCtx, "query-blocker", struct {
			Data string `json:"data"`
		}{Data: strings.Repeat("x", 4<<20)}, nil)
	}()
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for range 200 {
		wg.Go(func() {
			errs <- client.Execute(ctx, "query-queued", nil, nil)
		})
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	waitDone(t, finished)

	close(errs)
	waitedOnQueue := false
	for err := range errs {
		if errors.Is(err, context.Canceled) {
			waitedOnQueue = true
		}
	}
	if !waitedOnQueue {
		t.Error("no Execute observed context cancellation")
	}
}

func TestCloseWithBlockedEventConsumer(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, sc := dialForTest(t, srv, accepted)

	sc.sendEvent("RESET", map[string]int{"n": 1})
	// Give the pump time to receive the event and block delivering it to the
	// unconsumed channel, then close.
	time.Sleep(50 * time.Millisecond)
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	done := make(chan struct{})
	go func() {
		for range client.Events() {
		}
		close(done)
	}()
	waitDone(t, done)
}

func TestEOFWithPendingRequest(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, sc := dialForTest(t, srv, accepted)

	done := make(chan error, 1)
	go func() {
		done <- client.Execute(t.Context(), "query-test", nil, nil)
	}()
	sc.readCommand()
	sc.conn.Close()

	err := waitChannel(t, done)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Execute = %v, want ErrClosed", err)
	}
	waitDone(t, client.Done())
	if _, ok := <-client.Events(); ok {
		t.Error("events channel still open after EOF")
	}
}

func TestCloseIdempotent(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, _ := dialForTest(t, srv, accepted)

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	waitDone(t, client.Done())
	if err := client.Execute(t.Context(), "query-test", nil, nil); !errors.Is(err, ErrClosed) {
		t.Errorf("Execute after Close = %v, want ErrClosed", err)
	}
}

func TestExecuteAlreadyCancelled(t *testing.T) {
	srv, accepted := startQPMServer(t)
	client, _ := dialForTest(t, srv, accepted)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := client.Execute(ctx, "query-test", nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute = %v, want canceled", err)
	}
}

func TestEventDecodeError(t *testing.T) {
	ev := Event{Name: "STOP", Data: jsontext.Value(`{"paused":"not-a-bool"}`)}
	var data struct {
		Paused bool `json:"paused"`
	}
	if err := ev.Decode(&data); err == nil {
		t.Fatal("Decode succeeded on a type mismatch")
	}
	if err := (Event{Name: "RESET"}).Decode(&data); err != nil {
		t.Fatalf("Decode of empty data: %v", err)
	}
}
