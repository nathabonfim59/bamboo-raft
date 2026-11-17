package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/server/config"
)

func TestRunStartupFailureClosesProvidedListeners(t *testing.T) {
	const sentinel = "postgres://user:SENTINEL-password@example.test/db"
	listeners := Listeners{}
	for _, field := range []*net.Listener{&listeners.API, &listeners.Node, &listeners.Gateway} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		*field = ln
	}
	cfg := &config.Config{
		Roles: config.Roles{config.RoleAPI: true, config.RoleLifecycle: true, config.RoleNodeHub: true},
		Server: config.Server{
			Listen:    listeners.API.Addr().String(),
			PublicURL: "http://127.0.0.1",
		},
		Node: config.Node{
			Listen:        listeners.Node.Addr().String(),
			GatewayListen: listeners.Gateway.Addr().String(),
			GatewayDial:   listeners.Gateway.Addr().String(),
			GatewayToken:  "gateway-token",
		},
		Secrets: config.Secrets{MasterKey: [32]byte{1}},
	}
	options := Options{
		Logger:    slog.New(slog.NewTextHandler(nopWriter{}, nil)),
		Listeners: listeners,
		StoreOpener: func(context.Context, config.Role) (*db.Store, error) {
			return nil, errors.New(sentinel)
		},
	}
	err := Run(t.Context(), cfg, options)
	if err == nil {
		t.Fatal("Run accepted a failing store opener")
	}
	if strings.Contains(err.Error(), "SENTINEL") {
		t.Fatal("Run leaked the driver error")
	}
	for name, ln := range map[string]net.Listener{"api": listeners.API, "node": listeners.Node, "gateway": listeners.Gateway} {
		if tcp, ok := ln.(*net.TCPListener); ok {
			_ = tcp.SetDeadline(time.Now())
		}
		if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("%s listener was left open: %v", name, err)
		}
	}
}

// nopWriter discards logger output.
type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestServeHTTPForceClosesSlowHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	requestCtx, cancelRequests := context.WithCancel(ctx)
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	var releaseOnce sync.Once
	// Deliberately ignores the request context so the test proves the
	// server joins the handler instead of abandoning it on a timeout.
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(started)
		<-release
		close(finished)
	})
	tracker := newRequestTracker()
	srv := &http.Server{
		Handler:     tracker.wrap(handler),
		BaseContext: func(net.Listener) context.Context { return requestCtx },
	}
	serveFinished := make(chan struct{})
	var serveErr error
	go func() {
		serveErr = serveHTTP(ctx, slog.New(slog.NewTextHandler(nopWriter{}, nil)), srv, listener, tracker, cancelRequests, 20*time.Millisecond)
		close(serveFinished)
	}()

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		response, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + listener.Addr().String() + "/")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
	}()
	t.Cleanup(func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		select {
		case <-serveFinished:
		case <-time.After(5 * time.Second):
			t.Error("serveHTTP did not finish during cleanup")
		}
		select {
		case <-clientDone:
		case <-time.After(5 * time.Second):
			t.Error("client did not finish during cleanup")
		}
	})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("request never reached the handler")
	}

	cancel()
	// Longer than the graceful timeout: shutdown has force-closed the
	// connection, but serveHTTP must still be waiting for the handler.
	select {
	case <-serveFinished:
		t.Fatal("serveHTTP returned before the handler was released")
	case <-time.After(5 * 20 * time.Millisecond):
	}
	select {
	case <-finished:
		t.Fatal("handler finished before release")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-serveFinished:
		if serveErr != nil {
			t.Fatalf("serveHTTP = %v", serveErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serveHTTP did not return after the handler was released")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("handler was not joined")
	}
	select {
	case <-clientDone:
	case <-time.After(2 * time.Second):
		t.Fatal("client was not joined after force-close")
	}
}

func TestServeHTTPListenerFailureCancelsAndJoins(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	requestCtx, cancelRequests := context.WithCancel(ctx)
	started := make(chan struct{})
	observed := make(chan struct{})
	tracker := newRequestTracker()
	srv := &http.Server{
		Handler: tracker.wrap(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			close(started)
			<-r.Context().Done()
			close(observed)
		})),
		BaseContext: func(net.Listener) context.Context { return requestCtx },
	}
	serveFinished := make(chan struct{})
	var serveErr error
	go func() {
		serveErr = serveHTTP(ctx, slog.New(slog.NewTextHandler(nopWriter{}, nil)), srv, listener, tracker, cancelRequests, time.Second)
		close(serveFinished)
	}()
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		response, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + listener.Addr().String() + "/")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-serveFinished:
		case <-time.After(5 * time.Second):
			t.Error("serveHTTP did not finish during cleanup")
		}
		select {
		case <-clientDone:
		case <-time.After(5 * time.Second):
			t.Error("client did not finish during cleanup")
		}
	})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("request never reached the handler")
	}
	// An unexpected listener failure, not a parent cancellation.
	_ = listener.Close()
	select {
	case <-serveFinished:
		if serveErr == nil {
			t.Fatal("serveHTTP hid an unexpected listener failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serveHTTP did not return after the listener failed")
	}
	select {
	case <-observed:
	case <-time.After(2 * time.Second):
		t.Fatal("listener failure did not cancel the in-flight handler")
	}
	select {
	case <-clientDone:
	case <-time.After(2 * time.Second):
		t.Fatal("client was not joined after listener failure")
	}
}
