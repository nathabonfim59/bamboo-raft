package server

import (
	"context"
	"crypto/tls"
	"encoding/json/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/server/config"
)

func TestSafeDefaultErrors(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/known", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	handler := apierr.RequestID(safeDefaults(mux))

	tests := []struct {
		name string
		req  *http.Request
		want int
		code string
		typ  string
	}{
		{"matched", httptest.NewRequest(http.MethodGet, "/api/v1/known", nil), http.StatusNoContent, "", ""},
		{"wrong method", httptest.NewRequest(http.MethodPost, "/api/v1/known", nil), http.StatusMethodNotAllowed, "method_not_allowed", "sandbox.error"},
		{"unknown path", httptest.NewRequest(http.MethodGet, "/api/v1/missing", nil), http.StatusNotFound, "not_found", "sandbox.error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, tc.req)
			if recorder.Code != tc.want {
				t.Fatalf("status = %d, want %d", recorder.Code, tc.want)
			}
			if !strings.HasPrefix(recorder.Header().Get("X-Request-Id"), "req_") {
				t.Fatalf("missing request id: %v", recorder.Header())
			}
			if tc.code == "" {
				return
			}
			var body struct {
				OK        bool   `json:"ok"`
				Type      string `json:"type"`
				Code      string `json:"code"`
				RequestID string `json:"requestId"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatalf("body is not JSON: %v", err)
			}
			if body.OK || body.Type != tc.typ || body.Code != tc.code || !strings.HasPrefix(body.RequestID, "req_") {
				t.Fatalf("body = %s", recorder.Body.String())
			}
			if tc.want == http.StatusMethodNotAllowed && !strings.Contains(recorder.Header().Get("Allow"), "GET") {
				t.Fatalf("Allow = %q", recorder.Header().Get("Allow"))
			}
		})
	}
}

func TestServeHTTPStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	requestCtx, cancelRequests := context.WithCancel(ctx)
	tracker := newRequestTracker()
	srv := &http.Server{
		Handler:     tracker.wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })),
		BaseContext: func(net.Listener) context.Context { return requestCtx },
	}
	done := make(chan error, 1)
	go func() { done <- serveHTTP(ctx, testLogger(), srv, listener, tracker, cancelRequests, time.Second) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveHTTP = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serveHTTP did not stop after cancellation")
	}
}

func TestTLSListenerClosesOnBadPair(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := tlsListener(listener, certFile, keyFile); err == nil {
		t.Fatal("bad TLS pair accepted")
	}
	if _, err := listener.Accept(); err == nil {
		t.Fatal("listener was not closed after the TLS failure")
	}
}

func TestGatewayClientTLSNeverSkipsVerification(t *testing.T) {
	node := &config.Node{}
	conf, err := gatewayClientTLS(t.Context(), node)
	if err != nil || conf != nil {
		t.Fatalf("plaintext gateway = %v, %v", conf, err)
	}

	node = &config.Node{GatewayTLSCertFile: "cert", GatewayTLSKeyFile: "key", GatewayServerName: "gateway.internal"}
	conf, err = gatewayClientTLS(t.Context(), node)
	if err != nil {
		t.Fatal(err)
	}
	if conf.InsecureSkipVerify || conf.ServerName != "gateway.internal" || conf.MinVersion != tls.VersionTLS12 {
		t.Fatalf("gateway TLS = %+v", conf)
	}

	badCA := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(badCA, []byte("no certificates here"), 0o600); err != nil {
		t.Fatal(err)
	}
	node.GatewayCACertFile = badCA
	if _, err := gatewayClientTLS(t.Context(), node); err == nil {
		t.Fatal("CA bundle without certificates accepted")
	}
}

func TestRoleListenerUsesProvidedListener(t *testing.T) {
	expected, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer expected.Close()
	got, err := roleListener(t.Context(), expected, "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	if got != expected {
		t.Fatal("roleListener ignored the provided listener")
	}
}
