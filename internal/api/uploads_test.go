package api_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

const (
	uploadsTestOwner          = "acct_uploads_test"
	uploadsTestOtherOwner     = "acct_uploads_other"
	uploadsTestSandboxID      = "bx_23456789"
	uploadsTestOtherSandboxID = "bx_abcdefgh"
	uploadsTestNodeID         = "node-uploads-test"
	uploadsTestNodeToken      = "node-uploads-token"
	uploadsTestGatewayToken   = "gateway-uploads-token"
)

var (
	uploadsTestKey     = []byte("uploads-test-signing-key-0123456789abcdef")
	uploadsTestBaseURL = "https://api.example.test"
	uploadsTestNow     = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
)

func uploadsTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// uploadsTestClock is a deterministic clock for issuance and redemption.
type uploadsTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newUploadsTestClock() *uploadsTestClock {
	return &uploadsTestClock{now: uploadsTestNow}
}

func (c *uploadsTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *uploadsTestClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// uploadsTestLookupCall records one owner-scoped lookup.
type uploadsTestLookupCall struct {
	Owner     string
	SandboxID string
}

// uploadsTestLookup is an owner-scoped CommandTargetLookup controlled by the
// test. The handler must call it explicitly on issuance and redemption.
type uploadsTestLookup struct {
	mu     sync.Mutex
	target api.CommandTarget
	err    error
	calls  []uploadsTestLookupCall
}

func (l *uploadsTestLookup) TargetLookup(_ context.Context, owner, sandboxID string) (api.CommandTarget, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, uploadsTestLookupCall{Owner: owner, SandboxID: sandboxID})
	return l.target, l.err
}

func (l *uploadsTestLookup) set(target api.CommandTarget, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.target = target
	l.err = err
}

func (l *uploadsTestLookup) snapshot() []uploadsTestLookupCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]uploadsTestLookupCall(nil), l.calls...)
}

func (l *uploadsTestLookup) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.calls)
}

func uploadsTestReadyLookup() *uploadsTestLookup {
	return &uploadsTestLookup{target: api.CommandTarget{NodeID: uploadsTestNodeID, State: api.StateReady}}
}

// uploadsTestEnv is a full node stack: nodefake behind nodelink behind the
// authenticated gateway, reached only through the handler's client factory.
type uploadsTestEnv struct {
	node  *nodefake.Node
	root  string
	hub   *nodelink.Hub
	link  *nodelink.Link
	serve *nodegateway.ClientFactory
}

func uploadsTestStartEnv(t *testing.T) *uploadsTestEnv {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	wantToken := sha256.Sum256([]byte(uploadsTestNodeToken))
	authn := func(_ context.Context, hello nodelink.Hello) error {
		gotToken := sha256.Sum256([]byte(hello.Token))
		if subtle.ConstantTimeCompare(gotToken[:], wantToken[:]) != 1 {
			return errors.New("bad node token")
		}
		return nil
	}
	// The background lifetimes are torn down in LIFO Cleanup order:
	// gateway first, then the node, then the hub. No role outlives the
	// dependency it dials or serves.
	hub := nodelink.NewHub(authn, uploadsTestLogger())
	hubCtx, hubCancel := context.WithCancel(context.Background())
	hubDone := make(chan struct{})
	go func() { _ = hub.Serve(hubCtx, ln); close(hubDone) }()
	t.Cleanup(func() { hubCancel(); <-hubDone })

	root := t.TempDir()
	node := nodefake.New(uploadsTestNodeID, root)
	server := grpc.NewServer(nodelink.ServerOptions()...)
	node.Register(server)
	nodeCtx, nodeCancel := context.WithCancel(context.Background())
	nodeDone := make(chan struct{})
	go func() {
		_ = nodelink.Serve(nodeCtx, nodelink.DialConfig{
			Addr: ln.Addr().String(), NodeID: uploadsTestNodeID, Token: uploadsTestNodeToken,
			Log: uploadsTestLogger(), MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		}, server)
		close(nodeDone)
	}()
	t.Cleanup(func() { nodeCancel(); <-nodeDone })

	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()
	link, err := hub.Wait(waitCtx, uploadsTestNodeID)
	if err != nil {
		t.Fatalf("node never connected: %v", err)
	}
	if _, err := link.Client.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: uploadsTestSandboxID}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	gateway, err := nodegateway.New(hub, uploadsTestGatewayToken)
	if err != nil {
		t.Fatal(err)
	}
	gatewayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gatewayCtx, gatewayCancel := context.WithCancel(context.Background())
	gatewayDone := make(chan error, 1)
	go func() { gatewayDone <- gateway.Serve(gatewayCtx, gatewayListener) }()
	t.Cleanup(func() {
		gatewayCancel()
		if err := <-gatewayDone; err != nil {
			t.Errorf("gateway Serve: %v", err)
		}
	})

	return &uploadsTestEnv{
		node:  node,
		root:  root,
		hub:   hub,
		link:  link,
		serve: nodegateway.NewClientFactory(gatewayListener.Addr().String(), uploadsTestGatewayToken, nil),
	}
}

func (e *uploadsTestEnv) createVM(t *testing.T, sandboxID string) {
	t.Helper()
	if _, err := e.link.Client.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: sandboxID}); err != nil {
		t.Fatalf("CreateVM %s: %v", sandboxID, err)
	}
}

func (e *uploadsTestEnv) factory() api.NodeClientFactory {
	return func(nodeID string) (nodev1.NodeClient, func() error, error) {
		client, err := e.serve.Dial(nodeID)
		if err != nil {
			return nil, nil, err
		}
		return client, client.Close, nil
	}
}

// uploadsTestGuestFile returns the host path of a canonical guest file.
func uploadsTestGuestFile(e *uploadsTestEnv, sandboxID, guestPath string) string {
	rel := strings.TrimPrefix(guestPath, "/")
	return filepath.Join(e.root, "vms", sandboxID, filepath.FromSlash(rel))
}

func uploadsTestReadGuestFile(t *testing.T, e *uploadsTestEnv, sandboxID, guestPath string) ([]byte, bool) {
	t.Helper()
	data, err := os.ReadFile(uploadsTestGuestFile(e, sandboxID, guestPath))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("read guest file: %v", err)
	}
	return data, true
}

func uploadsTestHandlers(t *testing.T, lookup api.CommandTargetLookup, factory api.NodeClientFactory, clock *uploadsTestClock, opts ...func(*api.UploadsOptions)) api.UploadsHandlers {
	t.Helper()
	options := api.UploadsOptions{
		Lookup:     lookup,
		Clients:    factory,
		SigningKey: uploadsTestKey,
		BaseURL:    uploadsTestBaseURL,
		Logger:     uploadsTestLogger(),
		Now:        clock.Now,
	}
	for _, opt := range opts {
		opt(&options)
	}
	handlers, err := api.NewUploadsHandlers(options)
	if err != nil {
		t.Fatalf("NewUploadsHandlers: %v", err)
	}
	return handlers
}

func uploadsTestIssue(t *testing.T, handlers api.UploadsHandlers, owner, sandboxID, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("POST "+api.UploadsPath, handlers.Issue)
	request := httptest.NewRequest(http.MethodPost,
		"/api/v1/sandboxes/"+sandboxID+"/files/upload-url", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if owner != "" {
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner, KeyID: "key_uploads_test"}))
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return recorder
}

func uploadsTestIssueDirect(t *testing.T, handler http.HandlerFunc, method, owner, sandboxID, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, "/api/v1/sandboxes/"+sandboxID+"/files/upload-url", strings.NewReader(body))
	request.SetPathValue(api.ParamSandboxID, sandboxID)
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if owner != "" {
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner, KeyID: "key_uploads_test"}))
	}
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	return recorder
}

func uploadsTestReceiveRequest(method, token string, body io.Reader) *http.Request {
	request := httptest.NewRequest(method, "/api/bbraft/uploads/"+token, body)
	request.SetPathValue(api.UploadTokenParam, token)
	return request
}

func uploadsTestReceive(t *testing.T, handlers api.UploadsHandlers, method, token string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handlers.Receive(recorder, uploadsTestReceiveRequest(method, token, body))
	return recorder
}

// uploadsTestTokenPresence proves a response did not echo the raw token.
func uploadsTestAssertNoToken(t *testing.T, recorder *httptest.ResponseRecorder, token string) {
	t.Helper()
	if token != "" && bytes.Contains(recorder.Body.Bytes(), []byte(token)) {
		t.Fatal("response body contains the raw signed token")
	}
}

type uploadsTestErrorEnvelope struct {
	OK      bool   `json:"ok"`
	Type    string `json:"type"`
	Status  int    `json:"status"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Error   struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Status  int            `json:"status"`
		Details map[string]any `json:"details"`
	} `json:"error"`
	RequestID string `json:"requestId"`
}

func uploadsTestDecodeError(t *testing.T, recorder *httptest.ResponseRecorder) uploadsTestErrorEnvelope {
	t.Helper()
	var envelope uploadsTestErrorEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		// Never print the body or decoder text; it may contain a token.
		t.Fatalf("decode error envelope failed (%T)", err)
	}
	if envelope.OK || envelope.Type != "sandbox.error" {
		t.Fatalf("envelope = ok:%v type:%q", envelope.OK, envelope.Type)
	}
	if envelope.Status != recorder.Code || envelope.Error.Status != recorder.Code || envelope.Error.Code != envelope.Code {
		t.Fatalf("error envelope status/code mismatch")
	}
	if envelope.RequestID == "" {
		t.Fatal("requestId is empty")
	}
	return envelope
}

func uploadsTestDecodeURL(t *testing.T, recorder *httptest.ResponseRecorder) api.FileURLResponse {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var response api.FileURLResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		// Never print the body: a success body carries the signed URL.
		t.Fatalf("decode upload URL response failed (%T)", err)
	}
	if !response.OK || response.Type != api.TypeFileUploadURL {
		t.Fatalf("envelope = ok:%v type:%q", response.OK, response.Type)
	}
	return response
}

func uploadsTestValidateCreateUploadURL(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	validator := compat.Load(t)
	if err := validator.Response("createUploadUrl", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
		// Print only the validation error class: validator text can echo
		// the signed URL, which must never appear in a test failure.
		t.Errorf("response does not validate against the pin (status %d, error type %T)", recorder.Code, err)
	}
	subset := compat.Load(t)
	if err := subset.Response("createUploadUrl", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
		t.Errorf("response does not validate against the v1 API document (status %d, error type %T)", recorder.Code, err)
	}
}

// uploadsTestTokenPayload mirrors the token body in tests.
type uploadsTestTokenPayload struct {
	Version   int    `json:"v"`
	Owner     string `json:"o"`
	SandboxID string `json:"s"`
	Path      string `json:"p"`
	ExpiresAt int64  `json:"e"`
	Nonce     string `json:"n"`
}

func uploadsTestTokenParts(t *testing.T, token string) (payload, signature []byte) {
	t.Helper()
	encodedPayload, encodedSignature, ok := strings.Cut(token, ".")
	if !ok {
		t.Fatal("issued token has no separator")
	}
	payload, err := base64.RawURLEncoding.DecodeString(encodedPayload)
	if err != nil {
		t.Fatal("issued token payload is not base64url")
	}
	signature, err = base64.RawURLEncoding.DecodeString(encodedSignature)
	if err != nil {
		t.Fatal("issued token signature is not base64url")
	}
	return payload, signature
}

func uploadsTestDecodeToken(t *testing.T, token string) uploadsTestTokenPayload {
	t.Helper()
	payload, _ := uploadsTestTokenParts(t, token)
	var decoded uploadsTestTokenPayload
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal("issued token payload is not JSON")
	}
	return decoded
}

func uploadsTestSignPayload(payload []byte) []byte {
	mac := hmac.New(sha256.New, uploadsTestKey)
	mac.Write(payload)
	return mac.Sum(nil)
}

func uploadsTestTokenFromRaw(payload []byte) string {
	return base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(uploadsTestSignPayload(payload))
}

// uploadsTestReader returns chunks and reports n>0 with io.EOF on the last
// one, exercising the final-chunk path.
type uploadsTestReader struct {
	data      []byte
	chunk     int
	offset    int
	reads     int
	completed bool
}

func (r *uploadsTestReader) Read(p []byte) (int, error) {
	r.reads++
	if r.offset >= len(r.data) {
		r.completed = true
		return 0, io.EOF
	}
	n := min(len(p), r.chunk, len(r.data)-r.offset)
	copy(p, r.data[r.offset:r.offset+n])
	r.offset += n
	if r.offset >= len(r.data) {
		r.completed = true
		return n, io.EOF
	}
	return n, nil
}

func (r *uploadsTestReader) Close() error { return nil }

// uploadsTestPutStream is a nodev1 client stream whose Send and final receive
// behavior the test controls. A real client-streaming server answers only
// after the client closes send, so RecvMsg waits for CloseSend unless early is
// set or a Send failure marks the stream terminal.
type uploadsTestPutStream struct {
	grpc.ClientStream
	ctx           context.Context
	send          func(*nodev1.PutFileRequest) error
	recv          func() (*nodev1.PutFileResponse, error)
	early         bool
	closeSend     chan struct{}
	closeSendOnce sync.Once
	terminal      chan struct{}
	terminalOnce  sync.Once
	recvMu        sync.Mutex
	recvN         int
	lastErr       error
}

func (s *uploadsTestPutStream) Context() context.Context { return s.ctx }

func (s *uploadsTestPutStream) Send(request *nodev1.PutFileRequest) error {
	err := s.send(request)
	if err != nil {
		s.markTerminal()
	}
	return err
}

func (s *uploadsTestPutStream) CloseSend() error {
	s.closeSendOnce.Do(func() { close(s.closeSend) })
	return nil
}

func (s *uploadsTestPutStream) markTerminal() {
	s.terminalOnce.Do(func() { close(s.terminal) })
}

func (s *uploadsTestPutStream) RecvMsg(message any) error {
	if !s.early {
		select {
		case <-s.closeSend:
		case <-s.terminal:
		case <-s.ctx.Done():
			select {
			case <-s.terminal:
				// A Send failure carries the real status.
			default:
				err := s.ctx.Err()
				s.recordRecv(err)
				return err
			}
		}
	}
	response, err := s.recv()
	s.recordRecv(err)
	if err != nil {
		return err
	}
	target, ok := message.(*nodev1.PutFileResponse)
	if !ok {
		return errors.New("uploads test: bad RecvMsg target")
	}
	if response != nil {
		target.Path = response.GetPath()
		target.Size = response.GetSize()
	}
	return nil
}

func (s *uploadsTestPutStream) recordRecv(err error) {
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	s.recvN++
	s.lastErr = err
}

func (s *uploadsTestPutStream) recvCalls() int {
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	return s.recvN
}

func (s *uploadsTestPutStream) lastRecvError() error {
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	return s.lastErr
}

func (s *uploadsTestPutStream) CloseAndRecv() (*nodev1.PutFileResponse, error) {
	_ = s.CloseSend()
	var response nodev1.PutFileResponse
	if err := s.RecvMsg(&response); err != nil {
		return nil, err
	}
	return &response, nil
}

// uploadsTestNodeClient is a nodev1.NodeClient whose only useful method is
// PutFile.
type uploadsTestNodeClient struct {
	nodev1.NodeClient
	putFile func(context.Context) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error)
}

func (c *uploadsTestNodeClient) PutFile(ctx context.Context, _ ...grpc.CallOption) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error) {
	return c.putFile(ctx)
}

func uploadsTestFactory(client nodev1.NodeClient) api.NodeClientFactory {
	return func(string) (nodev1.NodeClient, func() error, error) {
		return client, func() error { return nil }, nil
	}
}

// uploadsTestIssueToken issues one token through the real handler and returns
// it. Failures never print the token.
func uploadsTestIssueToken(t *testing.T, handlers api.UploadsHandlers, owner, sandboxID, path string) string {
	t.Helper()
	return uploadsTestIssueTokenTTL(t, handlers, owner, sandboxID, path, 0)
}

// uploadsTestIssueTokenTTL issues one token with an explicit TTL (0 means
// omit the member).
func uploadsTestIssueTokenTTL(t *testing.T, handlers api.UploadsHandlers, owner, sandboxID, path string, ttlSeconds int) string {
	t.Helper()
	body := map[string]any{"path": path}
	if ttlSeconds != 0 {
		body["ttlSeconds"] = ttlSeconds
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal issuance body: %v", err)
	}
	recorder := uploadsTestIssue(t, handlers, owner, sandboxID, string(raw))
	response := uploadsTestDecodeURL(t, recorder)
	if !strings.HasPrefix(response.URL, uploadsTestBaseURL+api.UploadReceiverPrefix+"/") {
		t.Fatalf("signed URL is not under the receiver prefix")
	}
	token := strings.TrimPrefix(response.URL, uploadsTestBaseURL+api.UploadReceiverPrefix+"/")
	if token == "" {
		t.Fatal("signed URL has an empty token")
	}
	return token
}

func uploadsTestTargets(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) uploadsTestErrorEnvelope {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d", recorder.Code, status)
	}
	envelope := uploadsTestDecodeError(t, recorder)
	if envelope.Code != code {
		t.Fatalf("code = %q, want %q", envelope.Code, code)
	}
	return envelope
}

// TestNewUploadsHandlersDependencies covers the constructor contract.
func TestNewUploadsHandlersDependencies(t *testing.T) {
	clock := newUploadsTestClock()
	lookup := uploadsTestReadyLookup()
	factory := uploadsTestFactory(&uploadsTestNodeClient{putFile: func(context.Context) (grpc.ClientStreamingClient[nodev1.PutFileRequest, nodev1.PutFileResponse], error) {
		return nil, errors.New("unused")
	}})
	if _, err := api.NewUploadsHandlers(api.UploadsOptions{Clients: factory, SigningKey: uploadsTestKey, BaseURL: uploadsTestBaseURL}); err == nil {
		t.Error("nil lookup accepted")
	}
	if _, err := api.NewUploadsHandlers(api.UploadsOptions{Lookup: lookup, SigningKey: uploadsTestKey, BaseURL: uploadsTestBaseURL}); err == nil {
		t.Error("nil client factory accepted")
	}
	if _, err := api.NewUploadsHandlers(api.UploadsOptions{Lookup: lookup, Clients: factory, SigningKey: []byte("short"), BaseURL: uploadsTestBaseURL}); err == nil {
		t.Error("short signing key accepted")
	}
	for _, base := range []string{
		"", "api.example.test", "ftp://api.example.test", "https://", "https://:443", "://bad",
		"https://[::1", "https://user:pass@api.example.test",
		"https://api.example.test?query=1", "https://api.example.test#fragment", "https://api.example.test/x?y#z",
	} {
		if _, err := api.NewUploadsHandlers(api.UploadsOptions{
			Lookup: lookup, Clients: factory, SigningKey: uploadsTestKey, BaseURL: base, Now: clock.Now,
		}); err == nil {
			t.Errorf("BaseURL %q accepted", base)
		}
	}
	handlers, err := api.NewUploadsHandlers(api.UploadsOptions{
		Lookup: lookup, Clients: factory, SigningKey: uploadsTestKey, BaseURL: uploadsTestBaseURL, Now: clock.Now,
	})
	if err != nil || handlers.Issue == nil || handlers.Receive == nil {
		t.Fatalf("valid options: handlers=%v err=%v", handlers, err)
	}
}

func TestUploadIssueSuccess(t *testing.T) {
	clock := newUploadsTestClock()
	lookup := uploadsTestReadyLookup()
	handlers := uploadsTestHandlers(t, lookup, uploadsTestFactory(&uploadsTestNodeClient{}), clock)
	recorder := uploadsTestIssue(t, handlers, uploadsTestOwner, uploadsTestSandboxID, `{"path":"data/dataset.parquet"}`)
	response := uploadsTestDecodeURL(t, recorder)
	uploadsTestValidateCreateUploadURL(t, recorder)

	if response.Method != api.FileMethodPut {
		t.Errorf("method = %q, want PUT", response.Method)
	}
	if response.Path != "/home/user/data/dataset.parquet" {
		t.Errorf("path = %q", response.Path)
	}
	wantExpiry := clock.Now().Add(api.DefaultUploadTTLSeconds * time.Second)
	if !response.ExpiresAt.Equal(wantExpiry) {
		t.Errorf("expiresAt = %s, want %s", response.ExpiresAt, wantExpiry)
	}
	if !strings.HasPrefix(response.URL, uploadsTestBaseURL+api.UploadReceiverPrefix+"/") {
		t.Errorf("url is not an absolute same-server receiver URL")
	}
	if strings.Contains(response.URL, "?") || strings.Contains(response.URL, "#") {
		t.Error("signed URL has a query or fragment")
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}
	calls := lookup.snapshot()
	if len(calls) != 1 || calls[0] != (uploadsTestLookupCall{Owner: uploadsTestOwner, SandboxID: uploadsTestSandboxID}) {
		t.Errorf("lookup calls = %+v", calls)
	}
	payload := uploadsTestDecodeToken(t, strings.TrimPrefix(response.URL, uploadsTestBaseURL+api.UploadReceiverPrefix+"/"))
	if payload.Owner != uploadsTestOwner || payload.SandboxID != uploadsTestSandboxID || payload.Path != response.Path {
		t.Error("token does not bind owner, sandbox and canonical path")
	}
	if payload.ExpiresAt != response.ExpiresAt.Unix() {
		t.Error("token expiry does not equal the response expiry")
	}
}

func TestUploadIssuePathAndTTL(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
		wantPath   string
		wantTTL    time.Duration
	}{
		{"relative", `{"path":"relative.txt"}`, http.StatusOK, "", "/home/user/relative.txt", api.DefaultUploadTTLSeconds * time.Second},
		{"absolute tmp", `{"path":"/tmp/archive.tar","ttlSeconds":3600}`, http.StatusOK, "", "/tmp/archive.tar", 3600 * time.Second},
		{"short ttl", `{"path":"x","ttlSeconds":1}`, http.StatusOK, "", "/home/user/x", time.Second},
		{"dot segments", `{"path":"/home/user/a/../b"}`, http.StatusOK, "", "/home/user/b", api.DefaultUploadTTLSeconds * time.Second},
		{"empty path", `{"path":""}`, http.StatusBadRequest, apierr.CodeInvalidPath, "", 0},
		{"missing path", `{}`, http.StatusBadRequest, apierr.CodeInvalidJSON, "", 0},
		{"null path", `{"path":null}`, http.StatusBadRequest, apierr.CodeInvalidJSON, "", 0},
		{"numeric path", `{"path":123}`, http.StatusBadRequest, apierr.CodeInvalidJSON, "", 0},
		{"outside home", `{"path":"/etc/passwd"}`, http.StatusBadRequest, apierr.CodeInvalidPath, "", 0},
		{"home root", `{"path":"/home/user"}`, http.StatusBadRequest, apierr.CodeInvalidPath, "", 0},
		{"tmp root", `{"path":"/tmp"}`, http.StatusBadRequest, apierr.CodeInvalidPath, "", 0},
		{"relative escape", `{"path":"a/../../etc/passwd"}`, http.StatusBadRequest, apierr.CodeInvalidPath, "", 0},
		{"absolute escape", `{"path":"/tmp/../etc/passwd"}`, http.StatusBadRequest, apierr.CodeInvalidPath, "", 0},
		{"nul", `{"path":"bad\u0000name"}`, http.StatusBadRequest, apierr.CodeInvalidPath, "", 0},
		{"ttl zero", `{"path":"x","ttlSeconds":0}`, http.StatusBadRequest, apierr.CodeInvalidTimeout, "", 0},
		{"ttl negative", `{"path":"x","ttlSeconds":-1}`, http.StatusBadRequest, apierr.CodeInvalidTimeout, "", 0},
		{"ttl too large", `{"path":"x","ttlSeconds":3601}`, http.StatusBadRequest, apierr.CodeInvalidTimeout, "", 0},
		{"ttl null", `{"path":"x","ttlSeconds":null}`, http.StatusBadRequest, apierr.CodeInvalidTimeout, "", 0},
		{"ttl string", `{"path":"x","ttlSeconds":"900"}`, http.StatusBadRequest, apierr.CodeInvalidTimeout, "", 0},
		{"ttl float", `{"path":"x","ttlSeconds":1.5}`, http.StatusBadRequest, apierr.CodeInvalidTimeout, "", 0},
		{"ttl bool", `{"path":"x","ttlSeconds":true}`, http.StatusBadRequest, apierr.CodeInvalidTimeout, "", 0},
		{"unknown member", `{"path":"x","extra":1}`, http.StatusBadRequest, apierr.CodeInvalidJSON, "", 0},
		{"trailing value", `{"path":"x"}{"path":"y"}`, http.StatusBadRequest, apierr.CodeInvalidJSON, "", 0},
		{"empty body", ``, http.StatusBadRequest, apierr.CodeInvalidJSON, "", 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newUploadsTestClock()
			handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestFactory(&uploadsTestNodeClient{}), clock)
			recorder := uploadsTestIssue(t, handlers, uploadsTestOwner, uploadsTestSandboxID, test.body)
			if test.wantStatus != http.StatusOK {
				uploadsTestTargets(t, recorder, test.wantStatus, test.wantCode)
				uploadsTestValidateCreateUploadURL(t, recorder)
				return
			}
			response := uploadsTestDecodeURL(t, recorder)
			uploadsTestValidateCreateUploadURL(t, recorder)
			if response.Path != test.wantPath {
				t.Errorf("path = %q, want %q", response.Path, test.wantPath)
			}
			wantExpiry := clock.Now().Add(test.wantTTL)
			if !response.ExpiresAt.Equal(wantExpiry) {
				t.Errorf("expiresAt = %s, want %s", response.ExpiresAt, wantExpiry)
			}
		})
	}
}

func TestUploadIssueStrictTransport(t *testing.T) {
	clock := newUploadsTestClock()
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestFactory(&uploadsTestNodeClient{}), clock)
	large := `{"path":"` + strings.Repeat("a", api.MaxJSONBodyBytes) + `"}`
	tests := []struct {
		name        string
		method      string
		owner       string
		body        string
		contentType string
		wantStatus  int
		wantCode    string
	}{
		{"unauthorized", http.MethodPost, "", `{"path":"x"}`, "application/json", http.StatusUnauthorized, apierr.CodeUnauthorized},
		{"wrong method", http.MethodGet, uploadsTestOwner, `{"path":"x"}`, "application/json", http.StatusMethodNotAllowed, apierr.CodeNotFound},
		{"wrong content type", http.MethodPost, uploadsTestOwner, `{"path":"x"}`, "text/plain", http.StatusBadRequest, apierr.CodeInvalidJSON},
		{"oversized body", http.MethodPost, uploadsTestOwner, large, "application/json", http.StatusBadRequest, apierr.CodeInvalidJSON},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := uploadsTestIssueDirect(t, handlers.Issue, test.method, test.owner, uploadsTestSandboxID, test.body, test.contentType)
			uploadsTestTargets(t, recorder, test.wantStatus, test.wantCode)
			if test.wantStatus != http.StatusMethodNotAllowed {
				uploadsTestValidateCreateUploadURL(t, recorder)
			}
		})
	}
}

func TestUploadIssueLookupAndState(t *testing.T) {
	tests := []struct {
		name       string
		target     api.CommandTarget
		err        error
		wantStatus int
		wantCode   string
		wantState  string
	}{
		{"missing", api.CommandTarget{}, api.ErrCommandTargetNotFound, http.StatusNotFound, apierr.CodeNotFound, ""},
		{"storage error", api.CommandTarget{}, errors.New("database unavailable"), http.StatusInternalServerError, apierr.CodeInternal, ""},
		{"storage apierr 404 is not missing", api.CommandTarget{}, apierr.New(http.StatusNotFound, apierr.CodeNotFound, "storage detail"), http.StatusInternalServerError, apierr.CodeInternal, ""},
		{"starting", api.CommandTarget{NodeID: uploadsTestNodeID, State: api.StateProvisioning}, nil, http.StatusConflict, apierr.CodeSandboxNotReady, string(api.StateProvisioning)},
		{"archived", api.CommandTarget{NodeID: uploadsTestNodeID, State: api.StateArchived}, nil, http.StatusConflict, apierr.CodeSandboxNotReady, string(api.StateArchived)},
		{"ready without node", api.CommandTarget{State: api.StateReady}, nil, http.StatusConflict, apierr.CodeSandboxNotReady, string(api.StateReady)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newUploadsTestClock()
			lookup := uploadsTestReadyLookup()
			lookup.set(test.target, test.err)
			handlers := uploadsTestHandlers(t, lookup, uploadsTestFactory(&uploadsTestNodeClient{}), clock)
			recorder := uploadsTestIssue(t, handlers, uploadsTestOwner, uploadsTestSandboxID, `{"path":"x"}`)
			envelope := uploadsTestTargets(t, recorder, test.wantStatus, test.wantCode)
			uploadsTestValidateCreateUploadURL(t, recorder)
			if test.wantState != "" && envelope.Error.Details["state"] != test.wantState {
				t.Errorf("state detail = %v, want %q", envelope.Error.Details["state"], test.wantState)
			}
			calls := lookup.snapshot()
			if len(calls) != 1 || calls[0].Owner != uploadsTestOwner || calls[0].SandboxID != uploadsTestSandboxID {
				t.Errorf("lookup calls = %+v", calls)
			}
		})
	}
}

// TestUploadIssueInternalFault500 validates the approved 500/http_500
// response against the pinned createUploadUrl operation. It depends on the
// shared compat exception the core owner adds; it is intentionally not
// skipped or redirected to another operation.
func TestUploadIssueInternalFault500(t *testing.T) {
	lookup := uploadsTestReadyLookup()
	lookup.set(api.CommandTarget{}, errors.New("database unavailable"))
	handlers := uploadsTestHandlers(t, lookup, uploadsTestFactory(&uploadsTestNodeClient{}), newUploadsTestClock())
	recorder := uploadsTestIssue(t, handlers, uploadsTestOwner, uploadsTestSandboxID, `{"path":"x"}`)
	uploadsTestTargets(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
	uploadsTestValidateCreateUploadURL(t, recorder)
}

func TestUploadIssueReadyStates(t *testing.T) {
	for _, state := range []api.SandboxState{api.StateReady, api.StateIdle, api.StateRunning} {
		t.Run(string(state), func(t *testing.T) {
			lookup := uploadsTestReadyLookup()
			lookup.set(api.CommandTarget{NodeID: uploadsTestNodeID, State: state}, nil)
			handlers := uploadsTestHandlers(t, lookup, uploadsTestFactory(&uploadsTestNodeClient{}), newUploadsTestClock())
			recorder := uploadsTestIssue(t, handlers, uploadsTestOwner, uploadsTestSandboxID, `{"path":"x"}`)
			uploadsTestDecodeURL(t, recorder)
			uploadsTestValidateCreateUploadURL(t, recorder)
		})
	}
}

func TestUploadIssueEmptySandbox(t *testing.T) {
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestFactory(&uploadsTestNodeClient{}), newUploadsTestClock())
	recorder := uploadsTestIssueDirect(t, handlers.Issue, http.MethodPost, uploadsTestOwner, "", `{"path":"x"}`, "application/json")
	uploadsTestTargets(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	uploadsTestValidateCreateUploadURL(t, recorder)
}

func TestUploadIssueMissingContentType(t *testing.T) {
	handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestFactory(&uploadsTestNodeClient{}), newUploadsTestClock())
	recorder := uploadsTestIssueDirect(t, handlers.Issue, http.MethodPost, uploadsTestOwner, uploadsTestSandboxID, `{"path":"x"}`, "")
	uploadsTestDecodeURL(t, recorder)
	uploadsTestValidateCreateUploadURL(t, recorder)
}

// uploadsTestFailBody fails every read.
type uploadsTestFailBody struct{}

func (uploadsTestFailBody) Read([]byte) (int, error) { return 0, errors.New("body failed") }
func (uploadsTestFailBody) Close() error             { return nil }

func TestUploadIssueBodyReadFailure(t *testing.T) {
	tests := []struct {
		name string
		body io.Reader
	}{
		{"read error", uploadsTestFailBody{}},
		{"nil body", nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handlers := uploadsTestHandlers(t, uploadsTestReadyLookup(), uploadsTestFactory(&uploadsTestNodeClient{}), newUploadsTestClock())
			request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/"+uploadsTestSandboxID+"/files/upload-url", test.body)
			request.SetPathValue(api.ParamSandboxID, uploadsTestSandboxID)
			request.Header.Set("Content-Type", "application/json")
			request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: uploadsTestOwner, KeyID: "key_uploads_test"}))
			if test.body == nil {
				request.Body = nil
			}
			recorder := httptest.NewRecorder()
			handlers.Issue(recorder, request)
			uploadsTestTargets(t, recorder, http.StatusBadRequest, apierr.CodeInvalidJSON)
			uploadsTestValidateCreateUploadURL(t, recorder)
		})
	}
}

func TestUploadLookupErrorDoesNotLeak(t *testing.T) {
	const secret = "lookup-secret-code-and-message"
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	lookup := uploadsTestReadyLookup()
	lookup.set(api.CommandTarget{}, apierr.New(http.StatusNotFound, secret, secret))
	handlers := uploadsTestHandlers(t, lookup, uploadsTestFactory(&uploadsTestNodeClient{}), newUploadsTestClock(), func(options *api.UploadsOptions) {
		options.Logger = logger
	})
	recorder := uploadsTestIssue(t, handlers, uploadsTestOwner, uploadsTestSandboxID, `{"path":"x"}`)
	uploadsTestTargets(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
	if bytes.Contains(recorder.Body.Bytes(), []byte(secret)) || strings.Contains(logs.String(), secret) {
		t.Fatal("lookup error text leaked into the response or logs")
	}
}
