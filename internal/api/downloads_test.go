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
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	downloadTestOwner          = "acct_download_test"
	downloadTestOtherOwner     = "acct_download_other"
	downloadTestSandboxID      = "bx_23456789"
	downloadTestOtherSandboxID = "bx_abcdefgh"
	downloadTestNodeID         = "node-downloads-test"
	downloadTestNodeToken      = "node-downloads-token"
	downloadTestGatewayToken   = "gateway-downloads-token"
)

var (
	downloadTestKey     = []byte("downloads-test-signing-key-0123456789abcdef")
	downloadTestBaseURL = "https://api.example.test"
	downloadTestNow     = time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
)

func downloadTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// downloadTestClock is a deterministic clock for issuance and redemption.
type downloadTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newDownloadTestClock() *downloadTestClock {
	return &downloadTestClock{now: downloadTestNow}
}

func (c *downloadTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *downloadTestClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// downloadTestLookupCall records one owner-scoped lookup.
type downloadTestLookupCall struct {
	Owner     string
	SandboxID string
}

// downloadTestLookup is an owner-scoped CommandTargetLookup controlled by the
// test. The handler must call it explicitly on issuance and redemption.
type downloadTestLookup struct {
	mu     sync.Mutex
	target api.CommandTarget
	err    error
	calls  []downloadTestLookupCall
}

func (l *downloadTestLookup) TargetLookup(_ context.Context, owner, sandboxID string) (api.CommandTarget, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, downloadTestLookupCall{Owner: owner, SandboxID: sandboxID})
	return l.target, l.err
}

func (l *downloadTestLookup) set(target api.CommandTarget, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.target = target
	l.err = err
}

func (l *downloadTestLookup) snapshot() []downloadTestLookupCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]downloadTestLookupCall(nil), l.calls...)
}

func (l *downloadTestLookup) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.calls)
}

func downloadTestReadyLookup() *downloadTestLookup {
	return &downloadTestLookup{target: api.CommandTarget{NodeID: downloadTestNodeID, State: api.StateReady}}
}

// downloadTestCancelingLookup cancels the request context inside the lookup so
// the caller observes a cancellation in the middle of the handler.
type downloadTestCancelingLookup struct {
	mu     sync.Mutex
	target api.CommandTarget
	cancel context.CancelFunc
	err    error
	calls  int
}

func (l *downloadTestCancelingLookup) TargetLookup(_ context.Context, _, _ string) (api.CommandTarget, error) {
	l.mu.Lock()
	l.calls++
	l.mu.Unlock()
	l.cancel()
	return l.target, l.err
}

func (l *downloadTestCancelingLookup) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

// downloadTestEnv is a full node stack: nodefake behind nodelink behind the
// authenticated gateway, reached only through the handler's client factory.
type downloadTestEnv struct {
	node  *nodefake.Node
	root  string
	hub   *nodelink.Hub
	link  *nodelink.Link
	serve *nodegateway.ClientFactory
}

func downloadTestStartEnv(t *testing.T) *downloadTestEnv {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	wantToken := sha256.Sum256([]byte(downloadTestNodeToken))
	authn := func(_ context.Context, hello nodelink.Hello) error {
		gotToken := sha256.Sum256([]byte(hello.Token))
		if subtle.ConstantTimeCompare(gotToken[:], wantToken[:]) == 1 {
			return nil
		}
		return errors.New("bad node token")
	}
	// Cleanups run LIFO: gateway first, then the node, then the hub. No role
	// outlives the dependency it dials or serves.
	hub := nodelink.NewHub(authn, downloadTestLogger())
	hubCtx, hubCancel := context.WithCancel(context.Background())
	hubDone := make(chan struct{})
	go func() { _ = hub.Serve(hubCtx, ln); close(hubDone) }()
	t.Cleanup(func() { hubCancel(); <-hubDone })

	root := t.TempDir()
	node := nodefake.New(downloadTestNodeID, root)
	server := grpc.NewServer(nodelink.ServerOptions()...)
	node.Register(server)
	nodeCtx, nodeCancel := context.WithCancel(context.Background())
	nodeDone := make(chan struct{})
	go func() {
		_ = nodelink.Serve(nodeCtx, nodelink.DialConfig{
			Addr: ln.Addr().String(), NodeID: downloadTestNodeID, Token: downloadTestNodeToken,
			Log: downloadTestLogger(), MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		}, server)
		close(nodeDone)
	}()
	t.Cleanup(func() { nodeCancel(); <-nodeDone })

	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()
	link, err := hub.Wait(waitCtx, downloadTestNodeID)
	if err != nil {
		t.Fatalf("node never connected: %v", err)
	}
	if _, err := link.Client.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: downloadTestSandboxID}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	gateway, err := nodegateway.New(hub, downloadTestGatewayToken)
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

	return &downloadTestEnv{
		node:  node,
		root:  root,
		hub:   hub,
		link:  link,
		serve: nodegateway.NewClientFactory(gatewayListener.Addr().String(), downloadTestGatewayToken, nil),
	}
}

func (e *downloadTestEnv) factory() api.NodeClientFactory {
	return func(nodeID string) (nodev1.NodeClient, func() error, error) {
		client, err := e.serve.Dial(nodeID)
		if err != nil {
			return nil, nil, err
		}
		return client, client.Close, nil
	}
}

// writeFile creates one guest file on the nodefake host tree.
func (e *downloadTestEnv) writeFile(t *testing.T, sandboxID, guestPath string, data []byte) {
	t.Helper()
	host := filepath.Join(e.root, "vms", sandboxID, filepath.FromSlash(strings.TrimPrefix(guestPath, "/")))
	if err := os.MkdirAll(filepath.Dir(host), 0o755); err != nil {
		t.Fatalf("mkdir guest file: %v", err)
	}
	if err := os.WriteFile(host, data, 0o644); err != nil {
		t.Fatalf("write guest file: %v", err)
	}
}

func downloadTestHandlers(t *testing.T, lookup api.CommandTargetLookup, factory api.NodeClientFactory, clock *downloadTestClock, opts ...func(*api.DownloadsOptions)) api.DownloadsHandlers {
	t.Helper()
	options := api.DownloadsOptions{
		Lookup:     lookup,
		Clients:    factory,
		SigningKey: downloadTestKey,
		BaseURL:    downloadTestBaseURL,
		Logger:     downloadTestLogger(),
		Now:        clock.Now,
	}
	for _, opt := range opts {
		opt(&options)
	}
	handlers, err := api.NewDownloadsHandlers(options)
	if err != nil {
		t.Fatalf("NewDownloadsHandlers: %v", err)
	}
	return handlers
}

func downloadTestIssue(t *testing.T, handlers api.DownloadsHandlers, owner, sandboxID, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("POST "+api.DownloadsPath, handlers.Issue)
	request := httptest.NewRequest(http.MethodPost,
		"/api/v1/sandboxes/"+sandboxID+"/files/download-url", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if owner != "" {
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner, KeyID: "key_download_test"}))
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	return recorder
}

func downloadTestIssueDirect(handler http.HandlerFunc, method, owner, sandboxID, body, contentType string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "/api/v1/sandboxes/"+sandboxID+"/files/download-url", strings.NewReader(body))
	request.SetPathValue(api.ParamSandboxID, sandboxID)
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if owner != "" {
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner, KeyID: "key_download_test"}))
	}
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	return recorder
}

func downloadTestDecodeURL(t *testing.T, recorder *httptest.ResponseRecorder) api.FileURLResponse {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var response api.FileURLResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		// Never print the body: a success body carries the signed URL.
		t.Fatalf("decode download URL response failed (%T)", err)
	}
	if !response.OK || response.Type != api.TypeFileDownloadURL {
		t.Fatalf("envelope = ok:%v type:%q", response.OK, response.Type)
	}
	return response
}

// downloadTestIssueToken issues one token through the real handler and returns
// it with its response. Failures never print the token.
func downloadTestIssueToken(t *testing.T, handlers api.DownloadsHandlers, owner, sandboxID, filePath string) (string, api.FileURLResponse) {
	t.Helper()
	return downloadTestIssueTokenTTL(t, handlers, owner, sandboxID, filePath, 0, downloadTestBaseURL)
}

func downloadTestIssueTokenTTL(t *testing.T, handlers api.DownloadsHandlers, owner, sandboxID, filePath string, ttlSeconds int, baseURL string) (string, api.FileURLResponse) {
	t.Helper()
	body := map[string]any{"path": filePath}
	if ttlSeconds != 0 {
		body["ttlSeconds"] = ttlSeconds
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal issuance body: %v", err)
	}
	recorder := downloadTestIssue(t, handlers, owner, sandboxID, string(raw))
	response := downloadTestDecodeURL(t, recorder)
	prefix := strings.TrimSuffix(baseURL, "/") + api.DownloadReceiverPrefix + "/"
	token, ok := strings.CutPrefix(response.URL, prefix)
	if !ok || token == "" {
		t.Fatalf("signed URL is not under the receiver prefix")
	}
	return token, response
}

func downloadTestValidateIssue(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	validator := compat.Load(t)
	if err := validator.Response("createDownloadUrl", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
		// Print only the validation error class: validator text can echo the
		// signed URL, which must never appear in a test failure.
		t.Errorf("response does not validate against the pin (status %d, error type %T)", recorder.Code, err)
	}
}

// downloadTestErrorEnvelope is the decoded sandbox.error body.
type downloadTestErrorEnvelope struct {
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

func downloadTestDecodeError(t *testing.T, recorder *httptest.ResponseRecorder) downloadTestErrorEnvelope {
	t.Helper()
	var envelope downloadTestErrorEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
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

func downloadTestTargets(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) downloadTestErrorEnvelope {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d", recorder.Code, status)
	}
	envelope := downloadTestDecodeError(t, recorder)
	if envelope.Code != code {
		t.Fatalf("code = %q, want %q", envelope.Code, code)
	}
	return envelope
}

// downloadTestAssertNoToken proves a response did not echo raw token material.
func downloadTestAssertNoToken(t *testing.T, recorder *httptest.ResponseRecorder, token string) {
	t.Helper()
	if token != "" && bytes.Contains(recorder.Body.Bytes(), []byte(token)) {
		t.Fatal("response body contains the raw signed token")
	}
}

// downloadTestTokenPayload mirrors the token body in tests.
type downloadTestTokenPayload struct {
	Version   int    `json:"v"`
	Purpose   string `json:"u"`
	Owner     string `json:"o"`
	SandboxID string `json:"s"`
	Path      string `json:"p"`
	ExpiresAt int64  `json:"e"`
	Nonce     string `json:"n"`
}

func downloadTestTokenParts(t *testing.T, token string) (payload, signature []byte) {
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

func downloadTestDecodeToken(t *testing.T, token string) downloadTestTokenPayload {
	t.Helper()
	payload, _ := downloadTestTokenParts(t, token)
	var decoded downloadTestTokenPayload
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal("issued token payload is not JSON")
	}
	return decoded
}

// downloadTestSignPayload reproduces the production domain-separated HMAC.
func downloadTestSignPayload(payload, key []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("bbraft/download-url/v1"))
	mac.Write([]byte{0})
	mac.Write(payload)
	return mac.Sum(nil)
}

func downloadTestTokenWithKey(payload, key []byte) string {
	signature := downloadTestSignPayload(payload, key)
	return base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(signature)
}

func downloadTestTokenFromRaw(payload []byte) string {
	return downloadTestTokenWithKey(payload, downloadTestKey)
}

func downloadTestFlipMiddle(value string) string {
	if value == "" {
		return "A"
	}
	middle := len(value) / 2
	replacement := byte('A')
	if value[middle] == 'A' {
		replacement = 'B'
	}
	return value[:middle] + string(replacement) + value[middle+1:]
}

func downloadTestObject(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal test JSON: %v", err)
	}
	return raw
}

// Issue tests ---------------------------------------------------------------

func TestDownloadIssueSuccessPinned(t *testing.T) {
	clock := newDownloadTestClock()
	lookup := downloadTestReadyLookup()
	factory := downloadTestRejectingFactory(t)
	handlers := downloadTestHandlers(t, lookup, factory.factory(), clock)

	recorder := downloadTestIssue(t, handlers, downloadTestOwner, downloadTestSandboxID, `{"path":"reports/summary.csv"}`)
	response := downloadTestDecodeURL(t, recorder)
	downloadTestValidateIssue(t, recorder)

	if response.Method != api.FileMethodGet {
		t.Errorf("method = %q, want GET", response.Method)
	}
	if response.Path != "/home/user/reports/summary.csv" {
		t.Errorf("path = %q, want canonical /home/user path", response.Path)
	}
	if want := downloadTestNow.Add(api.DefaultDownloadTTLSeconds * time.Second); !response.ExpiresAt.Equal(want) {
		t.Errorf("expiresAt = %v, want %v", response.ExpiresAt, want)
	}
	if !strings.HasPrefix(response.URL, downloadTestBaseURL+api.DownloadReceiverPrefix+"/") {
		t.Fatal("URL is not under the receiver prefix")
	}
	if calls := lookup.snapshot(); len(calls) != 1 || calls[0].Owner != downloadTestOwner || calls[0].SandboxID != downloadTestSandboxID {
		t.Errorf("lookup calls = %+v", calls)
	}
	if factory.calls.Load() != 0 {
		t.Fatal("issuance dialed the node")
	}
}

func TestDownloadIssueTTLAndBasePrefix(t *testing.T) {
	clock := newDownloadTestClock()
	lookup := downloadTestReadyLookup()
	const base = "https://api.example.test/bbraft/"
	handlers := downloadTestHandlers(t, lookup, downloadTestRejectingFactory(t).factory(), clock,
		func(o *api.DownloadsOptions) { o.BaseURL = base })

	token, response := downloadTestIssueTokenTTL(t, handlers, downloadTestOwner, downloadTestSandboxID, "/tmp/archive.tar", 60, base)
	if token == "" {
		t.Fatal("empty token")
	}
	if want := downloadTestNow.Add(60 * time.Second); !response.ExpiresAt.Equal(want) {
		t.Errorf("expiresAt = %v, want %v", response.ExpiresAt, want)
	}
	if !strings.HasPrefix(response.URL, "https://api.example.test/bbraft"+api.DownloadReceiverPrefix+"/") {
		t.Fatal("BaseURL path prefix was not preserved")
	}
	downloadTestValidateIssue(t, downloadTestIssue(t, handlers, downloadTestOwner, downloadTestSandboxID, `{"path":"/tmp/archive.tar","ttlSeconds":60}`))
}

func TestDownloadIssueIgnoresHostAndProxyHeaders(t *testing.T) {
	clock := newDownloadTestClock()
	handlers := downloadTestHandlers(t, downloadTestReadyLookup(), downloadTestRejectingFactory(t).factory(), clock)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/"+downloadTestSandboxID+"/files/download-url",
		strings.NewReader(`{"path":"x"}`))
	request.SetPathValue(api.ParamSandboxID, downloadTestSandboxID)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Host", "evil.example.test")
	request.Header.Set("X-Forwarded-Host", "evil.example.test")
	request.Header.Set("X-Forwarded-Proto", "http")
	request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: downloadTestOwner}))
	recorder := httptest.NewRecorder()
	handlers.Issue(recorder, request)
	response := downloadTestDecodeURL(t, recorder)
	if !strings.HasPrefix(response.URL, downloadTestBaseURL+api.DownloadReceiverPrefix+"/") {
		t.Fatal("signed URL used an untrusted Host or proxy header")
	}
}

func TestDownloadIssueErrorsPinned(t *testing.T) {
	tests := []struct {
		name       string
		owner      string
		target     api.CommandTarget
		lookupErr  error
		body       string
		content    string
		wantStatus int
		wantCode   string
	}{
		{name: "missing identity", owner: "", body: `{"path":"x"}`, wantStatus: http.StatusUnauthorized, wantCode: apierr.CodeUnauthorized},
		{name: "empty body", owner: downloadTestOwner, body: "", wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidJSON},
		{name: "malformed json", owner: downloadTestOwner, body: `{"path":`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidJSON},
		{name: "unknown member", owner: downloadTestOwner, body: `{"path":"x","bogus":1}`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidJSON},
		{name: "missing path", owner: downloadTestOwner, body: `{}`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidJSON},
		{name: "null path", owner: downloadTestOwner, body: `{"path":null}`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidJSON},
		{name: "non-string path", owner: downloadTestOwner, body: `{"path":12}`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidJSON},
		{name: "non-json media", owner: downloadTestOwner, body: `{"path":"x"}`, content: "text/plain", wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidJSON},
		{name: "ttl null", owner: downloadTestOwner, body: `{"path":"x","ttlSeconds":null}`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidTimeout},
		{name: "ttl zero", owner: downloadTestOwner, body: `{"path":"x","ttlSeconds":0}`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidTimeout},
		{name: "ttl negative", owner: downloadTestOwner, body: `{"path":"x","ttlSeconds":-1}`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidTimeout},
		{name: "ttl too long", owner: downloadTestOwner, body: `{"path":"x","ttlSeconds":3601}`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidTimeout},
		{name: "ttl fraction", owner: downloadTestOwner, body: `{"path":"x","ttlSeconds":1.5}`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidTimeout},
		{name: "ttl string", owner: downloadTestOwner, body: `{"path":"x","ttlSeconds":"900"}`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidTimeout},
		{name: "path outside roots", owner: downloadTestOwner, body: `{"path":"/etc/passwd"}`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidPath},
		{name: "path traversal", owner: downloadTestOwner, body: `{"path":"../../etc/passwd"}`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidPath},
		{name: "path root", owner: downloadTestOwner, body: `{"path":"/home/user"}`, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidPath},
		{name: "path nul", owner: downloadTestOwner, body: "{\"path\":\"/home/user/a\\u0000b\"}", wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidPath},
		{name: "sandbox missing", owner: downloadTestOwner, body: `{"path":"x"}`, lookupErr: api.ErrCommandTargetNotFound, wantStatus: http.StatusNotFound, wantCode: apierr.CodeNotFound},
		{name: "not ready", owner: downloadTestOwner, target: api.CommandTarget{NodeID: downloadTestNodeID, State: api.StateArchived}, body: `{"path":"x"}`, wantStatus: http.StatusConflict, wantCode: apierr.CodeSandboxNotReady},
		{name: "ready without node", owner: downloadTestOwner, target: api.CommandTarget{State: api.StateReady}, body: `{"path":"x"}`, wantStatus: http.StatusInternalServerError, wantCode: apierr.CodeInternal},
		{name: "lookup fault", owner: downloadTestOwner, body: `{"path":"x"}`, lookupErr: errors.New("db unavailable"), wantStatus: http.StatusInternalServerError, wantCode: apierr.CodeInternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lookup := downloadTestReadyLookup()
			if test.target != (api.CommandTarget{}) || test.lookupErr != nil {
				lookup.set(test.target, test.lookupErr)
			}
			if test.name == "missing identity" {
				lookup = downloadTestReadyLookup()
			}
			handlers := downloadTestHandlers(t, lookup, downloadTestRejectingFactory(t).factory(), newDownloadTestClock())
			contentType := test.content
			if contentType == "" {
				contentType = "application/json"
			}
			recorder := downloadTestIssueDirect(handlers.Issue, http.MethodPost, test.owner, downloadTestSandboxID, test.body, contentType)
			envelope := downloadTestTargets(t, recorder, test.wantStatus, test.wantCode)
			if test.wantCode == apierr.CodeSandboxNotReady {
				if state, _ := envelope.Error.Details["state"].(string); state != string(api.StateArchived) {
					t.Errorf("state detail = %q", state)
				}
				if retryable, _ := envelope.Error.Details["retryable"].(bool); retryable {
					t.Error("not-ready answer is retryable")
				}
			}
			if test.wantStatus != http.StatusInternalServerError {
				downloadTestValidateIssue(t, recorder)
			}
			if test.wantCode == apierr.CodeNotFound || test.wantCode == apierr.CodeSandboxNotReady {
				if calls := lookup.snapshot(); len(calls) != 1 || calls[0].Owner != test.owner {
					t.Errorf("lookup calls = %+v", calls)
				}
			}
		})
	}
}

func TestDownloadIssueWrongMethod(t *testing.T) {
	lookup := downloadTestReadyLookup()
	handlers := downloadTestHandlers(t, lookup, downloadTestRejectingFactory(t).factory(), newDownloadTestClock())
	recorder := downloadTestIssueDirect(handlers.Issue, http.MethodGet, downloadTestOwner, downloadTestSandboxID, `{"path":"x"}`, "application/json")
	downloadTestTargets(t, recorder, http.StatusMethodNotAllowed, apierr.CodeNotFound)
	if lookup.count() != 0 {
		t.Fatal("wrong method reached the lookup")
	}
}

// downloadTestRejectingFactory fails the test if the handler dials the node.
func downloadTestRejectingFactory(t *testing.T) *downloadTestFactoryCounter {
	t.Helper()
	return &downloadTestFactoryCounter{t: t}
}

type downloadTestFactoryCounter struct {
	t     *testing.T
	calls atomic.Int64
}

func (f *downloadTestFactoryCounter) factory() api.NodeClientFactory {
	return func(string) (nodev1.NodeClient, func() error, error) {
		f.calls.Add(1)
		return nil, nil, errors.New("node should not be dialed")
	}
}

// downloadTestNodeClient is a nodev1.NodeClient whose only useful method is
// ReadFile.
type downloadTestNodeClient struct {
	nodev1.NodeClient
	readFile func(context.Context, *nodev1.ReadFileRequest) (nodev1.Node_ReadFileClient, error)
}

func (c *downloadTestNodeClient) ReadFile(ctx context.Context, request *nodev1.ReadFileRequest, _ ...grpc.CallOption) (nodev1.Node_ReadFileClient, error) {
	return c.readFile(ctx, request)
}

func downloadTestFactory(client nodev1.NodeClient) api.NodeClientFactory {
	return func(string) (nodev1.NodeClient, func() error, error) {
		return client, func() error { return nil }, nil
	}
}

// downloadTestStream is a nodev1.ReadFile stream controlled by the test.
type downloadTestStream struct {
	grpc.ClientStream
	ctx  context.Context
	recv func() (*nodev1.ReadFileResponse, error)
	mu   sync.Mutex
	n    int
}

func (s *downloadTestStream) Context() context.Context { return s.ctx }

func (s *downloadTestStream) Recv() (*nodev1.ReadFileResponse, error) {
	s.mu.Lock()
	s.n++
	s.mu.Unlock()
	return s.recv()
}

func (s *downloadTestStream) recvCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

func downloadTestInfoMessage(filePath string, size int64) *nodev1.ReadFileResponse {
	return &nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Info{Info: &nodev1.FileInfo{
		Path: filePath,
		Name: path.Base(filePath),
		Type: nodev1.FileType_FILE_TYPE_REGULAR,
		Size: size,
		Mode: 0o644,
	}}}
}

func downloadTestDataMessage(data []byte) *nodev1.ReadFileResponse {
	return &nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Data{Data: data}}
}

// downloadTestServe starts a real HTTP server around one handler route, for
// tests that need the net/http abort/recovery behaviour.
func downloadTestServe(t *testing.T, handler http.HandlerFunc, pattern string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc(pattern, handler)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}
