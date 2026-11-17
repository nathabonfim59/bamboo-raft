package api

// Handler tests for POST /api/v1/sandboxes/{sandboxId}/sshkey. The node is an
// instrumented nodev1 client and the store is an in-memory model of the
// owner-scoped, generation-fenced SSH metadata write. The real gateway and the
// real database have their own suites.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const (
	sshTestOwnerID   = "018f4b1c-7e2d-7a3b-9c4d-123456789abc"
	sshTestSandboxID = "bx_abcdefgh"
	sshTestNodeID    = "ssh-test-node"
)

// sshTestKeyLine builds one well-formed ssh-ed25519 public key line.
func sshTestKeyLine(fill byte, comment string) string {
	return sshTestKeyLineType("ssh-ed25519", fill, comment)
}

// sshTestKeyLineType builds one well-formed public key line of keyType.
func sshTestKeyLineType(keyType string, fill byte, comment string) string {
	blob := binary.BigEndian.AppendUint32(nil, uint32(len(keyType)))
	blob = append(blob, keyType...)
	blob = binary.BigEndian.AppendUint32(blob, 32)
	blob = append(blob, bytes.Repeat([]byte{fill}, 32)...)
	line := keyType + " " + base64.StdEncoding.EncodeToString(blob)
	if comment != "" {
		line += " " + comment
	}
	return line
}

// sshTestWireString encodes one SSH string (uint32 length plus bytes).
func sshTestWireString(value []byte) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(value))), value...)
}

// sshTestWireBlob concatenates SSH wire fields into one blob.
func sshTestWireBlob(parts ...[]byte) []byte {
	var out []byte
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}

func sshTestBody(t *testing.T, key string) string {
	t.Helper()
	raw, err := json.Marshal(SSHKeyRequest{Key: key})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return string(raw)
}

func sshTestRow() db.Sandbox {
	node := sshTestNodeID
	return db.Sandbox{
		ID:                  sshTestSandboxID,
		OwnerID:             uuid.MustParse(sshTestOwnerID),
		NodeID:              &node,
		State:               db.StateReady,
		LifecycleGeneration: 7,
		Version:             3,
	}
}

// sshTestStore models the accepted SSH metadata contract in memory.
type sshTestStore struct {
	mu        sync.Mutex
	row       db.Sandbox
	lookupErr error
	lookupRow *db.Sandbox
	setErr    error
	returnRow *db.Sandbox
	afterSet  func()

	lookupCalls int
	setCalls    int
	setParams   []db.SetSSHAccessParams
}

func newSSHTestStore(row db.Sandbox) *sshTestStore {
	return &sshTestStore{row: row}
}

func (s *sshTestStore) Sandbox(_ context.Context, owner uuid.UUID, id string) (db.Sandbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lookupCalls++
	if s.lookupErr != nil {
		return db.Sandbox{}, s.lookupErr
	}
	if s.lookupRow != nil {
		return *s.lookupRow, nil
	}
	if s.row.ID != id || s.row.OwnerID != owner {
		return db.Sandbox{}, db.ErrNotFound
	}
	return s.row, nil
}

func (s *sshTestStore) SetSandboxSSHAccess(_ context.Context, p db.SetSSHAccessParams) (db.Sandbox, error) {
	s.mu.Lock()
	s.setCalls++
	s.setParams = append(s.setParams, p)
	hook := s.afterSet
	if s.setErr != nil {
		err := s.setErr
		s.mu.Unlock()
		return db.Sandbox{}, err
	}
	if s.returnRow != nil {
		row := *s.returnRow
		s.mu.Unlock()
		if hook != nil {
			hook()
		}
		return row, nil
	}
	if s.row.ID != p.SandboxID || s.row.OwnerID != p.Owner {
		s.mu.Unlock()
		return db.Sandbox{}, db.ErrNotFound
	}
	if s.row.NodeID == nil || *s.row.NodeID != p.NodeID || s.row.LifecycleGeneration != p.Generation {
		s.mu.Unlock()
		return db.Sandbox{}, db.ErrSSHAccessStale
	}
	switch s.row.State {
	case db.StateReady, db.StateIdle, db.StateRunning:
	default:
		s.mu.Unlock()
		return db.Sandbox{}, db.ErrSSHAccessStale
	}
	s.row.SSH = &db.SSHAccess{Endpoint: p.Endpoint, HostKey: p.HostKey}
	row := s.row
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return row, nil
}

func (s *sshTestStore) snapshot() db.Sandbox {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.row
}

func (s *sshTestStore) mutate(fn func(*db.Sandbox)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.row)
}

func (s *sshTestStore) calls() (lookup, set int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookupCalls, s.setCalls
}

func (s *sshTestStore) params() []db.SetSSHAccessParams {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.setParams)
}

// sshTestNode is an instrumented nodev1 client. Only AddSSHKey is implemented;
// every other method is unreachable because the handler calls nothing else.
type sshTestNode struct {
	nodev1.NodeClient
	mu        sync.Mutex
	calls     int
	response  *nodev1.SSHAccess
	err       error
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

func newSSHTestNode(response *nodev1.SSHAccess) *sshTestNode {
	return &sshTestNode{response: response}
}

func newSSHTestBlockingNode(response *nodev1.SSHAccess) *sshTestNode {
	return &sshTestNode{response: response, started: make(chan struct{}), release: make(chan struct{})}
}

func (n *sshTestNode) AddSSHKey(_ context.Context, _ *nodev1.AddSSHKeyRequest, _ ...grpc.CallOption) (*nodev1.SSHAccess, error) {
	n.mu.Lock()
	n.calls++
	n.mu.Unlock()
	if n.started != nil {
		n.startOnce.Do(func() { close(n.started) })
	}
	if n.release != nil {
		<-n.release
	}
	return n.response, n.err
}

func (n *sshTestNode) callCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.calls
}

func sshTestFactory(client nodev1.NodeClient, err error) NodeClientFactory {
	return func(string) (nodev1.NodeClient, func() error, error) {
		if err != nil {
			return nil, nil, err
		}
		return client, func() error { return nil }, nil
	}
}

// sshTestAwait waits for ch with a bound, so a stuck goroutine fails the test
// instead of hanging it. It is never used as a sleep.
func sshTestAwait(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
		t.Fatalf("%s: timed out after 5s", label)
	}
}

// sshTestBlocked is one handler call blocked inside AddSSHKey. Its cleanup
// always cancels, releases and joins under a bound, so a fatal assertion in the
// test body cannot leave the handler goroutine behind.
type sshTestBlocked struct {
	recorder    *httptest.ResponseRecorder
	node        *sshTestNode
	done        chan struct{}
	cancel      context.CancelFunc
	releaseOnce func()
}

func sshTestBlockCall(t *testing.T, handler http.Handler, request *http.Request, node *sshTestNode, cancel context.CancelFunc) *sshTestBlocked {
	t.Helper()
	blocked := &sshTestBlocked{
		recorder:    httptest.NewRecorder(),
		node:        node,
		done:        make(chan struct{}),
		cancel:      cancel,
		releaseOnce: sync.OnceFunc(func() { close(node.release) }),
	}
	mux := sshTestMux(handler)
	go func() {
		defer close(blocked.done)
		mux.ServeHTTP(blocked.recorder, request)
	}()
	t.Cleanup(func() {
		if blocked.cancel != nil {
			blocked.cancel()
		}
		blocked.release()
		blocked.join(t)
	})
	return blocked
}

func (b *sshTestBlocked) release() { b.releaseOnce() }

func (b *sshTestBlocked) waitStarted(t *testing.T) {
	t.Helper()
	sshTestAwait(t, b.node.started, "AddSSHKey start")
}

func (b *sshTestBlocked) join(t *testing.T) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-b.done:
	case <-timer.C:
		t.Errorf("handler did not finish after release")
	}
}

func (b *sshTestBlocked) finish(t *testing.T) {
	t.Helper()
	b.release()
	b.join(t)
}

func sshTestAccess(fill byte, host string, port uint32) *nodev1.SSHAccess {
	return &nodev1.SSHAccess{
		User:    "user",
		Host:    host,
		Port:    port,
		HostKey: sshTestKeyLine(fill, "host"),
	}
}

func sshTestNewHandler(t *testing.T, store SSHKeyStore, clients NodeClientFactory, logger *slog.Logger) http.Handler {
	t.Helper()
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	handler, err := NewSSHKeyHandler(SSHKeyOptions{Store: store, Clients: clients, Logger: logger})
	if err != nil {
		t.Fatalf("NewSSHKeyHandler: %v", err)
	}
	return handler
}

func sshTestMux(handler http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("POST "+PathSandboxSSHKey, handler)
	return mux
}

func sshTestRequest(t *testing.T, owner, sandboxID, body string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost,
		"/api/v1/sandboxes/"+sandboxID+"/sshkey", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if owner != "" {
		request = request.WithContext(auth.WithIdentity(request.Context(), auth.Identity{Owner: owner}))
	}
	return request
}

func sshTestServe(t *testing.T, handler http.Handler, request *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	sshTestMux(handler).ServeHTTP(recorder, request)
	return recorder
}

type sshTestSuccess struct {
	OK          bool   `json:"ok"`
	Type        string `json:"type"`
	Success     bool   `json:"success"`
	MachineIP   string `json:"machineIp"`
	SSHUser     string `json:"sshUser"`
	SSHEndpoint string `json:"sshEndpoint"`
	HostKey     string `json:"hostKey"`
}

type sshTestErrorEnvelope struct {
	OK        bool   `json:"ok"`
	Type      string `json:"type"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"requestId"`
	Error     struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Status  int    `json:"status"`
		Details struct {
			Retryable         *bool `json:"retryable"`
			RetryAfterSeconds *int  `json:"retryAfterSeconds"`
		} `json:"details"`
	} `json:"error"`
}

func sshTestAssertError(t *testing.T, recorder *httptest.ResponseRecorder, wantStatus int, wantCode string) sshTestErrorEnvelope {
	t.Helper()
	if recorder.Code != wantStatus {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, wantStatus, recorder.Body.Bytes())
	}
	var body sshTestErrorEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error envelope: %v; body: %s", err, recorder.Body.Bytes())
	}
	if body.OK || body.Type != "sandbox.error" || body.Status != wantStatus || body.Code != wantCode ||
		body.Error.Code != wantCode || body.Error.Status != wantStatus || body.Error.Message == "" ||
		body.RequestID == "" {
		t.Fatalf("error envelope = %+v, want ok:false type:sandbox.error status:%d code:%q with nested fields",
			body, wantStatus, wantCode)
	}
	return body
}

func TestSSHKeyHandlerSuccess(t *testing.T) {
	store := newSSHTestStore(sshTestRow())
	key := sshTestKeyLine(0xaa, "user@example")
	access := sshTestAccess(0xbb, "203.0.113.10", 22001)
	node := newSSHTestNode(access)
	handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)

	recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", recorder.Code, recorder.Body.Bytes())
	}
	var response sshTestSuccess
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode success: %v; body: %s", err, recorder.Body.Bytes())
	}
	want := sshTestSuccess{
		OK: true, Type: TypeSSHKeyConfigured, Success: true,
		MachineIP: "203.0.113.10", SSHUser: "user",
		SSHEndpoint: "203.0.113.10:22001", HostKey: access.HostKey,
	}
	if response != want {
		t.Errorf("response = %+v, want %+v", response, want)
	}
	if node.callCount() != 1 {
		t.Errorf("AddSSHKey calls = %d, want 1", node.callCount())
	}
	lookup, set := store.calls()
	if lookup != 1 || set != 1 {
		t.Errorf("store calls = lookup:%d set:%d, want 1/1", lookup, set)
	}
	params := store.params()
	if len(params) != 1 {
		t.Fatalf("set params = %d, want 1", len(params))
	}
	wantParams := db.SetSSHAccessParams{
		Owner:      uuid.MustParse(sshTestOwnerID),
		SandboxID:  sshTestSandboxID,
		NodeID:     sshTestNodeID,
		Generation: 7,
		Endpoint:   "203.0.113.10:22001",
		HostKey:    access.HostKey,
	}
	if params[0] != wantParams {
		t.Errorf("set params = %+v, want %+v", params[0], wantParams)
	}

	// The pinned sshKey operation is the source of truth for the success
	// envelope. The cache may be absent in a worktree, in which case the
	// validator skips this assertion by design.
	validator := compat.Load(t)
	if err := validator.Response("sshKey", recorder.Code, recorder.Header(), recorder.Body.Bytes()); err != nil {
		t.Errorf("response does not validate against the pin: %v; body: %s", err, recorder.Body.Bytes())
	}
}

func TestSSHKeyHandlerBadBodies(t *testing.T) {
	valid := sshTestKeyLine(0xaa, "user@example")
	tests := []struct {
		name string
		body string
	}{
		{name: "empty body", body: ""},
		{name: "invalid json", body: "{"},
		{name: "trailing json", body: sshTestBody(t, valid) + "x"},
		{name: "unknown member", body: `{"key":"` + valid + `","other":true}`},
		{name: "missing key", body: `{}`},
		{name: "null key", body: `{"key":null}`},
		{name: "number key", body: `{"key":123}`},
		{name: "empty key", body: `{"key":""}`},
		{name: "whitespace key", body: `{"key":"   "}`},
		{name: "private key", body: sshTestBody(t, "-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n-----END OPENSSH PRIVATE KEY-----")},
		{name: "multiline key", body: sshTestBody(t, valid+"\n"+valid)},
		{name: "options", body: sshTestBody(t, `command="id" `+valid)},
		{name: "bad base64", body: sshTestBody(t, "ssh-ed25519 !!!!")},
		{name: "embedded type mismatch", body: sshTestBody(t, "ssh-rsa "+strings.Split(valid, " ")[1]+" user")},
		{name: "unknown type", body: sshTestBody(t, "ssh-magic AAAA user")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newSSHTestStore(sshTestRow())
			node := newSSHTestNode(sshTestAccess(0xbb, "203.0.113.10", 22))
			handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)
			recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, test.body))
			sshTestAssertError(t, recorder, http.StatusBadRequest, apierr.CodeInvalidJSON)
			lookup, set := store.calls()
			if lookup != 0 || set != 0 || node.callCount() != 0 {
				t.Errorf("bad body reached lookup/AddSSHKey/store: lookup=%d node=%d set=%d",
					lookup, node.callCount(), set)
			}
		})
	}
}

func TestSSHKeyHandlerAuthAndLookup(t *testing.T) {
	key := sshTestKeyLine(0xaa, "user@example")

	t.Run("missing identity", func(t *testing.T) {
		store := newSSHTestStore(sshTestRow())
		handler := sshTestNewHandler(t, store, sshTestFactory(newSSHTestNode(nil), nil), nil)
		recorder := sshTestServe(t, handler, sshTestRequest(t, "", sshTestSandboxID, sshTestBody(t, key)))
		sshTestAssertError(t, recorder, http.StatusUnauthorized, apierr.CodeUnauthorized)
		if lookup, set := store.calls(); lookup != 0 || set != 0 {
			t.Errorf("unauthenticated request reached the store: lookup=%d set=%d", lookup, set)
		}
	})

	t.Run("malformed owner", func(t *testing.T) {
		store := newSSHTestStore(sshTestRow())
		handler := sshTestNewHandler(t, store, sshTestFactory(newSSHTestNode(nil), nil), nil)
		recorder := sshTestServe(t, handler, sshTestRequest(t, "not-a-uuid", sshTestSandboxID, sshTestBody(t, key)))
		sshTestAssertError(t, recorder, http.StatusUnauthorized, apierr.CodeUnauthorized)
	})

	t.Run("unknown sandbox", func(t *testing.T) {
		store := newSSHTestStore(sshTestRow())
		node := newSSHTestNode(nil)
		handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)
		recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, "bx_zzzzzzzz", sshTestBody(t, key)))
		sshTestAssertError(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
		if node.callCount() != 0 {
			t.Errorf("AddSSHKey calls = %d, want 0", node.callCount())
		}
	})

	t.Run("foreign sandbox", func(t *testing.T) {
		row := sshTestRow()
		row.OwnerID = uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-999999999999")
		store := newSSHTestStore(row)
		node := newSSHTestNode(nil)
		handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)
		recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
		sshTestAssertError(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
		if node.callCount() != 0 {
			t.Errorf("foreign sandbox reached the node: %d calls", node.callCount())
		}
	})

	t.Run("store failure", func(t *testing.T) {
		store := newSSHTestStore(sshTestRow())
		store.lookupErr = errors.New("database unavailable for " + key)
		node := newSSHTestNode(nil)
		handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)
		recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
		sshTestAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
		if bytes.Contains(recorder.Body.Bytes(), []byte(key)) {
			t.Errorf("response leaked the public key: %s", recorder.Body.Bytes())
		}
		if node.callCount() != 0 {
			t.Errorf("store failure reached the node: %d calls", node.callCount())
		}
	})
}

func TestSSHKeyHandlerStates(t *testing.T) {
	key := sshTestKeyLine(0xaa, "user@example")
	nonReady := []db.SandboxState{
		db.StateInit, db.StateProvisioning, db.StateProvisioned, db.StateCloning,
		db.StateArchiving, db.StateArchived, db.StateError, db.StateCancelled,
	}
	for _, state := range nonReady {
		t.Run(string(state), func(t *testing.T) {
			row := sshTestRow()
			row.State = state
			store := newSSHTestStore(row)
			node := newSSHTestNode(sshTestAccess(0xbb, "203.0.113.10", 22))
			handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)
			recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
			sshTestAssertError(t, recorder, http.StatusBadRequest, apierr.CodeMachineNotRunning)
			if node.callCount() != 0 {
				t.Errorf("%s reached AddSSHKey", state)
			}
			if _, set := store.calls(); set != 0 {
				t.Errorf("%s wrote metadata", state)
			}
		})
	}

	t.Run("ready without node", func(t *testing.T) {
		row := sshTestRow()
		row.NodeID = nil
		store := newSSHTestStore(row)
		node := newSSHTestNode(sshTestAccess(0xbb, "203.0.113.10", 22))
		handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)
		recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
		sshTestAssertError(t, recorder, http.StatusBadRequest, apierr.CodeMachineNotRunning)
		if node.callCount() != 0 {
			t.Errorf("node-less sandbox reached AddSSHKey")
		}
	})

	ready := []db.SandboxState{db.StateReady, db.StateIdle, db.StateRunning}
	for _, state := range ready {
		t.Run("accepted "+string(state), func(t *testing.T) {
			row := sshTestRow()
			row.State = state
			store := newSSHTestStore(row)
			handler := sshTestNewHandler(t, store, sshTestFactory(newSSHTestNode(sshTestAccess(0xbb, "203.0.113.10", 22)), nil), nil)
			recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
			if recorder.Code != http.StatusOK {
				t.Fatalf("%s status = %d; body: %s", state, recorder.Code, recorder.Body.Bytes())
			}
		})
	}
}

func TestSSHKeyHandlerNodeErrors(t *testing.T) {
	key := sshTestKeyLine(0xaa, "user@example")
	tests := []struct {
		name          string
		err           error
		wantStatus    int
		wantCode      string
		wantRetryable *bool
		wantRetrySecs *int
	}{
		{name: "invalid argument", err: status.Error(codes.InvalidArgument, "bad key "+key),
			wantStatus: http.StatusBadRequest, wantCode: apierr.CodeInvalidJSON},
		{name: "not found", err: status.Error(codes.NotFound, "gone"),
			wantStatus: http.StatusNotFound, wantCode: apierr.CodeNotFound},
		{name: "failed precondition", err: status.Error(codes.FailedPrecondition, "not ready"),
			wantStatus: http.StatusBadRequest, wantCode: apierr.CodeMachineNotRunning},
		{name: "unavailable", err: status.Error(codes.Unavailable, "host key missing"),
			wantStatus: http.StatusServiceUnavailable, wantCode: apierr.CodeSSHHostKeyUnavailable,
			wantRetrySecs: new(sshHostKeyRetrySeconds)},
		{name: "resource exhausted", err: status.Error(codes.ResourceExhausted, "too large"),
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed, wantRetryable: new(false)},
		{name: "aborted", err: status.Error(codes.Aborted, "deleted"),
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed, wantRetryable: new(true)},
		{name: "deadline", err: status.Error(codes.DeadlineExceeded, "slow"),
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed, wantRetryable: new(true)},
		{name: "internal", err: status.Error(codes.Internal, "boom"),
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed, wantRetryable: new(true)},
		{name: "unknown", err: status.Error(codes.Unknown, "?"),
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed, wantRetryable: new(true)},
		{name: "permission", err: status.Error(codes.PermissionDenied, "no"),
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed, wantRetryable: new(true)},
		{name: "plain error", err: errors.New("transport " + key),
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed, wantRetryable: new(true)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newSSHTestStore(sshTestRow())
			node := newSSHTestNode(nil)
			node.err = test.err
			handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)
			recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
			body := sshTestAssertError(t, recorder, test.wantStatus, test.wantCode)
			if test.wantRetryable != nil {
				if body.Error.Details.Retryable == nil || *body.Error.Details.Retryable != *test.wantRetryable {
					t.Errorf("retryable = %v, want %v", body.Error.Details.Retryable, *test.wantRetryable)
				}
			}
			if test.wantRetrySecs != nil {
				if body.Error.Details.RetryAfterSeconds == nil ||
					*body.Error.Details.RetryAfterSeconds != *test.wantRetrySecs {
					t.Errorf("retryAfterSeconds = %v, want %d",
						body.Error.Details.RetryAfterSeconds, *test.wantRetrySecs)
				}
			}
			if bytes.Contains(recorder.Body.Bytes(), []byte(key)) {
				t.Errorf("response leaked the public key: %s", recorder.Body.Bytes())
			}
			if _, set := store.calls(); set != 0 {
				t.Errorf("failed node call wrote metadata")
			}
		})
	}

	t.Run("dial failure", func(t *testing.T) {
		store := newSSHTestStore(sshTestRow())
		node := newSSHTestNode(nil)
		handler := sshTestNewHandler(t, store, sshTestFactory(node, errors.New("node offline")), nil)
		recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
		body := sshTestAssertError(t, recorder, http.StatusBadGateway, apierr.CodeSandboxDirectFailed)
		if body.Error.Details.Retryable == nil || !*body.Error.Details.Retryable {
			t.Errorf("dial failure retryable = %v, want true", body.Error.Details.Retryable)
		}
		if node.callCount() != 0 {
			t.Errorf("dial failure still called AddSSHKey")
		}
	})

	t.Run("nil client", func(t *testing.T) {
		store := newSSHTestStore(sshTestRow())
		handler := sshTestNewHandler(t, store, sshTestFactory(nil, nil), nil)
		recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
		sshTestAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
	})
}

func TestSSHKeyHandlerNodeResults(t *testing.T) {
	key := sshTestKeyLine(0xaa, "user@example")
	tests := []struct {
		name       string
		response   *nodev1.SSHAccess
		wantStatus int
		wantCode   string
	}{
		{name: "nil", response: nil, wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "empty host key", response: &nodev1.SSHAccess{User: "user", Host: "203.0.113.10", Port: 22},
			wantStatus: http.StatusServiceUnavailable, wantCode: apierr.CodeSSHHostKeyUnavailable},
		{name: "blank host key", response: &nodev1.SSHAccess{User: "user", Host: "203.0.113.10", Port: 22, HostKey: "  \t "},
			wantStatus: http.StatusServiceUnavailable, wantCode: apierr.CodeSSHHostKeyUnavailable},
		{name: "malformed host key", response: &nodev1.SSHAccess{User: "user", Host: "203.0.113.10", Port: 22, HostKey: "ssh-ed25519 !!!"},
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "empty user", response: &nodev1.SSHAccess{Host: "203.0.113.10", Port: 22, HostKey: sshTestKeyLine(0xbb, "h")},
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "padded user", response: &nodev1.SSHAccess{User: " user", Host: "203.0.113.10", Port: 22, HostKey: sshTestKeyLine(0xbb, "h")},
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "user with space", response: &nodev1.SSHAccess{User: "us er", Host: "203.0.113.10", Port: 22, HostKey: sshTestKeyLine(0xbb, "h")},
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "empty host", response: &nodev1.SSHAccess{User: "user", Port: 22, HostKey: sshTestKeyLine(0xbb, "h")},
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "padded host", response: &nodev1.SSHAccess{User: "user", Host: " 203.0.113.10", Port: 22, HostKey: sshTestKeyLine(0xbb, "h")},
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "host with space", response: &nodev1.SSHAccess{User: "user", Host: "203.0.113.10 extra", Port: 22, HostKey: sshTestKeyLine(0xbb, "h")},
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "zero port", response: &nodev1.SSHAccess{User: "user", Host: "203.0.113.10", Port: 0, HostKey: sshTestKeyLine(0xbb, "h")},
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "port above range", response: &nodev1.SSHAccess{User: "user", Host: "203.0.113.10", Port: 65536, HostKey: sshTestKeyLine(0xbb, "h")},
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "brackets not ip", response: &nodev1.SSHAccess{User: "user", Host: "[node]", Port: 22, HostKey: sshTestKeyLine(0xbb, "h")},
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
		{name: "hostname with colon", response: &nodev1.SSHAccess{User: "user", Host: "node:22", Port: 22, HostKey: sshTestKeyLine(0xbb, "h")},
			wantStatus: http.StatusBadGateway, wantCode: apierr.CodeSandboxDirectFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newSSHTestStore(sshTestRow())
			node := newSSHTestNode(test.response)
			handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)
			recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
			sshTestAssertError(t, recorder, test.wantStatus, test.wantCode)
			if _, set := store.calls(); set != 0 {
				t.Errorf("invalid node result wrote metadata")
			}
		})
	}
}

func TestSSHKeyHandlerStoreErrors(t *testing.T) {
	key := sshTestKeyLine(0xaa, "user@example")
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{name: "stale", err: db.ErrSSHAccessStale, wantStatus: http.StatusBadRequest, wantCode: apierr.CodeMachineNotRunning},
		{name: "not found", err: db.ErrNotFound, wantStatus: http.StatusNotFound, wantCode: apierr.CodeNotFound},
		{name: "invalid metadata", err: db.ErrInvalidSSHAccess, wantStatus: http.StatusInternalServerError, wantCode: apierr.CodeInternal},
		{name: "operational", err: errors.New("write failed " + key), wantStatus: http.StatusInternalServerError, wantCode: apierr.CodeInternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newSSHTestStore(sshTestRow())
			store.setErr = test.err
			handler := sshTestNewHandler(t, store, sshTestFactory(newSSHTestNode(sshTestAccess(0xbb, "203.0.113.10", 22)), nil), nil)
			recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
			sshTestAssertError(t, recorder, test.wantStatus, test.wantCode)
			if bytes.Contains(recorder.Body.Bytes(), []byte(key)) {
				t.Errorf("response leaked the public key: %s", recorder.Body.Bytes())
			}
		})
	}
}

func TestSSHKeyHandlerStoredRowValidation(t *testing.T) {
	key := sshTestKeyLine(0xaa, "user@example")
	validRow := sshTestRow()
	validRow.SSH = &db.SSHAccess{Endpoint: "203.0.113.10:22", HostKey: sshTestKeyLine(0xbb, "host")}
	tests := []struct {
		name string
		row  func() db.Sandbox
	}{
		{name: "wrong owner", row: func() db.Sandbox {
			row := validRow
			row.OwnerID = uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-999999999999")
			return row
		}},
		{name: "wrong id", row: func() db.Sandbox { row := validRow; row.ID = "bx_zzzzzzzz"; return row }},
		{name: "wrong node", row: func() db.Sandbox {
			row := validRow
			node := "other-node"
			row.NodeID = &node
			return row
		}},
		{name: "wrong generation", row: func() db.Sandbox { row := validRow; row.LifecycleGeneration++; return row }},
		{name: "missing pair", row: func() db.Sandbox { row := validRow; row.SSH = nil; return row }},
		{name: "archived state", row: func() db.Sandbox { row := validRow; row.State = db.StateArchived; return row }},
		{name: "deleted row", row: func() db.Sandbox {
			row := validRow
			now := time.Now()
			row.DeletedAt = &now
			return row
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newSSHTestStore(sshTestRow())
			row := test.row()
			store.returnRow = &row
			handler := sshTestNewHandler(t, store, sshTestFactory(newSSHTestNode(sshTestAccess(0xbb, "203.0.113.10", 22)), nil), nil)
			recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
			sshTestAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
		})
	}
}

func TestSSHKeyHandlerDelayedStopFence(t *testing.T) {
	store := newSSHTestStore(sshTestRow())
	node := newSSHTestBlockingNode(sshTestAccess(0xbb, "203.0.113.10", 22))
	handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)
	blocked := sshTestBlockCall(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID,
		sshTestBody(t, sshTestKeyLine(0xaa, "user@example"))), node, nil)

	blocked.waitStarted(t)
	store.mutate(func(row *db.Sandbox) {
		row.State = db.StateArchiving
		row.SSH = nil
	})
	blocked.finish(t)

	sshTestAssertError(t, blocked.recorder, http.StatusBadRequest, apierr.CodeMachineNotRunning)
	if got := store.snapshot().SSH; got != nil {
		t.Errorf("late old-boot response restored metadata: %+v", *got)
	}
	if _, set := store.calls(); set != 1 {
		t.Errorf("set calls = %d, want 1 fenced attempt", set)
	}
}

func TestSSHKeyHandlerDelayedResumeFenceAndRefresh(t *testing.T) {
	store := newSSHTestStore(sshTestRow())
	oldKey := sshTestKeyLine(0xaa, "user@example")
	oldNode := newSSHTestBlockingNode(sshTestAccess(0xbb, "203.0.113.10", 22))
	handler := sshTestNewHandler(t, store, sshTestFactory(oldNode, nil), nil)
	blocked := sshTestBlockCall(t, handler,
		sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, oldKey)), oldNode, nil)

	blocked.waitStarted(t)
	// A completed resume bumps the lifecycle generation and clears the pair
	// before the old AddSSHKey answers.
	store.mutate(func(row *db.Sandbox) {
		row.LifecycleGeneration++
		row.State = db.StateReady
		row.SSH = nil
	})
	blocked.finish(t)

	sshTestAssertError(t, blocked.recorder, http.StatusBadRequest, apierr.CodeMachineNotRunning)
	if got := store.snapshot().SSH; got != nil {
		t.Errorf("old boot metadata restored after resume: %+v", *got)
	}

	// The next call runs against the new boot and stores its pair.
	fresh := sshTestAccess(0xcc, "2001:db8::7", 22002)
	handler = sshTestNewHandler(t, store, sshTestFactory(newSSHTestNode(fresh), nil), nil)
	recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, oldKey)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("refresh status = %d; body: %s", recorder.Code, recorder.Body.Bytes())
	}
	stored := store.snapshot()
	if stored.SSH == nil || stored.SSH.Endpoint != "[2001:db8::7]:22002" || stored.SSH.HostKey != fresh.HostKey {
		t.Errorf("refreshed metadata = %+v", stored.SSH)
	}
	if stored.LifecycleGeneration != 8 {
		t.Errorf("generation = %d, want 8", stored.LifecycleGeneration)
	}
}

func TestSSHKeyHandlerCanceledDuringRPC(t *testing.T) {
	store := newSSHTestStore(sshTestRow())
	node := newSSHTestBlockingNode(sshTestAccess(0xbb, "203.0.113.10", 22))
	handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)
	ctx, cancel := context.WithCancel(t.Context())
	request := sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, sshTestKeyLine(0xaa, "user@example")))
	request = request.WithContext(auth.WithIdentity(ctx, auth.Identity{Owner: sshTestOwnerID}))
	blocked := sshTestBlockCall(t, handler, request, node, cancel)

	blocked.waitStarted(t)
	blocked.cancel()
	blocked.finish(t)

	if _, set := store.calls(); set != 0 {
		t.Errorf("canceled RPC success wrote metadata: %d calls", set)
	}
	if blocked.recorder.Body.Len() != 0 {
		t.Errorf("canceled request wrote a body: %s", blocked.recorder.Body.Bytes())
	}
}

func TestSSHKeyHandlerCanceledAfterStore(t *testing.T) {
	store := newSSHTestStore(sshTestRow())
	ctx, cancel := context.WithCancel(t.Context())
	store.afterSet = cancel
	handler := sshTestNewHandler(t, store, sshTestFactory(newSSHTestNode(sshTestAccess(0xbb, "203.0.113.10", 22)), nil), nil)
	request := sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, sshTestKeyLine(0xaa, "user@example")))
	request = request.WithContext(auth.WithIdentity(ctx, auth.Identity{Owner: sshTestOwnerID}))
	recorder := sshTestServe(t, handler, request)

	if _, set := store.calls(); set != 1 {
		t.Errorf("set calls = %d, want 1", set)
	}
	if recorder.Body.Len() != 0 {
		t.Errorf("canceled-after-store request wrote a body: %s", recorder.Body.Bytes())
	}
	if got := store.snapshot().SSH; got == nil {
		t.Error("metadata was not persisted before the cancel")
	}
}

func TestSSHKeyHandlerLogsNoKey(t *testing.T) {
	key := sshTestKeyLine(0xaa, "user@example")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	store := newSSHTestStore(sshTestRow())
	node := newSSHTestNode(nil)
	node.err = status.Error(codes.Internal, "guest rejected "+key)
	handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), logger)
	recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
	sshTestAssertError(t, recorder, http.StatusBadGateway, apierr.CodeSandboxDirectFailed)
	if strings.Contains(logs.String(), key) || strings.Contains(logs.String(), "guest rejected") {
		t.Errorf("log leaked key or node text: %s", logs.String())
	}
	if bytes.Contains(recorder.Body.Bytes(), []byte(key)) {
		t.Errorf("response leaked the public key")
	}
}

func TestSSHKeyHandlerLookupRowContract(t *testing.T) {
	key := sshTestKeyLine(0xaa, "user@example")
	otherOwner := uuid.MustParse("018f4b1c-7e2d-7a3b-9c4d-999999999999")
	deletedAt := time.Now()
	tests := []struct {
		name string
		row  func() db.Sandbox
	}{
		{name: "wrong owner", row: func() db.Sandbox { row := sshTestRow(); row.OwnerID = otherOwner; return row }},
		{name: "wrong id", row: func() db.Sandbox { row := sshTestRow(); row.ID = "bx_zzzzzzzz"; return row }},
		{name: "deleted", row: func() db.Sandbox { row := sshTestRow(); row.DeletedAt = &deletedAt; return row }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newSSHTestStore(sshTestRow())
			row := test.row()
			store.lookupRow = &row
			node := newSSHTestNode(sshTestAccess(0xbb, "203.0.113.10", 22))
			handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)
			recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
			sshTestAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
			if node.callCount() != 0 {
				t.Errorf("invalid lookup row reached AddSSHKey")
			}
			if _, set := store.calls(); set != 0 {
				t.Errorf("invalid lookup row wrote metadata")
			}
		})
	}
}

func TestSSHKeyHandlerDialCleanup(t *testing.T) {
	key := sshTestKeyLine(0xaa, "user@example")

	t.Run("nil client closes cleanup", func(t *testing.T) {
		store := newSSHTestStore(sshTestRow())
		closed := 0
		factory := func(string) (nodev1.NodeClient, func() error, error) {
			return nil, func() error { closed++; return nil }, nil
		}
		handler := sshTestNewHandler(t, store, factory, nil)
		recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
		sshTestAssertError(t, recorder, http.StatusInternalServerError, apierr.CodeInternal)
		if closed != 1 {
			t.Errorf("cleanup calls = %d, want 1", closed)
		}
	})

	t.Run("client and error closes cleanup", func(t *testing.T) {
		store := newSSHTestStore(sshTestRow())
		node := newSSHTestNode(nil)
		closed := 0
		factory := func(string) (nodev1.NodeClient, func() error, error) {
			return node, func() error { closed++; return nil }, errors.New("factory failed")
		}
		handler := sshTestNewHandler(t, store, factory, nil)
		recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
		sshTestAssertError(t, recorder, http.StatusBadGateway, apierr.CodeSandboxDirectFailed)
		if closed != 1 {
			t.Errorf("cleanup calls = %d, want 1", closed)
		}
		if node.callCount() != 0 {
			t.Errorf("failed factory reached AddSSHKey")
		}
	})

	t.Run("canceled factory writes nothing", func(t *testing.T) {
		store := newSSHTestStore(sshTestRow())
		ctx, cancel := context.WithCancel(t.Context())
		factory := func(string) (nodev1.NodeClient, func() error, error) {
			cancel()
			return nil, nil, errors.New("canceled while dialing")
		}
		handler := sshTestNewHandler(t, store, factory, nil)
		request := sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key))
		request = request.WithContext(auth.WithIdentity(ctx, auth.Identity{Owner: sshTestOwnerID}))
		recorder := sshTestServe(t, handler, request)
		if recorder.Body.Len() != 0 {
			t.Errorf("canceled factory wrote a body: %s", recorder.Body.Bytes())
		}
	})
}

func TestSSHKeyNewHandlerRequiresDependencies(t *testing.T) {
	if _, err := NewSSHKeyHandler(SSHKeyOptions{}); err == nil {
		t.Error("NewSSHKeyHandler accepted a nil store")
	}
	store := newSSHTestStore(sshTestRow())
	if _, err := NewSSHKeyHandler(SSHKeyOptions{Store: store}); err == nil {
		t.Error("NewSSHKeyHandler accepted nil node clients")
	}
}

func TestSSHKeyHandlerDialStatus(t *testing.T) {
	key := sshTestKeyLine(0xaa, "user@example")
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{name: "resource exhausted", err: status.Error(codes.ResourceExhausted, "no room"), retryable: false},
		{name: "unavailable", err: status.Error(codes.Unavailable, "link down"), retryable: true},
		{name: "internal", err: status.Error(codes.Internal, "broken"), retryable: true},
		{name: "plain", err: errors.New("dial failed"), retryable: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newSSHTestStore(sshTestRow())
			handler := sshTestNewHandler(t, store, sshTestFactory(nil, test.err), nil)
			recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID, sshTestBody(t, key)))
			body := sshTestAssertError(t, recorder, http.StatusBadGateway, apierr.CodeSandboxDirectFailed)
			if body.Error.Details.Retryable == nil || *body.Error.Details.Retryable != test.retryable {
				t.Errorf("retryable = %v, want %v", body.Error.Details.Retryable, test.retryable)
			}
			if _, set := store.calls(); set != 0 {
				t.Errorf("dial failure wrote metadata")
			}
		})
	}
}

func TestSSHKeyHandlerRejectsUnusableHostKey(t *testing.T) {
	// The old validation accepted a blob holding only the algorithm string.
	// An ed25519 host key with no key bytes is unusable and must be a 502
	// with no metadata write.
	blob := sshTestWireString([]byte("ssh-ed25519"))
	hostKey := "ssh-ed25519 " + base64.StdEncoding.EncodeToString(blob)
	store := newSSHTestStore(sshTestRow())
	node := newSSHTestNode(&nodev1.SSHAccess{User: "user", Host: "203.0.113.10", Port: 22, HostKey: hostKey})
	handler := sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)
	recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, sshTestSandboxID,
		sshTestBody(t, sshTestKeyLine(0xaa, "user@example"))))
	sshTestAssertError(t, recorder, http.StatusBadGateway, apierr.CodeSandboxDirectFailed)
	if _, set := store.calls(); set != 0 {
		t.Errorf("unusable host key wrote metadata")
	}
}
