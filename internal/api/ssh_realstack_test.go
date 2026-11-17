package api

// Real-stack tests: nodefake behind nodelink behind the authenticated
// nodegateway, reached only through the handler's node-client factory. They
// prove AddSSHKey actually writes the guest's authorized_keys and that the
// returned metadata is stored only after validation.

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

const (
	sshTestGatewayNodeID = "ssh-gateway-node"
	sshTestNodeToken     = "ssh-node-token"
	sshTestGatewayToken  = "ssh-gateway-token"
)

type sshTestEnv struct {
	node      *nodefake.Node
	root      string
	hub       *nodelink.Hub
	serve     *nodegateway.ClientFactory
	sandboxID string
}

func sshTestStartEnv(t *testing.T) *sshTestEnv {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	wantToken := sha256.Sum256([]byte(sshTestNodeToken))
	authn := func(_ context.Context, hello nodelink.Hello) error {
		gotToken := sha256.Sum256([]byte(hello.Token))
		if subtle.ConstantTimeCompare(gotToken[:], wantToken[:]) != 1 {
			return errors.New("bad node token")
		}
		return nil
	}
	hub := nodelink.NewHub(authn, logger)
	hubCtx, hubCancel := context.WithCancel(t.Context())
	hubDone := make(chan struct{})
	go func() {
		_ = hub.Serve(hubCtx, ln)
		close(hubDone)
	}()
	t.Cleanup(func() {
		hubCancel()
		<-hubDone
	})

	root := t.TempDir()
	node := nodefake.New(sshTestGatewayNodeID, root)
	server := grpc.NewServer(nodelink.ServerOptions()...)
	node.Register(server)
	nodeCtx, nodeCancel := context.WithCancel(t.Context())
	nodeDone := make(chan struct{})
	go func() {
		_ = nodelink.Serve(nodeCtx, nodelink.DialConfig{
			Addr: ln.Addr().String(), NodeID: sshTestGatewayNodeID, Token: sshTestNodeToken,
			Log: logger, MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		}, server)
		close(nodeDone)
	}()
	t.Cleanup(func() {
		nodeCancel()
		<-nodeDone
	})

	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()
	link, err := hub.Wait(waitCtx, sshTestGatewayNodeID)
	if err != nil {
		t.Fatalf("node never connected: %v", err)
	}
	if _, err := link.Client.CreateVM(t.Context(), &nodev1.CreateVMRequest{SandboxId: sshTestSandboxID}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	gateway, err := nodegateway.New(hub, sshTestGatewayToken)
	if err != nil {
		t.Fatal(err)
	}
	gatewayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gatewayCtx, gatewayCancel := context.WithCancel(t.Context())
	gatewayDone := make(chan error, 1)
	go func() { gatewayDone <- gateway.Serve(gatewayCtx, gatewayListener) }()
	t.Cleanup(func() {
		gatewayCancel()
		if err := <-gatewayDone; err != nil {
			t.Errorf("gateway Serve: %v", err)
		}
	})

	return &sshTestEnv{
		node:      node,
		root:      root,
		hub:       hub,
		serve:     nodegateway.NewClientFactory(gatewayListener.Addr().String(), sshTestGatewayToken, nil),
		sandboxID: sshTestSandboxID,
	}
}

// sshTestGatewayClient counts real gateway AddSSHKey calls and can rewrite a
// successful result before the handler validates it.
type sshTestGatewayClient struct {
	nodev1.NodeClient
	calls  *atomic.Int64
	mutate func(*nodev1.SSHAccess)
}

func (c *sshTestGatewayClient) AddSSHKey(ctx context.Context, in *nodev1.AddSSHKeyRequest, opts ...grpc.CallOption) (*nodev1.SSHAccess, error) {
	c.calls.Add(1)
	response, err := c.NodeClient.AddSSHKey(ctx, in, opts...)
	if err == nil && c.mutate != nil {
		c.mutate(response)
	}
	return response, err
}

func (e *sshTestEnv) factory(calls *atomic.Int64, mutate func(*nodev1.SSHAccess)) NodeClientFactory {
	return func(nodeID string) (nodev1.NodeClient, func() error, error) {
		if nodeID != sshTestGatewayNodeID {
			return nil, nil, errors.New("node is not connected")
		}
		inner, err := e.serve.Dial(nodeID)
		if err != nil {
			return nil, nil, err
		}
		return &sshTestGatewayClient{NodeClient: inner, calls: calls, mutate: mutate}, inner.Close, nil
	}
}

func (e *sshTestEnv) authorizedKeys(t *testing.T) []string {
	t.Helper()
	path := filepath.Join(e.root, "vms", e.sandboxID, "home", "user", ".ssh", "authorized_keys")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read authorized_keys: %v", err)
	}
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

func (e *sshTestEnv) row() db.Sandbox {
	row := sshTestRow()
	node := sshTestGatewayNodeID
	row.NodeID = &node
	return row
}

func TestSSHKeyRealStackAuthorizedKeys(t *testing.T) {
	env := sshTestStartEnv(t)
	store := newSSHTestStore(env.row())
	var calls atomic.Int64
	handler := sshTestNewHandler(t, store, env.factory(&calls, nil), nil)

	keyA := sshTestKeyLine(0xaa, "user@example")
	recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, env.sandboxID, sshTestBody(t, keyA)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", recorder.Code, recorder.Body.Bytes())
	}
	var response sshTestSuccess
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if response.MachineIP != "127.0.0.1" || response.SSHEndpoint != "127.0.0.1:22" ||
		response.SSHUser != "user" || response.HostKey != nodefake.HostKey {
		t.Errorf("response = %+v", response)
	}
	if calls.Load() != 1 {
		t.Errorf("gateway AddSSHKey calls = %d, want 1", calls.Load())
	}
	lines := env.authorizedKeys(t)
	if len(lines) != 1 || lines[0] != keyA {
		t.Fatalf("authorized_keys = %q", lines)
	}

	// The same key is idempotent: no duplicate line.
	recorder = sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, env.sandboxID, sshTestBody(t, keyA)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("repeat status = %d; body: %s", recorder.Code, recorder.Body.Bytes())
	}
	if lines = env.authorizedKeys(t); len(lines) != 1 {
		t.Errorf("repeat added a duplicate: %q", lines)
	}

	// A distinct key adds exactly one more line.
	keyB := sshTestKeyLine(0xcc, "second@example")
	recorder = sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, env.sandboxID, sshTestBody(t, keyB)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("second key status = %d; body: %s", recorder.Code, recorder.Body.Bytes())
	}
	lines = env.authorizedKeys(t)
	if len(lines) != 2 || lines[0] != keyA || lines[1] != keyB {
		t.Errorf("authorized_keys after second key = %q", lines)
	}
	if calls.Load() != 3 {
		t.Errorf("gateway AddSSHKey calls = %d, want 3", calls.Load())
	}

	// Request validation still happens before the gateway.
	recorder = sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, env.sandboxID, `{"key":"private"}`))
	sshTestAssertError(t, recorder, http.StatusBadRequest, apierr.CodeInvalidJSON)
	if calls.Load() != 3 {
		t.Errorf("invalid key reached the gateway: %d calls", calls.Load())
	}
}

func TestSSHKeyRealStackHostKeyAndIPv6(t *testing.T) {
	env := sshTestStartEnv(t)
	store := newSSHTestStore(env.row())
	var calls atomic.Int64

	// The guest answers but its host key is not generated yet.
	handler := sshTestNewHandler(t, store, env.factory(&calls, func(access *nodev1.SSHAccess) {
		access.HostKey = ""
	}), nil)
	recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, env.sandboxID,
		sshTestBody(t, sshTestKeyLine(0xaa, "user@example"))))
	body := sshTestAssertError(t, recorder, http.StatusServiceUnavailable, apierr.CodeSSHHostKeyUnavailable)
	if body.Error.Details.RetryAfterSeconds == nil || *body.Error.Details.RetryAfterSeconds != sshHostKeyRetrySeconds {
		t.Errorf("retryAfterSeconds = %v, want %d", body.Error.Details.RetryAfterSeconds, sshHostKeyRetrySeconds)
	}
	if _, set := store.calls(); set != 0 {
		t.Errorf("missing host key wrote metadata")
	}

	// A malformed non-empty host key is the protocol failure.
	handler = sshTestNewHandler(t, store, env.factory(&calls, func(access *nodev1.SSHAccess) {
		access.HostKey = "ssh-ed25519 !!!"
	}), nil)
	recorder = sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, env.sandboxID,
		sshTestBody(t, sshTestKeyLine(0xaa, "user@example"))))
	sshTestAssertError(t, recorder, http.StatusBadGateway, apierr.CodeSandboxDirectFailed)

	// IPv6 host and forwarded port render through net.JoinHostPort.
	handler = sshTestNewHandler(t, store, env.factory(&calls, func(access *nodev1.SSHAccess) {
		access.Host = "2001:0db8::7"
		access.Port = 22002
	}), nil)
	recorder = sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, env.sandboxID,
		sshTestBody(t, sshTestKeyLine(0xaa, "user@example"))))
	if recorder.Code != http.StatusOK {
		t.Fatalf("ipv6 status = %d; body: %s", recorder.Code, recorder.Body.Bytes())
	}
	var response sshTestSuccess
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if response.MachineIP != "2001:db8::7" || response.SSHEndpoint != "[2001:db8::7]:22002" {
		t.Errorf("ipv6 response = %+v", response)
	}
}

func TestSSHKeyRealStackNodeAbsent(t *testing.T) {
	env := sshTestStartEnv(t)
	row := sshTestRow()
	nodeID := "ssh-absent-node"
	row.NodeID = &nodeID
	store := newSSHTestStore(row)
	var calls atomic.Int64
	handler := sshTestNewHandler(t, store, env.factory(&calls, nil), nil)
	recorder := sshTestServe(t, handler, sshTestRequest(t, sshTestOwnerID, env.sandboxID,
		sshTestBody(t, sshTestKeyLine(0xaa, "user@example"))))
	body := sshTestAssertError(t, recorder, http.StatusBadGateway, apierr.CodeSandboxDirectFailed)
	if body.Error.Details.Retryable == nil || !*body.Error.Details.Retryable {
		t.Errorf("retryable = %v, want true", body.Error.Details.Retryable)
	}
	if calls.Load() != 0 {
		t.Errorf("absent node was called")
	}
}
