package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

// Fixed identities of the in-process command stack.
const (
	execTestNodeID       = "node-exec"
	execTestNodeToken    = "node-token"
	execTestGatewayToken = "gateway-token"
	execTestAPIKey       = "api-key-exec"
	execTestOwner        = "acct_exec"
)

// execTestLookup is the tiny owner-scoped lookup the command handler needs.
type execTestLookup struct{}

func (execTestLookup) TargetLookup(_ context.Context, owner, sandboxID string) (api.CommandTarget, error) {
	if owner != execTestOwner || sandboxID != cliTestSandboxID {
		return api.CommandTarget{}, api.ErrCommandTargetNotFound
	}
	return api.CommandTarget{NodeID: execTestNodeID, State: api.StateReady}, nil
}

// execTestStartStack starts nodefake behind nodelink and the authenticated
// gateway, then serves the real command handler through the API key middleware.
func execTestStartStack(t *testing.T) string {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)

	nodeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("node listener: %v", err)
	}
	wantNodeToken := sha256.Sum256([]byte(execTestNodeToken))
	nodeAuth := func(_ context.Context, hello nodelink.Hello) error {
		got := sha256.Sum256([]byte(hello.Token))
		if subtle.ConstantTimeCompare(got[:], wantNodeToken[:]) != 1 {
			return errors.New("bad node token")
		}
		return nil
	}
	// Server lifetimes outlive individual requests and are stopped by the
	// cleanups below, so they deliberately do not use the test context.
	baseCtx := context.Background()
	hub := nodelink.NewHub(nodeAuth, logger)
	hubCtx, hubCancel := context.WithCancel(baseCtx)
	hubDone := make(chan struct{})
	go func() {
		_ = hub.Serve(hubCtx, nodeListener)
		close(hubDone)
	}()
	t.Cleanup(func() {
		hubCancel()
		<-hubDone
	})

	fake := nodefake.New(execTestNodeID, t.TempDir())
	grpcServer := grpc.NewServer(nodelink.ServerOptions()...)
	fake.Register(grpcServer)
	nodeCtx, nodeCancel := context.WithCancel(baseCtx)
	nodeDone := make(chan struct{})
	go func() {
		_ = nodelink.Serve(nodeCtx, nodelink.DialConfig{
			Addr:       nodeListener.Addr().String(),
			NodeID:     execTestNodeID,
			Token:      execTestNodeToken,
			Log:        logger,
			MinBackoff: 10 * time.Millisecond,
			MaxBackoff: 50 * time.Millisecond,
		}, grpcServer)
		close(nodeDone)
	}()
	t.Cleanup(func() {
		nodeCancel()
		<-nodeDone
	})

	waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer waitCancel()
	link, err := hub.Wait(waitCtx, execTestNodeID)
	if err != nil {
		t.Fatalf("node never connected: %v", err)
	}
	if _, err := link.Client.CreateVM(waitCtx, &nodev1.CreateVMRequest{SandboxId: cliTestSandboxID}); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	gateway, err := nodegateway.New(hub, execTestGatewayToken)
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	gatewayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("gateway listener: %v", err)
	}
	gatewayCtx, gatewayCancel := context.WithCancel(baseCtx)
	gatewayDone := make(chan error, 1)
	go func() { gatewayDone <- gateway.Serve(gatewayCtx, gatewayListener) }()
	t.Cleanup(func() {
		gatewayCancel()
		if err := <-gatewayDone; err != nil {
			t.Errorf("gateway Serve: %v", err)
		}
	})

	factory := nodegateway.NewClientFactory(gatewayListener.Addr().String(), execTestGatewayToken, nil)
	commandsHandler, err := api.NewCommandsHandler(api.CommandsOptions{
		Lookup: execTestLookup{},
		Clients: func(nodeID string) (nodev1.NodeClient, func() error, error) {
			client, err := factory.Dial(nodeID)
			if err != nil {
				return nil, nil, err
			}
			return client, client.Close, nil
		},
		Logger: logger,
	})
	if err != nil {
		t.Fatalf("NewCommandsHandler: %v", err)
	}

	keys, err := auth.NewKeys([]auth.KeyConfig{{
		ID:    "exec-test",
		Owner: execTestOwner,
		Raw:   execTestAPIKey,
	}})
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("POST "+api.CommandsPath, commandsHandler)
	mux.HandleFunc("GET "+execInfoPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true,"type":"bbraft.info","version":"exec-test",`+
			`"commands":{"streamMaxSeconds":900,"syncMaxSeconds":600}}`)
	})
	server := httptest.NewServer(keys.Middleware(mux))
	t.Cleanup(server.Close)
	return server.URL
}

// execTestNewIntegration builds an App whose saved configuration points at the
// in-process command stack.
func execTestNewIntegration(t *testing.T, apiURL string, isTTY bool) (*App, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	app := NewApp(Options{
		Stdin:        strings.NewReader(""),
		Stdout:       stdout,
		Stderr:       stderr,
		HTTPClient:   &http.Client{},
		ConfigDir:    t.TempDir(),
		IsTTY:        func(io.Writer) bool { return isTTY },
		PollInterval: time.Millisecond,
		WaitTimeout:  2 * time.Second,
		Getenv: func(key string) string {
			switch key {
			case "BBRAFT_API_URL":
				return apiURL
			case "BBRAFT_API_KEY":
				return execTestAPIKey
			default:
				return ""
			}
		},
	})
	return app, stdout, stderr
}

// TestExecRealStack runs the CLI through real HTTP, auth middleware, the
// commands handler, the node gateway and nodefake.
func TestExecRealStack(t *testing.T) {
	apiURL := execTestStartStack(t)

	t.Run("sync exit code", func(t *testing.T) {
		app, stdout, stderr := execTestNewIntegration(t, apiURL, true)
		app.SetArgs([]string{"exec", cliTestSandboxID, "printf out; printf err >&2; exit 3"})
		if got := app.Run(t.Context()); got != 3 {
			t.Fatalf("exit status = %d, want 3; stderr=%q", got, stderr.String())
		}
		if stdout.String() != "out" || stderr.String() != "err" {
			t.Fatalf("stdout/stderr = %q/%q, want out/err", stdout.String(), stderr.String())
		}
	})

	t.Run("stream", func(t *testing.T) {
		app, stdout, stderr := execTestNewIntegration(t, apiURL, true)
		app.SetArgs([]string{"exec", "--stream", cliTestSandboxID, "printf a; printf b >&2; exit 0"})
		if got := app.Run(t.Context()); got != 0 {
			t.Fatalf("exit status = %d, want 0; stderr=%q", got, stderr.String())
		}
		if stdout.String() != "a" || stderr.String() != "b" {
			t.Fatalf("stdout/stderr = %q/%q, want a/b", stdout.String(), stderr.String())
		}
	})

	t.Run("argv quoting", func(t *testing.T) {
		app, stdout, stderr := execTestNewIntegration(t, apiURL, true)
		app.SetArgs([]string{"exec", cliTestSandboxID, "--", "printf", "%s\\n", "a b"})
		if got := app.Run(t.Context()); got != 0 {
			t.Fatalf("exit status = %d, want 0; stderr=%q", got, stderr.String())
		}
		if stdout.String() != "a b\n" {
			t.Fatalf("stdout = %q, want %q", stdout.String(), "a b\n")
		}
	})

	t.Run("timeout zero discovers", func(t *testing.T) {
		app, stdout, stderr := execTestNewIntegration(t, apiURL, true)
		app.SetArgs([]string{"exec", "--timeout", "0", cliTestSandboxID, "printf hi"})
		if got := app.Run(t.Context()); got != 0 {
			t.Fatalf("exit status = %d, want 0; stderr=%q", got, stderr.String())
		}
		if stdout.String() != "hi" {
			t.Fatalf("stdout = %q, want hi", stdout.String())
		}
		if stderr.String() != "" {
			t.Fatalf("stderr = %q, want no fallback note", stderr.String())
		}
	})
}
