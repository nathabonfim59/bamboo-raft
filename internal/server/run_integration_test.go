package server

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json/v2"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/auth"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/server/config"
	"github.com/nathabonfim59/bamboo-raft/internal/server/nodegateway"
)

const runTestNodeID = "node-integration"

// lockedBuffer is a race-safe log sink for concurrent roles.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// runEnv is one isolated database schema, a node identity, configured API
// keys and pre-bound listeners.
type runEnv struct {
	t         *testing.T
	store     *db.Store
	dsn       string
	master    [32]byte
	cfg       *config.Config
	listeners Listeners
	logs      *lockedBuffer
	owner     string
	key       string
	otherKey  string
	nodeToken string
	apiBase   string
	client    *http.Client
	node      *nodefake.Node
	nodeRoot  string
}

// newRunTestSchema creates one isolated schema with a known installation
// master. It uses dbtest's gated pool configuration (skip unless
// BBRAFT_TEST_DATABASE_URL is set) but not dbtest.Open, because the run
// integration tests need independent stores and their own inspection handle.
func newRunTestSchema(t *testing.T) (string, [32]byte) {
	t.Helper()
	cfg := dbtest.PoolConfig(t)
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	schema := "bbraft_servertest_" + hex.EncodeToString(suffix[:])
	ctx := t.Context()
	admin, err := pgx.ConnectConfig(ctx, cfg.ConnConfig)
	if err != nil {
		t.Fatal("schema connect failed")
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close(context.WithoutCancel(ctx))
		t.Fatal("schema create failed")
	}
	admin.Close(context.WithoutCancel(ctx))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		drop, err := pgx.ConnectConfig(dropCtx, cfg.ConnConfig)
		if err != nil {
			t.Error("schema drop connect failed")
			return
		}
		_, dropErr := drop.Exec(dropCtx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		drop.Close(context.WithoutCancel(dropCtx))
		if dropErr != nil {
			t.Error("schema drop failed")
		}
	})
	var master [32]byte
	if _, err := rand.Read(master[:]); err != nil {
		t.Fatal(err)
	}
	return dsnWithSchema(t, os.Getenv("BBRAFT_TEST_DATABASE_URL"), schema), master
}

// dsnWithSchema adds the isolated schema as a pgx runtime parameter. It
// never prints the DSN: the base URL can carry a password.
func dsnWithSchema(t *testing.T, base, schema string) string {
	t.Helper()
	if strings.Contains(base, "://") {
		u, err := url.Parse(base)
		if err != nil {
			t.Fatal("test database URL is not a URL")
		}
		values := u.Query()
		values.Set("search_path", schema)
		u.RawQuery = values.Encode()
		return u.String()
	}
	return base + " search_path=" + schema
}

func newRunEnv(t *testing.T) *runEnv {
	t.Helper()
	dsn, master := newRunTestSchema(t)
	secrets, err := db.NewSecrets(master[:])
	if err != nil {
		t.Fatal(err)
	}
	store, err := db.Open(t.Context(), dsn, secrets)
	if err != nil {
		t.Fatal("inspection store open failed")
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatalf("inspection store migrate: %v", err)
	}
	dir := t.TempDir()
	write := func(name, content string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	owner := "owner-" + strings.ReplaceAll(t.Name(), "/", "-")
	key := "bmb_" + strings.ReplaceAll(t.Name(), "/", "-") + "-main-key"
	otherKey := "bmb_" + strings.ReplaceAll(t.Name(), "/", "-") + "-other-key"
	keysFile := write("keys.json", fmt.Sprintf(`{"keys":[
		{"id":"main","owner":%q,"key":%q},
		{"id":"other","owner":"other-owner","key":%q}
	]}`, owner, key, otherKey))
	nodeToken := "node-token-" + strings.ReplaceAll(t.Name(), "/", "-")
	sum := sha256.Sum256([]byte(nodeToken))
	tokensFile := write("tokens.json", fmt.Sprintf(`{"nodes":[{"id":%q,"tokenSha256":%q}]}`, runTestNodeID, hex.EncodeToString(sum[:])))

	apiLn := runTestListen(t)
	nodeLn := runTestListen(t)
	gatewayLn := runTestListen(t)
	cfg := &config.Config{
		Roles: config.Roles{config.RoleAPI: true, config.RoleLifecycle: true, config.RoleNodeHub: true},
		Server: config.Server{
			Listen:    apiLn.Addr().String(),
			PublicURL: "http://" + apiLn.Addr().String(),
		},
		Node: config.Node{
			Listen:        nodeLn.Addr().String(),
			TokensFile:    tokensFile,
			GatewayListen: gatewayLn.Addr().String(),
			GatewayDial:   gatewayLn.Addr().String(),
			GatewayToken:  "gateway-integration-token",
		},
		Auth:      config.Auth{KeysFile: keysFile, KeyPrefix: "bmb_"},
		Commands:  config.Commands{StreamMaxSeconds: 901},
		Lifecycle: config.Lifecycle{RetrySeconds: 1, ReaperSeconds: 1, JobPollSeconds: 1},
		Database:  config.Database{URL: dsn},
		Secrets:   config.Secrets{MasterKey: master},
	}
	var logs lockedBuffer
	return &runEnv{
		t:         t,
		store:     store,
		dsn:       dsn,
		master:    master,
		cfg:       cfg,
		listeners: Listeners{API: apiLn, Node: nodeLn, Gateway: gatewayLn},
		logs:      &logs,
		owner:     owner,
		key:       key,
		otherKey:  otherKey,
		nodeToken: nodeToken,
		apiBase:   "http://" + apiLn.Addr().String(),
		client:    &http.Client{Timeout: 20 * time.Second},
	}
}

func (e *runEnv) options() Options {
	return Options{
		Logger:            slog.New(slog.NewTextHandler(e.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Listeners:         e.listeners,
		InventoryInterval: 20 * time.Millisecond,
		ShutdownTimeout:   2 * time.Second,
	}
}

// openRoleStore opens an independent role-shaped store for tests that need to
// observe its lifetime. Production uses Options.storeOpener's default.
func (e *runEnv) openRoleStore(ctx context.Context) (*db.Store, error) {
	secrets, err := db.NewSecrets(e.master[:])
	if err != nil {
		return nil, err
	}
	store, err := db.Open(ctx, e.dsn, secrets)
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(ctx); err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

// start runs one Run instance for roles and requires a clean shutdown.
func (e *runEnv) start(roles config.Roles) func() {
	cfg := *e.cfg
	cfg.Roles = roles
	ctx, cancel := context.WithCancel(e.t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, &cfg, e.options()) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					e.t.Errorf("Run(%s) = %v", roles.String(), err)
				}
			case <-time.After(15 * time.Second):
				e.t.Errorf("Run(%s) did not stop", roles.String())
			}
		})
	}
	e.t.Cleanup(stop)
	return stop
}

func (e *runEnv) startNode(bootDelay time.Duration) func() {
	e.t.Helper()
	root := e.t.TempDir()
	e.nodeRoot = root
	node := nodefake.New(runTestNodeID, root)
	node.BootDelay = bootDelay
	e.node = node
	server := grpc.NewServer(nodelink.ServerOptions()...)
	node.Register(server)
	ctx, cancel := context.WithCancel(e.t.Context())
	done := make(chan struct{})
	go func() {
		_ = nodelink.Serve(ctx, nodelink.DialConfig{
			Addr:       e.listeners.Node.Addr().String(),
			NodeID:     runTestNodeID,
			Token:      e.nodeToken,
			Log:        testLogger(),
			MinBackoff: 10 * time.Millisecond,
			MaxBackoff: 50 * time.Millisecond,
		}, server)
		close(done)
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				e.t.Error("node did not stop")
			}
		})
	}
	e.t.Cleanup(stop)
	return stop
}

func runTestListen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

type runResponse struct {
	status int
	header http.Header
	body   []byte
}

func (e *runEnv) do(method, path, key, body string, headers map[string]string) runResponse {
	e.t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request, err := http.NewRequestWithContext(e.t.Context(), method, e.apiBase+path, reader)
	if err != nil {
		e.t.Fatal(err)
	}
	if key != "" {
		request.Header.Set("Authorization", "Bearer "+key)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	response, err := e.client.Do(request)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		e.t.Fatal(err)
	}
	return runResponse{status: response.StatusCode, header: response.Header.Clone(), body: data}
}

func (e *runEnv) waitFor(duration time.Duration, what string, ready func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	e.t.Fatalf("timed out waiting for %s", what)
}

func (e *runEnv) waitNodeFresh() {
	e.t.Helper()
	e.waitFor(10*time.Second, "fresh node inventory", func() bool {
		node, err := e.store.Node(e.t.Context(), runTestNodeID)
		return err == nil && time.Since(node.LastSeenAt) < time.Minute
	})
}

// waitNodeSeenAfter proves a new inventory write landed after mark, not just
// that an old fresh timestamp is still within the freshness window.
func (e *runEnv) waitNodeSeenAfter(mark time.Time) {
	e.t.Helper()
	e.waitFor(10*time.Second, "inventory write after mark", func() bool {
		node, err := e.store.Node(e.t.Context(), runTestNodeID)
		return err == nil && node.LastSeenAt.After(mark)
	})
}

// waitAPI blocks until the selected API process actually serves liveness.
func (e *runEnv) waitAPI() {
	e.t.Helper()
	e.waitFor(10*time.Second, "api readiness", func() bool {
		response, err := e.client.Get(e.apiBase + HealthPath)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		return response.StatusCode == http.StatusOK
	})
}

func (e *runEnv) createSandbox(body string) string {
	e.t.Helper()
	response := e.do(http.MethodPost, "/api/v1/sandboxes", e.key, body, nil)
	if response.status != http.StatusAccepted {
		e.t.Fatalf("create status = %d", response.status)
	}
	var created struct {
		Sandbox struct {
			ID string `json:"id"`
		} `json:"sandbox"`
	}
	if err := json.Unmarshal(response.body, &created); err != nil || created.Sandbox.ID == "" {
		e.t.Fatal("create response has no readable sandbox id")
	}
	return created.Sandbox.ID
}

func (e *runEnv) waitReady(id string) {
	e.t.Helper()
	e.waitFor(15*time.Second, "sandbox ready", func() bool {
		response := e.do(http.MethodGet, "/api/v1/sandboxes/"+id, e.key, "", nil)
		if response.status != http.StatusOK {
			return false
		}
		var got struct {
			Sandbox struct {
				State string `json:"state"`
			} `json:"sandbox"`
		}
		if err := json.Unmarshal(response.body, &got); err != nil {
			return false
		}
		return got.Sandbox.State == "ready" || got.Sandbox.State == "idle"
	})
}

func (e *runEnv) deleteSandbox(id string) (string, runResponse) {
	e.t.Helper()
	response := e.do(http.MethodDelete, "/api/v1/sandboxes/"+id, e.key, "", map[string]string{"X-Ascii-Confirm-Delete": id})
	if response.status != http.StatusAccepted {
		e.t.Fatalf("delete status = %d", response.status)
	}
	var deleted struct {
		Operation struct {
			ID string `json:"id"`
		} `json:"operation"`
	}
	if err := json.Unmarshal(response.body, &deleted); err != nil || deleted.Operation.ID == "" {
		e.t.Fatal("delete response has no readable operation id")
	}
	return deleted.Operation.ID, response
}

func (e *runEnv) waitDeletionComplete(operationID string) runResponse {
	e.t.Helper()
	var last runResponse
	e.waitFor(15*time.Second, "deletion completed", func() bool {
		last = e.do(http.MethodGet, "/api/v1/deletion-operations/"+operationID, e.key, "", nil)
		if last.status != http.StatusOK {
			return false
		}
		var got struct {
			Operation struct {
				Status string `json:"status"`
			} `json:"operation"`
		}
		if err := json.Unmarshal(last.body, &got); err != nil {
			return false
		}
		return got.Operation.Status == "completed"
	})
	return last
}

// runTestValidators loads the v1 API document and the extension document.
func runTestValidators(t *testing.T) (*compat.Validator, *compat.Validator) {
	t.Helper()
	root := compat.Root(t)
	return compat.Load(t),
		compat.LoadFile(t, filepath.Join(root, "api", "bbraft-ext.yaml"))
}

func runTestCheck(t *testing.T, validator *compat.Validator, operationID string, response runResponse) {
	t.Helper()
	if err := validator.Response(operationID, response.status, response.header, response.body); err != nil {
		t.Errorf("%s %d failed spec validation", operationID, response.status)
	}
}

func TestRunAllRolesSmoke(t *testing.T) {
	env := newRunEnv(t)
	v1, extension := runTestValidators(t)
	stop := env.start(config.Roles{config.RoleAPI: true, config.RoleLifecycle: true, config.RoleNodeHub: true})
	env.startNode(0)
	env.waitNodeFresh()
	env.waitAPI()

	created := env.do(http.MethodPost, "/api/v1/sandboxes", env.key, `{"type":"small","env":{"LEAK":"sentinel-env-value"},"ttlSeconds":600}`, nil)
	if created.status != http.StatusAccepted {
		t.Fatalf("create status = %d", created.status)
	}
	for _, validator := range []*compat.Validator{v1} {
		runTestCheck(t, validator, "create", created)
	}
	var createBody struct {
		Status  string `json:"status"`
		Sandbox struct {
			ID    string `json:"id"`
			State string `json:"state"`
		} `json:"sandbox"`
	}
	if err := json.Unmarshal(created.body, &createBody); err != nil || createBody.Sandbox.ID == "" {
		t.Fatal("create response is malformed")
	}
	if createBody.Status != "provisioning" || createBody.Sandbox.State != "provisioning" {
		t.Fatalf("create status = %q state = %q", createBody.Status, createBody.Sandbox.State)
	}
	id := createBody.Sandbox.ID
	env.waitReady(id)

	got := env.do(http.MethodGet, "/api/v1/sandboxes/"+id, env.key, "", nil)
	if got.status != http.StatusOK {
		t.Fatalf("get status = %d", got.status)
	}
	runTestCheck(t, v1, "get", got)
	if bytes.Contains(got.body, []byte("sentinel-env-value")) {
		t.Fatal("sandbox response leaked env")
	}

	command := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/commands", env.key, `{"command":"echo integration-ok","timeoutSeconds":5}`, nil)
	if command.status != http.StatusOK {
		t.Fatalf("command status = %d", command.status)
	}
	runTestCheck(t, v1, "command", command)
	if !bytes.Contains(command.body, []byte("integration-ok")) {
		t.Fatal("command output is missing")
	}

	info := env.do(http.MethodGet, "/api/bbraft/info", env.key, "", nil)
	if info.status != http.StatusOK {
		t.Fatalf("info status = %d", info.status)
	}
	runTestCheck(t, extension, "bbraftInfo", info)
	var infoBody struct {
		Commands struct {
			StreamMaxSeconds int `json:"streamMaxSeconds"`
			SyncMaxSeconds   int `json:"syncMaxSeconds"`
		} `json:"commands"`
	}
	if err := json.Unmarshal(info.body, &infoBody); err != nil {
		t.Fatal("info response is malformed")
	}
	if infoBody.Commands.StreamMaxSeconds != 901 || infoBody.Commands.SyncMaxSeconds != 600 {
		t.Fatalf("info caps = %+v", infoBody.Commands)
	}

	unauthorized := env.do(http.MethodGet, "/api/v1/me", "", "", nil)
	if unauthorized.status != http.StatusUnauthorized {
		t.Fatalf("missing key status = %d", unauthorized.status)
	}
	runTestCheck(t, v1, "me", unauthorized)
	foreign := env.do(http.MethodGet, "/api/v1/sandboxes/"+id, env.otherKey, "", nil)
	if foreign.status != http.StatusNotFound {
		t.Fatalf("foreign key status = %d", foreign.status)
	}
	runTestCheck(t, v1, "get", foreign)

	malformed := env.do(http.MethodPost, "/api/v1/sandboxes", env.key, `{"type":"SENTINEL-body-value"`, nil)
	if malformed.status != http.StatusBadRequest {
		t.Fatalf("malformed body status = %d", malformed.status)
	}

	// Durable auth mapping and last-used persistence.
	wantDigest := sha256.Sum256([]byte(env.key))
	keyRow, err := env.store.APIKeyBySHA256(t.Context(), wantDigest)
	if err != nil {
		t.Fatalf("persisted key missing: %v", err)
	}
	if keyRow.Name != "main" || keyRow.LastUsedAt == nil {
		t.Fatal("persisted key name/last-used mismatch")
	}
	account, err := env.store.EnsureAccount(t.Context(), env.owner)
	if err != nil {
		t.Fatalf("owner account missing: %v", err)
	}
	if keyRow.AccountID != account.ID {
		t.Fatal("api key is not mapped to the configured owner account")
	}
	if strings.Contains(env.logs.String(), env.key) || strings.Contains(env.logs.String(), "sentinel-env-value") {
		t.Fatal("logs leaked a raw key or env value")
	}

	operation, deleted := env.deleteSandbox(id)
	runTestCheck(t, v1, "deleteSandbox", deleted)
	completed := env.waitDeletionComplete(operation)
	runTestCheck(t, v1, "getDeletionOperation", completed)

	// Join every role, then sweep all responses and logs after shutdown.
	stop()
	for _, response := range []runResponse{created, got, command, info, unauthorized, foreign, malformed, deleted, completed} {
		for _, secret := range []string{env.key, env.otherKey, "sentinel-env-value", "SENTINEL"} {
			if bytes.Contains(response.body, []byte(secret)) {
				t.Fatal("an API response leaked a credential, env value or rejected body")
			}
		}
	}
	logs := env.logs.String()
	for _, secret := range []string{env.key, env.otherKey, "sentinel-env-value", "SENTINEL"} {
		if strings.Contains(logs, secret) {
			t.Fatal("logs leaked a credential, env value or rejected body")
		}
	}
}

func TestRunSeparateRoles(t *testing.T) {
	env := newRunEnv(t)
	env.start(config.Roles{config.RoleNodeHub: true})
	env.startNode(0)
	env.start(config.Roles{config.RoleLifecycle: true})
	env.start(config.Roles{config.RoleAPI: true})
	env.waitNodeFresh()
	env.waitAPI()

	id := env.createSandbox(`{"type":"small"}`)
	env.waitReady(id)
	command := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/commands", env.key, `{"command":"true"}`, nil)
	if command.status != http.StatusOK {
		t.Fatalf("command status = %d", command.status)
	}
	operation, _ := env.deleteSandbox(id)
	env.waitDeletionComplete(operation)
}

func TestRunSplitAPIStopKeepsNodeHub(t *testing.T) {
	env := newRunEnv(t)
	env.start(config.Roles{config.RoleNodeHub: true})
	env.startNode(0)
	env.start(config.Roles{config.RoleLifecycle: true})
	apiStop := env.start(config.Roles{config.RoleAPI: true})
	env.waitNodeFresh()
	env.waitAPI()

	apiStop()
	mark := time.Now()
	// The nodehub and lifecycle roles keep talking to PostgreSQL and the
	// node after the api process stops: a strictly newer inventory write
	// proves the old timestamp is not just still fresh.
	env.waitNodeSeenAfter(mark)

	env.listeners.API = runTestListen(t)
	env.cfg.Server.Listen = env.listeners.API.Addr().String()
	env.apiBase = "http://" + env.listeners.API.Addr().String()
	env.start(config.Roles{config.RoleAPI: true})
	env.waitAPI()
	me := env.do(http.MethodGet, "/api/v1/me", env.key, "", nil)
	if me.status != http.StatusOK {
		t.Fatalf("restarted api status = %d", me.status)
	}
}

func TestRunNodeHubInventoryStaleness(t *testing.T) {
	env := newRunEnv(t)
	env.start(config.Roles{config.RoleNodeHub: true})
	stopNode := env.startNode(0)
	env.waitNodeFresh()

	stopNode()
	env.waitFor(10*time.Second, "node marked stale", func() bool {
		node, err := env.store.Node(t.Context(), runTestNodeID)
		return err == nil && node.LastSeenAt.Before(time.Unix(1, 0))
	})
	capacity, err := env.store.NodesWithCapacity(t.Context(), db.Resources{}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range capacity {
		if node.ID == runTestNodeID {
			t.Fatal("stale disconnected node still advertises capacity")
		}
	}

	env.startNode(0)
	env.waitNodeFresh()
}

// runObservingInventoryHub wraps the real hub and records when the
// inventory poller subscribes and snapshots. firstLinks closes after the
// first (necessarily empty) links snapshot, giving the late-connection
// regression a deterministic startup barrier without changing the poller's
// default interval.
type runObservingInventoryHub struct {
	*nodelink.Hub

	mu                     sync.Mutex
	changes                int
	links                  int
	changesAtFirstSnapshot int

	firstLinks chan struct{}
	firstOnce  sync.Once
}

func newRunObservingInventoryHub(hub *nodelink.Hub) *runObservingInventoryHub {
	return &runObservingInventoryHub{Hub: hub, firstLinks: make(chan struct{})}
}

func (h *runObservingInventoryHub) Changes() <-chan struct{} {
	h.mu.Lock()
	h.changes++
	h.mu.Unlock()
	return h.Hub.Changes()
}

func (h *runObservingInventoryHub) Links() []*nodelink.Link {
	links := h.Hub.Links()
	h.mu.Lock()
	h.links++
	if h.links == 1 {
		h.changesAtFirstSnapshot = h.changes
	}
	h.mu.Unlock()
	h.firstOnce.Do(func() { close(h.firstLinks) })
	return links
}

func (h *runObservingInventoryHub) subscribedBeforeFirstSnapshot() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.links > 0 && h.changesAtFirstSnapshot > 0
}

// TestRunLateNodeInventoryAndCreate starts the daemon roles before any node
// exists, waits for the inventory poller's deterministic initial empty pass,
// then connects the real nodelink/nodefake node. Publication must follow the
// hub change promptly with the default 30s interval retained, and a
// failFast create immediately after publication must answer 202, never 500.
func TestRunLateNodeInventoryAndCreate(t *testing.T) {
	env := newRunEnv(t)
	v1, _ := runTestValidators(t)
	env.start(config.Roles{config.RoleAPI: true, config.RoleLifecycle: true})
	env.waitAPI()

	tokens, err := auth.LoadNodeTokensFile(t.Context(), env.cfg.Node.TokensFile)
	if err != nil {
		t.Fatal("loading configured node tokens failed")
	}
	hub := nodelink.NewHub(tokens.Authenticator(), testLogger())
	obs := newRunObservingInventoryHub(hub)
	gateway, err := nodegateway.New(hub, env.cfg.Node.GatewayToken)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := nodegateway.NewInventory(obs,
		inventoryWriter{store: env.store, now: time.Now}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately leave Interval at zero: the regression keeps the
	// production default of 30s, so only the hub change can publish soon.

	ctx, cancel := context.WithCancel(t.Context())
	hubDone := make(chan error, 1)
	go func() { hubDone <- hub.Serve(ctx, env.listeners.Node) }()
	gatewayDone := make(chan error, 1)
	go func() { gatewayDone <- gateway.Serve(ctx, env.listeners.Gateway) }()
	inventoryDone := make(chan error, 1)
	go func() { inventoryDone <- inventory.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-inventoryDone; err != nil {
			t.Error("inventory Run failed")
		}
		if err := <-hubDone; err != nil {
			t.Error("hub Serve failed")
		}
		if err := <-gatewayDone; err != nil {
			t.Error("gateway Serve failed")
		}
	})

	select {
	case <-obs.firstLinks:
	case <-time.After(10 * time.Second):
		t.Fatal("inventory never completed its initial empty pass")
	}
	if !obs.subscribedBeforeFirstSnapshot() {
		t.Fatal("inventory did not subscribe to hub changes before its first snapshot")
	}
	if _, err := env.store.Node(t.Context(), runTestNodeID); !errors.Is(err, db.ErrNotFound) {
		t.Fatal("node row exists before the node connected")
	}

	connectStarted := time.Now()
	env.startNode(0)
	env.waitFor(5*time.Second, "initial node publication", func() bool {
		_, err := env.store.Node(t.Context(), runTestNodeID)
		return err == nil
	})
	if elapsed := time.Since(connectStarted); elapsed >= 5*time.Second {
		t.Fatalf("initial publication took %s", elapsed)
	}

	// The publication row proves the node is visible; create immediately.
	// A bounded retry accepts only the documented 503 pair, validating each
	// actual refusal against the v1 API document before retrying. Any other
	// status, including the unmapped scheduler capacity 500, fails at once.
	deadline := time.Now().Add(3 * time.Second)
	var created runResponse
	for {
		created = env.do(http.MethodPost, "/api/v1/sandboxes", env.key,
			`{"type":"small","failFast":true}`, nil)
		switch created.status {
		case http.StatusAccepted:
			// Final success is validated below.
		case http.StatusServiceUnavailable:
			runTestCheck(t, v1, "create", created)
			var refusal struct {
				Code  string `json:"code"`
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(created.body, &refusal); err != nil {
				t.Fatal("refusal response is malformed")
			}
			if refusal.Code != apierr.CodeNoReadyMachine && refusal.Code != apierr.CodeOutOfCapacity {
				t.Fatal("refusal used an unexpected error code")
			}
			if refusal.Error.Code != refusal.Code {
				t.Fatal("refusal envelope codes disagree")
			}
			if time.Now().After(deadline) {
				t.Fatalf("create still refused with %s", refusal.Code)
			}
			time.Sleep(25 * time.Millisecond)
			continue
		default:
			t.Fatalf("create status = %d, want only 202 or the documented 503 pair", created.status)
		}
		break
	}
	runTestCheck(t, v1, "create", created)
	var response struct {
		Status  string `json:"status"`
		Sandbox struct {
			ID    string `json:"id"`
			State string `json:"state"`
		} `json:"sandbox"`
	}
	if err := json.Unmarshal(created.body, &response); err != nil || response.Sandbox.ID == "" {
		t.Fatal("create response is malformed")
	}
	if response.Status != "provisioning" || response.Sandbox.State != "provisioning" {
		t.Fatalf("create status = %q state = %q", response.Status, response.Sandbox.State)
	}
	env.waitReady(response.Sandbox.ID)

	account, err := env.store.EnsureAccount(t.Context(), env.owner)
	if err != nil {
		t.Fatal("finding the owner account failed")
	}
	row, err := env.store.Sandbox(t.Context(), account.ID, response.Sandbox.ID)
	if err != nil {
		t.Fatal("the created sandbox was not durable")
	}
	if row.State != db.StateReady && row.State != db.StateIdle && row.State != db.StateRunning {
		t.Fatalf("durable sandbox state = %q, want ready", row.State)
	}
}

func TestRunAuthBootstrapFailureJoinsSiblings(t *testing.T) {
	env := newRunEnv(t)
	// A configured key that does not use the public prefix fails startup
	// before any handler or last-used effect.
	badKeys := filepath.Join(t.TempDir(), "keys.json")
	if err := os.WriteFile(badKeys, []byte(`{"keys":[{"id":"main","owner":"o","key":"wrong-prefix-key"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := *env.cfg
	cfg.Auth.KeysFile = badKeys
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, &cfg, env.options()) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run accepted an invalid configured key prefix")
		}
		if strings.Contains(err.Error(), "wrong-prefix-key") {
			t.Fatal("startup error leaked key material")
		}
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("Run did not join siblings after auth bootstrap failure")
	}
	// Run owns the provided listeners: none may stay open.
	for name, ln := range map[string]net.Listener{"api": env.listeners.API, "node": env.listeners.Node, "gateway": env.listeners.Gateway} {
		if tcp, ok := ln.(*net.TCPListener); ok {
			_ = tcp.SetDeadline(time.Now())
		}
		if _, err := ln.Accept(); err == nil {
			t.Fatalf("%s listener stayed open", name)
		}
	}
	// The wrong prefix must fail before any account or key row exists.
	var accounts, keys int
	if err := env.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM accounts`).Scan(&accounts); err != nil {
		t.Fatal(err)
	}
	if err := env.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM api_keys`).Scan(&keys); err != nil {
		t.Fatal(err)
	}
	if accounts != 0 || keys != 0 {
		t.Fatalf("bad prefix wrote side effects: %d accounts, %d keys", accounts, keys)
	}
}

func TestRunListenerFailureJoinsSiblings(t *testing.T) {
	env := newRunEnv(t)
	// A dead node listener fails the hub immediately while lifecycle and
	// the gateway are starting; Run must cancel and join them.
	if err := env.listeners.Node.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := *env.cfg
	cfg.Roles = config.Roles{config.RoleNodeHub: true, config.RoleLifecycle: true}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, &cfg, env.options()) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run accepted a dead node listener")
		}
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("Run did not join siblings after a listener failure")
	}
	// Only listeners owned by the selected roles were closed. The unused
	// API socket stays open for its own (possible) process.
	if tcp, ok := env.listeners.API.(*net.TCPListener); ok {
		_ = tcp.SetDeadline(time.Now().Add(50 * time.Millisecond))
	}
	if _, err := env.listeners.API.Accept(); errors.Is(err, net.ErrClosed) {
		t.Fatal("unselected API listener was closed")
	}
	if tcp, ok := env.listeners.Gateway.(*net.TCPListener); ok {
		_ = tcp.SetDeadline(time.Now())
	}
	if _, err := env.listeners.Gateway.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatal("selected gateway listener stayed open")
	}
}

func TestRunWaitsForHandlerBeforeStoreClose(t *testing.T) {
	env := newRunEnv(t)
	blocked := make(chan struct{})
	release := make(chan struct{})
	handlerDone := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
	})
	options := env.options()
	options.ShutdownTimeout = 20 * time.Millisecond
	options.APIHooks.RegisterUploads = func(mux *http.ServeMux, _ func(http.Handler) http.Handler, _ UploadDeps) error {
		mux.HandleFunc("GET /block", func(http.ResponseWriter, *http.Request) {
			close(blocked)
			<-release
			close(handlerDone)
		})
		return nil
	}
	roleStores := make(chan *db.Store, 1)
	options.StoreOpener = func(ctx context.Context, _ config.Role) (*db.Store, error) {
		store, err := env.openRoleStore(ctx)
		if err == nil {
			roleStores <- store
		}
		return store, err
	}
	cfg := *env.cfg
	cfg.Roles = config.Roles{config.RoleAPI: true}
	ctx, cancel := context.WithCancel(t.Context())
	runFinished := make(chan struct{})
	var runErr error
	go func() {
		runErr = Run(ctx, &cfg, options)
		close(runFinished)
	}()
	var roleStore *db.Store
	var clientDone chan struct{}
	t.Cleanup(func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		select {
		case <-runFinished:
		case <-time.After(10 * time.Second):
			t.Error("Run did not finish during cleanup")
		}
		if roleStore != nil {
			roleStore.Close()
		}
		if clientDone != nil {
			select {
			case <-clientDone:
			case <-time.After(5 * time.Second):
				t.Error("client did not finish during cleanup")
			}
		}
	})
	env.waitAPI()
	roleStore = <-roleStores

	clientDone = make(chan struct{})
	go func() {
		defer close(clientDone)
		response, err := env.client.Get(env.apiBase + "/block")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("blocking handler never started")
	}

	cancel()
	// Past the graceful timeout the connection is force-closed, but Run
	// must not return (and therefore must not close its role Store)
	// while the handler is still live.
	select {
	case <-runFinished:
		t.Fatal("Run returned before the handler was released")
	case <-time.After(5 * 20 * time.Millisecond):
	}
	if err := roleStore.Pool().Ping(t.Context()); err != nil {
		t.Fatal("role Store was closed while its handler was still live")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-runFinished:
		if runErr != nil {
			t.Fatalf("Run = %v", runErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the handler was released")
	}
	if err := roleStore.Pool().Ping(t.Context()); err == nil {
		t.Fatal("role Store stayed open after Run returned")
	}
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("handler was not joined")
	}
	select {
	case <-clientDone:
	case <-time.After(2 * time.Second):
		t.Fatal("client was not joined")
	}
}

func TestRunCancellationJoinsBlockedCommand(t *testing.T) {
	env := newRunEnv(t)
	stop := env.start(config.Roles{config.RoleAPI: true, config.RoleLifecycle: true, config.RoleNodeHub: true})
	env.startNode(0)
	env.waitNodeFresh()
	env.waitAPI()
	id := env.createSandbox(`{"type":"small"}`)
	env.waitReady(id)

	streamCtx, streamCancel := context.WithCancel(t.Context())
	streamDone := make(chan struct{})
	t.Cleanup(func() {
		streamCancel()
		select {
		case <-streamDone:
		case <-time.After(5 * time.Second):
			t.Error("blocked command stream did not finish during cleanup")
		}
	})
	go func() {
		defer close(streamDone)
		request, err := http.NewRequestWithContext(streamCtx, http.MethodPost, env.apiBase+"/api/v1/sandboxes/"+id+"/commands", strings.NewReader(`{"command":"sleep 30 & echo $! > child.pid; wait","stream":true,"timeoutSeconds":30}`))
		if err != nil {
			return
		}
		request.Header.Set("Authorization", "Bearer "+env.key)
		request.Header.Set("Content-Type", "application/json")
		response, err := (&http.Client{Timeout: 40 * time.Second}).Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
	}()

	// Wait for the command to actually run on the node (and publish its
	// child PID), not for a fixed delay.
	pidPath := filepath.Join(env.nodeRoot, "vms", id, "home/user", "child.pid")
	var childPID int
	env.waitFor(10*time.Second, "command start marker", func() bool {
		data, err := os.ReadFile(pidPath)
		if err != nil {
			return false
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil || pid <= 0 {
			return false
		}
		childPID = pid
		return true
	})

	started := time.Now()
	stop()
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("Run shutdown took %s", elapsed)
	}
	select {
	case <-streamDone:
	case <-time.After(5 * time.Second):
		streamCancel()
		t.Fatal("blocked command client was not joined")
	}
	env.waitFor(5*time.Second, "remote process group gone", func() bool {
		return errors.Is(syscall.Kill(childPID, 0), syscall.ESRCH)
	})
}

func TestRunCancellationJoinsBlockedBoot(t *testing.T) {
	env := newRunEnv(t)
	stop := env.start(config.Roles{config.RoleAPI: true, config.RoleLifecycle: true, config.RoleNodeHub: true})
	env.startNode(5 * time.Second)
	env.waitNodeFresh()
	env.waitAPI()
	id := env.createSandbox(`{"type":"small"}`)
	// The provision job has reached the node and the (delayed) boot is in
	// flight before cancellation.
	env.waitFor(10*time.Second, "boot in flight", func() bool {
		_, err := os.Stat(filepath.Join(env.nodeRoot, "vms", id))
		return err == nil
	})
	started := time.Now()
	stop()
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("Run shutdown took %s", elapsed)
	}
}

func TestRunUploadsReceiver(t *testing.T) {
	env := newRunEnv(t)
	v1, extension := runTestValidators(t)
	env.start(config.Roles{config.RoleAPI: true, config.RoleLifecycle: true, config.RoleNodeHub: true})
	env.startNode(0)
	env.waitNodeFresh()
	env.waitAPI()
	id := env.createSandbox(`{"type":"small"}`)
	env.waitReady(id)

	issued := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/upload-url", env.key, `{"path":"/home/user/upload.txt","ttlSeconds":60}`, nil)
	if issued.status != http.StatusOK {
		t.Fatalf("issue status = %d", issued.status)
	}
	runTestCheck(t, v1, "createUploadUrl", issued)
	var urlBody struct {
		URL    string `json:"url"`
		Method string `json:"method"`
		Path   string `json:"path"`
	}
	if err := json.Unmarshal(issued.body, &urlBody); err != nil {
		t.Fatal("upload URL response is malformed")
	}
	if urlBody.Method != "PUT" || urlBody.Path != "/home/user/upload.txt" || !strings.HasPrefix(urlBody.URL, env.cfg.Server.PublicURL+"/api/bbraft/uploads/") {
		t.Fatal("upload URL response does not use the trusted base URL")
	}

	payload := bytes.Repeat([]byte("bamboo-raft-upload-"), (2<<20)/len("bamboo-raft-upload-")+1)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPut, urlBody.URL, bytes.NewReader(payload))
	if err != nil {
		t.Fatal("could not build the upload request")
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := env.client.Do(request)
	if err != nil {
		t.Fatal("upload receiver request failed")
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal("upload receiver response could not be read")
	}
	received := runResponse{status: response.StatusCode, header: response.Header.Clone(), body: body}
	if received.status != http.StatusOK {
		t.Fatalf("receiver status = %d", received.status)
	}
	runTestCheck(t, extension, "bbraftUploadReceiver", received)
	var uploaded struct {
		Type string `json:"type"`
		Path string `json:"path"`
		Size int64  `json:"size"`
	}
	if err := json.Unmarshal(received.body, &uploaded); err != nil {
		t.Fatal("receiver success is malformed")
	}
	if uploaded.Type != "file.uploaded" || uploaded.Path != "/home/user/upload.txt" || uploaded.Size != int64(len(payload)) {
		t.Fatal("receiver success fields do not match the uploaded file")
	}
	stored, err := os.ReadFile(filepath.Join(env.nodeRoot, "vms", id, "home/user", "upload.txt"))
	if err != nil {
		t.Fatal("uploaded file is missing")
	}
	if !bytes.Equal(stored, payload) {
		t.Fatal("uploaded bytes differ from the request body")
	}

	// A bad token is the same safe 404 without a bearer header.
	bad := env.do(http.MethodPut, api.UploadReceiverPrefix+"/not-a-token", "", "", nil)
	if bad.status != http.StatusNotFound {
		t.Fatalf("bad token status = %d", bad.status)
	}
	runTestCheck(t, extension, "bbraftUploadReceiver", bad)

	// Issuance still requires the bearer credential.
	unauthorized := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/files/upload-url", "", `{"path":"/home/user/other.txt"}`, nil)
	if unauthorized.status != http.StatusUnauthorized {
		t.Fatalf("unauthenticated issue status = %d", unauthorized.status)
	}
}

func TestRunAPITLS(t *testing.T) {
	env := newRunEnv(t)
	certFile, keyFile, pool := runTestTLSCert(t)
	cfg := *env.cfg
	cfg.Roles = config.Roles{config.RoleAPI: true}
	cfg.Server.TLSCertFile = certFile
	cfg.Server.TLSKeyFile = keyFile
	cfg.Server.PublicURL = "https://" + env.listeners.API.Addr().String()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, &cfg, env.options()) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("TLS Run = %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("TLS Run did not stop")
		}
	}()
	// Trust exactly the fixture CA, never skip verification.
	env.client = &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}},
	}
	env.apiBase = "https://" + env.listeners.API.Addr().String()
	env.waitAPI()

	// A different CA must be rejected.
	_, _, wrongPool := runTestTLSCert(t)
	wrongCA := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: wrongPool}},
	}
	if response, err := wrongCA.Get(env.apiBase + "/healthz"); err == nil {
		response.Body.Close()
		t.Fatal("TLS client with the wrong CA was accepted")
	}
	// A wrong certificate name must be rejected too.
	wrongName := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "wrong.example"}},
	}
	if response, err := wrongName.Get(env.apiBase + "/healthz"); err == nil {
		response.Body.Close()
		t.Fatal("TLS client with the wrong server name was accepted")
	}
}

func TestRunGatewayTLS(t *testing.T) {
	env := newRunEnv(t)
	certFile, keyFile, _ := runTestTLSCert(t)
	env.cfg.Node.GatewayTLSCertFile = certFile
	env.cfg.Node.GatewayTLSKeyFile = keyFile
	env.cfg.Node.GatewayCACertFile = certFile
	env.cfg.Node.GatewayServerName = "127.0.0.1"
	env.start(config.Roles{config.RoleAPI: true, config.RoleLifecycle: true, config.RoleNodeHub: true})
	env.startNode(0)
	env.waitNodeFresh()
	env.waitAPI()
	id := env.createSandbox(`{"type":"small"}`)
	env.waitReady(id)
	command := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/commands", env.key, `{"command":"true"}`, nil)
	if command.status != http.StatusOK {
		t.Fatalf("command over TLS gateway status = %d", command.status)
	}
}

// runTestTLSCert writes a self-signed certificate for 127.0.0.1 and returns
// the CA pool clients use to verify it.
func runTestTLSCert(t *testing.T) (string, string, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("fixture certificate did not load")
	}
	return certFile, keyFile, pool
}
