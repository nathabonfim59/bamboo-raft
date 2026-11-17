package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/json/v2"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/api"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/server/config"
)

// sshIntegrationPublicKey returns one deterministic, well-formed ed25519
// OpenSSH public key line. It is a public fixture, never a credential.
func sshIntegrationPublicKey() string {
	seed := bytes.Repeat([]byte{0x42}, ed25519.SeedSize)
	private := ed25519.NewKeyFromSeed(seed)
	public := private.Public().(ed25519.PublicKey)
	blob := binary.BigEndian.AppendUint32(nil, uint32(len("ssh-ed25519")))
	blob = append(blob, "ssh-ed25519"...)
	blob = binary.BigEndian.AppendUint32(blob, uint32(len(public)))
	blob = append(blob, public...)
	return "ssh-ed25519 " + base64.StdEncoding.EncodeToString(blob) + " integration@bamboo"
}

// sshIntegrationResponse is the pinned sshKey success envelope.
type sshIntegrationResponse struct {
	Type        string  `json:"type"`
	Success     bool    `json:"success"`
	MachineIP   *string `json:"machineIp"`
	SSHUser     string  `json:"sshUser"`
	SSHEndpoint *string `json:"sshEndpoint"`
	HostKey     string  `json:"hostKey"`
}

// sshIntegrationSandbox is the subset of a sandbox object this test reads.
type sshIntegrationSandbox struct {
	ID          string  `json:"id"`
	State       string  `json:"state"`
	SSHEndpoint *string `json:"sshEndpoint"`
}

// sshIntegrationExpect asserts the status and validates the complete response
// against the v1 API document.
func sshIntegrationExpect(t *testing.T, v1 *compat.Validator, operationID string, response runResponse, wantStatus int) {
	t.Helper()
	if response.status != wantStatus {
		t.Fatalf("%s status = %d, want %d", operationID, response.status, wantStatus)
	}
	runTestCheck(t, v1, operationID, response)
}

// sshIntegrationAssertEndpointField checks that the raw sandbox object
// serializes the pinned nullable sshEndpoint member (present, null or value)
// and never a hostKey member.
func sshIntegrationAssertEndpointField(t *testing.T, object map[string]any, want *string) {
	t.Helper()
	value, present := object["sshEndpoint"]
	if !present {
		t.Fatal("sandbox object omitted the pinned nullable sshEndpoint member")
	}
	switch {
	case want == nil && value != nil:
		t.Fatalf("sshEndpoint = %v, want explicit null", value)
	case want != nil && value != *want:
		t.Fatalf("sshEndpoint = %v, want %q", value, *want)
	}
	if _, leaked := object["hostKey"]; leaked {
		t.Fatal("sandbox object exposed a hostKey member")
	}
}

// sshIntegrationGet validates and decodes one get response.
func sshIntegrationGet(t *testing.T, env *runEnv, v1 *compat.Validator, id string) sshIntegrationSandbox {
	t.Helper()
	response := env.do(http.MethodGet, "/api/v1/sandboxes/"+id, env.key, "", nil)
	sshIntegrationExpect(t, v1, "get", response, http.StatusOK)
	var got struct {
		Sandbox sshIntegrationSandbox `json:"sandbox"`
	}
	if err := json.Unmarshal(response.body, &got); err != nil {
		t.Fatal("get response is malformed")
	}
	if got.Sandbox.ID != id {
		t.Fatalf("get returned sandbox %q, want %q", got.Sandbox.ID, id)
	}
	var raw struct {
		Sandbox map[string]any `json:"sandbox"`
	}
	if err := json.Unmarshal(response.body, &raw); err != nil {
		t.Fatal("get response raw object is malformed")
	}
	sshIntegrationAssertEndpointField(t, raw.Sandbox, got.Sandbox.SSHEndpoint)
	return got.Sandbox
}

// sshIntegrationList validates and decodes the requested sandbox of a list
// response.
func sshIntegrationList(t *testing.T, env *runEnv, v1 *compat.Validator, id string) sshIntegrationSandbox {
	t.Helper()
	response := env.do(http.MethodGet, "/api/v1/sandboxes?limit=10", env.key, "", nil)
	sshIntegrationExpect(t, v1, "sandboxes", response, http.StatusOK)
	var got struct {
		Sandboxes []sshIntegrationSandbox `json:"sandboxes"`
	}
	if err := json.Unmarshal(response.body, &got); err != nil {
		t.Fatal("list response is malformed")
	}
	var raw struct {
		Sandboxes []map[string]any `json:"sandboxes"`
	}
	if err := json.Unmarshal(response.body, &raw); err != nil {
		t.Fatal("list response raw objects are malformed")
	}
	if len(raw.Sandboxes) != len(got.Sandboxes) {
		t.Fatal("list response typed and raw lengths differ")
	}
	for i, sandbox := range got.Sandboxes {
		if sandbox.ID == id {
			sshIntegrationAssertEndpointField(t, raw.Sandboxes[i], sandbox.SSHEndpoint)
			return sandbox
		}
	}
	t.Fatalf("list response does not contain sandbox %q", id)
	return sshIntegrationSandbox{}
}

// sshIntegrationStored reads the durable row directly.
func sshIntegrationStored(t *testing.T, env *runEnv, id string) db.Sandbox {
	t.Helper()
	account, err := env.store.EnsureAccount(t.Context(), env.owner)
	if err != nil {
		t.Fatal("owner account is missing")
	}
	row, err := env.store.Sandbox(t.Context(), account.ID, id)
	if err != nil {
		t.Fatalf("durable sandbox is missing: %T", err)
	}
	return row
}

// sshIntegrationPost sends one sshKey request and returns its response.
func sshIntegrationPost(env *runEnv, id, key string) runResponse {
	return env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/sshkey", env.key, `{"key":"`+key+`"}`, nil)
}

// TestRunSSHKeyLifecycleMetadata proves the public SSH route and the durable
// endpoint lifetime through the real API, lifecycle, nodehub, gateway and
// nodefake. It requires BBRAFT_TEST_DATABASE_URL; newRunEnv skips without it.
// Nodefake returns reference metadata only: this test makes no real SSH
// reachability claim.
func TestRunSSHKeyLifecycleMetadata(t *testing.T) {
	env := newRunEnv(t)
	v1, _ := runTestValidators(t)
	stop := env.start(config.Roles{config.RoleAPI: true, config.RoleLifecycle: true, config.RoleNodeHub: true})
	env.startNode(0)
	env.waitNodeFresh()
	env.waitAPI()

	id := env.createSandbox(`{"type":"small"}`)
	env.waitReady(id)
	key := sshIntegrationPublicKey()

	// Before any sshKey call the pinned nullable field is an explicit null,
	// both on get and list.
	if got := sshIntegrationGet(t, env, v1, id); got.SSHEndpoint != nil {
		t.Fatalf("sshEndpoint before sshKey = %q, want null", *got.SSHEndpoint)
	}
	if got := sshIntegrationList(t, env, v1, id); got.SSHEndpoint != nil {
		t.Fatalf("list sshEndpoint before sshKey = %q, want null", *got.SSHEndpoint)
	}

	// A malformed key is a declared 400 and persists nothing, including a
	// sentinel that must never reach a log.
	invalid := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/sshkey", env.key,
		`{"key":"SENTINEL-INVALID-PRIVATE-KEY"}`, nil)
	sshIntegrationExpect(t, v1, "sshKey", invalid, http.StatusBadRequest)
	if stored := sshIntegrationStored(t, env, id); stored.SSH != nil {
		t.Fatal("invalid key persisted SSH metadata")
	}

	configured := sshIntegrationPost(env, id, key)
	sshIntegrationExpect(t, v1, "sshKey", configured, http.StatusOK)
	var body sshIntegrationResponse
	if err := json.Unmarshal(configured.body, &body); err != nil {
		t.Fatal("sshKey response is malformed")
	}
	if body.Type != api.TypeSSHKeyConfigured || !body.Success {
		t.Fatalf("sshKey type/success = %q/%v", body.Type, body.Success)
	}
	if body.SSHUser != "user" || body.MachineIP == nil || *body.MachineIP != "127.0.0.1" ||
		body.SSHEndpoint == nil || *body.SSHEndpoint != "127.0.0.1:22" || body.HostKey != nodefake.HostKey {
		t.Fatal("sshKey response does not carry the node-confirmed reference metadata")
	}
	if stored := sshIntegrationStored(t, env, id); stored.SSH == nil ||
		stored.SSH.Endpoint != *body.SSHEndpoint || stored.SSH.HostKey != body.HostKey {
		t.Fatal("sshKey response metadata was not persisted")
	}

	// get and list export the durable endpoint and never a host-key member.
	for _, got := range []sshIntegrationSandbox{
		sshIntegrationGet(t, env, v1, id),
		sshIntegrationList(t, env, v1, id),
	} {
		if got.SSHEndpoint == nil || *got.SSHEndpoint != *body.SSHEndpoint {
			t.Fatalf("sshEndpoint = %v, want %q", got.SSHEndpoint, *body.SSHEndpoint)
		}
	}

	// Repeating the same key is idempotent and keeps the same metadata.
	repeat := sshIntegrationPost(env, id, key)
	sshIntegrationExpect(t, v1, "sshKey", repeat, http.StatusOK)
	if stored := sshIntegrationStored(t, env, id); stored.SSH == nil || stored.SSH.Endpoint != *body.SSHEndpoint {
		t.Fatal("repeat installation changed the stored endpoint")
	}

	// Owner isolation and a missing sandbox are the same safe 404 and never
	// reach another tenant's row or metadata.
	foreign := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/sshkey", env.otherKey, `{"key":"`+key+`"}`, nil)
	sshIntegrationExpect(t, v1, "sshKey", foreign, http.StatusNotFound)
	missing := sshIntegrationPost(env, "bx_22222222", key)
	sshIntegrationExpect(t, v1, "sshKey", missing, http.StatusNotFound)

	// A stop clears the pair; while archived sshKey is the pinned 400, never
	// a 409, and persists nothing.
	stopped := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/stop", env.key, `{}`, nil)
	if stopped.status != http.StatusAccepted {
		t.Fatalf("stop status = %d", stopped.status)
	}
	env.lifecycleRunWaitState(id, "archived", 30*time.Second)
	if got := sshIntegrationGet(t, env, v1, id); got.SSHEndpoint != nil {
		t.Fatal("stop did not clear the exported sshEndpoint")
	}
	if got := sshIntegrationList(t, env, v1, id); got.SSHEndpoint != nil {
		t.Fatal("stop did not clear the listed sshEndpoint")
	}
	if stored := sshIntegrationStored(t, env, id); stored.SSH != nil {
		t.Fatal("stop did not clear the durable SSH pair")
	}
	notRunning := sshIntegrationPost(env, id, key)
	sshIntegrationExpect(t, v1, "sshKey", notRunning, http.StatusBadRequest)
	var notRunningBody struct {
		Code  string `json:"code"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(notRunning.body, &notRunningBody); err != nil {
		t.Fatal("stopped sshKey response is malformed")
	}
	if notRunningBody.Code != "machine_not_running" || notRunningBody.Error.Code != "machine_not_running" {
		t.Fatalf("stopped sshKey code = %q, want machine_not_running", notRunningBody.Code)
	}
	if stored := sshIntegrationStored(t, env, id); stored.SSH != nil {
		t.Fatal("stopped sshKey persisted metadata")
	}

	// Resume keeps the endpoint absent until a successful sshKey refresh.
	resumed := env.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/resume", env.key, `{}`, nil)
	if resumed.status != http.StatusAccepted {
		t.Fatalf("resume status = %d", resumed.status)
	}
	env.waitReady(id)
	if got := sshIntegrationGet(t, env, v1, id); got.SSHEndpoint != nil {
		t.Fatal("resume exposed a stale sshEndpoint before a refresh")
	}
	if got := sshIntegrationList(t, env, v1, id); got.SSHEndpoint != nil {
		t.Fatal("resume listed a stale sshEndpoint before a refresh")
	}
	if stored := sshIntegrationStored(t, env, id); stored.SSH != nil {
		t.Fatal("resume did not leave the durable SSH pair absent")
	}
	refreshed := sshIntegrationPost(env, id, key)
	sshIntegrationExpect(t, v1, "sshKey", refreshed, http.StatusOK)
	if stored := sshIntegrationStored(t, env, id); stored.SSH == nil || stored.SSH.Endpoint != "127.0.0.1:22" {
		t.Fatal("refreshed SSH metadata was not persisted")
	}
	if got := sshIntegrationGet(t, env, v1, id); got.SSHEndpoint == nil || *got.SSHEndpoint != "127.0.0.1:22" {
		t.Fatal("refreshed sshEndpoint is not exported on get")
	}
	if got := sshIntegrationList(t, env, v1, id); got.SSHEndpoint == nil || *got.SSHEndpoint != "127.0.0.1:22" {
		t.Fatal("refreshed sshEndpoint is not exported on list")
	}

	// Delete removes the row and the route becomes owner-scoped missing.
	operation, deleted := env.deleteSandbox(id)
	runTestCheck(t, v1, "deleteSandbox", deleted)
	completed := env.waitDeletionComplete(operation)
	runTestCheck(t, v1, "getDeletionOperation", completed)
	gone := sshIntegrationPost(env, id, key)
	sshIntegrationExpect(t, v1, "sshKey", gone, http.StatusNotFound)

	// Join every role before inspecting the captured logs; lockedBuffer is
	// already synchronized. Failure output never prints the key or log text.
	stop()
	logs := env.logs.String()
	if strings.Contains(logs, key) {
		t.Fatal("role logs leaked the user public key (classification: ssh-user-key)")
	}
	if strings.Contains(logs, "SENTINEL-INVALID-PRIVATE-KEY") {
		t.Fatal("role logs leaked a rejected private-key body (classification: ssh-rejected-body)")
	}
}

// sshFailingNode wraps nodefake and overrides only AddSSHKey, so the failure
// mapping can be exercised through the real server, gateway and nodelink
// without editing the shared fake.
type sshFailingNode struct {
	*nodefake.Node
	err   error
	calls *atomic.Int64
}

// AddSSHKey records the call and returns the configured failure.
func (n *sshFailingNode) AddSSHKey(context.Context, *nodev1.AddSSHKeyRequest) (*nodev1.SSHAccess, error) {
	n.calls.Add(1)
	return nil, n.err
}

// sshIntegrationStartFailingNode starts one nodelink node that fails AddSSHKey.
func sshIntegrationStartFailingNode(t *testing.T, env *runEnv, addErr error) *atomic.Int64 {
	t.Helper()
	root := t.TempDir()
	env.nodeRoot = root
	calls := new(atomic.Int64)
	node := &sshFailingNode{Node: nodefake.New(runTestNodeID, root), err: addErr, calls: calls}
	server := grpc.NewServer(nodelink.ServerOptions()...)
	nodev1.RegisterNodeServer(server, node)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		_ = nodelink.Serve(ctx, nodelink.DialConfig{
			Addr:       env.listeners.Node.Addr().String(),
			NodeID:     runTestNodeID,
			Token:      env.nodeToken,
			Log:        testLogger(),
			MinBackoff: 10 * time.Millisecond,
			MaxBackoff: 50 * time.Millisecond,
		}, server)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("failing node did not stop")
		}
	})
	return calls
}

// TestRunSSHKeyNodeFailureMapping proves the approved 503/502 sshKey answers
// through the real API role and that no failed RPC persists metadata.
func TestRunSSHKeyNodeFailureMapping(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantStatus  int
		wantCode    string
		wantRetry   *bool
		wantRetryIn bool
	}{
		{
			name:        "host key unavailable",
			err:         status.Error(codes.Unavailable, "guest host key is not ready"),
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    "ssh_host_key_unavailable",
			wantRetryIn: true,
		},
		{
			name:       "resource exhausted is not retryable",
			err:        status.Error(codes.ResourceExhausted, "transport limit"),
			wantStatus: http.StatusBadGateway,
			wantCode:   "sandbox_direct_failed",
			wantRetry:  new(false),
		},
		{
			name:       "other node failure is retryable",
			err:        status.Error(codes.Internal, "node fault"),
			wantStatus: http.StatusBadGateway,
			wantCode:   "sandbox_direct_failed",
			wantRetry:  new(true),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := newRunEnv(t)
			v1, _ := runTestValidators(t)
			env.start(config.Roles{config.RoleAPI: true, config.RoleLifecycle: true, config.RoleNodeHub: true})
			calls := sshIntegrationStartFailingNode(t, env, test.err)
			env.waitNodeFresh()
			env.waitAPI()
			id := env.createSandbox(`{"type":"small"}`)
			env.waitReady(id)

			response := sshIntegrationPost(env, id, sshIntegrationPublicKey())
			sshIntegrationExpect(t, v1, "sshKey", response, test.wantStatus)
			var body struct {
				Code  string `json:"code"`
				Error struct {
					Code    string         `json:"code"`
					Details map[string]any `json:"details"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.body, &body); err != nil {
				t.Fatal("sshKey failure response is malformed")
			}
			if body.Code != test.wantCode || body.Error.Code != test.wantCode {
				t.Fatalf("sshKey code = %q/%q, want %q", body.Code, body.Error.Code, test.wantCode)
			}
			if test.wantRetryIn {
				seconds, ok := body.Error.Details["retryAfterSeconds"].(float64)
				if !ok || seconds != 5 {
					t.Fatalf("retryAfterSeconds = %v, want 5", body.Error.Details["retryAfterSeconds"])
				}
			}
			if test.wantRetry != nil {
				retryable, ok := body.Error.Details["retryable"].(bool)
				if !ok || retryable != *test.wantRetry {
					t.Fatalf("retryable = %v, want %v", body.Error.Details["retryable"], *test.wantRetry)
				}
			}
			if calls.Load() == 0 {
				t.Fatal("AddSSHKey was not called")
			}
			if stored := sshIntegrationStored(t, env, id); stored.SSH != nil {
				t.Fatal("failed sshKey persisted SSH metadata")
			}
			if got := sshIntegrationGet(t, env, v1, id); got.SSHEndpoint != nil {
				t.Fatal("failed sshKey exported an sshEndpoint")
			}
		})
	}
}
