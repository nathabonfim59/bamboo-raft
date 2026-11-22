package server

// Milestone 2 exit: two nodes behind one control plane. The real api,
// lifecycle and nodehub roles run against PostgreSQL and two nodefake nodes
// (node-a, node-b) that dial through nodelink and share one object store; the
// snapshot worker and the reaper tick every second. One test per scenario:
// node loss, stop on one node and resume on the other, a fork across nodes, a
// saved snapshot deployed with count over both nodes, share codes, and
// recover.
//
// A node disappears by cancelling its nodelink.Serve (the link closes, so the
// inventory goes stale and the node no longer answers) and comes back by
// serving the same fake again. A node that connects later than the other is
// how a test decides where the first sandbox lands: placement only sees
// connected nodes.
//
// The tests need PostgreSQL: they skip unless BBRAFT_TEST_DATABASE_URL names a
// database, like the other server integration tests. The memstore case always
// runs with it; the garage case runs the same scenario against a real S3
// bucket and skips unless BBRAFT_TEST_S3_GARAGE=1 and a garage binary is in
// PATH (BBRAFT_TEST_REQUIRE_ALL=1 turns either missing piece into a failure).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/nodelink"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/s3"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/s3/s3test"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

const (
	// m2exitFilePath is the guest file the scenarios write.
	m2exitFilePath = "/home/user/m2.txt"
	// m2exitWait bounds each wait in the scenarios.
	m2exitWait = 45 * time.Second
	// m2exitNodeA and m2exitNodeB are the node ids.
	m2exitNodeA = "node-a"
	m2exitNodeB = "node-b"
	// m2exitSavedName is the saved snapshot the deploy scenarios use.
	m2exitSavedName = "m2-base"
	// m2exitNodeLostSeconds is the node-loss grace of the node-loss scenario.
	// The harness hands the server a config literal, so the 60 second floor
	// the command line enforces does not apply.
	m2exitNodeLostSeconds = 2
)

// m2exitNode is a nodefake behind the two test switches of the recover
// scenario: the guest agent can be made unreachable (Exec fails) and every
// ResetVM is counted. ResetVM brings the guest back.
type m2exitNode struct {
	*nodefake.Node
	id    string
	token string

	unreachable atomic.Bool
	resets      atomic.Int32

	mu   sync.Mutex
	stop func() // ends the running Serve; nil while the node is away
}

// Exec fails while the guest is unreachable.
func (n *m2exitNode) Exec(req *nodev1.ExecRequest, stream grpc.ServerStreamingServer[nodev1.ExecEvent]) error {
	if n.unreachable.Load() {
		return status.Error(codes.Unavailable, "guest agent does not answer")
	}
	return n.Node.Exec(req, stream)
}

// ResetVM counts the call and makes the guest reachable again.
func (n *m2exitNode) ResetVM(ctx context.Context, req *nodev1.ResetVMRequest) (*nodev1.VM, error) {
	n.resets.Add(1)
	vm, err := n.Node.ResetVM(ctx, req)
	if err == nil {
		n.unreachable.Store(false)
	}
	return vm, err
}

// hasVM reports whether the node still holds a VM for the sandbox.
func (n *m2exitNode) hasVM(t *testing.T, sandboxID string) bool {
	t.Helper()
	_, err := n.Node.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: sandboxID})
	switch status.Code(err) {
	case codes.OK:
		return true
	case codes.NotFound:
		return false
	default:
		t.Fatalf("GetVM %s on %s: %v", sandboxID, n.id, err)
		return false
	}
}

// m2exitEnv is a run environment with two nodes that are not connected yet.
type m2exitEnv struct {
	*runEnv
	v1      *compat.Validator
	nodes   map[string]*m2exitNode
	account db.Account
}

// m2exitEach runs scenario once per object store.
func m2exitEach(t *testing.T, scenario func(*testing.T, objstore.Store)) {
	t.Helper()
	stores := []struct {
		name string
		open func(t *testing.T) objstore.Store
	}{
		{"memstore", func(*testing.T) objstore.Store { return memstore.New() }},
		{"garage", func(t *testing.T) objstore.Store {
			cfg := s3test.StartGarage(t)
			store, err := s3.New(t.Context(), cfg)
			if err != nil {
				t.Fatalf("s3.New against garage: %v", err)
			}
			return store
		}},
	}
	for _, tc := range stores {
		t.Run(tc.name, func(t *testing.T) {
			scenario(t, tc.open(t))
		})
	}
}

// m2exitStart runs the three server roles over objects, with a node-loss
// grace of nodeLostSeconds (0 turns node loss off), and prepares both nodes.
// Neither is connected.
func m2exitStart(t *testing.T, objects objstore.Store, nodeLostSeconds int) *m2exitEnv {
	t.Helper()
	env := newRunEnv(t)
	v1, _ := runTestValidators(t)
	e := &m2exitEnv{runEnv: env, v1: v1, nodes: map[string]*m2exitNode{}}
	var entries []string
	for _, id := range []string{m2exitNodeA, m2exitNodeB} {
		token := "m2exit-token-" + id + "-" + strings.ReplaceAll(t.Name(), "/", "-")
		sum := sha256.Sum256([]byte(token))
		entries = append(entries, fmt.Sprintf(`{"id":%q,"tokenSha256":%q}`, id, hex.EncodeToString(sum[:])))
		fake := nodefake.New(id, t.TempDir())
		fake.Store = objects
		e.nodes[id] = &m2exitNode{Node: fake, id: id, token: token}
	}
	tokens := `{"nodes":[` + strings.Join(entries, ",") + `]}`
	if err := os.WriteFile(env.cfg.Node.TokensFile, []byte(tokens), 0o600); err != nil {
		t.Fatal(err)
	}
	env.cfg.Lifecycle.NodeLostSeconds = nodeLostSeconds
	env.p2snapStart(objects)
	account, err := env.store.EnsureAccount(t.Context(), env.owner)
	if err != nil {
		t.Fatalf("owner account: %v", err)
	}
	e.account = account
	env.waitAPI()
	return e
}

// up connects the node and waits until the server has its inventory.
func (e *m2exitEnv) up(id string) {
	e.t.Helper()
	n := e.nodes[id]
	n.mu.Lock()
	if n.stop != nil {
		n.mu.Unlock()
		e.t.Fatalf("%s is already connected", id)
	}
	server := grpc.NewServer(nodelink.ServerOptions()...)
	nodev1.RegisterNodeServer(server, n)
	ctx, cancel := context.WithCancel(e.t.Context())
	done := make(chan struct{})
	go func() {
		_ = nodelink.Serve(ctx, nodelink.DialConfig{
			Addr:       e.listeners.Node.Addr().String(),
			NodeID:     id,
			Token:      n.token,
			Log:        testLogger(),
			MinBackoff: 10 * time.Millisecond,
			MaxBackoff: 50 * time.Millisecond,
		}, server)
		close(done)
	}()
	n.stop = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			e.t.Errorf("%s did not stop", id)
		}
	}
	n.mu.Unlock()
	e.t.Cleanup(func() { e.down(id) })
	e.waitNodeSeen(id, true)
}

// down ends the node's link for good (until up). It is safe to repeat.
func (e *m2exitEnv) down(id string) {
	n := e.nodes[id]
	n.mu.Lock()
	stop := n.stop
	n.stop = nil
	n.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// waitNodeSeen waits until the node's inventory row is fresh (seen) or has
// been marked stale because the link is gone (not seen).
func (e *m2exitEnv) waitNodeSeen(id string, seen bool) {
	e.t.Helper()
	what := id + " inventory stale"
	if seen {
		what = id + " inventory fresh"
	}
	e.waitFor(m2exitWait, what, func() bool {
		node, err := e.store.Node(e.t.Context(), id)
		if err != nil {
			return false
		}
		if seen {
			return time.Since(node.LastSeenAt) < 2*time.Second
		}
		return time.Since(node.LastSeenAt) > time.Minute
	})
}

// fill takes all of the node's compute in the database, so placement and a
// resume see it as full.
func (e *m2exitEnv) fill(id string) {
	e.t.Helper()
	if _, err := e.store.Pool().Exec(e.t.Context(), `
		UPDATE nodes SET reserved_vcpus = total_vcpus, reserved_memory_mib = total_memory_mib
		WHERE id = $1`, id); err != nil {
		e.t.Fatalf("fill %s: %v", id, err)
	}
}

// m2exitCreated is the part of a create response the scenarios read.
type m2exitCreated struct {
	Sandbox struct {
		ID string `json:"id"`
	} `json:"sandbox"`
	Sandboxes []struct {
		ID string `json:"id"`
	} `json:"sandboxes"`
}

// create creates sandboxes as the owner of key and returns the response.
func (e *m2exitEnv) create(key, body string) m2exitCreated {
	e.t.Helper()
	response := e.do(http.MethodPost, "/api/v1/sandboxes", key, body, nil)
	if response.status != http.StatusAccepted {
		e.t.Fatalf("create %s status = %d body = %s", body, response.status, response.body)
	}
	runTestCheck(e.t, e.v1, "create", response)
	var created m2exitCreated
	if err := json.Unmarshal(response.body, &created); err != nil || created.Sandbox.ID == "" {
		e.t.Fatalf("create response has no readable sandbox: %v", err)
	}
	return created
}

// waitSandboxReady waits until the sandbox of the key's owner is ready.
func (e *m2exitEnv) waitSandboxReady(key, id string) {
	e.t.Helper()
	e.waitFor(m2exitWait, "sandbox "+id+" ready", func() bool {
		response := e.do(http.MethodGet, "/api/v1/sandboxes/"+id, key, "", nil)
		if response.status != http.StatusOK {
			return false
		}
		var got struct {
			Sandbox struct {
				State string  `json:"state"`
				Error *string `json:"error"`
			} `json:"sandbox"`
		}
		if err := json.Unmarshal(response.body, &got); err != nil {
			return false
		}
		if got.Sandbox.State == "error" {
			e.t.Fatalf("sandbox %s is in error: %v", id, got.Sandbox.Error)
		}
		if got.Sandbox.State != "ready" && got.Sandbox.State != "idle" {
			return false
		}
		runTestCheck(e.t, e.v1, "get", response)
		return true
	})
}

// writeFile writes the scenario file through the files API.
func (e *m2exitEnv) writeFile(key, id, content string) {
	e.t.Helper()
	body := `{"path":` + strconv.Quote(m2exitFilePath) + `,"content":` + strconv.Quote(content) + `}`
	response := e.do(http.MethodPut, "/api/v1/sandboxes/"+id+"/files", key, body, nil)
	if response.status != http.StatusOK {
		e.t.Fatalf("write %q status = %d body = %s", content, response.status, response.body)
	}
	runTestCheck(e.t, e.v1, "writeFile", response)
}

// tryReadFile reads the scenario file; ok is false for any non-200 answer.
func (e *m2exitEnv) tryReadFile(key, id string) (content string, ok bool) {
	e.t.Helper()
	response := e.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/files?path="+url.QueryEscape(m2exitFilePath), key, "", nil)
	if response.status != http.StatusOK {
		return "", false
	}
	runTestCheck(e.t, e.v1, "readFile", response)
	var body struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(response.body, &body); err != nil {
		return "", false
	}
	return body.Content, true
}

// readFile reads the scenario file and fails unless the read succeeds.
func (e *m2exitEnv) readFile(key, id string) string {
	e.t.Helper()
	content, ok := e.tryReadFile(key, id)
	if !ok {
		e.t.Fatalf("read %s of %s failed", m2exitFilePath, id)
	}
	return content
}

// waitFile waits until the scenario file of the sandbox reads as want.
func (e *m2exitEnv) waitFile(key, id, want string) {
	e.t.Helper()
	e.waitFor(m2exitWait, "file "+want+" in "+id, func() bool {
		content, ok := e.tryReadFile(key, id)
		return ok && content == want
	})
}

// row reads the sandbox row of the main account.
func (e *m2exitEnv) row(id string) db.Sandbox {
	e.t.Helper()
	row, err := e.store.Sandbox(e.t.Context(), e.account.ID, id)
	if err != nil {
		e.t.Fatalf("sandbox row %s: %v", id, err)
	}
	return row
}

// m2exitReadyState reports the ready-ish states of a sandbox row.
func m2exitReadyState(state db.SandboxState) bool {
	return state == db.StateReady || state == db.StateIdle || state == db.StateRunning
}

// waitGenerationAfter waits until the sandbox's latest generation was
// captured after at, so it holds everything written before at.
func (e *m2exitEnv) waitGenerationAfter(id string, at time.Time) p2snapSummary {
	e.t.Helper()
	e.waitFor(m2exitWait, "a generation captured after the write", func() bool {
		response := e.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/snapshots/latest", e.key, "", nil)
		if response.status != http.StatusOK {
			return false
		}
		var body struct {
			Snapshot *p2snapSummary `json:"snapshot"`
		}
		if err := json.Unmarshal(response.body, &body); err != nil || body.Snapshot == nil {
			return false
		}
		return body.Snapshot.CreatedAt.After(at)
	})
	latest := e.p2snapLatest(e.v1, id)
	if latest == nil {
		e.t.Fatal("latest snapshot disappeared")
	}
	return *latest
}

// listIDs returns the ids the API lists for the key's owner.
func (e *m2exitEnv) listIDs(key string) []string {
	e.t.Helper()
	response := e.do(http.MethodGet, "/api/v1/sandboxes", key, "", nil)
	if response.status != http.StatusOK {
		e.t.Fatalf("list status = %d body = %s", response.status, response.body)
	}
	runTestCheck(e.t, e.v1, "sandboxes", response)
	var body struct {
		Sandboxes []struct {
			ID string `json:"id"`
		} `json:"sandboxes"`
	}
	if err := json.Unmarshal(response.body, &body); err != nil {
		e.t.Fatalf("list: malformed body: %v", err)
	}
	ids := make([]string, 0, len(body.Sandboxes))
	for _, s := range body.Sandboxes {
		ids = append(ids, s.ID)
	}
	return ids
}

// expectOnlySandbox fails unless the API lists exactly the sandbox id.
func (e *m2exitEnv) expectOnlySandbox(id string) {
	e.t.Helper()
	if ids := e.listIDs(e.key); len(ids) != 1 || ids[0] != id {
		e.t.Fatalf("the API lists %v, want only %s", ids, id)
	}
}

// bootOnA connects node-a, creates a small sandbox that lands on it and
// writes content to the scenario file.
func (e *m2exitEnv) bootOnA(content string) string {
	e.t.Helper()
	e.up(m2exitNodeA)
	id := e.create(e.key, `{"type":"small"}`).Sandbox.ID
	e.waitSandboxReady(e.key, id)
	if got := derefString(e.row(id).NodeID); got != m2exitNodeA {
		e.t.Fatalf("the sandbox landed on %q, want %s", got, m2exitNodeA)
	}
	e.writeFile(e.key, id, content)
	return id
}

// saveSnapshot saves the sandbox under name and waits until it is ready.
func (e *m2exitEnv) saveSnapshot(id, name string) {
	e.t.Helper()
	body := `{"sandboxId":` + strconv.Quote(id) + `,"name":` + strconv.Quote(name) + `}`
	saved := e.do(http.MethodPost, "/api/v1/saved-snapshots", e.key, body, nil)
	if saved.status != http.StatusAccepted {
		e.t.Fatalf("save status = %d body = %s", saved.status, saved.body)
	}
	runTestCheck(e.t, e.v1, "saveSavedSnapshot", saved)
	e.waitFor(m2exitWait, "saved snapshot "+name+" ready", func() bool {
		response := e.do(http.MethodGet, "/api/v1/saved-snapshots/"+name, e.key, "", nil)
		if response.status != http.StatusOK {
			return false
		}
		var got struct {
			Snapshot struct {
				Status string `json:"status"`
				Error  string `json:"error"`
			} `json:"snapshot"`
		}
		if err := json.Unmarshal(response.body, &got); err != nil {
			return false
		}
		if got.Snapshot.Status == "failed" {
			e.t.Fatalf("saving %s failed: %s", name, got.Snapshot.Error)
		}
		if got.Snapshot.Status != "ready" {
			return false
		}
		runTestCheck(e.t, e.v1, "getSavedSnapshot", response)
		return true
	})
}

// m2exitErrorCode returns the code of an error response.
func m2exitErrorCode(t *testing.T, response runResponse) string {
	t.Helper()
	var body struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(response.body, &body); err != nil {
		t.Fatalf("error response is malformed: %v", err)
	}
	return body.Code
}

// TestRunPhase2NodeLoss: a sandbox on node-a with a completed generation,
// node-a gone past the grace. It is ready on node-b with the file; when
// node-a returns its copy is deleted and the API never shows a second
// sandbox.
func TestRunPhase2NodeLoss(t *testing.T) {
	m2exitEach(t, func(t *testing.T, store objstore.Store) {
		e := m2exitStart(t, store, m2exitNodeLostSeconds)
		a := e.nodes[m2exitNodeA]
		id := e.bootOnA("before")
		gen := e.waitGenerationAfter(id, time.Now())
		e.up(m2exitNodeB)

		e.down(m2exitNodeA)
		e.waitNodeSeen(m2exitNodeA, false)
		e.waitFor(m2exitWait, "the sandbox ready on node-b", func() bool {
			row := e.row(id)
			return m2exitReadyState(row.State) && derefString(row.NodeID) == m2exitNodeB
		})
		e.waitSandboxReady(e.key, id)
		e.waitFile(e.key, id, "before")
		if row := e.row(id); gen.ChainID == nil || row.ChainID.String() != *gen.ChainID {
			t.Fatalf("the sandbox continues chain %s, want %v", row.ChainID, gen.ChainID)
		}
		e.expectOnlySandbox(id)
		if !a.hasVM(t, id) {
			t.Fatal("node-a lost its copy while away (test premise)")
		}

		e.up(m2exitNodeA)
		e.waitFor(m2exitWait, "node-a's stale copy deleted", func() bool { return !a.hasVM(t, id) })
		e.expectOnlySandbox(id)
		if row := e.row(id); !m2exitReadyState(row.State) || derefString(row.NodeID) != m2exitNodeB {
			t.Fatalf("after node-a returned: %s on %q, want ready on %s", row.State, derefString(row.NodeID), m2exitNodeB)
		}
		if got := e.readFile(e.key, id); got != "before" {
			t.Fatalf("file after node-a returned = %q, want before", got)
		}
	})
}

// TestRunPhase2StopOnAResumeOnB: a sandbox stops on node-a with its final
// generation; node-a is full, so resume lands it on node-b with the file.
func TestRunPhase2StopOnAResumeOnB(t *testing.T) {
	m2exitEach(t, func(t *testing.T, store objstore.Store) {
		e := m2exitStart(t, store, 0)
		id := e.bootOnA("stopped on a")
		e.p2snapStop(e.v1, id)
		e.up(m2exitNodeB)
		e.fill(m2exitNodeA)

		resumed := e.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/resume", e.key, `{}`, nil)
		if resumed.status != http.StatusAccepted {
			t.Fatalf("resume status = %d body = %s", resumed.status, resumed.body)
		}
		runTestCheck(t, e.v1, "resume", resumed)
		e.waitSandboxReady(e.key, id)
		row := e.row(id)
		if derefString(row.NodeID) != m2exitNodeB || derefString(row.DiskNodeID) != m2exitNodeB {
			t.Fatalf("resumed on %q (disk %q), want %s", derefString(row.NodeID), derefString(row.DiskNodeID), m2exitNodeB)
		}
		e.waitFile(e.key, id, "stopped on a")
		e.expectOnlySandbox(id)
	})
}

// TestRunPhase2ForkAcrossNodes: a ready sandbox on node-a, node-a full: the
// fork is ready on node-b with the source's file, and the source stays ready
// on node-a, unchanged by later writes to the fork.
func TestRunPhase2ForkAcrossNodes(t *testing.T) {
	m2exitEach(t, func(t *testing.T, store objstore.Store) {
		e := m2exitStart(t, store, 0)
		source := e.bootOnA("fork me")
		e.up(m2exitNodeB)
		e.fill(m2exitNodeA)

		forked := e.do(http.MethodPost, "/api/v1/sandboxes/"+source+"/fork", e.key, "", nil)
		if forked.status != http.StatusAccepted {
			t.Fatalf("fork status = %d body = %s", forked.status, forked.body)
		}
		runTestCheck(t, e.v1, "fork", forked)
		var body struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(forked.body, &body); err != nil || body.ID == "" || body.ID == source {
			t.Fatalf("fork response id = %q (err %v), want a new sandbox id", body.ID, err)
		}
		fork := body.ID
		e.waitSandboxReady(e.key, fork)
		if got := derefString(e.row(fork).NodeID); got != m2exitNodeB {
			t.Fatalf("the fork is on %q, want %s", got, m2exitNodeB)
		}
		e.waitFile(e.key, fork, "fork me")

		srcRow := e.row(source)
		if !m2exitReadyState(srcRow.State) || derefString(srcRow.NodeID) != m2exitNodeA {
			t.Fatalf("the source is %s on %q, want ready on %s", srcRow.State, derefString(srcRow.NodeID), m2exitNodeA)
		}
		e.writeFile(e.key, fork, "fork only")
		if got := e.readFile(e.key, source); got != "fork me" {
			t.Fatalf("source file = %q after a write to the fork, want %q", got, "fork me")
		}
		if got := e.readFile(e.key, fork); got != "fork only" {
			t.Fatalf("fork file = %q, want %q", got, "fork only")
		}
	})
}

// TestRunPhase2CountAcrossNodes: a saved snapshot deployed ten times at once
// gives ten ready sandboxes spread over both nodes, each with the saved file.
func TestRunPhase2CountAcrossNodes(t *testing.T) {
	m2exitEach(t, func(t *testing.T, store objstore.Store) {
		e := m2exitStart(t, store, 0)
		source := e.bootOnA("saved bytes")
		e.up(m2exitNodeB)
		e.saveSnapshot(source, m2exitSavedName)

		const count = 10
		created := e.create(e.key, `{"from":`+strconv.Quote(m2exitSavedName)+`,"count":`+strconv.Itoa(count)+`}`)
		if len(created.Sandboxes) != count {
			t.Fatalf("the response lists %d sandboxes, want %d", len(created.Sandboxes), count)
		}
		if created.Sandboxes[0].ID != created.Sandbox.ID {
			t.Fatalf("sandbox = %s, want the first of sandboxes (%s)", created.Sandbox.ID, created.Sandboxes[0].ID)
		}
		perNode := map[string]int{}
		seen := map[string]bool{}
		for _, s := range created.Sandboxes {
			if seen[s.ID] {
				t.Fatalf("sandbox %s listed twice", s.ID)
			}
			seen[s.ID] = true
			e.waitSandboxReady(e.key, s.ID)
			perNode[derefString(e.row(s.ID).NodeID)]++
			e.waitFile(e.key, s.ID, "saved bytes")
		}
		for _, node := range []string{m2exitNodeA, m2exitNodeB} {
			if perNode[node] == 0 {
				t.Fatalf("no sandbox of the batch landed on %s: %v", node, perNode)
			}
		}
	})
}

// TestRunPhase2ShareCode: a second account deploys a saved snapshot from its
// share code and gets the file; after the code is revoked, a create from it is
// 404 snapshot_not_found while the sandbox already created keeps working.
func TestRunPhase2ShareCode(t *testing.T) {
	m2exitEach(t, func(t *testing.T, store objstore.Store) {
		e := m2exitStart(t, store, 0)
		source := e.bootOnA("shared bytes")
		e.up(m2exitNodeB)
		e.saveSnapshot(source, m2exitSavedName)

		shared := e.do(http.MethodPost, "/api/v1/saved-snapshots/"+m2exitSavedName+"/shares", e.key, "", nil)
		if shared.status != http.StatusCreated {
			t.Fatalf("share status = %d", shared.status)
		}
		runTestCheck(t, e.v1, "createSnapshotShare", shared)
		var share struct {
			ShareCode string `json:"shareCode"`
			Share     struct {
				ID string `json:"id"`
			} `json:"share"`
		}
		if err := json.Unmarshal(shared.body, &share); err != nil || !strings.HasPrefix(share.ShareCode, "bsh_") || share.Share.ID == "" {
			t.Fatalf("share response has no readable code and id: %v", err)
		}

		from := `{"from":` + strconv.Quote(share.ShareCode) + `}`
		id := e.create(e.otherKey, from).Sandbox.ID
		e.waitSandboxReady(e.otherKey, id)
		e.waitFile(e.otherKey, id, "shared bytes")
		if ids := e.listIDs(e.key); len(ids) != 1 || ids[0] != source {
			t.Fatalf("the sharer lists %v, want only %s", ids, source)
		}
		if ids := e.listIDs(e.otherKey); len(ids) != 1 || ids[0] != id {
			t.Fatalf("the second account lists %v, want only %s", ids, id)
		}

		revoked := e.do(http.MethodDelete, "/api/v1/saved-snapshots/"+m2exitSavedName+"/shares/"+share.Share.ID, e.key, "", nil)
		if revoked.status != http.StatusOK {
			t.Fatalf("revoke status = %d body = %s", revoked.status, revoked.body)
		}
		runTestCheck(t, e.v1, "revokeSnapshotShare", revoked)
		refused := e.do(http.MethodPost, "/api/v1/sandboxes", e.otherKey, from, nil)
		if refused.status != http.StatusNotFound {
			t.Fatalf("create from a revoked code status = %d body = %s, want 404", refused.status, refused.body)
		}
		runTestCheck(t, e.v1, "create", refused)
		if code := m2exitErrorCode(t, refused); code != apierr.CodeSnapshotNotFound {
			t.Fatalf("create from a revoked code = %q, want %s", code, apierr.CodeSnapshotNotFound)
		}
		if got := e.readFile(e.otherKey, id); got != "shared bytes" {
			t.Fatalf("the created sandbox reads %q after the revoke, want shared bytes", got)
		}
	})
}

// TestRunPhase2Recover: a ready sandbox whose guest does not answer is
// power-cycled once by recover (200 recovering), records sandbox.recovered and
// is ready again with its disk intact.
func TestRunPhase2Recover(t *testing.T) {
	m2exitEach(t, func(t *testing.T, store objstore.Store) {
		e := m2exitStart(t, store, 0)
		a := e.nodes[m2exitNodeA]
		id := e.bootOnA("survives the reset")

		a.unreachable.Store(true)
		recovered := e.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/recover", e.key, "", nil)
		if recovered.status != http.StatusOK {
			t.Fatalf("recover status = %d body = %s", recovered.status, recovered.body)
		}
		runTestCheck(t, e.v1, "recover", recovered)
		var body struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		}
		if err := json.Unmarshal(recovered.body, &body); err != nil || body.Type != "sandbox.recovering" || body.Status != "recovering" {
			t.Fatalf("recover response = %+v (err %v), want sandbox.recovering/recovering", body, err)
		}

		e.waitFor(m2exitWait, "sandbox.recovered", func() bool {
			events, err := e.store.SandboxEvents(t.Context(), e.account.ID, id, 100)
			if err != nil {
				return false
			}
			for _, ev := range events {
				switch ev.Type {
				case "sandbox.recover_failed":
					t.Fatalf("recover failed: %s", ev.Data)
				case "sandbox.recovered":
					return true
				}
			}
			return false
		})
		if n := a.resets.Load(); n != 1 {
			t.Fatalf("ResetVM calls = %d, want 1", n)
		}
		e.waitSandboxReady(e.key, id)
		e.waitFile(e.key, id, "survives the reset")
	})
}
