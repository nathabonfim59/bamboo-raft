package server

// Milestone 1 exit: the snapshot pipeline through the whole server. The real
// api, lifecycle and nodehub roles run against PostgreSQL and a nodefake node
// that dials through nodelink; the periodic snapshot worker and the stop's
// final generation write to one shared object store, and a second fake node
// restores from the newest manifest.
//
// The test needs PostgreSQL: it skips unless BBRAFT_TEST_DATABASE_URL names a
// database, like the other server integration tests. The memstore case always
// runs with it; the garage case runs the same scenario against a real S3
// bucket and skips unless BBRAFT_TEST_S3_GARAGE=1 and a garage binary is in
// PATH (BBRAFT_TEST_REQUIRE_ALL=1 turns either missing piece into a failure).

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/compat"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/s3"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/s3/s3test"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
	"github.com/nathabonfim59/bamboo-raft/internal/snapformat"
)

const (
	// p2snapFilePath is the guest file the scenario writes.
	p2snapFilePath = "/home/user/p2.txt"
	// p2snapWait bounds each wait in the scenario.
	p2snapWait = 30 * time.Second
)

// p2snapSummary is the snapshot object of the list and latest endpoints.
type p2snapSummary struct {
	ID               string    `json:"id"`
	SandboxID        string    `json:"sandboxId"`
	Status           string    `json:"status"`
	Kind             *string   `json:"kind"`
	Generation       *int64    `json:"generation"`
	ChainID          *string   `json:"chainId"`
	CreatedAt        time.Time `json:"createdAt"`
	SizeBytes        *int64    `json:"sizeBytes"`
	FileCount        *int64    `json:"fileCount"`
	ContentSizeBytes *int64    `json:"contentSizeBytes"`
	ContentFileCount *int64    `json:"contentFileCount"`
	Origin           string    `json:"origin"`
}

// p2snapGeneration is the generation number of s, failing the test when the
// API left it null.
func p2snapGeneration(t *testing.T, s p2snapSummary) int64 {
	t.Helper()
	if s.Generation == nil {
		t.Fatalf("snapshot %s has a null generation", s.ID)
	}
	return *s.Generation
}

// p2snapStart runs the three server roles against store with the snapshot
// worker ticking every second, and requires a clean shutdown at cleanup.
func (e *runEnv) p2snapStart(store objstore.Store) {
	e.t.Helper()
	cfg := *e.cfg
	cfg.Lifecycle.SnapshotIntervalSeconds = 1
	cfg.Lifecycle.SnapshotTimeoutSeconds = 30
	options := e.options()
	options.ObjectStore = store
	ctx, cancel := context.WithCancel(e.t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, &cfg, options) }()
	var once sync.Once
	e.t.Cleanup(func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					e.t.Errorf("Run = %v", err)
				}
			case <-time.After(15 * time.Second):
				e.t.Error("Run did not stop")
			}
		})
	})
}

// p2snapWrite writes content to the scenario file through the files API.
func (e *runEnv) p2snapWrite(v1 *compat.Validator, id, content string) {
	e.t.Helper()
	body := `{"path":` + strconv.Quote(p2snapFilePath) + `,"content":` + strconv.Quote(content) + `}`
	response := e.do(http.MethodPut, "/api/v1/sandboxes/"+id+"/files", e.key, body, nil)
	if response.status != http.StatusOK {
		e.t.Fatalf("write %q status = %d body = %s", content, response.status, response.body)
	}
	runTestCheck(e.t, v1, "writeFile", response)
}

// p2snapList reads one snapshot listing and validates it against the v1
// document under operationID.
func (e *runEnv) p2snapList(v1 *compat.Validator, operationID, path string) []p2snapSummary {
	e.t.Helper()
	response := e.do(http.MethodGet, path, e.key, "", nil)
	if response.status != http.StatusOK {
		e.t.Fatalf("GET %s status = %d body = %s", path, response.status, response.body)
	}
	runTestCheck(e.t, v1, operationID, response)
	var body struct {
		Type      string          `json:"type"`
		Snapshots []p2snapSummary `json:"snapshots"`
	}
	if err := json.Unmarshal(response.body, &body); err != nil {
		e.t.Fatalf("GET %s: malformed body: %v", path, err)
	}
	return body.Snapshots
}

// p2snapLatest reads GET /snapshots/latest; nil is the explicit null.
func (e *runEnv) p2snapLatest(v1 *compat.Validator, id string) *p2snapSummary {
	e.t.Helper()
	response := e.do(http.MethodGet, "/api/v1/sandboxes/"+id+"/snapshots/latest", e.key, "", nil)
	if response.status != http.StatusOK {
		e.t.Fatalf("latest status = %d body = %s", response.status, response.body)
	}
	runTestCheck(e.t, v1, "getLatestSandboxSnapshot", response)
	var body struct {
		Snapshot *p2snapSummary `json:"snapshot"`
	}
	if err := json.Unmarshal(response.body, &body); err != nil {
		e.t.Fatalf("latest: malformed body: %v", err)
	}
	return body.Snapshot
}

// p2snapStop stops the sandbox and waits until it is archived.
func (e *runEnv) p2snapStop(v1 *compat.Validator, id string) {
	e.t.Helper()
	response := e.do(http.MethodPost, "/api/v1/sandboxes/"+id+"/stop", e.key, `{}`, nil)
	if response.status != http.StatusAccepted {
		e.t.Fatalf("stop status = %d body = %s", response.status, response.body)
	}
	runTestCheck(e.t, v1, "stop", response)
	e.lifecycleRunWaitState(id, "archived", p2snapWait)
}

// p2snapKeys lists the object keys under prefix.
func p2snapKeys(t *testing.T, store objstore.Store, prefix string) []string {
	t.Helper()
	var keys []string
	for info, err := range store.List(t.Context(), prefix) {
		if err != nil {
			t.Fatalf("list %q: %v", prefix, err)
		}
		keys = append(keys, info.Key)
	}
	return keys
}

// TestRunPhase2SnapshotsMilestone1Exit is the Milestone 1 exit scenario, run
// once per object store: periodic generations, the final generation of a
// stop, the listing endpoints, a restore on a second node and a sandbox with
// snapshots off.
func TestRunPhase2SnapshotsMilestone1Exit(t *testing.T) {
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
			p2snapScenario(t, tc.open(t))
		})
	}
}

func p2snapScenario(t *testing.T, store objstore.Store) {
	env := newRunEnv(t)
	v1, _ := runTestValidators(t)
	env.p2snapStart(store)
	env.startNode(0)
	env.node.Store = store
	env.waitNodeFresh()
	env.waitAPI()

	// 2. A sandbox with a file.
	id := env.createSandbox(`{"type":"small"}`)
	env.waitReady(id)
	env.p2snapWrite(v1, id, "gen-A")

	// 3. The worker takes a generation within a few intervals; the first one
	// is the base.
	listPath := "/api/v1/sandboxes/" + id + "/snapshots?limit=200&sort=asc"
	var periodic []p2snapSummary
	env.waitFor(15*time.Second, "a periodic generation", func() bool {
		response := env.do(http.MethodGet, listPath, env.key, "", nil)
		if response.status != http.StatusOK {
			return false
		}
		var body struct {
			Snapshots []p2snapSummary `json:"snapshots"`
		}
		if err := json.Unmarshal(response.body, &body); err != nil {
			return false
		}
		periodic = body.Snapshots
		return len(periodic) > 0
	})
	periodic = env.p2snapList(v1, "listSandboxSnapshots", listPath)
	if len(periodic) == 0 {
		t.Fatal("the generation disappeared from the listing")
	}
	base := periodic[0]
	if base.Generation == nil || *base.Generation != 0 {
		t.Fatalf("first generation = %v, want 0", base.Generation)
	}
	if base.Kind == nil || *base.Kind != "base" {
		t.Fatalf("generation 0 kind = %v, want base", base.Kind)
	}
	if base.SizeBytes == nil || *base.SizeBytes <= 0 {
		t.Fatalf("generation 0 sizeBytes = %v, want > 0", base.SizeBytes)
	}
	if base.FileCount != nil {
		t.Fatalf("generation 0 fileCount = %d, want null", *base.FileCount)
	}
	if base.Origin != "captured" || base.Status != "completed" || base.SandboxID != id {
		t.Fatalf("generation 0 = %+v, want a completed captured snapshot of %s", base, id)
	}
	if base.ChainID == nil || !snapformat.ValidChainID(*base.ChainID) {
		t.Fatalf("generation 0 chainId = %v, want a canonical chain id", base.ChainID)
	}
	chainID := *base.ChainID
	var earlier int64
	for _, s := range periodic {
		earlier = max(earlier, p2snapGeneration(t, s))
	}

	// 4. Overwrite the file, then stop: the stop's final generation is cut
	// after this write.
	env.p2snapWrite(v1, id, "gen-B")
	wroteAt := time.Now().Truncate(time.Millisecond)
	env.p2snapStop(v1, id)

	// 5. The newest generation follows every earlier one and was cut after
	// the second write.
	latest := env.p2snapLatest(v1, id)
	if latest == nil {
		t.Fatal("latest snapshot is null after a stop")
	}
	latestGeneration := p2snapGeneration(t, *latest)
	if latestGeneration <= earlier {
		t.Fatalf("latest generation = %d, want greater than the earlier %d", latestGeneration, earlier)
	}
	if latest.CreatedAt.Before(wroteAt) {
		t.Fatalf("latest createdAt = %s, want after the gen-B write at %s", latest.CreatedAt, wroteAt)
	}
	if latest.ChainID == nil || *latest.ChainID != chainID {
		t.Fatalf("latest chainId = %v, want %s", latest.ChainID, chainID)
	}

	// 6. A second node restores that manifest and reads the final bytes.
	source, err := env.node.GetVM(t.Context(), &nodev1.GetVMRequest{SandboxId: id})
	if err != nil {
		t.Fatalf("GetVM on node-a: %v", err)
	}
	nodeBRoot := t.TempDir()
	nodeB := nodefake.New("node-b", nodeBRoot)
	nodeB.Store = store
	manifestKey := snapformat.ManifestKey(chainID, uint32(latestGeneration))
	restored, err := nodeB.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId:    id,
		Resources:    source.GetResources(),
		FromManifest: manifestKey,
	})
	if err != nil {
		t.Fatalf("CreateVM from %s on node-b: %v", manifestKey, err)
	}
	if restored.GetState() != nodev1.VMState_VM_STATE_READY {
		t.Fatalf("restored VM state = %s, want READY", restored.GetState())
	}
	data, err := os.ReadFile(filepath.Join(nodeBRoot, "vms", id, "home/user", "p2.txt"))
	if err != nil {
		t.Fatalf("read the restored file: %v", err)
	}
	if string(data) != "gen-B" {
		t.Fatalf("restored file = %q, want %q", data, "gen-B")
	}

	// 7. The account listing holds the sandbox's rows newest first, and
	// sort=asc reverses it exactly.
	desc := env.p2snapList(v1, "listSnapshots", "/api/v1/snapshots?limit=200")
	asc := env.p2snapList(v1, "listSnapshots", "/api/v1/snapshots?limit=200&sort=asc")
	if len(desc) < 2 {
		t.Fatalf("account listing has %d rows, want at least the base and the final generation", len(desc))
	}
	if desc[0].ID != latest.ID {
		t.Fatalf("newest listed row = %s (generation %d), want the latest %s",
			desc[0].ID, p2snapGeneration(t, desc[0]), latest.ID)
	}
	for i := 1; i < len(desc); i++ {
		if desc[i].SandboxID != id {
			t.Fatalf("row %s belongs to %s, want %s", desc[i].ID, desc[i].SandboxID, id)
		}
		if desc[i].CreatedAt.After(desc[i-1].CreatedAt) || p2snapGeneration(t, desc[i]) >= p2snapGeneration(t, desc[i-1]) {
			t.Fatalf("listing is not newest first at row %d: generation %d createdAt %s after generation %d createdAt %s",
				i, p2snapGeneration(t, desc[i]), desc[i].CreatedAt, p2snapGeneration(t, desc[i-1]), desc[i-1].CreatedAt)
		}
	}
	reversed := slices.Clone(desc)
	slices.Reverse(reversed)
	if len(asc) != len(reversed) {
		t.Fatalf("sort=asc returned %d rows, want %d", len(asc), len(reversed))
	}
	for i := range asc {
		if asc[i].ID != reversed[i].ID {
			t.Fatalf("sort=asc row %d = %s, want %s", i, asc[i].ID, reversed[i].ID)
		}
	}

	// 8. A sandbox with snapshots off writes no manifest, and its stop erases
	// the disk, so resume is refused.
	account, err := env.store.EnsureAccount(t.Context(), env.owner)
	if err != nil {
		t.Fatalf("owner account: %v", err)
	}
	offID := env.createSandbox(`{"type":"small","snapshots":false}`)
	env.waitReady(offID)
	env.p2snapWrite(v1, offID, "no-snapshots")
	offRow, err := env.store.Sandbox(t.Context(), account.ID, offID)
	if err != nil {
		t.Fatalf("read the snapshots-off sandbox: %v", err)
	}
	if offRow.SnapshotsEnabled {
		t.Fatal("sandbox created with snapshots:false has snapshots enabled")
	}
	env.p2snapStop(v1, offID)
	prefix := snapformat.ManifestPrefix + offRow.ChainID.String() + "/"
	if keys := p2snapKeys(t, store, prefix); len(keys) != 0 {
		t.Fatalf("snapshots-off sandbox left manifests behind: %v", keys)
	}
	if rows := env.p2snapList(v1, "listSandboxSnapshots", "/api/v1/sandboxes/"+offID+"/snapshots"); len(rows) != 0 {
		t.Fatalf("snapshots-off sandbox lists %d snapshots", len(rows))
	}
	if got := env.p2snapLatest(v1, offID); got != nil {
		t.Fatalf("snapshots-off sandbox latest = %+v, want null", got)
	}
	resumed := env.do(http.MethodPost, "/api/v1/sandboxes/"+offID+"/resume", env.key, `{}`, nil)
	if resumed.status != http.StatusConflict {
		t.Fatalf("resume status = %d body = %s, want 409", resumed.status, resumed.body)
	}
	runTestCheck(t, v1, "resume", resumed)
	var refusal struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(resumed.body, &refusal); err != nil || refusal.Code != apierr.CodeSnapshotsDisabled {
		t.Fatalf("resume code = %q (err %v), want %s", refusal.Code, err, apierr.CodeSnapshotsDisabled)
	}
}
