package api

// PostgreSQL integration tests for the SSH key endpoint. They need
// BBRAFT_TEST_DATABASE_URL and skip without it (dbtest.Open). Each test gets
// its own schema; the second store in the lifetime test points at the same
// schema to model a server restart.

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/apierr"
	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// sshPGAccount creates one account and returns its id.
func sshPGAccount(t *testing.T, store *db.Store) uuid.UUID {
	t.Helper()
	account, err := store.EnsureAccount(t.Context(), "ssh-"+uuid.NewV7().String())
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}
	return account.ID
}

// sshPGNode registers one node with room for several sandboxes.
func sshPGNode(t *testing.T, store *db.Store, id string) {
	t.Helper()
	err := store.UpsertNodeInfo(t.Context(), db.NodeInfo{
		ID: id, CPUModel: "test-cpu", QEMUVersion: "9.2", DefaultImage: "img",
		Capacity:    db.Resources{VCPUs: 64, MemoryMiB: 64 << 10},
		DiskFreeGiB: 1000,
		SeenAt:      time.Now(),
	})
	if err != nil {
		t.Fatalf("UpsertNodeInfo: %v", err)
	}
}

// sshPGInsert writes one live sandbox fixture row in state with nodeID.
func sshPGInsert(t *testing.T, store *db.Store, owner uuid.UUID, id string, state db.SandboxState, nodeID string) {
	t.Helper()
	vcpus, memory, disk := int64(4), int64(8192), int64(51)
	if state == db.StateArchived {
		vcpus, memory = 0, 0
	}
	_, err := store.Pool().Exec(t.Context(), `
		INSERT INTO sandboxes (
			id, owner_id, state, machine_type, name, labels, node_id,
			reserved_vcpus, reserved_memory_mib, reserved_disk_gib,
			desired_vcpus, desired_memory_mib, desired_disk_gib,
			ttl_seconds, created_at, updated_at
		) VALUES ($1, $2, $3, 'default', 'ssh-fixture', '{}'::jsonb, $4,
			$5, $6, $7, 4, 8192, 51, 3600, now(), now())`,
		id, owner, string(state), nodeID, vcpus, memory, disk)
	if err != nil {
		t.Fatalf("insert sandbox %s: %v", id, err)
	}
}

// sshPGReopen opens a second store over the same schema, like a restarted
// server with the same database.
func sshPGReopen(t *testing.T, store *db.Store) *db.Store {
	t.Helper()
	cfg := dbtest.PoolConfig(t)
	var schema string
	if err := store.Pool().QueryRow(t.Context(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("current schema: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	second, err := db.OpenPool(t.Context(), cfg, db.Secrets{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(second.Close)
	return second
}

func sshPGHandler(t *testing.T, store *db.Store, node nodev1.NodeClient) http.Handler {
	t.Helper()
	return sshTestNewHandler(t, store, sshTestFactory(node, nil), nil)
}

func sshPGPost(t *testing.T, handler http.Handler, owner uuid.UUID, id, key string) *httptest.ResponseRecorder {
	t.Helper()
	return sshTestServe(t, handler, sshTestRequest(t, owner.String(), id, sshTestBody(t, key)))
}

func TestSSHKeyPostgresPersistAcrossStoreReopen(t *testing.T) {
	store := dbtest.Open(t)
	owner := sshPGAccount(t, store)
	node := "ssh-db-node"
	sshPGNode(t, store, node)
	id := ids.Sandbox()
	sshPGInsert(t, store, owner, id, db.StateReady, node)

	before, err := store.Sandbox(t.Context(), owner, id)
	if err != nil {
		t.Fatalf("Sandbox before: %v", err)
	}
	access := sshTestAccess(0xbb, "203.0.113.10", 22001)
	handler := sshPGHandler(t, store, newSSHTestNode(access))
	recorder := sshPGPost(t, handler, owner, id, sshTestKeyLine(0xaa, "user@example"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", recorder.Code, recorder.Body.Bytes())
	}

	stored, err := store.Sandbox(t.Context(), owner, id)
	if err != nil {
		t.Fatalf("Sandbox after: %v", err)
	}
	if stored.SSH == nil || stored.SSH.Endpoint != "203.0.113.10:22001" || stored.SSH.HostKey != access.HostKey {
		t.Fatalf("stored SSH = %+v", stored.SSH)
	}
	if stored.Version != before.Version || stored.LifecycleGeneration != before.LifecycleGeneration {
		t.Errorf("metadata write changed version/generation: %d/%d, want %d/%d",
			stored.Version, stored.LifecycleGeneration, before.Version, before.LifecycleGeneration)
	}

	// A fresh store over the same schema sees the metadata: persistence is
	// in Postgres, not process memory.
	restarted, err := sshPGReopen(t, store).Sandbox(t.Context(), owner, id)
	if err != nil {
		t.Fatalf("Sandbox after reopen: %v", err)
	}
	if restarted.SSH == nil || *restarted.SSH != *stored.SSH {
		t.Errorf("SSH after reopen = %+v, want %+v", restarted.SSH, stored.SSH)
	}
}

func TestSSHKeyPostgresStopResumeFencing(t *testing.T) {
	store := dbtest.Open(t)
	owner := sshPGAccount(t, store)
	node := "ssh-db-resume-node"
	sshPGNode(t, store, node)
	id := ids.Sandbox()
	sshPGInsert(t, store, owner, id, db.StateReady, node)
	key := sshTestKeyLine(0xaa, "user@example")

	// First successful call stores boot-zero metadata.
	first := sshTestAccess(0xbb, "203.0.113.10", 22001)
	handler := sshPGHandler(t, store, newSSHTestNode(first))
	if recorder := sshPGPost(t, handler, owner, id, key); recorder.Code != http.StatusOK {
		t.Fatalf("first status = %d; body: %s", recorder.Code, recorder.Body.Bytes())
	}

	// A second key call is in flight when the stop lands. Its lookup saw the
	// ready row; its AddSSHKey answers with old-boot metadata after the stop
	// cleared the pair. The metadata write must be fenced out.
	oldBoot := newSSHTestBlockingNode(sshTestAccess(0xcc, "203.0.113.11", 22002))
	handler = sshPGHandler(t, store, oldBoot)
	blocked := sshTestBlockCall(t, handler,
		sshTestRequest(t, owner.String(), id, sshTestBody(t, key)), oldBoot, nil)
	blocked.waitStarted(t)

	op, archiving, _, created, err := store.AcceptSandboxStop(t.Context(), owner, id, false)
	if err != nil || !created {
		t.Fatalf("AcceptSandboxStop: created=%v err=%v", created, err)
	}
	if archiving.State != db.StateArchiving || archiving.SSH != nil {
		t.Fatalf("archiving row = %+v", archiving)
	}
	blocked.finish(t)

	sshTestAssertError(t, blocked.recorder, http.StatusBadRequest, apierr.CodeMachineNotRunning)
	fenced, err := store.Sandbox(t.Context(), owner, id)
	if err != nil {
		t.Fatalf("Sandbox after fence: %v", err)
	}
	if fenced.SSH != nil {
		t.Fatalf("late old-boot response restored metadata: %+v", fenced.SSH)
	}

	// This test drives the DB lease/finalize methods directly; the public
	// HTTP worker flow belongs to the next wiring slice.
	stopJob := sshPGClaim(t, store, db.JobStop)
	stopped, _, err := store.FinalizeSandboxStop(t.Context(), stopJob.ID, "ssh-db-worker", stopJob.LeaseToken,
		db.FinalizeStopParams{Owner: owner, SandboxID: id, StopID: op.ID, ExpectedVersion: archiving.Version})
	if err != nil {
		t.Fatalf("FinalizeSandboxStop: %v", err)
	}
	if stopped.State != db.StateArchived || stopped.SSH != nil {
		t.Fatalf("stopped row = %+v", stopped)
	}
	if err := store.CompleteJob(t.Context(), stopJob.ID, "ssh-db-worker", stopJob.LeaseToken); err != nil {
		t.Fatalf("CompleteJob stop: %v", err)
	}

	// Resume acceptance bumps the generation and must leave the pair absent.
	resumed, err := store.AcceptSandboxResume(t.Context(), db.ResumeParams{Owner: owner, SandboxID: id})
	if err != nil {
		t.Fatalf("AcceptSandboxResume: %v", err)
	}
	if resumed.Sandbox.State != db.StateProvisioning || resumed.Sandbox.SSH != nil {
		t.Fatalf("resumed row = %+v", resumed.Sandbox)
	}
	if resumed.Sandbox.LifecycleGeneration != archiving.LifecycleGeneration+1 {
		t.Fatalf("resume generation = %d, want %d",
			resumed.Sandbox.LifecycleGeneration, archiving.LifecycleGeneration+1)
	}
	// sshKey while the resumed VM is still provisioning is machine_not_running.
	provisioningHandler := sshPGHandler(t, store, newSSHTestNode(sshTestAccess(0xdd, "203.0.113.12", 22003)))
	if recorder := sshPGPost(t, provisioningHandler, owner, id, key); recorder.Code != http.StatusBadRequest {
		t.Fatalf("provisioning status = %d; body: %s", recorder.Code, recorder.Body.Bytes())
	}

	resumeJob := sshPGClaim(t, store, db.JobResume)
	ready, err := store.FinalizeSandboxResume(t.Context(), resumeJob.ID, "ssh-db-worker", resumeJob.LeaseToken,
		db.FinalizeResumeParams{Owner: owner, SandboxID: id, Generation: resumed.Sandbox.LifecycleGeneration,
			Version: resumed.Sandbox.Version})
	if err != nil {
		t.Fatalf("FinalizeSandboxResume: %v", err)
	}
	if ready.State != db.StateReady || ready.SSH != nil {
		t.Fatalf("ready row = %+v", ready)
	}
	if err := store.CompleteJob(t.Context(), resumeJob.ID, "ssh-db-worker", resumeJob.LeaseToken); err != nil {
		t.Fatalf("CompleteJob resume: %v", err)
	}

	// The next call runs against the new boot and refreshes the pair.
	refreshed := sshTestAccess(0xee, "198.51.100.9", 22010)
	handler = sshPGHandler(t, store, newSSHTestNode(refreshed))
	if recorder := sshPGPost(t, handler, owner, id, key); recorder.Code != http.StatusOK {
		t.Fatalf("refresh status = %d; body: %s", recorder.Code, recorder.Body.Bytes())
	}
	final, err := store.Sandbox(t.Context(), owner, id)
	if err != nil {
		t.Fatalf("Sandbox after refresh: %v", err)
	}
	if final.SSH == nil || final.SSH.Endpoint != "198.51.100.9:22010" || final.SSH.HostKey != refreshed.HostKey {
		t.Fatalf("refreshed SSH = %+v", final.SSH)
	}
}

// sshPGClaim claims the next job and requires want kind.
func sshPGClaim(t *testing.T, store *db.Store, want db.JobKind) db.Job {
	t.Helper()
	job, err := store.ClaimJob(t.Context(), "ssh-db-worker", time.Minute)
	if err != nil {
		t.Fatalf("ClaimJob: %v", err)
	}
	if job.Kind != want {
		t.Fatalf("job kind = %q, want %q", job.Kind, want)
	}
	return job
}

func TestSSHKeyPostgresPatchDuringRPC(t *testing.T) {
	store := dbtest.Open(t)
	owner := sshPGAccount(t, store)
	node := "ssh-db-patch-node"
	sshPGNode(t, store, node)
	id := ids.Sandbox()
	sshPGInsert(t, store, owner, id, db.StateReady, node)
	key := sshTestKeyLine(0xaa, "user@example")

	blocking := newSSHTestBlockingNode(sshTestAccess(0xbb, "203.0.113.10", 22001))
	handler := sshPGHandler(t, store, blocking)
	blocked := sshTestBlockCall(t, handler,
		sshTestRequest(t, owner.String(), id, sshTestBody(t, key)), blocking, nil)
	blocked.waitStarted(t)

	// PATCH advances the row version between lookup and metadata write. It
	// does not change the boot, so the same node and generation still win.
	name := "patched while sshkey ran"
	if _, err := store.PatchSandbox(t.Context(), db.PatchSandboxParams{
		Owner: owner, SandboxID: id, Name: &name,
	}); err != nil {
		t.Fatalf("PatchSandbox: %v", err)
	}
	blocked.finish(t)

	if blocked.recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", blocked.recorder.Code, blocked.recorder.Body.Bytes())
	}
	row, err := store.Sandbox(t.Context(), owner, id)
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	if row.Name != name || row.SSH == nil || row.SSH.Endpoint != "203.0.113.10:22001" {
		t.Fatalf("row after patched RPC = %+v", row)
	}
}

func TestSSHKeyPostgresOwnerIsolation(t *testing.T) {
	store := dbtest.Open(t)
	owner := sshPGAccount(t, store)
	other := sshPGAccount(t, store)
	node := "ssh-db-owner-node"
	sshPGNode(t, store, node)
	id := ids.Sandbox()
	sshPGInsert(t, store, owner, id, db.StateReady, node)

	nodeClient := newSSHTestNode(sshTestAccess(0xbb, "203.0.113.10", 22001))
	handler := sshPGHandler(t, store, nodeClient)
	recorder := sshPGPost(t, handler, other, id, sshTestKeyLine(0xaa, "user@example"))
	sshTestAssertError(t, recorder, http.StatusNotFound, apierr.CodeNotFound)
	if nodeClient.callCount() != 0 {
		t.Errorf("foreign owner reached AddSSHKey")
	}
	row, err := store.Sandbox(t.Context(), owner, id)
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	if row.SSH != nil {
		t.Errorf("foreign request wrote metadata: %+v", row.SSH)
	}
}
