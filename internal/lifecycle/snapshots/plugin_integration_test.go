package snapshots

// End-to-end save/delete through the real lifecycle worker: the plugin's
// frozen handlers are exercised with the accepted PostgreSQL store and a
// nodefake node over gRPC. Requires BBRAFT_TEST_DATABASE_URL (skips
// otherwise).

import (
	"context"
	"encoding/json/v2"
	"io"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func pluginStoreReadySource(t *testing.T, store *db.Store, owner uuid.UUID, nodeID string) db.Sandbox {
	t.Helper()
	if err := store.UpsertNodeInfo(t.Context(), db.NodeInfo{
		ID: nodeID, CPUModel: "fake", QEMUVersion: "none",
		Capacity:    db.Resources{VCPUs: 8, MemoryMiB: 8192},
		DiskFreeGiB: 200,
		SeenAt:      time.Now(),
	}); err != nil {
		t.Fatalf("node info: %v", err)
	}
	res, err := store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: owner, MachineType: "small", Name: "source",
		NodeID: nodeID, Reserve: db.Resources{VCPUs: 2, MemoryMiB: 4096, DiskGiB: 51},
	})
	if err != nil {
		t.Fatalf("create source: %v", err)
	}
	if _, err := store.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET state = 'ready' WHERE id = $1`, res.Sandbox.ID); err != nil {
		t.Fatalf("mark source ready: %v", err)
	}
	if _, err := store.Pool().Exec(t.Context(), `
		UPDATE jobs SET state = 'done' WHERE sandbox_id = $1 AND kind = 'sandbox.provision'`,
		res.Sandbox.ID); err != nil {
		t.Fatalf("finish provision job: %v", err)
	}
	sbx, err := store.SandboxAny(t.Context(), owner, res.Sandbox.ID)
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	return sbx
}

func pluginWorker(t *testing.T, store *db.Store, client lifecycle.NodeClient) *lifecycle.Worker {
	t.Helper()
	plugin, err := New(Options{Store: store, Dial: dialOne(client, nil), Logger: pluginQuiet()})
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	worker, err := lifecycle.New(lifecycle.Options{
		Store: store, Dial: dialOne(client, nil), Logger: pluginQuiet(),
		Lease: 300 * time.Millisecond, BootTimeout: 5 * time.Second,
		RetryBase: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond,
		Handlers: plugin.Handlers(),
	})
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	return worker
}

func TestPluginSaveThenDeleteThroughWorker(t *testing.T) {
	store := dbtest.Open(t)
	account, err := store.EnsureAccount(t.Context(), "acct-"+uuid.NewV7().String())
	if err != nil {
		t.Fatalf("account: %v", err)
	}
	fake, client := pluginNode(t, "node-a")
	source := pluginStoreReadySource(t, store, account.ID, "node-a")
	if _, err := fake.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: source.ID, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 4096, DiskGib: 51},
	}); err != nil {
		t.Fatalf("create source vm: %v", err)
	}

	save, err := store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: account.ID, SandboxID: source.ID, Name: "base",
	})
	if err != nil {
		t.Fatalf("accept save: %v", err)
	}
	worker := pluginWorker(t, store, client)
	if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
		t.Fatalf("save job: worked=%v err=%v", worked, err)
	}
	ready, err := store.SavedSnapshot(t.Context(), account.ID, save.ID)
	if err != nil {
		t.Fatalf("read saved: %v", err)
	}
	if ready.Status != db.SnapshotReady || ready.SizeBytes == nil || *ready.SizeBytes < 0 {
		t.Fatalf("saved row status=%s size_present=%v", ready.Status, ready.SizeBytes != nil)
	}

	if _, err := store.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: account.ID, Name: "base",
	}); err != nil {
		t.Fatalf("accept delete: %v", err)
	}
	if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
		t.Fatalf("delete job: worked=%v err=%v", worked, err)
	}
	purged, err := store.SavedSnapshot(t.Context(), account.ID, save.ID)
	if err != nil {
		t.Fatalf("read purged: %v", err)
	}
	if purged.PurgedAt == nil {
		t.Fatalf("artifact not purged after confirmed delete: status=%s", purged.Status)
	}
}

// unsafeRowStore returns a tampered canonical row for one generation while
// delegating every other operation to the real store. The database CHECK can
// never store such a row; this models a future/driver fault and proves the
// handler rejects it before any node RPC.
type unsafeRowStore struct {
	*db.Store
	id  uuid.UUID
	row db.SavedSnapshot
}

func (s *unsafeRowStore) SavedSnapshot(ctx context.Context, owner uuid.UUID, id uuid.UUID) (db.SavedSnapshot, error) {
	if id == s.id {
		return s.row, nil
	}
	return s.Store.SavedSnapshot(ctx, owner, id)
}

// trackingDial records whether the handler attempted a node connection.
type trackingDial struct {
	client lifecycle.NodeClient
	calls  int
}

func (d *trackingDial) Dial(string) (lifecycle.NodeClient, io.Closer, error) {
	d.calls++
	return d.client, nil, nil
}

func TestPluginRejectsUnsafeArtifactBeforeNodeRPC(t *testing.T) {
	store := dbtest.Open(t)
	account, err := store.EnsureAccount(t.Context(), "acct-"+uuid.NewV7().String())
	if err != nil {
		t.Fatalf("account: %v", err)
	}
	fake, client := pluginNode(t, "node-a")
	source := pluginStoreReadySource(t, store, account.ID, "node-a")
	if _, err := fake.CreateVM(t.Context(), &nodev1.CreateVMRequest{
		SandboxId: source.ID, Resources: &nodev1.Resources{Vcpus: 2, MemoryMib: 4096, DiskGib: 51},
	}); err != nil {
		t.Fatalf("create source vm: %v", err)
	}

	save, err := store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: account.ID, SandboxID: source.ID, Name: "base",
	})
	if err != nil {
		t.Fatalf("accept save: %v", err)
	}
	// Tamper both the job payload copy and the canonical row to the same
	// unsafe id. The DB row itself keeps a valid id; the wrapper supplies
	// the unsafe pair to the handler.
	var raw string
	if err := store.Pool().QueryRow(t.Context(), `
		SELECT payload::text FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'
		ORDER BY created_at DESC LIMIT 1`, account.ID, save.ID).Scan(&raw); err != nil {
		t.Fatalf("save payload: %v", err)
	}
	var savePayload db.SaveSnapshotPayload
	if err := json.Unmarshal([]byte(raw), &savePayload); err != nil {
		t.Fatalf("decode save payload: %v", err)
	}
	savePayload.ArtifactID = "not-a-snapshot"
	unsafePayload, err := json.Marshal(savePayload)
	if err != nil {
		t.Fatalf("encode save payload: %v", err)
	}
	if _, err := store.Pool().Exec(t.Context(), `
		UPDATE jobs SET payload = $3::jsonb
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'`,
		account.ID, save.ID, unsafePayload); err != nil {
		t.Fatalf("tamper save payload: %v", err)
	}
	unsafeRow := save
	unsafeRow.ArtifactID = savePayload.ArtifactID
	wrapped := &unsafeRowStore{Store: store, id: save.ID, row: unsafeRow}

	tracked := &trackingDial{client: client}
	plugin, err := New(Options{Store: wrapped, Dial: tracked.Dial, Logger: pluginQuiet()})
	if err != nil {
		t.Fatalf("plugin: %v", err)
	}
	worker, err := lifecycle.New(lifecycle.Options{
		Store: wrapped, Dial: tracked.Dial, Logger: pluginQuiet(),
		Lease: 300 * time.Millisecond, RetryBase: 10 * time.Millisecond,
		MaxBackoff: 100 * time.Millisecond, Handlers: plugin.Handlers(),
	})
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
		t.Fatalf("unsafe save job: worked=%v err=%v", worked, err)
	}
	if tracked.calls != 0 {
		t.Fatalf("unsafe save dialed the node %d times", tracked.calls)
	}
	still, err := store.SavedSnapshot(t.Context(), account.ID, save.ID)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	if still.Status != db.SnapshotSaving {
		t.Fatalf("unsafe save changed the row to %s", still.Status)
	}

	// A ready row is needed for delete acceptance; the row is still saving
	// because the handler rejected the tampered pair.
	if _, err := store.Pool().Exec(t.Context(), `
		UPDATE saved_snapshots SET status = 'ready' WHERE id = $1`, save.ID); err != nil {
		t.Fatalf("mark ready: %v", err)
	}
	if _, err := store.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{
		Owner: account.ID, Name: "base",
	}); err != nil {
		t.Fatalf("accept delete: %v", err)
	}
	if err := store.Pool().QueryRow(t.Context(), `
		SELECT payload::text FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.delete'
		ORDER BY created_at DESC LIMIT 1`, account.ID, save.ID).Scan(&raw); err != nil {
		t.Fatalf("delete payload: %v", err)
	}
	var deletePayload db.DeleteSnapshotPayload
	if err := json.Unmarshal([]byte(raw), &deletePayload); err != nil {
		t.Fatalf("decode delete payload: %v", err)
	}
	deletePayload.ArtifactID = "not-a-snapshot"
	unsafeDelete, err := json.Marshal(deletePayload)
	if err != nil {
		t.Fatalf("encode delete payload: %v", err)
	}
	if _, err := store.Pool().Exec(t.Context(), `
		UPDATE jobs SET payload = $3::jsonb
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.delete'`,
		account.ID, save.ID, unsafeDelete); err != nil {
		t.Fatalf("tamper delete payload: %v", err)
	}
	deleted, err := store.SavedSnapshot(t.Context(), account.ID, save.ID)
	if err != nil {
		t.Fatalf("read deleted row: %v", err)
	}
	deleted.ArtifactID = deletePayload.ArtifactID
	wrapped.row = deleted
	if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
		t.Fatalf("unsafe delete job: worked=%v err=%v", worked, err)
	}
	if tracked.calls != 0 {
		t.Fatalf("unsafe delete dialed the node %d times", tracked.calls)
	}
	purged, err := store.SavedSnapshot(t.Context(), account.ID, save.ID)
	if err != nil {
		t.Fatalf("read purged: %v", err)
	}
	if purged.PurgedAt != nil {
		t.Fatal("unsafe delete marked the artifact purged")
	}
}
