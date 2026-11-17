package lifecycle_test

// Create-from restore boot: the provision job must send the canonical
// from-snapshot artifact with an empty image, use only the request's env and
// release the artifact pin after the sandbox is confirmed ready. Requires
// BBRAFT_TEST_DATABASE_URL (skips otherwise); the node is nodefake through
// the real nodelink gateway.

import (
	"strings"
	"testing"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/machinetype"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

func TestProvisionBootsFromSavedSnapshot(t *testing.T) {
	env := newLifecycleEnv(t)
	node := env.startNode(t, "node-a")
	env.registerNode(t, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 200})
	owner := env.owner(t)
	worker := env.worker(t, lifecycle.Options{})

	// Boot a source sandbox so the node has a real disk to snapshot.
	source := env.createSandboxEnv(t, owner, "node-a", 3, map[string]string{"SOURCE_ONLY": "source"})
	if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
		t.Fatalf("boot source: worked=%v err=%v", worked, err)
	}
	src := env.sandbox(t, owner, source.Sandbox.ID)
	if src.State != db.StateReady {
		t.Fatalf("source state = %s", src.State)
	}

	artifact := "snap-" + strings.Repeat("a", 32)
	if _, err := node.SaveSnapshot(t.Context(), &nodev1.SaveSnapshotRequest{
		SandboxId: src.ID, SnapshotId: artifact,
	}); err != nil {
		t.Fatalf("save source snapshot: %v", err)
	}
	if _, err := env.store.Pool().Exec(t.Context(), `
		INSERT INTO saved_snapshots
			(id, owner_id, name, sandbox_id, status, artifact_id, generation,
			 machine_type, disk_gib, node_id, is_current, created_at, updated_at, ready_at)
		VALUES ($1, $2, 'base', $3, 'ready', $4, 1, 'small', 51, 'node-a', true, now(), now(), now())`,
		uuid.NewV7(), owner, src.ID, artifact); err != nil {
		t.Fatalf("insert saved source: %v", err)
	}
	snapshotSource, err := env.store.SnapshotRestoreSource(t.Context(), owner, "base")
	if err != nil {
		t.Fatalf("resolve saved source: %v", err)
	}

	small, ok := machinetype.Lookup("small")
	if !ok {
		t.Fatal("small type missing")
	}
	resources := db.Resources{VCPUs: small.VCPUs, MemoryMiB: small.MemoryMiB, DiskGiB: small.DiskGiB}
	restore, err := env.store.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: owner, MachineType: small.Name, Name: "restored",
		Env:    map[string]string{"REQUEST_ONLY": "request"},
		NodeID: "node-a", Reserve: resources, Restore: &snapshotSource,
	})
	if err != nil {
		t.Fatalf("create restore: %v", err)
	}
	pinned := env.sandbox(t, owner, restore.Sandbox.ID)
	if !pinned.SnapshotPin || pinned.FromSnapshotID != artifact {
		t.Fatalf("restore row before boot pin=%v from_match=%v", pinned.SnapshotPin, pinned.FromSnapshotID == artifact)
	}

	if worked, err := worker.ProcessNext(t.Context()); err != nil || !worked {
		t.Fatalf("boot restore: worked=%v err=%v", worked, err)
	}

	calls := node.createCalls()
	if len(calls) < 2 {
		t.Fatalf("CreateVM calls = %d", len(calls))
	}
	last := calls[len(calls)-1]
	if last.GetSandboxId() != restore.Sandbox.ID || last.GetFromSnapshot() != artifact || last.GetImage() != "" {
		t.Fatalf("restore CreateVM sandbox_match=%v from_match=%v image_empty=%v",
			last.GetSandboxId() == restore.Sandbox.ID, last.GetFromSnapshot() == artifact, last.GetImage() == "")
	}
	got := last.GetEnv()
	if len(got) != 1 {
		t.Fatalf("restore env count = %d, want 1", len(got))
	}
	if got["REQUEST_ONLY"] != "request" {
		t.Fatal("restore env missing the request value")
	}
	if _, ok := got["SOURCE_ONLY"]; ok {
		t.Fatal("restore inherited the source sandbox env")
	}

	booted := env.sandbox(t, owner, restore.Sandbox.ID)
	if booted.State != db.StateReady || booted.SnapshotPin {
		t.Fatalf("restored sandbox state=%s pin=%v", booted.State, booted.SnapshotPin)
	}
	var pins int
	if err := env.store.Pool().QueryRow(t.Context(),
		`SELECT pins FROM saved_snapshots WHERE id = $1`, snapshotSource.RowID).Scan(&pins); err != nil {
		t.Fatalf("read artifact pins: %v", err)
	}
	if pins != 0 {
		t.Fatalf("artifact pins after ready = %d", pins)
	}
}
