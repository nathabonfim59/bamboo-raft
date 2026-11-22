package snapshots_test

// Generation-backed saved snapshots: with an object store a save pins a
// generation instead of a node artifact. READY sources take a fresh
// generation; STOPPED sources pin their latest completed generation. The
// suite needs BBRAFT_TEST_DATABASE_URL and skips without it.

import (
	"context"
	"errors"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle"
	"github.com/nathabonfim59/bamboo-raft/internal/lifecycle/snapshots"
	"github.com/nathabonfim59/bamboo-raft/internal/nodefake"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/memstore"
)

// generationSaveCutter binds the plugin to the worker after both exist, the
// same late binding the server role uses.
type generationSaveCutter struct {
	worker *lifecycle.Worker
}

func (c *generationSaveCutter) CutGeneration(ctx context.Context, lease *lifecycle.Lease,
	owner uuid.UUID, sandboxID string,
) (db.Generation, error) {
	if c.worker == nil {
		return db.Generation{}, errors.New("test: worker not bound")
	}
	return c.worker.CutGeneration(ctx, lease, owner, sandboxID)
}

// snapshotFlowGenerationWorker builds the real worker with an object store and
// a generation-cutting saved-snapshot plugin.
func snapshotFlowGenerationWorker(t *testing.T, env *snapshotFlowEnv, objects objstore.Store) *lifecycle.Worker {
	t.Helper()
	cutter := &generationSaveCutter{}
	plugin, err := snapshots.New(snapshots.Options{
		Store: env.store, Dial: env.dial, Logger: snapshotFlowQuiet(),
		RetryBase: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		Cutter: cutter,
	})
	if err != nil {
		t.Fatalf("snapshot plugin: %v", err)
	}
	worker, err := lifecycle.New(lifecycle.Options{
		Store: env.store, Dial: env.dial, Logger: snapshotFlowQuiet(),
		Lease: 5 * time.Second, BootTimeout: 10 * time.Second,
		RetryBase: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		Handlers: plugin.Handlers(), ObjectStore: objects,
	})
	if err != nil {
		t.Fatalf("lifecycle worker: %v", err)
	}
	cutter.worker = worker
	return worker
}

// TestGenerationSavedSnapshotSave saves a READY source and a STOPPED source
// with an object store and asserts both rows pin a completed generation with
// no node artifact.
func TestGenerationSavedSnapshotSave(t *testing.T) {
	objects := memstore.New()
	node := nodefake.New("node-a", t.TempDir())
	node.Store = objects
	env := snapshotFlowNewEnv(t, "node-a", node)
	res := db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 51}
	worker := snapshotFlowGenerationWorker(t, env, objects)

	t.Run("ready source", func(t *testing.T) {
		source := env.readySource(t, "default", "ready-save", res)
		env.createVM(t, source.ID, res)
		snap, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
			Owner: env.owner, SandboxID: source.ID, Name: "ready-name", GenerationBacked: true,
		})
		if err != nil {
			t.Fatalf("accept save: %v", err)
		}
		snapshotFlowRunUntil(t, worker, "ready saved row", func() bool {
			row, err := env.store.SavedSnapshot(t.Context(), env.owner, snap.ID)
			return err == nil && row.Status != db.SnapshotSaving
		})
		row, err := env.store.SavedSnapshot(t.Context(), env.owner, snap.ID)
		if err != nil {
			t.Fatalf("saved row: %v", err)
		}
		if row.Status != db.SnapshotReady {
			t.Fatalf("saved row status = %s (error %q), want ready", row.Status, row.ErrorReason)
		}
		if row.GenerationID == nil {
			t.Fatal("saved row has no generation")
		}
		if row.NodeID != nil {
			t.Fatalf("generation-backed saved row kept node %q", *row.NodeID)
		}
		gen, ok, err := env.store.LatestGeneration(t.Context(), env.owner, source.ID)
		if err != nil || !ok || gen.ID != *row.GenerationID {
			t.Fatalf("pinned generation = %v, %v, %v; want %s", gen.ID, ok, err, *row.GenerationID)
		}
		src, err := env.store.SnapshotRestoreSource(t.Context(), env.owner, "ready-name")
		if err != nil {
			t.Fatalf("resolve saved name: %v", err)
		}
		if !src.GenerationBacked() || src.ManifestKey != gen.ManifestKey {
			t.Fatalf("restore source = %+v, want generation %s", src, gen.ManifestKey)
		}
	})

	t.Run("stopped source", func(t *testing.T) {
		source := env.readySource(t, "default", "stopped-save", res)
		env.createVM(t, source.ID, res)
		enqueued, err := env.store.EnqueueSnapshotJob(t.Context(), env.owner, source.ID)
		if err != nil || !enqueued {
			t.Fatalf("enqueue snapshot: %v, %v", enqueued, err)
		}
		var latest db.Generation
		snapshotFlowRunUntil(t, worker, "a completed generation", func() bool {
			gen, ok, err := env.store.LatestGeneration(t.Context(), env.owner, source.ID)
			if err == nil && ok {
				latest = gen
			}
			return ok
		})
		if _, err := env.store.Pool().Exec(t.Context(), `
			UPDATE sandboxes SET state = 'archived' WHERE id = $1`, source.ID); err != nil {
			t.Fatalf("mark source archived: %v", err)
		}
		snap, err := env.store.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
			Owner: env.owner, SandboxID: source.ID, Name: "stopped-name", GenerationBacked: true,
		})
		if err != nil {
			t.Fatalf("accept stopped save: %v", err)
		}
		snapshotFlowRunUntil(t, worker, "stopped saved row", func() bool {
			row, err := env.store.SavedSnapshot(t.Context(), env.owner, snap.ID)
			return err == nil && row.Status != db.SnapshotSaving
		})
		row, err := env.store.SavedSnapshot(t.Context(), env.owner, snap.ID)
		if err != nil {
			t.Fatalf("saved row: %v", err)
		}
		if row.Status != db.SnapshotReady {
			t.Fatalf("saved row status = %s (error %q), want ready", row.Status, row.ErrorReason)
		}
		if row.GenerationID == nil || *row.GenerationID != latest.ID {
			t.Fatalf("stopped save pinned %v, want %s", row.GenerationID, latest.ID)
		}
		if row.NodeID != nil {
			t.Fatalf("stopped generation row kept node %q", *row.NodeID)
		}
	})
}
