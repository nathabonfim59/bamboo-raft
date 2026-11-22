package db_test

// PostgreSQL tests of generation-backed saved snapshots and share codes.
// Without BBRAFT_TEST_DATABASE_URL they skip (BBRAFT_TEST_REQUIRE_ALL=1 makes
// the skip a failure); each test owns a fresh schema.

import (
	"bytes"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"strings"
	"testing"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// shareReadySnapshot saves one ready node-local generation of a ready source
// sandbox on node-a.
func shareReadySnapshot(t *testing.T, s *db.Store, own uuid.UUID) (db.Sandbox, db.SavedSnapshot) {
	t.Helper()
	sbx := snapshotReadySource(t, s, own, snapshotNode(t, s))
	snap := snapshotAcceptSave(t, s, own, sbx.ID, "web")
	snap = snapshotCompleteSave(t, s, snap, 4096)
	return sbx, snap
}

// shareGenerationSnapshot promotes a saved row to a generation-backed one that
// points at a fresh completed generation of the source sandbox.
func shareGenerationSnapshot(t *testing.T, s *db.Store, own uuid.UUID) (db.Sandbox, db.SavedSnapshot, db.Generation) {
	t.Helper()
	sbx, snap := shareReadySnapshot(t, s, own)
	gen := genComplete(t, s, own, sbx.ID, time.Now().Add(-time.Minute))
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE saved_snapshots
		SET generation_id = $2, node_id = NULL, size_bytes = $3
		WHERE id = $1 AND owner_id = $4`,
		snap.ID, gen.ID, int64(gen.DiskBytes), own); err != nil {
		t.Fatalf("promote saved snapshot: %v", err)
	}
	row, err := s.SavedSnapshot(t.Context(), own, snap.ID)
	if err != nil {
		t.Fatalf("reload saved snapshot: %v", err)
	}
	return sbx, row, gen
}

func TestSnapshotShareLifecycle(t *testing.T) {
	s, own := genStore(t)
	_, snap := shareReadySnapshot(t, s, own)

	share, code, err := s.CreateSnapshotShare(t.Context(), own, "web")
	if err != nil {
		t.Fatalf("create share: %v", err)
	}
	if !db.ValidSnapshotShareCode(code) {
		t.Fatalf("code %q is not canonical", code)
	}
	if !db.ValidSnapshotShareID(share.ID) || share.Status != db.SnapshotShareActive {
		t.Fatalf("share = %+v", share)
	}
	if share.UseCount != 0 || share.LastUsedAt != nil {
		t.Fatalf("fresh share usage = %+v", share)
	}
	want := sha256.Sum256([]byte(code))
	if !bytes.Equal(share.CodeSHA256, want[:]) {
		t.Fatal("stored digest is not SHA-256 of the code")
	}
	var stored []byte
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT code_sha256 FROM snapshot_shares WHERE id = $1`, share.ID).Scan(&stored); err != nil {
		t.Fatalf("read digest: %v", err)
	}
	if !bytes.Equal(stored, want[:]) || bytes.Contains(stored, []byte(code)) {
		t.Fatal("database does not hold only the digest")
	}

	rows, err := s.ListSnapshotShares(t.Context(), own, "web")
	if err != nil || len(rows) != 1 || rows[0].ID != share.ID {
		t.Fatalf("list shares = %+v, %v", rows, err)
	}

	source, err := s.SnapshotShareSource(t.Context(), code)
	if err != nil {
		t.Fatalf("resolve share: %v", err)
	}
	if source.ShareID != share.ID || source.OwnerID != own || source.Name != "web" ||
		source.RowID != snap.ID || source.NodeID != "node-a" {
		t.Fatalf("share source = %+v", source)
	}

	revoked, err := s.RevokeSnapshotShare(t.Context(), own, "web", share.ID)
	if err != nil || revoked.Status != db.SnapshotShareRevoked || revoked.RevokedAt == nil {
		t.Fatalf("revoked = %+v, %v", revoked, err)
	}
	if _, err := s.SnapshotShareSource(t.Context(), code); !errors.Is(err, db.ErrSnapshotNotFound) {
		t.Fatalf("revoked code resolved: %v", err)
	}
	// Revoking twice is idempotent.
	if again, err := s.RevokeSnapshotShare(t.Context(), own, "web", share.ID); err != nil || again.Status != db.SnapshotShareRevoked {
		t.Fatalf("second revoke = %+v, %v", again, err)
	}
	// A foreign owner cannot revoke it.
	other := owner(t, s)
	if _, err := s.RevokeSnapshotShare(t.Context(), other, "web", share.ID); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign revoke = %v", err)
	}
}

func TestSnapshotShareDeleteRevokesCodes(t *testing.T) {
	s, own := genStore(t)
	shareReadySnapshot(t, s, own)
	if _, _, err := s.CreateSnapshotShare(t.Context(), own, "web"); err != nil {
		t.Fatalf("create share: %v", err)
	}
	if _, err := s.AcceptSnapshotDelete(t.Context(), db.AcceptSnapshotDeleteParams{Owner: own, Name: "web"}); err != nil {
		t.Fatalf("delete name: %v", err)
	}
	rows, err := s.ListSnapshotShares(t.Context(), own, "web")
	if !errors.Is(err, db.ErrNotFound) {
		// The name is gone, so its share list is not visible either.
		t.Fatalf("list after delete = %+v, %v", rows, err)
	}
	var active int64
	if err := s.Pool().QueryRow(t.Context(), `
		SELECT count(*) FROM snapshot_shares WHERE owner_id = $1 AND name = 'web' AND status = 'active'`,
		own).Scan(&active); err != nil {
		t.Fatalf("count shares: %v", err)
	}
	if active != 0 {
		t.Fatalf("%d active shares survived the delete", active)
	}
}

func TestSnapshotGenerationSourceRestoresAnywhere(t *testing.T) {
	s, own := genStore(t)
	node(t, s, "node-b", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 200})
	_, snap, gen := shareGenerationSnapshot(t, s, own)

	source, err := s.SnapshotRestoreSource(t.Context(), own, "web")
	if err != nil {
		t.Fatalf("resolve saved generation: %v", err)
	}
	if !source.GenerationBacked() || *source.GenerationID != gen.ID ||
		source.ManifestKey != gen.ManifestKey || source.Image != gen.Image ||
		source.DiskBytes != gen.DiskBytes || source.NodeID != "node-a" {
		t.Fatalf("source = %+v", source)
	}
	if source.DiskGiB < db.DiskGiBFor(gen.DiskBytes) {
		t.Fatalf("disk %d is smaller than the generation's %d", source.DiskGiB, db.DiskGiBFor(gen.DiskBytes))
	}
	if snap.GenerationID == nil || *snap.GenerationID != gen.ID {
		t.Fatalf("saved row generation = %v", snap.GenerationID)
	}

	res := create(t, s, db.CreateSandboxParams{
		Owner: own, NodeID: "node-b", MachineType: "default",
		Reserve:            db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: source.DiskGiB},
		FromManifest:       source.ManifestKey,
		SourceGenerationID: source.GenerationID,
	})
	if res.Sandbox.NodeID == nil || *res.Sandbox.NodeID != "node-b" {
		t.Fatalf("restored sandbox node = %v, want node-b", res.Sandbox.NodeID)
	}
	if res.Sandbox.FromManifestKey != source.ManifestKey {
		t.Fatalf("from manifest = %q", res.Sandbox.FromManifestKey)
	}
	if res.Job.SourceGenerationID == nil || *res.Job.SourceGenerationID != gen.ID {
		t.Fatalf("provision job source generation = %v", res.Job.SourceGenerationID)
	}

	// The legacy path still pins the artifact to its recorded node: a
	// generation id is what makes a row restorable anywhere, so the same
	// row without one is not a generation source.
	if source.GenerationID == nil {
		t.Fatal("generation source lost its generation id")
	}
	if _, err := s.SnapshotRestoreSource(t.Context(), own, "missing"); !errors.Is(err, db.ErrSnapshotNotFound) {
		t.Fatalf("missing name = %v", err)
	}
}

func TestCreateShareRecordsUseInTransaction(t *testing.T) {
	s, own := genStore(t)
	shareGenerationSnapshot(t, s, own)
	_, code, err := s.CreateSnapshotShare(t.Context(), own, "web")
	if err != nil {
		t.Fatalf("create share: %v", err)
	}
	source, err := s.SnapshotShareSource(t.Context(), code)
	if err != nil {
		t.Fatalf("resolve share: %v", err)
	}
	caller := owner(t, s)
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		Owner: caller, NodeID: "node-a", MachineType: "default",
		Reserve:            db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: source.DiskGiB},
		FromManifest:       source.ManifestKey,
		SourceGenerationID: source.GenerationID,
		ShareID:            source.ShareID,
	}); err != nil {
		t.Fatalf("create from shared generation: %v", err)
	}
	rows, err := s.ListSnapshotShares(t.Context(), own, "web")
	if err != nil || len(rows) != 1 || rows[0].UseCount != 1 || rows[0].LastUsedAt == nil {
		t.Fatalf("share usage = %+v, %v", rows, err)
	}

	// A revoked code cannot create anymore.
	if _, err := s.RevokeSnapshotShare(t.Context(), own, "web", source.ShareID); err != nil {
		t.Fatalf("revoke share: %v", err)
	}
	_, err = s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		Owner: caller, NodeID: "node-a", MachineType: "default",
		Reserve:            db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: source.DiskGiB},
		FromManifest:       source.ManifestKey,
		SourceGenerationID: source.GenerationID,
		ShareID:            source.ShareID,
	})
	if !errors.Is(err, db.ErrSnapshotNotFound) {
		t.Fatalf("create with revoked share = %v", err)
	}
}

// TestSnapshotGenerationSave completes a generation-backed save from a
// stopped source and records the generation with no node artifact.
func TestSnapshotGenerationSave(t *testing.T) {
	s, own := genStore(t)
	sbx := snapshotReadySource(t, s, own, snapshotNode(t, s))
	gen := genComplete(t, s, own, sbx.ID, time.Now().Add(-time.Minute))
	genExec(t, s, `UPDATE sandboxes SET state = 'archived' WHERE id = $1 AND owner_id = $2`, sbx.ID, own)

	snap, err := s.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: own, SandboxID: sbx.ID, Name: "web", GenerationBacked: true,
	})
	if err != nil {
		t.Fatalf("accept generation save: %v", err)
	}
	job := snapshotClaimSave(t, s, snap)
	size := int64(gen.ContentBytes)
	done, changed, err := s.CompleteSnapshotSave(t.Context(), db.CompleteSnapshotSaveParams{
		Owner: own, SnapshotID: snap.ID, ArtifactID: snap.ArtifactID,
		Generation: snap.Generation, SizeBytes: &size, GenerationID: &gen.ID,
		JobID: job.ID, Worker: "snap-worker", Token: job.LeaseToken,
	})
	if err != nil || !changed {
		t.Fatalf("complete generation save = %+v, %v, %v", done, changed, err)
	}
	if done.Status != db.SnapshotReady || done.GenerationID == nil || *done.GenerationID != gen.ID {
		t.Fatalf("row = %+v", done)
	}
	if done.NodeID != nil {
		t.Fatalf("generation-backed row kept node %q", *done.NodeID)
	}
	if done.SizeBytes == nil || *done.SizeBytes != size {
		t.Fatalf("size = %v, want %d", done.SizeBytes, size)
	}
	source, err := s.SnapshotRestoreSource(t.Context(), own, "web")
	if err != nil || !source.GenerationBacked() || source.ManifestKey != gen.ManifestKey {
		t.Fatalf("restore source = %+v, %v", source, err)
	}
}

// TestSnapshotSaveSourceRules covers the source-state matrix of a save,
// including the stopped-source generation path and snapshots off.
func TestSnapshotSaveSourceRules(t *testing.T) {
	s, own := genStore(t)
	sbx := snapshotReadySource(t, s, own, snapshotNode(t, s))

	// snapshots:false refuses a save before any artifact row exists.
	genExec(t, s, `UPDATE sandboxes SET snapshots_enabled = false WHERE id = $1`, sbx.ID)
	if _, err := s.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: own, SandboxID: sbx.ID, Name: "web", GenerationBacked: true,
	}); !errors.Is(err, db.ErrSnapshotsDisabled) {
		t.Fatalf("save with snapshots off = %v", err)
	}
	genExec(t, s, `UPDATE sandboxes SET snapshots_enabled = true, state = 'archived' WHERE id = $1`, sbx.ID)

	// A stopped source is only acceptable on the generation path.
	if _, err := s.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: own, SandboxID: sbx.ID, Name: "web",
	}); !errors.Is(err, db.ErrSnapshotSourceNotReady) {
		t.Fatalf("node-local save of a stopped source = %v", err)
	}

	// A generation-backed save of a stopped source with no local disk is
	// accepted and its job carries no node.
	genExec(t, s, `UPDATE sandboxes SET node_id = NULL, disk_node_id = NULL WHERE id = $1`, sbx.ID)
	snap, err := s.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: own, SandboxID: sbx.ID, Name: "web", GenerationBacked: true,
	})
	if err != nil {
		t.Fatalf("generation save of a stopped source = %v", err)
	}
	var payload db.SaveSnapshotPayload
	var raw []byte
	if err := s.Pool().QueryRow(t.Context(), `
		SELECT payload FROM jobs WHERE snapshot_id = $1 AND kind = 'snapshot.save'`,
		snap.ID).Scan(&raw); err != nil {
		t.Fatalf("read save payload: %v", err)
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode save payload: %v", err)
	}
	if payload.NodeID != "" {
		t.Fatalf("job node = %q, want none", payload.NodeID)
	}
}

// TestSnapshotGenerationSaveRejectsForeignGeneration refuses a completion that
// pins another sandbox's generation.
func TestSnapshotGenerationSaveRejectsForeignGeneration(t *testing.T) {
	s, own := genStore(t)
	first := snapshotReadySource(t, s, own, snapshotNode(t, s))
	second := snapshotReadySource(t, s, own, snapshotNode(t, s))
	gen := genComplete(t, s, own, second.ID, time.Now().Add(-time.Minute))

	snap, err := s.AcceptSnapshotSave(t.Context(), db.AcceptSnapshotSaveParams{
		Owner: own, SandboxID: first.ID, Name: "web", GenerationBacked: true,
	})
	if err != nil {
		t.Fatalf("accept save: %v", err)
	}
	job := snapshotClaimSave(t, s, snap)
	size := int64(1)
	_, _, err = s.CompleteSnapshotSave(t.Context(), db.CompleteSnapshotSaveParams{
		Owner: own, SnapshotID: snap.ID, ArtifactID: snap.ArtifactID,
		Generation: snap.Generation, SizeBytes: &size, GenerationID: &gen.ID,
		JobID: job.ID, Worker: "snap-worker", Token: job.LeaseToken,
	})
	if !errors.Is(err, db.ErrSnapshotNotFound) {
		t.Fatalf("foreign generation completion = %v", err)
	}
}

// TestSharedSnapshotRestores covers the cross-account rule for a legacy
// node-local saved snapshot: only an active share authorizes the create, and
// its use and pin are recorded in the create transaction.
func TestSharedSnapshotRestores(t *testing.T) {
	s, own := genStore(t)
	_, snap := shareReadySnapshot(t, s, own)
	caller := owner(t, s)

	_, code, err := s.CreateSnapshotShare(t.Context(), own, "web")
	if err != nil {
		t.Fatalf("create share: %v", err)
	}
	src, err := s.SnapshotShareSource(t.Context(), code)
	if err != nil {
		t.Fatalf("resolve share: %v", err)
	}
	res := create(t, s, db.CreateSandboxParams{
		Owner: caller, NodeID: "node-a", Restore: &src, ShareID: src.ShareID,
		Reserve: db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: 51},
	})
	if res.Sandbox.OwnerID != caller {
		t.Fatalf("restored sandbox owner = %s, want %s", res.Sandbox.OwnerID, caller)
	}
	rows, err := s.ListSnapshotShares(t.Context(), own, "web")
	if err != nil || rows[0].UseCount != 1 || rows[0].LastUsedAt == nil {
		t.Fatalf("share usage = %+v, %v", rows, err)
	}
	pinned, err := s.SavedSnapshot(t.Context(), own, snap.ID)
	if err != nil || pinned.Pins != 1 {
		t.Fatalf("sharer pins = %+v, %v", pinned, err)
	}

	// Releasing the pin works by row id even though the sandbox belongs to
	// the other account.
	genExec(t, s, `UPDATE sandboxes SET state = 'ready' WHERE id = $1`, res.Sandbox.ID)
	if err := s.ReleaseSnapshotPin(t.Context(), caller, res.Sandbox.ID); err != nil {
		t.Fatalf("release cross-account pin: %v", err)
	}
	pinned, err = s.SavedSnapshot(t.Context(), own, snap.ID)
	if err != nil || pinned.Pins != 0 {
		t.Fatalf("pins after release = %+v, %v", pinned, err)
	}

	// Without the share the same source is a foreign snapshot.
	noShare := src
	noShare.ShareID = ""
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		Owner: caller, NodeID: "node-a", Restore: &noShare,
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 51},
	}); !errors.Is(err, db.ErrSnapshotNotFound) {
		t.Fatalf("foreign restore without share = %v", err)
	}

	// A revoked code no longer authorizes a create.
	revokedShare, revokedCode, err := s.CreateSnapshotShare(t.Context(), own, "web")
	if err != nil {
		t.Fatalf("create second share: %v", err)
	}
	revokedSrc, err := s.SnapshotShareSource(t.Context(), revokedCode)
	if err != nil {
		t.Fatalf("resolve second share: %v", err)
	}
	if _, err := s.RevokeSnapshotShare(t.Context(), own, "web", revokedShare.ID); err != nil {
		t.Fatalf("revoke second share: %v", err)
	}
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		Owner: caller, NodeID: "node-a", Restore: &revokedSrc, ShareID: revokedSrc.ShareID,
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 51},
	}); !errors.Is(err, db.ErrSnapshotNotFound) {
		t.Fatalf("revoked share restore = %v", err)
	}
}

// TestSharedGenerationRestores covers the same rule for a generation-backed
// saved snapshot, whose generation owner must match the share's current
// deployable generation.
func TestSharedGenerationRestores(t *testing.T) {
	s, own := genStore(t)
	node(t, s, "node-b", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 200})
	_, _, gen := shareGenerationSnapshot(t, s, own)
	_, code, err := s.CreateSnapshotShare(t.Context(), own, "web")
	if err != nil {
		t.Fatalf("create share: %v", err)
	}
	caller := owner(t, s)
	src, err := s.SnapshotShareSource(t.Context(), code)
	if err != nil {
		t.Fatalf("resolve share: %v", err)
	}
	if !src.GenerationBacked() || *src.GenerationID != gen.ID {
		t.Fatalf("share source = %+v", src)
	}

	// Without a share a foreign generation is not found.
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		Owner: caller, NodeID: "node-b", FromManifest: src.ManifestKey,
		SourceGenerationID: src.GenerationID,
		Reserve:            db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: src.DiskGiB},
	}); !errors.Is(err, db.ErrSnapshotNotFound) {
		t.Fatalf("foreign generation without share = %v", err)
	}

	res := create(t, s, db.CreateSandboxParams{
		Owner: caller, NodeID: "node-b", FromManifest: src.ManifestKey,
		SourceGenerationID: src.GenerationID, ShareID: src.ShareID,
		Reserve: db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: src.DiskGiB},
	})
	if res.Sandbox.OwnerID != caller {
		t.Fatalf("restored sandbox owner = %s, want %s", res.Sandbox.OwnerID, caller)
	}
	rows, err := s.ListSnapshotShares(t.Context(), own, "web")
	if err != nil || rows[0].UseCount != 1 {
		t.Fatalf("share usage = %+v, %v", rows, err)
	}

	// A code revoked after resolution cannot create in the same window.
	if _, err := s.RevokeSnapshotShare(t.Context(), own, "web", src.ShareID); err != nil {
		t.Fatalf("revoke share: %v", err)
	}
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		Owner: caller, NodeID: "node-b", FromManifest: src.ManifestKey,
		SourceGenerationID: src.GenerationID, ShareID: src.ShareID,
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: src.DiskGiB},
	}); !errors.Is(err, db.ErrSnapshotNotFound) {
		t.Fatalf("revoked generation share = %v", err)
	}
}

// TestBatchFromSharedSnapshotCountsUses proves a count batch counts one use
// per created sandbox inside the single create transaction, and that a
// rolled-back batch counts none.
func TestBatchFromSharedSnapshotCountsUses(t *testing.T) {
	s, own := genStore(t)
	// Room for both three-sandbox batches so the bad element, not capacity,
	// is what rolls the second batch back.
	node(t, s, "node-b", db.Resources{VCPUs: 32, MemoryMiB: 65536, DiskGiB: 500})
	_, _, gen := shareGenerationSnapshot(t, s, own)
	_, code, err := s.CreateSnapshotShare(t.Context(), own, "web")
	if err != nil {
		t.Fatalf("create share: %v", err)
	}
	caller := owner(t, s)
	src, err := s.SnapshotShareSource(t.Context(), code)
	if err != nil {
		t.Fatalf("resolve share: %v", err)
	}
	params := make([]db.CreateSandboxParams, 3)
	for i := range params {
		params[i] = db.CreateSandboxParams{
			ID: ids.Sandbox(), Owner: caller, NodeID: "node-b",
			FromManifest: src.ManifestKey, SourceGenerationID: src.GenerationID,
			ShareID: src.ShareID,
			Reserve: db.Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: src.DiskGiB},
		}
	}
	if _, err := s.CreateSandboxes(t.Context(), params); err != nil {
		t.Fatalf("batch from share: %v", err)
	}
	rows, err := s.ListSnapshotShares(t.Context(), own, "web")
	if err != nil || rows[0].UseCount != 3 || rows[0].LastUsedAt == nil {
		t.Fatalf("batch share usage = %+v, %v", rows, err)
	}

	// One bad element rolls the whole batch back, including its use counts.
	bad := make([]db.CreateSandboxParams, 3)
	for i := range bad {
		bad[i] = params[i]
		bad[i].ID = ids.Sandbox()
	}
	bad[2].FromManifest = "bbraft/chains/x/generations/9999999999.json"
	bad[2].SourceGenerationID = new(gen.ID)
	if _, err := s.CreateSandboxes(t.Context(), bad); !errors.Is(err, db.ErrSnapshotNotFound) {
		t.Fatalf("batch with a bad element = %v", err)
	}
	rows, err = s.ListSnapshotShares(t.Context(), own, "web")
	if err != nil || rows[0].UseCount != 3 {
		t.Fatalf("usage after rollback = %+v, %v", rows, err)
	}
}

// TestPhase2SavedMigration covers migration 00010 up, the guarded down and up
// again on a fresh schema.
func TestPhase2SavedMigration(t *testing.T) {
	s := dbtestOpen(t)
	provider := phase1Provider(t, s)
	ctx := t.Context()

	// An empty schema can roll the migration back and forward.
	if _, err := provider.DownTo(ctx, 9); err != nil {
		t.Fatalf("down to 9: %v", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("up again: %v", err)
	}

	// A share code blocks the rollback: dropping the table would orphan it.
	own := owner(t, s)
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO snapshot_shares (id, owner_id, name, code_sha256, code_prefix)
		VALUES ($1, $2, 'web', $3, 'bsh_abcd1234')`,
		"shr_"+strings.Repeat("a", 32), own, bytes.Repeat([]byte{1}, 32)); err != nil {
		t.Fatalf("insert share: %v", err)
	}
	if _, err := provider.DownTo(ctx, 9); err == nil {
		t.Fatal("rollback with a share code was allowed")
	}
	if _, err := s.Pool().Exec(ctx, `DELETE FROM snapshot_shares`); err != nil {
		t.Fatalf("clear shares: %v", err)
	}
	if _, err := provider.DownTo(ctx, 9); err != nil {
		t.Fatalf("down after clearing shares: %v", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("final up: %v", err)
	}
}
