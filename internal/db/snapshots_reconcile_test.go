package db_test

// Paged missing-job discovery for the snapshot reconciler. The suite opens
// its own PostgreSQL schema through dbtest; without BBRAFT_TEST_DATABASE_URL
// it skips (BBRAFT_TEST_REQUIRE_ALL=1 still fails, never weakens the gate).

import (
	"slices"
	"testing"

	"encoding/json/v2"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
)

func TestSavingSnapshotsPagesOnlySaving(t *testing.T) {
	store := dbtest.Open(t)
	own := owner(t, store)
	nodeID := snapshotNode(t, store)
	source := snapshotReadySource(t, store, own, nodeID)
	first := snapshotAcceptSave(t, store, own, source.ID, "alpha")
	second := snapshotAcceptSave(t, store, own, source.ID, "beta")
	third := snapshotAcceptSave(t, store, own, source.ID, "gamma")

	// A completed generation must never appear in the missing-job scan.
	if _, err := store.Pool().Exec(t.Context(), `
		UPDATE saved_snapshots SET status = 'ready', updated_at = now() WHERE id = $1`,
		first.ID); err != nil {
		t.Fatalf("mark first ready: %v", err)
	}
	// Equal timestamps force the (updated_at, id) tie-break: both remaining
	// rows must still traverse in two limit-1 pages.
	if _, err := store.Pool().Exec(t.Context(), `
		UPDATE saved_snapshots SET updated_at = '2026-01-01T00:00:00Z'
		WHERE id = $1 OR id = $2`, second.ID, third.ID); err != nil {
		t.Fatalf("equalize updated_at: %v", err)
	}

	page1, cursor1, err := store.SavingSnapshots(t.Context(), nil, 1)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(page1) != 1 || page1[0].Status != db.SnapshotSaving || cursor1 == nil {
		t.Fatalf("page 1 rows=%d status_ok=%v cursor=%v", len(page1), len(page1) == 1 && page1[0].Status == db.SnapshotSaving, cursor1 != nil)
	}
	page2, cursor2, err := store.SavingSnapshots(t.Context(), cursor1, 1)
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(page2) != 1 || page2[0].Status != db.SnapshotSaving || cursor2 != nil {
		t.Fatalf("page 2 rows=%d status_ok=%v last_page=%v", len(page2), len(page2) == 1 && page2[0].Status == db.SnapshotSaving, cursor2 == nil)
	}

	got := []uuid.UUID{page1[0].ID, page2[0].ID}
	if !slices.Contains(got, second.ID) || !slices.Contains(got, third.ID) || slices.Contains(got, first.ID) {
		t.Fatalf("saving rows count=%d has_second=%v has_third=%v has_ready=%v",
			len(got), slices.Contains(got, second.ID), slices.Contains(got, third.ID), slices.Contains(got, first.ID))
	}

	// A non-positive page size uses the bounded default and returns both
	// saving rows while still excluding the ready generation.
	all, next, err := store.SavingSnapshots(t.Context(), nil, 0)
	if err != nil || len(all) != 2 || next != nil {
		t.Fatalf("page size 0 rows=%d next=%v err=%v", len(all), next != nil, err != nil)
	}
	if slices.ContainsFunc(all, func(row db.SavedSnapshot) bool { return row.ID == first.ID }) {
		t.Fatal("ready generation leaked into the saving scan")
	}
}

func TestReconcileSnapshotSaveFailureSeesMissingJob(t *testing.T) {
	store := dbtest.Open(t)
	own := owner(t, store)
	nodeID := snapshotNode(t, store)
	source := snapshotReadySource(t, store, own, nodeID)
	snap := snapshotAcceptSave(t, store, own, source.ID, "orphan")

	// Remove the only save job so the reconciler's atomic proof sees a
	// missing job, not an active one.
	if _, err := store.Pool().Exec(t.Context(), `
		DELETE FROM jobs WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.save'`,
		own, snap.ID); err != nil {
		t.Fatalf("delete save job: %v", err)
	}
	rows, _, err := store.SavingSnapshots(t.Context(), nil, 100)
	if err != nil {
		t.Fatalf("SavingSnapshots: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != snap.ID {
		t.Fatalf("saving rows len=%d match=%v", len(rows), len(rows) == 1 && rows[0].ID == snap.ID)
	}
	updated, changed, err := store.ReconcileSnapshotSaveFailure(t.Context(), db.ReconcileSnapshotSaveFailureParams{
		Owner: own, SnapshotID: snap.ID, ArtifactID: snap.ArtifactID,
		Generation: snap.Generation, Reason: "save job missing",
	})
	if err != nil {
		t.Fatalf("reconcile missing job: %v", err)
	}
	if !changed || updated.Status != db.SnapshotFailed {
		t.Fatalf("reconciled status=%s changed=%v", updated.Status, changed)
	}
	var payload db.DeleteSnapshotPayload
	var raw string
	if err := store.Pool().QueryRow(t.Context(), `
		SELECT payload::text FROM jobs
		WHERE owner_id = $1 AND snapshot_id = $2 AND kind = 'snapshot.delete'
		ORDER BY created_at DESC LIMIT 1`, own, snap.ID).Scan(&raw); err != nil {
		t.Fatalf("cleanup job: %v", err)
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode cleanup payload: %v", err)
	}
	if payload.ArtifactID != snap.ArtifactID {
		t.Fatalf("cleanup artifact = %q", payload.ArtifactID)
	}
}
