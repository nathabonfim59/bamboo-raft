package db_test

// Saved snapshot reads (the save lifecycle is B2) plus the unique active
// name constraint that backs its 409s, and schema isolation between tests.

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"encoding/json/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// insertSnapshot writes a raw snapshot row: the save lifecycle belongs to
// B2, so tests seed rows directly.
func insertSnapshot(t *testing.T, s *db.Store, own uuid.UUID, sandboxID, name, artifact string, gen int64) uuid.UUID {
	t.Helper()
	id := uuid.NewV7()
	_, err := s.Pool().Exec(t.Context(), `
		INSERT INTO saved_snapshots (id, owner_id, name, sandbox_id, status, artifact_id, generation)
		VALUES ($1, $2, $3, $4, 'saving', $5, $6)`,
		id, own, name, sandboxID, artifact, gen)
	if err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	return id
}

func TestSavedSnapshotReads(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	own := owner(t, s)
	res := create(t, s, db.CreateSandboxParams{Owner: own})

	first := insertSnapshot(t, s, own, res.Sandbox.ID, "cache", "snap-"+strings.Repeat("a", 32), 1)
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE saved_snapshots SET status = 'ready', size_bytes = 12345 WHERE id = $1`, first); err != nil {
		t.Fatal(err)
	}
	insertSnapshot(t, s, own, res.Sandbox.ID, "nightly", "snap-"+strings.Repeat("b", 32), 1)

	byName, err := s.SavedSnapshotByName(t.Context(), own, "cache")
	if err != nil || byName.ID != first || byName.Status != db.SnapshotReady {
		t.Fatalf("by name: %v %+v", err, byName)
	}
	byID, err := s.SavedSnapshot(t.Context(), own, first)
	if err != nil || byID.ArtifactID != "snap-"+strings.Repeat("a", 32) || *byID.SizeBytes != 12345 {
		t.Fatalf("by id: %v %+v", err, byID)
	}

	// Isolation and absence.
	other := owner(t, s)
	if _, err := s.SavedSnapshotByName(t.Context(), other, "cache"); err != db.ErrNotFound {
		t.Fatalf("foreign name lookup: %v", err)
	}
	if _, err := s.SavedSnapshot(t.Context(), own, uuid.NewV7()); err != db.ErrNotFound {
		t.Fatalf("missing id lookup: %v", err)
	}

	// Pagination.
	own2 := owner(t, s)
	res2 := create(t, s, db.CreateSandboxParams{Owner: own2})
	for i := range 7 {
		insertSnapshot(t, s, own2, res2.Sandbox.ID, "s"+string(rune('0'+i)),
			"snap-"+strings.Repeat("c", 31)+string(rune('0'+i)), 1)
	}
	// One shared microsecond created_at: paging must rely on the id
	// tie-break rather than on distinct timestamps.
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE saved_snapshots
		SET created_at = (SELECT min(created_at) FROM saved_snapshots WHERE owner_id = $1)
		WHERE owner_id = $1`, own2); err != nil {
		t.Fatal(err)
	}
	var total int
	var after *db.PageCursor
	for range 5 {
		page, next, err := s.ListSavedSnapshots(t.Context(), db.SnapshotListQuery{Owner: own2, Limit: 3, After: after})
		if err != nil {
			t.Fatalf("list snapshots: %v", err)
		}
		total += len(page)
		if next == nil {
			break
		}
		after = next
	}
	if total != 7 {
		t.Fatalf("snapshots paged: %d", total)
	}
}

func TestSavedSnapshotUniqueActiveName(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	own := owner(t, s)
	res := create(t, s, db.CreateSandboxParams{Owner: own})

	insertSnapshot(t, s, own, res.Sandbox.ID, "dup", "snap-"+strings.Repeat("d", 32), 1)
	// A second save of the same owner+name while the first is active
	// collides: this is the durable back of the 409 save_in_progress /
	// name-taken behavior B2 layers on top.
	err := insertSnapshotQuiet(t, s, own, res.Sandbox.ID, "dup", "snap-"+strings.Repeat("e", 32), 2)
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pgErr.Code != "23505" || pgErr.ConstraintName != "saved_snapshots_owner_name_current_uk" {
		t.Fatalf("duplicate active name: %v", err)
	}
	// A different owner may use the same name.
	other := owner(t, s)
	res2 := create(t, s, db.CreateSandboxParams{Owner: other})
	insertSnapshot(t, s, other, res2.Sandbox.ID, "dup", "snap-"+strings.Repeat("f", 32), 1)
	// Soft-deleting the name frees it.
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE saved_snapshots SET deleted_at = now(), status = 'failed' WHERE owner_id = $1 AND name = 'dup'`, own); err != nil {
		t.Fatal(err)
	}
	insertSnapshot(t, s, own, res.Sandbox.ID, "dup", "snap-"+strings.Repeat("9", 32), 2)
}

func insertSnapshotQuiet(t *testing.T, s *db.Store, own uuid.UUID, sandboxID, name, artifact string, gen int64) error {
	t.Helper()
	_, err := s.Pool().Exec(t.Context(), `
		INSERT INTO saved_snapshots (id, owner_id, name, sandbox_id, status, artifact_id, generation)
		VALUES ($1, $2, $3, $4, 'saving', $5, $6)`,
		uuid.NewV7(), own, name, sandboxID, artifact, gen)
	return err
}

// TestSavedSnapshotArtifactShape pins the server-generated artifact format:
// "snap-" plus 32 lowercase hex digits, 37 characters total. Shapes that are
// safe filenames but not our generated form are rejected.
func TestSavedSnapshotArtifactShape(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	own := owner(t, s)
	res := create(t, s, db.CreateSandboxParams{Owner: own})

	bad := []string{
		"snap-" + strings.Repeat("a", 31),
		"snap-" + strings.Repeat("a", 33),
		"snap-" + strings.Repeat("A", 32),
		"snap-" + strings.Repeat("g", 32),
		"other-" + strings.Repeat("a", 32),
		"snap-snap-" + strings.Repeat("a", 27),
	}
	for i, artifact := range bad {
		if err := insertSnapshotQuiet(t, s, own, res.Sandbox.ID, "bad"+string(rune('0'+i)), artifact, 1); err == nil {
			t.Fatalf("artifact %q accepted", artifact)
		}
	}
	insertSnapshot(t, s, own, res.Sandbox.ID, "ok", "snap-"+strings.Repeat("0", 32), 1)
}

func TestSchemaIsolationBetweenStores(t *testing.T) {
	s1 := dbtestOpen(t)
	s2 := dbtestOpen(t)
	node(t, s1, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	own := owner(t, s1)
	res := create(t, s1, db.CreateSandboxParams{Owner: own})

	// The second store's schema never sees the first store's rows.
	if n := count(t, s2, `SELECT count(*) FROM sandboxes`); n != 0 {
		t.Fatalf("sandbox leaked across schemas: %d", n)
	}
	if n := count(t, s2, `SELECT count(*) FROM install_keys`); n != 1 {
		t.Fatalf("install keys in second schema: %d", n)
	}
	if _, err := s2.Sandbox(t.Context(), own, res.Sandbox.ID); err != db.ErrNotFound {
		t.Fatalf("cross-schema lookup: %v", err)
	}
}

func TestAccountAndAPIKeyOps(t *testing.T) {
	s := dbtestOpen(t)

	a1, err := s.EnsureAccount(t.Context(), "alpha")
	if err != nil {
		t.Fatal(err)
	}
	a2, err := s.EnsureAccount(t.Context(), "alpha")
	if err != nil || a1.ID != a2.ID {
		t.Fatalf("ensure account: %v %v %v", err, a1.ID, a2.ID)
	}

	var digest [32]byte
	for i := range digest {
		digest[i] = byte(i)
	}
	k1, err := s.EnsureAPIKey(t.Context(), a1.ID, "main", "bmb_", digest)
	if err != nil {
		t.Fatalf("ensure key: %v", err)
	}
	// Restart replay: same digest, same owner, updates display fields only.
	k2, err := s.EnsureAPIKey(t.Context(), a1.ID, "renamed", "bmb_", digest)
	if err != nil || k2.ID != k1.ID || k2.Name != "renamed" {
		t.Fatalf("replay key: %v %+v", err, k2)
	}
	// Same digest under another account: refused, not reassigned.
	b, err := s.EnsureAccount(t.Context(), "beta")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureAPIKey(t.Context(), b.ID, "stolen", "bmb_", digest); err != db.ErrKeyOwned {
		t.Fatalf("cross-owner key: %v", err)
	}
	// Shared display prefix is fine (no unique constraint on prefix).
	otherDigest := digest
	otherDigest[0] ^= 0xFF
	if _, err := s.EnsureAPIKey(t.Context(), a1.ID, "second", "bmb_", otherDigest); err != nil {
		t.Fatalf("second key with same prefix: %v", err)
	}

	// Lookup by digest and usage touch.
	got, err := s.APIKeyBySHA256(t.Context(), digest)
	if err != nil || got.AccountID != a1.ID || got.Name != "renamed" {
		t.Fatalf("by digest: %v %+v", err, got)
	}
	var missing [32]byte
	if _, err := s.APIKeyBySHA256(t.Context(), missing); err != db.ErrNotFound {
		t.Fatalf("missing digest: %v", err)
	}
	if err := s.TouchAPIKeyUsed(t.Context(), k1.ID); err != nil {
		t.Fatalf("touch: %v", err)
	}
	var lastUsed *time.Time
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT last_used_at FROM api_keys WHERE id = $1`, k1.ID).Scan(&lastUsed); err != nil {
		t.Fatal(err)
	}
	if lastUsed == nil {
		t.Fatal("last_used_at not recorded")
	}
}

func TestEnsureAPIKeyConcurrentStarts(t *testing.T) {
	s := dbtestOpen(t)
	a, err := s.EnsureAccount(t.Context(), "concurrent")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.EnsureAccount(t.Context(), "concurrent-b")
	if err != nil {
		t.Fatal(err)
	}
	var digest [32]byte
	for i := range digest {
		digest[i] = byte(200 - i)
	}

	// Several concurrent starts with the same digest and owner: exactly
	// one row, all of them return it.
	const racers = 5
	keys := make([]db.APIKey, racers)
	errs := make([]error, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range racers {
		wg.Go(func() {
			<-start
			keys[i], errs[i] = s.EnsureAPIKey(t.Context(), a.ID, "boot", "bmb_", digest)
		})
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("racer %d: %v", i, err)
		}
		if keys[i].ID != keys[0].ID {
			t.Fatalf("racer %d saw a different row", i)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM api_keys WHERE key_sha256 = $1`, digest[:]); n != 1 {
		t.Fatalf("digest rows after concurrent starts: %d", n)
	}

	// Racing starts across owners never reassign: exactly one owner wins.
	otherDigest := digest
	otherDigest[31] ^= 0x01
	errsA := make([]error, 3)
	errsB := make([]error, 3)
	for i := range 3 {
		wg.Go(func() {
			_, errsA[i] = s.EnsureAPIKey(t.Context(), a.ID, "x", "bmb_", otherDigest)
		})
		wg.Go(func() {
			_, errsB[i] = s.EnsureAPIKey(t.Context(), b.ID, "x", "bmb_", otherDigest)
		})
	}
	wg.Wait()
	var aWins, bWins, refused int
	for i := range 3 {
		switch {
		case errsA[i] == nil:
			aWins++
		case errors.Is(errsA[i], db.ErrKeyOwned):
			refused++
		default:
			t.Fatalf("owner A racer: %v", errsA[i])
		}
		switch {
		case errsB[i] == nil:
			bWins++
		case errors.Is(errsB[i], db.ErrKeyOwned):
			refused++
		default:
			t.Fatalf("owner B racer: %v", errsB[i])
		}
	}
	// The winning owner's racers all replay its row; the other side is
	// refused wholesale. One digest, one owner, never reassigned.
	if !((aWins == 3 && bWins == 0) || (aWins == 0 && bWins == 3)) || refused != 3 {
		t.Fatalf("cross-owner race: aWins=%d bWins=%d refused=%d", aWins, bWins, refused)
	}

	// A revoked key stays rejected even though configuration still lists
	// it.
	if _, err := s.Pool().Exec(t.Context(),
		`UPDATE api_keys SET revoked_at = now() WHERE key_sha256 = $1`, digest[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnsureAPIKey(t.Context(), a.ID, "boot", "bmb_", digest); !errors.Is(err, db.ErrKeyRevoked) {
		t.Fatalf("revoked key ensured: %v", err)
	}
}

func TestAppendAndListEvents(t *testing.T) {
	s := dbtestOpen(t)
	node(t, s, "node-a", db.Resources{VCPUs: 4, MemoryMiB: 4096, DiskGiB: 40})
	own := owner(t, s)
	res := create(t, s, db.CreateSandboxParams{Owner: own})

	if err := s.AppendEvent(t.Context(), own, res.Sandbox.ID, db.EventInput{
		Type: "agent.note", Data: `{"note":"hello"}`,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	events, err := s.SandboxEvents(t.Context(), own, res.Sandbox.ID, 10)
	if err != nil || len(events) != 2 {
		t.Fatalf("events: %v %d", err, len(events))
	}
	// Compare decoded structures: jsonb text form adds whitespace.
	if events[1].Type != "agent.note" {
		t.Fatalf("appended event: %+v", events[1])
	}
	var data map[string]string
	if err := json.Unmarshal([]byte(events[1].Data), &data); err != nil {
		t.Fatalf("event data decode: %v", err)
	}
	if data["note"] != "hello" || len(data) != 1 {
		t.Fatalf("event data: %v", data)
	}
	other := owner(t, s)
	if err := s.AppendEvent(t.Context(), other, res.Sandbox.ID, db.EventInput{Type: "x"}); err != db.ErrNotFound {
		t.Fatalf("foreign append: %v", err)
	}
	if _, err := s.SandboxEvents(t.Context(), other, res.Sandbox.ID, 10); err != db.ErrNotFound {
		t.Fatalf("foreign list: %v", err)
	}
}
