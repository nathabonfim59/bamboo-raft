package db_test

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
)

// dbtestOpen is the local name for the per-schema store factory.
func dbtestOpen(t *testing.T) *db.Store {
	t.Helper()
	return dbtest.Open(t)
}

// snapshotMigrateDownTo rolls the embedded goose schema back to version in the
// test's own schema. It is the rollback seam for populated-state migration
// tests; the prefix keeps it distinct from helpers owned by other areas.
func snapshotMigrateDownTo(t *testing.T, s *db.Store, version int64) error {
	t.Helper()
	gsql := stdlib.OpenDBFromPool(s.Pool())
	defer gsql.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, gsql, os.DirFS("migrations"))
	if err != nil {
		t.Fatalf("goose provider: %v", err)
	}
	_, err = provider.DownTo(t.Context(), version)
	return err
}

// TestMigrateIdempotent runs the embedded migrations twice on an already
// migrated schema and re-ensures the install key.
func TestMigrateIdempotent(t *testing.T) {
	s := dbtestOpen(t)
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM sandboxes`); n != 0 {
		t.Fatalf("sandboxes after migrate: %d, want 0", n)
	}
}

// TestMigrateDownUp proves goose can roll the schema back and forward.
// information_schema counts are scoped to the test's own schema so
// parallel schemas cannot contaminate them.
func TestMigrateDownUp(t *testing.T) {
	s := dbtestOpen(t)
	gsql := stdlib.OpenDBFromPool(s.Pool())
	defer gsql.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, gsql, os.DirFS("migrations"))
	if err != nil {
		t.Fatalf("goose provider: %v", err)
	}
	if _, err := provider.DownTo(t.Context(), 0); err != nil {
		t.Fatalf("down: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM information_schema.tables
		WHERE table_name = 'sandboxes' AND table_schema = current_schema()`); n != 0 {
		t.Fatal("sandboxes table survived down migration")
	}
	if err := s.Migrate(t.Context()); err != nil {
		t.Fatalf("migrate after down: %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM information_schema.tables
		WHERE table_name = 'sandboxes' AND table_schema = current_schema()`); n != 1 {
		t.Fatal("sandboxes table missing after re-migrate")
	}
}
