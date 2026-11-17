package db_test

// Fixtures shared by the internal/db integration tests: every test gets its
// own schema through dbtest.Open, so cross-test interference is impossible
// and helpers only save typing.

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/db/dbtest"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// openSameSchema opens a second store over the same schema as s (fresh
// parsed config, no shared state) with the given secrets.
func openSameSchema(t *testing.T, s *db.Store, secrets db.Secrets) (*db.Store, error) {
	t.Helper()
	cfg := freshPoolConfig(t)
	var schema string
	if err := s.Pool().QueryRow(t.Context(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	return db.OpenPool(t.Context(), cfg, secrets)
}

// freshPoolConfig parses the test DSN into a fresh pool configuration. It
// goes through dbtest.PoolConfig so a missing database URL skips (or fails
// under BBRAFT_TEST_REQUIRE_ALL=1) instead of falling back to pgx's default
// local socket, and parse errors never echo the DSN.
func freshPoolConfig(t *testing.T) *pgxpool.Config {
	t.Helper()
	return dbtest.PoolConfig(t)
}

// freshSchema creates a brand-new empty schema and returns its name plus a
// pool config aimed at it; cleanup drops it.
func freshSchema(t *testing.T) (string, *pgxpool.Config) {
	t.Helper()
	name := "bbraft_fresh_" + ids.New("", 8)
	cfg := freshPoolConfig(t)
	admin, err := pgx.ConnectConfig(t.Context(), cfg.ConnConfig)
	if err != nil {
		t.Fatal("fresh schema: connect failed")
	}
	if _, err := admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	admin.Close(context.WithoutCancel(t.Context()))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		drop, err := pgx.ConnectConfig(dropCtx, cfg.ConnConfig)
		if err != nil {
			t.Error("fresh schema: drop: connect failed")
			return
		}
		_, dropErr := drop.Exec(dropCtx, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE")
		drop.Close(context.WithoutCancel(dropCtx))
		if dropErr != nil {
			t.Errorf("fresh schema: drop: %v", dropErr)
		}
	})
	poolCfg := freshPoolConfig(t)
	poolCfg.ConnConfig.RuntimeParams["search_path"] = name
	poolCfg.ConnConfig.RuntimeParams["application_name"] = "bbraft_fresh_" + name
	return name, poolCfg
}

// owner returns a fresh account id.
func owner(t *testing.T, s *db.Store) uuid.UUID {
	t.Helper()
	a, err := s.EnsureAccount(t.Context(), "acct-"+uuid.NewV7().String())
	if err != nil {
		t.Fatalf("ensure account: %v", err)
	}
	return a.ID
}

// node registers a node with the given totals.
func node(t *testing.T, s *db.Store, id string, total db.Resources) {
	t.Helper()
	err := s.UpsertNodeInfo(t.Context(), db.NodeInfo{
		ID: id, CPUModel: "test-cpu", QEMUVersion: "9.2", DefaultImage: "img",
		Capacity:    db.Resources{VCPUs: total.VCPUs, MemoryMiB: total.MemoryMiB},
		DiskFreeGiB: total.DiskGiB, // fresh row: no reservations, free == total
		SeenAt:      time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert node %s: %v", id, err)
	}
}

// create is CreateSandbox with common defaults and a fresh bx_-shaped id.
func create(t *testing.T, s *db.Store, p db.CreateSandboxParams) db.CreateSandboxResult {
	t.Helper()
	if p.ID == "" {
		p.ID = ids.Sandbox()
	}
	if p.Owner == uuid.Nil() {
		p.Owner = owner(t, s)
	}
	if p.NodeID == "" {
		p.NodeID = "node-a"
	}
	if p.Reserve == (db.Resources{}) {
		p.Reserve = db.Resources{VCPUs: 1, MemoryMiB: 1024, DiskGiB: 10}
	}
	res, err := s.CreateSandbox(t.Context(), p)
	if err != nil {
		t.Fatalf("create sandbox: %v", err)
	}
	return res
}

// count runs a scalar COUNT query for fixture assertions.
func count(t *testing.T, s *db.Store, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := s.Pool().QueryRow(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

// nodeReserved reads one node's reservation tuple.
func nodeReserved(t *testing.T, s *db.Store, nodeID string) db.Resources {
	t.Helper()
	n, err := s.Node(t.Context(), nodeID)
	if err != nil {
		t.Fatalf("node %s: %v", nodeID, err)
	}
	return n.Reserved
}
