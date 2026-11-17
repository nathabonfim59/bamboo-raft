// Package dbtest opens test stores with one throwaway PostgreSQL schema per
// test, for the integration suites of internal/db and later lifecycle and
// API packages.
//
// A test store is created only when BBRAFT_TEST_DATABASE_URL is set; without
// it tests skip, unless BBRAFT_TEST_REQUIRE_ALL=1, in which case the missing
// variable is a failure (the test VM base sets it). Connection-string errors
// are never echoed: pgx embeds the DSN in parse errors and it may carry a
// password.
package dbtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// errBadDSN hides the cause of connection-string parse failures.
var errBadDSN = errors.New("dbtest: BBRAFT_TEST_DATABASE_URL is not a valid connection string")

// parseConfig is pgxpool.ParseConfig without echoing the DSN.
func parseConfig(dsn string) (cfg *pgxpool.Config, err error) {
	defer func() {
		if err != nil {
			err = errBadDSN
		}
	}()
	return pgxpool.ParseConfig(dsn)
}

// PoolConfig returns a fresh pool configuration for the test database. It is
// the one gate every fresh-schema helper uses: an unset URL skips, unless
// BBRAFT_TEST_REQUIRE_ALL=1, where it fails. It never falls back to pgx's
// default socket, and a malformed URL is reported without echoing it.
func PoolConfig(t testing.TB) *pgxpool.Config {
	t.Helper()
	dsn := os.Getenv("BBRAFT_TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("BBRAFT_TEST_REQUIRE_ALL") == "1" {
			t.Fatal("BBRAFT_TEST_REQUIRE_ALL=1 but BBRAFT_TEST_DATABASE_URL is not set")
		}
		t.Skip("set BBRAFT_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	cfg, err := parseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// Open returns a migrated Store in a fresh schema. Cleanup is registered the
// moment the schema exists, so a failure during open or migrate still drops
// it; the master key is random per test and the install key bootstrap runs
// under it.
func Open(t testing.TB) *db.Store {
	t.Helper()
	cfg := PoolConfig(t)
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("dbtest: schema name randomness: %v", err)
	}
	suffix := hex.EncodeToString(b[:])
	schema := "bbraft_dbtest_" + suffix
	// A unique application_name per store lets listener tests identify
	// exactly their own backend instead of matching every LISTEN session
	// in the cluster.
	cfg.ConnConfig.RuntimeParams["application_name"] = "bbraft_dbtest_" + suffix

	ctx := t.Context()
	admin, err := pgx.ConnectConfig(ctx, cfg.ConnConfig)
	if err != nil {
		// The raw pgx error may echo connection parameters; keep the
		// DSN out of test output.
		t.Fatal("dbtest: connect: connection failed")
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close(context.WithoutCancel(ctx))
		t.Fatalf("dbtest: create schema: %v", err)
	}
	admin.Close(context.WithoutCancel(ctx))
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		drop, err := pgx.ConnectConfig(dropCtx, cfg.ConnConfig)
		if err != nil {
			t.Error("dbtest: drop schema: connect failed")
			return
		}
		_, dropErr := drop.Exec(dropCtx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		drop.Close(context.WithoutCancel(dropCtx))
		if dropErr != nil {
			t.Errorf("dbtest: drop schema: %v", dropErr)
		}
	})

	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatalf("dbtest: master key randomness: %v", err)
	}
	secrets, err := db.NewSecrets(master)
	if err != nil {
		t.Fatalf("dbtest: secrets: %v", err)
	}
	store, err := db.OpenPool(ctx, cfg, secrets)
	if err != nil {
		// Keep connection details out of test output.
		t.Fatal("dbtest: open pool: connection failed")
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("dbtest: migrate: %v", err)
	}
	return store
}
