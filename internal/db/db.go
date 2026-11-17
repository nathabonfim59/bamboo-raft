// Package db owns the control-plane PostgreSQL schema, the row models, the
// hand-written SQL over pgx/v5 and the durable job queue.
//
// Every Store method is a single database transaction: consumers compose
// atomic Store operations instead of holding a transaction open across RPCs,
// and lifecycle code never performs an RPC inside a SQL transaction. An
// operation that needs more atomicity becomes a new Store method, added by
// the owner of this package.
//
// Rows are owner-scoped everywhere: every method that reads or writes an
// owner-bearing row takes the owner explicitly, and a row owned by another
// account answers ErrNotFound. Sandbox env values are sensitive: they are
// stored for the guest but never included in errors, logs or [Sandbox.String].
package db

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store is the database facade. It is safe for concurrent use; the pool is
// shared and every method runs its own transaction.
type Store struct {
	pool    *pgxpool.Pool
	secrets Secrets
	env     sealedEnv // install data-key cipher, set by Migrate
}

// Open connects a pool for url (a Postgres keyword/value string or URL),
// verifies the connection and adopts the install secrets. The pool is tuned
// through URL parameters; defaults otherwise.
func Open(ctx context.Context, url string, secrets Secrets) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		// pgx echoes the connection string in parse errors; it may carry
		// a password, so the cause is dropped, not wrapped.
		return nil, errors.New("db: invalid connection string")
	}
	return OpenPool(ctx, cfg, secrets)
}

// OpenPool connects a pool from an explicit configuration and adopts the
// install secrets. Tests use it to point a pool at a private schema.
func OpenPool(ctx context.Context, cfg *pgxpool.Config, secrets Secrets) (*Store, error) {
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: ping: %w", err)
	}
	return &Store{pool: pool, secrets: secrets}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Pool exposes the underlying pool for tests and maintenance commands only;
// regular state changes go through Store methods.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Migrate applies the embedded goose migrations and then ensures the install
// data key exists (first start creates it wrapped by the master key; later
// starts reuse it, and a mismatched master key fails with
// [ErrMasterKeyMismatch]). It is idempotent and safe to run at every server
// start.
func (s *Store) Migrate(ctx context.Context) error {
	gsql := stdlib.OpenDBFromPool(s.pool)
	defer gsql.Close()
	// goose globs *.sql at the root of the file system it is handed, so
	// hand it the migrations directory rather than the embed root.
	root, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("db: migrations fs: %w", err)
	}
	// The session locker is per-call, not shared package state; its
	// constructor only validates options.
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("db: session locker: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, gsql, root,
		goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("db: goose provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("db: migrate up: %w", err)
	}
	return s.ensureInstallKey(ctx)
}
