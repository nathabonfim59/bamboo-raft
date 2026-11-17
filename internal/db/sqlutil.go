package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// scanner is satisfied by pgx.Row and pgx.Rows.
type scanner interface {
	Scan(dest ...any) error
}

// rowQuerier is satisfied by *pgxpool.Pool and pgx.Tx, so helpers that read
// one row can run either on the pool or inside a caller's transaction.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// fmtError wraps err with the operation as context.
func fmtError(op string, err error) error {
	return fmt.Errorf("db: %s: %w", op, err)
}

// errBadDigest marks a stored api-key digest of the wrong length; the schema
// forbids it, so it indicates corruption.
var errBadDigest = errors.New("db: api key digest is not 32 bytes")

// runTx runs fn inside one transaction on the Store pool. The transaction is
// rolled back on error; this is the only transaction helper in the package
// and every Store method with more than one statement goes through it.
func (s *Store) runTx(ctx context.Context, name string, fn func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("db: %s: begin: %w", name, err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // no-op after commit
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("db: %s: commit: %w", name, err)
	}
	return nil
}

// errNoRows converts pgx.ErrNoRows into [ErrNotFound], wrapping anything
// else with op as context.
func errNoRows(op string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return fmt.Errorf("db: %s: %w", op, err)
}

// isUniqueViolation reports whether err is a unique-constraint violation on
// the named constraint ("" matches any).
func isUniqueViolation(err error, constraint string) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok || pgErr.Code != "23505" {
		return false
	}
	return constraint == "" || pgErr.ConstraintName == constraint
}

// marshalStringMap encodes m as a JSON object for a jsonb column.
func marshalStringMap(m map[string]string) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	b, err := jsonMarshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// unmarshalStringMap decodes a jsonb column value into a map. A nil or empty
// value yields a non-nil empty map.
func unmarshalStringMap(raw []byte) (map[string]string, error) {
	m := make(map[string]string, 8)
	if len(raw) == 0 {
		return m, nil
	}
	if err := jsonUnmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("db: decode jsonb map: %w", err)
	}
	return m, nil
}

// escapeLike escapes SQL LIKE metacharacters so the pattern matches
// literally. The caller appends its own wildcard.
func escapeLike(s string) string {
	var b []byte
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\', '%', '_':
			b = append(b, '\\')
		}
		b = append(b, s[i])
	}
	return string(b)
}

// clamp bounds v into [lo, hi], substituting def when v is zero.
func clamp(v, lo, hi, def int) int {
	switch {
	case v == 0:
		return def
	case v < lo:
		return lo
	case v > hi:
		return hi
	default:
		return v
	}
}
