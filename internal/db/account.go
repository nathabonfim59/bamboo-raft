package db

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// ErrKeyOwned refuses to move a key digest to a different account.
var ErrKeyOwned = errors.New("db: api key belongs to another account")

// ErrKeyRevoked refuses to resurrect a revoked key through configuration.
var ErrKeyRevoked = errors.New("db: api key is revoked")

// Account is one tenant. Even a single-user install has exactly one.
type Account struct {
	ID        uuid.UUID
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// APIKey is a stored credential record. KeySHA256 is the SHA-256 digest of
// the raw key; raw keys are never stored or returned by this package.
type APIKey struct {
	ID         uuid.UUID
	AccountID  uuid.UUID
	Name       string
	Prefix     string
	KeySHA256  [32]byte
	CreatedAt  time.Time
	LastUsedAt *time.Time
	RevokedAt  *time.Time
}

const accountCols = `id, name, created_at, updated_at`

func scanAccount(row scanner) (Account, error) {
	var a Account
	err := row.Scan(&a.ID, &a.Name, &a.CreatedAt, &a.UpdatedAt)
	return a, err
}

const apiKeyCols = `id, account_id, name, prefix, key_sha256, created_at, last_used_at, revoked_at`

func scanAPIKey(row scanner) (APIKey, error) {
	var (
		k APIKey
		h []byte
	)
	err := row.Scan(&k.ID, &k.AccountID, &k.Name, &k.Prefix, &h, &k.CreatedAt, &k.LastUsedAt, &k.RevokedAt)
	if err == nil && len(h) != 32 {
		return APIKey{}, errBadDigest
	}
	copy(k.KeySHA256[:], h)
	return k, err
}

// EnsureAccount returns the account named name, creating it on first use.
// It is idempotent and safe to run at every server start for each configured
// owner.
func (s *Store) EnsureAccount(ctx context.Context, name string) (Account, error) {
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO accounts (id, name) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING`,
		uuid.NewV7(), name); err != nil {
		return Account{}, fmtError("ensure account", err)
	}
	row := s.pool.QueryRow(ctx, `SELECT `+accountCols+` FROM accounts WHERE name = $1`, name)
	a, err := scanAccount(row)
	if err != nil {
		return Account{}, errNoRows("ensure account select", err)
	}
	return a, nil
}

// EnsureAPIKey makes the configured credential durable, idempotently at
// every server start: a key whose digest already exists for the same owner
// returns that row (refreshing name and prefix); the same digest under a
// different owner is refused rather than reassigned; a revoked key is never
// resurrected; anything else is inserted. Insert-or-nothing followed by one
// locked read keeps both the fresh and the replay path on the same checks
// without ever selecting inside an aborted transaction. prefix is a
// non-secret display identifier several keys may share; keySHA256 is the
// digest of the raw key.
func (s *Store) EnsureAPIKey(ctx context.Context, accountID uuid.UUID, name, prefix string, keySHA256 [32]byte) (APIKey, error) {
	var key APIKey
	err := s.runTx(ctx, "ensure api key", func(ctx context.Context, tx pgx.Tx) error {
		// ON CONFLICT DO NOTHING keeps the transaction usable on a
		// concurrent start; the locked read below then applies the same
		// owner/revoked/refresh rules to whoever won.
		if _, err := tx.Exec(ctx, `
			INSERT INTO api_keys (`+apiKeyCols+`)
			VALUES ($1, $2, $3, $4, $5, now(), NULL, NULL)
			ON CONFLICT (key_sha256) DO NOTHING`,
			uuid.NewV7(), accountID, name, prefix, keySHA256[:]); err != nil {
			return fmtError("insert api key", err)
		}
		row := tx.QueryRow(ctx,
			`SELECT `+apiKeyCols+` FROM api_keys WHERE key_sha256 = $1 FOR UPDATE`, keySHA256[:])
		var err error
		key, err = scanAPIKey(row)
		if err != nil {
			return errNoRows("ensure api key select", err)
		}
		if key.AccountID != accountID {
			return ErrKeyOwned
		}
		if key.RevokedAt != nil {
			return ErrKeyRevoked
		}
		if key.Name != name || key.Prefix != prefix {
			if _, err := tx.Exec(ctx, `UPDATE api_keys SET name = $2, prefix = $3 WHERE id = $1`,
				key.ID, name, prefix); err != nil {
				return fmtError("refresh api key", err)
			}
			key.Name, key.Prefix = name, prefix
		}
		return nil
	})
	if err != nil {
		return APIKey{}, err
	}
	return key, nil
}

// APIKeyBySHA256 returns the active key whose digest is keySHA256. Raw keys
// are hashed by the caller (internal/auth) before this lookup.
func (s *Store) APIKeyBySHA256(ctx context.Context, keySHA256 [32]byte) (APIKey, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+apiKeyCols+` FROM api_keys
		WHERE key_sha256 = $1 AND revoked_at IS NULL`, keySHA256[:])
	k, err := scanAPIKey(row)
	if err != nil {
		return APIKey{}, errNoRows("select api key", err)
	}
	return k, nil
}

// TouchAPIKeyUsed records last use for usage display.
func (s *Store) TouchAPIKeyUsed(ctx context.Context, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE api_keys SET last_used_at = now() WHERE id = $1 AND revoked_at IS NULL`, id)
	if err != nil {
		return fmtError("touch api key", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
