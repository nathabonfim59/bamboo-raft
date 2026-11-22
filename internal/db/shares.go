package db

// Saved-snapshot share codes: secret codes that let any account create a
// sandbox from one owner's saved-snapshot name. The code is shown once, only
// its SHA-256 is stored, and a use deploys the name's current generation.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// Share-code errors.
var (
	// ErrInvalidShareCode rejects a code outside the pinned bsh_ shape and a
	// share id outside the pinned shr_ shape.
	ErrInvalidShareCode = errors.New("db: invalid snapshot share code")
)

var (
	shareCodePattern = regexp.MustCompile(`^bsh_[a-z2-7]{52}$`)
	shareIDPattern   = regexp.MustCompile(`^shr_[a-f0-9]{32}$`)
)

// SnapshotShareStatus is the lifecycle of one share code.
type SnapshotShareStatus string

// Share statuses. A revoked code never deploys again.
const (
	SnapshotShareActive  SnapshotShareStatus = "active"
	SnapshotShareRevoked SnapshotShareStatus = "revoked"
)

// ShareCodePrefix is the display prefix of every share code.
const ShareCodePrefix = "bsh_"

// ShareCodePrefixChars is how many characters of the code the stored display
// prefix keeps (the bsh_ marker plus eight base32 characters).
const ShareCodePrefixChars = 12

// SnapshotShare is one share code of a saved snapshot. The code itself is
// never stored; CodeSHA256 is its only durable identity.
type SnapshotShare struct {
	ID         string
	OwnerID    uuid.UUID
	Name       string
	CodeSHA256 []byte
	CodePrefix string
	Status     SnapshotShareStatus
	UseCount   int64
	LastUsedAt *time.Time
	RevokedAt  *time.Time
	CreatedAt  time.Time
}

const shareCols = `id, owner_id, name, code_sha256, code_prefix, status, use_count,
	last_used_at, revoked_at, created_at`

func scanShare(row scanner) (SnapshotShare, error) {
	var s SnapshotShare
	err := row.Scan(&s.ID, &s.OwnerID, &s.Name, &s.CodeSHA256, &s.CodePrefix, &s.Status,
		&s.UseCount, &s.LastUsedAt, &s.RevokedAt, &s.CreatedAt)
	return s, err
}

// NewSnapshotShareID returns a fresh server-chosen share id: "shr_" plus 32
// lowercase hex digits.
func NewSnapshotShareID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("db: snapshot share id: %w", err)
	}
	return "shr_" + hex.EncodeToString(b[:]), nil
}

// shareCodeAlphabet is lowercase RFC 4648 base32 without padding, the share
// code's body alphabet.
var shareCodeAlphabet = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// NewSnapshotShareCode returns a fresh code: "bsh_" plus 32 random bytes in
// unpadded lowercase base32. It is returned to the caller exactly once and
// never stored.
func NewSnapshotShareCode() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("db: snapshot share code: %w", err)
	}
	return ShareCodePrefix + shareCodeAlphabet.EncodeToString(b[:]), nil
}

// ValidSnapshotShareCode reports the exact code shape the API accepts.
func ValidSnapshotShareCode(code string) bool { return shareCodePattern.MatchString(code) }

// ValidSnapshotShareID reports the exact share id shape.
func ValidSnapshotShareID(id string) bool { return shareIDPattern.MatchString(id) }

// ShareCodeDigest is the SHA-256 of a presented code. Only this digest is
// stored or compared against a stored row.
func ShareCodeDigest(code string) [sha256.Size]byte { return sha256.Sum256([]byte(code)) }

// CreateSnapshotShare makes one active code for the owner's live name and
// returns the share row together with the plaintext code. The code is never
// part of the row. A missing, deleted or foreign name is [ErrNotFound].
func (s *Store) CreateSnapshotShare(ctx context.Context, owner uuid.UUID, name string) (SnapshotShare, string, error) {
	if owner == uuid.Nil() || !ValidSnapshotName(name) {
		return SnapshotShare{}, "", ErrNotFound
	}
	code, err := NewSnapshotShareCode()
	if err != nil {
		return SnapshotShare{}, "", err
	}
	digest := ShareCodeDigest(code)
	shareID, err := NewSnapshotShareID()
	if err != nil {
		return SnapshotShare{}, "", err
	}
	var out SnapshotShare
	err = s.runTx(ctx, "create snapshot share", func(ctx context.Context, tx pgx.Tx) error {
		if err := liveSnapshotNameTx(ctx, tx, owner, name); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			INSERT INTO snapshot_shares (id, owner_id, name, code_sha256, code_prefix)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING `+shareCols,
			shareID, owner, name, digest[:], code[:ShareCodePrefixChars])
		out, err = scanShare(row)
		if err != nil {
			return fmtError("insert snapshot share", err)
		}
		return nil
	})
	if err != nil {
		return SnapshotShare{}, "", err
	}
	return out, code, nil
}

// ListSnapshotShares returns every share of the owner's name, active and
// revoked, newest first. A missing, deleted or foreign name is [ErrNotFound].
func (s *Store) ListSnapshotShares(ctx context.Context, owner uuid.UUID, name string) ([]SnapshotShare, error) {
	if owner == uuid.Nil() || !ValidSnapshotName(name) {
		return nil, ErrNotFound
	}
	var out []SnapshotShare
	err := s.runTx(ctx, "list snapshot shares", func(ctx context.Context, tx pgx.Tx) error {
		if err := liveSnapshotNameTx(ctx, tx, owner, name); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT `+shareCols+` FROM snapshot_shares
			WHERE owner_id = $1 AND name = $2
			ORDER BY created_at DESC, id DESC`, owner, name)
		if err != nil {
			return fmtError("list snapshot shares", err)
		}
		defer rows.Close()
		for rows.Next() {
			share, err := scanShare(rows)
			if err != nil {
				return fmtError("scan snapshot share", err)
			}
			out = append(out, share)
		}
		return errOrNil(rows.Err(), "list snapshot shares")
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RevokeSnapshotShare revokes one code of the owner's name and returns the
// row. Revoking an already revoked code of the same name is idempotent; a
// missing, foreign or mismatched share id is [ErrNotFound].
func (s *Store) RevokeSnapshotShare(ctx context.Context, owner uuid.UUID, name, shareID string) (SnapshotShare, error) {
	if owner == uuid.Nil() || !ValidSnapshotName(name) || !ValidSnapshotShareID(shareID) {
		return SnapshotShare{}, ErrNotFound
	}
	var out SnapshotShare
	err := s.runTx(ctx, "revoke snapshot share", func(ctx context.Context, tx pgx.Tx) error {
		if err := liveSnapshotNameTx(ctx, tx, owner, name); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			UPDATE snapshot_shares SET status = 'revoked', revoked_at = now()
			WHERE id = $1 AND owner_id = $2 AND name = $3 AND status = 'active'
			RETURNING `+shareCols, shareID, owner, name)
		var err error
		out, err = scanShare(row)
		if err == nil {
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmtError("revoke snapshot share", err)
		}
		out, err = scanShare(tx.QueryRow(ctx, `
			SELECT `+shareCols+` FROM snapshot_shares
			WHERE id = $1 AND owner_id = $2 AND name = $3`, shareID, owner, name))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmtError("read snapshot share", err)
		}
		return nil
	})
	if err != nil {
		return SnapshotShare{}, err
	}
	return out, nil
}

// SnapshotShareSource resolves a presented share code to the sharer's current
// deployable generation. A malformed, unknown or revoked code is
// [ErrSnapshotNotFound]; a live code whose name has no ready generation is the
// same answer.
func (s *Store) SnapshotShareSource(ctx context.Context, code string) (SnapshotSource, error) {
	if !ValidSnapshotShareCode(code) {
		return SnapshotSource{}, ErrSnapshotNotFound
	}
	digest := ShareCodeDigest(code)
	var (
		shareID string
		owner   uuid.UUID
		name    string
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id, owner_id, name FROM snapshot_shares
		WHERE code_sha256 = $1 AND status = 'active'`, digest[:]).Scan(&shareID, &owner, &name)
	if errors.Is(err, pgx.ErrNoRows) {
		return SnapshotSource{}, ErrSnapshotNotFound
	}
	if err != nil {
		return SnapshotSource{}, fmtError("resolve snapshot share", err)
	}
	src, err := s.snapshotRestoreSource(ctx, s.pool, owner, name)
	if err != nil {
		return SnapshotSource{}, err
	}
	src.ShareID = shareID
	return src, nil
}

// lockSnapshotShareGrantTx locks one active share and returns the owner and
// name it deploys. A missing, malformed or revoked share is
// [ErrSnapshotNotFound]; the row stays locked for the rest of the create
// transaction, so a concurrent revoke cannot slip between the check and the
// sandbox insert.
func lockSnapshotShareGrantTx(ctx context.Context, tx pgx.Tx, shareID string) (uuid.UUID, string, error) {
	if !ValidSnapshotShareID(shareID) {
		return uuid.Nil(), "", ErrSnapshotNotFound
	}
	var (
		owner uuid.UUID
		name  string
	)
	err := tx.QueryRow(ctx, `
		SELECT owner_id, name FROM snapshot_shares
		WHERE id = $1 AND status = 'active' FOR UPDATE`, shareID).Scan(&owner, &name)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil(), "", ErrSnapshotNotFound
	}
	if err != nil {
		return uuid.Nil(), "", fmtError("lock snapshot share", err)
	}
	return owner, name, nil
}

// consumeSnapshotRestoreShareTx authorizes one cross-account restore of the
// locked saved row and records its use. The share must still be active and
// deploy exactly this owner+name; anything else is a safe
// [ErrSnapshotNotFound].
func consumeSnapshotRestoreShareTx(ctx context.Context, tx pgx.Tx, shareID string, snap SavedSnapshot) error {
	owner, name, err := lockSnapshotShareGrantTx(ctx, tx, shareID)
	if err != nil {
		return err
	}
	if owner != snap.OwnerID || name != snap.Name {
		return ErrSnapshotNotFound
	}
	return useSnapshotShareTx(ctx, tx, shareID)
}

// consumeSnapshotGenerationShareTx authorizes one cross-account restore from a
// generation and records its use. The share must still deploy the name's
// current ready generation and that generation must be the one being restored;
// a re-save between resolution and create leaves the code deploying another
// generation, which the resolved source no longer matches.
func consumeSnapshotGenerationShareTx(ctx context.Context, tx pgx.Tx, shareID string,
	generationOwner uuid.UUID, generation uuid.UUID,
) error {
	owner, name, err := lockSnapshotShareGrantTx(ctx, tx, shareID)
	if err != nil {
		return err
	}
	if owner != generationOwner {
		return ErrSnapshotNotFound
	}
	var current *uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT generation_id FROM saved_snapshots
		WHERE owner_id = $1 AND name = $2 AND deleted_at IS NULL
		  AND status = 'ready' AND retired_at IS NULL
		ORDER BY generation DESC, id DESC
		LIMIT 1
		FOR SHARE`, owner, name).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrSnapshotNotFound
	}
	if err != nil {
		return fmtError("read shared snapshot generation", err)
	}
	if current == nil || *current != generation {
		return ErrSnapshotNotFound
	}
	return useSnapshotShareTx(ctx, tx, shareID)
}

// useSnapshotShareTx counts one sandbox created from the locked active share.
func useSnapshotShareTx(ctx context.Context, tx pgx.Tx, shareID string) error {
	tag, err := tx.Exec(ctx, `
		UPDATE snapshot_shares
		SET use_count = use_count + 1, last_used_at = now()
		WHERE id = $1 AND status = 'active'`, shareID)
	if err != nil {
		return fmtError("use snapshot share", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrSnapshotNotFound
	}
	return nil
}

// liveSnapshotNameTx proves the owner still has a live name. It does not
// require a ready generation: a replacement may be saving while an earlier
// ready row stays deployable.
func liveSnapshotNameTx(ctx context.Context, tx pgx.Tx, owner uuid.UUID, name string) error {
	var live bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM saved_snapshots
			WHERE owner_id = $1 AND name = $2 AND deleted_at IS NULL AND is_current)`,
		owner, name).Scan(&live); err != nil {
		return fmtError("check snapshot name", err)
	}
	if !live {
		return ErrNotFound
	}
	return nil
}

// errOrNil wraps a rows traversal error with the operation, or returns nil.
func errOrNil(err error, op string) error {
	if err != nil {
		return fmtError(op, err)
	}
	return nil
}
