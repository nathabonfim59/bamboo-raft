package db

// PATCH persistence (B1.1): sandbox name, TTL and subdomain updates that
// distinguish an omitted field from an explicit null. The three states matter
// for TTL: omitted keeps the current auto-stop configuration, null disables
// it, and a value replaces both the configured duration and its deadline.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"uuid"
)

// MaxSandboxNameLength is the pinned PATCH name limit, in Unicode
// characters. Longer names are rejected; the API may truncate before calling.
const MaxSandboxNameLength = 120

// MaxSubdomainLength is the pinned subdomain limit in bytes (the allowed
// character set is ASCII, so bytes and characters coincide).
const MaxSubdomainLength = 40

// MinSubdomainLength is the pinned minimum subdomain length.
const MinSubdomainLength = 3

// TTLOption is a three-state TTL field for PATCH and resume. Set=false means
// the request omitted the field and the stored value is kept. Set=true with
// Seconds=nil means an explicit JSON null, disabling auto-stop. Set=true
// with Seconds set is a new duration, which also resets the deadline.
type TTLOption struct {
	Set     bool
	Seconds *int
}

// EnvChange is a three-state env field for resume. Set=false keeps the
// existing sealed env untouched. Set=true replaces it, including an explicit
// empty map (which seals to no env at all).
type EnvChange struct {
	Set    bool
	Values map[string]string
}

// ValidateName checks a sandbox display name. Empty names and names longer
// than [MaxSandboxNameLength] Unicode characters answer [ErrInvalidName].
func ValidateName(name string) error {
	if name == "" || utf8.RuneCountInString(name) > MaxSandboxNameLength {
		return ErrInvalidName
	}
	return nil
}

// ValidateSubdomain checks the pinned subdomain shape: 3..40 lowercase ASCII
// letters, digits and single hyphens; no leading, trailing or double
// hyphens; and no `-desktop` or `-<number>` suffix (reserved for desktop and
// hosted-port routes). Violations answer [ErrInvalidSubdomain].
func ValidateSubdomain(subdomain string) error {
	if len(subdomain) < MinSubdomainLength || len(subdomain) > MaxSubdomainLength {
		return ErrInvalidSubdomain
	}
	if !isLowerAlnum(subdomain[0]) || !isLowerAlnum(subdomain[len(subdomain)-1]) {
		return ErrInvalidSubdomain
	}
	for i := 1; i < len(subdomain)-1; i++ {
		c := subdomain[i]
		if c == '-' {
			if subdomain[i-1] == '-' {
				return ErrInvalidSubdomain
			}
			continue
		}
		if !isLowerAlnum(c) {
			return ErrInvalidSubdomain
		}
	}
	if strings.HasSuffix(subdomain, "-desktop") {
		return ErrInvalidSubdomain
	}
	if _, suffix, ok := strings.CutLast(subdomain, "-"); ok && suffix != "" && isDigits(suffix) {
		return ErrInvalidSubdomain
	}
	return nil
}

func isLowerAlnum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}

func isDigits(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// PatchSandboxParams is one owner-scoped PATCH. Nil Name and Subdomain mean
// "omitted"; TTL.Set says whether the request carried the field at all.
type PatchSandboxParams struct {
	Owner     uuid.UUID
	SandboxID string
	Name      *string
	TTL       TTLOption
	Subdomain *string
}

// PatchSandbox applies a PATCH atomically and returns the updated row. Name
// must be non-empty and at most [MaxSandboxNameLength] Unicode characters
// ([ErrInvalidName]). A TTL value must be in the create range
// ([ErrInvalidTTL]); it stores the configured duration and resets the
// deadline from the database clock, while null clears both. A subdomain is
// validated ([ErrInvalidSubdomain]) and globally unique among live sandboxes
// ([ErrSubdomainTaken]). An empty patch returns the current row unchanged.
func (s *Store) PatchSandbox(ctx context.Context, p PatchSandboxParams) (Sandbox, error) {
	if p.Owner == uuid.Nil() {
		return Sandbox{}, ErrNoOwner
	}
	if p.Name == nil && !p.TTL.Set && p.Subdomain == nil {
		return s.Sandbox(ctx, p.Owner, p.SandboxID)
	}
	args := []any{p.SandboxID, p.Owner}
	sets := []string{"version = version + 1", "updated_at = now()"}
	if p.Name != nil {
		if err := ValidateName(*p.Name); err != nil {
			return Sandbox{}, err
		}
		args = append(args, *p.Name)
		sets = append(sets, fmt.Sprintf("name = $%d", len(args)))
	}
	if p.TTL.Set {
		if p.TTL.Seconds != nil {
			if err := ValidateTTLSeconds(p.TTL.Seconds); err != nil {
				return Sandbox{}, err
			}
			args = append(args, *p.TTL.Seconds)
			n := len(args)
			sets = append(sets,
				fmt.Sprintf("ttl_seconds = $%d", n),
				fmt.Sprintf("expires_at = now() + make_interval(secs => $%d::integer)", n))
		} else {
			sets = append(sets, "ttl_seconds = NULL", "expires_at = NULL")
		}
	}
	if p.Subdomain != nil {
		if err := ValidateSubdomain(*p.Subdomain); err != nil {
			return Sandbox{}, err
		}
		args = append(args, *p.Subdomain)
		sets = append(sets, fmt.Sprintf("subdomain = $%d", len(args)))
	}

	row := s.pool.QueryRow(ctx,
		`UPDATE sandboxes SET `+strings.Join(sets, ", ")+`
		 WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL
		 RETURNING `+sandboxCols,
		args...)
	sbx, err := scanSandbox(row)
	if isUniqueViolation(err, "sandboxes_subdomain_live_uk") {
		return Sandbox{}, ErrSubdomainTaken
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return Sandbox{}, ErrNotFound
	}
	if err != nil {
		return Sandbox{}, fmtError("patch sandbox", err)
	}
	return sbx, nil
}
