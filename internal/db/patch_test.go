package db_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

func TestValidateNameAndSubdomain(t *testing.T) {
	t.Parallel()
	nameTests := []struct {
		name  string
		value string
		ok    bool
	}{
		{"normal", "Customer support run", true},
		{"empty", "", false},
		{"120 ascii", strings.Repeat("a", 120), true},
		{"121 ascii", strings.Repeat("a", 121), false},
		{"120 unicode characters", strings.Repeat("\u00e9", 120), true},
		{"121 unicode characters", strings.Repeat("\u00e9", 121), false},
	}
	for _, tc := range nameTests {
		t.Run("name/"+tc.name, func(t *testing.T) {
			err := db.ValidateName(tc.value)
			if tc.ok && err != nil {
				t.Fatalf("ValidateName(%q) = %v", tc.value, err)
			}
			if !tc.ok && !errors.Is(err, db.ErrInvalidName) {
				t.Fatalf("ValidateName(%q) = %v, want ErrInvalidName", tc.value, err)
			}
		})
	}

	subdomainTests := []struct {
		name  string
		value string
		ok    bool
	}{
		{"minimum", "abc", true},
		{"words", "acme-staging", true},
		{"digits", "a1-b2", true},
		{"mixed suffix", "abc-1x", true},
		{"desktop word", "abc-desktopx", true},
		{"too short", "ab", false},
		{"too long", strings.Repeat("a", 41), false},
		{"uppercase", "Acme", false},
		{"underscore", "acme_1", false},
		{"leading hyphen", "-acme", false},
		{"trailing hyphen", "acme-", false},
		{"double hyphen", "acme--staging", false},
		{"desktop suffix", "acme-desktop", false},
		{"numeric suffix", "acme-8080", false},
		{"dot", "acme.dev", false},
	}
	for _, tc := range subdomainTests {
		t.Run("subdomain/"+tc.name, func(t *testing.T) {
			err := db.ValidateSubdomain(tc.value)
			if tc.ok && err != nil {
				t.Fatalf("ValidateSubdomain(%q) = %v", tc.value, err)
			}
			if !tc.ok && !errors.Is(err, db.ErrInvalidSubdomain) {
				t.Fatalf("ValidateSubdomain(%q) = %v, want ErrInvalidSubdomain", tc.value, err)
			}
		})
	}
}

func TestPatchSandboxNameTTLAndSubdomain(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	ttl := 60
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{TTLSeconds: &ttl})
	original, err := s.Sandbox(t.Context(), own, sbx.ID)
	if err != nil {
		t.Fatal(err)
	}

	// An empty patch is a read: no version bump.
	same, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: own, SandboxID: sbx.ID})
	if err != nil || same.Version != original.Version {
		t.Fatalf("empty patch: version=%d err=%v", same.Version, err)
	}

	name := "Customer support run"
	patched, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: own, SandboxID: sbx.ID, Name: &name})
	if err != nil {
		t.Fatalf("patch name: %v", err)
	}
	if patched.Name != name || patched.Version != original.Version+1 {
		t.Fatalf("patched name: %+v", patched)
	}
	if patched.TTLSeconds == nil || *patched.TTLSeconds != ttl || patched.ExpiresAt == nil || !patched.ExpiresAt.Equal(*original.ExpiresAt) {
		t.Fatalf("name patch changed ttl: %+v %v", patched.TTLSeconds, patched.ExpiresAt)
	}

	// Unicode names count characters, not bytes.
	unicodeName := strings.Repeat("\u00e9", 120)
	if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: own, SandboxID: sbx.ID, Name: &unicodeName}); err != nil {
		t.Fatalf("120-character unicode name: %v", err)
	}
	empty := ""
	if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: own, SandboxID: sbx.ID, Name: &empty}); !errors.Is(err, db.ErrInvalidName) {
		t.Fatalf("empty name: %v", err)
	}
	tooLong := strings.Repeat("a", 121)
	if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: own, SandboxID: sbx.ID, Name: &tooLong}); !errors.Is(err, db.ErrInvalidName) {
		t.Fatalf("long name: %v", err)
	}

	// TTL value replaces the configured duration and resets the deadline.
	value := 120
	patched, err = s.PatchSandbox(t.Context(), db.PatchSandboxParams{
		Owner: own, SandboxID: sbx.ID, TTL: db.TTLOption{Set: true, Seconds: &value},
	})
	if err != nil {
		t.Fatalf("patch ttl: %v", err)
	}
	if patched.TTLSeconds == nil || *patched.TTLSeconds != value || patched.ExpiresAt == nil {
		t.Fatalf("ttl value: %v %v", patched.TTLSeconds, patched.ExpiresAt)
	}
	if d := time.Until(*patched.ExpiresAt); d < 110*time.Second || d > 130*time.Second {
		t.Fatalf("ttl deadline in %s", d)
	}
	// Explicit null disables auto-stop.
	patched, err = s.PatchSandbox(t.Context(), db.PatchSandboxParams{
		Owner: own, SandboxID: sbx.ID, TTL: db.TTLOption{Set: true},
	})
	if err != nil {
		t.Fatalf("patch null ttl: %v", err)
	}
	if patched.TTLSeconds != nil || patched.ExpiresAt != nil {
		t.Fatalf("null ttl: %v %v", patched.TTLSeconds, patched.ExpiresAt)
	}
	// Out-of-range values are rejected without touching the row.
	zero := 0
	negative := -1
	tooBig := db.MaxTTLSeconds + 1
	for _, invalid := range []*int{&zero, &negative, &tooBig} {
		if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{
			Owner: own, SandboxID: sbx.ID, TTL: db.TTLOption{Set: true, Seconds: invalid},
		}); !errors.Is(err, db.ErrInvalidTTL) {
			t.Fatalf("invalid ttl %d: %v", *invalid, err)
		}
	}

	// Subdomain validation, global uniqueness and release on deletion.
	sub := "acme-staging"
	if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: own, SandboxID: sbx.ID, Subdomain: &sub}); err != nil {
		t.Fatalf("patch subdomain: %v", err)
	}
	if same, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: own, SandboxID: sbx.ID, Subdomain: &sub}); err != nil || same.Subdomain == nil || *same.Subdomain != sub {
		t.Fatalf("re-set own subdomain: %+v err=%v", same, err)
	}
	for _, invalid := range []string{"ab", "Acme", "acme--x", "acme-8080", "acme-desktop", "acme_1"} {
		bad := invalid
		if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: own, SandboxID: sbx.ID, Subdomain: &bad}); !errors.Is(err, db.ErrInvalidSubdomain) {
			t.Fatalf("invalid subdomain %q: %v", invalid, err)
		}
	}

	second := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
	if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: own, SandboxID: second.ID, Subdomain: &sub}); !errors.Is(err, db.ErrSubdomainTaken) {
		t.Fatalf("same-owner duplicate: %v", err)
	}
	other := owner(t, s)
	third := phase1CreateReady(t, s, other, db.CreateSandboxParams{})
	if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: other, SandboxID: third.ID, Subdomain: &sub}); !errors.Is(err, db.ErrSubdomainTaken) {
		t.Fatalf("cross-owner duplicate: %v", err)
	}
	otherSub := "other-label"
	if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: other, SandboxID: third.ID, Subdomain: &otherSub}); err != nil {
		t.Fatalf("other owner subdomain: %v", err)
	}

	if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: other, SandboxID: sbx.ID}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign patch: %v", err)
	}

	// Deleting the holder releases the label for another live sandbox.
	op, _, err := s.AcceptSandboxDeletion(t.Context(), own, sbx.ID, db.DeletionExplicit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteDeletion(t.Context(), own, op.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: own, SandboxID: second.ID, Subdomain: &sub}); err != nil {
		t.Fatalf("subdomain after delete: %v", err)
	}
	if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{Owner: own, SandboxID: sbx.ID, Subdomain: &otherSub}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("deleted sandbox patch: %v", err)
	}
}

// TestPatchSandboxOwnerIsolation proves a foreign PATCH with real changes is
// not found and leaves the owner's row, version and deadline untouched.
func TestPatchSandboxOwnerIsolation(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	other := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	ttl := 60
	sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{Name: "before", TTLSeconds: &ttl})
	before, err := s.Sandbox(t.Context(), own, sbx.ID)
	if err != nil {
		t.Fatal(err)
	}

	name := "stolen"
	foreignTTL := 999
	if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{
		Owner: other, SandboxID: sbx.ID, Name: &name,
		TTL: db.TTLOption{Set: true, Seconds: &foreignTTL},
	}); !errors.Is(err, db.ErrNotFound) {
		t.Fatalf("foreign patch: %v", err)
	}
	after, err := s.Sandbox(t.Context(), own, sbx.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Name != before.Name || after.Version != before.Version ||
		after.TTLSeconds == nil || *after.TTLSeconds != ttl ||
		after.ExpiresAt == nil || !after.ExpiresAt.Equal(*before.ExpiresAt) {
		t.Fatalf("foreign patch changed the row: %+v", after)
	}
}

// TestCreateSandboxLegacyExpiryKeepsNoConfiguredTTL proves the deliberate
// choice: a legacy absolute ExpiresAt is stored as a deadline without a
// configured duration (TTLSeconds nil), distinct from the migration backfill
// of exact integral durations.
func TestCreateSandboxLegacyExpiryKeepsNoConfiguredTTL(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})

	deadline := time.Now().Add(10 * time.Minute).UTC().Truncate(time.Microsecond)
	legacy, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: "node-a",
		Reserve:   db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1},
		ExpiresAt: &deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Sandbox.ExpiresAt == nil || !legacy.Sandbox.ExpiresAt.Equal(deadline) {
		t.Fatalf("legacy deadline: %v", legacy.Sandbox.ExpiresAt)
	}
	if legacy.Sandbox.TTLSeconds != nil {
		t.Fatalf("legacy ExpiresAt stored configured ttl %v", *legacy.Sandbox.TTLSeconds)
	}
	var ttlSeconds *int
	if err := s.Pool().QueryRow(t.Context(), `SELECT ttl_seconds FROM sandboxes WHERE id = $1`, legacy.Sandbox.ID).Scan(&ttlSeconds); err != nil {
		t.Fatal(err)
	}
	if ttlSeconds != nil {
		t.Fatalf("legacy row ttl_seconds = %v", *ttlSeconds)
	}

	configured := 99
	res, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: "node-a",
		Reserve:    db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1},
		TTLSeconds: &configured,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Sandbox.TTLSeconds == nil || *res.Sandbox.TTLSeconds != configured {
		t.Fatalf("configured ttl: %v", res.Sandbox.TTLSeconds)
	}
}
