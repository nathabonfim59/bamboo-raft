package db_test

import (
	"errors"
	"testing"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

func TestValidateTTLSeconds(t *testing.T) {
	positive := 60
	max := db.MaxTTLSeconds
	tooBig := max + 1
	zero := 0
	negative := -1
	tests := []struct {
		name string
		ttl  *int
		ok   bool
	}{
		{"unlimited", nil, true},
		{"positive", &positive, true},
		{"max", &max, true},
		{"zero", &zero, false},
		{"negative", &negative, false},
		{"above max", &tooBig, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := db.ValidateTTLSeconds(tc.ttl)
			if tc.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tc.ok && !errors.Is(err, db.ErrInvalidTTL) {
				t.Fatalf("error = %v, want ErrInvalidTTL", err)
			}
		})
	}
}

// TestCreateSandboxTTLExact proves the duration source is computed from the
// same database now() as created_at: expires_at - created_at is exactly the
// requested TTL regardless of application clock or placement delay.
func TestCreateSandboxTTLExact(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	ttl := 123
	res, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: "node-a",
		Reserve:    db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1},
		TTLSeconds: &ttl,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Sandbox.ExpiresAt == nil {
		t.Fatal("TTL create has no expiry")
	}
	if got := res.Sandbox.ExpiresAt.Sub(res.Sandbox.CreatedAt); got != time.Duration(ttl)*time.Second {
		t.Fatalf("returned expires-created = %s, want %ds", got, ttl)
	}
	var (
		exact  bool
		exact2 bool
		expiry time.Time
	)
	err = s.Pool().QueryRow(t.Context(), `
		SELECT (expires_at - created_at) = make_interval(secs => $2::integer),
		       expires_at = created_at + make_interval(secs => $2::integer),
		       expires_at
		FROM sandboxes WHERE id = $1`,
		res.Sandbox.ID, ttl).Scan(&exact, &exact2, &expiry)
	if err != nil {
		t.Fatal(err)
	}
	if !exact || !exact2 {
		t.Fatal("expires_at was not computed from created_at + ttl")
	}
	if !expiry.Equal(*res.Sandbox.ExpiresAt) {
		t.Fatalf("returned expiry %s != stored %s", res.Sandbox.ExpiresAt, expiry)
	}
}

func TestCreateSandboxTTLValidation(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	base := db.CreateSandboxParams{
		Owner: own, NodeID: "node-a",
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1},
	}
	positive := 60
	max := db.MaxTTLSeconds
	tooBig := max + 1
	zero := 0
	negative := -1
	future := time.Now().Add(time.Hour)
	tests := []struct {
		name string
		ttl  *int
		exp  *time.Time
	}{
		{"zero", &zero, nil},
		{"negative", &negative, nil},
		{"above max", &tooBig, nil},
		{"duration and absolute", &positive, &future},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := base
			p.ID = ids.Sandbox()
			p.TTLSeconds = tc.ttl
			p.ExpiresAt = tc.exp
			if _, err := s.CreateSandbox(t.Context(), p); !errors.Is(err, db.ErrInvalidTTL) {
				t.Fatalf("error = %v, want ErrInvalidTTL", err)
			}
		})
	}
	if n := count(t, s, `SELECT count(*) FROM sandboxes`); n != 0 {
		t.Fatalf("invalid TTL creates wrote %d sandbox rows", n)
	}

	ok := max
	if _, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: "node-a",
		Reserve:    db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1},
		TTLSeconds: &ok,
	}); err != nil {
		t.Fatalf("max TTL rejected: %v", err)
	}
}

func TestCreateSandboxUnlimitedAndLegacyExpiry(t *testing.T) {
	s := dbtestOpen(t)
	own := owner(t, s)
	node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
	unlimited, err := s.CreateSandbox(t.Context(), db.CreateSandboxParams{
		ID: ids.Sandbox(), Owner: own, NodeID: "node-a",
		Reserve: db.Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if unlimited.Sandbox.ExpiresAt != nil {
		t.Fatalf("nil TTL produced expiry %s", unlimited.Sandbox.ExpiresAt)
	}
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
		t.Fatalf("legacy expiry = %s, want %s", legacy.Sandbox.ExpiresAt, deadline)
	}
}
