package db_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/db"
)

// phase1SchemaVersion reads the applied goose version of the test schema.
func phase1SchemaVersion(t *testing.T, s *db.Store) int64 {
	t.Helper()
	var version int64
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT version_id FROM goose_db_version WHERE is_applied ORDER BY id DESC LIMIT 1`).Scan(&version); err != nil {
		t.Fatalf("schema version: %v", err)
	}
	return version
}

// phase1Provider builds a goose provider over the test's own schema.
func phase1Provider(t *testing.T, s *db.Store) *goose.Provider {
	t.Helper()
	gsql := stdlib.OpenDBFromPool(s.Pool())
	t.Cleanup(func() { _ = gsql.Close() })
	provider, err := goose.NewProvider(goose.DialectPostgres, gsql, os.DirFS("migrations"))
	if err != nil {
		t.Fatalf("goose provider: %v", err)
	}
	return provider
}

// TestPhase1MigrationBackfill proves existing finite TTLs keep auto-stop
// across the upgrade when they are exact integral durations, and that a
// populated Phase-0.5-representable schema still rolls back through 00004.
func TestPhase1MigrationBackfill(t *testing.T) {
	s := dbtestOpen(t)
	provider := phase1Provider(t, s)
	if _, err := provider.DownTo(t.Context(), 3); err != nil {
		t.Fatalf("down to 3: %v", err)
	}
	own := owner(t, s)
	base := time.Now().UTC().Truncate(time.Microsecond)
	fixtures := []struct {
		id      string
		offset  string
		vcpus   int
		memory  int64
		disk    int64
		wantTTL *int
	}{
		{"bx_legacy01", "123 seconds", 3, 3072, 31, new(123)},
		{"bx_legacy02", "1.5 seconds", 1, 512, 7, nil},
		{"bx_legacy03", "2592001 seconds", 2, 2048, 12, nil},
		{"bx_legacy04", "0 seconds", 4, 4096, 40, nil},
	}
	for _, f := range fixtures {
		if _, err := s.Pool().Exec(t.Context(), `
			INSERT INTO sandboxes (id, owner_id, state, machine_type, name,
				reserved_vcpus, reserved_memory_mib, reserved_disk_gib, created_at, expires_at)
			VALUES ($1, $2, 'ready', 'default', $3, $4, $5, $6, $7, $7::timestamptz + $8::interval)`,
			f.id, own, "legacy-"+f.id, f.vcpus, f.memory, f.disk, base, f.offset); err != nil {
			t.Fatalf("insert %s: %v", f.id, err)
		}
	}

	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatalf("up: %v", err)
	}
	for _, f := range fixtures {
		var (
			gotTTL                       *int
			vcpus, memory, disk, desired int64
		)
		if err := s.Pool().QueryRow(t.Context(), `
			SELECT ttl_seconds, desired_vcpus, desired_memory_mib, desired_disk_gib,
				reserved_vcpus + reserved_memory_mib + reserved_disk_gib
			FROM sandboxes WHERE id = $1`, f.id).Scan(&gotTTL, &vcpus, &memory, &disk, &desired); err != nil {
			t.Fatalf("backfill %s: %v", f.id, err)
		}
		switch {
		case f.wantTTL == nil && gotTTL != nil:
			t.Fatalf("%s ttl = %d, want NULL", f.id, *gotTTL)
		case f.wantTTL != nil && (gotTTL == nil || *gotTTL != *f.wantTTL):
			t.Fatalf("%s ttl = %v, want %d", f.id, gotTTL, *f.wantTTL)
		}
		if vcpus != int64(f.vcpus) || memory != f.memory || disk != f.disk {
			t.Fatalf("%s desired backfill = %d/%d/%d, want %d/%d/%d",
				f.id, vcpus, memory, disk, f.vcpus, f.memory, f.disk)
		}
		if wantSum := int64(f.vcpus) + f.memory + f.disk; desired != wantSum {
			t.Fatalf("%s reserved total = %d, want %d", f.id, desired, wantSum)
		}
	}

	// A populated but Phase-0.5-representable schema rolls back: backfilled
	// TTLs and zeroed reservations do not block it.
	if _, err := s.Pool().Exec(t.Context(), `
		UPDATE sandboxes SET reserved_vcpus = 0, reserved_memory_mib = 0, reserved_disk_gib = 0
		WHERE id = 'bx_legacy01'`); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.DownTo(t.Context(), 2); err != nil {
		t.Fatalf("populated down to 2: %v", err)
	}
	if _, err := provider.Up(t.Context()); err != nil {
		t.Fatalf("up after populated rollback: %v", err)
	}
	var ttl *int
	if err := s.Pool().QueryRow(t.Context(),
		`SELECT ttl_seconds FROM sandboxes WHERE id = 'bx_legacy01'`).Scan(&ttl); err != nil {
		t.Fatal(err)
	}
	if ttl == nil || *ttl != 123 {
		t.Fatalf("ttl after re-up: %v", ttl)
	}
}

// TestPhase1MigrationDownRefusals proves unrepresentable Phase 1 state stops
// a rollback with its intended reason while leaving schema and data intact;
// the safe cases above still roll back.
func TestPhase1MigrationDownRefusals(t *testing.T) {
	cases := []struct {
		name    string
		message string
		apply   func(t *testing.T, s *db.Store, own uuid.UUID) string
		verify  func(t *testing.T, s *db.Store, sandboxID string)
	}{
		{
			name:    "stop operation",
			message: "stop operations exist",
			apply: func(t *testing.T, s *db.Store, own uuid.UUID) string {
				node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
				sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
				if _, _, _, _, err := s.AcceptSandboxStop(t.Context(), own, sbx.ID, false); err != nil {
					t.Fatal(err)
				}
				return sbx.ID
			},
			verify: func(t *testing.T, s *db.Store, sandboxID string) {
				if n := count(t, s, `SELECT count(*) FROM stop_operations WHERE sandbox_id = $1`, sandboxID); n != 1 {
					t.Fatalf("stop operation lost after refused rollback: %d", n)
				}
			},
		},
		{
			name:    "live subdomain",
			message: "live stopped or named sandbox exists",
			apply: func(t *testing.T, s *db.Store, own uuid.UUID) string {
				node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
				sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
				sub := "live-label"
				if _, err := s.PatchSandbox(t.Context(), db.PatchSandboxParams{
					Owner: own, SandboxID: sbx.ID, Subdomain: &sub,
				}); err != nil {
					t.Fatal(err)
				}
				return sbx.ID
			},
			verify: func(t *testing.T, s *db.Store, sandboxID string) {
				var sub *string
				if err := s.Pool().QueryRow(t.Context(),
					`SELECT subdomain FROM sandboxes WHERE id = $1`, sandboxID).Scan(&sub); err != nil {
					t.Fatal(err)
				}
				if sub == nil || *sub != "live-label" {
					t.Fatalf("subdomain lost after refused rollback: %v", sub)
				}
			},
		},
		{
			name:    "resume generation",
			message: "a resume generation exists",
			apply: func(t *testing.T, s *db.Store, own uuid.UUID) string {
				node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
				sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
				if _, err := s.Pool().Exec(t.Context(),
					`UPDATE sandboxes SET lifecycle_generation = 1 WHERE id = $1`, sbx.ID); err != nil {
					t.Fatal(err)
				}
				return sbx.ID
			},
			verify: func(t *testing.T, s *db.Store, sandboxID string) {
				var generation int64
				if err := s.Pool().QueryRow(t.Context(),
					`SELECT lifecycle_generation FROM sandboxes WHERE id = $1`, sandboxID).Scan(&generation); err != nil {
					t.Fatal(err)
				}
				if generation != 1 {
					t.Fatalf("generation after refused rollback: %d", generation)
				}
			},
		},
		{
			name:    "live archived sandbox",
			message: "live stopped or named sandbox exists",
			apply: func(t *testing.T, s *db.Store, own uuid.UUID) string {
				node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
				sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
				if _, err := s.Pool().Exec(t.Context(),
					`UPDATE sandboxes SET state = 'archived', stopped_at = now() WHERE id = $1`, sbx.ID); err != nil {
					t.Fatal(err)
				}
				return sbx.ID
			},
			verify: func(t *testing.T, s *db.Store, sandboxID string) {
				var state string
				if err := s.Pool().QueryRow(t.Context(),
					`SELECT state FROM sandboxes WHERE id = $1`, sandboxID).Scan(&state); err != nil {
					t.Fatal(err)
				}
				if state != "archived" {
					t.Fatalf("state after refused rollback: %s", state)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := dbtestOpen(t)
			own := owner(t, s)
			sandboxID := tc.apply(t, s, own)
			provider := phase1Provider(t, s)
			_, err := provider.DownTo(t.Context(), 3)
			if err == nil {
				t.Fatalf("rollback with %s was allowed", tc.name)
			}
			if !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("rollback error %q does not contain %q", err, tc.message)
			}
			if version := phase1SchemaVersion(t, s); version != 4 {
				t.Fatalf("schema version after refused rollback: %d, want 4", version)
			}
			if n := count(t, s, `SELECT count(*) FROM sandboxes WHERE id = $1`, sandboxID); n != 1 {
				t.Fatalf("sandbox row lost after refused rollback: %d", n)
			}
			if tc.verify != nil {
				tc.verify(t, s, sandboxID)
			}
		})
	}

	// A deleted row with no live lifecycle behaviour rolls back.
	t.Run("deleted sandbox", func(t *testing.T) {
		s := dbtestOpen(t)
		own := owner(t, s)
		node(t, s, "node-a", db.Resources{VCPUs: 8, MemoryMiB: 8192, DiskGiB: 100})
		sbx := phase1CreateReady(t, s, own, db.CreateSandboxParams{})
		op, _, err := s.AcceptSandboxDeletion(t.Context(), own, sbx.ID, db.DeletionExplicit)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.CompleteDeletion(t.Context(), own, op.ID); err != nil {
			t.Fatal(err)
		}
		provider := phase1Provider(t, s)
		if _, err := provider.DownTo(t.Context(), 3); err != nil {
			t.Fatalf("deleted-row rollback: %v", err)
		}
	})
}
