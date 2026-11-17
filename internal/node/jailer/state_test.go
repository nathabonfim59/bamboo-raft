package jailer

import (
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

// TestStateWriteFaultOutcomes injects a failure at every durable record
// boundary and checks the writer's own classification: proven-clean
// failures carry errRecordNotPublished and leave no attempt entry; every
// other outcome is uncertain, and a possibly published inode is reported.
func TestStateWriteFaultOutcomes(t *testing.T) {
	tests := []struct {
		fault      stateFault
		cleanup    bool
		clean      bool
		published  bool
		leftovers  int
		identified bool
	}{
		{fault: faultStateCreate, clean: true},
		{fault: faultStateWrite, clean: true},
		{fault: faultStateSync, clean: true},
		{fault: faultStateClose, clean: true},
		{fault: faultStateWrite, cleanup: true, leftovers: 1},
		{fault: faultStateIdentity, leftovers: 1},
		{fault: faultStateRename, leftovers: 1},
		{fault: faultStateParentSync, published: true, identified: true},
	}
	for _, tt := range tests {
		name := string(tt.fault)
		if tt.cleanup {
			name += "+cleanup"
		}
		t.Run(name, func(t *testing.T) {
			root, _, stateDir := jailerReviewRoots(t)
			store := fileStates{fault: func(stage stateFault) error {
				if stage == tt.fault || (tt.cleanup && stage == faultStateCleanup) {
					return errJailerInjected
				}
				return nil
			}}
			identity, err := store.Write(root, jailerReviewRecord(jailerReviewID, 50000), resourceOwnership{}, inodeIdentity{})
			if !errors.Is(err, errJailerInjected) {
				t.Fatalf("Write = %v, want the injected failure", err)
			}
			if got := errors.Is(err, errRecordNotPublished); got != tt.clean {
				t.Fatalf("clean = %v, want %v (%v)", got, tt.clean, err)
			}
			if identity.valid() != tt.identified {
				t.Fatalf("identity valid = %v, want %v", identity.valid(), tt.identified)
			}
			current, statErr := statRegularNoFollow(root, recordPath(jailerReviewID))
			if (statErr == nil) != tt.published {
				t.Fatalf("published = %v, want %v", statErr == nil, tt.published)
			}
			if tt.published && !current.equal(identity) {
				t.Fatal("reported identity is not the published inode")
			}
			entries, err := os.ReadDir(filepath.Join(stateDir, recordsDir))
			if err != nil {
				t.Fatal(err)
			}
			temps := 0
			for _, entry := range entries {
				if strings.Contains(entry.Name(), ".json.tmp-") {
					temps++
				}
			}
			if temps != tt.leftovers {
				t.Fatalf("%d attempt temps remain, want %d", temps, tt.leftovers)
			}
		})
	}
}

// TestStateWriteRenameKeepsOlderRecord: a failed replacement never adopts or
// removes the previously published record.
func TestStateWriteRenameKeepsOlderRecord(t *testing.T) {
	root, _, _ := jailerReviewRoots(t)
	first, err := (fileStates{}).Write(root, jailerReviewRecord(jailerReviewID, 50000), resourceOwnership{}, inodeIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	store := fileStates{fault: func(stage stateFault) error {
		if stage == faultStateRename {
			return errJailerInjected
		}
		return nil
	}}
	identity, err := store.Write(root, jailerReviewRecord(jailerReviewID, 50000), resourceOwnership{}, first)
	if err == nil || identity.valid() {
		t.Fatalf("Write = %v, %v", identity, err)
	}
	current, err := statRegularNoFollow(root, recordPath(jailerReviewID))
	if err != nil || !current.equal(first) {
		t.Fatal("the older published record was replaced or removed")
	}
}

// TestStateRemoveOwnership removes only the expected inode and retries a
// missing record as a completed removal.
func TestStateRemoveOwnership(t *testing.T) {
	root, _, _ := jailerReviewRoots(t)
	identity, err := (fileStates{}).Write(root, jailerReviewRecord(jailerReviewID, 50000), resourceOwnership{}, inodeIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := (fileStates{}).Write(root, jailerReviewRecord(jailerReviewID, 50000), resourceOwnership{}, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := (fileStates{}).Remove(root, jailerReviewID, identity); err == nil {
		t.Fatal("Remove deleted a replaced record")
	}
	if err := (fileStates{}).Remove(root, jailerReviewID, inodeIdentity{}); err == nil {
		t.Fatal("Remove deleted a record without a known identity")
	}
	if err := (fileStates{}).Remove(root, jailerReviewID, replacement); err != nil {
		t.Fatal(err)
	}
	if err := (fileStates{}).Remove(root, jailerReviewID, replacement); err != nil {
		t.Fatalf("retry after removal = %v", err)
	}
}

// TestLoadRecordsRefusesCorruptState: corrupt, oversized, misnamed and
// symlinked records stop New before any host mutation.
func TestLoadRecordsRefusesCorruptState(t *testing.T) {
	valid := func(t *testing.T, root *os.Root) []byte {
		if _, err := (fileStates{}).Write(root, jailerReviewRecord(jailerReviewID, 50000), resourceOwnership{}, inodeIdentity{}); err != nil {
			t.Fatal(err)
		}
		raw, err := root.ReadFile(recordPath(jailerReviewID))
		if err != nil {
			t.Fatal(err)
		}
		if err := root.Remove(recordPath(jailerReviewID)); err != nil {
			t.Fatal(err)
		}
		return raw
	}
	tests := []struct {
		name   string
		setup  func(t *testing.T, root *os.Root, stateDir string)
		reason string
	}{
		{name: "truncated json", setup: func(t *testing.T, root *os.Root, _ string) {
			raw := valid(t, root)
			if err := root.WriteFile(recordPath(jailerReviewID), raw[:len(raw)/2], 0o600); err != nil {
				t.Fatal(err)
			}
		}, reason: "decode record"},
		{name: "unknown schema", setup: func(t *testing.T, root *os.Root, _ string) {
			raw := strings.Replace(string(valid(t, root)), `"schemaVersion":2`, `"schemaVersion":3`, 1)
			if err := root.WriteFile(recordPath(jailerReviewID), []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
		}, reason: "schema"},
		{name: "older schema 1 is not migrated", setup: func(t *testing.T, root *os.Root, _ string) {
			raw := strings.Replace(string(valid(t, root)), `"schemaVersion":2`, `"schemaVersion":1`, 1)
			if err := root.WriteFile(recordPath(jailerReviewID), []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
		}, reason: "schema"},
		{name: "misnamed record", setup: func(t *testing.T, root *os.Root, _ string) {
			if err := root.WriteFile(recordPath(jailerReviewID2), valid(t, root), 0o600); err != nil {
				t.Fatal(err)
			}
		}, reason: "belongs to"},
		{name: "oversized record", setup: func(t *testing.T, root *os.Root, _ string) {
			if err := root.WriteFile(recordPath(jailerReviewID), make([]byte, maxRecordBytes+1), 0o600); err != nil {
				t.Fatal(err)
			}
		}, reason: "exceeds"},
		{name: "symlinked record", setup: func(t *testing.T, _ *os.Root, stateDir string) {
			if err := os.Symlink("/etc/passwd", filepath.Join(stateDir, recordPath(jailerReviewID))); err != nil {
				t.Fatal(err)
			}
		}, reason: "read record"},
		{name: "process identity without ticks", setup: func(t *testing.T, root *os.Root, _ string) {
			raw := strings.Replace(string(valid(t, root)), `"bootId"`, `"pid":42,"bootId"`, 1)
			if err := root.WriteFile(recordPath(jailerReviewID), []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
		}, reason: "complete process identity"},
		{name: "unsafe owned path", setup: func(t *testing.T, root *os.Root, _ string) {
			own := resourceOwnership{Jail: inodeIdentity{Dev: 1, Ino: 2}, Entries: []ownedEntry{{Path: "../escape", Dev: 1, Ino: 3}}}
			raw := valid(t, root)
			if _, err := (fileStates{}).Write(root, jailerReviewRecord(jailerReviewID, 50000), own, inodeIdentity{}); err == nil {
				t.Fatal("writer accepted an unsafe owned path")
			}
			bad := strings.Replace(string(raw), `"ownership":{}`, `"ownership":{"jail":{"dev":1,"ino":2},"entries":[{"path":"../escape","dev":1,"ino":3}]}`, 1)
			if err := root.WriteFile(recordPath(jailerReviewID), []byte(bad), 0o600); err != nil {
				t.Fatal(err)
			}
		}, reason: "unsafe owned jail path"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, _, stateDir := jailerReviewRoots(t)
			tt.setup(t, root, stateDir)
			_, _, _, err := loadRecords(root)
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("loadRecords = %v, want %q", err, tt.reason)
			}
		})
	}
}

// TestNewRefusesUnsafeStateLayout: a symlinked or regular-file records or
// jails directory and a non-regular lock are never accepted.
func TestNewRefusesUnsafeStateLayout(t *testing.T) {
	for _, name := range []string{recordsDir, jailsDir, stateLockName} {
		t.Run(name, func(t *testing.T) {
			cfg := jailerReviewConfig(t)
			if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(cfg.CgroupParent, 0o700); err != nil {
				t.Fatal(err)
			}
			target := t.TempDir()
			if err := os.Symlink(target, filepath.Join(cfg.StateDir, name)); err != nil {
				t.Fatal(err)
			}
			m, err := New(cfg)
			if err == nil {
				m.Close()
				t.Fatalf("New accepted a symlinked %s", name)
			}
			if entries, _ := os.ReadDir(target); len(entries) != 0 {
				t.Fatalf("New wrote through the symlinked %s", name)
			}
		})
	}
}

// TestRemoveOwnedJailRetry removes a recorded jail, tolerating entries a
// previous attempt already removed, and refuses foreign additions.
func TestRemoveOwnedJailRetry(t *testing.T) {
	root, _, _ := jailerReviewRoots(t)
	base := path.Join(jailsDir, jailerReviewID)
	if err := root.Mkdir(base, 0o700); err != nil {
		t.Fatal(err)
	}
	identity, err := statDirectoryNoFollow(root, base)
	if err != nil {
		t.Fatal(err)
	}
	own := resourceOwnership{Jail: identity}
	b := newJailBuilder(root, base, func(e ownedEntry) { own.Entries = append(own.Entries, e) }, nil)
	if err := b.mkdirAll("root/a/b"); err != nil {
		t.Fatal(err)
	}
	f, _, err := b.create("root/a/b/file", 0o600)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := b.verify(t.Context(), own.Entries); err != nil {
		t.Fatal(err)
	}
	// A foreign entry blocks removal and is preserved.
	if err := root.WriteFile(path.Join(base, "root/a/foreign"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := b.verify(t.Context(), own.Entries); err == nil {
		t.Fatal("verify accepted a foreign entry")
	}
	if err := removeOwnedJail(t.Context(), root, jailerReviewID, own, 50000); err == nil {
		t.Fatal("removal ignored a foreign entry")
	}
	if _, err := root.Stat(path.Join(base, "root/a/foreign")); err != nil {
		t.Fatalf("foreign entry removed: %v", err)
	}
	if err := root.Remove(path.Join(base, "root/a/foreign")); err != nil {
		t.Fatal(err)
	}
	// A partial earlier cleanup is retried to completion.
	if err := root.Remove(path.Join(base, "root/a/b/file")); err != nil {
		t.Fatal(err)
	}
	if err := removeOwnedJail(t.Context(), root, jailerReviewID, own, 50000); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Stat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("jail remains: %v", err)
	}
	if err := removeOwnedJail(t.Context(), root, jailerReviewID, own, 50000); err != nil {
		t.Fatalf("retry after removal = %v", err)
	}
}
