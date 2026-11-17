package storage

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// TestListQuarantinedIDsNameAndTypeCases pins the read-only orphan
// inspection: only directory names that parse as "<valid-sandbox-id>-..."
// are reported (sorted, de-duplicated), and nothing else under orphans/ is
// followed, trusted or removed — not files, not symlinks pointing at real
// directories, not malformed or non-sandbox names.
func TestListQuarantinedIDsNameAndTypeCases(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir(), ImageDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	root := filepath.Join(store.DataDir(), "orphans")

	outsider := t.TempDir() // a real directory a symlink could point at
	for _, tc := range []struct {
		name string
		kind string // "dir", "file", "symlink"
	}{
		{"bx_22222222-1794432573-00112233", "dir"},
		{"bx_22222222-1794439999-deadbeef", "dir"}, // same id twice: dedupe
		{"bx_33333333-1794432573-00112233", "dir"},
		{"bx_12222222-1794432573-00112233", "dir"}, // invalid sandbox id (1 not in the id alphabet)
		{"garbage", "dir"},                         // no quarantine suffix
		{"-1794432573-00112233", "dir"},            // empty id prefix
		{"bx_44444444-1794432573-aa", "file"},      // regular file, not a dir
		{"bx_55555555-1794432573-aa", "symlink"},   // symlink to a real dir
	} {
		path := filepath.Join(root, tc.name)
		switch tc.kind {
		case "dir":
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		case "file":
			if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		case "symlink":
			if err := os.Symlink(outsider, path); err != nil {
				t.Fatal(err)
			}
		}
	}

	got, err := store.ListQuarantinedIDs()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"bx_22222222", "bx_33333333"}; !slices.Equal(got, want) {
		t.Fatalf("ListQuarantinedIDs() = %v, want %v", got, want)
	}
	// Nothing was followed or removed: the symlink and every foreign entry
	// are exactly as they were.
	if _, err := os.Stat(filepath.Join(root, "bx_55555555-1794432573-aa")); err != nil {
		t.Fatalf("the symlink entry did not survive the inspection: %v", err)
	}
	for _, name := range []string{"garbage", "bx_12222222-1794432573-00112233", "bx_44444444-1794432573-aa"} {
		if _, err := os.Lstat(filepath.Join(root, name)); err != nil {
			t.Fatalf("entry %s did not survive the inspection: %v", name, err)
		}
	}
}

// TestListQuarantinedIDsEmpty proves a fresh data directory reports nothing.
func TestListQuarantinedIDsEmpty(t *testing.T) {
	store, err := Open(Config{DataDir: t.TempDir(), ImageDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	got, err := store.ListQuarantinedIDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("ListQuarantinedIDs() = %v, want none", got)
	}
}
