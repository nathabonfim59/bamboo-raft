package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// TestOpenDeclaresPrivateStoreDirs: the Store-owned private directories
// are created with the declared 0700 (what production already gets under
// the unit's UMask=0077, now independent of the umask), while an existing
// directory is used as it is and never chmod'ed.
func TestOpenDeclaresPrivateStoreDirs(t *testing.T) {
	dirs := []string{vmsDir, orphansDir, snapshotsDir, snapshotStagingDir, snapshotCommittedDir}
	t.Run("created 0700", func(t *testing.T) {
		data := t.TempDir()
		s, err := Open(Config{DataDir: data, ImageDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		for _, d := range dirs {
			info, err := os.Lstat(filepath.Join(data, d))
			if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
				t.Errorf("%s: %v, %v; want a 0700 directory", d, info, err)
			}
		}
	})
	t.Run("existing paths are not chmod'ed", func(t *testing.T) {
		data := t.TempDir()
		for _, d := range dirs {
			if err := os.Mkdir(filepath.Join(data, d), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(filepath.Join(data, d), 0o750); err != nil {
				t.Fatal(err)
			}
		}
		s, err := Open(Config{DataDir: data, ImageDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		for _, d := range dirs {
			info, err := os.Lstat(filepath.Join(data, d))
			if err != nil || info.Mode().Perm() != 0o750 {
				t.Errorf("existing %s: %v, %v; want it left at 0750", d, info, err)
			}
		}
	})
}

// TestOpenCreatesMissingRootsPrivate: a missing data or image root (and any
// missing parent Open creates for it) is created 0700, umask-independent and
// private; an existing root, such as the role's shared image root at 0555,
// is used as it is and never chmod'ed or migrated.
func TestOpenCreatesMissingRootsPrivate(t *testing.T) {
	t.Run("fresh roots", func(t *testing.T) {
		parent := t.TempDir()
		data := filepath.Join(parent, "fresh", "data")
		images := filepath.Join(parent, "fresh-images", "images")
		s, err := Open(Config{DataDir: data, ImageDir: images})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		for _, p := range []string{data, filepath.Dir(data), images, filepath.Dir(images)} {
			info, err := os.Lstat(p)
			if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
				t.Errorf("%s: %v, %v; want a fresh 0700 directory", p, info, err)
			}
		}
	})
	t.Run("existing roots are not chmod'ed", func(t *testing.T) {
		data, images := t.TempDir(), t.TempDir()
		if err := os.Chmod(data, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(images, 0o555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(images, 0o700) })
		s, err := Open(Config{DataDir: data, ImageDir: images})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		for p, want := range map[string]os.FileMode{data: 0o750, images: 0o555} {
			if info, err := os.Lstat(p); err != nil || info.Mode().Perm() != want {
				t.Errorf("existing %s: %v, %v; want it left at %v", p, info, err, want)
			}
		}
	})
}
