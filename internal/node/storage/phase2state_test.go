package storage

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// phase2TestStore opens a store whose rename seam can be failed for one
// target name.
func phase2TestStore(t *testing.T, dataDir string, failRename *string) *Store {
	t.Helper()
	s, err := Open(Config{
		DataDir:  dataDir,
		ImageDir: t.TempDir(),
		Imager:   &stopImager{},
		Snapshots: SnapshotOptions{Rename: func(root *os.Root, old, new string) error {
			if failRename != nil && *failRename == new {
				return errors.New("injected rename failure")
			}
			return root.Rename(old, new)
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func phase2TestVM(t *testing.T, failRename *string) (*Store, string) {
	t.Helper()
	s := phase2TestStore(t, t.TempDir(), failRename)
	storageWriteBase(t, s, "img-v0")
	const id = "bx_pstate2a"
	stopTestVM(t, s, id)
	return s, id
}

func TestPhase2StateFreshVM(t *testing.T) {
	s, id := phase2TestVM(t, nil)
	st, err := s.ReadPhase2(id)
	if err != nil {
		t.Fatalf("ReadPhase2: %v", err)
	}
	if st.FullScanNext || st.SnapshotInProgress || st.LastManifestKey != "" {
		t.Fatalf("fresh state = %+v", st)
	}
	if _, _, ok, err := s.LastManifest(id); ok || err != nil {
		t.Fatalf("LastManifest of a fresh VM = ok %t, %v", ok, err)
	}
	if _, err := s.ReadPhase2("not-an-id"); err == nil {
		t.Fatal("ReadPhase2 accepted an invalid id")
	}
	if err := s.SetFullScanNext("../escape"); err == nil {
		t.Fatal("SetFullScanNext accepted an invalid id")
	}
	if err := s.SetFullScanNext("bx_nxsuchvm"); err == nil {
		t.Fatal("SetFullScanNext created state for a VM that does not exist")
	}
}

func TestPhase2StateFlagsAreDurable(t *testing.T) {
	dataDir := t.TempDir()
	s := phase2TestStore(t, dataDir, nil)
	storageWriteBase(t, s, "img-v0")
	const id = "bx_pstate2b"
	stopTestVM(t, s, id)

	steps := []struct {
		name string
		op   func(string) error
		want Phase2State
	}{
		{"set full scan", s.SetFullScanNext, Phase2State{FullScanNext: true}},
		{"set in progress", s.SetSnapshotInProgress, Phase2State{FullScanNext: true, SnapshotInProgress: true}},
		{"clear full scan", s.ClearFullScanNext, Phase2State{SnapshotInProgress: true}},
		{"clear in progress", s.ClearSnapshotInProgress, Phase2State{}},
		{"set in progress again", s.SetSnapshotInProgress, Phase2State{SnapshotInProgress: true}},
		{"recover with QEMU alive", func(id string) error { return s.RecoverInterruptedSnapshot(id, false) }, Phase2State{}},
		{"set in progress a third time", s.SetSnapshotInProgress, Phase2State{SnapshotInProgress: true}},
		{"recover with QEMU gone", func(id string) error { return s.RecoverInterruptedSnapshot(id, true) }, Phase2State{FullScanNext: true}},
	}
	for _, step := range steps {
		if err := step.op(id); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		got, err := s.ReadPhase2(id)
		if err != nil {
			t.Fatalf("%s: ReadPhase2: %v", step.name, err)
		}
		if got.FullScanNext != step.want.FullScanNext || got.SnapshotInProgress != step.want.SnapshotInProgress {
			t.Fatalf("%s: state = %+v, want %+v", step.name, got, step.want)
		}
	}

	// A reopened store reads the same state.
	s.Close()
	s2 := phase2TestStore(t, dataDir, nil)
	got, err := s2.ReadPhase2(id)
	if err != nil {
		t.Fatal(err)
	}
	if !got.FullScanNext || got.SnapshotInProgress {
		t.Fatalf("state after reopen = %+v", got)
	}
	info, err := os.Stat(filepath.Join(s2.VMDir(id), phase2StateName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("phase2.json mode %v, want 0600", info.Mode().Perm())
	}
}

func TestPhase2LastManifest(t *testing.T) {
	s, id := phase2TestVM(t, nil)
	if err := s.SetFullScanNext(id); err != nil {
		t.Fatal(err)
	}
	first := []byte("manifest generation 1")
	if err := s.SetLastManifest(id, "chains/c1/manifests/00000001", first); err != nil {
		t.Fatalf("SetLastManifest: %v", err)
	}
	key, raw, ok, err := s.LastManifest(id)
	if err != nil || !ok || key != "chains/c1/manifests/00000001" || !bytes.Equal(raw, first) {
		t.Fatalf("LastManifest = %q, %q, %t, %v", key, raw, ok, err)
	}
	// Setting the manifest keeps the other fields: clearing full scan is
	// the caller's explicit decision.
	if st, _ := s.ReadPhase2(id); !st.FullScanNext {
		t.Fatalf("SetLastManifest dropped FullScanNext: %+v", st)
	}
	second := []byte("manifest generation 2, longer than the first")
	if err := s.SetLastManifest(id, "chains/c1/manifests/00000002", second); err != nil {
		t.Fatal(err)
	}
	key, raw, ok, err = s.LastManifest(id)
	if err != nil || !ok || key != "chains/c1/manifests/00000002" || !bytes.Equal(raw, second) {
		t.Fatalf("LastManifest after replace = %q, %q, %t, %v", key, raw, ok, err)
	}
	if err := s.SetLastManifest(id, "", second); err == nil {
		t.Fatal("SetLastManifest accepted an empty key")
	}

	// A tampered or torn copy is never returned as the previous manifest.
	if err := os.WriteFile(filepath.Join(s.VMDir(id), lastManifestName), []byte("torn"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := s.LastManifest(id); ok || !errors.Is(err, ErrPhase2StateCorrupt) {
		t.Fatalf("LastManifest of a tampered copy = ok %t, %v", ok, err)
	}
	if err := os.Remove(filepath.Join(s.VMDir(id), lastManifestName)); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := s.LastManifest(id); ok || !errors.Is(err, ErrPhase2StateCorrupt) {
		t.Fatalf("LastManifest of a missing copy = ok %t, %v", ok, err)
	}
}

// TestPhase2StateWritesAreAtomic fails each rename: the published state is
// the old one, no temporary file remains, and a crash between the manifest
// copy and its state shows as a digest mismatch, never the old key with the
// new bytes.
func TestPhase2StateWritesAreAtomic(t *testing.T) {
	var fail string
	s, id := phase2TestVM(t, &fail)
	old := []byte("old manifest")
	if err := s.SetLastManifest(id, "k/old", old); err != nil {
		t.Fatal(err)
	}

	fail = phase2StateName
	if err := s.SetSnapshotInProgress(id); err == nil {
		t.Fatal("SetSnapshotInProgress succeeded with a failing rename")
	}
	if st, err := s.ReadPhase2(id); err != nil || st.SnapshotInProgress || st.LastManifestKey != "k/old" {
		t.Fatalf("state after a failed write = %+v, %v", st, err)
	}

	// The copy is published, its state is not: the copy no longer matches.
	if err := s.SetLastManifest(id, "k/new", []byte("new manifest")); err == nil {
		t.Fatal("SetLastManifest succeeded with a failing state rename")
	}
	if _, _, ok, err := s.LastManifest(id); ok || !errors.Is(err, ErrPhase2StateCorrupt) {
		t.Fatalf("LastManifest after a torn update = ok %t, %v", ok, err)
	}

	// A failing copy rename leaves both files as they were.
	fail = lastManifestName
	if err := s.SetLastManifest(id, "k/other", []byte("other")); err == nil {
		t.Fatal("SetLastManifest succeeded with a failing copy rename")
	}
	entries, err := os.ReadDir(s.VMDir(id))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == manifestTmpExt {
			t.Fatalf("temporary file %s left behind", e.Name())
		}
	}
}

func TestPhase2StateCorruptIsReplaced(t *testing.T) {
	s, id := phase2TestVM(t, nil)
	if err := s.SetLastManifest(id, "k/1", []byte("m")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.VMDir(id), phase2StateName)
	for _, bad := range []string{`{`, `{"schemaVersion":2}`, `{"schemaVersion":1,"lastManifestKey":"k/1"}`} {
		if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ReadPhase2(id); !errors.Is(err, ErrPhase2StateCorrupt) {
			t.Fatalf("ReadPhase2(%s) = %v, want ErrPhase2StateCorrupt", bad, err)
		}
		// The next update never trusts it: the manifest is dropped and a
		// full scan is forced.
		if err := s.SetSnapshotInProgress(id); err != nil {
			t.Fatal(err)
		}
		st, err := s.ReadPhase2(id)
		if err != nil || !st.FullScanNext || !st.SnapshotInProgress || st.LastManifestKey != "" {
			t.Fatalf("state replacing %s = %+v, %v", bad, st, err)
		}
	}

	// A symlink in place of the state is not followed.
	target := filepath.Join(t.TempDir(), "elsewhere")
	if err := os.WriteFile(target, []byte(`{"schemaVersion":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	os.Remove(path)
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadPhase2(id); err == nil {
		t.Fatal("ReadPhase2 followed a symlink")
	}
}

func TestPhase2StateRemovedWithVM(t *testing.T) {
	s, id := phase2TestVM(t, nil)
	if err := s.SetLastManifest(id, "k/1", []byte("m")); err != nil {
		t.Fatal(err)
	}
	if err := s.SetFullScanNext(id); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveVM(id); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{phase2StateName, lastManifestName} {
		if _, err := os.Lstat(filepath.Join(s.VMDir(id), name)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s after RemoveVM: %v", name, err)
		}
	}
}
