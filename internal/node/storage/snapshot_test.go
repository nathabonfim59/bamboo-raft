package storage

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type snapshotImager struct {
	basePath    string
	virtualSize uint64
	backing     string
	format      string
	checkErr    error
}

func (i snapshotImager) CreateOverlay(_ context.Context, dst ImageRef, _ uint64) error {
	return os.WriteFile(dst.File.Name(), []byte("source disk"), 0o600)
}
func (i snapshotImager) Grow(context.Context, ImageRef, uint64) error { return nil }
func (i snapshotImager) Info(_ context.Context, ref ImageRef) (QemuInfo, error) {
	file := ref.File.Name()
	format := i.format
	if format == "" {
		format = "qcow2"
	}
	info := QemuInfo{Format: format, VirtualSize: i.virtualSize}
	if filepath.Base(file) != "root.qcow2" {
		info.BackingFile = i.backing
		if info.BackingFile == "" {
			info.BackingFile = i.basePath
		}
	}
	return info, nil
}
func (i snapshotImager) Check(context.Context, ImageRef) error { return i.checkErr }

func snapshotStoreForTest(t *testing.T, options SnapshotOptions) (*Store, Image) {
	t.Helper()
	dataDir, imageDir := t.TempDir(), t.TempDir()
	img := Image{
		ID: "img-v0", Dir: filepath.Join(imageDir, "img-v0"),
		RootVirtualSize: testGiB << 30, RootVirtualSizeGiB: testGiB,
		RootSHA256: strings.Repeat("a", 64),
	}
	imager := snapshotImager{basePath: filepath.Join(img.Dir, "root.qcow2"), virtualSize: testGiB << 30}
	s, err := Open(Config{DataDir: dataDir, ImageDir: imageDir, Imager: imager, Snapshots: options})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	storageWriteBase(t, s, img.ID)
	meta := VMMeta{
		SandboxID: "bx_22222222", Image: img.ID,
		Resources: Resources{VCPUs: 2, MemoryMiB: 2048, DiskGiB: testGiB}, VsockCID: 3,
	}
	if err := s.CreateVM(t.Context(), meta, img); err != nil {
		t.Fatal(err)
	}
	return s, img
}

func publishSnapshotForTest(t *testing.T, s *Store, img Image, id string) Snapshot {
	t.Helper()
	stage, err := s.BeginSnapshot(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Copy(t.Context(), "bx_22222222", img, testGiB<<30); err != nil {
		t.Fatal(err)
	}
	snap, published, err := stage.Publish(t.Context(), "bx_22222222", img, time.Now().UnixMilli())
	if err != nil || !published {
		t.Fatalf("Publish: published=%t, %v", published, err)
	}
	return snap
}

func TestSnapshotRecoveryPreservesCommittedAndCleansOwnedStages(t *testing.T) {
	s, img := snapshotStoreForTest(t, SnapshotOptions{})
	const id = "opaque / snapshot"
	saved := publishSnapshotForTest(t, s, img, id)
	if saved.Meta.DiskSHA256 == "" || saved.Meta.SizeBytes == 0 || saved.Meta.ImageSHA256 != img.RootSHA256 {
		t.Fatalf("snapshot integrity metadata = %+v", saved.Meta)
	}
	if _, err := s.BeginSnapshot("incomplete / stage"); err != nil {
		t.Fatal(err)
	}
	foreignStage := filepath.Join(s.DataDir(), snapshotStagingDir, "foreign")
	foreignCommitted := filepath.Join(s.DataDir(), snapshotCommittedDir, "foreign")
	for _, dir := range []string{foreignStage, foreignCommitted} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "keep"), []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	recovered, err := s.RecoverSnapshots(t.Context(), []Image{img})
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0].Meta.SnapshotID != id {
		t.Fatalf("recovered = %+v", recovered)
	}
	if _, err := os.Stat(filepath.Join(s.DataDir(), snapshotStagingDir, SnapshotKey("incomplete / stage"))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("owned stage survived recovery: %v", err)
	}
	for _, dir := range []string{foreignStage, foreignCommitted} {
		if _, err := os.Stat(filepath.Join(dir, "keep")); err != nil {
			t.Fatalf("foreign directory %s removed: %v", dir, err)
		}
	}
	if _, err := os.Stat(filepath.Join(saved.Dir, diskName)); err != nil {
		t.Fatalf("committed disk removed: %v", err)
	}
}

func TestSnapshotRecoveryRejectsDiskMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *Store, Snapshot)
	}{
		{"hash mismatch same size", func(t *testing.T, s *Store, snap Snapshot) {
			data, err := os.ReadFile(snap.DiskPath)
			if err != nil {
				t.Fatal(err)
			}
			data[0] ^= 0xff
			if err := os.WriteFile(snap.DiskPath, data, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"size mismatch", func(t *testing.T, _ *Store, snap Snapshot) {
			if err := os.WriteFile(snap.DiskPath, []byte("different size"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"manifest id mismatch", func(t *testing.T, _ *Store, snap Snapshot) {
			meta := snap.Meta
			meta.SnapshotID = "different"
			writeSnapshotManifestForTest(t, snap.Dir, meta)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, img := snapshotStoreForTest(t, SnapshotOptions{})
			snap := publishSnapshotForTest(t, s, img, "committed")
			tc.mutate(t, s, snap)
			if _, err := s.RecoverSnapshots(t.Context(), []Image{img}); err == nil {
				t.Fatal("corrupt committed snapshot passed recovery")
			}
			if _, err := os.Stat(snap.DiskPath); err != nil {
				t.Fatalf("recovery deleted committed data: %v", err)
			}
		})
	}
}

// TestSnapshotRejectsInvalidCommittedEntrySet enforces the exact two-regular-
// file layout. Extra files, missing files and symlinked entries must fail
// recovery instead of being followed or silently accepted.
// TestSnapshotDeleteFsyncErrorReportsRemoved proves a directory fsync failure
// after RemoveAll is a durability error, not an absence ambiguity: removed is
// true, the path is gone, and a retry succeeds.
func TestSnapshotDeleteFsyncErrorReportsRemoved(t *testing.T) {
	failSync := false
	s, img := snapshotStoreForTest(t, SnapshotOptions{SyncDir: func(root *os.Root, name string) error {
		if failSync && name == snapshotCommittedDir {
			return errors.New("fsync after remove")
		}
		return syncDir(root, name)
	}})
	snap := publishSnapshotForTest(t, s, img, "delete-fsync")
	failSync = true
	removed, err := s.DeleteSnapshot(snap.Meta.SnapshotID)
	if err == nil || !removed {
		t.Fatalf("DeleteSnapshot = removed=%t, err=%v; want removed with fsync error", removed, err)
	}
	if _, statErr := os.Stat(snap.Dir); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("removed snapshot path still present: %v", statErr)
	}
	failSync = false
	if removed, err := s.DeleteSnapshot(snap.Meta.SnapshotID); err != nil || !removed {
		t.Fatalf("idempotent retry = removed=%t, %v", removed, err)
	}
}

func TestSnapshotRejectsInvalidCommittedEntrySet(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *Store, Snapshot)
	}{
		{"extra file", func(t *testing.T, _ *Store, snap Snapshot) {
			if err := os.WriteFile(filepath.Join(snap.Dir, "extra"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"missing disk", func(t *testing.T, _ *Store, snap Snapshot) {
			if err := os.Remove(snap.DiskPath); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlinked disk", func(t *testing.T, _ *Store, snap Snapshot) {
			outside := filepath.Join(t.TempDir(), "outside")
			if err := os.WriteFile(outside, []byte("unchanged"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(snap.DiskPath); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, snap.DiskPath); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlinked manifest", func(t *testing.T, _ *Store, snap Snapshot) {
			manifest := filepath.Join(snap.Dir, snapshotManifestName)
			raw, err := os.ReadFile(manifest)
			if err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(t.TempDir(), "outside")
			if err := os.WriteFile(outside, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(manifest); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, manifest); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, img := snapshotStoreForTest(t, SnapshotOptions{})
			snap := publishSnapshotForTest(t, s, img, "entry-set")
			tc.mutate(t, s, snap)
			if _, err := s.RecoverSnapshots(t.Context(), []Image{img}); err == nil {
				t.Fatal("invalid committed entry set passed recovery")
			}
			if _, err := s.ReadSnapshot(t.Context(), "entry-set", map[string]Image{img.ID: img}); err == nil {
				t.Fatal("invalid committed entry set passed ReadSnapshot")
			}
		})
	}
}

func writeSnapshotManifestForTest(t *testing.T, dir string, meta SnapshotMeta) {
	t.Helper()
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, snapshotManifestName), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotSyncFaultsNeverPublishPartialData(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts SnapshotOptions
	}{
		{"copy file sync", SnapshotOptions{SyncFile: func(f *os.File) error {
			return errors.New("sync file fault")
		}}},
		{"stage directory sync", SnapshotOptions{SyncDir: func(root *os.Root, name string) error {
			if strings.HasPrefix(name, snapshotStagingDir+"/") {
				return errors.New("sync stage fault")
			}
			return syncDir(root, name)
		}}},
		{"committed directory sync", SnapshotOptions{SyncDir: func(root *os.Root, name string) error {
			if name == snapshotCommittedDir {
				return errors.New("sync committed fault")
			}
			return syncDir(root, name)
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, img := snapshotStoreForTest(t, tc.opts)
			stage, err := s.BeginSnapshot("sync-fault")
			if err != nil {
				t.Fatal(err)
			}
			copyErr := stage.Copy(t.Context(), "bx_22222222", img, testGiB<<30)
			if copyErr == nil {
				_, _, err = stage.Publish(t.Context(), "bx_22222222", img, time.Now().UnixMilli())
			} else {
				err = copyErr
			}
			if err == nil {
				t.Fatal("injected sync fault succeeded")
			}
			if err := stage.Abort(); err != nil && tc.name != "committed directory sync" {
				t.Fatalf("Abort: %v", err)
			}
		})
	}
}

func TestSnapshotCopyCancellationAndAbort(t *testing.T) {
	entered := make(chan struct{})
	s, img := snapshotStoreForTest(t, SnapshotOptions{Copy: func(ctx context.Context, _, _ *os.File) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}})
	stage, err := s.BeginSnapshot("cancel")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- stage.Copy(ctx, "bx_22222222", img, testGiB<<30) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Copy cancellation = %v", err)
	}
	if err := stage.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(s.DataDir(), snapshotStagingDir, SnapshotKey("cancel"))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("aborted stage remains: %v", err)
	}
	if removed, err := s.DeleteSnapshot("cancel"); err != nil || !removed {
		t.Fatalf("DeleteSnapshot(cancel) = removed=%t, %v", removed, err)
	}
	if removed, err := s.DeleteSnapshot(""); err != nil || !removed {
		t.Fatalf("DeleteSnapshot(empty) = removed=%t, %v", removed, err)
	}
}

func TestSnapshotRejectsUnsafeBacking(t *testing.T) {
	s, img := snapshotStoreForTest(t, SnapshotOptions{})
	s.imager = snapshotImager{basePath: img.Dir, backing: "/unverified/base", virtualSize: testGiB << 30}
	stage, err := s.BeginSnapshot("backing")
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Copy(t.Context(), "bx_22222222", img, testGiB<<30); err == nil || !strings.Contains(err.Error(), "backing") {
		t.Fatalf("unsafe backing accepted: %v", err)
	}
	if err := stage.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotManifestTempNoFollow(t *testing.T) {
	s, img := snapshotStoreForTest(t, SnapshotOptions{})
	stage, err := s.BeginSnapshot("nofollow")
	if err != nil {
		t.Fatal(err)
	}
	if err := stage.Copy(t.Context(), "bx_22222222", img, testGiB<<30); err != nil {
		t.Fatal(err)
	}
	stageDir := filepath.Join(s.DataDir(), snapshotStagingDir, stage.key)
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(stageDir, snapshotManifestName+manifestTmpExt)); err != nil {
		t.Fatal(err)
	}
	snap, published, err := stage.Publish(t.Context(), "bx_22222222", img, time.Now().UnixMilli())
	if err != nil || !published || snap.Meta.SnapshotID != "nofollow" {
		t.Fatalf("safe temp replacement failed: published=%t err=%v", published, err)
	}
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "unchanged" {
		t.Fatalf("outside file changed: %q, %v", data, err)
	}
	if err := stage.Abort(); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotCopyDestinationSymlinkRejectedBeforeCopy proves the real default
// copy never runs when the staged destination is a symlink: the O_EXCL
// no-follow creation fails first and the external sentinel stays unchanged.
func TestSnapshotCopyDestinationSymlinkRejectedBeforeCopy(t *testing.T) {
	s, img := snapshotStoreForTest(t, SnapshotOptions{})
	stage, err := s.BeginSnapshot("nofollow-disk")
	if err != nil {
		t.Fatal(err)
	}
	stageDir := filepath.Join(s.DataDir(), snapshotStagingDir, stage.key)
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(stageDir, diskName)); err != nil {
		t.Fatal(err)
	}
	if err := stage.Copy(t.Context(), "bx_22222222", img, testGiB<<30); err == nil {
		t.Fatal("Copy followed destination symlink")
	}
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "unchanged" {
		t.Fatalf("outside file changed before rejection: %q, %v", data, err)
	}
	if err := stage.Abort(); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotCopySourceSymlinkRejectedBeforeCopy proves a symlinked source
// disk is rejected before the real cp runs, so data outside the VM directory
// cannot be copied into a snapshot.
func TestSnapshotCopySourceSymlinkRejectedBeforeCopy(t *testing.T) {
	s, img := snapshotStoreForTest(t, SnapshotOptions{})
	stage, err := s.BeginSnapshot("nofollow-source")
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	disk := filepath.Join(s.DataDir(), vmsDir, "bx_22222222", diskName)
	if err := os.Remove(disk); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, disk); err != nil {
		t.Fatal(err)
	}
	if err := stage.Copy(t.Context(), "bx_22222222", img, testGiB<<30); err == nil {
		t.Fatal("Copy followed source symlink")
	}
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "sentinel" {
		t.Fatalf("outside source changed: %q, %v", data, err)
	}
	if err := stage.Abort(); err != nil {
		t.Fatal(err)
	}
}
