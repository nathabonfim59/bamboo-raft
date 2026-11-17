package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

const (
	snapshotsDir         = "snapshots"
	snapshotStagingDir   = snapshotsDir + "/staging"
	snapshotCommittedDir = snapshotsDir + "/committed"
	snapshotManifestName = "snapshot.json"
	snapshotSchema       = 1
)

// ErrSnapshotExists reports that a snapshot id is committed or being saved.
var ErrSnapshotExists = errors.New("storage: snapshot already exists")

// SnapshotOptions are deterministic fault-injection seams for snapshot and
// child-disk storage. Nil fields use the real filesystem and cp operations.
// Production configurations leave every field nil.
type SnapshotOptions struct {
	// Copy makes a private copy from source to destination. Both files are
	// already pinned through confined no-follow opens. The real implementation
	// uses cp --reflink=auto with inherited descriptors.
	Copy func(ctx context.Context, source, destination *os.File) error
	// SyncFile makes file contents durable. Nil uses File.Sync.
	SyncFile func(*os.File) error
	// SyncDir makes a directory entry durable. Nil uses fsync on the directory.
	SyncDir func(root *os.Root, name string) error
	// Rename atomically moves old to new below root. Nil uses Root.Rename.
	Rename func(root *os.Root, old, new string) error
}

func (o SnapshotOptions) withDefaults() SnapshotOptions {
	if o.Copy == nil {
		o.Copy = reflinkCopy
	}
	if o.SyncFile == nil {
		o.SyncFile = func(f *os.File) error { return f.Sync() }
	}
	if o.SyncDir == nil {
		o.SyncDir = syncDir
	}
	if o.Rename == nil {
		o.Rename = func(root *os.Root, old, new string) error { return root.Rename(old, new) }
	}
	return o
}

// SnapshotMeta is the durable manifest of one committed snapshot. SnapshotID
// is opaque: only its sha256 becomes a directory name.
type SnapshotMeta struct {
	SchemaVersion   int    `json:"schemaVersion"`
	SnapshotID      string `json:"snapshotId"`
	SourceSandboxID string `json:"sourceSandboxId"`
	Image           string `json:"image"`
	ImageSHA256     string `json:"imageSha256"`
	VirtualSize     uint64 `json:"virtualSizeBytes"`
	DiskSHA256      string `json:"diskSha256"`
	SizeBytes       int64  `json:"sizeBytes"`
	CreatedAtUnixMs int64  `json:"createdAtUnixMs"`
}

// Snapshot is a validated committed snapshot and its private disk location.
type Snapshot struct {
	Meta     SnapshotMeta
	Dir      string
	DiskPath string
}

// SnapshotStage owns one unpublished snapshot directory. Its methods are
// called by only the save operation that created it.
type SnapshotStage struct {
	store       *Store
	id          string
	key         string
	size        int64
	virtualSize uint64
	diskSHA256  string
	published   bool
	// retained reports that a tool run on the staged disk was retained:
	// Abort and recovery keep the stage for the tool's settlement.
	retained bool
}

// SnapshotKey returns the deterministic, path-safe key for an opaque id.
func SnapshotKey(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

func validSnapshotKey(key string) bool {
	if len(key) != sha256.Size*2 {
		return false
	}
	for i := range len(key) {
		c := key[i]
		if c < '0' || c > '9' && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// BeginSnapshot atomically reserves snapshotID with a private staging
// directory. A committed or in-progress id returns ErrSnapshotExists.
func (s *Store) BeginSnapshot(snapshotID string) (*SnapshotStage, error) {
	if snapshotID == "" {
		return nil, errors.New("storage: snapshot id is required")
	}
	key := SnapshotKey(snapshotID)
	committed := path.Join(snapshotCommittedDir, key)
	if _, err := s.root.Lstat(committed); err == nil {
		return nil, ErrSnapshotExists
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("storage: check snapshot: %w", err)
	}
	stage := path.Join(snapshotStagingDir, key)
	if err := s.root.Mkdir(stage, 0o700); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil, ErrSnapshotExists
		}
		return nil, fmt.Errorf("storage: create snapshot staging: %w", err)
	}
	if err := s.cfg.Snapshots.SyncDir(s.root, snapshotStagingDir); err != nil {
		_ = s.root.RemoveAll(stage)
		return nil, fmt.Errorf("storage: sync snapshot staging: %w", err)
	}
	return &SnapshotStage{store: s, id: snapshotID, key: key}, nil
}

// Copy copies a stopped VM disk into staging, syncs it, and validates the
// resulting qcow2. The copy may refer only to img's verified immutable base.
func (st *SnapshotStage) Copy(ctx context.Context, sourceSandboxID string, img Image, virtualSize uint64) error {
	if !ids.ValidSandbox(sourceSandboxID) {
		return fmt.Errorf("storage: invalid snapshot source %q", sourceSandboxID)
	}
	if virtualSize == 0 {
		return errors.New("storage: snapshot virtual size is zero")
	}
	s := st.store
	if err := ctx.Err(); err != nil {
		return err
	}
	sourceRoot, err := s.root.OpenRoot(path.Join(vmsDir, sourceSandboxID))
	if err != nil {
		return fmt.Errorf("storage: open snapshot source root: %w", err)
	}
	defer sourceRoot.Close()
	source, err := openNoFollow(sourceRoot, diskName)
	if err != nil {
		return fmt.Errorf("storage: open snapshot source: %w", err)
	}
	stageRoot, err := s.root.OpenRoot(path.Join(snapshotStagingDir, st.key))
	if err != nil {
		source.Close()
		return fmt.Errorf("storage: open snapshot stage: %w", err)
	}
	defer stageRoot.Close()
	// O_EXCL plus O_NOFOLLOW pins a newly created regular destination before
	// cp runs. The copy receives inherited fds, never replaceable pathnames.
	f, err := stageRoot.OpenFile(diskName, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		source.Close()
		return fmt.Errorf("storage: create staged disk: %w", err)
	}
	if err := s.cfg.Snapshots.Copy(ctx, source, f); err != nil {
		source.Close()
		f.Close()
		return fmt.Errorf("storage: copy snapshot: %w", err)
	}
	if err := ctx.Err(); err != nil {
		source.Close()
		f.Close()
		return err
	}
	info, statErr := f.Stat()
	syncErr := s.cfg.Snapshots.SyncFile(f)
	closeErr := errors.Join(source.Close(), f.Close())
	relative := path.Join(snapshotStagingDir, st.key, diskName)
	if err := errors.Join(statErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("storage: sync staged disk: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("storage: staged disk is not a regular file")
	}
	if err := s.cfg.Snapshots.SyncDir(s.root, path.Join(snapshotStagingDir, st.key)); err != nil {
		return fmt.Errorf("storage: sync staged directory: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.validateSnapshotDisk(ctx, relative, img, virtualSize); err != nil {
		if errors.Is(err, ErrToolRetained) {
			st.retained = true
		}
		return err
	}
	diskSHA256, err := hashFile(ctx, s.root, relative)
	if err != nil {
		return fmt.Errorf("storage: hash staged disk: %w", err)
	}
	st.size = info.Size()
	st.virtualSize = virtualSize
	st.diskSHA256 = diskSHA256
	return ctx.Err()
}

// Publish durably commits a fully validated staged snapshot. published reports
// whether the committed rename happened, including when a later fsync fails.
// A published snapshot must remain visible even if Publish returns an error.
func (st *SnapshotStage) Publish(ctx context.Context, sourceSandboxID string, img Image, createdAtUnixMs int64) (snap Snapshot, published bool, err error) {
	s := st.store
	meta := SnapshotMeta{
		SchemaVersion:   snapshotSchema,
		SnapshotID:      st.id,
		SourceSandboxID: sourceSandboxID,
		Image:           img.ID,
		ImageSHA256:     img.RootSHA256,
		VirtualSize:     st.virtualSize,
		DiskSHA256:      st.diskSHA256,
		SizeBytes:       st.size,
		CreatedAtUnixMs: createdAtUnixMs,
	}
	if err := validateSnapshotMeta(meta, st.key); err != nil {
		return Snapshot{}, false, err
	}
	if st.virtualSize == 0 {
		return Snapshot{}, false, errors.New("storage: snapshot has no copied disk")
	}
	stageRel := path.Join(snapshotStagingDir, st.key)
	stageRoot, err := s.root.OpenRoot(stageRel)
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("storage: open snapshot stage: %w", err)
	}
	defer stageRoot.Close()

	raw, err := json.Marshal(meta)
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("storage: encode snapshot manifest: %w", err)
	}
	tmp := snapshotManifestName + manifestTmpExt
	if err := stageRoot.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return Snapshot{}, false, fmt.Errorf("storage: clear snapshot manifest temp: %w", err)
	}
	f, err := stageRoot.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("storage: create snapshot manifest: %w", err)
	}
	_, writeErr := f.Write(raw)
	syncErr := s.cfg.Snapshots.SyncFile(f)
	closeErr := f.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		_ = stageRoot.Remove(tmp)
		return Snapshot{}, false, fmt.Errorf("storage: write snapshot manifest: %w", err)
	}
	if err := stageRoot.Rename(tmp, snapshotManifestName); err != nil {
		return Snapshot{}, false, fmt.Errorf("storage: stage snapshot manifest: %w", err)
	}
	if err := s.cfg.Snapshots.SyncDir(s.root, stageRel); err != nil {
		return Snapshot{}, false, fmt.Errorf("storage: sync snapshot manifest: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, false, err
	}

	committedRel := path.Join(snapshotCommittedDir, st.key)
	if _, err := s.root.Lstat(committedRel); err == nil {
		return Snapshot{}, false, ErrSnapshotExists
	} else if !errors.Is(err, fs.ErrNotExist) {
		return Snapshot{}, false, fmt.Errorf("storage: recheck snapshot destination: %w", err)
	}
	snap = Snapshot{
		Meta:     meta,
		Dir:      filepath.Join(s.dataDir, committedRel),
		DiskPath: filepath.Join(s.dataDir, committedRel, diskName),
	}
	renameErr := s.cfg.Snapshots.Rename(s.root, stageRel, committedRel)
	if renameErr != nil {
		_, statErr := s.root.Lstat(committedRel)
		st.published = statErr == nil
		return snap, st.published, fmt.Errorf("storage: publish snapshot: %w", renameErr)
	}
	st.published = true
	syncCommittedErr := s.cfg.Snapshots.SyncDir(s.root, snapshotCommittedDir)
	syncStagingErr := s.cfg.Snapshots.SyncDir(s.root, snapshotStagingDir)
	if err := errors.Join(syncCommittedErr, syncStagingErr); err != nil {
		return snap, true, fmt.Errorf("storage: sync published snapshot: %w", err)
	}
	return snap, true, ctx.Err()
}

// Abort removes the unpublished stage. It never removes a committed snapshot,
// including when publication returned an fsync error after the rename, and
// never a stage whose disk a jailed tool still holds (or whose tool failure
// was ErrToolRetained): that stage stays exactly as it is.
func (st *SnapshotStage) Abort() error {
	if st.published {
		return nil
	}
	s := st.store
	if stage := path.Join(snapshotStagingDir, st.key); st.retained || s.treeHeld(stage) {
		return fmt.Errorf("storage: snapshot stage keeps a target a jailed tool still holds: %w", ErrToolRetained)
	}
	if err := s.root.RemoveAll(path.Join(snapshotStagingDir, st.key)); err != nil {
		return fmt.Errorf("storage: remove snapshot staging: %w", err)
	}
	if err := s.cfg.Snapshots.SyncDir(s.root, snapshotStagingDir); err != nil {
		return fmt.Errorf("storage: sync aborted snapshot: %w", err)
	}
	return nil
}

// DeleteSnapshot idempotently removes a committed snapshot. An in-progress
// stage is owned by its save operation and is left alone. removed reports that
// the committed path is proven absent, including when the later directory fsync
// fails: callers must retire in-memory metadata for a proven-absent snapshot
// even while returning the durability error.
func (s *Store) DeleteSnapshot(snapshotID string) (removed bool, err error) {
	if snapshotID == "" {
		return true, nil
	}
	rel := path.Join(snapshotCommittedDir, SnapshotKey(snapshotID))
	if err := s.root.RemoveAll(rel); err != nil {
		if _, statErr := s.root.Lstat(rel); errors.Is(statErr, fs.ErrNotExist) {
			return true, fmt.Errorf("storage: delete snapshot: %w", err)
		}
		return false, fmt.Errorf("storage: delete snapshot: %w", err)
	}
	if err := s.cfg.Snapshots.SyncDir(s.root, snapshotCommittedDir); err != nil {
		return true, fmt.Errorf("storage: sync snapshot deletion: %w", err)
	}
	return true, nil
}

// ReadSnapshot returns and validates one committed snapshot.
func (s *Store) ReadSnapshot(ctx context.Context, snapshotID string, images map[string]Image) (Snapshot, error) {
	if snapshotID == "" {
		return Snapshot{}, fs.ErrNotExist
	}
	return s.readCommittedSnapshot(ctx, SnapshotKey(snapshotID), images)
}

// RecoverSnapshots removes owned incomplete stages and validates every
// committed snapshot. Foreign names and all committed data are preserved.
func (s *Store) RecoverSnapshots(ctx context.Context, images []Image) ([]Snapshot, error) {
	if err := s.cleanSnapshotStaging(); err != nil {
		return nil, err
	}
	byID := make(map[string]Image, len(images))
	for _, img := range images {
		byID[img.ID] = img
	}
	dir, err := s.root.Open(snapshotCommittedDir)
	if err != nil {
		return nil, fmt.Errorf("storage: list committed snapshots: %w", err)
	}
	entries, readErr := dir.ReadDir(-1)
	closeErr := dir.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, fmt.Errorf("storage: list committed snapshots: %w", err)
	}
	out := make([]Snapshot, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !entry.IsDir() || !validSnapshotKey(entry.Name()) {
			s.log.Warn("storage: foreign entry under committed snapshots; leaving it alone", "name", entry.Name())
			continue
		}
		snap, err := s.readCommittedSnapshot(ctx, entry.Name(), byID)
		if err != nil {
			return nil, fmt.Errorf("storage: recover snapshot %s: %w", entry.Name(), err)
		}
		out = append(out, snap)
	}
	slices.SortFunc(out, func(a, b Snapshot) int { return strings.Compare(a.Meta.SnapshotID, b.Meta.SnapshotID) })
	return out, nil
}

func (s *Store) cleanSnapshotStaging() error {
	dir, err := s.root.Open(snapshotStagingDir)
	if err != nil {
		return fmt.Errorf("storage: list snapshot staging: %w", err)
	}
	entries, readErr := dir.ReadDir(-1)
	closeErr := dir.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return fmt.Errorf("storage: list snapshot staging: %w", err)
	}
	removed := false
	for _, entry := range entries {
		if !entry.IsDir() || !validSnapshotKey(entry.Name()) {
			s.log.Warn("storage: foreign entry under snapshot staging; leaving it alone", "name", entry.Name())
			continue
		}
		if s.treeHeld(path.Join(snapshotStagingDir, entry.Name())) {
			s.log.Warn("storage: snapshot stage holds a target a jailed tool still holds; leaving it alone", "name", entry.Name())
			continue
		}
		if err := s.root.RemoveAll(path.Join(snapshotStagingDir, entry.Name())); err != nil {
			return fmt.Errorf("storage: clean snapshot staging %s: %w", entry.Name(), err)
		}
		removed = true
	}
	if removed {
		return s.cfg.Snapshots.SyncDir(s.root, snapshotStagingDir)
	}
	return nil
}

func (s *Store) readCommittedSnapshot(ctx context.Context, key string, images map[string]Image) (Snapshot, error) {
	rel := path.Join(snapshotCommittedDir, key)
	info, err := s.root.Lstat(rel)
	if err != nil {
		return Snapshot{}, err
	}
	if !info.IsDir() {
		return Snapshot{}, fmt.Errorf("storage: committed snapshot %s is not a directory", key)
	}
	root, err := s.root.OpenRoot(rel)
	if err != nil {
		return Snapshot{}, fmt.Errorf("storage: open committed snapshot %s: %w", key, err)
	}
	entryErr := validateCommittedEntrySet(root)
	root.Close()
	if entryErr != nil {
		return Snapshot{}, fmt.Errorf("storage: committed snapshot %s: %w", key, entryErr)
	}
	f, err := openNoFollow(s.root, path.Join(rel, snapshotManifestName))
	if err != nil {
		return Snapshot{}, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(f, 1<<20))
	closeErr := f.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return Snapshot{}, fmt.Errorf("storage: read snapshot manifest: %w", err)
	}
	var meta SnapshotMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return Snapshot{}, fmt.Errorf("storage: decode snapshot manifest: %w", err)
	}
	if err := validateSnapshotMeta(meta, key); err != nil {
		return Snapshot{}, err
	}
	img, ok := images[meta.Image]
	if !ok {
		return Snapshot{}, fmt.Errorf("storage: snapshot image %q is not verified and installed", meta.Image)
	}
	if meta.ImageSHA256 != img.RootSHA256 {
		return Snapshot{}, errors.New("storage: snapshot image identity does not match the verified image")
	}
	disk := filepath.Join(s.dataDir, rel, diskName)
	if err := s.validateSnapshotDisk(ctx, path.Join(rel, diskName), img, meta.VirtualSize); err != nil {
		return Snapshot{}, err
	}
	diskInfo, err := s.root.Lstat(path.Join(rel, diskName))
	if err != nil {
		return Snapshot{}, fmt.Errorf("storage: stat committed disk: %w", err)
	}
	if !diskInfo.Mode().IsRegular() || diskInfo.Size() != meta.SizeBytes {
		return Snapshot{}, errors.New("storage: committed disk size does not match its manifest")
	}
	diskSHA256, err := hashFile(ctx, s.root, path.Join(rel, diskName))
	if err != nil {
		return Snapshot{}, fmt.Errorf("storage: hash committed disk: %w", err)
	}
	if diskSHA256 != meta.DiskSHA256 {
		return Snapshot{}, errors.New("storage: committed disk sha256 does not match its manifest")
	}
	return Snapshot{Meta: meta, Dir: filepath.Join(s.dataDir, rel), DiskPath: disk}, nil
}

// validateCommittedEntrySet accepts exactly the two regular files a committed
// snapshot owns. Extra, missing or symlinked entries could alias another
// snapshot or an outside file and are rejected instead of followed.
func validateCommittedEntrySet(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("open directory: %w", err)
	}
	entries, readErr := dir.ReadDir(-1)
	closeErr := dir.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return fmt.Errorf("list directory: %w", err)
	}
	if len(entries) != 2 {
		return fmt.Errorf("has %d entries, want exactly %d", len(entries), 2)
	}
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("inspect %q: %w", entry.Name(), err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("entry %q is not a regular file", entry.Name())
		}
		switch entry.Name() {
		case snapshotManifestName, diskName:
			seen[entry.Name()] = true
		default:
			return fmt.Errorf("unexpected entry %q", entry.Name())
		}
	}
	if !seen[snapshotManifestName] || !seen[diskName] {
		return errors.New("manifest or disk is missing")
	}
	return nil
}

func validateSnapshotMeta(meta SnapshotMeta, key string) error {
	switch {
	case meta.SchemaVersion != snapshotSchema:
		return fmt.Errorf("storage: snapshot schemaVersion %d, want %d", meta.SchemaVersion, snapshotSchema)
	case meta.SnapshotID == "":
		return errors.New("storage: snapshot id is empty")
	case SnapshotKey(meta.SnapshotID) != key:
		return errors.New("storage: snapshot id does not match its directory key")
	case !ids.ValidSandbox(meta.SourceSandboxID):
		return errors.New("storage: snapshot source sandbox id is invalid")
	case !ValidImageID(meta.Image):
		return errors.New("storage: snapshot image id is invalid")
	case !validSHA256(meta.ImageSHA256):
		return errors.New("storage: snapshot image sha256 is invalid")
	case meta.VirtualSize == 0:
		return errors.New("storage: snapshot virtual size is zero")
	case !validSHA256(meta.DiskSHA256):
		return errors.New("storage: snapshot disk sha256 is invalid")
	case meta.SizeBytes < 0:
		return errors.New("storage: snapshot size is negative")
	case meta.CreatedAtUnixMs <= 0:
		return errors.New("storage: snapshot creation time is invalid")
	}
	return nil
}

func (s *Store) validateSnapshotDisk(ctx context.Context, rel string, img Image, virtualSize uint64) error {
	disk, closeRef, err := s.overlayRef(rel, img.ID, false)
	if err != nil {
		return fmt.Errorf("storage: inspect snapshot disk: %w", err)
	}
	defer closeRef()
	info, err := s.imager.Info(ctx, disk)
	if err != nil {
		return fmt.Errorf("storage: inspect snapshot disk: %w", err)
	}
	if info.Format != "qcow2" {
		return fmt.Errorf("storage: snapshot disk format %q, want qcow2", info.Format)
	}
	if info.VirtualSize != virtualSize {
		return fmt.Errorf("storage: snapshot virtual size %d, want %d", info.VirtualSize, virtualSize)
	}
	wantBacking := s.imageBaseName(img.ID)
	if info.BackingFile != wantBacking {
		return fmt.Errorf("storage: snapshot backing %q, want verified image base %q", info.BackingFile, wantBacking)
	}
	if err := s.imager.Check(ctx, disk); err != nil {
		return fmt.Errorf("storage: check snapshot disk: %w", err)
	}
	return nil
}
