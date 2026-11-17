// Package storage owns the node's on-disk state under the configured data
// directory: per-VM boxes with durable manifests and qcow2
// overlays, image verification, the exclusive node lock and quarantine of
// unprovable state after a restart.
//
// Layout:
//
//	node.lock               exclusive manager lock (flock)
//	vms/<sandbox-id>/       one directory per VM, mode 0700
//	  vm.json               durable manifest, mode 0600 (contains the VM env)
//	  disk.qcow2            private qcow2 overlay over the verified image
//	  qmp.sock, serial.log  created by QEMU itself
//	snapshots/staging/<sha256(id)>/    incomplete private copies; cleaned on restart
//	snapshots/committed/<sha256(id)>/  durable disk.qcow2 and snapshot.json
//	orphans/<name>-<unix>/  quarantined VM dirs; never touched again by the node
//
// Identifiers never become unchecked filesystem paths: sandbox ids are
// validated with [github.com/nathabonfim59/bamboo-raft/internal/ids] and
// image ids with [storage.ValidImageID] before any path is built, and
// mutation happens through [os.Root] confined to the data directory.
package storage

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// Dir and file names inside the data directory. They are fixed strings, never
// derived from request data.
const (
	lockName       = "node.lock"
	vmsDir         = "vms"
	orphansDir     = "orphans"
	manifestName   = "vm.json"
	diskName       = "disk.qcow2"
	manifestTmpExt = ".tmp"
)

// ErrLocked reports that another bbraft-node process owns the data directory.
var ErrLocked = errors.New("storage: data directory is owned by another bbraft-node")

// ErrManifestDurabilityUnknown marks a manifest write whose rename already
// succeeded but whose containing-directory sync failed: the new content is
// live, and only its crash durability is unproven. Callers must not assume
// the previous manifest still stands.
var ErrManifestDurabilityUnknown = errors.New("storage: manifest rename succeeded but durability is unproven")

// Config configures a [Store].
type Config struct {
	// DataDir is the node's writable state root. Created if missing.
	DataDir string
	// ImageDir is the read-only root holding image version directories.
	ImageDir string
	// Leases reports inodes held by jailed tools; removal and rollback paths
	// keep any tree containing one. nil: nothing is held.
	Leases TargetLeases
	// Imager creates and inspects qcow2 files. nil: the real qemu-img
	// wrapper. Tests inject a recorder.
	Imager Imager
	// Snapshots supplies test seams for copy, fsync and rename faults.
	Snapshots SnapshotOptions
	// Log receives diagnostics. nil: logs are discarded.
	Log *slog.Logger
}

// Store is the node's durable state. It holds the exclusive data-directory
// lock from Open until Close.
type Store struct {
	cfg  Config
	root *os.Root
	// dataDir and imageDir are the absolute roots; QEMU needs plain
	// absolute paths for its own validated Config.
	dataDir  string
	imageDir string
	imager   Imager
	log      *slog.Logger
	lockFile *os.File
	// tc holds the tool self-check's test seams (zero: production).
	tc toolCheckHooks
}

// Open creates the layout if needed and takes the exclusive node lock. A
// second Open on the same directory fails with an error wrapping
// [ErrLocked]; there is never more than one manager per data directory.
func Open(cfg Config) (*Store, error) {
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	if cfg.Imager == nil {
		cfg.Imager = QemuImg{}
	}
	cfg.Snapshots = cfg.Snapshots.withDefaults()
	dataDir, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("storage: data dir: %w", err)
	}
	imageDir, err := filepath.Abs(cfg.ImageDir)
	if err != nil {
		return nil, fmt.Errorf("storage: image dir: %w", err)
	}
	if dataDir == string(filepath.Separator) || imageDir == string(filepath.Separator) {
		return nil, errors.New("storage: refusing to use / as a storage root")
	}
	// A missing data or image root (and any missing parent) is created
	// private, 0700, whatever the umask; an existing one (the role's 0700
	// data root, its shared 0555 image root) is used as it is, never
	// chmod'ed or migrated. No jailed process reaches either root by path:
	// jails are chrooted and get supervisor-opened descriptors only.
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("storage: create data dir: %w", err)
	}
	if err := os.MkdirAll(imageDir, 0o700); err != nil {
		return nil, fmt.Errorf("storage: create image dir: %w", err)
	}

	root, err := os.OpenRoot(dataDir)
	if err != nil {
		return nil, fmt.Errorf("storage: open data root: %w", err)
	}
	s := &Store{cfg: cfg, root: root, dataDir: dataDir, imageDir: imageDir, imager: cfg.Imager, log: cfg.Log}
	// The Store's private directories are declared 0700, so their mode does
	// not depend on the umask (production's UMask=0077 already gave 0700).
	// An existing directory is used as it is, never chmod'ed.
	for _, d := range []string{vmsDir, orphansDir, snapshotsDir, snapshotStagingDir, snapshotCommittedDir} {
		if err := root.Mkdir(d, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			root.Close()
			return nil, fmt.Errorf("storage: create %s: %w", d, err)
		}
	}
	lock, err := lockDir(root)
	if err != nil {
		root.Close()
		return nil, err
	}
	s.lockFile = lock
	return s, nil
}

// Close releases the exclusive lock and the data root. VM state stays on
// disk for the next start.
func (s *Store) Close() error {
	var errs []error
	if s.lockFile != nil {
		errs = append(errs, unlock(s.lockFile))
		s.lockFile = nil
	}
	errs = append(errs, s.root.Close())
	return errors.Join(errs...)
}

// DataDir returns the absolute data directory.
func (s *Store) DataDir() string { return s.dataDir }

// ImageDir returns the absolute image root.
func (s *Store) ImageDir() string { return s.imageDir }

// Imager returns the configured qcow2 tool.
func (s *Store) Imager() Imager { return s.imager }

// VMDir returns the absolute directory of one VM's box. The id must be a
// valid sandbox id; it is the only variable path component.
func (s *Store) VMDir(id string) string {
	return filepath.Join(s.dataDir, vmsDir, id)
}

// DiskPath returns the absolute overlay path of one VM's disk.
func (s *Store) DiskPath(id string) string {
	return filepath.Join(s.VMDir(id), diskName)
}

// VMEntry is one directory under vms/, classified for reconciliation.
type VMEntry struct {
	// Name is the directory name as found on disk.
	Name string
	// Kind says how the name relates to the node's sandbox-id namespace.
	Kind VMEntryKind
}

// VMEntryKind classifies a vms/ entry.
type VMEntryKind int

const (
	// VMKindOwned is a directory whose name is a valid sandbox id: the
	// node owns it and may manage or quarantine it.
	VMKindOwned VMEntryKind = iota
	// VMKindForeign is anything else: never touched, only logged.
	VMKindForeign
)

// ListVMDirs lists the entries under vms/ with their classification.
func (s *Store) ListVMDirs() ([]VMEntry, error) {
	dir, err := s.root.Open(vmsDir)
	if err != nil {
		return nil, fmt.Errorf("storage: list vms: %w", err)
	}
	names, err := dir.ReadDir(-1)
	if err != nil {
		dir.Close()
		return nil, fmt.Errorf("storage: list vms: %w", err)
	}
	if err := dir.Close(); err != nil {
		return nil, fmt.Errorf("storage: list vms: %w", err)
	}
	out := make([]VMEntry, 0, len(names))
	for _, n := range names {
		kind := VMKindForeign
		if n.IsDir() && ids.ValidSandbox(n.Name()) {
			kind = VMKindOwned
		}
		out = append(out, VMEntry{Name: n.Name(), Kind: kind})
	}
	// Recovery order must be deterministic: tests and operators rely on
	// vms/<id> being processed by id, not by directory hash order.
	slices.SortFunc(out, func(a, b VMEntry) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// randomSuffix returns a short collision-resistant suffix for quarantine
// names. Failure is impossible in practice; the zero suffix still never
// overwrites because the timestamp and original name are present.
func randomSuffix() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "00000000"
	}
	return hex.EncodeToString(b[:])
}

// syncDir fsyncs one directory under root so a create, rename or delete is
// durable after a crash.
func syncDir(root *os.Root, name string) error {
	d, err := root.Open(name)
	if err != nil {
		return fmt.Errorf("storage: open dir %s: %w", name, err)
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	return errors.Join(syncErr, closeErr)
}

// Quarantine moves an owned VM directory under orphans/ for manual review.
// The original name is preserved with a timestamp suffix so repeated
// quarantines never collide. Nothing in orphans/ is ever read or deleted by
// the node again.
//
// ListQuarantinedIDs is the one inspection exception, and it is deliberately
// read-only: it lists directory names only, never opens a manifest or
// trusts orphan content, and exists so restart reconciliation can keep a
// quarantined sandbox's other durable resources (its network allocation)
// reserved until an operator resolves the orphan.
func (s *Store) Quarantine(id string) (string, error) {
	if !ids.ValidSandbox(id) {
		return "", fmt.Errorf("storage: quarantine: %q is not a sandbox id", id)
	}
	dst := fmt.Sprintf("%s-%d-%s", id, time.Now().Unix(), randomSuffix())
	// Both names are manager-generated (validated id, timestamp, random
	// suffix) and the rename runs through the data root, so it cannot escape
	// it or collide with an earlier quarantine.
	if err := s.root.Rename(filepath.Join(vmsDir, id), filepath.Join(orphansDir, dst)); err != nil {
		return "", fmt.Errorf("storage: quarantine %s: %w", id, err)
	}
	var errs []error
	errs = append(errs, syncDir(s.root, vmsDir))
	errs = append(errs, syncDir(s.root, orphansDir))
	if err := errors.Join(errs...); err != nil {
		return dst, fmt.Errorf("storage: quarantine %s: %w", id, err)
	}
	return dst, nil
}

// ListQuarantinedIDs returns the sandbox ids that have a quarantined
// directory under orphans/. Names carry the quarantine suffix
// "<id>-<unix>-<hex>"; the id is cut at the first dash (sandbox ids never
// contain one) and accepted only when it validates. The inspection is
// read-only: no orphan is opened, parsed as a manifest, trusted or deleted,
// and unparseable names are skipped and left alone. It is the durable
// counterpart of the manager's per-boot quarantine set, so a quarantined
// sandbox's retained resources survive restarts until an operator resolves
// the orphan.
func (s *Store) ListQuarantinedIDs() ([]string, error) {
	dir, err := s.root.Open(orphansDir)
	if err != nil {
		return nil, fmt.Errorf("storage: list orphans: %w", err)
	}
	names, err := dir.ReadDir(-1)
	closeErr := dir.Close()
	if err := errors.Join(err, closeErr); err != nil {
		return nil, fmt.Errorf("storage: list orphans: %w", err)
	}
	seen := make(map[string]struct{}, len(names))
	for _, n := range names {
		if !n.IsDir() {
			continue
		}
		id, _, ok := strings.Cut(n.Name(), "-")
		if !ok || !ids.ValidSandbox(id) {
			continue
		}
		seen[id] = struct{}{}
	}
	return slices.Sorted(maps.Keys(seen)), nil
}
