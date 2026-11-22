package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"syscall"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// Phase 2 per-VM files, next to vm.json. They live and die with the VM's
// directory: RemoveVM deletes them, Quarantine moves them.
const (
	phase2StateName    = "phase2.json"
	lastManifestName   = "last-manifest"
	phase2StateSchema  = 1
	phase2StateMaxSize = 64 << 10
)

// ErrPhase2StateCorrupt reports a phase2.json or last-manifest that cannot
// be trusted: undecodable, a different schema, or a manifest copy whose
// digest does not match the recorded one. A snapshot that finds it has no
// usable previous manifest and must scan the whole disk.
var ErrPhase2StateCorrupt = errors.New("storage: phase 2 state is corrupt")

// Phase2State is a VM's durable Phase 2 snapshot state (vms/<id>/phase2.json).
// The zero value is a VM that never had a Phase 2 snapshot.
type Phase2State struct {
	SchemaVersion int `json:"schemaVersion"`
	// LastManifestKey is the object key of the last manifest this VM's disk
	// matches (written by a successful SnapshotVM or restore).
	LastManifestKey string `json:"lastManifestKey,omitempty"`
	// LastManifestSHA256 is the hex SHA-256 of the local copy in
	// last-manifest, so a torn or stale copy is detected.
	LastManifestSHA256 string `json:"lastManifestSha256,omitempty"`
	// FullScanNext is set when the dirty bitmap cannot be trusted (lost,
	// replaced or found disabled): the next snapshot scans the whole disk.
	FullScanNext bool `json:"fullScanNext,omitzero"`
	// SnapshotInProgress is set before a live cut and cleared after its
	// teardown; a restart that finds it set recovers the cut.
	SnapshotInProgress bool `json:"snapshotInProgress,omitzero"`
}

// ReadPhase2 returns the VM's Phase 2 state. A VM without phase2.json has
// the zero state. A file that cannot be trusted wraps ErrPhase2StateCorrupt.
func (s *Store) ReadPhase2(id string) (Phase2State, error) {
	s.phase2Mu.Lock()
	defer s.phase2Mu.Unlock()
	return s.readPhase2Locked(id)
}

func (s *Store) readPhase2Locked(id string) (Phase2State, error) {
	if !ids.ValidSandbox(id) {
		return Phase2State{}, fmt.Errorf("storage: %q is not a sandbox id", id)
	}
	raw, err := s.readVMFile(id, phase2StateName, phase2StateMaxSize)
	if errors.Is(err, fs.ErrNotExist) {
		return Phase2State{SchemaVersion: phase2StateSchema}, nil
	}
	if err != nil {
		return Phase2State{}, err
	}
	var st Phase2State
	if err := json.Unmarshal(raw, &st); err != nil {
		return Phase2State{}, fmt.Errorf("storage: vm %s: %s: %w: %w", id, phase2StateName, ErrPhase2StateCorrupt, err)
	}
	if st.SchemaVersion != phase2StateSchema {
		return Phase2State{}, fmt.Errorf("storage: vm %s: %s schemaVersion %d, want %d: %w", id, phase2StateName, st.SchemaVersion, phase2StateSchema, ErrPhase2StateCorrupt)
	}
	if (st.LastManifestKey == "") != (st.LastManifestSHA256 == "") {
		return Phase2State{}, fmt.Errorf("storage: vm %s: %s has a manifest key without its digest: %w", id, phase2StateName, ErrPhase2StateCorrupt)
	}
	return st, nil
}

// updatePhase2 applies fn to the VM's state and writes it atomically.
func (s *Store) updatePhase2(id string, fn func(*Phase2State)) error {
	s.phase2Mu.Lock()
	defer s.phase2Mu.Unlock()
	st, err := s.readPhase2Locked(id)
	if errors.Is(err, ErrPhase2StateCorrupt) {
		// A corrupt state is replaced, never trusted: its manifest copy is
		// dropped and the next snapshot scans fully.
		st = Phase2State{FullScanNext: true}
	} else if err != nil {
		return err
	}
	fn(&st)
	st.SchemaVersion = phase2StateSchema
	return s.writePhase2Locked(id, st)
}

func (s *Store) writePhase2Locked(id string, st Phase2State) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("storage: vm %s: encode %s: %w", id, phase2StateName, err)
	}
	return s.writeVMFile(id, phase2StateName, raw)
}

// LastManifest returns the key and the local encoded copy of the VM's last
// manifest. ok is false when the VM has none. A copy that is missing or
// does not match its recorded digest wraps ErrPhase2StateCorrupt.
func (s *Store) LastManifest(id string) (key string, encoded []byte, ok bool, err error) {
	s.phase2Mu.Lock()
	defer s.phase2Mu.Unlock()
	st, err := s.readPhase2Locked(id)
	if err != nil {
		return "", nil, false, err
	}
	if st.LastManifestKey == "" {
		return "", nil, false, nil
	}
	raw, err := s.readVMFile(id, lastManifestName, 0)
	if err != nil {
		return "", nil, false, fmt.Errorf("storage: vm %s: %s: %w: %w", id, lastManifestName, ErrPhase2StateCorrupt, err)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != st.LastManifestSHA256 {
		return "", nil, false, fmt.Errorf("storage: vm %s: %s does not match its recorded digest: %w", id, lastManifestName, ErrPhase2StateCorrupt)
	}
	return st.LastManifestKey, raw, true, nil
}

// SetLastManifest records key and its encoded manifest as the VM's last
// manifest. The copy is written first and the state naming its digest
// second, both atomically, so a crash between them leaves a digest mismatch
// (a full scan), never a wrong previous manifest.
func (s *Store) SetLastManifest(id, key string, encoded []byte) error {
	if key == "" {
		return fmt.Errorf("storage: vm %s: empty manifest key", id)
	}
	s.phase2Mu.Lock()
	defer s.phase2Mu.Unlock()
	st, err := s.readPhase2Locked(id)
	if errors.Is(err, ErrPhase2StateCorrupt) {
		st = Phase2State{FullScanNext: true}
	} else if err != nil {
		return err
	}
	if err := s.writeVMFile(id, lastManifestName, encoded); err != nil {
		return err
	}
	sum := sha256.Sum256(encoded)
	st.SchemaVersion = phase2StateSchema
	st.LastManifestKey = key
	st.LastManifestSHA256 = hex.EncodeToString(sum[:])
	return s.writePhase2Locked(id, st)
}

// SetFullScanNext marks the VM's dirty bitmap untrusted: the next snapshot
// scans the whole disk.
func (s *Store) SetFullScanNext(id string) error {
	return s.updatePhase2(id, func(st *Phase2State) { st.FullScanNext = true })
}

// ClearFullScanNext records that a full scan completed (or the disk now
// equals a known manifest), so the bitmap is trusted again.
func (s *Store) ClearFullScanNext(id string) error {
	return s.updatePhase2(id, func(st *Phase2State) { st.FullScanNext = false })
}

// SetSnapshotInProgress durably marks a live cut as started. It must be
// written before the cut's transaction.
func (s *Store) SetSnapshotInProgress(id string) error {
	return s.updatePhase2(id, func(st *Phase2State) { st.SnapshotInProgress = true })
}

// ClearSnapshotInProgress clears the live cut marker after its teardown.
func (s *Store) ClearSnapshotInProgress(id string) error {
	return s.updatePhase2(id, func(st *Phase2State) { st.SnapshotInProgress = false })
}

// RecoverInterruptedSnapshot settles a marker found after a restart in one
// atomic write: the marker is cleared, and fullScan also marks the bitmap
// untrusted (QEMU died with the cut, so its dirty set is lost).
func (s *Store) RecoverInterruptedSnapshot(id string, fullScan bool) error {
	return s.updatePhase2(id, func(st *Phase2State) {
		st.SnapshotInProgress = false
		if fullScan {
			st.FullScanNext = true
		}
	})
}

// readVMFile reads vms/<id>/name without following a symlink. limit > 0
// bounds the size.
func (s *Store) readVMFile(id, name string, limit int64) ([]byte, error) {
	f, err := s.root.OpenFile(path.Join(vmsDir, id, name), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("storage: vm %s: stat %s: %w", id, name, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("storage: vm %s: %s is not a regular file: %w", id, name, ErrPhase2StateCorrupt)
	}
	if limit > 0 && info.Size() > limit {
		return nil, fmt.Errorf("storage: vm %s: %s is %d bytes, over %d: %w", id, name, info.Size(), limit, ErrPhase2StateCorrupt)
	}
	raw := make([]byte, info.Size())
	if _, err := f.ReadAt(raw, 0); err != nil && info.Size() > 0 {
		return nil, fmt.Errorf("storage: vm %s: read %s: %w", id, name, err)
	}
	return raw, nil
}

// writeVMFile replaces vms/<id>/name atomically with writeManifest's
// pattern: a fresh temporary file (never through a symlink), fsync, rename
// over the name, then a directory sync. A failed directory sync after the
// rename wraps ErrManifestDurabilityUnknown.
func (s *Store) writeVMFile(id, name string, data []byte) error {
	if !ids.ValidSandbox(id) {
		return fmt.Errorf("storage: %q is not a sandbox id", id)
	}
	vmRoot, err := s.root.OpenRoot(path.Join(vmsDir, id))
	if err != nil {
		return fmt.Errorf("storage: open vm root %s: %w", id, err)
	}
	defer vmRoot.Close()
	tmp := name + manifestTmpExt
	if err := vmRoot.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("storage: vm %s: clear %s: %w", id, tmp, err)
	}
	f, err := vmRoot.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("storage: vm %s: write %s: %w", id, name, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		vmRoot.Remove(tmp)
		return fmt.Errorf("storage: vm %s: write %s: %w", id, name, err)
	}
	if err := s.cfg.Snapshots.SyncFile(f); err != nil {
		f.Close()
		vmRoot.Remove(tmp)
		return fmt.Errorf("storage: vm %s: sync %s: %w", id, name, err)
	}
	if err := f.Close(); err != nil {
		vmRoot.Remove(tmp)
		return fmt.Errorf("storage: vm %s: close %s: %w", id, name, err)
	}
	if err := s.cfg.Snapshots.Rename(vmRoot, tmp, name); err != nil {
		vmRoot.Remove(tmp)
		return fmt.Errorf("storage: vm %s: publish %s: %w", id, name, err)
	}
	if err := s.cfg.Snapshots.SyncDir(vmRoot, "."); err != nil {
		return fmt.Errorf("storage: vm %s: sync dir: %w:%w", id, ErrManifestDurabilityUnknown, err)
	}
	return nil
}

// Phase 2 scratch names in a VM's directory. A live cut's fleece image and
// QEMU's NBD socket exist only during one snapshot session; a restart that
// finds them left behind removes them (RemovePhase2Scratch).
const (
	// FleeceName is the live cut's temporary qcow2 in vms/<id>/.
	FleeceName = "fleece.qcow2"
	// NBDSocketName is QEMU's NBD server socket: vms/<id>/control/ for a
	// jailed QEMU (/control/nbd.sock inside the jail), vms/<id>/ otherwise.
	NBDSocketName = "nbd.sock"
)

// FleecePath returns the host path of the VM's live-cut fleece image.
func (s *Store) FleecePath(id string) string {
	return filepath.Join(s.VMDir(id), FleeceName)
}

// RemovePhase2Scratch removes a leftover fleece image and stale NBD sockets
// of a snapshot session that did not finish. Only a regular fleece file and
// sockets are removed; anything else under those names is an error and left
// alone. Missing names are fine.
func (s *Store) RemovePhase2Scratch(id string) error {
	if !ids.ValidSandbox(id) {
		return fmt.Errorf("storage: %q is not a sandbox id", id)
	}
	dir := path.Join(vmsDir, id)
	var errs []error
	for _, c := range []struct {
		rel  string
		want fs.FileMode
	}{
		{path.Join(dir, FleeceName), 0},
		{path.Join(dir, NBDSocketName), fs.ModeSocket},
		{path.Join(dir, ControlDirName, NBDSocketName), fs.ModeSocket},
	} {
		info, err := s.root.Lstat(c.rel)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("storage: vm %s: inspect %s: %w", id, c.rel, err))
			continue
		}
		if info.Mode().Type() != c.want {
			errs = append(errs, fmt.Errorf("storage: vm %s: %s has type %v; leaving it alone", id, c.rel, info.Mode().Type()))
			continue
		}
		if err := s.root.Remove(c.rel); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("storage: vm %s: remove %s: %w", id, c.rel, err))
		}
	}
	return errors.Join(errs...)
}
