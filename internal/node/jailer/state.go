package jailer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"slices"
	"strings"
	"syscall"
	"time"

	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

const (
	recordSchema     = 2
	recordsDir       = "records"
	jailsDir         = "jails"
	stateLockName    = ".jailer.lock"
	maxRecordBytes   = 2 << 20
	maxRecordFiles   = 10000
	maxOwnedEntries  = 1024
	maxJailDepth     = 64
	recordTempTries  = 32
	recordTempRandom = 16
)

// Record is the durable reservation and host identity of one jailed process.
// A PID is trusted only together with BootID and StartTicks. Attempt is the
// immutable generation identity the caller chose before Start: a uuid that
// never repeats, unlike timestamps, pids or recycled inode numbers.
// ControlDev/Ino and LogDev/Ino prevent two attempts from sharing pinned
// control resources; TargetDev/Ino is the caller resource the jailer gave to
// the attempt's uid (the writable overlay of a VM). The Manager's reclaim
// policy receives the record, so these identities outlive every chown they
// must undo. Filesystem ownership of the jail itself is kept in a separate
// private envelope.
type Record struct {
	SchemaVersion int    `json:"schemaVersion"`
	SandboxID     string `json:"sandboxId"`
	Attempt       string `json:"attempt"`
	Kind          string `json:"kind"`
	UID           uint32 `json:"uid"`
	PID           int    `json:"pid,omitzero"`
	BootID        string `json:"bootId"`
	StartTicks    uint64 `json:"startTicks,omitzero"`
	ControlDev    uint64 `json:"controlDev"`
	ControlIno    uint64 `json:"controlIno"`
	LogDev        uint64 `json:"logDev"`
	LogIno        uint64 `json:"logIno"`
	TargetDev     uint64 `json:"targetDev,omitzero"`
	TargetIno     uint64 `json:"targetIno,omitzero"`
	// TargetRoot, TargetPath and TargetAccess locate and qualify the
	// target for reclaim after a restart (tool and vm kinds).
	TargetRoot    TargetRoot   `json:"targetRoot,omitzero"`
	TargetPath    string       `json:"targetPath,omitzero"`
	TargetAccess  TargetAccess `json:"targetAccess,omitzero"`
	CreatedUnixMs int64        `json:"createdUnixMs"`
}

// Record kinds. A VM record is keyed by its sandbox id; the CPU probe uses
// the reserved ProbeID, which no sandbox id can equal.
const (
	KindVM    = "vm"
	KindProbe = "probe"
)

// ProbeID is the reserved record id of the node's CPU admission probe. It
// contains 'o', which the sandbox id alphabet excludes.
const ProbeID = "probe_cpu"

// Ref names exactly one attempt: a record matches a Ref only when both its
// SandboxID and its Attempt are equal.
type Ref struct {
	SandboxID string
	Attempt   string
}

// Ref returns the record's attempt reference.
func (r Record) Ref() Ref { return Ref{SandboxID: r.SandboxID, Attempt: r.Attempt} }

// validAttempt requires the canonical lowercase text of a non-nil uuid.
func validAttempt(s string) bool {
	u, err := uuid.Parse(s)
	return err == nil && u != uuid.Nil() && u.String() == s
}

// validRecordID reports whether id may key a record of kind.
func validRecordID(kind, id string) bool {
	switch kind {
	case KindVM:
		return ids.ValidSandbox(id)
	case KindProbe:
		return id == ProbeID
	case KindTool:
		hex, ok := strings.CutPrefix(id, ToolIDPrefix)
		return ok && len(hex) == 32 && strings.Trim(hex, "0123456789abcdef") == ""
	}
	return false
}

// Validate checks fields used for resource ownership and liveness decisions.
func (r Record) Validate() error {
	if r.SchemaVersion != recordSchema {
		return fmt.Errorf("jailer: record schema %d, want %d (records from an older jailer are not migrated)", r.SchemaVersion, recordSchema)
	}
	if !validRecordID(r.Kind, r.SandboxID) {
		return fmt.Errorf("jailer: invalid %q record id %q", r.Kind, r.SandboxID)
	}
	if !validAttempt(r.Attempt) {
		return fmt.Errorf("jailer: record %s has an invalid attempt %q", r.SandboxID, r.Attempt)
	}
	if r.UID == 0 {
		return errors.New("jailer: record has zero uid")
	}
	switch r.Kind {
	case KindVM:
		if r.ControlIno == 0 || r.LogIno == 0 {
			return errors.New("jailer: vm record has no control identity")
		}
		if r.TargetIno == 0 {
			return errors.New("jailer: vm record has no overlay identity")
		}
		// The overlay's location is optional (records written before it
		// existed), but when present it is the data root's owner-rw target.
		if (r.TargetAccess != "" && r.TargetAccess != AccessOwnerRW) || (r.TargetRoot != "" && r.TargetRoot != TargetRootData) ||
			(r.TargetPath != "" && !validTargetPath(r.TargetPath)) {
			return errors.New("jailer: vm record has an invalid overlay location")
		}
	case KindProbe:
		if r.ControlIno == 0 || r.LogIno == 0 {
			return errors.New("jailer: probe record has no control identity")
		}
		if r.TargetIno != 0 || r.TargetDev != 0 || r.TargetRoot != "" || r.TargetPath != "" || r.TargetAccess != "" {
			return errors.New("jailer: probe record names a target")
		}
	case KindTool:
		if r.SandboxID != ToolID(r.Attempt) {
			return errors.New("jailer: tool record id is not its attempt's")
		}
		if r.ControlDev != 0 || r.ControlIno != 0 || r.LogDev != 0 || r.LogIno != 0 {
			return errors.New("jailer: tool record names control resources")
		}
		if r.TargetIno == 0 || r.TargetDev == 0 {
			return errors.New("jailer: tool record has no target identity")
		}
		if err := validTarget(r.TargetRoot, r.TargetPath, r.TargetAccess); err != nil {
			return fmt.Errorf("jailer: tool record: %w", err)
		}
	}
	if r.CreatedUnixMs <= 0 {
		return errors.New("jailer: record has no creation time")
	}
	if r.PID == 0 {
		if r.StartTicks != 0 {
			return errors.New("jailer: launch-intent record has start ticks without a pid")
		}
	} else {
		if r.PID < 0 || r.StartTicks == 0 || r.BootID == "" {
			return errors.New("jailer: running record lacks complete process identity")
		}
	}
	return nil
}

type inodeIdentity struct {
	Dev uint64 `json:"dev"`
	Ino uint64 `json:"ino"`
}

func (id inodeIdentity) valid() bool { return id.Ino != 0 }

func (id inodeIdentity) equal(other inodeIdentity) bool {
	return id.Dev == other.Dev && id.Ino == other.Ino
}

func (id inodeIdentity) Validate() error {
	if !id.valid() || id.Dev == 0 {
		return errors.New("jailer: zero filesystem identity")
	}
	return nil
}

type ownedEntry struct {
	Path string `json:"path"`
	Dev  uint64 `json:"dev"`
	Ino  uint64 `json:"ino"`
	Dir  bool   `json:"dir,omitzero"`
}

func (e ownedEntry) identity() inodeIdentity { return inodeIdentity{Dev: e.Dev, Ino: e.Ino} }

type resourceOwnership struct {
	Jail    inodeIdentity `json:"jail,omitzero"`
	Cgroup  inodeIdentity `json:"cgroup,omitzero"`
	Entries []ownedEntry  `json:"entries,omitempty"`
	// Exec is the jail-relative path ("root/...") of the staged executable,
	// the entry whose inode a live process's /proc/pid/exe must match.
	Exec string `json:"exec,omitzero"`
}

func (o resourceOwnership) validate() error {
	// A jail base identity is persisted before the tree is populated, so a
	// valid jail with no entries is legal. Entries without a base are not.
	if !o.Jail.valid() && len(o.Entries) != 0 {
		return errors.New("jailer: jail inventory has no base identity")
	}
	if len(o.Entries) > maxOwnedEntries {
		return fmt.Errorf("jailer: jail inventory exceeds %d entries", maxOwnedEntries)
	}
	if o.Jail.valid() {
		if err := o.Jail.Validate(); err != nil {
			return fmt.Errorf("jailer: jail identity: %w", err)
		}
		seen := make(map[string]struct{}, len(o.Entries))
		for _, entry := range o.Entries {
			if entry.Path == "" || entry.Path == "." || path.Clean(entry.Path) != entry.Path ||
				strings.HasPrefix(entry.Path, "/") || strings.HasPrefix(entry.Path, "../") {
				return fmt.Errorf("jailer: unsafe owned jail path %q", entry.Path)
			}
			if _, exists := seen[entry.Path]; exists {
				return fmt.Errorf("jailer: duplicate owned jail path %q", entry.Path)
			}
			seen[entry.Path] = struct{}{}
			if entry.Ino == 0 || entry.Dev == 0 {
				return fmt.Errorf("jailer: zero identity for owned jail path %q", entry.Path)
			}
		}
	}
	if o.Exec != "" {
		if !slices.ContainsFunc(o.Entries, func(e ownedEntry) bool { return e.Path == o.Exec && !e.Dir }) {
			return fmt.Errorf("jailer: staged executable %q is not an owned file", o.Exec)
		}
	}
	if o.Cgroup.valid() {
		if err := o.Cgroup.Validate(); err != nil {
			return fmt.Errorf("jailer: cgroup identity: %w", err)
		}
	}
	return nil
}

type persistedRecord struct {
	Record    Record            `json:"record"`
	Ownership resourceOwnership `json:"ownership"`
}

type stateStore interface {
	// Write publishes record. expected is the record inode this owner last
	// published, or invalid for the first write, when the path must not exist.
	Write(root *os.Root, record Record, ownership resourceOwnership, expected inodeIdentity) (inodeIdentity, error)
	Remove(root *os.Root, sandboxID string, expected inodeIdentity) error
}

// errRecordNotPublished marks a Write failure proven to have left nothing
// behind: the attempt never created its temporary inode, or created it and
// durably removed exactly that inode before any rename. Every other failure,
// including a failed rename, a failed cleanup, a failed identity read and a
// failed parent sync after rename, is uncertain.
var errRecordNotPublished = errors.New("jailer: record attempt left no published state")

// stateFault names a durable record boundary. fileStates.fault, when set,
// runs before the operation it names; an error simulates that operation's
// failure. It is a per-store test seam, never package state.
type stateFault string

const (
	faultStateCreate     stateFault = "state-create"
	faultStateIdentity   stateFault = "state-identity"
	faultStateWrite      stateFault = "state-write"
	faultStateSync       stateFault = "state-sync"
	faultStateClose      stateFault = "state-close"
	faultStateRename     stateFault = "state-rename"
	faultStateParentSync stateFault = "state-parent-sync"
	faultStateCleanup    stateFault = "state-cleanup"
	faultStateRemove     stateFault = "state-remove"
	// faultStateRemoveSync is the directory barrier after a removal.
	faultStateRemoveSync stateFault = "state-remove-sync"
)

type fileStates struct {
	fault func(stateFault) error
}

func (s fileStates) inject(stage stateFault) error {
	if s.fault == nil {
		return nil
	}
	return s.fault(stage)
}

func recordPath(id string) string { return path.Join(recordsDir, id+".json") }

// Write atomically replaces id's record with a random attempt-owned temporary
// file. The returned identity is valid whenever the new inode may be the
// published record, including with an error after rename.
//
// Before anything is created, the current final path must be exactly what
// the owner expects: absent for a first write, the expected inode for an
// update. A foreign or replaced record is refused and left untouched; the
// owner's mutex and the StateDir flock serialize its legitimate writes.
func (s fileStates) Write(root *os.Root, record Record, ownership resourceOwnership, expected inodeIdentity) (inodeIdentity, error) {
	notPublished := func(err error) (inodeIdentity, error) {
		return inodeIdentity{}, errors.Join(err, errRecordNotPublished)
	}
	if err := record.Validate(); err != nil {
		return notPublished(err)
	}
	if err := ownership.validate(); err != nil {
		return notPublished(err)
	}
	raw, err := json.Marshal(persistedRecord{Record: record, Ownership: ownership})
	if err != nil {
		return notPublished(fmt.Errorf("jailer: encode record: %w", err))
	}
	if len(raw) > maxRecordBytes {
		return notPublished(fmt.Errorf("jailer: encoded record exceeds %d bytes", maxRecordBytes))
	}
	return s.publish(root, recordsDir, record.SandboxID+".json", "record", raw, expected)
}

// publish atomically replaces dir/base with raw through a random
// attempt-owned O_EXCL temporary file (dir/base.tmp-<random>), with the
// ownership rules of Write: the final path is proven to be expected (absent
// for a first publish) before the temporary is created and again right
// before the rename; a failed attempt removes only its own temporary inode;
// errRecordNotPublished is joined only when nothing can have been published.
// noun names the file kind in errors.
func (s fileStates) publish(root *os.Root, dir, base, noun string, raw []byte, expected inodeIdentity) (inodeIdentity, error) {
	notPublished := func(err error) (inodeIdentity, error) {
		return inodeIdentity{}, errors.Join(err, errRecordNotPublished)
	}
	name := path.Join(dir, base)
	if err := checkRecordOwner(root, name, expected); err != nil {
		return notPublished(err)
	}
	if err := s.inject(faultStateCreate); err != nil {
		return notPublished(fmt.Errorf("jailer: create %s: %w", noun, err))
	}
	tmp, f, tempID, err := createRecordTemp(root, dir, base, noun, s.inject(faultStateIdentity))
	if err != nil {
		if tmp == "" {
			return notPublished(err)
		}
		// The attempt inode exists but could not be identified: it can never
		// be removed by proof, so the outcome is uncertain.
		return inodeIdentity{}, err
	}
	// cleanup removes exactly this attempt's inode. Only a complete, durable
	// removal proves that the failed write left nothing behind.
	cleanup := func(cause error, closed bool) (inodeIdentity, error) {
		var closeErr error
		if !closed {
			closeErr = f.Close()
		}
		removeErr := s.inject(faultStateCleanup)
		if removeErr == nil {
			removeErr = removeOwnedRegular(root, tmp, tempID)
		}
		syncErr := syncDirectory(root, dir)
		if removeErr != nil || syncErr != nil {
			return inodeIdentity{}, errors.Join(cause, closeErr, removeErr, syncErr)
		}
		return notPublished(errors.Join(cause, closeErr))
	}
	if err := s.inject(faultStateWrite); err != nil {
		return cleanup(fmt.Errorf("jailer: write %s: %w", noun, err), false)
	}
	written, err := f.Write(raw)
	if err != nil {
		return cleanup(fmt.Errorf("jailer: write %s: %w", noun, err), false)
	}
	if written != len(raw) {
		return cleanup(fmt.Errorf("jailer: write %s: %w", noun, io.ErrShortWrite), false)
	}
	err = s.inject(faultStateSync)
	if err == nil {
		err = f.Sync()
	}
	if err != nil {
		return cleanup(fmt.Errorf("jailer: sync %s: %w", noun, err), false)
	}
	err = s.inject(faultStateClose)
	if err != nil {
		f.Close()
	} else {
		err = f.Close()
	}
	if err != nil {
		return cleanup(fmt.Errorf("jailer: close %s: %w", noun, err), true)
	}
	err = s.inject(faultStateRename)
	if err == nil {
		// Re-prove the final path right before replacing it, so a file that
		// appeared or was replaced while this attempt was written is
		// preserved rather than overwritten.
		if ownerErr := checkRecordOwner(root, name, expected); ownerErr != nil {
			return cleanup(ownerErr, true)
		}
		err = root.Rename(tmp, name)
	}
	if err != nil {
		// A rename error is not proof of absence. Report the attempt inode
		// only if it is now the published name; never remove what may have
		// been published, and never adopt an older file's identity.
		if current, statErr := statRegularNoFollow(root, name); statErr == nil && current.equal(tempID) {
			return tempID, fmt.Errorf("jailer: publish %s: %w", noun, err)
		}
		return inodeIdentity{}, fmt.Errorf("jailer: publish %s (outcome uncertain): %w", noun, err)
	}
	// The temporary inode is now the published file. A parent-sync error is
	// an uncertain commit: return its identity with the error and never remove
	// it or permit its resources to be reused.
	err = s.inject(faultStateParentSync)
	if err == nil {
		err = syncDirectory(root, dir)
	}
	if err != nil {
		return tempID, fmt.Errorf("jailer: sync published %s: %w", noun, err)
	}
	return tempID, nil
}

// checkRecordOwner proves the final record path is what its owner expects.
func checkRecordOwner(root *os.Root, name string, expected inodeIdentity) error {
	current, err := statRegularNoFollow(root, name)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		if expected.valid() {
			return fmt.Errorf("jailer: owned record %s disappeared; refusing to recreate it", name)
		}
		return nil
	case err != nil:
		return fmt.Errorf("jailer: inspect record %s: %w", name, err)
	case !expected.valid():
		return fmt.Errorf("jailer: foreign record %s already exists; preserving it", name)
	case !current.equal(expected):
		return fmt.Errorf("jailer: record %s was replaced; preserving it", name)
	}
	return nil
}

// createRecordTemp creates a random attempt-owned temporary file
// dir/base.tmp-<random>. On an identity failure it closes the file but still
// returns its name, telling the caller that an unidentified inode may remain.
func createRecordTemp(root *os.Root, dir, base, noun string, identityFault error) (string, *os.File, inodeIdentity, error) {
	for range recordTempTries {
		var random [recordTempRandom]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", nil, inodeIdentity{}, fmt.Errorf("jailer: create %s attempt name: %w", noun, err)
		}
		name := path.Join(dir, base+".tmp-"+hex.EncodeToString(random[:]))
		f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return "", nil, inodeIdentity{}, fmt.Errorf("jailer: create %s: %w", noun, err)
		}
		identity, err := fileInode(f)
		if identityFault != nil {
			err = identityFault
		}
		if err != nil {
			f.Close()
			return name, nil, inodeIdentity{}, fmt.Errorf("jailer: identify %s attempt: %w", noun, err)
		}
		return name, f, identity, nil
	}
	return "", nil, inodeIdentity{}, fmt.Errorf("jailer: could not allocate a unique %s attempt name", noun)
}

// Remove unlinks id's record only when it is still the expected inode.
func (s fileStates) Remove(root *os.Root, sandboxID string, expected inodeIdentity) error {
	if err := s.removePublished(root, recordsDir, sandboxID+".json", expected); err != nil {
		return fmt.Errorf("jailer: record %s: %w", sandboxID, err)
	}
	return nil
}

// removePublished unlinks dir/base only while it is the expected inode, then
// syncs dir. An already absent name still gets the barrier: a previous unlink
// may have happened before its uncertain parent sync.
func (s fileStates) removePublished(root *os.Root, dir, base string, expected inodeIdentity) error {
	if err := s.inject(faultStateRemove); err != nil {
		return fmt.Errorf("remove: %w", err)
	}
	name := path.Join(dir, base)
	current, err := statRegularNoFollow(root, name)
	if errors.Is(err, fs.ErrNotExist) {
		return s.syncPublishedDir(root, dir)
	}
	if err != nil {
		return fmt.Errorf("inspect for removal: %w", err)
	}
	if !expected.valid() || !current.equal(expected) {
		return fmt.Errorf("%s was replaced; preserving it", name)
	}
	if err := root.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove: %w", err)
	}
	return s.syncPublishedDir(root, dir)
}

func (s fileStates) syncPublishedDir(root *os.Root, dir string) error {
	if err := s.inject(faultStateRemoveSync); err != nil {
		return fmt.Errorf("jailer: sync directory %s: %w", dir, err)
	}
	return syncDirectory(root, dir)
}

func removeOwnedRegular(root *os.Root, name string, expected inodeIdentity) error {
	current, err := statRegularNoFollow(root, name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !current.equal(expected) {
		return fmt.Errorf("jailer: attempt entry %s was replaced; preserving it", name)
	}
	if err := root.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("jailer: remove attempt entry %s: %w", name, err)
	}
	return nil
}

func fileInode(f *os.File) (inodeIdentity, error) {
	info, err := f.Stat()
	if err != nil {
		return inodeIdentity{}, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return inodeIdentity{}, errors.New("jailer: file identity is not a Linux stat")
	}
	return inodeIdentity{Dev: uint64(st.Dev), Ino: st.Ino}, nil
}

// openNoFollow opens name under root without following a final symlink.
// os.Root follows a final in-root symlink even with O_NOFOLLOW, so the name
// is Lstat'ed first and must be the wanted type (a directory, or a regular
// file), the open is nonblocking (a FIFO or device never blocks; its type
// was refused before), and the opened inode must be the Lstat'ed one, which
// the name must still be afterwards. Nothing is created or truncated.
func openNoFollow(root *os.Root, name string, flag int, wantDir bool) (*os.File, error) {
	before, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !noFollowType(before, wantDir) {
		return nil, fmt.Errorf("jailer: %s is not a %s of its own name", name, noFollowKind(wantDir))
	}
	if wantDir {
		flag |= syscall.O_DIRECTORY
	}
	f, err := root.OpenFile(name, flag|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	opened, err := f.Stat()
	if err == nil {
		var after os.FileInfo
		if after, err = root.Lstat(name); err == nil &&
			(!noFollowType(after, wantDir) || !os.SameFile(before, opened) || !os.SameFile(after, opened)) {
			err = fmt.Errorf("jailer: %s changed while it was opened", name)
		}
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func noFollowType(info os.FileInfo, wantDir bool) bool {
	if wantDir {
		return info.Mode().IsDir()
	}
	return info.Mode().IsRegular()
}

func noFollowKind(wantDir bool) string {
	if wantDir {
		return "directory"
	}
	return "regular file"
}

func statRegularNoFollow(root *os.Root, name string) (inodeIdentity, error) {
	f, err := openNoFollow(root, name, os.O_RDONLY, false)
	if err != nil {
		return inodeIdentity{}, err
	}
	defer f.Close()
	return fileInode(f)
}

func statDirectoryNoFollow(root *os.Root, name string) (inodeIdentity, error) {
	f, err := openNoFollow(root, name, os.O_RDONLY, true)
	if err != nil {
		return inodeIdentity{}, err
	}
	defer f.Close()
	return fileInode(f)
}

func syncDirectory(root *os.Root, name string) error {
	d, err := openNoFollow(root, name, os.O_RDONLY, true)
	if err != nil {
		return fmt.Errorf("jailer: open directory %s for sync: %w", name, err)
	}
	err = errors.Join(d.Sync(), d.Close())
	if err != nil {
		return fmt.Errorf("jailer: sync directory %s: %w", name, err)
	}
	return nil
}

func readBoundedRegular(root *os.Root, name string, limit int64) ([]byte, inodeIdentity, error) {
	f, err := openNoFollow(root, name, os.O_RDONLY, false)
	if err != nil {
		return nil, inodeIdentity{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, inodeIdentity{}, err
	}
	if info.Size() < 0 || info.Size() > limit {
		return nil, inodeIdentity{}, fmt.Errorf("jailer: %s exceeds %d bytes", name, limit)
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, inodeIdentity{}, err
	}
	if int64(len(raw)) > limit {
		return nil, inodeIdentity{}, fmt.Errorf("jailer: %s exceeds %d bytes", name, limit)
	}
	identity, err := fileInode(f)
	if err != nil {
		return nil, inodeIdentity{}, err
	}
	return raw, identity, nil
}

func loadRecords(root *os.Root) (map[string]Record, map[string]resourceOwnership, map[string]inodeIdentity, error) {
	dir, err := root.OpenFile(recordsDir, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("jailer: open records: %w", err)
	}
	entries, err := dir.ReadDir(maxRecordFiles + 1)
	closeErr := dir.Close()
	if errors.Is(err, io.EOF) { // an empty directory
		err = nil
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("jailer: read records: %w", err)
	}
	if closeErr != nil {
		return nil, nil, nil, fmt.Errorf("jailer: close records: %w", closeErr)
	}
	if len(entries) > maxRecordFiles {
		return nil, nil, nil, fmt.Errorf("jailer: more than %d record entries", maxRecordFiles)
	}
	records := make(map[string]Record, len(entries))
	ownerships := make(map[string]resourceOwnership, len(entries))
	identities := make(map[string]inodeIdentity, len(entries))
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		name := path.Join(recordsDir, entry.Name())
		raw, identity, err := readBoundedRegular(root, name, maxRecordBytes)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("jailer: read record %s: %w", entry.Name(), err)
		}
		var stored persistedRecord
		if err := json.Unmarshal(raw, &stored); err != nil {
			return nil, nil, nil, fmt.Errorf("jailer: decode record %s: %w", entry.Name(), err)
		}
		record := stored.Record
		if err := record.Validate(); err != nil {
			return nil, nil, nil, fmt.Errorf("jailer: record %s: %w", entry.Name(), err)
		}
		if err := stored.Ownership.validate(); err != nil {
			return nil, nil, nil, fmt.Errorf("jailer: record %s ownership: %w", entry.Name(), err)
		}
		if recordPath(record.SandboxID) != name {
			return nil, nil, nil, fmt.Errorf("jailer: record %s belongs to %s", entry.Name(), record.SandboxID)
		}
		if _, exists := records[record.SandboxID]; exists {
			return nil, nil, nil, fmt.Errorf("jailer: duplicate record for %s", record.SandboxID)
		}
		records[record.SandboxID] = record
		ownerships[record.SandboxID] = stored.Ownership
		identities[record.SandboxID] = identity
	}
	return records, ownerships, identities, nil
}

func validateInventory(records map[string]Record, uids UIDRange) error {
	usedUIDs := make(map[uint32]string, len(records))
	// held maps every caller resource inode, in any role, to its holder: an
	// inode may be only one record's control directory, log or target.
	type holder struct {
		id, role string
		access   TargetAccess
	}
	held := make(map[inodeIdentity]holder, 3*len(records))
	for _, id := range slices.Sorted(maps.Keys(records)) {
		record := records[id]
		if record.SandboxID != id {
			return fmt.Errorf("jailer: record map key %s belongs to %s", id, record.SandboxID)
		}
		if record.UID < uids.Min || record.UID > uids.Max {
			return fmt.Errorf("jailer: record %s uid %d is outside configured range %d-%d", id, record.UID, uids.Min, uids.Max)
		}
		if other, exists := usedUIDs[record.UID]; exists {
			return fmt.Errorf("jailer: records %s and %s share uid %d", other, id, record.UID)
		}
		usedUIDs[record.UID] = id
		for _, r := range callerResources(record) {
			if other, exists := held[r.identity]; exists {
				// Only shared-ro readers may share an inode.
				if r.role == "target" && other.role == "target" && !targetsConflict(other.access, r.access) {
					continue
				}
				shared := r.role
				if other.role != r.role {
					shared = other.role + "/" + r.role
				}
				return fmt.Errorf("jailer: records %s and %s share %s identity", other.id, id, shared)
			}
			held[r.identity] = holder{id: id, role: r.role, access: r.access}
		}
	}
	return nil
}

func sortedRecords(records map[string]Record) []Record {
	out := make([]Record, 0, len(records))
	for _, record := range records {
		out = append(out, record)
	}
	slices.SortFunc(out, func(a, b Record) int {
		if a.UID != b.UID {
			return int(a.UID) - int(b.UID)
		}
		return strings.Compare(a.SandboxID, b.SandboxID)
	})
	return out
}

// privateDeviceHost maps the jail path of a package-fixed device node the
// helper creates (mknod, VM-owned) to the host node whose rdev it must have.
// Only these entries may be devices in a jail.
func privateDeviceHost(entryPath string) (string, bool) {
	switch entryPath {
	case "root/dev/kvm":
		return "/dev/kvm", true
	case "root/dev/urandom":
		return "/dev/urandom", true
	case "root/dev/null":
		return "/dev/null", true
	}
	return "", false
}

func isPrivateDevice(entryPath string) bool {
	_, ok := privateDeviceHost(entryPath)
	return ok
}

// jailInventory returns the ordinary files and directories below base. When
// allowDevices is true, only the three package-created character devices
// (privateDeviceHost: kvm, urandom, null) are admitted; their rdev and owner
// are checked here and their inode again before removal.
func jailInventory(ctx context.Context, root *os.Root, base string, allowDevices bool, uid uint32) ([]ownedEntry, error) {
	var entries []ownedEntry
	var walk func(string, int) error
	walk = func(relative string, depth int) error {
		if depth > maxJailDepth {
			return fmt.Errorf("jailer: jail depth exceeds %d", maxJailDepth)
		}
		dirPath := path.Join(base, relative)
		dir, err := root.OpenFile(dirPath, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return fmt.Errorf("jailer: open jail directory %s: %w", relative, err)
		}
		children, readErr := dir.ReadDir(maxOwnedEntries + 1)
		closeErr := dir.Close()
		if errors.Is(readErr, io.EOF) { // an empty directory
			readErr = nil
		}
		if readErr != nil {
			return fmt.Errorf("jailer: read jail directory %s: %w", relative, readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("jailer: close jail directory %s: %w", relative, closeErr)
		}
		for _, child := range children {
			if err := ctx.Err(); err != nil {
				return context.Cause(ctx)
			}
			if len(entries) >= maxOwnedEntries {
				return fmt.Errorf("jailer: jail exceeds %d owned entries", maxOwnedEntries)
			}
			name := child.Name()
			if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') {
				return fmt.Errorf("jailer: unsafe jail entry %q", name)
			}
			entryPath := path.Join(relative, name)
			info, err := root.Lstat(path.Join(base, entryPath))
			if err != nil {
				return fmt.Errorf("jailer: inspect jail entry %s: %w", entryPath, err)
			}
			st, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return fmt.Errorf("jailer: jail entry %s has no Linux identity", entryPath)
			}
			switch {
			case info.IsDir():
				entries = append(entries, ownedEntry{Path: entryPath, Dev: uint64(st.Dev), Ino: st.Ino, Dir: true})
				if err := walk(entryPath, depth+1); err != nil {
					return err
				}
			case info.Mode().IsRegular():
				entries = append(entries, ownedEntry{Path: entryPath, Dev: uint64(st.Dev), Ino: st.Ino})
			case allowDevices && info.Mode()&os.ModeDevice != 0 && info.Mode()&os.ModeCharDevice != 0 &&
				isPrivateDevice(entryPath):
				hostName, _ := privateDeviceHost(entryPath)
				rdev, err := fixedDevice(hostName)
				if err != nil {
					return err
				}
				if uint64(st.Rdev) != rdev || st.Uid != uid || st.Gid != uid {
					return fmt.Errorf("jailer: jail device %s does not match its package-owned identity", entryPath)
				}
				entries = append(entries, ownedEntry{Path: entryPath, Dev: uint64(st.Dev), Ino: st.Ino})
			default:
				return fmt.Errorf("jailer: unexpected or special jail entry %s", entryPath)
			}
		}
		return nil
	}
	if err := walk(".", 0); err != nil {
		return nil, err
	}
	slices.SortFunc(entries, func(a, b ownedEntry) int { return strings.Compare(a.Path, b.Path) })
	return entries, nil
}

func removeOwnedJail(ctx context.Context, root *os.Root, sandboxID string, ownership resourceOwnership, uid uint32) error {
	base := path.Join(jailsDir, sandboxID)
	if _, err := statDirectoryNoFollow(root, base); errors.Is(err, fs.ErrNotExist) {
		return syncDirectory(root, jailsDir)
	} else if err != nil {
		return fmt.Errorf("jailer: inspect jail for removal: %w", err)
	}
	if !ownership.Jail.valid() {
		return fmt.Errorf("jailer: jail %s exists without persisted original identity; preserving it", sandboxID)
	}
	currentBase, err := statDirectoryNoFollow(root, base)
	if err != nil {
		return err
	}
	if !currentBase.equal(ownership.Jail) {
		return fmt.Errorf("jailer: jail %s was replaced; preserving it", sandboxID)
	}
	actual, err := jailInventory(ctx, root, base, true, uid)
	if err != nil {
		return err
	}
	expected := make(map[string]ownedEntry, len(ownership.Entries))
	for _, entry := range ownership.Entries {
		expected[entry.Path] = entry
	}
	current := make(map[string]ownedEntry, len(actual))
	for _, entry := range actual {
		current[entry.Path] = entry
		old, known := expected[entry.Path]
		if !known {
			if !isPrivateDevice(entry.Path) {
				return fmt.Errorf("jailer: foreign jail entry %s; preserving it", entry.Path)
			}
			continue
		}
		if old.Dir != entry.Dir || !old.identity().equal(entry.identity()) {
			return fmt.Errorf("jailer: jail entry %s was replaced; preserving it", entry.Path)
		}
	}
	// Deepest entries are removed first. Missing entries are the result of an
	// earlier retryable cleanup and are not recreated or treated as foreign.
	slices.SortFunc(actual, func(a, b ownedEntry) int {
		if da, db := strings.Count(a.Path, "/"), strings.Count(b.Path, "/"); da != db {
			return db - da
		}
		return strings.Compare(b.Path, a.Path)
	})
	for _, entry := range actual {
		name := path.Join(base, entry.Path)
		var err error
		if entry.Dir {
			var identity inodeIdentity
			identity, err = statDirectoryNoFollow(root, name)
			if err == nil && !identity.equal(entry.identity()) {
				err = errors.New("directory inode changed during cleanup")
			}
			if err == nil {
				err = root.Remove(name)
			}
		} else if isPrivateDevice(entry.Path) {
			var info fs.FileInfo
			info, err = root.Lstat(name)
			if err == nil {
				st, ok := info.Sys().(*syscall.Stat_t)
				if !ok || st.Dev != entry.Dev || st.Ino != entry.Ino {
					err = errors.New("device inode changed during cleanup")
				} else {
					err = root.Remove(name)
				}
			}
		} else {
			err = removeOwnedRegular(root, name, entry.identity())
		}
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("jailer: remove owned jail entry %s: %w", entry.Path, err)
		}
		if err := syncDirectory(root, path.Dir(name)); err != nil {
			return err
		}
	}
	// Entries already absent on a retry still need their parent directories
	// removed when those directories themselves were absent from actual only
	// because a prior cleanup removed them; actual contains every live parent.
	if err := root.Remove(base); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("jailer: remove jail %s: %w", sandboxID, err)
	}
	return syncDirectory(root, jailsDir)
}

func newRecord(spec LaunchSpec, kind string, uid uint32) (Record, error) {
	controlDev, controlIno, err := fileIdentity(spec.ControlDir)
	if err != nil {
		return Record{}, fmt.Errorf("jailer: control identity: %w", err)
	}
	logDev, logIno, err := fileIdentity(spec.SerialLog)
	if err != nil {
		return Record{}, fmt.Errorf("jailer: log identity: %w", err)
	}
	var target inodeIdentity
	if len(spec.Disks) > 0 {
		if target, err = fileInode(spec.descriptor(spec.Disks[len(spec.Disks)-1])); err != nil {
			return Record{}, fmt.Errorf("jailer: overlay identity: %w", err)
		}
	}
	bootID, err := hostBootID()
	if err != nil {
		return Record{}, err
	}
	record := Record{
		SchemaVersion: recordSchema,
		SandboxID:     spec.SandboxID,
		Attempt:       spec.Attempt,
		Kind:          kind,
		UID:           uid,
		BootID:        bootID,
		ControlDev:    controlDev,
		ControlIno:    controlIno,
		LogDev:        logDev,
		LogIno:        logIno,
		TargetDev:     target.Dev,
		TargetIno:     target.Ino,
		CreatedUnixMs: time.Now().UnixMilli(),
	}
	return record, record.Validate()
}
