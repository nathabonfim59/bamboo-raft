package storage

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// manifestSchema is the version of vms/<id>/vm.json this node writes and
// reads. Older manifests are treated as unreadable.
const manifestSchema = 1

// Resources is the durable form of a VM's sizing.
type Resources struct {
	VCPUs     uint32 `json:"vcpus"`
	MemoryMiB uint64 `json:"memoryMiB"`
	DiskGiB   uint64 `json:"diskGiB"`
}

// QEMUState tracks how far a launch progressed before it was interrupted.
// The window between QEMULaunching and QEMURunning is recovered after a
// crash through QMP peer identity, never through a bare pid.
type QEMUState string

const (
	// QEMULaunching means the node recorded its intent to start QEMU but
	// had not yet committed the child's identity.
	QEMULaunching QEMUState = "launching"
	// QEMURunning means the child's pid and identity are committed.
	QEMURunning QEMUState = "running"
)

// QEMURecord identifies the QEMU process of one VM. A pid alone is never
// trusted after a restart: the host boot id plus the process's start ticks
// from /proc/<pid>/stat make the recorded pid unambiguous even after pid
// reuse, and the launcher rechecks exe, command line and owned paths before
// touching the process.
type QEMURecord struct {
	State           QEMUState `json:"state"`
	PID             int       `json:"pid,omitzero"`
	BootID          string    `json:"bootId"`
	StartTicks      uint64    `json:"startTicks,omitzero"`
	StartedAtUnixMs int64     `json:"startedAtUnixMs"`
	// JailAttempt and ArgvSHA256 are set for a jailed launch before the
	// jailer runs: the attempt that owns the QEMU and the sha256 of its
	// exact argv (each element NUL-terminated, as /proc/pid/cmdline).
	JailAttempt string `json:"jailAttempt,omitzero"`
	ArgvSHA256  string `json:"argvSha256,omitzero"`
}

// VMMeta is the durable manifest of one VM (vms/<id>/vm.json, mode 0600).
// It carries the VM environment, so the file and its directory stay private
// to the node's service account.
type VMMeta struct {
	SchemaVersion   int               `json:"schemaVersion"`
	SandboxID       string            `json:"sandboxId"`
	Image           string            `json:"image"`
	FromSnapshot    string            `json:"fromSnapshot,omitzero"`
	Resources       Resources         `json:"resources"`
	Env             map[string]string `json:"env,omitzero"`
	Hostname        string            `json:"hostname,omitzero"`
	VsockCID        uint32            `json:"vsockCID"`
	CreatedAtUnixMs int64             `json:"createdAtUnixMs"`
	// StartedAtUnixMs is when the VM last became READY (CreateVM or
	// StartVM); 0 if it never did.
	StartedAtUnixMs int64 `json:"startedAtUnixMs,omitzero"`
	// StoppedAtUnixMs marks a STOPPED VM: no QEMU process exists and the
	// disk is kept on this node. It stays set while a StartVM is in flight
	// (the interrupted start recovers as STOPPED) and is cleared when the
	// VM becomes READY again. With QEMU == nil it is the difference between
	// a kept disk and a never-launched orphan.
	StoppedAtUnixMs int64 `json:"stoppedAtUnixMs,omitzero"`
	// LastError explains a FAILED VM (dead QEMU and similar); persisted so
	// the failure survives node restarts. It never contains environment
	// values.
	LastError string      `json:"lastError,omitzero"`
	QEMU      *QEMURecord `json:"qemu,omitzero"`
	// Control and Console are the recorded inodes of the jailed launch's
	// control directory and console log (PinLaunch). Absent for a VM that
	// was never launched jailed.
	Control *Identity `json:"control,omitzero"`
	Console *Identity `json:"console,omitzero"`
}

// Stopped reports the durable STOPPED marker: no QEMU record exists and a
// completed stop left its timestamp. Reconciliation uses this to keep the
// disk instead of treating the record as a never-launched orphan.
func (m VMMeta) Stopped() bool {
	return m.QEMU == nil && m.StoppedAtUnixMs > 0
}

// HostBootID returns the current host boot id: with a process's start ticks
// it pins a recorded pid to one particular incarnation of that pid.
func HostBootID() (string, error) {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("storage: host boot id: %w", err)
	}
	id := strings.TrimSpace(string(b))
	if id == "" {
		return "", errors.New("storage: host boot id is empty")
	}
	return id, nil
}

// ProcStartTicks returns field 22 of /proc/<pid>/stat: the process's start
// time in clock ticks. It errors when the process does not exist.
func ProcStartTicks(pid int) (uint64, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	// The comm field can contain spaces and parens; the fields the node
	// needs start after the last ')'.
	_, rest, ok := bytes.CutLast(raw, []byte(")"))
	if !ok {
		return 0, fmt.Errorf("storage: proc %d: malformed stat", pid)
	}
	fields := strings.Fields(string(rest))
	// rest starts with " <state> ppid ..."; field 22 of the full stat line
	// is startticks = fields[19] of the remainder (state=1).
	if len(fields) < 20 {
		return 0, fmt.Errorf("storage: proc %d: short stat line", pid)
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("storage: proc %d: bad start ticks: %w", pid, err)
	}
	return ticks, nil
}

// CreateVM creates the VM's directory, writes the initial manifest and
// builds the disk overlay. The manifest is durable before the overlay
// exists, so a crash can never leave a disk without a readable record.
// requestedGiB must be at least the image's base size; larger values grow
// the overlay after creation.
func (s *Store) CreateVM(ctx context.Context, meta VMMeta, img Image) error {
	if !ids.ValidSandbox(meta.SandboxID) {
		return fmt.Errorf("storage: %q is not a sandbox id", meta.SandboxID)
	}
	if meta.Resources.DiskGiB < img.RootVirtualSizeGiB {
		return fmt.Errorf("storage: disk %d GiB is smaller than image base %d GiB", meta.Resources.DiskGiB, img.RootVirtualSizeGiB)
	}
	dir := path.Join(vmsDir, meta.SandboxID)
	if err := s.root.Mkdir(dir, 0o700); err != nil {
		return fmt.Errorf("storage: create vm dir: %w", err)
	}
	if err := syncDir(s.root, vmsDir); err != nil {
		s.removeCreated(dir, err)
		return err
	}
	meta.SchemaVersion = manifestSchema
	if err := s.writeManifest(meta); err != nil {
		s.removeCreated(dir, err)
		return err
	}
	// The overlay is a new pinned file the tool fills: it never creates or
	// reopens a name of its own.
	rel := path.Join(dir, diskName)
	overlay, err := s.root.OpenFile(rel, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600)
	if err != nil {
		s.removeCreated(dir, err)
		return fmt.Errorf("storage: create overlay: %w", err)
	}
	defer overlay.Close()
	ref := ImageRef{File: overlay, Root: ImageRootData, Path: rel, Format: "qcow2", BackingName: s.imageBaseName(img.ID)}
	if err := s.imager.CreateOverlay(ctx, ref, img.RootVirtualSize); err != nil {
		s.removeCreated(dir, err)
		return fmt.Errorf("storage: overlay: %w", err)
	}
	if meta.Resources.DiskGiB > img.RootVirtualSizeGiB {
		grow, closeRef, err := s.overlayRef(rel, img.ID, true)
		if err == nil {
			err = s.imager.Grow(ctx, grow, meta.Resources.DiskGiB<<30)
			closeRef()
		}
		if err != nil {
			s.removeCreated(dir, err)
			return fmt.Errorf("storage: grow overlay: %w", err)
		}
	}
	return nil
}

// removeCreated removes a half-built VM directory after cause. It is best
// effort: the caller is already returning cause. A directory holding a
// target a tool still holds (or whose tool failure is ErrToolRetained) is
// kept exactly as it is, for the tool's settlement and a later DeleteVM.
func (s *Store) removeCreated(dir string, cause error) {
	if s.keepForTool(dir, cause) {
		s.log.Warn("storage: keeping incomplete vm: a jailed tool still holds its disk", "dir", dir, "error", cause)
		return
	}
	if err := s.root.RemoveAll(dir); err != nil {
		s.log.Warn("storage: remove incomplete vm", "dir", dir, "error", err)
	}
}

// CreateVMFromSnapshot creates a VM with a private copy of a committed
// snapshot. The caller holds the snapshot's per-id reservation until this
// function returns, so DeleteSnapshot cannot remove the source during copying.
func (s *Store) CreateVMFromSnapshot(ctx context.Context, meta VMMeta, img Image, snap Snapshot) error {
	if !ids.ValidSandbox(meta.SandboxID) {
		return fmt.Errorf("storage: %q is not a sandbox id", meta.SandboxID)
	}
	if snap.Meta.SnapshotID == "" || snap.Meta.Image != img.ID || snap.Meta.ImageSHA256 != img.RootSHA256 {
		return errors.New("storage: snapshot identity does not match its verified image")
	}
	if meta.Resources.DiskGiB < ceilGiB(snap.Meta.VirtualSize) {
		return fmt.Errorf("storage: disk %d GiB is smaller than snapshot size %d bytes", meta.Resources.DiskGiB, snap.Meta.VirtualSize)
	}
	// Revalidate the pinned source immediately before copying: a committed
	// disk that changed after startup must never become a child disk.
	committed, err := s.ReadSnapshot(ctx, snap.Meta.SnapshotID, map[string]Image{img.ID: img})
	if err != nil {
		return fmt.Errorf("storage: validate snapshot source: %w", err)
	}
	dir := path.Join(vmsDir, meta.SandboxID)
	if err := s.root.Mkdir(dir, 0o700); err != nil {
		return fmt.Errorf("storage: create vm dir: %w", err)
	}
	if err := syncDir(s.root, vmsDir); err != nil {
		s.removeCreated(dir, err)
		return err
	}
	meta.SchemaVersion = manifestSchema
	if err := s.writeManifest(meta); err != nil {
		s.removeCreated(dir, err)
		return err
	}
	committedRoot, err := s.root.OpenRoot(path.Join(snapshotCommittedDir, SnapshotKey(committed.Meta.SnapshotID)))
	if err != nil {
		s.removeCreated(dir, err)
		return fmt.Errorf("storage: open committed snapshot root: %w", err)
	}
	source, err := openNoFollow(committedRoot, diskName)
	committedRoot.Close()
	if err != nil {
		s.removeCreated(dir, err)
		return fmt.Errorf("storage: open committed snapshot disk: %w", err)
	}
	vmRoot, err := s.root.OpenRoot(dir)
	if err != nil {
		source.Close()
		s.removeCreated(dir, err)
		return fmt.Errorf("storage: open snapshot child root: %w", err)
	}
	f, err := vmRoot.OpenFile(diskName, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	vmRoot.Close()
	if err != nil {
		source.Close()
		s.removeCreated(dir, err)
		return fmt.Errorf("storage: create snapshot child: %w", err)
	}
	if err := s.cfg.Snapshots.Copy(ctx, source, f); err != nil {
		source.Close()
		f.Close()
		s.removeCreated(dir, err)
		return fmt.Errorf("storage: copy snapshot child: %w", err)
	}
	relative := path.Join(dir, diskName)
	syncErr := s.cfg.Snapshots.SyncFile(f)
	closeErr := errors.Join(source.Close(), f.Close())
	if err := errors.Join(syncErr, closeErr); err != nil {
		s.removeCreated(dir, err)
		return fmt.Errorf("storage: sync snapshot child: %w", err)
	}
	if err := s.validateSnapshotDisk(ctx, relative, img, snap.Meta.VirtualSize); err != nil {
		s.removeCreated(dir, err)
		return err
	}
	diskSHA256, err := hashFile(ctx, s.root, relative)
	if err != nil || diskSHA256 != snap.Meta.DiskSHA256 {
		s.removeCreated(dir, err)
		return fmt.Errorf("storage: verify snapshot child hash: %w", errors.Join(err, errors.New("sha256 mismatch")))
	}
	if err := syncDir(s.root, dir); err != nil {
		s.removeCreated(dir, err)
		return err
	}
	if meta.Resources.DiskGiB > ceilGiB(snap.Meta.VirtualSize) {
		grow, closeRef, err := s.overlayRef(relative, img.ID, true)
		if err == nil {
			err = s.imager.Grow(ctx, grow, meta.Resources.DiskGiB<<30)
			closeRef()
		}
		if err != nil {
			s.removeCreated(dir, err)
			return fmt.Errorf("storage: grow snapshot child: %w", err)
		}
	}
	return nil
}

// WriteLaunchIntent records that QEMU is about to start, before it does:
// a crash in the window after Start but before CommitRunning is recovered
// from this record plus QMP peer identity.
func (s *Store) WriteLaunchIntent(id string) error {
	meta, err := s.ReadVM(id)
	if err != nil {
		return err
	}
	bootID, err := HostBootID()
	if err != nil {
		return err
	}
	meta.QEMU = &QEMURecord{State: QEMULaunching, BootID: bootID, StartedAtUnixMs: time.Now().UnixMilli()}
	return s.writeManifest(meta)
}

// CommitRunning records the child's pid and start ticks once QEMU is up and
// its QMP socket has been proven to belong to that pid.
func (s *Store) CommitRunning(id string, pid int) error {
	meta, err := s.ReadVM(id)
	if err != nil {
		return err
	}
	if meta.QEMU == nil {
		return fmt.Errorf("storage: vm %s has no launch intent to commit", id)
	}
	ticks, err := ProcStartTicks(pid)
	if err != nil {
		return fmt.Errorf("storage: vm %s: %w", id, err)
	}
	meta.QEMU.State = QEMURunning
	meta.QEMU.PID = pid
	meta.QEMU.StartTicks = ticks
	return s.writeManifest(meta)
}

// CommitStopped durably records a stopped VM: no QEMU process exists, the
// disk is kept and a previous failure explanation no longer applies. The
// caller must have proven the process gone before calling.
func (s *Store) CommitStopped(id string, nowUnixMs int64) error {
	if nowUnixMs <= 0 {
		return fmt.Errorf("storage: vm %s: stop timestamp %d is invalid", id, nowUnixMs)
	}
	meta, err := s.ReadVM(id)
	if err != nil {
		return err
	}
	meta.QEMU = nil
	meta.LastError = ""
	meta.StoppedAtUnixMs = nowUnixMs
	return s.writeManifest(meta)
}

// CommitReady records the moment a VM became READY (CreateVM or StartVM) and
// clears the stop marker of the boot that just completed.
func (s *Store) CommitReady(id string, nowUnixMs int64) error {
	if nowUnixMs <= 0 {
		return fmt.Errorf("storage: vm %s: ready timestamp %d is invalid", id, nowUnixMs)
	}
	meta, err := s.ReadVM(id)
	if err != nil {
		return err
	}
	meta.StartedAtUnixMs = nowUnixMs
	meta.StoppedAtUnixMs = 0
	return s.writeManifest(meta)
}

// PrepareStart durably applies a StartVM's replacement fields — resources,
// env and hostname — to a currently stopped VM. The stop marker deliberately
// stays set until the boot reaches READY, so a crash before the launch intent
// recovers the VM as STOPPED rather than deleting its disk. resources must
// already be validated (no shrink) by the caller; env replaces the previous
// value wholesale, hostname only when non-empty (empty means unchanged).
func (s *Store) PrepareStart(id string, res Resources, env map[string]string, hostname string) error {
	meta, err := s.ReadVM(id)
	if err != nil {
		return err
	}
	if !meta.Stopped() {
		return fmt.Errorf("storage: vm %s is not stopped (qemu=%v stoppedAt=%d)",
			id, meta.QEMU != nil, meta.StoppedAtUnixMs)
	}
	if res.DiskGiB < meta.Resources.DiskGiB {
		return fmt.Errorf("storage: vm %s: disk %d GiB is smaller than the kept disk %d GiB",
			id, res.DiskGiB, meta.Resources.DiskGiB)
	}
	meta.Resources = res
	meta.Env = maps.Clone(env)
	if hostname != "" {
		meta.Hostname = hostname
	}
	return s.writeManifest(meta)
}

// RevertStart restores the fields a failed start attempt wrote before its
// boot was claimed; the VM must still be durably stopped. It may record a
// disk smaller than the current file only because its single caller invokes
// it exclusively when nothing was persisted: once a start's new size is
// durably recorded (or its commit outcome is unknown), the caller keeps the
// grown logical size instead — reverting a manifest that already promises a
// bigger disk would undercount it and let a later request shrink it.
func (s *Store) RevertStart(id string, res Resources, env map[string]string, hostname string) error {
	meta, err := s.ReadVM(id)
	if err != nil {
		return err
	}
	if !meta.Stopped() {
		return fmt.Errorf("storage: vm %s is not stopped (qemu=%v stoppedAt=%d)",
			id, meta.QEMU != nil, meta.StoppedAtUnixMs)
	}
	meta.Resources = res
	meta.Env = maps.Clone(env)
	meta.Hostname = hostname
	return s.writeManifest(meta)
}

// GrowVMDisk grows the kept disk of a stopped VM to gib when it is smaller
// than that. Comparing the disk's actual virtual size keeps the operation
// idempotent across a crash between growth and the manifest update.
func (s *Store) GrowVMDisk(ctx context.Context, id string, gib uint64) error {
	if !ids.ValidSandbox(id) {
		return fmt.Errorf("storage: %q is not a sandbox id", id)
	}
	want := gib << 30
	meta, err := s.ReadVM(id)
	if err != nil {
		return err
	}
	rel := path.Join(vmsDir, id, diskName)
	read, closeRead, err := s.overlayRef(rel, meta.Image, false)
	if err != nil {
		return fmt.Errorf("storage: vm %s: %w", id, err)
	}
	info, err := s.imager.Info(ctx, read)
	closeRead()
	if err != nil {
		return fmt.Errorf("storage: vm %s: inspect kept disk: %w", id, err)
	}
	if info.VirtualSize >= want {
		return nil
	}
	grow, closeGrow, err := s.overlayRef(rel, meta.Image, true)
	if err != nil {
		return fmt.Errorf("storage: vm %s: %w", id, err)
	}
	defer closeGrow()
	if err := s.imager.Grow(ctx, grow, want); err != nil {
		return fmt.Errorf("storage: vm %s: grow kept disk: %w", id, err)
	}
	return nil
}

// SyncVMData flushes a stopped VM's disk file and its directory: after QEMU
// exited, the kept data must survive a host crash. It must never run while
// QEMU owns the disk.
func (s *Store) SyncVMData(id string) error {
	if !ids.ValidSandbox(id) {
		return fmt.Errorf("storage: %q is not a sandbox id", id)
	}
	vmRoot, err := s.root.OpenRoot(path.Join(vmsDir, id))
	if err != nil {
		return fmt.Errorf("storage: open vm root %s: %w", id, err)
	}
	defer vmRoot.Close()
	f, err := vmRoot.OpenFile(diskName, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("storage: vm %s: open disk: %w", id, err)
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("storage: vm %s: sync disk: %w", id, err)
	}
	if err := syncDir(vmRoot, "."); err != nil {
		return fmt.Errorf("storage: vm %s: sync dir: %w", id, err)
	}
	return nil
}

// SetVMError persists a VM's failure reason. It is best effort: a failing
// disk must not mask the original error.
func (s *Store) SetVMError(id, msg string) error {
	meta, err := s.ReadVM(id)
	if err != nil {
		return err
	}
	meta.LastError = msg
	return s.writeManifest(meta)
}

// ReadVM reads and validates one VM's manifest.
func (s *Store) ReadVM(id string) (VMMeta, error) {
	if !ids.ValidSandbox(id) {
		return VMMeta{}, fmt.Errorf("storage: %q is not a sandbox id", id)
	}
	raw, err := s.root.ReadFile(path.Join(vmsDir, id, manifestName))
	if err != nil {
		return VMMeta{}, err
	}
	var meta VMMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return VMMeta{}, fmt.Errorf("storage: vm %s: manifest: %w", id, err)
	}
	if err := validateVMMeta(meta, id); err != nil {
		return VMMeta{}, err
	}
	return meta, nil
}

// validateVMMeta checks every typed field recovery trusts. A syntactically
// valid manifest with nonsense values gets the same conservative handling as
// corrupt JSON: it is never used to launch, kill or free capacity.
func validateVMMeta(meta VMMeta, id string) error {
	if meta.SchemaVersion != manifestSchema {
		return fmt.Errorf("storage: vm %s: schemaVersion %d, want %d", id, meta.SchemaVersion, manifestSchema)
	}
	if meta.SandboxID != id {
		return fmt.Errorf("storage: vm dir %s holds manifest for %q", id, meta.SandboxID)
	}
	if !ValidImageID(meta.Image) {
		return fmt.Errorf("storage: vm %s: invalid image id %q", id, meta.Image)
	}
	res := meta.Resources
	switch {
	case res.VCPUs == 0 || res.VCPUs > 1024:
		return fmt.Errorf("storage: vm %s: vcpus %d out of range", id, res.VCPUs)
	case res.MemoryMiB < 64 || res.MemoryMiB > 1<<20:
		return fmt.Errorf("storage: vm %s: memory %d MiB out of range", id, res.MemoryMiB)
	}
	if _, ok := checkedDiskBytes(res.DiskGiB); !ok || res.DiskGiB == 0 {
		return fmt.Errorf("storage: vm %s: disk %d GiB out of range", id, res.DiskGiB)
	}
	if meta.VsockCID < 3 || meta.VsockCID == math.MaxUint32 {
		return fmt.Errorf("storage: vm %s: vsock cid %d out of range", id, meta.VsockCID)
	}
	if (meta.Control == nil) != (meta.Console == nil) ||
		meta.Control != nil && (meta.Control.Ino == 0 || meta.Console.Ino == 0) {
		return fmt.Errorf("storage: vm %s: incomplete jailed layout identity", id)
	}
	if meta.StartedAtUnixMs < 0 || meta.StoppedAtUnixMs < 0 {
		return fmt.Errorf("storage: vm %s: negative ready/stop timestamp", id)
	}
	if meta.QEMU != nil {
		switch meta.QEMU.State {
		case QEMULaunching:
		case QEMURunning:
			if meta.QEMU.PID <= 0 {
				return fmt.Errorf("storage: vm %s: running record has pid %d", id, meta.QEMU.PID)
			}
			if meta.QEMU.StartTicks == 0 {
				// A zero start time would make the identity check a
				// wildcard for pid reuse.
				return fmt.Errorf("storage: vm %s: running record has no start ticks", id)
			}
		default:
			return fmt.Errorf("storage: vm %s: unknown QEMU state %q", id, meta.QEMU.State)
		}
		if meta.QEMU.BootID == "" {
			return fmt.Errorf("storage: vm %s: QEMU record has no boot id", id)
		}
		if meta.QEMU.JailAttempt != "" || meta.QEMU.ArgvSHA256 != "" {
			if !validJailAttempt(meta.QEMU.JailAttempt) {
				return fmt.Errorf("storage: vm %s: invalid jail attempt %q", id, meta.QEMU.JailAttempt)
			}
			if !validArgvDigest(meta.QEMU.ArgvSHA256) {
				return fmt.Errorf("storage: vm %s: jail attempt without a valid argv digest", id)
			}
		}
	}
	return nil
}

// RemoveVM deletes the VM's directory including its disk. It is called only
// by DeleteVM and by creation cleanup before any launch intent existed.
func (s *Store) RemoveVM(id string) error {
	if !ids.ValidSandbox(id) {
		return fmt.Errorf("storage: %q is not a sandbox id", id)
	}
	if s.treeHeld(path.Join(vmsDir, id)) {
		return fmt.Errorf("storage: vm %s: a jailed tool still holds its disk: %w", id, ErrToolRetained)
	}
	if err := s.root.RemoveAll(path.Join(vmsDir, id)); err != nil {
		return fmt.Errorf("storage: remove vm %s: %w", id, err)
	}
	if err := syncDir(s.root, vmsDir); err != nil {
		return fmt.Errorf("storage: remove vm %s: %w", id, err)
	}
	return nil
}

// writeManifest replaces vm.json atomically: the replacement is fully
// written and synced, renamed over the old name, and the directory entry is
// synced too, so a crash leaves either the old or the new manifest, never a
// truncated one. The rename and the directory sync run through the
// configured fault seams, and a failure of the post-rename directory sync
// reports [ErrManifestDurabilityUnknown]: the new manifest is already the
// durable name, so callers cannot assume the old content still stands.
func (s *Store) writeManifest(meta VMMeta) error {
	dir := path.Join(vmsDir, meta.SandboxID)
	vmRoot, err := s.root.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("storage: open vm root %s: %w", meta.SandboxID, err)
	}
	defer vmRoot.Close()

	raw, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("storage: vm %s: encode manifest: %w", meta.SandboxID, err)
	}
	tmp := manifestName + manifestTmpExt
	// Remove a leftover temporary name first: O_NOFOLLOW then guarantees a
	// symlink planted at the temporary path is never followed.
	if err := vmRoot.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("storage: vm %s: clear manifest temp: %w", meta.SandboxID, err)
	}
	f, err := vmRoot.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("storage: vm %s: write manifest: %w", meta.SandboxID, err)
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		vmRoot.Remove(tmp)
		return fmt.Errorf("storage: vm %s: write manifest: %w", meta.SandboxID, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		vmRoot.Remove(tmp)
		return fmt.Errorf("storage: vm %s: sync manifest: %w", meta.SandboxID, err)
	}
	if err := f.Close(); err != nil {
		vmRoot.Remove(tmp)
		return fmt.Errorf("storage: vm %s: close manifest: %w", meta.SandboxID, err)
	}
	if err := s.cfg.Snapshots.Rename(vmRoot, tmp, manifestName); err != nil {
		vmRoot.Remove(tmp)
		return fmt.Errorf("storage: vm %s: publish manifest: %w", meta.SandboxID, err)
	}
	if err := s.cfg.Snapshots.SyncDir(vmRoot, "."); err != nil {
		return fmt.Errorf("storage: vm %s: sync dir: %w:%w", meta.SandboxID, ErrManifestDurabilityUnknown, err)
	}
	return nil
}

// DiskFreeBytes reports the data directory's current free space.
func (s *Store) DiskFreeBytes() (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(s.dataDir, &st); err != nil {
		return 0, fmt.Errorf("storage: statfs %s: %w", s.dataDir, err)
	}
	if st.Bsize < 0 {
		return 0, fmt.Errorf("storage: statfs %s: negative block size", s.dataDir)
	}
	return uint64(st.Bsize) * st.Bavail, nil
}
