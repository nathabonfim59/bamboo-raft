package jailer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"golang.org/x/sys/unix"
)

const (
	// TAPName and VhostNetName are the A8 network descriptor names. They must
	// be the first two entries in LaunchSpec.Files, hence child fds 3 and 4.
	TAPName        = "tap"
	VhostNetName   = "vhost-net"
	KernelName     = "kernel"
	InitrdName     = "initrd"
	VhostVsockName = "vhost-vsock"

	// GuestQMPPath and GuestSerialPath are the only control paths QEMU needs
	// inside the chroot. The host QMP socket is qmp.sock in the pinned control
	// directory supplied in LaunchSpec.ControlDir.
	GuestQMPPath    = "/control/qmp.sock"
	GuestSerialPath = "/serial.log"

	maxDescriptorName = 32
	maxDescriptors    = 64
	maxArgs           = 4096
	maxLaunchArgs     = 1 << 20
	maxFirmwareFiles  = 64
)

var (
	// ErrAlive reports that a jail's recorded QEMU process is still running,
	// or that its cgroup is not empty. Its resources must stay reserved.
	ErrAlive = errors.New("jailer: QEMU is still alive")
	// ErrClosed reports use of a closed Manager.
	ErrClosed = errors.New("jailer: manager is closed")
	// ErrPoolExhausted reports that no per-VM uid is available.
	ErrPoolExhausted = errors.New("jailer: uid pool exhausted")
	// ErrRetained reports a Start for an id that still has a durable record.
	// Nothing was reserved or changed: the caller must settle that record,
	// through its own attempt and the reclaim policy, before launching again.
	ErrRetained = errors.New("jailer: a retained reservation exists for this id")
	// ErrSuperseded reports that the id's current record belongs to another
	// attempt. It is a conflict, never proof that the named attempt is gone:
	// nothing was touched.
	ErrSuperseded = errors.New("jailer: the current record belongs to another attempt")
	// ErrTargetBusy reports that another record already holds the target
	// inode (a VM overlay): one inode has at most one owner at a time.
	ErrTargetBusy = errors.New("jailer: the target inode is held by another record")
)

// ReclaimFunc returns the caller resources a record names (the control
// directory, serial log and overlay of a VM) to their stopped ownership. The
// Manager runs it under its lock, only after the attempt's death and empty
// owned cgroup are proven and before any jail, cgroup, record or uid is
// removed; an error retains the complete reservation. It must be idempotent,
// must touch only resources whose current inode equals the record's, and
// must not call back into the Manager.
type ReclaimFunc func(ctx context.Context, record Record) error

// UIDRange is an inclusive range of host uids used as per-VM uid/gid pairs.
// No VM receives supplementary groups.
type UIDRange struct {
	Min uint32
	Max uint32
}

// Config configures one node's jailer. StateDir and CgroupParent must be
// private to this node. CgroupParent is the delegated cgroup v2 directory;
// the jailer enables cpu, memory, io and pids in its subtree_control.
type Config struct {
	StateDir     string
	CgroupParent string
	UIDs         UIDRange
	// LibraryDirs are trusted, root-owned directories searched for ELF
	// DT_NEEDED entries. Empty uses the reference Debian multiarch paths.
	// No ldd, loader subprocess or environment-controlled search is used.
	LibraryDirs []string
	// FirmwareFiles are trusted, root-owned, not group/other writable regular
	// data files QEMU loads by name, such as the q35 BIOS and option ROMs.
	// Each is copied read-only to the same absolute path inside the chroot.
	// They count toward the runtime file and byte budget. No directory, glob
	// or implicit data-tree search is supported.
	FirmwareFiles []string
	// Reclaim is the mandatory policy every uid-freeing path runs; see
	// ReclaimFunc. New refuses a nil policy.
	Reclaim ReclaimFunc
	Log     *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.Log == nil {
		c.Log = slog.New(slog.DiscardHandler)
	}
	if len(c.LibraryDirs) == 0 {
		c.LibraryDirs = []string{
			"/lib64",
			"/lib/x86_64-linux-gnu",
			"/usr/lib/x86_64-linux-gnu",
			"/usr/lib64",
			"/lib",
			"/usr/lib",
		}
	}
	return c
}

// Validate checks configuration without changing the host.
func (c Config) Validate() error {
	if c.Reclaim == nil {
		return errors.New("jailer: a reclaim policy is required")
	}
	if err := cleanAbsolute("state dir", c.StateDir); err != nil {
		return err
	}
	if c.StateDir == "/" {
		return errors.New("jailer: refusing to use / as state dir")
	}
	if err := cleanAbsolute("cgroup parent", c.CgroupParent); err != nil {
		return err
	}
	if c.UIDs.Min == 0 || c.UIDs.Min > c.UIDs.Max || c.UIDs.Max >= 1<<31 {
		return fmt.Errorf("jailer: invalid uid range %d-%d", c.UIDs.Min, c.UIDs.Max)
	}
	for _, dir := range c.LibraryDirs {
		if err := cleanAbsolute("library dir", dir); err != nil {
			return err
		}
	}
	if len(c.FirmwareFiles) > maxFirmwareFiles {
		return fmt.Errorf("jailer: %d firmware files exceed %d", len(c.FirmwareFiles), maxFirmwareFiles)
	}
	seen := make(map[string]struct{}, len(c.FirmwareFiles))
	for _, name := range c.FirmwareFiles {
		if err := cleanAbsolute("firmware file", name); err != nil {
			return err
		}
		if _, err := runtimeTarget(name); err != nil {
			return err
		}
		if _, dup := seen[name]; dup {
			return fmt.Errorf("jailer: duplicate firmware file %q", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func cleanAbsolute(what, p string) error {
	if p == "" || !filepath.IsAbs(p) || filepath.Clean(p) != p {
		return fmt.Errorf("jailer: %s %q is not an absolute clean path", what, p)
	}
	return nil
}

// Descriptor is one allowlisted file inherited by QEMU. Its position in
// LaunchSpec.Files determines its child descriptor: index zero is fd 3.
type Descriptor struct {
	Name string
	File *os.File
}

// BlockDevice identifies the explicit host block device constrained by IO.
// It is configuration, not inferred from a guest-controlled image header.
type BlockDevice struct {
	Major uint32
	Minor uint32
}

// IOLimits bounds one block device through cgroup v2 io.max. Zero rate fields
// are omitted from the rule; at least one field must be nonzero.
type IOLimits struct {
	Device    BlockDevice
	ReadBPS   uint64
	WriteBPS  uint64
	ReadIOPS  uint64
	WriteIOPS uint64
}

// Limits are the required cgroup v2 bounds for one QEMU process.
type Limits struct {
	// CPUQuotaPercent is the quota per 100 ms period: 100 is one CPU, 400 is
	// four. It must be nonzero and at most 1,000,000 percent.
	CPUQuotaPercent uint32
	MemoryBytes     uint64
	Pids            uint32
	IO              IOLimits
}

// Validate rejects an unlimited or internally inconsistent limit set.
func (l Limits) Validate() error {
	if l.CPUQuotaPercent == 0 || l.CPUQuotaPercent > 1_000_000 {
		return fmt.Errorf("jailer: CPU quota percent %d out of range", l.CPUQuotaPercent)
	}
	if l.MemoryBytes < 64<<20 {
		return fmt.Errorf("jailer: memory limit %d is below 64 MiB", l.MemoryBytes)
	}
	if l.Pids < 16 || l.Pids > 1<<20 {
		return fmt.Errorf("jailer: pids limit %d out of range 16..%d", l.Pids, 1<<20)
	}
	if l.IO.Device.Major == 0 {
		return errors.New("jailer: IO device major is zero; configure the explicit backing block device")
	}
	if l.IO.ReadBPS|l.IO.WriteBPS|l.IO.ReadIOPS|l.IO.WriteIOPS == 0 {
		return errors.New("jailer: at least one IO rate limit is required")
	}
	return nil
}

// LaunchSpec describes a jailed QEMU exec. Args excludes argv0 and must use
// only the supported option forms documented on validateArgs: chroot paths
// GuestQMPPath and GuestSerialPath, and /proc/self/fd/N for inherited files.
// The caller closes Files, ControlDir and SerialLog after Start returns; the
// jailer never takes ownership of those descriptors.
type LaunchSpec struct {
	SandboxID string
	// Attempt is the caller-chosen immutable generation identity, the
	// canonical text of a uuid (uuid.NewV7). It is persisted before any host
	// mutation and names this attempt in ReleaseAttempt.
	Attempt string
	// Binary is the trusted host QEMU executable. It and every parent must be
	// root-owned and not group/other writable. It is staged read-only at the
	// same clean path inside the chroot and executed from there, so QEMU's
	// relocated data and firmware paths match the host layout. Its ELF
	// dependencies are checked and staged the same way.
	Binary string
	Args   []string
	// Files starts with TAPName and VhostNetName at fds 3 and 4. It must
	// contain exactly those, KernelName, InitrdName, VhostVsockName and the
	// Disks entries: an unreferenced descriptor would hand QEMU an
	// undeclared host resource. A Maintenance spec has KernelName and
	// InitrdName at fds 3 and 4, then the Disks entries, and nothing else.
	Files []Descriptor
	// Disks names entries in Files from the oldest backing file to the
	// writable overlay. Args builds one explicit -blockdev per disk; stored
	// backing filenames are never opened. All but the overlay must be opened
	// read-only.
	Disks []string
	// ControlDir is a pinned per-VM directory. It may contain only qmp.sock
	// and serial.log, so VM manifests, disks and host directories cannot be
	// exposed accidentally. Start clones its mount, non-recursively and
	// nosuid/nodev/noexec, and the helper attaches that clone at /control.
	ControlDir *os.File
	// SerialLog is a pinned per-VM regular file, attached at /serial.log the
	// same way as ControlDir.
	SerialLog *os.File
	Limits    Limits
	Stdout    io.Writer
	Stderr    io.Writer
	// Maintenance selects the maintenance form (see validateMaintenance in
	// maintenance.go): a paused QEMU that only serves the VM's disk graph
	// over QMP and its NBD server. Files are then KernelName, InitrdName
	// and the Disks entries, with no TAP, vhost-net or vhost-vsock, and
	// the argv must carry -S and no -netdev, -device or -object. It is an
	// explicit mode: a VM spec never gets these looser descriptor rules.
	Maintenance bool
}

// Validate checks the spec exactly as Start does before any reservation:
// descriptors, disk chain, control resources, limits and the argv forms. It
// changes nothing on the host. The device role and TAP flag checks need the
// Manager and run only in Start.
func (s LaunchSpec) Validate() error {
	_, err := s.validate()
	return err
}

// validate checks the spec without changing the host and returns whether
// each disk in Disks is qcow2 (true) or a standalone raw image (false).
func (s LaunchSpec) validate() ([]bool, error) {
	if !ids.ValidSandbox(s.SandboxID) {
		return nil, fmt.Errorf("jailer: invalid sandbox id %q", s.SandboxID)
	}
	if !validAttempt(s.Attempt) {
		return nil, fmt.Errorf("jailer: invalid attempt %q", s.Attempt)
	}
	if err := cleanAbsolute("QEMU binary", s.Binary); err != nil {
		return nil, err
	}
	if _, err := runtimeTarget(s.Binary); err != nil {
		return nil, err
	}
	if len(s.Args) > maxArgs {
		return nil, fmt.Errorf("jailer: too many QEMU arguments: %d", len(s.Args))
	}
	total := 0
	for _, arg := range s.Args {
		if strings.ContainsRune(arg, 0) {
			return nil, errors.New("jailer: QEMU argument contains NUL")
		}
		total += len(arg)
	}
	if total > maxLaunchArgs {
		return nil, fmt.Errorf("jailer: QEMU arguments exceed %d bytes", maxLaunchArgs)
	}
	if err := s.validateFiles(); err != nil {
		return nil, err
	}
	if s.ControlDir == nil || s.SerialLog == nil {
		return nil, errors.New("jailer: pinned control directory and serial log are required")
	}
	info, err := s.ControlDir.Stat()
	if err != nil {
		return nil, fmt.Errorf("jailer: control directory: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("jailer: control descriptor is not a directory")
	}
	info, err = s.SerialLog.Stat()
	if err != nil {
		return nil, fmt.Errorf("jailer: serial log: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("jailer: serial log is not a regular file")
	}
	if err := s.checkAliases(); err != nil {
		return nil, err
	}
	if err := s.Limits.Validate(); err != nil {
		return nil, err
	}
	if err := validateControlEntries(s.ControlDir); err != nil {
		return nil, err
	}
	formats, err := validateDiskChain(s)
	if err != nil {
		return nil, err
	}
	if err := validateArgs(s, formats); err != nil {
		return nil, err
	}
	return formats, nil
}

// descriptorKind is the file type and access a named descriptor must have.
type descriptorKind struct {
	charDevice bool
	readOnly   bool
}

func (s LaunchSpec) validateFiles() error {
	if s.Maintenance {
		return s.validateMaintenanceFiles()
	}
	if len(s.Files) < 6 {
		return errors.New("jailer: TAP, vhost-net, kernel, initrd, vhost-vsock and disk descriptors are required")
	}
	if len(s.Files) > maxDescriptors {
		return fmt.Errorf("jailer: %d descriptors exceed %d", len(s.Files), maxDescriptors)
	}
	if s.Files[0].Name != TAPName || s.Files[1].Name != VhostNetName {
		return fmt.Errorf("jailer: first descriptors must be %s and %s at fds 3 and 4", TAPName, VhostNetName)
	}
	kinds := map[string]descriptorKind{
		TAPName:        {charDevice: true},
		VhostNetName:   {charDevice: true},
		VhostVsockName: {charDevice: true},
		KernelName:     {readOnly: true},
		InitrdName:     {readOnly: true},
	}
	return s.checkDescriptors(kinds)
}

// checkDescriptors adds the Disks entries to the fixed kinds and requires
// Files to hold exactly those names, each of its kind and access.
func (s LaunchSpec) checkDescriptors(kinds map[string]descriptorKind) error {
	if len(s.Disks) == 0 {
		return errors.New("jailer: disk chain is empty")
	}
	for i, name := range s.Disks {
		if _, dup := kinds[name]; dup {
			if slices.Index(s.Disks, name) != i {
				return fmt.Errorf("jailer: disk descriptor %q occurs twice in the chain", name)
			}
			return fmt.Errorf("jailer: disk %q reuses a fixed descriptor name", name)
		}
		kinds[name] = descriptorKind{readOnly: i < len(s.Disks)-1}
	}
	seen := make(map[string]struct{}, len(s.Files))
	for _, d := range s.Files {
		if d.Name == "" || len(d.Name) > maxDescriptorName || strings.ContainsAny(d.Name, "/\\\x00\n\r\t ,=") {
			return fmt.Errorf("jailer: invalid descriptor name %q", d.Name)
		}
		if _, ok := seen[d.Name]; ok {
			return fmt.Errorf("jailer: duplicate descriptor %q", d.Name)
		}
		seen[d.Name] = struct{}{}
		kind, declared := kinds[d.Name]
		if !declared {
			return fmt.Errorf("jailer: descriptor %q is neither a fixed resource nor a declared disk", d.Name)
		}
		if d.File == nil {
			return fmt.Errorf("jailer: descriptor %q is nil", d.Name)
		}
		info, err := d.File.Stat()
		if err != nil {
			return fmt.Errorf("jailer: descriptor %q: %w", d.Name, err)
		}
		if kind.charDevice {
			if info.Mode()&os.ModeCharDevice == 0 {
				return fmt.Errorf("jailer: descriptor %q is not a character device", d.Name)
			}
		} else if !info.Mode().IsRegular() {
			return fmt.Errorf("jailer: descriptor %q is not a regular file", d.Name)
		}
		if kind.readOnly {
			flags, err := unix.FcntlInt(d.File.Fd(), unix.F_GETFL, 0)
			if err != nil {
				return fmt.Errorf("jailer: descriptor %q flags: %w", d.Name, err)
			}
			if flags&unix.O_ACCMODE != unix.O_RDONLY {
				return fmt.Errorf("jailer: descriptor %q must be opened read-only", d.Name)
			}
		}
	}
	if len(seen) != len(kinds) {
		for name := range kinds {
			if _, ok := seen[name]; !ok {
				return fmt.Errorf("jailer: required descriptor %q is missing", name)
			}
		}
	}
	return nil
}

// checkAliases requires every descriptor, the control directory and the
// serial log to be distinct inodes. Start chowns the control directory, the
// serial log and the overlay to the VM uid, so none of them may also be the
// kernel, initrd, a backing layer or another role.
func (s LaunchSpec) checkAliases() error {
	seen := make(map[inodeIdentity]string, len(s.Files)+2)
	add := func(role string, f *os.File) error {
		identity, err := fileInode(f)
		if err != nil {
			return fmt.Errorf("jailer: %s: %w", role, err)
		}
		if other, dup := seen[identity]; dup {
			return fmt.Errorf("jailer: %s aliases %s", role, other)
		}
		seen[identity] = role
		return nil
	}
	for _, d := range s.Files {
		if err := add("descriptor "+strconv.Quote(d.Name), d.File); err != nil {
			return err
		}
	}
	if err := add("control directory", s.ControlDir); err != nil {
		return err
	}
	return add("serial log", s.SerialLog)
}

// fdOf returns the child descriptor number of a named file, or -1.
func (s LaunchSpec) fdOf(name string) int {
	i := slices.IndexFunc(s.Files, func(d Descriptor) bool { return d.Name == name })
	if i < 0 {
		return -1
	}
	return 3 + i
}

func (s LaunchSpec) descriptor(name string) *os.File {
	i := slices.IndexFunc(s.Files, func(d Descriptor) bool { return d.Name == name })
	if i < 0 {
		return nil
	}
	return s.Files[i].File
}

func validateControlEntries(dir *os.File) error {
	if _, err := dir.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("jailer: rewind control directory: %w", err)
	}
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return fmt.Errorf("jailer: inspect control directory: %w", err)
	}
	for _, entry := range entries {
		switch entry.Name() {
		case "serial.log":
		case "qmp.sock":
			return errors.New("jailer: refusing a pre-existing QMP socket")
		default:
			return fmt.Errorf("jailer: control directory contains non-control entry %q", entry.Name())
		}
	}
	return nil
}

func fileIdentity(f *os.File) (uint64, uint64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, errors.New("jailer: file identity is not a Linux stat")
	}
	return uint64(st.Dev), st.Ino, nil
}
