package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"uuid"

	"golang.org/x/sys/unix"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

// ProbeDirName is the fixed, never-removed parent of CPU probe scratch under
// the node data directory.
const ProbeDirName = "probe"

const (
	probeControlSuffix = ".control"
	probeSerialSuffix  = ".serial"
	// reclaimVMsDir is storage's vms/ directory, the parent of every box.
	reclaimVMsDir = "vms"
	// reclaimDiskName is storage's overlay name inside a box.
	reclaimDiskName = "disk.qcow2"
)

// JailReclaimer is the jailer's mandatory reclaim policy, independent of any
// node.Manager: it owns its own no-follow roots on the data directory and its
// probe parent, takes no lock (storage.Store keeps the only node.lock) and
// never calls the jailer or takes node locks. It is built before the jailer
// and closed after it.
type JailReclaimer struct {
	dataDir string
	data    *os.Root
	probe   *os.Root
	// imageDir and images are the image root tool records may name.
	imageDir string
	images   *os.Root
	// sync makes one pinned entry durable (test seam; nil: File.Sync).
	sync func(*os.File) error
}

// NewJailReclaimer opens the data directory and its probe parent. The data
// directory's parent must already exist; each root is created 0700 when
// missing. Its parent's directory barrier runs on the creating and on the
// existing-entry path alike, so a construction that failed after a create
// is completed durably by the next one. The opened root itself, not an
// earlier path lookup, must be a directory owned by the node's euid with a
// safe mode, and must be the inode its name holds without following a final
// symlink; a foreign or replaced root is refused, never chmodded, chowned,
// adopted or removed. The data directory, which storage may have created
// 0755, is accepted as long as neither group nor others can write it; the
// probe parent must be 0700.
//
// imageDir is the image root tool records may name (TargetRootImages). It
// must already exist: it is never created, and its opened inode is bound
// and validated like the data root's (owner, type, no group/other write, no
// final symlink).
func NewJailReclaimer(ctx context.Context, dataDir, imageDir string) (*JailReclaimer, error) {
	return newJailReclaimer(ctx, dataDir, imageDir, reclaimRootOps{})
}

// reclaimRootOps are the per-instance seams of root construction.
type reclaimRootOps struct {
	// syncDir is a root parent's directory barrier (nil: File.Sync).
	syncDir func(*os.File) error
	// beforeBind runs after a root's create-or-discover and its parent
	// barrier, before the root is opened and bound.
	beforeBind func(path string)
}

func newJailReclaimer(ctx context.Context, dataDir, imageDir string, ops reclaimRootOps) (_ *JailReclaimer, err error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("node: jail reclaimer: %w", context.Cause(ctx))
	}
	if dataDir == "" {
		return nil, errors.New("node: jail reclaimer: empty data directory")
	}
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("node: jail reclaimer: %w", err)
	}
	if abs == "/" {
		return nil, errors.New("node: jail reclaimer: refusing / as the data directory")
	}
	parent, err := os.OpenRoot(filepath.Dir(abs))
	if err != nil {
		return nil, fmt.Errorf("node: jail reclaimer: the data directory's parent must exist: %w", err)
	}
	defer parent.Close()
	r := &JailReclaimer{dataDir: abs, imageDir: imageDir}
	defer func() {
		if err != nil {
			r.Close()
		}
	}()
	if r.data, err = bindReclaimRoot(ctx, parent, filepath.Base(abs), abs, 0o022, ops); err != nil {
		return nil, err
	}
	if r.probe, err = bindReclaimRoot(ctx, r.data, ProbeDirName, filepath.Join(abs, ProbeDirName), 0o077, ops); err != nil {
		return nil, err
	}
	if r.images, err = bindImagesRoot(ctx, imageDir); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("node: jail reclaimer: %w", context.Cause(ctx))
	}
	return r, nil
}

// bindImagesRoot binds the existing image root without creating anything.
func bindImagesRoot(ctx context.Context, imageDir string) (*os.Root, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("node: jail reclaimer: %w", context.Cause(ctx))
	}
	if imageDir == "" {
		return nil, errors.New("node: jail reclaimer: empty image directory")
	}
	abs, err := filepath.Abs(imageDir)
	if err != nil {
		return nil, fmt.Errorf("node: jail reclaimer: %w", err)
	}
	parent, err := os.OpenRoot(filepath.Dir(abs))
	if err != nil {
		return nil, fmt.Errorf("node: jail reclaimer: the image directory's parent: %w", err)
	}
	defer parent.Close()
	root, err := parent.OpenRoot(filepath.Base(abs))
	if err != nil {
		return nil, fmt.Errorf("node: jail reclaimer: the image directory must exist: %w", err)
	}
	if err := checkBoundRoot(parent, filepath.Base(abs), abs, root, 0o022); err != nil {
		root.Close()
		return nil, err
	}
	return root, nil
}

// checkBoundRoot validates an opened root's own inode (owner, type, mode
// sharing no bit with forbidden) and that name, not followed, is it.
func checkBoundRoot(parent *os.Root, name, full string, root *os.Root, forbidden os.FileMode) error {
	opened, err := root.Stat(".")
	if err != nil {
		return fmt.Errorf("node: jail reclaimer: %s: %w", full, err)
	}
	if err := checkReclaimDir(full, opened, forbidden); err != nil {
		return err
	}
	named, err := parent.Lstat(name)
	if err != nil || named.Mode()&os.ModeSymlink != 0 || !os.SameFile(named, opened) {
		return fmt.Errorf("node: jail reclaimer: %s changed while it was bound", full)
	}
	return nil
}

// bindReclaimRoot creates name (0700) in parent when missing, runs parent's
// directory barrier on both paths, then opens name and validates the opened
// inode itself: owner, type and a mode sharing no bit with forbidden, and
// that name, without following it, is that same directory.
func bindReclaimRoot(ctx context.Context, parent *os.Root, name, full string, forbidden os.FileMode, ops reclaimRootOps) (*os.Root, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("node: jail reclaimer: %w", context.Cause(ctx))
	}
	if err := parent.Mkdir(name, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("node: jail reclaimer: create %s: %w", full, err)
	}
	dir, err := parent.OpenFile(".", os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, fmt.Errorf("node: jail reclaimer: open the parent of %s: %w", full, err)
	}
	sync := ops.syncDir
	if sync == nil {
		sync = (*os.File).Sync
	}
	err = errors.Join(sync(dir), dir.Close())
	if err != nil {
		return nil, fmt.Errorf("node: jail reclaimer: sync the parent of %s: %w", full, err)
	}
	if ops.beforeBind != nil {
		ops.beforeBind(full)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("node: jail reclaimer: %w", context.Cause(ctx))
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		return nil, fmt.Errorf("node: jail reclaimer: open %s: %w", full, err)
	}
	opened, err := root.Stat(".")
	if err == nil {
		err = checkReclaimDir(full, opened, forbidden)
	}
	if err == nil {
		named, lerr := parent.Lstat(name)
		if lerr != nil || named.Mode()&os.ModeSymlink != 0 || !os.SameFile(named, opened) {
			err = fmt.Errorf("node: jail reclaimer: %s changed while it was bound", full)
		}
	}
	if err != nil {
		root.Close()
		return nil, err
	}
	return root, nil
}

func checkReclaimDir(dir string, info os.FileInfo, forbidden os.FileMode) error {
	st, ok := info.Sys().(*syscall.Stat_t)
	switch {
	case info.Mode()&os.ModeSymlink != 0 || !info.IsDir():
		return fmt.Errorf("node: jail reclaimer: %s is not a directory", dir)
	case !ok || int(st.Uid) != os.Geteuid():
		return fmt.Errorf("node: jail reclaimer: %s is not owned by the node", dir)
	case info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || info.Mode().Perm()&forbidden != 0:
		return fmt.Errorf("node: jail reclaimer: %s has unsafe mode %v", dir, info.Mode())
	}
	return nil
}

// syncEntry makes one entry durable through the sync seam.
func (r *JailReclaimer) syncEntry(ctx context.Context, f *os.File) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sync %s: %w", f.Name(), context.Cause(ctx))
	}
	sync := r.sync
	if sync == nil {
		sync = (*os.File).Sync
	}
	if err := sync(f); err != nil {
		return fmt.Errorf("sync %s: %w", f.Name(), err)
	}
	return nil
}

// Reclaim is jailer.Config.Reclaim: it dispatches on the record kind (vm,
// probe); any other kind is an error that retains the reservation.
func (r *JailReclaimer) Reclaim(ctx context.Context, rec jailer.Record) error {
	switch rec.Kind {
	case jailer.KindVM:
		return r.reclaimVM(ctx, rec)
	case jailer.KindProbe:
		return r.reclaimProbe(ctx, rec)
	case jailer.KindTool:
		return r.reclaimTool(ctx, rec)
	default:
		return fmt.Errorf("node: reclaim %s: unsupported record kind %q", rec.SandboxID, rec.Kind)
	}
}

// reclaimPin is one opened entry a reclaim restores.
type reclaimPin struct {
	name string
	file *os.File
	mode os.FileMode
}

// checkPinIdentity requires f to be the inode dev/ino.
func checkPinIdentity(f *os.File, dev, ino uint64) error {
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return err
	}
	if st.Dev != dev || st.Ino != ino {
		return errors.New("is not the attempt's inode; preserving it")
	}
	return nil
}

// controlSockets returns the control directory's only permitted entry, the
// attempt's QMP socket owned by uid. The directory is VM-writable: at most
// one entry beyond the supported one is read, so an attacker-sized
// directory is refused, not walked.
func controlSockets(control *os.File, uid uint32) ([]string, error) {
	entries, err := control.ReadDir(2)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read control: %w", err)
	}
	if len(entries) > 1 {
		return nil, errors.New("control directory holds more than its QMP socket; preserving it")
	}
	var sockets []string
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("control entry %s: %w", entry.Name(), err)
		}
		st, _ := info.Sys().(*syscall.Stat_t)
		if entry.Name() != qemu.QMPFilename || info.Mode()&os.ModeSocket == 0 || st == nil || st.Uid != uid {
			return nil, fmt.Errorf("foreign control entry %q; preserving it", entry.Name())
		}
		sockets = append(sockets, entry.Name())
	}
	return sockets, nil
}

func removeSockets(control *os.File, sockets []string) error {
	for _, name := range sockets {
		if err := unix.Unlinkat(int(control.Fd()), name, 0); err != nil && !errors.Is(err, unix.ENOENT) {
			return fmt.Errorf("remove %s: %w", name, err)
		}
	}
	return nil
}

// reclaimVM is the VM kind's reclaim policy: it returns the control
// directory, console log and overlay a dead attempt held to the node's own
// ownership, verified by the record's inodes. Every identity is verified
// before anything changes; a foreign or replaced entry fails the reclaim and
// is left untouched. Each entry's ownership, mode (and, for the control
// directory, the socket removal) is durable before success, because the
// jailer then removes the record and may hand the uid to another VM. A
// failed step leaves the reservation retained; a retry by the same record
// redoes every idempotent step and barrier.
func (r *JailReclaimer) reclaimVM(ctx context.Context, rec jailer.Record) (returnErr error) {
	defer func() {
		if returnErr != nil {
			returnErr = fmt.Errorf("node: reclaim %s: %w", rec.SandboxID, returnErr)
		}
	}()
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if !ids.ValidSandbox(rec.SandboxID) {
		return fmt.Errorf("%q is not a sandbox id", rec.SandboxID)
	}
	box, err := r.data.OpenRoot(path.Join(reclaimVMsDir, rec.SandboxID))
	if err != nil {
		return fmt.Errorf("open vm root: %w", err)
	}
	defer box.Close()
	var pins []reclaimPin
	defer func() {
		for _, p := range pins {
			p.file.Close()
		}
	}()
	for _, want := range []struct {
		name     string
		flag     int
		dev, ino uint64
		mode     os.FileMode
	}{
		{storage.ControlDirName, os.O_RDONLY | syscall.O_DIRECTORY, rec.ControlDev, rec.ControlIno, 0o700},
		{storage.ConsoleLogName, os.O_RDONLY, rec.LogDev, rec.LogIno, 0o600},
		{reclaimDiskName, os.O_RDONLY, rec.TargetDev, rec.TargetIno, 0o600},
	} {
		f, err := openNoFollowChecked(box, want.name, want.flag&^syscall.O_DIRECTORY, want.flag&syscall.O_DIRECTORY != 0)
		if err != nil {
			return err
		}
		pins = append(pins, reclaimPin{want.name, f, want.mode})
		if err := checkPinIdentity(f, want.dev, want.ino); err != nil {
			return fmt.Errorf("%s %w", want.name, err)
		}
	}
	sockets, err := controlSockets(pins[0].file, rec.UID)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if err := removeSockets(pins[0].file, sockets); err != nil {
		return err
	}
	uid, gid := os.Geteuid(), os.Getegid()
	for _, p := range pins {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		if err := p.file.Chown(uid, gid); err != nil {
			return fmt.Errorf("own %s: %w", p.name, err)
		}
		if err := p.file.Chmod(p.mode); err != nil {
			return fmt.Errorf("protect %s: %w", p.name, err)
		}
		if err := r.syncEntry(ctx, p.file); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	return nil
}

// reclaimTool is the tool kind's reclaim policy: a shared-ro target (an
// immutable image base) is never touched; a group-ro or owner-rw target is
// pinned at TargetPath under its TargetRoot without following symlinks,
// required to be the record's inode, returned to the node's owner with mode
// 0600 and made durable. Only data-root targets may be non-shared. A moved,
// replaced or foreign target fails the reclaim and is left untouched.
func (r *JailReclaimer) reclaimTool(ctx context.Context, rec jailer.Record) (returnErr error) {
	defer func() {
		if returnErr != nil {
			returnErr = fmt.Errorf("node: reclaim tool %s: %w", rec.SandboxID, returnErr)
		}
	}()
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	p := rec.TargetPath
	if p == "" || path.IsAbs(p) || path.Clean(p) != p || p == ".." || strings.HasPrefix(p, "../") || rec.TargetIno == 0 {
		return fmt.Errorf("unsafe target %q", p)
	}
	var root *os.Root
	switch rec.TargetRoot {
	case jailer.TargetRootData:
		root = r.data
	case jailer.TargetRootImages:
		root = r.images
	default:
		return fmt.Errorf("unknown target root %q", rec.TargetRoot)
	}
	switch rec.TargetAccess {
	case jailer.AccessShared:
		// A verified immutable base is never chowned or chmodded.
		return nil
	case jailer.AccessGroupRO, jailer.AccessOwnerRW:
	default:
		return fmt.Errorf("unknown target access %q", rec.TargetAccess)
	}
	if rec.TargetRoot != jailer.TargetRootData {
		return errors.New("only data-root targets are ever writable or group-readable")
	}
	if root == nil {
		return errors.New("the target root is not bound")
	}
	f, err := openNoFollowChecked(root, p, os.O_RDONLY, false)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := checkPinIdentity(f, rec.TargetDev, rec.TargetIno); err != nil {
		return fmt.Errorf("%s %w", p, err)
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file; preserving it", p)
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if err := f.Chown(os.Geteuid(), os.Getegid()); err != nil {
		return fmt.Errorf("own %s: %w", p, err)
	}
	if err := f.Chmod(0o600); err != nil {
		return fmt.Errorf("protect %s: %w", p, err)
	}
	return r.syncEntry(ctx, f)
}

// openNoFollowChecked opens name under root without following a final
// symlink. os.Root follows a final in-root symlink even with O_NOFOLLOW, so
// the name is Lstat'ed first and must be the wanted type (a directory, or a
// regular file), the open is nonblocking (a FIFO never blocks; its type was
// refused before), and the opened inode must be the Lstat'ed one, which the
// name must still be afterwards. It never creates or truncates.
func openNoFollowChecked(root *os.Root, name string, flag int, wantDir bool) (*os.File, error) {
	if flag&(os.O_CREATE|os.O_TRUNC) != 0 {
		return nil, fmt.Errorf("%s: a checked open never creates or truncates", name)
	}
	kind := func(info os.FileInfo) bool {
		if wantDir {
			return info.Mode().IsDir()
		}
		return info.Mode().IsRegular()
	}
	before, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !kind(before) {
		return nil, fmt.Errorf("%s is a symlink or not the expected type; preserving it", name)
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
		if after, err = root.Lstat(name); err == nil && (!kind(after) || !os.SameFile(before, opened) || !os.SameFile(after, opened)) {
			err = fmt.Errorf("%s changed while it was opened; preserving it", name)
		}
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// validProbeAttempt requires the canonical text of a non-nil uuid.
func validProbeAttempt(s string) bool {
	u, err := uuid.Parse(s)
	return err == nil && u != uuid.Nil() && u.String() == s
}

// openProbeEntry opens one probe scratch entry without following symlinks;
// a missing entry is (nil, nil).
func (r *JailReclaimer) openProbeEntry(name string, flag int) (*os.File, error) {
	f, err := openNoFollowChecked(r.probe, name, flag&^syscall.O_DIRECTORY, flag&syscall.O_DIRECTORY != 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return f, err
}

// reclaimProbe is the probe kind's reclaim policy: it removes the attempt's
// control directory (with at most its QMP socket) and serial log, each only
// while it is the record's inode. It is idempotent: an already removed entry
// is skipped. Any foreign or replaced entry fails the reclaim before
// anything changes.
func (r *JailReclaimer) reclaimProbe(ctx context.Context, rec jailer.Record) (returnErr error) {
	defer func() {
		if returnErr != nil {
			returnErr = fmt.Errorf("node: reclaim probe attempt %s: %w", rec.Attempt, returnErr)
		}
	}()
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if rec.SandboxID != jailer.ProbeID || !validProbeAttempt(rec.Attempt) {
		return fmt.Errorf("not a probe record (%q)", rec.SandboxID)
	}
	controlName, serialName := rec.Attempt+probeControlSuffix, rec.Attempt+probeSerialSuffix
	control, err := r.openProbeEntry(controlName, os.O_RDONLY|syscall.O_DIRECTORY)
	if err != nil {
		return err
	}
	if control != nil {
		defer control.Close()
		if err := checkPinIdentity(control, rec.ControlDev, rec.ControlIno); err != nil {
			return fmt.Errorf("%s %w", controlName, err)
		}
	}
	serial, err := r.openProbeEntry(serialName, os.O_RDONLY)
	if err != nil {
		return err
	}
	if serial != nil {
		defer serial.Close()
		if err := checkPinIdentity(serial, rec.LogDev, rec.LogIno); err != nil {
			return fmt.Errorf("%s %w", serialName, err)
		}
	}
	var sockets []string
	if control != nil {
		if sockets, err = controlSockets(control, rec.UID); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if control != nil {
		if err := removeSockets(control, sockets); err != nil {
			return err
		}
		if err := r.probe.Remove(controlName); err != nil {
			return fmt.Errorf("remove %s: %w", controlName, err)
		}
	}
	if serial != nil {
		if err := r.probe.Remove(serialName); err != nil {
			return fmt.Errorf("remove %s: %w", serialName, err)
		}
	}
	return r.syncProbeDir(ctx)
}

// syncProbeDir makes the probe parent's entries durable.
func (r *JailReclaimer) syncProbeDir(ctx context.Context) error {
	dir, err := r.probe.OpenFile(".", os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return err
	}
	defer dir.Close()
	return r.syncEntry(ctx, dir)
}

// ProbeScratch creates the probe attempt's control directory
// (<attempt>.control, 0700) and serial log (<attempt>.serial, 0600)
// exclusively in the probe parent, makes both inodes and the parent durable
// and returns them pinned.
func (r *JailReclaimer) ProbeScratch(ctx context.Context, attempt string) (control, serial *os.File, err error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, fmt.Errorf("node: probe scratch: %w", context.Cause(ctx))
	}
	if !validProbeAttempt(attempt) {
		return nil, nil, fmt.Errorf("node: probe scratch: invalid attempt %q", attempt)
	}
	controlName, serialName := attempt+probeControlSuffix, attempt+probeSerialSuffix
	if err := r.probe.Mkdir(controlName, 0o700); err != nil {
		return nil, nil, fmt.Errorf("node: probe scratch: %w", err)
	}
	defer func() {
		if err != nil {
			if control != nil {
				control.Close()
			}
			if serial != nil {
				serial.Close()
			}
			control, serial = nil, nil
		}
	}()
	if control, err = r.probe.OpenFile(controlName, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0); err != nil {
		return nil, nil, fmt.Errorf("node: probe scratch: %w", err)
	}
	if serial, err = r.probe.OpenFile(serialName, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600); err != nil {
		return nil, nil, fmt.Errorf("node: probe scratch: %w", err)
	}
	for _, f := range []*os.File{control, serial} {
		if err = f.Chmod(map[bool]os.FileMode{true: 0o700, false: 0o600}[f == control]); err != nil {
			return nil, nil, fmt.Errorf("node: probe scratch: %w", err)
		}
		if err = r.syncEntry(ctx, f); err != nil {
			return nil, nil, fmt.Errorf("node: probe scratch: %w", err)
		}
	}
	if err = r.syncProbeDir(ctx); err != nil {
		return nil, nil, fmt.Errorf("node: probe scratch: %w", err)
	}
	return control, serial, nil
}

// RemoveProbeScratch removes a probe attempt's scratch that no jail record
// names (its launch failed before the jailer recorded it), each entry only
// while it is still the given pinned inode. Foreign replacements stay.
func (r *JailReclaimer) RemoveProbeScratch(ctx context.Context, attempt string, control, serial *os.File) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("node: remove probe scratch: %w", context.Cause(ctx))
	}
	if !validProbeAttempt(attempt) {
		return fmt.Errorf("node: remove probe scratch: invalid attempt %q", attempt)
	}
	var errs []error
	for _, e := range []struct {
		name string
		pin  *os.File
		flag int
	}{
		{attempt + probeControlSuffix, control, os.O_RDONLY | syscall.O_DIRECTORY},
		{attempt + probeSerialSuffix, serial, os.O_RDONLY},
	} {
		if e.pin == nil {
			continue
		}
		pinned, err := e.pin.Stat()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		current, err := r.probe.Lstat(e.name)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !os.SameFile(pinned, current) {
			errs = append(errs, fmt.Errorf("%s was replaced; preserving it", e.name))
			continue
		}
		if err := r.probe.Remove(e.name); err != nil {
			errs = append(errs, err)
		}
	}
	errs = append(errs, r.syncProbeDir(context.WithoutCancel(ctx)))
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("node: remove probe scratch %s: %w", attempt, err)
	}
	return nil
}

// dataDirForTest returns the data directory path.
func (r *JailReclaimer) dataDirForTest() string { return r.dataDir }

// Close closes the roots.
func (r *JailReclaimer) Close() error {
	if r == nil {
		return nil
	}
	var errs []error
	for _, root := range []*os.Root{r.data, r.probe, r.images} {
		if root != nil {
			errs = append(errs, root.Close())
		}
	}
	return errors.Join(errs...)
}
