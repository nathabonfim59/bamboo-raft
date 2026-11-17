package storage

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
	"time"
	"uuid"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// Jailed launch layout inside vms/<id>. The control directory holds only
// QEMU's QMP socket and is attached at /control in the chroot; the console
// log is attached at /serial.log. Both are created by the node, never by
// QEMU, and their inodes are recorded in the manifest before first use.
const (
	ControlDirName = "control"
	ConsoleLogName = "console.log"
)

// Identity is a durable dev+ino pair: the inode a node-created resource
// must still be before it is used or reclaimed.
type Identity struct {
	Dev uint64 `json:"dev"`
	Ino uint64 `json:"ino"`
}

// LaunchPins are the pinned host resources of one jailed launch, every one
// opened without following a final symlink and verified against its
// expected identity, type, owner and mode. The caller passes them to the
// jailer and closes them with Close once the launch returned.
type LaunchPins struct {
	Control *os.File // vms/<id>/control, O_RDONLY directory
	Console *os.File // vms/<id>/console.log, O_RDWR|O_APPEND
	Overlay *os.File // vms/<id>/disk.qcow2, O_RDWR
	Base    *os.File // <image>/root.qcow2, O_RDONLY
	Kernel  *os.File // <image>/kernel, O_RDONLY
	Initrd  *os.File // <image>/initrd, O_RDONLY
	// BaseName is the exact stored backing name the overlay header carries.
	BaseName string
}

// Close closes every pinned descriptor exactly once. It is idempotent.
func (p *LaunchPins) Close() error {
	if p == nil {
		return nil
	}
	var errs []error
	for _, f := range []**os.File{&p.Control, &p.Console, &p.Overlay, &p.Base, &p.Kernel, &p.Initrd} {
		if *f != nil {
			errs = append(errs, (*f).Close())
			*f = nil
		}
	}
	return errors.Join(errs...)
}

// PinLaunch opens and verifies the jailed launch resources of a VM whose
// QEMU is proven absent and whose previous jail is released:
//
//   - control: a directory with the manifest's recorded inode, owned by the
//     node's euid, mode 0700 and empty;
//   - console.log: a regular file with the recorded inode, owned by the
//     node's euid;
//   - disk.qcow2: a qcow2 overlay whose stored backing name is exactly the
//     verified image's root.qcow2 path, without dirty, corrupt, encrypted or
//     external-data-file features;
//   - the image's root.qcow2 (standalone qcow2), kernel and initrd.
//
// A VM never launched jailed has no recorded identities: PinLaunch then
// creates control and console.log exclusively, records both inodes durably
// and only then pins them. An existing unrecorded or mismatched entry is
// refused and preserved for operator cleanup. The stored backing name is
// only compared, never opened.
func (s *Store) PinLaunch(ctx context.Context, id string, img Image) (_ *LaunchPins, returnErr error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("storage: vm %s: pin launch: %w", id, context.Cause(ctx))
	}
	meta, err := s.ReadVM(id)
	if err != nil {
		return nil, err
	}
	if meta.Image != img.ID {
		return nil, fmt.Errorf("storage: vm %s uses image %q, not %q", id, meta.Image, img.ID)
	}
	vmRoot, err := s.root.OpenRoot(path.Join(vmsDir, id))
	if err != nil {
		return nil, fmt.Errorf("storage: open vm root %s: %w", id, err)
	}
	defer vmRoot.Close()
	pins := &LaunchPins{BaseName: filepath.Join(img.Dir, "root.qcow2")}
	defer func() {
		if returnErr != nil {
			pins.Close()
		}
	}()
	if pins.Overlay, err = openChecked(vmRoot, diskName, os.O_RDWR, false); err != nil {
		return nil, fmt.Errorf("storage: vm %s: open overlay: %w", id, err)
	}
	if err := checkRegularOwned(pins.Overlay, "overlay"); err != nil {
		return nil, fmt.Errorf("storage: vm %s: %w", id, err)
	}
	header, err := readQCOWHeader(pins.Overlay)
	if err != nil {
		return nil, fmt.Errorf("storage: vm %s: overlay: %w", id, err)
	}
	if header.backing != pins.BaseName {
		return nil, fmt.Errorf("storage: vm %s: overlay backing name %q is not the verified base %q", id, header.backing, pins.BaseName)
	}
	imgRoot, err := os.OpenRoot(img.Dir)
	if err != nil {
		return nil, fmt.Errorf("storage: open image %s: %w", img.ID, err)
	}
	defer imgRoot.Close()
	for _, f := range []struct {
		name string
		dst  **os.File
	}{{"root.qcow2", &pins.Base}, {"kernel", &pins.Kernel}, {"initrd", &pins.Initrd}} {
		file, err := imgRoot.OpenFile(f.name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		if err != nil {
			return nil, fmt.Errorf("storage: image %s: open %s: %w", img.ID, f.name, err)
		}
		*f.dst = file
		info, err := file.Stat()
		if err != nil {
			return nil, fmt.Errorf("storage: image %s: %s: %w", img.ID, f.name, err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("storage: image %s: %s is not a regular file", img.ID, f.name)
		}
	}
	base, err := readQCOWHeader(pins.Base)
	if err != nil {
		return nil, fmt.Errorf("storage: image %s: base: %w", img.ID, err)
	}
	if base.backing != "" {
		return nil, fmt.Errorf("storage: image %s: base is not standalone (backing %q)", img.ID, base.backing)
	}
	// Only a fully verified disk graph reaches the control layout: a refused
	// launch creates nothing.
	if meta.Control == nil || meta.Console == nil {
		if meta.Control != nil || meta.Console != nil {
			return nil, fmt.Errorf("storage: vm %s records only part of its jailed layout; refusing it", id)
		}
		if err := s.createJailedLayout(ctx, vmRoot, &meta, pins); err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			// The layout record is published; the retry pins it.
			return nil, fmt.Errorf("storage: vm %s: pin launch: %w", id, context.Cause(ctx))
		}
	} else {
		if pins.Control, err = pinControl(vmRoot, id, *meta.Control); err != nil {
			return nil, err
		}
		if pins.Console, err = pinConsole(vmRoot, id, *meta.Console); err != nil {
			return nil, err
		}
		// A recorded layout may come from a write whose barrier failed
		// (ErrManifestDurabilityUnknown): every pin re-establishes them.
		if err := s.layoutBarriers(vmRoot, id, pins); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("storage: vm %s: pin launch: %w", id, context.Cause(ctx))
	}
	return pins, nil
}

// createJailedLayout makes the control directory and console log of a VM
// that has none, exclusively, and records both inodes before they are used.
// If the record cannot be written the new entries are removed again by
// their pinned identities; an uncertain manifest write keeps them.
func (s *Store) createJailedLayout(ctx context.Context, vmRoot *os.Root, meta *VMMeta, pins *LaunchPins) (returnErr error) {
	id := meta.SandboxID
	// created tracks the entries this call made; on any failure before the
	// manifest that records them is published they are removed again, each
	// only if it is still the inode this call created.
	created := map[string]Identity{}
	published := false
	defer func() {
		if returnErr == nil || published {
			return
		}
		for _, name := range []string{ConsoleLogName, ControlDirName} {
			if want, ok := created[name]; ok {
				if current, err := lstatIdentity(vmRoot, name); err == nil && current == want {
					_ = vmRoot.Remove(name)
				}
			}
		}
	}()
	canceled := func(stage string) error {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("storage: vm %s: %s: %w", id, stage, context.Cause(ctx))
		}
		return nil
	}
	for _, name := range []string{ControlDirName, ConsoleLogName} {
		if _, err := vmRoot.Lstat(name); err == nil {
			return fmt.Errorf("storage: vm %s: unrecorded %s exists; preserved for operator cleanup", id, name)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("storage: vm %s: inspect %s: %w", id, name, err)
		}
	}
	if err := canceled("create control directory"); err != nil {
		return err
	}
	if err := vmRoot.Mkdir(ControlDirName, 0o700); err != nil {
		return fmt.Errorf("storage: vm %s: create control directory: %w", id, err)
	}
	control, err := openChecked(vmRoot, ControlDirName, os.O_RDONLY, true)
	if err != nil {
		// Unpinned: the new directory is left for operator cleanup rather
		// than removed by a name that may no longer be ours.
		return fmt.Errorf("storage: vm %s: open new control directory: %w", id, err)
	}
	pins.Control = control
	controlID, err := identityOf(control)
	if err != nil {
		return err
	}
	created[ControlDirName] = controlID
	if err := canceled("create console log"); err != nil {
		return err
	}
	console, err := vmRoot.OpenFile(ConsoleLogName, os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600)
	if err != nil {
		return fmt.Errorf("storage: vm %s: create console log: %w", id, err)
	}
	pins.Console = console
	consoleID, err := identityOf(console)
	if err != nil {
		return err
	}
	created[ConsoleLogName] = consoleID
	// Each new inode and the directory naming both are durable before the
	// manifest that records them is written.
	if err := s.layoutBarriers(vmRoot, id, pins); err != nil {
		return err
	}
	if err := canceled("record jailed layout"); err != nil {
		return err
	}
	meta.Control, meta.Console = &controlID, &consoleID
	if err := s.writeManifest(*meta); err != nil {
		// A rename that may have published the record keeps the entries: the
		// manifest can name them, so a retry pins them by identity.
		published = errors.Is(err, ErrManifestDurabilityUnknown)
		return fmt.Errorf("storage: vm %s: record jailed layout: %w", id, err)
	}
	published = true
	return nil
}

// layoutBarriers syncs the control directory and console log inodes, then
// the VM directory that names them (and the manifest).
func (s *Store) layoutBarriers(vmRoot *os.Root, id string, pins *LaunchPins) error {
	for _, f := range []*os.File{pins.Control, pins.Console} {
		if err := s.cfg.Snapshots.SyncFile(f); err != nil {
			return fmt.Errorf("storage: vm %s: sync %s: %w", id, f.Name(), err)
		}
	}
	if err := s.cfg.Snapshots.SyncDir(vmRoot, "."); err != nil {
		return fmt.Errorf("storage: vm %s: sync jailed layout: %w", id, err)
	}
	return nil
}

func pinControl(vmRoot *os.Root, id string, want Identity) (*os.File, error) {
	f, err := openChecked(vmRoot, ControlDirName, os.O_RDONLY, true)
	if err != nil {
		return nil, fmt.Errorf("storage: vm %s: open control directory: %w", id, err)
	}
	fail := func(err error) (*os.File, error) {
		f.Close()
		return nil, fmt.Errorf("storage: vm %s: %w", id, err)
	}
	got, err := identityOf(f)
	if err != nil {
		return fail(err)
	}
	if got != want {
		return fail(errors.New("control directory is not the recorded inode; preserved for operator cleanup"))
	}
	info, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if st := info.Sys().(*syscall.Stat_t); int(st.Uid) != os.Geteuid() {
		return fail(fmt.Errorf("control directory is owned by uid %d, not the node", st.Uid))
	}
	if info.Mode().Perm() != 0o700 {
		return fail(fmt.Errorf("control directory mode %v, want 0700", info.Mode().Perm()))
	}
	entries, err := f.ReadDir(1)
	if err != nil && !errors.Is(err, io.EOF) {
		return fail(fmt.Errorf("read control directory: %w", err))
	}
	if len(entries) != 0 {
		return fail(fmt.Errorf("control directory is not empty (%s); preserved for operator cleanup", entries[0].Name()))
	}
	return f, nil
}

func pinConsole(vmRoot *os.Root, id string, want Identity) (*os.File, error) {
	f, err := openChecked(vmRoot, ConsoleLogName, os.O_RDWR|os.O_APPEND, false)
	if err != nil {
		return nil, fmt.Errorf("storage: vm %s: open console log: %w", id, err)
	}
	got, err := identityOf(f)
	if err == nil && got != want {
		err = errors.New("console log is not the recorded inode; preserved for operator cleanup")
	}
	if err == nil {
		err = checkRegularOwned(f, "console log")
	}
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("storage: vm %s: %w", id, err)
	}
	return f, nil
}

func checkRegularOwned(f *os.File, what string) error {
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", what)
	}
	if st := info.Sys().(*syscall.Stat_t); int(st.Uid) != os.Geteuid() {
		return fmt.Errorf("%s is owned by uid %d, not the node", what, st.Uid)
	}
	return nil
}

func identityOf(f *os.File) (Identity, error) {
	info, err := f.Stat()
	if err != nil {
		return Identity{}, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return Identity{}, errors.New("storage: no Linux file identity")
	}
	return Identity{Dev: uint64(st.Dev), Ino: st.Ino}, nil
}

func lstatIdentity(root *os.Root, name string) (Identity, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return Identity{}, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return Identity{}, errors.New("storage: no Linux file identity")
	}
	return Identity{Dev: uint64(st.Dev), Ino: st.Ino}, nil
}

// validJailAttempt requires the canonical text of a non-nil uuid.
func validJailAttempt(s string) bool {
	u, err := uuid.Parse(s)
	return err == nil && u != uuid.Nil() && u.String() == s
}

// validArgvDigest requires 64 lowercase hex digits.
func validArgvDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// ErrIntentPublished marks a launch-intent error returned after the intent
// was (or may have been) published: the caller must not launch, and must
// treat the manifest as naming that attempt.
var ErrIntentPublished = errors.New("storage: launch intent was published")

// SyncEntry makes an opened VM entry durable through the store's sync seam.
func (s *Store) SyncEntry(ctx context.Context, f *os.File) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("storage: sync %s: %w", f.Name(), context.Cause(ctx))
	}
	if err := s.cfg.Snapshots.SyncFile(f); err != nil {
		return fmt.Errorf("storage: sync %s: %w", f.Name(), err)
	}
	return nil
}

// WriteJailedLaunchIntent records the launch intent of a jailed QEMU before
// the jailer is called: the attempt that will own it and the sha256 of its
// exact argv, which a restarted node compares with the live process.
func (s *Store) WriteJailedLaunchIntent(ctx context.Context, id, attempt, argvSHA256 string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("storage: vm %s: launch intent: %w", id, context.Cause(ctx))
	}
	if !validJailAttempt(attempt) {
		return fmt.Errorf("storage: vm %s: invalid jail attempt %q", id, attempt)
	}
	if !validArgvDigest(argvSHA256) {
		return fmt.Errorf("storage: vm %s: invalid argv digest %q", id, argvSHA256)
	}
	meta, err := s.ReadVM(id)
	if err != nil {
		return err
	}
	bootID, err := HostBootID()
	if err != nil {
		return err
	}
	meta.QEMU = &QEMURecord{
		State: QEMULaunching, BootID: bootID, StartedAtUnixMs: time.Now().UnixMilli(),
		JailAttempt: attempt, ArgvSHA256: argvSHA256,
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("storage: vm %s: launch intent: %w", id, context.Cause(ctx))
	}
	if err := s.writeManifest(meta); err != nil {
		if errors.Is(err, ErrManifestDurabilityUnknown) {
			return fmt.Errorf("storage: vm %s: %w: %w", id, ErrIntentPublished, err)
		}
		return err
	}
	// The intent is durable; a context that ended meanwhile is still
	// reported, and the caller must not launch.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("storage: vm %s: %w, but the launch was canceled: %w", id, ErrIntentPublished, context.Cause(ctx))
	}
	return nil
}

// OpenVMEntry opens name inside vms/<id> without following symlinks, for
// reclamation that then verifies the inode against a durable record. name
// must be one path element.
func (s *Store) OpenVMEntry(ctx context.Context, id, name string, flag int) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("storage: vm %s: open %s: %w", id, name, context.Cause(ctx))
	}
	if !ids.ValidSandbox(id) {
		return nil, fmt.Errorf("storage: %q is not a sandbox id", id)
	}
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return nil, fmt.Errorf("storage: invalid vm entry name %q", name)
	}
	vmRoot, err := s.root.OpenRoot(path.Join(vmsDir, id))
	if err != nil {
		return nil, fmt.Errorf("storage: open vm root %s: %w", id, err)
	}
	defer vmRoot.Close()
	if flag&(os.O_CREATE|os.O_TRUNC) != 0 {
		return nil, fmt.Errorf("storage: vm %s: open %s: entries are opened, never created or truncated", id, name)
	}
	return openChecked(vmRoot, name, flag&^syscall.O_DIRECTORY, flag&syscall.O_DIRECTORY != 0)
}
