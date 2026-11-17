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
	"syscall"

	"golang.org/x/sys/unix"
)

// maxHeldWalk bounds the scan of a tree for leased targets; a larger tree
// is treated as holding one.
const maxHeldWalk = 4096

// imageBaseName is the exact backing name every overlay of img stores.
func (s *Store) imageBaseName(imageID string) string {
	return filepath.Join(s.ImagePath(imageID), "root.qcow2")
}

// openImageBase pins imageID's base read-only without following symlinks.
func (s *Store) openImageBase(imageID string) (*os.File, error) {
	if !ValidImageID(imageID) {
		return nil, fmt.Errorf("storage: invalid image id %q", imageID)
	}
	root, err := os.OpenRoot(s.imageDir)
	if err != nil {
		return nil, fmt.Errorf("storage: open image root: %w", err)
	}
	defer root.Close()
	f, err := pinNoFollow(root, path.Join(imageID, "root.qcow2"), os.O_RDONLY)
	if err != nil {
		return nil, fmt.Errorf("storage: pin image base %s: %w", imageID, err)
	}
	return f, nil
}

// pinNoFollow pins a regular file under root (see openChecked).
func pinNoFollow(root *os.Root, name string, flag int) (*os.File, error) {
	return openChecked(root, name, flag, false)
}

// openChecked opens name under root without following a final symlink.
// os.Root follows a final in-root symlink even with O_NOFOLLOW, so the name
// is Lstat'ed first and must be the wanted type (a directory, or a regular
// file), the open is nonblocking (a FIFO or device never blocks; its type
// was refused before), and the opened inode must be the Lstat'ed one, which
// the name must still be afterwards. It never creates or truncates.
func openChecked(root *os.Root, name string, flag int, wantDir bool) (*os.File, error) {
	if flag&(os.O_CREATE|os.O_TRUNC) != 0 {
		return nil, fmt.Errorf("storage: %s: a checked open never creates or truncates", name)
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
		return nil, fmt.Errorf("storage: %s is a symlink or not the expected type", name)
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
			err = fmt.Errorf("storage: %s changed while it was opened", name)
		}
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// overlayRef pins the data-root overlay rel (read-only or read-write) with
// its image's base: the Store's expected backing is the authority, never the
// overlay's header.
func (s *Store) overlayRef(rel, imageID string, write bool) (ImageRef, func(), error) {
	flag := os.O_RDONLY
	if write {
		flag = os.O_RDWR
	}
	f, err := pinNoFollow(s.root, rel, flag)
	if err != nil {
		return ImageRef{}, nil, fmt.Errorf("storage: pin %s: %w", rel, err)
	}
	base, err := s.openImageBase(imageID)
	if err != nil {
		f.Close()
		return ImageRef{}, nil, err
	}
	ref := ImageRef{File: f, Root: ImageRootData, Path: rel, Format: "qcow2", ExpectedBacking: s.imageBaseName(imageID), Base: base}
	return ref, func() { f.Close(); base.Close() }, nil
}

// treeHeld reports whether any regular file under the data-root dir is a
// target a jailed tool still holds. An unreadable or oversized tree counts
// as held: removal never guesses.
func (s *Store) treeHeld(dir string) bool {
	if s.cfg.Leases == nil {
		return false
	}
	seen := 0
	held := false
	err := fs.WalkDir(s.root.FS(), dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		seen++
		if seen > maxHeldWalk {
			return errors.New("tree too large")
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok && s.cfg.Leases.TargetHeld(uint64(st.Dev), st.Ino) {
			held = true
			return fs.SkipAll
		}
		return nil
	})
	return held || err != nil
}

// keepForTool reports whether a rollback of dir must keep it: the failure
// left a retained tool, or a tool still holds something under dir.
func (s *Store) keepForTool(dir string, cause error) bool {
	return errors.Is(cause, ErrToolRetained) || s.treeHeld(dir)
}

// reflinkCopy makes a private copy of source into the new, empty pinned
// destination, in this process: a reflink (FICLONE) when the filesystem
// supports it, else copy_file_range, else a userspace copy, each chunk
// checked against ctx. Neither file is reopened by name.
func reflinkCopy(ctx context.Context, source, destination *os.File) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := unix.IoctlFileClone(int(destination.Fd()), int(source.Fd())); err == nil {
		return ctx.Err()
	}
	const chunk = 8 << 20
	var off int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		srcOff, dstOff := off, off
		n, err := unix.CopyFileRange(int(source.Fd()), &srcOff, int(destination.Fd()), &dstOff, chunk, 0)
		if err != nil {
			if off == 0 && (errors.Is(err, unix.EXDEV) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP)) {
				return userspaceCopy(ctx, source, destination)
			}
			return fmt.Errorf("copy_file_range: %w", err)
		}
		if n == 0 {
			return nil
		}
		off += int64(n)
	}
}

// userspaceCopy copies source to destination from offset 0 in bounded
// chunks, checking ctx between them.
func userspaceCopy(ctx context.Context, source, destination *os.File) error {
	buf := make([]byte, 1<<20)
	var off int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := source.ReadAt(buf, off)
		if n > 0 {
			if _, werr := destination.WriteAt(buf[:n], off); werr != nil {
				return fmt.Errorf("copy: %w", werr)
			}
			off += int64(n)
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("copy: %w", err)
		}
	}
}
