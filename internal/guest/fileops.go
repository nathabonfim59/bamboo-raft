package guest

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// maxReadChunk is the largest ReadFile data message, matching nodev1.
const maxReadChunk = 1 << 20

// maxUploadChunk is the largest PutFile data message, matching nodev1.
const maxUploadChunk = 1 << 20

// File list limits from guest.proto's ListFilesRequest.limit.
const (
	defaultListLimit = 1000
	maxListLimit     = 10000
)

// maxSymlinkHops bounds symlink traversal within one resolved path, the
// same limit Linux applies before returning ELOOP.
const maxSymlinkHops = 40

// fileServer implements the guest file operations as the process's own
// user. In the real guest it runs inside the credential-dropped helper
// process, so the kernel — not user-space checks — enforces the sandbox
// user's mode bits, access ACLs and sticky directories. It is also used
// in-process when the agent already runs as the configured sandbox user
// (tests and unprivileged smoke runs). Paths stay confined with [os.Root],
// and symlinks are resolved component by component across the home and tmp
// roots with a bounded number of hops.
type fileServer struct {
	home    *os.Root
	tmp     *os.Root
	syncDir func(*os.File) error
	log     *slog.Logger
}

// newFileServer binds the operations to an existing pair of confinement
// roots. The caller keeps ownership of the roots.
func newFileServer(home, tmp *os.Root, syncDir func(*os.File) error, log *slog.Logger) *fileServer {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if syncDir == nil {
		syncDir = func(d *os.File) error { return d.Sync() }
	}
	return &fileServer{home: home, tmp: tmp, syncDir: syncDir, log: log}
}

// openFileServerFDs opens the helper's roots from the pinned directory
// descriptors it inherited from the parent. /proc/self/fd/N resolves to the
// already-open inode, so a configured path renamed or replaced after the
// service started cannot move the helper to another directory. The reopen
// runs as the dropped user, so the kernel still enforces access to the root.
func openFileServerFDs(homeFD, tmpFD int) (*fileServer, error) {
	home, err := os.OpenRoot("/proc/self/fd/" + strconv.Itoa(homeFD))
	if err != nil {
		return nil, logicalRootError(homeSpec, err)
	}
	tmp, err := os.OpenRoot("/proc/self/fd/" + strconv.Itoa(tmpFD))
	if err != nil {
		home.Close()
		return nil, logicalRootError(tmpSpec, err)
	}
	return newFileServer(home, tmp, nil, nil), nil
}

// rootPermissionError is a root the sandbox user cannot open for lack of
// permission (for example a search-only 0111 directory). It is a client
// error, unlike other startup failures.
type rootPermissionError struct {
	spec string
	err  error
}

func (e *rootPermissionError) Error() string {
	return fmt.Sprintf("permission denied on the logical root %s: %v", e.spec, e.err)
}

func (e *rootPermissionError) Unwrap() error { return e.err }

// logicalRootError names the v1 API root a helper could not open and keeps
// permission failures distinguishable from internal startup failures.
func logicalRootError(spec string, err error) error {
	if errors.Is(err, fs.ErrPermission) {
		return &rootPermissionError{spec: spec, err: err}
	}
	return fmt.Errorf("cannot open the logical root %s: %w", spec, err)
}

// helperStartupCode maps a root startup failure to its client-visible code:
// only permission failures are INVALID_ARGUMENT, everything else is INTERNAL.
func helperStartupCode(err error) codes.Code {
	if _, ok := errors.AsType[*rootPermissionError](err); ok {
		return codes.InvalidArgument
	}
	return codes.Internal
}

// readFile streams one regular file: first info, then data chunks of at most
// 1 MiB. A file over maxBytes is refused before the info frame. The entry is
// lstat'ed so a symlink keeps its own path and is_symlink; the target is
// resolved under the allowed roots and the opened inode's type and size
// decide what is read.
func (f *fileServer) readFile(ctx context.Context, p string, maxBytes int64, send func(*nodev1.ReadFileResponse) error) error {
	_, _, logical, err := f.resolve(p)
	if err != nil {
		return err
	}
	entryRT, entryRel, err := f.walk("read", p, false, false)
	if err != nil {
		return err
	}
	lfi, err := entryRT.r.Lstat(entryRel)
	if err != nil {
		return fileErr("read", logical, err)
	}
	targetRT, targetRel, err := f.walk("read", p, true, false)
	if err != nil {
		return err
	}
	// Refuse non-regular targets before opening them: a FIFO or device would
	// otherwise be opened (even non-blocking) just to be rejected.
	tfi, err := targetRT.r.Lstat(targetRel)
	if err != nil {
		return fileErr("read", logical, err)
	}
	if !tfi.Mode().IsRegular() {
		return status.Errorf(codes.InvalidArgument, "read %s: not a regular file", logical)
	}
	// O_NONBLOCK keeps a FIFO swapped in after resolution from blocking the
	// open; regular files ignore it.
	file, err := targetRT.r.OpenFile(targetRel, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return fileErr("read", logical, err)
	}
	defer file.Close()
	fi, err := file.Stat()
	if err != nil {
		return fileErr("read", logical, err)
	}
	if !fi.Mode().IsRegular() {
		return status.Errorf(codes.InvalidArgument, "read %s: not a regular file", logical)
	}
	if maxBytes > 0 && fi.Size() > maxBytes {
		return status.Errorf(codes.OutOfRange, "read %s: %d bytes is above the %d byte limit", logical, fi.Size(), maxBytes)
	}
	info := &nodev1.FileInfo{
		Path:             logical,
		Name:             path.Base(logical),
		Type:             nodev1.FileType_FILE_TYPE_REGULAR,
		IsSymlink:        lfi.Mode()&fs.ModeSymlink != 0,
		Size:             fi.Size(),
		Mode:             unixMode(fi.Mode()),
		ModifiedAtUnixMs: fi.ModTime().UnixMilli(),
	}
	if err := send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Info{Info: info}}); err != nil {
		return err
	}
	buf := make([]byte, maxReadChunk)
	var sent int64
	for {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		n, rerr := file.Read(buf)
		if n > 0 {
			// The size precheck above covers the file as it was opened; a
			// file that grows while streaming must not slip past max_bytes.
			// Never send a chunk that crosses the limit.
			if maxBytes > 0 && sent+int64(n) > maxBytes {
				return status.Errorf(codes.OutOfRange, "read %s: file grew past the %d byte limit", logical, maxBytes)
			}
			sent += int64(n)
			chunk := bytes.Clone(buf[:n])
			if err := send(&nodev1.ReadFileResponse{Part: &nodev1.ReadFileResponse_Data{Data: chunk}}); err != nil {
				return err
			}
		}
		switch {
		case errors.Is(rerr, io.EOF):
			return nil
		case rerr != nil:
			return status.Errorf(codes.Internal, "read %s: %v", logical, rerr)
		}
	}
}

// statFile describes one path. A symlink keeps its own path and name and
// is_symlink; when its target resolves under the allowed roots, type, size,
// mode and modification time describe the target.
func (f *fileServer) statFile(_ context.Context, p string) (*nodev1.FileInfo, error) {
	_, _, logical, err := f.resolve(p)
	if err != nil {
		return nil, err
	}
	rt, rel, err := f.walk("stat", p, false, false)
	if err != nil {
		return nil, err
	}
	info, err := f.entryInfo(rt, rel, logical)
	if err != nil {
		return nil, fileErr("stat", logical, err)
	}
	return info, nil
}

// listFiles lists a directory's direct entries, sorted by name, at most
// limit of them; truncated reports the ones left out. An empty path lists
// the home directory, as the v1 API documents.
func (f *fileServer) listFiles(_ context.Context, p string, limit uint32) (*nodev1.ListFilesResponse, error) {
	n := int(limit)
	if n == 0 {
		n = defaultListLimit
	}
	if n > maxListLimit {
		return nil, status.Errorf(codes.InvalidArgument, "limit %d is above %d", n, maxListLimit)
	}
	if p == "" {
		p = homeSpec
	}
	_, _, logical, err := f.resolve(p)
	if err != nil {
		return nil, err
	}
	rt, rel, err := f.walk("list", p, true, false)
	if err != nil {
		return nil, err
	}
	d, err := rt.r.Open(rel)
	if err != nil {
		return nil, fileErr("list", logical, err)
	}
	defer d.Close()
	fi, err := d.Stat()
	if err != nil {
		return nil, fileErr("list", logical, err)
	}
	if !fi.IsDir() {
		return nil, status.Errorf(codes.InvalidArgument, "list %s: not a directory", logical)
	}
	entries, err := d.ReadDir(-1)
	if err != nil {
		return nil, fileErr("list", logical, err)
	}
	slices.SortFunc(entries, func(a, b fs.DirEntry) int {
		return cmp.Compare(a.Name(), b.Name())
	})
	out := &nodev1.ListFilesResponse{Path: logical}
	for _, de := range entries {
		if len(out.Entries) == n {
			out.Truncated = true
			break
		}
		info, err := f.entryInfo(rt, path.Join(rel, de.Name()), path.Join(logical, de.Name()))
		if err != nil {
			continue // removed while listing
		}
		out.Entries = append(out.Entries, info)
	}
	return out, nil
}

// deleteFile removes a file, a symlink (never its target) or an empty
// directory; with recursive, a directory and everything in it. The roots
// themselves cannot be deleted. Everything runs as the sandbox user, so the
// kernel decides write and sticky-bit access at the unlink.
func (f *fileServer) deleteFile(ctx context.Context, p string, recursive bool) (*nodev1.DeleteFileResponse, error) {
	_, _, logical, err := f.resolve(p)
	if err != nil {
		return nil, err
	}
	if isRootPath(logical) {
		return nil, status.Errorf(codes.InvalidArgument, "delete %s: cannot delete a root", logical)
	}
	rt, rel, err := f.walk("delete", p, false, false)
	if err != nil {
		return nil, err
	}
	lfi, err := rt.r.Lstat(rel)
	if err != nil {
		return nil, fileErr("delete", logical, err)
	}
	if recursive && lfi.IsDir() {
		if err := f.removeTree(ctx, rt, rel, logical); err != nil {
			return nil, err
		}
	} else if err := f.removeEntry(rt, rel, logical, "delete"); err != nil {
		return nil, err
	}
	return &nodev1.DeleteFileResponse{Path: logical}, nil
}

// makeDir creates a directory and its missing parents as the sandbox user.
// An existing directory is returned unchanged; an existing non-directory is
// ALREADY_EXISTS. A requested mode of 0 means 0755, and modes are limited to
// 0o7777.
func (f *fileServer) makeDir(_ context.Context, p string, mode uint32) (*nodev1.FileInfo, error) {
	if mode == 0 {
		mode = 0o755
	}
	if mode > 0o7777 {
		return nil, status.Errorf(codes.InvalidArgument, "mkdir %s: mode %#o is above 07777", p, mode)
	}
	_, _, logical, err := f.resolve(p)
	if err != nil {
		return nil, err
	}
	rt, rel, err := f.walk("mkdir", p, false, true)
	if err != nil {
		return nil, err
	}
	if fi, err := rt.r.Lstat(rel); err == nil {
		if !fi.IsDir() {
			return nil, status.Errorf(codes.AlreadyExists, "mkdir %s: exists and is not a directory", logical)
		}
		return f.entryInfo(rt, rel, logical)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fileErr("mkdir", logical, err)
	}
	if err := f.ensureParents(rt, rel, logical, "mkdir"); err != nil {
		return nil, err
	}
	if err := rt.r.Mkdir(rel, 0o700); err != nil {
		if errors.Is(err, fs.ErrExist) {
			// A concurrent creator won the race; its directory and mode
			// stand, exactly like a second sequential call.
			if fi, serr := rt.r.Lstat(rel); serr == nil && fi.IsDir() {
				return f.entryInfo(rt, rel, logical)
			}
		}
		return nil, fileErr("mkdir", logical, err)
	}
	d, err := rt.r.Open(rel)
	if err != nil {
		return nil, fileErr("mkdir", logical, err)
	}
	chmodErr := d.Chmod(fileMode(mode)) // Mkdir's mode is filtered by umask
	closeErr := d.Close()
	if err := errors.Join(chmodErr, closeErr); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir %s: %v", logical, err)
	}
	return f.entryInfo(rt, rel, logical)
}

// moveFile renames source to destination as the sandbox user, following
// symlinks on the way but moving the source entry itself (a symlink is
// moved, never its target). Missing destination parents are created. An
// existing destination is ALREADY_EXISTS unless overwrite, which replaces a
// file or an empty directory. Cross-device moves are reported, never
// silently copied.
func (f *fileServer) moveFile(_ context.Context, source, destination string, overwrite bool) (*nodev1.MoveFileResponse, error) {
	_, _, srcLogical, err := f.resolve(source)
	if err != nil {
		return nil, err
	}
	_, _, dstLogical, err := f.resolve(destination)
	if err != nil {
		return nil, err
	}
	if isRootPath(srcLogical) || isRootPath(dstLogical) {
		return nil, status.Errorf(codes.InvalidArgument, "move %s: cannot move a root", srcLogical)
	}
	srcRT, srcRel, err := f.walk("move", source, false, false)
	if err != nil {
		return nil, err
	}
	dstRT, dstRel, err := f.walk("move", destination, false, true)
	if err != nil {
		return nil, err
	}
	sfi, err := srcRT.r.Lstat(srcRel)
	if err != nil {
		return nil, fileErr("move", srcLogical, err)
	}
	if fi, err := dstRT.r.Lstat(dstRel); err == nil {
		if !overwrite {
			return nil, status.Errorf(codes.AlreadyExists, "move %s: %s exists", srcLogical, dstLogical)
		}
		if fi.IsDir() && !sfi.IsDir() {
			// A file taking an empty directory's place: rename cannot
			// replace a directory with a non-directory, so remove it first.
			if err := f.removeEntry(dstRT, dstRel, dstLogical, "move"); err != nil {
				return nil, err
			}
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fileErr("move", dstLogical, err)
	}
	if err := f.ensureParents(dstRT, dstRel, dstLogical, "move"); err != nil {
		return nil, err
	}
	srcDir, err := openParent(srcRT, srcRel)
	if err != nil {
		return nil, fileErr("move", srcLogical, err)
	}
	defer srcDir.Close()
	dstDir, err := openParent(dstRT, dstRel)
	if err != nil {
		return nil, fileErr("move", dstLogical, err)
	}
	defer dstDir.Close()

	flags := uint(0)
	if !overwrite {
		// Check-and-rename is not atomic; RENAME_NOREPLACE makes the
		// ALREADY_EXISTS contract hold even against a concurrent creator.
		flags = unix.RENAME_NOREPLACE
	}
	err = unix.Renameat2(int(srcDir.Fd()), path.Base(srcRel), int(dstDir.Fd()), path.Base(dstRel), flags)
	if err != nil {
		if errors.Is(err, unix.EXDEV) {
			return nil, status.Errorf(codes.Internal, "move %s to %s: cross-device move is not supported", srcLogical, dstLogical)
		}
		return nil, fileErr("move", srcLogical, err)
	}
	return &nodev1.MoveFileResponse{Source: srcLogical, Destination: dstLogical}, nil
}

// chmodFile sets permission bits (0o7777 at most) on the resolved target as
// the sandbox user. The mode change uses the pinned opened inode, so a racing
// symlink swap can never redirect it, and the kernel enforces ownership.
func (f *fileServer) chmodFile(_ context.Context, p string, mode uint32) (*nodev1.FileInfo, error) {
	if mode > 0o7777 {
		return nil, status.Errorf(codes.InvalidArgument, "chmod %s: mode %#o is above 07777", p, mode)
	}
	_, _, logical, err := f.resolve(p)
	if err != nil {
		return nil, err
	}
	rt, rel, err := f.walk("chmod", p, true, false)
	if err != nil {
		return nil, err
	}
	// O_PATH opens any file type, including a FIFO, without blocking and
	// without requiring read permission.
	file, err := rt.r.OpenFile(rel, unix.O_PATH, 0)
	if err != nil {
		return nil, fileErr("chmod", logical, err)
	}
	defer file.Close()
	// fchmodat2 with AT_EMPTY_PATH changes exactly the pinned inode; there is
	// deliberately no path-based fallback, where a swapped symlink could
	// redirect the operation at another file.
	if err := unix.Fchmodat(int(file.Fd()), "", mode, unix.AT_EMPTY_PATH); err != nil {
		return nil, fileErr("chmod", logical, err)
	}
	entryRT, entryRel, err := f.walk("chmod", p, false, false)
	if err != nil {
		return nil, err
	}
	info, err := f.entryInfo(entryRT, entryRel, logical)
	if err != nil {
		return nil, fileErr("chmod", logical, err)
	}
	return info, nil
}

// resolve maps a path onto fileServer's roots.
func (f *fileServer) resolve(p string) (*root, string, string, error) {
	return resolveIn(f.home, f.tmp, p)
}

// walk resolves one v1 API path to the confinement root that holds it and
// the path relative to that root. Symlinks are resolved component by
// component across both roots: an absolute /home/user or /tmp target is
// rebased onto the matching [os.Root], and traversal stops after
// maxSymlinkHops. followFinal follows a symlink in the last component; when
// it is false the returned rel names the entry itself. allowMissing stops at
// the first absent component and returns the remaining path, for operations
// that create it.
func (f *fileServer) walk(op, p string, followFinal, allowMissing bool) (*root, string, error) {
	rt, rel, _, err := f.resolve(p)
	if err != nil {
		return nil, "", err
	}
	curRT := rt
	curRel := "."
	curCanonical := rt.spec
	queue := splitRel(rel)
	hops := 0
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		nextRel := path.Join(curRel, name)
		fi, err := curRT.r.Lstat(nextRel)
		if err != nil {
			if allowMissing && errors.Is(err, fs.ErrNotExist) {
				rest := append([]string{name}, queue...)
				return curRT, path.Join(append([]string{curRel}, rest...)...), nil
			}
			return nil, "", fileErr(op, p, err)
		}
		if fi.Mode()&fs.ModeSymlink != 0 && (len(queue) > 0 || followFinal) {
			hops++
			if hops > maxSymlinkHops {
				return nil, "", status.Errorf(codes.InvalidArgument, "%s %s: too many levels of symbolic links", op, p)
			}
			target, err := curRT.r.Readlink(nextRel)
			if err != nil {
				return nil, "", fileErr(op, p, err)
			}
			tc := target
			if !path.IsAbs(tc) {
				tc = path.Join(curCanonical, tc)
			}
			trt, trel, _, err := f.resolve(tc)
			if err != nil {
				return nil, "", status.Errorf(codes.InvalidArgument, "%s %s: symlink target %q is outside %s and %s", op, p, tc, homeSpec, tmpSpec)
			}
			queue = append(splitRel(trel), queue...)
			curRT, curRel, curCanonical = trt, ".", trt.spec
			continue
		}
		if len(queue) > 0 && !fi.IsDir() {
			return nil, "", status.Errorf(codes.InvalidArgument, "%s %s: %s is not a directory", op, p, path.Join(curCanonical, name))
		}
		curRel = nextRel
		curCanonical = path.Join(curCanonical, name)
	}
	return curRT, cmp.Or(curRel, "."), nil
}

// entryInfo builds the FileInfo for an entry at rel under rt whose API path
// is canonical. A symlink keeps that path, name and is_symlink; when its
// target resolves under the allowed roots, the target's type, size, mode and
// modification time are reported, otherwise the link's own (type OTHER).
func (f *fileServer) entryInfo(rt *root, rel, canonical string) (*nodev1.FileInfo, error) {
	lfi, err := rt.r.Lstat(rel)
	if err != nil {
		return nil, err
	}
	fi := lfi
	out := &nodev1.FileInfo{
		Path:      canonical,
		Name:      path.Base(canonical),
		IsSymlink: lfi.Mode()&fs.ModeSymlink != 0,
	}
	if out.IsSymlink {
		if target, err := rt.r.Readlink(rel); err == nil {
			tc := target
			if !path.IsAbs(tc) {
				tc = path.Join(path.Dir(canonical), tc)
			}
			if trt, trel, terr := f.walk("stat", tc, true, false); terr == nil {
				if tfi, terr := trt.r.Lstat(trel); terr == nil {
					fi = tfi
				}
			}
		}
	}
	switch {
	case fi.Mode().IsRegular():
		out.Type = nodev1.FileType_FILE_TYPE_REGULAR
	case fi.IsDir():
		out.Type = nodev1.FileType_FILE_TYPE_DIRECTORY
	default:
		out.Type = nodev1.FileType_FILE_TYPE_OTHER
	}
	out.Size = fi.Size()
	out.Mode = unixMode(fi.Mode())
	out.ModifiedAtUnixMs = fi.ModTime().UnixMilli()
	return out, nil
}

// removeTree recursively deletes rel as the sandbox user: every directory it
// enters must be readable for listing and writable for unlinking, and the
// kernel applies the sticky rule to each unlink. Symlinks are unlinked, never
// followed. Cancellation stops between entries; whatever was already deleted
// stays deleted.
func (f *fileServer) removeTree(ctx context.Context, rt *root, rel, p string) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	d, err := rt.r.Open(rel)
	if err != nil {
		return fileErr("delete", p, err)
	}
	fi, err := d.Stat()
	if err != nil {
		d.Close()
		return fileErr("delete", p, err)
	}
	if !fi.IsDir() {
		d.Close()
		return status.Errorf(codes.InvalidArgument, "delete %s: not a directory", p)
	}
	entries, err := d.ReadDir(-1)
	d.Close()
	if err != nil {
		return fileErr("delete", p, err)
	}
	for _, de := range entries {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		childRel := path.Join(rel, de.Name())
		childPath := path.Join(p, de.Name())
		cfi, err := rt.r.Lstat(childRel)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue // removed while walking
			}
			return fileErr("delete", childPath, err)
		}
		if cfi.IsDir() {
			if err := f.removeTree(ctx, rt, childRel, childPath); err != nil {
				return err
			}
			continue
		}
		if err := f.removeEntry(rt, childRel, childPath, "delete"); err != nil {
			return err
		}
	}
	return f.removeEntry(rt, rel, p, "delete")
}

// removeEntry unlinks one directory entry through a parent descriptor the
// sandbox user may search; the unlinkat syscall itself enforces write and
// sticky-bit access.
func (f *fileServer) removeEntry(rt *root, rel, p, op string) error {
	lfi, err := rt.r.Lstat(rel)
	if err != nil {
		return fileErr(op, p, err)
	}
	dir, err := openParent(rt, rel)
	if err != nil {
		return fileErr(op, p, err)
	}
	defer dir.Close()
	var flags int
	if lfi.IsDir() {
		flags = unix.AT_REMOVEDIR
	}
	if err := unix.Unlinkat(int(dir.Fd()), path.Base(rel), flags); err != nil {
		return fileErr(op, p, err)
	}
	return nil
}

// ensureParents creates rel's missing parents as 0755 directories owned by
// the creating user and lets the kernel decide whether each mkdir is allowed.
func (f *fileServer) ensureParents(rt *root, rel, p, op string) error {
	dir := path.Dir(rel)
	cur := "."
	if dir != "." {
		for _, part := range strings.Split(dir, "/") {
			next := path.Join(cur, part)
			if fi, err := rt.r.Stat(next); err == nil {
				if !fi.IsDir() {
					return status.Errorf(codes.InvalidArgument, "%s %s: %s is not a directory", op, p, next)
				}
				cur = next
				continue
			} else if !errors.Is(err, fs.ErrNotExist) {
				return fileErr(op, p, err)
			}
			if err := f.mkdirParentLevel(rt, next); err != nil {
				return fileErr(op, p, err)
			}
			cur = next
		}
	}
	return nil
}

// mkdirParentLevel creates one parent directory with 0755. If a concurrent
// creator already made it, its directory and mode stand: EEXIST is success
// and never chmodded, so a racing mkdir cannot rewrite another request's mode.
func (f *fileServer) mkdirParentLevel(rt *root, rel string) error {
	if err := rt.r.Mkdir(rel, 0o755); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil
		}
		return err
	}
	return rt.r.Chmod(rel, 0o755)
}

// openParent opens the directory containing rel without following the entry
// itself; unlinkat and renameat use the returned descriptor.
func openParent(rt *root, rel string) (*os.File, error) {
	return rt.r.OpenFile(path.Dir(rel), unix.O_PATH|unix.O_DIRECTORY, 0)
}

// splitRel splits a clean path relative to a confinement root into
// components; "." is the root itself.
func splitRel(rel string) []string {
	if rel == "." || rel == "" {
		return nil
	}
	return strings.Split(rel, "/")
}

// isRootPath reports whether canonical names one of the deletable/movable
// roots (which are never allowed).
func isRootPath(canonical string) bool {
	return canonical == homeSpec || canonical == tmpSpec
}

// fileErr maps filesystem errors to the frozen nodev1 codes. Permission
// failures are bad-path answers (the v1 API has no distinct file
// permission status), missing paths are NOT_FOUND and conflicting
// destinations ALREADY_EXISTS.
func fileErr(op, p string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fs.ErrNotExist):
		return status.Errorf(codes.NotFound, "%s %s: no such file or directory", op, p)
	case errors.Is(err, syscall.ENOTEMPTY):
		return status.Errorf(codes.InvalidArgument, "%s %s: directory not empty", op, p)
	case errors.Is(err, fs.ErrExist):
		return status.Errorf(codes.AlreadyExists, "%s %s: exists", op, p)
	case errors.Is(err, fs.ErrPermission), errors.Is(err, syscall.EACCES), errors.Is(err, syscall.EPERM),
		errors.Is(err, syscall.EINVAL), badPathErr(err):
		return status.Errorf(codes.InvalidArgument, "%s %s: %v", op, p, err)
	}
	return status.Errorf(codes.Internal, "%s %s: %v", op, p, err)
}

// badPathErr reports whether err is a client path problem rather than a
// guest-side failure. os.Root symlink-escape rejections arrive as EXDEV from
// openat2 or as its own "path escapes from parent" error when the portable
// fallback runs.
func badPathErr(err error) bool {
	if errors.Is(err, syscall.ENOTDIR) ||
		errors.Is(err, syscall.EISDIR) ||
		errors.Is(err, syscall.EXDEV) ||
		errors.Is(err, syscall.ELOOP) ||
		errors.Is(err, syscall.ENOENT) ||
		errors.Is(err, fs.ErrNotExist) ||
		errors.Is(err, fs.ErrInvalid) {
		return true
	}
	if pe, ok := errors.AsType[*fs.PathError](err); ok {
		return pe.Err != nil && pe.Err.Error() == "path escapes from parent"
	}
	return false
}

// unixMode converts fs.FileMode into the 0o7777 bits nodev1.FileInfo uses.
func unixMode(m fs.FileMode) uint32 {
	u := uint32(m.Perm())
	if m&fs.ModeSetuid != 0 {
		u |= 0o4000
	}
	if m&fs.ModeSetgid != 0 {
		u |= 0o2000
	}
	if m&fs.ModeSticky != 0 {
		u |= 0o1000
	}
	return u
}

// fileMode is unixMode's inverse.
func fileMode(u uint32) fs.FileMode {
	m := fs.FileMode(u & 0o777)
	if u&0o4000 != 0 {
		m |= fs.ModeSetuid
	}
	if u&0o2000 != 0 {
		m |= fs.ModeSetgid
	}
	if u&0o1000 != 0 {
		m |= fs.ModeSticky
	}
	return m
}
