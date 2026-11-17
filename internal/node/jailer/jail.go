package jailer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"slices"
	"strings"
	"syscall"
)

// jailOp names a jail builder operation for the per-Manager fault seam. Each
// hook runs right after the operation it names has happened on disk.
type jailOp string

const (
	opMkdirIdentity  jailOp = "mkdir-identity"  // directory created, not yet identified
	opCreateIdentity jailOp = "create-identity" // file created and open, not yet identified
	opWrite          jailOp = "write"           // file recorded, before its content is written
	opChmod          jailOp = "chmod"           // content written, before chmod
	opSync           jailOp = "sync"            // chmod done, before fsync
	opClose          jailOp = "close"           // fsync done, the file is closed with the error
	opBarrier        jailOp = "barrier"         // before one parent directory fsync
	opDirMode        jailOp = "dir-mode"        // directory recorded, before its exact mode is set
)

// copyChunk bounds how much runtime data is copied between cancellation
// checks.
const copyChunk = 1 << 20

// jailBuilder creates entries below one attempt-owned jail base. Each new
// directory or file is recorded by inode the moment it is identified, through
// record, so a failed population removes exactly what this attempt made and
// never anything that was already there. Every entry's parent directory is
// queued for an fsync barrier.
type jailBuilder struct {
	root    *os.Root
	base    string
	record  func(ownedEntry)
	fault   func(jailOp, string) error
	dirs    map[string]struct{}
	pending map[string]struct{} // parent dirs (relative to base) to fsync
}

func newJailBuilder(root *os.Root, base string, record func(ownedEntry), fault func(jailOp, string) error) *jailBuilder {
	return &jailBuilder{
		root: root, base: base, record: record, fault: fault,
		dirs: make(map[string]struct{}), pending: make(map[string]struct{}),
	}
}

func (b *jailBuilder) inject(op jailOp, rel string) error {
	if b.fault == nil {
		return nil
	}
	if err := b.fault(op, rel); err != nil {
		return fmt.Errorf("jailer: jail %s %s: %w", op, rel, err)
	}
	return nil
}

// mkdir creates rel (relative to the base) exclusively and records it.
func (b *jailBuilder) mkdir(rel string, perm os.FileMode) error {
	name := path.Join(b.base, rel)
	if err := b.root.Mkdir(name, perm); err != nil {
		return fmt.Errorf("jailer: create jail %s: %w", rel, err)
	}
	b.pending[path.Dir(rel)] = struct{}{}
	// Until it is identified, the new directory cannot be proven ours: a
	// failure here leaves it in place and the reservation retained.
	if err := b.inject(opMkdirIdentity, rel); err != nil {
		return err
	}
	identity, err := statDirectoryNoFollow(b.root, name)
	if err != nil {
		return fmt.Errorf("jailer: identify jail %s: %w", rel, err)
	}
	b.record(ownedEntry{Path: rel, Dev: identity.Dev, Ino: identity.Ino, Dir: true})
	b.dirs[rel] = struct{}{}
	// Mkdir's mode is narrowed by the process umask (the unit's UMask=0077
	// leaves 0700, which locks the dropped jail uid out of its own chroot).
	// The directory is recorded first, so a failure here leaves it owned
	// and removed by the attempt's rollback.
	if err := b.inject(opDirMode, rel); err != nil {
		return err
	}
	if err := chmodCreatedDir(b.root, name, identity, perm); err != nil {
		return fmt.Errorf("jailer: set the mode of jail %s: %w", rel, err)
	}
	return nil
}

// chmodCreatedDir sets exactly perm on the directory created as want. It is
// opened through openNoFollow (lstat before and after the open, all three the
// same inode, never a symlink), and both the descriptor and the name must
// still be the recorded inode; a replacement is refused and left untouched.
// The resulting mode is read back from the descriptor.
func chmodCreatedDir(root *os.Root, name string, want inodeIdentity, perm os.FileMode) error {
	d, err := openNoFollow(root, name, os.O_RDONLY, true)
	if err != nil {
		return err
	}
	defer d.Close()
	got, err := fileInode(d)
	if err != nil {
		return err
	}
	if !got.equal(want) {
		return fmt.Errorf("%s is inode %d:%d, not the created %d:%d; the replacement is left as it is", name, got.Dev, got.Ino, want.Dev, want.Ino)
	}
	if err := d.Chmod(perm); err != nil {
		return err
	}
	info, err := d.Stat()
	if err != nil {
		return err
	}
	if info.Mode()&(os.ModePerm|os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != perm {
		return fmt.Errorf("%s has mode %v after chmod, want %v", name, info.Mode(), perm)
	}
	named, err := statDirectoryNoFollow(root, name)
	if err != nil {
		return err
	}
	if !named.equal(want) {
		return fmt.Errorf("%s was replaced while its mode was set", name)
	}
	return nil
}

// mkdirAll creates every missing parent of rel that this builder has not
// already made. A pre-existing directory it did not create is an error.
func (b *jailBuilder) mkdirAll(rel string) error {
	var current string
	for part := range strings.SplitSeq(rel, "/") {
		current = path.Join(current, part)
		if _, ok := b.dirs[current]; ok {
			continue
		}
		if err := b.mkdir(current, 0o755); err != nil {
			return err
		}
	}
	return nil
}

// create makes a new regular file exclusively and records it.
func (b *jailBuilder) create(rel string, perm os.FileMode) (*os.File, inodeIdentity, error) {
	name := path.Join(b.base, rel)
	f, err := b.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, perm)
	if err != nil {
		return nil, inodeIdentity{}, fmt.Errorf("jailer: create jail %s: %w", rel, err)
	}
	b.pending[path.Dir(rel)] = struct{}{}
	identity, err := fileInode(f)
	if faultErr := b.inject(opCreateIdentity, rel); faultErr != nil {
		err = faultErr
	}
	if err != nil {
		f.Close()
		return nil, inodeIdentity{}, fmt.Errorf("jailer: identify jail %s: %w", rel, err)
	}
	b.record(ownedEntry{Path: rel, Dev: identity.Dev, Ino: identity.Ino})
	return f, identity, nil
}

// copyFile writes size bytes from in into a new read-only (0555) file,
// checking ctx between chunks.
func (b *jailBuilder) copyFile(ctx context.Context, rel string, in io.Reader, size uint64) (inodeIdentity, error) {
	if err := b.mkdirAll(path.Dir(rel)); err != nil {
		return inodeIdentity{}, err
	}
	out, identity, err := b.create(rel, 0o600)
	if err != nil {
		return inodeIdentity{}, err
	}
	err = b.inject(opWrite, rel)
	if err == nil {
		var written int64
		written, err = copyContext(ctx, out, io.LimitReader(in, int64(size)+1))
		if err == nil && uint64(written) != size {
			err = fmt.Errorf("size changed: got %d, want %d", written, size)
		}
	}
	if err == nil {
		if err = b.inject(opChmod, rel); err == nil {
			err = out.Chmod(0o555)
		}
	}
	if err == nil {
		if err = b.inject(opSync, rel); err == nil {
			err = out.Sync()
		}
	}
	closeErr := out.Close()
	if err == nil {
		if err = b.inject(opClose, rel); err == nil {
			err = closeErr
		}
	}
	if err != nil {
		return inodeIdentity{}, fmt.Errorf("jailer: stage %s: %w", rel, err)
	}
	return identity, nil
}

// copyContext copies in chunks and stops at the first chunk boundary after
// ctx is done.
func copyContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, context.Cause(ctx)
		}
		n, err := io.CopyN(dst, src, copyChunk)
		total += n
		if errors.Is(err, io.EOF) {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}

// barrier fsyncs every directory that gained an entry, deepest first. A
// directory leaves the pending set only after its fsync succeeds, so a
// failed barrier can be retried and syncs exactly what is still owed.
func (b *jailBuilder) barrier() error {
	dirs := slices.Collect(maps.Keys(b.pending))
	depth := func(dir string) int {
		if dir == "." { // the jail base itself, synced last
			return -1
		}
		return strings.Count(dir, "/")
	}
	slices.SortFunc(dirs, func(a, c string) int {
		if da, dc := depth(a), depth(c); da != dc {
			return dc - da
		}
		return strings.Compare(a, c)
	})
	var errs []error
	for _, dir := range dirs {
		err := b.inject(opBarrier, dir)
		if err == nil {
			err = syncDirectory(b.root, path.Join(b.base, dir))
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		delete(b.pending, dir)
	}
	return errors.Join(errs...)
}

// verify proves the jail holds exactly the recorded entries: nothing foreign
// appeared while it was populated.
func (b *jailBuilder) verify(ctx context.Context, owned []ownedEntry) error {
	actual, err := jailInventory(ctx, b.root, b.base, false, 0)
	if err != nil {
		return err
	}
	want := slices.Clone(owned)
	slices.SortFunc(want, func(a, b ownedEntry) int { return strings.Compare(a.Path, b.Path) })
	if !slices.Equal(actual, want) {
		return errors.New("jailer: jail inventory differs from the attempt's own entries")
	}
	return nil
}

// populateJail creates the fixed jail skeleton and stages the runtime. It
// returns the staged QEMU binary's inode for the exec proof.
func populateJail(ctx context.Context, b *jailBuilder, files []runtimeFile) (inodeIdentity, error) {
	if err := b.mkdir("root", 0o755); err != nil {
		return inodeIdentity{}, err
	}
	for _, dir := range []string{"root/proc", "root/control", "root/dev", "root/tmp"} {
		if err := b.mkdir(dir, 0o755); err != nil {
			return inodeIdentity{}, err
		}
	}
	serial, _, err := b.create("root/serial.log", 0o600)
	if err != nil {
		return inodeIdentity{}, err
	}
	if err := serial.Close(); err != nil {
		return inodeIdentity{}, fmt.Errorf("jailer: close serial mount point: %w", err)
	}
	return copyRuntime(ctx, b, files)
}
