// Package cli implements the bbraft command-line client.
package cli

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

// ArchiveInfo describes the archive written by [WriteArchive].
type ArchiveInfo struct {
	// BaseName is the cleaned base name of the source directory. It is the
	// directory name a push uses when no explicit destination is given.
	BaseName string
	// Files is the number of regular files and symbolic links written.
	// Directory entries are not counted.
	Files int
}

// WriteArchive streams a tar archive of source to out.
//
// source is resolved to an absolute path and must be a directory. When the
// directory lies inside a git work tree, the archive contains the files git
// tracks and the untracked files that git does not ignore; tracked files that
// are missing from the working tree are skipped. Otherwise the archive
// contains every entry below source except entries named ".git".
//
// Archive names are relative to source itself, without a leading directory
// component, so extracting the archive into a destination directory yields
// that directory's contents. Executable bits and symbolic links are
// preserved; symbolic links are never followed. Git path output is
// NUL-delimited, so names containing whitespace or newlines survive. Paths
// that would escape source are rejected.
//
// The archive is written incrementally: file contents are copied straight
// into out and never held in memory as a whole. out is not closed. The
// context cancels the git child process (including its process group) and
// stops the write between entries.
func WriteArchive(ctx context.Context, source string, out io.Writer) (ArchiveInfo, error) {
	if out == nil {
		return ArchiveInfo{}, errors.New("archive: nil writer")
	}
	if source == "" {
		return ArchiveInfo{}, errors.New("archive: empty source")
	}
	if err := ctx.Err(); err != nil {
		return ArchiveInfo{}, fmt.Errorf("archive: %w", err)
	}

	abs, err := filepath.Abs(source)
	if err != nil {
		return ArchiveInfo{}, fmt.Errorf("archive: resolve source %q: %w", source, err)
	}
	info := ArchiveInfo{BaseName: filepath.Base(abs)}

	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return ArchiveInfo{}, fmt.Errorf("archive: resolve source %q: %w", source, err)
	}
	stat, err := os.Stat(resolved)
	if err != nil {
		return ArchiveInfo{}, fmt.Errorf("archive: stat source %q: %w", source, err)
	}
	if !stat.IsDir() {
		return ArchiveInfo{}, fmt.Errorf("archive: source %q is not a directory", source)
	}

	root, err := os.OpenRoot(resolved)
	if err != nil {
		return ArchiveInfo{}, fmt.Errorf("archive: open source %q: %w", source, err)
	}
	defer root.Close()

	inGit, err := insideGitWorkTree(resolved)
	if err != nil {
		return ArchiveInfo{}, err
	}

	tw := tar.NewWriter(out)
	var files int
	if inGit {
		files, err = writeGitEntries(ctx, root, resolved, tw)
		if errors.Is(err, errNotGitRepository) {
			// A stray .git entry that git does not recognize: treat the
			// directory as a plain tree, still excluding .git entries.
			files, err = writeWalkEntries(ctx, root, tw)
		}
	} else {
		files, err = writeWalkEntries(ctx, root, tw)
	}
	if err != nil {
		return ArchiveInfo{}, err
	}
	if err := tw.Close(); err != nil {
		return ArchiveInfo{}, fmt.Errorf("archive: finish tar: %w", err)
	}

	info.Files = files
	return info, nil
}

// insideGitWorkTree reports whether a .git entry exists at dir or one of its
// ancestors. A .git directory and the .git file of a linked worktree both
// count. This check is separate from running git so that a git failure inside
// a repository is reported instead of being mistaken for a plain directory.
func insideGitWorkTree(dir string) (bool, error) {
	for {
		gitPath := filepath.Join(dir, ".git")
		_, err := os.Lstat(gitPath)
		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, fs.ErrNotExist):
		case err != nil:
			return false, fmt.Errorf("archive: inspect %q: %w", gitPath, err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return false, nil
		}
		dir = parent
	}
}

// writeGitEntries writes the tracked and untracked-not-ignored files below
// source. Paths are emitted in a deterministic order.
func writeGitEntries(ctx context.Context, root *os.Root, source string, tw *tar.Writer) (int, error) {
	names, err := gitFileList(ctx, source)
	if err != nil {
		return 0, err
	}
	slices.Sort(names)
	names = slices.Compact(names)

	var files int
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return files, fmt.Errorf("archive: %w", err)
		}
		rel, err := archiveRelPath(name)
		if err != nil {
			return files, err
		}
		added, err := writeRootEntry(ctx, root, rel, tw, false)
		if errors.Is(err, fs.ErrNotExist) {
			// A tracked file deleted from the working tree.
			continue
		}
		if err != nil {
			return files, fmt.Errorf("archive: add %q: %w", rel, err)
		}
		if added {
			files++
		}
	}
	return files, nil
}

// writeWalkEntries writes every entry below source except entries named
// ".git". It walks root's filesystem view, so directory enumeration stays
// confined to the opened root rather than going through host pathnames.
// Symbolic links are recorded as links and never descended.
func writeWalkEntries(ctx context.Context, root *os.Root, tw *tar.Writer) (int, error) {
	var files int
	err := fs.WalkDir(root.FS(), ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if p == "." {
			return nil
		}
		if d.Name() == ".git" {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		added, err := writeRootEntry(ctx, root, p, tw, true)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("archive: add %q: %w", p, err)
		}
		if added {
			files++
		}
		return nil
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return files, fmt.Errorf("archive: %w", ctxErr)
		}
		return files, fmt.Errorf("archive: walk source: %w", err)
	}
	return files, nil
}

// writeRootEntry writes one entry named rel, opening it through root so a
// symbolic link cannot be used to read outside the source tree. It reports
// whether the entry was a regular file or symbolic link worth counting.
func writeRootEntry(ctx context.Context, root *os.Root, rel string, tw *tar.Writer, includeDirs bool) (bool, error) {
	name := filepath.FromSlash(rel)
	info, err := root.Lstat(name)
	if err != nil {
		return false, err
	}

	mode := info.Mode()
	switch {
	case mode.IsDir():
		if !includeDirs {
			return false, nil
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return false, err
		}
		hdr.Name = rel + "/"
		if err := tw.WriteHeader(hdr); err != nil {
			return false, err
		}
		return false, nil

	case mode.IsRegular():
		f, err := root.Open(name)
		if err != nil {
			return false, err
		}
		opened, err := f.Stat()
		if err != nil {
			f.Close()
			return false, err
		}
		if !os.SameFile(info, opened) {
			f.Close()
			return false, errors.New("file changed while archiving")
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			f.Close()
			return false, err
		}
		hdr.Name = rel
		hdr.Size = info.Size()
		if err := tw.WriteHeader(hdr); err != nil {
			f.Close()
			return false, err
		}
		if err := copyFile(ctx, tw, f, info.Size()); err != nil {
			f.Close()
			return false, err
		}
		if err := f.Close(); err != nil {
			return false, err
		}
		return true, nil

	case mode&fs.ModeSymlink != 0:
		target, err := root.Readlink(name)
		if err != nil {
			return false, err
		}
		hdr, err := tar.FileInfoHeader(info, target)
		if err != nil {
			return false, err
		}
		hdr.Name = rel
		if err := tw.WriteHeader(hdr); err != nil {
			return false, err
		}
		return true, nil

	default:
		// Sockets, devices, and named pipes are not part of a project push.
		return false, nil
	}
}

// copyFile copies exactly size bytes from f to w, checking ctx between reads.
// A final read that returns data together with io.EOF is accepted when it
// completes size. A short write without an error is io.ErrShortWrite.
func copyFile(ctx context.Context, w io.Writer, f io.Reader, size int64) error {
	buf := make([]byte, 32<<10)
	remaining := size
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := int64(len(buf))
		if remaining < chunk {
			chunk = remaining
		}
		n, readErr := f.Read(buf[:chunk])
		if n > 0 {
			written, writeErr := w.Write(buf[:n])
			if written < n && writeErr == nil {
				return io.ErrShortWrite
			}
			if writeErr != nil {
				return writeErr
			}
			remaining -= int64(n)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				if remaining == 0 {
					return nil
				}
				return io.ErrUnexpectedEOF
			}
			return readErr
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

// errNotGitRepository marks a git failure that only means the selected
// directory is not inside a work tree, despite a .git entry above it.
var errNotGitRepository = errors.New("not a git repository")

// gitFileList runs git ls-files below source and returns the NUL-separated
// paths. It is an error, not a fallback to a directory walk, when git fails
// inside a work tree. The child runs in its own process group so cancellation
// also terminates git's helpers.
func gitFileList(ctx context.Context, source string) ([]string, error) {
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := exec.CommandContext(childCtx, "git", "--no-optional-locks", "-C", source,
		"ls-files", "-z", "--cached", "--others", "--exclude-standard")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
		return nil
	}
	cmd.WaitDelay = 2 * time.Second
	cmd.Env = archiveGitEnv()

	var stderr archiveStderrBuffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("archive: list git files: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("archive: list git files: %w", err)
	}

	var names []string
	readErr := readNulList(stdout, &names)
	if readErr != nil {
		cancel()
	}
	waitErr := cmd.Wait()

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("archive: list git files: %w", err)
	}
	if readErr != nil {
		return nil, fmt.Errorf("archive: list git files: %w", readErr)
	}
	if waitErr != nil {
		msg := strings.TrimSpace(stderr.String())
		if isNotGitRepository(waitErr, msg) {
			return nil, errNotGitRepository
		}
		if msg != "" {
			return nil, fmt.Errorf("archive: list git files: %w: %s", waitErr, msg)
		}
		return nil, fmt.Errorf("archive: list git files: %w", waitErr)
	}
	return names, nil
}

// archiveStderrLimit is how much child stderr is retained for error messages.
const archiveStderrLimit = 4 << 10

// archiveStderrBuffer accepts every write but keeps at most
// archiveStderrLimit bytes, so a noisy child cannot grow memory while its
// output is being collected.
type archiveStderrBuffer struct {
	buf bytes.Buffer
}

// Write appends up to the remaining limit and reports all bytes accepted.
func (b *archiveStderrBuffer) Write(p []byte) (int, error) {
	if room := archiveStderrLimit - b.buf.Len(); room > 0 {
		b.buf.Write(p[:min(room, len(p))])
	}
	return len(p), nil
}

// String returns the retained stderr bytes.
func (b *archiveStderrBuffer) String() string {
	return b.buf.String()
}

// archiveGitEnv returns the environment for git listing. Inherited variables
// that point git at a different repository, index, or common directory are
// removed so the source directory's own rules apply. The C locale keeps the
// not-a-repository message matchable.
func archiveGitEnv() []string {
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool {
		name, _, _ := strings.Cut(kv, "=")
		switch name {
		case "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR":
			return true
		}
		return false
	})
	return append(env, "LC_ALL=C", "LANGUAGE=C")
}

// isNotGitRepository reports whether a git failure is only the absence of a
// repository, as opposed to a real error in a repository. The message is
// matched with git forced to the C locale.
func isNotGitRepository(err error, message string) bool {
	exitErr, ok := errors.AsType[*exec.ExitError](err)
	if !ok || exitErr.ExitCode() != 128 {
		return false
	}
	return strings.Contains(message, "not a git repository") ||
		strings.Contains(message, "invalid gitfile format")
}

// readNulList appends NUL-terminated names read from r to names.
func readNulList(r io.Reader, names *[]string) error {
	br := bufio.NewReader(r)
	for {
		b, err := br.ReadBytes(0)
		if len(b) > 0 {
			if b[len(b)-1] != 0 {
				return errors.New("git output ended without a NUL separator")
			}
			if name := string(b[:len(b)-1]); name != "" {
				*names = append(*names, name)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// archiveRelPath validates a path emitted by git and returns its cleaned
// slash-separated form without a trailing slash. Traversal, absolute paths,
// empty components, and .git components are rejected.
func archiveRelPath(name string) (string, error) {
	if name == "" {
		return "", errors.New("archive: empty path from git")
	}
	if strings.HasPrefix(name, "/") {
		return "", fmt.Errorf("archive: absolute path %q from git", name)
	}
	rel := strings.TrimSuffix(name, "/")
	if rel == "" {
		return "", fmt.Errorf("archive: invalid path %q from git", name)
	}
	for part := range strings.SplitSeq(rel, "/") {
		switch part {
		case "", ".", "..":
			return "", fmt.Errorf("archive: path %q escapes the source directory", name)
		case ".git":
			return "", fmt.Errorf("archive: path %q names .git", name)
		}
	}
	return rel, nil
}
