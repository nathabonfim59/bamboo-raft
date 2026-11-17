package cli

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// archiveTarEntry is one logical archive entry as read back by a tar.Reader.
type archiveTarEntry struct {
	Type     byte
	Mode     int64
	Linkname string
	Body     []byte
}

// archiveFixture writes source into a buffer and returns the parsed entries.
func archiveFixture(t *testing.T, source string) (ArchiveInfo, map[string]archiveTarEntry, int) {
	t.Helper()
	var buf bytes.Buffer
	info, err := WriteArchive(t.Context(), source, &buf)
	if err != nil {
		t.Fatalf("WriteArchive(%q): %v", source, err)
	}
	return info, archiveReadTar(t, buf.Bytes()), buf.Len()
}

// archiveReadTar parses a tar stream into logical entries keyed by name.
func archiveReadTar(t *testing.T, data []byte) map[string]archiveTarEntry {
	t.Helper()
	entries := make(map[string]archiveTarEntry)
	tr := tar.NewReader(bytes.NewReader(data))
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return entries
		}
		if err != nil {
			t.Fatalf("read tar: %v", err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read tar entry %q: %v", hdr.Name, err)
		}
		entries[hdr.Name] = archiveTarEntry{
			Type:     hdr.Typeflag,
			Mode:     hdr.Mode & 0o777,
			Linkname: hdr.Linkname,
			Body:     body,
		}
	}
}

// archiveWriteFile creates parent directories and writes content to name.
func archiveWriteFile(t *testing.T, name, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatalf("create parent of %q: %v", name, err)
	}
	if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
		t.Fatalf("write %q: %v", name, err)
	}
}

// archiveRequireGit skips a test when git is not installed.
func archiveRequireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
}

// archiveGitOutput runs git in dir and returns its trimmed combined output.
func archiveGitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git -C %s %v: %v\n%s", dir, args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// archiveInitRepo creates an empty git repository with a local test identity.
func archiveInitRepo(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create repo dir: %v", err)
	}
	archiveGitOutput(t, dir, "init", "-q")
	archiveGitOutput(t, dir, "config", "user.name", "bbraft test")
	archiveGitOutput(t, dir, "config", "user.email", "test@example.invalid")
	archiveGitOutput(t, dir, "config", "commit.gpgsign", "false")
}

// archiveCommitAll stages every change and commits it.
func archiveCommitAll(t *testing.T, dir, message string) {
	t.Helper()
	archiveGitOutput(t, dir, "add", "-A")
	archiveGitOutput(t, dir, "commit", "-q", "-m", message)
}

// archiveFakeGit puts a shell script named git first on PATH for the test.
func archiveFakeGit(t *testing.T, script string) string {
	t.Helper()
	dir := t.TempDir()
	name := filepath.Join(dir, "git")
	if err := os.WriteFile(name, []byte("#!/bin/sh\n"+script+"\n"), 0o755); err != nil {
		t.Fatalf("write fake git: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return dir
}

func TestWriteArchiveOutsideGit(t *testing.T) {
	source := filepath.Join(t.TempDir(), "project")
	outside := t.TempDir()
	archiveWriteFile(t, filepath.Join(source, "plain.txt"), "plain")
	archiveWriteFile(t, filepath.Join(source, "exec.sh"), "#!/bin/sh\necho hi\n")
	if err := os.Chmod(filepath.Join(source, "exec.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	archiveWriteFile(t, filepath.Join(source, "dir", "nested.txt"), "nested")
	archiveWriteFile(t, filepath.Join(source, "space name.txt"), "space")
	archiveWriteFile(t, filepath.Join(source, "line\nbreak.txt"), "newline")
	archiveWriteFile(t, filepath.Join(source, "héllo.txt"), "unicode")
	archiveWriteFile(t, filepath.Join(source, "-dash.txt"), "dash")
	archiveWriteFile(t, filepath.Join(source, strings.Repeat("long", 40)+".txt"), "long")
	if err := os.Mkdir(filepath.Join(source, "empty"), 0o750); err != nil {
		t.Fatalf("mkdir empty: %v", err)
	}
	archiveWriteFile(t, filepath.Join(source, "linktarget.txt"), "target")
	if err := os.Symlink("linktarget.txt", filepath.Join(source, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := os.Symlink("dir", filepath.Join(source, "dirlink")); err != nil {
		t.Fatalf("symlink dir: %v", err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(source, "escape")); err != nil {
		t.Fatalf("symlink escape: %v", err)
	}
	archiveWriteFile(t, filepath.Join(outside, "secret.txt"), "outside")

	// Entries named .git are never archived, in any form.
	archiveWriteFile(t, filepath.Join(source, ".git", "config"), "gitdir")
	archiveWriteFile(t, filepath.Join(source, "nested", ".git", "config"), "nested gitdir")
	archiveWriteFile(t, filepath.Join(source, "withfile", ".git"), "gitfile")
	archiveWriteFile(t, filepath.Join(source, "withfile", "kept.txt"), "kept")

	info, entries, _ := archiveFixture(t, source)
	if info.BaseName != "project" {
		t.Errorf("BaseName = %q, want project", info.BaseName)
	}

	for _, name := range []string{
		"plain.txt", "exec.sh", "dir/nested.txt", "dir/", "space name.txt",
		"line\nbreak.txt", "héllo.txt", "-dash.txt", "empty/",
		"linktarget.txt", "link", "dirlink", "escape", "withfile/kept.txt",
		strings.Repeat("long", 40) + ".txt",
	} {
		if _, ok := entries[name]; !ok {
			t.Errorf("missing entry %q; got %v", name, slices.Sorted(maps.Keys(entries)))
		}
	}
	for name := range entries {
		if strings.HasPrefix(name, ".git") || strings.Contains(name, "/.git") {
			t.Errorf("archived .git entry %q", name)
		}
		if strings.HasPrefix(name, "dirlink/") {
			t.Errorf("followed directory symlink into %q", name)
		}
		if name == "withfile/.git" {
			t.Errorf("archived .git file entry")
		}
	}

	if got := entries["exec.sh"].Mode; got != 0o755 {
		t.Errorf("exec.sh mode = %#o, want 0755", got)
	}
	if e := entries["link"]; e.Type != tar.TypeSymlink || e.Linkname != "linktarget.txt" {
		t.Errorf("link = type %c target %q, want symlink -> linktarget.txt", e.Type, e.Linkname)
	}
	if e := entries["escape"]; e.Type != tar.TypeSymlink || e.Linkname != filepath.Join(outside, "secret.txt") {
		t.Errorf("escape = type %c target %q, want preserved absolute symlink", e.Type, e.Linkname)
	}
	if e := entries["empty/"]; e.Type != tar.TypeDir || e.Mode != 0o750 {
		t.Errorf("empty/ = type %c mode %#o, want dir 0750", e.Type, e.Mode)
	}
	if got := string(entries["line\nbreak.txt"].Body); got != "newline" {
		t.Errorf("newline filename body = %q", got)
	}
	if got := string(entries["dir/nested.txt"].Body); got != "nested" {
		t.Errorf("nested body = %q", got)
	}

	// Files counts regular files and symlinks, not directories.
	wantFiles := 0
	for _, e := range entries {
		if e.Type == tar.TypeReg || e.Type == tar.TypeSymlink {
			wantFiles++
		}
	}
	if info.Files != wantFiles {
		t.Errorf("Files = %d, want %d", info.Files, wantFiles)
	}
}

func TestWriteArchiveGitRepository(t *testing.T) {
	archiveRequireGit(t)
	repo := filepath.Join(t.TempDir(), "repo")
	archiveInitRepo(t, repo)

	archiveWriteFile(t, filepath.Join(repo, ".gitignore"), "ignored.txt\nignored-untracked.txt\nbuild/\n")
	archiveWriteFile(t, filepath.Join(repo, "tracked.txt"), "tracked")
	archiveWriteFile(t, filepath.Join(repo, "sub", "nested.txt"), "nested")
	archiveWriteFile(t, filepath.Join(repo, "exec.sh"), "#!/bin/sh\n")
	if err := os.Chmod(filepath.Join(repo, "exec.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	archiveWriteFile(t, filepath.Join(repo, "space name.txt"), "space")
	archiveWriteFile(t, filepath.Join(repo, "line\nbreak.txt"), "newline")
	archiveWriteFile(t, filepath.Join(repo, "héllo.txt"), "unicode")
	archiveWriteFile(t, filepath.Join(repo, "target.txt"), "target")
	if err := os.Symlink("target.txt", filepath.Join(repo, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	archiveWriteFile(t, filepath.Join(repo, "ignored.txt"), "tracked ignored")
	archiveWriteFile(t, filepath.Join(repo, "gone.txt"), "missing later")

	archiveCommitAll(t, repo, "initial")
	// Force an ignored file into the index: tracked ignored files are pushed.
	archiveGitOutput(t, repo, "add", "-f", "ignored.txt")
	archiveGitOutput(t, repo, "commit", "-q", "-m", "force ignored")
	// A tracked file deleted in the working tree is skipped, not an error.
	if err := os.Remove(filepath.Join(repo, "gone.txt")); err != nil {
		t.Fatalf("remove tracked file: %v", err)
	}

	// Untracked, ignored, and ignored build output stay out of the archive.
	archiveWriteFile(t, filepath.Join(repo, "new.txt"), "untracked")
	archiveWriteFile(t, filepath.Join(repo, "ignored-untracked.txt"), "ignored")
	archiveWriteFile(t, filepath.Join(repo, "build", "out.bin"), "build")
	// An untracked nested repository is not descended into.
	archiveInitRepo(t, filepath.Join(repo, "vendor", "repo"))
	archiveWriteFile(t, filepath.Join(repo, "vendor", "repo", "x.txt"), "nested repo")

	info, entries, _ := archiveFixture(t, repo)
	if info.BaseName != "repo" {
		t.Errorf("BaseName = %q, want repo", info.BaseName)
	}

	want := map[string]bool{
		".gitignore":      true,
		"tracked.txt":     true,
		"sub/nested.txt":  true,
		"exec.sh":         true,
		"space name.txt":  true,
		"line\nbreak.txt": true,
		"héllo.txt":       true,
		"target.txt":      true,
		"link":            true,
		"ignored.txt":     true,
		"new.txt":         true,
	}
	for name := range want {
		if _, ok := entries[name]; !ok {
			t.Errorf("missing git entry %q; got %v", name, slices.Sorted(maps.Keys(entries)))
		}
	}
	for name := range entries {
		if want[name] {
			continue
		}
		t.Errorf("unexpected git entry %q", name)
	}
	if _, ok := entries["gone.txt"]; ok {
		t.Errorf("missing tracked file was archived")
	}
	if _, ok := entries["ignored-untracked.txt"]; ok {
		t.Errorf("ignored untracked file was archived")
	}
	if _, ok := entries["build/out.bin"]; ok {
		t.Errorf("ignored build output was archived")
	}
	if _, ok := entries["vendor/repo/x.txt"]; ok {
		t.Errorf("nested repository was archived")
	}
	if e := entries["exec.sh"]; e.Mode != 0o755 {
		t.Errorf("exec.sh mode = %#o, want 0755", e.Mode)
	}
	if e := entries["link"]; e.Type != tar.TypeSymlink || e.Linkname != "target.txt" {
		t.Errorf("link = type %c target %q, want symlink -> target.txt", e.Type, e.Linkname)
	}
	if info.Files != len(want) {
		t.Errorf("Files = %d, want %d", info.Files, len(want))
	}
}

func TestWriteArchiveGitSubdirectory(t *testing.T) {
	archiveRequireGit(t)
	repo := filepath.Join(t.TempDir(), "repo")
	archiveInitRepo(t, repo)
	archiveWriteFile(t, filepath.Join(repo, "root.txt"), "root")
	archiveWriteFile(t, filepath.Join(repo, "sub", "in.txt"), "in")
	archiveWriteFile(t, filepath.Join(repo, "sub", "deep", "x.txt"), "deep")
	archiveWriteFile(t, filepath.Join(repo, "sub", "skip.txt"), "skip")
	archiveWriteFile(t, filepath.Join(repo, "sub", ".gitignore"), "skip.txt\n")
	archiveCommitAll(t, repo, "initial")

	sub := filepath.Join(repo, "sub")
	info, entries, _ := archiveFixture(t, sub)
	if info.BaseName != "sub" {
		t.Errorf("BaseName = %q, want sub", info.BaseName)
	}
	if _, ok := entries["in.txt"]; !ok {
		t.Errorf("missing in.txt; got %v", slices.Sorted(maps.Keys(entries)))
	}
	if _, ok := entries["deep/x.txt"]; !ok {
		t.Errorf("missing deep/x.txt; got %v", slices.Sorted(maps.Keys(entries)))
	}
	if _, ok := entries["root.txt"]; ok {
		t.Errorf("archived file outside the selected subdirectory")
	}
	if _, ok := entries["skip.txt"]; ok {
		t.Errorf("archived ignored file")
	}
}

func TestWriteArchiveLinkedWorktree(t *testing.T) {
	archiveRequireGit(t)
	base := t.TempDir()
	repo := filepath.Join(base, "repo")
	archiveInitRepo(t, repo)
	archiveWriteFile(t, filepath.Join(repo, "file.txt"), "file")
	archiveCommitAll(t, repo, "initial")

	worktree := filepath.Join(base, "linked")
	archiveGitOutput(t, repo, "worktree", "add", "-q", "--detach", worktree, "HEAD")
	archiveWriteFile(t, filepath.Join(worktree, "untracked.txt"), "new")

	stat, err := os.Lstat(filepath.Join(worktree, ".git"))
	if err != nil {
		t.Fatalf("lstat worktree .git: %v", err)
	}
	if stat.IsDir() {
		t.Fatalf("linked worktree .git is a directory, want a file")
	}

	info, entries, _ := archiveFixture(t, worktree)
	if info.BaseName != "linked" {
		t.Errorf("BaseName = %q, want linked", info.BaseName)
	}
	for _, name := range []string{"file.txt", "untracked.txt"} {
		if _, ok := entries[name]; !ok {
			t.Errorf("missing worktree entry %q; got %v", name, slices.Sorted(maps.Keys(entries)))
		}
	}
	if _, ok := entries[".git"]; ok {
		t.Errorf("archived the worktree .git file")
	}
}

func TestWriteArchiveGitRejectsTraversal(t *testing.T) {
	archiveRequireGit(t)
	source := filepath.Join(t.TempDir(), "repo")
	archiveInitRepo(t, source)
	archiveWriteFile(t, filepath.Join(source, "ok.txt"), "ok")
	archiveCommitAll(t, source, "initial")
	archiveFakeGit(t, `printf '%s\0' '../escape'`)

	var buf bytes.Buffer
	_, err := WriteArchive(t.Context(), source, &buf)
	if err == nil {
		t.Fatalf("WriteArchive accepted a traversal path")
	}
	if !strings.Contains(err.Error(), "escapes the source directory") {
		t.Errorf("error = %v, want traversal rejection", err)
	}
}

func TestWriteArchiveGitRejectsSymlinkEscape(t *testing.T) {
	archiveRequireGit(t)
	source := filepath.Join(t.TempDir(), "repo")
	archiveInitRepo(t, source)
	outside := t.TempDir()
	archiveWriteFile(t, filepath.Join(outside, "secret.txt"), "outside")
	if err := os.Symlink(outside, filepath.Join(source, "escape")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	archiveCommitAll(t, source, "initial")
	archiveFakeGit(t, `printf '%s\0' 'escape/secret.txt'`)

	var buf bytes.Buffer
	_, err := WriteArchive(t.Context(), source, &buf)
	if err == nil {
		t.Fatalf("WriteArchive followed a symlink outside the source directory")
	}
	if buf.Len() != 0 {
		t.Errorf("wrote %d bytes after a symlink escape", buf.Len())
	}
}

func TestWriteArchiveGitFailureIsAnError(t *testing.T) {
	archiveRequireGit(t)
	source := filepath.Join(t.TempDir(), "repo")
	archiveInitRepo(t, source)
	archiveWriteFile(t, filepath.Join(source, "file.txt"), "file")
	archiveCommitAll(t, source, "initial")
	archiveFakeGit(t, `echo 'fatal: simulated failure' >&2; exit 1`)

	var buf bytes.Buffer
	_, err := WriteArchive(t.Context(), source, &buf)
	if err == nil {
		t.Fatalf("WriteArchive ignored a git failure")
	}
	if !strings.Contains(err.Error(), "simulated failure") {
		t.Errorf("error = %v, want git stderr", err)
	}
	if buf.Len() != 0 {
		t.Errorf("wrote %d bytes after a git failure", buf.Len())
	}
}

// archiveFailWriter fails every write after limit bytes have been accepted.
type archiveFailWriter struct {
	limit int
	err   error
}

func (w *archiveFailWriter) Write(p []byte) (int, error) {
	if w.limit <= 0 {
		return 0, w.err
	}
	if len(p) > w.limit {
		n := w.limit
		w.limit = 0
		return n, w.err
	}
	w.limit -= len(p)
	return len(p), nil
}

func TestWriteArchiveWriterFailure(t *testing.T) {
	source := t.TempDir()
	archiveWriteFile(t, filepath.Join(source, "file.txt"), strings.Repeat("data", 1024))
	boom := errors.New("writer failed")
	w := &archiveFailWriter{limit: 700, err: boom}
	_, err := WriteArchive(t.Context(), source, w)
	if !errors.Is(err, boom) {
		t.Fatalf("WriteArchive error = %v, want %v", err, boom)
	}
}

func TestWriteArchiveLargeFile(t *testing.T) {
	source := t.TempDir()
	content := bytes.Repeat([]byte("0123456789abcdef"), (5<<20+123)/16+1)[:5<<20+123]
	if err := os.WriteFile(filepath.Join(source, "large.bin"), content, 0o644); err != nil {
		t.Fatalf("write large file: %v", err)
	}
	info, entries, size := archiveFixture(t, source)
	if info.Files != 1 {
		t.Errorf("Files = %d, want 1", info.Files)
	}
	if size <= 5<<20 {
		t.Errorf("archive size = %d, want more than 5 MiB", size)
	}
	got, ok := entries["large.bin"]
	if !ok {
		t.Fatalf("missing large.bin; got %v", slices.Sorted(maps.Keys(entries)))
	}
	if !bytes.Equal(got.Body, content) {
		t.Errorf("large file body differs (%d vs %d bytes)", len(got.Body), len(content))
	}
}

func TestWriteArchiveContextCancelsGitProcessGroup(t *testing.T) {
	archiveRequireGit(t)
	pidDir := t.TempDir()
	source := filepath.Join(t.TempDir(), "repo")
	archiveInitRepo(t, source)
	archiveWriteFile(t, filepath.Join(source, "file.txt"), "file")
	archiveCommitAll(t, source, "initial")
	archiveFakeGit(t, "sleep 300 &\necho $! > \""+pidDir+"/child.pid\"\necho $$ > \""+pidDir+"/parent.pid\"\nwait")

	ctx, cancel := context.WithCancel(t.Context())
	var buf bytes.Buffer
	result := make(chan error, 1)
	go func() {
		_, err := WriteArchive(ctx, source, &buf)
		result <- err
	}()

	parentPID := archiveWaitForPID(t, filepath.Join(pidDir, "parent.pid"))
	childPID := archiveWaitForPID(t, filepath.Join(pidDir, "child.pid"))
	t.Cleanup(func() {
		for _, pid := range []int{childPID, parentPID} {
			if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
				t.Logf("cleanup kill %d: %v", pid, err)
			}
		}
	})

	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WriteArchive error = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("WriteArchive did not return after cancellation")
	}

	for _, tc := range []struct {
		name string
		pid  int
	}{
		{"git process", parentPID},
		{"git child", childPID},
	} {
		if archiveWaitForProcessExit(tc.pid, 5*time.Second) {
			continue
		}
		t.Errorf("%s %d is still running after cancellation", tc.name, tc.pid)
	}
	if buf.Len() != 0 {
		t.Errorf("wrote %d bytes for a canceled git listing", buf.Len())
	}
}

// archiveWaitForPID polls a pid file written by the fake git script.
func archiveWaitForPID(t *testing.T, name string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, err := os.ReadFile(name)
		if err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
				return pid
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// archiveWaitForProcessExit reports whether pid stops existing within timeout.
func archiveWaitForProcessExit(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// archiveEOFFinalReader returns its data and io.EOF from the same read when
// the remaining data fits in the buffer.
type archiveEOFFinalReader struct {
	data []byte
}

func (r *archiveEOFFinalReader) Read(p []byte) (int, error) {
	if len(r.data) <= len(p) {
		n := copy(p, r.data)
		r.data = nil
		return n, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

// archiveShortWriter reports one byte fewer than offered, with no error.
type archiveShortWriter struct{}

func (archiveShortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

func TestCopyFileFinalEOFCompletesSize(t *testing.T) {
	data := []byte("abcdef")
	var out bytes.Buffer
	if err := copyFile(t.Context(), &out, &archiveEOFFinalReader{data: data}, int64(len(data))); err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("copied %q, want %q", out.Bytes(), data)
	}
}

func TestCopyFileShortWrite(t *testing.T) {
	err := copyFile(t.Context(), archiveShortWriter{}, strings.NewReader("abc"), 3)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("copyFile error = %v, want io.ErrShortWrite", err)
	}
}

func TestCopyFileShortSource(t *testing.T) {
	err := copyFile(t.Context(), io.Discard, strings.NewReader("ab"), 3)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("copyFile error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestCopyFileCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := copyFile(ctx, io.Discard, strings.NewReader("abc"), 3)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("copyFile error = %v, want context.Canceled", err)
	}
}

func TestArchiveStderrBufferCapsRetainedBytes(t *testing.T) {
	var buf archiveStderrBuffer
	data := bytes.Repeat([]byte("x"), 3*archiveStderrLimit)
	n, err := buf.Write(data)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(data) {
		t.Errorf("Write accepted %d bytes, want %d", n, len(data))
	}
	if got := buf.buf.Len(); got != archiveStderrLimit {
		t.Errorf("retained %d bytes, want %d", got, archiveStderrLimit)
	}
	if got := len(buf.String()); got != archiveStderrLimit {
		t.Errorf("String length = %d, want %d", got, archiveStderrLimit)
	}
}

func TestWriteArchiveGitLargeStderr(t *testing.T) {
	archiveRequireGit(t)
	source := filepath.Join(t.TempDir(), "repo")
	archiveInitRepo(t, source)
	archiveWriteFile(t, filepath.Join(source, "file.txt"), "file")
	archiveCommitAll(t, source, "initial")
	archiveFakeGit(t, `yes E | head -c 100000 >&2; exit 1`)

	var out bytes.Buffer
	_, err := WriteArchive(t.Context(), source, &out)
	if err == nil {
		t.Fatalf("WriteArchive ignored a git failure with large stderr")
	}
	msg := err.Error()
	if !strings.Contains(msg, "exit status 1") {
		t.Errorf("error = %v, want git exit status", msg)
	}
	if wantMax := archiveStderrLimit + 64; len(msg) > wantMax {
		t.Errorf("error message retained %d bytes, want at most %d", len(msg), wantMax)
	}
}

func TestWriteArchiveIgnoresInheritedGitEnv(t *testing.T) {
	archiveRequireGit(t)
	base := t.TempDir()
	repoA := filepath.Join(base, "a")
	repoB := filepath.Join(base, "b")
	archiveInitRepo(t, repoA)
	archiveWriteFile(t, filepath.Join(repoA, "a.txt"), "a")
	archiveCommitAll(t, repoA, "initial")
	archiveInitRepo(t, repoB)
	archiveWriteFile(t, filepath.Join(repoB, "b.txt"), "b")
	archiveCommitAll(t, repoB, "initial")

	t.Setenv("GIT_DIR", filepath.Join(repoB, ".git"))
	t.Setenv("GIT_WORK_TREE", repoB)
	t.Setenv("GIT_INDEX_FILE", filepath.Join(repoB, ".git", "index"))
	t.Setenv("GIT_COMMON_DIR", filepath.Join(repoB, ".git"))

	_, entries, _ := archiveFixture(t, repoA)
	if _, ok := entries["a.txt"]; !ok {
		t.Errorf("missing a.txt from the selected repository; got %v", slices.Sorted(maps.Keys(entries)))
	}
	if _, ok := entries["b.txt"]; ok {
		t.Errorf("inherited GIT_* variables selected another repository")
	}
}

func TestWriteArchiveSourceErrors(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file.txt")
	archiveWriteFile(t, file, "not a directory")
	tests := []struct {
		name   string
		source string
	}{
		{"empty", ""},
		{"missing", filepath.Join(t.TempDir(), "missing")},
		{"file", file},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if _, err := WriteArchive(t.Context(), tc.source, &buf); err == nil {
				t.Fatalf("WriteArchive(%q) succeeded, want error", tc.source)
			}
		})
	}
}

func TestArchiveRelPath(t *testing.T) {
	valid := map[string]string{
		"a":               "a",
		"dir/file":        "dir/file",
		"space name.txt":  "space name.txt",
		"line\nbreak.txt": "line\nbreak.txt",
		"héllo.txt":       "héllo.txt",
		"-dash.txt":       "-dash.txt",
		"back\\slash":     "back\\slash",
		"nested/repo/":    "nested/repo",
	}
	for in, want := range valid {
		got, err := archiveRelPath(in)
		if err != nil {
			t.Errorf("archiveRelPath(%q) error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("archiveRelPath(%q) = %q, want %q", in, got, want)
		}
	}

	invalid := []string{
		"", "/absolute", "../escape", "a/../b", "./a", ".", "..", "a//b",
		"a/.git/b", ".git", ".git/config",
	}
	for _, in := range invalid {
		if got, err := archiveRelPath(in); err == nil {
			t.Errorf("archiveRelPath(%q) = %q, want error", in, got)
		}
	}
}
