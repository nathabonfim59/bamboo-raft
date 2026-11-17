package guest_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// putFile sends header + data chunks of the given size.
func putFile(ctx context.Context, h *harness, path string, mode uint32, data []byte, chunk int) (*nodev1.PutFileResponse, error) {
	st, err := h.c.PutFile(ctx)
	if err != nil {
		return nil, err
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: path, Mode: mode},
	}}); err != nil {
		return nil, err
	}
	for len(data) > 0 {
		n := min(chunk, len(data))
		if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Data{Data: data[:n]}}); err != nil {
			break // the real error comes from CloseAndRecv
		}
		data = data[n:]
	}
	return st.CloseAndRecv()
}

func TestPutFileBasic(t *testing.T) {
	h := newHarness(t, nil)
	resp, err := putFile(t.Context(), h, "put/dir/hello.txt", 0, []byte("hello file\n"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPath() != "/home/user/put/dir/hello.txt" || resp.GetSize() != 11 {
		t.Fatalf("resp %v", resp)
	}
	got, err := os.ReadFile(filepath.Join(h.home, "put/dir/hello.txt"))
	if err != nil || string(got) != "hello file\n" {
		t.Fatalf("file %q, err %v", got, err)
	}
	// Default mode 0644, owned by the configured user; the created parent
	// directories are owned by it too.
	for _, p := range []string{"put/dir/hello.txt", "put/dir", "put"} {
		fi, err := os.Lstat(filepath.Join(h.home, p))
		if err != nil {
			t.Fatal(err)
		}
		if !fi.IsDir() && fi.Mode().Perm() != 0o644 {
			t.Fatalf("%s mode %v", p, fi.Mode().Perm())
		}
		if stat, ok := fi.Sys().(*syscall.Stat_t); ok && stat.Uid != uint32(h.uid) {
			t.Fatalf("%s uid %d", p, stat.Uid)
		}
	}
}

func TestPutFileCustomMode(t *testing.T) {
	h := newHarness(t, nil)
	if _, err := putFile(t.Context(), h, "put/run.sh", 0o755, []byte("#!/bin/sh\necho ran\n"), 1<<20); err != nil {
		t.Fatal(err)
	}
	res := h.exec(t, "./put/run.sh")
	if res.Stdout != "ran\n" {
		t.Fatalf("stdout %q stderr %q exit %v", res.Stdout, res.Stderr, res.Exit)
	}
}

func TestPutFileEmpty(t *testing.T) {
	h := newHarness(t, nil)
	resp, err := putFile(t.Context(), h, "put/empty", 0, nil, 1)
	if err != nil || resp.GetSize() != 0 {
		t.Fatalf("empty file: %v, %v", resp, err)
	}
	fi, err := os.Stat(filepath.Join(h.home, "put/empty"))
	if err != nil || fi.Size() != 0 {
		t.Fatalf("stat: %v %v", fi, err)
	}
}

func TestPutFileLarge(t *testing.T) {
	h := newHarness(t, nil)
	data := bytes.Repeat([]byte("0123456789abcdef"), 1<<20) // 16 MiB
	resp, err := putFile(t.Context(), h, "put/large.bin", 0, data, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetSize() != int64(len(data)) {
		t.Fatalf("size %d", resp.GetSize())
	}
	got, err := os.ReadFile(filepath.Join(h.home, "put/large.bin"))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back: %d bytes, err %v", len(got), err)
	}
}

func TestPutFileAtomicOverwrite(t *testing.T) {
	h := newHarness(t, nil)
	if _, err := putFile(t.Context(), h, "put/f", 0, []byte("old"), 1<<20); err != nil {
		t.Fatal(err)
	}
	if _, err := putFile(t.Context(), h, "put/f", 0, []byte("new content"), 4); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(h.home, "put/f"))
	if err != nil || string(got) != "new content" {
		t.Fatalf("after replace: %q, %v", got, err)
	}
	assertNoTempFiles(t, h.home)
}

// TestPutFileCanceledLeavesOldFile aborts the stream mid-upload: the
// previous file stays intact and the temporary sibling is removed. The
// client-side cancel returns before the server handler has necessarily
// finished its cleanup, so the test first proves the staging file exists,
// then waits (bounded) for the server's cleanup rather than asserting
// immediately.
func TestPutFileCanceledLeavesOldFile(t *testing.T) {
	h := newHarness(t, nil)
	if _, err := putFile(t.Context(), h, "put/f", 0, []byte("old"), 1<<20); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	st, err := h.c.PutFile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "put/f"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Data{Data: []byte("ne")}}); err != nil {
		t.Fatal(err)
	}

	// Prove the server staged the temporary sibling before canceling, so
	// the assertions below really cover the cleanup branch.
	stagingDir := filepath.Join(h.home, "put")
	waitUntil(t, "staging file before cancel", func() bool { return hasTempFile(t, stagingDir) })

	cancel()
	// Do not half-close (CloseSend/CloseAndRecv): that can race the cancel
	// and deliver EOF first, which legitimately commits the data the server
	// already received. Receive the stream status directly instead.
	var resp nodev1.PutFileResponse
	if err := st.RecvMsg(&resp); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled upload returned %v, want Canceled", err)
	}

	// Cancellation completes on the client before the server handler
	// unwinds; wait for the actual bounded cleanup instead of sleeping.
	waitUntil(t, "server staging cleanup after cancel", func() bool { return !hasTempFile(t, stagingDir) })

	got, err := os.ReadFile(filepath.Join(h.home, "put/f"))
	if err != nil || string(got) != "old" {
		t.Fatalf("after cancel: %q, %v", got, err)
	}
	assertNoTempFiles(t, h.home)
}

func TestPutFileMalformedStreams(t *testing.T) {
	h := newHarness(t, nil)

	// First message is data, not a header.
	st, err := h.c.PutFile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Data{Data: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	_, err = st.CloseAndRecv()
	wantCode(t, err, codes.InvalidArgument)

	// No messages at all.
	st, err = h.c.PutFile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.CloseAndRecv()
	wantCode(t, err, codes.InvalidArgument)

	// A second header mid-stream.
	st, err = h.c.PutFile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	hdr := &guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "put/mid"},
	}}
	if err := st.Send(hdr); err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Data{Data: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Send(hdr); err != nil {
		t.Fatal(err)
	}
	_, err = st.CloseAndRecv()
	wantCode(t, err, codes.InvalidArgument)
	if _, err := os.Stat(filepath.Join(h.home, "put/mid")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("malformed upload created a file: %v", err)
	}
	assertNoTempFiles(t, h.home)
}

// TestPutFileOversizeChunk rejects data chunks above 1 MiB.
func TestPutFileOversizeChunk(t *testing.T) {
	h := newHarness(t, nil)
	st, err := h.c.PutFile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "put/big"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Data{
		Data: bytes.Repeat([]byte("x"), 1<<20+1),
	}}); err != nil {
		t.Fatal(err)
	}
	_, err = st.CloseAndRecv()
	wantCode(t, err, codes.InvalidArgument)
	assertNoTempFiles(t, h.home)
}

func TestPutFileBadPaths(t *testing.T) {
	h := newHarness(t, nil)
	for _, p := range []string{"", "/etc/passwd", "../escape", "/home/user/../../etc/x", "/home/userx/f", "/tmp/../etc/passwd", "/home/user", "/tmp"} {
		_, err := putFile(t.Context(), h, p, 0, []byte("x"), 1<<20)
		if statusOf(err) != codes.InvalidArgument {
			t.Errorf("PutFile(%q): %v, want InvalidArgument", p, err)
		}
	}
}

func TestPutFileUnderTmp(t *testing.T) {
	h := newHarness(t, nil)
	resp, err := putFile(t.Context(), h, "/tmp/bbraft-guest-test", 0, []byte("tmp"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPath() != "/tmp/bbraft-guest-test" {
		t.Fatalf("resp %v", resp)
	}
	got, err := os.ReadFile(filepath.Join(h.tmp, "bbraft-guest-test"))
	if err != nil || string(got) != "tmp" {
		t.Fatalf("file %q, err %v", got, err)
	}
}

// TestPutFileSymlinkEscape proves os.Root confines uploads: a directory
// symlink leading out of the root is rejected, while a final-component
// symlink is atomically replaced, never followed.
func TestPutFileSymlinkEscape(t *testing.T) {
	h := newHarness(t, nil)
	escapeTarget := t.TempDir()
	if err := os.Symlink(escapeTarget, h.home+"/esc"); err != nil {
		t.Fatal(err)
	}
	_, err := putFile(t.Context(), h, "esc/pwned", 0, []byte("x"), 1<<20)
	if statusOf(err) != codes.InvalidArgument {
		t.Fatalf("PutFile through escaping dir: %v, want InvalidArgument", err)
	}
	if _, err := os.Stat(filepath.Join(escapeTarget, "pwned")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("file escaped the root: %v", err)
	}
	assertNoTempFiles(t, h.home)

	// A final-component symlink is replaced, not followed.
	if err := os.Symlink(filepath.Join(escapeTarget, "stolen"), h.home+"/lf"); err != nil {
		t.Fatal(err)
	}
	if _, err := putFile(t.Context(), h, "lf", 0, []byte("mine"), 1<<20); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Lstat(filepath.Join(h.home, "lf"))
	if err != nil || fi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("target still a symlink: %v %v", fi, err)
	}
	got, err := os.ReadFile(filepath.Join(h.home, "lf"))
	if err != nil || string(got) != "mine" {
		t.Fatalf("file %q, err %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(escapeTarget, "stolen")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("symlink was followed: %v", err)
	}
}

// TestPutFileParentIsFile rejects an upload whose parent path component
// exists as a file.
func TestPutFileParentIsFile(t *testing.T) {
	h := newHarness(t, nil)
	if _, err := putFile(t.Context(), h, "put", 0, []byte("i am a file"), 1<<20); err != nil {
		t.Fatal(err)
	}
	_, err := putFile(t.Context(), h, "put/inside", 0, []byte("x"), 1<<20)
	if statusOf(err) != codes.InvalidArgument {
		t.Fatalf("PutFile under a file: %v, want InvalidArgument", err)
	}
}

// assertNoTempFiles fails if any PutFile staging file was left behind.
func assertNoTempFiles(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(d.Name(), ".bbraft-put.") {
			return errors.New("leftover temporary file " + d.Name())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func statusOf(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	return status.Code(err)
}

// TestPutFileModeBits rejects modes with bits beyond the permission bits
// instead of silently masking them.
func TestPutFileModeBits(t *testing.T) {
	h := newHarness(t, nil)
	for _, m := range []uint32{0o1777, 0o4755, 0o100644, 0xFFFFFFFF} {
		_, err := putFile(t.Context(), h, "put/m", m, []byte("x"), 1<<20)
		if statusOf(err) != codes.InvalidArgument {
			t.Errorf("PutFile mode %#o: %v, want InvalidArgument", m, err)
		}
	}
	if _, err := putFile(t.Context(), h, "put/m", 0o777, []byte("x"), 1<<20); err != nil {
		t.Fatalf("mode 0777 rejected: %v", err)
	}
	fi, err := os.Stat(filepath.Join(h.home, "put/m"))
	if err != nil || fi.Mode().Perm() != 0o777 {
		t.Fatalf("mode: %v %v", fi, err)
	}
}

// TestPutFileEmptyMessageRejectsUnknownOneof: a message with no part set
// is malformed, not an empty chunk.
func TestPutFileEmptyMessageRejectsUnknownOneof(t *testing.T) {
	h := newHarness(t, nil)
	st, err := h.c.PutFile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "put/x"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{}); err != nil { // no oneof set
		t.Fatal(err)
	}
	_, err = st.CloseAndRecv()
	wantCode(t, err, codes.InvalidArgument)
	if _, err := os.Stat(filepath.Join(h.home, "put/x")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("malformed upload created a file: %v", err)
	}
	assertNoTempFiles(t, h.home)
}

// flipContext reports no error on the first Err call — the post-EOF check —
// and context.Canceled afterwards, fixing the race where a cancel lands
// between EOF and the rename.
type flipContext struct {
	context.Context
	calls int
}

func (c *flipContext) Err() error {
	c.calls++
	if c.calls == 1 {
		return nil
	}
	return context.Canceled
}

// commitBoundaryStream feeds PutFile a complete stream through a context
// that is canceled exactly at the commit boundary.
type commitBoundaryStream struct {
	grpc.ServerStream
	ctx  context.Context
	msgs []*guestv1.PutFileRequest
}

func (s *commitBoundaryStream) Context() context.Context { return s.ctx }

func (s *commitBoundaryStream) Recv() (*guestv1.PutFileRequest, error) {
	if len(s.msgs) == 0 {
		return nil, io.EOF
	}
	m := s.msgs[0]
	s.msgs = s.msgs[1:]
	return m, nil
}

func (s *commitBoundaryStream) SendAndClose(*nodev1.PutFileResponse) error { return nil }

// TestPutFileCancelAtCommitBoundary: a cancel that arrives after the last
// chunk and after the post-EOF check, while chmod/fsync runs, must still
// leave the previous file intact and no staging debris.
func TestPutFileCancelAtCommitBoundary(t *testing.T) {
	h := newHarness(t, nil)
	if _, err := putFile(t.Context(), h, "put/f", 0, []byte("old"), 1<<20); err != nil {
		t.Fatal(err)
	}
	st := &commitBoundaryStream{
		ctx: &flipContext{Context: t.Context()},
		msgs: []*guestv1.PutFileRequest{
			{Part: &guestv1.PutFileRequest_Header_{
				Header: &guestv1.PutFileRequest_Header{Path: "put/f"},
			}},
			{Part: &guestv1.PutFileRequest_Data{Data: []byte("new")}},
		},
	}
	err := h.svc.PutFile(st)
	wantCode(t, err, codes.Canceled)
	got, readErr := os.ReadFile(filepath.Join(h.home, "put/f"))
	if readErr != nil || string(got) != "old" {
		t.Fatalf("after cancel at commit: %q, %v", got, readErr)
	}
	assertNoTempFiles(t, h.home)
}

// TestPutFileRootLevel: a file directly under the confinement root commits
// normally, so the parent-directory sync covers "." too.
func TestPutFileRootLevel(t *testing.T) {
	h := newHarness(t, nil)
	resp, err := putFile(t.Context(), h, "root-level.txt", 0, []byte("root"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPath() != "/home/user/root-level.txt" || resp.GetSize() != 4 {
		t.Fatalf("resp %v", resp)
	}
	got, err := os.ReadFile(filepath.Join(h.home, "root-level.txt"))
	if err != nil || string(got) != "root" {
		t.Fatalf("file %q, err %v", got, err)
	}
}

// TestPutFileRootLevelSyncsRoot proves the parent-directory sync runs for a
// root-level file and syncs the confinement root itself.
func TestPutFileRootLevelSyncsRoot(t *testing.T) {
	var synced []os.FileInfo
	h := newHarness(t, func(cfg *guest.Config) {
		cfg.SyncDir = func(d *os.File) error {
			fi, err := d.Stat()
			if err != nil {
				return err
			}
			synced = append(synced, fi)
			return d.Sync()
		}
	})
	if _, err := putFile(t.Context(), h, "root-sync.txt", 0, []byte("x"), 4); err != nil {
		t.Fatal(err)
	}
	if len(synced) != 1 {
		t.Fatalf("parent sync calls = %d, want 1", len(synced))
	}
	rootInfo, err := os.Stat(h.home)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(synced[0], rootInfo) {
		t.Fatalf("synced %q, want the root directory", synced[0].Name())
	}
}

// TestPutFileParentSyncFailureReported: a parent-directory sync failure is
// returned as an error rather than logged and swallowed.
func TestPutFileParentSyncFailureReported(t *testing.T) {
	h := newHarness(t, func(cfg *guest.Config) {
		cfg.SyncDir = func(*os.File) error { return errors.New("sync boom") }
	})
	_, err := putFile(t.Context(), h, "root-fail.txt", 0, []byte("x"), 4)
	wantCode(t, err, codes.Internal)
	assertNoTempFiles(t, h.home)
	if _, statErr := os.Stat(filepath.Join(h.home, "root-fail.txt")); statErr != nil {
		t.Fatalf("renamed file missing: %v", statErr)
	}
}

// TestPutFileSubdirSyncFailureReported: the same truthfulness for a nested
// parent.
func TestPutFileSubdirSyncFailureReported(t *testing.T) {
	h := newHarness(t, func(cfg *guest.Config) {
		cfg.SyncDir = func(*os.File) error { return errors.New("sync boom") }
	})
	_, err := putFile(t.Context(), h, "deep/dir/file.txt", 0, []byte("x"), 4)
	wantCode(t, err, codes.Internal)
	assertNoTempFiles(t, h.home)
}

// stagedUploadStream feeds PutFile from a channel, so a test can hold the
// handler after staging began and then cancel it.
type stagedUploadStream struct {
	grpc.ServerStream
	ctx  context.Context
	msgs chan *guestv1.PutFileRequest
}

func (s *stagedUploadStream) Context() context.Context { return s.ctx }

func (s *stagedUploadStream) Recv() (*guestv1.PutFileRequest, error) {
	select {
	case m, ok := <-s.msgs:
		if !ok {
			return nil, io.EOF
		}
		return m, nil
	case <-s.ctx.Done():
		// A failing test may never close the channel; returning on the
		// context keeps the handler from leaking while parked in Recv.
		return nil, s.ctx.Err()
	}
}

func (s *stagedUploadStream) SendAndClose(*nodev1.PutFileResponse) error { return nil }

// waitUntil polls cond for up to five seconds, naming the condition.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// hasTempFile reports whether a PutFile staging sibling exists under root.
func hasTempFile(t *testing.T, root string) bool {
	t.Helper()
	found := false
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(d.Name(), ".bbraft-put.") {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// TestPutFileStagedUploadSurvivesShutdown: an upload parked mid-stream
// when a successful Shutdown closes the service roots must still clean up
// its staging sibling through the root it borrowed for the upload, leaving
// the old target intact and no debris.
func TestPutFileStagedUploadSurvivesShutdown(t *testing.T) {
	h := newHarness(t, nil)
	if _, err := putFile(t.Context(), h, "put/f", 0, []byte("old"), 1<<20); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	st := &stagedUploadStream{ctx: ctx, msgs: make(chan *guestv1.PutFileRequest, 2)}
	uploadDone := make(chan error, 1)
	go func() { uploadDone <- h.svc.PutFile(st) }()
	st.msgs <- &guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "put/f"},
	}}
	st.msgs <- &guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Data{Data: []byte("new")}}

	waitFor(t, 3*time.Second, func() bool { return hasTempFile(t, filepath.Join(h.home, "put")) })
	if err := h.svc.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown while an upload is staged: %v", err)
	}
	cancel()
	close(st.msgs)
	if err := <-uploadDone; status.Code(err) != codes.Canceled {
		t.Fatalf("staged upload returned %v, want Canceled", err)
	}
	got, err := os.ReadFile(filepath.Join(h.home, "put/f"))
	if err != nil || string(got) != "old" {
		t.Fatalf("after staged cancel: %q, %v", got, err)
	}
	assertNoTempFiles(t, h.home)
}

// TestPutFilePinnedRootSurvivesRenameAndSymlink proves the upload borrows
// its confined root from the inode the service pinned at startup: after the
// configured path is renamed away and replaced by a symlink to an external
// directory, staging, shutdown and cleanup all stay on the original root
// and never touch the external target.
func TestPutFilePinnedRootSurvivesRenameAndSymlink(t *testing.T) {
	h := newHarness(t, nil)
	if _, err := putFile(t.Context(), h, "put/f", 0, []byte("old"), 1<<20); err != nil {
		t.Fatal(err)
	}
	moved := h.home + "-moved"
	if err := os.Rename(h.home, moved); err != nil {
		t.Fatal(err)
	}
	external := t.TempDir()
	if err := os.Symlink(external, h.home); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	st := &stagedUploadStream{ctx: ctx, msgs: make(chan *guestv1.PutFileRequest, 2)}
	uploadDone := make(chan error, 1)
	go func() { uploadDone <- h.svc.PutFile(st) }()
	st.msgs <- &guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "put/f"},
	}}
	st.msgs <- &guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Data{Data: []byte("new")}}

	// Staging must appear on the original inode, not through the symlink.
	deadline := time.Now().Add(3 * time.Second)
	for !hasTempFile(t, filepath.Join(moved, "put")) {
		if time.Now().After(deadline) {
			entries, _ := os.ReadDir(external)
			t.Fatalf("staging never appeared on the pinned root; external entries=%v", entries)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if entries, err := os.ReadDir(external); err != nil || len(entries) != 0 {
		t.Fatalf("upload followed the swapped configured path: entries=%v err=%v", entries, err)
	}

	if err := h.svc.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown while an upload is staged: %v", err)
	}
	cancel()
	close(st.msgs)
	if err := <-uploadDone; status.Code(err) != codes.Canceled {
		t.Fatalf("staged upload returned %v, want Canceled", err)
	}
	got, err := os.ReadFile(filepath.Join(moved, "put/f"))
	if err != nil || string(got) != "old" {
		t.Fatalf("after staged cancel: %q, %v", got, err)
	}
	assertNoTempFiles(t, moved)
	if entries, err := os.ReadDir(external); err != nil || len(entries) != 0 {
		t.Fatalf("external target was written: entries=%v err=%v", entries, err)
	}
}
