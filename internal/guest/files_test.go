package guest_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/nodev1"
)

// fileWrite creates a file under dir with the given content and mode.
func fileWrite(t *testing.T, dir, name, content string, mode fs.FileMode) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		t.Fatal(err)
	}
	return p
}

// fileModeOf is the 0o7777 mode of p, following symlinks.
func fileModeOf(t *testing.T, p string) uint32 {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return unixModeOf(fi.Mode())
}

// unixModeOf mirrors the guest's FileInfo mode conversion.
func unixModeOf(m fs.FileMode) uint32 {
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

// fileStatT is the raw stat of p (lstat, not following a final symlink).
func fileStatT(t *testing.T, p string) *syscall.Stat_t {
	t.Helper()
	fi, err := os.Lstat(p)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("%s has no syscall.Stat_t", p)
	}
	return st
}

// fileReadFile collects one ReadFile stream, enforcing the frame contract:
// one info first, then data chunks of at most 1 MiB.
func fileReadFile(t *testing.T, h *harness, p string, maxBytes int64) (*nodev1.FileInfo, []byte, int, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	st, err := h.c.ReadFile(ctx, &guestv1.ReadFileRequest{Path: p, MaxBytes: maxBytes})
	if err != nil {
		return nil, nil, 0, err
	}
	var fi *nodev1.FileInfo
	var data []byte
	chunks := 0
	for {
		msg, err := st.Recv()
		if errors.Is(err, io.EOF) {
			return fi, data, chunks, nil
		}
		if err != nil {
			return fi, data, chunks, err
		}
		if fi == nil && chunks > 0 {
			t.Error("ReadFile sent data before info")
		}
		switch part := msg.GetPart().(type) {
		case *nodev1.ReadFileResponse_Info:
			if fi != nil {
				t.Error("ReadFile sent a second info message")
			}
			fi = part.Info
		case *nodev1.ReadFileResponse_Data:
			if fi == nil {
				t.Error("ReadFile data before info")
			}
			if len(part.Data) > 1<<20 {
				t.Errorf("ReadFile chunk of %d bytes is above 1 MiB", len(part.Data))
			}
			chunks++
			data = append(data, part.Data...)
		}
	}
}

func TestReadFileInfoFirstAndBoundedChunks(t *testing.T) {
	h := newHarness(t, nil)
	payload := bytes.Repeat([]byte("0123456789abcdef"), (5<<20)/16+3) // 5 MiB + 3: several chunks
	fileWrite(t, h.home, "read/big.bin", string(payload), 0o640)

	fi, data, chunks, err := fileReadFile(t, h, "read/big.bin", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("ReadFile returned %d bytes, want %d", len(data), len(payload))
	}
	if chunks < 5 {
		t.Fatalf("ReadFile used %d chunks for %d bytes, want several", chunks, len(payload))
	}
	if fi.GetPath() != "/home/user/read/big.bin" || fi.GetName() != "big.bin" ||
		fi.GetType() != nodev1.FileType_FILE_TYPE_REGULAR || fi.GetIsSymlink() ||
		fi.GetSize() != int64(len(payload)) || fi.GetMode() != 0o640 || fi.GetModifiedAtUnixMs() == 0 {
		t.Fatalf("ReadFile info = %v", fi)
	}

	// A file above max_bytes fails before the info message.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	st, err := h.c.ReadFile(ctx, &guestv1.ReadFileRequest{Path: "read/big.bin", MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := st.Recv()
	if err == nil {
		t.Fatalf("ReadFile above max_bytes sent %v, want OUT_OF_RANGE", msg)
	}
	wantCode(t, err, codes.OutOfRange)

	// Exactly max_bytes is allowed.
	if _, data, _, err := fileReadFile(t, h, "read/big.bin", int64(len(payload))); err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("ReadFile at max_bytes = %d bytes, %v", len(data), err)
	}
}

func TestReadFileErrors(t *testing.T) {
	h := newHarness(t, nil)
	fileWrite(t, h.home, "err/file", "x", 0o644)
	fileWrite(t, h.home, "err/exact", "12345", 0o644)
	if err := syscall.Mkfifo(filepath.Join(h.home, "err/fifo"), 0o644); err != nil {
		t.Fatal(err)
	}
	sock, err := net.Listen("unix", filepath.Join(h.home, "err/sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sock.Close() })
	for _, tc := range []struct {
		name string
		call func() error
		want codes.Code
	}{
		{"missing", func() error {
			_, _, _, err := fileReadFile(t, h, "err/missing", 0)
			return err
		}, codes.NotFound},
		{"directory", func() error {
			_, _, _, err := fileReadFile(t, h, "err", 0)
			return err
		}, codes.InvalidArgument},
		{"fifo", func() error {
			_, _, _, err := fileReadFile(t, h, "err/fifo", 0)
			return err
		}, codes.InvalidArgument},
		{"socket", func() error {
			_, _, _, err := fileReadFile(t, h, "err/sock", 0)
			return err
		}, codes.InvalidArgument},
		{"outside", func() error {
			_, _, _, err := fileReadFile(t, h, "/etc/passwd", 0)
			return err
		}, codes.InvalidArgument},
		{"dot-dot", func() error {
			_, _, _, err := fileReadFile(t, h, "../../etc/passwd", 0)
			return err
		}, codes.InvalidArgument},
		{"escaping link", func() error {
			if err := os.Symlink("/etc/passwd", filepath.Join(h.home, "err/out")); err != nil {
				return err
			}
			_, _, _, err := fileReadFile(t, h, "err/out", 0)
			return err
		}, codes.InvalidArgument},
		{"dangling link", func() error {
			if err := os.Symlink("nowhere", filepath.Join(h.home, "err/dangling")); err != nil {
				return err
			}
			_, _, _, err := fileReadFile(t, h, "err/dangling", 0)
			return err
		}, codes.NotFound},
		{"symlink loop", func() error {
			if err := os.Symlink("loop-b", filepath.Join(h.home, "err/loop-a")); err != nil {
				return err
			}
			if err := os.Symlink("loop-a", filepath.Join(h.home, "err/loop-b")); err != nil {
				return err
			}
			_, _, _, err := fileReadFile(t, h, "err/loop-a", 0)
			return err
		}, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatalf("succeeded, want %s", tc.want)
			}
			wantCode(t, err, tc.want)
		})
	}
}

// TestReadFileSymlinks covers in-root, absolute cross-root and relative
// cross-root symlinks, which all resolve under the allowed roots, plus the
// entry-versus-target FileInfo split.
func TestReadFileSymlinks(t *testing.T) {
	h := newHarness(t, nil)
	fileWrite(t, h.home, "link/target.txt", "target content", 0o640)
	fileWrite(t, h.tmp, "cross/a.txt", "from tmp", 0o604)
	for _, tc := range []struct {
		name, link, want string
	}{
		{"relative in root", "target.txt", "target content"},
		{"absolute in root", "/home/user/link/target.txt", "target content"},
		{"absolute cross root", "/tmp/cross/a.txt", "from tmp"},
		{"relative cross root", "../../../tmp/cross/a.txt", "from tmp"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lp := filepath.Join(h.home, "link", strings.ReplaceAll(tc.name, " ", "-"))
			if err := os.Symlink(tc.link, lp); err != nil {
				t.Fatal(err)
			}
			rel := strings.TrimPrefix(lp, h.home+"/")
			fi, data, _, err := fileReadFile(t, h, rel, 0)
			if err != nil {
				t.Fatalf("ReadFile symlink: %v", err)
			}
			if string(data) != tc.want {
				t.Fatalf("ReadFile through symlink = %q, want %q", data, tc.want)
			}
			if !fi.GetIsSymlink() || fi.GetPath() != "/home/user/"+rel || fi.GetName() != filepath.Base(rel) {
				t.Fatalf("Link info = %v; want the link path and is_symlink", fi)
			}
			if fi.GetType() != nodev1.FileType_FILE_TYPE_REGULAR || fi.GetSize() != int64(len(tc.want)) {
				t.Fatalf("Link info type/size = %v/%d, want the target's", fi.GetType(), fi.GetSize())
			}
		})
	}
}

func TestStatFileTypes(t *testing.T) {
	h := newHarness(t, nil)
	fileWrite(t, h.home, "stat/f", "four", 0o640)
	if err := os.MkdirAll(filepath.Join(h.home, "stat/d"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/home/user/stat/f", filepath.Join(h.home, "stat/abs")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc/passwd", filepath.Join(h.home, "stat/out")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", filepath.Join(h.home, "stat/dangling")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("loop-b", filepath.Join(h.home, "stat/loop-a")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("loop-a", filepath.Join(h.home, "stat/loop-b")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(h.home, "stat/fifo"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name      string
		path      string
		wantType  nodev1.FileType
		symlink   bool
		wantMode  uint32
		wantSize  int64
		wantError codes.Code
	}{
		{name: "file", path: "stat/f", wantType: nodev1.FileType_FILE_TYPE_REGULAR, wantMode: 0o640, wantSize: 4},
		{name: "dir", path: "stat/d", wantType: nodev1.FileType_FILE_TYPE_DIRECTORY, wantMode: 0o750},
		{name: "fifo", path: "stat/fifo", wantType: nodev1.FileType_FILE_TYPE_OTHER, wantMode: 0o600},
		{name: "absolute link", path: "stat/abs", wantType: nodev1.FileType_FILE_TYPE_REGULAR, symlink: true, wantMode: 0o640, wantSize: 4},
		{name: "escaping link", path: "stat/out", wantType: nodev1.FileType_FILE_TYPE_OTHER, symlink: true, wantSize: int64(len("/etc/passwd"))},
		{name: "dangling link", path: "stat/dangling", wantType: nodev1.FileType_FILE_TYPE_OTHER, symlink: true},
		{name: "loop link", path: "stat/loop-a", wantType: nodev1.FileType_FILE_TYPE_OTHER, symlink: true},
		{name: "missing", path: "stat/missing", wantError: codes.NotFound},
		{name: "outside", path: "/etc/passwd", wantError: codes.InvalidArgument},
		{name: "dot-dot", path: "../../etc", wantError: codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fi, err := h.c.StatFile(t.Context(), &guestv1.StatFileRequest{Path: tc.path})
			if tc.wantError != 0 {
				wantCode(t, err, tc.wantError)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wantPath := tc.path
			if !strings.HasPrefix(wantPath, "/") {
				wantPath = "/home/user/" + wantPath
			}
			if fi.GetPath() != wantPath || fi.GetName() != filepath.Base(wantPath) {
				t.Fatalf("StatFile path/name = %q/%q, want %q", fi.GetPath(), fi.GetName(), wantPath)
			}
			if fi.GetType() != tc.wantType || fi.GetIsSymlink() != tc.symlink {
				t.Fatalf("StatFile type/symlink = %v/%v, want %v/%v", fi.GetType(), fi.GetIsSymlink(), tc.wantType, tc.symlink)
			}
			if tc.wantMode != 0 && fi.GetMode() != tc.wantMode {
				t.Fatalf("StatFile mode = %#o, want %#o", fi.GetMode(), tc.wantMode)
			}
			if tc.wantSize != 0 && fi.GetSize() != tc.wantSize {
				t.Fatalf("StatFile size = %d, want %d", fi.GetSize(), tc.wantSize)
			}
			if fi.GetModifiedAtUnixMs() == 0 {
				t.Fatal("StatFile modification time is zero")
			}
		})
	}

	// The root itself is stat-able; its name is the last path element.
	fi, err := h.c.StatFile(t.Context(), &guestv1.StatFileRequest{Path: "/home/user"})
	if err != nil || fi.GetPath() != "/home/user" || fi.GetName() != "user" || fi.GetType() != nodev1.FileType_FILE_TYPE_DIRECTORY {
		t.Fatalf("stat root = %v, %v", fi, err)
	}
}

func TestListFiles(t *testing.T) {
	h := newHarness(t, nil)
	for _, name := range []string{"c", "b"} {
		fileWrite(t, h.home, "list/"+name, name, 0o644)
	}
	fileWrite(t, h.home, "list/a.txt", "a", 0o600)
	if err := os.MkdirAll(filepath.Join(h.home, "list/x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc/passwd", filepath.Join(h.home, "list/out")); err != nil {
		t.Fatal(err)
	}

	ls, err := h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "list"})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ls.GetEntries() {
		names = append(names, e.GetName())
		if e.GetPath() != "/home/user/list/"+e.GetName() {
			t.Fatalf("entry path %q", e.GetPath())
		}
	}
	if want := []string{"a.txt", "b", "c", "out", "x"}; strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("ListFiles = %v, want %v", names, want)
	}
	if ls.GetTruncated() || ls.GetPath() != "/home/user/list" {
		t.Fatalf("ListFiles truncated/path = %v/%q", ls.GetTruncated(), ls.GetPath())
	}
	if ls.GetEntries()[3].GetType() != nodev1.FileType_FILE_TYPE_OTHER || !ls.GetEntries()[3].GetIsSymlink() {
		t.Fatalf("escaping link entry = %v, want OTHER symlink", ls.GetEntries()[3])
	}
	if ls.GetEntries()[4].GetType() != nodev1.FileType_FILE_TYPE_DIRECTORY || ls.GetEntries()[2].GetSize() != 1 || ls.GetEntries()[2].GetMode() != 0o644 {
		t.Fatalf("entries = %v", ls.GetEntries())
	}

	ls, err = h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "list", Limit: 2})
	if err != nil || len(ls.GetEntries()) != 2 || !ls.GetTruncated() {
		t.Fatalf("ListFiles limit 2 = %v, %v; want 2 entries and truncated", ls, err)
	}

	// Empty path defaults to the home directory.
	fileWrite(t, h.home, "homefile", "h", 0o644)
	ls, err = h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{})
	if err != nil || ls.GetPath() != "/home/user" {
		t.Fatalf("ListFiles default = %v, %v; want the home directory", ls, err)
	}
	if !hasEntry(ls.GetEntries(), "homefile") {
		t.Fatalf("ListFiles default entries = %v, want homefile", ls.GetEntries())
	}

	// A symlink to a directory resolves under the roots: its entries are
	// listed under the link path.
	if err := os.Symlink("/tmp/listtmp", filepath.Join(h.home, "list/absdir")); err != nil {
		t.Fatal(err)
	}
	fileWrite(t, h.tmp, "listtmp/inside", "i", 0o644)
	ls, err = h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "list/absdir"})
	if err != nil || ls.GetPath() != "/home/user/list/absdir" {
		t.Fatalf("ListFiles via link = %v, %v", ls, err)
	}
	if len(ls.GetEntries()) != 1 || ls.GetEntries()[0].GetPath() != "/home/user/list/absdir/inside" {
		t.Fatalf("ListFiles via link entries = %v", ls.GetEntries())
	}

	for _, tc := range []struct {
		name string
		path string
		lim  uint32
		want codes.Code
	}{
		{"file", "list/a.txt", 0, codes.InvalidArgument},
		{"missing", "list/missing", 0, codes.NotFound},
		{"outside", "/var", 0, codes.InvalidArgument},
		{"escaping link", "list/out", 0, codes.InvalidArgument},
		{"limit", "list", 10001, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: tc.path, Limit: tc.lim})
			wantCode(t, err, tc.want)
		})
	}
}

func hasEntry(entries []*nodev1.FileInfo, name string) bool {
	for _, e := range entries {
		if e.GetName() == name {
			return true
		}
	}
	return false
}

func TestDeleteFile(t *testing.T) {
	h := newHarness(t, nil)
	fileWrite(t, h.home, "del/tree/a/b/file", "x", 0o644)
	if err := os.MkdirAll(filepath.Join(h.home, "del/empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	fileWrite(t, h.home, "del/single", "s", 0o644)

	_, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "del/tree"})
	wantCode(t, err, codes.InvalidArgument) // non-empty without recursive

	resp, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "del/single"})
	if err != nil || resp.GetPath() != "/home/user/del/single" {
		t.Fatalf("DeleteFile file = %v, %v", resp, err)
	}
	if _, err := os.Lstat(filepath.Join(h.home, "del/single")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("deleted file still exists: %v", err)
	}
	if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "del/empty"}); err != nil {
		t.Fatalf("DeleteFile empty dir: %v", err)
	}
	if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "del/tree", Recursive: true}); err != nil {
		t.Fatalf("DeleteFile recursive: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(h.home, "del/tree")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("recursive delete left the tree: %v", err)
	}

	for _, tc := range []struct {
		name string
		path string
		rec  bool
		want codes.Code
	}{
		{"home", "/home/user", true, codes.InvalidArgument},
		{"tmp", "/tmp", true, codes.InvalidArgument},
		{"home dot", "/home/user/.", false, codes.InvalidArgument},
		{"missing", "del/missing", false, codes.NotFound},
		{"outside", "/etc/passwd", false, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: tc.path, Recursive: tc.rec})
			wantCode(t, err, tc.want)
		})
	}

	// A symlink is deleted itself; the target (here outside the roots) stays.
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(h.home, "del/out")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "del/out"}); err != nil {
		t.Fatalf("DeleteFile link: %v", err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("deleting a link removed its target: %v", err)
	}
	if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "del/out"}); status.Code(err) != codes.NotFound {
		t.Fatalf("second delete = %v, want NotFound", err)
	}

	// A recursive delete of a symlink to a directory removes the link only.
	fileWrite(t, h.tmp, "deltarget/keep.txt", "keep", 0o644)
	if err := os.Symlink("/tmp/deltarget", filepath.Join(h.home, "del/dirlink")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "del/dirlink", Recursive: true}); err != nil {
		t.Fatalf("DeleteFile dir link: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(h.home, "del/dirlink")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("directory link not removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(h.tmp, "deltarget/keep.txt")); err != nil {
		t.Fatalf("recursive delete followed a directory symlink: %v", err)
	}

	// Cancellation is reported and already-deleted entries stay deleted.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = h.c.DeleteFile(ctx, &guestv1.DeleteFileRequest{Path: "del", Recursive: true})
	wantCode(t, err, codes.Canceled)
}

func TestMakeDir(t *testing.T) {
	h := newHarness(t, nil)
	fi, err := h.c.MakeDir(t.Context(), &guestv1.MakeDirRequest{Path: "mk/a/b", Mode: 0o700})
	if err != nil {
		t.Fatal(err)
	}
	if fi.GetPath() != "/home/user/mk/a/b" || fi.GetType() != nodev1.FileType_FILE_TYPE_DIRECTORY || fi.GetMode() != 0o700 {
		t.Fatalf("MakeDir = %v", fi)
	}
	// Every created level is owned by the sandbox user with a readable mode.
	for _, p := range []string{"mk", "mk/a", "mk/a/b"} {
		st := fileStatT(t, filepath.Join(h.home, p))
		if int(st.Uid) != h.uid || int(st.Gid) != h.gid {
			t.Fatalf("%s owner = %d:%d, want %d:%d", p, st.Uid, st.Gid, h.uid, h.gid)
		}
	}
	if got := fileModeOf(t, filepath.Join(h.home, "mk/a/b")); got != 0o700 {
		t.Fatalf("created mode = %#o, want 0700", got)
	}
	if got := fileModeOf(t, filepath.Join(h.home, "mk/a")); got != 0o755 {
		t.Fatalf("created parent mode = %#o, want 0755", got)
	}

	// An existing directory keeps its mode; the default mode is 0755.
	again, err := h.c.MakeDir(t.Context(), &guestv1.MakeDirRequest{Path: "mk/a/b", Mode: 0o755})
	if err != nil || again.GetMode() != 0o700 {
		t.Fatalf("MakeDir again = %v, %v; want the existing 0700", again, err)
	}
	def, err := h.c.MakeDir(t.Context(), &guestv1.MakeDirRequest{Path: "mk/default"})
	if err != nil || def.GetMode() != 0o755 {
		t.Fatalf("MakeDir default = %v, %v; want 0755", def, err)
	}

	// Special bits are preserved (the guest applies its own umask-free mode).
	special, err := h.c.MakeDir(t.Context(), &guestv1.MakeDirRequest{Path: "mk/special", Mode: 0o2755})
	if err != nil || special.GetMode() != 0o2755 || fileModeOf(t, filepath.Join(h.home, "mk/special")) != 0o2755 {
		t.Fatalf("MakeDir special = %v, %v", special, err)
	}

	// A path whose parent is an absolute symlink into /tmp is created there.
	if err := os.Symlink("/tmp/mkdir-target", filepath.Join(h.home, "mk/link")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.c.MakeDir(t.Context(), &guestv1.MakeDirRequest{Path: "mk/link/deep"}); err != nil {
		t.Fatalf("MakeDir through absolute symlink: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(h.tmp, "mkdir-target/deep")); err != nil || !fi.IsDir() {
		t.Fatalf("directory not created at the symlink target: %v, %v", fi, err)
	}

	for _, tc := range []struct {
		name string
		path string
		mode uint32
		want codes.Code
	}{
		{"existing file", "mk/file", 0, codes.AlreadyExists},
		{"parent is a file", "mk/file/sub", 0, codes.InvalidArgument},
		{"outside", "/var/mk", 0, codes.InvalidArgument},
		{"mode too large", "mk/bit", 0o10000, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "existing file" {
				fileWrite(t, h.home, "mk/file", "f", 0o644)
			}
			_, err := h.c.MakeDir(t.Context(), &guestv1.MakeDirRequest{Path: tc.path, Mode: tc.mode})
			wantCode(t, err, tc.want)
		})
	}
}

func TestMoveFile(t *testing.T) {
	h := newHarness(t, nil)
	fileWrite(t, h.home, "mv/a", "content a", 0o640)
	mv, err := h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "mv/a", Destination: "mv/b"})
	if err != nil || mv.GetSource() != "/home/user/mv/a" || mv.GetDestination() != "/home/user/mv/b" {
		t.Fatalf("MoveFile = %v, %v", mv, err)
	}
	if got, err := os.ReadFile(filepath.Join(h.home, "mv/b")); err != nil || string(got) != "content a" {
		t.Fatalf("moved file = %q, %v", got, err)
	}
	if _, err := os.Lstat(filepath.Join(h.home, "mv/a")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("source still exists: %v", err)
	}

	// Cross-root move with missing parents: they are created for the user.
	fileWrite(t, h.home, "mv/cross", "cross", 0o600)
	if _, err := h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "mv/cross", Destination: "/tmp/mv/deep/cross"}); err != nil {
		t.Fatalf("MoveFile cross root: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(h.tmp, "mv/deep/cross")); err != nil || string(got) != "cross" {
		t.Fatalf("cross-root destination = %q, %v", got, err)
	}
	for _, p := range []string{"mv", "mv/deep"} {
		st := fileStatT(t, filepath.Join(h.tmp, p))
		if int(st.Uid) != h.uid || int(st.Gid) != h.gid {
			t.Fatalf("created parent %s owner = %d:%d, want %d:%d", p, st.Uid, st.Gid, h.uid, h.gid)
		}
	}
	// The moved file keeps its owner across roots.
	if st := fileStatT(t, filepath.Join(h.tmp, "mv/deep/cross")); int(st.Uid) != h.uid {
		t.Fatalf("moved file owner = %d, want %d", st.Uid, h.uid)
	}

	// Destination through an absolute symlink into /tmp.
	fileWrite(t, h.home, "mv/through", "through", 0o644)
	if err := os.Symlink("/tmp/mv-target", filepath.Join(h.home, "mv/link")); err != nil {
		t.Fatal(err)
	}
	if mv, err := h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "mv/through", Destination: "mv/link/x"}); err != nil {
		t.Fatalf("MoveFile through symlink: %v", err)
	} else if mv.GetDestination() != "/home/user/mv/link/x" {
		t.Fatalf("MoveFile logical destination = %q", mv.GetDestination())
	}
	if got, err := os.ReadFile(filepath.Join(h.tmp, "mv-target/x")); err != nil || string(got) != "through" {
		t.Fatalf("destination through symlink = %q, %v", got, err)
	}

	// Conflicts.
	fileWrite(t, h.home, "mv/src", "src", 0o644)
	fileWrite(t, h.home, "mv/dst", "dst", 0o644)
	_, err = h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "mv/src", Destination: "mv/dst"})
	wantCode(t, err, codes.AlreadyExists)
	if _, err := h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "mv/src", Destination: "mv/dst", Overwrite: true}); err != nil {
		t.Fatalf("MoveFile overwrite: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(h.home, "mv/dst")); err != nil || string(got) != "src" {
		t.Fatalf("overwritten destination = %q, %v", got, err)
	}

	// Overwrite an empty directory with a file.
	fileWrite(t, h.home, "mv/fileover", "f", 0o644)
	if err := os.MkdirAll(filepath.Join(h.home, "mv/emptydir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "mv/fileover", Destination: "mv/emptydir", Overwrite: true}); err != nil {
		t.Fatalf("MoveFile over empty dir: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(h.home, "mv/emptydir")); err != nil || !fi.Mode().IsRegular() {
		t.Fatalf("empty dir was not replaced by a file: %v, %v", fi, err)
	}

	// A directory replaces an existing empty directory.
	if err := os.MkdirAll(filepath.Join(h.home, "mv/srcdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(h.home, "mv/dstdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "mv/srcdir", Destination: "mv/dstdir", Overwrite: true}); err != nil {
		t.Fatalf("MoveFile dir over empty dir: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(h.home, "mv/dstdir")); err != nil || !fi.IsDir() {
		t.Fatalf("empty dir replacement = %v, %v", fi, err)
	}

	// Overwrite a non-empty directory is refused, and nothing is lost.
	fileWrite(t, h.home, "mv/full/inner", "keep", 0o644)
	fileWrite(t, h.home, "mv/incoming", "incoming", 0o644)
	_, err = h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "mv/incoming", Destination: "mv/full", Overwrite: true})
	wantCode(t, err, codes.InvalidArgument)
	if _, err := os.Stat(filepath.Join(h.home, "mv/full/inner")); err != nil {
		t.Fatalf("non-empty destination partially deleted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(h.home, "mv/incoming")); err != nil {
		t.Fatalf("source lost after refused overwrite: %v", err)
	}

	// A directory over an existing file is a type conflict.
	if err := os.MkdirAll(filepath.Join(h.home, "mv/dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err = h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "mv/dir", Destination: "mv/dst", Overwrite: true})
	wantCode(t, err, codes.InvalidArgument)
	if fi, err := os.Stat(filepath.Join(h.home, "mv/dir")); err != nil || !fi.IsDir() {
		t.Fatalf("source directory lost after type conflict: %v, %v", fi, err)
	}

	// A symlink is moved itself, never its target, even across roots.
	fileWrite(t, h.home, "mv/real", "real", 0o644)
	if err := os.Symlink("/etc/passwd", filepath.Join(h.home, "mv/evil")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "mv/evil", Destination: "/tmp/mv/evil"}); err != nil {
		t.Fatalf("MoveFile symlink: %v", err)
	}
	if target, err := os.Readlink(filepath.Join(h.tmp, "mv/evil")); err != nil || target != "/etc/passwd" {
		t.Fatalf("moved symlink = %q, %v; want it unchanged", target, err)
	}
	if _, err := os.Stat("/etc/passwd"); err != nil {
		t.Fatalf("symlink target touched: %v", err)
	}

	for _, tc := range []struct {
		name, src, dst string
		want           codes.Code
	}{
		{"missing source", "mv/missing", "mv/x", codes.NotFound},
		{"outside dest", "mv/real", "/etc/x", codes.InvalidArgument},
		{"move home", "/home/user", "/tmp/home", codes.InvalidArgument},
		{"move tmp", "/tmp/mv", "/home/user/mv", codes.InvalidArgument},
		{"dot-dot dest", "mv/real", "../../etc/x", codes.InvalidArgument},
		{"dir into itself", "mv/dir", "mv/dir/sub", codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: tc.src, Destination: tc.dst, Overwrite: true})
			wantCode(t, err, tc.want)
		})
	}
}

// TestReadFileThroughDirectorySymlink reads through an absolute symlink to a
// directory in the other confinement root: every component is rebound to the
// /tmp os.Root before the file is opened.
func TestReadFileThroughDirectorySymlink(t *testing.T) {
	h := newHarness(t, nil)
	fileWrite(t, h.tmp, "dirlink/inner.txt", "through the link", 0o640)
	if err := os.Symlink("/tmp/dirlink", filepath.Join(h.home, "absdir")); err != nil {
		t.Fatal(err)
	}
	fi, data, _, err := fileReadFile(t, h, "absdir/inner.txt", 0)
	if err != nil || string(data) != "through the link" {
		t.Fatalf("ReadFile through directory link = %q, %v", data, err)
	}
	if fi.GetPath() != "/home/user/absdir/inner.txt" || fi.GetName() != "inner.txt" || fi.GetIsSymlink() ||
		fi.GetType() != nodev1.FileType_FILE_TYPE_REGULAR {
		t.Fatalf("ReadFile through directory link info = %v", fi)
	}
	// The entry itself is followed like any other path component.
	if _, err := h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "absdir/inner.txt", Mode: 0o600}); err != nil {
		t.Fatalf("ChmodFile through directory link: %v", err)
	}
	if mode := fileModeOf(t, filepath.Join(h.tmp, "dirlink/inner.txt")); mode != 0o600 {
		t.Fatalf("chmod through directory link = %#o, want 0600", mode)
	}
	// Deleting through the link removes the real entry, not the link.
	if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "absdir/inner.txt"}); err != nil {
		t.Fatalf("DeleteFile through directory link: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(h.tmp, "dirlink/inner.txt")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("delete through directory link left the real entry: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(h.home, "absdir")); err != nil {
		t.Fatalf("delete through directory link removed the link: %v", err)
	}
}

// TestMakeDirConcurrent proves idempotent creation when several callers race:
// one creates, the others see the directory and keep its mode.
func TestMakeDirConcurrent(t *testing.T) {
	h := newHarness(t, nil)
	const callers = 8
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			_, errs[i] = h.c.MakeDir(t.Context(), &guestv1.MakeDirRequest{Path: "concurrent/a/b", Mode: 0o700})
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("MakeDir caller %d: %v", i, err)
		}
	}
	if got := fileModeOf(t, filepath.Join(h.home, "concurrent/a/b")); got != 0o700 {
		t.Fatalf("concurrent MakeDir mode = %#o, want 0700", got)
	}
}

func TestChmodFile(t *testing.T) {
	h := newHarness(t, nil)
	p := fileWrite(t, h.home, "chmod/f", "x", 0o600)
	fi, err := h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "chmod/f", Mode: 0o640})
	if err != nil || fi.GetMode() != 0o640 || fileModeOf(t, p) != 0o640 {
		t.Fatalf("ChmodFile = %v, %v; want 0640 on disk", fi, err)
	}
	// Special bits are set: setuid, setgid and sticky.
	fi, err = h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "chmod/f", Mode: 0o7777})
	if err != nil || fi.GetMode() != 0o7777 {
		t.Fatalf("ChmodFile special = %v, %v", fi, err)
	}
	stat := fileStatT(t, p)
	if stat.Mode&0o7000 != 0o7000 {
		t.Fatalf("special bits on disk = %#o, want 07000", stat.Mode&0o7000)
	}
	// A mode-0000 file can still be chmodded by its owner (O_PATH open).
	if _, err := h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "chmod/f", Mode: 0}); err != nil {
		t.Fatalf("ChmodFile to 0000: %v", err)
	}
	if _, err := h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "chmod/f", Mode: 0o600}); err != nil {
		t.Fatalf("ChmodFile after 0000: %v", err)
	}
	// Directories and symlinks (following to the in-root target).
	if err := os.MkdirAll(filepath.Join(h.home, "chmod/d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if fi, err := h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "chmod/d", Mode: 0o1777}); err != nil || fi.GetMode() != 0o1777 {
		t.Fatalf("ChmodFile dir = %v, %v", fi, err)
	}
	if err := os.Symlink("/home/user/chmod/f", filepath.Join(h.home, "chmod/link")); err != nil {
		t.Fatal(err)
	}
	fi, err = h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "chmod/link", Mode: 0o604})
	if err != nil || fi.GetMode() != 0o604 || !fi.GetIsSymlink() || fi.GetPath() != "/home/user/chmod/link" {
		t.Fatalf("ChmodFile symlink = %v, %v; want the link path with target mode", fi, err)
	}
	if mode := fileModeOf(t, p); mode != 0o604 {
		t.Fatalf("chmod through symlink changed target to %#o, want 0604", mode)
	}
	if target, err := os.Readlink(filepath.Join(h.home, "chmod/link")); err != nil || target != "/home/user/chmod/f" {
		t.Fatalf("chmod replaced the symlink: %q, %v", target, err)
	}
	// Absolute cross-root target.
	fileWrite(t, h.tmp, "chmod/cross", "c", 0o644)
	if err := os.Symlink("/tmp/chmod/cross", filepath.Join(h.home, "chmod/crosslink")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "chmod/crosslink", Mode: 0o600}); err != nil {
		t.Fatalf("ChmodFile cross-root link: %v", err)
	}
	if mode := fileModeOf(t, filepath.Join(h.tmp, "chmod/cross")); mode != 0o600 {
		t.Fatalf("cross-root target mode = %#o, want 0600", mode)
	}

	// The escaping link never changed /etc/passwd's mode.
	before, err := os.Stat("/etc/passwd")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		path string
		mode uint32
		want codes.Code
	}{
		{"missing", "chmod/missing", 0o644, codes.NotFound},
		{"outside", "/etc/passwd", 0o644, codes.InvalidArgument},
		{"mode too large", "chmod/f", 0o10000, codes.InvalidArgument},
		{"escaping link", "chmod/out", 0o644, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "escaping link" {
				if err := os.Symlink("/etc/passwd", filepath.Join(h.home, "chmod/out")); err != nil {
					t.Fatal(err)
				}
			}
			_, err := h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: tc.path, Mode: tc.mode})
			wantCode(t, err, tc.want)
		})
	}
	after, err := os.Stat("/etc/passwd")
	if err != nil {
		t.Fatal(err)
	}
	if after.Mode() != before.Mode() {
		t.Fatalf("/etc/passwd mode changed from %v to %v", before.Mode(), after.Mode())
	}
}

// TestFilesWithPutFile keeps the upload API and the file API consistent.
func TestFilesWithPutFile(t *testing.T) {
	h := newHarness(t, nil)
	if _, err := putFile(t.Context(), h, "interop/uploaded.txt", 0o640, []byte("uploaded body"), 4); err != nil {
		t.Fatal(err)
	}
	fi, data, _, err := fileReadFile(t, h, "interop/uploaded.txt", 0)
	if err != nil || string(data) != "uploaded body" {
		t.Fatalf("ReadFile uploaded = %q, %v", data, err)
	}
	if fi.GetMode() != 0o640 || fi.GetIsSymlink() {
		t.Fatalf("uploaded info = %v", fi)
	}
	if st := fileStatT(t, filepath.Join(h.home, "interop/uploaded.txt")); int(st.Uid) != h.uid {
		t.Fatalf("uploaded owner = %d, want %d", st.Uid, h.uid)
	}
	ls, err := h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "interop"})
	if err != nil || len(ls.GetEntries()) != 1 || ls.GetEntries()[0].GetName() != "uploaded.txt" {
		t.Fatalf("ListFiles uploaded = %v, %v", ls, err)
	}
	if _, err := h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "interop/uploaded.txt", Destination: "/tmp/interop/uploaded.txt"}); err != nil {
		t.Fatalf("MoveFile uploaded: %v", err)
	}
	if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "/tmp/interop", Recursive: true}); err != nil {
		t.Fatalf("DeleteFile uploaded: %v", err)
	}
}

// TestMoveFileCrossDevice proves an EXDEV rename is reported as a guest
// failure, never silently turned into a copy-and-delete. It needs /dev/shm
// on a different filesystem than the test's temp home; it skips otherwise.
func TestMoveFileCrossDevice(t *testing.T) {
	shm, err := os.MkdirTemp("/dev/shm", "bbraft-p1-a-files-")
	if err != nil {
		t.Skipf("/dev/shm is not usable: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(shm) })
	base := t.TempDir()
	homeDev, err := os.Stat(base)
	if err != nil {
		t.Fatal(err)
	}
	shmDev, err := os.Stat(shm)
	if err != nil {
		t.Fatal(err)
	}
	if homeDev.Sys().(*syscall.Stat_t).Dev == shmDev.Sys().(*syscall.Stat_t).Dev {
		t.Skip("/dev/shm and the test temp dir are on the same filesystem; no EXDEV to inject")
	}
	h := newHarness(t, func(c *guest.Config) { c.TmpRoot = shm })
	fileWrite(t, h.home, "exdev/file", "keep me", 0o644)

	_, err = h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "exdev/file", Destination: "/tmp/exdev/file"})
	if err == nil {
		t.Fatal("cross-device move succeeded, want an explicit failure")
	}
	wantCode(t, err, codes.Internal)
	if !strings.Contains(status.Convert(err).Message(), "cross-device") {
		t.Fatalf("cross-device error = %q, want it classified", status.Convert(err).Message())
	}
	if got, err := os.ReadFile(filepath.Join(h.home, "exdev/file")); err != nil || string(got) != "keep me" {
		t.Fatalf("source was deleted or changed after a failed cross-device move: %q, %v", got, err)
	}
	if _, err := os.Lstat(filepath.Join(shm, "exdev/file")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a destination appeared after a failed cross-device move: %v", err)
	}
}

// TestSymlinkReplacementRace swaps a symlink between an in-root target and an
// escaping one while the guest resolves it: every operation either succeeds
// inside the roots or fails, and never touches the escaping target.
func TestSymlinkReplacementRace(t *testing.T) {
	h := newHarness(t, nil)
	fileWrite(t, h.home, "race/inside.txt", "inside only", 0o644)
	fileWrite(t, h.home, "race/other.txt", "other", 0o644)
	if err := os.MkdirAll(filepath.Join(h.home, "race/dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(h.home, "race/link")
	if err := os.Symlink("/home/user/race/inside.txt", link); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		targets := []string{"/home/user/race/inside.txt", "/etc/passwd", "/home/user/race/other.txt"}
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			tmp := fmt.Sprintf("%s.%d", link, i%3)
			os.Remove(tmp)
			if err := os.Symlink(targets[i%len(targets)], tmp); err != nil {
				return
			}
			if err := os.Rename(tmp, link); err != nil {
				return
			}
		}
	}()
	defer func() {
		close(stop)
		<-done
	}()

	for range 100 {
		fi, data, _, err := fileReadFile(t, h, "race/link", 0)
		if err != nil {
			continue // a swap to the escaping target or a vanished entry
		}
		if !fi.GetIsSymlink() {
			t.Fatalf("race info lost is_symlink: %v", fi)
		}
		if strings.Contains(string(data), "root:") {
			t.Fatalf("ReadFile leaked the escaping target: %q", data)
		}
		if s := string(data); s != "inside only" && s != "other" {
			t.Fatalf("ReadFile returned unexpected content %q", s)
		}
	}

	// Chmod races too: every call either fails (escaping target) or changes
	// the in-root target; /etc/passwd's mode is never touched.
	before, err := os.Stat("/etc/passwd")
	if err != nil {
		t.Fatal(err)
	}
	for range 50 {
		_, _ = h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "race/link", Mode: 0o600})
	}
	after, err := os.Stat("/etc/passwd")
	if err != nil {
		t.Fatal(err)
	}
	if after.Mode() != before.Mode() {
		t.Fatalf("chmod race changed /etc/passwd from %v to %v", before.Mode(), after.Mode())
	}
	for _, name := range []string{"inside.txt", "other.txt"} {
		if mode := fileModeOf(t, filepath.Join(h.home, "race", name)); mode != 0o600 && mode != 0o644 {
			t.Fatalf("chmod race set %s to unexpected mode %#o", name, mode)
		}
	}
}

// TestRecursiveDeleteChecksPermissions proves the recursive walk applies the
// sandbox user's permissions per directory instead of letting the root agent
// remove a tree the user could not.
func TestRecursiveDeleteChecksPermissions(t *testing.T) {
	h := newHarness(t, nil)
	if err := os.MkdirAll(filepath.Join(h.home, "tree/locked"), 0o755); err != nil {
		t.Fatal(err)
	}
	fileWrite(t, h.home, "tree/locked/file", "keep", 0o644)
	if err := os.Chmod(filepath.Join(h.home, "tree/locked"), 0o555); err != nil {
		t.Fatal(err)
	}
	_, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "tree", Recursive: true})
	wantCode(t, err, codes.InvalidArgument)
	if _, err := os.Stat(filepath.Join(h.home, "tree/locked/file")); err != nil {
		t.Fatalf("recursive delete removed a file in a read-only directory: %v", err)
	}
	if err := os.Chmod(filepath.Join(h.home, "tree/locked"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "tree", Recursive: true}); err != nil {
		t.Fatalf("recursive delete after restoring permissions: %v", err)
	}
}

// TestFilesParentSearch checks that an unsearchable parent stops even an
// operation on an otherwise readable file.
func TestFilesParentSearch(t *testing.T) {
	h := newHarness(t, nil)
	fileWrite(t, h.home, "search/locked/f", "data", 0o644)
	if err := os.Chmod(filepath.Join(h.home, "search/locked"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(h.home, "search/locked"), 0o755) })
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"read", func() error {
			_, _, _, err := fileReadFile(t, h, "search/locked/f", 0)
			return err
		}},
		{"stat", func() error {
			_, err := h.c.StatFile(t.Context(), &guestv1.StatFileRequest{Path: "search/locked/f"})
			return err
		}},
		{"list", func() error {
			_, err := h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "search/locked"})
			return err
		}},
		{"chmod", func() error {
			_, err := h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "search/locked/f", Mode: 0o644})
			return err
		}},
		{"delete", func() error {
			_, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "search/locked/f"})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantCode(t, tc.call(), codes.InvalidArgument)
		})
	}
}

// TestFilesKernelDenials proves the in-process path runs with ordinary user
// access: the kernel denies unreadable files and unwritable directories, and
// a denied operation changes nothing.
func TestFilesKernelDenials(t *testing.T) {
	h := newHarness(t, nil) // the agent here already is the sandbox user
	fileWrite(t, h.home, "deny/secret", "secret", 0o000)
	fileWrite(t, h.home, "deny/no-read-dir/f", "x", 0o644)
	fileWrite(t, h.home, "deny/read-only-dir/f", "x", 0o644)
	fileWrite(t, h.home, "deny/movable", "m", 0o644)
	if err := os.Chmod(filepath.Join(h.home, "deny/no-read-dir"), 0o300); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(h.home, "deny/read-only-dir"), 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.Chmod(filepath.Join(h.home, "deny/no-read-dir"), 0o755)
		os.Chmod(filepath.Join(h.home, "deny/read-only-dir"), 0o755)
	})

	// Reading an unreadable file is denied, but metadata needs only parent
	// search and stays available.
	_, _, _, err := fileReadFile(t, h, "deny/secret", 0)
	wantCode(t, err, codes.InvalidArgument)
	if fi, err := h.c.StatFile(t.Context(), &guestv1.StatFileRequest{Path: "deny/secret"}); err != nil || fi.GetMode() != 0 {
		t.Fatalf("stat unreadable = %v, %v; want the 0000 metadata", fi, err)
	}
	// Listing a directory without read permission is denied.
	_, err = h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "deny/no-read-dir"})
	wantCode(t, err, codes.InvalidArgument)
	// A read-only directory refuses creation, deletion and a move target.
	_, err = h.c.MakeDir(t.Context(), &guestv1.MakeDirRequest{Path: "deny/read-only-dir/new"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "deny/read-only-dir/f"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "deny/movable", Destination: "deny/read-only-dir/m"})
	wantCode(t, err, codes.InvalidArgument)
	if _, err := os.Stat(filepath.Join(h.home, "deny/read-only-dir/f")); err != nil {
		t.Fatalf("denied delete removed the file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(h.home, "deny/movable")); err != nil {
		t.Fatalf("denied move removed the source: %v", err)
	}
}

// TestFileHelperRoundTrip exercises every file RPC through the real
// credential-dropped helper protocol, including PutFile's explicit commit.
func TestFileHelperRoundTrip(t *testing.T) {
	h := newHelperHarness(t)
	fileWrite(t, h.home, "helper/dir/f.txt", "hello helper", 0o640)

	fi, err := h.c.StatFile(t.Context(), &guestv1.StatFileRequest{Path: "helper/dir/f.txt"})
	if err != nil || fi.GetPath() != "/home/user/helper/dir/f.txt" || fi.GetMode() != 0o640 || fi.GetSize() != 12 {
		t.Fatalf("helper StatFile = %v, %v", fi, err)
	}
	rf, data, chunks, err := fileReadFile(t, h, "helper/dir/f.txt", 0)
	if err != nil || string(data) != "hello helper" || chunks != 1 || rf.GetIsSymlink() {
		t.Fatalf("helper ReadFile = %q (%d chunks), %v", data, chunks, err)
	}
	ls, err := h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "helper/dir"})
	if err != nil || len(ls.GetEntries()) != 1 || ls.GetEntries()[0].GetName() != "f.txt" {
		t.Fatalf("helper ListFiles = %v, %v", ls, err)
	}
	md, err := h.c.MakeDir(t.Context(), &guestv1.MakeDirRequest{Path: "helper/new/deep", Mode: 0o750})
	if err != nil || md.GetMode() != 0o750 {
		t.Fatalf("helper MakeDir = %v, %v", md, err)
	}
	if st := fileStatT(t, filepath.Join(h.home, "helper/new/deep")); int(st.Uid) != h.uid || int(st.Gid) != h.gid {
		t.Fatalf("helper-created dir owner = %d:%d, want %d:%d", st.Uid, st.Gid, h.uid, h.gid)
	}
	if _, err := h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "helper/new/deep", Mode: 0o1777}); err != nil {
		t.Fatalf("helper ChmodFile: %v", err)
	}
	if mode := fileModeOf(t, filepath.Join(h.home, "helper/new/deep")); mode != 0o1777 {
		t.Fatalf("helper chmod mode = %#o, want 01777", mode)
	}
	mv, err := h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "helper/dir/f.txt", Destination: "/tmp/helper/f.txt"})
	if err != nil || mv.GetDestination() != "/tmp/helper/f.txt" {
		t.Fatalf("helper MoveFile = %v, %v", mv, err)
	}
	if got, err := os.ReadFile(filepath.Join(h.tmp, "helper/f.txt")); err != nil || string(got) != "hello helper" {
		t.Fatalf("helper-moved file = %q, %v", got, err)
	}
	if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "/tmp/helper", Recursive: true}); err != nil {
		t.Fatalf("helper DeleteFile: %v", err)
	}
	if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "helper/new", Recursive: true}); err != nil {
		t.Fatalf("helper DeleteFile dir: %v", err)
	}
	if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "helper/dir"}); err != nil {
		t.Fatalf("helper DeleteFile empty dir: %v", err)
	}

	resp, err := putFile(t.Context(), h, "helper/upload.txt", 0o600, []byte("uploaded through helper"), 5)
	if err != nil || resp.GetSize() != int64(len("uploaded through helper")) {
		t.Fatalf("helper PutFile = %v, %v", resp, err)
	}
	if got, err := os.ReadFile(filepath.Join(h.home, "helper/upload.txt")); err != nil || string(got) != "uploaded through helper" {
		t.Fatalf("helper upload = %q, %v", got, err)
	}
	if mode := fileModeOf(t, filepath.Join(h.home, "helper/upload.txt")); mode != 0o600 {
		t.Fatalf("helper upload mode = %#o, want 0600", mode)
	}
	if st := fileStatT(t, filepath.Join(h.home, "helper/upload.txt")); int(st.Uid) != h.uid {
		t.Fatalf("helper upload owner = %d, want %d", st.Uid, h.uid)
	}
	assertNoTempFiles(t, h.home)
}

// TestFileHelperPutFileCancel proves the helper never renames before the
// explicit commit: a canceled upload leaves the old file and no staging
// debris.
func TestFileHelperPutFileCancel(t *testing.T) {
	h := newHelperHarness(t)
	if _, err := putFile(t.Context(), h, "cancel/f", 0, []byte("old"), 1<<20); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	st, err := h.c.PutFile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "cancel/f"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Data{Data: []byte("new")}}); err != nil {
		t.Fatal(err)
	}
	// Wait for the helper's staged file and the written bytes before
	// canceling, so this exercises real cleanup rather than a no-op.
	waitStagedFile(t, filepath.Join(h.home, "cancel"), 3)
	// Cancel without half-closing: CloseSend would deliver a clean EOF, which
	// can legitimately commit complete data if it reaches the guest before the
	// cancellation. Receive the stream status directly instead.
	cancel()
	var resp nodev1.PutFileResponse
	err = st.RecvMsg(&resp)
	wantCode(t, err, codes.Canceled)
	waitNoTempFiles(t, h.home)
	if got, err := os.ReadFile(filepath.Join(h.home, "cancel/f")); err != nil || string(got) != "old" {
		t.Fatalf("canceled helper upload = %q, %v; want the old file", got, err)
	}

	// A malformed second header also aborts and leaves the old file.
	st, err = h.c.PutFile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "cancel/f"},
	}})
	st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "cancel/f"},
	}})
	_, err = st.CloseAndRecv()
	wantCode(t, err, codes.InvalidArgument)
	waitNoTempFiles(t, h.home)
	if got, err := os.ReadFile(filepath.Join(h.home, "cancel/f")); err != nil || string(got) != "old" {
		t.Fatalf("malformed helper upload = %q, %v; want the old file", got, err)
	}

	// Cancelling right after the header covers the handshake: the child has
	// staged a temporary file but must discard it without a commit.
	ctx, cancel = context.WithCancel(t.Context())
	st, err = h.c.PutFile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "cancel/f"},
	}}); err != nil {
		t.Fatal(err)
	}
	// The header-only upload must have staged its empty temporary file before
	// the cancel, otherwise cleanup is not exercised.
	waitStagedFile(t, filepath.Join(h.home, "cancel"), 0)
	cancel()
	err = st.RecvMsg(&resp)
	wantCode(t, err, codes.Canceled)
	waitNoTempFiles(t, h.home)
	if got, err := os.ReadFile(filepath.Join(h.home, "cancel/f")); err != nil || string(got) != "old" {
		t.Fatalf("canceled handshake upload = %q, %v; want the old file", got, err)
	}

	// Cancelling before the last chunk, with no intentional EOF, is a plain
	// canceled stream: the guest sees cancellation, never a clean end of
	// stream, and must discard staging while keeping the old file.
	ctx, cancel = context.WithCancel(t.Context())
	st, err = h.c.PutFile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Header_{
		Header: &guestv1.PutFileRequest_Header{Path: "cancel/f"},
	}}); err != nil {
		t.Fatal(err)
	}
	waitStagedFile(t, filepath.Join(h.home, "cancel"), 0)
	// The client intended "new" but sends only its first two bytes: the last
	// chunk is still outstanding when it cancels.
	if err := st.Send(&guestv1.PutFileRequest{Part: &guestv1.PutFileRequest_Data{Data: []byte("ne")}}); err != nil {
		t.Fatal(err)
	}
	waitStagedFile(t, filepath.Join(h.home, "cancel"), 2)
	cancel()
	err = st.RecvMsg(&resp)
	wantCode(t, err, codes.Canceled)
	waitNoTempFiles(t, h.home)
	if got, err := os.ReadFile(filepath.Join(h.home, "cancel/f")); err != nil || string(got) != "old" {
		t.Fatalf("canceled partial upload = %q, %v; want the old file", got, err)
	}

	// A later clean upload of the same path still commits.
	if _, err := putFile(t.Context(), h, "cancel/f", 0o600, []byte("new"), 4); err != nil {
		t.Fatalf("upload after cancellation: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(h.home, "cancel/f")); err != nil || string(got) != "new" {
		t.Fatalf("post-cancel commit = %q, %v; want the new file", got, err)
	}
	assertNoTempFiles(t, h.home)
}

// TestFileHelperReadCancel cancels a helper ReadFile: the parent kills the
// child's process group and reports Canceled.
func TestFileHelperReadCancel(t *testing.T) {
	h := newHelperHarness(t)
	fileWrite(t, h.home, "cancel/big", strings.Repeat("x", 16<<20), 0o644)
	ctx, cancel := context.WithCancel(t.Context())
	st, err := h.c.ReadFile(ctx, &guestv1.ReadFileRequest{Path: "cancel/big"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recv(); err != nil {
		t.Fatalf("first frame: %v", err)
	}
	cancel()
	for {
		_, err := st.Recv()
		if err == nil {
			continue
		}
		wantCode(t, err, codes.Canceled)
		return
	}
}

// waitStagedFile waits for a PutFile staging file of wantSize under dir.
func waitStagedFile(t *testing.T, dir string, wantSize int64) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, err := os.ReadDir(dir)
		if err == nil {
			for _, e := range entries {
				if !strings.HasPrefix(e.Name(), ".bbraft-put.") {
					continue
				}
				fi, ierr := e.Info()
				if ierr == nil && fi.Size() == wantSize {
					return filepath.Join(dir, e.Name())
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %d-byte staging file in %s", wantSize, dir)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitNoTempFiles waits for the helper's asynchronous abort cleanup.
func waitNoTempFiles(t *testing.T, root string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		leftover := ""
		_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
			if err == nil && strings.HasPrefix(d.Name(), ".bbraft-put.") {
				leftover = d.Name()
			}
			return nil
		})
		if leftover == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("leftover temporary file %s", leftover)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestFileHelperErrors checks the child's operation errors survive the
// protocol with their gRPC codes, distinct from protocol failures.
func TestFileHelperErrors(t *testing.T) {
	h := newHelperHarness(t)
	for _, tc := range []struct {
		name string
		call func() error
		want codes.Code
	}{
		{"stat missing", func() error {
			_, err := h.c.StatFile(t.Context(), &guestv1.StatFileRequest{Path: "missing"})
			return err
		}, codes.NotFound},
		{"stat outside", func() error {
			_, err := h.c.StatFile(t.Context(), &guestv1.StatFileRequest{Path: "/etc/passwd"})
			return err
		}, codes.InvalidArgument},
		{"list limit", func() error {
			_, err := h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: ".", Limit: 10001})
			return err
		}, codes.InvalidArgument},
		{"chmod mode", func() error {
			_, err := h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "f", Mode: 0o10000})
			return err
		}, codes.InvalidArgument},
		{"put outside", func() error {
			_, err := putFile(t.Context(), h, "/etc/helper-upload", 0, []byte("x"), 4)
			return err
		}, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantCode(t, tc.call(), tc.want)
		})
	}
}

// TestFileHelperHeaderRejected proves a child-rejected header surfaces its
// INVALID_ARGUMENT before any upload data is accepted, even when the client
// has a large body ready to send.
func TestFileHelperHeaderRejected(t *testing.T) {
	h := newHelperHarness(t)
	big := bytes.Repeat([]byte("x"), 4<<20)
	_, err := putFile(t.Context(), h, "reject/f", 0o1000, big, 1<<20) // mode beyond 0777
	wantCode(t, err, codes.InvalidArgument)
	_, err = putFile(t.Context(), h, "/etc/helper-upload", 0, []byte("x"), 4) // outside the roots
	wantCode(t, err, codes.InvalidArgument)
	if _, err := os.Stat(filepath.Join(h.home, "reject/f")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("rejected upload created a file: %v", err)
	}
	assertNoTempFiles(t, h.home)
}

// TestFileHelperUploadShapes covers the ready handshake for a header-only
// upload and a multi-MiB streaming upload, and that an existing custom-mode
// parent is never rewritten by the upload's parent creation.
func TestFileHelperUploadShapes(t *testing.T) {
	h := newHelperHarness(t)
	resp, err := putFile(t.Context(), h, "shapes/empty", 0o600, nil, 1)
	if err != nil || resp.GetSize() != 0 {
		t.Fatalf("empty helper upload = %v, %v", resp, err)
	}
	payload := bytes.Repeat([]byte("0123456789abcdef"), (4<<20)/16)
	resp, err = putFile(t.Context(), h, "shapes/large", 0o640, payload, 1<<20)
	if err != nil || resp.GetSize() != int64(len(payload)) {
		t.Fatalf("large helper upload = %v, %v", resp, err)
	}
	if got, err := os.ReadFile(filepath.Join(h.home, "shapes/large")); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("large helper upload content = %d bytes, %v", len(got), err)
	}
	pmode := filepath.Join(h.home, "pmode")
	if err := os.MkdirAll(pmode, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(pmode, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := putFile(t.Context(), h, "pmode/file", 0, []byte("p"), 4); err != nil {
		t.Fatalf("upload under an existing parent: %v", err)
	}
	if mode := fileModeOf(t, pmode); mode != 0o700 {
		t.Fatalf("upload rewrote the existing parent mode to %#o", mode)
	}
	assertNoTempFiles(t, h.home)
}

// TestMakeDirPreservesExistingParentMode checks the shared parent creation in
// the in-process path as well.
func TestMakeDirPreservesExistingParentMode(t *testing.T) {
	h := newHarness(t, nil)
	parent := filepath.Join(h.home, "pm")
	if err := os.MkdirAll(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := h.c.MakeDir(t.Context(), &guestv1.MakeDirRequest{Path: "pm/child"}); err != nil {
		t.Fatalf("MakeDir: %v", err)
	}
	if mode := fileModeOf(t, parent); mode != 0o700 {
		t.Fatalf("MakeDir rewrote the existing parent mode to %#o", mode)
	}
}

// TestFilesPinnedRootsAfterReplacement proves file RPCs stay on the roots
// pinned at New. The helper path used to reopen configured path strings, so
// renaming or replacing home/tmp after New redirected it at another inode;
// both execution paths now operate on the original roots and leave the
// replacement untouched.
func TestFilesPinnedRootsAfterReplacement(t *testing.T) {
	for _, tc := range []struct {
		name  string
		start func(*testing.T) *harness
	}{
		{"in-process", func(t *testing.T) *harness { return newHarness(t, nil) }},
		{"helper", func(t *testing.T) *harness { return newHelperHarness(t) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := tc.start(t)
			fileWrite(t, h.home, "original.txt", "original home", 0o640)
			fileWrite(t, h.tmp, "original-tmp.txt", "original tmp", 0o640)

			pinnedHome := h.home + ".pinned"
			pinnedTmp := h.tmp + ".pinned"
			if err := os.Rename(h.home, pinnedHome); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(h.tmp, pinnedTmp); err != nil {
				t.Fatal(err)
			}
			for _, d := range []string{h.home, h.tmp} {
				if err := os.Mkdir(d, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			fileWrite(t, h.home, "impostor-home", "impostor", 0o644)
			fileWrite(t, h.tmp, "impostor-tmp", "impostor", 0o644)

			// Reads and stats follow the pinned inode, not the replacement.
			if _, data, _, err := fileReadFile(t, h, "original.txt", 0); err != nil || string(data) != "original home" {
				t.Fatalf("read pinned home = %q, %v", data, err)
			}
			if _, data, _, err := fileReadFile(t, h, "/tmp/original-tmp.txt", 0); err != nil || string(data) != "original tmp" {
				t.Fatalf("read pinned tmp = %q, %v", data, err)
			}
			if _, err := h.c.StatFile(t.Context(), &guestv1.StatFileRequest{Path: "impostor-home"}); status.Code(err) != codes.NotFound {
				t.Fatalf("stat resolved into the replacement home: %v", err)
			}

			// Upload, mkdir, cross-root move and delete all stay pinned.
			if _, err := putFile(t.Context(), h, "parity/up.txt", 0o600, []byte("pinned"), 4); err != nil {
				t.Fatalf("PutFile: %v", err)
			}
			if got, err := os.ReadFile(filepath.Join(pinnedHome, "parity/up.txt")); err != nil || string(got) != "pinned" {
				t.Fatalf("upload landed outside the pinned home: %q, %v", got, err)
			}
			if _, err := os.Stat(filepath.Join(h.home, "parity/up.txt")); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("upload touched the replacement home: %v", err)
			}
			if _, err := h.c.MakeDir(t.Context(), &guestv1.MakeDirRequest{Path: "parity/dir"}); err != nil {
				t.Fatalf("MakeDir: %v", err)
			}
			if _, err := os.Stat(filepath.Join(pinnedHome, "parity/dir")); err != nil {
				t.Fatalf("mkdir landed outside the pinned home: %v", err)
			}
			if _, err := h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "parity/up.txt", Destination: "/tmp/parity/moved.txt"}); err != nil {
				t.Fatalf("MoveFile: %v", err)
			}
			if got, err := os.ReadFile(filepath.Join(pinnedTmp, "parity/moved.txt")); err != nil || string(got) != "pinned" {
				t.Fatalf("move landed outside the pinned tmp: %q, %v", got, err)
			}
			if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "/tmp/original-tmp.txt"}); err != nil {
				t.Fatalf("DeleteFile: %v", err)
			}
			if _, err := os.Stat(filepath.Join(pinnedTmp, "original-tmp.txt")); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("delete did not hit the pinned tmp: %v", err)
			}

			// Replacement contents are untouched.
			if got, err := os.ReadFile(filepath.Join(h.home, "impostor-home")); err != nil || string(got) != "impostor" {
				t.Fatalf("replacement home changed: %q, %v", got, err)
			}
			if got, err := os.ReadFile(filepath.Join(h.tmp, "impostor-tmp")); err != nil || string(got) != "impostor" {
				t.Fatalf("replacement tmp changed: %q, %v", got, err)
			}

			// A symlink planted at the configured path is equally ineffective.
			evil := filepath.Join(t.TempDir(), "evil")
			if err := os.Mkdir(evil, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(h.home); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(evil, h.home); err != nil {
				t.Fatal(err)
			}
			if _, err := putFile(t.Context(), h, "symlink/up.txt", 0, []byte("sym"), 4); err != nil {
				t.Fatalf("PutFile through a replaced symlink path: %v", err)
			}
			if got, err := os.ReadFile(filepath.Join(pinnedHome, "symlink/up.txt")); err != nil || string(got) != "sym" {
				t.Fatalf("symlink replacement redirected the upload: %q, %v", got, err)
			}
			if _, err := os.Stat(filepath.Join(evil, "symlink/up.txt")); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("upload followed the replacement symlink: %v", err)
			}
		})
	}
}

// TestFileHelperLargeListBatches proves the internal list framing handles a
// result whose JSON exceeds the 2 MiB helper frame cap while the public
// unary response still fits gRPC's 4 MiB message limit.
func TestFileHelperLargeListBatches(t *testing.T) {
	h := newHelperHarness(t)
	dir := filepath.Join(h.home, "biglist")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	const entries = 5000
	long := strings.Repeat("n", 230)
	for i := range entries {
		name := fmt.Sprintf("%s-%04d", long, i)
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	ls, err := h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "biglist", Limit: entries})
	if err != nil {
		t.Fatalf("ListFiles of %d entries: %v", entries, err)
	}
	if len(ls.GetEntries()) != entries || ls.GetTruncated() {
		t.Fatalf("ListFiles returned %d entries (truncated %v), want %d", len(ls.GetEntries()), ls.GetTruncated(), entries)
	}
	approxJSON := 0
	for i, e := range ls.GetEntries() {
		if i > 0 && e.GetName() <= ls.GetEntries()[i-1].GetName() {
			t.Fatalf("entries are not ordered at %q", e.GetName())
		}
		approxJSON += len(e.GetPath()) + len(e.GetName()) + 96
	}
	if approxJSON <= 2<<20 {
		t.Fatalf("fixture JSON is only ~%d bytes; it must exceed the old 2 MiB frame cap", approxJSON)
	}
	if size := proto.Size(ls); size >= 4<<20 {
		t.Fatalf("marshaled ListFilesResponse is %d bytes, want below 4 MiB", size)
	}

	// A limited request goes through the same batch path and truncates.
	limited, err := h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "biglist", Limit: 3})
	if err != nil || len(limited.GetEntries()) != 3 || !limited.GetTruncated() {
		t.Fatalf("limited ListFiles = %d entries (truncated %v), %v", len(limited.GetEntries()), limited.GetTruncated(), err)
	}
}
