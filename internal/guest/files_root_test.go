//go:build linux

package guest_test

import (
	"encoding/binary"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
)

// These root-only regressions prove the credential-dropped helper really is
// the sandbox user: the test process is root, so a root agent would bypass
// every one of these denials. Run them only in a throwaway VM:
//
//	BBRAFT_TEST_FILES_ROOT=1 sudo -E go test ./internal/guest -run TestRoot -v
//
// They need the A5 shared credential builder for the group tests: the helper
// must carry the sandbox user's primary and supplementary groups.

const rootSandboxUID = 1000

// requireRootFiles gates the root-only suite: no run without the explicit
// environment flag, a hard failure (not a skip) when the flag is set but the
// fixture cannot work.
func requireRootFiles(t *testing.T) {
	t.Helper()
	if os.Getenv("BBRAFT_TEST_FILES_ROOT") != "1" {
		t.Skip("set BBRAFT_TEST_FILES_ROOT=1 and run as root in a throwaway VM")
	}
	if os.Geteuid() != 0 {
		t.Fatal("BBRAFT_TEST_FILES_ROOT=1 requires root: run it in a throwaway VM as root")
	}
	if _, err := user.LookupId(strconv.Itoa(rootSandboxUID)); err != nil {
		t.Fatalf("sandbox uid %d does not exist, cannot drop credentials: %v", rootSandboxUID, err)
	}
}

// newRootHelperHarness starts a service whose file operations drop to the
// sandbox user. The fixture base is a direct, traversable child of /tmp
// (t.TempDir's 0700 testing parent would block the dropped helper), and the
// returned harness proves the dropped user can use its home, so a broken
// fixture fails loudly instead of making denial tests pass vacuously.
func newRootHelperHarness(t *testing.T) *harness {
	t.Helper()
	requireRootFiles(t)
	base, err := os.MkdirTemp("/tmp", "bbraft-root-files-")
	if err != nil {
		t.Fatalf("create fixture base under /tmp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(base) })
	if err := os.Chmod(base, 0o755); err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(base, "home/user")
	tmp := filepath.Join(base, "tmp")
	for _, d := range []string{home, tmp} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(d, rootSandboxUID, rootSandboxUID); err != nil {
			t.Fatal(err)
		}
	}
	fz := &fakeFreezer{}
	cfg := guest.Config{
		Log:             slog.New(slog.DiscardHandler),
		Version:         "root-test",
		UID:             rootSandboxUID,
		GID:             rootSandboxUID,
		ForceFileHelper: true,
		HomeRoot:        home,
		TmpRoot:         tmp,
		DefaultLease:    2 * time.Minute,
		WatchdogRetry:   5 * time.Second,
		Freezer:         fz,
	}
	h := startHarness(t, cfg, home, tmp, fz, rootSandboxUID, rootSandboxUID)

	// Positive controls: the dropped helper must read its own file and list
	// its home. Without these, every denial below could pass because the
	// helper never started (e.g. an untraversable fixture parent).
	probe := fileWrite(t, home, "harness-probe", "probe", 0o600)
	if err := os.Chown(probe, rootSandboxUID, rootSandboxUID); err != nil {
		t.Fatal(err)
	}
	if _, data, _, err := fileReadFile(t, h, "harness-probe", 0); err != nil || string(data) != "probe" {
		t.Fatalf("positive control: dropped helper cannot read its own file: %q, %v", data, err)
	}
	if _, err := h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "."}); err != nil {
		t.Fatalf("positive control: dropped helper cannot list its home: %v", err)
	}
	return h
}

// Root ACL tag and permission bits.
const (
	rootACLUserObj  = 0x01
	rootACLUser     = 0x02
	rootACLGroupObj = 0x04
	rootACLGroup    = 0x08
	rootACLMask     = 0x10
	rootACLOther    = 0x20
	rootACLUndef    = ^uint32(0)
)

type rootACLEntry struct {
	tag  uint16
	perm uint32
	id   uint32
}

func setRootACL(t *testing.T, p string, entries ...rootACLEntry) {
	t.Helper()
	raw := binary.LittleEndian.AppendUint32(nil, 2)
	for _, e := range entries {
		raw = binary.LittleEndian.AppendUint16(raw, e.tag)
		raw = binary.LittleEndian.AppendUint16(raw, uint16(e.perm))
		raw = binary.LittleEndian.AppendUint32(raw, e.id)
	}
	if err := unix.Setxattr(p, "system.posix_acl_access", raw, 0); err != nil {
		t.Fatalf("set access ACL on %s: %v", p, err)
	}
}

// sandboxSupplementaryGIDs returns the sandbox user's groups other than the
// primary one.
func sandboxSupplementaryGIDs(t *testing.T) []int {
	t.Helper()
	u, err := user.LookupId(strconv.Itoa(rootSandboxUID))
	if err != nil {
		t.Fatal(err)
	}
	ids, err := u.GroupIds()
	if err != nil {
		t.Fatal(err)
	}
	var out []int
	for _, s := range ids {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
		if n != rootSandboxUID {
			out = append(out, n)
		}
	}
	return out
}

// TestRootHelperACLAndGroups covers access ACLs and supplementary-group
// access; mode bits alone would allow the deny and deny the group grants.
func TestRootHelperACLAndGroups(t *testing.T) {
	h := newRootHelperHarness(t)

	// A named-user deny overrides other::r--.
	deny := fileWrite(t, h.home, "acl/deny", "secret", 0o644)
	setRootACL(t, deny,
		rootACLEntry{rootACLUserObj, 6, rootACLUndef},
		rootACLEntry{rootACLUser, 0, rootSandboxUID},
		rootACLEntry{rootACLGroupObj, 4, rootACLUndef},
		rootACLEntry{rootACLMask, 4, rootACLUndef},
		rootACLEntry{rootACLOther, 4, rootACLUndef},
	)
	_, _, _, err := fileReadFile(t, h, "acl/deny", 0)
	wantCode(t, err, codes.InvalidArgument)

	// A named-user allow overrides other::---.
	allow := fileWrite(t, h.home, "acl/allow", "allowed", 0o600)
	setRootACL(t, allow,
		rootACLEntry{rootACLUserObj, 6, rootACLUndef},
		rootACLEntry{rootACLUser, 4, rootSandboxUID},
		rootACLEntry{rootACLGroupObj, 0, rootACLUndef},
		rootACLEntry{rootACLMask, 4, rootACLUndef},
		rootACLEntry{rootACLOther, 0, rootACLUndef},
	)
	if _, data, _, err := fileReadFile(t, h, "acl/allow", 0); err != nil || string(data) != "allowed" {
		t.Fatalf("named-user ACL allow = %q, %v", data, err)
	}

	// A traversed parent ACL must be evaluated too.
	if err := os.MkdirAll(filepath.Join(h.home, "acl/dir-allow"), 0o700); err != nil {
		t.Fatal(err)
	}
	fileWrite(t, h.home, "acl/dir-allow/f", "through", 0o644)
	setRootACL(t, filepath.Join(h.home, "acl/dir-allow"),
		rootACLEntry{rootACLUserObj, 7, rootACLUndef},
		rootACLEntry{rootACLUser, 5, rootSandboxUID},
		rootACLEntry{rootACLGroupObj, 0, rootACLUndef},
		rootACLEntry{rootACLMask, 5, rootACLUndef},
		rootACLEntry{rootACLOther, 0, rootACLUndef},
	)
	if _, data, _, err := fileReadFile(t, h, "acl/dir-allow/f", 0); err != nil || string(data) != "through" {
		t.Fatalf("parent ACL search allow = %q, %v", data, err)
	}

	groups := sandboxSupplementaryGIDs(t)
	if len(groups) == 0 {
		t.Fatal("sandbox user has no supplementary groups; cannot verify inherited groups")
	}
	// Mode-only group access through a supplementary group.
	modeOnly := fileWrite(t, h.home, "acl/group-mode", "group mode", 0o640)
	if err := os.Chown(modeOnly, 0, groups[0]); err != nil {
		t.Fatal(err)
	}
	if _, data, _, err := fileReadFile(t, h, "acl/group-mode", 0); err != nil || string(data) != "group mode" {
		t.Fatalf("supplementary group mode access = %q, %v", data, err)
	}
	// Named-group ACL access through the same group.
	groupACL := fileWrite(t, h.home, "acl/group-acl", "group acl", 0o600)
	setRootACL(t, groupACL,
		rootACLEntry{rootACLUserObj, 6, rootACLUndef},
		rootACLEntry{rootACLGroupObj, 0, rootACLUndef},
		rootACLEntry{rootACLGroup, 4, uint32(groups[0])},
		rootACLEntry{rootACLMask, 4, rootACLUndef},
		rootACLEntry{rootACLOther, 0, rootACLUndef},
	)
	if _, data, _, err := fileReadFile(t, h, "acl/group-acl", 0); err != nil || string(data) != "group acl" {
		t.Fatalf("supplementary group ACL access = %q, %v", data, err)
	}
	// A group the user does not belong to grants nothing.
	foreign := fileWrite(t, h.home, "acl/foreign-group", "foreign", 0o640)
	if err := os.Chown(foreign, 0, 65533); err != nil {
		t.Fatal(err)
	}
	_, _, _, err = fileReadFile(t, h, "acl/foreign-group", 0)
	wantCode(t, err, codes.InvalidArgument)
}

// TestRootHelperStickyAndChmod covers the sticky-bit rule and owner-only
// chmod, which the kernel enforces on the helper's syscalls.
func TestRootHelperStickyAndChmod(t *testing.T) {
	h := newRootHelperHarness(t)
	sticky := filepath.Join(h.tmp, "sticky")
	if err := os.MkdirAll(sticky, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sticky, 0o777|fs.ModeSticky); err != nil {
		t.Fatal(err)
	}
	victim := fileWrite(t, h.tmp, "sticky/theirs", "theirs", 0o644)

	if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "/tmp/sticky/theirs"}); statusOf(err) != codes.InvalidArgument {
		t.Fatalf("sticky delete = %v, want InvalidArgument", err)
	}
	if _, err := h.c.MoveFile(t.Context(), &guestv1.MoveFileRequest{Source: "/tmp/sticky/theirs", Destination: "/tmp/sticky2/theirs"}); statusOf(err) != codes.InvalidArgument {
		t.Fatalf("sticky move = %v, want InvalidArgument", err)
	}
	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("sticky victim disappeared: %v", err)
	}

	if _, err := h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "/tmp/sticky/theirs", Mode: 0o600}); statusOf(err) != codes.InvalidArgument {
		t.Fatalf("non-owner chmod = %v, want InvalidArgument", err)
	}
	if mode := fileModeOf(t, victim); mode != 0o644 {
		t.Fatalf("non-owner chmod changed the mode to %#o", mode)
	}

	// The same user's own file is readable and writable.
	own := fileWrite(t, h.home, "own/file", "own", 0o600)
	if err := os.Chown(own, rootSandboxUID, rootSandboxUID); err != nil {
		t.Fatal(err)
	}
	if _, data, _, err := fileReadFile(t, h, "own/file", 0); err != nil || string(data) != "own" {
		t.Fatalf("owner read = %q, %v", data, err)
	}
}

// TestRootHelperPutFileDenied is the PutFile root-bypass regression: the old
// root handler wrote into any directory inside the roots; the helper must
// fail where the sandbox user cannot write.
func TestRootHelperPutFileDenied(t *testing.T) {
	h := newRootHelperHarness(t)
	protected := filepath.Join(h.home, "write-protected")
	if err := os.MkdirAll(protected, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(protected, 0o555); err != nil {
		t.Fatal(err)
	}
	_, err := putFile(t.Context(), h, "write-protected/new.txt", 0, []byte("leak"), 4)
	wantCode(t, err, codes.InvalidArgument)
	if _, err := os.Stat(filepath.Join(protected, "new.txt")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("PutFile wrote into a directory the sandbox user cannot write: %v", err)
	}
	_, err = h.c.MakeDir(t.Context(), &guestv1.MakeDirRequest{Path: "write-protected/dir"})
	wantCode(t, err, codes.InvalidArgument)

	// The user's own home still accepts uploads with the right owner.
	if _, err := putFile(t.Context(), h, "owned.txt", 0o600, []byte("ok"), 4); err != nil {
		t.Fatalf("PutFile in the user's home: %v", err)
	}
	if st := fileStatT(t, filepath.Join(h.home, "owned.txt")); int(st.Uid) != rootSandboxUID {
		t.Fatalf("uploaded owner = %d, want %d", st.Uid, rootSandboxUID)
	}
}

// TestRootHelperPinnedRoots proves the dropped helper uses the inodes pinned
// at New after both configured paths are replaced, and that the kernel still
// denies what the sandbox user cannot do inside those pinned roots.
func TestRootHelperPinnedRoots(t *testing.T) {
	h := newRootHelperHarness(t)
	protected := filepath.Join(h.home, "write-protected")
	if err := os.MkdirAll(protected, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(protected, 0o555); err != nil {
		t.Fatal(err)
	}
	sticky := filepath.Join(h.tmp, "sticky")
	if err := os.MkdirAll(sticky, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sticky, 0o777|fs.ModeSticky); err != nil {
		t.Fatal(err)
	}
	fileWrite(t, h.tmp, "sticky/theirs", "theirs", 0o644)

	pinnedHome := h.home + ".pinned"
	pinnedTmp := h.tmp + ".pinned"
	if err := os.Rename(h.home, pinnedHome); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(h.tmp, pinnedTmp); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{h.home, h.tmp} {
		if err := os.Mkdir(d, 0o777); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(d, 0o777); err != nil {
			t.Fatal(err)
		}
	}

	// The root-owned read-only directory in the pinned home still denies the
	// upload, even though the replacement home is world-writable.
	_, err := putFile(t.Context(), h, "write-protected/x", 0, []byte("x"), 4)
	wantCode(t, err, codes.InvalidArgument)
	if _, statErr := os.Stat(filepath.Join(h.home, "write-protected/x")); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("denied upload appeared in the replacement home: %v", statErr)
	}
	// A permitted upload lands in the pinned home; the replacement stays empty.
	if _, err := putFile(t.Context(), h, "pinned.txt", 0, []byte("ok"), 4); err != nil {
		t.Fatalf("PutFile into the pinned home: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(h.home, "pinned.txt")); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("upload used the replacement home: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(pinnedHome, "pinned.txt")); statErr != nil {
		t.Fatalf("upload missed the pinned home: %v", statErr)
	}
	// Reads still reach the pinned inode (the harness probe lives there).
	if _, data, _, err := fileReadFile(t, h, "harness-probe", 0); err != nil || string(data) != "probe" {
		t.Fatalf("read through the pinned root = %q, %v", data, err)
	}
	// Sticky deletion in the pinned tmp still denies another user's entry.
	if _, err := h.c.DeleteFile(t.Context(), &guestv1.DeleteFileRequest{Path: "/tmp/sticky/theirs"}); statusOf(err) != codes.InvalidArgument {
		t.Fatalf("sticky delete = %v, want InvalidArgument", err)
	}
	// Non-owner chmod inside the pinned home still denies.
	rootOwned := fileWrite(t, pinnedHome, "root-owned", "r", 0o644)
	if _, err := h.c.ChmodFile(t.Context(), &guestv1.ChmodFileRequest{Path: "root-owned", Mode: 0o600}); statusOf(err) != codes.InvalidArgument {
		t.Fatalf("non-owner chmod = %v, want InvalidArgument", err)
	}
	if mode := fileModeOf(t, rootOwned); mode != 0o644 {
		t.Fatalf("non-owner chmod changed the pinned file to %#o", mode)
	}
}

// TestRootHelperSearchOnlyRoots is the production-style search-only fixture:
// Exec keeps working for known paths (traversal needs only x), while every
// file RPC returns a clean INVALID_ARGUMENT naming the logical root. The
// helper must never run file operations as root or fall back to another
// inode, and it must not fail internally or hang.
func TestRootHelperSearchOnlyRoots(t *testing.T) {
	h := newRootHelperHarness(t)
	// The harness positive controls already proved file RPCs work while the
	// roots are readable; now make a known file readable and both roots
	// search-only.
	known := fileWrite(t, h.home, "known.txt", "known\n", 0o644)
	if err := os.Chown(known, rootSandboxUID, rootSandboxUID); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{h.home, h.tmp} {
		if err := os.Chmod(d, 0o111); err != nil {
			t.Fatal(err)
		}
	}

	// Exec still works: x on the roots, read on the known file.
	res := h.exec(t, "cat known.txt")
	if res.Stdout != "known\n" || res.Exit.GetExitCode() != 0 {
		t.Fatalf("Exec under search-only roots = stdout %q, stderr %q, exit %v", res.Stdout, res.Stderr, res.Exit)
	}

	want := "permission denied on the logical root /home/user"
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"stat", func() error {
			_, err := h.c.StatFile(t.Context(), &guestv1.StatFileRequest{Path: "known.txt"})
			return err
		}},
		{"read", func() error {
			_, _, _, err := fileReadFile(t, h, "known.txt", 0)
			return err
		}},
		{"list", func() error {
			_, err := h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "."})
			return err
		}},
		{"mkdir", func() error {
			_, err := h.c.MakeDir(t.Context(), &guestv1.MakeDirRequest{Path: "new"})
			return err
		}},
		{"put", func() error {
			_, err := putFile(t.Context(), h, "new.txt", 0, []byte("x"), 4)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if statusOf(err) != codes.InvalidArgument || !strings.Contains(status.Convert(err).Message(), want) {
				t.Fatalf("search-only %s = %v, want InvalidArgument with %q", tc.name, err, want)
			}
		})
	}

	// Exec is still usable after the file failures.
	if res := h.exec(t, "cat known.txt"); res.Stdout != "known\n" || res.Exit.GetExitCode() != 0 {
		t.Fatalf("Exec after file failures = stdout %q, exit %v", res.Stdout, res.Exit)
	}

	// With only tmp search-only, the typed error names /tmp.
	if err := os.Chmod(h.home, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "/tmp"}); statusOf(err) != codes.InvalidArgument || !strings.Contains(status.Convert(err).Message(), "/tmp") {
		t.Fatalf("tmp search-only list = %v, want InvalidArgument naming /tmp", err)
	}
}
