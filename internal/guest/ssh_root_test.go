//go:build linux

package guest_test

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
)

// These root-only SSH regressions prove AddSSHKey runs as the sandbox user and
// inherits A2's confinement guarantees. The test process is root, so a
// root-side implementation would bypass every ownership and search-only check
// below. Run them only in a throwaway VM:
//
//	BBRAFT_TEST_FILES_ROOT=1 sudo -E go test ./internal/guest -run TestRootAddSSHKey -v

// sshTestRootHarness starts a root-run service whose file operations drop to
// the sandbox user, with the host public key path outside the fixture.
func sshTestRootHarness(t *testing.T) (*harness, string) {
	t.Helper()
	requireRootFiles(t)
	// The fixture base must be a traversable direct child of /tmp: t.TempDir's
	// 0700 testing parent would block the dropped helper.
	base, err := os.MkdirTemp("/tmp", "bbraft-root-ssh-")
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
	hostKeyPath := filepath.Join(base, "ssh_host_ed25519_key.pub")
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
		SSHHostKeyFile:  hostKeyPath,
	}
	h := startHarness(t, cfg, home, tmp, fz, rootSandboxUID, rootSandboxUID)
	// Positive control: if the dropped helper cannot even list its home, the
	// fixture is broken and every denial below would pass vacuously.
	if _, err := h.c.ListFiles(t.Context(), &guestv1.ListFilesRequest{Path: "."}); err != nil {
		t.Fatalf("positive control: dropped helper cannot list its home: %v", err)
	}
	return h, hostKeyPath
}

// TestRootAddSSHKeyOwnership proves the key files belong to the sandbox user,
// not root, with the modes sshd's StrictModes requires.
func TestRootAddSSHKeyOwnership(t *testing.T) {
	h, hostKeyPath := sshTestRootHarness(t)
	hostLine := sshTestKey(t, "bbraft-guest")
	sshTestWriteHostKey(t, hostKeyPath, hostLine+"\n")

	key := sshTestKey(t, "sandbox")
	acc := sshTestAdd(t, h, key)
	if acc.GetHostKey() != hostLine {
		t.Fatalf("host key = %q, want %q", acc.GetHostKey(), hostLine)
	}
	authPath := sshTestFileName(h)
	if got, want := sshTestContent(t, authPath), key+"\n"; got != want {
		t.Fatalf("authorized_keys = %q, want %q", got, want)
	}
	for _, tc := range []struct {
		path string
		mode uint32
	}{
		{path: filepath.Join(h.home, ".ssh"), mode: 0o700},
		{path: authPath, mode: 0o600},
	} {
		st := fileStatT(t, tc.path)
		if int(st.Uid) != rootSandboxUID || int(st.Gid) != rootSandboxUID {
			t.Fatalf("%s owner = %d:%d, want %d:%d", tc.path, st.Uid, st.Gid, rootSandboxUID, rootSandboxUID)
		}
		if got := sshTestFileMode(t, tc.path); got != tc.mode {
			t.Fatalf("%s mode = %#o, want %#o", tc.path, got, tc.mode)
		}
	}

	// Idempotent for the dropped user too, including a comment variant.
	sshTestAdd(t, h, sshTestRecomment(t, key, "other"))
	if got := sshTestContent(t, authPath); got != key+"\n" {
		t.Fatalf("authorized_keys after re-adding = %q", got)
	}
}

// TestRootAddSSHKeySearchOnlyHome proves AddSSHKey inherits A2's search-only
// root contract: a clean INVALID_ARGUMENT naming the logical root, never
// INTERNAL, while Exec keeps working for known paths.
func TestRootAddSSHKeySearchOnlyHome(t *testing.T) {
	h, hostKeyPath := sshTestRootHarness(t)
	sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")

	known := fileWrite(t, h.home, "known.txt", "known\n", 0o644)
	if err := os.Chown(known, rootSandboxUID, rootSandboxUID); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{h.home, h.tmp} {
		if err := os.Chmod(d, 0o111); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, d := range []string{h.home, h.tmp} {
			if err := os.Chmod(d, 0o755); err != nil {
				t.Errorf("restore %s: %v", d, err)
			}
		}
	})

	// Exec still works: x on the roots, read on the known file.
	if res := h.exec(t, "cat known.txt"); res.Stdout != "known\n" || res.Exit.GetExitCode() != 0 {
		t.Fatalf("Exec under search-only roots = stdout %q, exit %v", res.Stdout, res.Exit)
	}

	_, err := h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: sshTestKey(t, "sandbox")})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("AddSSHKey under search-only home = %v, want InvalidArgument", err)
	}
	want := "permission denied on the logical root /home/user"
	if !strings.Contains(status.Convert(err).Message(), want) {
		t.Fatalf("AddSSHKey error = %v, want it to name %q", err, want)
	}
	if _, err := os.Lstat(sshTestFileName(h)); !os.IsNotExist(err) {
		t.Fatalf("search-only AddSSHKey published a key file: %v", err)
	}

	// Exec is still usable after the file failure.
	if res := h.exec(t, "cat known.txt"); res.Stdout != "known\n" || res.Exit.GetExitCode() != 0 {
		t.Fatalf("Exec after AddSSHKey failure = stdout %q, exit %v", res.Stdout, res.Exit)
	}
}

// TestRootAddSSHKeyForeignSSHDirDenied proves the install path is denied by the
// kernel when .ssh belongs to another user. Ownership assertions alone could be
// mimicked by a root-side chown, so this requires an actual denial through
// AddSSHKey: a user-owned install is the positive control, then a root-owned
// foreign .ssh and authorized_keys must make the call fail without changing a
// byte or adding the key.
func TestRootAddSSHKeyForeignSSHDirDenied(t *testing.T) {
	h, hostKeyPath := sshTestRootHarness(t)
	sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")

	// Positive control: the dropped-user SSH path works and owns its files.
	first := sshTestKey(t, "first")
	sshTestAdd(t, h, first)
	if got, want := sshTestContent(t, sshTestFileName(h)), first+"\n"; got != want {
		t.Fatalf("positive control authorized_keys = %q, want %q", got, want)
	}

	// Replace the user-owned .ssh with a foreign root-owned one that the
	// sandbox user may read but not chmod or write.
	dir := filepath.Join(h.home, ".ssh")
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	authPath := sshTestFileName(h)
	foreign := sshTestKey(t, "foreign-owner")
	if err := os.WriteFile(authPath, []byte(foreign+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dirMode := sshTestFileMode(t, dir)
	before := sshTestContent(t, authPath)

	// Fixture sanity: the file really is foreign (not the sandbox user) and the
	// sandbox user can read it, so any failure below is on the write side.
	if st := fileStatT(t, authPath); int(st.Uid) == rootSandboxUID || int(st.Gid) == rootSandboxUID {
		t.Fatalf("fixture authorized_keys is owned by the sandbox user: %d:%d", st.Uid, st.Gid)
	}
	if res := h.exec(t, "cat .ssh/authorized_keys"); res.Exit.GetExitCode() != 0 ||
		!strings.Contains(res.Stdout, strings.Fields(foreign)[1]) {
		t.Fatalf("positive control: sandbox user cannot read the foreign file: stdout %q stderr %q exit %v",
			res.Stdout, res.Stderr, res.Exit)
	}

	denied := sshTestKey(t, "denied")
	_, err := h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: denied})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("AddSSHKey over a foreign .ssh = %v, want InvalidArgument from the kernel denial", err)
	}
	if got := sshTestContent(t, authPath); got != before {
		t.Fatalf("foreign authorized_keys changed:\ngot  %q\nwant %q", got, before)
	}
	if strings.Contains(before, sshTestBare(t, denied)) || strings.Contains(sshTestContent(t, authPath), sshTestBare(t, denied)) {
		t.Fatal("the denied key was published")
	}
	if got := sshTestFileMode(t, dir); got != dirMode {
		t.Fatalf(".ssh mode changed from %#o to %#o", dirMode, got)
	}
}
