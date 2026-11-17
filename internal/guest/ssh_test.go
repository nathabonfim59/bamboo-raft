package guest_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc/codes"

	"github.com/nathabonfim59/bamboo-raft/internal/guest"
	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
)

// sshTestKey returns a fresh ed25519 authorized_keys line, with the optional
// comment appended.
func sshTestKey(t *testing.T, comment string) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key)))
	if comment != "" {
		line += " " + comment
	}
	return line
}

// sshTestRecomment rewrites a key line with another comment, keeping the key
// material identical.
func sshTestRecomment(t *testing.T, line, comment string) string {
	t.Helper()
	key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	out := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key)))
	if comment != "" {
		out += " " + comment
	}
	return out
}

// sshTestBare strips the comment from a key line.
func sshTestBare(t *testing.T, line string) string {
	t.Helper()
	return sshTestRecomment(t, line, "")
}

// sshTestHarness starts a service whose SSH host public key path is a private
// temp file, in the in-process or the credential-dropped helper file mode.
func sshTestHarness(t *testing.T, forceHelper bool) (*harness, string) {
	t.Helper()
	hostKeyPath := filepath.Join(t.TempDir(), "ssh_host_ed25519_key.pub")
	return newHarnessMode(t, func(cfg *guest.Config) {
		cfg.SSHHostKeyFile = hostKeyPath
	}, forceHelper), hostKeyPath
}

// sshTestWriteHostKey publishes a host public key file.
func sshTestWriteHostKey(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write host key %s: %v", path, err)
	}
}

// sshTestFileName is the authorized_keys path under the harness home.
func sshTestFileName(h *harness) string { return filepath.Join(h.home, ".ssh", "authorized_keys") }

// sshTestSeed writes .ssh and authorized_keys with the given modes.
func sshTestSeed(t *testing.T, h *harness, content string, dirMode, fileMode fs.FileMode) {
	t.Helper()
	dir := filepath.Join(h.home, ".ssh")
	if err := os.MkdirAll(dir, dirMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, dirMode); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sshTestFileName(h), []byte(content), fileMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sshTestFileName(h), fileMode); err != nil {
		t.Fatal(err)
	}
}

// sshTestFileMode is the 0o7777 mode of p, following symlinks.
func sshTestFileMode(t *testing.T, p string) uint32 {
	t.Helper()
	return fileModeOf(t, p)
}

// sshTestContent reads the file, failing the test when it cannot.
func sshTestContent(t *testing.T, p string) string {
	t.Helper()
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return string(data)
}

// sshTestAdd installs one key through the RPC.
func sshTestAdd(t *testing.T, h *harness, key string) *guestv1.AddSSHKeyResponse {
	t.Helper()
	acc, err := h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: key})
	if err != nil {
		t.Fatalf("AddSSHKey: %v", err)
	}
	return acc
}

// sshTestModes runs f in both file-operation modes: the in-process file server
// and the credential-dropped helper.
func sshTestModes(t *testing.T, f func(t *testing.T, forceHelper bool)) {
	t.Helper()
	for _, tc := range []struct {
		name  string
		force bool
	}{{name: "direct", force: false}, {name: "helper", force: true}} {
		t.Run(tc.name, func(t *testing.T) { f(t, tc.force) })
	}
}

func TestAddSSHKeyInstallsKey(t *testing.T) {
	sshTestModes(t, func(t *testing.T, forceHelper bool) {
		h, hostKeyPath := sshTestHarness(t, forceHelper)
		hostLine := sshTestKey(t, "bbraft-guest")
		sshTestWriteHostKey(t, hostKeyPath, hostLine+"\n")

		key := sshTestKey(t, "sandbox")
		acc := sshTestAdd(t, h, key)
		if acc.GetUser() != "user" {
			t.Fatalf("user = %q, want %q", acc.GetUser(), "user")
		}
		if acc.GetHostKey() != hostLine {
			t.Fatalf("host key = %q, want the ssh-keygen line %q", acc.GetHostKey(), hostLine)
		}
		authPath := sshTestFileName(h)
		if got, want := sshTestContent(t, authPath), key+"\n"; got != want {
			t.Fatalf("authorized_keys = %q, want %q", got, want)
		}
		if got := sshTestFileMode(t, filepath.Join(h.home, ".ssh")); got != 0o700 {
			t.Fatalf(".ssh mode = %#o, want 0700", got)
		}
		if got := sshTestFileMode(t, authPath); got != 0o600 {
			t.Fatalf("authorized_keys mode = %#o, want 0600", got)
		}
		st := fileStatT(t, authPath)
		if int(st.Uid) != h.uid || int(st.Gid) != h.gid {
			t.Fatalf("authorized_keys owner = %d:%d, want %d:%d", st.Uid, st.Gid, h.uid, h.gid)
		}
		if st2 := fileStatT(t, filepath.Join(h.home, ".ssh")); int(st2.Uid) != h.uid || int(st2.Gid) != h.gid {
			t.Fatalf(".ssh owner = %d:%d, want %d:%d", st2.Uid, st2.Gid, h.uid, h.gid)
		}

		// The same key with another comment and with padding is not added
		// again: idempotency compares the public material.
		for _, variant := range []string{
			sshTestBare(t, key),
			sshTestRecomment(t, key, "other comment"),
			"  " + sshTestBare(t, key) + "\t ",
		} {
			sshTestAdd(t, h, variant)
			if got := sshTestContent(t, authPath); got != key+"\n" {
				t.Fatalf("after re-adding the key, authorized_keys = %q", got)
			}
		}

		other := sshTestKey(t, "second")
		sshTestAdd(t, h, other)
		if got, want := sshTestContent(t, authPath), key+"\n"+other+"\n"; got != want {
			t.Fatalf("authorized_keys = %q, want %q", got, want)
		}
	})
}

func TestAddSSHKeyValidation(t *testing.T) {
	h, hostKeyPath := sshTestHarness(t, true)
	sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")
	key := sshTestKey(t, "sandbox")
	second := sshTestKey(t, "second")

	tests := []struct {
		name string
		in   string
	}{
		{name: "empty", in: ""},
		{name: "spaces", in: "   "},
		{name: "not a key", in: "not a key"},
		{name: "type only", in: "ssh-ed25519"},
		{name: "bad base64", in: "ssh-ed25519 !!!notbase64"},
		{name: "options", in: `command="rm -rf ~" ` + key},
		{name: "from option", in: `from="192.0.2.7" ` + key},
		{name: "two keys", in: key + "\n" + second},
		{name: "trailing newline", in: key + "\n"},
		{name: "carriage return", in: key + "\r"},
		{name: "nul byte", in: key + "\x00"},
		{name: "type and blob mismatch", in: "ssh-rsa " + strings.Fields(key)[1]},
		{name: "unsupported type", in: "ssh-dss " + strings.Fields(key)[1]},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: tc.in})
			wantCode(t, err, codes.InvalidArgument)
		})
	}
	if _, err := os.Lstat(filepath.Join(h.home, ".ssh")); !os.IsNotExist(err) {
		t.Fatalf("a rejected key created .ssh: %v", err)
	}
	sshTestAdd(t, h, key)
	if got, want := sshTestContent(t, sshTestFileName(h)), key+"\n"; got != want {
		t.Fatalf("authorized_keys = %q, want %q", got, want)
	}
}

// TestAddSSHKeyRestrictedLineNeverDuplicated is the required regression for
// restricted authorized_keys lines: the contract's idempotency compares the
// parsed public material, so an existing from=/command= line counts as the
// same key and must never gain an unrestricted duplicate. The whole file must
// stay byte-identical across request comment and whitespace variants.
func TestAddSSHKeyRestrictedLineNeverDuplicated(t *testing.T) {
	sshTestModes(t, func(t *testing.T, forceHelper bool) {
		h, hostKeyPath := sshTestHarness(t, forceHelper)
		sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")

		keyA := sshTestKey(t, "restricted-a")
		keyB := sshTestKey(t, "restricted-b")
		keyC := sshTestKey(t, "restricted-c")
		seed := `from="192.0.2.7" ` + keyA + " c1\n" +
			`command="/usr/bin/true" ` + keyB + " c2\n" +
			`from="192.0.2.7",command="/bin/echo hi" ` + keyC + " c3\n"
		sshTestSeed(t, h, seed, 0o700, 0o600)

		keys := []string{keyA, keyB, keyC}
		// The direct run carries every comment/whitespace variant; the helper
		// run adds each restricted key once, so it does not spawn a child per
		// variant.
		variantsFor := func(key string) []string {
			if forceHelper {
				return []string{sshTestBare(t, key)}
			}
			return []string{
				key,
				sshTestBare(t, key),
				sshTestRecomment(t, key, "another comment"),
				"  " + sshTestBare(t, key) + "  ",
				sshTestBare(t, key) + "\t tabbed",
			}
		}
		for _, key := range keys {
			for _, variant := range variantsFor(key) {
				sshTestAdd(t, h, variant)
				if got := sshTestContent(t, sshTestFileName(h)); got != seed {
					t.Fatalf("AddSSHKey(%q) changed the restricted file:\ngot  %q\nwant %q", variant, got, seed)
				}
			}
		}
		// Every restricted key — from-only, command-only and combined — is
		// still present exactly once and still restricted: no bare line was
		// added for any of them.
		content := sshTestContent(t, sshTestFileName(h))
		for _, key := range keys {
			blob := strings.Fields(key)[1]
			if got := strings.Count(content, blob); got != 1 {
				t.Fatalf("restricted key %q appears %d times, want 1:\n%s", blob, got, content)
			}
			for line := range strings.SplitSeq(content, "\n") {
				if strings.Contains(line, blob) &&
					!strings.HasPrefix(line, `from=`) && !strings.HasPrefix(line, `command=`) {
					t.Fatalf("unrestricted duplicate line added: %q", line)
				}
			}
		}

		// A genuinely new key still appends, leaving the restricted lines
		// untouched.
		fresh := sshTestKey(t, "fresh")
		sshTestAdd(t, h, fresh)
		if got, want := sshTestContent(t, sshTestFileName(h)), seed+fresh+"\n"; got != want {
			t.Fatalf("authorized_keys after a new key = %q, want %q", got, want)
		}
	})
}

func TestAddSSHKeyPreservesForeignLines(t *testing.T) {
	h, hostKeyPath := sshTestHarness(t, true)
	sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")
	other := sshTestKey(t, "foreign")
	seed := "# keep this comment\n\n" + other + " old\nnot a key line"
	sshTestSeed(t, h, seed, 0o700, 0o600)

	key := sshTestKey(t, "sandbox")
	sshTestAdd(t, h, key)
	want := seed + "\n" + key + "\n"
	if got := sshTestContent(t, sshTestFileName(h)); got != want {
		t.Fatalf("authorized_keys = %q, want %q", got, want)
	}
}

func TestAddSSHKeyTightensExistingModes(t *testing.T) {
	h, hostKeyPath := sshTestHarness(t, true)
	sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")
	key := sshTestKey(t, "sandbox")
	seed := key + "\n"
	sshTestSeed(t, h, seed, 0o755, 0o644)

	sshTestAdd(t, h, key)
	if got := sshTestContent(t, sshTestFileName(h)); got != seed {
		t.Fatalf("authorized_keys content changed to %q", got)
	}
	if got := sshTestFileMode(t, filepath.Join(h.home, ".ssh")); got != 0o700 {
		t.Fatalf(".ssh mode = %#o, want 0700", got)
	}
	if got := sshTestFileMode(t, sshTestFileName(h)); got != 0o600 {
		t.Fatalf("authorized_keys mode = %#o, want 0600", got)
	}
}

func TestAddSSHKeyRejectsEntries(t *testing.T) {
	key := sshTestKey(t, "sandbox")
	other := sshTestKey(t, "other")

	t.Run("ssh path is a file", func(t *testing.T) {
		h, hostKeyPath := sshTestHarness(t, false)
		sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")
		fileWrite(t, h.home, ".ssh", "not a directory", 0o600)
		_, err := h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: key})
		wantCode(t, err, codes.InvalidArgument)
	})

	t.Run("ssh path is a symlink", func(t *testing.T) {
		h, hostKeyPath := sshTestHarness(t, false)
		sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")
		realDir := filepath.Join(h.home, "real-ssh")
		if err := os.Mkdir(realDir, 0o700); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(h.home, ".ssh")
		// Relative target inside home, so the logical-root walker sees the
		// same live directory a real user would create.
		if err := os.Symlink("real-ssh", link); err != nil {
			t.Fatal(err)
		}
		linkInfo, err := os.Stat(link)
		if err != nil {
			t.Fatal(err)
		}
		targetInfo, err := os.Stat(realDir)
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(linkInfo, targetInfo) {
			t.Fatalf(".ssh symlink does not point at the live fixture directory")
		}
		_, err = h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: key})
		wantCode(t, err, codes.InvalidArgument)
		if fi, err := os.Lstat(link); err != nil || fi.Mode()&fs.ModeSymlink == 0 {
			t.Fatalf(".ssh symlink was replaced: %v, %v", fi, err)
		}
	})

	t.Run("authorized_keys is a symlink", func(t *testing.T) {
		h, hostKeyPath := sshTestHarness(t, false)
		sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")
		if err := os.Mkdir(filepath.Join(h.home, ".ssh"), 0o700); err != nil {
			t.Fatal(err)
		}
		target := fileWrite(t, h.home, "keys.txt", other+"\n", 0o600)
		link := sshTestFileName(h)
		// Relative target one level up: the link lives in .ssh, the file in
		// home, so the logical-root walker resolves a live existing file.
		if err := os.Symlink("../keys.txt", link); err != nil {
			t.Fatal(err)
		}
		linkInfo, err := os.Stat(link)
		if err != nil {
			t.Fatal(err)
		}
		targetInfo, err := os.Stat(target)
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(linkInfo, targetInfo) {
			t.Fatal("authorized_keys symlink does not resolve to the seeded target")
		}
		before := fileStatT(t, link)
		_, err = h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: key})
		wantCode(t, err, codes.InvalidArgument)
		if got := sshTestContent(t, target); got != other+"\n" {
			t.Fatalf("symlink target changed to %q", got)
		}
		after, err := os.Lstat(link)
		if err != nil {
			t.Fatal(err)
		}
		if after.Mode()&fs.ModeSymlink == 0 {
			t.Fatal("authorized_keys symlink was replaced")
		}
		if got := fileStatT(t, link); got.Ino != before.Ino {
			t.Fatalf("authorized_keys link inode changed from %d to %d", before.Ino, got.Ino)
		}
	})

	t.Run("authorized_keys is a dangling symlink", func(t *testing.T) {
		h, hostKeyPath := sshTestHarness(t, false)
		sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")
		if err := os.Mkdir(filepath.Join(h.home, ".ssh"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("missing-target", sshTestFileName(h)); err != nil {
			t.Fatal(err)
		}
		_, err := h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: key})
		wantCode(t, err, codes.InvalidArgument)
		if fi, err := os.Lstat(sshTestFileName(h)); err != nil || fi.Mode()&fs.ModeSymlink == 0 {
			t.Fatalf("dangling authorized_keys symlink was replaced: %v, %v", fi, err)
		}
	})

	t.Run("authorized_keys is a directory", func(t *testing.T) {
		h, hostKeyPath := sshTestHarness(t, false)
		sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")
		if err := os.MkdirAll(sshTestFileName(h), 0o700); err != nil {
			t.Fatal(err)
		}
		_, err := h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: key})
		wantCode(t, err, codes.InvalidArgument)
	})

	t.Run("authorized_keys is too large", func(t *testing.T) {
		h, hostKeyPath := sshTestHarness(t, false)
		sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")
		seed := strings.Repeat("# pad\n", (4<<20)/2+1)
		sshTestSeed(t, h, seed, 0o700, 0o600)
		_, err := h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: key})
		wantCode(t, err, codes.OutOfRange)
		if got := sshTestContent(t, sshTestFileName(h)); got != seed {
			t.Fatal("an oversized file was rewritten")
		}
	})
}

func TestAddSSHKeyHostKeyStates(t *testing.T) {
	h, hostKeyPath := sshTestHarness(t, false)
	key := sshTestKey(t, "sandbox")

	// No host key yet: UNAVAILABLE, but the key is installed (install first,
	// so the retry that finds the key is idempotent).
	_, err := h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: key})
	wantCode(t, err, codes.Unavailable)
	if got, want := sshTestContent(t, sshTestFileName(h)), key+"\n"; got != want {
		t.Fatalf("authorized_keys = %q, want the key installed", got)
	}

	// A partially written or foreign host key file is retryable, not
	// internal, and its bytes never appear in the error.
	const marker = "HOSTFILE-MARKER"
	sshTestWriteHostKey(t, hostKeyPath, "ssh-ed25519 "+marker)
	_, err = h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: key})
	wantCode(t, err, codes.Unavailable)
	if strings.Contains(err.Error(), marker) {
		t.Fatalf("error echoes host key file bytes: %v", err)
	}

	// A symlinked host key path is never followed.
	target := filepath.Join(t.TempDir(), "target.pub")
	sshTestWriteHostKey(t, target, sshTestKey(t, "bbraft-guest")+"\n")
	if err := os.Remove(hostKeyPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, hostKeyPath); err != nil {
		t.Fatal(err)
	}
	_, err = h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: key})
	wantCode(t, err, codes.Internal)

	// The real shape succeeds and is returned canonically.
	hostLine := sshTestKey(t, "bbraft-guest")
	if err := os.Remove(hostKeyPath); err != nil {
		t.Fatal(err)
	}
	sshTestWriteHostKey(t, hostKeyPath, hostLine+"\n")
	acc := sshTestAdd(t, h, key)
	if acc.GetHostKey() != hostLine {
		t.Fatalf("host key = %q, want %q", acc.GetHostKey(), hostLine)
	}
}

func TestAddSSHKeyConcurrent(t *testing.T) {
	h, hostKeyPath := sshTestHarness(t, true)
	sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")
	key := sshTestKey(t, "duplicate")

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: key}); err != nil {
				t.Errorf("duplicate AddSSHKey: %v", err)
			}
		})
	}
	wg.Wait()
	if got, want := sshTestContent(t, sshTestFileName(h)), key+"\n"; got != want {
		t.Fatalf("authorized_keys = %q, want one copy of the key", got)
	}

	keys := make([]string, 8)
	for i := range keys {
		keys[i] = sshTestKey(t, "distinct")
	}
	for _, k := range keys {
		wg.Go(func() {
			if _, err := h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: k}); err != nil {
				t.Errorf("distinct AddSSHKey: %v", err)
			}
		})
	}
	wg.Wait()
	content := sshTestContent(t, sshTestFileName(h))
	if got := strings.Count(content, "\n"); got != len(keys)+1 {
		t.Fatalf("authorized_keys has %d lines, want %d:\n%s", got, len(keys)+1, content)
	}
	for _, k := range keys {
		if !strings.Contains(content, k+"\n") {
			t.Fatalf("key %q was lost:\n%s", k, content)
		}
	}
}

func TestAddSSHKeyCanceled(t *testing.T) {
	sshTestModes(t, func(t *testing.T, forceHelper bool) {
		h, hostKeyPath := sshTestHarness(t, forceHelper)
		sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := h.c.AddSSHKey(ctx, &guestv1.AddSSHKeyRequest{PublicKey: sshTestKey(t, "sandbox")})
		wantCode(t, err, codes.Canceled)
		if _, err := os.Lstat(sshTestFileName(h)); !os.IsNotExist(err) {
			t.Fatalf("a canceled call published authorized_keys: %v", err)
		}
	})
}

func TestAddSSHKeyChunkedWrite(t *testing.T) {
	sshTestModes(t, func(t *testing.T, forceHelper bool) {
		h, hostKeyPath := sshTestHarness(t, forceHelper)
		sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")
		seed := strings.Repeat("# pad line\n", 120_000) // 1.32 MiB: two chunks
		if len(seed) <= 1<<20 {
			t.Fatalf("test seed is only %d bytes", len(seed))
		}
		sshTestSeed(t, h, seed, 0o700, 0o600)
		key := sshTestKey(t, "sandbox")
		sshTestAdd(t, h, key)
		got := sshTestContent(t, sshTestFileName(h))
		want := seed + key + "\n"
		if got != want {
			t.Fatalf("authorized_keys is %d bytes with %d matching=%v, want %d bytes",
				len(got), len(want), bytes.Equal([]byte(got), []byte(want)), len(want))
		}
	})
}

// sshTestAuthorizedKeysLimit mirrors the agent's total authorized_keys bound.
// The actual constant is unexported; a divergence makes the boundary fixture
// fail loudly instead of silently testing the wrong size.
const sshTestAuthorizedKeysLimit = 4 << 20

// TestAddSSHKeyTotalSizeBound proves the bound applies to the rewritten file,
// not only to reading it: a duplicate at the exact bound still succeeds, while
// an append that would cross it is refused with OUT_OF_RANGE and leaves every
// byte unchanged instead of writing a file its own reader would then reject.
func TestAddSSHKeyTotalSizeBound(t *testing.T) {
	sshTestModes(t, func(t *testing.T, forceHelper bool) {
		h, hostKeyPath := sshTestHarness(t, forceHelper)
		sshTestWriteHostKey(t, hostKeyPath, sshTestKey(t, "bbraft-guest")+"\n")

		key := sshTestKey(t, "sandbox")
		padding := strings.Repeat("#", sshTestAuthorizedKeysLimit-len(key)-2)
		seed := padding + "\n" + key + "\n"
		if len(seed) != sshTestAuthorizedKeysLimit {
			t.Fatalf("fixture is %d bytes, want exactly %d", len(seed), sshTestAuthorizedKeysLimit)
		}
		sshTestSeed(t, h, seed, 0o700, 0o600)

		// Duplicate at the exact bound: already authorized, so no rewrite and
		// no bound error.
		sshTestAdd(t, h, key)
		if got := sshTestContent(t, sshTestFileName(h)); got != seed {
			t.Fatalf("duplicate at the bound changed the file (%d bytes, want %d)", len(got), len(seed))
		}

		// A new key would cross the bound: refused, bytes untouched.
		other := sshTestKey(t, "overflow")
		_, err := h.c.AddSSHKey(t.Context(), &guestv1.AddSSHKeyRequest{PublicKey: other})
		wantCode(t, err, codes.OutOfRange)
		if got := sshTestContent(t, sshTestFileName(h)); got != seed {
			t.Fatalf("an overflowing append rewrote the file (%d bytes, want %d)", len(got), len(seed))
		}
		if strings.Contains(sshTestContent(t, sshTestFileName(h)), sshTestBare(t, other)) {
			t.Fatal("an overflowing append published the new key")
		}
	})
}
