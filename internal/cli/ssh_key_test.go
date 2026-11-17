package cli

import (
	"bytes"
	"encoding/pem"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
)

// sshTestKeyMode returns a file's permission bits.
func sshTestKeyMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Mode().Perm()
}

// TestEnsureSSHKeyFreshCreatesSafeKey covers first-use generation and modes.
func TestEnsureSSHKeyFreshCreatesSafeKey(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".ssh")
	material, err := ensureSSHKey(dir)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if material.PrivatePath != filepath.Join(dir, sshKeyFileName) {
		t.Fatalf("private path = %q", material.PrivatePath)
	}
	if !strings.HasPrefix(material.PublicKey, "ssh-ed25519 ") || !strings.HasSuffix(material.PublicKey, " "+sshKeyComment) {
		t.Fatalf("public key = %q", material.PublicKey)
	}
	if mode := sshTestKeyMode(t, material.PrivatePath); mode != 0o600 {
		t.Fatalf("private mode = %o, want 600", mode)
	}
	if mode := sshTestKeyMode(t, filepath.Join(dir, sshKeyPubName)); mode != 0o644 {
		t.Fatalf("public mode = %o, want 644", mode)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("dir mode = %o, want 700", info.Mode().Perm())
	}
	raw, err := os.ReadFile(material.PrivatePath)
	if err != nil {
		t.Fatalf("read private: %v", err)
	}
	_, parsedPublic, err := parseManagedSSHKey(raw)
	if err != nil {
		t.Fatalf("parse generated key: %v", err)
	}
	if parsedPublic != material.PublicKey {
		t.Fatalf("parsed public = %q, want %q", parsedPublic, material.PublicKey)
	}
}

// TestEnsureSSHKeyReusesAndDerivesPublicKey proves reuse never regenerates and
// that a missing .pub is rebuilt from the private key.
func TestEnsureSSHKeyReusesAndDerivesPublicKey(t *testing.T) {
	dir := t.TempDir()
	first, err := ensureSSHKey(dir)
	if err != nil {
		t.Fatalf("first ensure: %v", err)
	}
	before, err := os.ReadFile(first.PrivatePath)
	if err != nil {
		t.Fatalf("read private: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, sshKeyPubName)); err != nil {
		t.Fatalf("remove pub: %v", err)
	}
	second, err := ensureSSHKey(dir)
	if err != nil {
		t.Fatalf("second ensure: %v", err)
	}
	after, err := os.ReadFile(second.PrivatePath)
	if err != nil {
		t.Fatalf("reread private: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("reuse changed the private key")
	}
	if second.PublicKey != first.PublicKey {
		t.Fatalf("public key changed: %q -> %q", first.PublicKey, second.PublicKey)
	}
	if got := sshTestReadFile(t, filepath.Join(dir, sshKeyPubName)); got != first.PublicKey+"\n" {
		t.Fatalf(".pub = %q, want derived public key", got)
	}
}

// TestEnsureSSHKeyConcurrentCreation proves one complete winner and no temp
// files when several invocations start together.
func TestEnsureSSHKeyConcurrentCreation(t *testing.T) {
	dir := t.TempDir()
	const workers = 8
	results := make([]sshKeyMaterial, workers)
	errs := make([]error, workers)
	var wait sync.WaitGroup
	for i := range workers {
		wait.Go(func() {
			results[i], errs[i] = ensureSSHKey(dir)
		})
	}
	wait.Wait()
	for i := range workers {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		if results[i].PublicKey != results[0].PublicKey {
			t.Fatalf("worker %d public key = %q, want %q", i, results[i].PublicKey, results[0].PublicKey)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".bbraft_ed25519.tmp-") {
			t.Fatalf("temporary key left behind: %s", entry.Name())
		}
	}
	if len(entries) != 2 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("dir entries = %v, want private key and .pub", names)
	}
	if mode := sshTestKeyMode(t, filepath.Join(dir, sshKeyFileName)); mode != 0o600 {
		t.Fatalf("private mode = %o", mode)
	}
}

// TestEnsureSSHKeyInjectedWriteFailure removes the temp and leaves no key.
func TestEnsureSSHKeyInjectedWriteFailure(t *testing.T) {
	dir := t.TempDir()
	files := sshKeyFiles{createTemp: func(root *os.Root) (string, *os.File, error) {
		name := ".bbraft_ed25519.tmp-injected"
		file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return "", nil, err
		}
		if err := file.Close(); err != nil {
			return "", nil, err
		}
		return name, file, nil
	}}
	_, err := ensureSSHKeyFiles(dir, files)
	if err == nil {
		t.Fatal("ensure succeeded with a failing writer")
	}
	cliErr, ok := errors.AsType[*CLIError](err)
	if !ok || cliErr.Code != "ssh_key_error" {
		t.Fatalf("error = %v, want ssh_key_error", err)
	}
	for _, name := range []string{sshKeyFileName, ".bbraft_ed25519.tmp-injected"} {
		if _, statErr := os.Lstat(filepath.Join(dir, name)); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("%s exists after failure (err %v)", name, statErr)
		}
	}
}

// TestEnsureSSHKeyRejectsForeignFiles never reads, chmods or overwrites a
// non-regular, foreign or corrupt key path.
func TestEnsureSSHKeyRejectsForeignFiles(t *testing.T) {
	t.Run("symlink", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "target-key")
		if err := os.WriteFile(target, []byte("not a key"), 0o644); err != nil {
			t.Fatalf("write target: %v", err)
		}
		if err := os.Symlink("target-key", filepath.Join(dir, sshKeyFileName)); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		if _, err := ensureSSHKey(dir); err == nil {
			t.Fatal("ensure followed a symlink")
		}
		if got := sshTestReadFile(t, target); got != "not a key" {
			t.Fatalf("symlink target changed: %q", got)
		}
		if mode := sshTestKeyMode(t, target); mode != 0o644 {
			t.Fatalf("symlink target mode = %o, want untouched 644", mode)
		}
	})
	t.Run("fifo", func(t *testing.T) {
		dir := t.TempDir()
		if err := syscall.Mkfifo(filepath.Join(dir, sshKeyFileName), 0o600); err != nil {
			t.Fatalf("mkfifo: %v", err)
		}
		if _, err := ensureSSHKey(dir); err == nil {
			t.Fatal("ensure accepted a FIFO")
		}
	})
	t.Run("corrupt", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, sshKeyFileName)
		if err := os.WriteFile(path, []byte("garbage"), 0o600); err != nil {
			t.Fatalf("write corrupt: %v", err)
		}
		_, err := ensureSSHKey(dir)
		cliErr, ok := errors.AsType[*CLIError](err)
		if !ok || cliErr.Code != "ssh_key_invalid" {
			t.Fatalf("error = %v, want ssh_key_invalid", err)
		}
		if got := sshTestReadFile(t, path); got != "garbage" {
			t.Fatalf("corrupt key was overwritten: %q", got)
		}
	})
	t.Run("encrypted", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, sshKeyFileName)
		if err := os.WriteFile(path, sshTestEncryptedKeyPEM(), 0o600); err != nil {
			t.Fatalf("write encrypted: %v", err)
		}
		_, err := ensureSSHKey(dir)
		cliErr, ok := errors.AsType[*CLIError](err)
		if !ok || cliErr.Code != "ssh_key_invalid" {
			t.Fatalf("error = %v, want ssh_key_invalid", err)
		}
	})
	t.Run("non ed25519 pem", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, sshKeyFileName)
		foreign := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: []byte("foreign")})
		if err := os.WriteFile(path, foreign, 0o600); err != nil {
			t.Fatalf("write foreign: %v", err)
		}
		if _, err := ensureSSHKey(dir); err == nil {
			t.Fatal("ensure accepted a foreign key file")
		}
	})
}

// TestEnsureSSHKeyRepairsOnlyManagedPermissions chmods an owned regular key
// back to 0600.
func TestEnsureSSHKeyRepairsOnlyManagedPermissions(t *testing.T) {
	dir := t.TempDir()
	material, err := ensureSSHKey(dir)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := os.Chmod(material.PrivatePath, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if _, err := ensureSSHKey(dir); err != nil {
		t.Fatalf("reuse: %v", err)
	}
	if mode := sshTestKeyMode(t, material.PrivatePath); mode != 0o600 {
		t.Fatalf("private mode = %o, want repaired 600", mode)
	}
}

// TestEnsureSSHKeyNeverOverwritesForeignPublicKey leaves an existing .pub
// untouched.
func TestEnsureSSHKeyNeverOverwritesForeignPublicKey(t *testing.T) {
	dir := t.TempDir()
	material, err := ensureSSHKey(dir)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	pubPath := filepath.Join(dir, sshKeyPubName)
	if err := os.WriteFile(pubPath, []byte("foreign public key\n"), 0o644); err != nil {
		t.Fatalf("write foreign pub: %v", err)
	}
	reused, err := ensureSSHKey(dir)
	if err != nil {
		t.Fatalf("reuse: %v", err)
	}
	if reused.PublicKey != material.PublicKey {
		t.Fatalf("public key changed: %q", reused.PublicKey)
	}
	if got := sshTestReadFile(t, pubPath); got != "foreign public key\n" {
		t.Fatalf("foreign pub overwritten: %q", got)
	}
}

// TestEnsureSSHKeyReadsCompleteMultiLinkWindow accepts a complete 0600 key
// while a concurrent publisher still has its temp hard link. This is the
// deterministic window between Link(temp, final) and Remove(temp).
func TestEnsureSSHKeyReadsCompleteMultiLinkWindow(t *testing.T) {
	dir := t.TempDir()
	first, err := ensureSSHKey(dir)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	tempName := filepath.Join(dir, ".bbraft_ed25519.tmp-window")
	if err := os.Link(filepath.Join(dir, sshKeyFileName), tempName); err != nil {
		t.Fatalf("hard link window: %v", err)
	}
	defer os.Remove(tempName)
	info, err := os.Lstat(filepath.Join(dir, sshKeyFileName))
	if err != nil {
		t.Fatalf("stat key: %v", err)
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || stat.Nlink != 2 {
		t.Fatalf("fixture link count = %v, want 2", info.Sys())
	}
	second, err := ensureSSHKey(dir)
	if err != nil {
		t.Fatalf("complete 0600 key in the publication window was rejected: %v", err)
	}
	if second.PublicKey != first.PublicKey {
		t.Fatalf("public key = %q, want %q", second.PublicKey, first.PublicKey)
	}
	if mode := sshTestKeyMode(t, filepath.Join(dir, sshKeyFileName)); mode != 0o600 {
		t.Fatalf("mode = %o, want unchanged 600", mode)
	}
}

// TestEnsureSSHKeyRefusesPermissiveMultiLinkRepair never chmods through a
// second name and reports the refusal, leaving the mode unchanged.
func TestEnsureSSHKeyRefusesPermissiveMultiLinkRepair(t *testing.T) {
	dir := t.TempDir()
	material, err := ensureSSHKey(dir)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	tempName := filepath.Join(dir, ".bbraft_ed25519.tmp-window")
	if err := os.Link(material.PrivatePath, tempName); err != nil {
		t.Fatalf("hard link window: %v", err)
	}
	defer os.Remove(tempName)
	if err := os.Chmod(material.PrivatePath, 0o644); err != nil {
		t.Fatalf("chmod permissive: %v", err)
	}
	_, err = ensureSSHKey(dir)
	cliErr, ok := errors.AsType[*CLIError](err)
	if !ok || cliErr.Code != "ssh_key_invalid" {
		t.Fatalf("error = %v, want ssh_key_invalid refusal", err)
	}
	for _, name := range []string{sshKeyFileName, filepath.Base(tempName)} {
		if mode := sshTestKeyMode(t, filepath.Join(dir, name)); mode != 0o644 {
			t.Fatalf("%s mode = %o, want untouched 644", name, mode)
		}
	}
}

// TestEnsureSSHKeyRepairsExtraBitsToExact0600 clears setuid and other extra
// private bits rather than silently retaining them.
func TestEnsureSSHKeyRepairsExtraBitsToExact0600(t *testing.T) {
	dir := t.TempDir()
	material, err := ensureSSHKey(dir)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := os.Chmod(material.PrivatePath, os.ModeSetuid|0o600); err != nil {
		t.Fatalf("chmod setuid: %v", err)
	}
	if _, err := ensureSSHKey(dir); err != nil {
		t.Fatalf("reuse: %v", err)
	}
	info, err := os.Lstat(material.PrivatePath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	const mask = os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky
	if got := info.Mode() & mask; got != 0o600 {
		t.Fatalf("key mode = %v, want exact 0600 (no extra bits)", got)
	}
}

// sshTestEncryptedKeyPEM builds an OpenSSH key that declares encryption.
func sshTestEncryptedKeyPEM() []byte {
	var writer sshWireWriter
	writer.buf.WriteString(sshKeyMagic)
	writer.stringBytes([]byte("aes256-ctr"))
	writer.stringBytes([]byte("bcrypt"))
	writer.stringBytes([]byte("salt"))
	writer.uint32(1)
	writer.stringBytes([]byte("public"))
	writer.stringBytes([]byte("private"))
	return pem.EncodeToMemory(&pem.Block{Type: sshKeyPEMType, Bytes: writer.bytes()})
}

// TestParseManagedSSHKeyAcceptsSSHKeygen keeps compatibility with a real key
// generated by the local OpenSSH tool. It never contacts a host.
func TestParseManagedSSHKeyAcceptsSSHKeygen(t *testing.T) {
	sshKeygen, err := exec.LookPath("ssh-keygen")
	if err != nil {
		t.Skip("ssh-keygen is not installed")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "key")
	command := exec.CommandContext(t.Context(), sshKeygen, "-q", "-t", "ed25519", "-N", "", "-C", "fixture", "-f", path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("ssh-keygen: %v: %s", err, output)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read key: %v", err)
	}
	_, publicLine, err := parseManagedSSHKey(raw)
	if err != nil {
		t.Fatalf("parse ssh-keygen key: %v", err)
	}
	pub, err := os.ReadFile(path + ".pub")
	if err != nil {
		t.Fatalf("read pub: %v", err)
	}
	fields := strings.Fields(string(pub))
	if len(fields) < 2 || !strings.HasPrefix(publicLine, fields[0]+" "+fields[1]) {
		t.Fatalf("derived public line %q does not match ssh-keygen output", publicLine)
	}
}

// TestParseManagedSSHKeyMalformedMatchesSSHKeygen proves a fixture our parser
// rejects is also rejected by the local OpenSSH tool. No key bytes are printed.
func TestParseManagedSSHKeyMalformedMatchesSSHKeygen(t *testing.T) {
	sshKeygen, err := exec.LookPath("ssh-keygen")
	if err != nil {
		t.Skip("ssh-keygen is not installed")
	}
	dir := t.TempDir()
	material, err := ensureSSHKey(dir)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	raw, err := os.ReadFile(material.PrivatePath)
	if err != nil {
		t.Fatalf("read key: %v", err)
	}
	block, rest := pem.Decode(raw)
	if block == nil || len(bytes.TrimSpace(rest)) != 0 {
		t.Fatal("test key did not decode")
	}
	cases := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{name: "truncated", mutate: func(blob []byte) []byte { return blob[:len(blob)-1] }},
		{name: "corrupt padding", mutate: func(blob []byte) []byte {
			clone := bytes.Clone(blob)
			clone[len(clone)-1] ^= 0xff
			return clone
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := pem.EncodeToMemory(&pem.Block{Type: sshKeyPEMType, Bytes: testCase.mutate(block.Bytes)})
			if _, _, err := parseManagedSSHKey(fixture); err == nil {
				t.Fatal("malformed key was accepted")
			}
			path := filepath.Join(t.TempDir(), "bad-key")
			if err := os.WriteFile(path, fixture, 0o600); err != nil {
				t.Fatalf("write fixture: %v", err)
			}
			command := exec.CommandContext(t.Context(), sshKeygen, "-y", "-f", path)
			if output, err := command.CombinedOutput(); err == nil {
				t.Fatalf("ssh-keygen accepted the fixture: %s", output)
			}
		})
	}
}
