package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestWriteTempKnownHosts creates one private pin file and removes it.
func TestWriteTempKnownHosts(t *testing.T) {
	key, err := parseSSHHostKey(sshTestHostKey(5))
	if err != nil {
		t.Fatalf("parse host key: %v", err)
	}
	path, cleanup, err := writeTempKnownHosts(key, sshTestSandboxID)
	if err != nil {
		t.Fatalf("write known hosts: %v", err)
	}
	if filepath.Dir(path) != os.TempDir() {
		t.Fatalf("known hosts dir = %q, want %q", filepath.Dir(path), os.TempDir())
	}
	if mode := sshTestKeyMode(t, path); mode != 0o600 {
		t.Fatalf("known hosts mode = %o, want 600", mode)
	}
	content := sshTestReadFile(t, path)
	if !strings.HasPrefix(content, sshTestSandboxID+" ssh-ed25519 ") || !strings.HasSuffix(content, "\n") {
		t.Fatalf("known hosts content = %q", content)
	}
	if strings.Count(content, "\n") != 1 {
		t.Fatalf("known hosts content has extra lines: %q", content)
	}
	cleanup()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("cleanup left %s (err %v)", path, err)
	}
}

// TestWriteTempKnownHostsIsolated proves concurrent invocations cannot share or
// lose a pin.
func TestWriteTempKnownHostsIsolated(t *testing.T) {
	firstKey, err := parseSSHHostKey(sshTestHostKey(6))
	if err != nil {
		t.Fatalf("parse first: %v", err)
	}
	secondKey, err := parseSSHHostKey(sshTestHostKey(7))
	if err != nil {
		t.Fatalf("parse second: %v", err)
	}
	firstPath, firstCleanup, err := writeTempKnownHosts(firstKey, sshTestSandboxID)
	if err != nil {
		t.Fatalf("write first: %v", err)
	}
	defer firstCleanup()
	secondPath, secondCleanup, err := writeTempKnownHosts(secondKey, sshTestSandboxID2)
	if err != nil {
		t.Fatalf("write second: %v", err)
	}
	defer secondCleanup()
	if firstPath == secondPath {
		t.Fatal("concurrent invocations shared one known_hosts file")
	}
	if !strings.HasPrefix(sshTestReadFile(t, firstPath), sshTestSandboxID+" ") {
		t.Fatalf("first file lost its alias: %q", sshTestReadFile(t, firstPath))
	}
	if !strings.HasPrefix(sshTestReadFile(t, secondPath), sshTestSandboxID2+" ") {
		t.Fatalf("second file lost its alias: %q", sshTestReadFile(t, secondPath))
	}
	if sshTestReadFile(t, firstPath) == sshTestReadFile(t, secondPath) {
		t.Fatal("both known_hosts files contain the same alias/key")
	}
}

// TestSSHConfigValue escapes a value for one -o argv element.
func TestSSHConfigValue(t *testing.T) {
	cases := []struct {
		value string
		want  string
	}{
		{value: "/tmp/known_hosts", want: `"/tmp/known_hosts"`},
		{value: "/tmp/a b/known_hosts", want: `"/tmp/a b/known_hosts"`},
		{value: `/tmp/a"b`, want: `"/tmp/a\"b"`},
		{value: `/tmp/a\b`, want: `"/tmp/a\\b"`},
	}
	for _, testCase := range cases {
		if got := sshConfigValue(testCase.value); got != testCase.want {
			t.Errorf("sshConfigValue(%q) = %q, want %q", testCase.value, got, testCase.want)
		}
	}
}

// TestSSHConfigQuotingWithSSHConfig asks the local OpenSSH config parser to
// prove the quoted space path and the pin-isolation options survive as one
// effective config. It never opens a connection.
func TestSSHConfigQuotingWithSSHConfig(t *testing.T) {
	sshPath, err := exec.LookPath("ssh")
	if err != nil {
		t.Skip("ssh is not installed")
	}
	path := "/tmp/p1 b/known hosts"
	args := append([]string{"-G"}, sshPinArgs(path, sshTestSandboxID)...)
	args = append(args, "example.invalid")
	command := exec.CommandContext(t.Context(), sshPath, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("ssh -G: %v: %s", err, output)
	}
	for _, want := range []string{
		"userknownhostsfile " + path,
		"globalknownhostsfile /dev/null",
		"stricthostkeychecking true",
		"identitiesonly yes",
		"checkhostip no",
		"verifyhostkeydns false",
		"updatehostkeys false",
		"hostkeyalias " + sshTestSandboxID,
	} {
		if !strings.Contains(string(output), want) {
			t.Errorf("ssh -G output is missing %q:\n%s", want, output)
		}
	}
}
