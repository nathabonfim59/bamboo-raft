package config

import (
	"crypto/subtle"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// keyEqual compares key bytes in constant time (AGENTS.md secrets rule);
// it compares known test keys only.
func keyEqual(a, b []byte) bool {
	return subtle.ConstantTimeCompare(a, b) == 1
}

// writeKey writes raw key bytes to a file in the fixture directory.
func writeKey(t *testing.T, f fixture, name string, key []byte) string {
	t.Helper()
	p := filepath.Join(f.dir, name)
	if err := os.WriteFile(p, key, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// testKey returns a deterministic 32-byte key with whitespace bytes in
// it, to prove the file is read raw.
func testKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i*9 + 1)
	}
	key[3] = ' '
	key[7] = '\n'
	key[11] = '\t'
	key[15] = '\r'
	key[31] = ' '
	return key
}

func TestSecretsMasterKeyFile(t *testing.T) {
	f := newFixture(t)
	key := testKey()
	p := writeKey(t, f, "master.key", key)
	args := append(f.fileArgs(), "-secrets.master-key-file", p)
	c, err := Load(t.Context(), args, noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if !keyEqual(c.Secrets.MasterKey[:], key) {
		t.Fatal("master key differs from key file")
	}
	if c.Secrets.MasterKeyFile != p {
		t.Fatalf("MasterKeyFile = %q, want %q", c.Secrets.MasterKeyFile, p)
	}
	if c.Secrets.Ephemeral {
		t.Fatal("file key marked ephemeral")
	}
	// Whitespace is key material: the trailing space and inner newline
	// must survive, so a trimmed read would mismatch.
	if c.Secrets.MasterKey[31] != ' ' || c.Secrets.MasterKey[7] != '\n' {
		t.Fatal("key file was trimmed")
	}
}

func TestSecretsMasterKeyEnv(t *testing.T) {
	f := newFixture(t)
	key := testKey()
	env := testEnv(map[string]string{EnvSecretsMasterKey: hex.EncodeToString(key)})
	c, err := Load(t.Context(), f.fileArgs(), env)
	if err != nil {
		t.Fatal(err)
	}
	if !keyEqual(c.Secrets.MasterKey[:], key) {
		t.Fatal("master key differs from env hex")
	}
	if c.Secrets.Ephemeral || c.Secrets.MasterKeyFile != "" {
		t.Fatalf("env key: ephemeral=%v file=%q", c.Secrets.Ephemeral, c.Secrets.MasterKeyFile)
	}
}

func TestSecretsKeyfileEnvFallback(t *testing.T) {
	f := newFixture(t)
	key := testKey()
	p := writeKey(t, f, "master.key", key)
	env := testEnv(map[string]string{"BBRAFT_SECRETS_MASTER_KEY_FILE": p})
	c, err := Load(t.Context(), f.fileArgs(), env)
	if err != nil {
		t.Fatal(err)
	}
	if !keyEqual(c.Secrets.MasterKey[:], key) {
		t.Fatal("master key differs from key file")
	}
}

func TestSecretsMalformed(t *testing.T) {
	f := newFixture(t)
	good31 := writeKey(t, f, "short.key", make([]byte, 31))
	good33 := writeKey(t, f, "long.key", make([]byte, 33))
	empty := writeKey(t, f, "empty.key", nil)
	// Sentinel secrets: the errors must name the source but never echo
	// the value.
	sentinel := "K3ySentinel" + strings.Repeat("z", 53)
	tests := []struct {
		name     string
		args     []string
		env      map[string]string
		want     string // substring of the error
		sentinel string // must not appear in the error
	}{
		{"file 31 bytes", append(f.fileArgs(), "-secrets.master-key-file", good31), nil, "exactly 32", ""},
		{"file 33 bytes", append(f.fileArgs(), "-secrets.master-key-file", good33), nil, "exactly 32", ""},
		{"file empty", append(f.fileArgs(), "-secrets.master-key-file", empty), nil, "exactly 32", ""},
		{"file missing", append(f.fileArgs(), "-secrets.master-key-file", filepath.Join(f.dir, "gone.key")), nil, "gone.key", ""},
		{"env 63 hex", f.fileArgs(), map[string]string{EnvSecretsMasterKey: strings.Repeat("ab", 31) + "a"}, "64 hexadecimal", ""},
		{"env 65 hex", f.fileArgs(), map[string]string{EnvSecretsMasterKey: strings.Repeat("ab", 32) + "c"}, "64 hexadecimal", ""},
		{"env not hex", f.fileArgs(), map[string]string{EnvSecretsMasterKey: sentinel}, "not valid hexadecimal", sentinel},
		{"env empty", f.fileArgs(), map[string]string{EnvSecretsMasterKey: ""}, "64 hexadecimal", ""},
		{"flag file + env", append(f.fileArgs(), "-secrets.master-key-file", good31), map[string]string{EnvSecretsMasterKey: strings.Repeat("ab", 32)}, "both set", strings.Repeat("ab", 32)},
		{"env file + env", f.fileArgs(), map[string]string{"BBRAFT_SECRETS_MASTER_KEY_FILE": good31, EnvSecretsMasterKey: strings.Repeat("ab", 32)}, "both set", strings.Repeat("ab", 32)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := noEnv
			if tc.env != nil {
				env = testEnv(tc.env)
			}
			_, err := Load(t.Context(), tc.args, env)
			if err == nil {
				t.Fatalf("accepted args %v env %v", tc.args, tc.env)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q misses %q", err, tc.want)
			}
			if tc.sentinel != "" && strings.Contains(err.Error(), tc.sentinel) {
				t.Fatalf("error %q discloses the key %q", err, tc.sentinel)
			}
		})
	}
}

func TestSecretsRequiredEveryRole(t *testing.T) {
	f := newFixture(t)
	tests := []struct {
		name string
		args []string
	}{
		{"all", f.fileArgs()},
		{"api", []string{"-roles", "api", "-auth.keys-file", f.keysFile, "-database.url-file", f.dbURLFile, "-node.gateway-token-file", f.gwFile}},
		{"nodehub", []string{"-roles", "nodehub", "-node.tokens-file", f.tokensFile, "-database.url-file", f.dbURLFile, "-node.gateway-token-file", f.gwFile}},
		{"lifecycle", []string{"-roles", "lifecycle", "-database.url-file", f.dbURLFile, "-node.gateway-token-file", f.gwFile}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(t.Context(), tc.args, noEnv)
			if err == nil || !strings.Contains(err.Error(), "secrets") {
				t.Fatalf("role %s without a key: %v", tc.name, err)
			}
		})
	}
}

func TestSecretsEphemeral(t *testing.T) {
	f := newFixture(t)
	args := append(f.fileArgs(), "-secrets.allow-ephemeral")
	c1, err := Load(t.Context(), args, noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if !c1.Secrets.Ephemeral || !c1.Secrets.AllowEphemeral {
		t.Fatalf("ephemeral=%v allow=%v", c1.Secrets.Ephemeral, c1.Secrets.AllowEphemeral)
	}
	var zero [masterKeyLen]byte
	if keyEqual(c1.Secrets.MasterKey[:], zero[:]) {
		t.Fatal("ephemeral key is all zero")
	}
	// Every Load generates a fresh key.
	c2, err := Load(t.Context(), args, noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if keyEqual(c1.Secrets.MasterKey[:], c2.Secrets.MasterKey[:]) {
		t.Fatal("two Loads generated the same ephemeral key")
	}

	// The environment fallback enables it too.
	c3, err := Load(t.Context(), f.fileArgs(), testEnv(map[string]string{
		"BBRAFT_SECRETS_ALLOW_EPHEMERAL": "true",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !c3.Secrets.Ephemeral {
		t.Fatal("env allow-ephemeral did not generate a key")
	}

	// A real key still wins over the permission.
	key := testKey()
	p := writeKey(t, f, "master.key", key)
	c4, err := Load(t.Context(), append(args, "-secrets.master-key-file", p), noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if c4.Secrets.Ephemeral || !keyEqual(c4.Secrets.MasterKey[:], key) {
		t.Fatalf("real key lost to ephemeral: ephemeral=%v", c4.Secrets.Ephemeral)
	}
}

func TestSecretsInvalidSourceNeverEphemeral(t *testing.T) {
	f := newFixture(t)
	short := writeKey(t, f, "short.key", make([]byte, 31))
	allow := []string{"-secrets.allow-ephemeral"}
	tests := []struct {
		name string
		args []string
		env  map[string]string
	}{
		{"bad file", append(f.fileArgs(), append(allow, "-secrets.master-key-file", short)...), nil},
		{"bad env hex", append(f.fileArgs(), allow...), map[string]string{EnvSecretsMasterKey: "not-hex"}},
		{"conflict", append(f.fileArgs(), append(allow, "-secrets.master-key-file", short)...), map[string]string{EnvSecretsMasterKey: strings.Repeat("ab", 32)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := noEnv
			if tc.env != nil {
				env = testEnv(tc.env)
			}
			if _, err := Load(t.Context(), tc.args, env); err == nil {
				t.Fatalf("malformed key with allow-ephemeral accepted: args %v env %v", tc.args, tc.env)
			}
		})
	}
}

func TestSecretsAllowEphemeralPrecedence(t *testing.T) {
	f := newFixture(t)
	bad := testEnv(map[string]string{"BBRAFT_SECRETS_ALLOW_EPHEMERAL": "notabool"})

	// A malformed env fallback only matters without a flag override.
	c, err := Load(t.Context(), append(f.fileArgs(), "-secrets.allow-ephemeral"), bad)
	if err != nil {
		t.Fatalf("explicit flag lost to malformed env: %v", err)
	}
	if !c.Secrets.Ephemeral {
		t.Fatal("flag did not enable ephemeral")
	}
	if _, err := Load(t.Context(), f.fileArgs(), bad); err == nil ||
		!strings.Contains(err.Error(), "not a boolean") {
		t.Fatalf("malformed env without flag: %v", err)
	}

	// An explicit false flag also wins: the malformed env is ignored and
	// the configured env key is used.
	key := testKey()
	env := testEnv(map[string]string{
		"BBRAFT_SECRETS_ALLOW_EPHEMERAL": "notabool",
		EnvSecretsMasterKey:              hex.EncodeToString(key),
	})
	c, err = Load(t.Context(), append(f.fileArgs(), "-secrets.allow-ephemeral=false"), env)
	if err != nil {
		t.Fatalf("explicit false flag lost to malformed env: %v", err)
	}
	if c.Secrets.Ephemeral || !keyEqual(c.Secrets.MasterKey[:], key) {
		t.Fatalf("flag=false: ephemeral=%v", c.Secrets.Ephemeral)
	}
}
