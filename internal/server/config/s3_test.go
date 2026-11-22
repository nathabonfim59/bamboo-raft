package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// s3TestSecretFile writes a 0600 secret file, as the shared object-store flag
// set requires, and returns its path.
func s3TestSecretFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "s3.secret")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// s3TestFlags are the flags of one completely configured object store. The
// value mapping itself is covered by internal/objstore/s3's flag tests; these
// tests cover the server's wiring of that shared flag set.
func s3TestFlags(secretFile string) []string {
	return []string{
		"-s3.endpoint", "https://s3.example.test:3900",
		"-s3.region", "garage",
		"-s3.bucket", "bbraft",
		"-s3.path-style",
		"-s3.access-key-id", "GKEXAMPLE",
		"-s3.location-hint", "wnam",
		"-s3.create-bucket",
		"-s3.secret-access-key-file", secretFile,
	}
}

// TestLoadObjectStoreFlags proves the shared -s3.* flags are registered on the
// server's flag set and reach the validated Config.
func TestLoadObjectStoreFlags(t *testing.T) {
	f := newFixture(t)
	secretFile := s3TestSecretFile(t, "file-s3-secret\n") // one trailing newline is trimmed
	c, err := Load(t.Context(), append(f.baseArgs(), s3TestFlags(secretFile)...), noEnv)
	if err != nil {
		t.Fatal(err)
	}
	s := c.ObjectStore
	if s == nil {
		t.Fatal("ObjectStore is nil with a configured bucket")
	}
	if s.Endpoint != "https://s3.example.test:3900" || s.Region != "garage" || s.Bucket != "bbraft" {
		t.Fatalf("endpoint/region/bucket = %q/%q/%q", s.Endpoint, s.Region, s.Bucket)
	}
	if !s.PathStyle || !s.CreateBucket || s.AccessKeyID != "GKEXAMPLE" || s.LocationHint != "wnam" {
		t.Fatalf("flags did not reach the store config: %+v", s)
	}
	if s.SecretAccessKey != "file-s3-secret" {
		t.Fatal("secret access key was not read from the file")
	}
}

// TestLoadObjectStoreEnv proves Load hands its injectable environment lookup to
// the shared flag set (no os.Getenv in tests) and that flags still win.
func TestLoadObjectStoreEnv(t *testing.T) {
	f := newFixture(t)
	const sentinel = "env-s3-secret"
	env := testEnv(map[string]string{
		"BBRAFT_S3_ENDPOINT":          "http://s3.example.test:3900",
		"BBRAFT_S3_REGION":            "auto",
		"BBRAFT_S3_BUCKET":            "bbraft-env",
		"BBRAFT_S3_PATH_STYLE":        "true",
		"BBRAFT_S3_ACCESS_KEY_ID":     "ENVKEY",
		"BBRAFT_S3_CREATE_BUCKET":     "true",
		"BBRAFT_S3_SECRET_ACCESS_KEY": sentinel,
	})
	c, err := Load(t.Context(), f.baseArgs(), env)
	if err != nil {
		t.Fatal(err)
	}
	s := c.ObjectStore
	if s == nil {
		t.Fatal("ObjectStore is nil with a bucket in the environment")
	}
	if s.Endpoint != "http://s3.example.test:3900" || s.Region != "auto" || s.Bucket != "bbraft-env" {
		t.Fatalf("environment did not reach the store config: %+v", s)
	}
	if !s.PathStyle || !s.CreateBucket || s.AccessKeyID != "ENVKEY" || s.SecretAccessKey != sentinel {
		t.Fatalf("environment settings = %+v", s)
	}

	// An explicitly set flag beats the environment value of the same flag.
	c, err = Load(t.Context(), append(f.baseArgs(), "-s3.bucket", "flag-bucket"), env)
	if err != nil {
		t.Fatal(err)
	}
	if c.ObjectStore.Bucket != "flag-bucket" {
		t.Fatalf("bucket = %q, want the flag's", c.ObjectStore.Bucket)
	}
}

// TestLoadObjectStoreUnset is Phase 1 mode: with no object store setting at
// all there is no object store.
func TestLoadObjectStoreUnset(t *testing.T) {
	f := newFixture(t)
	c, err := Load(t.Context(), f.baseArgs(), noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if c.ObjectStore != nil {
		t.Fatalf("ObjectStore = %+v, want nil (Phase 1 mode)", c.ObjectStore)
	}
}

// TestLoadObjectStoreIncomplete: any object store setting without a bucket
// refuses to start (a half-applied environment file must not start the
// server quietly without its store). The error names the setting, never its
// value.
func TestLoadObjectStoreIncomplete(t *testing.T) {
	f := newFixture(t)
	for _, tc := range []struct {
		name, variable, value string
	}{
		{"region only", "BBRAFT_S3_REGION", "garage-sentinel-region"},
		{"malformed path style", "BBRAFT_S3_PATH_STYLE", "not-a-bool-sentinel"},
		{"secret without a bucket", "BBRAFT_S3_SECRET_ACCESS_KEY", "sentinel-without-bucket"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(t.Context(), f.baseArgs(), testEnv(map[string]string{tc.variable: tc.value}))
			if err == nil {
				t.Fatalf("%s set without a bucket was accepted", tc.variable)
			}
			if !strings.Contains(err.Error(), "BBRAFT_S3_BUCKET") || !strings.Contains(err.Error(), tc.variable) {
				t.Fatalf("error = %q, want it to name BBRAFT_S3_BUCKET and %s", err, tc.variable)
			}
			if strings.Contains(err.Error(), tc.value) {
				t.Fatalf("error leaks the value of %s: %q", tc.variable, err)
			}
		})
	}
}

// TestLoadObjectStoreErrors proves a shared-flag-set error is a config error
// that names the flag, never the secret.
func TestLoadObjectStoreErrors(t *testing.T) {
	f := newFixture(t)
	const sentinel = "SENTINEL-s3-secret"
	secretFile := s3TestSecretFile(t, sentinel)
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{
			name: "bucket without access key id",
			args: []string{"-s3.bucket", "bbraft", "-s3.secret-access-key-file", secretFile},
			want: "s3.access-key-id",
		},
		{
			name: "bucket without secret",
			args: []string{"-s3.bucket", "bbraft", "-s3.access-key-id", "key"},
			want: "BBRAFT_S3_SECRET_ACCESS_KEY",
		},
		{
			name: "conflicting secret sources",
			args: s3TestFlags(secretFile),
			env:  map[string]string{"BBRAFT_S3_SECRET_ACCESS_KEY": sentinel},
			want: "BBRAFT_S3_SECRET_ACCESS_KEY",
		},
		{
			name: "malformed boolean env",
			args: []string{"-s3.bucket", "bbraft", "-s3.access-key-id", "key", "-s3.secret-access-key-file", secretFile},
			env:  map[string]string{"BBRAFT_S3_PATH_STYLE": "sometimes"},
			want: "BBRAFT_S3_PATH_STYLE",
		},
		{
			name: "secret file too permissive",
			args: []string{"-s3.bucket", "bbraft", "-s3.access-key-id", "key",
				"-s3.secret-access-key-file", permissiveSecretFile(t, sentinel)},
			want: "0600",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := noEnv
			if tc.env != nil {
				env = testEnv(tc.env)
			}
			_, err := Load(t.Context(), append(f.baseArgs(), tc.args...), env)
			if err == nil {
				t.Fatalf("%s: invalid object store accepted", tc.name)
			}
			if !strings.HasPrefix(err.Error(), "config: ") {
				t.Fatalf("%s: error %q is not a config error", tc.name, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s: error %q does not mention %q", tc.name, err, tc.want)
			}
			if strings.Contains(err.Error(), sentinel) {
				t.Fatalf("%s: error leaks the secret", tc.name)
			}
		})
	}
}

// permissiveSecretFile writes a secret file the shared flag set must refuse.
func permissiveSecretFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "s3.secret")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestObjectStoreSecretNeverPrinted proves the credential never reaches a
// formatted server config or a log line: the shared s3.Config is what the
// server holds.
func TestObjectStoreSecretNeverPrinted(t *testing.T) {
	f := newFixture(t)
	const sentinel = "SENTINEL-s3-secret-value"
	c, err := Load(t.Context(), append(f.baseArgs(), s3TestFlags(s3TestSecretFile(t, sentinel))...), noEnv)
	if err != nil {
		t.Fatal(err)
	}
	printed := []string{
		fmt.Sprint(c),
		fmt.Sprintf("%v", c),
		fmt.Sprintf("%+v", c),
		fmt.Sprint(c.ObjectStore),
		fmt.Sprintf("%+v", c.ObjectStore),
	}
	var logged strings.Builder
	logger := slog.New(slog.NewTextHandler(&logged, nil))
	logger.Info("object store", "object_store", c.ObjectStore)
	printed = append(printed, logged.String())
	for _, out := range printed {
		if strings.Contains(out, sentinel) {
			t.Fatalf("the secret appeared in printed output: %q", out)
		}
	}
}
