package s3

import (
	"flag"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	testAccessKeyID = "AKIAIOSFODNN7EXAMPLE"
	testSecret      = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

// flagsFrom parses args with the -s3.* flags registered.
func flagsFrom(t *testing.T, args ...string) *Flags {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	f := RegisterFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse %v: %v", args, err)
	}
	return f
}

// lookupEnv returns an os.LookupEnv-shaped function over m.
func lookupEnv(m map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := m[name]
		return v, ok
	}
}

// writeSecret writes a secret file with mode perm.
func writeSecret(t *testing.T, secret string, perm os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte(secret), perm); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, perm); err != nil { // WriteFile is subject to umask
		t.Fatal(err)
	}
	return path
}

func TestFlagsConfig(t *testing.T) {
	envAll := map[string]string{
		"BBRAFT_S3_ENDPOINT":          "http://env.example:3900",
		"BBRAFT_S3_REGION":            "env-region",
		"BBRAFT_S3_BUCKET":            "env-bucket",
		"BBRAFT_S3_PATH_STYLE":        "true",
		"BBRAFT_S3_ACCESS_KEY_ID":     testAccessKeyID,
		"BBRAFT_S3_SECRET_ACCESS_KEY": testSecret,
		"BBRAFT_S3_CREATE_BUCKET":     "true",
		"BBRAFT_S3_LOCATION_HINT":     "env-hint",
	}
	envAllCfg := Config{
		Endpoint:        "http://env.example:3900",
		Region:          "env-region",
		Bucket:          "env-bucket",
		PathStyle:       true,
		AccessKeyID:     testAccessKeyID,
		SecretAccessKey: testSecret,
		CreateBucket:    true,
		LocationHint:    "env-hint",
	}
	argsAll := []string{
		"-s3.endpoint=http://flag.example:3900",
		"-s3.region=flag-region",
		"-s3.bucket=flag-bucket",
		"-s3.path-style",
		"-s3.access-key-id=" + testAccessKeyID,
		"-s3.create-bucket",
		"-s3.location-hint=flag-hint",
	}
	argsAllCfg := Config{
		Endpoint:        "http://flag.example:3900",
		Region:          "flag-region",
		Bucket:          "flag-bucket",
		PathStyle:       true,
		AccessKeyID:     testAccessKeyID,
		SecretAccessKey: testSecret,
		CreateBucket:    true,
		LocationHint:    "flag-hint",
	}

	tests := []struct {
		name       string
		args       []string
		env        map[string]string
		want       Config
		configured bool
		wantErr    string // substring of the error, empty for nil
	}{
		{
			name: "not configured",
			want: Config{},
		},
		{
			name: "empty variables are not settings",
			env: map[string]string{
				"BBRAFT_S3_ENDPOINT":   "",
				"BBRAFT_S3_REGION":     "",
				"BBRAFT_S3_PATH_STYLE": "",
			},
			want: Config{},
		},
		{
			name:       "environment only",
			env:        envAll,
			want:       envAllCfg,
			configured: true,
		},
		{
			name:       "flags only",
			args:       argsAll,
			env:        map[string]string{"BBRAFT_S3_SECRET_ACCESS_KEY": testSecret},
			want:       argsAllCfg,
			configured: true,
		},
		{
			name:       "flags win over environment",
			args:       argsAll,
			env:        envAll,
			want:       argsAllCfg,
			configured: true,
		},
		{
			name: "explicit false flag wins over true environment",
			args: []string{"-s3.bucket=b", "-s3.access-key-id=" + testAccessKeyID, "-s3.path-style=false"},
			env: map[string]string{
				"BBRAFT_S3_SECRET_ACCESS_KEY": testSecret,
				"BBRAFT_S3_PATH_STYLE":        "true",
			},
			want:       Config{Bucket: "b", AccessKeyID: testAccessKeyID, SecretAccessKey: testSecret},
			configured: true,
		},
		{
			name:    "bad boolean names the variable, not the value",
			args:    []string{"-s3.bucket=b"},
			env:     map[string]string{"BBRAFT_S3_PATH_STYLE": "maybe"},
			wantErr: "BBRAFT_S3_PATH_STYLE",
		},
		{
			name:    "missing access key id",
			args:    []string{"-s3.bucket=b"},
			env:     map[string]string{"BBRAFT_S3_SECRET_ACCESS_KEY": testSecret},
			wantErr: "access key id",
		},
		{
			name:    "missing secret",
			args:    []string{"-s3.bucket=b", "-s3.access-key-id=" + testAccessKeyID},
			wantErr: "no secret access key",
		},
		{
			name:    "empty environment secret",
			args:    []string{"-s3.bucket=b", "-s3.access-key-id=" + testAccessKeyID},
			env:     map[string]string{"BBRAFT_S3_SECRET_ACCESS_KEY": ""},
			wantErr: "set but empty",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := flagsFrom(t, tt.args...)
			cfg, configured, err := f.Config(lookupEnv(tt.env))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Config error = %v, want one containing %q", err, tt.wantErr)
				}
				if strings.Contains(err.Error(), testSecret) {
					t.Errorf("error leaks the secret: %v", err)
				}
				if tt.wantErr == "BBRAFT_S3_PATH_STYLE" && strings.Contains(err.Error(), "maybe") {
					t.Errorf("boolean error echoes the bad value: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Config: %v", err)
			}
			if configured != tt.configured {
				t.Errorf("configured = %t, want %t", configured, tt.configured)
			}
			if cfg != tt.want {
				t.Errorf("Config = %s, want %s", cfg, tt.want)
			}
		})
	}
}

func TestFlagsConfigSecretFile(t *testing.T) {
	tests := []struct {
		name    string
		content string
		perm    os.FileMode
		want    string
		wantErr string
	}{
		{name: "trailing newline trimmed", content: testSecret + "\n", perm: 0o600, want: testSecret},
		{name: "crlf trimmed", content: testSecret + "\r\n", perm: 0o600, want: testSecret},
		{name: "mode 0400 accepted", content: testSecret, perm: 0o400, want: testSecret},
		{name: "group readable refused", content: testSecret, perm: 0o640, wantErr: "0600 or stricter"},
		{name: "world readable refused", content: testSecret, perm: 0o604, wantErr: "0600 or stricter"},
		{name: "empty file refused", content: "\n", perm: 0o600, wantErr: "is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeSecret(t, tt.content, tt.perm)
			f := flagsFrom(t, "-s3.bucket=b", "-s3.access-key-id="+testAccessKeyID, "-s3.secret-access-key-file="+path)
			cfg, configured, err := f.Config(lookupEnv(nil))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Config error = %v, want one containing %q", err, tt.wantErr)
				}
				if strings.Contains(err.Error(), testSecret) {
					t.Errorf("error leaks the secret: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Config: %v", err)
			}
			if !configured || cfg.SecretAccessKey != tt.want {
				t.Errorf("configured = %t, secret = %s (want %q)", configured, cfg, tt.want)
			}
		})
	}
}

// TestFlagsConfigSecretFileEnvironment covers the secret file path from the
// environment and the flag's precedence over it.
func TestFlagsConfigSecretFileEnvironment(t *testing.T) {
	path := writeSecret(t, testSecret, 0o600)
	env := map[string]string{
		"BBRAFT_S3_BUCKET":                 "b",
		"BBRAFT_S3_ACCESS_KEY_ID":          testAccessKeyID,
		"BBRAFT_S3_SECRET_ACCESS_KEY_FILE": path,
	}
	f := flagsFrom(t)
	cfg, configured, err := f.Config(lookupEnv(env))
	if err != nil || !configured {
		t.Fatalf("Config: configured %t, err %v", configured, err)
	}
	if cfg.SecretAccessKey != testSecret {
		t.Errorf("secret = %q, want the file's", cfg.SecretAccessKey)
	}

	// A flag path wins over the environment variable.
	other := writeSecret(t, "flag-secret", 0o600)
	f = flagsFrom(t, "-s3.secret-access-key-file="+other)
	cfg, _, err = f.Config(lookupEnv(env))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SecretAccessKey != "flag-secret" {
		t.Errorf("secret = %q, want the flag file's", cfg.SecretAccessKey)
	}
}

func TestFlagsConfigSecretFileNotRegular(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(dir, "link")
	if err := os.Symlink(writeSecret(t, testSecret, 0o600), link); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, link} {
		f := flagsFrom(t, "-s3.bucket=b", "-s3.access-key-id="+testAccessKeyID, "-s3.secret-access-key-file="+path)
		if _, _, err := f.Config(lookupEnv(nil)); err == nil {
			t.Errorf("Config with secret file %s succeeded", path)
		}
	}
}

// TestFlagsConfigBothSecretSources refuses the ambiguous configuration and
// keeps both secrets out of the error.
func TestFlagsConfigBothSecretSources(t *testing.T) {
	path := writeSecret(t, "file-secret-do-not-log", 0o600)
	f := flagsFrom(t, "-s3.bucket=b", "-s3.access-key-id="+testAccessKeyID, "-s3.secret-access-key-file="+path)
	_, _, err := f.Config(lookupEnv(map[string]string{"BBRAFT_S3_SECRET_ACCESS_KEY": testSecret}))
	if err == nil || !strings.Contains(err.Error(), "set only one of") {
		t.Fatalf("Config error = %v, want one containing \"set only one of\"", err)
	}
	for _, secret := range []string{testSecret, "file-secret-do-not-log"} {
		if strings.Contains(err.Error(), secret) {
			t.Errorf("error leaks a secret: %v", err)
		}
	}
}

// TestConfigRedactsSecret checks that no rendering of a Config, and no error
// from the flags, contains the secret.
func TestConfigRedactsSecret(t *testing.T) {
	cfg := Config{
		Endpoint:        "http://127.0.0.1:3900",
		Region:          "garage",
		Bucket:          "chunks",
		PathStyle:       true,
		AccessKeyID:     testAccessKeyID,
		SecretAccessKey: testSecret,
		CreateBucket:    true,
		LocationHint:    "hint",
	}
	if s := cfg.String(); strings.Contains(s, testSecret) {
		t.Errorf("String leaks the secret: %s", s)
	}
	if !strings.Contains(cfg.String(), "[REDACTED]") {
		t.Errorf("String does not mark the secret as redacted: %s", cfg.String())
	}
	var b strings.Builder
	slog.New(slog.NewTextHandler(&b, nil)).Info("config", "s3", cfg)
	if strings.Contains(b.String(), testSecret) {
		t.Errorf("LogValue leaks the secret: %s", b.String())
	}
	if !strings.Contains(b.String(), "[REDACTED]") {
		t.Errorf("LogValue does not mark the secret as redacted: %s", b.String())
	}
}

// TestFlagsConfigIncompleteWithoutBucket: a node whose bucket is unset but
// that has any other object store setting is misconfigured (for example an
// ignored EnvironmentFile that left only the credential variable). It refuses
// instead of running quietly without a store. The error names the settings,
// never their values.
func TestFlagsConfigIncompleteWithoutBucket(t *testing.T) {
	secretFile := writeSecret(t, testSecret, 0o600)
	const valueMarker = "VALUE-MARKER-not-in-errors"
	tests := []struct {
		name string
		args []string
		env  map[string]string
		// want names every setting the error must mention.
		want []string
	}{
		{name: "secret file variable", env: map[string]string{"BBRAFT_S3_SECRET_ACCESS_KEY_FILE": secretFile},
			want: []string{"-s3.secret-access-key-file (BBRAFT_S3_SECRET_ACCESS_KEY_FILE)"}},
		{name: "secret variable", env: map[string]string{"BBRAFT_S3_SECRET_ACCESS_KEY": testSecret},
			want: []string{"BBRAFT_S3_SECRET_ACCESS_KEY"}},
		{name: "access key id", env: map[string]string{"BBRAFT_S3_ACCESS_KEY_ID": valueMarker},
			want: []string{"-s3.access-key-id (BBRAFT_S3_ACCESS_KEY_ID)"}},
		{name: "endpoint", env: map[string]string{"BBRAFT_S3_ENDPOINT": "http://" + valueMarker + ":3900"},
			want: []string{"-s3.endpoint (BBRAFT_S3_ENDPOINT)"}},
		{name: "region", env: map[string]string{"BBRAFT_S3_REGION": valueMarker},
			want: []string{"-s3.region (BBRAFT_S3_REGION)"}},
		{name: "path style", env: map[string]string{"BBRAFT_S3_PATH_STYLE": "true"},
			want: []string{"-s3.path-style (BBRAFT_S3_PATH_STYLE)"}},
		{name: "create bucket", env: map[string]string{"BBRAFT_S3_CREATE_BUCKET": "true"},
			want: []string{"-s3.create-bucket (BBRAFT_S3_CREATE_BUCKET)"}},
		{name: "location hint", env: map[string]string{"BBRAFT_S3_LOCATION_HINT": valueMarker},
			want: []string{"-s3.location-hint (BBRAFT_S3_LOCATION_HINT)"}},
		{name: "malformed boolean is still a setting", env: map[string]string{"BBRAFT_S3_PATH_STYLE": valueMarker},
			want: []string{"-s3.path-style (BBRAFT_S3_PATH_STYLE)"}},
		{name: "flag", args: []string{"-s3.region=" + valueMarker}, want: []string{"-s3.region (BBRAFT_S3_REGION)"}},
		{name: "empty bucket variable and others", env: map[string]string{"BBRAFT_S3_BUCKET": "", "BBRAFT_S3_ENDPOINT": "http://x:1"},
			want: []string{"-s3.endpoint (BBRAFT_S3_ENDPOINT)"}},
		{name: "several", env: map[string]string{
			"BBRAFT_S3_SECRET_ACCESS_KEY_FILE": secretFile, "BBRAFT_S3_ACCESS_KEY_ID": testAccessKeyID, "BBRAFT_S3_ENDPOINT": "http://x:1",
		}, want: []string{"-s3.endpoint", "-s3.access-key-id", "-s3.secret-access-key-file"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, configured, err := flagsFrom(t, tt.args...).Config(lookupEnv(tt.env))
			if err == nil {
				t.Fatalf("Config = %v, configured %t, want an error", cfg, configured)
			}
			msg := err.Error()
			for _, want := range append([]string{"object store settings are incomplete", "-s3.bucket (BBRAFT_S3_BUCKET) is not set but"}, tt.want...) {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q does not mention %q", msg, want)
				}
			}
			for _, secret := range []string{testSecret, testAccessKeyID, valueMarker, secretFile, "http://"} {
				if strings.Contains(msg, secret) {
					t.Errorf("error leaks a value %q: %s", secret, msg)
				}
			}
			if cfg != (Config{}) || configured {
				t.Errorf("Config = %s, configured %t on error", cfg, configured)
			}
		})
	}
	// Only the settings that are set are named.
	_, _, err := flagsFrom(t).Config(lookupEnv(map[string]string{"BBRAFT_S3_REGION": "r"}))
	if err == nil || strings.Contains(err.Error(), "BBRAFT_S3_ENDPOINT") || strings.Contains(err.Error(), "BBRAFT_S3_SECRET") {
		t.Fatalf("error names settings that are not set: %v", err)
	}
}

// TestFlagsConfigBucketSecretFileMissing: a bucket with an unreadable secret
// file is an error naming the path, never a secret.
func TestFlagsConfigBucketSecretFileMissing(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-secret")
	_, _, err := flagsFrom(t).Config(lookupEnv(map[string]string{
		"BBRAFT_S3_BUCKET": "b", "BBRAFT_S3_ACCESS_KEY_ID": testAccessKeyID, "BBRAFT_S3_SECRET_ACCESS_KEY_FILE": missing,
		"BBRAFT_S3_SECRET_ACCESS_KEY_UNUSED": testSecret,
	}))
	if err == nil || !strings.Contains(err.Error(), missing) {
		t.Fatalf("error = %v, want one naming %s", err, missing)
	}
	if strings.Contains(err.Error(), testSecret) {
		t.Fatalf("error leaks the secret: %v", err)
	}
}
