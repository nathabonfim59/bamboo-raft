package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigPrecedence(t *testing.T) {
	dir := t.TempDir()
	saved := Config{APIURL: "https://saved.example", APIKey: "saved-key"}
	if err := SaveConfig(t.Context(), dir, saved); err != nil {
		t.Fatalf("save config: %v", err)
	}
	tests := []struct {
		name    string
		env     map[string]string
		wantURL string
		wantKey string
	}{
		{
			name:    "saved",
			env:     nil,
			wantURL: saved.APIURL,
			wantKey: saved.APIKey,
		},
		{
			name: "env overrides saved",
			env: map[string]string{
				"BBRAFT_API_URL": "https://bbraft.example",
				"BBRAFT_API_KEY": "bbraft-key",
			},
			wantURL: "https://bbraft.example",
			wantKey: "bbraft-key",
		},
		{
			name:    "empty env is ignored",
			env:     map[string]string{"BBRAFT_API_URL": "", "BBRAFT_API_KEY": ""},
			wantURL: saved.APIURL,
			wantKey: saved.APIKey,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := LoadConfig(t.Context(), dir, func(key string) string { return tc.env[key] })
			if err != nil {
				t.Fatalf("load config: %v", err)
			}
			if got.APIURL != tc.wantURL || !cliTestSecretEqual(got.APIKey, tc.wantKey) {
				t.Fatalf("config mismatch: url=%q keyMatches=%v", got.APIURL, cliTestSecretEqual(got.APIKey, tc.wantKey))
			}
		})
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	got, err := LoadConfig(t.Context(), t.TempDir(), func(string) string { return "" })
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if got.APIURL != DefaultAPIURL {
		t.Fatalf("url = %q, want %q", got.APIURL, DefaultAPIURL)
	}
	if got.APIKey != "" {
		t.Fatal("key is not empty, want empty")
	}
}

func TestSaveConfigProtectsFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested")
	if err := SaveConfig(t.Context(), dir, Config{APIURL: "https://api.example", APIKey: "secret"}); err != nil {
		t.Fatalf("save config: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("dir mode = %o, want 700", got)
	}
	path := filepath.Join(dir, configFileName)
	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("stat config: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("file mode = %o, want 600", got)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != configFileName {
		t.Fatalf("dir entries = %v, want only %s", entries, configFileName)
	}
	got, err := LoadConfig(t.Context(), dir, func(string) string { return "" })
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cliTestSecretEqual(got.APIKey, "secret") {
		t.Fatal("saved key does not match")
	}
}

func TestLoadConfigRefusesReadableByOthers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, configFileName)
	if err := os.WriteFile(path, []byte(`{"apiUrl":"https://api.example","apiKey":"secret"}`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := LoadConfig(t.Context(), dir, func(string) string { return "" }); err == nil {
		t.Fatal("load config succeeded, want a permission error")
	}
}

func TestLoadConfigDoesNotEchoInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, configFileName)
	const secret = "super-secret-key"
	if err := os.WriteFile(path, []byte(`{"apiKey":"`+secret+`",`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	_, err := LoadConfig(t.Context(), dir, func(string) string { return "" })
	if err == nil {
		t.Fatal("load config succeeded, want a parse error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("parse error contains the key")
	}
}

func TestConfigHonoursCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dir := t.TempDir()
	if err := SaveConfig(ctx, dir, Config{APIURL: "https://api.example", APIKey: "secret"}); err == nil {
		t.Fatal("SaveConfig succeeded with a cancelled context")
	}
	if _, err := LoadConfig(ctx, dir, func(string) string { return "" }); err == nil {
		t.Fatal("LoadConfig succeeded with a cancelled context")
	}
}

func TestValidateAPIURL(t *testing.T) {
	tests := []struct {
		name  string
		raw   string
		valid bool
	}{
		{name: "loopback", raw: "http://127.0.0.1:7700", valid: true},
		{name: "https with path", raw: "https://api.example/base", valid: true},
		{name: "empty", raw: "", valid: false},
		{name: "wrong scheme", raw: "ftp://api.example", valid: false},
		{name: "missing scheme", raw: "api.example", valid: false},
		{name: "credentials", raw: "https://user:pass@api.example", valid: false},
		{name: "query", raw: "https://api.example?token=1", valid: false},
		{name: "fragment", raw: "https://api.example#frag", valid: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAPIURL(tc.raw)
			if (err == nil) != tc.valid {
				t.Fatalf("ValidateAPIURL valid = %v, want %v (error=%v)", err == nil, tc.valid, err != nil)
			}
		})
	}
}
