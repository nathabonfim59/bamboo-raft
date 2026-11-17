package cli

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"uuid"
)

// DefaultAPIURL is the loopback control plane used when nothing else is
// configured.
const DefaultAPIURL = "http://127.0.0.1:7700"

// configFileName is the saved configuration inside the config directory.
const configFileName = "config.json"

// maxConfigBytes bounds config.json. Real files are tiny; the bound keeps a
// corrupted or hostile file from exhausting memory.
const maxConfigBytes = 64 << 10

// Config is the saved and effective CLI configuration. APIKey is a secret:
// it is never logged or printed, and the file that holds it is mode 0600.
type Config struct {
	APIURL string `json:"apiUrl"`
	APIKey string `json:"apiKey,omitempty"`
}

// DefaultConfigDir returns <user config dir>/bbraft, normally
// ~/.config/bbraft.
func DefaultConfigDir() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("locate user config directory: %w", err)
	}
	return filepath.Join(base, "bbraft"), nil
}

// ValidateAPIURL checks a base URL. It must be http or https, name a host and
// carry no embedded credentials, query or fragment; credentials in a URL end
// up in logs and errors. Messages never repeat the supplied value, because a
// malformed URL can itself contain a secret.
func ValidateAPIURL(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return errors.New("api url is empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return errors.New("invalid api url")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("invalid api url: scheme must be http or https")
	}
	if u.Host == "" {
		return errors.New("invalid api url: host is empty")
	}
	if u.User != nil {
		return errors.New("invalid api url: must not contain credentials")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return errors.New("invalid api url: must not contain a query or fragment")
	}
	return nil
}

// LoadConfig reads the saved configuration and applies the environment in
// precedence order: BBRAFT_API_URL/BBRAFT_API_KEY, then the saved file, then
// DefaultAPIURL. It does not validate
// the selected URL: the caller applies the --api-url flag on top and then
// validates the final value, so a valid flag can override a malformed fallback.
func LoadConfig(ctx context.Context, dir string, getenv func(string) string) (Config, error) {
	if err := ctx.Err(); err != nil {
		return Config{}, err
	}
	if getenv == nil {
		getenv = os.Getenv
	}
	saved, err := readSavedConfig(ctx, dir)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{APIURL: DefaultAPIURL}
	if saved.APIURL != "" {
		cfg.APIURL = saved.APIURL
	}
	if saved.APIKey != "" {
		cfg.APIKey = saved.APIKey
	}
	if value := getenv("BBRAFT_API_URL"); value != "" {
		cfg.APIURL = value
	}
	if value := getenv("BBRAFT_API_KEY"); value != "" {
		cfg.APIKey = value
	}
	return cfg, nil
}

// readSavedConfig reads config.json. A missing directory or file is an empty
// configuration, not an error. A file readable by other users is refused so a
// leaked key is caught instead of used.
func readSavedConfig(ctx context.Context, dir string) (Config, error) {
	if err := ctx.Err(); err != nil {
		return Config{}, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Config{}, nil
		}
		return Config{}, fmt.Errorf("open config directory: %w", err)
	}
	defer root.Close()

	file, err := root.Open(configFileName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Config{}, nil
		}
		return Config{}, fmt.Errorf("open config file: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return Config{}, fmt.Errorf("stat config file: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return Config{}, fmt.Errorf("config file %s must not be accessible by group or others (mode 0600)", filepath.Join(dir, configFileName))
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil {
		return Config{}, errors.New("read config file failed")
	}
	if len(raw) > maxConfigBytes {
		return Config{}, errors.New("config file is too large")
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		// The decoder can quote the input in its message; never repeat it.
		return Config{}, errors.New("config file is not valid JSON")
	}
	return cfg, nil
}

// SaveConfig writes the configuration atomically with mode 0600 inside a
// directory only the user can access. The temporary file is created in the
// same directory and renamed over the old one, so a crash never leaves a
// partial config and a reader never sees a half-written key.
func SaveConfig(ctx context.Context, dir string, cfg Config) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateAPIURL(cfg.APIURL); err != nil {
		return err
	}
	if cfg.APIKey == "" {
		return errors.New("config: api key is empty")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("protect config directory: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("open config directory: %w", err)
	}
	defer root.Close()

	raw, err := json.Marshal(cfg)
	if err != nil {
		// The key is in cfg; never let an encoder error echo it.
		return errors.New("encode config failed")
	}
	tmp := ".config-" + uuid.NewV7().String() + ".tmp"
	file, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create temporary config: %w", err)
	}
	cleanup := func() {
		file.Close()
		root.Remove(tmp)
	}
	if _, err := file.Write(raw); err != nil {
		cleanup()
		return fmt.Errorf("write temporary config: %w", err)
	}
	if err := file.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync temporary config: %w", err)
	}
	if err := file.Close(); err != nil {
		root.Remove(tmp)
		return fmt.Errorf("close temporary config: %w", err)
	}
	if err := ctx.Err(); err != nil {
		root.Remove(tmp)
		return err
	}
	if err := root.Rename(tmp, configFileName); err != nil {
		root.Remove(tmp)
		return fmt.Errorf("replace config file: %w", err)
	}
	// Persist the rename itself. The config is already in place; report a sync
	// failure instead of claiming the login was persisted when it may not be.
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open config directory for sync: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return fmt.Errorf("sync config directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close config directory: %w", closeErr)
	}
	return nil
}
