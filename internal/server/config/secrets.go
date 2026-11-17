package config

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
)

// masterKeyLen is the required secrets master key size in bytes.
const masterKeyLen = 32

// Secrets holds the master key encrypting secrets at rest (the database
// wraps its keys under it). MasterKey is key material: it never goes to
// logs or errors.
type Secrets struct {
	// MasterKeyFile names the file the raw key was read from
	// (-secrets.master-key-file, BBRAFT_SECRETS_MASTER_KEY_FILE). Empty
	// when the key came from EnvSecretsMasterKey or was generated.
	MasterKeyFile string
	// AllowEphemeral is -secrets.allow-ephemeral
	// (BBRAFT_SECRETS_ALLOW_EPHEMERAL): when no key source is configured,
	// generate a random throwaway key instead of refusing to start. It
	// exists for tests and local development; it never rescues a
	// configured but malformed key.
	AllowEphemeral bool
	// Ephemeral reports whether MasterKey was generated randomly during
	// Load; such a key decrypts nothing after a restart. Safe to log.
	Ephemeral bool
	// MasterKey is the resolved key. Secret.
	MasterKey [masterKeyLen]byte
}

// resolveSecrets fills c.Secrets.MasterKey from its single configured
// source: -secrets.master-key-file holds the exact raw 32 bytes (no
// trimming — whitespace is key material) and EnvSecretsMasterKey holds
// exactly 64 hexadecimal characters. The sources conflict. With no
// source, every role refuses to start unless allow-ephemeral was given,
// which generates one random key per Load. A malformed supplied key is
// always an error; it never falls back to ephemeral. Errors name the
// source, never the key.
func (c *Config) resolveSecrets(ctx context.Context, env func(string) (string, bool)) error {
	direct, hasDirect := env(EnvSecretsMasterKey)
	if c.Secrets.MasterKeyFile != "" && hasDirect {
		return fmt.Errorf("config: secrets.master-key: file and %s both set; pick one", EnvSecretsMasterKey)
	}
	if hasDirect {
		return c.masterKeyFromHex(direct)
	}
	if c.Secrets.MasterKeyFile != "" {
		return c.masterKeyFromFile(ctx)
	}
	if !c.Secrets.AllowEphemeral {
		return fmt.Errorf("config: a secrets master key is required: -secrets.master-key-file, %s or %s; for tests pass -secrets.allow-ephemeral", envName("secrets.master-key-file"), EnvSecretsMasterKey)
	}
	if _, err := rand.Read(c.Secrets.MasterKey[:]); err != nil {
		return fmt.Errorf("config: secrets.master-key: generate ephemeral key: %w", err)
	}
	c.Secrets.Ephemeral = true
	return nil
}

// masterKeyFromHex decodes the EnvSecretsMasterKey value: exactly 64
// hexadecimal characters for 32 bytes. The value never appears in errors.
func (c *Config) masterKeyFromHex(direct string) error {
	if len(direct) != hex.EncodedLen(masterKeyLen) {
		return fmt.Errorf("config: %s must be exactly %d hexadecimal characters (32 bytes), got %d", EnvSecretsMasterKey, hex.EncodedLen(masterKeyLen), len(direct))
	}
	raw, err := hex.DecodeString(direct)
	if err != nil {
		// Not wrapped: the decode error echoes offending bytes of the key.
		return fmt.Errorf("config: %s is not valid hexadecimal", EnvSecretsMasterKey)
	}
	copy(c.Secrets.MasterKey[:], raw)
	return nil
}

// masterKeyFromFile reads the exact 32 raw bytes from
// c.Secrets.MasterKeyFile.
func (c *Config) masterKeyFromFile(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := os.ReadFile(c.Secrets.MasterKeyFile)
	if err != nil {
		return fmt.Errorf("config: secrets.master-key: read file: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(raw) != masterKeyLen {
		return fmt.Errorf("config: secrets.master-key: file must contain exactly %d bytes, got %d", masterKeyLen, len(raw))
	}
	copy(c.Secrets.MasterKey[:], raw)
	return nil
}
