package db

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"

	"uuid"
)

// Secrets holds the install master key. It comes from
// secrets.master-key-file or BBRAFT_SECRETS_MASTER_KEY, parsed by the config
// package; this package only takes raw bytes. The key wraps the install data
// key that encrypts sandbox secrets at rest.
type Secrets struct {
	MasterKey [32]byte
}

// NewSecrets builds Secrets from exactly 32 key bytes.
func NewSecrets(masterKey []byte) (Secrets, error) {
	var s Secrets
	if len(masterKey) != len(s.MasterKey) {
		return Secrets{}, fmt.Errorf("db: master key must be %d bytes, got %d",
			len(s.MasterKey), len(masterKey))
	}
	copy(s.MasterKey[:], masterKey)
	return s, nil
}

// aead returns the AES-256-GCM cipher for the master key.
func (s Secrets) aead() (cipher.AEAD, error) {
	block, err := aes.NewCipher(s.MasterKey[:])
	if err != nil {
		return nil, fmt.Errorf("db: master key cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("db: gcm: %w", err)
	}
	return gcm, nil
}

// hint fingerprints the master key (key id) so a wrong key is detected
// without attempting decrypts and without enabling key recovery.
func (s Secrets) hint() string {
	sum := sha256.Sum256(s.MasterKey[:])
	return hex.EncodeToString(sum[:8])
}

// sealedEnv is one encrypted field value: a fresh 96-bit nonce followed by
// the AES-256-GCM ciphertext over the plaintext and the binding AAD.
type sealedEnv struct {
	gcm cipher.AEAD
}

// aad builds the additional data binding a sealed value to its owner,
// sandbox and field. JSON with fixed keys is unambiguous.
func aad(owner uuid.UUID, sandboxID, field string) []byte {
	return fmt.Appendf(nil, `{"field":%q,"owner":%q,"sandbox":%q}`, field, owner.String(), sandboxID)
}

// seal encrypts plaintext under the binding AAD with a fresh nonce.
func (e sealedEnv) seal(plaintext []byte, additional []byte) ([]byte, error) {
	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("db: seal nonce: %w", err)
	}
	return e.gcm.Seal(nonce, nonce, plaintext, additional), nil
}

// open decrypts a value produced by seal, failing closed on any mismatch of
// key, nonce, ciphertext or AAD.
func (e sealedEnv) open(sealed, additional []byte) ([]byte, error) {
	ns := e.gcm.NonceSize()
	if len(sealed) < ns+e.gcm.Overhead() {
		return nil, errors.New("db: sealed value too short")
	}
	plaintext, err := e.gcm.Open(nil, sealed[:ns], sealed[ns:], additional)
	if err != nil {
		return nil, errors.New("db: sealed value does not open") // no material in the error
	}
	return plaintext, nil
}

// ErrMasterKeyMismatch means the stored install key was wrapped with a
// different master key; the install refuses to run rather than fall back or
// rewrap on its own.
var ErrMasterKeyMismatch = errors.New("db: master key does not match the installed key")

// ensureInstallKey bootstraps the single install data key: the first writer
// wins durably, every later start reuses it, and a master key that cannot
// unwrap it fails with [ErrMasterKeyMismatch]. It runs after migrations.
func (s *Store) ensureInstallKey(ctx context.Context) error {
	var fresh [32]byte
	if _, err := rand.Read(fresh[:]); err != nil {
		return fmt.Errorf("db: install key: %w", err)
	}
	gcm, err := s.secrets.aead()
	if err != nil {
		return err
	}
	sealed, err := sealedEnv{gcm: gcm}.seal(fresh[:], []byte(`{"field":"install_key"}`))
	if err != nil {
		return err
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO install_keys (id, wrapped_key, key_hint) VALUES (1, $1, $2)
		ON CONFLICT (id) DO NOTHING`,
		sealed, s.secrets.hint())
	if err != nil {
		return fmt.Errorf("db: install key insert: %w", err)
	}

	var (
		stored     []byte
		storedHint string
	)
	if err := s.pool.QueryRow(ctx,
		`SELECT wrapped_key, key_hint FROM install_keys WHERE id = 1`).
		Scan(&stored, &storedHint); err != nil {
		return fmt.Errorf("db: install key read: %w", err)
	}
	// Authenticated unwrap is the primary check: failure is generic and
	// reveals no key or ciphertext material. The stored hint is only a
	// fingerprint, so compare it in constant time as a secondary
	// corruption check instead of rejecting on it before the unwrap.
	key, err := sealedEnv{gcm: gcm}.open(stored, []byte(`{"field":"install_key"}`))
	if err != nil {
		return ErrMasterKeyMismatch
	}
	if subtle.ConstantTimeCompare([]byte(storedHint), []byte(s.secrets.hint())) != 1 {
		return ErrMasterKeyMismatch
	}
	envAEAD, err := dataKeyAEAD(key)
	if err != nil {
		return err
	}
	s.env = sealedEnv{gcm: envAEAD}
	return nil
}

// dataKeyAEAD builds the value cipher from the unwrapped install data key.
func dataKeyAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("db: data key cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("db: data key gcm: %w", err)
	}
	return gcm, nil
}

// errEnvEncode replaces JSON encode failures of the secret env map: the
// underlying error text may echo the values it failed on, so it is never
// wrapped or returned.
var errEnvEncode = errors.New("db: env encoding failed")

// errEnvDecode replaces decode failures of authenticated env plaintext:
// malformed content is never echoed either.
var errEnvDecode = errors.New("db: sealed env plaintext is malformed")

// errStoreNotMigrated means a Store was opened without running [Store.Migrate],
// so it has no install data key and must never silently store plaintext.
var errStoreNotMigrated = errors.New("db: store has no install key; run Migrate first")

// sealEnv encrypts one sandbox env map for at-rest storage, bound to its
// owner and sandbox id. An empty map seals to nil.
func (s *Store) sealEnv(env map[string]string, owner uuid.UUID, sandboxID string) ([]byte, error) {
	if len(env) == 0 {
		return nil, nil
	}
	if s.env.gcm == nil {
		return nil, errStoreNotMigrated
	}
	plain, err := jsonMarshal(env)
	if err != nil {
		return nil, errEnvEncode
	}
	sealed, err := s.env.seal(plain, aad(owner, sandboxID, "env"))
	if err != nil {
		return nil, fmt.Errorf("db: seal env: %w", err)
	}
	return sealed, nil
}

// decodeEnv turns authenticated plaintext into the env map, reporting
// malformed content with a generic error.
func decodeEnv(plain []byte) (map[string]string, error) {
	out := make(map[string]string, 8)
	if err := jsonUnmarshal(plain, &out); err != nil {
		return nil, errEnvDecode
	}
	return out, nil
}

// DecryptSandboxEnvForNode decrypts a sandbox's env for node CreateVM
// dispatch only. It is the single decrypt path: ordinary sandbox reads and
// API responses never see plaintext env.
func (s *Store) DecryptSandboxEnvForNode(ctx context.Context, owner uuid.UUID, sandboxID string) (map[string]string, error) {
	var sealed []byte
	err := s.pool.QueryRow(ctx,
		`SELECT env_sealed FROM sandboxes WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL`,
		sandboxID, owner).Scan(&sealed)
	if err != nil {
		return nil, errNoRows("decrypt env select", err)
	}
	out := make(map[string]string, 8)
	if sealed == nil {
		return out, nil
	}
	if s.env.gcm == nil {
		return nil, errStoreNotMigrated
	}
	plain, err := s.env.open(sealed, aad(owner, sandboxID, "env"))
	if err != nil {
		return nil, err
	}
	return decodeEnv(plain)
}
