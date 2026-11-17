package db

// Unit tests for the sealed-value machinery. These run without PostgreSQL:
// wrong keys, AAD bindings and nonce randomness are pure crypto properties.

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"uuid"
)

func testMaster(t *testing.T) Secrets {
	t.Helper()
	b := make([]byte, 32)
	for i := range b {
		b[i] = byte(i + 1)
	}
	s, err := NewSecrets(b)
	if err != nil {
		t.Fatalf("NewSecrets: %v", err)
	}
	return s
}

func TestNewSecretsRejectsWrongLength(t *testing.T) {
	if _, err := NewSecrets(make([]byte, 16)); err == nil {
		t.Fatal("16-byte key accepted, want rejection")
	}
	if _, err := NewSecrets(make([]byte, 33)); err == nil {
		t.Fatal("33-byte key accepted, want rejection")
	}
}

func TestSealedRoundTrip(t *testing.T) {
	gcm, err := testMaster(t).aead()
	if err != nil {
		t.Fatalf("aead: %v", err)
	}
	e := sealedEnv{gcm: gcm}
	owner, sandbox := uuid.MustParse("0196b1d0-6c34-7cc0-9a8f-2a1d64c51b01"), "bx_abcdefgh"
	plain := []byte(`{"PATH":"/usr/bin"}`)
	sealed, err := e.seal(plain, aad(owner, sandbox, "env"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	got, err := e.open(sealed, aad(owner, sandbox, "env"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round trip: plaintext mismatch (got %d bytes, want %d)", len(got), len(plain))
	}
}

func TestSealedRejectsWrongKey(t *testing.T) {
	e1 := mustAEAD(t, testMaster(t))
	other := testMaster(t)
	other.MasterKey[0] ^= 0xFF
	e2 := mustAEAD(t, other)
	owner, sandbox := uuid.MustParse("0196b1d0-6c34-7cc0-9a8f-2a1d64c51b02"), "bx_abcdef12"
	sealed, err := e1.seal([]byte(`{"A":"b"}`), aad(owner, sandbox, "env"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, err := e2.open(sealed, aad(owner, sandbox, "env")); err == nil {
		t.Fatal("opened with wrong master key, want failure")
	}
}

func TestSealedBindsAAD(t *testing.T) {
	e := mustAEAD(t, testMaster(t))
	owner := uuid.MustParse("0196b1d0-6c34-7cc0-9a8f-2a1d64c51b03")
	otherOwner := uuid.MustParse("0196b1d0-6c34-7cc0-9a8f-2a1d64c51b04")
	sealed, err := e.seal([]byte(`{"A":"b"}`), aad(owner, "bx_aaaabbbb", "env"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	cases := []struct {
		name string
		aad  []byte
	}{
		{"wrong owner", aad(otherOwner, "bx_aaaabbbb", "env")},
		{"wrong sandbox", aad(owner, "bx_bbbbcccc", "env")},
		{"wrong field", aad(owner, "bx_aaaabbbb", "labels")},
		{"no aad", nil},
	}
	for _, tc := range cases {
		if _, err := e.open(sealed, tc.aad); err == nil {
			t.Fatalf("%s: opened, want failure", tc.name)
		}
	}
}

func TestSealedNonceRandomness(t *testing.T) {
	e := mustAEAD(t, testMaster(t))
	owner, sandbox := uuid.MustParse("0196b1d0-6c34-7cc0-9a8f-2a1d64c51b05"), "bx_acdef123"
	bind := aad(owner, sandbox, "env")
	a, err := e.seal([]byte(`{"same":"value"}`), bind)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	b, err := e.seal([]byte(`{"same":"value"}`), bind)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("two seals of the same value are identical, want fresh nonces")
	}
	if bytes.Equal(a[:12], b[:12]) {
		t.Fatal("nonce prefix repeated")
	}
}

func TestSealedRejectsTruncated(t *testing.T) {
	e := mustAEAD(t, testMaster(t))
	owner, sandbox := uuid.MustParse("0196b1d0-6c34-7cc0-9a8f-2a1d64c51b06"), "bx_acdef124"
	sealed, err := e.seal([]byte(`{"A":"b"}`), aad(owner, sandbox, "env"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	for _, cut := range [][]byte{sealed[:5], sealed[:24], append(bytes.Clone(sealed), 0)} {
		if _, err := e.open(cut, aad(owner, sandbox, "env")); err == nil {
			t.Fatalf("tampered value of len %d opened, want failure", len(cut))
		}
	}
}

func mustAEAD(t *testing.T, s Secrets) sealedEnv {
	t.Helper()
	gcm, err := s.aead()
	if err != nil {
		t.Fatalf("aead: %v", err)
	}
	return sealedEnv{gcm: gcm}
}

// TestSealEnvInvalidUTF8FailsSafely: a value JSON cannot encode must fail
// with the generic sentinel, never an error echoing the value.
func TestSealEnvInvalidUTF8FailsSafely(t *testing.T) {
	st := &Store{env: mustAEAD(t, testMaster(t))}
	owner := uuid.MustParse("0196b1d0-6c34-7cc0-9a8f-2a1d64c51b07")
	bad := "prefix-\xff\xfe-suffix"
	_, err := st.sealEnv(map[string]string{"SECRET": bad}, owner, "bx_aaaacccc")
	if !errors.Is(err, errEnvEncode) {
		t.Fatal("seal invalid UTF-8: unexpected error class")
	}
	if err != nil && strings.Contains(err.Error(), "prefix") {
		t.Fatal("seal error echoes secret material")
	}
}

// TestSealEnvRequiresMigratedStore: a store opened without an install key
// must refuse to seal instead of dereferencing a nil cipher.
func TestSealEnvRequiresMigratedStore(t *testing.T) {
	st := &Store{}
	owner := uuid.MustParse("0196b1d0-6c34-7cc0-9a8f-2a1d64c51b09")
	if _, err := st.sealEnv(map[string]string{"K": "v"}, owner, "bx_aaaacccc"); !errors.Is(err, errStoreNotMigrated) {
		t.Fatalf("seal before migrate: %v", err)
	}
}

// TestDecodeEnvMalformedFailsSafely: even authenticated-but-malformed
// plaintext reports the generic sentinel without echoing bytes.
func TestDecodeEnvMalformedFailsSafely(t *testing.T) {
	e := mustAEAD(t, testMaster(t))
	owner := uuid.MustParse("0196b1d0-6c34-7cc0-9a8f-2a1d64c51b08")
	for _, plain := range []string{`{"broken`, `not json at all`, `{"k":"v"`} {
		if _, err := decodeEnv([]byte(plain)); !errors.Is(err, errEnvDecode) {
			t.Fatal("decode malformed plaintext: unexpected error class")
		}
	}
	// End to end: genuinely sealed garbage decodes to the same sentinel.
	sealed, err := e.seal([]byte(`{"AUTHENTICATED_GARBAGE`), aad(owner, "bx_aaaacccc", "env"))
	if err != nil {
		t.Fatal(err)
	}
	opened, err := e.open(sealed, aad(owner, "bx_aaaacccc", "env"))
	if err != nil {
		t.Fatal(err)
	}
	_, decodeErr := decodeEnv(opened)
	if !errors.Is(decodeErr, errEnvDecode) {
		t.Fatalf("decode authenticated garbage: %v", decodeErr)
	}
	if strings.Contains(decodeErr.Error(), "GARBAGE") {
		t.Fatal("sentinel leaked material")
	}
}
