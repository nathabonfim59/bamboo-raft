package server

import (
	"crypto/hkdf"
	"crypto/sha256"
	"fmt"
)

// UploadSigningKeyLen is the HMAC-SHA256 key length used to sign upload
// tokens. Upload handlers require at least this many bytes.
const UploadSigningKeyLen = 32

// uploadKeyInfo is the fixed HKDF context. It is part of the on-wire
// contract: every server instance derives the same key from the same
// installation master, and the value is unrelated to the database
// encryption key because the context differs.
const uploadKeyInfo = "bbraft/upload-url-signing/v1"

// DeriveUploadSigningKey derives the upload URL signing key from the
// installation master key with HKDF-SHA256 under a fixed, upload-specific
// context. It is deterministic across API instances, so a URL signed by one
// process verifies on another, and it never runs HKDF with an empty or
// per-process key. The result is key material: never log it or send it to a
// guest or node.
func DeriveUploadSigningKey(master [32]byte) ([32]byte, error) {
	var key [32]byte
	raw, err := hkdf.Key(sha256.New, master[:], nil, uploadKeyInfo, len(key))
	if err != nil {
		return key, fmt.Errorf("server: derive upload signing key: %w", err)
	}
	copy(key[:], raw)
	return key, nil
}
