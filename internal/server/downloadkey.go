package server

import (
	"crypto/hkdf"
	"crypto/sha256"
	"fmt"
)

// DownloadSigningKeyLen is the HMAC-SHA256 key length used to sign download
// tokens. Download handlers require at least this many bytes.
const DownloadSigningKeyLen = 32

// downloadKeyInfo is the fixed HKDF context of the download-URL signing key.
// It is part of the on-wire contract: every server instance derives the same
// key from the same installation master, and the value is unrelated to the
// upload URL signing key because the context differs.
const downloadKeyInfo = "bbraft/download-url-signing/v1"

// DeriveDownloadSigningKey derives the download URL signing key from the
// installation master key with HKDF-SHA256 under a fixed, download-specific
// context. It is deterministic across API instances, so a URL signed by one
// process verifies on another, and it never runs HKDF with an empty or
// per-process key. The result is key material: never log it or send it to a
// guest or node.
//
// The upload and download contexts differ, so a configuration mistake that
// handed the same master key to both issuers still produces unrelated keys.
func DeriveDownloadSigningKey(master [32]byte) ([32]byte, error) {
	var key [32]byte
	raw, err := hkdf.Key(sha256.New, master[:], nil, downloadKeyInfo, len(key))
	if err != nil {
		return key, fmt.Errorf("server: derive download signing key: %w", err)
	}
	copy(key[:], raw)
	return key, nil
}
