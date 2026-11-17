package server

import (
	"encoding/hex"
	"testing"
)

// downloadKeyTestMaster is a deterministic installation master for the
// derivation tests.
func downloadKeyTestMaster() [32]byte {
	var master [32]byte
	for i := range master {
		master[i] = byte(i)
	}
	return master
}

// TestDeriveDownloadSigningKeyStable pins the exact HKDF output. A server
// restart, a replica or a future refactor must keep deriving the same key from
// the same master, or every outstanding download URL breaks.
func TestDeriveDownloadSigningKeyStable(t *testing.T) {
	const want = "b2157ee5ac88c551fa548a6006dd52c412951c165f3314ddf2d8d0baa18fd938"
	first, err := DeriveDownloadSigningKey(downloadKeyTestMaster())
	if err != nil {
		t.Fatalf("DeriveDownloadSigningKey: %v", err)
	}
	if got := hex.EncodeToString(first[:]); got != want {
		t.Fatalf("derived key = %s, want %s", got, want)
	}
	if DownloadSigningKeyLen != len(first) {
		t.Fatalf("DownloadSigningKeyLen = %d, want %d", DownloadSigningKeyLen, len(first))
	}
}

// TestDeriveDownloadSigningKeyPurposeSeparated proves the download key is
// unrelated to the upload key and to the master, even though all three come
// from the same installation secret.
func TestDeriveDownloadSigningKeyPurposeSeparated(t *testing.T) {
	master := downloadKeyTestMaster()
	download, err := DeriveDownloadSigningKey(master)
	if err != nil {
		t.Fatalf("DeriveDownloadSigningKey: %v", err)
	}
	upload, err := DeriveUploadSigningKey(master)
	if err != nil {
		t.Fatalf("DeriveUploadSigningKey: %v", err)
	}
	if download == upload {
		t.Fatal("download key equals upload key")
	}
	if download == master {
		t.Fatal("download key equals the installation master")
	}
	var zero [32]byte
	if download == zero {
		t.Fatal("download key is all zeroes")
	}
}

// TestDeriveDownloadSigningKeyDeterministic covers two independent derivations
// (separate replicas) and two different masters.
func TestDeriveDownloadSigningKeyDeterministic(t *testing.T) {
	first, err := DeriveDownloadSigningKey(downloadKeyTestMaster())
	if err != nil {
		t.Fatalf("first derivation: %v", err)
	}
	second, err := DeriveDownloadSigningKey(downloadKeyTestMaster())
	if err != nil {
		t.Fatalf("second derivation: %v", err)
	}
	if first != second {
		t.Fatal("two derivations from the same master differ")
	}

	otherMaster := downloadKeyTestMaster()
	otherMaster[0] ^= 0xff
	other, err := DeriveDownloadSigningKey(otherMaster)
	if err != nil {
		t.Fatalf("other derivation: %v", err)
	}
	if other == first {
		t.Fatal("different masters derived the same key")
	}
}
