package server

import (
	"crypto/subtle"
	"testing"
)

func TestDeriveUploadSigningKeyIsStableAndIsolated(t *testing.T) {
	master := [32]byte{1, 2, 3, 4}
	key, err := DeriveUploadSigningKey(master)
	if err != nil {
		t.Fatal(err)
	}
	again, err := DeriveUploadSigningKey(master)
	if err != nil {
		t.Fatal(err)
	}
	if subtle.ConstantTimeCompare(key[:], again[:]) != 1 {
		t.Fatal("derivation is not stable across calls")
	}
	if len(key) != UploadSigningKeyLen {
		t.Fatalf("key length = %d", len(key))
	}
	if subtle.ConstantTimeCompare(key[:], master[:]) == 1 {
		t.Fatal("upload key equals the installation master key")
	}
	other, err := DeriveUploadSigningKey([32]byte{9, 9, 9, 9})
	if err != nil {
		t.Fatal(err)
	}
	if subtle.ConstantTimeCompare(key[:], other[:]) == 1 {
		t.Fatal("different masters derived the same upload key")
	}
}
