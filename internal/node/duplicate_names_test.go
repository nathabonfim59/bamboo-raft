package node

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/qemu"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

// TestDuplicatedNamesAgree pins the names two packages each spell on their own
// because neither may import the other: the maintenance QEMU's -name marker
// (qemu builds it, the jailer's argv validator checks it) and the QMP
// socket's file name (qemu creates it, storage's confined opener finds it).
// A drift would make the jailer refuse every maintenance launch, or the node
// look for a socket that QEMU never made.
func TestDuplicatedNamesAgree(t *testing.T) {
	for _, id := range []string{ids.Sandbox(), "bx_00000000", "bx_zzzzzzzz"} {
		if got, want := jailer.MaintenanceName(id), qemu.MaintenanceName(id); got != want {
			t.Errorf("jailer.MaintenanceName(%q) = %q, qemu.MaintenanceName = %q", id, got, want)
		}
	}
	if storage.QMPSocketName != qemu.QMPFilename {
		t.Errorf("storage.QMPSocketName = %q, qemu.QMPFilename = %q", storage.QMPSocketName, qemu.QMPFilename)
	}
}

// TestKVMQEMUPrefixBin: BBRAFT_TEST_QEMU_PREFIX must name a usable prefix.
func TestKVMQEMUPrefixBin(t *testing.T) {
	stub := func(t *testing.T, dir string, tools ...string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Join(dir, "bin"), 0o755); err != nil {
			t.Fatal(err)
		}
		for _, tool := range tools {
			if err := os.WriteFile(filepath.Join(dir, "bin", tool), []byte("#!/bin/sh\n"), 0o755); err != nil {
				t.Fatal(err)
			}
		}
	}
	good, noImg, plain := t.TempDir(), t.TempDir(), t.TempDir()
	stub(t, good, "qemu-system-x86_64", "qemu-img")
	stub(t, noImg, "qemu-system-x86_64")
	stub(t, plain, "qemu-system-x86_64", "qemu-img")
	if err := os.Chmod(filepath.Join(plain, "bin", "qemu-img"), 0o644); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		prefix string
		ok     bool
	}{
		{"usable", good, true},
		{"no qemu-img", noImg, false},
		{"not executable", plain, false},
		{"missing", filepath.Join(good, "nope"), false},
		{"relative", "qemu/prefix", false},
		{"not clean", good + "/../" + filepath.Base(good), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bin, err := kvmQEMUPrefixBin(tt.prefix)
			if (err == nil) != tt.ok || (tt.ok && bin != filepath.Join(tt.prefix, "bin")) {
				t.Fatalf("kvmQEMUPrefixBin(%q) = %q, %v; want ok=%v", tt.prefix, bin, err, tt.ok)
			}
		})
	}
}
