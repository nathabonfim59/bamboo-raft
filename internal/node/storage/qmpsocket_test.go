package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestRemoveStaleQMPSocket proves a crashed QEMU's leftover socket is
// removed while anything else under qmp.sock is refused and kept.
func TestRemoveStaleQMPSocket(t *testing.T) {
	for _, tt := range []struct {
		name   string
		make   func(t *testing.T, p string)
		reason string // "" = success, the name is gone afterwards
	}{
		{name: "missing", make: func(*testing.T, string) {}},
		{name: "socket", make: func(t *testing.T, p string) {
			if err := syscall.Mknod(p, syscall.S_IFSOCK|0o600, 0); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "regular file", reason: "leaving it alone", make: func(t *testing.T, p string) {
			if err := os.WriteFile(p, []byte("not a socket"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "directory", reason: "leaving it alone", make: func(t *testing.T, p string) {
			if err := os.Mkdir(p, 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", reason: "leaving it alone", make: func(t *testing.T, p string) {
			if err := os.Symlink("/dev/null", p); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, id := phase2TestVM(t, nil)
			p := filepath.Join(s.VMDir(id), QMPSocketName)
			tt.make(t, p)
			err := s.RemoveStaleQMPSocket(id)
			_, lerr := os.Lstat(p)
			if tt.reason == "" {
				if err != nil {
					t.Fatalf("RemoveStaleQMPSocket: %v", err)
				}
				if !errors.Is(lerr, fs.ErrNotExist) {
					t.Fatalf("%s still present after removal: %v", p, lerr)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("RemoveStaleQMPSocket = %v, want %q", err, tt.reason)
			}
			if lerr != nil {
				t.Fatalf("refused %s was not kept: %v", p, lerr)
			}
		})
	}
	s, _ := phase2TestVM(t, nil)
	if err := s.RemoveStaleQMPSocket("../escape"); err == nil {
		t.Fatal("RemoveStaleQMPSocket accepted an invalid id")
	}
}
