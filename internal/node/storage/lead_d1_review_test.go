package storage

import (
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLeadStorageRejectsLiveInRootSymlinks(t *testing.T) {
	s, img := storageJailFixture(t, nil, nil)
	pins, err := s.PinLaunch(t.Context(), storageJailID, img)
	if err != nil {
		t.Fatal(err)
	}
	pins.Close()
	vm := s.VMDir(storageJailID)
	if err := os.Symlink(diskName, filepath.Join(vm, "link")); err != nil {
		t.Fatal(err)
	}
	if f, err := s.OpenVMEntry(t.Context(), storageJailID, "link", os.O_RDONLY); err == nil {
		f.Close()
		t.Error("OpenVMEntry followed a live relative final symlink")
	}
	if f, err := openNoFollow(s.root, filepath.Join(vmsDir, storageJailID, "link")); err == nil {
		f.Close()
		t.Error("openNoFollow followed a live relative final symlink")
	}
}

func TestLeadPinsRefuseSymlinkToRecordedInode(t *testing.T) {
	for _, name := range []string{ControlDirName, ConsoleLogName} {
		t.Run(name, func(t *testing.T) {
			s, img := storageJailFixture(t, nil, nil)
			pins, err := s.PinLaunch(t.Context(), storageJailID, img)
			if err != nil {
				t.Fatal(err)
			}
			pins.Close()
			original := filepath.Join(s.VMDir(storageJailID), name)
			if err := os.Rename(original, original+".kept"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(name+".kept", original); err != nil {
				t.Fatal(err)
			}
			a, err := os.Stat(original)
			if err != nil {
				t.Fatal(err)
			}
			b, err := os.Stat(original + ".kept")
			if err != nil || !os.SameFile(a, b) {
				t.Fatal("fixture is not a live same-inode link")
			}
			if pins, err := s.PinLaunch(t.Context(), storageJailID, img); err == nil {
				pins.Close()
				t.Fatal("PinLaunch accepted a symlink to the recorded inode")
			}
		})
	}
}

func TestLeadPinNoFollowRefusesFIFOWithoutBlocking(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "fifo")
	if err := unix.Mkfifo(name, 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	done := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		if joined {
			return
		}
		fd, err := unix.Open(name, unix.O_RDWR|unix.O_NONBLOCK, 0)
		if err != nil {
			t.Errorf("unblock FIFO fixture: %v", err)
			return
		}
		defer unix.Close(fd)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("FIFO pinning goroutine never joined")
		}
	})
	go func() {
		f, err := pinNoFollow(root, "fifo", os.O_RDONLY)
		if f != nil {
			f.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		joined = true
		if err == nil {
			t.Fatal("pinNoFollow accepted a FIFO")
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("pinNoFollow blocked before it could refuse a FIFO")
	}
}
