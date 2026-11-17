package storage

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"uuid"
)

// A10 jailed launch pins: layout creation and reuse, conservative refusal of
// every unproven layout or disk graph, context-first cancellation and
// manifest-fault durability.

const storageJailID = "bx_pppppppp"

// storageQCOW returns a qcow2 v3 header naming backing (empty: standalone).
func storageQCOW(backing string, incompatible uint64) []byte {
	raw := make([]byte, 512)
	copy(raw, []byte{'Q', 'F', 'I', 0xfb})
	binary.BigEndian.PutUint32(raw[4:8], 3)
	if backing != "" {
		binary.BigEndian.PutUint64(raw[8:16], 256)
		binary.BigEndian.PutUint32(raw[16:20], uint32(len(backing)))
		copy(raw[256:], backing)
	}
	binary.BigEndian.PutUint64(raw[72:80], incompatible)
	binary.BigEndian.PutUint32(raw[100:104], 104)
	return raw
}

// storageFaults injects manifest/sync faults through the store's seams. fail
// returns the error for the n-th call of op (1-based), or nil.
type storageFaults struct {
	mu    sync.Mutex
	calls map[string]int
	fail  func(op string, n int, root *os.Root) error
}

func (f *storageFaults) hit(op string, root *os.Root) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[op]++
	n, fail := f.calls[op], f.fail
	f.mu.Unlock()
	if fail == nil {
		return nil
	}
	return fail(op, n, root)
}

// storageJailFixture creates a store whose verified image already has a
// real qcow2 base (hashed into its manifest before verification) and one VM
// whose overlay names that base exactly. base overrides the base bytes.
func storageJailFixture(t *testing.T, base []byte, faults *storageFaults) (*Store, Image) {
	t.Helper()
	imageDir := t.TempDir()
	if base == nil {
		base = storageQCOW("", 0)
	}
	writeImage(t, imageDir, "img-v1", func(dir string, m map[string]any) {
		if err := os.WriteFile(filepath.Join(dir, "root.qcow2"), base, 0o444); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(base)
		m["artifacts"].(map[string]any)["root.qcow2"] = map[string]any{"sha256": hex.EncodeToString(sum[:]), "size": len(base)}
	})
	opts := SnapshotOptions{
		Rename: func(root *os.Root, old, new string) error {
			if err := faults.hit("rename", root); err != nil {
				return err
			}
			return root.Rename(old, new)
		},
		SyncDir: func(root *os.Root, name string) error {
			if err := faults.hit("syncdir", root); err != nil {
				return err
			}
			return syncDir(root, name)
		},
		SyncFile: func(f *os.File) error {
			if err := faults.hit("syncfile:"+filepath.Base(f.Name()), nil); err != nil {
				return err
			}
			return f.Sync()
		},
	}
	s, err := Open(Config{
		DataDir: t.TempDir(), ImageDir: imageDir, Snapshots: opts,
		Imager: testImager{virtualSize: testGiB << 30, format: "qcow2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	images, err := s.VerifyImages(t.Context())
	if err != nil || len(images) != 1 {
		t.Fatalf("VerifyImages = %v, %v", images, err)
	}
	img := images[0]
	meta := VMMeta{SandboxID: storageJailID, Image: img.ID, Resources: Resources{VCPUs: 1, MemoryMiB: 512, DiskGiB: testGiB}, VsockCID: 3, CreatedAtUnixMs: 1}
	if err := s.CreateVM(t.Context(), meta, img); err != nil {
		t.Fatal(err)
	}
	storageWriteOverlay(t, s, storageQCOW(filepath.Join(img.Dir, "root.qcow2"), 0))
	if faults != nil {
		faults.mu.Lock()
		faults.calls = nil
		faults.mu.Unlock()
	}
	return s, img
}

func storageWriteOverlay(t *testing.T, s *Store, content []byte) {
	t.Helper()
	if err := os.WriteFile(s.DiskPath(storageJailID), content, 0o600); err != nil {
		t.Fatal(err)
	}
}

// storageEntry is one filesystem entry's exact state.
type storageEntry struct {
	ino     uint64
	mode    fs.FileMode
	content string // sha256 of a regular file's bytes, or a symlink target
}

// storageTree records every entry under dirs.
func storageTree(t *testing.T, dirs ...string) map[string]storageEntry {
	t.Helper()
	out := map[string]storageEntry{}
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := os.Lstat(p)
			if err != nil {
				return err
			}
			e := storageEntry{ino: info.Sys().(*syscall.Stat_t).Ino, mode: info.Mode()}
			switch {
			case info.Mode().IsRegular():
				raw, err := os.ReadFile(p)
				if err != nil {
					return err
				}
				sum := sha256.Sum256(raw)
				e.content = hex.EncodeToString(sum[:])
			case info.Mode()&fs.ModeSymlink != 0:
				e.content, _ = os.Readlink(p)
			}
			out[p] = e
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func storageIdentity(t *testing.T, f *os.File) Identity {
	t.Helper()
	id, err := identityOf(f)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func storagePinOnce(t *testing.T, s *Store, img Image) (control, console Identity) {
	t.Helper()
	pins, err := s.PinLaunch(t.Context(), storageJailID, img)
	if err != nil {
		t.Fatal(err)
	}
	control, console = storageIdentity(t, pins.Control), storageIdentity(t, pins.Console)
	if err := pins.Close(); err != nil {
		t.Fatal(err)
	}
	return control, console
}

func TestStorePinLaunchRecordsAndReusesLayout(t *testing.T) {
	s, img := storageJailFixture(t, nil, nil)
	pins, err := s.PinLaunch(t.Context(), storageJailID, img)
	if err != nil {
		t.Fatal(err)
	}
	control, console := storageIdentity(t, pins.Control), storageIdentity(t, pins.Console)
	if pins.BaseName != filepath.Join(img.Dir, "root.qcow2") {
		t.Fatalf("BaseName = %q", pins.BaseName)
	}
	for name, f := range map[string]*os.File{"overlay": pins.Overlay, "base": pins.Base, "kernel": pins.Kernel, "initrd": pins.Initrd} {
		if f == nil {
			t.Fatalf("%s not pinned", name)
		}
	}
	if err := pins.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pins.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	meta, err := s.ReadVM(storageJailID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Control == nil || *meta.Control != control || meta.Console == nil || *meta.Console != console {
		t.Fatalf("manifest identities %+v %+v, pinned %+v %+v", meta.Control, meta.Console, control, console)
	}
	again, err := s.PinLaunch(t.Context(), storageJailID, img)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if storageIdentity(t, again.Control) != control || storageIdentity(t, again.Console) != console {
		t.Fatal("a second launch did not reuse the recorded control and console")
	}
}

func TestStorePinLaunchRefusesUnprovenLayout(t *testing.T) {
	tests := []struct {
		name   string
		base   []byte
		setup  func(t *testing.T, s *Store, img Image)
		reason string
	}{
		{"unrecorded control dir", nil, func(t *testing.T, s *Store, _ Image) {
			if err := os.Mkdir(filepath.Join(s.VMDir(storageJailID), ControlDirName), 0o700); err != nil {
				t.Fatal(err)
			}
		}, "unrecorded"},
		{"unrecorded console", nil, func(t *testing.T, s *Store, _ Image) {
			if err := os.WriteFile(filepath.Join(s.VMDir(storageJailID), ConsoleLogName), []byte("old"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, "unrecorded"},
		{"replaced control dir", nil, func(t *testing.T, s *Store, img Image) {
			storagePinOnce(t, s, img)
			dir := filepath.Join(s.VMDir(storageJailID), ControlDirName)
			if err := os.Rename(dir, dir+".old"); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
		}, "not the recorded"},
		{"stale socket in control", nil, func(t *testing.T, s *Store, img Image) {
			storagePinOnce(t, s, img)
			if err := syscall.Mknod(filepath.Join(s.VMDir(storageJailID), ControlDirName, "qmp.sock"), syscall.S_IFSOCK|0o600, 0); err != nil {
				t.Fatal(err)
			}
		}, "not empty"},
		{"control mode", nil, func(t *testing.T, s *Store, img Image) {
			storagePinOnce(t, s, img)
			if err := os.Chmod(filepath.Join(s.VMDir(storageJailID), ControlDirName), 0o755); err != nil {
				t.Fatal(err)
			}
		}, "mode"},
		{"symlinked console", nil, func(t *testing.T, s *Store, img Image) {
			storagePinOnce(t, s, img)
			p := filepath.Join(s.VMDir(storageJailID), ConsoleLogName)
			if err := os.Rename(p, p+".old"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(p+".old", p); err != nil {
				t.Fatal(err)
			}
		}, "console"},
		{"host backing", nil, func(t *testing.T, s *Store, _ Image) {
			storageWriteOverlay(t, s, storageQCOW("/etc/shadow", 0))
		}, "backing"},
		{"json backing", nil, func(t *testing.T, s *Store, _ Image) {
			storageWriteOverlay(t, s, storageQCOW(`json:{"driver":"file","filename":"/etc/shadow"}`, 0))
		}, "backing"},
		{"protocol backing", nil, func(t *testing.T, s *Store, _ Image) {
			storageWriteOverlay(t, s, storageQCOW("nbd://203.0.113.1/x", 0))
		}, "backing"},
		{"relative backing", nil, func(t *testing.T, s *Store, _ Image) {
			storageWriteOverlay(t, s, storageQCOW("root.qcow2", 0))
		}, "backing"},
		{"standalone overlay", nil, func(t *testing.T, s *Store, _ Image) {
			storageWriteOverlay(t, s, storageQCOW("", 0))
		}, "backing"},
		{"dirty overlay", nil, func(t *testing.T, s *Store, img Image) {
			storageWriteOverlay(t, s, storageQCOW(filepath.Join(img.Dir, "root.qcow2"), 1))
		}, "feature"},
		{"external data file", nil, func(t *testing.T, s *Store, img Image) {
			storageWriteOverlay(t, s, storageQCOW(filepath.Join(img.Dir, "root.qcow2"), 4))
		}, "feature"},
		{"raw overlay", nil, func(t *testing.T, s *Store, _ Image) {
			storageWriteOverlay(t, s, []byte("not a qcow2 image"))
		}, "qcow2"},
		// The verified base itself names a backing file: the verified bytes,
		// not a later rewrite, are what the launch refuses.
		{"base with backing", storageQCOW("/srv/other.qcow2", 0), func(*testing.T, *Store, Image) {}, "standalone"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, img := storageJailFixture(t, tt.base, nil)
			tt.setup(t, s, img)
			before := storageTree(t, s.VMDir(storageJailID), img.Dir)
			pins, err := s.PinLaunch(t.Context(), storageJailID, img)
			if err == nil {
				pins.Close()
				t.Fatalf("PinLaunch accepted %s", tt.name)
			}
			if !strings.Contains(err.Error(), tt.reason) {
				t.Fatalf("PinLaunch = %v, want %q", err, tt.reason)
			}
			if after := storageTree(t, s.VMDir(storageJailID), img.Dir); !maps.Equal(before, after) {
				t.Fatalf("a refused PinLaunch changed the VM or image tree\nbefore %v\nafter  %v", before, after)
			}
		})
	}
}

func TestStorePinLaunchCanceledChangesNothing(t *testing.T) {
	s, img := storageJailFixture(t, nil, nil)
	before := storageTree(t, s.VMDir(storageJailID))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if pins, err := s.PinLaunch(ctx, storageJailID, img); !errors.Is(err, context.Canceled) {
		pins.Close()
		t.Fatalf("PinLaunch = %v, want Canceled", err)
	}
	if err := s.WriteJailedLaunchIntent(ctx, storageJailID, uuid.NewV7().String(), strings.Repeat("ab", 32)); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteJailedLaunchIntent = %v, want Canceled", err)
	}
	if f, err := s.OpenVMEntry(ctx, storageJailID, diskName, os.O_RDONLY); !errors.Is(err, context.Canceled) {
		f.Close()
		t.Fatalf("OpenVMEntry = %v, want Canceled", err)
	}
	if after := storageTree(t, s.VMDir(storageJailID)); !maps.Equal(before, after) {
		t.Fatal("canceled calls changed the VM tree")
	}
}

// TestStorePinLaunchLayoutFaults: a layout whose record is not published is
// removed again by its own identities (never a replacement), a possibly
// published record keeps it for a retry by identity, and every outcome is
// retryable.
func TestStorePinLaunchLayoutFaults(t *testing.T) {
	injected := errors.New("injected fault")
	cases := []struct {
		name      string
		fail      func(op string, n int, root *os.Root) error
		keepNew   bool // the created entries survive the failure
		published bool // the manifest names them after the failure
	}{
		{"parent fsync before the record", func(op string, n int, _ *os.Root) error {
			if op == "syncdir" && n == 1 {
				return injected
			}
			return nil
		}, false, false},
		{"manifest rename", func(op string, n int, _ *os.Root) error {
			if op == "rename" && n == 1 {
				return injected
			}
			return nil
		}, false, false},
		{"directory sync after the rename", func(op string, n int, _ *os.Root) error {
			if op == "syncdir" && n == 2 {
				return injected
			}
			return nil
		}, true, true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			faults := &storageFaults{}
			s, img := storageJailFixture(t, nil, faults)
			faults.mu.Lock()
			faults.fail = tt.fail
			faults.mu.Unlock()
			if pins, err := s.PinLaunch(t.Context(), storageJailID, img); !errors.Is(err, injected) {
				pins.Close()
				t.Fatalf("PinLaunch = %v, want the injected fault", err)
			}
			dir := s.VMDir(storageJailID)
			_, controlErr := os.Lstat(filepath.Join(dir, ControlDirName))
			_, consoleErr := os.Lstat(filepath.Join(dir, ConsoleLogName))
			if kept := controlErr == nil && consoleErr == nil; kept != tt.keepNew {
				t.Fatalf("created entries kept = %v (%v, %v), want %v", kept, controlErr, consoleErr, tt.keepNew)
			}
			if !tt.keepNew && (!errors.Is(controlErr, fs.ErrNotExist) || !errors.Is(consoleErr, fs.ErrNotExist)) {
				t.Fatal("a partial layout remained without its record")
			}
			meta, err := s.ReadVM(storageJailID)
			if err != nil {
				t.Fatal(err)
			}
			if (meta.Control != nil) != tt.published {
				t.Fatalf("manifest names the layout = %v, want %v", meta.Control != nil, tt.published)
			}
			faults.mu.Lock()
			faults.fail = nil
			faults.mu.Unlock()
			pins, err := s.PinLaunch(t.Context(), storageJailID, img)
			if err != nil {
				t.Fatalf("retry: %v", err)
			}
			defer pins.Close()
			if tt.published && storageIdentity(t, pins.Control) != *meta.Control {
				t.Fatal("the retry did not reuse the published layout")
			}
		})
	}
	t.Run("replacement during an unpublished failure is preserved", func(t *testing.T) {
		faults := &storageFaults{}
		s, img := storageJailFixture(t, nil, faults)
		dir := filepath.Join(s.VMDir(storageJailID), ControlDirName)
		faults.mu.Lock()
		faults.fail = func(op string, n int, _ *os.Root) error {
			if op != "rename" {
				return nil
			}
			// Someone swaps the new control directory for their own before
			// the record is published.
			if err := os.Rename(dir, dir+".ours"); err != nil {
				t.Error(err)
			}
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Error(err)
			}
			return injected
		}
		faults.mu.Unlock()
		if _, err := s.PinLaunch(t.Context(), storageJailID, img); !errors.Is(err, injected) {
			t.Fatalf("PinLaunch = %v", err)
		}
		if _, err := os.Lstat(dir); err != nil {
			t.Fatal("the replacement directory was removed")
		}
	})
}

func TestStoreWriteJailedLaunchIntent(t *testing.T) {
	s, _ := storageJailFixture(t, nil, nil)
	attempt := uuid.NewV7().String()
	sha := strings.Repeat("ab", 32)
	if err := s.WriteJailedLaunchIntent(t.Context(), storageJailID, attempt, sha); err != nil {
		t.Fatal(err)
	}
	meta, err := s.ReadVM(storageJailID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.QEMU == nil || meta.QEMU.State != QEMULaunching || meta.QEMU.JailAttempt != attempt || meta.QEMU.ArgvSHA256 != sha {
		t.Fatalf("intent = %+v", meta.QEMU)
	}
	for _, bad := range [][2]string{{"", sha}, {"not-a-uuid", sha}, {strings.ToUpper(attempt), sha}, {attempt, "short"}, {attempt, strings.Repeat("AB", 32)}} {
		if err := s.WriteJailedLaunchIntent(t.Context(), storageJailID, bad[0], bad[1]); err == nil {
			t.Fatalf("WriteJailedLaunchIntent(%q, %q) accepted", bad[0], bad[1])
		}
	}
	if after, err := s.ReadVM(storageJailID); err != nil || *after.QEMU != *meta.QEMU {
		t.Fatal("a refused intent changed the manifest")
	}
}

func TestStoreWriteJailedLaunchIntentFaults(t *testing.T) {
	injected := errors.New("injected fault")
	faults := &storageFaults{}
	s, _ := storageJailFixture(t, nil, faults)
	faults.mu.Lock()
	faults.fail = func(op string, n int, _ *os.Root) error {
		if op == "rename" {
			return injected
		}
		return nil
	}
	faults.mu.Unlock()
	attempt := uuid.NewV7().String()
	if err := s.WriteJailedLaunchIntent(t.Context(), storageJailID, attempt, strings.Repeat("ab", 32)); !errors.Is(err, injected) {
		t.Fatalf("intent = %v", err)
	}
	if meta, _ := s.ReadVM(storageJailID); meta.QEMU != nil {
		t.Fatal("an unpublished intent is visible")
	}
	faults.mu.Lock()
	faults.fail = func(op string, n int, _ *os.Root) error {
		if op == "syncdir" {
			return injected
		}
		return nil
	}
	faults.mu.Unlock()
	if err := s.WriteJailedLaunchIntent(t.Context(), storageJailID, attempt, strings.Repeat("ab", 32)); !errors.Is(err, ErrManifestDurabilityUnknown) {
		t.Fatalf("intent after an uncertain sync = %v, want ErrManifestDurabilityUnknown", err)
	}
	if meta, _ := s.ReadVM(storageJailID); meta.QEMU == nil || meta.QEMU.JailAttempt != attempt {
		t.Fatal("the possibly published intent does not name its attempt")
	}
}

func TestReadVMRejectsInvalidJailedIntent(t *testing.T) {
	s, _ := storageJailFixture(t, nil, nil)
	meta, err := s.ReadVM(storageJailID)
	if err != nil {
		t.Fatal(err)
	}
	meta.QEMU = &QEMURecord{State: QEMULaunching, BootID: "boot", JailAttempt: "nope", ArgvSHA256: strings.Repeat("ab", 32)}
	if err := s.writeManifest(meta); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadVM(storageJailID); err == nil || !strings.Contains(err.Error(), "attempt") {
		t.Fatalf("ReadVM = %v, want an attempt refusal", err)
	}
	meta.QEMU.JailAttempt, meta.QEMU.ArgvSHA256 = uuid.NewV7().String(), ""
	if err := s.writeManifest(meta); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadVM(storageJailID); err == nil {
		t.Fatal("ReadVM accepted an attempt without its argv digest")
	}
	meta.QEMU = nil
	meta.Control = &Identity{Dev: 1, Ino: 2}
	if err := s.writeManifest(meta); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadVM(storageJailID); err == nil {
		t.Fatal("ReadVM accepted a control identity without its console")
	}
}

func TestStoreOpenVMEntryDoesNotFollowSymlinks(t *testing.T) {
	s, _ := storageJailFixture(t, nil, nil)
	link := filepath.Join(s.VMDir(storageJailID), "link")
	if err := os.Symlink(s.DiskPath(storageJailID), link); err != nil {
		t.Fatal(err)
	}
	if f, err := s.OpenVMEntry(t.Context(), storageJailID, "link", os.O_RDONLY); err == nil {
		f.Close()
		t.Fatal("OpenVMEntry followed a symlink")
	}
	f, err := s.OpenVMEntry(t.Context(), storageJailID, diskName, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	for _, name := range []string{"../x", "a/b", "", ".", ".."} {
		if f, err := s.OpenVMEntry(t.Context(), storageJailID, name, os.O_RDONLY); err == nil || !strings.Contains(err.Error(), "entry name") {
			f.Close()
			t.Fatalf("OpenVMEntry(%q) = %v", name, err)
		}
	}
}
