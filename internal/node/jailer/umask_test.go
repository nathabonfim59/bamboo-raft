package jailer

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
)

// jailerUmaskChildEnv marks the re-exec'ed child of
// TestJailBuilderModesUnderUmask; only that child process sets umask 0077.
const jailerUmaskChildEnv = "BBRAFT_JAILER_UMASK0077_CHILD"

// jailerUmaskRuntime returns two trusted (root-owned) runtime sources staged
// at a QEMU and a library path, so copyRuntime creates nested directories.
func jailerUmaskRuntime(t *testing.T) []runtimeFile {
	t.Helper()
	var files []runtimeFile
	for source, target := range map[string]string{"/usr/bin/true": "usr/bin/qemu-system-x86_64", "/usr/bin/false": "usr/lib/x86_64-linux-gnu/libjailtest.so.1"} {
		resolved, err := filepath.EvalSymlinks(source)
		if err != nil {
			t.Skipf("no %s: %v", source, err)
		}
		info, err := os.Stat(resolved)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, runtimeFile{source: resolved, target: target, size: uint64(info.Size())})
	}
	// files[0] must be the QEMU binary.
	slices.SortFunc(files, func(a, b runtimeFile) int { return strings.Compare(a.target, b.target) })
	return files
}

// TestJailBuilderModesUnderUmask: the jail tree's modes are the declared
// ones whatever the process umask is. The production unit runs the node
// with UMask=0077; a 0755 mkdir would leave 0700 root-owned directories the
// dropped jail uid cannot traverse (its /proc/self/status open then fails).
// The scenario runs in a re-exec'ed child that sets umask 0077 for itself,
// so this test process's umask is never changed.
func TestJailBuilderModesUnderUmask(t *testing.T) {
	if os.Getenv(jailerUmaskChildEnv) == "1" {
		syscall.Umask(0o077)
		if got := syscall.Umask(0o077); got != 0o077 {
			t.Fatalf("child umask %#o, want 0077", got)
		}
		jailerUmaskScenario(t)
		return
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestJailBuilderModesUnderUmask$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), jailerUmaskChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("umask 0077 child: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "--- PASS: TestJailBuilderModesUnderUmask") || strings.Contains(string(out), "--- SKIP") {
		t.Fatalf("the umask 0077 child did not run the scenario:\n%s", out)
	}
}

func jailerUmaskScenario(t *testing.T) {
	root, _, _ := jailerReviewRoots(t)
	base := path.Join(jailsDir, jailerReviewID)
	if err := root.Mkdir(base, 0o700); err != nil {
		t.Fatal(err)
	}
	var owned []ownedEntry
	b := newJailBuilder(root, base, func(e ownedEntry) { owned = append(owned, e) }, nil)
	if _, err := populateJail(t.Context(), b, jailerUmaskRuntime(t)); err != nil {
		t.Fatal(err)
	}
	want := map[string]fs.FileMode{
		"root": 0o755, "root/proc": 0o755, "root/control": 0o755, "root/dev": 0o755, "root/tmp": 0o755,
		"root/usr": 0o755, "root/usr/bin": 0o755, "root/usr/lib": 0o755, "root/usr/lib/x86_64-linux-gnu": 0o755,
		"root/serial.log":                                0o600,
		"root/usr/bin/qemu-system-x86_64":                0o555,
		"root/usr/lib/x86_64-linux-gnu/libjailtest.so.1": 0o555,
	}
	var got []string
	for _, e := range owned {
		got = append(got, e.Path)
		info, err := root.Lstat(path.Join(base, e.Path))
		if err != nil {
			t.Fatal(err)
		}
		mode, ok := want[e.Path]
		if !ok {
			t.Errorf("unexpected entry %s", e.Path)
			continue
		}
		if info.Mode().Perm() != mode || info.Mode()&(fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky) != 0 || info.IsDir() != e.Dir {
			t.Errorf("%s mode %v (dir %v), want exactly %v under umask 0077", e.Path, info.Mode(), e.Dir, mode)
		}
	}
	if len(got) != len(want) {
		t.Errorf("recorded %q, want exactly %d entries", got, len(want))
	}
	if err := b.verify(t.Context(), owned); err != nil {
		t.Fatalf("inventory after exact modes: %v", err)
	}

	// The jail cgroup is created with its declared 0755 too. A plain
	// directory stands in for cgroupfs: the first limit write then fails
	// (no cpu.max), after the mode is set, with the created inode returned
	// for the attempt's ownership.
	cg, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cg.Close()
	for name, data := range map[string]string{"cgroup.controllers": "cpu memory io pids\n", "cgroup.subtree_control": ""} {
		if err := cg.WriteFile(name, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	identity, err := prepareCgroup(cg, jailerReviewID, Limits{CPUQuotaPercent: 100, MemoryBytes: 64 << 20, Pids: 16, IO: IOLimits{Device: BlockDevice{Major: 8, Minor: 1}, ReadBPS: 1024}}, nil)
	if err == nil || !strings.Contains(err.Error(), "cpu.max") || !identity.valid() {
		t.Fatalf("prepareCgroup = %v, %v: want the created cgroup's identity and the cpu.max write failure", identity, err)
	}
	if info, err := cg.Lstat(cgroupName(jailerReviewID)); err != nil || info.Mode().Perm() != 0o755 || !info.IsDir() {
		t.Fatalf("jail cgroup %v, %v: want exactly 0755 under umask 0077", info, err)
	}
}

// TestJailBuilderDirModeIsPinned: the exact mode is set only on the
// directory this builder created and recorded, opened without following a
// final symlink and proven by name and inode; a replacement or a fault
// fails the mkdir with the created directory still recorded (rollback owns
// it) and the replacement untouched.
func TestJailBuilderDirModeIsPinned(t *testing.T) {
	setup := func(t *testing.T, hook func(root *os.Root, name string) error) (*os.Root, string, *[]ownedEntry, error) {
		root, _, _ := jailerReviewRoots(t)
		base := path.Join(jailsDir, jailerReviewID)
		if err := root.Mkdir(base, 0o700); err != nil {
			t.Fatal(err)
		}
		owned := &[]ownedEntry{}
		b := newJailBuilder(root, base, func(e ownedEntry) { *owned = append(*owned, e) }, func(op jailOp, rel string) error {
			if op == opDirMode && rel == "root/dev" {
				return hook(root, path.Join(base, rel))
			}
			return nil
		})
		if err := b.mkdir("root", 0o755); err != nil {
			t.Fatal(err)
		}
		return root, base, owned, b.mkdir("root/dev", 0o755)
	}
	recorded := func(owned []ownedEntry, rel string) bool {
		return slices.ContainsFunc(owned, func(e ownedEntry) bool { return e.Path == rel && e.Dir })
	}
	t.Run("a fault before the mode keeps the directory recorded", func(t *testing.T) {
		_, _, owned, err := setup(t, func(*os.Root, string) error { return errJailerInjected })
		if !errors.Is(err, errJailerInjected) || !recorded(*owned, "root/dev") {
			t.Fatalf("mkdir = %v, recorded %v: want the injected failure with root/dev owned", err, *owned)
		}
	})
	t.Run("a foreign directory under the name is never chmod'ed", func(t *testing.T) {
		root, base, owned, err := setup(t, func(root *os.Root, name string) error {
			if err := root.Rename(name, name+".ours"); err != nil {
				return err
			}
			return root.Mkdir(name, 0o700)
		})
		if err == nil {
			t.Fatal("a replaced directory was accepted")
		}
		info, serr := root.Lstat(path.Join(base, "root/dev"))
		if serr != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("foreign replacement %v, %v: want it untouched at 0700", info, serr)
		}
		if !recorded(*owned, "root/dev") {
			t.Fatal("the created directory was not recorded")
		}
	})
	t.Run("a symlink under the name is never followed", func(t *testing.T) {
		root, base, _, err := setup(t, func(root *os.Root, name string) error {
			if err := root.Rename(name, name+".ours"); err != nil {
				return err
			}
			if err := root.Chmod(name+".ours", 0o700); err != nil {
				return err
			}
			return root.Symlink("dev.ours", name)
		})
		if err == nil {
			t.Fatal("a symlinked name was accepted")
		}
		info, serr := root.Lstat(path.Join(base, "root/dev.ours"))
		if serr != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("symlink target %v, %v: want it untouched at 0700", info, serr)
		}
		if li, lerr := root.Lstat(path.Join(base, "root/dev")); lerr != nil || li.Mode()&fs.ModeSymlink == 0 {
			t.Fatalf("the symlink was replaced: %v, %v", li, lerr)
		}
	})
}

// TestCgroupDirModeFaultKeepsOwnership: a failure at the new group's
// dir-mode step (after its identity is captured) returns that valid
// identity with the error, so the attempt retains and later removes exactly
// that inode; a retry then creates a new group with the exact mode.
func TestCgroupDirModeFaultKeepsOwnership(t *testing.T) {
	cg, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cg.Close()
	for name, data := range map[string]string{"cgroup.controllers": "cpu memory io pids\n", "cgroup.subtree_control": ""} {
		if err := cg.WriteFile(name, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	limits := Limits{CPUQuotaPercent: 100, MemoryBytes: 64 << 20, Pids: 16, IO: IOLimits{Device: BlockDevice{Major: 8, Minor: 1}, ReadBPS: 1024}}
	name := cgroupName(jailerReviewID)
	var points []string
	failing := cgroupFS{root: cg, fault: func(op jailOp, rel string) error {
		points = append(points, string(op)+" "+rel)
		return errJailerInjected
	}}
	identity, err := failing.Prepare(jailerReviewID, limits)
	if !errors.Is(err, errJailerInjected) || !identity.valid() {
		t.Fatalf("Prepare = %v, %v: want the injected dir-mode failure with the created identity", identity, err)
	}
	if !slices.Equal(points, []string{"dir-mode " + name}) {
		t.Fatalf("fault points %q", points)
	}
	retained, err := statDirectoryNoFollow(cg, name)
	if err != nil || !retained.equal(identity) {
		t.Fatalf("the created group %v, %v is not the returned identity %v", retained, err, identity)
	}
	// Rollback removes exactly the owned inode.
	if err := failing.Remove(jailerReviewID, identity); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, err := cg.Lstat(name); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the owned group survived rollback: %v", err)
	}
	// Retry: the hook stands in for cgroupfs creating the interface files.
	retry := cgroupFS{root: cg, fault: func(op jailOp, rel string) error {
		for _, f := range []string{"cpu.max", "memory.max", "io.max", "pids.max"} {
			if err := cg.WriteFile(path.Join(rel, f), nil, 0o600); err != nil {
				return err
			}
		}
		return nil
	}}
	again, err := retry.Prepare(jailerReviewID, limits)
	if err != nil || !again.valid() {
		t.Fatalf("retry = %v, %v: want a new owned group", again, err)
	}
	// The removed inode number may be reused (ext4), so the name is proven
	// instead of comparing with the old identity.
	if named, err := statDirectoryNoFollow(cg, name); err != nil || !named.equal(again) {
		t.Fatalf("retried group %v, %v is not the returned identity %v", named, err, again)
	}
	if info, err := cg.Lstat(name); err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("retried group %v, %v: want exactly 0755", info, err)
	}
}
