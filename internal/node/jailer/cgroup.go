package jailer

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

const requiredControllerNames = "cpu memory io pids"

func cgroupName(sandboxID string) string { return "qemu-" + sandboxID }

// cgroupOps is the Manager's view of its delegated cgroup v2 parent. Every
// operation after Prepare proves the recorded directory inode first. Tests
// substitute an in-memory implementation; production uses cgroupFS.
type cgroupOps interface {
	// Prepare creates a new group and writes its limits. A valid identity is
	// returned whenever the directory was created, even with an error.
	Prepare(sandboxID string, limits Limits) (inodeIdentity, error)
	// OpenOwned opens the group directory for CLONE_INTO_CGROUP after
	// proving it is still the owned inode. The caller closes it.
	OpenOwned(sandboxID string, owned inodeIdentity) (*os.File, error)
	// Joined verifies that pid, spawned into the group by clone3, is a
	// member. It confirms containment; it never attaches.
	Joined(sandboxID string, pid int, owned inodeIdentity) error
	// Empty reports true for a missing group or one without processes.
	Empty(sandboxID string, owned inodeIdentity) (bool, error)
	Kill(sandboxID string, owned inodeIdentity) error
	// Remove is a no-op for a missing group and refuses a replaced one.
	Remove(sandboxID string, owned inodeIdentity) error
	// List returns the names of the parent's child groups (read-only).
	List() ([]string, error)
	Close() error
}

// cgroupFS implements cgroupOps under an os.Root opened at the delegated
// parent, so no operation can escape it through a symlink. fault is this
// instance's test seam (nil in production): it runs at opDirMode, after a
// new group's identity is captured and before its mode is set.
type cgroupFS struct {
	root  *os.Root
	fault func(op jailOp, rel string) error
}

func (c cgroupFS) Prepare(id string, limits Limits) (inodeIdentity, error) {
	return prepareCgroup(c.root, id, limits, c.fault)
}

func (c cgroupFS) OpenOwned(id string, owned inodeIdentity) (*os.File, error) {
	if !owned.valid() {
		return nil, fmt.Errorf("jailer: cgroup %s has no recorded identity", id)
	}
	f, err := c.root.OpenFile(cgroupName(id), os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("jailer: open owned cgroup %s: %w", id, err)
	}
	identity, err := fileInode(f)
	if err != nil || !identity.equal(owned) {
		f.Close()
		if err == nil {
			err = errors.New("replaced")
		}
		return nil, fmt.Errorf("jailer: cgroup %s is not the owned inode: %w", id, err)
	}
	return f, nil
}

func (c cgroupFS) Joined(id string, pid int, owned inodeIdentity) error {
	return joinedCgroup(c.root, id, pid, owned)
}

func (c cgroupFS) Empty(id string, owned inodeIdentity) (bool, error) {
	return cgroupEmptyOwned(c.root, id, owned)
}

func (c cgroupFS) Kill(id string, owned inodeIdentity) error {
	return killOwnedCgroup(c.root, id, owned)
}

func (c cgroupFS) Remove(id string, owned inodeIdentity) error {
	return removeOwnedCgroup(c.root, id, owned)
}

func (c cgroupFS) List() ([]string, error) {
	dir, err := c.root.OpenFile(".", os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("jailer: list cgroups: %w", err)
	}
	defer dir.Close()
	entries, err := dir.ReadDir(maxRecordFiles + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("jailer: list cgroups: %w", err)
	}
	if len(entries) > maxRecordFiles {
		return nil, fmt.Errorf("jailer: more than %d child cgroups", maxRecordFiles)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

func (c cgroupFS) Close() error { return c.root.Close() }

// prepareCgroup creates a new cgroup and never adopts an existing path. The
// returned identity is available even when a later limit write fails, allowing
// the same attempt to clean up only the inode it created. fault (nil in
// production) runs at opDirMode once that identity is captured.
func prepareCgroup(root *os.Root, sandboxID string, limits Limits, fault func(op jailOp, rel string) error) (inodeIdentity, error) {
	if err := enableControllers(root); err != nil {
		return inodeIdentity{}, err
	}
	name := cgroupName(sandboxID)
	if err := root.Mkdir(name, 0o755); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return inodeIdentity{}, fmt.Errorf("jailer: cgroup %s already exists without a new attempt's ownership", sandboxID)
		}
		return inodeIdentity{}, fmt.Errorf("jailer: create cgroup: %w", err)
	}
	identity, err := statDirectoryNoFollow(root, name)
	if err != nil {
		return inodeIdentity{}, fmt.Errorf("jailer: identify new cgroup: %w", err)
	}
	// Its declared mode, whatever the process umask (UMask=0077) left. The
	// identity is returned on failure so the attempt still owns the inode.
	// The seam and a real chmod failure share this one return.
	if fault != nil {
		err = fault(opDirMode, name)
	}
	if err == nil {
		err = chmodCreatedDir(root, name, identity, 0o755)
	}
	if err != nil {
		return identity, fmt.Errorf("jailer: set the mode of the new cgroup: %w", err)
	}
	values := map[string]string{
		"cpu.max":    strconv.FormatUint(uint64(limits.CPUQuotaPercent)*1000, 10) + " 100000",
		"memory.max": strconv.FormatUint(limits.MemoryBytes, 10),
		"pids.max":   strconv.FormatUint(uint64(limits.Pids), 10),
		"io.max":     ioMax(limits.IO),
	}
	for _, file := range []string{"cpu.max", "memory.max", "io.max", "pids.max"} {
		if err := writeCgroup(root, path.Join(name, file), values[file]); err != nil {
			return identity, err
		}
	}
	return identity, nil
}

func enableControllers(root *os.Root) error {
	raw, _, err := readBoundedRegular(root, "cgroup.controllers", 1<<20)
	if err != nil {
		return fmt.Errorf("jailer: read cgroup controllers (cgroup v2 required): %w", err)
	}
	available := strings.Fields(string(raw))
	required := strings.Fields(requiredControllerNames)
	for _, controller := range required {
		if !slices.Contains(available, controller) {
			return fmt.Errorf("jailer: cgroup controller %q is unavailable", controller)
		}
	}
	value := " +" + strings.Join(required, " +")
	if err := writeCgroup(root, "cgroup.subtree_control", value); err != nil {
		return fmt.Errorf("jailer: enable delegated controllers: %w", err)
	}
	return nil
}

func ioMax(io IOLimits) string {
	var value strings.Builder
	fmt.Fprintf(&value, "%d:%d", io.Device.Major, io.Device.Minor)
	for _, limit := range []struct {
		name  string
		value uint64
	}{
		{"rbps", io.ReadBPS},
		{"wbps", io.WriteBPS},
		{"riops", io.ReadIOPS},
		{"wiops", io.WriteIOPS},
	} {
		if limit.value != 0 {
			fmt.Fprintf(&value, " %s=%d", limit.name, limit.value)
		}
	}
	return value.String()
}

func writeCgroup(root *os.Root, name, value string) error {
	f, err := root.OpenFile(name, os.O_WRONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return fmt.Errorf("jailer: open %s: %w", name, err)
	}
	written, writeErr := f.WriteString(value)
	if writeErr == nil && written != len(value) {
		writeErr = fmt.Errorf("short write %d of %d", written, len(value))
	}
	closeErr := f.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return fmt.Errorf("jailer: write %s: %w", name, err)
	}
	return nil
}

func verifyOwnedCgroup(root *os.Root, sandboxID string, expected inodeIdentity) error {
	if !expected.valid() {
		return fmt.Errorf("jailer: cgroup %s has no recorded identity", sandboxID)
	}
	current, err := statDirectoryNoFollow(root, cgroupName(sandboxID))
	if err != nil {
		return fmt.Errorf("jailer: inspect owned cgroup %s: %w", sandboxID, err)
	}
	if !current.equal(expected) {
		return fmt.Errorf("jailer: cgroup %s was replaced; preserving it", sandboxID)
	}
	return nil
}

// joinedCgroup proves pid is listed in the owned group's cgroup.procs.
func joinedCgroup(root *os.Root, sandboxID string, pid int, expected inodeIdentity) error {
	if err := verifyOwnedCgroup(root, sandboxID, expected); err != nil {
		return err
	}
	raw, _, err := readBoundedRegular(root, path.Join(cgroupName(sandboxID), "cgroup.procs"), 1<<20)
	if err != nil {
		return fmt.Errorf("jailer: read cgroup membership: %w", err)
	}
	want := strconv.Itoa(pid)
	for member := range strings.FieldsSeq(string(raw)) {
		if member == want {
			return nil
		}
	}
	return fmt.Errorf("jailer: pid %d is not a member of cgroup %s", pid, sandboxID)
}

func cgroupEmptyOwned(root *os.Root, sandboxID string, expected inodeIdentity) (bool, error) {
	name := cgroupName(sandboxID)
	if _, err := statDirectoryNoFollow(root, name); errors.Is(err, fs.ErrNotExist) {
		return true, nil
	}
	if err := verifyOwnedCgroup(root, sandboxID, expected); err != nil {
		return false, err
	}
	raw, _, err := readBoundedRegular(root, path.Join(name, "cgroup.procs"), 1<<20)
	if errors.Is(err, fs.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("jailer: read cgroup membership: %w", err)
	}
	return len(strings.Fields(string(raw))) == 0, nil
}

func killOwnedCgroup(root *os.Root, sandboxID string, expected inodeIdentity) error {
	if err := verifyOwnedCgroup(root, sandboxID, expected); err != nil {
		return err
	}
	return writeCgroup(root, path.Join(cgroupName(sandboxID), "cgroup.kill"), "1")
}

func removeOwnedCgroup(root *os.Root, sandboxID string, expected inodeIdentity) error {
	name := cgroupName(sandboxID)
	if _, err := statDirectoryNoFollow(root, name); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err := verifyOwnedCgroup(root, sandboxID, expected); err != nil {
		return err
	}
	if err := root.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("jailer: remove cgroup: %w", err)
	}
	return nil
}
