package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
)

// jailPlan is the validated jailed-QEMU configuration of a tap-mode node.
type jailPlan struct {
	// UIDs is the whole configured range, all of it the jailer's pool: every
	// VM, the CPU probe and (D1) each tool attempt reserve one uid from it.
	UIDs   jailer.UIDRange
	Cgroup string
	Binary string
	// ToolBinary is the trusted qemu-img staged for image tools.
	ToolBinary  string
	Firmware    []string
	LibraryDirs []string
	IO          jailer.IOLimits
}

// cgroup2SuperMagic is statfs(2)'s f_type of a cgroup v2 mount.
const cgroup2SuperMagic = 0x63677270

// jail validates the jail flags. In tap mode the uid range, delegated
// cgroup, IO device and at least one IO rate are required; the dev/test user
// mode refuses every operator-supplied jail setting, by flag or BBRAFT_*
// variable, even one equal to its default, zero or empty, and returns nil.
func (o *options) jail() (*jailPlan, error) {
	if o.networkMode != networkModeTap {
		// A local fixed array keeps package state immutable.
		for _, name := range [...]string{
			"node.jail-uid-range",
			"node.jail-cgroup",
			"node.jail-qemu-binary",
			"node.jail-tool-binary",
			"node.jail-firmware",
			"node.jail-library-dirs",
			"node.jail-io-device",
			"node.jail-io-read-bps",
			"node.jail-io-write-bps",
			"node.jail-io-read-iops",
			"node.jail-io-write-iops",
		} {
			if o.supplied[name] {
				return nil, fmt.Errorf("-%s cannot be combined with -node.network-mode=%s: the dev/test user mode runs QEMU unjailed", name, o.networkMode)
			}
		}
		return nil, nil
	}
	uids, err := parseUIDRange(o.jailUIDRange)
	if err != nil {
		return nil, err
	}
	if o.jailCgroup == "" {
		return nil, errors.New("-node.jail-cgroup is required in tap mode")
	}
	if !cleanAbsolute(o.jailCgroup) || filepath.Base(o.jailCgroup) != "vms" {
		return nil, fmt.Errorf("-node.jail-cgroup %q must be the absolute clean <service cgroup>/vms directory", o.jailCgroup)
	}
	if !cleanAbsolute(o.jailQEMUBinary) {
		return nil, fmt.Errorf("-node.jail-qemu-binary %q is not an absolute clean path", o.jailQEMUBinary)
	}
	if !cleanAbsolute(o.jailToolBinary) {
		return nil, fmt.Errorf("-node.jail-tool-binary %q is not an absolute clean path", o.jailToolBinary)
	}
	firmware, err := cleanAbsoluteList("-node.jail-firmware", o.jailFirmware)
	if err != nil {
		return nil, err
	}
	libraries, err := cleanAbsoluteList("-node.jail-library-dirs", o.jailLibraryDirs)
	if err != nil {
		return nil, err
	}
	if o.jailIODevice == "" {
		return nil, errors.New("-node.jail-io-device is required in tap mode")
	}
	device, err := parseBlockDevice(o.jailIODevice)
	if err != nil {
		return nil, err
	}
	io := jailer.IOLimits{Device: device, ReadBPS: o.jailIOReadBPS, WriteBPS: o.jailIOWriteBPS, ReadIOPS: o.jailIOReadIOPS, WriteIOPS: o.jailIOWriteIOPS}
	if io.ReadBPS|io.WriteBPS|io.ReadIOPS|io.WriteIOPS == 0 {
		return nil, errors.New("at least one -node.jail-io-* rate limit is required in tap mode")
	}
	return &jailPlan{UIDs: uids, Cgroup: o.jailCgroup, Binary: o.jailQEMUBinary, ToolBinary: o.jailToolBinary, Firmware: firmware, LibraryDirs: libraries, IO: io}, nil
}

func cleanAbsolute(p string) bool { return p != "" && filepath.IsAbs(p) && filepath.Clean(p) == p }

// cleanAbsoluteList parses a comma-separated list of distinct absolute
// clean paths; empty is none.
func cleanAbsoluteList(flag, value string) ([]string, error) {
	if value == "" {
		return nil, nil
	}
	var out []string
	for p := range strings.SplitSeq(value, ",") {
		if !cleanAbsolute(p) {
			return nil, fmt.Errorf("%s: %q is not an absolute clean path", flag, p)
		}
		if slices.Contains(out, p) {
			return nil, fmt.Errorf("%s: duplicate %q", flag, p)
		}
		out = append(out, p)
	}
	return out, nil
}

// parseUIDRange parses "min-max": nonzero, at least two uids, below 2^31.
func parseUIDRange(value string) (jailer.UIDRange, error) {
	if value == "" {
		return jailer.UIDRange{}, errors.New("-node.jail-uid-range is required in tap mode")
	}
	lo, hi, ok := strings.Cut(value, "-")
	minUID, err1 := strconv.ParseUint(lo, 10, 32)
	maxUID, err2 := strconv.ParseUint(hi, 10, 32)
	if !ok || err1 != nil || err2 != nil || minUID == 0 || maxUID <= minUID || maxUID >= 1<<31 {
		return jailer.UIDRange{}, fmt.Errorf("-node.jail-uid-range %q must be min-max with 0 < min < max < 2^31", value)
	}
	return jailer.UIDRange{Min: uint32(minUID), Max: uint32(maxUID)}, nil
}

// parseBlockDevice parses "MAJ:MIN" with a nonzero major.
func parseBlockDevice(value string) (jailer.BlockDevice, error) {
	maj, minor, ok := strings.Cut(value, ":")
	ma, err1 := strconv.ParseUint(maj, 10, 32)
	mi, err2 := strconv.ParseUint(minor, 10, 32)
	if !ok || err1 != nil || err2 != nil || ma == 0 {
		return jailer.BlockDevice{}, fmt.Errorf("-node.jail-io-device %q must be MAJ:MIN of a whole disk", value)
	}
	return jailer.BlockDevice{Major: uint32(ma), Minor: uint32(mi)}, nil
}

// checkJailAccounts refuses a uid range that overlaps any account or group
// id in the host's passwd and group databases (the files; NSS sources are
// documented as unsupported).
func checkJailAccounts(passwd, group io.Reader, uids jailer.UIDRange) error {
	inRange := func(v uint64) bool { return v >= uint64(uids.Min) && v <= uint64(uids.Max) }
	scan := func(name string, r io.Reader, fields int, ids ...int) error {
		lines := bufio.NewScanner(r)
		for n := 1; lines.Scan(); n++ {
			line := strings.TrimSpace(lines.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			parts := strings.Split(line, ":")
			if len(parts) < fields {
				return fmt.Errorf("jail uid range: %s line %d is malformed", name, n)
			}
			for _, i := range ids {
				id, err := strconv.ParseUint(parts[i], 10, 32)
				if err != nil {
					return fmt.Errorf("jail uid range: %s line %d has a malformed id", name, n)
				}
				if inRange(id) {
					return fmt.Errorf("jail uid range %d-%d overlaps %s entry %q (id %d)", uids.Min, uids.Max, name, parts[0], id)
				}
			}
		}
		if err := lines.Err(); err != nil {
			return fmt.Errorf("jail uid range: read %s: %w", name, err)
		}
		return nil
	}
	if err := scan("passwd", passwd, 7, 2, 3); err != nil {
		return err
	}
	return scan("group", group, 4, 2)
}

// delegationEnv is what the read-only cgroup delegation proof inspects.
type delegationEnv struct {
	// CgroupRoot is the cgroup2 mount (/sys/fs/cgroup).
	CgroupRoot string
	// FSType is statfs(CgroupRoot).Type.
	FSType int64
	// SelfCgroup is /proc/self/cgroup.
	SelfCgroup []byte
	// SelfNS and InitNS are the inode numbers of /proc/self/ns/cgroup and
	// /proc/1/ns/cgroup.
	SelfNS, InitNS uint64
	// Xattr reads one extended attribute of a path.
	Xattr func(path, name string) (string, error)
	// JailCgroup is the configured -node.jail-cgroup.
	JailCgroup string
	// Owner is the uid that must own the delegated hierarchy (0 on a host).
	Owner uint32
}

// checkRootSupervisor requires the real and effective uid and gid to be 0:
// the production supervisor is root (it stages a root-owned runtime and
// hands each jail its own uid), never a capability-only nonroot process.
func checkRootSupervisor(uid, euid, gid, egid int) error {
	if uid != 0 || euid != 0 || gid != 0 || egid != 0 {
		return fmt.Errorf("jail host: tap mode needs a root supervisor (uid/euid/gid/egid 0), running as %d/%d/%d/%d", uid, euid, gid, egid)
	}
	return nil
}

// checkHierarchyDir proves, without following a final symlink, that path is
// a directory owned by owner that neither group nor others can write.
func checkHierarchyDir(path string, owner uint32) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("jail cgroup: %w", err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	switch {
	case !info.IsDir():
		return nil, fmt.Errorf("jail cgroup: %s is not a directory", path)
	case !ok || st.Uid != owner:
		return nil, fmt.Errorf("jail cgroup: %s is not owned by uid %d", path, owner)
	case info.Mode().Perm()&0o022 != 0:
		return nil, fmt.Errorf("jail cgroup: %s is group- or other-writable (%v)", path, info.Mode().Perm())
	}
	return info, nil
}

// checkDelegation proves, without changing anything, that the node runs in
// the supervisor leaf of a systemd-delegated service cgroup on the unified
// hierarchy outside any cgroup namespace, that the service cgroup holds no
// process and that the configured jail cgroup is exactly <service>/vms. It
// returns the service cgroup directory.
func checkDelegation(env delegationEnv) (string, error) {
	if env.FSType != cgroup2SuperMagic {
		return "", fmt.Errorf("jail cgroup: %s is not a cgroup v2 mount", env.CgroupRoot)
	}
	if env.SelfNS == 0 || env.SelfNS != env.InitNS {
		return "", errors.New("jail cgroup: the node runs inside a cgroup namespace")
	}
	lines := strings.Split(strings.TrimSpace(string(env.SelfCgroup)), "\n")
	if len(lines) != 1 {
		return "", errors.New("jail cgroup: /proc/self/cgroup is not a single unified-hierarchy entry")
	}
	self, ok := strings.CutPrefix(lines[0], "0::")
	if !ok || !cleanAbsolute(self) || path.Base(self) != "supervisor" || path.Dir(self) == "/" {
		return "", fmt.Errorf("jail cgroup: the node is not in a <service>/supervisor leaf (%q)", lines[0])
	}
	service := filepath.Join(env.CgroupRoot, path.Dir(self))
	if _, err := checkHierarchyDir(service, env.Owner); err != nil {
		return "", err
	}
	if value, err := env.Xattr(service, "trusted.delegate"); err != nil || value != "1" {
		return "", fmt.Errorf("jail cgroup: %s is not delegated (trusted.delegate): %v", service, err)
	}
	procs, err := os.ReadFile(filepath.Join(service, "cgroup.procs"))
	if err != nil {
		return "", fmt.Errorf("jail cgroup: %w", err)
	}
	if strings.TrimSpace(string(procs)) != "" {
		return "", fmt.Errorf("jail cgroup: the service cgroup %s holds processes outside its leaves", service)
	}
	if env.JailCgroup != service+"/vms" {
		return "", fmt.Errorf("jail cgroup: -node.jail-cgroup %q must be exactly %s/vms", env.JailCgroup, service)
	}
	if _, err := os.Lstat(env.JailCgroup); err == nil {
		if _, err := checkHierarchyDir(env.JailCgroup, env.Owner); err != nil {
			return "", err
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("jail cgroup: %w", err)
	}
	return service, nil
}

// checkJailIODevice requires dev to be the whole disk under every dir.
func checkJailIODevice(dev jailer.BlockDevice, dirs []string, wholeDisk func(dir string) (jailer.BlockDevice, error)) error {
	for _, dir := range dirs {
		got, err := wholeDisk(dir)
		if err != nil {
			return fmt.Errorf("jail io device: %s: %w", dir, err)
		}
		if got != dev {
			return fmt.Errorf("jail io device: %s is on %d:%d, not the configured %d:%d", dir, got.Major, got.Minor, dev.Major, dev.Minor)
		}
	}
	return nil
}

// prepareJailCgroup enables cpu, memory, io and pids for the service's
// children and creates the jail cgroup if absent, on a root-owned hierarchy.
func prepareJailCgroup(ctx context.Context, serviceDir, jailCgroup string) error {
	return prepareJailCgroupAs(ctx, serviceDir, jailCgroup, 0)
}

// prepareJailCgroupAs is prepareJailCgroup for a hierarchy owned by owner.
// Before its first write it re-proves the service directory (no-follow,
// owner, not group/other writable) and binds that very inode; an existing
// jail cgroup must be a proven directory directly inside it. Cancellation
// is honored before each mutation and before reporting success.
func prepareJailCgroupAs(ctx context.Context, serviceDir, jailCgroup string, owner uint32) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("jail cgroup: %w", context.Cause(ctx))
	}
	if jailCgroup != serviceDir+"/vms" {
		return fmt.Errorf("jail cgroup: %q is not %s/vms", jailCgroup, serviceDir)
	}
	proven, err := checkHierarchyDir(serviceDir, owner)
	if err != nil {
		return err
	}
	service, err := os.OpenRoot(serviceDir)
	if err != nil {
		return fmt.Errorf("jail cgroup: %w", err)
	}
	defer service.Close()
	if opened, err := service.Stat("."); err != nil || !os.SameFile(proven, opened) {
		return fmt.Errorf("jail cgroup: %s changed while it was opened", serviceDir)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("jail cgroup: %w", context.Cause(ctx))
	}
	control, err := service.OpenFile("cgroup.subtree_control", os.O_WRONLY|os.O_TRUNC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("jail cgroup: %w", err)
	}
	_, werr := control.Write([]byte("+cpu +memory +io +pids"))
	if err := errors.Join(werr, control.Close()); err != nil {
		return fmt.Errorf("jail cgroup: enable controllers: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("jail cgroup: %w", context.Cause(ctx))
	}
	if err := service.Mkdir("vms", 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("jail cgroup: %w", err)
	}
	vms, err := service.Lstat("vms")
	if err != nil {
		return fmt.Errorf("jail cgroup: %w", err)
	}
	st, ok := vms.Sys().(*syscall.Stat_t)
	if !vms.IsDir() || !ok || st.Uid != owner || vms.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("jail cgroup: %s is not a directory owned by uid %d that only it can write", jailCgroup, owner)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("jail cgroup: %w", context.Cause(ctx))
	}
	return nil
}

// realJailHost runs the read-only host proofs against the real host: the
// uid range against /etc/passwd and /etc/group, the cgroup delegation and
// the IO device. It changes nothing.
func realJailHost(ctx context.Context, plan *jailPlan, dataDir, imageDir string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", context.Cause(ctx)
	}
	if err := checkRootSupervisor(os.Getuid(), os.Geteuid(), os.Getgid(), os.Getegid()); err != nil {
		return "", err
	}
	passwd, err := os.Open("/etc/passwd")
	if err != nil {
		return "", err
	}
	defer passwd.Close()
	group, err := os.Open("/etc/group")
	if err != nil {
		return "", err
	}
	defer group.Close()
	if err := checkJailAccounts(passwd, group, plan.UIDs); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", context.Cause(ctx)
	}
	const root = "/sys/fs/cgroup"
	var st unix.Statfs_t
	if err := unix.Statfs(root, &st); err != nil {
		return "", fmt.Errorf("jail cgroup: statfs %s: %w", root, err)
	}
	selfCgroup, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", err
	}
	nsIno := func(p string) (uint64, error) {
		var s unix.Stat_t
		if err := unix.Stat(p, &s); err != nil {
			return 0, fmt.Errorf("jail cgroup: %w", err)
		}
		return s.Ino, nil
	}
	selfNS, err := nsIno("/proc/self/ns/cgroup")
	if err != nil {
		return "", err
	}
	initNS, err := nsIno("/proc/1/ns/cgroup")
	if err != nil {
		return "", err
	}
	service, err := checkDelegation(delegationEnv{
		CgroupRoot: root, FSType: int64(st.Type), SelfCgroup: selfCgroup, SelfNS: selfNS, InitNS: initNS,
		Xattr: func(p, name string) (string, error) {
			buf := make([]byte, 64)
			n, err := unix.Lgetxattr(p, name, buf)
			if err != nil {
				return "", err
			}
			return string(buf[:n]), nil
		},
		JailCgroup: plan.Cgroup,
		Owner:      0,
	})
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", context.Cause(ctx)
	}
	if err := checkJailIODevice(plan.IO.Device, []string{dataDir, imageDir}, hostWholeDisk); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", context.Cause(ctx)
	}
	return service, nil
}

// hostWholeDisk returns the whole disk holding dir (or its nearest existing
// ancestor): a partition resolves to its parent disk through sysfs; a
// device-mapper or other unpartitioned device is itself.
func hostWholeDisk(dir string) (jailer.BlockDevice, error) {
	var st unix.Stat_t
	for p := dir; ; p = filepath.Dir(p) {
		err := unix.Stat(p, &st)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.ENOENT) || p == "/" {
			return jailer.BlockDevice{}, err
		}
	}
	maj, minor := unix.Major(st.Dev), unix.Minor(st.Dev)
	sys, err := filepath.EvalSymlinks(fmt.Sprintf("/sys/dev/block/%d:%d", maj, minor))
	if err != nil {
		return jailer.BlockDevice{}, fmt.Errorf("no block device for %d:%d: %w", maj, minor, err)
	}
	if _, err := os.Stat(filepath.Join(sys, "partition")); err == nil {
		raw, err := os.ReadFile(filepath.Join(filepath.Dir(sys), "dev"))
		if err != nil {
			return jailer.BlockDevice{}, err
		}
		return parseBlockDevice(strings.TrimSpace(string(raw)))
	}
	return jailer.BlockDevice{Major: maj, Minor: minor}, nil
}
