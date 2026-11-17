package jailer

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// HelperArg is the private re-execution selector. The node command checks
// HelperMain before parsing flags or opening platform credentials.
const HelperArg = "__bbraft_jailer_v1"

// AdmitHelperArg selects the startup admission helper (Manager.Admit).
const AdmitHelperArg = "__bbraft_jailer_admit_v1"

// HelperExitCode is the only exit status of a helper that returns before
// exec, whatever the reason: setup error, a closed release gate or a nil
// error. A tool's completion proof relies on it: a clean 0 can only come from
// the exec'd program.
const HelperExitCode = 125

// RunHelperIfSelected runs the selected helper and never returns when one is
// selected: a helper that reaches exec becomes the program, and every other
// outcome exits HelperExitCode. It must be the first thing the node command
// does, before flags, environment or credentials are read. Error text goes
// to the parent's status pipe only and never contains credentials.
func RunHelperIfSelected() {
	if handled, _ := HelperMain(); handled {
		// The helper reports its error on the status pipe; whatever it was,
		// a return before exec is never a clean exit.
		os.Exit(HelperExitCode)
	}
}

// HelperMain runs the same-PID setup helper when os.Args selects HelperArg.
// handled is false for every normal invocation. The helper locks its OS thread
// until exec because Linux credentials and namespace operations are per-thread.
// The parent, not this helper, created the mount/PID/IPC namespaces with clone
// flags; there is no helper fork and QEMU remains the direct host child.
func HelperMain() (handled bool, err error) {
	if len(os.Args) >= 2 && os.Args[1] == AdmitHelperArg {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		return true, admitHelperMain()
	}
	if len(os.Args) < 2 || os.Args[1] != HelperArg {
		return false, nil
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if len(os.Args) != 5 {
		return true, errors.New("jailer: helper expects config, release and status fds")
	}
	fds := make([]int, 3)
	for i := range fds {
		fd, err := strconv.Atoi(os.Args[i+2])
		if err != nil || fd < 3 {
			return true, fmt.Errorf("jailer: invalid helper fd %q", os.Args[i+2])
		}
		fds[i] = fd
	}
	return true, runHelper(fds[0], fds[1], fds[2])
}

func runHelper(configFD, releaseFD, statusFD int) (returnErr error) {
	status := os.NewFile(uintptr(statusFD), "exec-status")
	if status == nil {
		return errors.New("jailer: invalid status descriptor")
	}
	// EOF on this pipe is the parent's successful-exec proof. Setup errors are
	// written before exit; no setup/config/status descriptor survives exec.
	if _, err := unix.FcntlInt(uintptr(statusFD), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		status.Close()
		return fmt.Errorf("jailer: cloexec status: %w", err)
	}
	defer func() {
		if returnErr != nil {
			message := []byte(returnErr.Error())
			if len(message) > 4096 {
				message = message[:4096]
			}
			_, _ = status.Write(message)
		}
		_ = status.Close()
	}()

	config := os.NewFile(uintptr(configFD), "helper-config")
	if config == nil {
		return errors.New("jailer: invalid config descriptor")
	}
	var cfg helperConfig
	err := json.UnmarshalRead(io.LimitReader(config, 1<<20), &cfg)
	closeErr := config.Close()
	if err != nil {
		return fmt.Errorf("jailer: decode helper config: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("jailer: close helper config: %w", closeErr)
	}
	if cfg.Root == "" || !filepath.IsAbs(cfg.Binary) || cfg.UID == 0 || cfg.RandomRdev == 0 || cfg.NullRdev == 0 {
		return errors.New("jailer: incomplete helper config")
	}
	if cfg.Tool {
		if cfg.ControlFD != 0 || cfg.LogFD != 0 || cfg.Control.valid() || cfg.Log.valid() || cfg.KVMRdev != 0 {
			return errors.New("jailer: inconsistent tool helper config")
		}
	} else if cfg.ControlFD < 3 || cfg.LogFD < 3 || !cfg.Control.valid() || !cfg.Log.valid() || cfg.KVMRdev == 0 {
		return errors.New("jailer: incomplete helper config")
	}

	release := os.NewFile(uintptr(releaseFD), "helper-release")
	if release == nil {
		return errors.New("jailer: invalid release descriptor")
	}
	var gate [1]byte
	_, err = io.ReadFull(release, gate[:])
	release.Close()
	if err != nil {
		return fmt.Errorf("jailer: launch cancelled before setup: %w", err)
	}
	if gate[0] != 1 {
		return errors.New("jailer: invalid helper release byte")
	}

	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		return fmt.Errorf("jailer: private mount propagation: %w", err)
	}
	if !cfg.Tool {
		if err := attachPinned(cfg.ControlFD, filepath.Join(cfg.Root, "control"), cfg.Control); err != nil {
			return fmt.Errorf("jailer: mount control: %w", err)
		}
		if err := attachPinned(cfg.LogFD, filepath.Join(cfg.Root, "serial.log"), cfg.Log); err != nil {
			return fmt.Errorf("jailer: mount serial log: %w", err)
		}
		if err := makePrivateDevice(filepath.Join(cfg.Root, "dev", "kvm"), cfg.KVMRdev, cfg.UID); err != nil {
			return fmt.Errorf("jailer: private KVM device: %w", err)
		}
	}
	if err := makePrivateDevice(filepath.Join(cfg.Root, "dev", "urandom"), cfg.RandomRdev, cfg.UID); err != nil {
		return fmt.Errorf("jailer: private urandom device: %w", err)
	}
	// QEMU opens /dev/null to probe OFD locks; without it its image locks
	// fall back to process-associated locks that any close drops.
	if err := makePrivateDevice(filepath.Join(cfg.Root, "dev", "null"), cfg.NullRdev, cfg.UID); err != nil {
		return fmt.Errorf("jailer: private null device: %w", err)
	}
	if err := unix.Mount("proc", filepath.Join(cfg.Root, "proc"), "proc", unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC, ""); err != nil {
		return fmt.Errorf("jailer: mount pid-namespace proc: %w", err)
	}
	if err := unix.Mount("tmpfs", filepath.Join(cfg.Root, "tmp"), "tmpfs", unix.MS_NOSUID|unix.MS_NODEV|unix.MS_NOEXEC, "mode=1777,size=16m"); err != nil {
		return fmt.Errorf("jailer: mount private tmp: %w", err)
	}
	// Remount only the root mount read-only. Control, proc and tmp remain
	// separate writable mounts; the staged QEMU/runtime stays root-owned 0555.
	if err := unix.Mount(cfg.Root, cfg.Root, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
		return fmt.Errorf("jailer: bind jail root: %w", err)
	}
	if err := unix.Mount("", cfg.Root, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY|unix.MS_NOSUID, ""); err != nil {
		return fmt.Errorf("jailer: remount jail runtime readonly: %w", err)
	}
	if err := unix.Chroot(cfg.Root); err != nil {
		return fmt.Errorf("jailer: chroot: %w", err)
	}
	if err := unix.Chdir("/"); err != nil {
		return fmt.Errorf("jailer: chdir jail root: %w", err)
	}
	if cfg.Tool {
		if err := applyToolRlimits(cfg); err != nil {
			return err
		}
	}
	if err := dropCredentials(cfg.UID); err != nil {
		return err
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("jailer: no_new_privs: %w", err)
	}
	// QEMU is staged at its host path, so its relocated data and firmware
	// directories resolve inside the chroot exactly as on the host.
	argv := append([]string{cfg.Binary}, cfg.Args...)
	env := []string{"PATH=/usr/bin:/bin", "LC_ALL=C", "HOME=/", "TMPDIR=/tmp"}
	if err := unix.Exec(cfg.Binary, argv, env); err != nil {
		return fmt.Errorf("jailer: exec QEMU: %w", err)
	}
	return nil
}

// moveMountFlags attach a detached tree given by its fd alone.
const moveMountFlags = unix.MOVE_MOUNT_F_EMPTY_PATH

// attachPinned moves a detached pinned mount (created by the parent with
// open_tree in the host namespace, already nosuid/nodev/noexec and private)
// onto target in this namespace. The tree and the mounted target must both
// be the expected pinned inode; the tree fd is closed before exec.
func attachPinned(fd int, target string, want inodeIdentity) error {
	if err := checkFDIdentity(fd, want); err != nil {
		return err
	}
	if err := unix.MoveMount(fd, "", unix.AT_FDCWD, target, moveMountFlags); err != nil {
		return fmt.Errorf("move_mount: %w", err)
	}
	if err := checkPathIdentity(target, want); err != nil {
		return err
	}
	return unix.Close(fd)
}

// checkFDIdentity requires fd to be the expected inode.
func checkFDIdentity(fd int, want inodeIdentity) error {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return fmt.Errorf("inspect pinned mount: %w", err)
	}
	if got := (inodeIdentity{Dev: st.Dev, Ino: st.Ino}); !got.equal(want) {
		return fmt.Errorf("pinned mount is inode %d:%d, want %d:%d", got.Dev, got.Ino, want.Dev, want.Ino)
	}
	return nil
}

// checkPathIdentity requires the mounted target to be the expected inode.
func checkPathIdentity(target string, want inodeIdentity) error {
	var st unix.Stat_t
	if err := unix.Lstat(target, &st); err != nil {
		return fmt.Errorf("inspect mounted %s: %w", target, err)
	}
	if got := (inodeIdentity{Dev: st.Dev, Ino: st.Ino}); !got.equal(want) {
		return fmt.Errorf("mounted %s is inode %d:%d, want %d:%d", target, got.Dev, got.Ino, want.Dev, want.Ino)
	}
	return nil
}

// makePrivateDevice creates a new jail-owned inode with the fixed host rdev.
// It never chmods or chowns a bind-mounted host device, which would mutate the
// host's /dev/kvm, /dev/urandom or /dev/null. Device-cgroup permission is still required.
func makePrivateDevice(target string, rdev uint64, uid uint32) error {
	if err := unix.Mknod(target, unix.S_IFCHR|0o600, int(rdev)); err != nil {
		return err
	}
	if err := unix.Chown(target, int(uid), int(uid)); err != nil {
		return err
	}
	return unix.Chmod(target, 0o600)
}

func dropCredentials(uid uint32) error {
	if err := unix.Prctl(unix.PR_CAP_AMBIENT, unix.PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0); err != nil {
		return fmt.Errorf("jailer: clear ambient capabilities: %w", err)
	}
	for capability := uintptr(0); capability < 64; capability++ {
		if err := unix.Prctl(unix.PR_CAPBSET_DROP, capability, 0, 0, 0); err != nil && !errors.Is(err, unix.EINVAL) {
			return fmt.Errorf("jailer: drop bounding capability %d: %w", capability, err)
		}
	}
	if err := unix.Setgroups([]int{}); err != nil {
		return fmt.Errorf("jailer: clear supplementary groups: %w", err)
	}
	if err := unix.Setgid(int(uid)); err != nil {
		return fmt.Errorf("jailer: set gid: %w", err)
	}
	if err := unix.Setuid(int(uid)); err != nil {
		return fmt.Errorf("jailer: set uid: %w", err)
	}
	header := unix.CapUserHeader{Version: 0x20080522, Pid: 0}
	data := [2]unix.CapUserData{}
	if err := unix.Capset(&header, &data[0]); err != nil {
		return fmt.Errorf("jailer: clear permitted/effective/inheritable capabilities: %w", err)
	}
	if err := verifyDroppedCredentials(uid); err != nil {
		return err
	}
	return nil
}

func verifyDroppedCredentials(uid uint32) error {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return fmt.Errorf("jailer: verify credentials: %w", err)
	}
	wantID := strconv.FormatUint(uint64(uid), 10)
	for line := range strings.SplitSeq(string(raw), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		switch key {
		case "Uid", "Gid":
			for id := range strings.FieldsSeq(value) {
				if id != wantID {
					return fmt.Errorf("jailer: %s was not fully changed to %s", key, wantID)
				}
			}
		case "Groups":
			if value != "" {
				return fmt.Errorf("jailer: supplementary groups survived: %s", value)
			}
		case "CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb":
			capabilities, err := strconv.ParseUint(value, 16, 64)
			if err != nil || capabilities != 0 {
				return fmt.Errorf("jailer: %s was not cleared (%s)", key, value)
			}
		}
	}
	return nil
}

// applyToolRlimits sets a tool helper's resource limits before the
// credential drop: the tool never dumps core. Its memory bound is the
// birth-entered cgroup's memory.max (with pids.max and cpu.max), not
// RLIMIT_AS: the helper is a Go process whose own address space already
// exceeds a tool's bound, so an RLIMIT_AS there makes any later Go heap
// growth a fatal runtime error before the exec.
func applyToolRlimits(helperConfig) error {
	if err := unix.Setrlimit(unix.RLIMIT_CORE, &unix.Rlimit{}); err != nil {
		return fmt.Errorf("jailer: tool RLIMIT_CORE: %w", err)
	}
	return nil
}
