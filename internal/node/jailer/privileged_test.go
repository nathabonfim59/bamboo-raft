package jailer

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mdlayher/vsock"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nathabonfim59/bamboo-raft/internal/proto/guestv1"
)

// Privileged jailer tests launch REAL jailed QEMU processes. They run only in
// a throwaway root VM, never on a developer machine or a real host, and only
// with BBRAFT_TEST_JAILER=1; BBRAFT_TEST_REQUIRE_ALL alone does not enable
// them. Once enabled, every missing prerequisite is fatal. Build the test
// binary as the normal user and run only that binary as root (never
// "sudo go test"):
//
//	go test -c -o "$dir/jailer.test" ./internal/node/jailer
//	sudo env BBRAFT_TEST_JAILER=1 \
//	  BBRAFT_TEST_IMAGE_DIR=/path/to/images/out \
//	  BBRAFT_TEST_JAILER_CGROUP=/sys/fs/cgroup/bbraft-test \
//	  BBRAFT_TEST_JAILER_IO_DEVICE=254:0 \
//	  BBRAFT_TEST_JAILER_FIRMWARE=/usr/share/qemu/bios-256k.bin,... \
//	  [BBRAFT_TEST_QEMU=/usr/bin/qemu-system-x86_64] [BBRAFT_TEST_CPU_MODEL=host] \
//	  "$dir/jailer.test" -test.run TestPrivileged -test.v -test.timeout 15m
//
// The image dir holds kernel, initrd and root.qcow2 (image v1 with
// bbraft-guest). The cgroup is a delegated cgroup v2 parent; the IO device is
// the whole block device for io.max. The firmware list is explicit: the boot
// test decides which files QEMU really needs. The virtio-net option ROM is not
// one of them: the fixture (like A10's renderer) passes romfile= to disable
// it for direct kernel boot, so no distribution ROM path is staged. The
// tests need /dev/kvm, /dev/net/tun, /dev/vhost-net, /dev/vhost-vsock and
// qemu-img (test overlays only), and use host uids 61000-61009.

// Privileged sandbox ids; TestJailerFixtureIDsValid pins their validity.
const (
	jailerPrivID1 = "bx_jjjjjjj2"
	jailerPrivID2 = "bx_jjjjjjj3"
	jailerPrivID3 = "bx_jjjjjjj4"
	jailerPrivID4 = "bx_jjjjjjj5"
)

const (
	jailerPrivUIDMin = 61000
	jailerPrivUIDMax = 61009
	// jailerPrivBoot bounds real guest readiness (guestv1 Ping). A9 gives
	// the guest an unbridged TAP and deliberately no DHCP path, while the
	// image orders bbraft-guest After=network-online.target with
	// systemd-networkd-wait-online and RequiredForOnline=yes: that wait can
	// legitimately take its full 120s timeout before the agent starts, plus
	// the boot itself. Readiness is not weakened and no guest service is
	// masked; the allowance covers that wait.
	jailerPrivBoot = 180 * time.Second
	// jailerPrivGuestPort is bbraft-guest's fixed AF_VSOCK port.
	jailerPrivGuestPort = 10789
	// jailerPrivAppend is the node's direct-boot kernel command line.
	jailerPrivAppend = "root=/dev/vda rw console=ttyS0,115200n8 panic=-1"
	jailerProbeEnv   = "BBRAFT_JAILER_PROBE"
)

type jailerPrivEnv struct {
	imageDir string
	cgroup   string
	ioDevice BlockDevice
	firmware []string
	qemu     string
	cpu      string
}

func jailerRequirePrivileged(t *testing.T) jailerPrivEnv {
	t.Helper()
	if os.Getenv("BBRAFT_TEST_JAILER") != "1" {
		t.Skip("set BBRAFT_TEST_JAILER=1 (root throwaway VM only)")
	}
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("privileged jailer prerequisite: "+format, args...)
	}
	if os.Geteuid() != 0 {
		fail("must run as root")
	}
	env := jailerPrivEnv{
		imageDir: os.Getenv("BBRAFT_TEST_IMAGE_DIR"),
		cgroup:   os.Getenv("BBRAFT_TEST_JAILER_CGROUP"),
		qemu:     cmp.Or(os.Getenv("BBRAFT_TEST_QEMU"), "/usr/bin/qemu-system-x86_64"),
		cpu:      cmp.Or(os.Getenv("BBRAFT_TEST_CPU_MODEL"), "host"),
	}
	if env.imageDir == "" {
		fail("BBRAFT_TEST_IMAGE_DIR is unset")
	}
	for _, name := range []string{"kernel", "initrd", "root.qcow2"} {
		if _, err := os.Stat(filepath.Join(env.imageDir, name)); err != nil {
			fail("image %s: %v", name, err)
		}
	}
	if env.cgroup == "" {
		fail("BBRAFT_TEST_JAILER_CGROUP is unset")
	}
	if _, err := os.Stat(filepath.Join(env.cgroup, "cgroup.controllers")); err != nil {
		fail("cgroup parent: %v", err)
	}
	major, minor, ok := strings.Cut(os.Getenv("BBRAFT_TEST_JAILER_IO_DEVICE"), ":")
	maj, err1 := strconv.ParseUint(major, 10, 32)
	minr, err2 := strconv.ParseUint(minor, 10, 32)
	if !ok || err1 != nil || err2 != nil || maj == 0 {
		fail("BBRAFT_TEST_JAILER_IO_DEVICE must be MAJOR:MINOR of a whole block device")
	}
	env.ioDevice = BlockDevice{Major: uint32(maj), Minor: uint32(minr)}
	for name := range strings.SplitSeq(os.Getenv("BBRAFT_TEST_JAILER_FIRMWARE"), ",") {
		if name = strings.TrimSpace(name); name != "" {
			env.firmware = append(env.firmware, name)
		}
	}
	if len(env.firmware) == 0 {
		fail("BBRAFT_TEST_JAILER_FIRMWARE is empty; list the q35 BIOS files explicitly")
	}
	for _, dev := range []string{env.qemu, "/dev/kvm", "/dev/net/tun", "/dev/vhost-net", "/dev/vhost-vsock"} {
		if _, err := os.Stat(dev); err != nil {
			fail("%s: %v", dev, err)
		}
	}
	if _, err := exec.LookPath("qemu-img"); err != nil {
		fail("qemu-img: %v", err)
	}
	return env
}

type jailerPrivVM struct {
	spec    LaunchSpec
	cid     uint32
	tapName string
	// setup holds the identities of the helper-only descriptors (config
	// memfd, release and status pipes, control dir, serial log) the spawn
	// passed after the caller's files, captured by jailerPrivCaptureSetup.
	setup      []inodeIdentity
	controlDir string
	serialLog  string
	stderr     string // QEMU's captured stderr
	overlay    string
	paths      []string // host paths of every inherited descriptor
}

// jailerPrivTraversable makes dir and its parent traversable (not listable)
// by other uids, so a cross-jail denial is caused by the VM-owned resource's
// own owner and mode, never by a root-only common parent.
func jailerPrivTraversable(t *testing.T, dir string) {
	t.Helper()
	for _, d := range []string{filepath.Dir(dir), dir} {
		if err := os.Chmod(d, 0o711); err != nil {
			t.Fatal(err)
		}
	}
}

// jailerPrivSpec opens real launch resources: a TAP, vhost-net, vhost-vsock,
// the image kernel/initrd, the read-only base and a writable overlay.
func jailerPrivSpec(t *testing.T, env jailerPrivEnv, id string, cid uint32) jailerPrivVM {
	t.Helper()
	dir := t.TempDir()
	jailerPrivTraversable(t, dir)
	tap := jailerOpen(t, "/dev/net/tun", os.O_RDWR)
	tapName := fmt.Sprintf("bbjt%d", cid)
	ifr, err := unix.NewIfreq(tapName)
	if err != nil {
		t.Fatal(err)
	}
	ifr.SetUint16(unix.IFF_TAP | unix.IFF_NO_PI | unix.IFF_VNET_HDR)
	if err := unix.IoctlIfreq(int(tap.Fd()), unix.TUNSETIFF, ifr); err != nil {
		t.Fatalf("TUNSETIFF: %v", err)
	}
	overlay := filepath.Join(dir, "disk.qcow2")
	base := filepath.Join(env.imageDir, "root.qcow2")
	if out, err := exec.CommandContext(t.Context(), "qemu-img", "create", "-q", "-f", "qcow2", "-F", "qcow2", "-b", base, overlay).CombinedOutput(); err != nil {
		t.Fatalf("qemu-img: %v: %s", err, out)
	}
	controlDir := filepath.Join(dir, "control")
	if err := os.Mkdir(controlDir, 0o700); err != nil {
		t.Fatal(err)
	}
	serial := filepath.Join(dir, "serial.log")
	if err := os.WriteFile(serial, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	stderrPath := filepath.Join(dir, "qemu.stderr")
	stderr, err := os.OpenFile(stderrPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stderr.Close()
		if raw := jailerPrivReadBounded(stderrPath); len(raw) != 0 {
			t.Logf("%s QEMU stderr:\n%s", id, raw)
		}
	})
	vm := jailerPrivVM{cid: cid, tapName: tapName, controlDir: controlDir, serialLog: serial, overlay: overlay, stderr: stderrPath}
	open := func(name, p string, flag int) Descriptor {
		vm.paths = append(vm.paths, p)
		return Descriptor{Name: name, File: jailerOpen(t, p, flag)}
	}
	vm.paths = append(vm.paths, "/dev/net/tun")
	files := []Descriptor{
		{Name: TAPName, File: tap},
		open(VhostNetName, "/dev/vhost-net", os.O_RDWR),
		open(KernelName, filepath.Join(env.imageDir, "kernel"), os.O_RDONLY),
		open(InitrdName, filepath.Join(env.imageDir, "initrd"), os.O_RDONLY),
		open(VhostVsockName, "/dev/vhost-vsock", os.O_RDWR),
		open("base", base, os.O_RDONLY),
		open("overlay", overlay, os.O_RDWR),
	}
	spec := LaunchSpec{
		SandboxID: id,
		// A10 schema 2: every launch names a fresh immutable attempt; the
		// record (and Reconcile/Kill/Release through jailerRefOf) keeps it.
		Attempt:    jailerAttempt(),
		Binary:     env.qemu,
		Files:      files,
		Disks:      []string{"base", "overlay"},
		ControlDir: jailerOpen(t, controlDir, os.O_RDONLY),
		SerialLog:  jailerOpen(t, serial, os.O_RDWR),
		// QEMU's (and a failing helper's) diagnostics go to a private file:
		// an *os.File is inherited as fd 2 with no copying goroutine, so the
		// owner's sole Wait never waits on a pipe. Cleanup logs it (bounded)
		// and tests assert on it. Only QEMU's own messages appear; the
		// jailer logs no argv or credentials.
		Stderr: stderr,
		Limits: Limits{
			CPUQuotaPercent: 200,
			MemoryBytes:     1 << 30,
			Pids:            256,
			IO:              IOLimits{Device: env.ioDevice, ReadBPS: 200 << 20, WriteIOPS: 5000},
		},
	}
	args := jailerArgs(spec, []bool{true, true})
	args = jailerArgvReplace(args, "-cpu", 0, env.cpu)
	args = jailerArgvReplace(args, "-m", 0, "512M")
	args = jailerArgvReplace(args, "-append", 0, jailerPrivAppend)
	args = jailerArgvReplace(args, "-device", 2, fmt.Sprintf("vhost-vsock-pci,guest-cid=%d,vhostfd=%d", cid, spec.fdOf(VhostVsockName)))
	spec.Args = args
	vm.spec = spec
	return vm
}

// jailerPrivReadBounded reads at most 64 KiB of a diagnostics file.
func jailerPrivReadBounded(name string) []byte {
	f, err := os.Open(name)
	if err != nil {
		return nil
	}
	defer f.Close()
	raw, _ := io.ReadAll(io.LimitReader(f, 64<<10))
	return raw
}

func jailerPrivManager(t *testing.T, env jailerPrivEnv, stateDir string) *Manager {
	t.Helper()
	m, err := New(Config{
		StateDir:      stateDir,
		CgroupParent:  env.cgroup,
		UIDs:          UIDRange{Min: jailerPrivUIDMin, Max: jailerPrivUIDMax},
		FirmwareFiles: env.firmware,
		Reclaim:       jailerNoReclaim,
		Log:           slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// jailerPrivCaptureSetup records, per spawn, the identities of the helper-
// only descriptors that follow the caller's files in ExtraFiles. It only
// observes the command; production behaviour is unchanged.
func jailerPrivCaptureSetup(m *Manager, files int) *[][]inodeIdentity {
	captured := new([][]inodeIdentity)
	m.hooks.command = func(cmd *exec.Cmd) {
		var ids []inodeIdentity
		for _, f := range cmd.ExtraFiles[files:] {
			identity, err := fileInode(f)
			if err == nil {
				ids = append(ids, identity)
			}
		}
		*captured = append(*captured, ids)
	}
	return captured
}

// jailerPrivCloseAtCleanup closes the Manager current returns at cleanup
// and fails the test if Close fails. Register it before any child owner.
func jailerPrivCloseAtCleanup(t *testing.T, current func() *Manager) {
	t.Helper()
	t.Cleanup(func() {
		if m := current(); m != nil {
			if err := m.Close(); err != nil {
				t.Errorf("cleanup Close: %v", err)
			}
		}
	})
}

// jailerPrivStart starts vm and registers its strict single owner at once,
// including when Start fails with a Child.
func jailerPrivStart(t *testing.T, m *Manager, vm jailerPrivVM) (*Child, *jailerOwner, error) {
	t.Helper()
	child, owner, err := jailerStart(t, m, t.Context(), vm.spec)
	if owner != nil {
		owner.strict = true
	}
	return child, owner, err
}

// jailerPrivUntil polls cond until it holds or the deadline passes. Real
// QEMU and guest startup have no completion signal but observable state.
func jailerPrivUntil(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// jailerPrivPing proves the guest booted: bbraft-guest answers a guestv1
// Ping over the VM's assigned vsock CID within the bounded boot deadline.
func jailerPrivPing(t *testing.T, cid uint32) string {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bbraft-guest",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return vsock.DialContext(ctx, cid, jailerPrivGuestPort, nil)
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := guestv1.NewGuestClient(conn)
	var version string
	jailerPrivUntil(t, fmt.Sprintf("guest Ping on cid %d", cid), jailerPrivBoot, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		resp, err := client.Ping(ctx, &guestv1.PingRequest{})
		if err != nil {
			return false
		}
		version = resp.GetVersion()
		return true
	})
	return version
}

func jailerPrivStatus(t *testing.T, pid int) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]string)
	for line := range strings.SplitSeq(string(raw), "\n") {
		if key, value, ok := strings.Cut(line, ":"); ok {
			out[key] = strings.TrimSpace(value)
		}
	}
	return out
}

// jailerProbe runs in a re-executed test binary with a VM's QEMU host
// uid/gid, no groups and no capabilities. Each argument is "mode:path"; it
// prints one "ok" or error line per argument. It is never run inside or
// passed to QEMU.
func jailerProbe(args []string) int {
	for _, arg := range args {
		mode, p, _ := strings.Cut(arg, ":")
		var err error
		switch mode {
		case "r", "rw":
			flag := os.O_RDONLY
			if mode == "rw" {
				flag = os.O_RDWR
			}
			var f *os.File
			if f, err = os.OpenFile(p, flag, 0); err == nil {
				f.Close()
			}
		case "dir":
			_, err = os.ReadDir(p)
		case "chmod":
			err = os.Chmod(p, 0o666)
		case "unix":
			var c net.Conn
			if c, err = net.Dial("unix", p); err == nil {
				c.Close()
			}
		default:
			err = fmt.Errorf("bad probe mode %q", mode)
		}
		if err != nil {
			errno, ok := errors.AsType[syscall.Errno](err)
			switch {
			case ok && errno == syscall.EACCES:
				fmt.Println("EACCES")
			case ok && errno == syscall.EPERM:
				fmt.Println("EPERM")
			case ok:
				fmt.Printf("errno %d: %v\n", int(errno), errno)
			default:
				fmt.Printf("error: %v\n", err)
			}
			continue
		}
		fmt.Println("ok")
	}
	return 0
}

// jailerPrivProbe opens each "mode:path" as a VM's QEMU HOST uid (its uid
// and gid, empty groups, no capabilities) and returns one result per probe.
func jailerPrivProbe(t *testing.T, uid uint32, probes ...string) []string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), exe, probes...)
	cmd.Env = []string{jailerProbeEnv + "=1"}
	cmd.Dir = "/"
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: uid, Groups: []uint32{}}}
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("probe as host uid %d: %v", uid, err)
	}
	results := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(results) != len(probes) {
		t.Fatalf("probe returned %q for %d probes", results, len(probes))
	}
	return results
}

// jailerPrivConfinement checks one running jailed QEMU from the host.
func jailerPrivConfinement(t *testing.T, env jailerPrivEnv, stateDir string, vm jailerPrivVM, child *Child) {
	id := vm.spec.SandboxID
	pid := child.Cmd.Process.Pid
	record := child.Identity()
	jailRoot := filepath.Join(stateDir, jailsDir, id, "root")

	t.Run("credentials", func(t *testing.T) {
		st := jailerPrivStatus(t, pid)
		want := strconv.Itoa(int(record.UID))
		for _, key := range []string{"Uid", "Gid"} {
			for v := range strings.FieldsSeq(st[key]) {
				if v != want {
					t.Fatalf("%s = %q, want all %s", key, st[key], want)
				}
			}
		}
		if st["Groups"] != "" {
			t.Fatalf("supplementary groups %q", st["Groups"])
		}
		for _, key := range []string{"CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"} {
			if v, err := strconv.ParseUint(st[key], 16, 64); err != nil || v != 0 {
				t.Fatalf("%s = %q", key, st[key])
			}
		}
		if st["NoNewPrivs"] != "1" || st["Seccomp"] != "2" {
			t.Fatalf("NoNewPrivs=%q Seccomp=%q", st["NoNewPrivs"], st["Seccomp"])
		}
		if record.UID < jailerPrivUIDMin || record.UID > jailerPrivUIDMax {
			t.Fatalf("uid %d outside the configured range", record.UID)
		}
	})

	t.Run("private device nodes", func(t *testing.T) {
		// Fresh VM-owned 0600 nodes with the host rdevs; never the host nodes.
		for _, name := range []string{"kvm", "urandom", "null"} {
			want, err := fixedDevice("/dev/" + name)
			if err != nil {
				t.Fatal(err)
			}
			var st, host unix.Stat_t
			if err := unix.Lstat(fmt.Sprintf("/proc/%d/root/dev/%s", pid, name), &st); err != nil {
				t.Fatalf("jail /dev/%s: %v", name, err)
			}
			if err := unix.Stat("/dev/"+name, &host); err != nil {
				t.Fatal(err)
			}
			if st.Mode&unix.S_IFMT != unix.S_IFCHR || st.Rdev != want || st.Uid != record.UID || st.Gid != record.UID || st.Mode&0o777 != 0o600 {
				t.Fatalf("jail /dev/%s mode %#o rdev %#x owner %d:%d, want char 0600 rdev %#x owned by %d", name, st.Mode, st.Rdev, st.Uid, st.Gid, want, record.UID)
			}
			if st.Dev == host.Dev && st.Ino == host.Ino {
				t.Fatalf("jail /dev/%s is the host node itself", name)
			}
		}
	})

	t.Run("namespaces and root", func(t *testing.T) {
		for _, ns := range []string{"mnt", "pid", "ipc"} {
			self, _ := os.Readlink("/proc/self/ns/" + ns)
			got, err := os.Readlink(fmt.Sprintf("/proc/%d/ns/%s", pid, ns))
			if err != nil || got == self {
				t.Fatalf("%s namespace %q not private (host %q, %v)", ns, got, self, err)
			}
		}
		root, err := os.Readlink(fmt.Sprintf("/proc/%d/root", pid))
		if err != nil || root != jailRoot {
			t.Fatalf("root %q (%v), want %q", root, err, jailRoot)
		}
		// QEMU's own /proc shows its PID namespace: only itself, as pid 1.
		entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/root/proc", pid))
		if err != nil {
			t.Fatal(err)
		}
		var pids []string
		for _, e := range entries {
			if _, err := strconv.Atoi(e.Name()); err == nil {
				pids = append(pids, e.Name())
			}
		}
		if !slices.Equal(pids, []string{"1"}) {
			t.Fatalf("QEMU's PID namespace shows %v, want only [1]", pids)
		}
		mounts, err := os.ReadFile(fmt.Sprintf("/proc/%d/mountinfo", pid))
		if err != nil {
			t.Fatal(err)
		}
		var rootRO bool
		for line := range strings.SplitSeq(string(mounts), "\n") {
			fields := strings.Fields(line)
			if len(fields) > 5 && fields[4] == "/" {
				rootRO = slices.Contains(strings.Split(fields[5], ","), "ro")
			}
		}
		if !rootRO {
			t.Fatal("the jail root is not mounted read-only in QEMU's namespace")
		}
		// The pinned control dir and serial log are the parent's detached
		// clones: nosuid, nodev, noexec and private (no shared:/master:).
		for _, point := range []string{"/control", "/serial.log"} {
			found := false
			for line := range strings.SplitSeq(string(mounts), "\n") {
				fields := strings.Fields(line)
				if len(fields) < 7 || fields[4] != point {
					continue
				}
				found = true
				opts := strings.Split(fields[5], ",")
				for _, want := range []string{"nosuid", "nodev", "noexec"} {
					if !slices.Contains(opts, want) {
						t.Errorf("%s mount options %q lack %s", point, fields[5], want)
					}
				}
				for _, optional := range fields[6:] {
					if optional == "-" {
						break
					}
					if strings.HasPrefix(optional, "shared:") || strings.HasPrefix(optional, "master:") {
						t.Errorf("%s mount propagation %q is not private", point, optional)
					}
				}
			}
			if !found {
				t.Errorf("no %s mount in QEMU's namespace", point)
			}
		}
	})

	t.Run("descriptors", func(t *testing.T) {
		entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
		if err != nil {
			t.Fatal(err)
		}
		allowed := append(slices.Clone(vm.paths), "/dev/null", vm.serialLog)
		// fd 2 is this VM's own test stderr fixture file (LaunchSpec.Stderr).
		// It is admitted only as fd 2 and only when it is exactly that inode;
		// no other stderr path or descriptor is allowed by it.
		var stderrFixture unix.Stat_t
		if err := unix.Stat(vm.stderr, &stderrFixture); err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, entry.Name()))
			if err != nil {
				continue // closed meanwhile
			}
			if entry.Name() == "2" {
				var st unix.Stat_t
				if err := unix.Stat(fmt.Sprintf("/proc/%d/fd/2", pid), &st); err == nil &&
					st.Dev == stderrFixture.Dev && st.Ino == stderrFixture.Ino {
					continue
				}
			}
			switch {
			case strings.HasPrefix(target, "anon_inode:"), strings.HasPrefix(target, "socket:"),
				strings.HasPrefix(target, "pipe:"), strings.HasPrefix(target, "/memfd:") && !strings.Contains(target, "bbraft-jailer"):
			case strings.HasPrefix(target, jailRoot+"/"), strings.HasPrefix(target, vm.controlDir+"/"):
			case slices.Contains(allowed, strings.TrimSuffix(target, " (deleted)")):
			default:
				t.Errorf("QEMU fd %s -> %q is outside the allowlist", entry.Name(), target)
			}
		}
		// The live descriptors carry the intended devices: fd 3 the TAP
		// (its tun fdinfo names the interface where the kernel reports it),
		// fd 4 vhost-net, and the vhost-vsock fd.
		for _, role := range []struct {
			fd   int
			node string
		}{
			{3, "/dev/net/tun"},
			{4, "/dev/vhost-net"},
			{vm.spec.fdOf(VhostVsockName), "/dev/vhost-vsock"},
		} {
			want, err := fixedDevice(role.node)
			if err != nil {
				t.Fatal(err)
			}
			var st unix.Stat_t
			if err := unix.Stat(fmt.Sprintf("/proc/%d/fd/%d", pid, role.fd), &st); err != nil {
				t.Fatalf("QEMU fd %d: %v", role.fd, err)
			}
			if st.Mode&unix.S_IFMT != unix.S_IFCHR || st.Rdev != want {
				t.Fatalf("QEMU fd %d is rdev %#x, want %s (%#x)", role.fd, st.Rdev, role.node, want)
			}
		}
		info, err := os.ReadFile(fmt.Sprintf("/proc/%d/fdinfo/3", pid))
		if err != nil {
			t.Fatal(err)
		}
		iff := ""
		for line := range strings.SplitSeq(string(info), "\n") {
			if v, ok := strings.CutPrefix(line, "iff:"); ok {
				iff = strings.TrimSpace(v)
			}
		}
		switch iff {
		case vm.tapName:
		case "":
			t.Logf("kernel reports no iff in tun fdinfo; TAP identity proven by rdev only")
		default:
			t.Fatalf("QEMU fd 3 is TAP %q, want %q", iff, vm.tapName)
		}
		// No helper-only descriptor survived the exec. The serial log inode
		// is open exactly once: QEMU's own chardev open of /serial.log.
		if len(vm.setup) != 5 {
			t.Fatalf("captured %d helper descriptors, want 5", len(vm.setup))
		}
		counts := make(map[inodeIdentity]int)
		for _, entry := range entries {
			var st unix.Stat_t
			if err := unix.Stat(fmt.Sprintf("/proc/%d/fd/%s", pid, entry.Name()), &st); err != nil {
				continue
			}
			counts[inodeIdentity{Dev: st.Dev, Ino: st.Ino}]++
		}
		// The cgroup directory fd used for CLONE_INTO_CGROUP stays with the
		// jailer; QEMU must not hold it.
		cgroupInfo, err := os.Stat(filepath.Join(env.cgroup, cgroupName(id)))
		if err != nil {
			t.Fatal(err)
		}
		if st, ok := cgroupInfo.Sys().(*syscall.Stat_t); ok {
			if got := counts[inodeIdentity{Dev: st.Dev, Ino: st.Ino}]; got != 0 {
				t.Errorf("QEMU holds %d descriptors of its cgroup directory", got)
			}
		}
		for i, identity := range vm.setup {
			want := 0
			if i == 4 { // the serial log
				want = 1
			}
			if got := counts[identity]; got != want {
				t.Errorf("helper descriptor #%d (%v) is open %d times in QEMU, want %d", i, identity, got, want)
			}
		}
	})

	t.Run("cgroup", func(t *testing.T) {
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(strings.TrimSpace(string(raw)), "/"+cgroupName(id)) {
			t.Fatalf("QEMU cgroup %q is not %s", raw, cgroupName(id))
		}
		dir := filepath.Join(env.cgroup, cgroupName(id))
		for file, value := range map[string]string{
			"cpu.max":    "200000 100000",
			"memory.max": strconv.FormatUint(1<<30, 10),
			"pids.max":   "256",
		} {
			got, err := os.ReadFile(filepath.Join(dir, file))
			if err != nil || strings.TrimSpace(string(got)) != value {
				t.Fatalf("%s = %q (%v), want %q", file, got, err, value)
			}
		}
		io, err := os.ReadFile(filepath.Join(dir, "io.max"))
		wantIO := fmt.Sprintf("%d:%d rbps=%d", env.ioDevice.Major, env.ioDevice.Minor, 200<<20)
		if err != nil || !strings.HasPrefix(strings.TrimSpace(string(io)), wantIO) || !strings.Contains(string(io), "wiops=5000") {
			t.Fatalf("io.max = %q (%v), want %s ... wiops=5000", io, err, wantIO)
		}
	})

	t.Run("qmp peer is the direct child", func(t *testing.T) {
		sock := filepath.Join(vm.controlDir, "qmp.sock")
		jailerPrivUntil(t, "QMP socket", 30*time.Second, func() bool { _, err := os.Stat(sock); return err == nil })
		conn, err := net.DialTimeout("unix", sock, 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		raw, err := conn.(*net.UnixConn).SyscallConn()
		if err != nil {
			t.Fatal(err)
		}
		var cred *unix.Ucred
		var credErr error
		if err := raw.Control(func(fd uintptr) { cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil || credErr != nil {
			t.Fatal(errors.Join(err, credErr))
		}
		if int(cred.Pid) != pid || cred.Uid != record.UID || cred.Gid != record.UID {
			t.Fatalf("QMP peer %+v, want pid %d uid/gid %d", cred, pid, record.UID)
		}
		greeting, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil || !strings.Contains(greeting, `"QMP"`) {
			t.Fatalf("QMP greeting %q (%v)", greeting, err)
		}
	})

	t.Run("guest boots with staged runtime and firmware", func(t *testing.T) {
		if version := jailerPrivPing(t, vm.cid); version == "" {
			t.Fatal("guest Ping returned no version")
		}
		// The guest booted from the disks, so QEMU long finished opening
		// them and its lock probe: it found /dev/null and uses OFD locks.
		if raw := jailerPrivReadBounded(vm.stderr); bytes.Contains(raw, []byte("OFD lock probing")) {
			t.Fatalf("QEMU could not probe OFD locks: %s", raw)
		}
		raw, _ := os.ReadFile(vm.serialLog)
		if !bytes.Contains(raw, []byte("Linux version")) {
			t.Fatal("no kernel banner on the serial log")
		}
		for _, f := range env.firmware {
			if _, err := os.Stat(filepath.Join(jailRoot, f)); err != nil {
				t.Fatalf("firmware %s not staged: %v", f, err)
			}
		}
	})
}

// TestPrivilegedJailedQEMU runs two jailed QEMUs at once with distinct host
// uids, checks each one's confinement and real guest boot, and checks the
// QEMU host-uid boundary between them, then kills, reaps and releases both.
func TestPrivilegedJailedQEMU(t *testing.T) {
	env := jailerRequirePrivileged(t)
	stateDir := t.TempDir()
	m := jailerPrivManager(t, env, stateDir)
	// Registered before any child owner, so (LIFO) every owner's kill, sole
	// Wait and Release run while this Manager is still open.
	jailerPrivCloseAtCleanup(t, func() *Manager { return m })
	cid := uint32(10000 + os.Getpid()%40000)
	vmA := jailerPrivSpec(t, env, jailerPrivID1, cid)
	vmB := jailerPrivSpec(t, env, jailerPrivID4, cid+1)
	setups := jailerPrivCaptureSetup(m, len(vmA.spec.Files))
	childA, ownerA, err := jailerPrivStart(t, m, vmA)
	if err != nil {
		t.Fatal(err)
	}
	childB, ownerB, err := jailerPrivStart(t, m, vmB)
	if err != nil {
		t.Fatal(err)
	}
	if len(*setups) != 2 {
		t.Fatalf("captured %d spawns, want 2", len(*setups))
	}
	vmA.setup, vmB.setup = (*setups)[0], (*setups)[1]
	uidA, uidB := childA.Identity().UID, childB.Identity().UID
	if uidA == uidB {
		t.Fatalf("two live VMs share host uid %d", uidA)
	}
	t.Run("A", func(t *testing.T) { jailerPrivConfinement(t, env, stateDir, vmA, childA) })
	t.Run("B", func(t *testing.T) { jailerPrivConfinement(t, env, stateDir, vmB, childB) })

	t.Run("QEMU host uid boundary", func(t *testing.T) {
		positive := []string{"rw:" + vmA.overlay, "rw:" + vmA.serialLog, "dir:" + vmA.controlDir}
		denied := []string{
			// B's resources, denied by their own owner and mode.
			"r:" + vmB.overlay, "r:" + vmB.serialLog, "dir:" + vmB.controlDir,
			"unix:" + filepath.Join(vmB.controlDir, "qmp.sock"),
			// B's jail (root-only by design) and B's QEMU view.
			"dir:" + filepath.Join(stateDir, jailsDir, jailerPrivID4),
			fmt.Sprintf("dir:/proc/%d/root", childB.Cmd.Process.Pid),
		}
		// Every probed path exists (checked as root), so a denial can only
		// be a permission decision, never a missing path.
		for _, probe := range append(slices.Clone(positive), denied...) {
			_, p, _ := strings.Cut(probe, ":")
			if _, err := os.Lstat(p); err != nil {
				t.Fatalf("probe target %s: %v", p, err)
			}
		}
		got := jailerPrivProbe(t, uidA, append(slices.Clone(positive), denied...)...)
		// Positive control: A's QEMU host uid reaches A's own resources
		// through the same traversable parents B's resources sit under.
		for i := range positive {
			if got[i] != "ok" {
				t.Fatalf("host uid %d cannot use its own %s: %s; the denials would be vacuous", uidA, positive[i], got[i])
			}
		}
		for i, probe := range denied {
			if r := got[len(positive)+i]; r != "EACCES" && r != "EPERM" {
				t.Errorf("host uid %d probing VM B %s = %s, want EACCES or EPERM", uidA, probe, r)
			}
		}
	})

	for _, o := range []*jailerOwner{ownerA, ownerB} {
		if err := o.child.Kill(); err != nil {
			t.Fatal(err)
		}
		o.waitKilled(t)
		if err := o.child.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{jailerPrivID1, jailerPrivID4} {
		for _, p := range []string{filepath.Join(stateDir, jailsDir, id), filepath.Join(env.cgroup, cgroupName(id)), filepath.Join(stateDir, recordPath(id))} {
			if _, err := os.Lstat(p); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s survived Release: %v", p, err)
			}
		}
	}
}

// TestPrivilegedRestartRetention closes the Manager while QEMU runs, then a
// new Manager retains, kills by recovered identity and reconciles it away.
func TestPrivilegedRestartRetention(t *testing.T) {
	env := jailerRequirePrivileged(t)
	stateDir := t.TempDir()
	// Restart cleanup plan: one cleanup, registered before the child owner,
	// closes whichever Manager currently owns StateDir. The owner releases
	// through that same current Manager after the deliberate restart, so
	// its cleanup always runs against an open owner.
	var current *Manager
	jailerPrivCloseAtCleanup(t, func() *Manager { return current })
	current = jailerPrivManager(t, env, stateDir)
	m := current
	vm := jailerPrivSpec(t, env, jailerPrivID2, uint32(60000+os.Getpid()%5000))
	child, owner, err := jailerPrivStart(t, m, vm)
	if owner != nil {
		owner.via = func() *Manager { return current }
	}
	if err != nil {
		t.Fatal(err)
	}
	current = nil
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m2 := jailerPrivManager(t, env, stateDir)
	current = m2
	report, err := m2.Reconcile(t.Context())
	if err != nil || !slices.Equal(report.Retained, []string{jailerPrivID2}) {
		t.Fatalf("Reconcile = %+v, %v; want retained", report, err)
	}
	if err := m2.Kill(t.Context(), jailerRefOf(m2, jailerPrivID2)); err != nil {
		t.Fatal(err)
	}
	owner.waitKilled(t)
	if err := child.Release(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("old Child.Release = %v, want ErrClosed", err)
	}
	report, err = m2.Reconcile(t.Context())
	if err != nil || !slices.Equal(report.Released, []string{jailerPrivID2}) {
		t.Fatalf("Reconcile = %+v, %v; want released", report, err)
	}
	if _, err := os.Lstat(filepath.Join(env.cgroup, cgroupName(jailerPrivID2))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cgroup survived: %v", err)
	}
}

// TestPrivilegedFaultRetention fails the launch with the real helper and
// real cgroup at the pin boundary (before identity capture, PID 0) and at
// the release boundary (after commit). The membership witness is read inside
// the boundary hook, before Start kills the helper: clone3 made it a member
// at birth. The caller reaps it, and only then is every resource released.
func TestPrivilegedFaultRetention(t *testing.T) {
	env := jailerRequirePrivileged(t)
	for _, tt := range []struct {
		stage launchStage
		id    string
		cid   uint32
	}{
		{stagePin, jailerPrivID3, uint32(70000 + os.Getpid()%5000)},
		{stageRelease, jailerPrivID2, uint32(75000 + os.Getpid()%5000)},
	} {
		t.Run(string(tt.stage), func(t *testing.T) {
			stateDir := t.TempDir()
			m := jailerPrivManager(t, env, stateDir)
			jailerPrivCloseAtCleanup(t, func() *Manager { return m })
			var members []string
			var witnessErr error
			m.hooks.stage = func(stage launchStage) error {
				if stage != tt.stage {
					return nil
				}
				raw, err := os.ReadFile(filepath.Join(env.cgroup, cgroupName(tt.id), "cgroup.procs"))
				members, witnessErr = strings.Fields(string(raw)), err
				return errJailerInjected
			}
			vm := jailerPrivSpec(t, env, tt.id, tt.cid)
			child, owner, err := jailerPrivStart(t, m, vm)
			if !errors.Is(err, errJailerInjected) || child == nil {
				t.Fatalf("Start = %v, %v", child, err)
			}
			if witnessErr != nil || !slices.Equal(members, []string{strconv.Itoa(child.Cmd.Process.Pid)}) {
				t.Fatalf("cgroup members at %s %v (%v), want only helper %d", tt.stage, members, witnessErr, child.Cmd.Process.Pid)
			}
			owner.waitKilled(t)
			if err := child.Release(t.Context()); err != nil {
				t.Fatal(err)
			}
			for _, p := range []string{filepath.Join(stateDir, jailsDir, tt.id), filepath.Join(env.cgroup, cgroupName(tt.id))} {
				if _, err := os.Lstat(p); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("%s survived: %v", p, err)
				}
			}
		})
	}
}
